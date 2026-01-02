import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Literal, Set

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field

BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_PAGES_CSV = BASE_DIR / "data" / "intermediate" / "llm_input_pages.csv"
PROMPT_FILE = BASE_DIR / "src" / "prompt_opportunity_extraction.txt"

OUT_CSV = BASE_DIR / "data" / "output" / "opportunities.csv"
PROGRESS_FILE = BASE_DIR / "data" / "intermediate" / "extraction_progress.txt"

MODEL = "gpt-4o-mini"

# Controls
MAX_CHARS = 18000
BATCH_SIZE = 25
SLEEP_SECONDS = 0.2  # tiny pause to reduce burstiness

class Opportunity(BaseModel):
    opportunity_name: str = ""
    opportunity_url: str = ""
    opportunity_type: Literal["research","education","QI","fellowship","travel","other","unclear"] = "unclear"
    eligibility_us: Literal["yes","no","unclear"] = "unclear"
    eligibility_text: str = ""
    deadline_text: str = ""
    award_amount_text: str = ""
    keywords_phrases: List[str] = Field(default_factory=list)
    summary_1_2_sentences: str = ""
    evidence_snippets: List[str] = Field(default_factory=list)
    confidence: Literal["low","med","high"] = "low"

class ExtractionResult(BaseModel):
    is_funding_related: bool = False
    opportunities: List[Opportunity] = Field(default_factory=list)

def truncate_text(text: str, max_chars: int) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n[TRUNCATED]"

def load_prompt_template() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")

def load_done_set() -> Set[str]:
    """Track processed json_path values so we can resume safely."""
    done = set()
    if PROGRESS_FILE.exists():
        for line in PROGRESS_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                done.add(line)
    return done

def mark_done(json_path: str) -> None:
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(PROGRESS_FILE, "a", encoding="utf-8") as f:
        f.write(json_path + "\n")

def append_rows(rows: List[Dict[str, Any]]) -> None:
    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    df = pd.DataFrame(rows)
    header = not OUT_CSV.exists()
    df.to_csv(OUT_CSV, mode="a", header=header, index=False)

def main():
    load_dotenv()
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY not found in .env")

    client = OpenAI(api_key=api_key)

    pages = pd.read_csv(INPUT_PAGES_CSV)
    prompt_template = load_prompt_template()

    done = load_done_set()

    total = len(pages)
    processed = 0
    written = 0

    batch_rows: List[Dict[str, Any]] = []

    for idx, row in pages.iterrows():
        json_path_str = str(row["json_path"])
        if json_path_str in done:
            continue

        json_path = Path(json_path_str)
        if not json_path.exists():
            json_path = BASE_DIR / json_path

        fid = str(row.get("foundation_id", ""))
        raw_name = row.get("foundation_name", "")
        source_url = str(row.get("url", ""))

        if not json_path.exists():
            # record the fact we attempted it, so we don't loop forever
            mark_done(json_path_str)
            done.add(json_path_str)
            continue

        data = json.loads(json_path.read_text(encoding="utf-8"))

        fname = "" if pd.isna(raw_name) else str(raw_name)
        if not fname:
            fname = str(data.get("foundation_name", ""))

        extracted_text = truncate_text(data.get("extracted_text", ""), MAX_CHARS)

        prompt = (
            prompt_template
            .replace("<<foundation_name>>", fname)
            .replace("<<foundation_id>>", fid)
            .replace("<<source_url>>", source_url)
            .replace("<<text>>", extracted_text)
        )

        try:
            resp = client.responses.parse(
                model=MODEL,
                input=prompt,
                text_format=ExtractionResult,
            )
            parsed = resp.output_parsed

            if parsed.is_funding_related and parsed.opportunities:
                for opp in parsed.opportunities:
                    batch_rows.append({
                        "foundation_id": fid,
                        "foundation_name": fname,
                        "source_url": source_url,
                        "opportunity_name": opp.opportunity_name,
                        "opportunity_url": opp.opportunity_url,
                        "opportunity_type": opp.opportunity_type,
                        "eligibility_us": opp.eligibility_us,
                        "eligibility_text": opp.eligibility_text,
                        "deadline_text": opp.deadline_text,
                        "award_amount_text": opp.award_amount_text,
                        "keywords_phrases": "|".join(opp.keywords_phrases),
                        "summary_1_2_sentences": opp.summary_1_2_sentences,
                        "evidence_snippets": " | ".join(opp.evidence_snippets),
                        "confidence": opp.confidence,
                    })

        except Exception as e:
            # For full run, we just skip but mark done; errors can be reviewed later by rerunning a smaller set
            pass

        # Mark this page as processed regardless of outcome
        mark_done(json_path_str)
        done.add(json_path_str)

        processed += 1

        # Flush batch to disk periodically
        if len(batch_rows) >= BATCH_SIZE:
            append_rows(batch_rows)
            written += len(batch_rows)
            batch_rows = []
            print(f"Processed pages: {processed} | Rows written so far: {written}")

        time.sleep(SLEEP_SECONDS)

    # flush any remaining rows
    if batch_rows:
        append_rows(batch_rows)
        written += len(batch_rows)

    print("DONE")
    print(f"Total pages in selection: {total}")
    print(f"Pages processed this run: {processed}")
    print(f"Total opportunity rows written: {written}")
    print(f"Output: {OUT_CSV}")
    print(f"Progress log: {PROGRESS_FILE}")

if __name__ == "__main__":
    main()
