import json
import os
from pathlib import Path
from typing import Any, Dict, List, Literal

import pandas as pd
from dotenv import load_dotenv
from openai import OpenAI
from pydantic import BaseModel, Field

# --------------------
# Configuration
# --------------------

BASE_DIR = Path(__file__).resolve().parents[1]

INPUT_PAGES_CSV = BASE_DIR / "data" / "intermediate" / "llm_input_pages.csv"
PROMPT_FILE = BASE_DIR / "src" / "prompt_opportunity_extraction.txt"
OUT_CSV = BASE_DIR / "data" / "intermediate" / "opportunities_raw.csv"

MODEL = "gpt-4o-mini"

# Pilot settings
MAX_PAGES = 10
MAX_CHARS = 18000  # truncate long pages to keep cost predictable


# --------------------
# Structured output schemas
# --------------------

class Opportunity(BaseModel):
    opportunity_name: str = ""
    opportunity_url: str = ""
    opportunity_type: Literal[
        "research", "education", "QI", "fellowship", "travel", "other", "unclear"
    ] = "unclear"
    eligibility_us: Literal["yes", "no", "unclear"] = "unclear"
    eligibility_text: str = ""
    deadline_text: str = ""
    award_amount_text: str = ""
    keywords_phrases: List[str] = Field(default_factory=list)
    summary_1_2_sentences: str = ""
    evidence_snippets: List[str] = Field(default_factory=list)
    confidence: Literal["low", "med", "high"] = "low"


class ExtractionResult(BaseModel):
    is_funding_related: bool = False
    opportunities: List[Opportunity] = Field(default_factory=list)


# --------------------
# Helpers
# --------------------

def truncate_text(text: str, max_chars: int) -> str:
    if not text:
        return ""
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + "\n\n[TRUNCATED]"


def load_prompt_template() -> str:
    return PROMPT_FILE.read_text(encoding="utf-8")


# --------------------
# Main
# --------------------

def main():
    load_dotenv()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise SystemExit("OPENAI_API_KEY not found in .env")

    client = OpenAI(api_key=api_key)

    pages = pd.read_csv(INPUT_PAGES_CSV)

    prompt_template = load_prompt_template()

    pages = pages.head(MAX_PAGES).copy()

    rows_out: List[Dict[str, Any]] = []

    for _, row in pages.iterrows():
        json_path = Path(str(row["json_path"]))
        if not json_path.exists():
            json_path = BASE_DIR / json_path

        if not json_path.exists():
            rows_out.append({
                "foundation_id": row.get("foundation_id", ""),
                "foundation_name": row.get("foundation_name", ""),
                "source_url": row.get("url", ""),
                "error": "missing_json_file"
            })
            continue

        data = json.loads(json_path.read_text(encoding="utf-8"))

        # Foundation name fallback
        raw_name = row.get("foundation_name", "")
        fname = "" if pd.isna(raw_name) else str(raw_name)
        if not fname:
            fname = str(data.get("foundation_name", ""))

        fid = str(row.get("foundation_id", ""))
        source_url = str(row.get("url", ""))

        extracted_text = truncate_text(
            data.get("extracted_text", ""), MAX_CHARS
        )

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

            if not parsed.is_funding_related or not parsed.opportunities:
                continue

            for opp in parsed.opportunities:
                rows_out.append({
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
                    "error": ""
                })

        except Exception as e:
            rows_out.append({
                "foundation_id": fid,
                "foundation_name": fname,
                "source_url": source_url,
                "error": f"parse_or_api_error: {e}"
            })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows_out).to_csv(OUT_CSV, index=False)

    print(f"Saved: {OUT_CSV}")
    print(f"Rows written: {len(rows_out)}")


if __name__ == "__main__":
    main()
