import json
import time
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from tenacity import retry, wait_exponential, stop_after_attempt
from tqdm import tqdm

from openai import OpenAI

# --------------------
# CONFIG
# --------------------
XLSX_PATH = Path("/home/lisa/projects/foundation-opportunity-scraper/data/output/foundations_and_opportunities_PREFILTERED.xlsx")
SHEET = "Opportunities_prefiltered"

# Resumable output (checkpoint file). Safe to delete if you want to start over.
OUTPUT_PATH = Path("classified_opportunities_checkpoint.csv")

# How often to save progress
SAVE_EVERY = 25

# LLM model (you can change later if needed)
MODEL = "gpt-4.1-mini"

TEXT_COLUMNS = [
    "opportunity_name",
    "summary_1_2_sentences",
    "award_amount_text",
    "opportunity_type",
    "eligibility_text",
    "deadline_text",
    "evidence_snippets",
    "opportunity_url",
    "source_url",
]

load_dotenv()
client = OpenAI()

# --------------------
# PROMPT
# --------------------
SYSTEM_PROMPT = """
You are a strict funding-opportunity classifier.
Goal: minimize false positives. It is OK to answer "unclear".

Classify whether the opportunity is REAL PROSPECTIVE FUNDING that provides money:
- YES: grants, fellowships, stipends, travel awards, salary support, funded programs with awards, paid research funding.
- NO: recognition-only awards, honorary titles, certificates, informational program pages with no application/funding, advocacy/awareness pages, listings of past recipients only, "call for nominations" with no money, conferences with no funding, membership benefits only.
- UNCLEAR: ambiguous, missing money info, or could be funding but not explicit.

Rules:
- If there is no explicit or strongly implied money/funding, prefer UNCLEAR over YES.
- If the text indicates honor/recognition without funding, answer NO.
- Do not invent details. Use only the provided row text.
Return JSON only, matching the schema exactly.
"""

USER_TEMPLATE = """
Classify this row.

Return JSON with:
- is_real_funding: "yes" | "no" | "unclear"
- reason: 1 sentence, specific to the row
- confidence: always "low"

Row fields:
{row_text}
"""

def row_to_text(row: pd.Series) -> str:
    parts = []
    for col in TEXT_COLUMNS:
        val = row.get(col, "")
        if pd.isna(val):
            val = ""
        val = str(val).strip()
        if val:
            parts.append(f"{col}: {val}")
    return "\n".join(parts) if parts else "(no text fields present)"

@retry(wait=wait_exponential(min=1, max=20), stop=stop_after_attempt(5))
def classify_with_llm(row_text: str) -> dict:
    resp = client.chat.completions.create(
        model=MODEL,
        temperature=0,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT.strip()},
            {"role": "user", "content": USER_TEMPLATE.format(row_text=row_text).strip()},
        ],
        response_format={"type": "json_object"},
    )
    content = resp.choices[0].message.content
    data = json.loads(content)

    # Hard validation / normalization (defensive)
    is_real = str(data.get("is_real_funding", "")).strip().lower()
    if is_real not in {"yes", "no", "unclear"}:
        is_real = "unclear"

    reason = str(data.get("reason", "")).strip()
    if not reason:
        reason = "Insufficient information in the row text to determine whether money is provided."

    confidence = "low"  # forced

    return {"is_real_funding": is_real, "reason": reason, "confidence": confidence}

def load_data() -> pd.DataFrame:
    return pd.read_excel(XLSX_PATH, sheet_name=SHEET)

def load_checkpoint() -> pd.DataFrame | None:
    if OUTPUT_PATH.exists():
        return pd.read_csv(OUTPUT_PATH)
    return None

def save_checkpoint(df_out: pd.DataFrame):
    df_out.to_csv(OUTPUT_PATH, index=False)

def main():
    df = load_data()

    ck = load_checkpoint()
    if ck is not None and len(ck) > 0:
        print(f"Resuming from checkpoint: {len(ck)} already classified")
        if "dedupe_key" in df.columns and "dedupe_key" in ck.columns:
            done = set(ck["dedupe_key"].astype(str).fillna(""))
            mask = ~df["dedupe_key"].astype(str).fillna("").isin(done)
            todo = df[mask].copy()
        else:
            todo = df.iloc[len(ck):].copy()
    else:
        print(f"Starting fresh: {len(df)} rows to classify")
        todo = df.copy()
        ck = pd.DataFrame()

    out_rows = []
    processed = 0

    for _, row in tqdm(todo.iterrows(), total=len(todo)):
        row_text = row_to_text(row)

        try:
            result = classify_with_llm(row_text)
        except Exception as e:
            # conservative failure mode: UNCLEAR
            result = {
                "is_real_funding": "unclear",
                "reason": f"LLM error; marked unclear. Error: {type(e).__name__}",
                "confidence": "low",
            }

        combined = row.to_dict()
        combined.update(result)

        out_rows.append(combined)
        processed += 1

        if processed % SAVE_EVERY == 0:
            df_new = pd.DataFrame(out_rows)
            if ck is not None and len(ck) > 0:
                df_out = pd.concat([ck, df_new], ignore_index=True)
            else:
                df_out = df_new
            save_checkpoint(df_out)
            ck = df_out
            out_rows = []
            time.sleep(0.2)

    if out_rows:
        df_new = pd.DataFrame(out_rows)
        if ck is not None and len(ck) > 0:
            df_out = pd.concat([ck, df_new], ignore_index=True)
        else:
            df_out = df_new
        save_checkpoint(df_out)

    print(f"Done. Wrote checkpoint file: {OUTPUT_PATH.resolve()}")

if __name__ == "__main__":
    main()
