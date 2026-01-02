import re
from pathlib import Path
import pandas as pd

INPUT = Path("classified_opportunities_checkpoint.csv")
OUTPUT = Path("classified_opportunities_checkpoint_rule_demoted_v2.csv")

# Only applies to rows currently labeled "unclear"
TARGET_LABEL = "unclear"

# Patterns that strongly indicate NOT a prospective funding opportunity
# (retrospective content, lists of past awardees, reports, etc.)
DEMOTION_RULES = [
    # Strong retrospective signals
    ("past_recipients", r"\b(past|previous|prior)\s+(recipients|awardees|winners|grantees)\b"),
    ("grant_recipients", r"\b(grant|award)\s+recipients\b"),
    ("funded_projects_list", r"\b(funded\s+projects|projects\s+funded|grants\s+awarded|awarded\s+grants)\b"),
    ("meet_the_awardees", r"\bmeet\s+(the\s+)?(awardees|recipients|grantees)\b"),
    ("list_of_awardees", r"\blist\s+of\s+(awardees|recipients|grantees|winners)\b"),

    # Reports / retrospective publications
    ("annual_report", r"\b(annual|impact|program)\s+report\b"),
    ("year_in_review", r"\b(year\s+in\s+review|highlights\s+of\s+the\s+year)\b"),

    # Conference program pages (not funding opportunities)
    ("conference_program_only", r"\b(conference|meeting)\s+(program|agenda|schedule)\b"),
]


# Columns to search for these signals (keep it simple and high-signal)
TEXT_COLS = [
    "opportunity_name",
    "summary_1_2_sentences",
    "evidence_snippets",
    "award_amount_text",
    "deadline_text",
    "eligibility_text",
    "opportunity_url",
    "source_url",
]

def build_search_text(row: pd.Series) -> str:
    parts = []
    for c in TEXT_COLS:
        v = row.get(c, "")
        if pd.isna(v):
            v = ""
        v = str(v)
        if v.strip():
            parts.append(v)
    return "\n".join(parts).lower()

def main():
    df = pd.read_csv(INPUT)

    # Ensure audit columns exist
    if "rule_demoted" not in df.columns:
        df["rule_demoted"] = ""
    if "rule_demote_reason" not in df.columns:
        df["rule_demote_reason"] = ""

    # Work only on unclear
    mask_unclear = df["is_real_funding"].astype(str).str.lower() == TARGET_LABEL
    unclear_idx = df.index[mask_unclear]

    demoted_count = 0

    for i in unclear_idx:
        row = df.loc[i]
        text = build_search_text(row)

        matched = None
        for rule_name, pattern in DEMOTION_RULES:
            if re.search(pattern, text, flags=re.IGNORECASE):
                matched = (rule_name, pattern)
                break

        if matched:
            rule_name, _ = matched
            df.at[i, "is_real_funding"] = "no"
            df.at[i, "confidence"] = "low"
            df.at[i, "rule_demoted"] = "yes"
            df.at[i, "rule_demote_reason"] = f"Rule demotion: matched '{rule_name}' pattern indicating retrospective/non-opportunity content."
            demoted_count += 1

    df.to_csv(OUTPUT, index=False)
    print("Input rows:", len(df))
    print("Unclear rows:", len(unclear_idx))
    print("Demoted unclear->no:", demoted_count)
    print("Wrote:", OUTPUT.resolve())

if __name__ == "__main__":
    main()
