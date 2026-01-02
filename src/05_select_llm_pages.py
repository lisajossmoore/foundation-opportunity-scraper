import pandas as pd
from pathlib import Path

TRIAGE_CSV = Path("data/intermediate/page_triage.csv")
OUT_CSV = Path("data/intermediate/llm_input_pages.csv")

# Caps per foundation
MAX_PDFS_PER_FOUNDATION = 4
MAX_HTML_PER_FOUNDATION = 4

def main():
    df = pd.read_csv(TRIAGE_CSV)

    # Keep only likely funding pages
    df = df[df["likely_funding"] == True].copy()

    # Normalize content type
    df["is_pdf"] = df["content_type"].fillna("").str.contains("pdf", case=False)

    # Sort by priority:
    # PDFs first, then by reason strength, then longer text
    reason_priority = {
        "pdf": 1,
        "url_good": 2,
        "url_good_overrides_bad": 3,
    }
    df["reason_rank"] = df["reason"].map(reason_priority).fillna(10)

    df = df.sort_values(
        ["foundation_id", "is_pdf", "reason_rank", "text_len"],
        ascending=[True, False, True, False]
    )

    selected_rows = []

    for fid, g in df.groupby("foundation_id"):
        pdfs = g[g["is_pdf"]].head(MAX_PDFS_PER_FOUNDATION)
        htmls = g[~g["is_pdf"]].head(MAX_HTML_PER_FOUNDATION)
        selected_rows.append(pdfs)
        selected_rows.append(htmls)

    out = pd.concat(selected_rows, ignore_index=True)
    out.to_csv(OUT_CSV, index=False)

    print(f"Saved: {OUT_CSV}")
    print(f"Foundations represented: {out['foundation_id'].nunique()}")
    print(f"Total pages selected for LLM: {len(out)}")

if __name__ == "__main__":
    main()
