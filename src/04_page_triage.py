import json
import re
from pathlib import Path
import pandas as pd
from tqdm import tqdm

PAGE_STORE = Path("page_store")
OUT_CSV = Path("data/intermediate/page_triage.csv")

# URL keywords that often indicate funding opportunities
URL_GOOD = [
    "grant", "grants", "funding", "award", "awards", "scholar", "scholarship",
    "fellow", "fellowship", "rfp", "proposal", "apply", "application", "guideline",
    "career-development", "young-investigator", "pilot", "seed"
]

# URL keywords that often indicate noise (not funding)
URL_BAD = [
    "leadership", "board", "staff", "team", "membership", "join", "renew",
    "donate", "giving", "news", "blog", "press", "event", "calendar",
    "job", "careers", "privacy", "terms", "contact", "about", "login", "signin"
]

# Text keywords that often appear in real opportunities
TEXT_GOOD = [
    "eligibility", "eligible", "deadline", "due date", "letter of intent", "loi",
    "award amount", "funding amount", "budget", "apply by", "application period",
    "request for proposals", "call for proposals", "submission", "proposal"
]

def classify(url: str, content_type: str, text: str):
    u = (url or "").lower()
    ct = (content_type or "").lower()
    t = (text or "").lower()

    # PDFs are often high-value for grants/guidelines
    if "application/pdf" in ct or u.endswith(".pdf"):
        return True, "pdf"

    if any(k in u for k in URL_BAD):
        # still allow if URL strongly suggests funding too
        if any(k in u for k in URL_GOOD):
            return True, "url_good_overrides_bad"
        return False, "url_bad"

    if any(k in u for k in URL_GOOD):
        return True, "url_good"

    # Text-based signal
    hits = sum(1 for k in TEXT_GOOD if k in t)
    if hits >= 2:
        return True, f"text_good_{hits}"

    # If text is extremely short, likely nav/empty
    if len(t.strip()) < 400:
        return False, "text_too_short"

    return False, "no_signal"

def main():
    json_files = list(PAGE_STORE.glob("*/*.json"))
    if not json_files:
        raise SystemExit("No JSON files found under page_store/. Did fetch step run?")

    rows = []
    for fp in tqdm(json_files, desc="Triaging pages"):
        try:
            data = json.loads(fp.read_text(encoding="utf-8"))
        except Exception as e:
            rows.append({
                "foundation_id": fp.parent.name,
                "json_path": str(fp),
                "url": "",
                "content_type": "",
                "http_status": "",
                "text_len": 0,
                "likely_funding": False,
                "reason": f"json_read_error:{e}",
                "error": "json_read_error",
            })
            continue

        fid = data.get("foundation_id", fp.parent.name)
        url = data.get("final_url") or data.get("url") or ""
        ct = data.get("content_type") or ""
        status = data.get("http_status")
        err = data.get("error") or ""
        text = data.get("extracted_text") or ""

        likely, reason = classify(url, ct, text)

        rows.append({
            "foundation_id": fid,
            "json_path": str(fp),
            "url": url,
            "content_type": ct,
            "http_status": status,
            "text_len": len(text),
            "likely_funding": bool(likely),
            "reason": reason,
            "error": err,
        })

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(rows).to_csv(OUT_CSV, index=False)
    print(f"Saved: {OUT_CSV}")
    print(f"Total pages: {len(rows)}")
    print(f"Likely funding pages: {sum(1 for r in rows if r['likely_funding'])}")

if __name__ == "__main__":
    main()
