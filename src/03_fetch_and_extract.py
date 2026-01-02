import csv
import json
import re
import time
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
import requests
import trafilatura
from tqdm import tqdm

import fitz  # PyMuPDF

# --------------------
# Config (tune later)
# --------------------
INPUT_CSV = Path("data/intermediate/candidate_pages.csv")
OUT_DIR = Path("page_store")

MAX_URLS_PER_FOUNDATION = 25     # high recall, but bounded (tune later)
SLEEP_SECONDS = 1.0             # be polite
TIMEOUT_SECONDS = 45
USER_AGENT = "Mozilla/5.0 (compatible; FoundationOpportunityScraper/1.0; +https://example.org)"

# Skip patterns to reduce junk
SKIP_URL_PATTERNS = [
    r"facebook\.com", r"twitter\.com", r"x\.com", r"instagram\.com", r"linkedin\.com",
    r"youtube\.com", r"youtu\.be",
    r"/donate", r"/giving", r"/support", r"/privacy", r"/terms", r"/cookie",
    r"/contact", r"/about", r"/staff", r"/team", r"/news", r"/press", r"/blog",
    r"/login", r"/signin", r"/sign-in", r"/account",
]

def should_skip(url: str) -> bool:
    u = (url or "").lower()
    return any(re.search(pat, u) for pat in SKIP_URL_PATTERNS)

def sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()

def fetch(url: str) -> requests.Response:
    headers = {"User-Agent": USER_AGENT}
    return requests.get(url, headers=headers, timeout=TIMEOUT_SECONDS, allow_redirects=True)

def extract_pdf_text(pdf_bytes: bytes) -> str:
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    parts = []
    for page in doc:
        parts.append(page.get_text("text"))
    doc.close()
    return "\n".join(parts).strip()

def save_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

def main():
    if not INPUT_CSV.exists():
        raise SystemExit(f"Missing input CSV: {INPUT_CSV}")

    df = pd.read_csv(INPUT_CSV)
    required = {"foundation_id", "foundation_name", "url"}
    if not required.issubset(df.columns):
        raise SystemExit(f"candidate_pages.csv missing columns: {required - set(df.columns)}")

    # Filter obvious junk early
    df = df[df["url"].astype(str).str.len() > 0].copy()
    df["skip"] = df["url"].apply(should_skip)
    df = df[~df["skip"]].copy()

    # Keep top N URLs per foundation by result_rank (if available)
    if "result_rank" in df.columns:
        df["result_rank"] = pd.to_numeric(df["result_rank"], errors="coerce")
        df = df.sort_values(["foundation_id", "result_rank", "url"], ascending=[True, True, True])
    else:
        df = df.sort_values(["foundation_id", "url"])

    df = df.groupby("foundation_id").head(MAX_URLS_PER_FOUNDATION).reset_index(drop=True)

    print(f"Foundations in input: {df['foundation_id'].nunique()}")
    print(f"URLs after filtering + cap: {len(df)} (cap {MAX_URLS_PER_FOUNDATION} per foundation)")

    session = requests.Session()

    for _, row in tqdm(df.iterrows(), total=len(df), desc="Fetch+extract"):
        fid = str(row["foundation_id"])
        fname = str(row.get("foundation_name", ""))
        url = str(row["url"])

        url_hash = sha1(url)[:12]
        out_base = OUT_DIR / fid / url_hash
        out_json = out_base.with_suffix(".json")

        # Skip if already done
        if out_json.exists():
            continue

        record: Dict[str, Any] = {
            "foundation_id": fid,
            "foundation_name": fname,
            "url": url,
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "http_status": None,
            "final_url": None,
            "content_type": None,
            "title": "",
            "extracted_text": "",
            "error": "",
        }

        try:
            resp = session.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT_SECONDS, allow_redirects=True)
            record["http_status"] = resp.status_code
            record["final_url"] = resp.url
            ctype = (resp.headers.get("Content-Type") or "").lower()
            record["content_type"] = ctype

            if resp.status_code >= 400:
                record["error"] = f"HTTP {resp.status_code}"
                save_json(out_json, record)
                time.sleep(SLEEP_SECONDS)
                continue

            # PDF
            if "application/pdf" in ctype or resp.url.lower().endswith(".pdf"):
                pdf_path = out_base.with_suffix(".pdf")
                pdf_path.parent.mkdir(parents=True, exist_ok=True)
                pdf_path.write_bytes(resp.content)
                text = extract_pdf_text(resp.content)
                record["extracted_text"] = text[:200000]  # cap to keep files sane
                save_json(out_json, record)
                time.sleep(SLEEP_SECONDS)
                continue

            # HTML (or other text-ish)
            html = resp.text
            # Trafilatura: extract main content
            extracted = trafilatura.extract(html, include_comments=False, include_tables=True) or ""
            record["extracted_text"] = extracted.strip()[:200000]  # cap
            save_json(out_json, record)

        except Exception as e:
            record["error"] = str(e)
            save_json(out_json, record)

        time.sleep(SLEEP_SECONDS)

    print("Done. Extracted pages saved under page_store/")

if __name__ == "__main__":
    main()
