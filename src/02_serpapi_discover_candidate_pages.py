import os
import time
import csv
from pathlib import Path
from typing import Dict, Any, List

import pandas as pd
import requests
import tldextract
from dotenv import load_dotenv
from tqdm import tqdm

# ---- Config ----
RESULTS_PER_QUERY = 10          # keep top N results per query
QUERIES_PER_FOUNDATION = 3      # fixed below
SLEEP_SECONDS = 0.8             # polite pacing to avoid hammering SerpAPI

QUERY_TEMPLATES = [
    # 1) General funding pages
    'site:{domain} (grant OR grants OR funding OR "request for proposals" OR RFP OR award OR fellowship)',
    # 2) Application / deadlines
    'site:{domain} (apply OR application OR "letter of intent" OR LOI OR deadline)',
    # 3) PDFs (often where the real details live)
    'site:{domain} filetype:pdf (grant OR RFP OR guidelines OR application OR deadline)',
]

# ---- Paths ----
BASE_DIR = Path(__file__).resolve().parents[1]
FOUNDATIONS_FILE = BASE_DIR / "data" / "output" / "foundations_with_ids.xlsx"
OUT_CSV = BASE_DIR / "data" / "intermediate" / "candidate_pages.csv"

def get_domain(url: str) -> str:
    """Extract registrable domain like 'example.org' from a URL."""
    if not isinstance(url, str) or not url.strip():
        return ""
    ext = tldextract.extract(url)
    if not ext.domain or not ext.suffix:
        return ""
    return f"{ext.domain}.{ext.suffix}"

def serpapi_search(api_key: str, q: str, num: int = 10) -> Dict[str, Any]:
    """Call SerpAPI Google Search API."""
    params = {
        "engine": "google",
        "q": q,
        "num": num,
        "api_key": api_key,
    }
    r = requests.get("https://serpapi.com/search.json", params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def main():
    load_dotenv()
    api_key = os.getenv("SERPAPI_API_KEY")
    if not api_key:
        raise SystemExit("SERPAPI_API_KEY not found. Check your .env file.")

    df = pd.read_excel(FOUNDATIONS_FILE)
    required_cols = {"foundation_id", "foundation_name", "website_url"}
    missing = required_cols - set(df.columns)
    if missing:
        raise SystemExit(f"Missing columns in foundations file: {missing}")

    rows: List[Dict[str, Any]] = []
    seen = set()  # (foundation_id, url)

    for _, rec in tqdm(df.iterrows(), total=len(df), desc="SerpAPI discovery"):
        fid = str(rec["foundation_id"])
        fname = str(rec["foundation_name"])
        url = str(rec["website_url"])
        domain = get_domain(url)

        if not domain:
            continue

        for template in QUERY_TEMPLATES[:QUERIES_PER_FOUNDATION]:
            q = template.format(domain=domain)

            try:
                data = serpapi_search(api_key, q, num=RESULTS_PER_QUERY)
            except Exception as e:
                rows.append({
                    "foundation_id": fid,
                    "foundation_name": fname,
                    "domain": domain,
                    "query": q,
                    "result_rank": "",
                    "title": "",
                    "snippet": "",
                    "url": "",
                    "error": str(e),
                })
                time.sleep(SLEEP_SECONDS)
                continue

            organic = data.get("organic_results", []) or []
            for i, item in enumerate(organic[:RESULTS_PER_QUERY], start=1):
                link = item.get("link", "") or ""
                key = (fid, link)
                if not link or key in seen:
                    continue
                seen.add(key)

                rows.append({
                    "foundation_id": fid,
                    "foundation_name": fname,
                    "domain": domain,
                    "query": q,
                    "result_rank": i,
                    "title": item.get("title", "") or "",
                    "snippet": item.get("snippet", "") or "",
                    "url": link,
                    "error": "",
                })

            time.sleep(SLEEP_SECONDS)

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else [
            "foundation_id","foundation_name","domain","query","result_rank","title","snippet","url","error"
        ])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {OUT_CSV}")
    print(f"Total candidate rows: {len(rows)}")
    print(f"Unique candidate URLs: {len(seen)}")

if __name__ == "__main__":
    main()
