# Foundation Opportunity Scraper

A Python pipeline for discovering, extracting, and classifying foundation funding opportunities from the web.

The goal is to produce a conservative, auditable list of *real prospective funding opportunities* (grants, fellowships, stipends, travel awards, salary support), while minimizing false positives.

## What this project does

1. Discovers candidate foundation pages
2. Fetches and extracts page content
3. Identifies pages likely to contain funding opportunities
4. Uses an LLM to extract structured opportunity rows
5. Cleans and deduplicates extracted opportunities
6. Prefilters obvious non-opportunities
7. Classifies opportunities as:
   - **yes** — real prospective funding
   - **no** — recognition-only or informational content
   - **unclear** — intentionally conservative holding category

## Repository structure

- `src/` — main scraping and extraction pipeline (numbered scripts)
- `tools/opportunity_classifier/` — LLM-based opportunity classifier and exports
- `data/` — input/output data (not committed to git)
- `page_store/` — cached fetched pages (not committed to git)
- `docs/` — pipeline design notes and rationale

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
