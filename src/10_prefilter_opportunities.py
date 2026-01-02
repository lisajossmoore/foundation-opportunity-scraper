import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

IN_XLSX = BASE_DIR / "data" / "output" / "foundations_and_opportunities_FINAL.xlsx"
OUT_XLSX = BASE_DIR / "data" / "output" / "foundations_and_opportunities_PREFILTERED.xlsx"

# --- Heuristic patterns for obvious non-funding pages/opportunities ---
URL_BAD_PATTERNS = [
    r"newsletter", r"/news", r"/blog", r"/press", r"/media", r"annual[-_]?report",
    r"/awardees", r"past[-_ ]recipients", r"previous[-_ ]winners", r"winners", r"recipient[s]?",
    r"honor[-_ ]roll", r"hall[-_ ]of[-_ ]fame", r"/events?", r"/gala", r"/conference",
    r"/staff", r"/board", r"/leadership", r"/about", r"/contact", r"/membership", r"/join",
]

NAME_BAD_PATTERNS = [
    r"past recipients", r"awardees", r"winners", r"recipient(s)?",
    r"honor roll", r"hall of fame", r"recognition", r"distinguished", r"lifetime achievement",
    r"newsletter", r"news", r"blog", r"press release", r"announcement",
    r"annual meeting", r"gala", r"event",
    r"policy", r"conflict of interest", r"bylaws", r"minutes",
]

# Text signals that something is actually an application-based funding opportunity
POSITIVE_SIGNALS = [
    "apply", "application", "rfa", "rfp", "request for proposals", "call for proposals",
    "deadline", "due", "letter of intent", "loi", "budget", "award amount", "funding",
    "grant", "grants", "stipend", "fellowship", "scholarship", "seed funding",
]

# Recognition-only signals (often no money, no application)
RECOGNITION_SIGNALS = [
    "recognizes", "honors", "celebrates", "recognition", "distinguished", "award for excellence",
    "lifetime achievement", "named lecture", "medal", "honorary",
]

def norm(s: str) -> str:
    s = "" if pd.isna(s) else str(s)
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    return s

def has_any(text: str, keywords) -> bool:
    t = norm(text)
    return any(k in t for k in keywords)

def regex_any(text: str, patterns) -> bool:
    t = norm(text)
    return any(re.search(p, t) for p in patterns)

def prefilter_row(r) -> tuple[bool, str]:
    """
    Returns (keep, reason)
    keep=True -> retain for LLM classification
    keep=False -> drop now (obvious non-funding)
    """
    name = norm(r.get("opportunity_name", ""))
    url = norm(r.get("source_url", "")) + " " + norm(r.get("opportunity_url", ""))
    summary = norm(r.get("summary_1_2_sentences", ""))
    elig = norm(r.get("eligibility_text", ""))
    deadline = norm(r.get("deadline_text", ""))
    amount = norm(r.get("award_amount_text", ""))
    evidence = norm(r.get("evidence_snippets", ""))

    blob = " ".join([name, summary, elig, deadline, amount, evidence])

    # 1) Hard drop if URL is clearly a past-awardee / newsletter / press / governance page
    if regex_any(url, URL_BAD_PATTERNS):
        # But keep if there's strong evidence of application-based funding
        if has_any(blob, POSITIVE_SIGNALS) or ("$" in blob) or ("grant" in blob) or ("funding" in blob):
            return True, "url_bad_but_positive_signal"
        return False, "drop:url_pattern"

    # 2) Hard drop if the "opportunity name" screams past winners / recognition pages
    if regex_any(name, NAME_BAD_PATTERNS):
        # Again, keep if strong funding signals exist
        if has_any(blob, POSITIVE_SIGNALS) or ("$" in blob) or ("grant" in blob) or ("funding" in blob):
            return True, "name_bad_but_positive_signal"
        return False, "drop:name_pattern"

    # 3) Recognition-only heuristic: recognition language + no amount + no deadline + no apply-language
    recognitionish = has_any(blob, RECOGNITION_SIGNALS)
    has_money_hint = ("$" in blob) or any(x in amount for x in ["up to", "usd", "dollar", "£", "€"]) or bool(amount.strip())
    has_deadline_hint = bool(deadline.strip())
    has_apply_hint = has_any(blob, POSITIVE_SIGNALS)

    if recognitionish and (not has_money_hint) and (not has_deadline_hint) and (not has_apply_hint):
        return False, "drop:recognition_no_money_no_apply"

    # Otherwise keep for LLM classifier
    return True, "keep"

def main():
    if not IN_XLSX.exists():
        raise SystemExit(f"Missing input: {IN_XLSX}")

    foundations = pd.read_excel(IN_XLSX, sheet_name="Foundations")
    opps = pd.read_excel(IN_XLSX, sheet_name="Opportunities")

    # Apply prefilter
    keep_flags = []
    reasons = []
    for _, r in opps.iterrows():
        keep, reason = prefilter_row(r)
        keep_flags.append(keep)
        reasons.append(reason)

    opps = opps.copy()
    opps["prefilter_keep"] = keep_flags
    opps["prefilter_reason"] = reasons

    kept = opps[opps["prefilter_keep"] == True].copy()
    dropped = opps[opps["prefilter_keep"] == False].copy()

    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        foundations.to_excel(writer, sheet_name="Foundations", index=False)
        kept.to_excel(writer, sheet_name="Opportunities_prefiltered", index=False)
        dropped.to_excel(writer, sheet_name="Dropped_by_prefilter", index=False)

    print(f"Saved: {OUT_XLSX}")
    print(f"Input opportunities: {len(opps)}")
    print(f"Kept for LLM: {len(kept)}")
    print(f"Dropped by rules: {len(dropped)}")
    print("Tip: Spot-check 'Dropped_by_prefilter' for false drops before LLM pass.")

if __name__ == "__main__":
    main()
