import re
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parents[1]

FOUNDATIONS_XLSX = BASE_DIR / "data" / "output" / "foundations_with_ids.xlsx"
OPPS_CSV = BASE_DIR / "data" / "output" / "opportunities.csv"

OUT_XLSX = BASE_DIR / "data" / "output" / "foundations_and_opportunities_FINAL.xlsx"

# --- Simple state list for heuristic eligibility checks ---
US_STATES = [
    "alabama","alaska","arizona","arkansas","california","colorado","connecticut","delaware",
    "florida","georgia","hawaii","idaho","illinois","indiana","iowa","kansas","kentucky",
    "louisiana","maine","maryland","massachusetts","michigan","minnesota","mississippi","missouri",
    "montana","nebraska","nevada","new hampshire","new jersey","new mexico","new york",
    "north carolina","north dakota","ohio","oklahoma","oregon","pennsylvania","rhode island",
    "south carolina","south dakota","tennessee","texas","utah","vermont","virginia","washington",
    "west virginia","wisconsin","wyoming","district of columbia","washington dc","d.c."
]

def norm_text(s: str) -> str:
    s = "" if pd.isna(s) else str(s)
    s = s.lower().strip()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"[^a-z0-9 \-\/]", "", s)
    return s

def score_row(r) -> int:
    # Choose "best" duplicate row
    conf = str(r.get("confidence", "")).lower()
    conf_score = {"high": 3, "med": 2, "low": 1}.get(conf, 0)

    has_deadline = 1 if str(r.get("deadline_text", "")).strip() not in ["", "nan"] else 0
    has_amount = 1 if str(r.get("award_amount_text", "")).strip() not in ["", "nan"] else 0
    kw_len = len(str(r.get("keywords_phrases", "")).split("|")) if str(r.get("keywords_phrases", "")).strip() not in ["", "nan"] else 0
    summary_len = len(str(r.get("summary_1_2_sentences", "")).strip())
    evidence_len = len(str(r.get("evidence_snippets", "")).strip())

    return conf_score * 1000 + (has_deadline + has_amount) * 100 + kw_len * 5 + summary_len // 50 + evidence_len // 50

def utah_eligible_flag(eligibility_us: str, eligibility_text: str) -> str:
    eu = "" if pd.isna(eligibility_us) else str(eligibility_us).lower().strip()
    et = norm_text(eligibility_text)

    # If model said explicitly no, respect it
    if eu == "no":
        return "no"

    # If eligibility mentions Utah explicitly, good
    if "utah" in et:
        return "yes"

    # Heuristic: detect "only residents of <state>" or "must be in <state>" patterns
    # Conservative: only mark "no" when the text strongly suggests a single non-Utah state restriction
    restrictive_markers = ["only", "must be", "restricted", "residents of", "resident of", "located in", "within the state of"]
    if any(m in et for m in restrictive_markers):
        mentioned_states = [st for st in US_STATES if st in et]
        mentioned_states = list(dict.fromkeys(mentioned_states))  # unique, preserve order

        # If only one state mentioned and it's not Utah, likely restricted
        if len(mentioned_states) == 1 and mentioned_states[0] != "utah":
            return "no"

        # If multiple states mentioned but Utah not included, unclear (could still be national)
        if len(mentioned_states) >= 1 and "utah" not in mentioned_states:
            return "review"

    # If model said yes, accept
    if eu == "yes":
        return "yes"

    # Otherwise unclear -> review
    return "review"

def main():
    if not FOUNDATIONS_XLSX.exists():
        raise SystemExit(f"Missing foundations file: {FOUNDATIONS_XLSX}")
    if not OPPS_CSV.exists():
        raise SystemExit(f"Missing opportunities file: {OPPS_CSV}")

    foundations = pd.read_excel(FOUNDATIONS_XLSX)
    opps = pd.read_csv(OPPS_CSV)

    # Basic cleanup
    for col in ["foundation_id", "foundation_name", "source_url", "opportunity_name", "opportunity_url"]:
        if col in opps.columns:
            opps[col] = opps[col].fillna("")

    # Create a dedupe key:
    # Prefer opportunity_url if present; otherwise normalize name.
    opps["norm_name"] = opps["opportunity_name"].apply(norm_text)
    opps["norm_url"] = opps["opportunity_url"].apply(norm_text)

    opps["dedupe_key"] = opps.apply(
        lambda r: f"{r['foundation_id']}|url|{r['norm_url']}" if r["norm_url"] else f"{r['foundation_id']}|name|{r['norm_name']}",
        axis=1
    )

    # Score rows and dedupe
    opps["row_score"] = opps.apply(score_row, axis=1)
    opps = opps.sort_values(["dedupe_key", "row_score"], ascending=[True, False])
    opps_deduped = opps.drop_duplicates(subset=["dedupe_key"], keep="first").copy()

    # Utah-eligible flag
    opps_deduped["utah_eligible_flag"] = opps_deduped.apply(
        lambda r: utah_eligible_flag(r.get("eligibility_us", ""), r.get("eligibility_text", "")),
        axis=1
    )

    # Filter out definite "no"
    before = len(opps_deduped)
    opps_final = opps_deduped[opps_deduped["utah_eligible_flag"] != "no"].copy()
    after = len(opps_final)

    # Drop helper cols not needed in final
    opps_final = opps_final.drop(columns=["norm_name", "norm_url", "row_score"], errors="ignore")

    # Sort nicely
    opps_final = opps_final.sort_values(["foundation_id", "foundation_name", "opportunity_name"])

    # Write Excel with 2 tabs
    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(OUT_XLSX, engine="openpyxl") as writer:
        foundations.to_excel(writer, sheet_name="Foundations", index=False)
        opps_final.to_excel(writer, sheet_name="Opportunities", index=False)

    print(f"Saved: {OUT_XLSX}")
    print(f"Opportunities raw rows: {len(opps)}")
    print(f"Opportunities deduped: {before}")
    print(f"Opportunities after Utah filter: {after}")
    print("Note: utah_eligible_flag='review' should be spot-checked.")

if __name__ == "__main__":
    main()
