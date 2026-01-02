import pandas as pd
from pathlib import Path

INPUT = Path("classified_opportunities_checkpoint_rule_demoted.csv")
OUTPUT = Path("foundations_and_opportunities_CLASSIFIED_rule_demoted.xlsx")

df = pd.read_csv(INPUT)

with pd.ExcelWriter(OUTPUT, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="ALL", index=False)

    for label in ["yes", "unclear", "no"]:
        subset = df[df["is_real_funding"] == label]
        subset.to_excel(writer, sheet_name=label.upper(), index=False)

    # Extra audit sheet: rows demoted by deterministic rules
    if "rule_demoted" in df.columns:
        demoted = df[df["rule_demoted"] == "yes"].copy()
        demoted.to_excel(writer, sheet_name="RULE_DEMOTED", index=False)

print("Wrote:", OUTPUT.resolve())
print("Counts:")
print(df["is_real_funding"].value_counts())
