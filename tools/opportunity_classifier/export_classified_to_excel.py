import pandas as pd
from pathlib import Path

INPUT = Path("classified_opportunities_checkpoint.csv")
OUTPUT = Path("foundations_and_opportunities_CLASSIFIED.xlsx")

df = pd.read_csv(INPUT)

with pd.ExcelWriter(OUTPUT, engine="openpyxl") as writer:
    df.to_excel(writer, sheet_name="ALL", index=False)

    for label in ["yes", "unclear", "no"]:
        subset = df[df["is_real_funding"] == label]
        subset.to_excel(
            writer,
            sheet_name=label.upper(),
            index=False
        )

print(f"Wrote {OUTPUT.resolve()}")
