import pandas as pd
from pathlib import Path

# ---- Paths ----
BASE_DIR = Path(__file__).resolve().parents[1]
INPUT_FILE = BASE_DIR / "data" / "input" / "foundations_raw.xlsx"
OUTPUT_FILE = BASE_DIR / "data" / "output" / "foundations_with_ids.xlsx"

# ---- Load Excel ----
df = pd.read_excel(INPUT_FILE)

# ---- Assign foundation IDs ----
df = df.reset_index(drop=True)
df["foundation_id"] = df.index + 1
df["foundation_id"] = df["foundation_id"].apply(lambda x: f"F{x:03d}")

# ---- Reorder columns (ID first) ----
cols = ["foundation_id"] + [c for c in df.columns if c != "foundation_id"]
df = df[cols]

# ---- Write output ----
OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
df.to_excel(OUTPUT_FILE, index=False)

print(f"Saved: {OUTPUT_FILE}")
print(f"Total foundations: {len(df)}")
