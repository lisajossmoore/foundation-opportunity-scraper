import pandas as pd
from pathlib import Path

XLSX_PATH = Path("/home/lisa/projects/foundation-opportunity-scraper/data/output/foundations_and_opportunities_PREFILTERED.xlsx")
SHEET = "Opportunities_prefiltered"

def main():
    print("File exists:", XLSX_PATH.exists())
    print("File:", XLSX_PATH)

    df = pd.read_excel(XLSX_PATH, sheet_name=SHEET)

    print("\nRows:", len(df))
    print("Columns:", len(df.columns))
    print("\nColumn names:")
    for c in df.columns:
        print(" -", c)

    print("\nSample rows (first 5):")
    with pd.option_context("display.max_columns", 200, "display.width", 200):
        print(df.head(5).to_string(index=False))

if __name__ == "__main__":
    main()
