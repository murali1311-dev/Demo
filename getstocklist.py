import pandas as pd
from datetime import date

SNAPSHOT_DATE = date.today().isoformat()

# ---------- NSE ----------


nse = pd.read_csv("data/nse_equities.csv")

# Strip spaces just in case (defensive programming)
nse.columns = nse.columns.str.strip()

# Keep only equity series
nse = nse[nse["SERIES"].isin(["EQ", "BE", "BZ"])]

# Select and rename columns
nse = nse[[
    "SYMBOL",
    "NAME OF COMPANY",
    "ISIN NUMBER"
]]

nse.columns = [
    "symbol_nse",
    "company_name",
    "isin"
]

print(f"NSE equities loaded: {len(nse)}")


# ---------- BSE ----------
bse = pd.read_csv("data/bse_equities.csv")
bse = bse[["Scrip code", "Security Name", "ISIN"]]
bse.columns = ["symbol_bse", "company_name", "isin"]

# ---------- MERGE ----------
master = pd.merge(nse, bse, on="isin", how="outer")

master["exchange_presence"] = master.apply(
    lambda r: "BOTH" if pd.notna(r.symbol_nse) and pd.notna(r.symbol_bse)
    else "NSE" if pd.notna(r.symbol_nse)
    else "BSE",
    axis=1
)

# ---------- INDUSTRY (from NIFTY 500) ----------
# ---------- NIFTY 500 ----------
n500 = pd.read_csv("data/nifty500.csv")
n500.columns = n500.columns.str.strip()

# Normalize column names
n500 = n500[[
    "ISIN Code",
    "Industry"
]].drop_duplicates()

n500.columns = [
    "isin",
    "industry"
]
master = master.merge(
    n500,
    on="isin",
    how="left"
)

master["industry"] = master["industry"].fillna("UNCLASSIFIED")


# ---------- INDEX MAP ----------
index_files = {
    "NIFTY50": "data/nifty50.csv",
    "NIFTY100": "data/nifty100.csv",
    "NIFTY200": "data/nifty200.csv",
    "NIFTY500": "data/nifty500.csv"
}

rows = []
for code, path in index_files.items():
    df = pd.read_csv(path)
    for isin in df["ISIN Code"].unique():
        rows.append({
            "isin": isin,
            "index_provider": "NSE",
            "index_code": code,
            "index_name": code,
            "snapshot_date": SNAPSHOT_DATE
        })

index_map = pd.DataFrame(rows)

# ---------- OUTPUT ----------
master.to_csv("equity_master.csv", index=False)
index_map.to_csv("equity_index_map.csv", index=False)

print("✅ Snapshot generated successfully")
