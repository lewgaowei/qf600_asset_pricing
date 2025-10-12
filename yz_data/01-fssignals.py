# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %%
# ------------------------------
# 0. Load Libraries and Configuration
# ------------------------------
import wrds
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import sys

def end_step(name):
    print(f"--- End of step: {name} ---", flush=True)

START_YEAR = 2015
require_wrds_load = False

# %%
# ------------------------------
# 1. Load Compustat fundamentals - ALL VARIABLES
# ------------------------------

if require_wrds_load == True:
    db = wrds.Connection(wrds_username="lewgaowei")   # will ask for password if no .pgpass

# ALL 240+ accounting variables from the SAS file - EXACT COPY
ALL_ACCOUNTING_VARIABLES = [
    # Basic identifiers and denominators
    "gvkey", "datadate", "fyear", "fyr", "indfmt", "datafmt", "popsrc", "consol",
    "csho", "prcc_f",  # for market cap
    
    
    "at", "act", "invt", "ppent", "lt", "lct", "dltt", "ceq", "seq", "icapt",
    "sale", "cogs", "xsga", "emp",  # denominators
    
    # ALL the accounting variables from SAS (240+ variables) - EXACT COPY
    "acchg", "aco", "acox", "act", "am", "ao", "aoloch", "aox", "ap", "apalch", 
    "aqc", "aqi", "aqs", "at", "bast", "caps", "capx", "capxv", "ceq", "ceql", 
    "ceqt", "ch", "che", "chech", "cld2", "cld3", "cld4", "cld5", "cogs", 
    "cstk", "cstkcv", "cstke", "dc", "dclo", "dcom", "dcpstk", "dcs", "dcvsr", 
    "dcvsub", "dcvt", "dd", "dd1", "dd2", "dd3", "dd4", "dd5", "dfs", "dfxa", 
    "diladj", "dilavx", "dlc", "dlcch", "dltis", "dlto", "dltp", "dltr", "dltt", 
    "dm", "dn", "do", "donr", "dp", "dpacb", "dpacc", "dpacli", "dpacls", 
    "dpacme", "dpacnr", "dpaco", "dpact", "dpc", "dpvieb", "dpvio", "dpvir", 
    "drc", "drlt", "ds", "dudd", "dv", "dvc", "dvp", "dvpa", "dvpibb", "dvt", 
    "dxd2", "dxd3", "dxd4", "dxd5", "ebit", "ebitda", "esopct", "esopdlt", 
    "esopnr", "esopr", "esopt", "esub", "esubc", "exre", "fatb", "fatc", 
    "fate", "fatl", "fatn", "fato", "fatp", "fca", "fiao", "fincf", "fopo", 
    "fopox", "fopt", "fsrco", "fsrct", "fuseo", "fuset", "gdwl", "gp", "ib", 
    "ibadj", "ibc", "ibcom", "icapt", "idit", "intan", "intc", "intpn", 
    "invch", "invfg", "invo", "invrm", "invt", "invwip", "itcb", "itci", 
    "ivaco", "ivaeq", "ivao", "ivch", "ivncf", "ivst", "ivstch", "lco", 
    "lcox", "lcoxdr", "lct", "lifr", "lo", "loxdr", "lse", "lt", "mib", 
    "mii", "mrc1", "mrc2", "mrc3", "mrc4", "mrc5", "mrct", "msa", "ni", 
    "niadj", "nieci", "nopi", "nopio", "np", "oancf", "ob", "oiadp", "oibdp", 
    "pi", "pidom", "pifo", "ppegt", "ppenb", "ppenc", "ppenli", "ppenls", 
    "ppenme", "ppennr", "ppeno", "ppent", "ppevbb", "ppeveb", "ppevo", 
    "ppevr", "prstkc", "pstk", "pstkc", "pstkl", "pstkn", "pstkr", "pstkrv", 
    "rdip", "re", "rea", "reajo", "recch", "recco", "recd", "rect", "recta", 
    "rectr", "reuna", "reunr", "revt", "sale", "seq", "siv", "spi", "sppe", 
    "sppiv", "sstk", "tlcf", "tstk", "tstkc", "tstkme", "tstkp", "txach", 
    "txbco", "txc", "txdb", "txdba", "txdbca", "txdbcl", "txdc", "txdfed", 
    "txdfo", "txdi", "txditc", "txds", "txfed", "txfo", "txndb", "txndba", 
    "txndbl", "txndbr", "txo", "txp", "txpd", "txr", "txs", "txt", "txw", 
    "wcap", "wcapc", "wcapch", "xacc", "xad", "xdepl", "xdp", "xi", "xido", 
    "xidoc", "xint", "xintd", "xopr", "xpp", "xpr", "xrd", "xrent", "xsga"
]

# Remove duplicates while preserving order
SAFE_ACCOUNTING_VARIABLES = []
seen = set()
for var in ALL_ACCOUNTING_VARIABLES:
    if var not in seen:
        SAFE_ACCOUNTING_VARIABLES.append(var)
        seen.add(var)

print(f"Total unique variables: {len(SAFE_ACCOUNTING_VARIABLES)}")

# Configuration: Choose which variables to process
# Option 1: Process only 4 variables (current setup - for testing)
# SIGNALS_TO_PROCESS = ["at", "sale", "lt", "xsga"]

# Option 2: Process ALL variables (uncomment to use - WARNING: Very large output!)
SIGNALS_TO_PROCESS = [var for var in SAFE_ACCOUNTING_VARIABLES 
                     if var not in ["gvkey", "datadate", "fyear", "fyr", 
                                   "indfmt", "datafmt", "popsrc", "consol", 
                                   "csho", "prcc_f"]]

# Option 3: Process a subset (uncomment and modify as needed)
# SIGNALS_TO_PROCESS = ["at", "sale", "lt", "xsga", "ni", "ib", "oancf", "ceq", 
#                      "act", "invt", "ppent", "dltt", "cogs", "emp", "do"]

print(f"Processing {len(SIGNALS_TO_PROCESS)} variables: {SIGNALS_TO_PROCESS}")

# Load Compustat data with all variables
if require_wrds_load == True:
    print("Loading Compustat data...")
    try:
        # Use raw SQL query to handle reserved words properly
        columns_str = ", ".join([f'"{col}"' for col in SAFE_ACCOUNTING_VARIABLES])
        query = f"SELECT {columns_str} FROM comp.funda WHERE indfmt='INDL' AND datafmt='STD' AND popsrc='D' AND consol='C'"
        
        comp = db.raw_sql(query)
        print("Successfully loaded Compustat data using raw SQL")
    except Exception as e:
        print(f"Error with raw SQL: {e}")
        print("Trying with get_table method...")
        try:
            comp = db.get_table("comp", "funda", columns=SAFE_ACCOUNTING_VARIABLES)
            # Apply filters
            comp = comp.query("indfmt=='INDL' & datafmt=='STD' & popsrc=='D' & consol=='C'").copy()
            print("Successfully loaded Compustat data using get_table")
        except Exception as e2:
            print(f"Error with get_table: {e2}")
            print("Trying with basic variables only...")
            # Fallback to basic variables
            basic_vars = ["gvkey", "datadate", "fyear", "fyr", "indfmt", "datafmt", "popsrc", "consol",
                        "csho", "prcc_f", "at", "act", "invt", "ppent", "lt", "lct", "dltt", "ceq", 
                        "seq", "icapt", "sale", "cogs", "xsga", "emp"]
            comp = db.get_table("comp", "funda", columns=basic_vars)
            comp = comp.query("indfmt=='INDL' & datafmt=='STD' & popsrc=='D' & consol=='C'").copy()
else:
    print("Loading raw data backup from parquet...")
    comp = pd.read_parquet("raw_compustat_data.parquet", engine="fastparquet")
    print(f"Loaded compustat data: {comp.shape[0]} rows, {comp.shape[1]} columns")
    with pd.option_context('display.width', 2000, 'display.max_columns', None):
        print(comp.head())
        
# Dates & fiscal year
comp["datadate"] = pd.to_datetime(comp["datadate"])
comp["year"] = comp["datadate"].dt.year
end_step("Dates & fiscal year")

# SAS-consistent: treat zero shares as missing before market cap
comp.loc[comp["csho"] == 0, "csho"] = np.nan
comp["mktcap"] = comp["csho"] * comp["prcc_f"]
end_step("Compute mktcap")

# Keep only what we need downstream
keep_ids = ["gvkey", "datadate", "fyear", "year"]
denominators = [
    "at", "act", "invt", "ppent", "lt", "lct", "dltt", "ceq", "seq", "icapt",
    "sale", "cogs", "xsga", "emp", "mktcap"
]

# Long form names for Denominators:
# at = Total Assets
# act = Current Assets
# invt = Inventory
# ppent = Property, Plant & Equipment, Net
# lt = Total Liabilities
# lct = Current Liabilities
# dltt = Long-term Debt
# ceq = Common Equity
# seq = Stockholders' Equity
# icapt = Invested Capital
# sale = Sales
# cogs = Cost of Goods Sold
# xsga = Selling, General & Administrative Expenses
# emp = Number of Employees
# mktcap = Market Capitalization (from Compustat)

# Filter to only include variables that exist in the data and are in our signal list
available_signals = [var for var in SIGNALS_TO_PROCESS if var in comp.columns]
missing_signals = [var for var in SIGNALS_TO_PROCESS if var not in comp.columns]

if missing_signals:
    print(f"Warning: Missing variables: {missing_signals}")

print(f"Processing {len(available_signals)} available signals")
print(f"Available columns: {list(comp.columns)[:20]}...")



# %%
# ------------------------------
# 2a. Save raw data copy and backup to parquet
# ------------------------------
if require_wrds_load == True:
    print("Saving raw data backup...")

    # Save a copy of the raw data before any processing
    raw_data_backup = comp.copy()

    # Save to parquet for future use (no need to query WRDS again)
    raw_data_backup.to_parquet("raw_compustat_data.parquet", engine="fastparquet", index=False)

    print(f"Raw data saved: {raw_data_backup.shape[0]} rows, {raw_data_backup.shape[1]} columns")
    print("Saved to: raw_compustat_data.parquet")
else:
    print("Raw data backup already exists")
end_step("2a. Save raw data backup")


# %%
# ------------------------------
# 2b. Filter data from START_YEAR onwards to reduce data size
# ------------------------------
print(f"Step 2b.Filtering data from {START_YEAR} onwards...")

# Convert datadate to datetime if not already
comp["datadate"] = pd.to_datetime(comp["datadate"])

# Filter from 2010 onwards
comp = comp[comp["datadate"].dt.year >= START_YEAR].copy()

# Ensure SAS-like ordering within the filtered window, then flag first two per gvkey
comp = comp.sort_values(["gvkey", "datadate"]).reset_index(drop=True)
comp["firsttwo"] = comp.groupby("gvkey").cumcount().le(1).astype(int)

print(f"After filtering: {comp.shape[0]} rows, {comp.shape[1]} columns")
print(f"Date range: {comp['datadate'].min()} to {comp['datadate'].max()}")
with pd.option_context('display.width', 2000, 'display.max_columns', None):
    print(comp.head())
    
print(comp.columns)
print(len(comp.columns))
end_step("2b. Filter timeframe")
    
# %%
# ------------------------------
# 3. Mark adjacency (fyear_t − fyear_{t-1} == 1 within gvkey)
# ------------------------------
print("Step 3. Mark adjacency (fyear_t − fyear_{t-1} == 1 within gvkey)")

comp["prev_fyear"] = comp.groupby("gvkey")["fyear"].shift(1)
comp["adjacent"] = (comp["fyear"] - comp["prev_fyear"] == 1).fillna(False).astype(int)
end_step("3. Mark adjacency")

# Compute lags for denominators (only keep lags when years are adjacent)
for col in denominators:
    if col in comp.columns:
        lag_col = f"{col}_lag"
        comp[lag_col] = comp.groupby("gvkey")[col].shift(1)
        comp.loc[comp["adjacent"] != 1, lag_col] = np.nan
end_step("Compute denominator lags")

# %%
# ------------------------------
# 4. save comp_base
# ------------------------------
print("Step 4. save comp_base")

# Validate required columns in `comp`
req_keys = ["gvkey","datadate","fyear","year","firsttwo","adjacent"]
req_denoms = ["at","act","invt","ppent","lt","lct","dltt","ceq","seq","icapt","sale","cogs","xsga","emp","mktcap"]
req_lags = [f"{c}_lag" for c in req_denoms]

missing = [c for c in (req_keys + req_denoms + req_lags) if c not in comp.columns]
print("Missing columns:", missing)

# Basic dtype checks
print("datadate dtype:", comp["datadate"].dtype)
num_cols = req_denoms + req_lags + ["firsttwo","adjacent"]
print("Non-numeric among numeric-expected:",
      [c for c in num_cols if c in comp.columns and not pd.api.types.is_numeric_dtype(comp[c])])

# Logical checks
# adjacent should match (fyear - prev_fyear == 1)
diff_ok = (comp.groupby("gvkey")["fyear"].diff() == 1)
mask = comp.groupby("gvkey")["fyear"].shift(1).notna()
mismatch_adjacent = ((comp["adjacent"] == 1) != diff_ok).where(mask).sum()
print("adjacent mismatches (where prev_fyear exists):", int(mismatch_adjacent))

# firsttwo should mark the first 2 rows per gvkey
ft_calc = comp.groupby("gvkey").cumcount().le(1).astype(int)
mismatch_firsttwo = (comp["firsttwo"] != ft_calc).sum()
print("firsttwo mismatches:", int(mismatch_firsttwo))

# Quick coverage + range
print("Rows, Cols:", comp.shape)
print("Date range:", comp["datadate"].min(), "→", comp["datadate"].max())
print("Unique gvkeys:", comp["gvkey"].nunique())

# If all good, save full comp (as you did)
comp.to_parquet(f"comp_base_{START_YEAR}.parquet", engine="fastparquet", index=False)
print(f"Saved comp_base_{START_YEAR}.parquet", comp.shape)

# Optional: save a trimmed base to reduce size
base_cols = req_keys + req_denoms + req_lags
base_cols = [c for c in base_cols if c in comp.columns]
comp_base_small = comp[base_cols].copy()
comp_base_small.to_parquet(f"comp_base_small_{START_YEAR}.parquet", engine="fastparquet", index=False)
print(f"Saved comp_base_small_{START_YEAR}.parquet", comp_base_small.shape)








# %%
# ------------------------------
# Step 5 — Link Compustat (comp_base) to CRSP via CCM (SAS t4)
# ------------------------------

import wrds
import pandas as pd
import numpy as np
import warnings
warnings.filterwarnings('ignore')
import sys

def end_step(name):
    print(f"--- End of step: {name} ---", flush=True)

print("Step 1: Building CCM date-valid link (t4) ...")

import wrds
import pandas as pd

db = wrds.Connection(wrds_username="lewgaowei")

# Load comp_base (saved earlier) if not in memory
try:
    comp_base  # noqa: F821
except NameError:
    comp_base = pd.read_parquet(f"comp_base_{START_YEAR}.parquet")

comp_base["datadate"] = pd.to_datetime(comp_base["datadate"])

# Prefer SAS-like library; fall back if needed
def _get_ccm_link(_db):
    for lib in ["ccm", "crsp_a_ccm", "crsp"]:
        try:
            df = _db.get_table(lib, "ccmxpf_linktable",
                               columns=["lpermno","gvkey","linkdt","linkenddt","linktype","usedflag"])
            if len(df):
                print(f"Loaded ccmxpf_linktable from {lib}")
                return df
        except Exception:
            pass
    raise RuntimeError("ccmxpf_linktable not found in ccm/crsp_a_ccm/crsp.")

link = _get_ccm_link(db)
link = link.query("linktype in ['LU','LC','LN','LS','LX'] & usedflag == 1").copy()
link["permno"] = link["lpermno"]
link["linkdt"] = pd.to_datetime(link["linkdt"], errors="coerce")
link["linkenddt"] = pd.to_datetime(link["linkenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
link = link[["permno","gvkey","linkdt","linkenddt"]]

# Keys from comp_base (SAS t3 equivalent)
keys = comp_base[["gvkey","datadate","year"]].drop_duplicates()

# SAS date-valid join (L421–L427)
t4 = keys.merge(link, on="gvkey", how="inner")
mask = ((t4["linkdt"].isna()) | (t4["linkdt"] <= t4["datadate"])) & \
       ((t4["linkenddt"].isna()) | (t4["datadate"] <= t4["linkenddt"]))
t4 = t4.loc[mask].copy()

print(f"t4 shape: {t4.shape}")
print(list(t4.columns))
end_step("Step 5 — CCM link (t4)")




# %%
# ------------------------------
# Step 6 — Build CRSP inputs and form t5/t6/t7/a0 (match SAS L51–L87, L431–L468)
# ------------------------------
print("Step 6: Building CRSP mktcap (Dec/June), price; merging to t5/t6/t7/a0 ...")

try:
    db  # noqa: F821
except NameError:
    import wrds
    print("Connecting to WRDS...")
    db = wrds.Connection(wrds_username="lewgaowei")

# Load CRSP msf (monthly) and compute market cap (SAS: mktcap = abs(prc)*shrout)
msf = db.get_table("crsp", "msf", columns=["permno","date","prc","shrout"])
msf["date"] = pd.to_datetime(msf["date"])
msf["year"] = msf["date"].dt.year
msf["month"] = msf["date"].dt.month
msf["mktcap"] = msf["prc"].abs() * msf["shrout"]

# Price dataset (SAS L89–L97)
price = msf[["permno","date"]].copy()
price["price"] = msf["prc"].abs()

# December market cap (SAS L63–L68)
decmktcap = (msf.loc[msf["month"] == 12, ["permno","year","mktcap"]]
               .rename(columns={"mktcap":"crsp_mktcap_12"}))

# June market cap (SAS L72–L77) — keep June date for later filters
junemktcap = (msf.loc[msf["month"] == 6, ["permno","year","mktcap","date"]]
               .rename(columns={"mktcap":"crsp_mktcap_6"}))

# Merge sequence (SAS L433–L457)
if "t4" not in globals():
    raise RuntimeError("t4 not found. Run Step 1 first.")

t5 = t4.merge(decmktcap, on=["permno","year"], how="left")     # SAS t5
t6 = t5.copy()
t6["year"] = t6["year"] + 1                                     # SAS t6: year = year + 1
t7 = t6.merge(junemktcap, on=["permno","year"], how="left")     # SAS t7
a0 = t7[t7["date"].notna()].copy()                              # SAS a0: require next June date

print(f"t5 shape: {t5.shape}, t6 shape: {t6.shape}, t7 shape: {t7.shape}, a0 shape: {a0.shape}")
end_step("Step 6 — CRSP inputs + t5/t6/t7/a0")


# %%
# ------------------------------
# Step 7 — Merge price, add CRSP header (msenames), map FF48, apply SAS filters → a1/a2/a3/a33
# ------------------------------
print("Step 7: Merging price, adding CRSP header via msenames, mapping FF48, applying SAS filters ...")

# Ensure price exists
if "price" not in globals():
    price = msf[["permno","date"]].copy()
    price["price"] = msf["prc"].abs()

if "a0" not in globals():
    raise RuntimeError("a0 not found. Run Step 2 first.")

# a1: add price
a1 = a0.merge(price, on=["permno","date"], how="left")

# CRSP msenames for shrcd/siccd (date-valid header)
msenames = db.get_table("crsp", "msenames",
                        columns=["permno","namedt","nameendt","shrcd","siccd"])
msenames["namedt"] = pd.to_datetime(msenames["namedt"], errors="coerce")
msenames["nameendt"] = pd.to_datetime(msenames["nameendt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
a2 = (a1.merge(msenames, on="permno", how="left")
         .loc[lambda d: (d["date"] >= d["namedt"]) & (d["date"] <= d["nameendt"])]
         .copy())

# Map SIC → FF48 industry (ind)
ff48 = db.get_table("ff", "industry48")
ff48.columns = [c.lower() for c in ff48.columns]
if "ffindnumber" in ff48.columns:
    ff48 = ff48.rename(columns={"ffindnumber": "ind"})
elif "indgrp" in ff48.columns:
    ff48 = ff48.rename(columns={"indgrp": "ind"})
elif "ffindustry" in ff48.columns:
    ff48 = ff48.rename(columns={"ffindustry": "ind"})
else:
    raise KeyError(f"No FF48 id column in ff.industry48: {list(ff48.columns)}")
for c in ("sic1","sic2"):
    if c not in ff48.columns:
        raise KeyError(f"No {c} in ff.industry48")
ff48["sic1"] = pd.to_numeric(ff48["sic1"], errors="coerce")
ff48["sic2"] = pd.to_numeric(ff48["sic2"], errors="coerce")
ff48 = ff48.loc[ff48["sic1"].notna() & ff48["sic2"].notna(), ["sic1","sic2","ind"]]

a2_sic = a2[["permno","date","siccd"]].drop_duplicates().copy()
ff48["__k"] = 1; a2_sic["__k"] = 1
map48 = (a2_sic.merge(ff48, on="__k", how="inner")
               .loc[lambda d: (d["siccd"] >= d["sic1"]) & (d["siccd"] <= d["sic2"]),
                    ["permno","date","ind"]]
               .drop_duplicates())
ff48.drop(columns="__k", inplace=True); a2_sic.drop(columns="__k", inplace=True)
a2 = a2.merge(map48, on=["permno","date"], how="left")

# SAS filters (use shrcd, price, siccd, year window)
LOWER_YEAR = 1963
UPPER_YEAR = None  # set to 2025 if desired

year_ok = a2["date"].dt.year >= LOWER_YEAR
if UPPER_YEAR is not None:
    year_ok &= a2["date"].dt.year <= UPPER_YEAR

a3 = a2[
    (a2["shrcd"].isin([10, 11])) &
    (a2["price"] >= 1) &
    year_ok &
    ~((a2["siccd"] >= 6000) & (a2["siccd"] < 7000))
].copy()

# a33: formation keys (keep industry)
a33 = a3.rename(columns={"date": "form_date"})[["gvkey","datadate","permno","form_date","crsp_mktcap_6","ind"]].copy()

print("a2 rows:", len(a2), "a3 rows:", len(a3), "a33 rows:", len(a33))
end_step("Step 7 — price + msenames + FF48 + filters (a1/a2/a3/a33)")







# %%
# %%
# ------------------------------
# Step 8 — Merge Compustat base (comp_base) with formation keys (a33)
# ------------------------------
print("Step 8: Merging comp_base with a33 (forming observations only) and reporting drops ...")

# Preconditions
for c in ["gvkey","datadate","year"]:
    if c not in comp_base.columns:
        raise KeyError(f"comp_base missing '{c}'")
for c in ["gvkey","datadate","permno","form_date","crsp_mktcap_6"]:
    if c not in a33.columns:
        raise KeyError(f"a33 missing '{c}'")

# Ensure datetime types
comp_base["datadate"] = pd.to_datetime(comp_base["datadate"])
a33["datadate"] = pd.to_datetime(a33["datadate"])
a33["form_date"] = pd.to_datetime(a33["form_date"])

# Baseline unique (gvkey, datadate) from comp_base
base_keys = comp_base[["gvkey","datadate","year"]].drop_duplicates()
n_base = len(base_keys)

# Merge by gvkey, datadate (inner) to keep only forming rows (SAS: a3 → a33)
comp_form = comp_base.merge(
    a33[["gvkey","datadate","permno","form_date","crsp_mktcap_6"]],
    on=["gvkey","datadate"],
    how="inner"
).drop_duplicates(subset=["gvkey","datadate","permno","form_date"])

# Unique (gvkey, datadate) after restricting to forming obs
form_keys = comp_form[["gvkey","datadate"]].drop_duplicates()
n_form = len(form_keys)
n_dropped = n_base - n_form
pct_dropped = (n_dropped / n_base * 100.0) if n_base else 0.0

print("comp_form shape:", comp_form.shape)
print("Form_date range:", comp_form["form_date"].min(), "→", comp_form["form_date"].max())
print("Unique permno:", comp_form["permno"].nunique())
print(f"Base unique (gvkey, datadate): {n_base}")
print(f"Kept unique (gvkey, datadate): {n_form}")
print(f"Dropped: {n_dropped} ({pct_dropped:.2f}%)")

# Year-by-year drops based on comp_base.year
yr_merge = base_keys.merge(form_keys, on=["gvkey","datadate"], how="left", indicator=True)
yr_stats = (yr_merge
            .assign(kept=lambda d: (d["_merge"] == "both").astype(int))
            .groupby("year", as_index=True)
            .agg(base=("gvkey","count"),
                 kept=("kept","sum"))
            .assign(dropped=lambda d: d["base"] - d["kept"],
                    pct_dropped=lambda d: d["dropped"] / d["base"] * 100.0))
print("Year-by-year base/kept/dropped (%):")
print(yr_stats)

# Persist for downstream per-variable (%fs) processing
comp_form.to_parquet(f"comp_form_{START_YEAR}.parquet", engine="fastparquet", index=False)
print(f"Saved comp_form_{START_YEAR}.parquet")

end_step("Step 8 — comp_base merged with a33 and drops reported")


# %%
# ------------------------------
# Full drop-diagnostics from comp_base → t4 (link) → a0 (next-June) → a3 (filters)
# ------------------------------
print("Running full diagnostics (using shrcd from msenames)...")

import pandas as pd
import numpy as np

def uniq(df, cols):
    return df[cols].drop_duplicates()

# Preconditions
for c in ["gvkey","datadate","year"]:
    if c not in comp_base.columns:
        raise KeyError(f"comp_base missing '{c}'")
for obj, name in [(t4,"t4"), (a0,"a0"), (a2,"a2"), (a3,"a3")]:
    if obj is None:
        raise RuntimeError(f"{name} is not defined. Ensure Steps 1–3 ran.")

# Normalize types
comp_base["datadate"] = pd.to_datetime(comp_base["datadate"], errors="coerce")
for df in (t4, a0, a2, a3):
    if "datadate" in df.columns:
        df["datadate"] = pd.to_datetime(df["datadate"], errors="coerce")

# Stage keys
base_keys = uniq(comp_base, ["gvkey","datadate","year"])
t4_keys   = uniq(t4,  ["gvkey","datadate"]).assign(linked=1)
a0_keys   = uniq(a0,  ["gvkey","datadate"]).assign(have_june=1)
a3_keys   = uniq(a3,  ["gvkey","datadate"]).assign(kept=1)

# Build null-safe filter flags on a2 (pre-filter table) — use shrcd and siccd
a2_tmp = a2.copy()

# ok_share via shrcd (10/11)
a2_tmp["ok_share"] = a2_tmp.get("shrcd", pd.Series(index=a2_tmp.index)).isin([10, 11]).fillna(False).astype(int)

# ok_price via price >= 1
price_series = pd.to_numeric(a2_tmp.get("price", pd.Series(index=a2_tmp.index)), errors="coerce")
a2_tmp["ok_price"] = (price_series >= 1).fillna(False).astype(int)

# ok_ind via siccd not in [6000,7000)
sic_series = pd.to_numeric(a2_tmp.get("siccd", pd.Series(index=a2_tmp.index)), errors="coerce")
fin_band = ((sic_series >= 6000) & (sic_series < 7000)).fillna(False)
a2_tmp["ok_ind"] = (~fin_band).astype(int)

# ok_year
LOWER_YEAR = 1963
UPPER_YEAR = None  # set to 2025 to cap, or leave None
years = a2_tmp["date"].dt.year
year_ok = (years >= LOWER_YEAR)
if UPPER_YEAR is not None:
    year_ok &= (years <= UPPER_YEAR)
a2_tmp["ok_year"] = year_ok.fillna(False).astype(int)

# Collapse to gvkey-datadate: if any record on that datadate passes a flag → pass
a2_flags = (a2_tmp
            .groupby(["gvkey","datadate"], as_index=False)
            .agg(ok_share=("ok_share","max"),
                 ok_price=("ok_price","max"),
                 ok_ind=("ok_ind","max"),
                 ok_year=("ok_year","max")))

# Build diagnostic frame
diag = (base_keys
        .merge(t4_keys, on=["gvkey","datadate"], how="left")
        .merge(a0_keys, on=["gvkey","datadate"], how="left")
        .merge(a2_flags, on=["gvkey","datadate"], how="left")
        .merge(a3_keys, on=["gvkey","datadate"], how="left"))

for c in ["linked","have_june","ok_share","ok_price","ok_ind","ok_year","kept"]:
    diag[c] = diag[c].fillna(0).astype(int)

def reason_row(r):
    if r["kept"] == 1: return "kept"
    if r["linked"] == 0: return "no_ccm_link_at_datadate"
    if r["have_june"] == 0: return "no_next_june_mktcap"
    if r["ok_share"] == 0: return "not_common_share(shrcd!=10/11)"
    if r["ok_price"] == 0: return "price<1"
    if r["ok_ind"] == 0: return "financials_excluded(6000-6999)"
    if r["ok_year"] == 0: return "outside_year_window"
    return "other"

diag["reason"] = diag.apply(reason_row, axis=1)

# Stage counts
print("\nStage unique (gvkey, datadate):")
print({
    "base": len(base_keys),
    "linked_t4": len(t4_keys),
    "have_june_a0": len(a0_keys),
    "kept_a3": len(a3_keys),
})

# Overall reasons
total = len(diag)
reason_tbl = (diag["reason"].value_counts()
              .to_frame("count")
              .assign(pct=lambda d: d["count"]/total*100))
print("\nPrimary drop reasons (overall):")
print(reason_tbl)

# Year-by-year kept/dropped
yr = (diag.assign(kept_flag=diag["kept"].astype(int))
          .groupby("year", as_index=True)
          .agg(base=("gvkey","count"),
               kept=("kept_flag","sum")))
yr["dropped"] = yr["base"] - yr["kept"]
yr["pct_kept"] = yr["kept"]/yr["base"]*100
yr["pct_dropped"] = yr["dropped"]/yr["base"]*100
print("\nYear-by-year kept/dropped (%):")
print(yr)

# Filter pass rates on the raw a2 universe
filt_rates = (a2_tmp.assign(y=a2_tmp["date"].dt.year)
              .groupby("y")[["ok_share","ok_price","ok_ind","ok_year"]]
              .mean()
              .mul(100))
print("\nFilter pass rates on a2 (%):")
print(filt_rates)



#  %%
# ------------------------------
# Step: Diagnostic summary of industry
# ------------------------------

print("ind in a3:", "ind" in a3.columns, "  ind in a33:", "ind" in a33.columns)
print("a3 ind missing %:", a3["ind"].isna().mean()*100 if "ind" in a3.columns else "n/a")

#  %%
# ------------------------------
# Step: Diagnostic summary of tickers
# ------------------------------

import pandas as pd

try:
    db  # WRDS connection
except NameError:
    import wrds
    db = wrds.Connection(wrds_username="lewgaowei")

# Load CRSP stocknames for tickers (date-valid)
stocknames = db.get_table("crsp", "stocknames",
                          columns=["permno","namedt","nameenddt","ticker","comnam"])
stocknames["namedt"] = pd.to_datetime(stocknames["namedt"], errors="coerce")
stocknames["nameenddt"] = pd.to_datetime(stocknames["nameenddt"], errors="coerce").fillna(pd.Timestamp("2099-12-31"))
stocknames["ticker"] = stocknames["ticker"].astype(str).str.upper()

# 1) Tickers in comp_base (use t4 date-valid link on datadate)
comp_base["datadate"] = pd.to_datetime(comp_base["datadate"], errors="coerce")
t4_keys = t4[["gvkey","datadate","permno"]].drop_duplicates().copy()

cb_tix = (t4_keys
          .merge(stocknames, on="permno", how="left")
          .loc[lambda d: (d["datadate"] >= d["namedt"]) & (d["datadate"] <= d["nameenddt"])]
          [["permno","ticker","comnam"]]
          .dropna(subset=["ticker"])
          .drop_duplicates()
          .sort_values(["ticker","permno"])
         )

print(f"Tickers in comp_base (unique): {cb_tix['ticker'].nunique()}")
print(cb_tix.head(20))
cb_tix.to_csv("comp_base_tickers.csv", index=False)
print("Saved comp_base_tickers.csv")

# 2) Tickers in formed panel (a33, by form_date)
a33["form_date"] = pd.to_datetime(a33["form_date"], errors="coerce")
formed_keys = a33[["permno","form_date"]].drop_duplicates().copy()

formed_tix = (formed_keys
              .merge(stocknames, on="permno", how="left")
              .loc[lambda d: (d["form_date"] >= d["namedt"]) & (d["form_date"] <= d["nameenddt"])]
              [["permno","ticker","comnam"]]
              .dropna(subset=["ticker"])
              .drop_duplicates()
              .sort_values(["ticker","permno"])
             )

print(f"Tickers in formed panel (a33, unique): {formed_tix['ticker'].nunique()}")
print(formed_tix.head(20))
formed_tix.to_csv("formed_tickers.csv", index=False)
print("Saved formed_tickers.csv")


# # %%
# Only run this if skipping step 1

# ALL_ACCOUNTING_VARIABLES = [
#     # Basic identifiers and denominators
#     "gvkey", "datadate", "fyear", "fyr", "indfmt", "datafmt", "popsrc", "consol",
#     "csho", "prcc_f",  # for market cap
#     "at", "act", "invt", "ppent", "lt", "lct", "dltt", "ceq", "seq", "icapt",
#     "sale", "cogs", "xsga", "emp",  # denominators
    
#     # ALL the accounting variables from SAS (240+ variables) - EXACT COPY
#     "acchg", "aco", "acox", "act", "am", "ao", "aoloch", "aox", "ap", "apalch", 
#     "aqc", "aqi", "aqs", "at", "bast", "caps", "capx", "capxv", "ceq", "ceql", 
#     "ceqt", "ch", "che", "chech", "cld2", "cld3", "cld4", "cld5", "cogs", 
#     "cstk", "cstkcv", "cstke", "dc", "dclo", "dcom", "dcpstk", "dcs", "dcvsr", 
#     "dcvsub", "dcvt", "dd", "dd1", "dd2", "dd3", "dd4", "dd5", "dfs", "dfxa", 
#     "diladj", "dilavx", "dlc", "dlcch", "dltis", "dlto", "dltp", "dltr", "dltt", 
#     "dm", "dn", "do", "donr", "dp", "dpacb", "dpacc", "dpacli", "dpacls", 
#     "dpacme", "dpacnr", "dpaco", "dpact", "dpc", "dpvieb", "dpvio", "dpvir", 
#     "drc", "drlt", "ds", "dudd", "dv", "dvc", "dvp", "dvpa", "dvpibb", "dvt", 
#     "dxd2", "dxd3", "dxd4", "dxd5", "ebit", "ebitda", "esopct", "esopdlt", 
#     "esopnr", "esopr", "esopt", "esub", "esubc", "exre", "fatb", "fatc", 
#     "fate", "fatl", "fatn", "fato", "fatp", "fca", "fiao", "fincf", "fopo", 
#     "fopox", "fopt", "fsrco", "fsrct", "fuseo", "fuset", "gdwl", "gp", "ib", 
#     "ibadj", "ibc", "ibcom", "icapt", "idit", "intan", "intc", "intpn", 
#     "invch", "invfg", "invo", "invrm", "invt", "invwip", "itcb", "itci", 
#     "ivaco", "ivaeq", "ivao", "ivch", "ivncf", "ivst", "ivstch", "lco", 
#     "lcox", "lcoxdr", "lct", "lifr", "lo", "loxdr", "lse", "lt", "mib", 
#     "mii", "mrc1", "mrc2", "mrc3", "mrc4", "mrc5", "mrct", "msa", "ni", 
#     "niadj", "nieci", "nopi", "nopio", "np", "oancf", "ob", "oiadp", "oibdp", 
#     "pi", "pidom", "pifo", "ppegt", "ppenb", "ppenc", "ppenli", "ppenls", 
#     "ppenme", "ppennr", "ppeno", "ppent", "ppevbb", "ppeveb", "ppevo", 
#     "ppevr", "prstkc", "pstk", "pstkc", "pstkl", "pstkn", "pstkr", "pstkrv", 
#     "rdip", "re", "rea", "reajo", "recch", "recco", "recd", "rect", "recta", 
#     "rectr", "reuna", "reunr", "revt", "sale", "seq", "siv", "spi", "sppe", 
#     "sppiv", "sstk", "tlcf", "tstk", "tstkc", "tstkme", "tstkp", "txach", 
#     "txbco", "txc", "txdb", "txdba", "txdbca", "txdbcl", "txdc", "txdfed", 
#     "txdfo", "txdi", "txditc", "txds", "txfed", "txfo", "txndb", "txndba", 
#     "txndbl", "txndbr", "txo", "txp", "txpd", "txr", "txs", "txt", "txw", 
#     "wcap", "wcapc", "wcapch", "xacc", "xad", "xdepl", "xdp", "xi", "xido", 
#     "xidoc", "xint", "xintd", "xopr", "xpp", "xpr", "xrd", "xrent", "xsga"
# ]

# # Remove duplicates while preserving order
# SAFE_ACCOUNTING_VARIABLES = []
# seen = set()
# for var in ALL_ACCOUNTING_VARIABLES:
#     if var not in seen:
#         SAFE_ACCOUNTING_VARIABLES.append(var)
#         seen.add(var)

# print(f"Total unique variables: {len(SAFE_ACCOUNTING_VARIABLES)}")

# # Configuration: Choose which variables to process
# # Option 1: Process only 4 variables (current setup - for testing)
# # SIGNALS_TO_PROCESS = ["at", "sale", "lt", "xsga"]

# # Option 2: Process ALL variables (uncomment to use - WARNING: Very large output!)
# SIGNALS_TO_PROCESS = [var for var in SAFE_ACCOUNTING_VARIABLES 
#                      if var not in ["gvkey", "datadate", "fyear", "fyr", 
#                                    "indfmt", "datafmt", "popsrc", "consol", 
#                                    "csho", "prcc_f"]]


# %%
# ------------------------------
# Step 9 — Build SAS-style long fsignal per variable (streamed, post-filter)
# ------------------------------
print("Step 9 — Build SAS-style long fsignal per variable (streamed, post-filter)")


comp_base = pd.read_parquet(f"comp_base_{START_YEAR}.parquet")
comp_form = pd.read_parquet(f"comp_form_{START_YEAR}.parquet")

# Reduce comp_base to gvkeys that actually form (safe for SAS lags)
formed_gvkeys = pd.Index(comp_form["gvkey"].unique())
comp_base = comp_base[comp_base["gvkey"].isin(formed_gvkeys)].copy()


print(f"comp_base reduced to formed gvkeys: {comp_base.shape}, gvkeys={comp_base['gvkey'].nunique()}")



# Denominators (15) for your var_over_den scheme
denominators = ["at","act","invt","ppent","lt","lct","dltt","ceq","seq","icapt",
                "sale","cogs","xsga","emp","mktcap"]

available_signals = [var for var in SIGNALS_TO_PROCESS if var in comp_base.columns]
missing_signals = [var for var in SIGNALS_TO_PROCESS if var not in comp_base.columns]

print(f"Available signals: {available_signals}"
      f"Missing signals: {missing_signals}"
      )


def safe_div(numer, denom):
    """SAS-style safe division with zero guard"""
    out = numer / denom
    out = out.where(denom > 0)  # SAS-style guard
    return out

def build_signals(df, v, denoms):
    out = pd.DataFrame(index=df.index)
    out["gvkey"] = df["gvkey"]
    out["datadate"] = df["datadate"]
    out["fyear"] = df["fyear"]
    out["year"] = df["year"]

    # Level
    out[f"{v}"] = df[v]

    # Level lag and deltas (compute, but don't keep 'lag'/'d' in output to match SAS)
    v_lag = df.groupby("gvkey")[v].shift(1)
    v_lag = v_lag.where(df["adjacent"] == 1)
    v_d = df[v] - v_lag
    v_pd = safe_div(df[v], v_lag) - 1
    out[f"{v}_pd"] = v_pd  # SAS keeps pd_var

    # Ratios to current denominators + their changes and pct changes
    for d in denoms:
        if d in df.columns:
            ratio = safe_div(df[v], df[d])
            out[f"{v}_over_{d}"] = ratio

            ratio_lag = ratio.groupby(df["gvkey"]).shift(1)
            ratio_lag = ratio_lag.where(df["adjacent"] == 1)

            out[f"{v}_over_{d}_d"] = ratio - ratio_lag                 # d_var_a..o
            out[f"{v}_over_{d}_pd"] = safe_div(ratio, ratio_lag) - 1    # pd_var_a..o

    # Δ normalized by lagged denominators (d_var_at..mktcap)
    for d in denoms:
        lag_col = f"{d}_lag"
        if lag_col in df.columns:
            out[f"{v}_d_over_{d}"] = safe_div(v_d, df[lag_col])

    # Relative %Δ vs denominators (pd_var_at..mktcap)
    for d in denoms:
        lag_col = f"{d}_lag"
        if lag_col in df.columns:
            pd_d = safe_div(df[d], df[lag_col]) - 1
            out[f"{v}_pd_minus_pd_{d}"] = v_pd - pd_d

    return out

# Build signals for all variables
print("Building signals...")
blocks = []
for i, v in enumerate(available_signals):
    if i % 10 == 0:
        print(f"Processing variable {i+1}/{len(available_signals)}: {v}")
    
    try:
        blk = build_signals(comp_base, v, denominators)
        blocks.append(blk)
    except Exception as e:
        print(f"Error processing {v}: {e}")
        continue
end_step("9. Build signal blocks")

# Join horizontally (keep keys only once)
print("Combining all signal blocks...")
if blocks:
    signals_df = blocks[0].copy()
    for b in blocks[1:]:
        # Drop key columns from subsequent blocks to avoid duplication
        key_cols = ["gvkey", "datadate", "fyear", "year"]
        b_clean = b.drop(columns=[col for col in key_cols if col in b.columns])
        signals_df = signals_df.join(b_clean, how='outer')
else:
    print("No blocks created!")
    signals_df = pd.DataFrame()
end_step("9/ Combine all signal blocks")

# after "Combining all signal blocks..."
signals_df = signals_df.sort_values(["gvkey", "datadate"]).reset_index(drop=True)
# Drop earliest two fiscal-year rows per gvkey (after signals are built)
signals_df = signals_df.merge(
    comp_base[["gvkey", "datadate", "firsttwo"]],
    on=["gvkey", "datadate"],
    how="left"
)
signals_df = signals_df[signals_df["firsttwo"] != 1].drop(columns=["firsttwo"])

# signals_df = signals_df[signals_df["rownum"] >= 2].drop(columns=["rownum"])
print(signals_df.head())
print(f"Final signals shape: {signals_df.shape}")


# %%
# ------------------------------
# 9b. Coverage diagnostics
# ------------------------------
print("Step 9b. Coverage diagnostics")

diag_cols = []
for v in available_signals:
    diag_cols += [f"{v}", f"{v}_lag", f"{v}_d", f"{v}_pd"]

# Only include columns that exist
diag_cols = [col for col in diag_cols if col in signals_df.columns]

coverage = (signals_df
            .assign(calyear=signals_df["datadate"].dt.year)
            .groupby("calyear")[diag_cols]
            .apply(lambda g: g.notna().sum())
            .reset_index())

print("Coverage by year:")
print(coverage.head())

print(f"Number of rows in signals_df: {len(signals_df)}")
print(f"Number of columns in signals_df: {len(signals_df.columns)}")


print(signals_df.columns)

end_step("Coverage diagnostics")


# %%
# ------------------------------
# 10. Save results
# ------------------------------
output_suffix = f"{len(available_signals)}vars"

# Full signals
signals_df.to_parquet(f"signals_{START_YEAR}.parquet", engine="fastparquet", index=False)

# Smaller preview
signals_df.head(1000).to_csv(f"signals_{START_YEAR}_sample.csv", index=False)

# Coverage diagnostics
coverage.to_csv(f"coverage_{START_YEAR}.csv", index=False)

print(f"Saved: signals_{START_YEAR}.parquet, signals_{START_YEAR}_sample.csv, coverage_{START_YEAR}.csv")
end_step("Save outputs")


# %%
# %%
# ------------------------------
# 11. Build 12-month forward returns (July→June), anchor to June formation
# ------------------------------

def end_step(name):
    print(f"--- End of step: {name} ---", flush=True)
print("Step 11: Building forward 12-month returns (July→June) ...")

import pandas as pd
import numpy as np

# Load saved signals and formed panelso
signals_df = pd.read_parquet(f"signals_{START_YEAR}.parquet", engine="fastparquet")
print(f"Loaded signals_df: {signals_df.shape}")

comp_form = pd.read_parquet(f"comp_form_{START_YEAR}.parquet")
comp_form["form_date"] = pd.to_datetime(comp_form["form_date"], errors="coerce")
formed_keys = (comp_form[["permno","form_date"]]
               .drop_duplicates()
               .assign(mindex=lambda d: d["form_date"].dt.year * 12 + d["form_date"].dt.month)
               [["permno","mindex"]])

# Ensure WRDS connection
try:
    db  # noqa: F821
except NameError:
    import wrds
    print("Connecting to WRDS ...")
    db = wrds.Connection(wrds_username="lewgaowei")

# Monthly returns from CRSP, with delisting returns
msf = db.get_table("crsp", "msf", columns=["permno","date","ret"])
msf["date"] = pd.to_datetime(msf["date"], errors="coerce")
msf = msf.dropna(subset=["permno","date"])  # basic sanity
msf["year"] = msf["date"].dt.year
msf["month"] = msf["date"].dt.month
msf["mindex"] = msf["year"] * 12 + msf["month"]

# Delisting returns (drop extreme dlret < -1 as in SAS)
dl = db.get_table("crsp", "msedelist", columns=["permno","dlstdt","dlret"])
dl["dlstdt"] = pd.to_datetime(dl["dlstdt"], errors="coerce")
dl = dl.loc[dl["dlret"].ge(-1) | dl["dlret"].isna(), ["permno","dlstdt","dlret"]].copy()
dl["year"] = dl["dlstdt"].dt.year
dl["month"] = dl["dlstdt"].dt.month
dl["mindex"] = dl["year"] * 12 + dl["month"]
dl = dl[["permno","mindex","dlret"]]

# Merge delisting returns into msf by permno + mindex
ret = msf.merge(dl, on=["permno","mindex"], how="left")
ret["ret"] = ret["ret"].where(ret["ret"].notna(), ret["dlret"])  # use dlret if ret missing
ret = ret.drop(columns=["dlret"])

# Build July→June cohorts
ret["cohort"] = ret["year"] - (ret["month"] < 7).astype(int)  # July..Dec → year; Jan..Jun → year-1
ret = ret.dropna(subset=["ret"])  # need valid monthly returns
ret["logret"] = np.log1p(ret["ret"])  # log(1+ret)

agg = (ret.groupby(["permno","cohort"], as_index=False)
          .agg(logret_sum=("logret","sum"),
               nmonth=("ret","count")))

agg["ret"] = np.expm1(agg["logret_sum"])  # exp(sum(logret)) - 1
agg["mindex"] = agg["cohort"] * 12 + 6   # anchor to June of cohort (mindex_begin - 1)
agg = agg[["permno","cohort","mindex","ret","nmonth"]].rename(columns={"cohort":"i"})

# Restrict to formed panel keys (permno, June formation mindex)
forward_returns = agg.merge(formed_keys, on=["permno","mindex"], how="inner")

print("Forward returns rows:", len(forward_returns),
      "unique permno:", forward_returns["permno"].nunique())

# Persist for Step 9/10
forward_returns.to_parquet("forward_returns.parquet", engine="fastparquet", index=False)
forward_returns.head(1000).to_csv("forward_returns_sample.csv", index=False)

end_step("Step 11 — forward 12M returns")

# %%
# ------------------------------
# 9. Build 12-month risk-free (July→June cohorts) and merge into forward returns
# ------------------------------
print("Step 12: Building cohort 12-month risk-free returns and merging ...")

import pandas as pd
import numpy as np

# FF monthly risk-free → build same cohort key as Step 8
ff = db.get_table("ff", "factors_monthly", columns=["date","rf"])  # rf in decimal
ff["date"] = pd.to_datetime(ff["date"], errors="coerce")
ff["year"] = ff["date"].dt.year
ff["month"] = ff["date"].dt.month
ff["cohort"] = ff["year"] - (ff["month"] < 7).astype(int)  # July..Dec → year; Jan..Jun → year-1
ff["logrf"] = np.log1p(ff["rf"])  # log(1+rf)

rf1 = (ff.groupby("cohort", as_index=False)
         .agg(logrf_sum=("logrf","sum")))
rf1["rf"] = np.expm1(rf1["logrf_sum"])  # exp(sum(logrf)) - 1
rf1 = rf1[["cohort","rf"]].rename(columns={"cohort":"i"})

# Load forward returns (Step 8), merge rf by i
forward_returns = pd.read_parquet("forward_returns.parquet", engine="fastparquet")
ret_with_rf = forward_returns.merge(rf1, on="i", how="left")

print("ret_with_rf rows:", len(ret_with_rf),
      "  rf missing %:", ret_with_rf["rf"].isna().mean()*100)

# Persist
ret_with_rf.to_parquet("forward_returns_with_rf.parquet", engine="fastparquet", index=False)
ret_with_rf.head(1000).to_csv("forward_returns_with_rf_sample.csv", index=False)

end_step("Step 12 — cohort RF merged into forward returns")

# %%
# ------------------------------
# 13. Merge signals with comp_form; attach forward returns + RF; diagnostics; save
# ------------------------------
print("Step 13: Merging signals with comp_form and attaching forward returns + RF ...")

import pandas as pd

# Load required inputs
signals_df = pd.read_parquet(f"signals_{START_YEAR}.parquet", engine="fastparquet")
comp_form = pd.read_parquet(f"comp_form_{START_YEAR}.parquet")
ret_with_rf = pd.read_parquet("forward_returns_with_rf.parquet", engine="fastparquet")

# Attach CRSP keys and formation date to signals via comp_form
for c in ["datadate","form_date"]:
    if c in comp_form.columns:
        comp_form[c] = pd.to_datetime(comp_form[c], errors="coerce")
signals_df["datadate"] = pd.to_datetime(signals_df["datadate"], errors="coerce")

sig_form = (signals_df
            .merge(comp_form[["gvkey","datadate","permno","form_date","crsp_mktcap_6"]],
                   on=["gvkey","datadate"], how="left"))

# Build form-month index and merge with forward returns (+ RF)
sig_form["mindex_form"] = sig_form["form_date"].dt.year * 12 + sig_form["form_date"].dt.month

merged = sig_form.merge(ret_with_rf, left_on=["permno","mindex_form"], right_on=["permno","mindex"], how="left")

# Diagnostics
n_total = len(merged)
miss_permno = merged["permno"].isna().mean()*100
miss_form = merged["form_date"].isna().mean()*100
miss_ret = merged["ret"].isna().mean()*100
miss_rf = merged["rf"].isna().mean()*100
print({
    "rows": n_total,
    "permno_missing_pct": miss_permno,
    "form_date_missing_pct": miss_form,
    "ret_missing_pct": miss_ret,
    "rf_missing_pct": miss_rf,
})

# Save outputs
out_name = f"signals_with_returns_{START_YEAR}.parquet"
merged.to_parquet(out_name, engine="fastparquet", index=False)
merged.head(1000).to_csv(f"signals_with_returns_{START_YEAR}_sample.csv", index=False)

print(f"Saved {out_name} and CSV sample")

end_step("Step 13 — merge + save")

# %%
# ------------------------------
# 14. Diagnostics and formed-only export
# ------------------------------
print("Step 14: Running diagnostics on signals_with_returns and exporting formed-only panel ...")

import pandas as pd
import numpy as np

df = pd.read_parquet(f"signals_with_returns_{START_YEAR}.parquet", engine="fastparquet")

# Missingness
miss = {
    "rows": len(df),
    "permno_missing_pct": df["permno"].isna().mean()*100,
    "form_date_missing_pct": df["form_date"].isna().mean()*100,
    "ret_missing_pct": df["ret"].isna().mean()*100,
    "rf_missing_pct": df["rf"].isna().mean()*100,
}
print(miss)

# Coverage by formation year
df["form_year"] = pd.to_datetime(df["form_date"], errors="coerce").dt.year
cov_year = (df.groupby("form_year")[ ["permno","ret","rf"] ]
              .agg(permno_nonmiss=("permno", lambda s: s.notna().sum()),
                   ret_nonmiss=("ret", lambda s: s.notna().sum()),
                   rf_nonmiss=("rf", lambda s: s.notna().sum()))
              .reset_index())

# nmonth distribution (formed only)
formed = df[df["permno"].notna() & df["form_date"].notna()].copy()
nmonth_dist = formed["nmonth"].value_counts().sort_index()

# Basic ret/rf stats (formed only)
stats = {
    "ret_mean": formed["ret"].mean(),
    "ret_median": formed["ret"].median(),
    "ret_std": formed["ret"].std(),
    "rf_mean": formed["rf"].mean(),
}
print("nmonth distribution (formed):")
print(nmonth_dist)
print("basic stats (formed):", stats)

# Save diagnostics
cov_year.to_csv(f"diagnostics_cov_by_year_{START_YEAR}.csv", index=False)
nmonth_dist.to_frame("count").to_csv(f"diagnostics_nmonth_dist_{START_YEAR}.csv")
pd.Series(stats).to_csv(f"diagnostics_basic_stats_{START_YEAR}.csv")

# Export formed-only dataset (SAS-style final panel)
out_final = formed.copy()
final_name = f"signals_with_returns_formed_{START_YEAR}.parquet"
out_final.to_parquet(final_name, engine="fastparquet", index=False)
out_final.head(1000).to_csv(f"signals_with_returns_formed_{START_YEAR}_sample.csv", index=False)
print(f"Saved {final_name} and CSV sample; diagnostics CSVs written")

end_step("Step 11 — diagnostics + formed-only export")

# Data accuracy notes
# 0% RF missing confirms cohort alignment is correct.
# nmonth should be mostly 12; values <12 indicate missing months in CRSP for that permno-year.
# SAS caps the sample to 1963.07–2019.12; we haven’t enforced that cap in Step 8/9. If you need strict parity, we can filter to those mindex bounds before aggregations.
# Delisting handling matches SAS: drop dlret < −1; if ret is missing and dlret ≥ −1, set ret = dlret.
# If you want, I can add the optional sample-window cap and basic diagnostics (nmonth distribution, RF range) next, then proceed to Step 10.

# %%
