"""
================================================================================
01-prepare-monthly-returns.py
================================================================================
Purpose: Extract monthly CRSP returns for each stock-year observation to enable
         monthly stop loss tracking

Input:  - signals_with_returns_and_tickers_{year}.parquet (for permno/form_date)
        - CRSP msf table (monthly stock file via WRDS)

Output: - monthly_returns_{year}.parquet with 12 monthly returns per observation

Author: Claude Code
Date:   2025-10-23
Version: 2.0 (Jupyter compatible with caching)
================================================================================
"""

# %%
# ===============================================================================
# IMPORTS AND CONFIGURATION
# ===============================================================================

import pandas as pd
import numpy as np
import wrds
from pathlib import Path
from tqdm.auto import tqdm
import warnings
import os
warnings.filterwarnings('ignore')

# Configuration
START_YEAR = 2000
WRDS_USERNAME = "lewgaowei"  # Update if needed
USE_CACHE = True  # Set to False to force recomputation

# File paths
BASE_DIR = Path.cwd()
INPUT_FILE = BASE_DIR / f"signals_with_returns_and_tickers_{START_YEAR}.parquet"
OUTPUT_FILE = BASE_DIR / f"monthly_returns_{START_YEAR}.parquet"

# Cache directory and files
CACHE_DIR = BASE_DIR / "cache_monthly_returns"
CACHE_DIR.mkdir(exist_ok=True)

CACHE_KEYS = CACHE_DIR / f"keys_{START_YEAR}.parquet"
CACHE_CRSP = CACHE_DIR / f"crsp_msf_{START_YEAR}.parquet"
CACHE_MONTHLY = CACHE_DIR / f"monthly_extracted_{START_YEAR}.parquet"
CACHE_TESLA_PRICES = CACHE_DIR / f"tesla_prices_{START_YEAR}.parquet"

print("=" * 80)
print("MONTHLY RETURNS PREPARATION - CRSP DATA EXTRACTION")
print("=" * 80)
print(f"Input:  {INPUT_FILE.name}")
print(f"Output: {OUTPUT_FILE.name}")
print(f"Cache:  {CACHE_DIR.name}/ (USE_CACHE={USE_CACHE})")
print("=" * 80)

# %%
# ===============================================================================
# STEP 1: LOAD EXISTING DATA
# ===============================================================================

print("\n" + "=" * 80)
print("STEP 1: LOADING EXISTING SIGNAL DATA")
print("=" * 80)

# Check if we have cached keys
if USE_CACHE and CACHE_KEYS.exists():
    print(f"📦 Loading cached keys from: {CACHE_KEYS.name}")
    keys = pd.read_parquet(CACHE_KEYS)
    print(f"✅ Loaded {len(keys):,} unique (permno, form_date) pairs from cache")
    print(f"   Date range: {keys['form_date'].min()} to {keys['form_date'].max()}")
else:
    print(f"📂 Loading signal data from: {INPUT_FILE.name}")
    df = pd.read_parquet(INPUT_FILE)
    print(f"✅ Loaded {len(df):,} observations")
    print(f"   Unique permnos: {df['permno'].nunique():,}")
    print(f"   Unique tickers: {df['ticker'].nunique():,}")
    print(f"   Date range: {df['form_date'].min()} to {df['form_date'].max()}")

    # Extract unique (permno, form_date, form_year) combinations
    keys = df[['permno', 'form_date', 'form_year']].drop_duplicates().copy()
    keys['form_date'] = pd.to_datetime(keys['form_date'])
    keys = keys.sort_values(['permno', 'form_date']).reset_index(drop=True)

    print(f"\n✅ Unique (permno, form_date) pairs: {len(keys):,}")
    print(f"   Sample:\n{keys.head(10)}")

    # Save to cache
    print(f"\n💾 Saving keys to cache: {CACHE_KEYS.name}")
    keys.to_parquet(CACHE_KEYS, index=False)

# %%
# ===============================================================================
# STEP 2: CONNECT TO WRDS AND LOAD CRSP MONTHLY DATA
# ===============================================================================

print("\n" + "=" * 80)
print("STEP 2: LOADING CRSP MONTHLY RETURNS")
print("=" * 80)

# Check if we have cached CRSP data
if USE_CACHE and CACHE_CRSP.exists():
    print(f"📦 Loading cached CRSP data from: {CACHE_CRSP.name}")
    msf = pd.read_parquet(CACHE_CRSP)
    msf['date'] = pd.to_datetime(msf['date'])
    print(f"✅ Loaded {len(msf):,} monthly return observations from cache")
    print(f"   Covering {msf['permno'].nunique():,} unique permnos")
    print(f"   Date range: {msf['date'].min()} to {msf['date'].max()}")
else:
    print(f"🔗 Connecting to WRDS as '{WRDS_USERNAME}'...")
    print("⚠️  This may take 10-15 minutes for first-time download")

    db = wrds.Connection(wrds_username=WRDS_USERNAME)

    # Determine date range to query (with buffer)
    min_form_date = keys['form_date'].min()
    max_form_date = keys['form_date'].max()

    # Query range: 1 month after earliest form_date to 13 months after latest
    query_start = min_form_date + pd.DateOffset(months=1)
    query_end = max_form_date + pd.DateOffset(months=13)

    print(f"📅 Query date range: {query_start.date()} to {query_end.date()}")
    print("📥 Loading CRSP monthly stock file (msf)...")

    # Load monthly returns
    msf = db.get_table("crsp", "msf", columns=["permno", "date", "ret"])
    msf['date'] = pd.to_datetime(msf['date'])

    # Filter to relevant date range and permnos
    relevant_permnos = keys['permno'].unique()
    msf = msf[
        (msf['date'] >= query_start) &
        (msf['date'] <= query_end) &
        (msf['permno'].isin(relevant_permnos))
    ].copy()

    print(f"✅ Loaded {len(msf):,} monthly return observations")
    print(f"   Covering {msf['permno'].nunique():,} unique permnos")
    print(f"   Date range: {msf['date'].min()} to {msf['date'].max()}")

    db.close()
    print("✅ WRDS connection closed")

    # Save to cache
    print(f"\n💾 Saving CRSP data to cache: {CACHE_CRSP.name}")
    msf.to_parquet(CACHE_CRSP, index=False)
    print(f"   Cache size: {CACHE_CRSP.stat().st_size / 1024 / 1024:.2f} MB")

# %%
# ===============================================================================
# STEP 3: EXTRACT 12-MONTH HOLDING PERIOD RETURNS
# ===============================================================================

print("\n" + "=" * 80)
print("STEP 3: EXTRACTING 12-MONTH HOLDING PERIOD RETURNS")
print("=" * 80)

# Check if we have cached monthly data
if USE_CACHE and CACHE_MONTHLY.exists():
    print(f"📦 Loading cached monthly returns from: {CACHE_MONTHLY.name}")
    monthly_df = pd.read_parquet(CACHE_MONTHLY)
    print(f"✅ Loaded {len(monthly_df):,} observations from cache")
else:
    def extract_holding_period_returns(permno, form_date, crsp_msf):
        """
        Extract 12 monthly returns starting from month after form_date.

        Parameters:
        -----------
        permno : int
            CRSP permno identifier
        form_date : pd.Timestamp
            Portfolio formation date
        crsp_msf : pd.DataFrame
            CRSP monthly returns data

        Returns:
        --------
        dict with monthly returns (ret_m1-ret_m12) and cumulative returns (cum_ret_m1-cum_ret_m12)
        """
        # Define holding period: month after form_date for 12 months
        start_date = form_date + pd.DateOffset(months=1)
        # Get first day of start month
        start_date = start_date.replace(day=1)

        # Filter to this stock's returns in the holding period
        stock_rets = crsp_msf[
            (crsp_msf['permno'] == permno) &
            (crsp_msf['date'] >= start_date)
        ].sort_values('date').head(12).copy()

        # Initialize result dictionary
        result = {
            'permno': permno,
            'form_date': form_date,
            'nmonth_available': len(stock_rets)
        }

        # Extract individual monthly returns
        monthly_returns = []
        for i in range(12):
            if i < len(stock_rets):
                ret = stock_rets.iloc[i]['ret']
                monthly_returns.append(ret)
                result[f'ret_m{i+1}'] = ret
            else:
                monthly_returns.append(np.nan)
                result[f'ret_m{i+1}'] = np.nan

        # Calculate cumulative returns at each month
        cumulative = 0.0
        for i, ret in enumerate(monthly_returns, 1):
            if pd.notna(ret):
                cumulative = (1 + cumulative) * (1 + ret) - 1
            result[f'cum_ret_m{i}'] = cumulative if pd.notna(ret) else np.nan

        return result

    # Process all observations with progress bar
    print(f"🔄 Processing {len(keys):,} observations...")
    monthly_data = []

    for idx, row in tqdm(keys.iterrows(), total=len(keys), desc="Extracting monthly returns"):
        result = extract_holding_period_returns(
            row['permno'],
            row['form_date'],
            msf
        )
        result['form_year'] = row['form_year']
        monthly_data.append(result)

    monthly_df = pd.DataFrame(monthly_data)

    print(f"\n✅ Extraction complete!")
    print(f"   Total observations: {len(monthly_df):,}")

    # Save to cache
    print(f"\n💾 Saving extracted monthly data to cache: {CACHE_MONTHLY.name}")
    monthly_df.to_parquet(CACHE_MONTHLY, index=False)

# %%
# ===============================================================================
# STEP 4: VALIDATE AND DIAGNOSE
# ===============================================================================

print("\n" + "=" * 80)
print("STEP 4: VALIDATION AND DIAGNOSTICS")
print("=" * 80)

# Check data completeness
print("\n📊 Data Completeness:")
print(f"  Observations with all 12 months: {(monthly_df['nmonth_available'] == 12).sum():,} ({(monthly_df['nmonth_available'] == 12).mean()*100:.1f}%)")
print(f"  Observations with <12 months:    {(monthly_df['nmonth_available'] < 12).sum():,} ({(monthly_df['nmonth_available'] < 12).mean()*100:.1f}%)")

print("\n📊 Missing Data by Month:")
for i in range(1, 13):
    missing_pct = monthly_df[f'ret_m{i}'].isna().mean() * 100
    print(f"  Month {i:2d}: {missing_pct:5.2f}% missing")

print("\n📊 Return Statistics (Month 1):")
print(monthly_df['ret_m1'].describe())

print("\n📊 Cumulative Return Statistics (Month 12):")
print(monthly_df['cum_ret_m12'].describe())

# Validate against original annual return (only if df is available)
if 'df' in locals():
    print("\n📊 Validation Against Original Annual Returns:")

    # Merge to compare
    validation = monthly_df.merge(
        df[['permno', 'form_date', 'ret', 'nmonth']],
        on=['permno', 'form_date'],
        how='left'
    )

    # Calculate discrepancies
    validation['annual_ret_calc'] = validation['cum_ret_m12']
    validation['discrepancy'] = (validation['ret'] - validation['annual_ret_calc']).abs()

    # Only compare where both are available
    valid_comparison = validation.dropna(subset=['ret', 'annual_ret_calc'])

    print(f"  Observations with both values: {len(valid_comparison):,}")
    print(f"  Correlation: {valid_comparison['ret'].corr(valid_comparison['annual_ret_calc']):.4f}")
    print(f"  Mean absolute discrepancy: {valid_comparison['discrepancy'].mean():.6f}")
    print(f"  Median absolute discrepancy: {valid_comparison['discrepancy'].median():.6f}")

    # Show largest discrepancies
    print("\n📊 Largest Discrepancies (Top 5):")
    top_disc = valid_comparison.nlargest(5, 'discrepancy')[
        ['permno', 'form_year', 'ret', 'annual_ret_calc', 'nmonth', 'nmonth_available', 'discrepancy']
    ]
    print(top_disc.to_string(index=False))

    print("\n💡 Note: Small discrepancies are expected due to:")
    print("   - Different aggregation methods (compound vs simple)")
    print("   - Timing differences in data extraction")
    print("   - Handling of missing months")
else:
    print("\n⚠️  Skipping validation (original data not loaded - using cache)")

# %%
# ===============================================================================
# STEP 5: SAVE OUTPUT
# ===============================================================================

print("\n" + "=" * 80)
print("STEP 5: SAVING FINAL OUTPUT")
print("=" * 80)

# Fix data types before saving to avoid parquet inference errors
print("\n🔧 Preparing data for parquet export...")

# Ensure form_date is datetime64
if monthly_df['form_date'].dtype == 'object':
    monthly_df['form_date'] = pd.to_datetime(monthly_df['form_date'])

# Ensure permno is int64 (convert via numeric first to handle any edge cases)
monthly_df['permno'] = pd.to_numeric(monthly_df['permno'], errors='coerce').astype('Int64')

# Ensure form_year is int (if it's a year)
if 'form_year' in monthly_df.columns:
    monthly_df['form_year'] = pd.to_numeric(monthly_df['form_year'], errors='coerce').astype('Int64')

# Ensure nmonth_available is int
monthly_df['nmonth_available'] = pd.to_numeric(monthly_df['nmonth_available'], errors='coerce').astype('Int64')

# Ensure all return columns are float64 (they may have NaN, so float is appropriate)
# Use pd.to_numeric with errors='coerce' to handle any non-numeric values
return_cols = [f'ret_m{i}' for i in range(1, 13)] + [f'cum_ret_m{i}' for i in range(1, 13)]
for col in return_cols:
    if col in monthly_df.columns:
        monthly_df[col] = pd.to_numeric(monthly_df[col], errors='coerce')

print(f"✅ Data types fixed:")
print(monthly_df.dtypes)

# Save using pyarrow engine (more robust than fastparquet)
print("\n💾 Saving to parquet...")
monthly_df.to_parquet(OUTPUT_FILE, engine="pyarrow", index=False)

print(f"\n✅ Monthly returns saved to: {OUTPUT_FILE.name}")
print(f"   File size: {OUTPUT_FILE.stat().st_size / 1024 / 1024:.2f} MB")
print(f"   Shape: {monthly_df.shape}")
print(f"   Columns: {list(monthly_df.columns)}")

# %%
# ===============================================================================
# SUMMARY
# ===============================================================================

print("\n" + "=" * 80)
print("SUMMARY")
print("=" * 80)

print(f"\n✅ Successfully prepared monthly returns for {len(monthly_df):,} observations")
print(f"\n📁 Output file: {OUTPUT_FILE}")
print(f"📁 Cache directory: {CACHE_DIR}")
print(f"   - {CACHE_KEYS.name} ({CACHE_KEYS.stat().st_size / 1024:.1f} KB)" if CACHE_KEYS.exists() else "")
print(f"   - {CACHE_CRSP.name} ({CACHE_CRSP.stat().st_size / 1024 / 1024:.2f} MB)" if CACHE_CRSP.exists() else "")
print(f"   - {CACHE_MONTHLY.name} ({CACHE_MONTHLY.stat().st_size / 1024 / 1024:.2f} MB)" if CACHE_MONTHLY.exists() else "")
print(f"   - {CACHE_TESLA_PRICES.name} ({CACHE_TESLA_PRICES.stat().st_size / 1024:.1f} KB)" if CACHE_TESLA_PRICES.exists() else "")

print(f"\n🔄 Next step: Run 04-apply-monthly-stoploss.py to analyze stop loss strategies")

print("\n💡 Note: Set USE_CACHE=False in configuration to force recomputation")

print("\n" + "=" * 80)
print("DONE!")
print("=" * 80)

# %%
# ===============================================================================
# STEP 6: DIAGNOSTIC VALIDATION (OPTIONAL)
# ===============================================================================

print("\n" + "=" * 80)
print("STEP 6: DIAGNOSTIC VALIDATION & EXCEL REPORT")
print("=" * 80)

# Load signals data if not already loaded (needed for validation)
if 'df' not in locals():
    print("\n📂 Loading signals data for validation...")
    df = pd.read_parquet(INPUT_FILE)
    print(f"   Loaded {len(df):,} observations")

print("\n🔍 Running validation checks...")

# Merge monthly with signals for comparison
validation_df = monthly_df.merge(
    df[['permno', 'form_date', 'ticker', 'ret', 'nmonth']],
    on=['permno', 'form_date'],
    how='left'
)

# Calculate discrepancies
validation_df['annual_from_monthly'] = validation_df['cum_ret_m12']
validation_df['annual_from_signals'] = validation_df['ret']
validation_df['discrepancy'] = (validation_df['annual_from_monthly'] - validation_df['annual_from_signals']).abs()

# Filter to valid comparisons
valid_comp = validation_df.dropna(subset=['annual_from_monthly', 'annual_from_signals'])

print("\n📊 VALIDATION RESULTS:")
print(f"  Total observations: {len(validation_df):,}")
print(f"  Comparable observations: {len(valid_comp):,}")
print(f"  Correlation: {valid_comp['annual_from_monthly'].corr(valid_comp['annual_from_signals']):.10f}")
print(f"  Mean absolute discrepancy: {valid_comp['discrepancy'].mean():.15f}")
print(f"  Max absolute discrepancy: {valid_comp['discrepancy'].max():.15f}")

# Check for real discrepancies
THRESHOLD = 0.001  # 0.1%
real_disc = valid_comp[valid_comp['discrepancy'] > THRESHOLD]
print(f"  Real discrepancies (>0.1%): {len(real_disc):,}")

if len(real_disc) == 0:
    print("\n✅ PERFECT MATCH: All monthly returns match annual returns!")

# Find Tesla as example
print("\n📊 EXAMPLE: TESLA (TSLA)")
tesla = validation_df[validation_df['ticker'].str.upper() == 'TSLA'].copy()

# Try to fetch Tesla monthly prices from WRDS or cache
tesla_prices_df = None
if len(tesla) > 0:
    print(f"  Found {len(tesla)} Tesla observations")

    # Check if we have cached Tesla prices
    if USE_CACHE and CACHE_TESLA_PRICES.exists():
        try:
            print("\n  📦 Loading cached Tesla prices...")
            tesla_prices = pd.read_parquet(CACHE_TESLA_PRICES)
            tesla_prices['date'] = pd.to_datetime(tesla_prices['date'])
            print(f"  ✅ Loaded {len(tesla_prices)} monthly price observations from cache")
            tesla_prices_df = tesla_prices

            # Add prices to tesla dataframe
            for idx, row in tesla.iterrows():
                form_date = row['form_date']
                start_date = (form_date + pd.DateOffset(months=1)).replace(day=1)

                # Get 12 months of prices
                for i in range(1, 13):
                    month_date = start_date + pd.DateOffset(months=i-1)
                    month_date = month_date.replace(day=1)

                    price_row = tesla_prices[tesla_prices['date'] == month_date]
                    if len(price_row) > 0:
                        tesla.loc[idx, f'prc_m{i}'] = price_row.iloc[0]['prc']

        except Exception as e:
            print(f"  ⚠️  Could not load cached Tesla prices: {e}")
    else:
        try:
            print("\n  🔗 Fetching Tesla monthly prices from WRDS...")
            tesla_permno = int(tesla.iloc[0]['permno'])

            db = wrds.Connection(wrds_username=WRDS_USERNAME)

            # Get Tesla's date range
            min_date = tesla['form_date'].min()
            max_date = tesla['form_date'].max()
            query_start = min_date + pd.DateOffset(months=1)
            query_end = max_date + pd.DateOffset(months=13)

            # Query Tesla prices
            tesla_prices = db.raw_sql(f"""
                SELECT date, prc, ret
                FROM crsp.msf
                WHERE permno = {tesla_permno}
                AND date >= '{query_start.date()}'
                AND date <= '{query_end.date()}'
                ORDER BY date
            """)

            db.close()

            tesla_prices['date'] = pd.to_datetime(tesla_prices['date'])
            tesla_prices['prc'] = tesla_prices['prc'].abs()  # CRSP uses negative for bid/ask average

            print(f"  ✅ Fetched {len(tesla_prices)} monthly price observations for Tesla")

            # Save to cache
            print(f"  💾 Saving Tesla prices to cache: {CACHE_TESLA_PRICES.name}")
            tesla_prices.to_parquet(CACHE_TESLA_PRICES, index=False)

            tesla_prices_df = tesla_prices

            # Add prices to tesla dataframe
            for idx, row in tesla.iterrows():
                form_date = row['form_date']
                start_date = (form_date + pd.DateOffset(months=1)).replace(day=1)

                # Get 12 months of prices
                for i in range(1, 13):
                    month_date = start_date + pd.DateOffset(months=i-1)
                    month_date = month_date.replace(day=1)

                    price_row = tesla_prices[tesla_prices['date'] == month_date]
                    if len(price_row) > 0:
                        tesla.loc[idx, f'prc_m{i}'] = price_row.iloc[0]['prc']

        except Exception as e:
            print(f"  ⚠️  Could not fetch Tesla prices: {e}")
            print("  (Monthly returns will still be available)")

    # Show one year in detail (preferably 2020)
    tesla_example = tesla[tesla['form_year'] == 2020] if 2020 in tesla['form_year'].values else tesla.iloc[[0]]

    if len(tesla_example) > 0:
        row = tesla_example.iloc[0]
        print(f"\n  Year {int(row['form_year'])} Detail:")
        print(f"  Permno: {int(row['permno'])}")
        print(f"  Form Date: {row['form_date']}")
        print(f"  Annual Return (signals): {row['annual_from_signals']:10.6f} ({row['annual_from_signals']*100:7.2f}%)")
        print(f"  Annual Return (monthly):  {row['annual_from_monthly']:10.6f} ({row['annual_from_monthly']*100:7.2f}%)")
        print(f"  Match: {'YES' if row['discrepancy'] < THRESHOLD else 'NO'}")
        print(f"  Months available: {int(row['nmonth_available'])}/12")

        print("\n  Month-by-month:")
        for i in range(1, 13):
            ret_val = row[f'ret_m{i}']
            cum_val = row[f'cum_ret_m{i}']
            prc_val = row.get(f'prc_m{i}', np.nan)

            if pd.notna(ret_val):
                if pd.notna(prc_val):
                    print(f"    M{i:2d}: ret={ret_val:8.4f}, cumulative={cum_val:8.4f}, price=${prc_val:8.2f}")
                else:
                    print(f"    M{i:2d}: ret={ret_val:8.4f}, cumulative={cum_val:8.4f}")
else:
    print("  Tesla (TSLA) not found in dataset")

# Create Excel diagnostic report
print("\n📝 Creating Excel diagnostic report...")

excel_file = BASE_DIR / f"monthly_returns_diagnostic_{START_YEAR}.xlsx"

with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
    # Sheet 1: Summary Statistics
    summary_data = pd.DataFrame({
        'Metric': [
            'Total Observations',
            'Comparable Observations',
            'Observations with all 12 months',
            'Observations with <12 months',
            'Correlation (monthly vs annual)',
            'Mean Absolute Discrepancy',
            'Max Absolute Discrepancy',
            'Real Discrepancies (>0.1%)',
            'Completeness Rate'
        ],
        'Value': [
            f"{len(validation_df):,}",
            f"{len(valid_comp):,}",
            f"{(validation_df['nmonth_available'] == 12).sum():,}",
            f"{(validation_df['nmonth_available'] < 12).sum():,}",
            f"{valid_comp['annual_from_monthly'].corr(valid_comp['annual_from_signals']):.10f}",
            f"{valid_comp['discrepancy'].mean():.15f}",
            f"{valid_comp['discrepancy'].max():.15f}",
            f"{len(real_disc):,}",
            f"{(validation_df['nmonth_available'] == 12).mean()*100:.2f}%"
        ]
    })
    summary_data.to_excel(writer, sheet_name='Summary', index=False)

    # Sheet 2: Tesla (if found) with all monthly returns and prices
    if len(tesla) > 0:
        # Start with basic info
        tesla_cols = [
            'ticker', 'permno', 'form_year', 'form_date',
            'annual_from_signals', 'annual_from_monthly', 'discrepancy',
            'nmonth_available'
        ]

        # Add all 12 monthly returns
        for i in range(1, 13):
            tesla_cols.append(f'ret_m{i}')

        # Add all 12 cumulative returns
        for i in range(1, 13):
            tesla_cols.append(f'cum_ret_m{i}')

        # Add all 12 monthly prices (if available)
        if f'prc_m1' in tesla.columns:
            for i in range(1, 13):
                tesla_cols.append(f'prc_m{i}')

        tesla_export = tesla[tesla_cols].copy()
        tesla_export.to_excel(writer, sheet_name='Tesla_Example', index=False)

    # Sheet 3: Stocks with missing monthly data
    missing_data = validation_df[validation_df['nmonth_available'] < 12][[
        'ticker', 'permno', 'form_year', 'form_date',
        'nmonth_available', 'annual_from_signals', 'annual_from_monthly'
    ]].copy()

    if len(missing_data) > 0:
        missing_data.to_excel(writer, sheet_name='Incomplete_Data', index=False)

    # Sheet 4: Real discrepancies (if any)
    if len(real_disc) > 0:
        disc_export = real_disc[[
            'ticker', 'permno', 'form_year', 'annual_from_signals',
            'annual_from_monthly', 'discrepancy', 'nmonth_available'
        ]].copy()
        disc_export.to_excel(writer, sheet_name='Discrepancies', index=False)

    # Sheet 5: Random sample for spot checking (with monthly returns)
    sample_size = min(100, len(validation_df))
    sample_cols = [
        'ticker', 'permno', 'form_year', 'annual_from_signals',
        'annual_from_monthly', 'discrepancy', 'nmonth_available'
    ]

    # Add all 12 monthly returns to sample
    for i in range(1, 13):
        sample_cols.append(f'ret_m{i}')

    # Add all 12 cumulative returns to sample
    for i in range(1, 13):
        sample_cols.append(f'cum_ret_m{i}')

    sample = validation_df.sample(sample_size, random_state=42)[sample_cols].copy()
    sample.to_excel(writer, sheet_name='Random_Sample_100', index=False)

print(f"\n✅ Excel report saved: {excel_file.name}")
print("   Sheets:")
print("   - Summary: Overall statistics")

if len(tesla) > 0 and f'prc_m1' in tesla.columns:
    print("   - Tesla_Example: Tesla with monthly returns (ret_m1-m12), cumulative (cum_ret_m1-m12), prices (prc_m1-m12)")
else:
    print("   - Tesla_Example: Tesla with all 12 monthly returns (ret_m1-m12, cum_ret_m1-m12)")

print("   - Incomplete_Data: Stocks with <12 months data")
print("   - Discrepancies: Stocks with real mismatches (if any)")
print("   - Random_Sample_100: 100 random stocks with all monthly returns")


print("\n" + "=" * 80)


# %%