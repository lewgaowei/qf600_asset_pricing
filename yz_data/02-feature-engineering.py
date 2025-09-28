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
from pathlib import Path
import xgboost as xgb
import time
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score

warnings.filterwarnings('ignore')
import sys

def end_step(name):
    print(f"--- End of step: {name} ---", flush=True)

START_YEAR = 2000
require_wrds_load = False
db = None
ALLOW_WRDS_LOOKUP = True  # set True only if WRDS is available

# ------------------------------
# 1. Load final signals-with-returns parquet
# ------------------------------
print("Loading formed panel with signals and forward returns ...")

_base_dir = Path(__file__).resolve().parent
data_path = _base_dir / f"signals_with_returns_and_tickers_{START_YEAR}.parquet"

df = pd.read_parquet(data_path, engine="fastparquet")

for c in ("datadate", "form_date"):
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")
        
df.head()

# %%
# ------------------------------
# 1a. Get Tickers from WRDS and Check for Missing Data
# ------------------------------
print("CHECKING TICKERS AND RETRIEVING FROM WRDS IF NEEDED")
print("=" * 60)

# Track initial column state
initial_columns = set(df.columns)
initial_column_count = len(df.columns)
print(f"INITIAL STATE:")
print(f"Total columns: {initial_column_count}")
print(f"DataFrame shape: {df.shape}")
print()

# Check if ticker column exists
has_ticker = 'ticker' in df.columns or 'tic' in df.columns

if has_ticker:
    ticker_col = 'ticker' if 'ticker' in df.columns else 'tic'
    print(f"Found ticker column: {ticker_col}")
    
    # Check for missing tickers
    missing_tickers = df[ticker_col].isna().sum()
    total_rows = len(df)
    missing_pct = missing_tickers / total_rows * 100
    
    print(f"Missing tickers: {missing_tickers:,} out of {total_rows:,} ({missing_pct:.2f}%)")
    
    if missing_tickers > 0:
        print("Some rows are missing tickers - will retrieve from WRDS")
        need_wrds_lookup = True
    else:
        print("✅ All rows have tickers")
        need_wrds_lookup = False
else:
    print("❌ No ticker column found - will retrieve from WRDS")
    need_wrds_lookup = True

# Retrieve tickers from WRDS if needed
if need_wrds_lookup and ALLOW_WRDS_LOOKUP:
    print("\nRetrieving tickers from WRDS...")
    
    try:
        # Connect to WRDS
        if db is None:
            print("Connecting to WRDS...")
            db = wrds.Connection(wrds_username="lewgaowei")
        
        # Get unique permnos that need tickers
        if has_ticker:
            # Get permnos with missing tickers
            missing_permno_df = df[df[ticker_col].isna()][['permno']].drop_duplicates()
        else:
            # Get all permnos
            missing_permno_df = df[['permno']].drop_duplicates()
        
        permnos_to_lookup = missing_permno_df['permno'].tolist()
        print(f"Looking up tickers for {len(permnos_to_lookup)} unique PERMNOs...")
        
        # Query WRDS for ticker mapping
        # Use CRSP stock names file to get tickers
        ticker_query = f"""
        SELECT DISTINCT permno, ticker, namedt, nameenddt
        FROM crsp.stocknames
        WHERE permno IN ({','.join(map(str, permnos_to_lookup))})
        AND ticker IS NOT NULL
        AND ticker != ''
        ORDER BY permno, namedt DESC
        """
        
        ticker_mapping = db.raw_sql(ticker_query)
        print(f"Retrieved {len(ticker_mapping)} ticker records from WRDS")
        
        # For each permno, get the most recent ticker
        latest_tickers = ticker_mapping.groupby('permno').first().reset_index()
        latest_tickers = latest_tickers[['permno', 'ticker']]
        
        print(f"Found tickers for {len(latest_tickers)} unique companies")
        
        # Merge tickers back to main dataset
        if has_ticker:
            # Update missing tickers only
            df_with_tickers = df.copy()
            
            # Create a mapping dict
            ticker_dict = dict(zip(latest_tickers['permno'], latest_tickers['ticker']))
            
            # Fill missing tickers
            mask = df_with_tickers[ticker_col].isna()
            df_with_tickers.loc[mask, ticker_col] = df_with_tickers.loc[mask, 'permno'].map(ticker_dict)
            
            # Check results
            remaining_missing = df_with_tickers[ticker_col].isna().sum()
            filled_count = missing_tickers - remaining_missing
            
            print(f"✅ Filled {filled_count} missing tickers")
            print(f"Still missing: {remaining_missing} tickers")
            
        else:
            # Add new ticker column
            df_with_tickers = df.merge(latest_tickers, on='permno', how='left')
            missing_after_merge = df_with_tickers['ticker'].isna().sum()
            
            print(f"✅ Added ticker column")
            print(f"Missing tickers after merge: {missing_after_merge} ({missing_after_merge/len(df)*100:.2f}%)")
        
        # Update main dataframe
        df = df_with_tickers
        ticker_col = 'ticker'
        
    except Exception as e:
        print(f"❌ Error retrieving tickers from WRDS: {str(e)}")
        print("Continuing without ticker data...")
        
elif need_wrds_lookup and not ALLOW_WRDS_LOOKUP:
    print("⚠️  WRDS lookup needed but ALLOW_WRDS_LOOKUP is False")
    print("Set ALLOW_WRDS_LOOKUP = True to enable ticker retrieval")

# Final ticker summary
if 'ticker' in df.columns or 'tic' in df.columns:
    ticker_col = 'ticker' if 'ticker' in df.columns else 'tic'
    
    print(f"\nFINAL TICKER SUMMARY:")
    print(f"Total rows: {len(df):,}")
    print(f"Rows with tickers: {df[ticker_col].notna().sum():,}")
    print(f"Rows missing tickers: {df[ticker_col].isna().sum():,}")
    print(f"Unique tickers: {df[ticker_col].nunique()}")
    
    # Show sample of tickers
    sample_tickers = df[df[ticker_col].notna()][ticker_col].value_counts().head(10)
    print(f"\nTop 10 most frequent tickers:")
    for ticker, count in sample_tickers.items():
        print(f"  {ticker}: {count} observations")
        
else:
    print("\n❌ No ticker data available")

# Track final column state and show changes
final_columns = set(df.columns)
final_column_count = len(df.columns)
new_columns = final_columns - initial_columns
removed_columns = initial_columns - final_columns

print("\n" + "=" * 60)
print("COLUMN SUMMARY - SECTION 1A")
print("=" * 60)
print(f"Initial columns: {initial_column_count}")
print(f"Final columns: {final_column_count}")
print(f"Net change: {final_column_count - initial_column_count:+d}")
print()

if new_columns:
    print(f"NEW COLUMNS ADDED ({len(new_columns)}):")
    for col in sorted(new_columns):
        print(f"  + {col}")
    print()

if removed_columns:
    print(f"COLUMNS REMOVED ({len(removed_columns)}):")
    for col in sorted(removed_columns):
        print(f"  - {col}")
    print()

if not new_columns and not removed_columns:
    print("No columns were added or removed (data updates only)")
    print()

print(f"Final DataFrame shape: {df.shape}")

if has_ticker == False:
    # Save DataFrame with tickers as parquet
    output_path = _base_dir / f"signals_with_returns_and_tickers_{START_YEAR}.parquet"
    df.to_parquet(output_path, engine="fastparquet")
    print(f"Saved DataFrame with tickers to: {output_path}")

    print("=" * 60)



# %%
# ------------------------------
# 1b. Check for Missing Tickers and Analyze Permno Frequency
# ------------------------------
print("CHECKING FOR MISSING TICKERS AND ANALYZING PERMNO FREQUENCY")
print("=" * 60)

# Check for missing tickers
ticker_cols = ['ticker', 'tic']
ticker_col = None

for col in ticker_cols:
    if col in df.columns:
        ticker_col = col
        break

if ticker_col:
    print(f"Using ticker column: {ticker_col}")
    
    # Check for missing tickers
    total_rows = len(df)
    missing_tickers = df[ticker_col].isna().sum()
    missing_pct = missing_tickers / total_rows * 100
    
    print(f"\nTICKER MISSING DATA ANALYSIS:")
    print(f"Total rows: {total_rows:,}")
    print(f"Rows with tickers: {(total_rows - missing_tickers):,}")
    print(f"Rows missing tickers: {missing_tickers:,} ({missing_pct:.2f}%)")
    
    if missing_tickers > 0:
        print(f"⚠️  WARNING: {missing_tickers:,} rows are missing ticker information")
        
        # Show sample of rows with missing tickers
        missing_sample = df[df[ticker_col].isna()][['permno', 'datadate', ticker_col]].head(10)
        print(f"\nSample rows with missing tickers:")
        print(missing_sample.to_string(index=False))
    else:
        print("✅ All rows have ticker information")
        
else:
    print("❌ No ticker column found in dataset")
    print(f"Available columns: {list(df.columns)}")

# Analyze permno frequency
print(f"\nPERMNO FREQUENCY ANALYSIS:")
if 'permno' in df.columns:
    permno_counts = df['permno'].value_counts()
    
    print(f"Total unique PERMNOs: {len(permno_counts):,}")
    print(f"Maximum times a PERMNO appears: {permno_counts.max():,}")
    print(f"Minimum times a PERMNO appears: {permno_counts.min():,}")
    print(f"Average times a PERMNO appears: {permno_counts.mean():.2f}")
    print(f"Median times a PERMNO appears: {permno_counts.median():.2f}")
    
    # Show top 10 most frequent PERMNOs
    print(f"\nTop 10 most frequent PERMNOs:")
    top_permnos = permno_counts.head(10)
    
    for permno, count in top_permnos.items():
        # Get ticker for this permno if available
        if ticker_col:
            sample_ticker = df[df['permno'] == permno][ticker_col].dropna()
            ticker_info = f" ({sample_ticker.iloc[0]})" if len(sample_ticker) > 0 else " (no ticker)"
        else:
            ticker_info = ""
            
        print(f"  PERMNO {permno}{ticker_info}: {count:,} observations")
    
    # Show distribution of permno frequencies
    print(f"\nPERMNO frequency distribution:")
    freq_dist = permno_counts.value_counts().sort_index()
    
    # Show key percentiles
    percentiles = [50, 75, 90, 95, 99]
    print(f"\nPercentiles of PERMNO observation counts:")
    for p in percentiles:
        pct_value = np.percentile(permno_counts, p)
        print(f"  {p}th percentile: {pct_value:.0f} observations")
    
    # Show companies with very high frequency (potential data issues)
    high_freq_threshold = np.percentile(permno_counts, 99)
    high_freq_permnos = permno_counts[permno_counts >= high_freq_threshold]
    
    if len(high_freq_permnos) > 0:
        print(f"\nCompanies with unusually high frequency (≥{high_freq_threshold:.0f} observations):")
        for permno, count in high_freq_permnos.items():
            if ticker_col:
                sample_ticker = df[df['permno'] == permno][ticker_col].dropna()
                ticker_info = f" ({sample_ticker.iloc[0]})" if len(sample_ticker) > 0 else " (no ticker)"
            else:
                ticker_info = ""
            print(f"  PERMNO {permno}{ticker_info}: {count:,} observations")
            
else:
    print("❌ No permno column found in dataset")
    print(f"Available columns: {list(df.columns)}")

print("=" * 60)
end_step("1b - Missing Ticker Check and Permno Analysis")

# %%
# ------------------------------
# 2. Analyze dataset structure - Base columns and Signal patterns
# ------------------------------
print("=" * 60)
print("DATASET STRUCTURE ANALYSIS")
print("=" * 60)

print(f"Dataset shape: {df.shape}")
print(f"Total columns: {len(df.columns)}")

# Base/metadata columns (non-signal columns)
print("BASE/METADATA COLUMNS:")
print("-" * 30)

base_columns = []
signal_columns = []

# Identify base columns (typically the first few columns that aren't signal transformations)
for col in df.columns:
    # Base columns don't have signal transformation patterns
    if not any(pattern in col for pattern in ['_over_', '_d_over_', '_pd_minus_pd_', '_pd']):
        # But exclude raw signal variables (accounting variables)
        if col in ['gvkey', 'datadate', 'fyear', 'year', 'permno', 'form_date', 
                   'crsp_mktcap_6', 'ret', 'rf', 'nmonth', 'i', 'mindex', 'mindex_form', 'ticker']:
            base_columns.append(col)
        else:
            # This might be a raw signal variable
            signal_columns.append(col)
    else:
        signal_columns.append(col)

print(f"Found {len(base_columns)} base columns:")
for i, col in enumerate(base_columns, 1):
    print(f"{i:2d}. {col}")

print(f"\nFound {len(signal_columns)} signal-related columns")
print(df.head())



# %%
# === 3. ANALYZE SIGNAL PATTERNS: SHOW FIRST 5 VARIABLES ===
# This section analyzes the signal columns to extract and display the first 5 unique base variables,
# along with their associated column patterns. It helps to understand the structure and diversity
# of engineered features derived from each base variable.
print("SIGNAL COLUMN PATTERNS (First 5 variables):")
print("-" * 50)

# Get unique base variable names (extract from signal columns)
base_vars = set()
for col in signal_columns:
    # Check most specific patterns first, then general ones
    if '_d_over_' in col:
        # For patterns like "acchg_d_over_at", extract "acchg" (remove the "_d" part)
        base_var = col.split('_d_over_')[0]
        if base_var.endswith('_d'):
            base_var = base_var[:-2]  # Remove trailing "_d"
        base_vars.add(base_var)
        
    elif '_pd_minus_pd_' in col:
        base_var = col.split('_pd_minus_pd_')[0]
        base_vars.add(base_var)
        
    elif '_over_' in col:
        # For patterns like "acchg_over_at" 
        base_var = col.split('_over_')[0]
        base_vars.add(base_var)
        
    elif col.endswith('_pd'):
        base_var = col[:-3]  # remove '_pd'
        base_vars.add(base_var)
    else:
        # Might be a raw variable
        base_vars.add(col)
        
base_vars = sorted(list(base_vars))
print(f"Total unique signal variables found: {len(base_vars)}")
print()

# Show patterns for first 5 variables
for i, var in enumerate(base_vars[:5], 1):
    print(f"{i}. Variable: '{var}' - Column patterns:")
    
    var_columns = []
    for col in df.columns:
        if col == var or col.startswith(f"{var}_"):
            var_columns.append(col)
    
    print(f"   Total columns for '{var}': {len(var_columns)}")
    
    # Group by pattern type
    patterns = {
        'base': [],
        'ratios': [],
        'ratio_changes': [], 
        'ratio_pct_changes': [],
        'delta_over_lag': [],
        'relative_pct_changes': [],
        'other': []
    }
    
    for col in var_columns:
        if col == var:
            patterns['base'].append(col)
        elif f'{var}_over_' in col and not '_d' in col.split('_over_')[1]:
            patterns['ratios'].append(col)
        elif f'{var}_over_' in col and col.endswith('_d'):
            patterns['ratio_changes'].append(col)
        elif f'{var}_over_' in col and col.endswith('_pd'):
            patterns['ratio_pct_changes'].append(col)
        elif f'{var}_d_over_' in col:
            patterns['delta_over_lag'].append(col)
        elif f'{var}_pd_minus_pd_' in col:
            patterns['relative_pct_changes'].append(col)
        elif col == f'{var}_pd':
            patterns['other'].append(col)
        else:
            patterns['other'].append(col)
    
    for pattern_name, cols in patterns.items():
        if cols:
            print(f"   {pattern_name.replace('_', ' ').title()}: {len(cols)} columns")
            if len(cols) <= 3:
                for col in cols:
                    print(f"     - {col}")
            else:
                print(f"     - {cols[0]}")
                print(f"     - {cols[1]}")
                print(f"     - ... ({len(cols)-2} more)")
    print()

print("=" * 60)

# %%
# ------------------------------
# 4. Create Excel output with signal analysis
# ------------------------------
# ------------------------------
if False:  # Skip Excel output section, set to TRUE to get Summary in Excel
    
    print("Creating Excel output with signal analysis...")

    import pandas as pd
    from collections import defaultdict

    # Create a comprehensive signal analysis
    signal_analysis = defaultdict(list)

    # Add base/metadata columns first
    base_metadata_cols = [col for col in base_columns if col in df.columns]
    print(f"Base/metadata columns: {len(base_metadata_cols)}")

    # For each true base variable, collect all its variations
    for var in base_vars:
        var_columns = []
        for col in df.columns:
            if (col == var or 
                col == f"{var}_pd" or
                col.startswith(f"{var}_over_") or
                col.startswith(f"{var}_d_over_") or
                col.startswith(f"{var}_pd_minus_pd_")):
                var_columns.append(col)
        
        # Store the variations for this variable
        signal_analysis[var] = var_columns

    # Create the Excel structure
    excel_data = []

    # First, add base/metadata columns info
    for i, col in enumerate(base_metadata_cols):
        row_data = {
            'Category': 'Base/Metadata',
            'Variable_Index': i + 1,
            'Column_Name': col,
            'Description': {
                'gvkey': 'Company identifier',
                'datadate': 'Fiscal year end date', 
                'fyear': 'Fiscal year',
                'year': 'Calendar year',
                'permno': 'CRSP permanent number',
                'form_date': 'Portfolio formation date (June)',
                'crsp_mktcap_6': 'Market cap in June',
                'ret': 'Total 12-month return',
                'rf': 'Risk-free rate',
                'expected_return': 'Excess return (ret - rf)',
                'nmonth': 'Number of months in return calculation',
                'mindex_form': 'Formation month index',
                'i': 'Cohort identifier',
                'mindex': 'Month index'
            }.get(col, 'Metadata column'),
            'Pattern_Type': 'Base',
            'Total_Missing_Pct': round(df[col].isna().mean() * 100, 2) if col in df.columns else 'N/A',
            'Mean': round(df[col].mean(), 4) if col in df.columns and pd.api.types.is_numeric_dtype(df[col]) else 'N/A',
            'Std': round(df[col].std(), 4) if col in df.columns and pd.api.types.is_numeric_dtype(df[col]) else 'N/A'
        }
        excel_data.append(row_data)

    # Then add signal variables and their variations
    for var_idx, (var, var_columns) in enumerate(signal_analysis.items(), 1):
        for col_idx, col in enumerate(var_columns):
            # Determine pattern type
            if col == var:
                pattern_type = 'Base_Variable'
                description = f'Raw {var} value'
            elif col == f"{var}_pd":
                pattern_type = 'Percentage_Change'
                description = f'% change in {var}'
            elif col.startswith(f"{var}_over_") and not col.endswith(('_d', '_pd')):
                denom = col.split(f"{var}_over_")[1]
                pattern_type = 'Ratio'
                description = f'{var} / {denom}'
            elif col.startswith(f"{var}_over_") and col.endswith('_d'):
                denom = col.split(f"{var}_over_")[1][:-2]  # remove '_d'
                pattern_type = 'Ratio_Change'
                description = f'Change in ({var} / {denom})'
            elif col.startswith(f"{var}_over_") and col.endswith('_pd'):
                denom = col.split(f"{var}_over_")[1][:-3]  # remove '_pd'
                pattern_type = 'Ratio_Pct_Change'
                description = f'% change in ({var} / {denom})'
            elif col.startswith(f"{var}_d_over_"):
                denom = col.split(f"{var}_d_over_")[1]
                pattern_type = 'Delta_Over_Lagged'
                description = f'Change in {var} / lagged {denom}'
            elif col.startswith(f"{var}_pd_minus_pd_"):
                denom = col.split(f"{var}_pd_minus_pd_")[1]
                pattern_type = 'Relative_Pct_Change'
                description = f'% change in {var} - % change in {denom}'
            else:
                pattern_type = 'Other'
                description = f'Other transformation of {var}'
            
            row_data = {
                'Category': 'Signal',
                'Variable_Index': var_idx,
                'Base_Variable': var,
                'Variation_Index': col_idx + 1,
                'Column_Name': col,
                'Description': description,
                'Pattern_Type': pattern_type,
                'Total_Missing_Pct': round(df[col].isna().mean() * 100, 2) if col in df.columns else 'N/A',
                'Mean': round(df[col].mean(), 4) if col in df.columns and pd.api.types.is_numeric_dtype(df[col]) else 'N/A',
                'Std': round(df[col].std(), 4) if col in df.columns and pd.api.types.is_numeric_dtype(df[col]) else 'N/A',
                'Total_Variations': len(var_columns)
            }
            excel_data.append(row_data)

    # Convert to DataFrame
    analysis_df = pd.DataFrame(excel_data)

    # Create summary statistics
    summary_stats = {
        'Total_Columns': len(df.columns),
        'Base_Metadata_Columns': len(base_metadata_cols),
        'Signal_Variables': len(base_vars),
        'Total_Signal_Columns': sum(len(cols) for cols in signal_analysis.values()),
        'Avg_Variations_Per_Signal': round(sum(len(cols) for cols in signal_analysis.values()) / len(base_vars), 1),
        'Dataset_Rows': len(df)
    }

    summary_df = pd.DataFrame([summary_stats])

    print(f"Analysis complete:")
    print(f"- {len(base_vars)} signal variables")
    print(f"- {sum(len(cols) for cols in signal_analysis.values())} total signal columns")
    print(f"- {len(base_metadata_cols)} base/metadata columns")

    # Save to Excel with multiple sheets
    excel_filename = f"signal_analysis_{len(base_vars)}vars.xlsx"

    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:
        # Summary sheet
        summary_df.to_excel(writer, sheet_name='Summary', index=False)
        
        # Full analysis sheet
        analysis_df.to_excel(writer, sheet_name='Full_Analysis', index=False)
        
        # Base/Metadata columns only
        base_analysis = analysis_df[analysis_df['Category'] == 'Base/Metadata'].copy()
        base_analysis.to_excel(writer, sheet_name='Base_Metadata', index=False)
        
        # Signal variables summary (one row per base variable)
        signal_summary = []
        for var, var_columns in signal_analysis.items():
            signal_summary.append({
                'Base_Variable': var,
                'Total_Variations': len(var_columns),
                'Base_Missing_Pct': round(df[var].isna().mean() * 100, 2) if var in df.columns else 'N/A',
                'Base_Mean': round(df[var].mean(), 4) if var in df.columns and pd.api.types.is_numeric_dtype(df[var]) else 'N/A',
                'Base_Std': round(df[var].std(), 4) if var in df.columns and pd.api.types.is_numeric_dtype(df[var]) else 'N/A',
                'Sample_Variations': ', '.join(var_columns[:3]) + ('...' if len(var_columns) > 3 else '')
            })
        
        signal_summary_df = pd.DataFrame(signal_summary)
        signal_summary_df.to_excel(writer, sheet_name='Signal_Summary', index=False)
        
        # Pattern type analysis
        pattern_analysis = analysis_df[analysis_df['Category'] == 'Signal'].groupby('Pattern_Type').agg({
            'Column_Name': 'count',
            'Total_Missing_Pct': 'mean',
            'Mean': lambda x: x.replace('N/A', pd.NA).astype(float).mean(),
            'Std': lambda x: x.replace('N/A', pd.NA).astype(float).mean()
        }).round(4)
        pattern_analysis.columns = ['Count', 'Avg_Missing_Pct', 'Avg_Mean', 'Avg_Std']
        pattern_analysis.to_excel(writer, sheet_name='Pattern_Analysis')

    print(f"Excel file saved: {excel_filename}")
    print("\nSheets created:")
    print("1. Summary - Overall statistics")
    print("2. Full_Analysis - Complete breakdown of all columns")
    print("3. Base_Metadata - Non-signal columns only")
    print("4. Signal_Summary - One row per signal variable")
    print("5. Pattern_Analysis - Statistics by transformation type")

    print("=" * 60)


# %%
# ------------------------------
# 5. Filter signal columns by missing data threshold
# ------------------------------
# In this step, we remove signal columns (feature engineering outputs) that have too much missing data.
# This is important for downstream machine learning and analysis, as columns with excessive missingness
# can degrade model performance or complicate imputation. We keep all base/metadata columns, and for
# signal columns, we only retain those where the proportion of missing values is below the specified threshold.
# The threshold can be adjusted as needed (default here is 85% missing allowed).
missing_threshold = 0.50  # 80%

print("FILTERING COLUMNS BY MISSING DATA")
print("=" * 50)

# Calculate missing percentages
missing_stats = []
for col in df.columns:
    if col not in base_columns:  # Skip metadata columns
        missing_pct = df[col].isna().mean()
        missing_stats.append({
            'column': col,
            'missing_pct': missing_pct,
            'keep': missing_pct <= missing_threshold
        })

missing_df = pd.DataFrame(missing_stats)

# Summary
total_signal_cols = len(missing_df)
high_missing = (missing_df['missing_pct'] > missing_threshold).sum()
keep_cols = (missing_df['missing_pct'] <= missing_threshold).sum()

print(f"Total signal columns: {total_signal_cols}")
print(f"High missing (>{missing_threshold*100}%): {high_missing}")
print(f"Keeping: {keep_cols}")
print(f"Dropping: {high_missing}")

# Get columns to keep
columns_to_keep = base_columns + missing_df[missing_df['keep']]['column'].tolist()

print(f"\nFinal dataset: {len(columns_to_keep)} columns")
print(f"Reduction: {len(df.columns)} → {len(columns_to_keep)} ({len(df.columns)-len(columns_to_keep)} dropped)")

# Create filtered dataset
df_ml = df[columns_to_keep].copy()

print(f"ML dataset shape: {df_ml.shape}")


# %%
# ------------------------------
# 5. Apply Better Scaling (Preserves Magnitude)
# ------------------------------
print("APPLYING BETTER SCALING (PRESERVES MAGNITUDE)")
print("=" * 60)

# Configuration - Easy to change
MAX_YEAR = 2010  # ✅ Easy to modify here
# Prepare data for scaling
df_for_scaling = df_ml.copy()
df_for_scaling['form_year'] = pd.to_datetime(df_for_scaling['form_date']).dt.year
# Filter to desired year range
df_for_scaling = df_for_scaling[df_for_scaling['form_year'] <= MAX_YEAR].copy()


def better_scaling(df, feature_cols):
    """Cross-sectional scaling that preserves magnitude and handles NaN"""
    scaled_df = df.copy()
    
    print(f"Scaling {len(feature_cols)} features across {scaled_df['form_year'].nunique()} years...")
    
    for year in sorted(scaled_df['form_year'].unique()):
        year_mask = scaled_df['form_year'] == year
        year_data = scaled_df.loc[year_mask, feature_cols]
        
        if len(year_data) == 0:
            continue
            
        print(f"  Processing year {year}: {len(year_data)} observations")
        
        # Process each column individually to handle NaN properly
        for col in feature_cols:
            col_data = year_data[col]
            
            # Skip if all NaN
            if col_data.notna().sum() == 0:
                continue
                
            # 1. Winsorize outliers (only on non-NaN values)
            non_nan_data = col_data.dropna()
            if len(non_nan_data) < 10:  # Need minimum observations
                scaled_df.loc[year_mask, col] = 0
                continue
                
            lower_bound = non_nan_data.quantile(0.01)
            upper_bound = non_nan_data.quantile(0.99)
            winsorized = col_data.clip(lower_bound, upper_bound)
            
            # 2. Cross-sectional z-score (only on non-NaN)
            mean_val = winsorized.mean()
            std_val = winsorized.std()
            
            if std_val > 1e-8:  # Avoid division by zero
                z_scored = (winsorized - mean_val) / std_val
            else:
                z_scored = winsorized * 0  # All same value -> set to 0
                
            # 3. Clip for neural networks
            final_scaled = z_scored.clip(-3, 3)
            
            # 4. Handle NaN: Set to 0 (neutral ranking like R code)
            final_scaled = final_scaled.fillna(0)
            
            # Update the main dataframe
            scaled_df.loc[year_mask, col] = final_scaled
    
    return scaled_df

# Apply better scaling
columns_to_keep = [col for col in df_ml.columns if df_ml[col].isna().mean() <= 0.50]
signal_columns_filtered = [col for col in columns_to_keep if col not in base_columns]

# Make sure form_year is NOT in feature_cols  
if 'form_year' in signal_columns_filtered:
    signal_columns_filtered.remove('form_year')
    print("Removed form_year from feature columns")

# Make sure expected_return is NOT in feature_cols
if 'expected_return' in signal_columns_filtered:
    signal_columns_filtered.remove('expected_return')
    print("Removed expected_return from feature columns")
    
print(f"Features to scale: {len(signal_columns_filtered)}")
print(f"Base columns (not scaled): {len(base_columns)}")

df_scaled = better_scaling(df_for_scaling, signal_columns_filtered)

print(f"Scaling complete. Dataset shape: {df_scaled.shape}")
print(f"Years covered: {df_scaled['form_year'].min()} to {df_scaled['form_year'].max()}")


# %%
print(df_scaled.head())
df_scaled_copy = df_scaled.copy()
# %%
# ------------------------------
# 6. Prepare for ML: Train/Test Split
# ------------------------------
print("PREPARING TRAIN/TEST SPLIT")
print("=" * 60)

# First, ensure expected_return is calculated
if 'expected_return' not in df_scaled.columns:
    print("Calculating expected_return = ret - rf...")
    df_scaled['expected_return'] = df_scaled['ret'] - df_scaled['rf']
    print("Expected return added to dataset")
else:
    print("Expected return already exists in dataset")

# Check expected return statistics
print(f"\nExpected return statistics:")
print(f"  Mean: {df_scaled['expected_return'].mean():.4f}")
print(f"  Std: {df_scaled['expected_return'].std():.4f}")
print(f"  Min: {df_scaled['expected_return'].min():.4f}")
print(f"  Max: {df_scaled['expected_return'].max():.4f}")
print(f"  Missing: {df_scaled['expected_return'].isna().sum()} ({df_scaled['expected_return'].isna().mean()*100:.1f}%)")

# Time-based split (no look-ahead bias)
test_years = 3  # Use last 3 years for testing
max_year = df_scaled['form_year'].max()
test_start_year = max_year - test_years + 1

train_data = df_scaled[df_scaled['form_year'] < test_start_year].copy()
test_data = df_scaled[df_scaled['form_year'] >= test_start_year].copy()

print(f"\nTrain data: {len(train_data)} obs, years {train_data['form_year'].min()}-{train_data['form_year'].max()}")
print(f"Test data: {len(test_data)} obs, years {test_data['form_year'].min()}-{test_data['form_year'].max()}")

# Prepare features and target
feature_columns = signal_columns_filtered
target_column = 'expected_return'

print(f"\nFeatures: {len(feature_columns)}")
print(f"Target: {target_column}")

# Remove any remaining NaNs in target variable
train_clean = train_data.dropna(subset=[target_column])
test_clean = test_data.dropna(subset=[target_column])

print(f"\nAfter removing NaNs in target:")
print(f"  Train: {len(train_clean)} obs")
print(f"  Test: {len(test_clean)} obs")

# Show target distribution by split
print(f"\nTarget distribution:")
print(f"  Train expected_return - Mean: {train_clean['expected_return'].mean():.4f}, Std: {train_clean['expected_return'].std():.4f}")
print(f"  Test expected_return - Mean: {test_clean['expected_return'].mean():.4f}, Std: {test_clean['expected_return'].std():.4f}")

print("=" * 60)


# %%
# ------------------------------
# 7. Fast Feature Selection - Univariate Correlation
# ------------------------------
print("FAST FEATURE SELECTION - UNIVARIATE CORRELATION")
print("=" * 60)

from scipy.stats import pearsonr
import warnings
warnings.filterwarnings('ignore')

print(f"Starting with {len(feature_columns)} features")
print(f"Training data: {len(train_clean)} observations")

# Calculate correlation with target for each feature
print("\n1. Calculating univariate correlations...")
feature_correlations = []
target = train_clean['expected_return']

# Progress tracking
total_features = len(feature_columns)
for i, col in enumerate(feature_columns):
    if i % 2000 == 0:
        print(f"  Progress: {i:,}/{total_features:,} ({i/total_features*100:.1f}%)")
    
    try:
        # Calculate correlation between this feature and expected returns
        corr, p_value = pearsonr(train_clean[col], target)
        
        # Store results
        feature_correlations.append({
            'feature': col,
            'correlation': corr,
            'abs_correlation': abs(corr),  # Use absolute for ranking
            'p_value': p_value
        })
    except:
        # Skip features that cause errors (e.g., all NaN, constant values)
        continue

print(f"  Completed correlation calculation for {len(feature_correlations)} features")

# Convert to DataFrame and sort by absolute correlation
print("\n2. Ranking features by correlation strength...")
corr_df = pd.DataFrame(feature_correlations)
corr_df = corr_df.sort_values('abs_correlation', ascending=False)

# Select top N features
top_n = 1000  # Adjust this number as needed
final_features = corr_df.head(top_n)['feature'].tolist()

print(f"\n3. Feature selection results:")
print(f"   Selected top {len(final_features)} features by correlation")
print(f"   Strongest correlation: {corr_df.iloc[0]['abs_correlation']:.4f} ({corr_df.iloc[0]['feature']})")
print(f"   {top_n}th correlation: {corr_df.iloc[top_n-1]['abs_correlation']:.4f}")
print(f"   Weakest correlation: {corr_df.iloc[-1]['abs_correlation']:.4f}")

# Show top 10 features
print(f"\nTop 10 most predictive features:")
for i in range(10):
    feat = corr_df.iloc[i]
    print(f"  {i+1:2d}. {feat['feature']:<30} (corr: {feat['correlation']:+.4f})")

# Final summary
print(f"\nFinal feature set: {len(final_features)} features")
print(f"Reduction: {len(feature_columns):,} → {len(final_features):,} ({(1-len(final_features)/len(feature_columns))*100:.1f}% reduction)")

# Save feature rankings for future reference
corr_df.to_csv('feature_correlations_ranking.csv', index=False)
print(f"Feature rankings saved to: feature_correlations_ranking.csv")

print("=" * 60)

# %%
# ------------------------------
# 8. Update train/test datasets with selected features
# ------------------------------
print("UPDATING DATASETS WITH SELECTED FEATURES")
print("=" * 50)

# Create final training and test sets with selected features
X_train = train_clean[final_features].copy()
y_train = train_clean['expected_return'].copy()
X_test = test_clean[final_features].copy()
y_test = test_clean['expected_return'].copy()

# Keep metadata for later analysis
train_meta = train_clean[['permno', 'form_year', 'form_date', 'crsp_mktcap_6']].copy()
test_meta = test_clean[['permno', 'form_year', 'form_date', 'crsp_mktcap_6']].copy()

print(f"Training set: {X_train.shape}")
print(f"Test set: {X_test.shape}")
print(f"Target variable: {y_train.name}")

# Check for any remaining missing values
print(f"\nData quality check:")
print(f"  X_train missing values: {X_train.isna().sum().sum()}")
print(f"  X_test missing values: {X_test.isna().sum().sum()}")
print(f"  y_train missing values: {y_train.isna().sum()}")
print(f"  y_test missing values: {y_test.isna().sum()}")

print("=" * 50)
print("READY FOR MACHINE LEARNING!")
print("=" * 50)




# %%
# ------------------------------
# 9. Fix Overfitting - Regularized XGBoost
# ------------------------------
print("STEP 9: FIXING OVERFITTING - REGULARIZED XGBOOST")
print("=" * 60)

# 9a. Reduce features to top 100 most predictive
print("9a. Reducing to top 100 features...")
top_100_features = corr_df.head(100)['feature'].tolist()

X_train_reduced = X_train[top_100_features].copy()
X_test_reduced = X_test[top_100_features].copy()

print(f"Reduced features: {len(top_100_features)}")


import xgboost as xgb
import time
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
# 9b. Train regularized XGBoost with conservative settings
print("\n9b. Training regularized XGBoost...")
start_time = time.time()

xgb_regularized = xgb.XGBRegressor(
    n_estimators=100,            # Conservative number of trees for 100 features
    max_depth=4,                # Shallow trees to prevent overfitting
    learning_rate=0.01,         # Conservative learning rate
    subsample=0.6,              # Use 60% of samples per tree
    colsample_bytree=0.6,       # Use 60% of features per tree (60 out of 100)
    reg_alpha=1.0,              # Moderate L1 regularization
    reg_lambda=1.0,             # Moderate L2 regularization
    min_child_weight=10,        # Conservative samples per leaf
    random_state=42,
    n_jobs=-1,
    verbosity=1
)

# Fit the regularized model
xgb_regularized.fit(X_train_reduced, y_train)
train_time = time.time() - start_time

print(f"Training completed in {train_time:.1f} seconds")

# %%
# 9c. Make predictions with regularized model
print("\n9c. Making predictions...")
train_pred_regularized = xgb_regularized.predict(X_train_reduced)
test_pred_regularized = xgb_regularized.predict(X_test_reduced)

# %%
# 9d. Evaluate regularized model performance
print("\n9d. Regularized Model Performance:")
print("=" * 50)

train_r2_reg = r2_score(y_train, train_pred_regularized)
test_r2_reg = r2_score(y_test, test_pred_regularized)
train_mse_reg = mean_squared_error(y_train, train_pred_regularized)
test_mse_reg = mean_squared_error(y_test, test_pred_regularized)

print(f"Training R²:     {train_r2_reg:.4f}")
print(f"Test R²:         {test_r2_reg:.4f}")
print(f"Training MSE:    {train_mse_reg:.6f}")
print(f"Test MSE:        {test_mse_reg:.6f}")

# %%
# 9e. Check for overfitting and model validation
print(f"\n9e. Overfitting check:")
overfitting_reg = train_r2_reg - test_r2_reg
print(f"Train R² - Test R² = {overfitting_reg:.4f}")

if overfitting_reg > 0.1:
    print("⚠️  Significant overfitting detected")
elif overfitting_reg > 0.05:
    print("⚠️  Moderate overfitting")
else:
    print("✅ Good generalization - ready for portfolio construction")

print("=" * 60)
print("REGULARIZED MODEL TRAINING COMPLETE!")
print("=" * 60)




# %%
# ------------------------------
# 10. Portfolio Construction - Long-Short Strategy
# ------------------------------
print("STEP 10: PORTFOLIO CONSTRUCTION - LONG-SHORT STRATEGY")
print("=" * 60)

# 10a. Prepare portfolio data using test predictions
print("10a. Preparing portfolio data...")

# Create portfolio dataset using test period only (2008-2010)
portfolio_data = pd.DataFrame({
    'permno': test_meta['permno'].values,
    'form_year': test_meta['form_year'].values,
    'form_date': test_meta['form_date'].values,
    'mktcap': test_meta['crsp_mktcap_6'].values,
    'actual_return': y_test.values,
    'predicted_return': test_pred_regularized
})

print(f"Portfolio data shape: {portfolio_data.shape}")
print(f"Test period: {portfolio_data['form_year'].min()}-{portfolio_data['form_year'].max()}")
print(f"Average stocks per year: {len(portfolio_data) / portfolio_data['form_year'].nunique():.0f}")

# %%
# 10b. Create long-short portfolios (Top 100 Long, Bottom 100 Short)
print("\n10b. Creating long-short portfolios (100 long, 100 short)...")

portfolio_results = []

for year in sorted(portfolio_data['form_year'].unique()):
    year_data = portfolio_data[portfolio_data['form_year'] == year].copy()
    
    if len(year_data) < 200:  # Need at least 200 stocks for 100 long + 100 short
        print(f"  Skipping {year}: only {len(year_data)} stocks")
        continue
    
    print(f"  Processing {year}: {len(year_data)} stocks")
    
    # Sort by predicted returns (highest to lowest)
    year_data = year_data.sort_values('predicted_return', ascending=False)
    
    # Get top 100 long and bottom 100 short
    TOP_N_STOCKS = 100
    BOTTOM_N_STOCKS = 100
    
    long_portfolio = year_data.head(TOP_N_STOCKS).copy()
    short_portfolio = year_data.tail(BOTTOM_N_STOCKS).copy()
    
    # Calculate portfolio returns
    long_stats = {
        'portfolio_type': 'long',
        'actual_return_mean': long_portfolio['actual_return'].mean(),
        'actual_return_std': long_portfolio['actual_return'].std(),
        'n_stocks': len(long_portfolio),
        'total_mktcap': long_portfolio['mktcap'].sum(),
        'predicted_return_mean': long_portfolio['predicted_return'].mean(),
        'year': year
    }
    
    short_stats = {
        'portfolio_type': 'short',
        'actual_return_mean': short_portfolio['actual_return'].mean(),
        'actual_return_std': short_portfolio['actual_return'].std(),
        'n_stocks': len(short_portfolio),
        'total_mktcap': short_portfolio['mktcap'].sum(),
        'predicted_return_mean': short_portfolio['predicted_return'].mean(),
        'year': year
    }
    
    portfolio_results.append(pd.DataFrame([long_stats, short_stats]))

# Combine all years
portfolio_df = pd.concat(portfolio_results, ignore_index=True)
print(f"Portfolio analysis complete: {len(portfolio_df)} portfolio-year observations")

# %%
# 10c. Calculate long-short strategy performance
print("\n10c. Long-Short Strategy Performance:")
print("=" * 50)

# Average returns by portfolio type across all test years
avg_returns_by_type = portfolio_df.groupby('portfolio_type')['actual_return_mean'].mean()

print("Average Annual Returns by Portfolio Type:")
for portfolio_type in ['long', 'short']:
    ret = avg_returns_by_type[portfolio_type]
    print(f"  {portfolio_type.upper()} (Top/Bottom 100): {ret:+.4f} ({ret*100:+.2f}%)")

# Long-short strategy results
long_return = avg_returns_by_type['long']
short_return = avg_returns_by_type['short']
long_short_spread = long_return - short_return

print(f"\nLong-Short Strategy:")
print(f"  Long (Top 100):     {long_return:+.4f} ({long_return*100:+.2f}%)")
print(f"  Short (Bottom 100): {short_return:+.4f} ({short_return*100:+.2f}%)")
print(f"  Long-Short Spread:  {long_short_spread:+.4f} ({long_short_spread*100:+.2f}%)")

# %%
# 10d. Calculate risk-adjusted performance metrics
print(f"\n10d. Risk-Adjusted Performance:")
print("=" * 40)

# Calculate annual long-short returns for Sharpe ratio
annual_ls_returns = []
for year in sorted(portfolio_df['year'].unique()):
    year_data = portfolio_df[portfolio_df['year'] == year]
    if len(year_data) >= 2:  # Ensure we have both long and short
        long_ret = year_data[year_data['portfolio_type'] == 'long']['actual_return_mean'].iloc[0]
        short_ret = year_data[year_data['portfolio_type'] == 'short']['actual_return_mean'].iloc[0]
        annual_ls_returns.append(long_ret - short_ret)

if len(annual_ls_returns) > 1:
    ls_mean = np.mean(annual_ls_returns)
    ls_std = np.std(annual_ls_returns)
    ls_sharpe = ls_mean / ls_std if ls_std > 0 else 0
    
    print(f"Strategy Statistics (Annual):")
    print(f"  Mean Return:     {ls_mean:+.4f} ({ls_mean*100:+.2f}%)")
    print(f"  Volatility:      {ls_std:.4f} ({ls_std*100:.2f}%)")
    print(f"  Sharpe Ratio:    {ls_sharpe:.2f}")
    print(f"  Years analyzed:  {len(annual_ls_returns)}")
    
    # Performance assessment
    if ls_sharpe > 1.0:
        print("  📊 Excellent risk-adjusted returns!")
    elif ls_sharpe > 0.5:
        print("  📈 Good risk-adjusted returns")
    elif ls_sharpe > 0.0:
        print("  📉 Positive but weak risk-adjusted returns")
    else:
        print("  ❌ Negative risk-adjusted returns")

# %%
# 10e. Model validation - Check prediction accuracy
print(f"\n10e. Model Validation:")
print("=" * 30)

# Check if higher predicted returns actually lead to higher actual returns
correlation_check = portfolio_df.groupby('portfolio_type')['actual_return_mean'].mean()
long_better_than_short = correlation_check['long'] > correlation_check['short']

print(f"Long outperforms Short: {'✅ Yes' if long_better_than_short else '❌ No'}")
print(f"Long portfolio performance: {correlation_check['long']*100:+.2f}%")
print(f"Short portfolio performance: {correlation_check['short']*100:+.2f}%")

print("=" * 60)
print("PORTFOLIO CONSTRUCTION COMPLETE!")
print("=" * 60)

# %%
# ------------------------------
# 11. Show Actual Stocks - Long and Short Positions by Year
# ------------------------------
print("STEP 11: ACTUAL STOCKS IN LONG/SHORT POSITIONS")
print("=" * 60)

# Get ticker information for the test period stocks
test_stocks_with_tickers = test_clean[['permno', 'form_year']].merge(
    df[['permno', 'ticker']].drop_duplicates(), 
    on='permno', 
    how='left'
)

# Merge with portfolio data to get predictions and actual returns
detailed_portfolio = portfolio_data.merge(
    test_stocks_with_tickers[['permno', 'ticker']], 
    on='permno', 
    how='left'
)

print(f"Portfolio data with tickers: {detailed_portfolio.shape}")
print(f"Stocks with ticker info: {detailed_portfolio['ticker'].notna().sum()}/{len(detailed_portfolio)}")

# Show long and short positions for each year
for year in sorted(detailed_portfolio['form_year'].unique()):
    year_data = detailed_portfolio[detailed_portfolio['form_year'] == year].copy()
    
    if len(year_data) < 100:  # Skip years with too few stocks
        continue
    
    print(f"\n" + "="*50)
    print(f"YEAR {year} - LONG/SHORT POSITIONS")
    print(f"="*50)
    
    # Sort by predicted returns
    year_data = year_data.sort_values('predicted_return', ascending=False)
    
    # Get realistic portfolio sizes - 100 long, 100 short (matching Step 10)
    TOP_N_STOCKS = 100  # Long top 100 stocks
    BOTTOM_N_STOCKS = 100  # Short bottom 100 stocks
    
    # Get long positions (top 100 stocks by predicted return)
    long_positions = year_data.head(TOP_N_STOCKS).copy()
    
    # Get short positions (bottom 100 stocks by predicted return)
    short_positions = year_data.tail(BOTTOM_N_STOCKS).copy()
    short_positions = short_positions.sort_values('predicted_return', ascending=True)
    
    print(f"\n🟢 LONG POSITIONS (Top {len(long_positions)} stocks):")
    print("-" * 70)
    print("Rank | PERMNO  | Ticker | Pred.Ret | Act.Ret | Mkt Cap ($M)")
    print("-" * 70)
    
    for i, (_, stock) in enumerate(long_positions.head(10).iterrows(), 1):  # Show top 10
        ticker = stock['ticker'] if pd.notna(stock['ticker']) else 'N/A'
        mktcap_m = stock['mktcap'] / 1000 if pd.notna(stock['mktcap']) else 0
        print(f"{i:4d} | {stock['permno']:7.0f} | {ticker:6s} | "
              f"{stock['predicted_return']:+7.4f} | {stock['actual_return']:+7.4f} | "
              f"{mktcap_m:8.0f}")
    
    if len(long_positions) > 10:
        print(f"     ... and {len(long_positions)-10} more long positions")
    
    print(f"\n🔴 SHORT POSITIONS (Bottom {len(short_positions)} stocks):")
    print("-" * 70)
    print("Rank | PERMNO  | Ticker | Pred.Ret | Act.Ret | Mkt Cap ($M)")
    print("-" * 70)
    
    for i, (_, stock) in enumerate(short_positions.head(10).iterrows(), 1):  # Show bottom 10
        ticker = stock['ticker'] if pd.notna(stock['ticker']) else 'N/A'
        mktcap_m = stock['mktcap'] / 1000 if pd.notna(stock['mktcap']) else 0
        print(f"{i:4d} | {stock['permno']:7.0f} | {ticker:6s} | "
              f"{stock['predicted_return']:+7.4f} | {stock['actual_return']:+7.4f} | "
              f"{mktcap_m:8.0f}")
    
    if len(short_positions) > 10:
        print(f"     ... and {len(short_positions)-10} more short positions")
    
    # Calculate portfolio performance for this year
    long_avg_return = long_positions['actual_return'].mean()
    short_avg_return = short_positions['actual_return'].mean()
    spread = long_avg_return - short_avg_return
    
    print(f"\n📊 YEAR {year} PERFORMANCE:")
    print(f"  Long portfolio avg return:   {long_avg_return:+.4f} ({long_avg_return*100:+.2f}%)")
    print(f"  Short portfolio avg return:  {short_avg_return:+.4f} ({short_avg_return*100:+.2f}%)")
    print(f"  Long-Short spread:          {spread:+.4f} ({spread*100:+.2f}%)")
    
    # Best and worst performers
    best_long = long_positions.loc[long_positions['actual_return'].idxmax()]
    worst_long = long_positions.loc[long_positions['actual_return'].idxmin()]
    best_short = short_positions.loc[short_positions['actual_return'].idxmax()]
    worst_short = short_positions.loc[short_positions['actual_return'].idxmin()]
    
    print(f"\n🏆 BEST/WORST PERFORMERS:")
    print(f"  Best long:  {best_long['ticker'] if pd.notna(best_long['ticker']) else 'N/A'} "
          f"(PERMNO {best_long['permno']:.0f}) - {best_long['actual_return']:+.4f}")
    print(f"  Worst long: {worst_long['ticker'] if pd.notna(worst_long['ticker']) else 'N/A'} "
          f"(PERMNO {worst_long['permno']:.0f}) - {worst_long['actual_return']:+.4f}")
    print(f"  Best short: {best_short['ticker'] if pd.notna(best_short['ticker']) else 'N/A'} "
          f"(PERMNO {best_short['permno']:.0f}) - {best_short['actual_return']:+.4f}")
    print(f"  Worst short: {worst_short['ticker'] if pd.notna(worst_short['ticker']) else 'N/A'} "
          f"(PERMNO {worst_short['permno']:.0f}) - {worst_short['actual_return']:+.4f}")

# Summary across all years
print(f"\n" + "="*60)
print("SUMMARY - TOP STOCKS ACROSS ALL YEARS")
print("="*60)

# Find stocks that appear frequently in long positions
all_long_stocks = []
all_short_stocks = []

for year in sorted(detailed_portfolio['form_year'].unique()):
    year_data = detailed_portfolio[detailed_portfolio['form_year'] == year].copy()
    if len(year_data) < 100:
        continue
    
    year_data = year_data.sort_values('predicted_return', ascending=False)
    year_data['decile'] = pd.qcut(year_data['predicted_return'], 
                                  q=10, labels=False, duplicates='drop') + 1
    
    long_stocks = year_data[year_data['decile'] == 10][['permno', 'ticker', 'actual_return', 'form_year']]
    short_stocks = year_data[year_data['decile'] == 1][['permno', 'ticker', 'actual_return', 'form_year']]
    
    all_long_stocks.append(long_stocks)
    all_short_stocks.append(short_stocks)

# Combine all years
all_long_df = pd.concat(all_long_stocks, ignore_index=True)
all_short_df = pd.concat(all_short_stocks, ignore_index=True)

# Find most frequent long positions
long_frequency = all_long_df['permno'].value_counts()
print(f"\n🟢 STOCKS MOST OFTEN IN LONG POSITIONS:")
print("PERMNO  | Ticker | Times Long | Avg Return")
print("-" * 45)

for permno, count in long_frequency.head(10).items():
    stock_data = all_long_df[all_long_df['permno'] == permno]
    ticker = stock_data['ticker'].iloc[0] if pd.notna(stock_data['ticker'].iloc[0]) else 'N/A'
    avg_return = stock_data['actual_return'].mean()
    print(f"{permno:7.0f} | {ticker:6s} | {count:10d} | {avg_return:+9.4f}")

# Find most frequent short positions  
short_frequency = all_short_df['permno'].value_counts()
print(f"\n🔴 STOCKS MOST OFTEN IN SHORT POSITIONS:")
print("PERMNO  | Ticker | Times Short | Avg Return")
print("-" * 46)

for permno, count in short_frequency.head(10).items():
    stock_data = all_short_df[all_short_df['permno'] == permno]
    ticker = stock_data['ticker'].iloc[0] if pd.notna(stock_data['ticker'].iloc[0]) else 'N/A'
    avg_return = stock_data['actual_return'].mean()
    print(f"{permno:7.0f} | {ticker:6s} | {count:11d} | {avg_return:+9.4f}")

print("=" * 60)
print("STOCK-LEVEL ANALYSIS COMPLETE!")
print("=" * 60)

# %%
# Calculate performance by year
yearly_performance = []

for year in sorted(portfolio_df['year'].unique()):
    year_data = portfolio_df[portfolio_df['year'] == year]
    
    if len(year_data) >= 2:  # Ensure we have both long and short
        # Get portfolio performance for this year
        portfolio_returns = year_data.set_index('portfolio_type')['actual_return_mean']
        
        long_return = portfolio_returns.loc['long']    # Long portfolio
        short_return = portfolio_returns.loc['short']  # Short portfolio
        spread = long_return - short_return
        
        # Market context
        market_condition = {
            2008: "Crisis (-37% S&P)",
            2009: "Recovery (+26% S&P)", 
            2010: "Growth (+15% S&P)"
        }.get(year, "Normal")
        
        yearly_performance.append({
            'year': year,
            'long_return': long_return,
            'short_return': short_return,
            'spread': spread,
            'market_condition': market_condition,
            'n_stocks': len(portfolio_data[portfolio_data['form_year'] == year])
        })

# Display results
print("Year-by-Year Strategy Performance:")
print("-" * 60)
print("Year | Market Context      | Long   | Short  | Spread | Stocks")
print("-" * 60)

for perf in yearly_performance:
    print(f"{perf['year']} | {perf['market_condition']:<18} | "
          f"{perf['long_return']:+.3f} | {perf['short_return']:+.3f} | "
          f"{perf['spread']:+.3f} | {perf['n_stocks']:,}")

# Summary statistics
spreads = [p['spread'] for p in yearly_performance]
print(f"\nStrategy Consistency:")
print(f"  Best year spread:    {max(spreads):+.4f} ({max(spreads)*100:+.2f}%)")
print(f"  Worst year spread:   {min(spreads):+.4f} ({min(spreads)*100:+.2f}%)")
print(f"  Average spread:      {np.mean(spreads):+.4f} ({np.mean(spreads)*100:+.2f}%)")
print(f"  Spread volatility:   {np.std(spreads):.4f} ({np.std(spreads)*100:.2f}%)")
print(f"  Positive years:      {sum(1 for s in spreads if s > 0)}/3")

# Performance assessment
if all(s > 0 for s in spreads):
    print("  📈 Strategy profitable in ALL years!")
elif sum(1 for s in spreads if s > 0) >= 2:
    print("  📊 Strategy profitable in majority of years")
else:
    print("  📉 Strategy struggled with consistency")

print("=" * 60)

