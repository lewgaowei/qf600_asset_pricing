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
"""
===========================================
GPU-ACCELERATED SYMBOLIC REGRESSION WITH PYSR
===========================================

INSTALLATION INSTRUCTIONS:
--------------------------

1. INSTALL JULIA (Required for PySR):
   Download and install Julia from: https://julialang.org/downloads/
   - Windows: Use the installer (julia-1.10.0-win64.exe)
   - Add Julia to PATH during installation
   - Verify: Open terminal and type `julia --version`

2. INSTALL PYSR:
   pip install pysr

3. CONFIGURE PYSR WITH JULIA:
   python -c "import pysr; pysr.install()"

   This will install the required Julia packages (SymbolicRegression.jl)

4. GPU SUPPORT (CUDA 13):
   PySR can leverage GPU for certain operations.
   Ensure you have:
   - CUDA 13.x installed
   - Compatible GPU drivers
   - Julia GPU packages (will be installed automatically when using GPU)

   To enable GPU in Julia (optional, for maximum performance):
   - Open Julia REPL: julia
   - Run: using Pkg; Pkg.add("CUDA")
   - Run: using CUDA; CUDA.functional()  # Should return true

   The PySR settings in this script enable:
   - turbo=True: SIMD vectorization + GPU optimizations
   - batching=True: Batch evaluations for GPU
   - multithreading=True: Julia parallel threads
   - procs=0: All CPU cores

USAGE NOTES:
------------
- PySR uses Julia backend (much faster than gplearn)
- GPU acceleration works best with large populations (>5000)
- PySR automatically discovers mathematical formulas
- Supports parallel processing across CPU cores + GPU acceleration
- Better at finding interpretable equations than gplearn

DIFFERENCES FROM GPLEARN:
-------------------------
- PySR: Julia-based, GPU support, faster, better equations
- gplearn: Python-only, CPU-only, slower, limited to basic operations
"""

# Core libraries
import numpy as np
import pandas as pd
import warnings
from pathlib import Path
import pickle
import os

# Machine Learning libraries - SYMBOLIC REGRESSION WITH GPU
from pysr import PySRRegressor
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.model_selection import ParameterGrid

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# %%
# ------------------------------
# GPU CONFIGURATION CHECK
# ------------------------------
print("=" * 60)
print("GPU CONFIGURATION CHECK")
print("=" * 60)

try:
    import pysr
    print("✅ PySR is installed")
    print(f"   Version: {pysr.__version__}")
except ImportError:
    print("❌ PySR not installed!")
    print("   Run: pip install pysr")
    print("   Then: python -c \"import pysr; pysr.install()\"")

print()
print("CUDA 13 GPU Acceleration:")
print("  - turbo=True: SIMD + GPU optimizations")
print("  - batching=True: GPU batch evaluations")
print("  - multithreading=True: Julia parallel threads")
print("  - procs=0: All CPU cores")

print()
print("Quick Setup Commands:")
print("  1. pip install pysr")
print("  2. python -c \"import pysr; pysr.install()\"")
print("  3. (Optional) Julia GPU: julia -e 'using Pkg; Pkg.add(\"CUDA\")'")

print("=" * 60)
print()

# %%
# ------------------------------
# STEP 1: Setup imports and load data from 02-feature-engineering.py
# ------------------------------
print("=" * 60)
print("STEP 1: LOADING DATA FROM 02-FEATURE-ENGINEERING.PY")
print("=" * 60)

# ------------------------------
# 1.1 Set up file paths
# ------------------------------
_base_dir = Path.cwd()
START_YEAR = 2000
# Input data path
data_path = _base_dir / f"signals_with_returns_and_tickers_{START_YEAR}.parquet"

# Create output directories for results
output_dir = _base_dir / "ml_gp_symbolic_results"
print(_base_dir)
print(output_dir)

cv_dir = output_dir / "CV"  # Cross-validation results
pred_dir = output_dir / "Pred"  # Predictions

# Create directories if they don't exist
output_dir.mkdir(exist_ok=True)
cv_dir.mkdir(exist_ok=True)
pred_dir.mkdir(exist_ok=True)

print(f"Data path: {data_path}")
print(f"Output directory: {output_dir}")
print(f"CV directory: {cv_dir}")
print(f"Prediction directory: {pred_dir}")

# ------------------------------
# 1.2 Load data
# ------------------------------
print(f"\nLoading data from: {data_path}")
df = pd.read_parquet(data_path, engine="fastparquet")

print(f"Data loaded successfully!")
print(f"Dataset shape: {df.shape}")
print(f"Columns: {len(df.columns)}")
print(f"Rows: {len(df):,}")

# ------------------------------
# 1.3 Ensure date columns are datetime
# ------------------------------
for c in ("datadate", "form_date"):
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

print(f"\nDate range: {df['form_date'].min()} to {df['form_date'].max()}")

# ------------------------------
# 1.4 Calculate expected_return if not exists
# ------------------------------
if 'expected_return' not in df.columns:
    print("\nCalculating expected_return = ret - rf...")
    df['expected_return'] = df['ret'] - df['rf']
else:
    print("\nExpected return already exists")

print(f"Expected return - Mean: {df['expected_return'].mean():.4f}, Std: {df['expected_return'].std():.4f}")

# ------------------------------
# 1.5 Identify base columns vs feature columns
# ------------------------------
print("\nIdentifying base columns and feature columns...")

# Base/metadata columns (not used as features)
base_columns = [
    'gvkey', 'datadate', 'fyear', 'year', 'permno', 'form_date', 'form_year',
    'crsp_mktcap_6', 'ret', 'rf', 'nmonth', 'i', 'mindex',
    'mindex_form', 'ticker', 'expected_return', 'counter'
]

# Filter to only base columns that exist in df
base_columns = [col for col in base_columns if col in df.columns]

# All other columns are potential features
all_columns = df.columns.tolist()
feature_columns = [col for col in all_columns if col not in base_columns]

print(f"Base/metadata columns: {len(base_columns)}")
print(f"Feature columns: {len(feature_columns)}")

# Show sample of base columns
print(f"\nBase columns: {base_columns}")

# Show first 10 feature columns
print(f"\nFirst 10 feature columns: {feature_columns[:10]}")

print("=" * 60)
print("STEP 1 COMPLETE: Data loaded successfully!")
print("=" * 60)


# %%
# ------------------------------
# STEP 2: Create time-based counter variable for cross-validation
# ------------------------------
print("\n" + "=" * 60)
print("STEP 2: CREATING TIME-BASED COUNTER VARIABLE")
print("=" * 60)

# ------------------------------
# 2.1 Create form_year column if it doesn't exist
# ------------------------------
if 'form_year' not in df.columns:
    print("Creating form_year column from form_date...")
    df['form_year'] = pd.to_datetime(df['form_date']).dt.year
else:
    print("form_year column already exists")

# ------------------------------
# 2.2 Create counter variable (sequential time period index)
# ------------------------------
print("\nCreating counter variable for time-series cross-validation...")

# Get unique years sorted
unique_years = sorted(df['form_year'].unique())
print(f"Data spans {len(unique_years)} years: {unique_years[0]} to {unique_years[-1]}")

# Create a mapping: year -> counter (starting from 1)
year_to_counter = {year: idx + 1 for idx, year in enumerate(unique_years)}

# Apply the mapping to create counter column
df['counter'] = df['form_year'].map(year_to_counter)

print(f"\nCounter variable created!")
print(f"Counter range: {df['counter'].min()} to {df['counter'].max()}")

# ------------------------------
# 2.3 Show the year-counter mapping
# ------------------------------
print("\nYear-to-Counter Mapping:")
print("-" * 30)
print("Year  | Counter")
print("-" * 30)

for year in unique_years[:10]:  # Show first 10 years
    counter = year_to_counter[year]
    print(f"{year} |    {counter}")

if len(unique_years) > 10:
    print(f"...   |   ...")
    for year in unique_years[-3:]:  # Show last 3 years
        counter = year_to_counter[year]
        print(f"{year} |   {counter}")

# ------------------------------
# 2.4 Verify counter distribution
# ------------------------------
print("\n" + "-" * 30)
print("Counter distribution (observations per period):")
counter_counts = df['counter'].value_counts().sort_index()

for counter in counter_counts.index[:5]:
    count = counter_counts[counter]
    year = [y for y, c in year_to_counter.items() if c == counter][0]
    print(f"  Counter {counter:2d} (Year {year}): {count:,} observations")

if len(counter_counts) > 5:
    print(f"  ...")
    for counter in counter_counts.index[-3:]:
        count = counter_counts[counter]
        year = [y for y, c in year_to_counter.items() if c == counter][0]
        print(f"  Counter {counter:2d} (Year {year}): {count:,} observations")

print("\n" + "=" * 60)
print("STEP 2 COMPLETE: Counter variable created!")
print("=" * 60)


# %%
# ------------------------------
# STEP 3: Configuration - ML Pipeline Parameters
# ------------------------------
print("\n" + "=" * 60)
print("STEP 3: CONFIGURATION PARAMETERS")
print("=" * 60)

# ------------------------------
# CONFIGURATION DICTIONARY
# ------------------------------
"""
PYSR GPU OPTIMIZATION TIPS:
---------------------------
1. For CUDA 13 GPU acceleration, ensure Julia CUDA.jl is installed
2. Increase populations × population_size for better GPU utilization
3. Use batching=True and adjust batch_size based on GPU memory
4. turbo=True enables SIMD vectorization (works with/without GPU)
5. multithreading=True uses Julia parallel threads on CPU
6. procs=0 uses all CPU cores (works alongside GPU)

RECOMMENDED SETTINGS FOR GPU:
- populations: 20-40 (more parallel populations)
- population_size: 50-100 (larger populations)
- batch_size: 50-100 (depends on GPU memory)
- turbo: True (always)
- batching: True (for GPU)
- multithreading: True (for CPU cores)
"""

CONFIG = {
    # Window type for time-series cross-validation
    'window': 'recursive',          # 'recursive' (expanding) or 'rolling' (sliding)
    
    # Training and validation periods
    'cv_train': 3,                  # Years for training (only used in 'rolling' mode)
    'cv_validation': 1,             # Years for validation (used in both modes)
    
    # Model settings
    'method': 'gp_symbolic',        # 'gp_symbolic' = Genetic Programming Symbolic Regression
    'dep_var': 'expected_return',   # Target variable to predict
    
    # Time periods for testing
    'begin': None,                  # Start counter (will be set automatically)
    'end': None,                    # End counter (will be set automatically)
    
    # Feature selection
    'missing_threshold': 0.50,      # Drop features with > 50% missing data
    'use_top_features': 100,        # REDUCED: GP works best with 50-150 features
                                    # 1000 features was too many → poor performance
    
    # Multicollinearity removal
    'remove_multicollinearity': True,  # Remove highly correlated features
    'correlation_threshold': 0.85       # Correlation threshold
}

# ------------------------------
# Set begin/end automatically based on data
# ------------------------------
min_counter = df['counter'].min()
max_counter = df['counter'].max()
MIN_TRAIN_YEARS = 3  # Minimum 3 years of training data

if CONFIG['window'] == 'recursive':
    CONFIG['begin'] = min_counter + MIN_TRAIN_YEARS + CONFIG['cv_validation']
elif CONFIG['window'] == 'rolling':
    CONFIG['begin'] = min_counter + CONFIG['cv_train'] + CONFIG['cv_validation']

CONFIG['end'] = max_counter

print(f"\n⚙️ Minimum training requirement: {MIN_TRAIN_YEARS} years (for recursive)")
print(f"   First test will be at counter {CONFIG['begin']}")

# ------------------------------
# Display configuration
# ------------------------------
print("\n📋 ML PIPELINE CONFIGURATION:")
print("-" * 50)
print(f"Window Type:          {CONFIG['window'].upper()}")
print(f"Method:               {CONFIG['method'].upper()} (Genetic Programming)")
print(f"Target Variable:      {CONFIG['dep_var']}")
print()
print("Time Periods:")
print(f"  Validation window:  {CONFIG['cv_validation']} year(s)")
if CONFIG['window'] == 'rolling':
    print(f"  Training window:    {CONFIG['cv_train']} years (fixed)")
else:
    print(f"  Training window:    Expanding (from start)")
print()
print("Test Period Range:")
print(f"  Start counter:      {CONFIG['begin']} (Year {[y for y, c in year_to_counter.items() if c == CONFIG['begin']][0]})")
print(f"  End counter:        {CONFIG['end']} (Year {[y for y, c in year_to_counter.items() if c == CONFIG['end']][0]})")
print(f"  Total test periods: {CONFIG['end'] - CONFIG['begin'] + 1}")
print()
print("Feature Settings:")
print(f"  Top features:       {CONFIG['use_top_features'] if CONFIG['use_top_features'] is not None else 'ALL'}")
print(f"  Missing threshold:  {CONFIG['missing_threshold']*100}%")
print()
print("⚠️  GP RECOMMENDATION: Use 50-150 features for best performance")
print("   Too many features → exponential search space → slow/poor results")

print("\n" + "=" * 60)
print("STEP 3 COMPLETE: Configuration set!")
print("=" * 60)


# %%
# ------------------------------
# STEP 4: Define Helper Functions for Train/Validation/Test Splits
# ------------------------------
print("\n" + "=" * 60)
print("STEP 4: DEFINING HELPER FUNCTIONS")
print("=" * 60)

# ------------------------------
# 4.1 Function: Get train and validation data
# ------------------------------
def train_validation_data(data, k, config, feature_cols):
    """
    Split data into training and validation sets based on counter k.
    """
    dep_var = config['dep_var']

    # Get training data based on window type
    if config['window'] == 'rolling':
        train_range = ((data['counter'] <= (k - config['cv_validation'])) &
                      (data['counter'] >= (k - config['cv_validation'] - config['cv_train'] + 1)))
    elif config['window'] == 'recursive':
        train_range = (data['counter'] <= (k - config['cv_validation']))

    X_train = data.loc[train_range, feature_cols]
    y_train = data.loc[train_range, dep_var]

    # Remove NaN in target variable
    valid_idx = y_train.notna()
    X_train = X_train[valid_idx]
    y_train = y_train[valid_idx]

    # Get validation data
    validation_range = ((data['counter'] >= (k - config['cv_validation'] + 1)) &
                       (data['counter'] <= k))
    X_validation = data.loc[validation_range, feature_cols]
    y_validation = data.loc[validation_range, dep_var]

    # Remove NaN in target variable
    valid_idx = y_validation.notna()
    X_validation = X_validation[valid_idx]
    y_validation = y_validation[valid_idx]

    return X_train, y_train, X_validation, y_validation


# ------------------------------
# 4.2 Function: Get train and test data
# ------------------------------
def train_test_data(data, k, config, feature_cols):
    """
    Split data into training and test sets based on counter k.
    """
    dep_var = config['dep_var']

    # Get training data
    if config['window'] == 'rolling':
        train_range = ((data['counter'] <= k) &
                      (data['counter'] >= (k - config['cv_validation'] - config['cv_train'] + 1)))
    elif config['window'] == 'recursive':
        train_range = (data['counter'] <= k)

    X_train = data.loc[train_range, feature_cols]
    y_train = data.loc[train_range, dep_var]

    # Remove NaN in target variable
    valid_idx = y_train.notna()
    X_train = X_train[valid_idx]
    y_train = y_train[valid_idx]

    # Get test data
    test_range = (data['counter'] == (k + 1))
    X_test = data.loc[test_range, feature_cols]

    # Get metadata for test period
    key_cols = ['permno', 'form_date', 'form_year', 'ticker', 'crsp_mktcap_6', dep_var]
    key_cols_available = [col for col in key_cols if col in data.columns]
    key_test = data.loc[test_range, key_cols_available].reset_index(drop=True)

    return X_train, y_train, X_test, key_test


# ------------------------------
# 4.3 Function: Output filename generator
# ------------------------------
def output_filename(config, mode='pred', counter=None):
    """
    Generate standardized filename for saving results.
    """
    if mode == 'cv':
        folder = cv_dir
    elif mode == 'pred':
        folder = pred_dir
    else:
        folder = output_dir

    # Build filename
    filename_parts = [
        config['method'],
        config['window'],
        f"dep_{config['dep_var']}",
        f"val_{config['cv_validation']}"
    ]

    if config['window'] == 'rolling':
        filename_parts.append(f"train_{config['cv_train']}")

    if counter is not None:
        filename_parts.append(f"counter_{counter}")

    if mode == 'cv':
        filename_parts.append('cv')
    elif mode == 'pred':
        filename_parts.append('pred')

    filename = '_'.join(filename_parts) + '.csv'

    return folder / filename


def remove_multicollinear_features(data, feature_cols, dep_var, threshold=0.85):
    """
    Remove highly correlated features - OPTIMIZED VERSION.
    """
    import time

    print(f"\n🔍 Removing multicollinear features (threshold: {threshold})...")
    print("-" * 60)

    start_time = time.time()

    # Prepare feature data
    feature_data = data[feature_cols].copy()

    # Step 1: Calculate feature-feature correlation
    print("⏱️ Step 1/3: Calculating feature-feature correlation matrix...")
    step_start = time.time()

    try:
        import cudf
        print("   🚀 GPU detected! Using cuDF...")
        feature_data_gpu = cudf.from_pandas(feature_data)
        corr_matrix = feature_data_gpu.corr().to_pandas()
    except:
        print("   💻 Using CPU (pandas)")
        corr_matrix = feature_data.corr()

    step_time = time.time() - step_start
    print(f"   ✅ Complete in {step_time:.2f}s")

    # Step 2: Calculate feature-target correlations (VECTORIZED)
    print("⏱️ Step 2/3: Calculating feature-target correlations...")
    step_start = time.time()

    target_correlations = feature_data.corrwith(data[dep_var]).abs().fillna(0).to_dict()

    step_time = time.time() - step_start
    print(f"   ✅ Complete in {step_time:.2f}s")

    # Step 3: Find highly correlated pairs
    print("⏱️ Step 3/3: Finding highly correlated pairs...")
    step_start = time.time()

    corr_array = corr_matrix.values
    corr_abs = np.abs(corr_array)
    corr_upper = np.triu(corr_abs, k=1)
    high_corr_indices = np.where(corr_upper > threshold)

    print(f"   Found {len(high_corr_indices[0]):,} pairs with correlation > {threshold}")

    features_to_remove = set()
    high_corr_pairs = []

    for idx in range(len(high_corr_indices[0])):
        i = high_corr_indices[0][idx]
        j = high_corr_indices[1][idx]

        feat1 = feature_cols[i]
        feat2 = feature_cols[j]

        if feat1 in features_to_remove or feat2 in features_to_remove:
            continue

        corr_val = corr_abs[i, j]

        # Keep feature with higher target correlation
        if target_correlations[feat1] >= target_correlations[feat2]:
            features_to_remove.add(feat2)
            removed_feat = feat2
            kept_feat = feat1
        else:
            features_to_remove.add(feat1)
            removed_feat = feat1
            kept_feat = feat2

        high_corr_pairs.append({
            'feature_1': feat1,
            'feature_2': feat2,
            'correlation': corr_val,
            'kept': kept_feat,
            'removed': removed_feat
        })

    step_time = time.time() - step_start
    print(f"   ✅ Complete in {step_time:.2f}s")

    filtered_features = [f for f in feature_cols if f not in features_to_remove]

    total_time = time.time() - start_time

    print(f"\n✅ Multicollinearity removal complete!")
    print(f"   Total time: {total_time:.2f}s")
    print(f"   Features before: {len(feature_cols):,}")
    print(f"   Features after:  {len(filtered_features):,}")
    print(f"   Features removed: {len(features_to_remove):,} ({len(features_to_remove)/len(feature_cols)*100:.1f}%)")

    return filtered_features, list(features_to_remove)


print("\n✅ Helper functions defined!")
print("\n" + "=" * 60)
print("STEP 4 COMPLETE: Helper functions ready!")
print("=" * 60)


# %%
# ------------------------------
# STEP 5: Filter and Scale Features
# ------------------------------
print("\n" + "=" * 60)
print("STEP 5: DATA PREPARATION & FEATURE SCALING")
print("=" * 60)

# ------------------------------
# 5.1 Filter features by missing data
# ------------------------------
print("\n📊 FILTERING FEATURES BY MISSING DATA...")
print("-" * 60)

missing_stats = []
for col in feature_columns:
    missing_pct = df[col].isna().mean()
    missing_stats.append({
        'column': col,
        'missing_pct': missing_pct,
        'keep': missing_pct <= CONFIG['missing_threshold']
    })

missing_df = pd.DataFrame(missing_stats)

#Summary
total_features = len(missing_df)
high_missing = (missing_df['missing_pct'] > CONFIG['missing_threshold']).sum()
kept_features = (missing_df['missing_pct'] <= CONFIG['missing_threshold']).sum()

print(f"Total feature columns: {total_features:,}")
print(f"Features with >{CONFIG['missing_threshold']*100}% missing: {high_missing:,}")
print(f"Features to keep: {kept_features:,}")

features_to_keep = missing_df[missing_df['keep']]['column'].tolist()

# ------------------------------
# 5.2 Apply cross-sectional scaling
# ------------------------------
print(f"\n⚖️ APPLYING CROSS-SECTIONAL SCALING...")
print("-" * 60)

scaled_data_file = _base_dir / f"df_scaled_{START_YEAR}.parquet"

if scaled_data_file.exists():
    print("\n✅ Found saved scaled data! Loading...")
    df_scaled = pd.read_parquet(scaled_data_file, engine="fastparquet")
    print(f"   Loaded {len(df_scaled):,} observations")
    print("\n⭐ Skipping scaling (using cached data)")
else:
    print("  1. Rank values cross-sectionally within each year")
    print("Scaling method: Rank-Range Method [-1, +1]")
    df_scaled = df.copy()
    unique_years = sorted(df_scaled['form_year'].unique())
    print(f"\nScaling {len(features_to_keep):,} features across {len(unique_years)} years...")

    for year in unique_years:
        year_mask = df_scaled['form_year'] == year
        year_data_size = year_mask.sum()
        print(f"  Processing year {year}: {year_data_size:,} observations", end='')

        for col in features_to_keep:
            col_data = df_scaled.loc[year_mask, col]
            ranks = col_data.rank(method='average', na_option='keep')
            valid_ranks = ranks.dropna()

            if len(valid_ranks) == 0:
                df_scaled.loc[year_mask, col] = 0
                continue

            min_rank = valid_ranks.min()
            max_rank = valid_ranks.max()

            if max_rank > min_rank:
                scaled = 2 * (ranks - min_rank) / (max_rank - min_rank) - 1
            else:
                scaled = ranks * 0

            scaled = scaled.fillna(0)
            df_scaled.loc[year_mask, col] = scaled

        print(" ✓")

    print(f"\n✅ Scaling complete!")
    print(f"   Method: Rank-based scaling to [-1, +1] range")
    print(f"   Missing values: Filled with 0")
    print(f"\n💾 Saving scaled data to: {scaled_data_file.name}")
    df_scaled.to_parquet(scaled_data_file, engine="fastparquet", compression="snappy")
    print("✅ Saved! Next time this step will be skipped.")

# ------------------------------
# 5.3 Create final feature list
# ------------------------------
print(f"\n📋 FINAL FEATURE SET:")
print("-" * 60)

final_feature_columns = features_to_keep

print(f"Total features for ML: {len(final_feature_columns):,}")

if CONFIG['dep_var'] not in df_scaled.columns:
    print(f"\n⚠️ WARNING: Target variable '{CONFIG['dep_var']}' not found!")
else:
    print(f"\n✅ Target variable '{CONFIG['dep_var']}' ready")

print("\n" + "=" * 60)
print("STEP 5 COMPLETE: Features filtered and scaled!")
print("=" * 60)


# %%
# ------------------------------
# STEP 6: Feature Selection by Correlation
# ------------------------------
feature_selection_required = True
if feature_selection_required:
    print("\n" + "=" * 60)
    print("STEP 6: FEATURE SELECTION (CRITICAL FOR GP)")
    print("=" * 60)

    if CONFIG['use_top_features'] is not None:
        print(f"\n📊 Selecting top {CONFIG['use_top_features']} features...")
        print("⚠️  GP performs best with 50-150 features")
        print("-" * 60)

        print("Calculating correlations...")
        feature_correlations = []

        for col in final_feature_columns:
            mask = df_scaled[col].notna() & df_scaled[CONFIG['dep_var']].notna()

            if mask.sum() < 100:
                continue

            corr = df_scaled.loc[mask, col].corr(df_scaled.loc[mask, CONFIG['dep_var']])

            if not pd.isna(corr):
                feature_correlations.append({
                    'feature': col,
                    'correlation': corr,
                    'abs_correlation': abs(corr)
                })

        corr_df = pd.DataFrame(feature_correlations).sort_values('abs_correlation', ascending=False)
        top_features = corr_df.head(CONFIG['use_top_features'])
        final_feature_columns = top_features['feature'].tolist()

        print(f"\n✅ Feature selection complete!")
        print(f"   Before: {len(features_to_keep):,} features")
        print(f"   After:  {len(final_feature_columns):,} features")

        print(f"\n📋 Top 10 features:")
        print("-" * 60)
        for rank, (idx, row) in enumerate(top_features.head(10).iterrows(), 1):
            print(f"  {rank:2d} | {row['feature'][:30]:32s} | {row['correlation']:+.4f}")

    else:
        print(f"\n⚠️  WARNING: Using ALL {len(final_feature_columns):,} features")
        print("   This may cause GP to run VERY slowly!")
        print("   Recommended: Set CONFIG['use_top_features'] = 100")

    print("\n" + "=" * 60)
    print("STEP 6 COMPLETE!")
    print("=" * 60)
    

# %%
# ------------------------------
# STEP 7: Optional Multicollinearity Removal
# ------------------------------
if CONFIG['remove_multicollinearity']:
    print("\n" + "=" * 60)
    print("STEP 7: MULTICOLLINEARITY REMOVAL")
    print("=" * 60)

    cache_filename = f"multicoll_filtered_features_n{len(final_feature_columns)}_thresh{CONFIG['correlation_threshold']}_{START_YEAR}.parquet"
    cache_file = _base_dir / cache_filename

    if cache_file.exists():
        print(f"\n✅ Found cached results!")
        cached_df = pd.read_parquet(cache_file, engine="fastparquet")
        features_before_multicoll = final_feature_columns.copy()
        final_feature_columns = cached_df['feature'].tolist()
        removed_features = [f for f in features_before_multicoll if f not in final_feature_columns]
        print(f"   Loaded {len(final_feature_columns):,} features")
    else:
        features_before_multicoll = final_feature_columns.copy()
        filtered_features, removed_features = remove_multicollinear_features(
            data=df_scaled,
            feature_cols=final_feature_columns,
            dep_var=CONFIG['dep_var'],
            threshold=CONFIG['correlation_threshold']
        )
        final_feature_columns = filtered_features

        print(f"\n💾 Caching results...")
        pd.DataFrame({'feature': final_feature_columns}).to_parquet(
            cache_file, engine="fastparquet", compression="snappy"
        )

    print(f"\n✅ Final feature count: {len(final_feature_columns):,}")
    print("=" * 60)

else:
    print("\n" + "=" * 60)
    print("STEP 7: MULTICOLLINEARITY REMOVAL SKIPPED")
    print("=" * 60)
    

# %%
# ------------------------------
# STEP 8: Implement Hyperparameter Grid for Genetic Programming
# ------------------------------
print("\n" + "=" * 60)
print("STEP 8: HYPERPARAMETER GRID FOR GENETIC PROGRAMMING")
print("=" * 60)

def get_hyperparameter_grid(method):
    """
    Define hyperparameter grid for PySR Symbolic Regression.

    PySR PARAMETERS:
    ----------------
    - niterations: Number of iterations (like generations)
    - populations: Number of populations (like population_size)
    - population_size: Size of each population
    - ncycles_per_iteration: Cycles per iteration
    - binary_operators: ["+", "-", "*", "/"]
    - unary_operators: ["sin", "cos", "exp", "log"]
    - complexity_of_operators: Complexity penalties for each operator
    - parsimony: Penalty for equation complexity (like parsimony_coefficient)
    - maxsize: Maximum complexity of equation
    - procs: Number of CPU processes (0 = all cores)
    - multithreading: Enable Julia multithreading
    """
    if method == 'gp_symbolic':
        # NOTE: PySR is primarily CPU-based. GPU helps with batch evaluation only.
        # Expect CPU at 100%, GPU at 5-30% (this is normal!)

        grid = {
            # Core evolution settings
            'niterations': [20, 30],                 # GRID SEARCH: Test more iterations for better solutions
            'populations': [40],                     # INCREASED: More populations = more GPU work
            'population_size': [100],                # INCREASED: Larger batches = better GPU use
                                                     # Total: 40 × 100 = 4000 programs (vs 1000)

            # Complexity controls
            'maxsize': [15, 20, 30],                 # GRID SEARCH: Test different complexity limits
            'parsimony': [0.01, 0.1],                # FIXED: Test baseline vs. high (removed 0.001 to reduce combos)

            # Operators
            'binary_operators': [["+", "-", "*", "/"]],  # Basic operations
            'unary_operators': [["abs"], ["abs", "square"], ["abs", "square", "sqrt", "log"]],  # FIXED: Add financial operators

            # Performance settings
            'procs': [0],                            # 0 = use all CPU cores
            'multithreading': [True],                # Enable Julia multithreading

            # GPU settings (CUDA 13) - OPTIMIZED FOR BETTER GPU USAGE
            'turbo': [True],                         # Enable SIMD optimizations
            'batching': [True],                      # Batch evaluations for GPU
            'batch_size': [200],                     # INCREASED: Larger batches use more GPU memory

            # Selection and mutation
            'ncycles_per_iteration': [550],          # Cycles per iteration
            'tournament_selection_n': [10],          # Tournament size
            'tournament_selection_p': [0.86],        # Tournament probability

            # Optimization
            'optimizer_algorithm': ["BFGS"],         # Local optimization algorithm
            'optimizer_nrestarts': [2],              # Restarts for optimization

            # Other settings
            'random_state': [42]                     # For reproducibility
        }
    else:
        raise ValueError(f"Unknown method: {method}")

    tunegrid = list(ParameterGrid(grid))
    return tunegrid


hyperparameter_grid = get_hyperparameter_grid(CONFIG['method'])

print(f"\n📊 Hyperparameter Grid for {CONFIG['method'].upper()} (PySR):")
print("-" * 50)
print(f"Total combinations to try: {len(hyperparameter_grid)}")
print()

# Show parameter ranges
print("Parameter ranges:")
niter_values = sorted(set([p['niterations'] for p in hyperparameter_grid]))
npop_values = sorted(set([p['populations'] for p in hyperparameter_grid]))
popsize_values = sorted(set([p['population_size'] for p in hyperparameter_grid]))
maxsize_values = sorted(set([p['maxsize'] for p in hyperparameter_grid]))
parsimony_values = sorted(set([p['parsimony'] for p in hyperparameter_grid]))

print(f"  niterations:             {niter_values}")
print(f"  populations:             {npop_values}")
print(f"  population_size:         {popsize_values}")
print(f"  Total programs:          {npop_values[0] * popsize_values[0]} (populations × population_size)")
print(f"  maxsize:                 {maxsize_values}")
print(f"  parsimony:               {parsimony_values}")

print()
print("First 5 combinations:")
print("-" * 90)
print("  #  | niter | pops | pop_size | total_progs | maxsize | parsimony | GPU")
print("-" * 90)

for i, params in enumerate(hyperparameter_grid[:5], 1):
    total_progs = params['populations'] * params['population_size']
    print(f"  {i:2d} | {params['niterations']:5d} | {params['populations']:4d} | "
          f"{params['population_size']:8d} | {total_progs:11d} | {params['maxsize']:7d} | "
          f"{params['parsimony']:9.4f} | {'Yes' if params['turbo'] else 'No':3s}")

if len(hyperparameter_grid) > 5:
    print(f"  ... and {len(hyperparameter_grid) - 5} more combinations")

# ------------------------------
# Explain PySR hyperparameters
# ------------------------------
print("\n" + "-" * 50)
print("📚 PYSR HYPERPARAMETERS (GPU-ACCELERATED):")
print("-" * 50)
print()
print("1. niterations (Evolution Cycles):")
print("   - Number of iterations of symbolic regression")
print("   - More iterations = Better solutions")
print("   - Typical: 10-40")
print()
print("2. populations × population_size (Total Programs):")
print("   - populations: Number of parallel populations")
print("   - population_size: Size of each population")
print("   - Total = populations × population_size")
print("   - Typical total: 500-2000")
print()
print("3. maxsize (Maximum Equation Complexity):")
print("   - Maximum number of nodes in equation tree")
print("   - Prevents overly complex formulas")
print("   - Typical: 15-30")
print()
print("4. parsimony (Simplicity Penalty):")
print("   - Penalty for equation complexity")
print("   - Higher = Simpler formulas preferred")
print("   - Prevents overfitting")
print()
print("5. GPU Acceleration (CUDA 13):")
print("   - turbo: SIMD vectorization")
print("   - batching: Batch evaluations on GPU")
print("   - multithreading: Julia parallel threads")
print()
print("6. procs (CPU Parallelization):")
print("   - 0 = Use all available CPU cores")
print("   - Works alongside GPU acceleration")

print("\n" + "=" * 60)
print("STEP 8 COMPLETE: Hyperparameter grid ready!")
print("=" * 60)
print(f"\n🚀 PySR WITH GPU ACCELERATION (CUDA 13)")
print("   ✅ Much faster than gplearn (CPU-only)")
print("   ✅ Better at finding interpretable equations")
print("   ✅ GPU + Multi-core CPU parallelization")
print(f"   Features: {len(final_feature_columns)}")
print(f"   Total programs per iteration: {npop_values[0] * popsize_values[0]}")


# %%
# ------------------------------
# STEP 9: Implement Cross-Validation Function for GP
# ------------------------------
print("\n" + "=" * 60)
print("STEP 9: CROSS-VALIDATION FUNCTION FOR GP")
print("=" * 60)

def run_cross_validation(data, k, config, feature_cols):
    """
    Perform grid search cross-validation for GP at time period k.
    """
    print(f"\n{'='*60}")
    print(f"Running GP Cross-Validation for Test Counter {k+1}")
    print(f"{'='*60}")

    # Get hyperparameter grid
    tunegrid = get_hyperparameter_grid(config['method'])
    print(f"Testing {len(tunegrid)} hyperparameter combinations...")

    # Get train and validation data
    print(f"Splitting data...")
    X_train, y_train, X_validation, y_validation = train_validation_data(
        data, k, config, feature_cols
    )

    print(f"  Training set:   {len(X_train):,} observations")
    print(f"  Validation set: {len(X_validation):,} observations")
    print(f"  Features:       {len(feature_cols)}")

    # Initialize results
    cv_results = pd.DataFrame(tunegrid)
    cv_results['r2_score'] = -100.0
    cv_results['mse'] = -100.0
    cv_results['program_length'] = 0
    cv_results['program_depth'] = 0

    print(f"\n🧬 Starting Genetic Programming evolution...")
    print("-" * 60)

    import time
    combo_start_time = time.time()

    for i, params in enumerate(tunegrid):
        iter_start = time.time()

        print(f"\n[{i+1}/{len(tunegrid)}] Testing combination:")
        total_progs = params['populations'] * params['population_size']
        print(f"  Iterations: {params['niterations']}, Total Programs: {total_progs}, "
              f"Maxsize: {params['maxsize']}, Parsimony: {params['parsimony']}")

        try:
            # Create PySR model with GPU support
            model = PySRRegressor(
                # Core settings
                niterations=params['niterations'],
                populations=params['populations'],
                population_size=params['population_size'],

                # Complexity controls
                maxsize=params['maxsize'],
                parsimony=params['parsimony'],

                # Operators
                binary_operators=params['binary_operators'],
                unary_operators=params['unary_operators'],

                # Performance (CPU + GPU)
                procs=params['procs'],                    # CPU parallelization
                multithreading=params['multithreading'],  # Julia threads
                turbo=params['turbo'],                    # SIMD + GPU optimizations
                batching=params['batching'],              # Batch for GPU
                batch_size=params['batch_size'],          # GPU batch size

                # Selection and evolution
                ncycles_per_iteration=params['ncycles_per_iteration'],
                tournament_selection_n=params['tournament_selection_n'],
                tournament_selection_p=params['tournament_selection_p'],

                # Optimization
                optimizer_algorithm=params['optimizer_algorithm'],
                optimizer_nrestarts=params['optimizer_nrestarts'],

                # Other settings
                random_state=params['random_state'],
                verbosity=0,  # Quiet during CV
                progress=False,  # No progress bar during CV
            )

            # Train the model
            print(f"  🧬 Evolving programs (GPU-accelerated)...", end='', flush=True)
            model.fit(X_train.values, y_train.values)  # PySR needs numpy arrays
            print(" ✓")

            # Make predictions
            y_pred = model.predict(X_validation.values)

            # Calculate metrics
            r2 = r2_score(y_validation, y_pred)
            mse = mean_squared_error(y_validation, y_pred)

            # Get program information (PySR stores equations differently)
            best_equation_obj = model.get_best()
            best_equation_str = str(best_equation_obj)
            program_length = len(best_equation_str)
            # Estimate depth by counting operators (approximate)
            program_depth = best_equation_str.count('(') if best_equation_str else 0

            # Store results
            cv_results.loc[i, 'r2_score'] = r2
            cv_results.loc[i, 'mse'] = mse
            cv_results.loc[i, 'program_length'] = program_length
            cv_results.loc[i, 'program_depth'] = program_depth

            iter_duration = time.time() - iter_start
            print(f"  ✅ R²={r2:+.4f}, MSE={mse:.6f}, "
                  f"Length={program_length}, Depth={program_depth}")
            print(f"  📐 Best equation: {best_equation_str[:80]}...")
            print(f"  ⏱️  Time: {iter_duration:.1f}s")

        except Exception as e:
            print(f"  ❌ Error: {str(e)[:100]}")
            iter_duration = time.time() - iter_start
            print(f"  ⏱️  Time: {iter_duration:.1f}s")
            continue

    # Sort by R² score
    cv_results = cv_results.sort_values('r2_score', ascending=False)

    # Display best results
    print("\n" + "-" * 60)
    print("TOP 3 HYPERPARAMETER COMBINATIONS:")
    print("-" * 60)

    for rank, (idx, row) in enumerate(cv_results.head(3).iterrows(), 1):
        print(f"\n{rank}. R²={row['r2_score']:+.4f}, MSE={row['mse']:.6f}")
        total_progs = row['populations'] * row['population_size']
        print(f"   Iterations: {row['niterations']:.0f}, Total Programs: {total_progs:.0f}")
        print(f"   Equation: Length={row['program_length']:.0f}, Depth={row['program_depth']:.0f}")

    # Save results
    output_file = output_filename(config, mode='cv', counter=k+1)
    cv_results.to_csv(output_file, index=False)
    print(f"\n✅ Results saved to: {output_file.name}")

    total_time = time.time() - combo_start_time
    print(f"\n⏱️  Total CV time: {total_time/60:.1f} minutes")

    return cv_results


print("\n✅ Cross-validation function ready!")
print("\n🚀 PySR WITH GPU ACCELERATION:")
print("   ✅ Much faster than gplearn")
print("   ✅ GPU + Multi-core CPU parallelization")
print("   ✅ Better at finding interpretable equations")
print("   ⏱️  Expect 5-30 minutes per test period (faster than gplearn)")

print("\n" + "=" * 60)
print("STEP 9 COMPLETE!")
print("=" * 60)


# %%
# ------------------------------
# STEP 10: Implement Prediction Function for GP
# ------------------------------
print("\n" + "=" * 60)
print("STEP 10: PREDICTION FUNCTION FOR GP")
print("=" * 60)

def run_prediction(data, k, config, feature_cols):
    """
    Train final GP model and make predictions for period k+1.
    """
    print(f"\n{'='*60}")
    print(f"Running GP Prediction for Counter {k+1}")
    print(f"{'='*60}")

    # Load best hyperparameters
    cv_file = output_filename(config, mode='cv', counter=k+1)

    if not cv_file.exists():
        print(f"⚠️ WARNING: CV file not found: {cv_file}")
        return None

    cv_results = pd.read_csv(cv_file)
    cv_results = cv_results.sort_values('r2_score', ascending=False)
    best_params = cv_results.iloc[0]

    total_progs = int(best_params['populations']) * int(best_params['population_size'])
    print(f"✅ Loaded best hyperparameters:")
    print(f"   Iterations: {int(best_params['niterations'])}")
    print(f"   Total Programs: {total_progs}")
    print(f"   R² score: {best_params['r2_score']:+.4f}")

    # Get train and test data
    print(f"\nSplitting data...")
    X_train, y_train, X_test, key_test = train_test_data(
        data, k, config, feature_cols
    )

    print(f"  Training set: {len(X_train):,} observations")
    print(f"  Test set:     {len(X_test):,} observations")

    # Train final model
    print(f"\n🧬 Training final PySR model with GPU acceleration...")

    import time
    train_start = time.time()

    # Parse lists from CSV strings
    binary_ops = eval(best_params['binary_operators']) if isinstance(best_params['binary_operators'], str) else best_params['binary_operators']
    unary_ops = eval(best_params['unary_operators']) if isinstance(best_params['unary_operators'], str) else best_params['unary_operators']

    model = PySRRegressor(
        # Core settings
        niterations=int(best_params['niterations']),
        populations=int(best_params['populations']),
        population_size=int(best_params['population_size']),

        # Complexity controls
        maxsize=int(best_params['maxsize']),
        parsimony=float(best_params['parsimony']),

        # Operators
        binary_operators=binary_ops,
        unary_operators=unary_ops,

        # Performance (CPU + GPU)
        procs=int(best_params['procs']),
        multithreading=bool(best_params['multithreading']),
        turbo=bool(best_params['turbo']),
        batching=bool(best_params['batching']),
        batch_size=int(best_params['batch_size']),

        # Selection and evolution
        ncycles_per_iteration=int(best_params['ncycles_per_iteration']),
        tournament_selection_n=int(best_params['tournament_selection_n']),
        tournament_selection_p=float(best_params['tournament_selection_p']),

        # Optimization
        optimizer_algorithm=best_params['optimizer_algorithm'],
        optimizer_nrestarts=int(best_params['optimizer_nrestarts']),

        # Other settings
        random_state=int(best_params['random_state']),
        verbosity=1,  # Show progress for final training
        progress=True,  # Show progress bar
    )

    model.fit(X_train.values, y_train.values)

    train_time = time.time() - train_start
    print(f"\n✅ Model trained in {train_time/60:.1f} minutes")

    # Print the evolved formula
    print(f"\n📐 EVOLVED FORMULA:")
    print("-" * 60)
    best_equation = model.get_best()
    best_equation_str = str(best_equation)
    print(f"{best_equation_str}")
    print("-" * 60)
    print(f"Equation length: {len(best_equation_str)}")
    print(f"Equation complexity: {best_equation_str.count('(')}")

    # Make predictions
    print(f"\n🔮 Making predictions...")
    predictions = model.predict(X_test.values)

    print(f"  Predictions range: [{predictions.min():+.4f}, {predictions.max():+.4f}]")
    print(f"  Predictions mean:  {predictions.mean():+.4f}")

    # Create result dataframe
    result_df = key_test.copy()
    result_df['predicted_return'] = predictions

    # Save predictions
    output_file = output_filename(config, mode='pred', counter=k+1)
    result_df.to_csv(output_file, index=False)
    print(f"\n✅ Predictions saved to: {output_file.name}")

    # Save the evolved formula separately
    formula_file = output_dir / f"formula_counter_{k+1}.txt"
    with open(formula_file, 'w') as f:
        f.write(f"PySR Evolved Formula for Counter {k+1}\n")
        f.write("=" * 60 + "\n\n")
        f.write(best_equation_str)
        f.write(f"\n\nEquation Stats:\n")
        f.write(f"  Length: {len(best_equation_str)}\n")
        f.write(f"  Complexity: {best_equation_str.count('(')}\n")
        f.write(f"  R² on validation: {best_params['r2_score']:+.4f}\n")
        f.write(f"\n\nFull Equation Hall of Fame:\n")
        f.write("=" * 60 + "\n")
        # PySR keeps multiple equations ranked by complexity/accuracy
        equations_df = model.equations_
        f.write(equations_df.to_string())

    print(f"✅ Formula saved to: {formula_file.name}")

    return result_df


print("\n✅ Prediction function ready!")

print("\n" + "=" * 60)
print("STEP 10 COMPLETE!")
print("=" * 60)


# %%
# ------------------------------
# STEP 11: Run Cross-Validation for All Time Periods
# ------------------------------
print("\n" + "=" * 60)
print("STEP 11: RUNNING CROSS-VALIDATION FOR ALL PERIODS")
print("=" * 60)

RUN_CV = True  # Set to True when ready
USE_PARALLEL = False  # GP doesn't parallelize well - keep False
MAX_WORKERS = 1

# Cross-validation range - run for all periods
start_period = CONFIG['begin']
end_period = CONFIG['end']  # Run for all periods from begin to end

if RUN_CV:
    print(f"\n🔄 Starting GP cross-validation...")
    print(f"Periods: {start_period} to {end_period} ({end_period - start_period + 1} periods)")
    print(f"Hyperparameter combinations per period: {len(hyperparameter_grid)}")
    
    print(f"\n⚠️  ESTIMATED TIME:")
    avg_time_per_combo = 10  # minutes (conservative estimate)
    total_combos = len(hyperparameter_grid) * (end_period - start_period + 1)
    estimated_hours = (total_combos * avg_time_per_combo) / 60
    print(f"   ~{estimated_hours:.1f} hours total")
    print(f"   ({avg_time_per_combo} min/combo × {len(hyperparameter_grid)} combos × {end_period - start_period + 1} periods)")
    print()

    import time
    start_time = time.time()

    # Sequential execution (recommended for GP)
    for k in range(start_period - 1, end_period):
        year = [y for y, c in year_to_counter.items() if c == k+1][0]
        print(f"\n{'='*60}")
        print(f"Processing CV for test counter {k+1} (Year {year:.0f})")
        print(f"{'='*60}")

        try:
            cv_results = run_cross_validation(df_scaled, k, CONFIG, final_feature_columns)
            print(f"✅ CV for test counter {k+1} complete!")

        except Exception as e:
            print(f"❌ Error at test counter {k+1}: {str(e)}")
            print("Continuing to next period...")
            continue

    elapsed_time = time.time() - start_time
    print(f"\n" + "="*60)
    print(f"✅ ALL CROSS-VALIDATION COMPLETE!")
    print(f"Time elapsed: {elapsed_time/3600:.1f} hours")
    print("="*60)

else:
    print("\n⚠️  Cross-validation not run (RUN_CV = False)")
    print("Set RUN_CV = True to execute")
    print("\n⏰ ESTIMATED TIME: Several hours to complete!")

print("\n" + "=" * 60)
print("STEP 11 STATUS: Ready when RUN_CV = True")
print("=" * 60)


# %%
# ------------------------------
# STEP 12: Run Final Predictions
# ------------------------------
print("\n" + "=" * 60)
print("STEP 12: RUNNING FINAL PREDICTIONS")
print("=" * 60)

RUN_PRED = True  # Set to True when ready

if RUN_PRED:
    print(f"\n🔄 Starting GP predictions...")
    
    all_predictions = []
    import time
    start_time = time.time()

    for k in range(CONFIG['begin'] - 1, CONFIG['end']):
        year = [y for y, c in year_to_counter.items() if c == k+1][0]
        print(f"\n{'='*60}")
        print(f"Predicting for counter {k+1} (Year {year:.0f})")
        print(f"{'='*60}")

        try:
            pred_results = run_prediction(df_scaled, k, CONFIG, final_feature_columns)

            if pred_results is not None:
                all_predictions.append(pred_results)
                print(f"✅ Counter {k+1} predictions saved!")
            else:
                print(f"⚠️ Skipping counter {k+1}")

        except Exception as e:
            print(f"❌ Error at counter {k+1}: {str(e)}")
            continue

    # Combine predictions
    if len(all_predictions) > 0:
        final_predictions = pd.concat(all_predictions, ignore_index=True)
        combined_file = output_dir / 'all_predictions.csv'
        final_predictions.to_csv(combined_file, index=False)

        elapsed_time = time.time() - start_time
        print(f"\n" + "="*60)
        print(f"✅ ALL PREDICTIONS COMPLETE!")
        print(f"Total observations: {len(final_predictions):,}")
        print(f"Time elapsed: {elapsed_time/3600:.1f} hours")
        print(f"Results saved to: {combined_file.name}")
        print("="*60)
    else:
        final_predictions = None

else:
    print("\n⚠️  Predictions not run (RUN_PRED = False)")
    final_predictions = None

print("\n" + "=" * 60)
print("STEP 12 STATUS: Ready when RUN_PRED = True")
print("=" * 60)


# %%
# ------------------------------
# STEP 13: Build Portfolios 
# ------------------------------
print("\n" + "=" * 60)
print("STEP 13: BUILDING PORTFOLIOS")
print("=" * 60)

if final_predictions is not None and RUN_PRED:
    print(f"\n📊 Creating long/short portfolios...")

    portfolio_data = final_predictions.copy()
    required_cols = ['form_year', 'predicted_return', CONFIG['dep_var']]
    missing_cols = [col for col in required_cols if col not in portfolio_data.columns]

    if missing_cols:
        print(f"⚠️ Missing columns: {missing_cols}")
        portfolio_df_fixed = None
        portfolio_df_decile = None
    else:
        # VERSION 1: Top 10% Long / Bottom 5% Short
        print(f"\n📈 Version 1: Top 10% Long / Bottom 5% Short (Decile)")
        print("-" * 60)
        portfolio_results_v1 = []

        for year in sorted(portfolio_data['form_year'].unique()):
            year_data = portfolio_data[portfolio_data['form_year'] == year].copy()
            if len(year_data) < 200:
                continue

            year_data = year_data.sort_values('predicted_return', ascending=False)

            n_stocks = len(year_data)
            long_portfolio = year_data.head(n_stocks // 10)   # Top 10%
            short_portfolio = year_data.tail(n_stocks // 20)  # Bottom 5%

            long_return = long_portfolio[CONFIG['dep_var']].mean()
            short_return = -short_portfolio[CONFIG['dep_var']].mean()
            spread = long_return - short_return

            portfolio_results_v1.append({
                'year': year,
                'long_return': long_return,
                'short_return': short_return,
                'spread': spread,
                'n_long': len(long_portfolio),
                'n_short': len(short_portfolio)
            })

            print(f"  Year {year:.0f}: Long {len(long_portfolio)}, Short {len(short_portfolio)}")

        portfolio_df_v1 = pd.DataFrame(portfolio_results_v1)
        portfolio_file_v1 = output_dir / 'portfolio_returns_10pct_short5pct.csv'
        portfolio_df_v1.to_csv(portfolio_file_v1, index=False)
        print(f"✅ Saved to: {portfolio_file_v1.name}")

        # VERSION 2: Top 10% Long / Bottom 100 Short (Fixed)
        print(f"\n📈 Version 2: Top 10% Long / Bottom 100 Short (Fixed)")
        print("-" * 60)
        portfolio_results_v2 = []

        for year in sorted(portfolio_data['form_year'].unique()):
            year_data = portfolio_data[portfolio_data['form_year'] == year].copy()
            if len(year_data) < 200:
                continue

            year_data = year_data.sort_values('predicted_return', ascending=False)

            n_stocks = len(year_data)
            long_portfolio = year_data.head(n_stocks // 10)  # Top 10%
            short_portfolio = year_data.tail(100)            # Bottom 100 tickers

            long_return = long_portfolio[CONFIG['dep_var']].mean()
            short_return = -short_portfolio[CONFIG['dep_var']].mean()
            spread = long_return - short_return

            portfolio_results_v2.append({
                'year': year,
                'long_return': long_return,
                'short_return': short_return,
                'spread': spread,
                'n_long': len(long_portfolio),
                'n_short': len(short_portfolio)
            })

            print(f"  Year {year:.0f}: Long {len(long_portfolio)}, Short {len(short_portfolio)}")

        portfolio_df_v2 = pd.DataFrame(portfolio_results_v2)
        portfolio_file_v2 = output_dir / 'portfolio_returns_10pct_short100.csv'
        portfolio_df_v2.to_csv(portfolio_file_v2, index=False)
        print(f"✅ Saved to: {portfolio_file_v2.name}")

        # Keep both for analysis
        portfolio_df_fixed = portfolio_df_v1
        portfolio_df_decile = portfolio_df_v2

else:
    print("\n⚠️ Portfolios not created")
    portfolio_df = None

print("\n" + "=" * 60)
print("STEP 13 COMPLETE!")
print("=" * 60)


# %%
# ------------------------------
# STEP 14: Evaluate Performance
# ------------------------------
print("\n" + "=" * 60)
print("STEP 10: EVALUATING GP PERFORMANCE")
print("=" * 60)

has_fixed = 'portfolio_df_fixed' in locals() and portfolio_df_fixed is not None
has_decile = 'portfolio_df_decile' in locals() and portfolio_df_decile is not None

if has_fixed or has_decile:
    print(f"\n📈 Calculating performance metrics...")

    # VERSION 1: Top 10% Long / Bottom 5% Short (Decile)
    if has_fixed:
        print("\n" + "="*60)
        print("VERSION 1: Top 10% Long / Bottom 5% Short (Decile)")
        print("="*60)

        avg_long_v1 = portfolio_df_fixed['long_return'].mean()
        avg_short_v1 = portfolio_df_fixed['short_return'].mean()
        avg_spread_v1 = portfolio_df_fixed['spread'].mean()
        spread_std_v1 = portfolio_df_fixed['spread'].std()
        sharpe_v1 = avg_spread_v1 / spread_std_v1 if spread_std_v1 > 0 else 0

        print()
        print("Portfolio Returns:")
        print(f"  Long:   {avg_long_v1:+.4f} ({avg_long_v1*100:+.2f}%)")
        print(f"  Short:  {avg_short_v1:+.4f} ({avg_short_v1*100:+.2f}%)")
        print(f"  Spread: {avg_spread_v1:+.4f} ({avg_spread_v1*100:+.2f}%)")
        print()
        print("Risk-Adjusted:")
        print(f"  Volatility:    {spread_std_v1:.4f}")
        print(f"  Sharpe Ratio:  {sharpe_v1:.2f}")
        print()
        print(f"Analysis Period: {len(portfolio_df_fixed)} years")

    # VERSION 2: Top 10% Long / Bottom 100 Short (Fixed)
    if has_decile:
        print("\n" + "="*60)
        print("VERSION 2: Top 10% Long / Bottom 100 Short (Fixed)")
        print("="*60)

        avg_long_v2 = portfolio_df_decile['long_return'].mean()
        avg_short_v2 = portfolio_df_decile['short_return'].mean()
        avg_spread_v2 = portfolio_df_decile['spread'].mean()
        spread_std_v2 = portfolio_df_decile['spread'].std()
        sharpe_v2 = avg_spread_v2 / spread_std_v2 if spread_std_v2 > 0 else 0

        print()
        print("Portfolio Returns:")
        print(f"  Long:   {avg_long_v2:+.4f} ({avg_long_v2*100:+.2f}%)")
        print(f"  Short:  {avg_short_v2:+.4f} ({avg_short_v2*100:+.2f}%)")
        print(f"  Spread: {avg_spread_v2:+.4f} ({avg_spread_v2*100:+.2f}%)")
        print()
        print("Risk-Adjusted:")
        print(f"  Volatility:    {spread_std_v2:.4f}")
        print(f"  Sharpe Ratio:  {sharpe_v2:.2f}")
        print()
        print(f"Analysis Period: {len(portfolio_df_decile)} years")

    # COMPARISON TABLE
    if has_fixed and has_decile:
        print("\n" + "="*60)
        print("COMPARISON: VERSION 1 vs VERSION 2")
        print("="*60)
        print()
        print("BOTH VERSIONS: Top 10% Long (Same)")
        print(f"{'Metric':<20} | {'V1: Short 5%':>15} | {'V2: Short 100':>15} | {'Difference':>12}")
        print("-" * 70)
        print(f"{'Long Return':<20} | {avg_long_v1*100:>14.2f}% | {avg_long_v2*100:>14.2f}% | {(avg_long_v2-avg_long_v1)*100:>11.2f}%")
        print(f"{'Short Return':<20} | {avg_short_v1*100:>14.2f}% | {avg_short_v2*100:>14.2f}% | {(avg_short_v2-avg_short_v1)*100:>11.2f}%")
        print(f"{'Spread':<20} | {avg_spread_v1*100:>14.2f}% | {avg_spread_v2*100:>14.2f}% | {(avg_spread_v2-avg_spread_v1)*100:>11.2f}%")
        print(f"{'Volatility':<20} | {spread_std_v1:>15.4f} | {spread_std_v2:>15.4f} | {spread_std_v2-spread_std_v1:>12.4f}")
        print(f"{'Sharpe Ratio':<20} | {sharpe_v1:>15.2f} | {sharpe_v2:>15.2f} | {sharpe_v2-sharpe_v1:>12.2f}")

    # Save summary
    if has_decile:
        summary = {
            'method': 'GP_Symbolic_V2',
            'strategy': 'Top_10pct_Long_Bottom_100_Short',
            'avg_spread': avg_spread_v2,
            'sharpe_ratio': sharpe_v2,
            'n_years': len(portfolio_df_decile)
        }
        pd.DataFrame([summary]).to_csv(output_dir / 'gp_performance_summary_v2.csv', index=False)

    if has_fixed:
        summary = {
            'method': 'GP_Symbolic_V1',
            'strategy': 'Top_100_Long_Bottom_100_Short',
            'avg_spread': avg_spread_v1,
            'sharpe_ratio': sharpe_v1,
            'n_years': len(portfolio_df_fixed)
        }
        pd.DataFrame([summary]).to_csv(output_dir / 'gp_performance_summary_v1.csv', index=False)

else:
    print("\n⚠️ No performance metrics available")

print("\n" + "=" * 60)
print("STEP 14 COMPLETE!")
print("=" * 60)

# %%

# %%

# %%

# %%

# %%

# %%

# %%

# %%

# %%
