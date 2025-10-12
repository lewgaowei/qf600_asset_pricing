# ---
# jupyter:
#   jupytext:
#     cell_metadata_filter: -all
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.17.1
# ---

# %%
# ------------------------------
# STEP 1: Setup imports and load data from 02-feature-engineering.py
# ------------------------------

# Core libraries
import numpy as np
import pandas as pd
import warnings
from pathlib import Path
import pickle
import os

# Machine Learning libraries
import lightgbm as lgb
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.model_selection import ParameterGrid

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

print("=" * 60)
print("STEP 1: LOADING DATA FROM 02-FEATURE-ENGINEERING.PY")
print("=" * 60)
# ------------------------------
# 1.1 Set up file paths
# ------------------------------
# _base_dir = Path.cwd()
_base_dir = Path(__file__).resolve().parent

START_YEAR = 2015
# Input data path (from 02-feature-engineering.py)
data_path = _base_dir / f"signals_with_returns_and_tickers_{START_YEAR}.parquet"

# Create output directories for results
output_dir = _base_dir / "ml_results"
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
# STEP 2.5: Configuration - ML Pipeline Parameters
# ------------------------------
print("\n" + "=" * 60)
print("STEP 2.5: CONFIGURATION PARAMETERS")
print("=" * 60)

# ------------------------------
# CONFIGURATION DICTIONARY
# ------------------------------
# This controls the entire ML pipeline behavior
# To switch between recursive/rolling, just change 'window' below!

CONFIG = {
    # Window type for time-series cross-validation
    'window': 'recursive',          # 'recursive' (expanding) or 'rolling' (sliding)
    # 'window': 'rolling',          # 'recursive' (expanding) or 'rolling' (sliding)

    # Training and validation periods
    'cv_train': 7,                  # Years for training (only used in 'rolling' mode)
    'cv_validation': 1,             # Years for validation (used in both modes)

    # Model settings
    'method': 'brt',                # 'brt' = LightGBM boosted regression tree
    'dep_var': 'expected_return',   # Target variable to predict

    # Time periods for testing
    'begin': None,                  # Start counter (will be set automatically)
    'end': None,                    # End counter (will be set automatically)

    # Feature selection
    'max_features': 1000,           # Maximum number of features to use
    'missing_threshold': 0.50,      # Drop features with > 50% missing data
    'use_top_features': 1000        # Use top N features by correlation (None = use ALL)
                                     # Set to 1000 for faster testing, None for full model
}

# ------------------------------
# Set begin/end automatically based on data
# ------------------------------
# Begin: First period where we have enough data for train+validation
min_counter = df['counter'].min()
max_counter = df['counter'].max()

# Set minimum training years required (before first prediction)
MIN_TRAIN_YEARS = 3  # Minimum 3 years of training data

if CONFIG['window'] == 'recursive':
    # Need: min training years + validation period
    # Example: 3 years train + 1 year val = start testing at counter 5
    CONFIG['begin'] = min_counter + MIN_TRAIN_YEARS + CONFIG['cv_validation']

elif CONFIG['window'] == 'rolling':
    # Need: rolling window size + validation period
    # Example: 5 years train + 1 year val = start testing at counter 7
    CONFIG['begin'] = min_counter + CONFIG['cv_train'] + CONFIG['cv_validation']

CONFIG['end'] = max_counter

print(f"\n⚙️  Minimum training requirement: {MIN_TRAIN_YEARS} years (for recursive)")
print(f"   First test will be at counter {CONFIG['begin']}")

# ------------------------------
# Display configuration
# ------------------------------
print("\n📋 ML PIPELINE CONFIGURATION:")
print("-" * 50)
print(f"Window Type:          {CONFIG['window'].upper()}")
print(f"Method:               {CONFIG['method'].upper()} (LightGBM)")
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
print(f"  Max features:       {CONFIG['max_features']}")
print(f"  Missing threshold:  {CONFIG['missing_threshold']*100}%")

# ------------------------------
# Example of how windows work
# ------------------------------
print("\n" + "-" * 50)
print("📚 EXAMPLE: How the window works")
print("-" * 50)

# Show example for first test period
k_example = CONFIG['begin']
year_example = [y for y, c in year_to_counter.items() if c == k_example][0]

print(f"\nFor Test Counter: {k_example} (Year {year_example})")
print()

if CONFIG['window'] == 'recursive':
    # In our implementation, k represents the VALIDATION endpoint
    # For test counter k_example, we need validation endpoint k = k_example - 1
    # Training: [min_counter, k - cv_validation]
    # Validation: [k - cv_validation + 1, k]
    # Test: k + 1
    k_for_cv = k_example - 1  # Validation endpoint for test counter k_example

    train_start = min_counter
    train_end = k_for_cv - CONFIG['cv_validation']
    val_start = k_for_cv - CONFIG['cv_validation'] + 1
    val_end = k_for_cv

    train_start_year = [y for y, c in year_to_counter.items() if c == train_start][0]
    train_end_year = [y for y, c in year_to_counter.items() if c == train_end][0]
    val_start_year = [y for y, c in year_to_counter.items() if c == val_start][0]
    val_end_year = [y for y, c in year_to_counter.items() if c == val_end][0]

    print(f"  TRAIN:    Counters {train_start:2d}-{train_end:2d}  (Years {train_start_year:.0f}-{train_end_year:.0f})")
    print(f"  VALIDATE: Counters {val_start:2d}-{val_end:2d}  (Years {val_start_year:.0f}-{val_end_year:.0f})")
    print(f"  TEST:     Counter  {k_example:2d}       (Year {year_example:.0f})")

elif CONFIG['window'] == 'rolling':
    # Same logic: k represents validation endpoint
    # For test counter k_example, validation endpoint k = k_example - 1
    k_for_cv = k_example - 1  # Validation endpoint for test counter k_example

    train_start = k_for_cv - CONFIG['cv_validation'] - CONFIG['cv_train'] + 1
    train_end = k_for_cv - CONFIG['cv_validation']
    val_start = k_for_cv - CONFIG['cv_validation'] + 1
    val_end = k_for_cv

    train_start_year = [y for y, c in year_to_counter.items() if c == train_start][0]
    train_end_year = [y for y, c in year_to_counter.items() if c == train_end][0]
    val_start_year = [y for y, c in year_to_counter.items() if c == val_start][0]
    val_end_year = [y for y, c in year_to_counter.items() if c == val_end][0]

    print(f"  TRAIN:    Counters {train_start:2d}-{train_end:2d}  (Years {train_start_year:.0f}-{train_end_year:.0f})  [{CONFIG['cv_train']} years]")
    print(f"  VALIDATE: Counters {val_start:2d}-{val_end:2d}  (Years {val_start_year:.0f}-{val_end_year:.0f})")
    print(f"  TEST:     Counter  {k_example:2d}       (Year {year_example:.0f})")

print("\n" + "=" * 60)
print("STEP 2.5 COMPLETE: Configuration set!")
print("=" * 60)
print("\n💡 TIP: To switch to rolling window, change CONFIG['window'] = 'rolling'")
print("=" * 60)

# %%
# ------------------------------
# STEP 3: Define Helper Functions for Train/Validation/Test Splits
# ------------------------------
print("\n" + "=" * 60)
print("STEP 3: DEFINING HELPER FUNCTIONS")
print("=" * 60)

# ------------------------------
# 3.1 Function: Get train and validation data
# ------------------------------
def train_validation_data(data, k, config, feature_cols):
    """
    Split data into training and validation sets based on counter k.

    Parameters:
    -----------
    data : DataFrame
        Full dataset with counter variable
    k : int
        Current counter (test period)
    config : dict
        Configuration dictionary with window settings
    feature_cols : list
        List of feature column names

    Returns:
    --------
    X_train, y_train, X_validation, y_validation : Arrays
        Training and validation features and targets
    """

    dep_var = config['dep_var']

    # Get training data based on window type
    if config['window'] == 'rolling':
        # Rolling: Use fixed window of last N years
        train_range = ((data['counter'] <= (k - config['cv_validation'])) &
                      (data['counter'] >= (k - config['cv_validation'] - config['cv_train'] + 1)))
    elif config['window'] == 'recursive':
        # Recursive: Use all data from start to k-validation
        train_range = (data['counter'] <= (k - config['cv_validation']))

    X_train = data.loc[train_range, feature_cols]
    y_train = data.loc[train_range, dep_var]

    # Remove NaN in target variable (not allowed in training)
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
# 3.2 Function: Get train and test data (for final predictions)
# ------------------------------
def train_test_data(data, k, config, feature_cols):
    """
    Split data into training and test sets based on counter k.
    Used for final prediction after cross-validation.

    Parameters:
    -----------
    data : DataFrame
        Full dataset with counter variable
    k : int
        Current counter (test period)
    config : dict
        Configuration dictionary with window settings
    feature_cols : list
        List of feature column names

    Returns:
    --------
    X_train, y_train, X_test, key_test : Arrays and DataFrame
        Training features/target, test features, and test metadata
    """

    dep_var = config['dep_var']

    # Get training data (including validation period this time)
    if config['window'] == 'rolling':
        # Rolling: Use fixed window up to k
        train_range = ((data['counter'] <= k) &
                      (data['counter'] >= (k - config['cv_validation'] - config['cv_train'] + 1)))
    elif config['window'] == 'recursive':
        # Recursive: Use all data from start to k
        train_range = (data['counter'] <= k)

    X_train = data.loc[train_range, feature_cols]
    y_train = data.loc[train_range, dep_var]

    # Remove NaN in target variable
    valid_idx = y_train.notna()
    X_train = X_train[valid_idx]
    y_train = y_train[valid_idx]

    # Get test data (k+1)
    test_range = (data['counter'] == (k + 1))
    X_test = data.loc[test_range, feature_cols]

    # Get metadata for test period (for saving predictions)
    key_cols = ['permno', 'form_date', 'form_year', 'ticker', 'crsp_mktcap_6', dep_var]
    key_cols_available = [col for col in key_cols if col in data.columns]
    key_test = data.loc[test_range, key_cols_available].reset_index(drop=True)

    return X_train, y_train, X_test, key_test


# ------------------------------
# 3.3 Function: Output filename generator
# ------------------------------
def output_filename(config, mode='pred', counter=None):
    """
    Generate standardized filename for saving results.

    Parameters:
    -----------
    config : dict
        Configuration dictionary
    mode : str
        'cv' for cross-validation, 'pred' for predictions
    counter : int
        Counter value (optional, for counter-specific files)

    Returns:
    --------
    filepath : Path
        Full path for output file
    """

    # Determine folder based on mode
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


# ------------------------------
# 3.4 Test the helper functions
# ------------------------------
print("\n✅ Helper functions defined:")
print("  - train_validation_data(): Splits data for cross-validation")
print("  - train_test_data(): Splits data for final predictions")
print("  - output_filename(): Generates standardized filenames")

# Test with example counter
k_test = CONFIG['begin']
print(f"\n📝 Example output filenames for counter {k_test}:")
print(f"  CV file:   {output_filename(CONFIG, mode='cv', counter=k_test).name}")
print(f"  Pred file: {output_filename(CONFIG, mode='pred', counter=k_test).name}")

print("\n" + "=" * 60)
print("STEP 3 COMPLETE: Helper functions ready!")
print("=" * 60)

# %%
# ------------------------------
# STEP 3.5: Filter and Scale Features (Professor's Approach)
# ------------------------------
print("\n" + "=" * 60)
print("STEP 3.5: DATA PREPARATION & FEATURE SCALING")
print("=" * 60)

# ------------------------------
# 3.5.1 Filter features by missing data threshold
# ------------------------------
print("\n📊 FILTERING FEATURES BY MISSING DATA...")
print("-" * 60)

# Calculate missing percentages for each feature column
missing_stats = []
for col in feature_columns:
    missing_pct = df[col].isna().mean()
    missing_stats.append({
        'column': col,
        'missing_pct': missing_pct,
        'keep': missing_pct <= CONFIG['missing_threshold']
    })

missing_df = pd.DataFrame(missing_stats)

# Summary
total_features = len(missing_df)
high_missing = (missing_df['missing_pct'] > CONFIG['missing_threshold']).sum()
kept_features = (missing_df['missing_pct'] <= CONFIG['missing_threshold']).sum()

print(f"Total feature columns: {total_features:,}")
print(f"Features with >{CONFIG['missing_threshold']*100}% missing: {high_missing:,}")
print(f"Features to keep: {kept_features:,}")
print(f"Reduction: {total_features:,} → {kept_features:,} ({high_missing:,} dropped)")

# Get columns to keep
features_to_keep = missing_df[missing_df['keep']]['column'].tolist()

# ------------------------------
# 3.5.2 Apply cross-sectional scaling (PROFESSOR'S EXACT METHOD)
# ------------------------------
print(f"\n⚖️  APPLYING CROSS-SECTIONAL SCALING (PROFESSOR'S METHOD)...")
print("-" * 60)

# Check if scaled data already exists
scaled_data_file = _base_dir / f"df_scaled_{START_YEAR}.parquet"

if scaled_data_file.exists():
    print("\n✅ Found saved scaled data! Loading...")
    print(f"   Loading from: {scaled_data_file.name}")

    # Load scaled dataframe
    df_scaled = pd.read_parquet(scaled_data_file, engine="fastparquet")

    print(f"   Loaded {len(df_scaled):,} observations")
    print("\n⏭️  Skipping scaling (using cached data)")

else:
    print("Scaling method: Professor's Rank-Range Method")
    print("  1. Rank values cross-sectionally within each year")
    print("  2. Scale ranks to range [-1, +1]")
    print("  3. Fill NaN with 0")
    print()
    print("This matches: reference/02-ML_Prediction/02_build_yz18k.R")

    # Create a copy for scaling
    df_scaled = df.copy()

    # Get unique years
    unique_years = sorted(df_scaled['form_year'].unique())
    print(f"\nScaling {len(features_to_keep):,} features across {len(unique_years)} years...")

    # Scale each year separately (cross-sectional scaling)
    for year in unique_years:
        year_mask = df_scaled['form_year'] == year
        year_data_size = year_mask.sum()

        print(f"  Processing year {year}: {year_data_size:,} observations", end='')

        # Process each feature column
        for col in features_to_keep:
            col_data = df_scaled.loc[year_mask, col]

            # Rank values (using average method for ties, keeps NaN as NaN)
            ranks = col_data.rank(method='average', na_option='keep')

            # Get min and max ranks (excluding NaN)
            valid_ranks = ranks.dropna()

            if len(valid_ranks) == 0:
                # All NaN - fill with 0
                df_scaled.loc[year_mask, col] = 0
                continue

            min_rank = valid_ranks.min()
            max_rank = valid_ranks.max()

            # Scale ranks to [-1, +1]
            if max_rank > min_rank:
                # Formula: 2 * (rank - min) / (max - min) - 1
                # This maps min_rank -> -1, max_rank -> +1
                scaled = 2 * (ranks - min_rank) / (max_rank - min_rank) - 1
            else:
                # All ranks are the same -> set to 0
                scaled = ranks * 0

            # Fill NaN with 0
            scaled = scaled.fillna(0)

            # Update the dataframe
            df_scaled.loc[year_mask, col] = scaled

        print(" ✓")

    print(f"\n✅ Scaling complete!")
    print(f"   Method: Rank-based scaling to [-1, +1] range")
    print(f"   Missing values: Filled with 0")

    # Save scaled data for future use
    print(f"\n💾 Saving scaled data to: {scaled_data_file.name}")
    df_scaled.to_parquet(scaled_data_file, engine="fastparquet", compression="snappy")
    print("✅ Saved! Next time this step will be skipped.")

# ------------------------------
# 3.5.3 Create final feature list
# ------------------------------
print(f"\n📋 FINAL FEATURE SET:")
print("-" * 60)

# Final features = kept features (professor uses all after missing filter)
final_feature_columns = features_to_keep

print(f"Total features for ML: {len(final_feature_columns):,}")
print(f"\nFirst 10 features:")
for i, col in enumerate(final_feature_columns[:10], 1):
    print(f"  {i:2d}. {col}")

if len(final_feature_columns) > 10:
    print(f"  ... and {len(final_feature_columns) - 10:,} more features")

# Verify expected_return column exists
if CONFIG['dep_var'] not in df_scaled.columns:
    print(f"\n⚠️  WARNING: Target variable '{CONFIG['dep_var']}' not found!")
else:
    print(f"\n✅ Target variable '{CONFIG['dep_var']}' ready")

print("\n" + "=" * 60)
print("STEP 3.5 COMPLETE: Features filtered and scaled!")
print("=" * 60)

# %%
# ------------------------------
# STEP 3.6: Optional Feature Selection by Correlation (AFTER Scaling)
# ------------------------------
feature_selection_required = True
if feature_selection_required == True:
    print("\n" + "=" * 60)
    print("STEP 3.6: OPTIONAL FEATURE SELECTION")
    print("=" * 60)

    if CONFIG['use_top_features'] is not None:
        print(f"\n📊 Selecting top {CONFIG['use_top_features']} features by correlation with {CONFIG['dep_var']}...")
        print("-" * 60)

        # Calculate correlation for each feature with the target variable
        print("Calculating correlations (this may take a minute)...")
        feature_correlations = []

        for col in final_feature_columns:
            # Get non-NaN pairs
            mask = df_scaled[col].notna() & df_scaled[CONFIG['dep_var']].notna()

            if mask.sum() < 100:  # Need at least 100 observations
                continue

            # Calculate Pearson correlation
            corr = df_scaled.loc[mask, col].corr(df_scaled.loc[mask, CONFIG['dep_var']])

            if not pd.isna(corr):
                feature_correlations.append({
                    'feature': col,
                    'correlation': corr,
                    'abs_correlation': abs(corr)
                })

        # Sort by absolute correlation (descending)
        corr_df = pd.DataFrame(feature_correlations).sort_values('abs_correlation', ascending=False)

        # Select top N features
        top_features = corr_df.head(CONFIG['use_top_features'])
        final_feature_columns = top_features['feature'].tolist()

        print(f"\n✅ Feature selection complete!")
        print(f"   Before: {len(features_to_keep):,} features")
        print(f"   After:  {len(final_feature_columns):,} features")
        print(f"   Reduction: {len(features_to_keep) - len(final_feature_columns):,} features removed")

        print(f"\n📋 Top 10 features by correlation:")
        print("-" * 60)
        print("Rank | Feature                          | Correlation")
        print("-" * 60)
        for rank, (idx, row) in enumerate(top_features.head(10).iterrows(), 1):
            feature_name = row['feature'][:30]  # Truncate long names
            print(f"  {rank:2d} | {feature_name:32s} | {row['correlation']:+.4f}")

        if len(final_feature_columns) > 10:
            print(f"  ... and {len(final_feature_columns) - 10:,} more features")

        print("\n💡 TIP: Set CONFIG['use_top_features'] = None to use ALL features")

    else:
        print(f"\n💡 Using ALL {len(final_feature_columns):,} features (no correlation selection)")
        print("   This follows professor's approach")
        print("\n💡 TIP: Set CONFIG['use_top_features'] = 1000 for faster testing with top features")

    print("\n" + "=" * 60)
    print("STEP 3.6 COMPLETE: Feature set finalized!")
    print("=" * 60)
    print(f"📊 Final feature count: {len(final_feature_columns):,}")
    print("=" * 60)

# %%
# ------------------------------
# STEP 4: Implement Hyperparameter Grid for LightGBM
# ------------------------------
print("\n" + "=" * 60)
print("STEP 4: HYPERPARAMETER GRID FOR LIGHTGBM")
print("=" * 60)

# ------------------------------
# 4.1 Define hyperparameter grid function
# ------------------------------
def get_hyperparameter_grid(method):
    """
    Define hyperparameter search grid for the specified method.

    Parameters:
    -----------
    method : str
        ML method ('brt' for LightGBM boosted regression tree)

    Returns:
    --------
    tunegrid : list of dicts
        All combinations of hyperparameters to try
    """

    if method == 'brt':
        # Hyperparameters for LightGBM Boosted Regression Tree
        grid = {
            'n_estimators': [20, 50, 100],  # Number of trees (matches professor's range)
            'learning_rate': [0.01, 0.05, 0.1],          # Learning rate (step size)
            # 'learning_rate': [0.01],          # Learning rate (step size)
            'max_depth': [-1]                             # Tree depth (-1 = no limit)
        }
    else:
        raise ValueError(f"Unknown method: {method}")

    # Expand to all combinations using sklearn's ParameterGrid
    tunegrid = list(ParameterGrid(grid))

    return tunegrid


# ------------------------------
# 4.2 Get the grid for our configured method
# ------------------------------
hyperparameter_grid = get_hyperparameter_grid(CONFIG['method'])

print(f"\n📊 Hyperparameter Grid for {CONFIG['method'].upper()}:")
print("-" * 50)
print(f"Total combinations to try: {len(hyperparameter_grid)}")
print()

# Show the parameter space
print("Parameter ranges:")
if CONFIG['method'] == 'brt':
    # Extract unique values for each parameter
    n_estimators_values = sorted(set([p['n_estimators'] for p in hyperparameter_grid]))
    learning_rate_values = sorted(set([p['learning_rate'] for p in hyperparameter_grid]))
    max_depth_values = sorted(set([p['max_depth'] for p in hyperparameter_grid]))

    print(f"  n_estimators (number of trees):  {n_estimators_values}")
    print(f"  learning_rate (step size):       {learning_rate_values}")
    print(f"  max_depth (tree depth):          {max_depth_values}")
    print(f"    Note: max_depth=-1 means no depth limit")

print()
print("First 5 combinations to try:")
print("-" * 50)
print("  #  | n_estimators | learning_rate | max_depth")
print("-" * 50)

for i, params in enumerate(hyperparameter_grid[:5], 1):
    print(f"  {i:2d} | {params['n_estimators']:12d} | {params['learning_rate']:13.2f} | {params['max_depth']:9d}")

if len(hyperparameter_grid) > 5:
    print(f"  ... and {len(hyperparameter_grid) - 5} more combinations")

# ------------------------------
# 4.3 Explain what each hyperparameter does
# ------------------------------
print("\n" + "-" * 50)
print("📚 HYPERPARAMETER EXPLANATIONS:")
print("-" * 50)
print()
print("1. n_estimators (Number of Trees):")
print("   - How many trees to build in the ensemble")
print("   - More trees = More complex model")
print("   - Too many = Overfitting + slow training")
print("   - Too few = Underfitting")
print()
print("2. learning_rate (Step Size):")
print("   - How much each tree contributes to final prediction")
print("   - Lower rate = More conservative, needs more trees")
print("   - Higher rate = Faster learning, but may overshoot")
print("   - Typical range: 0.01 to 0.1")
print()
print("3. max_depth (Tree Depth):")
print("   - Maximum depth of each tree")
print("   - -1 = No limit (LightGBM default)")
print("   - Deeper trees = Capture more interactions")
print("   - We use -1 to let LightGBM control complexity")

print("\n" + "=" * 60)
print("STEP 4 COMPLETE: Hyperparameter grid ready!")
print("=" * 60)

# %%
# ------------------------------
# STEP 5: Implement Cross-Validation Function
# ------------------------------
print("\n" + "=" * 60)
print("STEP 5: CROSS-VALIDATION FUNCTION")
print("=" * 60)

# ------------------------------
# 5.1 Main Cross-Validation Function
# ------------------------------
def run_cross_validation(data, k, config, feature_cols):
    """
    Perform grid search cross-validation for time period k.

    This function:
    1. Splits data into train/validation sets
    2. Tries all hyperparameter combinations
    3. Evaluates each combination on validation set
    4. Saves results to CSV file

    Parameters:
    -----------
    data : DataFrame
        Full dataset with counter variable
    k : int
        Current counter (for determining train/validation split)
    config : dict
        Configuration dictionary
    feature_cols : list
        List of feature column names

    Returns:
    --------
    cv_results : DataFrame
        Results for all hyperparameter combinations with R² and MSE scores
    """

    print(f"\n{'='*60}")
    print(f"Running Cross-Validation for Test Counter {k+1}")
    print(f"  (Validation endpoint: {k}, Test counter: {k+1})")
    print(f"{'='*60}")

    # Get hyperparameter grid
    tunegrid = get_hyperparameter_grid(config['method'])
    print(f"Testing {len(tunegrid)} hyperparameter combinations...")

    # Get train and validation data
    # k is the validation endpoint, so we validate on counter k, test on k+1
    print(f"Splitting data: train up to counter {k-config['cv_validation']}, validate on counters {k-config['cv_validation']+1}-{k}...")
    X_train, y_train, X_validation, y_validation = train_validation_data(
        data, k, config, feature_cols
    )

    print(f"  Training set:   {len(X_train):,} observations")
    print(f"  Validation set: {len(X_validation):,} observations")
    print(f"  Features:       {len(feature_cols)}")

    # Initialize results DataFrame
    cv_results = pd.DataFrame(tunegrid)
    cv_results['r2_score'] = -100.0  # Placeholder
    cv_results['mse'] = -100.0       # Placeholder

    # Loop through all hyperparameter combinations
    print(f"\nTesting hyperparameters...")
    print("-" * 60)

    import time
    combo_start_time = time.time()

    for i, params in enumerate(tunegrid):
        # Start timing this combination
        iter_start = time.time()

        # Train model with current hyperparameters
        if config['method'] == 'brt':
            model = lgb.LGBMRegressor(
                learning_rate=params['learning_rate'],
                max_depth=int(params['max_depth']),
                n_estimators=int(params['n_estimators']),
                objective='regression',
                random_state=42,
                device='cuda',  # 🚀 CUDA acceleration enabled (RTX 5090)
                # force_col_wise=True,  # Removed - conflicts with CUDA
                # n_jobs=-1,  # Removed - ignored by CUDA
                verbose=-1  # Suppress output for cleaner logs
            )

            model.fit(X_train, y_train)

        # Make predictions on validation set
        y_pred = model.predict(X_validation)

        # Calculate performance metrics
        r2 = r2_score(y_validation, y_pred)
        mse = mean_squared_error(y_validation, y_pred)

        # Store results
        cv_results.loc[i, 'r2_score'] = r2
        cv_results.loc[i, 'mse'] = mse

        # Calculate duration for this iteration
        iter_duration = time.time() - iter_start

        # Print progress every 5 combinations
        if (i + 1) % 5 == 0 or (i + 1) == len(tunegrid):
            elapsed = time.time() - combo_start_time
            avg_time = elapsed / (i + 1)
            remaining = avg_time * (len(tunegrid) - (i + 1))

            print(f"  Completed {i+1:2d}/{len(tunegrid)} | "
                  f"n_est={params['n_estimators']:4d}, lr={params['learning_rate']:.2f} | "
                  f"R²={r2:+.4f} | "
                  f"⏱️ {iter_duration:.1f}s (avg: {avg_time:.1f}s, ETA: {remaining:.0f}s)")

    # Sort by R² score (descending - higher is better)
    cv_results = cv_results.sort_values('r2_score', ascending=False)

    # Display best results
    print("\n" + "-" * 60)
    print("TOP 3 HYPERPARAMETER COMBINATIONS:")
    print("-" * 60)
    print("Rank | n_estimators | learning_rate | R²        | MSE")
    print("-" * 60)

    for rank, (idx, row) in enumerate(cv_results.head(3).iterrows(), 1):
        print(f"  {rank}  | {row['n_estimators']:12.0f} | {row['learning_rate']:13.2f} | "
              f"{row['r2_score']:+9.4f} | {row['mse']:.6f}")

    # Save results to CSV
    # k is validation endpoint, save for test counter k+1
    output_file = output_filename(config, mode='cv', counter=k+1)
    cv_results.to_csv(output_file, index=False)
    print(f"\n✅ Results saved to: {output_file.name}")
    print(f"   (These hyperparameters are optimized for testing counter {k+1})")

    return cv_results


# ------------------------------
# 5.2 Explain the cross-validation process
# ------------------------------
print("\n📚 HOW CROSS-VALIDATION WORKS:")
print("-" * 50)
print()
print("For each time period k:")
print("  1. Split data into TRAIN and VALIDATION")
print("  2. For each hyperparameter combination:")
print("     a. Train LightGBM model on TRAIN data")
print("     b. Predict on VALIDATION data")
print("     c. Calculate R² and MSE scores")
print("  3. Save all results to CSV file")
print("  4. Best hyperparameters = highest R² score")
print()
print("Why R² score?")
print("  - R² measures prediction accuracy (0 = random, 1 = perfect)")
print("  - Higher R² = Better predictions")
print("  - We want the model that predicts returns most accurately!")
print()
print("Why MSE (Mean Squared Error)?")
print("  - MSE measures average prediction error")
print("  - Lower MSE = Better predictions")
print("  - Backup metric if R² is similar")

print("\n" + "=" * 60)
print("STEP 5 COMPLETE: Cross-validation function ready!")
print("=" * 60)

# %%
# ------------------------------
# STEP 6: Implement Main Training/Prediction Function
# ------------------------------
print("\n" + "=" * 60)
print("STEP 6: MAIN TRAINING/PREDICTION FUNCTION")
print("=" * 60)

# ------------------------------
# 6.1 Main Prediction Function
# ------------------------------
def run_prediction(data, k, config, feature_cols):
    """
    Train final model and make predictions for period k+1.
    Uses best hyperparameters from cross-validation results.

    This function:
    1. Loads best hyperparameters from CV file
    2. Trains on MORE data (train + validation combined)
    3. Makes predictions on test period (k+1)
    4. Saves predictions with metadata

    Parameters:
    -----------
    data : DataFrame
        Full dataset with counter variable
    k : int
        Current counter (will predict for k+1)
    config : dict
        Configuration dictionary
    feature_cols : list
        List of feature column names

    Returns:
    --------
    result_df : DataFrame
        Predictions with metadata (permno, ticker, predicted_return, etc.)
    """

    print(f"\n{'='*60}")
    print(f"Running Prediction for Counter {k+1}")
    print(f"{'='*60}")

    # 1. Load best hyperparameters from CV file
    cv_file = output_filename(config, mode='cv', counter=k+1)

    if not cv_file.exists():
        print(f"⚠️  WARNING: CV file not found: {cv_file}")
        print("   Run cross-validation first (Step 7) before predictions!")
        return None

    cv_results = pd.read_csv(cv_file)
    cv_results = cv_results.sort_values('r2_score', ascending=False)
    best_params = cv_results.iloc[0]

    print(f"✅ Loaded best hyperparameters from CV:")
    print(f"   n_estimators:  {int(best_params['n_estimators'])}")
    print(f"   learning_rate: {best_params['learning_rate']:.3f}")
    print(f"   R² score:      {best_params['r2_score']:+.4f}")

    # 2. Get train and test data
    print(f"\nSplitting data for prediction...")
    X_train, y_train, X_test, key_test = train_test_data(
        data, k, config, feature_cols
    )

    print(f"  Training set: {len(X_train):,} observations (includes validation period!)")
    print(f"  Test set:     {len(X_test):,} observations")
    print(f"  Features:     {len(feature_cols):,}")

    # 3. Train model with best hyperparameters
    print(f"\nTraining final model...")

    if config['method'] == 'brt':
        model = lgb.LGBMRegressor(
            learning_rate=float(best_params['learning_rate']),
            max_depth=int(best_params['max_depth']),
            n_estimators=int(best_params['n_estimators']),
            objective='regression',
            random_state=42,
            device='cuda',  # 🚀 CUDA acceleration enabled (RTX 5090)
            # force_col_wise=True,  # Removed - conflicts with CUDA
            # n_jobs=-1,  # Removed - ignored by CUDA
            verbose=-1  # Suppress output for cleaner logs
        )

        model.fit(X_train, y_train)
        print(f"✅ Model trained successfully!")

    # 4. Make predictions on test period
    print(f"Making predictions...")
    predictions = model.predict(X_test)

    print(f"  Predictions range: [{predictions.min():+.4f}, {predictions.max():+.4f}]")
    print(f"  Predictions mean:  {predictions.mean():+.4f}")

    # 5. Create result dataframe with predictions and metadata
    result_df = key_test.copy()
    result_df['predicted_return'] = predictions

    # 6. Save predictions to file
    output_file = output_filename(config, mode='pred', counter=k+1)
    result_df.to_csv(output_file, index=False)

    print(f"\n✅ Predictions saved to: {output_file.name}")

    return result_df


# ------------------------------
# 6.2 Explain the prediction process
# ------------------------------
print("\n📚 HOW PREDICTION WORKS:")
print("-" * 50)
print()
print("For each time period k:")
print("  1. Load best hyperparameters from CV results (Step 5)")
print("  2. Train on MORE data than CV:")
print("     - CV used:  [train] [validation] | [test]")
print("     - Now use:  [train + validation] | [test]")
print("  3. Make predictions on test period (k+1)")
print("  4. Save predictions with metadata:")
print("     - permno, ticker, form_year, form_date")
print("     - predicted_return, actual_return (if available)")
print()
print("Why train on more data?")
print("  - CV already found the best hyperparameters")
print("  - Now we use validation data for training too!")
print("  - More training data = Better predictions")

print("\n" + "=" * 60)
print("STEP 6 COMPLETE: Prediction function ready!")
print("=" * 60)

# %%
# ------------------------------
# STEP 7: Run Cross-Validation for All Time Periods
# ------------------------------
print("\n" + "=" * 60)
print("STEP 7: RUNNING CROSS-VALIDATION FOR ALL PERIODS")
print("=" * 60)

# This step will take time! It runs CV for every test period.
# Set RUN_CV = True to execute

RUN_CV = True  # Change to True when ready to run
USE_PARALLEL = False  # ⚠️ Set to False when using GPU (GPU processes can't run in parallel)
MAX_WORKERS = 3  # Number of parallel processes (only used if USE_PARALLEL=True)

if RUN_CV:
    print(f"\n🔄 Starting cross-validation...")
    print(f"Periods to process: {CONFIG['begin']} to {CONFIG['end']} ({CONFIG['end'] - CONFIG['begin'] + 1} periods)")
    print(f"Hyperparameter combinations per period: {len(hyperparameter_grid)}")

    if USE_PARALLEL:
        print(f"⚡ Parallel mode: {MAX_WORKERS} workers")
        print(f"Estimated time: ~{(CONFIG['end'] - CONFIG['begin'] + 1) * 5 / MAX_WORKERS:.0f} minutes")
    else:
        print(f"🔄 Sequential mode")
        print(f"Estimated time: ~{(CONFIG['end'] - CONFIG['begin'] + 1) * 5} minutes")
    print()

    import time
    start_time = time.time()

    if USE_PARALLEL:
        # Parallel execution using ProcessPoolExecutor
        from concurrent.futures import ProcessPoolExecutor, as_completed

        # Worker function for parallel processing
        def cv_worker(k):
            """Worker function to run CV for a single counter k (validation endpoint)
            This will optimize hyperparameters for testing counter k+1"""
            try:
                year = [y for y, c in year_to_counter.items() if c == k+1][0]  # k+1 is the test counter
                cv_results = run_cross_validation(df_scaled, k, CONFIG, final_feature_columns)
                return (k, year, True, None)
            except Exception as e:
                return (k, year, False, str(e))

        # Submit all jobs
        # k represents validation endpoint, k+1 will be the test counter
        counters = list(range(CONFIG['begin'] - 1, CONFIG['end']))

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            # Submit all tasks
            futures = {executor.submit(cv_worker, k): k for k in counters}

            # Process results as they complete
            completed = 0
            for future in as_completed(futures):
                k, year, success, error = future.result()
                completed += 1

                if success:
                    print(f"✅ [{completed}/{len(counters)}] CV for test counter {k+1} (Year {year:.0f}) complete!")
                else:
                    print(f"❌ [{completed}/{len(counters)}] CV for test counter {k+1} (Year {year:.0f}) failed: {error}")

    else:
        # Sequential execution (original approach)
        # k represents validation endpoint, k+1 will be the test counter
        for k in range(CONFIG['begin'] - 1, CONFIG['end']):
            year = [y for y, c in year_to_counter.items() if c == k+1][0]  # k+1 is the test counter
            print(f"\n{'='*60}")
            print(f"Processing CV for test counter {k+1} (Year {year:.0f})")
            print(f"  (Training: ≤{k-CONFIG['cv_validation']}, Validation: {k-CONFIG['cv_validation']+1}-{k})")
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
    print(f"Time elapsed: {elapsed_time/60:.1f} minutes")
    print("="*60)

else:
    print("\n⚠️  Cross-validation not run (RUN_CV = False)")
    print("Set RUN_CV = True to execute cross-validation")
    print("\n💡 TIP: CV will take ~2-3 hours for full dataset")
    print("💡 TIP: Set USE_PARALLEL = True for 2-3x speedup")

print("\n" + "=" * 60)
print("STEP 7 STATUS: Ready to run when RUN_CV = True")
print("=" * 60)

# %%
# ------------------------------
# STEP 8: Run Final Predictions on Test Periods
# ------------------------------
print("\n" + "=" * 60)
print("STEP 8: RUNNING FINAL PREDICTIONS")
print("=" * 60)

# This step requires Step 7 to be complete!
# Set RUN_PRED = True to execute

RUN_PRED = True  # Change to True when ready to run
USE_PARALLEL = False  # Set to True for parallel processing
MAX_WORKERS = 4  # Number of parallel processes (recommended: 2-4)

if RUN_PRED:
    print(f"\n🔄 Starting predictions...")
    print(f"Periods to predict: {CONFIG['begin']} to {CONFIG['end']}")

    if USE_PARALLEL:
        print(f"⚡ Parallel mode: {MAX_WORKERS} workers")
    else:
        print(f"🔄 Sequential mode")
    print()

    all_predictions = []
    import time
    start_time = time.time()

    if USE_PARALLEL:
        # Parallel execution using ProcessPoolExecutor
        from concurrent.futures import ProcessPoolExecutor, as_completed

        def prediction_worker(k):
            """
            Worker function to run prediction for a single counter.
            Returns: (k, year, pred_results, success, error_msg)
            """
            year = None
            try:
                year = [y for y, c in year_to_counter.items() if c == k+1][0]
                print(f"🔄 [Worker] Starting counter {k+1} (Year {year:.0f})...")
                pred_results = run_prediction(df_scaled, k, CONFIG, final_feature_columns)

                if pred_results is not None:
                    return (k, year, pred_results, True, None)
                else:
                    return (k, year, None, False, "CV file not found")

            except Exception as e:
                # If year wasn't set, try to get it for error reporting
                if year is None:
                    try:
                        year = [y for y, c in year_to_counter.items() if c == k+1][0]
                    except:
                        year = -1  # Fallback if year lookup fails
                return (k, year, None, False, str(e))

        # Get list of counters to process
        counters = list(range(CONFIG['begin'] - 1, CONFIG['end']))

        print(f"📋 Counters to process: {counters}")
        print(f"📋 Total tasks: {len(counters)}")

        # Submit all tasks to the executor
        print(f"🔄 Creating ProcessPoolExecutor with {MAX_WORKERS} workers...")

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            print(f"✅ Executor created!")

            # Submit all prediction tasks
            print(f"🔄 Submitting {len(counters)} tasks...")
            futures = {executor.submit(prediction_worker, k): k for k in counters}
            print(f"✅ All {len(futures)} tasks submitted!")

            # Process results as they complete
            completed = 0
            total = len(futures)

            print(f"⏳ Waiting for results...")

            for future in as_completed(futures):
                k, year, pred_results, success, error = future.result()
                completed += 1

                print(f"\n{'='*60}")
                print(f"[{completed}/{total}] Counter {k+1} (Year {year:.0f})")
                print(f"{'='*60}")

                if success:
                    all_predictions.append(pred_results)
                    print(f"✅ Predictions saved!")
                else:
                    print(f"⚠️  Skipped: {error}")

    else:
        # Sequential execution (original approach)
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
                    print(f"⚠️  Skipping counter {k+1} (CV file not found)")

            except Exception as e:
                print(f"❌ Error at counter {k+1}: {str(e)}")
                print("Continuing to next period...")
                continue

    # Combine all predictions
    if len(all_predictions) > 0:
        final_predictions = pd.concat(all_predictions, ignore_index=True)

        # Save combined predictions
        combined_file = output_dir / 'all_predictions.csv'
        final_predictions.to_csv(combined_file, index=False)

        elapsed_time = time.time() - start_time
        print(f"\n" + "="*60)
        print(f"✅ ALL PREDICTIONS COMPLETE!")
        print(f"Total observations: {len(final_predictions):,}")
        print(f"Time elapsed: {elapsed_time/60:.1f} minutes")
        print(f"Results saved to: {combined_file.name}")
        print("="*60)
    else:
        print("\n⚠️  No predictions generated!")
        final_predictions = None

else:
    print("\n⚠️  Predictions not run (RUN_PRED = False)")
    print("Set RUN_PRED = True to execute predictions")
    print("⚠️  IMPORTANT: Run Step 7 (Cross-Validation) first!")
    final_predictions = None

print("\n" + "=" * 60)
print("STEP 8 STATUS: Ready to run when RUN_PRED = True")
print("=" * 60)

# %%
# ------------------------------
# STEP 9: Combine Predictions and Build Portfolios
# ------------------------------
print("\n" + "=" * 60)
print("STEP 9: BUILDING PORTFOLIOS")
print("=" * 60)

# This step requires Step 8 to be complete!

if final_predictions is not None and RUN_PRED:
    print(f"\n📊 Creating long/short portfolios...")

    # Create portfolio dataset
    portfolio_data = final_predictions.copy()

    # Verify we have the necessary columns
    required_cols = ['form_year', 'predicted_return', CONFIG['dep_var']]
    missing_cols = [col for col in required_cols if col not in portfolio_data.columns]

    if missing_cols:
        print(f"⚠️  Missing required columns: {missing_cols}")
        portfolio_df_fixed = None
        portfolio_df_decile = None
    else:
        # ========================================
        # VERSION 1: FIXED TOP 100 / BOTTOM 100
        # ========================================
        print(f"\n📈 Building portfolios - VERSION 1: Fixed Top 100 / Bottom 100")
        print("-" * 60)
        portfolio_results_fixed = []

        for year in sorted(portfolio_data['form_year'].unique()):
            year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

            if len(year_data) < 200:
                continue

            # Sort by predicted returns
            year_data = year_data.sort_values('predicted_return', ascending=False)

            # Fixed: Top 100 long, bottom 100 short
            TOP_N = 100
            BOTTOM_N = 100

            long_portfolio = year_data.head(TOP_N)
            short_portfolio = year_data.tail(BOTTOM_N)

            # Calculate returns
            long_return = long_portfolio[CONFIG['dep_var']].mean()
            short_return = -short_portfolio[CONFIG['dep_var']].mean()
            spread = long_return - short_return

            portfolio_results_fixed.append({
                'year': year,
                'long_return': long_return,
                'short_return': short_return,
                'spread': spread,
                'n_long': len(long_portfolio),
                'n_short': len(short_portfolio)
            })

            print(f"  Year {year:.0f}: {len(year_data):,} stocks → Long {len(long_portfolio)}, Short {len(short_portfolio)}")

        portfolio_df_fixed = pd.DataFrame(portfolio_results_fixed)
        portfolio_file_fixed = output_dir / 'portfolio_returns_fixed100.csv'
        portfolio_df_fixed.to_csv(portfolio_file_fixed, index=False)
        print(f"✅ Fixed-100 portfolios saved to: {portfolio_file_fixed.name}")

        # ========================================
        # VERSION 2: DECILE METHOD (10% LONG / 5% SHORT)
        # ========================================
        print(f"\n📈 Building portfolios - VERSION 2: Decile Method (Top 10% / Bottom 5%)")
        print("-" * 60)
        portfolio_results_decile = []

        for year in sorted(portfolio_data['form_year'].unique()):
            year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

            if len(year_data) < 200:
                continue

            # Sort by predicted returns
            year_data = year_data.sort_values('predicted_return', ascending=False)

            # Custom: Top 10% long, Bottom 5% short
            n_stocks = len(year_data)
            decile_size_long = n_stocks // 10   # 10% of stocks for long
            decile_size_short = n_stocks // 20  # 5% of stocks for short

            TOP_N = decile_size_long       # Top 10% (Decile 10)
            BOTTOM_N = decile_size_short   # Bottom 5% (Half decile)

            long_portfolio = year_data.head(TOP_N)
            short_portfolio = year_data.tail(BOTTOM_N)

            # Calculate returns
            long_return = long_portfolio[CONFIG['dep_var']].mean()
            short_return = -short_portfolio[CONFIG['dep_var']].mean()
            spread = long_return - short_return

            portfolio_results_decile.append({
                'year': year,
                'long_return': long_return,
                'short_return': short_return,
                'spread': spread,
                'n_long': len(long_portfolio),
                'n_short': len(short_portfolio)
            })

            print(f"  Year {year:.0f}: {len(year_data):,} stocks → Long {len(long_portfolio)}, Short {len(short_portfolio)}")

        portfolio_df_decile = pd.DataFrame(portfolio_results_decile)
        portfolio_file_decile = output_dir / 'portfolio_returns_decile10pct.csv'
        portfolio_df_decile.to_csv(portfolio_file_decile, index=False)
        print(f"✅ Decile-10% portfolios saved to: {portfolio_file_decile.name}")

        # Store decile version as default for Step 10
        portfolio_df = portfolio_df_decile

else:
    print("\n⚠️  Portfolios not created (predictions not available)")
    print("Run Steps 7-8 first!")
    portfolio_df = None

print("\n" + "=" * 60)
print("STEP 9 STATUS: Portfolios ready if predictions exist")
print("=" * 60)

# %%
# ------------------------------
# STEP 10: Evaluate Model Performance and Generate Results
# ------------------------------
print("\n" + "=" * 60)
print("STEP 10: EVALUATING PERFORMANCE")
print("=" * 60)

# Check if both portfolio versions exist
has_fixed = 'portfolio_df_fixed' in locals() and portfolio_df_fixed is not None and len(portfolio_df_fixed) > 0
has_decile = 'portfolio_df_decile' in locals() and portfolio_df_decile is not None and len(portfolio_df_decile) > 0

if has_fixed or has_decile:
    print(f"\n📈 Calculating performance metrics for both portfolio versions...")

    # ========================================
    # VERSION 1: FIXED TOP 100 / BOTTOM 100
    # ========================================
    if has_fixed:
        print("\n" + "="*60)
        print("VERSION 1: FIXED TOP 100 / BOTTOM 100 RESULTS")
        print("="*60)

        # Calculate performance metrics
        avg_long_fixed = portfolio_df_fixed['long_return'].mean()
        avg_short_fixed = portfolio_df_fixed['short_return'].mean()
        avg_spread_fixed = portfolio_df_fixed['spread'].mean()
        spread_std_fixed = portfolio_df_fixed['spread'].std()
        sharpe_ratio_fixed = avg_spread_fixed / spread_std_fixed if spread_std_fixed > 0 else 0

        print()
        print("Portfolio Returns (Annual Average):")
        print(f"  Long Portfolio (Top 100):     {avg_long_fixed:+.4f} ({avg_long_fixed*100:+.2f}%)")
        print(f"  Short Portfolio (Bottom 100): {avg_short_fixed:+.4f} ({avg_short_fixed*100:+.2f}%)")
        print(f"  Long-Short Spread:            {avg_spread_fixed:+.4f} ({avg_spread_fixed*100:+.2f}%)")
        print()
        print("Risk-Adjusted Performance:")
        print(f"  Spread Volatility:  {spread_std_fixed:.4f} ({spread_std_fixed*100:.2f}%)")
        print(f"  Sharpe Ratio:       {sharpe_ratio_fixed:.2f}")
        print()
        print(f"Analysis Period:")
        print(f"  Years analyzed:     {len(portfolio_df_fixed)}")
        print(f"  First year:         {portfolio_df_fixed['year'].min():.0f}")
        print(f"  Last year:          {portfolio_df_fixed['year'].max():.0f}")
        print("="*60)

        # Assessment
        if sharpe_ratio_fixed > 1.0:
            print("\n✅ Excellent risk-adjusted returns!")
        elif sharpe_ratio_fixed > 0.5:
            print("\n✅ Good risk-adjusted returns")
        elif sharpe_ratio_fixed > 0.0:
            print("\n⚠️  Positive but weak risk-adjusted returns")
        else:
            print("\n❌ Negative risk-adjusted returns")

        # Save summary
        summary_fixed = {
            'portfolio_type': 'Fixed_Top100',
            'avg_long_return': avg_long_fixed,
            'avg_short_return': avg_short_fixed,
            'avg_spread': avg_spread_fixed,
            'spread_volatility': spread_std_fixed,
            'sharpe_ratio': sharpe_ratio_fixed,
            'n_years': len(portfolio_df_fixed),
            'first_year': portfolio_df_fixed['year'].min(),
            'last_year': portfolio_df_fixed['year'].max()
        }

        summary_df_fixed = pd.DataFrame([summary_fixed])
        summary_file_fixed = output_dir / 'performance_summary_fixed100.csv'
        summary_df_fixed.to_csv(summary_file_fixed, index=False)
        print(f"\n✅ Fixed-100 summary saved to: {summary_file_fixed.name}")

    # ========================================
    # VERSION 2: DECILE METHOD (10% LONG / 5% SHORT)
    # ========================================
    if has_decile:
        print("\n" + "="*60)
        print("VERSION 2: DECILE METHOD (TOP 10% LONG / BOTTOM 5% SHORT) RESULTS")
        print("="*60)

        # Calculate performance metrics
        avg_long_decile = portfolio_df_decile['long_return'].mean()
        avg_short_decile = portfolio_df_decile['short_return'].mean()
        avg_spread_decile = portfolio_df_decile['spread'].mean()
        spread_std_decile = portfolio_df_decile['spread'].std()
        sharpe_ratio_decile = avg_spread_decile / spread_std_decile if spread_std_decile > 0 else 0

        print()
        print("Portfolio Returns (Annual Average):")
        print(f"  Long Portfolio (Top 10%):      {avg_long_decile:+.4f} ({avg_long_decile*100:+.2f}%)")
        print(f"  Short Portfolio (Bottom 5%):   {avg_short_decile:+.4f} ({avg_short_decile*100:+.2f}%)")
        print(f"  Long-Short Spread:                   {avg_spread_decile:+.4f} ({avg_spread_decile*100:+.2f}%)")
        print()
        print("Risk-Adjusted Performance:")
        print(f"  Spread Volatility:  {spread_std_decile:.4f} ({spread_std_decile*100:.2f}%)")
        print(f"  Sharpe Ratio:       {sharpe_ratio_decile:.2f}")
        print()
        print(f"Analysis Period:")
        print(f"  Years analyzed:     {len(portfolio_df_decile)}")
        print(f"  First year:         {portfolio_df_decile['year'].min():.0f}")
        print(f"  Last year:          {portfolio_df_decile['year'].max():.0f}")
        print("="*60)

        # Assessment
        if sharpe_ratio_decile > 1.0:
            print("\n✅ Excellent risk-adjusted returns!")
        elif sharpe_ratio_decile > 0.5:
            print("\n✅ Good risk-adjusted returns")
        elif sharpe_ratio_decile > 0.0:
            print("\n⚠️  Positive but weak risk-adjusted returns")
        else:
            print("\n❌ Negative risk-adjusted returns")

        # Save summary
        summary_decile = {
            'portfolio_type': 'Decile_10pct',
            'avg_long_return': avg_long_decile,
            'avg_short_return': avg_short_decile,
            'avg_spread': avg_spread_decile,
            'spread_volatility': spread_std_decile,
            'sharpe_ratio': sharpe_ratio_decile,
            'n_years': len(portfolio_df_decile),
            'first_year': portfolio_df_decile['year'].min(),
            'last_year': portfolio_df_decile['year'].max()
        }

        summary_df_decile = pd.DataFrame([summary_decile])
        summary_file_decile = output_dir / 'performance_summary_decile10pct.csv'
        summary_df_decile.to_csv(summary_file_decile, index=False)
        print(f"\n✅ Decile-10% summary saved to: {summary_file_decile.name}")

    # ========================================
    # COMPARISON (if both exist)
    # ========================================
    if has_fixed and has_decile:
        print("\n" + "="*60)
        print("COMPARISON: FIXED 100 vs DECILE 10%")
        print("="*60)
        print()
        print(f"{'Metric':<25} | {'Fixed 100':>12} | {'Decile 10%':>12} | {'Difference':>12}")
        print("-" * 70)
        print(f"{'Long Return':<25} | {avg_long_fixed*100:>11.2f}% | {avg_long_decile*100:>11.2f}% | {(avg_long_decile-avg_long_fixed)*100:>+11.2f}%")
        print(f"{'Short Return':<25} | {avg_short_fixed*100:>11.2f}% | {avg_short_decile*100:>11.2f}% | {(avg_short_decile-avg_short_fixed)*100:>+11.2f}%")
        print(f"{'Spread':<25} | {avg_spread_fixed*100:>11.2f}% | {avg_spread_decile*100:>11.2f}% | {(avg_spread_decile-avg_spread_fixed)*100:>+11.2f}%")
        print(f"{'Volatility':<25} | {spread_std_fixed*100:>11.2f}% | {spread_std_decile*100:>11.2f}% | {(spread_std_decile-spread_std_fixed)*100:>+11.2f}%")
        print(f"{'Sharpe Ratio':<25} | {sharpe_ratio_fixed:>12.2f} | {sharpe_ratio_decile:>12.2f} | {sharpe_ratio_decile-sharpe_ratio_fixed:>+12.2f}")
        print("="*60)

        # Save combined comparison
        comparison_df = pd.concat([summary_df_fixed, summary_df_decile], ignore_index=True)
        comparison_file = output_dir / 'performance_comparison.csv'
        comparison_df.to_csv(comparison_file, index=False)
        print(f"\n✅ Comparison saved to: {comparison_file.name}")

else:
    print("\n⚠️  No performance metrics available")
    print("Run Steps 7-9 first!")

print("\n" + "=" * 60)
print("STEP 10 COMPLETE!")
print("=" * 60)

# %%
# ------------------------------
# STEP 11: Export Long/Short Positions to Excel
# ------------------------------
print("\n" + "=" * 60)
print("STEP 11: EXPORTING LONG/SHORT POSITIONS TO EXCEL")
print("=" * 60)

if final_predictions is not None and RUN_PRED:
    print("\n📊 Creating Excel file with long/short positions by year...")

    # Create Excel writer
    excel_filename = output_dir / 'long_short_positions.xlsx'

    with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:

        # Summary sheet - all years combined
        all_positions = []

        # Process each year
        for year in sorted(final_predictions['form_year'].unique()):
            year_data = final_predictions[final_predictions['form_year'] == year].copy()
            # print(year_data.head())
            
            if len(year_data) < 200:
                print(f"  Skipping {year:.0f}: only {len(year_data)} stocks")
                continue

            print(f"  Processing {year:.0f}: {len(year_data):,} stocks")

            # Sort by predicted returns
            year_data = year_data.sort_values('predicted_return', ascending=False)
            year_data['rank'] = range(1, len(year_data) + 1)

            # Top 100 long, bottom 100 short
            TOP_N = 100
            BOTTOM_N = 100

            long_portfolio = year_data.head(TOP_N).copy()
            long_portfolio['position'] = 'LONG'
            long_portfolio['position_rank'] = range(1, len(long_portfolio) + 1)

            short_portfolio = year_data.tail(BOTTOM_N).copy()
            # Reverse order so rank 1 = worst (most negative) predicted return
            short_portfolio = short_portfolio.sort_values('predicted_return', ascending=True)
            short_portfolio['position'] = 'SHORT'
            short_portfolio['position_rank'] = range(1, len(short_portfolio) + 1)

            # Combine long and short for this year
            year_positions = pd.concat([long_portfolio, short_portfolio], ignore_index=True)

            # Select and rename columns for clarity
            columns_to_export = [
                'position', 'position_rank', 'ticker', 'permno',
                'form_year', 'form_date', 'predicted_return', CONFIG['dep_var']
            ]

            # Add market cap if available
            if 'crsp_mktcap_6' in year_positions.columns:
                columns_to_export.insert(4, 'crsp_mktcap_6')

            # Filter to available columns
            available_cols = [col for col in columns_to_export if col in year_positions.columns]
            year_export = year_positions[available_cols].copy()

            # Rename for clarity
            year_export = year_export.rename(columns={
                CONFIG['dep_var']: 'actual_return',
                'crsp_mktcap_6': 'market_cap'
            })

            # Calculate performance metrics for this year
            long_return = long_portfolio[CONFIG['dep_var']].mean()
            short_return = -short_portfolio[CONFIG['dep_var']].mean()
            spread = long_return - short_return

            # Add summary row at top
            summary_row = pd.DataFrame([{
                'position': 'SUMMARY',
                'position_rank': '',
                'ticker': f'Year {year:.0f}',
                'permno': '',
                'form_year': year,
                'form_date': '',
                'predicted_return': '',
                'actual_return': f'Long: {long_return:+.4f} | Short: {short_return:+.4f} | Spread: {spread:+.4f}'
            }])

            # Combine summary with positions
            year_export_with_summary = pd.concat([summary_row, year_export], ignore_index=True)

            # Export to sheet (sheet name limited to 31 chars)
            sheet_name = f'Year_{int(year)}'
            year_export_with_summary.to_excel(writer, sheet_name=sheet_name, index=False)

            # Add to all positions list
            all_positions.append(year_export)

        # Create combined sheet with all years
        if len(all_positions) > 0:
            all_positions_df = pd.concat(all_positions, ignore_index=True)
            all_positions_df = all_positions_df.sort_values(['form_year', 'position', 'position_rank'])
            all_positions_df.to_excel(writer, sheet_name='All_Years', index=False)
            print(f"\n  Combined sheet 'All_Years' created with {len(all_positions_df):,} positions")

        # Create summary sheet
        if portfolio_df is not None:
            portfolio_summary = portfolio_df.copy()
            portfolio_summary['year'] = portfolio_summary['year'].astype(int)
            portfolio_summary.to_excel(writer, sheet_name='Summary', index=False)
            print(f"  Summary sheet created with {len(portfolio_summary)} years")

    print(f"\n✅ Excel file created: {excel_filename.name}")
    print(f"   Location: {excel_filename}")
    print(f"   Sheets: {len(final_predictions['form_year'].unique())} year sheets + All_Years + Summary")

else:
    print("\n⚠️  Cannot export positions (predictions not available)")
    print("   Run Steps 7-8 first!")

print("\n" + "=" * 60)
print("STEP 11 COMPLETE!")
print("=" * 60)

# %%


print("\n\n" + "="*60)
print("🎉 MACHINE LEARNING PIPELINE COMPLETE!")
print("="*60)
print()
print("Summary of what you can do:")
print()
print("1. TO RUN CROSS-VALIDATION:")
print("   - Set RUN_CV = True in Step 7")
print("   - Run Steps 1-7")
print("   - Wait ~2-3 hours")
print()
print("2. TO RUN PREDICTIONS:")
print("   - Complete Step 7 first!")
print("   - Set RUN_PRED = True in Step 8")
print("   - Run Steps 8-10")
print()
print("3. RESULTS WILL BE IN:")
print(f"   - CV results: {cv_dir}")
print(f"   - Predictions: {pred_dir}")
print(f"   - Performance: {output_dir}")
print()
print("="*60)

# %%
