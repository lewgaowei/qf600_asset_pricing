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
# Core libraries
import numpy as np
import pandas as pd
import warnings
from pathlib import Path
import pickle
import os

# Machine Learning libraries - GENETIC PROGRAMMING
from gplearn.genetic import SymbolicRegressor
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.model_selection import ParameterGrid

# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

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
    'use_top_features': 70,        # CRITICAL: GP works best with 50-150 features
                                    # Too many features = exponential complexity
    
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
    Define hyperparameter grid for Genetic Programming Symbolic Regression.
    """
    if method == 'gp_symbolic':
        grid = {
            # Population and evolution settings
            'population_size': [1000],        # Number of programs in each generation
            'generations': [20],                 # Number of evolutionary generations

            # Program complexity controls
            'init_depth': [(2, 6), (3, 6)],          # Initial depth range (min, max) for random programs
            'init_method': ['half and half'],        # How to create initial population
            
            # Evolution operators (probabilities sum to 1.0)
            'p_crossover': [0.7],                    # Probability of crossover (combining programs)
            'p_subtree_mutation': [0.1],             # Probability of subtree mutation
            'p_hoist_mutation': [0.05],              # Probability of hoist mutation
            'p_point_mutation': [0.1],               # Probability of point mutation
            
            # Selection and parsimony
            'parsimony_coefficient': [0.01],  # Penalty for program complexity
            'tournament_size': [20],                 # Number of programs competing in tournament
            
            # Stopping criteria
            'stopping_criteria': [0.0],              # Stop if fitness reaches this value
            
            # Function set
            'function_set': [('add', 'sub', 'mul', 'div')],  # Mathematical operations
            
            # Other settings
            'random_state': [42]                     # For reproducibility
        }
    else:
        raise ValueError(f"Unknown method: {method}")

    tunegrid = list(ParameterGrid(grid))
    return tunegrid


hyperparameter_grid = get_hyperparameter_grid(CONFIG['method'])

print(f"\n📊 Hyperparameter Grid for {CONFIG['method'].upper()}:")
print("-" * 50)
print(f"Total combinations to try: {len(hyperparameter_grid)}")
print()

# Show parameter ranges
print("Parameter ranges:")
pop_values = sorted(set([p['population_size'] for p in hyperparameter_grid]))
gen_values = sorted(set([p['generations'] for p in hyperparameter_grid]))
depth_values = sorted(set([p['init_depth'] for p in hyperparameter_grid]))
parsimony_values = sorted(set([p['parsimony_coefficient'] for p in hyperparameter_grid]))

print(f"  population_size:         {pop_values}")
print(f"  generations:             {gen_values}")
print(f"  init_depth:              {depth_values}")
print(f"  parsimony_coefficient:   {parsimony_values}")

print()
print("First 5 combinations:")
print("-" * 80)
print("  #  | pop_size | gens |    depth    | parsimony")
print("-" * 80)

for i, params in enumerate(hyperparameter_grid[:5], 1):
    print(f"  {i:2d} | {params['population_size']:8d} | {params['generations']:4d} | "
          f"{str(params['init_depth']):11s} | {params['parsimony_coefficient']:9.4f}")

if len(hyperparameter_grid) > 5:
    print(f"  ... and {len(hyperparameter_grid) - 5} more combinations")

# ------------------------------
# Explain GP hyperparameters
# ------------------------------
print("\n" + "-" * 50)
print("📚 GENETIC PROGRAMMING HYPERPARAMETERS:")
print("-" * 50)
print()
print("1. population_size (Number of Programs):")
print("   - How many mathematical expressions evolve simultaneously")
print("   - Larger = Better exploration, slower training")
print("   - Typical: 1000-5000")
print()
print("2. generations (Evolution Cycles):")
print("   - How many iterations of evolution")
print("   - More generations = Better solutions, but diminishing returns")
print("   - Typical: 20-50")
print()
print("3. init_depth (Initial Program Complexity):")
print("   - Starting depth of expression trees")
print("   - Deeper = More complex initial formulas")
print("   - Typical: 2-6")
print()
print("4. parsimony_coefficient (Simplicity Penalty):")
print("   - Penalty for overly complex expressions")
print("   - Higher = Simpler formulas preferred")
print("   - Prevents overfitting")
print()
print("5. Evolution Operators:")
print("   - p_crossover: Combine two good formulas")
print("   - p_subtree_mutation: Replace part of formula")
print("   - p_hoist_mutation: Simplify by removing branches")
print("   - p_point_mutation: Change a single operation/variable")

print("\n" + "=" * 60)
print("STEP 8 COMPLETE: Hyperparameter grid ready!")
print("=" * 60)
print(f"\n⚠️  NOTE: GP training will be MUCH slower than XGBoost")
print("   Each combination may take 5-30 minutes depending on:")
print("   - Number of features ({len(final_feature_columns)})")
print("   - Population size")
print("   - Number of generations")


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
        print(f"  Population: {params['population_size']}, Generations: {params['generations']}, "
              f"Depth: {params['init_depth']}, Parsimony: {params['parsimony_coefficient']}")

        try:
            # Create GP model
            model = SymbolicRegressor(
                population_size=params['population_size'],
                generations=params['generations'],
                stopping_criteria=params['stopping_criteria'],
                p_crossover=params['p_crossover'],
                p_subtree_mutation=params['p_subtree_mutation'],
                p_hoist_mutation=params['p_hoist_mutation'],
                p_point_mutation=params['p_point_mutation'],
                max_samples=0.9,  # Use 90% of data for fitness evaluation (speeds up)
                verbose=0,
                parsimony_coefficient=params['parsimony_coefficient'],
                random_state=params['random_state'],
                n_jobs=-1,  # Use all available CPU cores
                function_set=params['function_set'],
                init_depth= params['init_depth'],
                init_method=params['init_method'],
                tournament_size=params['tournament_size']
            )

            # Train the model
            print(f"  🧬 Evolving programs...", end='', flush=True)
            model.fit(X_train, y_train)
            print(" ✓")

            # Make predictions
            y_pred = model.predict(X_validation)

            # Calculate metrics
            r2 = r2_score(y_validation, y_pred)
            mse = mean_squared_error(y_validation, y_pred)

            # Get program information
            program_length = len(str(model._program))
            program_depth = model._program.depth_

            # Store results
            cv_results.loc[i, 'r2_score'] = r2
            cv_results.loc[i, 'mse'] = mse
            cv_results.loc[i, 'program_length'] = program_length
            cv_results.loc[i, 'program_depth'] = program_depth

            iter_duration = time.time() - iter_start
            print(f"  ✅ R²={r2:+.4f}, MSE={mse:.6f}, "
                  f"Length={program_length}, Depth={program_depth}")
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
        print(f"   Population: {row['population_size']:.0f}, Generations: {row['generations']:.0f}")
        print(f"   Program: Length={row['program_length']:.0f}, Depth={row['program_depth']:.0f}")

    # Save results
    output_file = output_filename(config, mode='cv', counter=k+1)
    cv_results.to_csv(output_file, index=False)
    print(f"\n✅ Results saved to: {output_file.name}")

    total_time = time.time() - combo_start_time
    print(f"\n⏱️  Total CV time: {total_time/60:.1f} minutes")

    return cv_results


print("\n✅ Cross-validation function ready!")
print("\n⚠️  WARNING: GP cross-validation is SLOW")
print("   Expect 10-60 minutes per test period")
print("   Consider reducing population_size or generations for testing")

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

    print(f"✅ Loaded best hyperparameters:")
    print(f"   Population: {int(best_params['population_size'])}")
    print(f"   Generations: {int(best_params['generations'])}")
    print(f"   R² score: {best_params['r2_score']:+.4f}")

    # Get train and test data
    print(f"\nSplitting data...")
    X_train, y_train, X_test, key_test = train_test_data(
        data, k, config, feature_cols
    )

    print(f"  Training set: {len(X_train):,} observations")
    print(f"  Test set:     {len(X_test):,} observations")

    # Train final model
    print(f"\n🧬 Training final GP model...")

    import time
    train_start = time.time()

    # Parse init_depth - it's saved as string like "(2, 6)" in CSV
    init_depth_val = eval(best_params['init_depth']) if isinstance(best_params['init_depth'], str) else best_params['init_depth']

    model = SymbolicRegressor(
        population_size=int(best_params['population_size']),
        generations=int(best_params['generations']),
        stopping_criteria=float(best_params['stopping_criteria']),
        p_crossover=float(best_params['p_crossover']),
        p_subtree_mutation=float(best_params['p_subtree_mutation']),
        p_hoist_mutation=float(best_params['p_hoist_mutation']),
        p_point_mutation=float(best_params['p_point_mutation']),
        max_samples=0.9,
        verbose=1,  # Show progress for final training
        parsimony_coefficient=float(best_params['parsimony_coefficient']),
        random_state=int(best_params['random_state']),
        n_jobs=-1,  # Use all available CPU cores
        function_set=eval(best_params['function_set']),
        init_depth=init_depth_val,
        init_method=best_params['init_method'],
        tournament_size=int(best_params['tournament_size'])
    )

    model.fit(X_train, y_train)

    train_time = time.time() - train_start
    print(f"\n✅ Model trained in {train_time/60:.1f} minutes")

    # Print the evolved formula
    print(f"\n📐 EVOLVED FORMULA:")
    print("-" * 60)
    print(f"{model._program}")
    print("-" * 60)
    print(f"Program length: {len(str(model._program))}")
    print(f"Program depth: {model._program.depth_}")

    # Make predictions
    print(f"\n🔮 Making predictions...")
    predictions = model.predict(X_test)

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
        f.write(f"Evolved Formula for Counter {k+1}\n")
        f.write("=" * 60 + "\n\n")
        f.write(str(model._program))
        f.write(f"\n\nProgram Stats:\n")
        f.write(f"  Length: {len(str(model._program))}\n")
        f.write(f"  Depth: {model._program.depth_}\n")
        f.write(f"  R² on validation: {best_params['r2_score']:+.4f}\n")

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
        # VERSION 1: Fixed Top 100 / Bottom 100
        print(f"\n📈 Version 1: Fixed Top 100 / Bottom 100")
        print("-" * 60)
        portfolio_results_fixed = []

        for year in sorted(portfolio_data['form_year'].unique()):
            year_data = portfolio_data[portfolio_data['form_year'] == year].copy()
            if len(year_data) < 200:
                continue

            year_data = year_data.sort_values('predicted_return', ascending=False)
            
            long_portfolio = year_data.head(100)
            short_portfolio = year_data.tail(100)

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

            print(f"  Year {year:.0f}: Long {len(long_portfolio)}, Short {len(short_portfolio)}")

        portfolio_df_fixed = pd.DataFrame(portfolio_results_fixed)
        portfolio_file_fixed = output_dir / 'portfolio_returns_fixed100.csv'
        portfolio_df_fixed.to_csv(portfolio_file_fixed, index=False)
        print(f"✅ Saved to: {portfolio_file_fixed.name}")

        # VERSION 2: Decile (10% / 5%)
        print(f"\n📈 Version 2: Decile (Top 10% / Bottom 5%)")
        print("-" * 60)
        portfolio_results_decile = []

        for year in sorted(portfolio_data['form_year'].unique()):
            year_data = portfolio_data[portfolio_data['form_year'] == year].copy()
            if len(year_data) < 200:
                continue

            year_data = year_data.sort_values('predicted_return', ascending=False)
            
            n_stocks = len(year_data)
            long_portfolio = year_data.head(n_stocks // 10)
            short_portfolio = year_data.tail(n_stocks // 20)

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

            print(f"  Year {year:.0f}: Long {len(long_portfolio)}, Short {len(short_portfolio)}")

        portfolio_df_decile = pd.DataFrame(portfolio_results_decile)
        portfolio_file_decile = output_dir / 'portfolio_returns_decile10pct.csv'
        portfolio_df_decile.to_csv(portfolio_file_decile, index=False)
        print(f"✅ Saved to: {portfolio_file_decile.name}")

        portfolio_df = portfolio_df_decile

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

    if has_decile:
        print("\n" + "="*60)
        print("GENETIC PROGRAMMING RESULTS (Decile 10%)")
        print("="*60)

        avg_long = portfolio_df_decile['long_return'].mean()
        avg_short = portfolio_df_decile['short_return'].mean()
        avg_spread = portfolio_df_decile['spread'].mean()
        spread_std = portfolio_df_decile['spread'].std()
        sharpe = avg_spread / spread_std if spread_std > 0 else 0

        print()
        print("Portfolio Returns:")
        print(f"  Long:   {avg_long:+.4f} ({avg_long*100:+.2f}%)")
        print(f"  Short:  {avg_short:+.4f} ({avg_short*100:+.2f}%)")
        print(f"  Spread: {avg_spread:+.4f} ({avg_spread*100:+.2f}%)")
        print()
        print("Risk-Adjusted:")
        print(f"  Volatility:    {spread_std:.4f}")
        print(f"  Sharpe Ratio:  {sharpe:.2f}")
        print()
        print(f"Analysis Period: {len(portfolio_df_decile)} years")

        summary = {
            'method': 'GP_Symbolic',
            'avg_spread': avg_spread,
            'sharpe_ratio': sharpe,
            'n_years': len(portfolio_df_decile)
        }
        
        pd.DataFrame([summary]).to_csv(output_dir / 'gp_performance_summary.csv', index=False)

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
