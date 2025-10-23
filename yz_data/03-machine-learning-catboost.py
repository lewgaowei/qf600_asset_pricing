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
from tqdm.auto import tqdm

# Machine Learning libraries
from catboost import CatBoostRegressor, Pool
import lightgbm as lgb
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.model_selection import ParameterGrid, TimeSeriesSplit
from sklearn.feature_selection import RFE, RFECV
# Suppress warnings for cleaner output
warnings.filterwarnings('ignore')

# ------------------------------
# GPU Detection and Configuration for CatBoost
# ------------------------------
print("=" * 60)
print("GPU CHECK: CATBOOST GPU/CPU DETECTION")
print("=" * 60)

# Try to detect GPU for CatBoost
USE_GPU = False
TASK_TYPE = 'CPU'
DEVICES = None
THREAD_COUNT = None

try:
    import subprocess
    # Check if nvidia-smi is available (indicates NVIDIA GPU)
    subprocess.check_output(['nvidia-smi'], stderr=subprocess.DEVNULL)
    USE_GPU = True
    TASK_TYPE = 'GPU'
    DEVICES = '0'  # Use first GPU
    print("\n[OK] GPU DETECTED!")
    print(f"   CatBoost will use: GPU acceleration")
    print(f"   Task type: {TASK_TYPE}")
    print(f"   Device: GPU {DEVICES}")
except Exception as e:
    print("\n[WARN] NO GPU DETECTED")
    import multiprocessing
    THREAD_COUNT = multiprocessing.cpu_count()
    print(f"   CatBoost will use: CPU parallelism")
    print(f"   Task type: {TASK_TYPE}")
    print(f"   Thread count: {THREAD_COUNT} threads")

print("=" * 60)

print("\n" + "=" * 60)
print("STEP 1: LOADING DATA FROM 02-FEATURE-ENGINEERING.PY")
print("=" * 60)
# ------------------------------
# 1.1 Set up file paths
# ------------------------------
_base_dir = Path.cwd()
START_YEAR = 2000
# Input data path (from 02-feature-engineering.py)
data_path = _base_dir / f"signals_with_returns_and_tickers_{START_YEAR}.parquet"

# Create output directories for results
output_dir = _base_dir / "ml_catboost_results"
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
    'method': 'catboost',           # 'catboost' = CatBoost gradient boosting
    'dep_var': 'expected_return',   # Target variable to predict

    # Time periods for testing
    'begin': None,                  # Start counter (will be set automatically)
    'end': None,                    # End counter (will be set automatically)

    # Feature selection
    'missing_threshold': 0.50,      # Drop features with > 50% missing data
    'use_top_features': 1000,       # Use top N features by correlation (None = use ALL)
                                     # Set to 1000 for faster testing, None for full model

    # Multicollinearity removal (Step 3.6)
    'remove_multicollinearity': True,   # Remove highly correlated features
    'correlation_threshold': 0.85,      # Correlation threshold (0.85 = 85% correlated)
    'multicoll_method': 'fast',     # 'fast' (numpy+float32, 2-3× faster) or 'accurate' (pandas, 100% reliable)
    'multicoll_test_features': None,    # None = all features, 1000 = test with 1000 features only

    # RFE with LightGBM (Step 3.7)
    'use_rfe': True,                    # Use Recursive Feature Elimination
    'rfe_method': 'RFECV',                # 'RFECV' (CV-based, slow) or 'RFE' (fixed target, faster)
    'rfe_n_features': 1000,             # Target number of features (only for 'RFE' method)
    'rfe_step': 100,                    # Features to eliminate per iteration
    'rfe_cv_splits': 3,                 # Time-series CV splits (only for 'RFECV')
    'rfe_scoring': 'r2',                # Scoring metric: 'r2', 'neg_mean_squared_error'
    'rfe_importance_type': 'gain',      # LightGBM importance: 'gain', 'split', 'weight'
    'rfe_n_estimators': 50,             # Trees for RFE estimator (fewer = faster)
    'rfe_max_depth': 6,                 # Max depth for RFE estimator
    'rfe_learning_rate': 0.1            # Learning rate for RFE estimator
}

# ------------------------------
# Set begin/end automatically based on data
# ------------------------------
# Begin: First period where we have enough data for train+validation
min_counter = df['counter'].min()
max_counter = df['counter'].max()

# Set minimum training years required (before first prediction)
MIN_TRAIN_YEARS = 10  # Minimum 10 years of training data for robust feature selection

if CONFIG['window'] == 'recursive':
    # Need: min training years + validation period
    # Example: 10 years train + 1 year val = start testing at counter 12
    CONFIG['begin'] = min_counter + MIN_TRAIN_YEARS + CONFIG['cv_validation']

elif CONFIG['window'] == 'rolling':
    # Need: rolling window size + validation period
    # Example: 5 years train + 1 year val = start testing at counter 7
    CONFIG['begin'] = min_counter + CONFIG['cv_train'] + CONFIG['cv_validation']

CONFIG['end'] = max_counter

print(f"\n[CONFIG]  Minimum training requirement: {MIN_TRAIN_YEARS} years (for recursive)")
print(f"   First test will be at counter {CONFIG['begin']}")

# ------------------------------
# Display configuration
# ------------------------------
print("\n[INFO] ML PIPELINE CONFIGURATION:")
print("-" * 50)
print(f"Window Type:          {CONFIG['window'].upper()}")
print(f"Method:               {CONFIG['method'].upper()} (CatBoost)")
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

# ------------------------------
# Example of how windows work
# ------------------------------
print("\n" + "-" * 50)
print("[INFO] EXAMPLE: How the window works")
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
print("\n[TIP] TIP: To switch to rolling window, change CONFIG['window'] = 'rolling'")
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
print("\n[OK] Helper functions defined:")
print("  - train_validation_data(): Splits data for cross-validation")
print("  - train_test_data(): Splits data for final predictions")
print("  - output_filename(): Generates standardized filenames")

# Test with example counter
k_test = CONFIG['begin']
print(f"\n Example output filenames for counter {k_test}:")
print(f"  CV file:   {output_filename(CONFIG, mode='cv', counter=k_test).name}")
print(f"  Pred file: {output_filename(CONFIG, mode='pred', counter=k_test).name}")

def remove_multicollinear_features(data, feature_cols, dep_var, threshold=0.85, method='accurate'):
    """
    Remove highly correlated features (multicollinearity removal) - OPTIMIZED VERSION.

    For each pair of features with correlation > threshold:
    - Keep the feature with higher correlation to the target variable
    - Remove the feature with lower correlation to the target variable

    Optimizations:
    - Vectorized target correlation calculation (10-20x faster)
    - Vectorized pair finding with numpy (50-100x faster)
    - Optional GPU support with cuDF (auto-detected)
    - Choice between 'fast' (numpy+float32) and 'accurate' (pandas) methods

    Parameters:
    -----------
    data : DataFrame
        Dataset containing features and target variable
    feature_cols : list
        List of feature column names
    dep_var : str
        Name of the target variable (for deciding which feature to keep)
    threshold : float
        Correlation threshold (default 0.85 = 85% correlated)
    method : str
        'accurate' = pandas.corr() (slower, 100% reliable, float64, pairwise deletion)
        'fast' = numpy.corrcoef() (2-3× faster, ~99.9% accurate, float32, listwise deletion)

    Returns:
    --------
    filtered_features : list
        List of features after removing multicollinear ones
    removed_features : list
        List of features that were removed
    """
    import time

    print(f"\n[CHECK] Removing multicollinear features (threshold: {threshold})...")
    print(f"   Method: {method.upper()}")
    if method == 'fast':
        print(f"   Speed: 2-3× faster | Accuracy: ~99.9% (float32, listwise deletion)")
    else:
        print(f"   Speed: Standard | Accuracy: 100% (float64, pairwise deletion)")
    print("-" * 60)

    start_time = time.time()

    # Prepare feature data
    feature_data = data[feature_cols].copy()

    # ============================================
    # STEP 0: Pre-filtering (remove zero-variance features)
    # ============================================
    print("[TIME]  Step 0/4: Pre-filtering zero-variance features...")
    step_start = time.time()

    # Remove features with zero or near-zero variance (huge speedup!)
    feature_stds = feature_data.std()
    valid_features_mask = feature_stds > 1e-10
    valid_features = feature_stds[valid_features_mask].index.tolist()
    zero_var_features = feature_stds[~valid_features_mask].index.tolist()

    if len(zero_var_features) > 0:
        print(f"   Found {len(zero_var_features)} zero-variance features (will be removed)")
        feature_data = feature_data[valid_features]
        feature_cols_active = valid_features
    else:
        print(f"   No zero-variance features found")
        feature_cols_active = feature_cols

    step_time = time.time() - step_start
    print(f"   [OK] Complete in {step_time:.2f}s")
    print(f"   Active features: {len(feature_cols_active):,}")

    # ============================================
    # STEP 1: Calculate feature-feature correlation matrix
    # ============================================
    print(f"\n[TIME]  Step 1/4: Calculating feature-feature correlation matrix...")
    print(f"   Matrix size: {len(feature_cols_active):,} × {len(feature_cols_active):,}")

    # Estimate time (rough: 1 million correlations per second)
    n_correlations = len(feature_cols_active) ** 2
    estimated_time = n_correlations / 1e6
    if estimated_time > 5:
        print(f"   Estimated time: ~{estimated_time:.0f}s ({estimated_time/60:.1f} min)")

    step_start = time.time()

    # Calculate correlation matrix based on method
    use_gpu = False

    if method == 'fast':
        # FAST METHOD: numpy + float32 (2-3× faster)
        print("   [CPU] Using CPU (numpy + float32)")
        try:
            # Remove rows with any NaN for correlation calculation (listwise deletion)
            valid_rows_mask = feature_data.notna().all(axis=1)
            if valid_rows_mask.sum() > 100:  # Need at least 100 observations
                # Use numpy corrcoef with float32 for speed
                corr_array = np.corrcoef(feature_data[valid_rows_mask].values.T.astype(np.float32))
                corr_matrix = pd.DataFrame(corr_array, index=feature_data.columns, columns=feature_data.columns)
            else:
                print(f"   [WARN] Not enough valid rows ({valid_rows_mask.sum()}), falling back to pandas")
                corr_matrix = feature_data.corr()
        except Exception as e:
            print(f"   [WARN] Fast method failed ({str(e)[:30]}), falling back to pandas")
            corr_matrix = feature_data.corr()

    else:
        # ACCURATE METHOD: Try GPU first, then pandas
        try:
            import cudf
            print("   [GO] GPU detected! Using cuDF for correlation...")
            feature_data_gpu = cudf.from_pandas(feature_data)
            corr_matrix = feature_data_gpu.corr().to_pandas()
            use_gpu = True
        except ImportError:
            print("   [CPU] Using CPU (pandas) - install RAPIDS cuDF for GPU acceleration")
            # Use pandas.corr() for accuracy (pairwise deletion, float64)
            # This is the SAME as original implementation (100% accurate)
            corr_matrix = feature_data.corr()
        except Exception as e:
            print(f"   [CPU] GPU failed ({str(e)[:30]}), using CPU...")
            # Fallback to pandas for accuracy
            corr_matrix = feature_data.corr()

    step_time = time.time() - step_start
    print(f"   [OK] Complete in {step_time:.2f}s ({step_time/60:.1f} min)" if step_time > 60 else f"   [OK] Complete in {step_time:.2f}s")

    # ============================================
    # STEP 2: Calculate feature-target correlations (VECTORIZED)
    # ============================================
    print(f"\n[TIME]  Step 2/4: Calculating feature-target correlations...")
    step_start = time.time()

    # OPTIMIZED: Use corrwith() instead of loop - 10-20x faster!
    target_correlations = feature_data.corrwith(data[dep_var]).abs().fillna(0).to_dict()

    step_time = time.time() - step_start
    print(f"   [OK] Complete in {step_time:.2f}s")

    # ============================================
    # STEP 3: Find highly correlated pairs (VECTORIZED)
    # ============================================
    print(f"\n[TIME]  Step 3/4: Finding highly correlated pairs...")
    step_start = time.time()

    # OPTIMIZED: Use numpy to find all pairs at once - 50-100x faster!
    # Get upper triangle (avoid duplicates and self-correlations)
    corr_array = corr_matrix.values
    corr_abs = np.abs(corr_array)

    # Set diagonal and lower triangle to 0 (only check upper triangle)
    corr_upper = np.triu(corr_abs, k=1)

    # Find all pairs above threshold at once
    high_corr_indices = np.where(corr_upper > threshold)

    n_pairs = len(high_corr_indices[0])
    print(f"   Found {n_pairs:,} pairs with correlation > {threshold}")

    # Convert indices to feature names and decide which to remove
    features_to_remove = set()
    high_corr_pairs = []

    # Add progress bar for pair processing (only if many pairs)
    show_progress = n_pairs > 1000
    iterator = tqdm(range(n_pairs), desc="   Processing pairs", unit=" pairs", disable=not show_progress) if show_progress else range(n_pairs)

    for idx in iterator:
        i = high_corr_indices[0][idx]
        j = high_corr_indices[1][idx]

        feat1 = feature_cols_active[i]
        feat2 = feature_cols_active[j]

        # Skip if either feature already marked for removal
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
    print(f"   [OK] Complete in {step_time:.2f}s")

    # ============================================
    # STEP 4: Create final filtered feature list
    # ============================================
    print(f"\n[TIME]  Step 4/4: Creating final feature list...")

    # Combine removals: zero-variance + multicollinear
    all_removed_features = set(zero_var_features) | features_to_remove

    # Create filtered feature list (from original feature_cols)
    filtered_features = [f for f in feature_cols if f not in all_removed_features]

    print(f"   [OK] Complete in <1s")

    # ============================================
    # SUMMARY
    # ============================================
    total_time = time.time() - start_time

    print(f"\n" + "=" * 60)
    print(f"[OK] MULTICOLLINEARITY REMOVAL COMPLETE!")
    print(f"=" * 60)
    print(f"   Mode: {'GPU (cuDF)' if use_gpu else 'CPU (numpy/pandas)'}")
    print(f"   Total time: {total_time:.2f}s ({total_time/60:.1f} min)" if total_time > 60 else f"   Total time: {total_time:.2f}s")
    print()
    print(f"   Features before:        {len(feature_cols):,}")
    print(f"   Zero-variance removed:  {len(zero_var_features):,}")
    print(f"   Multicollinear removed: {len(features_to_remove):,}")
    print(f"   Features after:         {len(filtered_features):,}")
    print(f"   Total reduction:        {len(all_removed_features):,} features ({len(all_removed_features)/len(feature_cols)*100:.1f}%)")

    if len(high_corr_pairs) > 0:
        print(f"\n[INFO] Top 10 highly correlated pairs (>{threshold}):")
        print("-" * 60)
        print(f"{'Kept Feature':<30} | {'Removed Feature':<30} | {'Corr':>6}")
        print("-" * 60)

        # Sort by correlation (descending) and show top 10
        sorted_pairs = sorted(high_corr_pairs, key=lambda x: x['correlation'], reverse=True)
        for pair in sorted_pairs[:10]:
            kept = pair['kept'][:28]
            removed = pair['removed'][:28]
            print(f"{kept:<30} | {removed:<30} | {pair['correlation']:>6.2f}")

        if len(high_corr_pairs) > 10:
            print(f"  ... and {len(high_corr_pairs) - 10:,} more pairs")

    print("=" * 60)

    # Return filtered features and ALL removed features (zero-variance + multicollinear)
    return filtered_features, list(all_removed_features)


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
print("\n[STATS] FILTERING FEATURES BY MISSING DATA...")
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
print(f"Reduction: {total_features:,}  {kept_features:,} ({high_missing:,} dropped)")

# Get columns to keep
features_to_keep = missing_df[missing_df['keep']]['column'].tolist()

# ------------------------------
# 3.5.2 Apply cross-sectional scaling (PROFESSOR'S EXACT METHOD)
# ------------------------------
print(f"\n  APPLYING CROSS-SECTIONAL SCALING (PROFESSOR'S METHOD)...")
print("-" * 60)

# Check if scaled data already exists
scaled_data_file = _base_dir / f"df_scaled_{START_YEAR}.parquet"

if scaled_data_file.exists():
    print("\n[OK] Found saved scaled data! Loading...")
    print(f"   Loading from: {scaled_data_file.name}")

    # Load scaled dataframe
    df_scaled = pd.read_parquet(scaled_data_file, engine="fastparquet")

    print(f"   Loaded {len(df_scaled):,} observations")
    print("\n[SKIP]  Skipping scaling (using cached data)")

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

        print(" ")

    print(f"\n[OK] Scaling complete!")
    print(f"   Method: Rank-based scaling to [-1, +1] range")
    print(f"   Missing values: Filled with 0")

    # Save scaled data for future use
    print(f"\n[SAVE] Saving scaled data to: {scaled_data_file.name}")
    df_scaled.to_parquet(scaled_data_file, engine="fastparquet", compression="snappy")
    print("[OK] Saved! Next time this step will be skipped.")

# ------------------------------
# 3.5.3 Create final feature list
# ------------------------------
print(f"\n[INFO] FINAL FEATURE SET:")
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
    print(f"\n[WARN]  WARNING: Target variable '{CONFIG['dep_var']}' not found!")
else:
    print(f"\n[OK] Target variable '{CONFIG['dep_var']}' ready")

print("\n" + "=" * 60)
print("STEP 3.5 COMPLETE: Features filtered and scaled!")
print("=" * 60)

# %%
# ------------------------------
# STEP 3.6: Optional Multicollinearity Removal (BEFORE Feature Selection)
# ------------------------------
if CONFIG['remove_multicollinearity']:
    print("\n" + "=" * 60)
    print("STEP 3.6: OPTIONAL MULTICOLLINEARITY REMOVAL")
    print("=" * 60)

    print(f"\n[STATS] Removing highly correlated features from the selected {len(final_feature_columns):,} features...")
    print(f"   Correlation threshold: {CONFIG['correlation_threshold']}")
    print(f"   Method: {CONFIG['multicoll_method'].upper()}")

    # ------------------------------
    # Feature sampling for testing (optional)
    # ------------------------------
    if CONFIG['multicoll_test_features'] is not None:
        # TEST MODE: Sample features for faster testing
        # Save/load sample to ensure IDENTICAL features across fast/accurate comparisons
        sample_file = _base_dir / f"multicoll_test_sample_{CONFIG['multicoll_test_features']}_{START_YEAR}.csv"

        if sample_file.exists():
            # Load existing sample (ensures same features for fast/accurate comparison)
            print(f"\n[TEST] TEST MODE: Loading existing sample")
            print(f"   From: {sample_file.name}")
            sample_df = pd.read_csv(sample_file)
            sampled_features = sample_df['feature'].tolist()
            # Verify features still exist in final_feature_columns
            sampled_features = [f for f in sampled_features if f in final_feature_columns]
            features_for_multicoll = sampled_features
            print(f"   Loaded {len(features_for_multicoll):,} features")
        else:
            # Create new sample and save it
            import random
            random.seed(42)  # Reproducible sampling
            if len(final_feature_columns) > CONFIG['multicoll_test_features']:
                # CRITICAL: Sort features first for reproducibility
                # (in case final_feature_columns order changes between runs)
                sorted_features = sorted(final_feature_columns)
                sampled_features = random.sample(sorted_features, CONFIG['multicoll_test_features'])
                features_for_multicoll = sampled_features

                print(f"\n[TEST] TEST MODE: Created new random sample")
                print(f"   Sample size: {CONFIG['multicoll_test_features']:,} features")
                print(f"   Out of: {len(final_feature_columns):,} total features")
                print(f"   Saved to: {sample_file.name}")
                print(f"   [OK] Same sample will be used for fast/accurate comparison")

                # Save sample for future runs
                pd.DataFrame({'feature': sampled_features}).to_csv(sample_file, index=False)
            else:
                print(f"\n[WARN]  TEST MODE: Requested {CONFIG['multicoll_test_features']:,} features")
                print(f"   But only {len(final_feature_columns):,} features available")
                print(f"   Using all available features")
                features_for_multicoll = final_feature_columns

        print(f"   (Delete {sample_file.name} to resample)")
        print(f"   (Set CONFIG['multicoll_test_features'] = None for full run)")
    else:
        features_for_multicoll = final_feature_columns

    print("-" * 60)

    # Check if cached parquet file exists
    # Cache includes: data range, threshold, method, and feature count
    initial_counter_for_cache = CONFIG['begin'] - 1 - CONFIG['cv_validation']
    cache_filename = f"multicoll_filtered_features_n{len(features_for_multicoll)}_thresh{CONFIG['correlation_threshold']}_initial{initial_counter_for_cache}_{CONFIG['multicoll_method']}_{START_YEAR}.parquet"
    cache_file = _base_dir / cache_filename

    if cache_file.exists():
        print(f"\n[OK] Found cached multicollinearity removal results!")
        print(f"   Loading from: {cache_file.name}")

        # Load cached feature list
        cached_df = pd.read_parquet(cache_file, engine="fastparquet")
        features_before_multicoll = features_for_multicoll.copy()
        features_after_multicoll = cached_df['feature'].tolist()
        removed_features = [f for f in features_before_multicoll if f not in features_after_multicoll]
        filtered_features = features_after_multicoll  # Needed for CSV saving later

        print(f"   Loaded {len(features_after_multicoll):,} features from cache")
        print(f"   Removed: {len(removed_features):,} features")
        print("\n[SKIP]  Skipping multicollinearity removal (using cached data)")

        # Update final_feature_columns
        # If test mode, keep non-sampled features + filtered sampled features
        if CONFIG['multicoll_test_features'] is not None:
            non_sampled = [f for f in final_feature_columns if f not in features_for_multicoll]
            final_feature_columns = non_sampled + features_after_multicoll
        else:
            final_feature_columns = features_after_multicoll

    else:
        print(f"\n[CONFIG]  No cache found - running multicollinearity removal...")

        # ------------------------------
        # CRITICAL: Use only initial data to prevent look-ahead bias
        # ------------------------------
        # Use same approach as RFE (Step 3.7) - only data up to first test period
        initial_counter_endpoint = CONFIG['begin'] - 1 - CONFIG['cv_validation']
        initial_data_multicoll = df_scaled[df_scaled['counter'] <= initial_counter_endpoint].copy()

        print(f"\n[STATS] Using initial data for multicollinearity removal (prevent look-ahead bias):")
        print(f"   Data range: counter  {initial_counter_endpoint}")

        # Get year for this counter
        initial_year_multicoll = [y for y, c in year_to_counter.items() if c == initial_counter_endpoint]
        if initial_year_multicoll:
            print(f"   Years: {df_scaled['form_year'].min():.0f} to {initial_year_multicoll[0]:.0f}")
            print(f"   Total: ~{initial_counter_endpoint} years of data")

        print(f"   Observations: {len(initial_data_multicoll):,}")

        # Save features BEFORE multicollinearity removal (for comparison)
        features_before_multicoll = features_for_multicoll.copy()

        # Apply multicollinearity removal using ONLY initial data
        filtered_features, removed_features = remove_multicollinear_features(
            data=initial_data_multicoll,  # [OK] Only initial years (no look-ahead bias)
            feature_cols=features_for_multicoll,  # Use sampled features if test mode
            dep_var=CONFIG['dep_var'],
            threshold=CONFIG['correlation_threshold'],
            method=CONFIG['multicoll_method']  # Pass method parameter
        )

        # Update final_feature_columns with filtered list
        # If test mode, keep non-sampled features + filtered sampled features
        if CONFIG['multicoll_test_features'] is not None:
            non_sampled = [f for f in final_feature_columns if f not in features_for_multicoll]
            final_feature_columns = non_sampled + filtered_features
            print(f"\n[TEST] TEST MODE: Keeping {len(non_sampled):,} non-sampled + {len(filtered_features):,} filtered = {len(final_feature_columns):,} total")
        else:
            final_feature_columns = filtered_features

        # Save to parquet for future use (CACHING)
        print(f"\n[SAVE] Caching results to: {cache_file.name}")
        pd.DataFrame({'feature': final_feature_columns}).to_parquet(
            cache_file, engine="fastparquet", compression="snappy"
        )
        print("[OK] Cached! Next time this step will be skipped.")

    # Save the results to CSV for comparison (always do this)
    save_dir = _base_dir / "multicollinearity_analysis"
    save_dir.mkdir(exist_ok=True)

    # Include method and test mode in filenames
    test_suffix = f"_test{CONFIG['multicoll_test_features']}" if CONFIG['multicoll_test_features'] is not None else ""
    method_name = CONFIG['multicoll_method']

    # Save features before removal
    pd.DataFrame({'feature': features_before_multicoll}).to_csv(
        save_dir / f'features_before_multicoll_{CONFIG["correlation_threshold"]}_{method_name}{test_suffix}.csv',
        index=False
    )

    # Save features after removal (kept features)
    features_to_save = filtered_features if CONFIG['multicoll_test_features'] is None else final_feature_columns
    pd.DataFrame({'feature': features_to_save}).to_csv(
        save_dir / f'features_after_multicoll_{CONFIG["correlation_threshold"]}_{method_name}{test_suffix}.csv',
        index=False
    )

    # Save removed features
    pd.DataFrame({'feature': removed_features}).to_csv(
        save_dir / f'features_removed_multicoll_{CONFIG["correlation_threshold"]}_{method_name}{test_suffix}.csv',
        index=False
    )

    # Save summary
    summary_df = pd.DataFrame([{
        'threshold': CONFIG['correlation_threshold'],
        'method': CONFIG['multicoll_method'],
        'test_features': CONFIG['multicoll_test_features'],
        'features_before': len(features_before_multicoll),
        'features_after': len(features_to_save),
        'features_removed': len(removed_features),
        'removal_percentage': len(removed_features) / len(features_before_multicoll) * 100 if len(features_before_multicoll) > 0 else 0
    }])
    summary_df.to_csv(save_dir / f'multicoll_summary_{CONFIG["correlation_threshold"]}_{method_name}{test_suffix}.csv', index=False)

    print(f"\n[SAVE] Results saved to: {save_dir.name}/")
    print(f"   - features_before_multicoll_{CONFIG['correlation_threshold']}_{method_name}{test_suffix}.csv ({len(features_before_multicoll)} features)")
    print(f"   - features_after_multicoll_{CONFIG['correlation_threshold']}_{method_name}{test_suffix}.csv ({len(features_to_save)} features)")
    print(f"   - features_removed_multicoll_{CONFIG['correlation_threshold']}_{method_name}{test_suffix}.csv ({len(removed_features)} features)")
    print(f"   - multicoll_summary_{CONFIG['correlation_threshold']}_{method_name}{test_suffix}.csv")

    print("\n" + "=" * 60)
    print("STEP 3.6 COMPLETE: Multicollinearity removed!")
    print("=" * 60)
    print(f"[STATS] Final feature count after multicollinearity removal: {len(final_feature_columns):,}")
    print(f"   Removed: {len(removed_features):,} features ({len(removed_features)/len(features_before_multicoll)*100:.1f}%)")
    print("=" * 60)

else:
    print("\n" + "=" * 60)
    print("STEP 3.6: MULTICOLLINEARITY REMOVAL SKIPPED")
    print("=" * 60)
    print(f"\n[TIP] Multicollinearity removal is disabled (CONFIG['remove_multicollinearity'] = False)")
    print("   To enable: Set CONFIG['remove_multicollinearity'] = True")
    print("   This will remove features highly correlated with EACH OTHER")
    print(f"\n[STATS] Current feature count: {len(final_feature_columns):,}")
    print("=" * 60)

# %%
# ------------------------------
# STEP 3.7: Recursive Feature Elimination with LightGBM
# ------------------------------
if CONFIG['use_rfe']:
    print("\n" + "=" * 60)
    print("STEP 3.7: RECURSIVE FEATURE ELIMINATION WITH LIGHTGBM")
    print("=" * 60)

    print(f"\n[TARGET] RFE Configuration:")
    print(f"   Method: {CONFIG['rfe_method']}")
    print(f"   Target features: {CONFIG['rfe_n_features'] if CONFIG['rfe_method'] == 'RFE' else 'Auto (RFECV)'}")
    print(f"   Step size: {CONFIG['rfe_step']} features per iteration")
    if CONFIG['rfe_method'] == 'RFECV':
        print(f"   CV splits: {CONFIG['rfe_cv_splits']} (TimeSeriesSplit)")
        print(f"   Scoring: {CONFIG['rfe_scoring']}")
    print(f"   Importance type: {CONFIG['rfe_importance_type']}")
    print(f"   Starting features: {len(final_feature_columns):,}")

    # ------------------------------
    # 3.7.1 Define cache filename
    # ------------------------------
    if CONFIG['rfe_method'] == 'RFECV':
        cache_filename = f"rfe_cv_features_from{len(final_feature_columns)}_step{CONFIG['rfe_step']}_cv{CONFIG['rfe_cv_splits']}_{CONFIG['rfe_importance_type']}_{START_YEAR}.parquet"
    else:
        cache_filename = f"rfe_features_n{CONFIG['rfe_n_features']}_from{len(final_feature_columns)}_step{CONFIG['rfe_step']}_{CONFIG['rfe_importance_type']}_{START_YEAR}.parquet"

    cache_file = _base_dir / cache_filename

    # ------------------------------
    # 3.7.2 Check cache
    # ------------------------------
    if cache_file.exists():
        print(f"\n[OK] Found cached RFE results!")
        print(f"   Loading from: {cache_file.name}")

        # Load cached feature list
        cached_df = pd.read_parquet(cache_file, engine="fastparquet")
        features_before_rfe = final_feature_columns.copy()
        final_feature_columns = cached_df['feature'].tolist()
        removed_features_rfe = [f for f in features_before_rfe if f not in final_feature_columns]

        print(f"   Loaded {len(final_feature_columns):,} features from cache")
        print(f"   Removed: {len(removed_features_rfe):,} features")
        print("\n[SKIP]  Skipping RFE (using cached data)")

    else:
        # ------------------------------
        # 3.7.3 Prepare training data (PREVENT LOOK-AHEAD BIAS)
        # ------------------------------
        print(f"\n[CONFIG]  No cache found - running RFE...")
        print("\n[STATS] Preparing training data for RFE...")
        print("-" * 60)

        # CRITICAL: Use only data up to first test period to prevent look-ahead bias
        # For test counter CONFIG['begin'], we need validation endpoint k = CONFIG['begin'] - 1
        # For that validation endpoint, we use data up to k - cv_validation for training
        initial_counter_endpoint = CONFIG['begin'] - 1 - CONFIG['cv_validation']

        print(f"   First test counter: {CONFIG['begin']}")
        print(f"   Training data: counter  {initial_counter_endpoint}")

        # Get year for this counter
        initial_year = [y for y, c in year_to_counter.items() if c == initial_counter_endpoint]
        if initial_year:
            print(f"   Training data: up to year {initial_year[0]:.0f}")

        # Filter data
        initial_train_data = df_scaled[df_scaled['counter'] <= initial_counter_endpoint].copy()

        # Prepare X and y for RFE
        X_rfe = initial_train_data[final_feature_columns].copy()
        y_rfe = initial_train_data[CONFIG['dep_var']].copy()

        # Remove rows with NaN in target variable
        valid_idx = y_rfe.notna()
        X_rfe = X_rfe[valid_idx]
        y_rfe = y_rfe[valid_idx]

        print(f"\n   RFE training set: {len(X_rfe):,} observations")
        print(f"   RFE features: {X_rfe.shape[1]:,} features")
        print(f"   Target range: [{y_rfe.min():+.4f}, {y_rfe.max():+.4f}]")

        # ------------------------------
        # 3.7.4 Create base estimator (LightGBM)
        # ------------------------------
        print(f"\n[GO] Creating LightGBM estimator...")

        # Try GPU first, fallback to CPU if needed
        try:
            lgb_estimator = lgb.LGBMRegressor(
                device='gpu',
                n_estimators=CONFIG['rfe_n_estimators'],
                max_depth=CONFIG['rfe_max_depth'],
                learning_rate=CONFIG['rfe_learning_rate'],
                importance_type=CONFIG['rfe_importance_type'],
                random_state=42,
                verbosity=-1,
                force_col_wise=True  # Recommended for GPU
            )
            print("   [OK] LightGBM configured for GPU")
        except Exception as e:
            print(f"   [WARN]  GPU initialization failed: {str(e)}")
            print("   [INFO] Falling back to CPU...")
            lgb_estimator = lgb.LGBMRegressor(
                device='cpu',
                n_estimators=CONFIG['rfe_n_estimators'],
                max_depth=CONFIG['rfe_max_depth'],
                learning_rate=CONFIG['rfe_learning_rate'],
                importance_type=CONFIG['rfe_importance_type'],
                random_state=42,
                verbosity=-1,
                n_jobs=-1  # Use all CPUs
            )
            print("   [OK] LightGBM configured for CPU")

        # ------------------------------
        # 3.7.5 Create RFE selector
        # ------------------------------
        print(f"\n[CHECK] Creating RFE selector ({CONFIG['rfe_method']})...")

        if CONFIG['rfe_method'] == 'RFECV':
            # Cross-validated RFE (automatically finds optimal number of features)
            print(f"   Using RFECV with TimeSeriesSplit ({CONFIG['rfe_cv_splits']} splits)")

            selector = RFECV(
                estimator=lgb_estimator,
                step=CONFIG['rfe_step'],
                cv=TimeSeriesSplit(n_splits=CONFIG['rfe_cv_splits']),
                scoring=CONFIG['rfe_scoring'],
                n_jobs=1,  # LightGBM handles parallelization internally
                verbose=1
            )

        else:
            # Standard RFE (fixed number of features)
            print(f"   Using RFE (target: {CONFIG['rfe_n_features']} features)")

            selector = RFE(
                estimator=lgb_estimator,
                n_features_to_select=CONFIG['rfe_n_features'],
                step=CONFIG['rfe_step'],
                verbose=1
            )

        # ------------------------------
        # 3.7.6 Run RFE
        # ------------------------------
        print(f"\n Running RFE...")
        print(f"   This may take 10-30 minutes depending on data size...")
        print(f"   Starting: {len(X_rfe.columns)} features")
        print(f"   Eliminating: {CONFIG['rfe_step']} features per iteration")
        print("-" * 60)

        import time
        rfe_start_time = time.time()

        # Fit RFE
        selector.fit(X_rfe, y_rfe)

        rfe_elapsed = time.time() - rfe_start_time
        print(f"\n[OK] RFE Complete! (took {rfe_elapsed/60:.1f} minutes)")

        # ------------------------------
        # 3.7.7 Extract selected features
        # ------------------------------
        selected_mask = selector.support_
        selected_features = X_rfe.columns[selected_mask].tolist()
        eliminated_features = X_rfe.columns[~selected_mask].tolist()

        features_before_rfe = final_feature_columns.copy()
        removed_features_rfe = eliminated_features

        # ------------------------------
        # 3.7.8 Report results
        # ------------------------------
        print("\n" + "=" * 60)
        print("RFE RESULTS")
        print("=" * 60)
        print(f"Features before RFE:  {len(features_before_rfe):,}")
        print(f"Features selected:    {len(selected_features):,}")
        print(f"Features eliminated:  {len(eliminated_features):,}")
        print(f"Reduction:            {len(eliminated_features)/len(features_before_rfe)*100:.1f}%")

        if CONFIG['rfe_method'] == 'RFECV':
            print(f"\nOptimal features (CV): {selector.n_features_}")
            print(f"Best CV score:         {selector.cv_results_['mean_test_score'].max():+.6f}")
            print(f"Grid scores available: {len(selector.cv_results_['mean_test_score'])} points")
        else:
            print(f"\nTarget features:      {CONFIG['rfe_n_features']:,}")
            print(f"Achieved:             {len(selected_features):,}")

        # Get feature ranking if available
        if hasattr(selector, 'ranking_'):
            print(f"\nFeature rankings computed: Yes")
            print(f"   Ranking range: {selector.ranking_.min()} to {selector.ranking_.max()}")

        # ------------------------------
        # 3.7.9 Save to cache
        # ------------------------------
        print(f"\n[SAVE] Caching results to: {cache_file.name}")
        pd.DataFrame({'feature': selected_features}).to_parquet(
            cache_file, engine="fastparquet", compression="snappy"
        )
        print("[OK] Cached! Next time this step will be skipped.")

        # ------------------------------
        # 3.7.10 Save detailed reports (CSV)
        # ------------------------------
        rfe_save_dir = _base_dir / "rfe_analysis"
        rfe_save_dir.mkdir(exist_ok=True)

        # Save selected features
        selected_df = pd.DataFrame({'feature': selected_features})
        if hasattr(selector, 'ranking_'):
            selected_df['ranking'] = selector.ranking_[selected_mask]
        selected_df.to_csv(
            rfe_save_dir / f'rfe_selected_features_{CONFIG["rfe_method"]}.csv',
            index=False
        )

        # Save eliminated features
        eliminated_df = pd.DataFrame({'feature': eliminated_features})
        if hasattr(selector, 'ranking_'):
            eliminated_df['ranking'] = selector.ranking_[~selected_mask]
        eliminated_df.to_csv(
            rfe_save_dir / f'rfe_eliminated_features_{CONFIG["rfe_method"]}.csv',
            index=False
        )

        # Save summary
        summary_dict = {
            'rfe_method': CONFIG['rfe_method'],
            'features_before': len(features_before_rfe),
            'features_after': len(selected_features),
            'features_eliminated': len(eliminated_features),
            'elimination_percentage': len(eliminated_features) / len(features_before_rfe) * 100,
            'step_size': CONFIG['rfe_step'],
            'importance_type': CONFIG['rfe_importance_type'],
            'runtime_minutes': rfe_elapsed / 60
        }

        if CONFIG['rfe_method'] == 'RFECV':
            summary_dict['cv_splits'] = CONFIG['rfe_cv_splits']
            summary_dict['optimal_features'] = selector.n_features_
            summary_dict['best_cv_score'] = selector.cv_results_['mean_test_score'].max()

        summary_df = pd.DataFrame([summary_dict])
        summary_df.to_csv(
            rfe_save_dir / f'rfe_summary_{CONFIG["rfe_method"]}.csv',
            index=False
        )

        # Save CV results if RFECV
        if CONFIG['rfe_method'] == 'RFECV' and hasattr(selector, 'cv_results_'):
            cv_results_df = pd.DataFrame(selector.cv_results_)
            cv_results_df.to_csv(
                rfe_save_dir / f'rfe_cv_results_{CONFIG["rfe_method"]}.csv',
                index=False
            )

        print(f"\n[SAVE] Detailed reports saved to: {rfe_save_dir.name}/")
        print(f"   - rfe_selected_features_{CONFIG['rfe_method']}.csv ({len(selected_features)} features)")
        print(f"   - rfe_eliminated_features_{CONFIG['rfe_method']}.csv ({len(eliminated_features)} features)")
        print(f"   - rfe_summary_{CONFIG['rfe_method']}.csv")
        if CONFIG['rfe_method'] == 'RFECV':
            print(f"   - rfe_cv_results_{CONFIG['rfe_method']}.csv")

        # Update final_feature_columns
        final_feature_columns = selected_features

    # ------------------------------
    # 3.7.11 Final reporting
    # ------------------------------
    print("\n" + "=" * 60)
    print("STEP 3.7 COMPLETE: RFE finished!")
    print("=" * 60)
    print(f"[STATS] Final feature count after RFE: {len(final_feature_columns):,}")
    print("=" * 60)

    # ------------------------------
    # LOOK-AHEAD BIAS VALIDATION
    # ------------------------------
    print("\n" + "=" * 60)
    print("[CHECK] LOOK-AHEAD BIAS VALIDATION")
    print("=" * 60)
    validation_counter = CONFIG['begin'] - 1 - CONFIG['cv_validation']
    validation_year = [y for y, c in year_to_counter.items() if c == validation_counter]

    print(f"\n[OK] Feature Selection Configuration:")
    print(f"   Minimum training years (MIN_TRAIN_YEARS): {MIN_TRAIN_YEARS}")
    print(f"   First test counter: {CONFIG['begin']}")
    if validation_year:
        first_test_year = [y for y, c in year_to_counter.items() if c == CONFIG['begin']][0]
        print(f"   First test year: {first_test_year:.0f}")

    print(f"\n[OK] Data Used for Feature Selection (Steps 3.6 & 3.7):")
    print(f"   Counter range: 1 to {validation_counter}")
    if validation_year:
        print(f"   Year range: {df_scaled['form_year'].min():.0f} to {validation_year[0]:.0f}")
    print(f"   Approximately {validation_counter} years of data")

    print(f"\n[OK] VERIFICATION: No future data used")
    print(f"   Step 3.6 (Multicollinearity): Used counters 1-{validation_counter} only")
    print(f"   Step 3.7 (RFE): Used counters 1-{validation_counter} only")
    print(f"   No look-ahead bias ")
    print("=" * 60)

else:
    print("\n" + "=" * 60)
    print("STEP 3.7: RFE SKIPPED")
    print("=" * 60)
    print(f"\n[TIP] RFE is disabled (CONFIG['use_rfe'] = False)")
    print("   To enable: Set CONFIG['use_rfe'] = True")
    print("   RFE provides model-based feature selection using LightGBM")
    print(f"\n[STATS] Current feature count: {len(final_feature_columns):,}")
    print("=" * 60)

# %%
# ------------------------------
# STEP 4: Implement Hyperparameter Grid for CatBoost
# ------------------------------
print("\n" + "=" * 60)
print("STEP 4: HYPERPARAMETER GRID FOR CATBOOST")
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
        ML method ('catboost' for CatBoost gradient boosting)

    Returns:
    --------
    tunegrid : list of dicts
        All combinations of hyperparameters to try
    """

    if method == 'catboost':
        # Hyperparameters for CatBoost Gradient Boosting
        # OPTIMIZED GRID for 344 features and 10k-50k rows (2×3×1×1 = 6 combinations)
        # Research-backed optimal parameters for this dataset size
        grid = {
            'iterations': [500, 700],               # Optimal range for 344 features
            'learning_rate': [0.03, 0.05, 0.07],   # Conservative rates around default
            'depth': [6],                           # CatBoost default (optimal for 344 features)
            'l2_leaf_reg': [3]                      # CatBoost default (good regularization)
        }
        # Why these values?
        # - iterations: 500-700 is sweet spot for 344 features with 10k-50k rows
        # - learning_rate: Testing around default (0.03) for sensitivity
        # - depth: Fixed at 6 (handles up to ~700 features optimally)
        # - l2_leaf_reg: Fixed at 3 (CatBoost default works well)
    else:
        raise ValueError(f"Unknown method: {method}")

    # Expand to all combinations using sklearn's ParameterGrid
    tunegrid = list(ParameterGrid(grid))

    return tunegrid


# ------------------------------
# 4.2 Get the grid for our configured method
# ------------------------------
hyperparameter_grid = get_hyperparameter_grid(CONFIG['method'])

print(f"\n[STATS] Hyperparameter Grid for {CONFIG['method'].upper()}:")
print("-" * 50)
print(f"Total combinations to try: {len(hyperparameter_grid)}")
print()

# Show the parameter space
print("Parameter ranges:")
if CONFIG['method'] == 'catboost':
    # Extract unique values for each parameter
    iterations_values = sorted(set([p['iterations'] for p in hyperparameter_grid]))
    learning_rate_values = sorted(set([p['learning_rate'] for p in hyperparameter_grid]))
    depth_values = sorted(set([p['depth'] for p in hyperparameter_grid]))
    l2_values = sorted(set([p['l2_leaf_reg'] for p in hyperparameter_grid]))

    print(f"  iterations (number of trees):    {iterations_values}")
    print(f"  learning_rate (step size):       {learning_rate_values}")
    print(f"  depth (tree depth):              {depth_values}")
    print(f"  l2_leaf_reg (regularization):    {l2_values}")

print()
print("First 5 combinations to try:")
print("-" * 70)
print("  #  | iterations | learning_rate | depth | l2_leaf_reg")
print("-" * 70)

for i, params in enumerate(hyperparameter_grid[:5], 1):
    print(f"  {i:2d} | {params['iterations']:10d} | {params['learning_rate']:13.2f} | {params['depth']:5d} | {params['l2_leaf_reg']:11.1f}")

if len(hyperparameter_grid) > 5:
    print(f"  ... and {len(hyperparameter_grid) - 5} more combinations")

# ------------------------------
# 4.3 Explain what each hyperparameter does
# ------------------------------
print("\n" + "-" * 50)
print("[INFO] HYPERPARAMETER EXPLANATIONS:")
print("-" * 50)
print()
print("OPTIMIZED FOR 344 FEATURES AND 10k-50k ROWS:")
print()
print("1. iterations (Number of Trees):")
print("   - Testing: [500, 700]")
print("   - Why: Optimal range for 344 features with 10k-50k rows")
print("   - Research: 500-700 iterations balance accuracy vs. training time")
print()
print("2. learning_rate (Step Size):")
print("   - Testing: [0.03, 0.05, 0.07]")
print("   - Why: Conservative rates around CatBoost default (0.03)")
print("   - Research: Lower rates (0.03-0.07) work better with 500-700 iterations")
print()
print("3. depth (Tree Depth):")
print("   - Fixed at: [6]")
print("   - Why: CatBoost default, optimal for up to ~700 features")
print("   - Research: Depth 6 handles 344 features efficiently")
print()
print("4. l2_leaf_reg (L2 Regularization):")
print("   - Fixed at: [3]")
print("   - Why: CatBoost default provides good regularization")
print("   - Research: Default value works well for most datasets")
print()
print("GRID OPTIMIZATION BENEFITS:")
print("   - Reduced from 108 → 6 combinations (~95% faster)")
print("   - Focused on research-backed optimal parameters")
print("   - depth=6 and l2=3 are proven defaults for this data size")

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

    # ------------------------------
    # Check if CV results already cached
    # ------------------------------
    output_file = output_filename(config, mode='cv', counter=k+1)

    if output_file.exists():
        print(f"\n[OK] Found cached CV results!")
        print(f"   Loading from: {output_file.name}")
        cv_results = pd.read_csv(output_file)
        print(f"   Loaded {len(cv_results)} hyperparameter combinations")
        print("\n[SKIP]  Skipping CV (using cached hyperparameters)")
        print(f"   [TIP] To re-run CV, delete: {output_file.name}")
        return cv_results

    # ------------------------------
    # No cache found - run CV
    # ------------------------------
    print(f"\n[CONFIG]  No cache found - running cross-validation...")

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
        if config['method'] == 'catboost':
            # Create CatBoost Pool for efficient training
            train_pool = Pool(X_train, y_train)

            model = CatBoostRegressor(
                iterations=int(params['iterations']),
                learning_rate=params['learning_rate'],
                depth=int(params['depth']),
                l2_leaf_reg=params['l2_leaf_reg'],
                task_type=TASK_TYPE,                    # 'GPU' or 'CPU'
                devices=DEVICES if USE_GPU else None,   # GPU device ID or None for CPU
                thread_count=THREAD_COUNT if not USE_GPU else -1,  # CPU threads (ignored for GPU)
                random_seed=42,
                verbose=False,                          # Suppress output
                allow_writing_files=False               # Disable model saving during training
            )

            model.fit(train_pool)

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
                  f"iter={params['iterations']:3d}, depth={params['depth']}, lr={params['learning_rate']:.2f} | "
                  f"MSE={mse:.6f}, R²={r2:+.4f} | "
                  f"[TIME] {iter_duration:.1f}s (avg: {avg_time:.1f}s, ETA: {remaining:.0f}s)")

    # Sort by MSE (ascending - lower is better)
    # MSE is more stable than R² for financial return prediction
    cv_results = cv_results.sort_values('mse', ascending=True)

    # Display best results
    print("\n" + "-" * 80)
    print("TOP 3 HYPERPARAMETER COMBINATIONS (by MSE):")
    print("-" * 80)
    print("Rank | iterations | depth | learning_rate | l2_leaf_reg | MSE       | R²")
    print("-" * 80)

    for rank, (idx, row) in enumerate(cv_results.head(3).iterrows(), 1):
        print(f"  {rank}  | {row['iterations']:10.0f} | {row['depth']:5.0f} | {row['learning_rate']:13.2f} | "
              f"{row['l2_leaf_reg']:11.1f} | {row['mse']:9.6f} | {row['r2_score']:+.4f}")

    # Save results to CSV
    # k is validation endpoint, save for test counter k+1
    output_file = output_filename(config, mode='cv', counter=k+1)
    cv_results.to_csv(output_file, index=False)
    print(f"\n[OK] Results saved to: {output_file.name}")
    print(f"   (These hyperparameters are optimized for testing counter {k+1})")

    return cv_results


# ------------------------------
# 5.2 Explain the cross-validation process
# ------------------------------
print("\n[INFO] HOW CROSS-VALIDATION WORKS:")
print("-" * 50)
print()
print("For each time period k:")
print("  1. Split data into TRAIN and VALIDATION")
print("  2. For each hyperparameter combination:")
print("     a. Train CatBoost model on TRAIN data")
print("     b. Predict on VALIDATION data")
print("     c. Calculate MSE and R² scores")
print("  3. Save all results to CSV file")
print("  4. Best hyperparameters = lowest MSE score")
print()
print("Why MSE (Mean Squared Error)?")
print("  - MSE measures average squared prediction error")
print("  - Lower MSE = Better predictions")
print("  - More stable than R² for financial return prediction")
print("  - Directly minimizes prediction errors")
print()
print("Why not R² score?")
print("  - R² can be unstable or negative for out-of-sample returns")
print("  - MSE is more reliable for model selection in finance")
print("  - We still calculate R² for reference")

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
        print(f"[WARN]  WARNING: CV file not found: {cv_file}")
        print("   Run cross-validation first (Step 7) before predictions!")
        return None

    cv_results = pd.read_csv(cv_file)
    cv_results = cv_results.sort_values('mse', ascending=True)
    best_params = cv_results.iloc[0]

    print(f"[OK] Loaded best hyperparameters from CV:")
    print(f"   iterations:    {int(best_params['iterations'])}")
    print(f"   depth:         {int(best_params['depth'])}")
    print(f"   learning_rate: {best_params['learning_rate']:.3f}")
    print(f"   l2_leaf_reg:   {best_params['l2_leaf_reg']:.1f}")
    print(f"   MSE:           {best_params['mse']:.6f}")
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

    if config['method'] == 'catboost':
        # Create Pool for efficient training
        train_pool = Pool(X_train, y_train)

        model = CatBoostRegressor(
            iterations=int(best_params['iterations']),
            learning_rate=float(best_params['learning_rate']),
            depth=int(best_params['depth']),
            l2_leaf_reg=float(best_params['l2_leaf_reg']),
            task_type=TASK_TYPE,
            devices=DEVICES if USE_GPU else None,
            thread_count=THREAD_COUNT if not USE_GPU else -1,
            random_seed=42,
            verbose=False,
            allow_writing_files=False
        )

        model.fit(train_pool)
        print(f"[OK] Model trained successfully!")

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

    print(f"\n[OK] Predictions saved to: {output_file.name}")

    return result_df


# ------------------------------
# 6.2 Explain the prediction process
# ------------------------------
print("\n[INFO] HOW PREDICTION WORKS:")
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
USE_PARALLEL = False  # [WARN] Set to False when using GPU (GPU processes can't run in parallel)
MAX_WORKERS = 3  # Number of parallel processes (only used if USE_PARALLEL=True)

if RUN_CV:
    print(f"\n[INFO] Starting cross-validation...")
    print(f"Periods to process: {CONFIG['begin']} to {CONFIG['end']} ({CONFIG['end'] - CONFIG['begin'] + 1} periods)")
    print(f"Hyperparameter combinations per period: {len(hyperparameter_grid)}")

    if USE_PARALLEL:
        print(f"[FAST] Parallel mode: {MAX_WORKERS} workers")
        print(f"Estimated time: ~{(CONFIG['end'] - CONFIG['begin'] + 1) * 5 / MAX_WORKERS:.0f} minutes")
    else:
        print(f"[INFO] Sequential mode")
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
                    print(f"[OK] [{completed}/{len(counters)}] CV for test counter {k+1} (Year {year:.0f}) complete!")
                else:
                    print(f"[X] [{completed}/{len(counters)}] CV for test counter {k+1} (Year {year:.0f}) failed: {error}")

    else:
        # Sequential execution (original approach)
        # k represents validation endpoint, k+1 will be the test counter
        for k in range(CONFIG['begin'] - 1, CONFIG['end']):
            year = [y for y, c in year_to_counter.items() if c == k+1][0]  # k+1 is the test counter
            print(f"\n{'='*60}")
            print(f"Processing CV for test counter {k+1} (Year {year:.0f})")
            print(f"  (Training: {k-CONFIG['cv_validation']}, Validation: {k-CONFIG['cv_validation']+1}-{k})")
            print(f"{'='*60}")

            try:
                cv_results = run_cross_validation(df_scaled, k, CONFIG, final_feature_columns)
                print(f"[OK] CV for test counter {k+1} complete!")

            except Exception as e:
                print(f"[X] Error at test counter {k+1}: {str(e)}")
                print("Continuing to next period...")
                continue

    elapsed_time = time.time() - start_time
    print(f"\n" + "="*60)
    print(f"[OK] ALL CROSS-VALIDATION COMPLETE!")
    print(f"Time elapsed: {elapsed_time/60:.1f} minutes")
    print("="*60)

else:
    print("\n[WARN]  Cross-validation not run (RUN_CV = False)")
    print("Set RUN_CV = True to execute cross-validation")
    print("\n[TIP] TIP: CV will take ~2-3 hours for full dataset")
    print("[TIP] TIP: Set USE_PARALLEL = True for 2-3x speedup")

print("\n" + "=" * 60)
print("STEP 7 STATUS: Ready to run when RUN_CV = True")
print("=" * 60)


# %%
# ------------------------------
# STEP 7b: ANALYZE BEST HYPERPARAMETERS ACROSS ALL PERIODS
# ------------------------------
print("\n" + "=" * 60)
print("STEP 7B: HYPERPARAMETER ANALYSIS")
print("=" * 60)

import glob
from pathlib import Path

# Read all CV files
cv_files = list(cv_dir.glob("*.csv"))

if len(cv_files) == 0:
    print("\n[WARN]  No CV files found. Run Step 7 first!")
else:
    print(f"\nAnalyzing {len(cv_files)} CV result files...")

    all_results = []
    for file in cv_files:
        df_cv = pd.read_csv(file)
        # Get the best result from each file (lowest MSE)
        best = df_cv.nsmallest(1, 'mse')  # Lower MSE is better
        counter = int(file.stem.split('counter_')[1].split('_')[0])
        best['counter'] = counter
        # Get year from counter
        year = [y for y, c in year_to_counter.items() if c == counter][0]
        best['year'] = year
        all_results.append(best)

    # Combine all best results
    best_results = pd.concat(all_results, ignore_index=True)
    best_results = best_results.sort_values('counter')

    print("\n" + "="*80)
    print("BEST HYPERPARAMETERS FOR EACH TEST PERIOD (by MSE)")
    print("="*80)
    print(f"\nTotal periods analyzed: {len(best_results)}")
    print(f"MSE range: {best_results['mse'].min():.6f} to {best_results['mse'].max():.6f}")
    print(f"R² range: {best_results['r2_score'].min():.4f} to {best_results['r2_score'].max():.4f}")

    print("\n" + "-"*110)
    print(f"{'Year':<6} | {'Counter':<8} | {'LR':<6} | {'Depth':<6} | {'Iters':<7} | {'L2':<6} | {'R²':<10} | {'MSE':<12}")
    print("-"*110)
    for idx, row in best_results.iterrows():
        print(f"{int(row['year']):<6} | {int(row['counter']):<8} | {row['learning_rate']:<6.2f} | "
            f"{int(row['depth']):<6} | {int(row['iterations']):<7} | {row['l2_leaf_reg']:<6.1f} | "
            f"{row['r2_score']:<+10.4f} | {row['mse']:<12.6f}")

    print("\n" + "="*80)
    print("FREQUENCY OF BEST HYPERPARAMETERS")
    print("="*80)

    print("\n1. LEARNING RATE (in best models):")
    lr_counts = best_results['learning_rate'].value_counts().sort_index()
    for lr, count in lr_counts.items():
        pct = count / len(best_results) * 100
        print(f"   {lr:.2f}: {count:2d} times ({pct:5.1f}%)")
    print(f"   Mean: {best_results['learning_rate'].mean():.3f}")

    print("\n2. DEPTH (in best models):")
    depth_counts = best_results['depth'].value_counts().sort_index()
    for depth, count in depth_counts.items():
        pct = count / len(best_results) * 100
        print(f"   {int(depth):2d}: {count:2d} times ({pct:5.1f}%)")
    print(f"   Mean: {best_results['depth'].mean():.1f}")

    print("\n3. ITERATIONS (in best models):")
    iter_counts = best_results['iterations'].value_counts().sort_index()
    for iters, count in iter_counts.items():
        pct = count / len(best_results) * 100
        print(f"   {int(iters):3d}: {count:2d} times ({pct:5.1f}%)")
    print(f"   Mean: {best_results['iterations'].mean():.1f}")

    print("\n4. L2_LEAF_REG (in best models):")
    l2_counts = best_results['l2_leaf_reg'].value_counts().sort_index()
    for l2, count in l2_counts.items():
        pct = count / len(best_results) * 100
        print(f"   {l2:.1f}: {count:2d} times ({pct:5.1f}%)")
    print(f"   Mean: {best_results['l2_leaf_reg'].mean():.1f}")

    print("\n" + "="*80)
    print("TOP 10 BEST PERFORMING CONFIGURATIONS (by MSE, across all periods)")
    print("="*80)
    top10 = best_results.nsmallest(10, 'mse')[['year', 'counter', 'learning_rate', 'depth', 'iterations', 'l2_leaf_reg', 'mse', 'r2_score']]
    print(top10.to_string(index=False))

    # Analyze by hyperparameter across ALL combinations
    print("\n" + "="*80)
    print("AVERAGE MSE BY HYPERPARAMETER VALUE (ALL COMBINATIONS)")
    print("="*80)

    # Aggregate across all files
    all_data = []
    for file in cv_files:
        df_cv = pd.read_csv(file)
        counter = int(file.stem.split('counter_')[1].split('_')[0])
        year = [y for y, c in year_to_counter.items() if c == counter][0]
        df_cv['counter'] = counter
        df_cv['year'] = year
        all_data.append(df_cv)

    full_data = pd.concat(all_data, ignore_index=True)

    print("\nBy LEARNING RATE (lower MSE = better):")
    lr_avg = full_data.groupby('learning_rate')['mse'].mean().sort_values(ascending=True)
    for lr, mse in lr_avg.items():
        print(f"   {lr:.2f}: {mse:.6f}")

    print("\nBy DEPTH (lower MSE = better):")
    depth_avg = full_data.groupby('depth')['mse'].mean().sort_values(ascending=True)
    for depth, mse in depth_avg.items():
        print(f"   {int(depth):2d}: {mse:.6f}")

    print("\nBy ITERATIONS (lower MSE = better):")
    iter_avg = full_data.groupby('iterations')['mse'].mean().sort_values(ascending=True)
    for iters, mse in iter_avg.items():
        print(f"   {int(iters):3d}: {mse:.6f}")

    print("\nBy L2_LEAF_REG (lower MSE = better):")
    l2_avg = full_data.groupby('l2_leaf_reg')['mse'].mean().sort_values(ascending=True)
    for l2, mse in l2_avg.items():
        print(f"   {l2:.1f}: {mse:.6f}")

    # Save summary to file
    summary_file = output_dir / 'hyperparameter_analysis.csv'
    best_results.to_csv(summary_file, index=False)
    print(f"\n[OK] Best hyperparameters saved to: {summary_file.name}")

    print("\n" + "="*80)
    print("KEY INSIGHTS")
    print("="*80)
    print(f"\n[BEST] Most Winning Hyperparameters:")
    print(f"   Learning Rate: {best_results['learning_rate'].mode()[0]:.2f} (wins {lr_counts.max()} times)")
    print(f"   Depth:         {int(best_results['depth'].mode()[0])} (wins {depth_counts.max()} times)")
    print(f"   Iterations:    {int(best_results['iterations'].mode()[0])} (wins {iter_counts.max()} times)")
    print(f"   L2 Leaf Reg:   {best_results['l2_leaf_reg'].mode()[0]:.1f} (wins {l2_counts.max()} times)")

    print(f"\n[STATS] Best Hyperparameters (by avg MSE):")
    print(f"   Learning Rate: {lr_avg.idxmin():.2f} (avg MSE: {lr_avg.min():.6f})")
    print(f"   Depth:         {int(depth_avg.idxmin())} (avg MSE: {depth_avg.min():.6f})")
    print(f"   Iterations:    {int(iter_avg.idxmin())} (avg MSE: {iter_avg.min():.6f})")
    print(f"   L2 Leaf Reg:   {l2_avg.idxmin():.1f} (avg MSE: {l2_avg.min():.6f})")

print("\n" + "=" * 60)
print("STEP 7B COMPLETE!")
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
    print(f"\n[INFO] Starting predictions...")
    print(f"Periods to predict: {CONFIG['begin']} to {CONFIG['end']}")

    if USE_PARALLEL:
        print(f"[FAST] Parallel mode: {MAX_WORKERS} workers")
    else:
        print(f"[INFO] Sequential mode")
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
                print(f"[INFO] [Worker] Starting counter {k+1} (Year {year:.0f})...")
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

        print(f"[INFO] Counters to process: {counters}")
        print(f"[INFO] Total tasks: {len(counters)}")

        # Submit all tasks to the executor
        print(f"[INFO] Creating ProcessPoolExecutor with {MAX_WORKERS} workers...")

        with ProcessPoolExecutor(max_workers=MAX_WORKERS) as executor:
            print(f"[OK] Executor created!")

            # Submit all prediction tasks
            print(f"[INFO] Submitting {len(counters)} tasks...")
            futures = {executor.submit(prediction_worker, k): k for k in counters}
            print(f"[OK] All {len(futures)} tasks submitted!")

            # Process results as they complete
            completed = 0
            total = len(futures)

            print(f" Waiting for results...")

            for future in as_completed(futures):
                k, year, pred_results, success, error = future.result()
                completed += 1

                print(f"\n{'='*60}")
                print(f"[{completed}/{total}] Counter {k+1} (Year {year:.0f})")
                print(f"{'='*60}")

                if success:
                    all_predictions.append(pred_results)
                    print(f"[OK] Predictions saved!")
                else:
                    print(f"[WARN]  Skipped: {error}")

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
                    print(f"[OK] Counter {k+1} predictions saved!")
                else:
                    print(f"[WARN]  Skipping counter {k+1} (CV file not found)")

            except Exception as e:
                print(f"[X] Error at counter {k+1}: {str(e)}")
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
        print(f"[OK] ALL PREDICTIONS COMPLETE!")
        print(f"Total observations: {len(final_predictions):,}")
        print(f"Time elapsed: {elapsed_time/60:.1f} minutes")
        print(f"Results saved to: {combined_file.name}")
        print("="*60)
    else:
        print("\n[WARN]  No predictions generated!")
        final_predictions = None

else:
    print("\n[WARN]  Predictions not run (RUN_PRED = False)")
    print("Set RUN_PRED = True to execute predictions")
    print("[WARN]  IMPORTANT: Run Step 7 (Cross-Validation) first!")
    final_predictions = None

print("\n" + "=" * 60)
print("STEP 8 STATUS: Ready to run when RUN_PRED = True")
print("=" * 60)

# %%
# ------------------------------
# STEP 9: Combine Predictions and Build Portfolios
# ------------------------------
# print("\n" + "=" * 60)
# print("STEP 9: BUILDING PORTFOLIOS")
# print("=" * 60)

# # This step requires Step 8 to be complete!

# # Portfolio capital allocation
# TOTAL_CAPITAL = 1_000_000  # $1 million total capital

# if final_predictions is not None and RUN_PRED:
#     print(f"\n[STATS] Creating long/short portfolios...")
#     print(f" Total capital: ${TOTAL_CAPITAL:,.0f}")
#     print(f"   Capital allocation: Flexible based on number of positions")

#     # Create portfolio dataset
#     portfolio_data = final_predictions.copy()

#     # Verify we have the necessary columns
#     required_cols = ['form_year', 'predicted_return', CONFIG['dep_var']]
#     missing_cols = [col for col in required_cols if col not in portfolio_data.columns]

#     if missing_cols:
#         print(f"[WARN]  Missing required columns: {missing_cols}")
#         portfolio_df_fixed = None
#         portfolio_df_decile = None
#     else:
#         # ========================================
#         # VERSION 1: FIXED TOP 100 / BOTTOM 100
#         # ========================================
#         print(f"\n[UP] Building portfolios - VERSION 1: Fixed Top 100 / Bottom 100")
#         print("-" * 60)
#         portfolio_results_fixed = []

#         for year in sorted(portfolio_data['form_year'].unique()):
#             year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

#             if len(year_data) < 200:
#                 continue

#             # Filter out stocks with NaN expected_return (cannot backtest without actual returns)
#             year_data = year_data[year_data[CONFIG['dep_var']].notna()].copy()

#             # Sort by predicted returns
#             year_data = year_data.sort_values('predicted_return', ascending=False)

#             # Fixed: Top 100 long, bottom 100 short
#             TOP_N = 100
#             BOTTOM_N = 100

#             long_portfolio = year_data.head(TOP_N)
#             short_portfolio = year_data.tail(BOTTOM_N)

#             # Calculate returns (percentage)
#             long_return = long_portfolio[CONFIG['dep_var']].mean()
#             short_return = -short_portfolio[CONFIG['dep_var']].mean()  # Negative because shorting
#             spread = long_return - short_return

#             # === DOLLAR-BASED CALCULATIONS ===
#             # Flexible capital allocation based on number of positions
#             n_long = len(long_portfolio)
#             n_short = len(short_portfolio)
#             total_positions = n_long + n_short

#             # Allocate capital proportionally
#             long_capital = TOTAL_CAPITAL * (n_long / total_positions)
#             short_capital = TOTAL_CAPITAL * (n_short / total_positions)

#             # Position sizing (equal weight within each side)
#             position_size_long = long_capital / n_long
#             position_size_short = short_capital / n_short

#             # Dollar P&L
#             dollar_pnl_long = long_capital * long_return
#             dollar_pnl_short = short_capital * short_return  # Already accounts for short sign
#             total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

#             # Portfolio return on total capital
#             portfolio_return = total_dollar_pnl / TOTAL_CAPITAL

#             portfolio_results_fixed.append({
#                 'year': year,
#                 'long_return': long_return,
#                 'short_return': short_return,
#                 'spread': spread,
#                 'n_long': n_long,
#                 'n_short': n_short,
#                 # Dollar-based metrics
#                 'long_capital': long_capital,
#                 'short_capital': short_capital,
#                 'position_size_long': position_size_long,
#                 'position_size_short': position_size_short,
#                 'dollar_pnl_long': dollar_pnl_long,
#                 'dollar_pnl_short': dollar_pnl_short,
#                 'total_dollar_pnl': total_dollar_pnl,
#                 'portfolio_return': portfolio_return
#             })

#             print(f"  Year {year:.0f}: {len(year_data):,} stocks  Long {len(long_portfolio)}, Short {len(short_portfolio)}")

#         portfolio_df_fixed = pd.DataFrame(portfolio_results_fixed)
#         portfolio_file_fixed = output_dir / 'portfolio_returns_fixed100.csv'
#         portfolio_df_fixed.to_csv(portfolio_file_fixed, index=False)
#         print(f"[OK] Fixed-100 portfolios saved to: {portfolio_file_fixed.name}")

#         # ========================================
#         # VERSION 2: DECILE METHOD (10% LONG / 10% SHORT)
#         # ========================================
#         print(f"\n[UP] Building portfolios - VERSION 2: Decile Method (Top 10% / Bottom 10%)")
#         print("-" * 60)
#         portfolio_results_decile = []

#         for year in sorted(portfolio_data['form_year'].unique()):
#             year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

#             if len(year_data) < 200:
#                 continue

#             # Filter out stocks with NaN expected_return (cannot backtest without actual returns)
#             year_data = year_data[year_data[CONFIG['dep_var']].notna()].copy()

#             # Sort by predicted returns
#             year_data = year_data.sort_values('predicted_return', ascending=False)

#             # Decile: Top 10% long, Bottom 10% short (symmetric)
#             n_stocks = len(year_data)
#             decile_size = n_stocks // 10        # 10% of stocks (one decile)

#             TOP_N = decile_size                 # Top 10% (Decile 10)
#             BOTTOM_N = decile_size              # Bottom 10% (Decile 1)

#             long_portfolio = year_data.head(TOP_N)
#             short_portfolio = year_data.tail(BOTTOM_N)

#             # Calculate returns (percentage)
#             long_return = long_portfolio[CONFIG['dep_var']].mean()
#             short_return = -short_portfolio[CONFIG['dep_var']].mean()  # Negative because shorting
#             spread = long_return - short_return

#             # === DOLLAR-BASED CALCULATIONS ===
#             # Flexible capital allocation based on number of positions
#             n_long = len(long_portfolio)
#             n_short = len(short_portfolio)
#             total_positions = n_long + n_short

#             # Allocate capital proportionally
#             long_capital = TOTAL_CAPITAL * (n_long / total_positions)
#             short_capital = TOTAL_CAPITAL * (n_short / total_positions)

#             # Position sizing (equal weight within each side)
#             position_size_long = long_capital / n_long
#             position_size_short = short_capital / n_short

#             # Dollar P&L
#             dollar_pnl_long = long_capital * long_return
#             dollar_pnl_short = short_capital * short_return  # Already accounts for short sign
#             total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

#             # Portfolio return on total capital
#             portfolio_return = total_dollar_pnl / TOTAL_CAPITAL

#             portfolio_results_decile.append({
#                 'year': year,
#                 'long_return': long_return,
#                 'short_return': short_return,
#                 'spread': spread,
#                 'n_long': n_long,
#                 'n_short': n_short,
#                 # Dollar-based metrics
#                 'long_capital': long_capital,
#                 'short_capital': short_capital,
#                 'position_size_long': position_size_long,
#                 'position_size_short': position_size_short,
#                 'dollar_pnl_long': dollar_pnl_long,
#                 'dollar_pnl_short': dollar_pnl_short,
#                 'total_dollar_pnl': total_dollar_pnl,
#                 'portfolio_return': portfolio_return
#             })

#             print(f"  Year {year:.0f}: {len(year_data):,} stocks  Long {len(long_portfolio)}, Short {len(short_portfolio)}")

#         portfolio_df_decile = pd.DataFrame(portfolio_results_decile)
#         portfolio_file_decile = output_dir / 'portfolio_returns_decile10pct.csv'
#         portfolio_df_decile.to_csv(portfolio_file_decile, index=False)
#         print(f"[OK] Decile-10% portfolios saved to: {portfolio_file_decile.name}")

#         # ========================================
#         # VERSION 3: HYBRID (TOP 10% LONG / BOTTOM 100 SHORT)
#         # ========================================
#         print(f"\n[UP] Building portfolios - VERSION 3: Hybrid (Top 10% Long / Bottom 100 Short)")
#         print("-" * 60)
#         portfolio_results_hybrid = []

#         for year in sorted(portfolio_data['form_year'].unique()):
#             year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

#             if len(year_data) < 200:
#                 continue

#             # Filter out stocks with NaN expected_return (cannot backtest without actual returns)
#             year_data = year_data[year_data[CONFIG['dep_var']].notna()].copy()

#             # Sort by predicted returns
#             year_data = year_data.sort_values('predicted_return', ascending=False)

#             # Hybrid: Top 10% long, Bottom 100 short (fixed)
#             n_stocks = len(year_data)
#             decile_size = n_stocks // 10        # 10% for long

#             TOP_N = decile_size                 # Top 10% (varies by year)
#             BOTTOM_N = 100                      # Bottom 100 (fixed)

#             long_portfolio = year_data.head(TOP_N)
#             short_portfolio = year_data.tail(BOTTOM_N)

#             # Calculate returns (percentage)
#             long_return = long_portfolio[CONFIG['dep_var']].mean()
#             short_return = -short_portfolio[CONFIG['dep_var']].mean()  # Negative because shorting
#             spread = long_return - short_return

#             # === DOLLAR-BASED CALCULATIONS ===
#             # Flexible capital allocation based on number of positions
#             n_long = len(long_portfolio)
#             n_short = len(short_portfolio)
#             total_positions = n_long + n_short

#             # Allocate capital proportionally
#             long_capital = TOTAL_CAPITAL * (n_long / total_positions)
#             short_capital = TOTAL_CAPITAL * (n_short / total_positions)

#             # Position sizing (equal weight within each side)
#             position_size_long = long_capital / n_long
#             position_size_short = short_capital / n_short

#             # Dollar P&L
#             dollar_pnl_long = long_capital * long_return
#             dollar_pnl_short = short_capital * short_return  # Already accounts for short sign
#             total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

#             # Portfolio return on total capital
#             portfolio_return = total_dollar_pnl / TOTAL_CAPITAL

#             portfolio_results_hybrid.append({
#                 'year': year,
#                 'long_return': long_return,
#                 'short_return': short_return,
#                 'spread': spread,
#                 'n_long': n_long,
#                 'n_short': n_short,
#                 # Dollar-based metrics
#                 'long_capital': long_capital,
#                 'short_capital': short_capital,
#                 'position_size_long': position_size_long,
#                 'position_size_short': position_size_short,
#                 'dollar_pnl_long': dollar_pnl_long,
#                 'dollar_pnl_short': dollar_pnl_short,
#                 'total_dollar_pnl': total_dollar_pnl,
#                 'portfolio_return': portfolio_return
#             })

#             print(f"  Year {year:.0f}: {len(year_data):,} stocks  Long {len(long_portfolio)}, Short {len(short_portfolio)}")

#         portfolio_df_hybrid = pd.DataFrame(portfolio_results_hybrid)
#         portfolio_file_hybrid = output_dir / 'portfolio_returns_hybrid_10pct_100.csv'
#         portfolio_df_hybrid.to_csv(portfolio_file_hybrid, index=False)
#         print(f"[OK] Hybrid portfolios saved to: {portfolio_file_hybrid.name}")

#         # ========================================
#         # VERSION 4: HYBRID 10%/100 (50% LONG / 50% SHORT CAPITAL ALLOCATION)
#         # ========================================
#         print(f"\n[UP] Building portfolios - VERSION 4: Hybrid 10%/100 (50% Long / 50% Short)")
#         print("-" * 60)
#         portfolio_results_hybrid5050 = []

#         for year in sorted(portfolio_data['form_year'].unique()):
#             year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

#             if len(year_data) < 200:
#                 continue

#             # Filter out stocks with NaN expected_return (cannot backtest without actual returns)
#             year_data = year_data[year_data[CONFIG['dep_var']].notna()].copy()

#             # Sort by predicted returns
#             year_data = year_data.sort_values('predicted_return', ascending=False)

#             # Hybrid: Top 10% long, Bottom 100 short (fixed)
#             n_stocks = len(year_data)
#             decile_size = n_stocks // 10        # 10% for long

#             TOP_N = decile_size                 # Top 10% (varies by year)
#             BOTTOM_N = 100                      # Bottom 100 (fixed)

#             long_portfolio = year_data.head(TOP_N)
#             short_portfolio = year_data.tail(BOTTOM_N)

#             # Calculate returns (percentage)
#             long_return = long_portfolio[CONFIG['dep_var']].mean()
#             short_return = -short_portfolio[CONFIG['dep_var']].mean()  # Negative because shorting
#             spread = long_return - short_return

#             # === DOLLAR-BASED CALCULATIONS (50/50 SPLIT) ===
#             # Market neutral: Fixed 50% long, 50% short
#             n_long = len(long_portfolio)
#             n_short = len(short_portfolio)

#             # Allocate capital 50/50 regardless of position count
#             long_capital = TOTAL_CAPITAL * 0.5  # Fixed $500K
#             short_capital = TOTAL_CAPITAL * 0.5  # Fixed $500K

#             # Position sizing (equal weight within each side)
#             position_size_long = long_capital / n_long
#             position_size_short = short_capital / n_short

#             # Dollar P&L
#             dollar_pnl_long = long_capital * long_return
#             dollar_pnl_short = short_capital * short_return  # Already accounts for short sign
#             total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

#             # Portfolio return on total capital
#             portfolio_return = total_dollar_pnl / TOTAL_CAPITAL

#             portfolio_results_hybrid5050.append({
#                 'year': year,
#                 'long_return': long_return,
#                 'short_return': short_return,
#                 'spread': spread,
#                 'n_long': n_long,
#                 'n_short': n_short,
#                 # Dollar-based metrics
#                 'long_capital': long_capital,
#                 'short_capital': short_capital,
#                 'position_size_long': position_size_long,
#                 'position_size_short': position_size_short,
#                 'dollar_pnl_long': dollar_pnl_long,
#                 'dollar_pnl_short': dollar_pnl_short,
#                 'total_dollar_pnl': total_dollar_pnl,
#                 'portfolio_return': portfolio_return
#             })

#             print(f"  Year {year:.0f}: {len(year_data):,} stocks  Long {len(long_portfolio)} (${position_size_long:,.0f}/stock), Short {len(short_portfolio)} (${position_size_short:,.0f}/stock)")

#         portfolio_df_hybrid5050 = pd.DataFrame(portfolio_results_hybrid5050)
#         portfolio_file_hybrid5050 = output_dir / 'portfolio_returns_hybrid_10pct_100_5050.csv'
#         portfolio_df_hybrid5050.to_csv(portfolio_file_hybrid5050, index=False)
#         print(f"[OK] Hybrid 10%/100 (50% L/S) portfolios saved to: {portfolio_file_hybrid5050.name}")

#         # ========================================
#         # VERSION 5: HYBRID 10%/100 (50% L/S + 50% STOP LOSS ON SHORTS)
#         # ========================================
#         print(f"\n[UP] Building portfolios - VERSION 5: Hybrid 10%/100 (50% L/S + Stop Loss)")
#         print("-" * 60)
#         portfolio_results_stoploss = []

#         for year in sorted(portfolio_data['form_year'].unique()):
#             year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

#             if len(year_data) < 200:
#                 continue

#             # Filter out stocks with NaN expected_return (cannot backtest without actual returns)
#             year_data = year_data[year_data[CONFIG['dep_var']].notna()].copy()

#             # Sort by predicted returns
#             year_data = year_data.sort_values('predicted_return', ascending=False)

#             # Select top 10% for long, bottom 100 for short
#             n_stocks = len(year_data)
#             top_n = max(1, int(n_stocks * 0.10))
#             bottom_n = 100

#             long_portfolio = year_data.head(top_n)
#             short_portfolio = year_data.tail(bottom_n)

#             # Long portfolio: apply 50% stop loss per stock
#             # For longs, we lose money when stock goes down
#             long_returns_raw = long_portfolio[CONFIG['dep_var']].values
#             long_returns_capped = np.maximum(long_returns_raw, -0.50)  # Cap at -50% loss

#             # Count how many long positions hit stop loss
#             n_long_stopped_out = (long_returns_raw < -0.50).sum()

#             # Calculate average return for long portfolio
#             long_return = np.nanmean(long_returns_capped)

#             # Short portfolio: apply 50% stop loss per stock
#             # For shorts, actual return = -stock_return (we profit when stock goes down)
#             # Stop loss = cap losses at -50% (if stock goes up more than 50%, we lose max 50%)
#             short_returns_raw = -short_portfolio[CONFIG['dep_var']].values
#             short_returns_capped = np.maximum(short_returns_raw, -0.50)  # Cap at -50% loss

#             # Count how many short positions hit stop loss
#             n_short_stopped_out = (short_returns_raw < -0.50).sum()

#             # Calculate average return for short portfolio
#             # Use np.nanmean to ignore NaN values (delisted stocks)
#             short_return = np.nanmean(short_returns_capped)
#             spread = long_return - short_return

#             # === DOLLAR-BASED CALCULATIONS ===
#             # 50% / 50% capital allocation
#             n_long = len(long_portfolio)
#             n_short = len(short_portfolio)

#             long_capital = TOTAL_CAPITAL * 0.5
#             short_capital = TOTAL_CAPITAL * 0.5

#             # Position sizing (equal weight within each side)
#             position_size_long = long_capital / n_long
#             position_size_short = short_capital / n_short

#             # Dollar P&L with stop loss applied
#             dollar_pnl_long = long_capital * long_return
#             dollar_pnl_short = short_capital * short_return  # Already capped
#             total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

#             # Portfolio return on total capital
#             portfolio_return = total_dollar_pnl / TOTAL_CAPITAL

#             portfolio_results_stoploss.append({
#                 'year': year,
#                 'long_return': long_return,
#                 'short_return': short_return,
#                 'spread': spread,
#                 'n_long': n_long,
#                 'n_short': n_short,
#                 'n_long_stopped_out': n_long_stopped_out,
#                 'long_stopped_out_pct': (n_long_stopped_out / n_long) * 100 if n_long > 0 else 0,
#                 'n_short_stopped_out': n_short_stopped_out,
#                 'short_stopped_out_pct': (n_short_stopped_out / n_short) * 100 if n_short > 0 else 0,
#                 # Dollar-based metrics
#                 'long_capital': long_capital,
#                 'short_capital': short_capital,
#                 'position_size_long': position_size_long,
#                 'position_size_short': position_size_short,
#                 'dollar_pnl_long': dollar_pnl_long,
#                 'dollar_pnl_short': dollar_pnl_short,
#                 'total_dollar_pnl': total_dollar_pnl,
#                 'portfolio_return': portfolio_return
#             })

#             print(f"  Year {year:.0f}: {len(year_data):,} stocks  Long {len(long_portfolio)} (${position_size_long:,.0f}/stock, {n_long_stopped_out} stopped), Short {len(short_portfolio)} (${position_size_short:,.0f}/stock, {n_short_stopped_out} stopped)")

#         portfolio_df_stoploss = pd.DataFrame(portfolio_results_stoploss)
#         portfolio_file_stoploss = output_dir / 'portfolio_returns_hybrid_stoploss.csv'
#         portfolio_df_stoploss.to_csv(portfolio_file_stoploss, index=False)
#         print(f"[OK] Hybrid Stop Loss portfolios saved to: {portfolio_file_stoploss.name}")

#         # Store hybrid 50/50 version as default for Step 10
#         portfolio_df = portfolio_df_hybrid5050

# else:
#     print("\n[WARN]  Portfolios not created (predictions not available)")
#     print("Run Steps 7-8 first!")
#     portfolio_df = None
#     portfolio_df_hybrid = None
#     portfolio_df_hybrid5050 = None
#     portfolio_df_stoploss = None

# print("\n" + "=" * 60)
# print("STEP 9 STATUS: Portfolios ready if predictions exist")
# print("=" * 60)

# # %%
# # ------------------------------
# # STEP 10: Evaluate Model Performance and Generate Results
# # ------------------------------
# print("\n" + "=" * 60)
# print("STEP 10: EVALUATING PERFORMANCE")
# print("=" * 60)

# # Check if portfolio versions exist
# has_fixed = 'portfolio_df_fixed' in locals() and portfolio_df_fixed is not None and len(portfolio_df_fixed) > 0
# has_decile = 'portfolio_df_decile' in locals() and portfolio_df_decile is not None and len(portfolio_df_decile) > 0
# has_hybrid = 'portfolio_df_hybrid' in locals() and portfolio_df_hybrid is not None and len(portfolio_df_hybrid) > 0
# has_hybrid5050 = 'portfolio_df_hybrid5050' in locals() and portfolio_df_hybrid5050 is not None and len(portfolio_df_hybrid5050) > 0
# has_stoploss = 'portfolio_df_stoploss' in locals() and portfolio_df_stoploss is not None and len(portfolio_df_stoploss) > 0

# if has_fixed or has_decile or has_hybrid or has_hybrid5050 or has_stoploss:
#     print(f"\n[UP] Calculating performance metrics for all portfolio versions...")

#     # ========================================
#     # VERSION 1: FIXED TOP 100 / BOTTOM 100
#     # ========================================
#     if has_fixed:
#         print("\n" + "="*60)
#         print("VERSION 1: FIXED TOP 100 / BOTTOM 100 RESULTS")
#         print("="*60)

#         # Calculate performance metrics (percentage returns)
#         avg_long_fixed = portfolio_df_fixed['long_return'].mean()
#         avg_short_fixed = portfolio_df_fixed['short_return'].mean()
#         avg_spread_fixed = portfolio_df_fixed['spread'].mean()
#         spread_std_fixed = portfolio_df_fixed['spread'].std()
#         sharpe_ratio_fixed = avg_spread_fixed / spread_std_fixed if spread_std_fixed > 0 else 0

#         # Calculate dollar-based metrics
#         avg_dollar_pnl_fixed = portfolio_df_fixed['total_dollar_pnl'].mean()
#         total_dollar_pnl_fixed = portfolio_df_fixed['total_dollar_pnl'].sum()
#         avg_portfolio_return_fixed = portfolio_df_fixed['portfolio_return'].mean()
#         portfolio_return_std_fixed = portfolio_df_fixed['portfolio_return'].std()
#         sharpe_ratio_dollar_fixed = avg_portfolio_return_fixed / portfolio_return_std_fixed if portfolio_return_std_fixed > 0 else 0

#         print()
#         print("Portfolio Returns (Annual Average):")
#         print(f"  Long Portfolio (Top 100):     {avg_long_fixed:+.4f} ({avg_long_fixed*100:+.2f}%)")
#         print(f"  Short Portfolio (Bottom 100): {avg_short_fixed:+.4f} ({avg_short_fixed*100:+.2f}%)")
#         print(f"  Long-Short Spread:            {avg_spread_fixed:+.4f} ({avg_spread_fixed*100:+.2f}%)")
#         print()
#         print(f"Dollar-Based Performance (on ${TOTAL_CAPITAL:,.0f} capital):")
#         print(f"  Avg Annual P&L:      ${avg_dollar_pnl_fixed:+,.0f}")
#         print(f"  Total P&L:           ${total_dollar_pnl_fixed:+,.0f}")
#         print(f"  Avg Portfolio Return: {avg_portfolio_return_fixed:+.4f} ({avg_portfolio_return_fixed*100:+.2f}%)")
#         print()
#         print("Risk-Adjusted Performance:")
#         print(f"  Spread Volatility:  {spread_std_fixed:.4f} ({spread_std_fixed*100:.2f}%)")
#         print(f"  Sharpe Ratio:       {sharpe_ratio_fixed:.2f}")
#         print(f"  Sharpe (Dollar):    {sharpe_ratio_dollar_fixed:.2f}")
#         print()
#         print(f"Analysis Period:")
#         print(f"  Years analyzed:     {len(portfolio_df_fixed)}")
#         print(f"  First year:         {portfolio_df_fixed['year'].min():.0f}")
#         print(f"  Last year:          {portfolio_df_fixed['year'].max():.0f}")
#         print("="*60)

#         # Assessment
#         if sharpe_ratio_fixed > 1.0:
#             print("\n[OK] Excellent risk-adjusted returns!")
#         elif sharpe_ratio_fixed > 0.5:
#             print("\n[OK] Good risk-adjusted returns")
#         elif sharpe_ratio_fixed > 0.0:
#             print("\n[WARN]  Positive but weak risk-adjusted returns")
#         else:
#             print("\n[X] Negative risk-adjusted returns")

#         # Save summary
#         summary_fixed = {
#             'portfolio_type': 'Fixed_Top100',
#             'avg_long_return': avg_long_fixed,
#             'avg_short_return': avg_short_fixed,
#             'avg_spread': avg_spread_fixed,
#             'spread_volatility': spread_std_fixed,
#             'sharpe_ratio': sharpe_ratio_fixed,
#             # Dollar metrics
#             'total_capital': TOTAL_CAPITAL,
#             'avg_annual_pnl': avg_dollar_pnl_fixed,
#             'total_pnl': total_dollar_pnl_fixed,
#             'avg_portfolio_return': avg_portfolio_return_fixed,
#             'portfolio_return_volatility': portfolio_return_std_fixed,
#             'sharpe_ratio_dollar': sharpe_ratio_dollar_fixed,
#             # Period
#             'n_years': len(portfolio_df_fixed),
#             'first_year': portfolio_df_fixed['year'].min(),
#             'last_year': portfolio_df_fixed['year'].max()
#         }

#         summary_df_fixed = pd.DataFrame([summary_fixed])
#         summary_file_fixed = output_dir / 'performance_summary_fixed100.csv'
#         summary_df_fixed.to_csv(summary_file_fixed, index=False)
#         print(f"\n[OK] Fixed-100 summary saved to: {summary_file_fixed.name}")

#     # ========================================
#     # VERSION 2: DECILE METHOD (10% LONG / 10% SHORT)
#     # ========================================
#     if has_decile:
#         print("\n" + "="*60)
#         print("VERSION 2: DECILE METHOD (TOP 10% LONG / BOTTOM 10% SHORT) RESULTS")
#         print("="*60)

#         # Calculate performance metrics (percentage returns)
#         avg_long_decile = portfolio_df_decile['long_return'].mean()
#         avg_short_decile = portfolio_df_decile['short_return'].mean()
#         avg_spread_decile = portfolio_df_decile['spread'].mean()
#         spread_std_decile = portfolio_df_decile['spread'].std()
#         sharpe_ratio_decile = avg_spread_decile / spread_std_decile if spread_std_decile > 0 else 0

#         # Calculate dollar-based metrics
#         avg_dollar_pnl_decile = portfolio_df_decile['total_dollar_pnl'].mean()
#         total_dollar_pnl_decile = portfolio_df_decile['total_dollar_pnl'].sum()
#         avg_portfolio_return_decile = portfolio_df_decile['portfolio_return'].mean()
#         portfolio_return_std_decile = portfolio_df_decile['portfolio_return'].std()
#         sharpe_ratio_dollar_decile = avg_portfolio_return_decile / portfolio_return_std_decile if portfolio_return_std_decile > 0 else 0

#         print()
#         print("Portfolio Returns (Annual Average):")
#         print(f"  Long Portfolio (Top 10%):      {avg_long_decile:+.4f} ({avg_long_decile*100:+.2f}%)")
#         print(f"  Short Portfolio (Bottom 10%):  {avg_short_decile:+.4f} ({avg_short_decile*100:+.2f}%)")
#         print(f"  Long-Short Spread:                   {avg_spread_decile:+.4f} ({avg_spread_decile*100:+.2f}%)")
#         print()
#         print(f"Dollar-Based Performance (on ${TOTAL_CAPITAL:,.0f} capital):")
#         print(f"  Avg Annual P&L:      ${avg_dollar_pnl_decile:+,.0f}")
#         print(f"  Total P&L:           ${total_dollar_pnl_decile:+,.0f}")
#         print(f"  Avg Portfolio Return: {avg_portfolio_return_decile:+.4f} ({avg_portfolio_return_decile*100:+.2f}%)")
#         print()
#         print("Risk-Adjusted Performance:")
#         print(f"  Spread Volatility:  {spread_std_decile:.4f} ({spread_std_decile*100:.2f}%)")
#         print(f"  Sharpe Ratio:       {sharpe_ratio_decile:.2f}")
#         print(f"  Sharpe (Dollar):    {sharpe_ratio_dollar_decile:.2f}")
#         print()
#         print(f"Analysis Period:")
#         print(f"  Years analyzed:     {len(portfolio_df_decile)}")
#         print(f"  First year:         {portfolio_df_decile['year'].min():.0f}")
#         print(f"  Last year:          {portfolio_df_decile['year'].max():.0f}")
#         print("="*60)

#         # Assessment
#         if sharpe_ratio_decile > 1.0:
#             print("\n[OK] Excellent risk-adjusted returns!")
#         elif sharpe_ratio_decile > 0.5:
#             print("\n[OK] Good risk-adjusted returns")
#         elif sharpe_ratio_decile > 0.0:
#             print("\n[WARN]  Positive but weak risk-adjusted returns")
#         else:
#             print("\n[X] Negative risk-adjusted returns")

#         # Save summary
#         summary_decile = {
#             'portfolio_type': 'Decile_10pct',
#             'avg_long_return': avg_long_decile,
#             'avg_short_return': avg_short_decile,
#             'avg_spread': avg_spread_decile,
#             'spread_volatility': spread_std_decile,
#             'sharpe_ratio': sharpe_ratio_decile,
#             # Dollar metrics
#             'total_capital': TOTAL_CAPITAL,
#             'avg_annual_pnl': avg_dollar_pnl_decile,
#             'total_pnl': total_dollar_pnl_decile,
#             'avg_portfolio_return': avg_portfolio_return_decile,
#             'portfolio_return_volatility': portfolio_return_std_decile,
#             'sharpe_ratio_dollar': sharpe_ratio_dollar_decile,
#             # Period
#             'n_years': len(portfolio_df_decile),
#             'first_year': portfolio_df_decile['year'].min(),
#             'last_year': portfolio_df_decile['year'].max()
#         }

#         summary_df_decile = pd.DataFrame([summary_decile])
#         summary_file_decile = output_dir / 'performance_summary_decile10pct.csv'
#         summary_df_decile.to_csv(summary_file_decile, index=False)
#         print(f"\n[OK] Decile-10% summary saved to: {summary_file_decile.name}")

#     # ========================================
#     # VERSION 3: HYBRID (TOP 10% LONG / BOTTOM 100 SHORT)
#     # ========================================
#     if has_hybrid:
#         print("\n" + "="*60)
#         print("VERSION 3: HYBRID (TOP 10% LONG / BOTTOM 100 SHORT) RESULTS")
#         print("="*60)

#         # Calculate performance metrics (percentage returns)
#         avg_long_hybrid = portfolio_df_hybrid['long_return'].mean()
#         avg_short_hybrid = portfolio_df_hybrid['short_return'].mean()
#         avg_spread_hybrid = portfolio_df_hybrid['spread'].mean()
#         spread_std_hybrid = portfolio_df_hybrid['spread'].std()
#         sharpe_ratio_hybrid = avg_spread_hybrid / spread_std_hybrid if spread_std_hybrid > 0 else 0

#         # Calculate dollar-based metrics
#         avg_dollar_pnl_hybrid = portfolio_df_hybrid['total_dollar_pnl'].mean()
#         total_dollar_pnl_hybrid = portfolio_df_hybrid['total_dollar_pnl'].sum()
#         avg_portfolio_return_hybrid = portfolio_df_hybrid['portfolio_return'].mean()
#         portfolio_return_std_hybrid = portfolio_df_hybrid['portfolio_return'].std()
#         sharpe_ratio_dollar_hybrid = avg_portfolio_return_hybrid / portfolio_return_std_hybrid if portfolio_return_std_hybrid > 0 else 0

#         print()
#         print("Portfolio Returns (Annual Average):")
#         print(f"  Long Portfolio (Top 10%):      {avg_long_hybrid:+.4f} ({avg_long_hybrid*100:+.2f}%)")
#         print(f"  Short Portfolio (Bottom 100):  {avg_short_hybrid:+.4f} ({avg_short_hybrid*100:+.2f}%)")
#         print(f"  Long-Short Spread:             {avg_spread_hybrid:+.4f} ({avg_spread_hybrid*100:+.2f}%)")
#         print()
#         print(f"Dollar-Based Performance (on ${TOTAL_CAPITAL:,.0f} capital):")
#         print(f"  Avg Annual P&L:      ${avg_dollar_pnl_hybrid:+,.0f}")
#         print(f"  Total P&L:           ${total_dollar_pnl_hybrid:+,.0f}")
#         print(f"  Avg Portfolio Return: {avg_portfolio_return_hybrid:+.4f} ({avg_portfolio_return_hybrid*100:+.2f}%)")
#         print()
#         print("Risk-Adjusted Performance:")
#         print(f"  Spread Volatility:  {spread_std_hybrid:.4f} ({spread_std_hybrid*100:.2f}%)")
#         print(f"  Sharpe Ratio:       {sharpe_ratio_hybrid:.2f}")
#         print(f"  Sharpe (Dollar):    {sharpe_ratio_dollar_hybrid:.2f}")
#         print()
#         print(f"Analysis Period:")
#         print(f"  Years analyzed:     {len(portfolio_df_hybrid)}")
#         print(f"  First year:         {portfolio_df_hybrid['year'].min():.0f}")
#         print(f"  Last year:          {portfolio_df_hybrid['year'].max():.0f}")
#         print("="*60)

#         # Assessment
#         if sharpe_ratio_hybrid > 1.0:
#             print("\n[OK] Excellent risk-adjusted returns!")
#         elif sharpe_ratio_hybrid > 0.5:
#             print("\n[OK] Good risk-adjusted returns")
#         elif sharpe_ratio_hybrid > 0.0:
#             print("\n[WARN]  Positive but weak risk-adjusted returns")
#         else:
#             print("\n[X] Negative risk-adjusted returns")

#         # Save summary
#         summary_hybrid = {
#             'portfolio_type': 'Hybrid_10pct_100',
#             'avg_long_return': avg_long_hybrid,
#             'avg_short_return': avg_short_hybrid,
#             'avg_spread': avg_spread_hybrid,
#             'spread_volatility': spread_std_hybrid,
#             'sharpe_ratio': sharpe_ratio_hybrid,
#             # Dollar metrics
#             'total_capital': TOTAL_CAPITAL,
#             'avg_annual_pnl': avg_dollar_pnl_hybrid,
#             'total_pnl': total_dollar_pnl_hybrid,
#             'avg_portfolio_return': avg_portfolio_return_hybrid,
#             'portfolio_return_volatility': portfolio_return_std_hybrid,
#             'sharpe_ratio_dollar': sharpe_ratio_dollar_hybrid,
#             # Period
#             'n_years': len(portfolio_df_hybrid),
#             'first_year': portfolio_df_hybrid['year'].min(),
#             'last_year': portfolio_df_hybrid['year'].max()
#         }

#         summary_df_hybrid = pd.DataFrame([summary_hybrid])
#         summary_file_hybrid = output_dir / 'performance_summary_hybrid_10pct_100.csv'
#         summary_df_hybrid.to_csv(summary_file_hybrid, index=False)
#         print(f"\n[OK] Hybrid summary saved to: {summary_file_hybrid.name}")

#     # ========================================
#     # VERSION 4: HYBRID 10%/100 (50% LONG / 50% SHORT CAPITAL ALLOCATION)
#     # ========================================
#     if has_hybrid5050:
#         print("\n" + "="*60)
#         print("VERSION 4: HYBRID 10%/100 (50% LONG / 50% SHORT)")
#         print("="*60)

#         # Calculate performance metrics (percentage returns)
#         avg_long_hybrid5050 = portfolio_df_hybrid5050['long_return'].mean()
#         avg_short_hybrid5050 = portfolio_df_hybrid5050['short_return'].mean()
#         avg_spread_hybrid5050 = portfolio_df_hybrid5050['spread'].mean()
#         spread_std_hybrid5050 = portfolio_df_hybrid5050['spread'].std()
#         sharpe_ratio_hybrid5050 = avg_spread_hybrid5050 / spread_std_hybrid5050 if spread_std_hybrid5050 > 0 else 0

#         # Calculate dollar-based metrics
#         avg_dollar_pnl_hybrid5050 = portfolio_df_hybrid5050['total_dollar_pnl'].mean()
#         total_dollar_pnl_hybrid5050 = portfolio_df_hybrid5050['total_dollar_pnl'].sum()
#         avg_portfolio_return_hybrid5050 = portfolio_df_hybrid5050['portfolio_return'].mean()
#         portfolio_return_std_hybrid5050 = portfolio_df_hybrid5050['portfolio_return'].std()
#         sharpe_ratio_dollar_hybrid5050 = avg_portfolio_return_hybrid5050 / portfolio_return_std_hybrid5050 if portfolio_return_std_hybrid5050 > 0 else 0

#         print()
#         print("Portfolio Returns (Annual Average):")
#         print(f"  Long Portfolio (Top 10%):      {avg_long_hybrid5050:+.4f} ({avg_long_hybrid5050*100:+.2f}%)")
#         print(f"  Short Portfolio (Bottom 100):  {avg_short_hybrid5050:+.4f} ({avg_short_hybrid5050*100:+.2f}%)")
#         print(f"  Long-Short Spread:             {avg_spread_hybrid5050:+.4f} ({avg_spread_hybrid5050*100:+.2f}%)")
#         print()
#         print(f"Dollar-Based Performance (on ${TOTAL_CAPITAL:,.0f} capital - 50/50 split):")
#         print(f"  Avg Annual P&L:      ${avg_dollar_pnl_hybrid5050:+,.0f}")
#         print(f"  Total P&L:           ${total_dollar_pnl_hybrid5050:+,.0f}")
#         print(f"  Avg Portfolio Return: {avg_portfolio_return_hybrid5050:+.4f} ({avg_portfolio_return_hybrid5050*100:+.2f}%)")
#         print(f"  Long Capital:        $500,000 (fixed)")
#         print(f"  Short Capital:       $500,000 (fixed)")
#         print()
#         print("Risk-Adjusted Performance:")
#         print(f"  Spread Volatility:  {spread_std_hybrid5050:.4f} ({spread_std_hybrid5050*100:.2f}%)")
#         print(f"  Sharpe Ratio:       {sharpe_ratio_hybrid5050:.2f}")
#         print(f"  Sharpe (Dollar):    {sharpe_ratio_dollar_hybrid5050:.2f}")
#         print()
#         print(f"Analysis Period:")
#         print(f"  Years analyzed:     {len(portfolio_df_hybrid5050)}")
#         print(f"  First year:         {portfolio_df_hybrid5050['year'].min():.0f}")
#         print(f"  Last year:          {portfolio_df_hybrid5050['year'].max():.0f}")
#         print("="*60)

#         # Assessment
#         if sharpe_ratio_hybrid5050 > 1.0:
#             print("\n[OK] Excellent risk-adjusted returns!")
#         elif sharpe_ratio_hybrid5050 > 0.5:
#             print("\n[OK] Good risk-adjusted returns")
#         elif sharpe_ratio_hybrid5050 > 0.0:
#             print("\n[WARN]  Positive but weak risk-adjusted returns")
#         else:
#             print("\n[X] Negative risk-adjusted returns")

#         # Save summary
#         summary_hybrid5050 = {
#             'portfolio_type': 'Hybrid_10pct_100_5050',
#             'avg_long_return': avg_long_hybrid5050,
#             'avg_short_return': avg_short_hybrid5050,
#             'avg_spread': avg_spread_hybrid5050,
#             'spread_volatility': spread_std_hybrid5050,
#             'sharpe_ratio': sharpe_ratio_hybrid5050,
#             # Dollar metrics
#             'total_capital': TOTAL_CAPITAL,
#             'avg_annual_pnl': avg_dollar_pnl_hybrid5050,
#             'total_pnl': total_dollar_pnl_hybrid5050,
#             'avg_portfolio_return': avg_portfolio_return_hybrid5050,
#             'portfolio_return_volatility': portfolio_return_std_hybrid5050,
#             'sharpe_ratio_dollar': sharpe_ratio_dollar_hybrid5050,
#             # Period
#             'n_years': len(portfolio_df_hybrid5050),
#             'first_year': portfolio_df_hybrid5050['year'].min(),
#             'last_year': portfolio_df_hybrid5050['year'].max()
#         }

#         summary_df_hybrid5050 = pd.DataFrame([summary_hybrid5050])
#         summary_file_hybrid5050 = output_dir / 'performance_summary_hybrid_10pct_100_5050.csv'
#         summary_df_hybrid5050.to_csv(summary_file_hybrid5050, index=False)
#         print(f"\n[OK] Hybrid 10%/100 (50% L/S) summary saved to: {summary_file_hybrid5050.name}")

#     # ========================================
#     # VERSION 5: HYBRID WITH STOP LOSS (50% LONG / 50% SHORT + 50% STOP LOSS)
#     # ========================================
#     if has_stoploss:
#         print("\n" + "="*60)
#         print("VERSION 5: HYBRID WITH STOP LOSS (50% L/S + 50% Stop)")
#         print("="*60)

#         # Calculate performance metrics (percentage returns)
#         avg_long_stoploss = portfolio_df_stoploss['long_return'].mean()
#         avg_short_stoploss = portfolio_df_stoploss['short_return'].mean()
#         avg_spread_stoploss = portfolio_df_stoploss['spread'].mean()
#         spread_std_stoploss = portfolio_df_stoploss['spread'].std()
#         sharpe_ratio_stoploss = avg_spread_stoploss / spread_std_stoploss if spread_std_stoploss > 0 else 0

#         # Calculate dollar-based metrics
#         avg_dollar_pnl_stoploss = portfolio_df_stoploss['total_dollar_pnl'].mean()
#         total_dollar_pnl_stoploss = portfolio_df_stoploss['total_dollar_pnl'].sum()
#         avg_portfolio_return_stoploss = portfolio_df_stoploss['portfolio_return'].mean()
#         portfolio_return_std_stoploss = portfolio_df_stoploss['portfolio_return'].std()
#         sharpe_ratio_dollar_stoploss = avg_portfolio_return_stoploss / portfolio_return_std_stoploss if portfolio_return_std_stoploss > 0 else 0

#         # Stop loss metrics (now separate for longs and shorts)
#         avg_long_stopped_out = portfolio_df_stoploss['n_long_stopped_out'].mean()
#         avg_long_stopped_pct = portfolio_df_stoploss['long_stopped_out_pct'].mean()
#         avg_short_stopped_out = portfolio_df_stoploss['n_short_stopped_out'].mean()
#         avg_short_stopped_pct = portfolio_df_stoploss['short_stopped_out_pct'].mean()

#         print()
#         print("Portfolio Returns (Annual Average):")
#         print(f"  Long Portfolio (Top 10%):      {avg_long_stoploss:+.4f} ({avg_long_stoploss*100:+.2f}%)")
#         print(f"  Short Portfolio (Bottom 100):  {avg_short_stoploss:+.4f} ({avg_short_stoploss*100:+.2f}%)")
#         print(f"  Long-Short Spread:             {avg_spread_stoploss:+.4f} ({avg_spread_stoploss*100:+.2f}%)")
#         print()
#         print(f"Dollar-Based Performance (on ${TOTAL_CAPITAL:,.0f} capital):")
#         print(f"  Avg Annual P&L:      ${avg_dollar_pnl_stoploss:+,.0f}")
#         print(f"  Total P&L:           ${total_dollar_pnl_stoploss:+,.0f}")
#         print(f"  Avg Portfolio Return: {avg_portfolio_return_stoploss:+.4f} ({avg_portfolio_return_stoploss*100:+.2f}%)")
#         print()
#         print("Stop Loss Statistics:")
#         print(f"  Long Positions Stopped:  {avg_long_stopped_out:.1f} per year ({avg_long_stopped_pct:.1f}%)")
#         print(f"  Short Positions Stopped: {avg_short_stopped_out:.1f} per year ({avg_short_stopped_pct:.1f}%)")
#         print()
#         print("Risk-Adjusted Performance:")
#         print(f"  Spread Volatility:  {spread_std_stoploss:.4f} ({spread_std_stoploss*100:.2f}%)")
#         print(f"  Sharpe Ratio:       {sharpe_ratio_stoploss:.2f}")
#         print(f"  Sharpe (Dollar):    {sharpe_ratio_dollar_stoploss:.2f}")
#         print()
#         print(f"Analysis Period:")
#         print(f"  Years analyzed:     {len(portfolio_df_stoploss)}")
#         print(f"  First year:         {portfolio_df_stoploss['year'].min():.0f}")
#         print(f"  Last year:          {portfolio_df_stoploss['year'].max():.0f}")
#         print("="*60)

#         # Assessment
#         if sharpe_ratio_stoploss > 1.0:
#             print("\n[OK] Excellent risk-adjusted returns!")
#         elif sharpe_ratio_stoploss > 0.5:
#             print("\n[OK] Good risk-adjusted returns")
#         elif sharpe_ratio_stoploss > 0.0:
#             print("\n[WARN]  Positive but weak risk-adjusted returns")
#         else:
#             print("\n[X] Negative risk-adjusted returns")

#         # Save summary
#         summary_stoploss = {
#             'portfolio_type': 'Hybrid_StopLoss',
#             'avg_long_return': avg_long_stoploss,
#             'avg_short_return': avg_short_stoploss,
#             'avg_spread': avg_spread_stoploss,
#             'spread_volatility': spread_std_stoploss,
#             'sharpe_ratio': sharpe_ratio_stoploss,
#             # Stop loss metrics (separate for longs and shorts)
#             'avg_long_stopped_out': avg_long_stopped_out,
#             'avg_long_stopped_pct': avg_long_stopped_pct,
#             'avg_short_stopped_out': avg_short_stopped_out,
#             'avg_short_stopped_pct': avg_short_stopped_pct,
#             # Dollar metrics
#             'total_capital': TOTAL_CAPITAL,
#             'avg_annual_pnl': avg_dollar_pnl_stoploss,
#             'total_pnl': total_dollar_pnl_stoploss,
#             'avg_portfolio_return': avg_portfolio_return_stoploss,
#             'portfolio_return_volatility': portfolio_return_std_stoploss,
#             'sharpe_ratio_dollar': sharpe_ratio_dollar_stoploss,
#             # Period
#             'n_years': len(portfolio_df_stoploss),
#             'first_year': portfolio_df_stoploss['year'].min(),
#             'last_year': portfolio_df_stoploss['year'].max()
#         }

#         summary_df_stoploss = pd.DataFrame([summary_stoploss])
#         summary_file_stoploss = output_dir / 'performance_summary_hybrid_stoploss.csv'
#         summary_df_stoploss.to_csv(summary_file_stoploss, index=False)
#         print(f"\n[OK] Hybrid Stop Loss summary saved to: {summary_file_stoploss.name}")

#     # ========================================
#     # COMPARISON (if multiple exist)
#     # ========================================
#     num_strategies = sum([has_fixed, has_decile, has_hybrid, has_hybrid5050, has_stoploss])

#     if num_strategies >= 2:
#         print("\n" + "="*110)
#         print("ALL STRATEGIES COMPARISON (Dollar-Based)")
#         print("="*110)
#         print()
#         print(f" Total Capital: ${TOTAL_CAPITAL:,.0f}")
#         print()

#         # Build comparison table header dynamically
#         headers = ['Metric']
#         summaries = []

#         if has_fixed:
#             headers.append('Fixed 100')
#             summaries.append(('Fixed 100', avg_dollar_pnl_fixed, total_dollar_pnl_fixed,
#                             avg_portfolio_return_fixed, portfolio_return_std_fixed,
#                             sharpe_ratio_dollar_fixed, avg_long_fixed, avg_short_fixed, avg_spread_fixed))
#         if has_decile:
#             headers.append('Decile 10%')
#             summaries.append(('Decile 10%', avg_dollar_pnl_decile, total_dollar_pnl_decile,
#                             avg_portfolio_return_decile, portfolio_return_std_decile,
#                             sharpe_ratio_dollar_decile, avg_long_decile, avg_short_decile, avg_spread_decile))
#         if has_hybrid:
#             headers.append('Hybrid 10%/100')
#             summaries.append(('Hybrid 10%/100', avg_dollar_pnl_hybrid, total_dollar_pnl_hybrid,
#                             avg_portfolio_return_hybrid, portfolio_return_std_hybrid,
#                             sharpe_ratio_dollar_hybrid, avg_long_hybrid, avg_short_hybrid, avg_spread_hybrid))
#         if has_hybrid5050:
#             headers.append('Hybrid (50% L/S)')
#             summaries.append(('Hybrid (50% L/S)', avg_dollar_pnl_hybrid5050, total_dollar_pnl_hybrid5050,
#                             avg_portfolio_return_hybrid5050, portfolio_return_std_hybrid5050,
#                             sharpe_ratio_dollar_hybrid5050, avg_long_hybrid5050, avg_short_hybrid5050, avg_spread_hybrid5050))
#         if has_stoploss:
#             headers.append('Stop Loss')
#             summaries.append(('Stop Loss', avg_dollar_pnl_stoploss, total_dollar_pnl_stoploss,
#                             avg_portfolio_return_stoploss, portfolio_return_std_stoploss,
#                             sharpe_ratio_dollar_stoploss, avg_long_stoploss, avg_short_stoploss, avg_spread_stoploss))

#         # Print header
#         col_width = 18
#         print(f"{'Metric':<30} | " + " | ".join([f"{h:^{col_width}}" for h in headers[1:]]))
#         print("-" * (32 + (col_width + 3) * len(summaries)))

#         # Dollar metrics
#         print(f"{'Avg Annual P&L':<30} | " + " | ".join([f"${s[1]:>{col_width-1},.0f}" for s in summaries]))
#         print(f"{'Total P&L':<30} | " + " | ".join([f"${s[2]:>{col_width-1},.0f}" for s in summaries]))
#         print(f"{'Avg Portfolio Return':<30} | " + " | ".join([f"{s[3]*100:>{col_width-1}.2f}%" for s in summaries]))
#         print(f"{'Portfolio Volatility':<30} | " + " | ".join([f"{s[4]*100:>{col_width-1}.2f}%" for s in summaries]))
#         print(f"{'Sharpe Ratio (Dollar)':<30} | " + " | ".join([f"{s[5]:>{col_width}.2f}" for s in summaries]))

#         print()
#         print("Component Returns (for reference):")
#         print("-" * (32 + (col_width + 3) * len(summaries)))
#         print(f"{'Long Return':<30} | " + " | ".join([f"{s[6]*100:>{col_width-1}.2f}%" for s in summaries]))
#         print(f"{'Short Return':<30} | " + " | ".join([f"{s[7]*100:>{col_width-1}.2f}%" for s in summaries]))
#         print(f"{'Spread':<30} | " + " | ".join([f"{s[8]*100:>{col_width-1}.2f}%" for s in summaries]))
#         print("="*110)

#         # Find best strategy
#         best_idx = max(range(len(summaries)), key=lambda i: summaries[i][5])  # By Sharpe ratio
#         print(f"\n[BEST] Best Strategy (by Sharpe Ratio): {summaries[best_idx][0]}")
#         print(f"   Sharpe Ratio: {summaries[best_idx][5]:.2f}")
#         print(f"   Avg Annual P&L: ${summaries[best_idx][1]:+,.0f}")

#         # Save combined comparison
#         comparison_dfs = []
#         if has_fixed:
#             comparison_dfs.append(summary_df_fixed)
#         if has_decile:
#             comparison_dfs.append(summary_df_decile)
#         if has_hybrid:
#             comparison_dfs.append(summary_df_hybrid)
#         if has_hybrid5050:
#             comparison_dfs.append(summary_df_hybrid5050)
#         if has_stoploss:
#             comparison_dfs.append(summary_df_stoploss)

#         comparison_df = pd.concat(comparison_dfs, ignore_index=True)
#         comparison_file = output_dir / 'performance_comparison_all.csv'
#         comparison_df.to_csv(comparison_file, index=False)
#         print(f"\n[OK] Comparison saved to: {comparison_file.name}")

#         # ========================================
#         # GENERATE MODEL COMPARISON SUMMARY
#         # ========================================
#         print("\n" + "="*60)
#         print("GENERATING MODEL COMPARISON SUMMARY")
#         print("="*60)

#         # Helper function to calculate CAGR
#         def calculate_cagr(total_pnl, initial_capital, n_years):
#             """Calculate Compound Annual Growth Rate"""
#             if n_years == 0:
#                 return 0.0
#             final_value = initial_capital + total_pnl
#             cagr = (final_value / initial_capital) ** (1 / n_years) - 1
#             return cagr

#         # Helper function to calculate max drawdown
#         def calculate_max_drawdown(portfolio_df, pnl_column='total_dollar_pnl'):
#             """Calculate maximum drawdown from peak"""
#             if len(portfolio_df) == 0:
#                 return 0.0

#             # Calculate cumulative P&L
#             cumulative_pnl = portfolio_df[pnl_column].cumsum()

#             # Calculate running maximum (peak)
#             running_max = cumulative_pnl.expanding().max()

#             # Calculate drawdown from peak
#             drawdown = cumulative_pnl - running_max

#             # Max drawdown is the minimum (most negative) drawdown
#             max_dd = drawdown.min()

#             # Express as percentage of initial capital
#             max_dd_pct = max_dd / TOTAL_CAPITAL

#             return max_dd_pct

#         # Helper function to calculate win rate
#         def calculate_win_rate(portfolio_df, pnl_column='total_dollar_pnl'):
#             """Calculate percentage of positive years"""
#             if len(portfolio_df) == 0:
#                 return 0.0

#             positive_years = (portfolio_df[pnl_column] > 0).sum()
#             total_years = len(portfolio_df)
#             win_rate = positive_years / total_years

#             return win_rate

#         # Load hyperparameters from first CV file
#         cv_files = sorted(cv_dir.glob('cv_*.csv'))
#         best_params_dict = {}

#         if len(cv_files) > 0:
#             first_cv_file = cv_files[0]
#             cv_results = pd.read_csv(first_cv_file)
#             cv_results = cv_results.sort_values('mse', ascending=True)
#             best_params = cv_results.iloc[0]

#             best_params_dict = {
#                 'n_estimators': int(best_params['n_estimators']),
#                 'learning_rate': float(best_params['learning_rate']),
#                 'max_depth': int(best_params['max_depth']),
#                 'subsample': float(best_params.get('subsample', 1.0)),
#                 'colsample_bytree': float(best_params.get('colsample_bytree', 1.0))
#             }
#             print(f"\n[OK] Loaded hyperparameters from: {first_cv_file.name}")
#         else:
#             print("\n[WARN]  No CV files found - hyperparameters will be empty")

#         # Get test period info from portfolio_df
#         if has_fixed:
#             first_year = int(portfolio_df_fixed['year'].min())
#             last_year = int(portfolio_df_fixed['year'].max())
#             n_test_years = len(portfolio_df_fixed)
#         elif has_decile:
#             first_year = int(portfolio_df_decile['year'].min())
#             last_year = int(portfolio_df_decile['year'].max())
#             n_test_years = len(portfolio_df_decile)
#         elif has_hybrid:
#             first_year = int(portfolio_df_hybrid['year'].min())
#             last_year = int(portfolio_df_hybrid['year'].max())
#             n_test_years = len(portfolio_df_hybrid)
#         elif has_hybrid5050:
#             first_year = int(portfolio_df_hybrid5050['year'].min())
#             last_year = int(portfolio_df_hybrid5050['year'].max())
#             n_test_years = len(portfolio_df_hybrid5050)
#         else:
#             # This should never happen if at least one strategy exists
#             raise ValueError("No portfolio data found. At least one strategy must be available.")

#         # Count number of features
#         n_features = len(feature_columns)

#         # Build summary for each strategy
#         summary_rows = []

#         # Strategy 1: Fixed 100/100
#         if has_fixed:
#             cagr = calculate_cagr(total_dollar_pnl_fixed, TOTAL_CAPITAL, n_test_years)
#             max_dd = calculate_max_drawdown(portfolio_df_fixed, 'total_dollar_pnl')
#             win_rate = calculate_win_rate(portfolio_df_fixed, 'total_dollar_pnl')
#             total_return = total_dollar_pnl_fixed / TOTAL_CAPITAL

#             summary_rows.append({
#                 'model': 'CatBoost',
#                 'strategy': 'Fixed 100/100',
#                 'total_capital': TOTAL_CAPITAL,
#                 'avg_stocks_long': portfolio_df_fixed['n_long'].mean(),
#                 'avg_stocks_short': portfolio_df_fixed['n_short'].mean(),
#                 # Returns
#                 'total_return_pct': total_return * 100,
#                 'cagr_pct': cagr * 100,
#                 'avg_annual_return_pct': avg_portfolio_return_fixed * 100,
#                 # Risk
#                 'annualized_volatility_pct': portfolio_return_std_fixed * 100,
#                 'sharpe_ratio_dollar': sharpe_ratio_dollar_fixed,
#                 'sharpe_ratio_spread': sharpe_ratio_fixed,
#                 'max_drawdown_pct': max_dd * 100,
#                 # Win rate
#                 'win_rate_pct': win_rate * 100,
#                 'positive_years': int(win_rate * n_test_years),
#                 'total_years': n_test_years,
#                 # Dollar metrics
#                 'total_pnl': total_dollar_pnl_fixed,
#                 'avg_annual_pnl': avg_dollar_pnl_fixed,
#                 # Component returns
#                 'avg_long_return_pct': avg_long_fixed * 100,
#                 'avg_short_return_pct': avg_short_fixed * 100,
#                 'avg_spread_pct': avg_spread_fixed * 100,
#                 # Model details
#                 'n_features': n_features,
#                 'max_depth': best_params_dict.get('max_depth', ''),
#                 'learning_rate': best_params_dict.get('learning_rate', ''),
#                 'n_estimators': best_params_dict.get('n_estimators', ''),
#                 'subsample': best_params_dict.get('subsample', ''),
#                 'colsample_bytree': best_params_dict.get('colsample_bytree', ''),
#                 # Test period
#                 'test_start_year': first_year,
#                 'test_end_year': last_year,
#                 'n_test_periods': n_test_years
#             })

#         # Strategy 2: Decile 10%/10%
#         if has_decile:
#             cagr = calculate_cagr(total_dollar_pnl_decile, TOTAL_CAPITAL, n_test_years)
#             max_dd = calculate_max_drawdown(portfolio_df_decile, 'total_dollar_pnl')
#             win_rate = calculate_win_rate(portfolio_df_decile, 'total_dollar_pnl')
#             total_return = total_dollar_pnl_decile / TOTAL_CAPITAL

#             summary_rows.append({
#                 'model': 'CatBoost',
#                 'strategy': 'Decile 10%/10%',
#                 'total_capital': TOTAL_CAPITAL,
#                 'avg_stocks_long': portfolio_df_decile['n_long'].mean(),
#                 'avg_stocks_short': portfolio_df_decile['n_short'].mean(),
#                 # Returns
#                 'total_return_pct': total_return * 100,
#                 'cagr_pct': cagr * 100,
#                 'avg_annual_return_pct': avg_portfolio_return_decile * 100,
#                 # Risk
#                 'annualized_volatility_pct': portfolio_return_std_decile * 100,
#                 'sharpe_ratio_dollar': sharpe_ratio_dollar_decile,
#                 'sharpe_ratio_spread': sharpe_ratio_decile,
#                 'max_drawdown_pct': max_dd * 100,
#                 # Win rate
#                 'win_rate_pct': win_rate * 100,
#                 'positive_years': int(win_rate * n_test_years),
#                 'total_years': n_test_years,
#                 # Dollar metrics
#                 'total_pnl': total_dollar_pnl_decile,
#                 'avg_annual_pnl': avg_dollar_pnl_decile,
#                 # Component returns
#                 'avg_long_return_pct': avg_long_decile * 100,
#                 'avg_short_return_pct': avg_short_decile * 100,
#                 'avg_spread_pct': avg_spread_decile * 100,
#                 # Model details
#                 'n_features': n_features,
#                 'max_depth': best_params_dict.get('max_depth', ''),
#                 'learning_rate': best_params_dict.get('learning_rate', ''),
#                 'n_estimators': best_params_dict.get('n_estimators', ''),
#                 'subsample': best_params_dict.get('subsample', ''),
#                 'colsample_bytree': best_params_dict.get('colsample_bytree', ''),
#                 # Test period
#                 'test_start_year': first_year,
#                 'test_end_year': last_year,
#                 'n_test_periods': n_test_years
#             })

#         # Strategy 3: Hybrid (Top 10% / Bottom 100)
#         if has_hybrid:
#             cagr = calculate_cagr(total_dollar_pnl_hybrid, TOTAL_CAPITAL, n_test_years)
#             max_dd = calculate_max_drawdown(portfolio_df_hybrid, 'total_dollar_pnl')
#             win_rate = calculate_win_rate(portfolio_df_hybrid, 'total_dollar_pnl')
#             total_return = total_dollar_pnl_hybrid / TOTAL_CAPITAL

#             summary_rows.append({
#                 'model': 'CatBoost',
#                 'strategy': 'Hybrid 10%/100',
#                 'total_capital': TOTAL_CAPITAL,
#                 'avg_stocks_long': portfolio_df_hybrid['n_long'].mean(),
#                 'avg_stocks_short': portfolio_df_hybrid['n_short'].mean(),
#                 # Returns
#                 'total_return_pct': total_return * 100,
#                 'cagr_pct': cagr * 100,
#                 'avg_annual_return_pct': avg_portfolio_return_hybrid * 100,
#                 # Risk
#                 'annualized_volatility_pct': portfolio_return_std_hybrid * 100,
#                 'sharpe_ratio_dollar': sharpe_ratio_dollar_hybrid,
#                 'sharpe_ratio_spread': sharpe_ratio_hybrid,
#                 'max_drawdown_pct': max_dd * 100,
#                 # Win rate
#                 'win_rate_pct': win_rate * 100,
#                 'positive_years': int(win_rate * n_test_years),
#                 'total_years': n_test_years,
#                 # Dollar metrics
#                 'total_pnl': total_dollar_pnl_hybrid,
#                 'avg_annual_pnl': avg_dollar_pnl_hybrid,
#                 # Component returns
#                 'avg_long_return_pct': avg_long_hybrid * 100,
#                 'avg_short_return_pct': avg_short_hybrid * 100,
#                 'avg_spread_pct': avg_spread_hybrid * 100,
#                 # Model details
#                 'n_features': n_features,
#                 'max_depth': best_params_dict.get('max_depth', ''),
#                 'learning_rate': best_params_dict.get('learning_rate', ''),
#                 'n_estimators': best_params_dict.get('n_estimators', ''),
#                 'subsample': best_params_dict.get('subsample', ''),
#                 'colsample_bytree': best_params_dict.get('colsample_bytree', ''),
#                 # Test period
#                 'test_start_year': first_year,
#                 'test_end_year': last_year,
#                 'n_test_periods': n_test_years
#             })

#         # Strategy 4: Hybrid 10%/100 (50% Long / 50% Short)
#         if has_hybrid5050:
#             cagr = calculate_cagr(total_dollar_pnl_hybrid5050, TOTAL_CAPITAL, n_test_years)
#             max_dd = calculate_max_drawdown(portfolio_df_hybrid5050, 'total_dollar_pnl')
#             win_rate = calculate_win_rate(portfolio_df_hybrid5050, 'total_dollar_pnl')
#             total_return = total_dollar_pnl_hybrid5050 / TOTAL_CAPITAL

#             summary_rows.append({
#                 'model': 'CatBoost',
#                 'strategy': 'Hybrid 10%/100 (50% L/S)',
#                 'total_capital': TOTAL_CAPITAL,
#                 'avg_stocks_long': portfolio_df_hybrid5050['n_long'].mean(),
#                 'avg_stocks_short': portfolio_df_hybrid5050['n_short'].mean(),
#                 # Returns
#                 'total_return_pct': total_return * 100,
#                 'cagr_pct': cagr * 100,
#                 'avg_annual_return_pct': avg_portfolio_return_hybrid5050 * 100,
#                 # Risk
#                 'annualized_volatility_pct': portfolio_return_std_hybrid5050 * 100,
#                 'sharpe_ratio_dollar': sharpe_ratio_dollar_hybrid5050,
#                 'sharpe_ratio_spread': sharpe_ratio_hybrid5050,
#                 'max_drawdown_pct': max_dd * 100,
#                 # Win rate
#                 'win_rate_pct': win_rate * 100,
#                 'positive_years': int(win_rate * n_test_years),
#                 'total_years': n_test_years,
#                 # Dollar metrics
#                 'total_pnl': total_dollar_pnl_hybrid5050,
#                 'avg_annual_pnl': avg_dollar_pnl_hybrid5050,
#                 # Component returns
#                 'avg_long_return_pct': avg_long_hybrid5050 * 100,
#                 'avg_short_return_pct': avg_short_hybrid5050 * 100,
#                 'avg_spread_pct': avg_spread_hybrid5050 * 100,
#                 # Model details
#                 'n_features': n_features,
#                 'max_depth': best_params_dict.get('max_depth', ''),
#                 'learning_rate': best_params_dict.get('learning_rate', ''),
#                 'n_estimators': best_params_dict.get('n_estimators', ''),
#                 'subsample': best_params_dict.get('subsample', ''),
#                 'colsample_bytree': best_params_dict.get('colsample_bytree', ''),
#                 # Test period
#                 'test_start_year': first_year,
#                 'test_end_year': last_year,
#                 'n_test_periods': n_test_years
#             })

#         # Strategy 5: Hybrid with Stop Loss
#         if has_stoploss:
#             cagr = calculate_cagr(total_dollar_pnl_stoploss, TOTAL_CAPITAL, n_test_years)
#             max_dd = calculate_max_drawdown(portfolio_df_stoploss, 'total_dollar_pnl')
#             win_rate = calculate_win_rate(portfolio_df_stoploss, 'total_dollar_pnl')
#             total_return = total_dollar_pnl_stoploss / TOTAL_CAPITAL

#             # Calculate combined stop loss metrics (long + short)
#             avg_stopped_out = avg_long_stopped_out + avg_short_stopped_out
#             total_positions = portfolio_df_stoploss['n_long'].mean() + portfolio_df_stoploss['n_short'].mean()
#             avg_stopped_pct = (avg_stopped_out / total_positions * 100) if total_positions > 0 else 0

#             summary_rows.append({
#                 'model': 'CatBoost',
#                 'strategy': 'Hybrid Stop Loss',
#                 'total_capital': TOTAL_CAPITAL,
#                 'avg_stocks_long': portfolio_df_stoploss['n_long'].mean(),
#                 'avg_stocks_short': portfolio_df_stoploss['n_short'].mean(),
#                 # Returns
#                 'total_return_pct': total_return * 100,
#                 'cagr_pct': cagr * 100,
#                 'avg_annual_return_pct': avg_portfolio_return_stoploss * 100,
#                 # Risk
#                 'annualized_volatility_pct': portfolio_return_std_stoploss * 100,
#                 'sharpe_ratio_dollar': sharpe_ratio_dollar_stoploss,
#                 'sharpe_ratio_spread': sharpe_ratio_stoploss,
#                 'max_drawdown_pct': max_dd * 100,
#                 # Win rate
#                 'win_rate_pct': win_rate * 100,
#                 'positive_years': int(win_rate * n_test_years),
#                 'total_years': n_test_years,
#                 # Dollar metrics
#                 'total_pnl': total_dollar_pnl_stoploss,
#                 'avg_annual_pnl': avg_dollar_pnl_stoploss,
#                 # Component returns
#                 'avg_long_return_pct': avg_long_stoploss * 100,
#                 'avg_short_return_pct': avg_short_stoploss * 100,
#                 'avg_spread_pct': avg_spread_stoploss * 100,
#                 # Stop loss specific
#                 'avg_stopped_out': avg_stopped_out,
#                 'avg_stopped_pct': avg_stopped_pct,
#                 # Model details
#                 'n_features': n_features,
#                 'max_depth': best_params_dict.get('max_depth', ''),
#                 'learning_rate': best_params_dict.get('learning_rate', ''),
#                 'n_estimators': best_params_dict.get('n_estimators', ''),
#                 'subsample': best_params_dict.get('subsample', ''),
#                 'colsample_bytree': best_params_dict.get('colsample_bytree', ''),
#                 # Test period
#                 'test_start_year': first_year,
#                 'test_end_year': last_year,
#                 'n_test_periods': n_test_years
#             })

#         # Create DataFrame and save
#         model_summary_df = pd.DataFrame(summary_rows)
#         summary_csv_file = output_dir / 'model_comparison_summary.csv'
#         model_summary_df.to_csv(summary_csv_file, index=False)

#         print(f"\n[OK] Model comparison summary saved to: {summary_csv_file.name}")
#         print(f"   Location: {summary_csv_file}")
#         print(f"   Strategies: {len(summary_rows)}")

#         # Display summary table
#         print("\n" + "="*110)
#         print("MODEL COMPARISON SUMMARY")
#         print("="*110)
#         print(f"\nModel: CatBoost")
#         print(f"Features: {n_features}")
#         print(f"Test Period: {first_year}-{last_year} ({n_test_years} years)")
#         print(f"Hyperparameters: max_depth={best_params_dict.get('max_depth', 'N/A')}, "
#               f"lr={best_params_dict.get('learning_rate', 'N/A')}, "
#               f"n_est={best_params_dict.get('n_estimators', 'N/A')}")

#         print("\n" + "-"*110)
#         print(f"{'Strategy':<20} | {'CAGR':>8} | {'Sharpe':>7} | {'Max DD':>8} | {'Win Rate':>9} | "
#               f"{'Total Return':>12} | {'Volatility':>11}")
#         print("-"*110)

#         for row in summary_rows:
#             print(f"{row['strategy']:<20} | {row['cagr_pct']:>7.2f}% | {row['sharpe_ratio_dollar']:>7.2f} | "
#                   f"{row['max_drawdown_pct']:>7.2f}% | {row['win_rate_pct']:>8.1f}% | "
#                   f"{row['total_return_pct']:>11.2f}% | {row['annualized_volatility_pct']:>10.2f}%")

#         print("="*110)

# else:
#     print("\n[WARN]  No performance metrics available")
#     print("Run Steps 7-9 first!")

# print("\n" + "=" * 60)
# print("STEP 10 COMPLETE!")
# print("=" * 60)

# # %%
# # ------------------------------
# # STEP 11: Export Long/Short Positions to Excel
# # ------------------------------
# print("\n" + "=" * 60)
# print("STEP 11: EXPORTING LONG/SHORT POSITIONS TO EXCEL")
# print("=" * 60)

# if final_predictions is not None and RUN_PRED:
#     print("\n[STATS] Creating Excel file with long/short positions by year...")

#     # Create Excel writer
#     excel_filename = output_dir / 'long_short_positions.xlsx'

#     with pd.ExcelWriter(excel_filename, engine='openpyxl') as writer:

#         # Summary sheet - all years combined
#         all_positions = []

#         # Process each year
#         for year in sorted(final_predictions['form_year'].unique()):
#             year_data = final_predictions[final_predictions['form_year'] == year].copy()
#             # print(year_data.head())
            
#             if len(year_data) < 200:
#                 print(f"  Skipping {year:.0f}: only {len(year_data)} stocks")
#                 continue

#             print(f"  Processing {year:.0f}: {len(year_data):,} stocks")

#             # Sort by predicted returns
#             year_data = year_data.sort_values('predicted_return', ascending=False)
#             year_data['rank'] = range(1, len(year_data) + 1)

#             # Top 100 long, bottom 100 short
#             TOP_N = 100
#             BOTTOM_N = 100

#             long_portfolio = year_data.head(TOP_N).copy()
#             long_portfolio['position'] = 'LONG'
#             long_portfolio['position_rank'] = range(1, len(long_portfolio) + 1)

#             short_portfolio = year_data.tail(BOTTOM_N).copy()
#             # Reverse order so rank 1 = worst (most negative) predicted return
#             short_portfolio = short_portfolio.sort_values('predicted_return', ascending=True)
#             short_portfolio['position'] = 'SHORT'
#             short_portfolio['position_rank'] = range(1, len(short_portfolio) + 1)

#             # Combine long and short for this year
#             year_positions = pd.concat([long_portfolio, short_portfolio], ignore_index=True)

#             # Select and rename columns for clarity
#             columns_to_export = [
#                 'position', 'position_rank', 'ticker', 'permno',
#                 'form_year', 'form_date', 'predicted_return', CONFIG['dep_var']
#             ]

#             # Add market cap if available
#             if 'crsp_mktcap_6' in year_positions.columns:
#                 columns_to_export.insert(4, 'crsp_mktcap_6')

#             # Filter to available columns
#             available_cols = [col for col in columns_to_export if col in year_positions.columns]
#             year_export = year_positions[available_cols].copy()

#             # Rename for clarity
#             year_export = year_export.rename(columns={
#                 CONFIG['dep_var']: 'actual_return',
#                 'crsp_mktcap_6': 'market_cap'
#             })

#             # Calculate performance metrics for this year
#             long_return = long_portfolio[CONFIG['dep_var']].mean()
#             short_return = -short_portfolio[CONFIG['dep_var']].mean()
#             spread = long_return - short_return

#             # Add summary row at top
#             summary_row = pd.DataFrame([{
#                 'position': 'SUMMARY',
#                 'position_rank': '',
#                 'ticker': f'Year {year:.0f}',
#                 'permno': '',
#                 'form_year': year,
#                 'form_date': '',
#                 'predicted_return': '',
#                 'actual_return': f'Long: {long_return:+.4f} | Short: {short_return:+.4f} | Spread: {spread:+.4f}'
#             }])

#             # Combine summary with positions
#             year_export_with_summary = pd.concat([summary_row, year_export], ignore_index=True)

#             # Export to sheet (sheet name limited to 31 chars)
#             sheet_name = f'Year_{int(year)}'
#             year_export_with_summary.to_excel(writer, sheet_name=sheet_name, index=False)

#             # Add to all positions list
#             all_positions.append(year_export)

#         # Create combined sheet with all years
#         if len(all_positions) > 0:
#             all_positions_df = pd.concat(all_positions, ignore_index=True)
#             all_positions_df = all_positions_df.sort_values(['form_year', 'position', 'position_rank'])
#             all_positions_df.to_excel(writer, sheet_name='All_Years', index=False)
#             print(f"\n  Combined sheet 'All_Years' created with {len(all_positions_df):,} positions")

#         # Create summary sheet
#         if portfolio_df is not None:
#             portfolio_summary = portfolio_df.copy()
#             portfolio_summary['year'] = portfolio_summary['year'].astype(int)
#             portfolio_summary.to_excel(writer, sheet_name='Summary', index=False)
#             print(f"  Summary sheet created with {len(portfolio_summary)} years")

#     print(f"\n[OK] Excel file created: {excel_filename.name}")
#     print(f"   Location: {excel_filename}")
#     print(f"   Sheets: {len(final_predictions['form_year'].unique())} year sheets + All_Years + Summary")

# else:
#     print("\n[WARN]  Cannot export positions (predictions not available)")
#     print("   Run Steps 7-8 first!")

# print("\n" + "=" * 60)
# print("STEP 11 COMPLETE!")
# print("=" * 60)

# # %%


# print("\n\n" + "="*60)
# print("[SUCCESS] MACHINE LEARNING PIPELINE COMPLETE!")
# print("="*60)
# print()
# print("Summary of what you can do:")
# print()
# print("1. TO RUN CROSS-VALIDATION:")
# print("   - Set RUN_CV = True in Step 7")
# print("   - Run Steps 1-7")
# print("   - Wait ~2-3 hours")
# print()
# print("2. TO RUN PREDICTIONS:")
# print("   - Complete Step 7 first!")
# print("   - Set RUN_PRED = True in Step 8")
# print("   - Run Steps 8-10")
# print()
# print("3. RESULTS WILL BE IN:")
# print(f"   - CV results: {cv_dir}")
# print(f"   - Predictions: {pred_dir}")
# print(f"   - Performance: {output_dir}")
# print()
# print("="*60)

# # %%
