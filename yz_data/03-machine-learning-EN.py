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
import gdown

# Machine Learning libraries
import xgboost as xgb
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
_base_dir = Path.cwd()
START_YEAR = 2000

# Set Google Drive file ID
file_id = "1iAUUKU9CjkRMmmNCTioBQrdV8g21A9ej"
output_file = f"signals_with_returns_and_tickers_{START_YEAR}.parquet"

# Download the file if not already present
data_path = Path("yz_data") / output_file
data_path.parent.mkdir(exist_ok=True)  # make folder if not exists

if not data_path.exists():
    url = f"https://drive.google.com/uc?id={file_id}"
    print(f"Downloading {output_file} from Google Drive...")
    gdown.download(url, str(data_path), quiet=False)
else:
    print(f"{output_file} already exists. Skipping download.")

# ------------------------------
# 1.2  Load the parquet file
# ------------------------------
print(f"Loading data from {data_path} ...")
df = pd.read_parquet(data_path, engine="fastparquet")
print("Data loaded successfully!")
print(df.head())

# ------------------------------
# 1.3 Create output directories
# ------------------------------
output_dir = _base_dir / "ml_xgboost_results"
cv_dir = output_dir / "CV"
pred_dir = output_dir / "Pred"

output_dir.mkdir(exist_ok=True)
cv_dir.mkdir(exist_ok=True)
pred_dir.mkdir(exist_ok=True)

print(f"Data path: {data_path}")
print(f"Output directory: {output_dir}")
print(f"CV directory: {cv_dir}")
print(f"Prediction directory: {pred_dir}")

# ------------------------------
# 1.4 Load data
# ------------------------------
print(f"\nLoading data from: {data_path}")
df = pd.read_parquet(data_path, engine="fastparquet")

print(f"Data loaded successfully!")
print(f"Dataset shape: {df.shape}")
print(f"Columns: {len(df.columns)}")
print(f"Rows: {len(df):,}")

# ------------------------------
# 1.5 Ensure date columns are datetime
# ------------------------------
for c in ("datadate", "form_date"):
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

print(f"\nDate range: {df['form_date'].min()} to {df['form_date'].max()}")

# ------------------------------
# 1.6 Calculate expected_return if not exists
# ------------------------------
if 'expected_return' not in df.columns:
    print("\nCalculating expected_return = ret - rf...")
    df['expected_return'] = df['ret'] - df['rf']
else:
    print("\nExpected return already exists")

print(f"Expected return - Mean: {df['expected_return'].mean():.4f}, Std: {df['expected_return'].std():.4f}")

# ------------------------------
# 1.7 Identify base columns vs feature columns
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