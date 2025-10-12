import numpy as np
import pandas as pd
from pathlib import Path
import pickle
import warnings
from tqdm import tqdm
from sklearn.linear_model import ElasticNetCV
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

warnings.filterwarnings("ignore")

# ==============================================================
# [EN03] STEP 1: SETUP & LOAD PRE-SCALED DATA
# ==============================================================

CONFIG = {
    'method': 'elasticnet',
    'window': 'recursive',          # or 'rolling'
    'cv_train': 7,
    'cv_validation': 1,
    'dep_var': 'expected_return',
    'max_features': 1000,
    'missing_threshold': 0.50,
    'use_top_features': 1000,
}

_base_dir = Path(__file__).resolve().parent
START_YEAR = 2015
scaled_data_file = _base_dir / f"df_scaled_{START_YEAR}.parquet"

print("\n" + "=" * 70)
print("[EN03] STEP 1: LOADING PRE-SCALED DATA")
print("=" * 70)

if not scaled_data_file.exists():
    raise FileNotFoundError(f"❌ [EN03] Scaled data not found: {scaled_data_file}\n"
        "Please run Step 3.5 in 03_machine_learning.py first!"
    )

df = pd.read_parquet(scaled_data_file, engine='fastparquet')
print(f"✅ [EN03] Loaded scaled dataset: {len(df):,} rows, {len(df.columns):,} columns")

# Feature list
features_file = _base_dir / "features_to_keep.pkl"
if features_file.exists():
    with open(features_file, "rb") as f:
        feature_columns = pickle.load(f)
    print(f"✅ [EN03] Loaded {len(feature_columns):,} features from {features_file.name}")
else:
    exclude_cols = ['permno', 'form_date', 'form_year', 'ticker', 'crsp_mktcap_6', 'expected_return', 'counter']
    feature_columns = [c for c in df.columns if c not in exclude_cols]
    print(f"⚠️ [EN03] features_to_keep.pkl not found — using {len(feature_columns):,} columns")

# ==============================================================
# [EN03] FIX: FILTER OUT NON-NUMERIC COLUMNS
# ==============================================================
# Prevent datetime or object-type columns from entering model training
numeric_features = df[feature_columns].select_dtypes(include=[np.number]).columns.tolist()
feature_columns = numeric_features

print(f"✅ [EN03] Using {len(feature_columns):,} numeric feature columns after filtering")

non_numeric = df[feature_columns].select_dtypes(exclude=[np.number]).columns.tolist()
if non_numeric:
    print(f"⚠️ [EN03] Dropped non-numeric columns: {non_numeric[:5]} ... (and others)")


# Define counter range
min_counter = df['counter'].min()
max_counter = df['counter'].max()
MIN_TRAIN_YEARS = 3

if CONFIG['window'] == 'recursive':
    CONFIG['begin'] = min_counter + MIN_TRAIN_YEARS + CONFIG['cv_validation']
else:
    CONFIG['begin'] = min_counter + CONFIG['cv_train'] + CONFIG['cv_validation']
CONFIG['end'] = max_counter

print(f"\n⚙️ [EN03] Training window type: {CONFIG['window']}")
print(f"   [EN03] Prediction range: {CONFIG['begin']} → {CONFIG['end']}")

# ==============================================================
# [EN03] STEP 2: MODEL TRAINING AND PREDICTION
# ==============================================================
print("\n" + "=" * 70)
print("[EN03] STEP 2: MODEL TRAINING AND PREDICTION (ELASTIC NET)")
print("=" * 70)

output_dir = _base_dir/"elasticnet_output"
cv_dir = output_dir / "en_cv_results"
pred_dir = output_dir / "en_predictions"

for folder in [output_dir, cv_dir, pred_dir]:
    folder.mkdir(parents=True, exist_ok=True)

def train_test_data(data, k, config, feature_cols):
    dep_var = config['dep_var']
    if config['window'] == 'rolling':
        train_range = ((data['counter'] <= k) &
                       (data['counter'] >= (k - config['cv_validation'] - config['cv_train'] + 1)))
        
    else:
        train_range = (data["counter"] <= k)
    
    X_train = data.loc[train_range, feature_cols]
    y_train = data.loc[train_range, dep_var]
    valid_idx = y_train.notna()
    X_train, y_train = X_train[valid_idx], y_train[valid_idx]

    # [EN03 FIX] Replace missing feature values with 0 for model stability
    X_train = X_train.fillna(0)

    # [EN03 CHECK] Warn if any NaNs still exist (should never happen)
    if X_train.isna().any().any():
        print(f"⚠️ [EN03] Warning: NaNs detected in training set for counter {k}")


    test_range = (data["counter"] == (k + 1))
    X_test = data.loc[test_range, feature_cols]
    X_test = X_test.fillna(0)
    if X_test.isna().any().any():
        print(f"⚠️ [EN03] Warning: NaNs detected in test set for counter {k+1}")
    key_cols = ["permno", "form_date", "form_year", "ticker", "crsp_mktcap_6", dep_var]
    key_cols_available = [c for c in key_cols if c in data.columns]
    key_test = data.loc[test_range, key_cols_available].reset_index(drop=True)

    return X_train, y_train, X_test, key_test

def output_filename(config, mode="pred", counter=None):
    folder = {"cv": cv_dir, "pred": pred_dir}.get(mode, output_dir)
    parts = [
        config["method"],
        config["window"],
        f"dep_{config['dep_var']}",
        f"val_{config['cv_validation']}",
    ]
    if config["window"] == "rolling":
        parts.append(f"train_{config['cv_train']}")
    if counter is not None:
        parts.append(f"counter_{counter}")
    parts.append(mode)
    
    return folder / ("_".join(parts) + ".csv")

results_all = []

for k in tqdm(range(CONFIG["begin"], CONFIG["end"]), desc="[EN03] ElasticNet rolling"):
    X_train, y_train, X_test, key_test = train_test_data(df, k, CONFIG, feature_columns)
    if X_train.empty or X_test.empty:
        continue

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("enet", ElasticNetCV(
            l1_ratio=[0.1, 0.5, 0.9],
            alphas=np.logspace(-4, 1, 20),
            cv=5,
            n_jobs=-1,
            random_state=42,
            max_iter=5000,
        )),
    ])

    model.fit(X_train, y_train)
    preds = model.predict(X_test)
    key_test["pred"] = preds
    key_test["counter"] = k + 1
    results_all.append(key_test)

    # Save per-period files
    key_test.to_csv(output_filename(CONFIG, "pred", k + 1), index=False)
    enet = model.named_steps["enet"]
    cv_summary = pd.DataFrame({
        "alpha": enet.alphas_,
        "mse_mean": np.mean(enet.mse_path_, axis=1),
        "l1_ratio": enet.l1_ratio_,
    })
    cv_summary.to_csv(output_filename(CONFIG, "cv", k + 1), index=False)

if results_all:
    df_pred = pd.concat(results_all, ignore_index=True)
    out_file = output_filename(CONFIG, "pred")
    df_pred.to_csv(out_file, index=False)
    print(f"\n✅ [EN03] Saved combined predictions: {out_file.name}")
else:
    print("\n⚠️ [EN03] No predictions generated.")

print("\n" + "=" * 70)
print("[EN03] STEP 2 COMPLETE – ELASTIC NET TRAINING FINISHED")
print("=" * 70)


# ==============================================================
# [EN03] STEP 10: EVALUATING PORTFOLIO PERFORMANCE
# ==============================================================

print("\n" + "=" * 70)
print("[EN03] STEP 10: EVALUATING PERFORMANCE")
print("=" * 70)

if 'df_pred' not in locals() or df_pred.empty:
    try:
        df_pred = pd.read_csv(out_file)
    except Exception:
        print("⚠️ [EN03] Could not reload prediction file for performance evaluation.")
        exit()

if not {"pred", CONFIG["dep_var"], "form_year"}.issubset(df_pred.columns):
    print("⚠️ [EN03] Missing columns for portfolio formation.")
    exit()

portfolio_df_fixed = []
portfolio_df_decile = []

for year, grp in df_pred.groupby("form_year"):
    grp = grp.dropna(subset=["pred", CONFIG["dep_var"]]).copy()
    if len(grp) < 50:
        continue

    # ---- Fixed top/bottom 100 ----
    top = grp.nlargest(100, "pred")
    bottom = grp.nsmallest(100, "pred")
    long_ret = top[CONFIG["dep_var"]].mean()
    short_ret = bottom[CONFIG["dep_var"]].mean()
    spread = long_ret - short_ret
    portfolio_df_fixed.append({"year": year, "long_return": long_ret, "short_return": short_ret, "spread": spread})

    # ---- Decile 10% / 5% ----
    n_long = int(len(grp) * 0.10)
    n_short = int(len(grp) * 0.05)
    long = grp.nlargest(n_long, "pred")[CONFIG["dep_var"]].mean()
    short = grp.nsmallest(n_short, "pred")[CONFIG["dep_var"]].mean()
    spread2 = long - short
    portfolio_df_decile.append({"year": year, "long_return": long, "short_return": short, "spread": spread2})

portfolio_df_fixed = pd.DataFrame(portfolio_df_fixed)
portfolio_df_decile = pd.DataFrame(portfolio_df_decile)

def summarize_portfolio(df, name):
    if df.empty:
        print(f"⚠️ [EN03] No data for {name}.")
        return
    avg_long, avg_short, avg_spread = df.mean()[["long_return", "short_return", "spread"]]
    spread_std = df["spread"].std()
    sharpe = avg_spread / spread_std if spread_std > 0 else 0
    print("\n" + "=" * 70)
    print(f"[EN03] {name} RESULTS")
    print("=" * 70)
    print(f"Avg Long Return:  {avg_long:+.4f}")
    print(f"Avg Short Return: {avg_short:+.4f}")
    print(f"Avg Spread:       {avg_spread:+.4f}")
    print(f"Sharpe Ratio:     {sharpe:.2f}")
    print(f"Period: {int(df['year'].min())}-{int(df['year'].max())}")
    print("=" * 70)
    return {"portfolio_type": name, "avg_long": avg_long, "avg_short": avg_short,
            "avg_spread": avg_spread, "sharpe": sharpe}

summary_fixed = summarize_portfolio(portfolio_df_fixed, "Fixed Top 100 / Bottom 100")
summary_decile = summarize_portfolio(portfolio_df_decile, "Decile 10% / 5%")

summary_df = pd.DataFrame([summary_fixed, summary_decile])
summary_file = output_dir / "elasticnet_performance_summary.csv"
summary_df.to_csv(summary_file, index=False)
print(f"\n✅ [EN03] Performance summary saved to {summary_file.name}")

print("\n" + "=" * 70)
print("[EN03] STEP 10 COMPLETE!")
print("=" * 70)