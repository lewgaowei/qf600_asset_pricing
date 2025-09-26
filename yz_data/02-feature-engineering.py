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

START_YEAR = 2000
require_wrds_load = False
db = None
ALLOW_WRDS_LOOKUP = False  # set True only if WRDS is available

# %%

# %%
# ------------------------------
# 1. Load final signals-with-returns parquet
# ------------------------------
from pathlib import Path

print("Loading formed panel with signals and forward returns ...")

_base_dir = Path(__file__).resolve().parent
_cand1 = _base_dir / f"signals_with_returns_formed_{START_YEAR}.parquet"
_cand2 = _base_dir / f"signals_with_returns_{START_YEAR}.parquet"

if _cand1.exists():
    data_path = _cand1
elif _cand2.exists():
    data_path = _cand2
else:
    raise FileNotFoundError("Could not find signals parquet. Expected one of: "
                            f"{_cand1.name}, { _cand2.name }")

df = pd.read_parquet(data_path, engine="fastparquet")

for c in ("datadate", "form_date"):
    if c in df.columns:
        df[c] = pd.to_datetime(df[c], errors="coerce")

# Cohort index (June-anchored) if not present
if "mindex_form" not in df.columns and "form_date" in df.columns:
    df["mindex_form"] = df["form_date"].dt.year * 12 + df["form_date"].dt.month

# Compute excess return if not present
if "exret" not in df.columns and {"ret", "rf"}.issubset(df.columns):
    df["exret"] = df["ret"] - df["rf"]

print(f"Loaded: {data_path.name} shape={df.shape}")
end_step("Load parquet")


# %%
# ------------------------------
# 2. Basic EDA
# ------------------------------

print({
    "rows": len(df),
    "cols": df.shape[1],
    "date_range": (
        df.get("form_date").min() if "form_date" in df.columns else None,
        df.get("form_date").max() if "form_date" in df.columns else None,
    ),
    "permno_unique": int(df.get("permno").nunique()) if "permno" in df.columns else None,
    "gvkey_unique": int(df.get("gvkey").nunique()) if "gvkey" in df.columns else None,
    "exret_missing_pct": float(df["exret"].isna().mean()*100) if "exret" in df.columns else None,
})

if "nmonth" in df.columns:
    print("nmonth summary:", df["nmonth"].describe())

with pd.option_context('display.width', 2000, 'display.max_columns', 20):
    print(df.head())

end_step("EDA")


# %%
# ------------------------------
# 2a. Show all Tesla rows in df
# ------------------------------

tesla_rows = pd.DataFrame()

if "ticker" in df.columns:
    tesla_rows = df[df["ticker"].astype(str).str.upper() == "TSLA"]
elif "comnam" in df.columns:
    tesla_rows = df[df["comnam"].astype(str).str.upper().str.contains("TESLA", na=False)]
elif "permno" in df.columns:
    # Fallback 1: use known Tesla PERMNO (93436) present in your output
    tesla_rows = df[df["permno"].astype(float) == 93436.0]
    # Fallback 2: use known Tesla GVKEY (184996) if still empty and gvkey exists
    if len(tesla_rows) == 0 and "gvkey" in df.columns:
        tesla_rows = df[df["gvkey"].astype(float) == 184996.0]
    # Optional fallback 3: WRDS lookup (disabled by default)
    if len(tesla_rows) == 0 and ALLOW_WRDS_LOOKUP:
        try:
            if db is None:
                db = wrds.Connection(wrds_username="lewgaowei")
            stocknames = db.get_table("crsp", "stocknames",
                                      columns=["permno","namedt","nameenddt","ticker","comnam"])
            stocknames["ticker"] = stocknames["ticker"].astype(str).str.upper()
            tsla_permnos = (stocknames.loc[stocknames["ticker"] == "TSLA", "permno"]
                                        .dropna().unique().tolist())
            if len(tsla_permnos) > 0:
                tesla_rows = df[df["permno"].isin(tsla_permnos)].copy()
        except Exception:
            pass

print(f"Tesla rows found: {len(tesla_rows)}")
with pd.option_context('display.width', 2000, 'display.max_columns', None):
    if len(tesla_rows):
        # Compute Compustat mktcap if possible and not present
        if "mktcap" not in tesla_rows.columns and {"csho","prcc_f"}.issubset(tesla_rows.columns):
            tesla_rows = tesla_rows.copy()
            tesla_rows["mktcap"] = tesla_rows["csho"] * tesla_rows["prcc_f"]

        # try to sort by date column if available
        sort_cols = [c for c in ["form_date", "mindex_form", "permno"] if c in tesla_rows.columns]

        # concise subset for display
        show_cols = [c for c in [
            "permno","gvkey","ticker","comnam",
            "form_date","mindex_form",
            "crsp_mktcap_6","crsp_mktcap_12","mktcap","csho","prcc_f",
            "ret","rf","exret","nmonth"
        ] if c in tesla_rows.columns]

        tesla_sorted = tesla_rows.sort_values(sort_cols)
        print(tesla_sorted[show_cols].head(200))

        # Implied net share issuance between consecutive June formations:
        # mktcap_{t+1} ≈ mktcap_t * (1 + ret_t) * (1 + issuance_t) →
        # issuance_t ≈ mktcap_{t+1} / (mktcap_t * (1 + ret_t)) - 1
        if {"crsp_mktcap_6","ret"}.issubset(tesla_sorted.columns):
            tmp = tesla_sorted.copy()
            tmp["crsp_mktcap_6_next"] = (
                tmp.groupby("permno")["crsp_mktcap_6"].shift(-1)
            )
            denom = (tmp["crsp_mktcap_6"] * (1.0 + tmp["ret"]))
            tmp["implied_net_issuance"] = tmp["crsp_mktcap_6_next"] / denom - 1
            iss_cols = [c for c in [
                "permno","form_date","crsp_mktcap_6","ret","crsp_mktcap_6_next","implied_net_issuance"
            ] if c in tmp.columns]
            print("Implied net issuance (June→next June):")
            print(tmp[iss_cols].head(20))
    else:
        print("No Tesla rows found. Consider merging tickers into the panel.")

end_step("Show Tesla rows")

# %%
# ------------------------------
# 3. Define feature set and cross-sectional scaling per formation date
# ------------------------------

id_cols = [
    c for c in ["gvkey","permno","datadate","form_date","mindex_form","crsp_mktcap_6"]
    if c in df.columns
]
target_cols = [c for c in ["ret","rf","exret","nmonth"] if c in df.columns]

# Candidate features: numeric, exclude ids/targets
num_cols = [c for c, t in df.dtypes.items() if np.issubdtype(t, np.number)]
feature_cols = [c for c in num_cols if c not in set(id_cols + target_cols)]

# Limit feature count for a light demo (can be increased later)
MAX_FEATURES = 300
feature_cols = feature_cols[:MAX_FEATURES]

print(f"Using {len(feature_cols)} features for demo analysis")

def cs_rank_to_minus1_plus1(values: pd.Series) -> pd.Series:
    ranks = values.rank(method="average", na_option="keep")
    n = ranks.notna().sum()
    if n <= 1:
        return pd.Series(np.zeros(len(values)), index=values.index)
    scaled = (ranks - 1) / (n - 1)  # [0,1]
    scaled = scaled * 2 - 1         # [-1,1]
    return scaled.fillna(0.0)

def cs_transform(df_in: pd.DataFrame, features: list, date_col: str) -> pd.DataFrame:
    out = df_in.copy()
    # Rank-scale each feature cross-sectionally by formation date
    for f in features:
        out[f"{f}__cs"] = (
            out.groupby(date_col, group_keys=False)[f]
               .apply(cs_rank_to_minus1_plus1)
               .astype(float)
        )
    return out

date_col = "form_date" if "form_date" in df.columns else "mindex_form"
df = cs_transform(df, feature_cols, date_col)

scaled_features = [f"{f}__cs" for f in feature_cols]

end_step("Cross-sectional scaling")


# %%
# ------------------------------
# 4. Univariate quantile long-short portfolios (value-weighted if possible)
# ------------------------------

def quantile_ls_returns(panel: pd.DataFrame, score_col: str, ret_col: str,
                        date_col: str, weight_col: str | None, q: int = 10) -> pd.Series:
    def _one_date(g: pd.DataFrame) -> float:
        g = g.copy()
        g["_score_q"] = pd.qcut(g[score_col].rank(method="first"), q=q, labels=False, duplicates="drop")
        lo = g[g["_score_q"] == 0]
        hi = g[g["_score_q"] == g["_score_q"].max()]
        if weight_col and weight_col in g.columns:
            w_lo = lo[weight_col].clip(lower=0)
            w_hi = hi[weight_col].clip(lower=0)
            w_lo = w_lo / (w_lo.sum() if w_lo.sum() != 0 else 1)
            w_hi = w_hi / (w_hi.sum() if w_hi.sum() != 0 else 1)
            r_lo = (w_lo * lo[ret_col]).sum()
            r_hi = (w_hi * hi[ret_col]).sum()
        else:
            r_lo = lo[ret_col].mean()
            r_hi = hi[ret_col].mean()
        return float(r_hi - r_lo)

    ts = panel.dropna(subset=[score_col, ret_col]).groupby(date_col).apply(_one_date)
    ts.name = f"LS_{score_col}"
    return ts

sample_feats = scaled_features[:5]
ls_results = {}
for f in sample_feats:
    ls_ts = quantile_ls_returns(
        df,
        score_col=f,
        ret_col="exret" if "exret" in df.columns else "ret",
        date_col=date_col,
        weight_col="crsp_mktcap_6" if "crsp_mktcap_6" in df.columns else None,
        q=10,
    )
    ls_results[f] = ls_ts

ls_df = pd.concat(ls_results.values(), axis=1)
print("Univariate LS preview:")
print(ls_df.tail())

end_step("Univariate long-short")


# %%
# ------------------------------
# 5. Rolling Ridge model (expanding window) → prediction long-short
# ------------------------------

try:
    from sklearn.linear_model import Ridge
    SK_OK = True
except Exception:
    SK_OK = False

def ridge_fit_predict(X_train: np.ndarray, y_train: np.ndarray,
                      X_test: np.ndarray, alpha: float = 1.0) -> np.ndarray:
    if SK_OK:
        model = Ridge(alpha=alpha, fit_intercept=True, random_state=0)
        model.fit(X_train, y_train)
        return model.predict(X_test)
    # Numpy fallback: (X'X + aI)^{-1} X'y
    xtx = X_train.T @ X_train
    aI = np.eye(xtx.shape[0]) * alpha
    beta = np.linalg.pinv(xtx + aI) @ (X_train.T @ y_train)
    return X_test @ beta

def build_ridge_ls(panel: pd.DataFrame, features: list, date_col: str,
                   ret_col: str, weight_col: str | None,
                   min_train: int = 24, alpha: float = 1.0, q: int = 10) -> pd.Series:
    dates = pd.Index(sorted(panel[date_col].dropna().unique()))
    rets = []
    for i in range(len(dates)):
        if i < min_train:
            continue
        train_dates = dates[:i]
        test_date = dates[i]

        train = panel[panel[date_col].isin(train_dates)].dropna(subset=features + [ret_col])
        test = panel[panel[date_col] == test_date].dropna(subset=features)
        if len(train) == 0 or len(test) == 0:
            continue

        X_tr = train[features].to_numpy(dtype=float)
        y_tr = train[ret_col].to_numpy(dtype=float)
        X_te = test[features].to_numpy(dtype=float)

        preds = ridge_fit_predict(X_tr, y_tr, X_te, alpha=alpha)
        test = test.copy()
        test["__score"] = preds

        ls_t = quantile_ls_returns(test, "__score", ret_col, date_col, weight_col, q=q)
        rets.append(ls_t)

    if len(rets) == 0:
        return pd.Series(dtype=float)
    out = pd.concat(rets).sort_index()
    out.name = "LS_ridge"
    return out

ridge_features = scaled_features[:50]  # use first 50 scaled features for speed
ridge_ls = build_ridge_ls(
    df,
    features=ridge_features,
    date_col=date_col,
    ret_col="exret" if "exret" in df.columns else "ret",
    weight_col="crsp_mktcap_6" if "crsp_mktcap_6" in df.columns else None,
    min_train=24,
    alpha=1.0,
    q=10,
)

print("Ridge LS preview:")
print(ridge_ls.tail())

end_step("Ridge long-short")


# %%
# ------------------------------
# 6. Evaluation metrics
# ------------------------------

def compute_metrics(ts: pd.Series, periods_per_year: int = 1) -> dict:
    ts = ts.dropna()
    if len(ts) == 0:
        return {"n": 0}
    mu = ts.mean() * periods_per_year
    sigma = ts.std(ddof=1) * np.sqrt(periods_per_year)
    sharpe = mu / sigma if sigma != 0 else np.nan
    # Max drawdown on cumulative wealth (1+R)
    wealth = (1 + ts).cumprod()
    cummax = wealth.cummax()
    mdd = ((wealth / cummax) - 1).min()
    return {
        "n": int(len(ts)),
        "ann_return": float(mu),
        "ann_vol": float(sigma),
        "sharpe": float(sharpe),
        "max_drawdown": float(mdd),
    }

metrics = {col: compute_metrics(ls_df[col].dropna()) for col in ls_df.columns}
if len(ridge_ls) > 0:
    metrics["LS_ridge"] = compute_metrics(ridge_ls.dropna())

print("Metrics:")
for k, v in metrics.items():
    print(k, v)

end_step("Evaluation metrics")


# %%
# ------------------------------
# 7. Save outputs (CSVs)
# ------------------------------

out_dir = _base_dir
ls_df.to_csv(out_dir / f"univariate_ls_{START_YEAR}.csv")
if len(ridge_ls) > 0:
    ridge_ls.to_csv(out_dir / f"ridge_ls_{START_YEAR}.csv")
pd.DataFrame(metrics).to_csv(out_dir / f"metrics_{START_YEAR}.csv")

print("Saved:", {
    "univariate_ls": f"univariate_ls_{START_YEAR}.csv",
    "ridge_ls": f"ridge_ls_{START_YEAR}.csv" if len(ridge_ls) > 0 else None,
    "metrics": f"metrics_{START_YEAR}.csv",
})

end_step("Save outputs")