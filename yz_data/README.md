# Asset Pricing Machine Learning Pipeline

This folder contains machine learning models for predicting stock returns using accounting-based features.

## Table of Contents
- [Forward-Looking Bias Verification](#forward-looking-bias-verification)
- [Model Comparison Summary](#model-comparison-summary)
- [Portfolio Strategies](#portfolio-strategies)
- [Data Timeline](#data-timeline)

---

## Forward-Looking Bias Verification

### ✅ **VERIFIED: No Forward-Looking Bias**

The model predictions are **properly constructed** with no look-ahead bias. Here's why:

### Data Timeline

```
Timeline for a stock in year Y:
│
├─ June Y (form_date)
│  │
│  ├─ Financial Statement Filed
│  │  └─ Features: Accounting ratios calculated from 10-K data
│  │
│  ├─ Model Prediction Made
│  │  └─ Predict returns for July Y → June Y+1
│  │
│  └─ Portfolio Formation
│     └─ Rank stocks and select long/short positions
│
└─ July Y → June Y+1 (12-month holding period)
   │
   └─ Actual Returns Realized
      └─ ret: Buy-and-hold return over 12 months
```

### Key Points

1. **Features are known BEFORE prediction**
   - Accounting data from financial statements filed by June Y
   - All ratios calculated from historical data available at form_date

2. **Returns occur AFTER prediction**
   - 12-month forward returns from July Y → June Y+1
   - Returns are measured AFTER portfolio formation date

3. **Time-Series Cross-Validation**
   - At counter k: Train on data from periods 1 to k
   - At counter k+1: Test on data from period k+1 (unseen during training)
   - No overlap between training and test sets

### Why This Is Correct

This follows standard academic methodology:
- **Fama-MacBeth (1973)**: Annual portfolio sorts on accounting variables
- **Asset Pricing Literature**: Features from fiscal year t predict returns from July t+1 → June t+2
- **Realistic Trading**: You observe June financial statements → Make predictions for next 12 months

### Data Construction

The returns are constructed in `01-fssignals.py`:

```python
# Step 11: Build 12-month forward returns (July→June)
# Anchor to June formation date
# Returns measured from July Y → June Y+1
```

**Timeline Check:**
- `form_date`: Date when features become available (June Y)
- `ret`: 12-month return starting July Y (AFTER form_date)
- `expected_return`: ret - rf (excess return)

---

## Model Comparison Summary

### Overview

All models generate a standardized comparison file: `model_comparison_summary.csv`

This file contains **one row per strategy** with comprehensive performance metrics, making it easy to compare different ML models.

### File Location

```
ml_xgboost_results/model_comparison_summary.csv
ml_lightgbm_results/model_comparison_summary.csv
ml_svr_results/model_comparison_summary.csv
... (other models)
```

### Metrics Included

#### Returns
- **total_return_pct**: Cumulative return over test period (%)
- **cagr_pct**: Compound Annual Growth Rate (%)
- **avg_annual_return_pct**: Average annual portfolio return (%)

#### Risk Metrics
- **annualized_volatility_pct**: Standard deviation of annual returns (%)
- **sharpe_ratio_dollar**: Dollar-weighted Sharpe ratio (accounts for diversification)
- **sharpe_ratio_spread**: Spread-based Sharpe ratio (long return - short return)
- **max_drawdown_pct**: Maximum peak-to-trough decline (%)

#### Win Rates & Consistency
- **win_rate_pct**: Percentage of profitable years (%)
- **positive_years**: Number of years with positive P&L
- **total_years**: Total number of test years

#### Portfolio Statistics
- **avg_stocks_long**: Average number of stocks in long portfolio
- **avg_stocks_short**: Average number of stocks in short portfolio
- **total_pnl**: Total dollar P&L over test period
- **avg_annual_pnl**: Average annual dollar P&L

#### Component Returns
- **avg_long_return_pct**: Average annual long portfolio return (%)
- **avg_short_return_pct**: Average annual short portfolio return (%)
- **avg_spread_pct**: Average annual long-short spread (%)

#### Model Details
- **n_features**: Number of features used
- **max_depth**: Tree depth (for tree-based models)
- **learning_rate**: Learning rate hyperparameter
- **n_estimators**: Number of trees/estimators
- **test_start_year**: First year of test period
- **test_end_year**: Last year of test period

### How to Compare Models

#### Example: Load and Compare Results

```python
import pandas as pd

# Load results from different models
xgb = pd.read_csv('ml_xgboost_results/model_comparison_summary.csv')
lgb = pd.read_csv('ml_lightgbm_results/model_comparison_summary.csv')
svr = pd.read_csv('ml_svr_results/model_comparison_summary.csv')

# Combine all results
all_models = pd.concat([xgb, lgb, svr], ignore_index=True)

# Sort by Sharpe ratio (descending)
all_models_sorted = all_models.sort_values('sharpe_ratio_dollar', ascending=False)

# Display top performers
print(all_models_sorted[['model', 'strategy', 'cagr_pct', 'sharpe_ratio_dollar',
                          'max_drawdown_pct', 'win_rate_pct']])
```

#### Example Output

```
     model              strategy  cagr_pct  sharpe_ratio_dollar  max_drawdown_pct  win_rate_pct
0  XGBoost        Fixed 100/100      8.45                 0.85            -12.30          75.0
1  LightGBM       Fixed 100/100      7.92                 0.78            -15.20          70.0
2  XGBoost       Decile 10%/10%      9.12                 0.92             -9.80          80.0
...
```

### Comparing Strategies Within a Model

```python
# Load XGBoost results
xgb = pd.read_csv('ml_xgboost_results/model_comparison_summary.csv')

# Compare all 4 strategies
print("\nXGBoost Strategy Comparison:")
print(xgb[['strategy', 'sharpe_ratio_dollar', 'cagr_pct', 'max_drawdown_pct']])
```

### Key Metrics for Comparison

**For Performance:**
- `cagr_pct`: Overall return (higher is better)
- `sharpe_ratio_dollar`: Risk-adjusted return (higher is better)

**For Risk:**
- `max_drawdown_pct`: Worst decline (closer to 0 is better)
- `annualized_volatility_pct`: Consistency (lower is better)

**For Reliability:**
- `win_rate_pct`: Percentage of profitable years (higher is better)
- `positive_years / total_years`: Track record consistency

---

## Portfolio Strategies

The models evaluate 4 different long-short portfolio strategies:

### Strategy 1: Fixed 100/100
- **Selection**: Top 100 stocks (long) / Bottom 100 stocks (short)
- **Capital Allocation**: Proportional to number of positions
  - If 100 long & 100 short → 50% capital each side
- **Best For**: Fixed portfolio size, consistent position count

### Strategy 2: Decile 10%/10%
- **Selection**: Top 10% of stocks (long) / Bottom 10% of stocks (short)
- **Capital Allocation**: Proportional to number of positions
  - If universe has 1000 stocks → 100 long, 100 short
- **Best For**: Adapting to changing universe size

### Strategy 3: Hybrid 10%/100
- **Selection**: Top 10% of stocks (long) / Bottom 100 stocks (short)
- **Capital Allocation**: Proportional to number of positions
  - Long typically gets more capital (more stocks)
- **Best For**: Asymmetric conviction (more confidence in long side)

### Strategy 4: Hybrid 10%/100 (50% L/S)
- **Selection**: Same as Strategy 3 (Top 10% / Bottom 100)
- **Capital Allocation**: Market neutral - 50% long / 50% short (fixed)
  - Regardless of position count, capital split is always 50/50
- **Best For**: Market neutral strategies, equal long/short exposure

### Capital Allocation Details

**Total Capital**: $1,000,000

**Flexible Allocation (Strategies 1-3):**
```
long_capital = $1M × (n_long / (n_long + n_short))
short_capital = $1M × (n_short / (n_long + n_short))
```

**50/50 Allocation (Strategy 4):**
```
long_capital = $500,000 (fixed)
short_capital = $500,000 (fixed)
```

**Position Sizing (all strategies):**
```
position_size_long = long_capital / n_long
position_size_short = short_capital / n_short
```

### Portfolio Return Calculation

**Dollar P&L:**
```python
dollar_pnl_long = long_capital × long_return
dollar_pnl_short = short_capital × short_return
total_dollar_pnl = dollar_pnl_long + dollar_pnl_short
```

**Portfolio Return:**
```python
portfolio_return = total_dollar_pnl / $1,000,000
```

---

## Data Structure

### Input Files

- `scaled_data_mc_85_*.parquet`: Scaled features after multicollinearity filtering
- Features are standardized (z-scored) for ML training

### Output Files

#### Cross-Validation Results
```
ml_xgboost_results/cv/cv_*.csv
```
- Hyperparameter tuning results for each time period
- Columns: n_estimators, learning_rate, max_depth, mse, r2_score

#### Predictions
```
ml_xgboost_results/pred/pred_*.csv
```
- Out-of-sample predictions for each time period
- Columns: permno, ticker, form_year, form_date, predicted_return, expected_return

#### Portfolio Performance
```
ml_xgboost_results/
├── portfolio_returns_fixed_100.csv          (Strategy 1)
├── portfolio_returns_decile10pct.csv        (Strategy 2)
├── portfolio_returns_hybrid_10pct_100.csv   (Strategy 3)
└── portfolio_returns_hybrid_10pct_100_5050.csv (Strategy 4)
```
- Year-by-year portfolio returns and statistics
- Columns: year, long_return, short_return, spread, total_dollar_pnl, portfolio_return

#### Summary Files
```
ml_xgboost_results/
├── performance_summary_fixed_100.csv
├── performance_summary_decile10pct.csv
├── performance_summary_hybrid_10pct_100.csv
├── performance_summary_hybrid_10pct_100_5050.csv
├── performance_comparison_all.csv
└── model_comparison_summary.csv            ← **Use this for model comparison**
```

#### Excel Export
```
ml_xgboost_results/long_short_positions.xlsx
```
- Long/short positions for each year
- One sheet per year + summary sheet

---

## Time-Series Cross-Validation

### Counter System

The data is organized by `counter` (sequential time periods):

```
counter = 1 → form_year = 1990
counter = 2 → form_year = 1991
...
counter = 30 → form_year = 2019
```

### Cross-Validation Process

For each test period k+1:

**Step 1: Cross-Validation (on period k)**
```
Train:      periods 1 to k-1
Validate:   period k
Tune:       hyperparameters (n_estimators, learning_rate, max_depth)
Save:       cv_results to cv/cv_{k+1}.csv
```

**Step 2: Final Prediction (on period k+1)**
```
Train:      periods 1 to k (includes validation period!)
Test:       period k+1
Predict:    returns for period k+1
Save:       predictions to pred/pred_{k+1}.csv
```

### Why k+1 in filenames?

Files are named by **test period** (k+1) for easier lookup:
- `cv_15.csv` → Hyperparameters tuned on periods 1-14, for testing on period 15
- `pred_15.csv` → Predictions for period 15

No look-ahead bias because:
- Hyperparameters chosen using only data up to period k
- Test data (k+1) never seen during training or validation

---

## Sharpe Ratio Comparison

### Two Types of Sharpe Ratios

**1. Sharpe Ratio (Spread-based)**
```
sharpe_ratio_spread = mean(long_return - short_return) / std(spread)
```
- Measures stock selection skill
- Uses volatility of the SPREAD between long and short
- Does NOT account for diversification benefits

**2. Sharpe Ratio (Dollar-based)**
```
sharpe_ratio_dollar = mean(portfolio_return) / std(portfolio_return)
```
- Measures actual portfolio performance
- Uses volatility of the weighted portfolio return
- DOES account for diversification benefits

### Why Dollar-based Sharpe is Usually Higher

The portfolio volatility is typically **lower** than spread volatility because:
- Long and short positions partially offset each other
- Diversification across many stocks reduces total risk
- Position sizing spreads risk evenly

**Example:**
```
Spread volatility:    17.6%  → Sharpe (Spread) = 0.28
Portfolio volatility: 10.2%  → Sharpe (Dollar) = 0.66
```

The dollar-based Sharpe properly credits the risk reduction from diversification.

---

## Notes

- All returns are excess returns (ret - rf)
- Test period typically starts after sufficient training data (e.g., 10+ years)
- Hyperparameters are tuned separately for each time period (walk-forward optimization)
- No feature selection is done on test data (RFE uses only training data)

---

## References

- Fama, E. F., & French, K. R. (1992). The cross-section of expected stock returns.
- Gu, S., Kelly, B., & Xiu, D. (2020). Empirical asset pricing via machine learning.

