# Monthly Stop Loss Implementation

## Overview

This implementation provides realistic monthly stop loss tracking for short positions using CRSP monthly return data. Unlike the simple annual cap approach (Option B in Strategy 5), this tracks positions month-by-month and closes positions when stop loss is hit.

## Files

### Data Preparation
- **`01-prepare-monthly-returns.py`**: Extracts monthly CRSP returns for each stock-year observation
- **Output**: `monthly_returns_2000.parquet` - Reusable across all ML models

### Analysis Engine
- **`04-apply-monthly-stoploss.py`**: Model-agnostic stop loss analyzer
- **Works with**: Any `ml_{model}_results/pred/` directory
- **Configurable**: Stop loss %, strategies, capital allocation

### Documentation
- **`strategy5_monthly_stoploss_plan.txt`**: Detailed implementation plan
- **`README_MONTHLY_STOPLOSS.md`**: This file

## Quick Start

### Step 1: Prepare Monthly Returns Data (One-Time Setup)

```bash
python 01-prepare-monthly-returns.py
```

**What it does:**
- Connects to WRDS and downloads CRSP monthly returns
- Extracts 12 monthly returns for each (permno, form_date) pair
- Calculates cumulative returns at each month
- Saves to `monthly_returns_2000.parquet` (~66K observations)

**Duration:** ~10-15 minutes (depending on WRDS connection)

**Output:**
```
monthly_returns_2000.parquet
Columns:
  - permno, form_date, form_year
  - ret_m1, ret_m2, ..., ret_m12 (individual monthly returns)
  - cum_ret_m1, cum_ret_m2, ..., cum_ret_m12 (cumulative returns)
  - nmonth_available (actual months with data)
```

### Step 2: Run Monthly Stop Loss Analysis

```bash
# Basic usage
python 04-apply-monthly-stoploss.py --model xgboost

# With custom stop loss
python 04-apply-monthly-stoploss.py --model xgboost --stop-loss 0.40

# Apply to long positions too
python 04-apply-monthly-stoploss.py --model xgboost --apply-to-long
```

**What it does:**
- Reads predictions from `ml_xgboost_results/pred/`
- Merges with monthly return data
- Tracks each short position month-by-month
- Closes positions when 50% loss is hit
- Generates comprehensive results and comparisons

**Duration:** ~2-5 minutes

**Output Directory:** `ml_xgboost_results/stoploss/`
```
stoploss/
├── portfolio_monthly_sl.csv        # Year-by-year results
├── summary_monthly_sl.csv          # Overall metrics
├── stop_loss_statistics.csv        # Stop-out details
└── comparison_monthly_vs_annual.csv # Comparison with annual cap
```

## Configuration Options

### Stop Loss Settings

```python
--stop-loss 0.50     # 50% stop loss (default)
--stop-loss 0.40     # 40% stop loss (more conservative)
--stop-loss 0.60     # 60% stop loss (more aggressive)
```

### Position Types

```python
# Shorts only (default)
python 04-apply-monthly-stoploss.py --model xgboost

# Apply to both longs and shorts
python 04-apply-monthly-stoploss.py --model xgboost --apply-to-long
```

### Model Selection

```python
--model xgboost      # XGBoost model results
--model lightgbm     # LightGBM model results
--model nn           # Neural network results
# Works with any model that has ml_{model}_results/pred/ directory
```

## Output Files Explained

### 1. portfolio_monthly_sl.csv
Year-by-year portfolio performance with stop loss statistics.

**Key Columns:**
- `year`: Portfolio formation year
- `long_return`, `short_return`, `spread`: Returns
- `n_short_stopped`: Number of shorts that hit stop loss
- `short_stopped_pct`: Percentage of shorts stopped out
- `avg_short_months`: Average holding period for shorts (1-12 months)
- `total_dollar_pnl`: Total P&L for the year
- `portfolio_return`: Return on total capital

**Example:**
```csv
year,long_return,short_return,n_short_stopped,short_stopped_pct,avg_short_months,total_dollar_pnl
2010,0.15,-0.08,12,12.0%,10.5,70000
2011,0.12,-0.35,45,45.0%,7.2,-115000
```

### 2. summary_monthly_sl.csv
Overall performance metrics across all years.

**Metrics:**
- `total_return_pct`: Cumulative return over test period
- `cagr_pct`: Compound annual growth rate
- `sharpe_ratio`: Risk-adjusted return
- `max_drawdown_pct`: Worst peak-to-trough decline
- `win_rate_pct`: Percentage of profitable years
- `total_pnl`: Total dollar profit/loss

### 3. stop_loss_statistics.csv
Detailed statistics on stop loss triggers.

**Columns:**
- `year`: Year
- `n_short`: Total short positions
- `n_short_stopped`: Positions stopped out
- `short_stopped_pct`: Stop-out rate
- `avg_short_months`: Average holding period

**Use Cases:**
- Identify which years had most stop-outs (bull markets hurt shorts)
- Understand if stops occur early or late in holding period
- Validate stop loss is working as expected

### 4. comparison_monthly_vs_annual.csv
Side-by-side comparison of monthly tracking vs annual cap.

**Expected Results:**
- **Monthly tracking**: LOWER returns (positions exit early)
- **Monthly tracking**: LOWER volatility (risk actually managed)
- **Monthly tracking**: HIGHER or similar Sharpe ratio (better risk-adjusted)

## How Monthly Stop Loss Works

### Position Tracking Algorithm

```python
For each short position:
    cumulative_return = 0

    For month 1 to 12:
        monthly_return = stock's return this month

        # For shorts: we profit when stock goes down
        short_return = -monthly_return

        # Update cumulative
        cumulative_return = (1 + cumulative_return) * (1 + short_return) - 1

        # Check stop loss
        if cumulative_return < -0.50:  # 50% loss
            Close position
            Return: -50% (capped loss)
            Stop month: current month
            Exit loop

    If not stopped:
        Return: cumulative_return over 12 months
```

### Example Walkthrough

**Stock XYZ (Short Position):**
- Form Date: June 2020
- Initial Price: $100

**Monthly Returns:**
```
Month 1 (Jul): +10% → Stock = $110, Short P&L = -10%
Month 2 (Aug): +15% → Stock = $126, Short P&L = -26%
Month 3 (Sep): +20% → Stock = $152, Short P&L = -52%  ⚠️ STOP TRIGGERED
```

**Result:**
- Position closed in Month 3
- Final return: -50% (capped at stop loss)
- Saved 2% in further losses
- Capital freed for 9 months

**Without Stop Loss:**
```
Month 4 (Oct): +5% → Stock = $160, Short P&L = -60%
...
Month 12 (Jun): Stock = $200, Short P&L = -100%  💥
```

## Comparison: Monthly vs Annual Approaches

### Option A: Monthly Tracking (This Implementation)
**Pros:**
- ✅ Realistic: Mimics actual trading behavior
- ✅ Risk Control: Actually limits downside
- ✅ Capital Efficiency: Freed capital can be redeployed
- ✅ Timing Info: Know when positions fail

**Cons:**
- ❌ Lower Returns: Miss potential recovery
- ❌ Complex: Requires monthly data
- ❌ Data Intensive: 12x more data points

### Option B: Annual Cap (Strategy 5 in 03-machine-learning-xgboost.py)
**Pros:**
- ✅ Simple: Works with annual returns
- ✅ Higher Returns: Captures full year
- ✅ Fast: No month-by-month tracking

**Cons:**
- ❌ Unrealistic: Can't exit mid-year in reality
- ❌ Overstates: Doesn't reflect actual risk management
- ❌ No Timing: Don't know when losses occur

## Use Cases

### Research & Analysis
1. **Backtesting Realism**: More accurate representation of live trading
2. **Risk Management**: Understand true impact of stop losses
3. **Strategy Optimization**: Test different stop loss levels

### Portfolio Management
1. **Capital Planning**: Know when capital is freed up
2. **Rebalancing**: Identify opportunities to redeploy capital
3. **Risk Budgeting**: Better estimate of actual risk exposure

### Model Evaluation
1. **Robustness**: Test if models work with active risk management
2. **Comparison**: Compare models under realistic constraints
3. **Stress Testing**: See performance in difficult years

## Troubleshooting

### Error: "Monthly returns file not found"
**Solution:** Run `01-prepare-monthly-returns.py` first

### Error: "Prediction directory not found"
**Solution:** Check model name and ensure predictions exist
```bash
ls ml_xgboost_results/pred/
```

### Warning: "X observations missing all monthly returns"
**Cause:** Some stocks delisted or missing CRSP data
**Impact:** These observations will use annual returns as fallback
**Action:** Usually acceptable if <5% of data

### Error: "WRDS connection failed"
**Solution:** Check WRDS credentials in 01-prepare-monthly-returns.py
```python
WRDS_USERNAME = "your_wrds_username"
```

## Performance Expectations

### Typical Results (50% Stop Loss on Shorts)

**Stop-Out Rates:**
- Average: 15-25% of short positions
- Bull markets: 30-50% stop-out rate
- Bear markets: 5-15% stop-out rate

**Holding Periods:**
- Average: 9-11 months (vs 12 months without stop)
- Stopped positions: 4-6 months
- Non-stopped: 12 months

**Performance Impact:**
- Returns: -5% to -10% vs no stop loss
- Volatility: -10% to -20% reduction
- Sharpe Ratio: +0.1 to +0.3 improvement

## Next Steps

1. ✅ Run `01-prepare-monthly-returns.py` to prepare data
2. ✅ Run `04-apply-monthly-stoploss.py --model xgboost` for analysis
3. 📊 Compare results with Strategy 5 (annual cap)
4. 🔧 Experiment with different stop loss levels (30%, 40%, 60%)
5. 📈 Test with other ML models (lightgbm, etc.)
6. 📝 Document findings and optimal stop loss level

## Advanced Features (Future Enhancements)

Planned but not yet implemented:

1. **Trailing Stop Loss**: Move stop loss up as position profits
2. **Profit Taking**: Close at +30% gain
3. **Reinvestment**: Redistribute stopped capital to active positions
4. **Transaction Costs**: Model trading costs
5. **Liquidity Filters**: Account for bid-ask spread
6. **Multi-Strategy**: Test multiple strategies simultaneously

## References

- **Plan Document**: `strategy5_monthly_stoploss_plan.txt`
- **Option B Implementation**: See Strategy 5 in `03-machine-learning-xgboost.py`
- **CRSP Data**: WRDS CRSP Monthly Stock File
- **Stop Loss Literature**: Kaminski & Lo (2014), Han et al. (2016)

## Questions?

Check the detailed plan: `strategy5_monthly_stoploss_plan.txt`

---
*Last Updated: 2025-10-23*
*Version: 1.0*
