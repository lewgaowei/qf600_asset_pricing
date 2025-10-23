# 05-backtesting.py - Centralized Backtesting Guide

## 📋 Overview

This script provides centralized backtesting for multiple ML models across 6 portfolio strategies. It separates backtesting logic from model training, making it easy to run comprehensive analysis on all your models at once.

---

## 🚀 Quick Start

### 1. Configure Your Models

Edit the `CONFIG['models']` section in `05-backtesting.py`:

```python
'models': {
    'xgboost': {
        'name': 'XGBoost',
        'pred_dir': 'ml_xgboost_results/Pred',
        'pred_file_pattern': 'brt_*.csv',
        'enabled': True
    },
    'lightgbm': {
        'name': 'LightGBM',
        'pred_dir': 'ml_lightgbm_results/Pred',
        'pred_file_pattern': 'lgb_*.csv',
        'enabled': True
    },
    # Add more models...
}
```

### 2. Enable/Disable Strategies

Toggle strategies in `CONFIG['strategies']`:

```python
'strategies': {
    'fixed_100': True,           # Strategy 1: Fixed 100/100
    'decile_10pct': True,        # Strategy 2: Decile 10%/10%
    'hybrid_10_100': True,       # Strategy 3: Hybrid 10%/100
    'hybrid_5050': True,         # Strategy 4: Hybrid 50/50 split
    'annual_stoploss': True,     # Strategy 5: Annual stop loss
    'monthly_stoploss': True     # Strategy 6: Monthly tracking
}
```

### 3. Run the Script

Open in Jupyter or VSCode with Jupyter extension:
```bash
# Run all cells
```

---

## 📊 Strategies Explained

### Strategy 1: Fixed 100/100
- Long: Top 100 stocks
- Short: Bottom 100 stocks
- Capital: Proportional allocation

### Strategy 2: Decile 10%/10%
- Long: Top 10% of stocks
- Short: Bottom 10% of stocks
- Capital: Proportional allocation

### Strategy 3: Hybrid 10%/100
- Long: Top 10% of stocks
- Short: Bottom 100 stocks
- Capital: Proportional allocation

### Strategy 4: Hybrid 10%/100 (50/50 Split)
- Long: Top 10% of stocks (50% of capital)
- Short: Bottom 100 stocks (50% of capital)
- Capital: Fixed 50/50 split

### Strategy 5: Annual Stop Loss
- Same as Strategy 4, but with stop loss applied to annual returns
- Stop loss: -50% (configurable)
- Slippage: +5% (optional, configurable)

### Strategy 6: Monthly Stop Loss
- Same as Strategy 4, but with month-by-month position tracking
- Stop loss checked each month on cumulative returns
- More granular risk management
- Requires `monthly_returns_2000.parquet` file

---

## 🗂️ Output Structure

```
final_output/
├── models/                          # Per-model results
│   ├── xgboost/
│   │   ├── portfolio_fixed_100.csv          # Strategy 1 details
│   │   ├── portfolio_decile_10pct.csv       # Strategy 2 details
│   │   ├── portfolio_hybrid_10_100.csv      # Strategy 3 details
│   │   ├── portfolio_hybrid_5050.csv        # Strategy 4 details
│   │   ├── portfolio_annual_stoploss.csv    # Strategy 5 details
│   │   ├── portfolio_monthly_stoploss.csv   # Strategy 6 details
│   │   └── summary_xgboost.csv              # All strategies summary
│   ├── lightgbm/
│   │   └── ...
│   └── ...
├── comparisons/                     # Cross-model/strategy analysis
│   ├── model_comparison.csv                 # Compare models (same strategy)
│   └── strategy_comparison.csv              # Compare strategies (same model)
└── summary/                         # Final results
    ├── all_results_summary.csv              # All models × strategies
    ├── best_performers.csv                  # Top 5 by Sharpe ratio
    └── performance_heatmap.csv              # Model × Strategy matrix
```

---

## ⚙️ Configuration Options

### Portfolio Settings
```python
'capital': 1_000_000,           # Total capital
'long_short_split': 0.50,       # 50% long, 50% short (for strategies 4-6)
'top_pct': 0.10,                # Top 10% for long
'bottom_n': 100,                # Bottom 100 for short
```

### Stop Loss Settings
```python
'stop_loss_pct': 0.50,          # 50% stop loss
'apply_slippage': True,         # Enable slippage
'slippage_pct': 0.05,           # 5% slippage (-50% → -55%)
'apply_to_long': True,          # Apply to long positions
```

### Column Names
```python
'dep_var': 'expected_return',   # Actual return column
'pred_var': 'predicted_return'  # Predicted return column
```

---

## 📈 Performance Metrics

Each strategy reports:

### Returns
- Average long return
- Average short return
- Average spread
- Total return
- CAGR (Compound Annual Growth Rate)

### Risk
- Volatility (annual)
- Sharpe ratio (spread & dollar-based)
- Maximum drawdown
- Win rate (% of positive years)

### Portfolio Details
- Average stocks long/short
- Position sizes
- Dollar P&L breakdown

### Stop Loss Metrics (Strategies 5-6)
- Number stopped out (long/short)
- Percentage stopped out
- Average months held (Strategy 6 only)

---

## 🔧 Adding a New Model

1. Run your model training script (e.g., `03-machine-learning-lightgbm.py`)
2. Ensure predictions are saved to a directory
3. Add to `CONFIG['models']`:
```python
'your_model': {
    'name': 'Your Model Name',
    'pred_dir': 'path/to/predictions',
    'pred_file_pattern': 'pred_*.csv',
    'enabled': True
}
```
4. Re-run `05-backtesting.py`

---

## 🎯 Workflow

### For New Models

**Before (old workflow):**
```python
# In 03-machine-learning-xgboost.py
STEP 1-8: Train model
STEP 9: Backtest portfolios ← Duplicate this for each model
```

**After (new workflow):**
```python
# In 03-machine-learning-xgboost.py
STEP 1-8: Train model
# STEP 9: Portfolio Building (Moved to 05-backtesting.py)

# Then run 05-backtesting.py once for all models
```

### Benefits
- ✅ No code duplication
- ✅ Consistent backtesting across models
- ✅ Easy to add new models
- ✅ Centralized output
- ✅ Cross-model comparisons

---

## 📊 Example Results

After running, check:

1. **Best Performers**: `final_output/summary/best_performers.csv`
   - Top 5 model-strategy combinations by Sharpe ratio

2. **Model Comparison**: `final_output/comparisons/model_comparison.csv`
   - Which model performs best for each strategy?

3. **Strategy Comparison**: `final_output/comparisons/strategy_comparison.csv`
   - Which strategy works best for each model?

4. **Performance Heatmap**: `final_output/summary/performance_heatmap.csv`
   - Sharpe ratios for all model-strategy combinations

---

## 🐛 Troubleshooting

### "No prediction files found"
- Check `pred_dir` path is correct
- Check `pred_file_pattern` matches your files
- Ensure model training completed successfully

### "Monthly returns file not found"
- Strategy 6 requires `monthly_returns_2000.parquet`
- Run your data preparation script first
- Or disable Strategy 6 if not needed

### "Missing required columns"
- Check `dep_var` and `pred_var` match your data
- Ensure prediction files contain these columns

---

## 📝 Notes

- The script is Jupyter-synced (`.py` with `# %%` cells)
- Compatible with VSCode, Jupyter Notebook, JupyterLab
- All strategies use consistent methodology
- Output is automatically saved (no manual export needed)
- Safe to re-run (overwrites previous results)

---

## 🔄 Updates from 04-apply-monthly-stoploss.py

Strategy 6 now includes:
- ✅ Configurable slippage (5% default)
- ✅ Toggle slippage on/off
- ✅ Applied to both long and short positions
- ✅ Month-by-month tracking
- ✅ Integrated into main backtesting workflow

---

**Happy backtesting! 🎉**
