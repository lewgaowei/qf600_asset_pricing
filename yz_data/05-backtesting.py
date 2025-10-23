# %%
"""
================================================================================
05-backtesting.py
================================================================================
Purpose: Centralized backtesting for multiple ML models and portfolio strategies

Features:
- Multi-model support (XGBoost, LightGBM, etc.)
- 6 portfolio strategies including monthly stop loss
- Comprehensive performance analysis
- Cross-model and cross-strategy comparisons
- Centralized output to final_output/ folder

Usage (Jupyter Notebook):
    1. Configure models and strategies in CONFIG
    2. Run all cells
    3. Results saved to final_output/

Author: Claude Code
Date:   2025-10-23
Version: 1.0
================================================================================
"""

import pandas as pd
import numpy as np
from pathlib import Path
from tqdm.auto import tqdm
import warnings
import glob
warnings.filterwarnings('ignore')

print("=" * 80)
print("05-BACKTESTING: MULTI-MODEL PORTFOLIO ANALYSIS")
print("=" * 80)

# %%
# ===============================================================================
# CONFIGURATION
# ===============================================================================

CONFIG = {
    # === Models to Backtest ===
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
            'pred_file_pattern': 'brt_*.csv',
            'enabled': True
        },
        'catboost': {
            'name': 'CatBoost',
            'pred_dir': 'ml_catboost_results/Pred',
            'pred_file_pattern': 'catboost_*.csv',
            'enabled': True
        },
        'lstm': {
            'name': 'LSTM',
            'pred_dir': 'ml_lstm_results/Pred',
            'pred_file_pattern': 'lstm_*.csv',
            'enabled': True
        },
        'elasticnet': {
            'name': 'ElasticNet',
            'pred_dir': 'ml_elasticnet_results/Pred',
            'pred_file_pattern': 'elasticnet_*.csv',
            'enabled': True
        },
        'gp_symbolic': {
            'name': 'GP Symbolic',
            'pred_dir': 'ml_gp_symbolic_results/Pred',
            'pred_file_pattern': 'gp_symbolic_recursive_*.csv',
            'enabled': True
        },
    },

    # === Data Files ===
    'monthly_returns_file': 'monthly_returns_2000.parquet',
    'output_dir': 'final_output',
    'start_year': 2000,

    # === Portfolio Settings ===
    'capital': 1_000_000,           # Total portfolio capital
    'long_short_split': 0.50,       # 50% long, 50% short
    'top_pct': 0.10,                # Top 10% for long portfolio
    'bottom_n': 100,                # Bottom 100 stocks for short portfolio

    # === Stop Loss Settings ===
    'stop_loss_pct': 0.50,          # 50% stop loss threshold
    'apply_slippage': True,         # Apply slippage to stop loss?
    'slippage_pct': 0.05,           # 5% slippage (exit at -55% instead of -50%)
    'apply_to_long': True,          # Apply stop loss to long positions?

    # === Strategies to Run ===
    'strategies': {
        'fixed_100': True,           # Strategy 1: Fixed 100/100
        'decile_10pct': True,        # Strategy 2: Decile 10%/10%
        'hybrid_10_100': True,       # Strategy 3: Hybrid 10%/100
        'hybrid_5050': True,         # Strategy 4: Hybrid 10%/100 (50% L/S)
        'annual_stoploss': True,     # Strategy 5: Annual stop loss
        'monthly_stoploss': True     # Strategy 6: Monthly stop loss (intra-year tracking)
    },

    # === Column Names ===
    'dep_var': 'expected_return',   # Actual return column
    'pred_var': 'predicted_return'  # Predicted return column
}

# Display configuration
print("\n Configuration:")
print(f"   Output directory: {CONFIG['output_dir']}")
print(f"   Total capital: ${CONFIG['capital']:,}")
print(f"   Stop loss: {CONFIG['stop_loss_pct']*100}% (slippage: {CONFIG['slippage_pct']*100}% {'ON' if CONFIG['apply_slippage'] else 'OFF'})")
print(f"\n Models enabled:")
for model_id, model_cfg in CONFIG['models'].items():
    if model_cfg.get('enabled', True):
        print(f"   - {model_cfg['name']} ({model_cfg['pred_dir']})")

print(f"\n Strategies enabled:")
for strategy_id, enabled in CONFIG['strategies'].items():
    if enabled:
        strategy_names = {
            'fixed_100': 'Fixed 100/100',
            'decile_10pct': 'Decile 10%/10%',
            'hybrid_10_100': 'Hybrid 10%/100',
            'hybrid_5050': 'Hybrid 10%/100 (50% L/S)',
            'annual_stoploss': 'Annual Stop Loss',
            'monthly_stoploss': 'Monthly Stop Loss'
        }
        print(f"   - {strategy_names[strategy_id]}")

print("=" * 80)

# %%
# ===============================================================================
# HELPER FUNCTIONS - Performance Metrics
# ===============================================================================

def calculate_cagr(portfolio_df, initial_capital, pnl_column='portfolio_return'):
    """
    Calculate Compound Annual Growth Rate using proper compounding

    Parameters:
    -----------
    portfolio_df : DataFrame
        Portfolio results with annual returns
    initial_capital : float
        Initial capital invested
    pnl_column : str
        Column name containing annual returns (as decimals)

    Returns:
    --------
    float : CAGR as decimal (e.g., 0.05 = 5%)
    """
    n_years = len(portfolio_df)
    if n_years == 0:
        return 0.0

    # Calculate compounded final value: initial * (1+r1) * (1+r2) * ... * (1+rn)
    cumulative_multiplier = (1 + portfolio_df[pnl_column]).prod()
    final_value = initial_capital * cumulative_multiplier

    # CAGR formula: (Final/Initial)^(1/n) - 1
    cagr = (final_value / initial_capital) ** (1 / n_years) - 1
    return cagr

def calculate_max_drawdown(portfolio_df, initial_capital, pnl_column='portfolio_return'):
    """
    Calculate maximum drawdown from peak using proper compounding

    Parameters:
    -----------
    portfolio_df : DataFrame
        Portfolio results with annual returns
    initial_capital : float
        Initial capital invested
    pnl_column : str
        Column name containing annual returns (as decimals)

    Returns:
    --------
    float : Maximum drawdown as decimal (e.g., -0.20 = -20%)
    """
    if len(portfolio_df) == 0:
        return 0.0

    # Calculate cumulative portfolio value with proper compounding
    cumulative_multiplier = (1 + portfolio_df[pnl_column]).cumprod()
    portfolio_value = initial_capital * cumulative_multiplier

    # Calculate running maximum (peak)
    running_max = portfolio_value.expanding().max()

    # Calculate drawdown from peak as percentage
    drawdown = (portfolio_value - running_max) / running_max

    # Max drawdown is the minimum (most negative) drawdown
    max_dd = drawdown.min()

    return max_dd

def calculate_win_rate(portfolio_df, pnl_column='total_dollar_pnl'):
    """Calculate percentage of positive years"""
    if len(portfolio_df) == 0:
        return 0.0

    positive_years = (portfolio_df[pnl_column] > 0).sum()
    total_years = len(portfolio_df)
    win_rate = positive_years / total_years

    return win_rate

# %%
# ===============================================================================
# HELPER FUNCTIONS - Data Loading
# ===============================================================================

def load_model_predictions(model_config):
    """
    Load predictions for a specific model

    Returns:
    --------
    DataFrame with predictions or None if not found
    """
    pred_dir = Path(model_config['pred_dir'])

    if not pred_dir.exists():
        print(f"  Prediction directory not found: {pred_dir}")
        return None

    # Find all prediction files
    pattern = model_config['pred_file_pattern']
    pred_files = list(pred_dir.glob(pattern))

    if len(pred_files) == 0:
        print(f"  No prediction files found matching: {pattern}")
        return None

    # Load and combine all prediction files
    predictions_list = []
    for file in pred_files:
        df = pd.read_csv(file)
        predictions_list.append(df)

    predictions = pd.concat(predictions_list, ignore_index=True)

    print(f" Loaded {len(predictions):,} predictions from {len(pred_files)} files")

    return predictions

def load_monthly_returns():
    """Load monthly returns data for Strategy 6"""
    monthly_file = Path(CONFIG['monthly_returns_file'])

    if not monthly_file.exists():
        print(f"  Monthly returns file not found: {monthly_file}")
        print("   Strategy 6 (Monthly Stop Loss) will be skipped")
        return None

    monthly_returns = pd.read_parquet(monthly_file, engine='fastparquet')
    print(f" Loaded monthly returns: {len(monthly_returns):,} observations")

    return monthly_returns

# %%
# ===============================================================================
# STRATEGY 6 FUNCTIONS - Monthly Stop Loss Tracking
# ===============================================================================

def track_position_with_stoploss(monthly_returns, stop_loss_pct=0.50, apply_slippage=True, slippage_pct=0.05, is_short=True):
    """
    Track a single position month-by-month and apply stop loss.

    Parameters:
    -----------
    monthly_returns : list
        List of 12 monthly returns (already flipped for shorts)
    stop_loss_pct : float
        Stop loss threshold (0.50 = 50%)
    apply_slippage : bool
        Whether to apply slippage when stop loss is hit
    slippage_pct : float
        Slippage amount (0.05 = 5% worse than stop loss)
    is_short : bool
        Whether this is a short position

    Returns:
    --------
    dict with:
        - final_return: realized return after stop loss
        - stopped_out: True if stopped out
        - stop_month: month when stopped (or None)
        - months_held: number of months held
    """
    cumulative_return = 0.0

    for month_idx, monthly_ret in enumerate(monthly_returns, 1):
        # Skip if no data for this month
        if pd.isna(monthly_ret):
            continue

        # Update cumulative return: (1+r1)*(1+r2) - 1
        cumulative_return = (1 + cumulative_return) * (1 + monthly_ret) - 1

        # Check stop loss
        if cumulative_return < -stop_loss_pct:
            # Calculate final return with optional slippage
            if apply_slippage:
                final_loss = -(stop_loss_pct + slippage_pct)  # e.g., -50% - 5% = -55%
            else:
                final_loss = -stop_loss_pct  # e.g., -50%

            return {
                'final_return': final_loss,
                'stopped_out': True,
                'stop_month': month_idx,
                'months_held': month_idx
            }

    # Position held for full period without stopping
    return {
        'final_return': cumulative_return,
        'stopped_out': False,
        'stop_month': None,
        'months_held': 12
    }

print(" Helper functions loaded")

# %%
# ===============================================================================
# STRATEGY IMPLEMENTATIONS - Portfolio Building
# ===============================================================================

def build_strategy1_fixed_100(predictions, config):
    """Strategy 1: Fixed Top 100 / Bottom 100"""
    portfolio_results = []

    for year in sorted(predictions['form_year'].unique()):
        year_data = predictions[predictions['form_year'] == year].copy()

        if len(year_data) < 200:
            continue

        # Filter out stocks with NaN expected_return
        year_data = year_data[year_data[config['dep_var']].notna()].copy()

        # Sort by predicted returns
        year_data = year_data.sort_values(config['pred_var'], ascending=False)

        # Fixed: Top 100 long, bottom 100 short
        TOP_N = 100
        BOTTOM_N = 100

        long_portfolio = year_data.head(TOP_N)
        short_portfolio = year_data.tail(BOTTOM_N)

        # Calculate returns (percentage)
        long_return = long_portfolio[config['dep_var']].mean()
        short_return = -short_portfolio[config['dep_var']].mean()  # Negative because shorting
        spread = long_return - short_return

        # Dollar-based calculations
        n_long = len(long_portfolio)
        n_short = len(short_portfolio)
        total_positions = n_long + n_short

        # Allocate capital proportionally
        long_capital = config['capital'] * (n_long / total_positions)
        short_capital = config['capital'] * (n_short / total_positions)

        # Position sizing (equal weight within each side)
        position_size_long = long_capital / n_long
        position_size_short = short_capital / n_short

        # Dollar P&L
        dollar_pnl_long = long_capital * long_return
        dollar_pnl_short = short_capital * short_return
        total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

        # Portfolio return on total capital
        portfolio_return = total_dollar_pnl / config['capital']

        portfolio_results.append({
            'year': year,
            'long_return': long_return,
            'short_return': short_return,
            'spread': spread,
            'n_long': n_long,
            'n_short': n_short,
            'long_capital': long_capital,
            'short_capital': short_capital,
            'position_size_long': position_size_long,
            'position_size_short': position_size_short,
            'dollar_pnl_long': dollar_pnl_long,
            'dollar_pnl_short': dollar_pnl_short,
            'total_dollar_pnl': total_dollar_pnl,
            'portfolio_return': portfolio_return
        })

    return pd.DataFrame(portfolio_results)

def build_strategy2_decile_10pct(predictions, config):
    """Strategy 2: Decile 10%/10%"""
    portfolio_results = []

    for year in sorted(predictions['form_year'].unique()):
        year_data = predictions[predictions['form_year'] == year].copy()

        if len(year_data) < 200:
            continue

        year_data = year_data[year_data[config['dep_var']].notna()].copy()
        year_data = year_data.sort_values(config['pred_var'], ascending=False)

        # Decile: Top 10% long, Bottom 10% short
        n_stocks = len(year_data)
        decile_size = n_stocks // 10

        TOP_N = decile_size
        BOTTOM_N = decile_size

        long_portfolio = year_data.head(TOP_N)
        short_portfolio = year_data.tail(BOTTOM_N)

        # Calculate returns
        long_return = long_portfolio[config['dep_var']].mean()
        short_return = -short_portfolio[config['dep_var']].mean()
        spread = long_return - short_return

        # Dollar-based calculations
        n_long = len(long_portfolio)
        n_short = len(short_portfolio)
        total_positions = n_long + n_short

        long_capital = config['capital'] * (n_long / total_positions)
        short_capital = config['capital'] * (n_short / total_positions)

        position_size_long = long_capital / n_long
        position_size_short = short_capital / n_short

        dollar_pnl_long = long_capital * long_return
        dollar_pnl_short = short_capital * short_return
        total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

        portfolio_return = total_dollar_pnl / config['capital']

        portfolio_results.append({
            'year': year,
            'long_return': long_return,
            'short_return': short_return,
            'spread': spread,
            'n_long': n_long,
            'n_short': n_short,
            'long_capital': long_capital,
            'short_capital': short_capital,
            'position_size_long': position_size_long,
            'position_size_short': position_size_short,
            'dollar_pnl_long': dollar_pnl_long,
            'dollar_pnl_short': dollar_pnl_short,
            'total_dollar_pnl': total_dollar_pnl,
            'portfolio_return': portfolio_return
        })

    return pd.DataFrame(portfolio_results)

def build_strategy3_hybrid_10_100(predictions, config):
    """Strategy 3: Hybrid (Top 10% / Bottom 100)"""
    portfolio_results = []

    for year in sorted(predictions['form_year'].unique()):
        year_data = predictions[predictions['form_year'] == year].copy()

        if len(year_data) < 200:
            continue

        year_data = year_data[year_data[config['dep_var']].notna()].copy()
        year_data = year_data.sort_values(config['pred_var'], ascending=False)

        # Hybrid: Top 10% long, Bottom 100 short (fixed)
        n_stocks = len(year_data)
        decile_size = n_stocks // 10

        TOP_N = decile_size
        BOTTOM_N = 100

        long_portfolio = year_data.head(TOP_N)
        short_portfolio = year_data.tail(BOTTOM_N)

        # Calculate returns
        long_return = long_portfolio[config['dep_var']].mean()
        short_return = -short_portfolio[config['dep_var']].mean()
        spread = long_return - short_return

        # Dollar-based calculations
        n_long = len(long_portfolio)
        n_short = len(short_portfolio)
        total_positions = n_long + n_short

        long_capital = config['capital'] * (n_long / total_positions)
        short_capital = config['capital'] * (n_short / total_positions)

        position_size_long = long_capital / n_long
        position_size_short = short_capital / n_short

        dollar_pnl_long = long_capital * long_return
        dollar_pnl_short = short_capital * short_return
        total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

        portfolio_return = total_dollar_pnl / config['capital']

        portfolio_results.append({
            'year': year,
            'long_return': long_return,
            'short_return': short_return,
            'spread': spread,
            'n_long': n_long,
            'n_short': n_short,
            'long_capital': long_capital,
            'short_capital': short_capital,
            'position_size_long': position_size_long,
            'position_size_short': position_size_short,
            'dollar_pnl_long': dollar_pnl_long,
            'dollar_pnl_short': dollar_pnl_short,
            'total_dollar_pnl': total_dollar_pnl,
            'portfolio_return': portfolio_return
        })

    return pd.DataFrame(portfolio_results)

def build_strategy4_hybrid_5050(predictions, config):
    """Strategy 4: Hybrid 10%/100 (50% Long / 50% Short)"""
    portfolio_results = []

    for year in sorted(predictions['form_year'].unique()):
        year_data = predictions[predictions['form_year'] == year].copy()

        if len(year_data) < 200:
            continue

        year_data = year_data[year_data[config['dep_var']].notna()].copy()
        year_data = year_data.sort_values(config['pred_var'], ascending=False)

        # Hybrid: Top 10% long, Bottom 100 short
        n_stocks = len(year_data)
        decile_size = n_stocks // 10

        TOP_N = decile_size
        BOTTOM_N = 100

        long_portfolio = year_data.head(TOP_N)
        short_portfolio = year_data.tail(BOTTOM_N)

        # Calculate returns
        long_return = long_portfolio[config['dep_var']].mean()
        short_return = -short_portfolio[config['dep_var']].mean()
        spread = long_return - short_return

        # Dollar-based calculations (50/50 split)
        n_long = len(long_portfolio)
        n_short = len(short_portfolio)

        long_capital = config['capital'] * config['long_short_split']  # Fixed 50%
        short_capital = config['capital'] * (1 - config['long_short_split'])  # Fixed 50%

        position_size_long = long_capital / n_long
        position_size_short = short_capital / n_short

        dollar_pnl_long = long_capital * long_return
        dollar_pnl_short = short_capital * short_return
        total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

        portfolio_return = total_dollar_pnl / config['capital']

        portfolio_results.append({
            'year': year,
            'long_return': long_return,
            'short_return': short_return,
            'spread': spread,
            'n_long': n_long,
            'n_short': n_short,
            'long_capital': long_capital,
            'short_capital': short_capital,
            'position_size_long': position_size_long,
            'position_size_short': position_size_short,
            'dollar_pnl_long': dollar_pnl_long,
            'dollar_pnl_short': dollar_pnl_short,
            'total_dollar_pnl': total_dollar_pnl,
            'portfolio_return': portfolio_return
        })

    return pd.DataFrame(portfolio_results)

def build_strategy5_annual_stoploss(predictions, config):
    """Strategy 5: Annual Stop Loss (applied to annual returns)"""
    portfolio_results = []

    for year in sorted(predictions['form_year'].unique()):
        year_data = predictions[predictions['form_year'] == year].copy()

        if len(year_data) < 200:
            continue

        year_data = year_data[year_data[config['dep_var']].notna()].copy()
        year_data = year_data.sort_values(config['pred_var'], ascending=False)

        # Select top 10% for long, bottom 100 for short
        n_stocks = len(year_data)
        top_n = max(1, int(n_stocks * config['top_pct']))
        bottom_n = config['bottom_n']

        long_portfolio = year_data.head(top_n)
        short_portfolio = year_data.tail(bottom_n)

        # Long portfolio: apply stop loss
        if config['apply_to_long']:
            long_returns_raw = long_portfolio[config['dep_var']].values
            stop_threshold = -(config['stop_loss_pct'] + (config['slippage_pct'] if config['apply_slippage'] else 0))
            long_returns_capped = np.maximum(long_returns_raw, stop_threshold)
            n_long_stopped_out = (long_returns_raw < stop_threshold).sum()
            long_return = np.nanmean(long_returns_capped)
        else:
            long_return = long_portfolio[config['dep_var']].mean()
            n_long_stopped_out = 0

        # Short portfolio: apply stop loss
        short_returns_raw = -short_portfolio[config['dep_var']].values
        stop_threshold = -(config['stop_loss_pct'] + (config['slippage_pct'] if config['apply_slippage'] else 0))
        short_returns_capped = np.maximum(short_returns_raw, stop_threshold)
        n_short_stopped_out = (short_returns_raw < stop_threshold).sum()
        short_return = np.nanmean(short_returns_capped)

        spread = long_return - short_return

        # Dollar-based calculations (50/50 split)
        n_long = len(long_portfolio)
        n_short = len(short_portfolio)

        long_capital = config['capital'] * config['long_short_split']
        short_capital = config['capital'] * (1 - config['long_short_split'])

        position_size_long = long_capital / n_long
        position_size_short = short_capital / n_short

        dollar_pnl_long = long_capital * long_return
        dollar_pnl_short = short_capital * short_return
        total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

        portfolio_return = total_dollar_pnl / config['capital']

        portfolio_results.append({
            'year': year,
            'long_return': long_return,
            'short_return': short_return,
            'spread': spread,
            'n_long': n_long,
            'n_short': n_short,
            'n_long_stopped_out': n_long_stopped_out,
            'long_stopped_out_pct': (n_long_stopped_out / n_long) * 100 if n_long > 0 else 0,
            'n_short_stopped_out': n_short_stopped_out,
            'short_stopped_out_pct': (n_short_stopped_out / n_short) * 100 if n_short > 0 else 0,
            'long_capital': long_capital,
            'short_capital': short_capital,
            'position_size_long': position_size_long,
            'position_size_short': position_size_short,
            'dollar_pnl_long': dollar_pnl_long,
            'dollar_pnl_short': dollar_pnl_short,
            'total_dollar_pnl': total_dollar_pnl,
            'portfolio_return': portfolio_return
        })

    return pd.DataFrame(portfolio_results)

def build_strategy6_monthly_stoploss(predictions, monthly_returns, config):
    """Strategy 6: Monthly Stop Loss (month-by-month tracking)"""
    if monthly_returns is None:
        print("  Monthly returns data not available, skipping Strategy 6")
        return None

    # Merge predictions with monthly returns
    merged = predictions.merge(
        monthly_returns,
        on=['permno', 'form_year'],
        how='inner'
    )

    portfolio_results = []

    for year in sorted(merged['form_year'].unique()):
        year_data = merged[merged['form_year'] == year].copy()

        if len(year_data) < 200:
            continue

        year_data = year_data[year_data[config['dep_var']].notna()].copy()
        year_data = year_data.sort_values(config['pred_var'], ascending=False)

        # Select portfolios
        n_stocks = len(year_data)
        top_n = max(1, int(n_stocks * config['top_pct']))
        bottom_n = config['bottom_n']

        long_portfolio = year_data.head(top_n)
        short_portfolio = year_data.tail(bottom_n)

        # Track long positions with monthly stop loss
        if config['apply_to_long']:
            long_results = []
            for idx, row in long_portfolio.iterrows():
                monthly_rets = [row[f'ret_m{i}'] for i in range(1, 13)]
                result = track_position_with_stoploss(
                    monthly_rets,
                    stop_loss_pct=config['stop_loss_pct'],
                    apply_slippage=config['apply_slippage'],
                    slippage_pct=config['slippage_pct'],
                    is_short=False
                )
                long_results.append(result)

            long_return = np.mean([r['final_return'] for r in long_results])
            n_long_stopped = sum(r['stopped_out'] for r in long_results)
            avg_long_months = np.mean([r['months_held'] for r in long_results])
        else:
            long_return = long_portfolio[config['dep_var']].mean()
            n_long_stopped = 0
            avg_long_months = 12.0

        # Track short positions with monthly stop loss
        short_results = []
        for idx, row in short_portfolio.iterrows():
            monthly_rets = [row[f'ret_m{i}'] for i in range(1, 13)]
            # For shorts: flip sign (profit when stock goes down)
            monthly_rets_short = [-r if pd.notna(r) else np.nan for r in monthly_rets]

            result = track_position_with_stoploss(
                monthly_rets_short,
                stop_loss_pct=config['stop_loss_pct'],
                apply_slippage=config['apply_slippage'],
                slippage_pct=config['slippage_pct'],
                is_short=True
            )
            short_results.append(result)

        short_return = np.mean([r['final_return'] for r in short_results])
        n_short_stopped = sum(r['stopped_out'] for r in short_results)
        avg_short_months = np.mean([r['months_held'] for r in short_results])

        spread = long_return - short_return

        # Dollar-based calculations
        n_long = len(long_portfolio)
        n_short = len(short_portfolio)

        long_capital = config['capital'] * config['long_short_split']
        short_capital = config['capital'] * (1 - config['long_short_split'])

        position_size_long = long_capital / n_long
        position_size_short = short_capital / n_short

        dollar_pnl_long = long_capital * long_return
        dollar_pnl_short = short_capital * short_return
        total_dollar_pnl = dollar_pnl_long + dollar_pnl_short

        portfolio_return = total_dollar_pnl / config['capital']

        portfolio_results.append({
            'year': year,
            'long_return': long_return,
            'short_return': short_return,
            'spread': spread,
            'n_long': n_long,
            'n_short': n_short,
            'n_long_stopped_out': n_long_stopped,
            'long_stopped_out_pct': (n_long_stopped / n_long) * 100 if n_long > 0 else 0,
            'n_short_stopped_out': n_short_stopped,
            'short_stopped_out_pct': (n_short_stopped / n_short) * 100 if n_short > 0 else 0,
            'avg_long_months_held': avg_long_months,
            'avg_short_months_held': avg_short_months,
            'long_capital': long_capital,
            'short_capital': short_capital,
            'position_size_long': position_size_long,
            'position_size_short': position_size_short,
            'dollar_pnl_long': dollar_pnl_long,
            'dollar_pnl_short': dollar_pnl_short,
            'total_dollar_pnl': total_dollar_pnl,
            'portfolio_return': portfolio_return
        })

    return pd.DataFrame(portfolio_results)

print(" Strategy implementations loaded")

# %%
# ===============================================================================
# PERFORMANCE ANALYSIS
# ===============================================================================

def calculate_strategy_metrics(portfolio_df, strategy_name, config):
    """Calculate comprehensive performance metrics for a strategy"""
    if portfolio_df is None or len(portfolio_df) == 0:
        return None

    # Basic returns
    avg_long = portfolio_df['long_return'].mean()
    avg_short = portfolio_df['short_return'].mean()
    avg_spread = portfolio_df['spread'].mean()
    spread_std = portfolio_df['spread'].std()

    # Dollar metrics
    avg_dollar_pnl = portfolio_df['total_dollar_pnl'].mean()
    avg_portfolio_return = portfolio_df['portfolio_return'].mean()
    portfolio_return_std = portfolio_df['portfolio_return'].std()

    # Risk metrics
    sharpe_ratio_spread = avg_spread / spread_std if spread_std > 0 else 0
    sharpe_ratio_dollar = avg_portfolio_return / portfolio_return_std if portfolio_return_std > 0 else 0

    # Period info
    n_years = len(portfolio_df)
    first_year = int(portfolio_df['year'].min())
    last_year = int(portfolio_df['year'].max())

    # Calculate compounded returns
    cumulative_multiplier = (1 + portfolio_df['portfolio_return']).prod()
    final_value = config['capital'] * cumulative_multiplier
    total_dollar_pnl = final_value - config['capital']  # Total P&L with compounding
    total_return = (final_value / config['capital']) - 1  # Total return with compounding

    # Advanced metrics using compounded values
    cagr = calculate_cagr(portfolio_df, config['capital'], 'portfolio_return')
    max_dd = calculate_max_drawdown(portfolio_df, config['capital'], 'portfolio_return')
    win_rate = calculate_win_rate(portfolio_df, 'total_dollar_pnl')

    metrics = {
        'strategy': strategy_name,
        'n_years': n_years,
        'first_year': first_year,
        'last_year': last_year,
        # Returns
        'avg_long_return': avg_long,
        'avg_short_return': avg_short,
        'avg_spread': avg_spread,
        'avg_long_return_pct': avg_long * 100,
        'avg_short_return_pct': avg_short * 100,
        'avg_spread_pct': avg_spread * 100,
        # Dollar metrics
        'total_capital': config['capital'],
        'avg_annual_pnl': avg_dollar_pnl,
        'total_pnl': total_dollar_pnl,
        'total_return': total_return,
        'total_return_pct': total_return * 100,
        'avg_portfolio_return': avg_portfolio_return,
        'avg_annual_return_pct': avg_portfolio_return * 100,
        # Risk metrics
        'spread_volatility': spread_std,
        'annualized_volatility_pct': portfolio_return_std * 100,
        'sharpe_ratio_spread': sharpe_ratio_spread,
        'sharpe_ratio_dollar': sharpe_ratio_dollar,
        'cagr': cagr,
        'cagr_pct': cagr * 100,
        'max_drawdown': max_dd,
        'max_drawdown_pct': max_dd * 100,
        'win_rate': win_rate,
        'win_rate_pct': win_rate * 100,
        'positive_years': int(win_rate * n_years),
        'total_years': n_years,
        # Portfolio sizing
        'avg_stocks_long': portfolio_df['n_long'].mean(),
        'avg_stocks_short': portfolio_df['n_short'].mean()
    }

    # Add stop loss metrics if available
    if 'n_long_stopped_out' in portfolio_df.columns:
        metrics['avg_long_stopped_out'] = portfolio_df['n_long_stopped_out'].mean()
        metrics['avg_long_stopped_pct'] = portfolio_df['long_stopped_out_pct'].mean()
        metrics['avg_short_stopped_out'] = portfolio_df['n_short_stopped_out'].mean()
        metrics['avg_short_stopped_pct'] = portfolio_df['short_stopped_out_pct'].mean()

        # Combined stop loss metrics
        total_positions = metrics['avg_stocks_long'] + metrics['avg_stocks_short']
        avg_stopped_out = metrics['avg_long_stopped_out'] + metrics['avg_short_stopped_out']
        metrics['avg_stopped_out'] = avg_stopped_out
        metrics['avg_stopped_pct'] = (avg_stopped_out / total_positions * 100) if total_positions > 0 else 0

    # Add monthly tracking metrics if available
    if 'avg_long_months_held' in portfolio_df.columns:
        metrics['avg_long_months_held'] = portfolio_df['avg_long_months_held'].mean()
        metrics['avg_short_months_held'] = portfolio_df['avg_short_months_held'].mean()

    return metrics

print(" Performance analysis functions loaded")

# %%
# ===============================================================================
# MAIN EXECUTION - Process All Models and Strategies
# ===============================================================================

print("\n" + "=" * 80)
print("STARTING BACKTESTING")
print("=" * 80)

# Create output directory
output_dir = Path(CONFIG['output_dir'])
output_dir.mkdir(exist_ok=True)

# Load monthly returns once (for Strategy 6)
monthly_returns = load_monthly_returns()

# Storage for all results
all_results = []

# Process each model
for model_id, model_config in CONFIG['models'].items():
    if not model_config.get('enabled', True):
        continue

    print("\n" + "=" * 80)
    print(f"PROCESSING MODEL: {model_config['name']}")
    print("=" * 80)

    # Load predictions
    predictions = load_model_predictions(model_config)
    if predictions is None:
        print(f"  Skipping {model_config['name']} (no predictions found)")
        continue

    # Create model-specific output directory
    model_output_dir = output_dir / 'models' / model_id
    model_output_dir.mkdir(parents=True, exist_ok=True)

    # Run each enabled strategy
    strategy_results = {}

    if CONFIG['strategies']['fixed_100']:
        print("\n Building Strategy 1: Fixed 100/100...")
        portfolio_df = build_strategy1_fixed_100(predictions, CONFIG)
        if len(portfolio_df) > 0:
            portfolio_df.to_csv(model_output_dir / 'portfolio_fixed_100.csv', index=False)
            metrics = calculate_strategy_metrics(portfolio_df, 'Fixed 100/100', CONFIG)
            if metrics:
                metrics['model'] = model_config['name']
                metrics['model_id'] = model_id
                strategy_results['fixed_100'] = metrics
                print(f"    Complete: {len(portfolio_df)} years, CAGR={metrics['cagr_pct']:.2f}%")

    if CONFIG['strategies']['decile_10pct']:
        print("\n Building Strategy 2: Decile 10%/10%...")
        portfolio_df = build_strategy2_decile_10pct(predictions, CONFIG)
        if len(portfolio_df) > 0:
            portfolio_df.to_csv(model_output_dir / 'portfolio_decile_10pct.csv', index=False)
            metrics = calculate_strategy_metrics(portfolio_df, 'Decile 10%/10%', CONFIG)
            if metrics:
                metrics['model'] = model_config['name']
                metrics['model_id'] = model_id
                strategy_results['decile_10pct'] = metrics
                print(f"    Complete: {len(portfolio_df)} years, CAGR={metrics['cagr_pct']:.2f}%")

    if CONFIG['strategies']['hybrid_10_100']:
        print("\n Building Strategy 3: Hybrid 10%/100...")
        portfolio_df = build_strategy3_hybrid_10_100(predictions, CONFIG)
        if len(portfolio_df) > 0:
            portfolio_df.to_csv(model_output_dir / 'portfolio_hybrid_10_100.csv', index=False)
            metrics = calculate_strategy_metrics(portfolio_df, 'Hybrid 10%/100', CONFIG)
            if metrics:
                metrics['model'] = model_config['name']
                metrics['model_id'] = model_id
                strategy_results['hybrid_10_100'] = metrics
                print(f"    Complete: {len(portfolio_df)} years, CAGR={metrics['cagr_pct']:.2f}%")

    if CONFIG['strategies']['hybrid_5050']:
        print("\n Building Strategy 4: Hybrid 10%/100 (50% L/S)...")
        portfolio_df = build_strategy4_hybrid_5050(predictions, CONFIG)
        if len(portfolio_df) > 0:
            portfolio_df.to_csv(model_output_dir / 'portfolio_hybrid_5050.csv', index=False)
            metrics = calculate_strategy_metrics(portfolio_df, 'Hybrid 10%/100 (50% L/S)', CONFIG)
            if metrics:
                metrics['model'] = model_config['name']
                metrics['model_id'] = model_id
                strategy_results['hybrid_5050'] = metrics
                print(f"    Complete: {len(portfolio_df)} years, CAGR={metrics['cagr_pct']:.2f}%")

    if CONFIG['strategies']['annual_stoploss']:
        print("\n Building Strategy 5: Annual Stop Loss...")
        portfolio_df = build_strategy5_annual_stoploss(predictions, CONFIG)
        if len(portfolio_df) > 0:
            portfolio_df.to_csv(model_output_dir / 'portfolio_annual_stoploss.csv', index=False)
            metrics = calculate_strategy_metrics(portfolio_df, 'Annual Stop Loss', CONFIG)
            if metrics:
                metrics['model'] = model_config['name']
                metrics['model_id'] = model_id
                strategy_results['annual_stoploss'] = metrics
                print(f"    Complete: {len(portfolio_df)} years, CAGR={metrics['cagr_pct']:.2f}%")

    if CONFIG['strategies']['monthly_stoploss']:
        print("\n Building Strategy 6: Monthly Stop Loss...")
        portfolio_df = build_strategy6_monthly_stoploss(predictions, monthly_returns, CONFIG)
        if portfolio_df is not None and len(portfolio_df) > 0:
            portfolio_df.to_csv(model_output_dir / 'portfolio_monthly_stoploss.csv', index=False)
            metrics = calculate_strategy_metrics(portfolio_df, 'Monthly Stop Loss', CONFIG)
            if metrics:
                metrics['model'] = model_config['name']
                metrics['model_id'] = model_id
                strategy_results['monthly_stoploss'] = metrics
                print(f"    Complete: {len(portfolio_df)} years, CAGR={metrics['cagr_pct']:.2f}%")

    # Save model summary
    if strategy_results:
        model_summary_df = pd.DataFrame(list(strategy_results.values()))
        model_summary_df.to_csv(model_output_dir / f'summary_{model_id}.csv', index=False)
        print(f"\n {model_config['name']} complete: {len(strategy_results)} strategies")

        # Add to all results
        all_results.extend(list(strategy_results.values()))

print("\n" + "=" * 80)
print("BACKTESTING COMPLETE")
print("=" * 80)

# %%
# ===============================================================================
# COMPARISONS AND FINAL OUTPUT
# ===============================================================================

if len(all_results) > 0:
    print("\n" + "=" * 80)
    print("GENERATING COMPARISONS")
    print("=" * 80)

    # Create comparisons and summary directories
    comp_dir = output_dir / 'comparisons'
    comp_dir.mkdir(exist_ok=True)

    summary_dir = output_dir / 'summary'
    summary_dir.mkdir(exist_ok=True)

    # All results DataFrame
    all_results_df = pd.DataFrame(all_results)
    all_results_df.to_csv(summary_dir / 'all_results_summary.csv', index=False)

    # Model comparison (same strategy across models)
    print("\n Creating model comparisons...")
    model_comp = all_results_df.pivot_table(
        index='strategy',
        columns='model',
        values=['cagr_pct', 'sharpe_ratio_dollar', 'max_drawdown_pct', 'win_rate_pct']
    )
    model_comp.to_csv(comp_dir / 'model_comparison.csv')
    print("    Model comparison saved")

    # Strategy comparison (same model across strategies)
    print("\n Creating strategy comparisons...")
    strategy_comp = all_results_df.pivot_table(
        index='model',
        columns='strategy',
        values=['cagr_pct', 'sharpe_ratio_dollar', 'max_drawdown_pct']
    )
    strategy_comp.to_csv(comp_dir / 'strategy_comparison.csv')
    print("    Strategy comparison saved")

    # Best performers
    print("\n Finding best performers...")

    # Top 10 by Sharpe Ratio (primary ranking)
    best_by_sharpe = all_results_df.nlargest(10, 'sharpe_ratio_dollar')[
        ['model', 'strategy', 'cagr_pct', 'sharpe_ratio_dollar', 'max_drawdown_pct', 'win_rate_pct', 'total_pnl']
    ]
    best_by_sharpe.to_csv(summary_dir / 'best_performers.csv', index=False)  # Primary best performers file
    best_by_sharpe.to_csv(summary_dir / 'top10_by_sharpe.csv', index=False)  # Detailed version

    # Top 10 by CAGR
    best_by_cagr = all_results_df.nlargest(10, 'cagr_pct')[
        ['model', 'strategy', 'cagr_pct', 'sharpe_ratio_dollar', 'max_drawdown_pct', 'win_rate_pct', 'total_pnl']
    ]
    best_by_cagr.to_csv(summary_dir / 'top10_by_cagr.csv', index=False)

    # Top 10 by Win Rate
    best_by_winrate = all_results_df.nlargest(10, 'win_rate_pct')[
        ['model', 'strategy', 'win_rate_pct', 'cagr_pct', 'sharpe_ratio_dollar', 'max_drawdown_pct', 'total_pnl']
    ]
    best_by_winrate.to_csv(summary_dir / 'top10_by_winrate.csv', index=False)

    # Display results
    print("\n" + "=" * 80)
    print("TOP 10 PERFORMERS (by Sharpe Ratio)")
    print("=" * 80)
    print(best_by_sharpe.to_string(index=False))

    print("\n" + "=" * 80)
    print("TOP 10 PERFORMERS (by CAGR)")
    print("=" * 80)
    print(best_by_cagr.to_string(index=False))

    print("\n" + "=" * 80)
    print("TOP 10 PERFORMERS (by Win Rate)")
    print("=" * 80)
    print(best_by_winrate.to_string(index=False))

    # Performance heatmap data
    heatmap_df = all_results_df.pivot_table(
        index='model',
        columns='strategy',
        values='sharpe_ratio_dollar'
    )
    heatmap_df.to_csv(summary_dir / 'performance_heatmap.csv')

    print(f"\n All outputs saved to: {output_dir}")
    print(f"   - Models: {output_dir / 'models'}")
    print(f"   - Comparisons: {comp_dir}")
    print(f"   - Summary: {summary_dir}")

else:
    print("\n  No results generated")

print("\n" + "=" * 80)
print(" ANALYSIS COMPLETE!")
print("=" * 80)

# %%
