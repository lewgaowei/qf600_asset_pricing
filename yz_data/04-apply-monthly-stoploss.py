# %%
"""
================================================================================
04-apply-monthly-stoploss.py
================================================================================
Purpose: Monthly stop loss analysis for ML model predictions (Jupyter Notebook)

Features:
- Tracks positions month-by-month with configurable stop loss
- Generates comprehensive performance analysis
- Compares with annual stop loss approach

Usage (Jupyter Notebook):
    1. Configure settings in CONFIG below
    2. Run all cells

Author: Claude Code
Date:   2025-10-23
Version: 2.0 (Jupyter-only)
================================================================================
"""

import pandas as pd
import numpy as np
from pathlib import Path
from tqdm.auto import tqdm
import warnings
warnings.filterwarnings('ignore')

# %%
# ===============================================================================
# CONFIGURATION - EDIT THESE VALUES
# ===============================================================================

CONFIG = {
    # === Model and Paths ===
    'model_name': 'xgboost',
    'pred_dir': 'ml_xgboost_results/Pred',     # Path to prediction files
    'monthly_returns_file': 'monthly_returns_2000.parquet',
    'output_dir': 'ml_xgboost_results/stoploss',  # Where to save results
    'start_year': 2000,

    # === Stop Loss Settings ===
    'stop_loss_pct': 0.50,           # 50% stop loss threshold
    'apply_slippage': True,          # Apply slippage to stop loss?
    'slippage_pct': 0.05,            # Slippage amount (5% = exit at -55% instead of -50%)
    'apply_to_long': True,          # Apply stop loss to long positions?

    # === Portfolio Settings ===
    'capital': 1_000_000,            # Total portfolio capital
    'long_short_split': 0.50,        # 50% long, 50% short
    'top_pct': 0.10,                 # Top 10% for long portfolio
    'bottom_n': 100,                 # Bottom 100 stocks for short portfolio

    # === Column Names ===
    'dep_var': 'expected_return',    # Actual return column name
    'pred_var': 'predicted_return'   # Predicted return column name
}

print("=" * 80)
print("MONTHLY STOP LOSS ANALYSIS - CONFIGURATION")
print("=" * 80)
print(f"Model: {CONFIG['model_name']}")
print(f"Prediction directory: {CONFIG['pred_dir']}")
print(f"Stop Loss: {CONFIG['stop_loss_pct']*100}%")
print(f"Slippage: {CONFIG['slippage_pct']*100}% ({'ON' if CONFIG['apply_slippage'] else 'OFF'})")
if CONFIG['apply_slippage']:
    effective_stop = (CONFIG['stop_loss_pct'] + CONFIG['slippage_pct']) * 100
    print(f"  → Effective stop loss: {effective_stop:.1f}%")
print(f"Apply to longs: {CONFIG['apply_to_long']}")
print(f"Start Year: {CONFIG['start_year']}")
print(f"Total Capital: ${CONFIG['capital']:,}")
print("=" * 80)

# %%
# ===============================================================================
# CORE FUNCTIONS - Position Tracking
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

# %%
# ===============================================================================
# CORE FUNCTIONS - Portfolio Building
# ===============================================================================

def build_year_portfolio_with_stoploss(year_data, config):
    """Build and evaluate portfolio for one year with monthly stop loss tracking"""

    if len(year_data) < 200:
        return None

    # Filter out stocks with NaN expected_return (cannot backtest without actual returns)
    year_data = year_data[year_data[config['dep_var']].notna()].copy()

    # Sort by predicted return
    year_data = year_data.sort_values(config['pred_var'], ascending=False)

    # === LONG PORTFOLIO ===
    n_stocks = len(year_data)
    top_n = max(1, int(n_stocks * config['top_pct']))
    long_portfolio = year_data.head(top_n)

    # Apply stop loss to longs if configured
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
        # No stop loss on longs: use annual return
        long_return = long_portfolio[config['dep_var']].mean()
        n_long_stopped = 0
        avg_long_months = 12.0

    # === SHORT PORTFOLIO ===
    short_portfolio = year_data.tail(config['bottom_n'])

    # Always apply stop loss to shorts
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

    # === PORTFOLIO METRICS ===
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

    return {
        'long_return': long_return,
        'short_return': short_return,
        'spread': spread,
        'n_long': n_long,
        'n_short': n_short,
        'n_long_stopped': n_long_stopped,
        'n_short_stopped': n_short_stopped,
        'long_stopped_pct': (n_long_stopped / n_long) * 100 if n_long > 0 else 0,
        'short_stopped_pct': (n_short_stopped / n_short) * 100 if n_short > 0 else 0,
        'avg_long_months': avg_long_months,
        'avg_short_months': avg_short_months,
        # Dollar metrics
        'long_capital': long_capital,
        'short_capital': short_capital,
        'position_size_long': position_size_long,
        'position_size_short': position_size_short,
        'dollar_pnl_long': dollar_pnl_long,
        'dollar_pnl_short': dollar_pnl_short,
        'total_dollar_pnl': total_dollar_pnl,
        'portfolio_return': portfolio_return
    }

# %%
# ===============================================================================
# CORE FUNCTIONS - Performance Metrics
# ===============================================================================

def calculate_performance_metrics(portfolio_df, total_capital):
    """Calculate aggregate performance metrics for a portfolio strategy"""

    # Basic statistics
    avg_return = portfolio_df['portfolio_return'].mean()
    return_std = portfolio_df['portfolio_return'].std()
    sharpe_ratio = avg_return / return_std if return_std > 0 else 0

    total_pnl = portfolio_df['total_dollar_pnl'].sum()
    n_years = len(portfolio_df)

    # CAGR
    total_return = total_pnl / total_capital
    cagr = (1 + total_return) ** (1 / n_years) - 1 if n_years > 0 else 0

    # Max drawdown
    cumulative_returns = (1 + portfolio_df['portfolio_return']).cumprod()
    running_max = cumulative_returns.cummax()
    drawdown = (cumulative_returns - running_max) / running_max
    max_drawdown = drawdown.min()

    # Win rate
    win_rate = (portfolio_df['portfolio_return'] > 0).mean()

    return {
        'total_return_pct': total_return * 100,
        'cagr_pct': cagr * 100,
        'sharpe_ratio': sharpe_ratio,
        'max_drawdown_pct': max_drawdown * 100,
        'win_rate_pct': win_rate * 100,
        'avg_annual_return_pct': avg_return * 100,
        'volatility_pct': return_std * 100,
        'total_pnl': total_pnl,
        'n_years': n_years
    }

# %%
# ===============================================================================
# DATA LOADING FUNCTIONS
# ===============================================================================

def load_predictions(pred_dir):
    """Load all prediction files from the specified directory"""
    pred_path = Path(pred_dir)

    if not pred_path.exists():
        raise FileNotFoundError(f"Prediction directory not found: {pred_path}")

    # Try multiple file patterns
    pred_files = sorted(pred_path.glob("pred_*.parquet"))
    if not pred_files:
        pred_files = sorted(pred_path.glob("*_pred.csv"))
    if not pred_files:
        pred_files = sorted(pred_path.glob("*_pred.parquet"))
    if not pred_files:
        pred_files = sorted(pred_path.glob("*.parquet"))
    if not pred_files:
        pred_files = sorted(pred_path.glob("*.csv"))

    if not pred_files:
        raise FileNotFoundError(
            f"No prediction files found in {pred_path}\n"
            f"Tried patterns: pred_*.parquet, *_pred.csv, *_pred.parquet, *.parquet, *.csv"
        )

    print(f"📂 Loading predictions from: {pred_path}")
    print(f"   Found {len(pred_files)} files")

    dfs = []
    for f in tqdm(pred_files, desc="Loading files"):
        # Detect file format and load accordingly
        if f.suffix == '.parquet':
            df = pd.read_parquet(f)
        elif f.suffix == '.csv':
            df = pd.read_csv(f)
        else:
            continue
        dfs.append(df)

    predictions = pd.concat(dfs, ignore_index=True)
    print(f"✅ Loaded {len(predictions):,} predictions")

    return predictions


def merge_monthly_returns(predictions, monthly_file):
    """Merge predictions with monthly return data"""
    monthly_path = Path(monthly_file)

    if not monthly_path.exists():
        raise FileNotFoundError(
            f"Monthly returns file not found: {monthly_path}\n"
            f"Run 01-prepare-monthly-returns.py first!"
        )

    print(f"\n📂 Loading monthly returns: {monthly_path.name}")
    monthly_df = pd.read_parquet(monthly_path)
    print(f"✅ Loaded {len(monthly_df):,} observations")

    # Ensure form_date is datetime
    if 'form_date' in predictions.columns:
        predictions['form_date'] = pd.to_datetime(predictions['form_date'])
    monthly_df['form_date'] = pd.to_datetime(monthly_df['form_date'])

    # Merge
    print("\n🔄 Merging predictions with monthly returns...")
    merged = predictions.merge(
        monthly_df,
        on=['permno', 'form_date'],
        how='inner',
        suffixes=('', '_monthly')
    )

    print(f"✅ Merged dataset: {len(merged):,} observations")

    # Check for missing monthly data
    missing_count = merged[[f'ret_m{i}' for i in range(1, 13)]].isna().all(axis=1).sum()
    if missing_count > 0:
        print(f"⚠️  Warning: {missing_count} observations missing all monthly returns")

    return merged

# %%
# ===============================================================================
# MAIN ANALYSIS FUNCTION
# ===============================================================================

def run_monthly_stoploss_analysis(config):
    """
    Run complete monthly stop loss analysis

    Parameters:
    -----------
    config : dict
        Configuration dictionary with all settings

    Returns:
    --------
    portfolio_df : DataFrame
        Year-by-year portfolio results
    metrics : dict
        Aggregate performance metrics
    """

    # ===================================================================
    # STEP 1: LOAD DATA
    # ===================================================================
    print("\n" + "=" * 80)
    print("STEP 1: LOADING DATA")
    print("=" * 80)

    predictions = load_predictions(config['pred_dir'])
    portfolio_data = merge_monthly_returns(predictions, config['monthly_returns_file'])

    # Ensure form_year exists
    if 'form_year' not in portfolio_data.columns:
        portfolio_data['form_year'] = pd.to_datetime(portfolio_data['form_date']).dt.year

    # ===================================================================
    # STEP 2: BUILD PORTFOLIOS WITH MONTHLY STOP LOSS
    # ===================================================================
    print("\n" + "=" * 80)
    print("STEP 2: BUILDING PORTFOLIOS WITH MONTHLY STOP LOSS")
    print("=" * 80)
    print(f"Stop Loss: {config['stop_loss_pct']*100}% on shorts")
    if config['apply_to_long']:
        print(f"           {config['stop_loss_pct']*100}% on longs")
    print("=" * 80)

    portfolio_results = []
    years = sorted(portfolio_data['form_year'].unique())

    print(f"\nProcessing {len(years)} years...")
    for year in tqdm(years, desc="Building portfolios"):
        year_data = portfolio_data[portfolio_data['form_year'] == year].copy()

        result = build_year_portfolio_with_stoploss(year_data, config)
        if result is not None:
            result['year'] = year
            portfolio_results.append(result)

    portfolio_df = pd.DataFrame(portfolio_results)
    print(f"\n✅ Built portfolios for {len(portfolio_df)} years")

    # ===================================================================
    # STEP 3: CALCULATE PERFORMANCE METRICS
    # ===================================================================
    print("\n" + "=" * 80)
    print("STEP 3: CALCULATING PERFORMANCE METRICS")
    print("=" * 80)

    metrics = calculate_performance_metrics(portfolio_df, config['capital'])

    print("\n📊 PERFORMANCE SUMMARY:")
    print(f"  Total Return:    {metrics['total_return_pct']:+.2f}%")
    print(f"  CAGR:            {metrics['cagr_pct']:+.2f}%")
    print(f"  Sharpe Ratio:    {metrics['sharpe_ratio']:.2f}")
    print(f"  Max Drawdown:    {metrics['max_drawdown_pct']:.2f}%")
    print(f"  Win Rate:        {metrics['win_rate_pct']:.1f}%")
    print(f"  Total P&L:       ${metrics['total_pnl']:,.0f}")

    print("\n📊 STOP LOSS STATISTICS:")
    avg_short_stopped = portfolio_df['short_stopped_pct'].mean()
    avg_short_months = portfolio_df['avg_short_months'].mean()
    print(f"  Avg Short Stop-Out Rate: {avg_short_stopped:.1f}%")
    print(f"  Avg Short Holding Period: {avg_short_months:.1f} months")

    if config['apply_to_long']:
        avg_long_stopped = portfolio_df['long_stopped_pct'].mean()
        avg_long_months = portfolio_df['avg_long_months'].mean()
        print(f"  Avg Long Stop-Out Rate:  {avg_long_stopped:.1f}%")
        print(f"  Avg Long Holding Period:  {avg_long_months:.1f} months")

    # ===================================================================
    # STEP 4: SAVE RESULTS
    # ===================================================================
    print("\n" + "=" * 80)
    print("STEP 4: SAVING RESULTS")
    print("=" * 80)

    output_dir = Path(config['output_dir'])
    output_dir.mkdir(parents=True, exist_ok=True)

    # Save portfolio results
    portfolio_file = output_dir / "portfolio_monthly_sl.csv"
    portfolio_df.to_csv(portfolio_file, index=False)
    print(f"✅ Portfolio results: {portfolio_file}")

    # Save summary metrics
    summary_df = pd.DataFrame([metrics])
    summary_df['model'] = config['model_name']
    summary_df['stop_loss_pct'] = config['stop_loss_pct'] * 100
    summary_file = output_dir / "summary_monthly_sl.csv"
    summary_df.to_csv(summary_file, index=False)
    print(f"✅ Summary metrics:   {summary_file}")

    # Save stop loss statistics
    stop_stats = portfolio_df[[
        'year', 'n_short', 'n_short_stopped', 'short_stopped_pct', 'avg_short_months'
    ]].copy()
    stop_stats_file = output_dir / "stop_loss_statistics.csv"
    stop_stats.to_csv(stop_stats_file, index=False)
    print(f"✅ Stop loss stats:   {stop_stats_file}")

    print(f"\n📁 All results saved to: {output_dir}")

    # ===================================================================
    # STEP 5: COMPARISON ANALYSIS (if annual results exist)
    # ===================================================================
    print("\n" + "=" * 80)
    print("STEP 5: COMPARISON ANALYSIS")
    print("=" * 80)

    # Check if annual stop loss results exist
    annual_file = Path(f"ml_{config['model_name']}_results") / "portfolio_returns_hybrid_stoploss.csv"

    if annual_file.exists():
        print(f"📂 Found annual stop loss results: {annual_file.name}")
        annual_df = pd.read_csv(annual_file)

        # Calculate metrics for annual approach
        annual_metrics = calculate_performance_metrics(annual_df, config['capital'])

        # Comparison
        comparison = pd.DataFrame({
            'Metric': ['Total Return %', 'CAGR %', 'Sharpe Ratio', 'Max Drawdown %', 'Win Rate %'],
            'Monthly Tracking': [
                f"{metrics['total_return_pct']:+.2f}",
                f"{metrics['cagr_pct']:+.2f}",
                f"{metrics['sharpe_ratio']:.2f}",
                f"{metrics['max_drawdown_pct']:.2f}",
                f"{metrics['win_rate_pct']:.1f}"
            ],
            'Annual Cap': [
                f"{annual_metrics['total_return_pct']:+.2f}",
                f"{annual_metrics['cagr_pct']:+.2f}",
                f"{annual_metrics['sharpe_ratio']:.2f}",
                f"{annual_metrics['max_drawdown_pct']:.2f}",
                f"{annual_metrics['win_rate_pct']:.1f}"
            ]
        })

        print("\n📊 COMPARISON: Monthly Tracking vs Annual Cap")
        print(comparison.to_string(index=False))

        # Save comparison
        comparison_file = output_dir / "comparison_monthly_vs_annual.csv"
        comparison.to_csv(comparison_file, index=False)
        print(f"\n✅ Comparison saved: {comparison_file}")
    else:
        print("ℹ️  Annual stop loss results not found")
        print("   (Run 03-machine-learning-xgboost.py with Strategy 5 to enable comparison)")

    print("\n" + "=" * 80)
    print("✅ ANALYSIS COMPLETE!")
    print("=" * 80)

    return portfolio_df, metrics

# %%
# ===============================================================================
# RUN ANALYSIS
# ===============================================================================

# Execute the analysis with the configuration above
portfolio_df, metrics = run_monthly_stoploss_analysis(CONFIG)

print("\n🎉 SUCCESS! Results saved to:", CONFIG['output_dir'])

# %%
# ===============================================================================
# DIAGNOSTIC ANALYSIS - Compare Stock Universes
# ===============================================================================

print("\n" + "=" * 80)
print("DIAGNOSTIC: COMPARING ANNUAL CAP VS MONTHLY TRACKING UNIVERSES")
print("=" * 80)

# Check if annual results exist for comparison
annual_file = Path(f"ml_{CONFIG['model_name']}_results") / "portfolio_returns_hybrid_stoploss.csv"

if annual_file.exists():
    print(f"\n📂 Loading files for comparison...")

    # Load annual cap results
    annual_df = pd.read_csv(annual_file)
    print(f"   Annual cap results: {len(annual_df)} years")

    # Load monthly tracking results
    monthly_file = Path(CONFIG['output_dir']) / "portfolio_monthly_sl.csv"
    monthly_df = pd.read_csv(monthly_file)
    print(f"   Monthly tracking results: {len(monthly_df)} years")

    # Load original data to check universes
    print(f"\n📊 Analyzing stock universes...")

    # Load predictions
    predictions = load_predictions(CONFIG['pred_dir'])
    print(f"   Total predictions: {len(predictions):,}")

    # Load monthly returns
    monthly_returns = pd.read_parquet(CONFIG['monthly_returns_file'])
    print(f"   Observations with monthly data: {len(monthly_returns):,}")

    # Merge to see what monthly tracking uses
    predictions['form_date'] = pd.to_datetime(predictions['form_date'])
    monthly_returns['form_date'] = pd.to_datetime(monthly_returns['form_date'])

    merged = predictions.merge(
        monthly_returns,
        on=['permno', 'form_date'],
        how='inner'
    )
    print(f"   After merge (monthly tracking universe): {len(merged):,}")

    # Calculate filtering
    filtered_out = len(predictions) - len(merged)
    filter_pct = (filtered_out / len(predictions)) * 100

    print(f"\n🔍 KEY FINDING:")
    print(f"   Stocks filtered out: {filtered_out:,} ({filter_pct:.1f}%)")
    print(f"   This means:")
    print(f"   - Annual cap uses: {len(predictions):,} predictions")
    print(f"   - Monthly tracking uses: {len(merged):,} predictions (only those with monthly data)")

    # Year-by-year comparison
    print(f"\n📈 YEAR-BY-YEAR UNIVERSE COMPARISON:")
    print("=" * 80)

    comparison_data = []

    if 'form_year' not in merged.columns:
        merged['form_year'] = pd.to_datetime(merged['form_date']).dt.year
    if 'form_year' not in predictions.columns:
        predictions['form_year'] = pd.to_datetime(predictions['form_date']).dt.year

    for year in sorted(merged['form_year'].unique()):
        year_pred = predictions[predictions['form_year'] == year]
        year_merged = merged[merged['form_year'] == year]

        n_pred = len(year_pred)
        n_merged = len(year_merged)
        n_filtered = n_pred - n_merged

        # Get returns from results
        annual_year = annual_df[annual_df['year'] == year]
        monthly_year = monthly_df[monthly_df['year'] == year]

        if len(annual_year) > 0 and len(monthly_year) > 0:
            comparison_data.append({
                'year': year,
                'predictions_total': n_pred,
                'monthly_tracking': n_merged,
                'filtered_out': n_filtered,
                'filter_pct': (n_filtered / n_pred * 100) if n_pred > 0 else 0,
                'annual_short_return': annual_year.iloc[0]['short_return'],
                'monthly_short_return': monthly_year.iloc[0]['short_return'],
                'return_diff': monthly_year.iloc[0]['short_return'] - annual_year.iloc[0]['short_return'],
                'annual_n_short': annual_year.iloc[0]['n_short'],
                'monthly_n_short': monthly_year.iloc[0]['n_short']
            })

            print(f"Year {int(year)}:")
            print(f"   Universe: {n_pred:,} total → {n_merged:,} after filter ({n_filtered:,} removed, {(n_filtered/n_pred*100):.1f}%)")
            print(f"   Short returns: Annual={annual_year.iloc[0]['short_return']:+.4f}, Monthly={monthly_year.iloc[0]['short_return']:+.4f} (Δ={monthly_year.iloc[0]['short_return'] - annual_year.iloc[0]['short_return']:+.4f})")

    # Create comparison DataFrame
    comparison_df = pd.DataFrame(comparison_data)

    # Summary statistics
    print(f"\n📊 SUMMARY STATISTICS:")
    print("=" * 80)
    print(f"Average stocks filtered per year: {comparison_df['filtered_out'].mean():.0f} ({comparison_df['filter_pct'].mean():.1f}%)")
    print(f"Average short return difference: {comparison_df['return_diff'].mean():+.4f}")
    print(f"Correlation between filter% and return diff: {comparison_df['filter_pct'].corr(comparison_df['return_diff']):.3f}")

    # Save diagnostic
    output_dir = Path(CONFIG['output_dir'])
    diag_file = output_dir / "diagnostic_universe_comparison.csv"
    comparison_df.to_csv(diag_file, index=False)
    print(f"\n✅ Diagnostic saved to: {diag_file}")

    print(f"\n💡 CONCLUSION:")
    if filter_pct > 5:
        print(f"   The {filter_pct:.1f}% difference in stock universes likely explains")
        print(f"   the return difference between annual cap and monthly tracking.")
        print(f"   Monthly tracking excludes stocks without complete monthly data.")
    else:
        print(f"   Stock universes are very similar ({filter_pct:.1f}% difference).")
        print(f"   Return difference must be due to other factors.")

else:
    print("\n⚠️  Annual results not found - cannot run diagnostic comparison")
    print("   Run 03-machine-learning-xgboost.py with Strategy 5 first")

print("\n" + "=" * 80)
# %%
