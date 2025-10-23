# %% [markdown]
# # EDA: Backtesting Results Analysis
#
# This notebook analyzes the backtesting results from all models and strategies.

# %%
import pandas as pd
import numpy as np
from lets_plot import *

# Initialize lets-plot
LetsPlot.setup_html()

# %% [markdown]
# ## 1. Load Data

# %%
# Load the summary results
df = pd.read_csv('final_output/summary/all_results_summary.csv')

# ====================================
# TOGGLE: Exclude Annual Stop Loss Strategy
# Set to True to exclude, False to include
# ====================================
EXCLUDE_ANNUAL_STOPLOSS = True

if EXCLUDE_ANNUAL_STOPLOSS:
    df = df[df['strategy'] != 'Annual Stop Loss'].reset_index(drop=True)
    print("⚠️  Annual Stop Loss strategy has been EXCLUDED from the analysis")
else:
    print("ℹ️  All strategies including Annual Stop Loss are INCLUDED")

print(f"\nDataset shape: {df.shape}")
print(f"\nColumns: {df.columns.tolist()}")
print(f"\nFirst few rows:")
print(df[['model', 'strategy', 'sharpe_ratio_dollar', 'cagr_pct', 'max_drawdown_pct', 'win_rate_pct']].head())
df.head()

# %% [markdown]
# ## 2. Data Overview

# %%
# Display basic statistics
print("Basic Statistics:")
print(df.describe())

# %%
# Check for missing values
print("\nMissing Values:")
print(df.isnull().sum())

# %% [markdown]
# ## 3. Bar Charts - Performance Metrics

# %% [markdown]
# ### 3.1 Top 10 Models by Sharpe Ratio

# %%
# Sort by Sharpe Ratio and get top 10
top_sharpe = df.nlargest(10, 'sharpe_ratio_dollar').reset_index(drop=True)
top_sharpe['label'] = top_sharpe['model'] + '\n' + top_sharpe['strategy']
top_sharpe['text_label'] = top_sharpe['sharpe_ratio_dollar'].apply(lambda x: f'{x:.2f}')

(ggplot(top_sharpe, aes(x='label', y='sharpe_ratio_dollar')) +
 geom_bar(stat='identity', fill='steelblue', alpha=0.8) +
 geom_text(aes(label='text_label'), va='bottom', size=9) +
 labs(title='Top 10 Models by Sharpe Ratio (Dollar Returns)',
      x='Model + Strategy',
      y='Sharpe Ratio') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       panel_grid_major_x=element_blank()) +
 ggsize(900, 500))

# %% [markdown]
# ### 3.2 Top 10 Models by CAGR

# %%
# Sort by CAGR and get top 10
top_cagr = df.nlargest(10, 'cagr_pct').reset_index(drop=True)
top_cagr['label'] = top_cagr['model'] + '\n' + top_cagr['strategy']
top_cagr['text_label'] = top_cagr['cagr_pct'].apply(lambda x: f'{x:.1f}%')

(ggplot(top_cagr, aes(x='label', y='cagr_pct')) +
 geom_bar(stat='identity', fill='forestgreen', alpha=0.8) +
 geom_text(aes(label='text_label'), va='bottom', size=9) +
 labs(title='Top 10 Models by CAGR',
      x='Model + Strategy',
      y='CAGR (%)') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       panel_grid_major_x=element_blank()) +
 ggsize(900, 500))

# %% [markdown]
# ### 3.3 Top 10 Models by Win Rate

# %%
# Sort by win_rate and get top 10
top_winrate = df.nlargest(10, 'win_rate_pct').reset_index(drop=True)
top_winrate['label'] = top_winrate['model'] + '\n' + top_winrate['strategy']
top_winrate['text_label'] = top_winrate['win_rate_pct'].apply(lambda x: f'{x:.1f}%')

(ggplot(top_winrate, aes(x='label', y='win_rate_pct')) +
 geom_bar(stat='identity', fill='coral', alpha=0.8) +
 geom_text(aes(label='text_label'), va='bottom', size=9) +
 labs(title='Top 10 Models by Win Rate',
      x='Model + Strategy',
      y='Win Rate (%)') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       panel_grid_major_x=element_blank()) +
 ggsize(900, 500))

# %% [markdown]
# ### 3.4 Top 10 Models by Max Drawdown (Lowest)

# %%
# Sort by max_drawdown (ascending, as we want lowest drawdown - note: these are negative values)
top_drawdown = df.nsmallest(10, 'max_drawdown_pct').reset_index(drop=True)
top_drawdown['label'] = top_drawdown['model'] + '\n' + top_drawdown['strategy']
top_drawdown['text_label'] = top_drawdown['max_drawdown_pct'].apply(lambda x: f'{x:.1f}%')

(ggplot(top_drawdown, aes(x='label', y='max_drawdown_pct')) +
 geom_bar(stat='identity', fill='crimson', alpha=0.8) +
 geom_text(aes(label='text_label'), va='bottom', size=9) +
 labs(title='Top 10 Models by Lowest Max Drawdown',
      x='Model + Strategy',
      y='Max Drawdown (%)') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       panel_grid_major_x=element_blank()) +
 ggsize(900, 500))

# %% [markdown]
# ### 3.5 Comparison by Model Type

# %%
# Average metrics by model
model_avg = df.groupby('model').agg({
    'sharpe_ratio_dollar': 'mean',
    'cagr_pct': 'mean',
    'max_drawdown_pct': 'mean',
    'win_rate_pct': 'mean'
}).reset_index()

# Sharpe Ratio by Model
p1 = (ggplot(model_avg, aes(x='model', y='sharpe_ratio_dollar')) +
      geom_bar(stat='identity', fill='steelblue', alpha=0.8) +
      labs(title='Average Sharpe Ratio by Model',
           x='Model',
           y='Average Sharpe Ratio') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

# CAGR by Model
p2 = (ggplot(model_avg, aes(x='model', y='cagr_pct')) +
      geom_bar(stat='identity', fill='forestgreen', alpha=0.8) +
      labs(title='Average CAGR by Model',
           x='Model',
           y='Average CAGR (%)') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

# Max Drawdown by Model
p3 = (ggplot(model_avg, aes(x='model', y='max_drawdown_pct')) +
      geom_bar(stat='identity', fill='crimson', alpha=0.8) +
      labs(title='Average Max Drawdown by Model',
           x='Model',
           y='Average Max Drawdown (%)') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

# Win Rate by Model
p4 = (ggplot(model_avg, aes(x='model', y='win_rate_pct')) +
      geom_bar(stat='identity', fill='coral', alpha=0.8) +
      labs(title='Average Win Rate by Model',
           x='Model',
           y='Average Win Rate (%)') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

gggrid([p1, p2, p3, p4], ncol=2)

# %% [markdown]
# ### 3.6 Comparison by Strategy Type

# %%
# Average metrics by strategy
strategy_avg = df.groupby('strategy').agg({
    'sharpe_ratio_dollar': 'mean',
    'cagr_pct': 'mean',
    'max_drawdown_pct': 'mean',
    'win_rate_pct': 'mean'
}).reset_index()

# Sharpe Ratio by Strategy
p1 = (ggplot(strategy_avg, aes(x='strategy', y='sharpe_ratio_dollar')) +
      geom_bar(stat='identity', fill='steelblue', alpha=0.8) +
      labs(title='Average Sharpe Ratio by Strategy',
           x='Strategy',
           y='Average Sharpe Ratio') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

# CAGR by Strategy
p2 = (ggplot(strategy_avg, aes(x='strategy', y='cagr_pct')) +
      geom_bar(stat='identity', fill='forestgreen', alpha=0.8) +
      labs(title='Average CAGR by Strategy',
           x='Strategy',
           y='Average CAGR (%)') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

# Max Drawdown by Strategy
p3 = (ggplot(strategy_avg, aes(x='strategy', y='max_drawdown_pct')) +
      geom_bar(stat='identity', fill='crimson', alpha=0.8) +
      labs(title='Average Max Drawdown by Strategy',
           x='Strategy',
           y='Average Max Drawdown (%)') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

# Win Rate by Strategy
p4 = (ggplot(strategy_avg, aes(x='strategy', y='win_rate_pct')) +
      geom_bar(stat='identity', fill='coral', alpha=0.8) +
      labs(title='Average Win Rate by Strategy',
           x='Strategy',
           y='Average Win Rate (%)') +
      theme_minimal() +
      theme(axis_text_x=element_text(angle=45, hjust=1),
            axis_title=element_text(face='bold'),
            plot_title=element_text(face='bold'),
            panel_grid_major_x=element_blank()) +
      ggsize(700, 500))

gggrid([p1, p2, p3, p4], ncol=2)

# %% [markdown]
# ### 3.7 Model Count by Type

# %%
# Count of results by model
model_counts_df = df['model'].value_counts().reset_index()
model_counts_df.columns = ['model', 'count']
model_counts_df['text_label'] = model_counts_df['count'].astype(str)

(ggplot(model_counts_df, aes(x='model', y='count')) +
 geom_bar(stat='identity', fill='mediumpurple', alpha=0.8) +
 geom_text(aes(label='text_label'), va='bottom', size=10) +
 labs(title='Number of Strategy Results by Model Type',
      x='Model',
      y='Number of Strategy Results') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       panel_grid_major_x=element_blank()) +
 ggsize(800, 500))

# %% [markdown]
# ## 4. Summary Statistics

# %%
print("\n" + "="*80)
print("SUMMARY STATISTICS")
print("="*80)

print(f"\nTotal number of backtesting results: {len(df)}")
print(f"Number of unique models: {df['model'].nunique()}")
print(f"Number of unique strategies: {df['strategy'].nunique()}")

print("\n" + "-"*80)
print("BEST OVERALL PERFORMERS")
print("-"*80)

best_sharpe = df.loc[df['sharpe_ratio_dollar'].idxmax()]
print(f"\nBest Sharpe Ratio: {best_sharpe['sharpe_ratio_dollar']:.3f}")
print(f"  Model: {best_sharpe['model']}, Strategy: {best_sharpe['strategy']}")

best_cagr = df.loc[df['cagr_pct'].idxmax()]
print(f"\nBest CAGR: {best_cagr['cagr_pct']:.2f}%")
print(f"  Model: {best_cagr['model']}, Strategy: {best_cagr['strategy']}")

best_winrate = df.loc[df['win_rate_pct'].idxmax()]
print(f"\nBest Win Rate: {best_winrate['win_rate_pct']:.2f}%")
print(f"  Model: {best_winrate['model']}, Strategy: {best_winrate['strategy']}")

best_drawdown = df.loc[df['max_drawdown_pct'].idxmin()]
print(f"\nLowest Max Drawdown: {best_drawdown['max_drawdown_pct']:.2f}%")
print(f"  Model: {best_drawdown['model']}, Strategy: {best_drawdown['strategy']}")

print("\n" + "="*80)

# %% [markdown]
# ## 5. Line Chart Comparisons

# %% [markdown]
# ### 5.1 Model Performance Across Strategies

# %%
# Load model comparison data (has multi-level headers)
model_comp_raw = pd.read_csv('final_output/comparisons/model_comparison.csv', header=[0, 1])

# Extract the data properly
models = ['CatBoost', 'ElasticNet', 'GP Symbolic', 'LSTM', 'LightGBM', 'XGBoost']
metrics_info = [
    ('cagr_pct', 'CAGR (%)'),
    ('max_drawdown_pct', 'Max Drawdown (%)'),
    ('sharpe_ratio_dollar', 'Sharpe Ratio'),
    ('win_rate_pct', 'Win Rate (%)')
]

# Reshape data for plotting
model_comp_long = []

# Get strategy column - it's the first column with multi-index ('Unnamed: 0_level_0', 'model')
strategies = model_comp_raw[('Unnamed: 0_level_0', 'model')].tolist()[1:]  # Skip first row which says "strategy"

for idx, strategy in enumerate(strategies):
    if EXCLUDE_ANNUAL_STOPLOSS and strategy == 'Annual Stop Loss':
        continue
    for model in models:
        for metric_col, metric_name in metrics_info:
            try:
                # Access the value using multi-level column indexing
                val = model_comp_raw[(metric_col, model)].iloc[idx + 1]
                model_comp_long.append({
                    'strategy': strategy,
                    'model': model,
                    'metric': metric_name,
                    'value': float(val) if pd.notna(val) else 0
                })
            except:
                pass

model_comp_df = pd.DataFrame(model_comp_long)

# %%
# Plot CAGR by Strategy (all models)
cagr_data = model_comp_df[model_comp_df['metric'] == 'CAGR (%)'].copy()

(ggplot(cagr_data, aes(x='strategy', y='value', color='model', group='model')) +
 geom_line(size=1.2) +
 geom_point(size=3) +
 labs(title='CAGR Comparison: All Models Across Strategies',
      x='Strategy',
      y='CAGR (%)') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       legend_position='right') +
 ggsize(1000, 500))

# %%
# Plot Sharpe Ratio by Strategy (all models)
sharpe_data = model_comp_df[model_comp_df['metric'] == 'Sharpe Ratio'].copy()

(ggplot(sharpe_data, aes(x='strategy', y='value', color='model', group='model')) +
 geom_line(size=1.2) +
 geom_point(size=3) +
 labs(title='Sharpe Ratio Comparison: All Models Across Strategies',
      x='Strategy',
      y='Sharpe Ratio') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       legend_position='right') +
 ggsize(1000, 500))

# %%
# Plot Max Drawdown by Strategy (all models)
dd_data = model_comp_df[model_comp_df['metric'] == 'Max Drawdown (%)'].copy()

(ggplot(dd_data, aes(x='strategy', y='value', color='model', group='model')) +
 geom_line(size=1.2) +
 geom_point(size=3) +
 labs(title='Max Drawdown Comparison: All Models Across Strategies',
      x='Strategy',
      y='Max Drawdown (%)') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       legend_position='right') +
 ggsize(1000, 500))

# %%
# Plot Win Rate by Strategy (all models)
wr_data = model_comp_df[model_comp_df['metric'] == 'Win Rate (%)'].copy()

(ggplot(wr_data, aes(x='strategy', y='value', color='model', group='model')) +
 geom_line(size=1.2) +
 geom_point(size=3) +
 labs(title='Win Rate Comparison: All Models Across Strategies',
      x='Strategy',
      y='Win Rate (%)') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       legend_position='right') +
 ggsize(1000, 500))

# %% [markdown]
# ### 5.2 Strategy Performance Across Models

# %%
# Load strategy comparison data (has multi-level headers)
strategy_comp_raw = pd.read_csv('final_output/comparisons/strategy_comparison.csv', header=[0, 1])

# Reshape data for plotting
strategy_comp_long = []

strategies_list = ['Annual Stop Loss', 'Decile 10%/10%', 'Fixed 100/100',
                   'Hybrid 10%/100', 'Hybrid 10%/100 (50% L/S)', 'Monthly Stop Loss']

# Get model column - it's the first column with multi-index ('Unnamed: 0_level_0', 'strategy')
models_in_file = strategy_comp_raw[('Unnamed: 0_level_0', 'strategy')].tolist()[1:]  # Skip first row which says "model"

for idx, model in enumerate(models_in_file):
    for strategy in strategies_list:
        if EXCLUDE_ANNUAL_STOPLOSS and strategy == 'Annual Stop Loss':
            continue
        for metric_col, metric_name in metrics_info:
            try:
                # Access the value using multi-level column indexing
                val = strategy_comp_raw[(metric_col, strategy)].iloc[idx + 1]
                strategy_comp_long.append({
                    'model': model,
                    'strategy': strategy,
                    'metric': metric_name,
                    'value': float(val) if pd.notna(val) else 0
                })
            except:
                pass

strategy_comp_df = pd.DataFrame(strategy_comp_long)

# %%
# Plot CAGR by Model (all strategies)
cagr_strat = strategy_comp_df[strategy_comp_df['metric'] == 'CAGR (%)'].copy()

(ggplot(cagr_strat, aes(x='model', y='value', color='strategy', group='strategy')) +
 geom_line(size=1.2) +
 geom_point(size=3) +
 labs(title='CAGR Comparison: All Strategies Across Models',
      x='Model',
      y='CAGR (%)') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       legend_position='right') +
 ggsize(1000, 500))

# %%
# Plot Sharpe Ratio by Model (all strategies)
sharpe_strat = strategy_comp_df[strategy_comp_df['metric'] == 'Sharpe Ratio'].copy()

(ggplot(sharpe_strat, aes(x='model', y='value', color='strategy', group='strategy')) +
 geom_line(size=1.2) +
 geom_point(size=3) +
 labs(title='Sharpe Ratio Comparison: All Strategies Across Models',
      x='Model',
      y='Sharpe Ratio') +
 theme_minimal() +
 theme(axis_text_x=element_text(angle=45, hjust=1, size=9),
       axis_title=element_text(size=12, face='bold'),
       plot_title=element_text(size=14, face='bold'),
       legend_position='right') +
 ggsize(1000, 500))

# %% [markdown]
# ## 6. Time Series Analysis (Test Period)

# %% [markdown]
# ### 6.1 Load Portfolio Time Series Data

# %%
# Load time series data for top performers
import os
import glob

# Get top 5 model+strategy combinations by Sharpe Ratio
top_combos = df.nlargest(5, 'sharpe_ratio_dollar')[['model', 'strategy', 'sharpe_ratio_dollar']].copy()

print("Top 5 combinations by Sharpe Ratio:")
print(top_combos)

# Map strategy names to file names
strategy_file_map = {
    'Fixed 100/100': 'portfolio_fixed_100.csv',
    'Decile 10%/10%': 'portfolio_decile_10pct.csv',
    'Hybrid 10%/100': 'portfolio_hybrid_10_100.csv',
    'Hybrid 10%/100 (50% L/S)': 'portfolio_hybrid_5050.csv',
    'Annual Stop Loss': 'portfolio_annual_stoploss.csv',
    'Monthly Stop Loss': 'portfolio_monthly_stoploss.csv'
}

# Map model names to folder names
model_folder_map = {
    'GP Symbolic': 'gp_symbolic',
    'XGBoost': 'xgboost',
    'LightGBM': 'lightgbm',
    'CatBoost': 'catboost',
    'ElasticNet': 'elasticnet',
    'LSTM': 'lstm'
}

# Load portfolio data for each top combo
portfolio_data = []
for idx, row in top_combos.iterrows():
    model = row['model']
    strategy = row['strategy']

    model_folder = model_folder_map.get(model)
    strategy_file = strategy_file_map.get(strategy)

    if model_folder and strategy_file:
        file_path = f'final_output/models/{model_folder}/{strategy_file}'
        if os.path.exists(file_path):
            ts_df = pd.read_csv(file_path)
            ts_df['model'] = model
            ts_df['strategy'] = strategy
            ts_df['combo'] = f"{model}\n{strategy}"
            portfolio_data.append(ts_df)
            print(f"✓ Loaded: {model} - {strategy}")
        else:
            print(f"✗ File not found: {file_path}")
    else:
        print(f"✗ Missing mapping for: {model} - {strategy}")

# Combine all portfolio data
if portfolio_data:
    portfolio_ts = pd.concat(portfolio_data, ignore_index=True)
    print(f"\n✓ Successfully loaded time series data for {len(portfolio_data)} model+strategy combinations")
    print(f"  Years covered: {portfolio_ts['year'].min():.0f} to {portfolio_ts['year'].max():.0f}")
    print(f"  Total rows: {len(portfolio_ts)}")
else:
    print("\n⚠️ No portfolio data loaded - time series charts will not be displayed")
    portfolio_ts = pd.DataFrame()

# %% [markdown]
# ### 6.2 Cumulative Returns Over Time

# %%
if not portfolio_ts.empty:
    # Calculate cumulative returns
    portfolio_ts_sorted = portfolio_ts.sort_values(['model', 'strategy', 'year']).copy()

    # Add a baseline year (year before first data year) with 0% return and $1M value
    baseline_year = int(portfolio_ts_sorted['year'].min()) - 1
    baseline_rows = []

    for combo in portfolio_ts_sorted['combo'].unique():
        model = portfolio_ts_sorted[portfolio_ts_sorted['combo'] == combo]['model'].iloc[0]
        strategy = portfolio_ts_sorted[portfolio_ts_sorted['combo'] == combo]['strategy'].iloc[0]
        baseline_rows.append({
            'year': baseline_year,
            'portfolio_return': 0.0,
            'model': model,
            'strategy': strategy,
            'combo': combo
        })

    baseline_df = pd.DataFrame(baseline_rows)
    portfolio_ts_sorted = pd.concat([baseline_df, portfolio_ts_sorted], ignore_index=True).sort_values(['model', 'strategy', 'year'])

    # Calculate cumulative product of (1 + return) for each strategy, then subtract 1
    portfolio_ts_sorted['cumulative_return'] = portfolio_ts_sorted.groupby(['model', 'strategy'])['portfolio_return'].transform(
        lambda x: (1 + x).cumprod() - 1
    )
    portfolio_ts_sorted['cumulative_value'] = (1 + portfolio_ts_sorted['cumulative_return']) * 1000000  # Starting with $1M

    print(f"Time series data shape: {portfolio_ts_sorted.shape}")
    print(f"Unique combinations: {portfolio_ts_sorted['combo'].nunique()}")
    print(f"Year range: {portfolio_ts_sorted['year'].min():.0f} - {portfolio_ts_sorted['year'].max():.0f}")

    # Check starting values
    print(f"\nBaseline year {baseline_year} values (should all be $1,000,000):")
    for combo in portfolio_ts_sorted['combo'].unique():
        first_val = portfolio_ts_sorted[portfolio_ts_sorted['combo'] == combo]['cumulative_value'].iloc[0]
        print(f"  {combo}: ${first_val:,.0f}")

    p_cumulative = (ggplot(portfolio_ts_sorted, aes(x='year', y='cumulative_value', color='combo', group='combo')) +
                    geom_line(size=1.5) +
                    geom_point(size=2.5) +
                    labs(title='Cumulative Portfolio Value Over Time (Starting: $1M)',
                         x='Year',
                         y='Portfolio Value ($)') +
                    theme_minimal() +
                    theme(axis_title=element_text(size=12, face='bold'),
                          plot_title=element_text(size=14, face='bold'),
                          legend_position='right',
                          legend_title=element_text(face='bold')) +
                    ggsize(1200, 600))
    p_cumulative.show()
else:
    print("⚠️ No portfolio time series data available to plot")

# %% [markdown]
# ### 6.2b Cumulative Returns vs S&P 500 Benchmark

# %%
if not portfolio_ts.empty:
    import yfinance as yf

    # Get S&P 500 data
    start_year = int(portfolio_ts_sorted['year'].min())
    end_year = int(portfolio_ts_sorted['year'].max()) + 1

    print(f"Downloading S&P 500 data from {start_year} to {end_year}...")

    # Download S&P 500 data
    sp500_raw = yf.download('^GSPC', start=f'{start_year}-01-01', end=f'{end_year}-01-01', progress=False)

    # Debug: Print raw data info
    print(f"\nS&P 500 raw data shape: {sp500_raw.shape}")
    print(f"S&P 500 columns: {sp500_raw.columns.tolist()}")
    print(f"S&P 500 column type: {type(sp500_raw.columns)}")

    # Flatten multi-index columns if present
    if isinstance(sp500_raw.columns, pd.MultiIndex):
        print("\nFlattening multi-index columns...")
        sp500_raw.columns = sp500_raw.columns.get_level_values(0)
        print(f"Columns after flattening: {sp500_raw.columns.tolist()}")

    # Get close prices (yfinance sometimes doesn't return 'Adj Close' for single ticker)
    if 'Adj Close' in sp500_raw.columns:
        sp500_prices = sp500_raw['Adj Close'].copy()
        print(f"\nUsing 'Adj Close' column")
    else:
        sp500_prices = sp500_raw['Close'].copy()
        print(f"\nUsing 'Close' column (Adj Close not available)")

    print(f"S&P 500 prices extracted: {len(sp500_prices)} rows")

    # Calculate annual returns for S&P 500
    sp500_yearly_prices = sp500_prices.resample('YE').last()
    sp500_annual_returns = sp500_yearly_prices.pct_change()

    # Create dataframe with year and returns
    sp500_annual_df = pd.DataFrame({
        'year': sp500_yearly_prices.index.year,
        'price': sp500_yearly_prices.values,
        'annual_return': sp500_annual_returns.values
    })

    # Add baseline year (same as portfolio baseline) with 0% return
    sp500_baseline = pd.DataFrame({
        'year': [baseline_year],
        'price': [sp500_prices.iloc[0]],
        'annual_return': [0.0]
    })
    sp500_annual_df = pd.concat([sp500_baseline, sp500_annual_df], ignore_index=True)

    print(f"S&P 500 annual returns calculated for {len(sp500_annual_df)} years (including baseline)")

    # Calculate cumulative returns for S&P 500 - starting from $1M
    sp500_annual_df['cumulative_return'] = (1 + sp500_annual_df['annual_return']).cumprod() - 1
    sp500_annual_df['cumulative_value'] = (1 + sp500_annual_df['cumulative_return']) * 1000000
    sp500_annual_df['combo'] = 'S&P 500 Benchmark'

    print(f"S&P 500 baseline year {baseline_year} value: ${sp500_annual_df['cumulative_value'].iloc[0]:,.0f}")
    print(f"S&P 500 ending value: ${sp500_annual_df['cumulative_value'].iloc[-1]:,.0f}")

    # Combine portfolio data with S&P 500
    portfolio_with_sp500 = pd.concat([
        portfolio_ts_sorted[['year', 'cumulative_value', 'combo']],
        sp500_annual_df[['year', 'cumulative_value', 'combo']]
    ], ignore_index=True)

    print(f"S&P 500 data loaded: {len(sp500_annual_df)} years")

    # Define a professional color palette
    # Top 5 strategies get distinct colors, S&P 500 gets neutral gray
    color_palette = {
        'S&P 500 Benchmark': '#757575',  # Gray for benchmark
        # Vibrant colors for strategies
        'color_1': '#2E7D32',  # Forest Green
        'color_2': '#1976D2',  # Blue
        'color_3': '#D32F2F',  # Red
        'color_4': '#7B1FA2',  # Purple
        'color_5': '#F57C00',  # Orange
    }

    # Assign colors to each strategy (non-benchmark combos)
    unique_combos = [c for c in portfolio_with_sp500['combo'].unique() if c != 'S&P 500 Benchmark']
    color_map = {'S&P 500 Benchmark': color_palette['S&P 500 Benchmark']}
    for idx, combo in enumerate(unique_combos):
        color_map[combo] = color_palette[f'color_{idx + 1}']

    # Create manual color scale for lets-plot
    color_values = [color_map[c] for c in portfolio_with_sp500['combo'].unique()]

    # Plot with S&P 500 benchmark
    p_cumulative_vs_sp500 = (ggplot(portfolio_with_sp500, aes(x='year', y='cumulative_value', color='combo', group='combo')) +
                             geom_line(size=1.5, alpha=0.9) +
                             geom_point(size=2.5, alpha=0.8) +
                             scale_color_manual(values=color_values) +
                             scale_x_continuous(format='d') +  # Remove comma from years
                             scale_y_continuous(format='.0f') +  # Show full numbers without abbreviation
                             labs(title='Cumulative Portfolio Value vs S&P 500 Benchmark (Starting: $1M)',
                                  x='Year',
                                  y='Portfolio Value ($)',
                                  color='Model + Strategy') +
                             theme_minimal() +
                             theme(axis_title=element_text(size=12, face='bold'),
                                   plot_title=element_text(size=14, face='bold'),
                                   legend_position='right',
                                   legend_title=element_text(size=11, face='bold'),
                                   legend_text=element_text(size=9),
                                   panel_grid_minor=element_blank()) +
                             ggsize(1200, 600))
    p_cumulative_vs_sp500.show()
else:
    print("⚠️ No portfolio time series data available to plot")

# %% [markdown]
# ### 6.3 Annual Returns Comparison

# %%
if not portfolio_ts.empty:
    portfolio_ts_sorted['return_pct'] = portfolio_ts_sorted['portfolio_return'] * 100

    p_returns = (ggplot(portfolio_ts_sorted, aes(x='year', y='return_pct', color='combo', group='combo')) +
                 geom_line(size=1.5) +
                 geom_point(size=2.5) +
                 geom_hline(yintercept=0, linetype='dashed', color='red', alpha=0.5) +
                 labs(title='Annual Returns Over Time',
                      x='Year',
                      y='Annual Return (%)') +
                 theme_minimal() +
                 theme(axis_title=element_text(size=12, face='bold'),
                       plot_title=element_text(size=14, face='bold'),
                       legend_position='right',
                       legend_title=element_text(face='bold')) +
                 ggsize(1200, 600))
    p_returns.show()
else:
    print("⚠️ No portfolio time series data available to plot")

# %% [markdown]
# ### 6.4 Annual PnL Comparison

# %%
if not portfolio_ts.empty:
    portfolio_ts_sorted['total_pnl_thousands'] = portfolio_ts_sorted['total_dollar_pnl'] / 1000

    p_pnl = (ggplot(portfolio_ts_sorted, aes(x='year', y='total_pnl_thousands', color='combo', group='combo')) +
             geom_line(size=1.5) +
             geom_point(size=2.5) +
             geom_hline(yintercept=0, linetype='dashed', color='red', alpha=0.5) +
             labs(title='Annual Dollar PnL Over Time',
                  x='Year',
                  y='Annual PnL ($1000s)') +
             theme_minimal() +
             theme(axis_title=element_text(size=12, face='bold'),
                   plot_title=element_text(size=14, face='bold'),
                   legend_position='right',
                   legend_title=element_text(face='bold')) +
             ggsize(1200, 600))
    p_pnl.show()
else:
    print("⚠️ No portfolio time series data available to plot")

# %% [markdown]
# ### 6.5 Strategy Spread Analysis

# %%
if not portfolio_ts.empty:
    p_spread = (ggplot(portfolio_ts_sorted, aes(x='year', y='spread', color='combo', group='combo')) +
                geom_line(size=1.5) +
                geom_point(size=2.5) +
                geom_hline(yintercept=0, linetype='dashed', color='black', alpha=0.5) +
                labs(title='Long-Short Spread Over Time',
                     x='Year',
                     y='Spread (Long Return - Short Return)') +
                theme_minimal() +
                theme(axis_title=element_text(size=12, face='bold'),
                      plot_title=element_text(size=14, face='bold'),
                      legend_position='right',
                      legend_title=element_text(face='bold')) +
                ggsize(1200, 600))
    p_spread.show()
else:
    print("⚠️ No portfolio time series data available to plot")

# %%