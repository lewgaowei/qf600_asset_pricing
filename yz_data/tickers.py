import pandas as pd

# df = pd.read_parquet("/Users/ngchunyue/Documents/MQF/QF600 Asset Pricing/Project/copy_qf600/qf600_asset_pricing/yz_data/signals_with_returns_2015.parquet")
# # print(df.columns.to_list())
# if "permno" in df.columns:
#     print('"permno" found in df')
# else:
#     print('"permno" not in df')
# df1 = pd.read_csv("/Users/ngchunyue/Documents/MQF/QF600 Asset Pricing/Project/copy_qf600/qf600_asset_pricing/yz_data/formed_tickers.csv")
# print(df)
# print(df1)

# merged_df = df.merge(df1, on=["permno"])
# output_path = "/Users/ngchunyue/Documents/MQF/QF600 Asset Pricing/Project/copy_qf600/qf600_asset_pricing/yz_data/signals_with_returns_and_tickers_2015.parquet"
# merged_df.to_parquet(output_path)
# print(f"merged_df successfully saved to {output_path}")

# if "ticker" in merged_df.columns:
#     print('"ticker" in merged_df')
# else:
#     print('"ticker" not in merged_df')

missing_cols = []
df2 = pd.read_parquet("/Users/ngchunyue/Documents/MQF/QF600 Asset Pricing/Project/copy_qf600/qf600_asset_pricing/yz_data/signals_with_returns_and_tickers_2000.parquet", engine="fastparquet")
print(len(df2))
print([col for col in df2.columns if "year" in col])
df2 = df2[df2["fyear"]>=2015]
print(len(df2))
df2.to_parquet("/Users/ngchunyue/Documents/MQF/QF600 Asset Pricing/Project/copy_qf600/qf600_asset_pricing/yz_data/signals_with_returns_and_tickers_2015.parquet")
print("df2 successfully saved!")

matches = [col for col in df2.columns if "abs_correlation" in col]
if matches:
    print(matches)
else:
    print("none found")


# df2["datayear"] = pd.to_datetime(df2["datadate"]).dt.year
# df2 = df2[df2["datayear"]>=2015]

# print(df2.columns.to_list()[:200])
# print([col for col in df.columns if "date" in col])
# for col in df2:
#     if col not in df1.columns:
#         # print(f"{col} not found in df1")
#         missing_cols.append(col)
# print(missing_cols)