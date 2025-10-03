## Process the computation for the GHZ94 dataset

## Baseline Period: 1987-2019
param_in = param_baseline_ghz94
param_in$version = BASELINE_VERSION

output = read_gc_results_by_methods(param_in) 

## Table 4: Performance of Portfolios Sorted by ML Predicted Returns on the GHZ Sample (1987-2019)
tab_1 = output %>% 
    filter(rank == 11) %>% 
    select(weight, method, Avg = ex, tstat = ex_t, SR = sr, capm, capm_t, ff3, ff3_t, carhart, carhart_t, ff5, ff5_t, ff5m, ff5m_t, q, q_t) %>% 
    arrange(weight)

write.csv(tab_1,
          file = paste0(TABLE_PATH, "Table-04", "_", param_in$version, ".csv"),
          row.names = FALSE)

