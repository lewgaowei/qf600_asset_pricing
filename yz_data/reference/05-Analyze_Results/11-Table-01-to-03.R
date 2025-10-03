# Generate tables for Table 1, 2, 3
# Baseline is the prediction using YZ's 18K signals. 

## Read the baseline prediction.
param_in = param_baseline
param_in$version = BASELINE_VERSION  

output = output_by_methods_funda_annual_rebalancing(param_in) 

## Table 1: Performance of Portfolios Sorted by BRT Predicted Returns
tab_1 = output %>% 
    filter(method == 'brt') %>% 
    select(weight, Pred = pred, Avg = ex, tstat = ex_t, SD = std, SR = sr)

write.csv(tab_1,
          file = paste0(TABLE_PATH, "Table-01", "_", param_in$version, ".csv"),
          row.names = FALSE)

## Table 2: Risk-adjusted Performance of Portfolios Sorted by BRT Predicted Returns
tab_2 = output %>% 
    filter(method == 'brt') %>% 
    select(weight, capm, capm_t, ff3, ff3_t, carhart, carhart_t, ff5, ff5_t, ff5m, ff5m_t, q, q_t)

write.csv(tab_2,
          file = paste0(TABLE_PATH, "Table-02", "_", param_in$version, ".csv"),
          row.names = FALSE)

## Table 3: Performance of Portfolios Sorted by NN Predicted Returns
tab_3 = output %>% 
    filter(method %in% c('nn1', 'nn2', 'nn3', 'nn4', 'nn5'), 
           rank == 11) %>% 
    select(weight, method, 
           Avg = ex, tstat = ex_t, 
           SR = sr, 
           capm, capm_t, ff3, ff3_t, 
           carhart, carhart_t, ff5, ff5_t, 
           ff5m, ff5m_t, q, q_t) %>% 
    arrange(weight)

write.csv(tab_3,
          file = paste0(TABLE_PATH, "Table-03", "_", param_in$version, ".csv"),
          row.names = FALSE)
