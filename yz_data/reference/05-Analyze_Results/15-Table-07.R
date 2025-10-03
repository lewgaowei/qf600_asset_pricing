# This function generates Table 07. 

## Read the results for PR119 and PR120. 
param_in = param_baseline_tech119
param_in$version = BASELINE_VERSION
output_tech119 = read_tech_results_by_methods(param_in) 

param_in = param_baseline_tech120
param_in$version = BASELINE_VERSION
output_tech120 = read_tech_results_by_methods(param_in) 

## Table 7: Performance of Portfolios Constructed Using Past-Return Signals
output_tech120_short = output_tech120 %>% 
    filter(rank == 11) %>% 
    arrange(weight)

output_tech119_short = output_tech119 %>% 
    filter(rank == 11) %>% 
    arrange(weight)

tab = bind_rows(output_tech119_short, output_tech120_short)

write.csv(tab,
          file = paste0(TABLE_PATH, "Table-07", "_", param_in$version, ".csv"),
          row.names = FALSE)


