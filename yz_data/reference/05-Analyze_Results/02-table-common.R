## This file contains functions for generating results for the tables. 

# Using the monthly rebalance, but we used output_by_methods_funda_annual_rebalancing() instead. 
output_by_methods_funda <- function(param) {
    param$method = 'brt'; 
    output_brt = Output_portfolio_detail_python(param, ensemble = FALSE) |> 
        mutate(method = 'brt')
    
    param$method = 'nn'; param$hidden_layers = c(32); 
    output_nn1 = Output_portfolio_detail_python(param, ensemble = TRUE) |> 
        mutate(method = 'nn1')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16); 
    output_nn2 = Output_portfolio_detail_python(param, ensemble = TRUE) |> 
        mutate(method = 'nn2')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8); 
    output_nn3 = Output_portfolio_detail_python(param, ensemble = TRUE) |> 
        mutate(method = 'nn3')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4); 
    output_nn4 = Output_portfolio_detail_python(param, ensemble = TRUE) |> 
        mutate(method = 'nn4')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4, 2); 
    output_nn5 = Output_portfolio_detail_python(param, ensemble = TRUE) |> 
        mutate(method = 'nn5')
    
    output = rbind(output_brt, output_nn1, output_nn2, output_nn3, output_nn4, output_nn5)
    return(output)
}

# 
output_by_methods_funda_annual_rebalancing <- function(param) {
    param$method = 'brt';
    output_brt = Output_portfolio_detail_annual_rebalance(param, ensemble = FALSE) |>
        mutate(method = 'brt')
    
    param$method = 'nn'; param$hidden_layers = c(32); 
    output_nn1 = Output_portfolio_detail_annual_rebalance(param, ensemble = TRUE) |> 
        mutate(method = 'nn1')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16); 
    output_nn2 = Output_portfolio_detail_annual_rebalance(param, ensemble = TRUE) |> 
        mutate(method = 'nn2')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8); 
    output_nn3 = Output_portfolio_detail_annual_rebalance(param, ensemble = TRUE) |> 
        mutate(method = 'nn3')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4); 
    output_nn4 = Output_portfolio_detail_annual_rebalance(param, ensemble = TRUE) |> 
        mutate(method = 'nn4')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4, 2); 
    output_nn5 = Output_portfolio_detail_annual_rebalance(param, ensemble = TRUE) |> 
        mutate(method = 'nn5')
    
    #output = rbind(output_nn1, output_nn2, output_nn3, output_nn4, output_nn5)
    output = rbind(output_brt, output_nn1, output_nn2, output_nn3, output_nn4, output_nn5)
    
    return(output)
}

# Read the results for the past-return signals. 
read_tech_results_by_methods <- function(param, ensemble_nn = TRUE) {
    param$method = 'brt'
    output_brt = Output_portfolio_detail_tech_python(param, ensemble = FALSE) %>%
        mutate(method = 'brt')
    
    param$method = 'nn'; param$hidden_layers = c(32); 
    output_nn1 = Output_portfolio_detail_tech_python(param, ensemble = ensemble_nn) %>% 
        mutate(method = 'nn1')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16); 
    output_nn2 = Output_portfolio_detail_tech_python(param, ensemble = ensemble_nn) %>% 
        mutate(method = 'nn2')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8); 
    output_nn3 = Output_portfolio_detail_tech_python(param, ensemble = ensemble_nn) %>% 
        mutate(method = 'nn3')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4); 
    output_nn4 = Output_portfolio_detail_tech_python(param, ensemble = ensemble_nn) %>% 
        mutate(method = 'nn4')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4, 2); 
    output_nn5 = Output_portfolio_detail_tech_python(param, ensemble = ensemble_nn) %>% 
        mutate(method = 'nn5')
    
    # Compare the two outputs
    output = rbind(output_brt, output_nn1, output_nn2,  output_nn3, output_nn4, output_nn5) %>%
        mutate(cov = param$cov,
               sample = param$sample,
               begin = param$begin,
               end = param$end)
}

# Read the results for the GHZ and CZ datasets. 
read_gc_results_by_methods <- function(param) {
    param$method = 'brt'
    output_brt = Output_portfolio_detail_gc_python(param, ensemble = FALSE) %>%
        mutate(method = 'brt')
    
    param$method = 'nn'; param$hidden_layers = c(32); 
    output_nn1 = Output_portfolio_detail_gc_python(param, ensemble = TRUE) %>% 
        mutate(method = 'nn1')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16); 
    output_nn2 = Output_portfolio_detail_gc_python(param, ensemble = TRUE) %>% 
        mutate(method = 'nn2')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8); 
    output_nn3 = Output_portfolio_detail_gc_python(param, ensemble = TRUE) %>% 
        mutate(method = 'nn3')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4); 
    output_nn4 = Output_portfolio_detail_gc_python(param, ensemble = TRUE) %>% 
        mutate(method = 'nn4')
    
    param$method = 'nn'; param$hidden_layers = c(32, 16, 8, 4, 2); 
    output_nn5 = Output_portfolio_detail_gc_python(param, ensemble = TRUE) %>% 
        mutate(method = 'nn5')
    
    # Compare the two outputs
    output = rbind(output_brt, output_nn1, output_nn2,  output_nn3, output_nn4, output_nn5) %>%
        mutate(cov = param$cov,
               sample = param$sample,
               begin = param$begin,
               end = param$end)
}
