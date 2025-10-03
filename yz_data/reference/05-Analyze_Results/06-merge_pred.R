## This file merges the single predictions (in months or annually) into a single merged file. 

# For a single batch (BRT or NN with a specific ver)
merge3_pred <- function(param_in = param_baseline, brt_ver = BASELINE_VERSION, nn_ver = BASELINE_BATCH) {
    merge2_pred(brt_ver, param_in)
    
    param_nn = param_in
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32); 
    tmp = lapply(nn_ver, merge2_pred, param_nn)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16); 
    tmp = lapply(nn_ver, merge2_pred, param_nn)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16, 8); 
    tmp = lapply(nn_ver, merge2_pred, param_nn)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16, 8, 4); 
    tmp = lapply(nn_ver, merge2_pred, param_nn)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16, 8, 4, 2); 
    tmp = lapply(nn_ver, merge2_pred, param_nn)
}

merge3_pred(param_baseline, brt_ver = BASELINE_VERSION, nn_ver = BASELINE_BATCH)
merge3_pred(param_baseline_ghz94, brt_ver = BASELINE_VERSION, nn_ver = BASELINE_BATCH)
merge3_pred(param_baseline_cz207, brt_ver = BASELINE_VERSION, nn_ver = BASELINE_BATCH)
merge3_pred(param_baseline_tech119, brt_ver = BASELINE_VERSION, nn_ver = BASELINE_BATCH)
merge3_pred(param_baseline_tech120, brt_ver = BASELINE_VERSION, nn_ver = BASELINE_BATCH)

## For NN with ensembles (10 batches)
merge6_pred <- function(param_in = param_baseline, nn_ver = BASELINE_BATCH) {
    param_nn = param_in
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32); 
    merge5_pred(param_nn, nn_ver = BASELINE_BATCH)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16); 
    merge5_pred(param_nn, nn_ver = BASELINE_BATCH)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16, 8); 
    merge5_pred(param_nn, nn_ver = BASELINE_BATCH)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16, 8, 4); 
    merge5_pred(param_nn, nn_ver = BASELINE_BATCH)
    
    param_nn$method = 'nn'; param_nn$hidden_layers = c(32, 16, 8, 4, 2); 
    merge5_pred(param_nn, nn_ver = BASELINE_BATCH)
}

merge6_pred(param_baseline, nn_ver = BASELINE_BATCH)
merge6_pred(param_baseline_ghz94, nn_ver = BASELINE_BATCH)
merge6_pred(param_baseline_cz207, nn_ver = BASELINE_BATCH)
merge6_pred(param_baseline_tech120, nn_ver = BASELINE_BATCH)
merge6_pred(param_baseline_tech119, nn_ver = BASELINE_BATCH)
