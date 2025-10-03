library(doParallel) 
library(dplyr)
library(tidyverse)
library(vroom)
library(rlang)
library(lightgbm)
library(useful)
library(stringi)
library(AER)
library(readr)
library(haven)
library(sqldf)
library(tidyr)
library(scales)
library(lubridate)
library(latex2exp)
library(maditr)

# This function ranks and rescales to [-1, +1]
rank_range <- function(x, na.rm = TRUE) {
  x1 <- rank(x, na.last = "keep", ties.method = ("average"))
  x2 <- rescale(x1, to = c(-1, 1), from = range(x1, na.rm = TRUE, finite = TRUE) )
  return(x2)
}

# This function calculates a month dummy (md) for indexing. 
# Month dummy is used widely throughout the project, calculated as:
#      md = year * 12 + month - 1925 * 12 - 11
# Another mindex is calculated as 
#      mindex = year * 12 + month
# These two are equivalent, but we use the different indices for different periods. 
md <- function(caldt, format = "%Y-%m-%d") {
  return(year(strptime(caldt, format = format)) *12 + month(strptime(caldt, format = format)) -1925 * 12 -11)
}

# This function converts md to year and month
dm <- function(md) {
    # This function reverts monthly dummy to actual date (YYYYMM) 
    month0 = (md+11+1925*12)%%12
    
    month = ifelse(month0 == 0, 12, month0)
    md = ifelse(month0 == 0, md-12, md)
    
    year = (md+11+1925*12)%/%12
    
    return(paste(year, str_pad(month, 2, pad = "0"), sep = '-'))
}

calc_rar2 <- function(formula, data, lag = 6) {
  model <- lm(formula = formula, data=data)
  model2 <- coeftest(model, vcov. = NeweyWest(model, lag = lag))
  
  return(model2)
}

# This function produces filename according to the parameters. 
output_file_name_new <- function(param_in, mode = 'pred', OUTPUT_Prefix = "../../") {
    # define the method parameters
    method = param_in$method        # default "brt"; "lasso", "nn"
    method_adj = ifelse(method == "nn", paste0(method, length(param_in$hidden_layers)), method)
    
    folder = case_when(
        mode == "pred" ~ "Output/Pred/",
        mode == "pred_merge" ~ "Output/Pred_merged/",
        mode == "sum"  ~ "Output/",
        mode == "imp"  ~ "Output/Imp/",
        mode == "plot" ~ "Output/Plot/",
        mode == "cv"   ~ "Output/CV/",
        TRUE           ~ "Output/Bin_Output/")
    
    file_name = paste0(OUTPUT_Prefix,
                       folder,
                       method_adj, 
                       "_", param_in$sample,
                       "_", param_in$window, 
                       "_dep_", param_in$dep_var,
                       "_cov_", param_in$cov, 
                       "_num_", param_in$cov_num,
                       "_t_", param_in$cv_train, 
                       "_v_", param_in$cv_validation, 
                       "_p", param_in$port_num,
                       "_ver_", param_in$version) 
    
    return(file_name)
}

# This function reads annual/monthly predictions (multiple files) and merge to a single output. 
read_results_new <- function(param_in) {
    registerDoParallel(cores = param_in$n_cores)
    
    results = foreach (k = (param_in$begin):(param_in$end), .combine=rbind) %dopar% {  
        temp = read.csv(file = paste0(output_file_name_new(param_in, mode = 'pred'), "_counter_", (k+1), "_pred_python", ".csv"))
        return(temp)
    }
    
    return(results)
}

# This function reads ensemble results, because we need to merge 10 batches for NN predictions. 
read_results_ensemble <- function(param_in) {
  param_tmp = param_in
  param_tmp$version = 'ensemble10'
  
  results = read.csv(file = paste0(output_file_name_new(param_tmp, mode = 'pred'), "_pred_python", ".csv")) 
  
  return(results)
}

# This function reads merged predictions in a single file. 
read_results_final <- function(param_in, ensemble = FALSE) {
  param_tmp = param_in
  if (ensemble == TRUE) {
    param_tmp$version = 'ensemble10'
  }
  
  results = read.csv(file = paste0(output_file_name_new(param_tmp, mode = 'pred_merge'), "_pred_python", ".csv")) %>% 
    filter(counter >= (param_in$begin+1), counter <= (param_in$end+1))
  
  return(results)
}

# The following functions are used to merge ML predictions from Python. This works for BRT. 
merge_results <- function(param_in) {
  registerDoParallel(cores = 10)

  results = foreach (k = (param_in$begin):(param_in$end), .combine=rbind) %dopar% {  
    temp = read.csv(file = paste0(output_file_name_new(param_in, mode = 'pred'), "_counter_", (k+1), "_pred_python", ".csv"))
    return(temp)
  }
  
  write.csv(results, file = paste0(output_file_name_new(param_in, mode = 'pred_merge'), "_pred_python", ".csv"), row.names = FALSE)
}

# This function merges prediction for a specified version. 
# The "version" parameter is used to distinguish different batches. 
merge2_pred <- function(version, param_in) {
  param_tmp = param_in
  param_tmp$version = version  
  merge_results(param_tmp)
}

# Functions merge4 to merge5 merge 10 NNs' predictions, and calculate the average of the predictions. 
merge4_pred <- function(version, param_in) {
  param_tmp = param_in
  param_tmp$version = version  
  
  # merge_results(param_tmp)
  results = read.csv(file = paste0(output_file_name_new(param_tmp, mode = 'pred_merge'), "_pred_python", ".csv")) %>% 
    select(permno, counter,  !!paste0('pred_return_1', '_', version) := pred_return_1)

  return(results)
}

merge5_pred <- function(param_in, nn_ver = BASELINE_BATCH) {
  param_nn = param_in
  param_nn$version = nn_ver[1]
  
  results = read.csv(file = paste0(output_file_name_new(param_nn, mode = 'pred_merge'), "_pred_python", ".csv")) %>% 
    rename(!!paste0('pred_return_1', '_', nn_ver[1]) := pred_return_1)
  
  data_list <- lapply(nn_ver[-1], merge4_pred, param_nn)
  final_data <- reduce(data_list, left_join, by = c("counter", "permno"))
  
  final_results = left_join(results, final_data, by = c("permno", "counter")) %>% 
    mutate(pred_return_1 = rowMeans(select(., !!paste0('pred_return_1', '_', nn_ver[1]),
                                           !!paste0('pred_return_1', '_', nn_ver[2]),
                                           !!paste0('pred_return_1', '_', nn_ver[3]),
                                           !!paste0('pred_return_1', '_', nn_ver[4]),
                                           !!paste0('pred_return_1', '_', nn_ver[5]),
                                           !!paste0('pred_return_1', '_', nn_ver[6]),
                                           !!paste0('pred_return_1', '_', nn_ver[7]),
                                           !!paste0('pred_return_1', '_', nn_ver[8]),
                                           !!paste0('pred_return_1', '_', nn_ver[9]),
                                           !!paste0('pred_return_1', '_', nn_ver[10]))))
  
  param_nn$version = 'ensemble10'
  write.csv(final_results, file = paste0(output_file_name_new(param_nn, mode = 'pred_merge'), "_pred_python", ".csv"), row.names = FALSE)
}

# This function conducts portfolio analysis using annual rebalance + buy and hold during the months of the fiscal year.
Output_portfolio_detail_python <- function(param_in) {
    # Extract the decile portfolios' details
    # Extract input parameters
    registerDoParallel(cores = param_in$n_cores)
    
    # define the method parameters
    results = read_results_new(param_in)
    
    # We expand the annual prediction with monthly returns. 
    results <- results %>%
        mutate(form_md_1 = form_md+1, 
               form_md_12 = form_md+12)
    msf <-  vroom("../../Data/msf.csv", show_col_types = FALSE)
    
    results2 <- sqldf("select distinct a.*, 
      b.md, b.mexret as ret, b.size_1 as size
      from results as a, msf as b
      where a.permno = b.permno 
      and md between form_md_1 and form_md_12;") %>%
        arrange(md, permno)
    
    # Performn portfolio sort based on 'pred_return_1' (sorting variable). 
    output <- single_sorts_funda(results2, "pred_return_1", (param_in$port_num))
    
    write.csv(output,
              file = paste(output_file_name_new(param_in, mode = "sum"), "_decile_detail_", (param_in$port_num), ".csv", sep = ""),
              row.names = FALSE)
    
    return(output)
}

single_sorts_one <- function(data, var, size, port_num = 10, is_tech = FALSE, test = 'ew') {
    lag = 12
    
    # Select the data and split the quantile for each month
    data <- data %>%
        select(permno, md, all_of(var), ret, all_of(size))  %>%
        drop_na %>%
        arrange(md, .data[[all_of(var)]]) %>%
        group_by(md) %>%
        mutate(rank = ntile(.data[[all_of(var)]], port_num))
    
    # Obtain the decile portfolio's returns
    portfolio_data <- data %>%
        group_by(md, rank) %>%
        summarize(
            pred = weighted.mean(!!as.name(var), !!as.name(size), na.rm = TRUE) * 100,
            ret = weighted.mean(ret, !!as.name(size), na.rm = TRUE) * 100, 
            .groups = 'drop') %>%
        select(md, rank, pred, ret)
    
    # Obtain the long short portfolio returns. 
    ls_portfolio <- portfolio_data %>%
        group_by(md) %>%
        summarize(
            pred = pred[which(rank == port_num)] - pred[which(rank == 1)],
            ret = ret[which(rank == port_num)] - ret[which(rank == 1)],
            .groups='drop') %>%
        add_column(rank = (port_num+1)) %>%
        select(md, rank, pred, ret)
    
    portfolio_data = bind_rows(portfolio_data, ls_portfolio) %>%
        arrange(md, rank)

    col_pred_avg = portfolio_data %>%
        group_by(rank) %>%
        summarize(
            pred = mean(pred, na.rm = TRUE)/12,
            std = sd(ret),
            ret = mean(ret, na.rm = TRUE),
            sr = ret/std*sqrt(12),
            .groups = 'drop')
    
    mom <- vroom(file = paste0("../../Data/", "mom", ".csv"), show_col_types = FALSE) %>%
        mutate(md = md(paste0(date, "15"), format = "%Y%m%d"),
               mom = Mom/100) %>%
        select(md, mom) %>%
        arrange(md)
    
    if (is_tech == TRUE) {
        ff3 <- vroom(file = paste0("../../Data/", "ff3", ".csv"), show_col_types = FALSE) %>%
            mutate(md = md(paste0(dateff, "15"), format = "%Y%m%d"),
                   rf = rf/100, mktrf = mktrf/100,
                   smb = smb/100, hml = hml/100) %>%
            select(md, rf, mktrf, smb, hml) %>%
            arrange(md)
        
        portfolio_data <- portfolio_data %>% 
            left_join(ff3, by = "md") %>%  
            left_join(mom, by = "md")
    } else if (is_tech == FALSE) {
        ff5 <- vroom(file = paste0("../../Data/", "ff5", ".csv"), show_col_types = FALSE) %>%
            mutate(md = md(paste0(dateff, "15"), format = "%Y%m%d"),
                   rf = rf/100, mktrf = mktrf/100,
                   smb = smb/100, hml = hml/100,
                   rmw = rmw/100, cma = cma/100) %>%
            select(md, rf, mktrf, smb, hml, rmw, cma) %>%
            arrange(md)
        
        qfactor <- vroom(file = paste0("../../Data/", "q5_factors_monthly_2019", ".csv"), show_col_types = FALSE) 
        colnames(qfactor) = tolower(colnames(qfactor))
        qfactor = qfactor %>%
            mutate(md = md(make_date(year, month, 15), format = "%Y-%m-%d"),
                   r_f = r_f/100, r_mkt = r_mkt/100,
                   r_me = r_me/100, r_ia = r_ia/100,
                   r_roe = r_roe/100, r_eg = r_eg/100) %>%
            select(md, r_f, r_mkt, r_me, r_ia, r_roe, r_eg) %>%
            arrange(md)
        
        portfolio_data <- portfolio_data %>% 
            left_join(ff5, by = "md") %>%   
            left_join(mom, by = "md") %>%  
            left_join(qfactor, by = "md")   
    }
    
    data_nested <- portfolio_data %>%
        group_by(rank) %>%
        nest()
    
    quantile_regs <- data_nested %>%
        mutate(model_ex = map(data, ~calc_rar2(formula = ret ~ 1, data = .x, lag = lag)),
               model_capm = map(data, ~calc_rar2(ret ~ mktrf, data = .x, lag = lag)),   # capm
               model_ff3 = map(data, ~calc_rar2(ret ~ mktrf + hml + smb, data = .x, lag = lag)),    # ff3
               model_carhart = map(data, ~calc_rar2(ret ~ mktrf + hml + smb + mom, data = .x, lag = lag))) # carhart
    
    if (is_tech == FALSE) {
        quantile_regs <- quantile_regs %>%
            mutate(           # ff5
                model_ff5 = map(data, ~calc_rar2(ret ~ mktrf + hml + smb + rmw + cma, data = .x, lag = lag)),
                # ff5+ mom
                model_ff5m = map(data, ~calc_rar2(ret ~ mktrf + hml + smb + rmw + cma + mom, data = .x, lag = lag)),
                # q: mkt is the market excess return, thus I do not subtract with rf
                model_q = map(data, ~calc_rar2(ret ~ r_mkt + r_me + r_ia + r_roe, data = .x, lag = lag)))
    }
    
    if (is_tech == TRUE) {
        model_tidy = quantile_regs %>%
            mutate(ex = map(model_ex, broom::tidy),
                   capm = map(model_capm, broom::tidy),
                   ff3 = map(model_ff3, broom::tidy),
                   carhart = map(model_carhart, broom::tidy))
    } else {
        model_tidy = quantile_regs %>%
            mutate(ex = map(model_ex, broom::tidy),
                   capm = map(model_capm, broom::tidy),
                   ff3 = map(model_ff3, broom::tidy),
                   carhart = map(model_carhart, broom::tidy),
                   ff5 = map(model_ff5, broom::tidy),
                   ff5m = map(model_ff5m, broom::tidy),
                   q = map(model_q, broom::tidy))
    }
    
    ex_ew_vw = model_tidy %>%
        select(rank, ex) %>%
        unnest(c(ex), names_sep = "_") %>%
        filter(ex_term == '(Intercept)') %>%
        select(rank, ex = ex_estimate, ex_t = ex_statistic)
    
    capm_ew_vw = model_tidy %>%
        unnest(c(capm), names_sep = "_") %>%
        filter(capm_term == '(Intercept)') %>%
        select(rank, 
               capm = capm_estimate, capm_t = capm_statistic)
    
    ff3_ew_vw = model_tidy %>%
        unnest(c(ff3), names_sep = "_") %>%
        filter(ff3_term == '(Intercept)') %>%
        select(rank, 
               ff3 = ff3_estimate, ff3_t = ff3_statistic)
    
    carhart_ew_vw = model_tidy %>%
        unnest(c(carhart), names_sep = "_") %>%
        filter(carhart_term == '(Intercept)') %>%
        select(rank, 
               carhart = carhart_estimate, carhart_t = carhart_statistic)
    
    if (is_tech == FALSE) {
        ff5_ew_vw = model_tidy %>%
            unnest(c(ff5), names_sep = "_") %>%
            filter(ff5_term == '(Intercept)') %>%
            select(rank, 
                   ff5 = ff5_estimate, ff5_t = ff5_statistic)
        
        ff5m_ew_vw = model_tidy %>%
            unnest(c(ff5m), names_sep = "_") %>%
            filter(ff5m_term == '(Intercept)') %>%
            select(rank, 
                   ff5m = ff5m_estimate, ff5m_t = ff5m_statistic)
        
        q_ew_vw = model_tidy %>%
            unnest(c(q), names_sep = "_") %>%
            filter(q_term == '(Intercept)') %>%
            select(rank, 
                   q = q_estimate, q_t = q_statistic)
    }
    
    output = col_pred_avg %>%
        left_join(ex_ew_vw, by = "rank") %>%
        left_join(capm_ew_vw, by = "rank") %>%
        left_join(ff3_ew_vw, by = "rank") %>%
        left_join(carhart_ew_vw, by = "rank")
    
    output = output %>%
        select(rank, 
               pred, ex, ex_t, std, sr, capm, capm_t, ff3, ff3_t, carhart, carhart_t)
    
    if (is_tech == FALSE) {
        output = output %>%
            left_join(ff5_ew_vw, by = "rank") %>%
            left_join(ff5m_ew_vw, by = "rank") %>%
            left_join(q_ew_vw, by = "rank")
    }
    
    return(output)
}

single_sorts <- function(results2, sort_var = 'pred_return_1', port_num = 10) {
    results2 <- results2 %>%
        mutate(weight_ew = 1) #  if equally weighted portfolio
    
    # Equal weighted
    output_ew <- single_sorts_one(results2, sort_var, 'weight_ew', port_num = port_num, test = 'ew') %>% 
        mutate(weight = 'ew')
    
    # Value Weighted
    output_vw <- single_sorts_one(results2, sort_var, 'size', port_num = port_num, test = 'vw')  %>% 
        mutate(weight = 'vw')
    
    # Combine three panels of results
    output = bind_rows(output_ew, output_vw)
    
    return(output)
}

simulate_trading3 <- function(results, param_in, weight_dummy = 'ew') {

  # Read the return data and match with the results
  msf <-  vroom("../../Data/msf.csv", delim = ',', show_col_types = FALSE)
  
  # Obtain the size column for the form_md
  results_prediction <- left_join(results, msf, by = c("permno", "form_md" = "md")) %>%
    select(counter, permno, form_md, pred_return_1, form_size = size) %>%
    group_by(form_md) %>%
    mutate(rank = ntile(pred_return_1, 10)) %>% 
    ungroup()
  
  results_prediction_01 = results_prediction %>% 
    mutate(md = form_md + 1)
  results_prediction_02 = results_prediction %>% 
    mutate(md = form_md + 2)
  results_prediction_03 = results_prediction %>% 
    mutate(md = form_md + 3)
  results_prediction_04 = results_prediction %>% 
    mutate(md = form_md + 4)
  results_prediction_05 = results_prediction %>% 
    mutate(md = form_md + 5)
  results_prediction_06 = results_prediction %>% 
    mutate(md = form_md + 6)
  results_prediction_07 = results_prediction %>% 
    mutate(md = form_md + 7)
  results_prediction_08 = results_prediction %>% 
    mutate(md = form_md + 8)
  results_prediction_09 = results_prediction %>% 
    mutate(md = form_md + 9)
  results_prediction_10 = results_prediction %>% 
    mutate(md = form_md + 10)
  results_prediction_11 = results_prediction %>% 
    mutate(md = form_md + 11)
  results_prediction_12 = results_prediction %>% 
    mutate(md = form_md + 12)
  
  results_prediction_final = bind_rows(results_prediction_01, 
                                       results_prediction_02, 
                                       results_prediction_03, 
                                       results_prediction_04, 
                                       results_prediction_05, 
                                       results_prediction_06, 
                                       results_prediction_07, 
                                       results_prediction_08, 
                                       results_prediction_09, 
                                       results_prediction_10, 
                                       results_prediction_11, 
                                       results_prediction_12)
  
  results2 = left_join(results_prediction_final, msf, by = c("permno", "md")) %>% 
    #mutate(ret_for_weight = replace_na(ret, 0)) %>% 
    select(md, rank, permno, ret, pred_return_1, form_size, form_md) %>% 
    arrange(rank, md)
  
  final_wt = tibble(
    rank = integer(),
    md = numeric(),
    permno = numeric(),
    weight = numeric())
  
  for (r in 1:10) {
    for (form_md_i in unique(results$form_md)){
      # The first month after prediction, i.e., July
      port = filter(results2, rank == r, md == form_md_i+1) %>% 
        select(rank, md, permno, form_size, ret) %>% 
        drop_na(form_size)
      
      if (weight_dummy == 'ew') {
        port = mutate(port, weight = 1/n()) 
      } else if (weight_dummy == 'vw') {
        port = port %>% 
          mutate(weight = form_size/sum(form_size, na.rm = TRUE)) 
      }
      
      port1 = select(port, rank, md, permno, weight)
      final_wt = rbind(final_wt, port1)
      
      for (md_i in 2:12) { 
        # Calculate portfolio for month md_i
        port = port %>% 
          mutate(temp = (1+ret)*weight,
                 weight_n = temp/sum(temp, na.rm = TRUE),
                 md = form_md_i + md_i) %>% 
          select(rank, md, permno, weight = weight_n)
        
        final_wt = rbind(final_wt, port)
        
        # ret for the next month
        tmp = filter(results2, rank == r, form_md == form_md_i, md == (form_md_i+md_i)) %>% 
          select(rank, md, permno, ret)
        
        port = port %>% 
          left_join(tmp, by = c("rank", "md", "permno"))
      }
    }
  }
  
  results3 = results2 %>% 
    left_join(final_wt, by = c("rank", "md", "permno")) %>% 
    select(md, rank, permno, weight, pred_return_1, ret)
  
  # Fetch the risk-free rates and calculate the excess return
  rf <- vroom(file = paste0("../../Data/", "ff5", ".csv"), show_col_types = FALSE) %>%
    mutate(md = md(paste0(dateff, "15"), format = "%Y%m%d"),
           rf = rf/100) %>%
    select(md, rf) %>%
    arrange(md)
  
  results4 = results3 %>% 
    left_join(rf, by = c("md")) %>% 
    mutate(excess_ret = ret - rf) %>% 
    drop_na(weight) %>% 
    select(md, rank, permno, weight, pred = pred_return_1, ret = excess_ret) %>% 
    group_by(rank, md) %>% 
    summarise(pred = weighted.mean(pred, weight, na.rm = TRUE),
              ret = weighted.mean(ret, weight, na.rm = TRUE),
              .groups = 'drop')
  
  # Obtain the long short prediction
  ls_portfolio <- results4 %>%
    group_by(md) %>%
    summarize(
      pred = pred[which(rank == 10)] - pred[which(rank == 1)],
      ret = ret[which(rank == 10)] - ret[which(rank == 1)],
      .groups = 'drop') %>%
    add_column(rank = (11)) %>% 
    select(md, rank, pred, ret)
  
  # Obtain the final results for the returns. 
  portfolio_data = bind_rows(results4, ls_portfolio) %>%
    arrange(md, rank) %>% 
    select(md, rank, pred, ret)
  
  write.csv(final_wt,
            file = paste0(output_file_name_new(param_in, mode = "pred"), "_decile_detail_monthly_weight_", weight_dummy, "_", param_in$note, ".csv"),
            row.names = FALSE)
  
  write.csv(portfolio_data,
            file = paste0(output_file_name_new(param_in, mode = "pred"), "_decile_detail_monthly_return_", weight_dummy, "_", param_in$note, ".csv"),
            row.names = FALSE)
  
  return(portfolio_data)
}

calcuate_rar <- function(portfolio_data, lag = 12) {
  # The structure of portfolio_data
  #   1. md: month dummy
  #   2. rank: decile or other portfolios
  #   3. ret: excess return = raw return - risk-free rates
  #   4. pred: predicted variables
  
  # Fetch the risk factor data
  ff5 <- vroom(file = paste0("../../Data/", "ff5", ".csv"), show_col_types = FALSE) %>%
    mutate(md = md(paste0(dateff, "15"), format = "%Y%m%d"),
           rf = rf/100, mktrf = mktrf/100,
           smb = smb/100, hml = hml/100,
           rmw = rmw/100, cma = cma/100) %>%
    select(md, rf, mktrf, smb, hml, rmw, cma) %>%
    arrange(md)
  
  mom <- vroom(file = paste0("../../Data/", "mom", ".csv"), show_col_types = FALSE) %>%
    mutate(md = md(paste0(date, "15"), format = "%Y%m%d"),
           mom = Mom/100) %>%
    select(md, mom) %>%
    arrange(md)
  
  qfactor <- vroom(file = paste0("../../Data/", "q5_factors_monthly_2019", ".csv"), show_col_types = FALSE) 
  colnames(qfactor) = tolower(colnames(qfactor))
  qfactor = qfactor %>%
    mutate(md = md(make_date(year, month, 15), format = "%Y-%m-%d"),
           r_f = r_f/100, r_mkt = r_mkt/100,
           r_me = r_me/100, r_ia = r_ia/100,
           r_roe = r_roe/100, r_eg = r_eg/100) %>%
    select(md, r_f, r_mkt, r_me, r_ia, r_roe, r_eg) %>%
    arrange(md)
  
  # Merge the risk-factor data with the prediction results. 
  portfolio_data <- portfolio_data %>% 
    left_join(ff5, by = "md") %>%   
    left_join(mom, by = "md") %>%   
    left_join(qfactor, by = "md")  
  
  portfolio_data = portfolio_data %>%
    mutate(ret = ret * 100)
  
  # Calculate the first set of results
  col_pred_avg = portfolio_data %>%
    group_by(rank) %>%
    summarise(
      pred = mean(pred, na.rm = TRUE)/12*100,    # Monthly prediction for annual prediction. This is not necessary for past-return signals. 
      std = sd(ret, na.rm = TRUE),
      ret = mean(ret, na.rm = TRUE),
      sr = ret/std*sqrt(12),  # Annualized Sharpe ratios
      .groups = 'drop') %>% 
    ungroup()
  
  data_nested <- portfolio_data %>%
    group_by(rank) %>%
    nest()
  
  quantile_regs <- data_nested %>%
    mutate(model_ex = map(data, ~calc_rar2(formula = ret ~ 1, data = .x, lag = lag)),
           # capm
           model_capm = map(data, ~calc_rar2(ret ~ mktrf, data = .x, lag = lag)),
           # ff3
           model_ff3 = map(data, ~calc_rar2(ret ~ mktrf + hml + smb, data = .x, lag = lag)),
           # carhart
           model_carhart = map(data, ~calc_rar2(ret ~ mktrf + hml + smb + mom, data = .x, lag = lag)),
           # ff5
           model_ff5 = map(data, ~calc_rar2(ret ~ mktrf + hml + smb + rmw + cma, data = .x, lag = lag)),
           # ff5+ mom
           model_ff5m = map(data, ~calc_rar2(ret ~ mktrf + hml + smb + rmw + cma + mom, data = .x, lag = lag)),
           # q: mkt is the market excess return, thus we do not subtract with rf
           model_q = map(data, ~calc_rar2(ret ~ r_mkt + r_me + r_ia + r_roe, data = .x, lag = lag)))
  
  model_tidy = quantile_regs %>%
    mutate(ex = map(model_ex, broom::tidy),
           capm = map(model_capm, broom::tidy),
           ff3 = map(model_ff3, broom::tidy),
           carhart = map(model_carhart, broom::tidy),
           ff5 = map(model_ff5, broom::tidy),
           ff5m = map(model_ff5m, broom::tidy),
           q = map(model_q, broom::tidy))
  
  ex_ew_vw = model_tidy %>%
    select(rank, ex) %>%
    unnest(c(ex), names_sep = "_") %>%
    filter(ex_term == '(Intercept)') %>%
    select(rank, ex = ex_estimate, ex_t = ex_statistic)
  
  capm_ew_vw = model_tidy %>%
    unnest(c(capm), names_sep = "_") %>%
    filter(capm_term == '(Intercept)') %>%
    select(rank, 
           capm = capm_estimate, capm_t = capm_statistic)
  
  ff3_ew_vw = model_tidy %>%
    unnest(c(ff3), names_sep = "_") %>%
    filter(ff3_term == '(Intercept)') %>%
    select(rank, 
           ff3 = ff3_estimate, ff3_t = ff3_statistic)
  
  carhart_ew_vw = model_tidy %>%
    unnest(c(carhart), names_sep = "_") %>%
    filter(carhart_term == '(Intercept)') %>%
    select(rank, 
           carhart = carhart_estimate, carhart_t = carhart_statistic)
  
  ff5_ew_vw = model_tidy %>%
    unnest(c(ff5), names_sep = "_") %>%
    filter(ff5_term == '(Intercept)') %>%
    select(rank, 
           ff5 = ff5_estimate, ff5_t = ff5_statistic)
  
  ff5m_ew_vw = model_tidy %>%
    unnest(c(ff5m), names_sep = "_") %>%
    filter(ff5m_term == '(Intercept)') %>%
    select(rank, 
           ff5m = ff5m_estimate, ff5m_t = ff5m_statistic)
  
  q_ew_vw = model_tidy %>%
    unnest(c(q), names_sep = "_") %>%
    filter(q_term == '(Intercept)') %>%
    select(rank, 
           q = q_estimate, q_t = q_statistic)
  
  output = col_pred_avg %>%
    left_join(ex_ew_vw, by = "rank") %>%
    left_join(capm_ew_vw, by = "rank") %>%
    left_join(ff3_ew_vw, by = "rank") %>%
    left_join(carhart_ew_vw, by = "rank") %>%
    left_join(ff5_ew_vw, by = "rank") %>%
    left_join(ff5m_ew_vw, by = "rank") %>%
    left_join(q_ew_vw, by = "rank") %>% 
    select(rank, pred, ret, std, sr, 
           ex, ex_t, capm, capm_t, 
           ff3, ff3_t, carhart, carhart_t,
           ff5, ff5_t, ff5m, ff5m_t, 
           q, q_t)
  
  return(output)
}

# The following three functions are used to output portfolio details for different datasets. 
# We write three different functions because these datasets have slightly different structure, as we create these datasets in different periods and from different sources. 
# 
# Output_portfolio_detail_annual_rebalance() is used for the fundamental signals. 
# Output_portfolio_detail_tech_python() is used for the past-return signals. 
# Output_portfolio_detail_gc_python() is used for GHZ and CZ signals.
Output_portfolio_detail_annual_rebalance <- function(param_in, ensemble = FALSE) {
  results = read_results_final(param_in, ensemble)
  
  ## This function outputs the decile portfolio details for fundamental signals (Rebalance annually + buy and hold during the months)
  portfolio_data_ew = simulate_trading3(results, param_in, weight_dummy = 'ew') 
  portfolio_data_vw = simulate_trading3(results, param_in, weight_dummy = 'vw') 
  
  output_ew = calcuate_rar(portfolio_data_ew) %>% mutate(weight = 'ew')
  output_vw = calcuate_rar(portfolio_data_vw) %>% mutate(weight = 'vw')
    
  output = rbind(output_ew, output_vw) %>% 
    mutate(md_min = min(portfolio_data_ew$md),
           md_max = max(portfolio_data_ew$md),)
  
  write.csv(output,
            file = paste0(output_file_name_new(param_in, mode = "sum"), "_decile_detail_annual", ".csv"),
            row.names = FALSE)
  
  return(output)
}

Output_portfolio_detail_tech_python <- function(param_in, ensemble = FALSE) {
  # Output portfolio detail for past-return datasets (PR in short)
  results = read_results_final(param_in, ensemble)
  
  msf <-  vroom("../../Data/msf_tech.csv", show_col_types = FALSE) %>% 
    mutate(md = md(date, format = "%Y%m%d"))
  results2 <- sqldf("select distinct a.counter, a.permno, 
      a.mindex, b.date, a.pred_return_1, 
      a.mexret as ret, b.lagmktcap as size, b.md
      from results as a, msf as b
      where a.permno = b.permno 
      and a.mindex = b.mindex;") %>%
    arrange(mindex, permno)
  
  output <- single_sorts(results2, "pred_return_1", (param_in$port_num)) %>% 
    mutate(counter_min = min(results2$counter), 
           counter_max = max(results2$counter))
  
  write.csv(output,
            file = paste0(output_file_name_new(param_in, mode = "sum"), "_decile_detail_", (param_in$port_num), ".csv"),
            row.names = FALSE)
  return(output)
}

Output_portfolio_detail_gc_python <- function(param_in, ensemble = FALSE) {
  # Output portfolio detail for GHZ and CZ datasets (GC in short)
  results = read_results_final(param_in, ensemble)
  
  msf <-  vroom("../../Data/msf_gc_lag.csv", show_col_types = FALSE) 
  results2 <- sqldf("select distinct a.counter, a.permno, 
      a.md, a.pred_return_1, a.mexret, 
      b.mexret as ret, b.me_lag1m as size
      from results as a, msf as b
      where a.permno = b.permno 
      and a.md = b.md;") %>%
    arrange(md, permno)
  
  output <- single_sorts(results2, "pred_return_1", (param_in$port_num)) %>% 
    mutate(counter_min = min(results2$counter), 
           counter_max = max(results2$counter))
  
  write.csv(output,
            file = paste0(output_file_name_new(param_in, mode = "sum"), 
                          "_decile_detail_", (param_in$port_num), 
                          "_counter_", (param_in$begin), "_", (param_in$end), 
                          ".csv"),
            row.names = FALSE)
  return(output)
}
