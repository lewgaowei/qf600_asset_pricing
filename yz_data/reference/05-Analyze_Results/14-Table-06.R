# This function conducts the recursive ranking strategy on the YZ dataset.
## Set the path ##
# setwd("")

## You need to change the path before running any the program ###


library(tidyverse)
library(data.table)
library(zoo)
library(haven)
library(vroom)

source('01-common.R')
source('03-common_settings.R')

calc_risk_adjusted_returns <- function(portfolio_data, lag = 12) {
    # Define the base path once to avoid repeated string construction
    base_path <- "../../Data/"
    
    # Load and prepare the `mom` dataset
    mom <- vroom(file = paste0(base_path, "mom.csv"), show_col_types = FALSE) |>
        select(date, mom = Mom) |>
        arrange(date)
    
    # Load and prepare the `ff5` dataset
    ff5 <- vroom(file = paste0(base_path, "ff5.csv"), show_col_types = FALSE) |>
        select(date = dateff, rf, mktrf, smb, hml, rmw, cma) |>
        arrange(date)
    
    # Load and prepare the `qfactor` dataset
    qfactor <- vroom(file = paste0(base_path, "q5_factors_monthly_2019.csv"), show_col_types = FALSE) |>
        rename_with(tolower) |>
        mutate(date = year * 100 + month) |>
        select(date, r_f, r_mkt, r_me, r_ia, r_roe, r_eg) |>
        arrange(date)
    
    # Prepare the `portfolio_data` and join with the other datasets
    portfolio_data <- portfolio_data |>
        mutate(date = as.numeric(format(yearm, "%Y%m"))) |>
        left_join(ff5, by = "date") |>
        left_join(mom, by = "date") |>
        left_join(qfactor, by = "date")
    
    # 1. Create a list of models to loop over
    model_formulas <- list(
        ex = ret ~ 1,
        capm = ret ~ mktrf,
        ff3 = ret ~ mktrf + hml + smb,
        carhart = ret ~ mktrf + hml + smb + mom,
        ff5 = ret ~ mktrf + hml + smb + rmw + cma,
        ff5m = ret ~ mktrf + hml + smb + rmw + cma + mom,
        q = ret ~ r_mkt + r_me + r_ia + r_roe
    )
    
    # 2. Define a function to calculate the model and tidy it
    calc_and_tidy_model <- function(data, formula, lag) {
        model <- calc_rar2(formula, data = data, lag = lag)
        broom::tidy(model)
    }
    
    # 3. Nest data and apply models
    quantile_regs <- portfolio_data %>%
        group_by(bin) %>%
        nest() %>%
        mutate(
            model_results = map(data, ~ map(model_formulas, calc_and_tidy_model, data = .x, lag = lag))
        )
    
    # 4. Convert the nested model results into separate columns for each model
    model_tidy <- quantile_regs %>%
        mutate(model_results = map(model_results, enframe)) %>%  # Convert model list to tibble
        unnest(model_results) %>%
        unnest(value)  # Unnest the model results (which contain tidy output)
    
    # 5. Extract the intercepts in a single step
    extract_intercepts <- function(df, model_name) {
        df %>%
            filter(name == model_name & term == '(Intercept)') %>%
            select(bin, estimate, statistic) %>%
            rename_with(~ paste0(model_name, "_", .), c("estimate", "statistic"))
    }
    
    # 6. Loop through each model and extract intercepts, then join the results by 'bin'
    results_list <- map(names(model_formulas), ~ extract_intercepts(model_tidy, .x))
    
    # 7. Join all the results together by 'bin' column
    final_results <- reduce(results_list, full_join, by = "bin")
    
    # summary stats to console
    base_results = portfolio_data %>% 
        group_by(bin) %>%
        summarize(
            rbar = mean(ret)
            , vol = sd(ret)
            , nmonth = n()
            , tstat = rbar/vol*sqrt(nmonth)
            , SR_ann = rbar/vol*sqrt(12)
            , yearm_start = min(yearm)
            , yearm_end = max(yearm)
        ) %>%
        ungroup() %>% 
        select(bin, rbar, tstat, SR_ann, vol, nmonth, yearm_start, yearm_end) %>% 
        arrange(bin)
    
    # 8. Select the output
    output = final_results %>%
        left_join(base_results, by = "bin") |> 
        select(bin, 
               rbar, tstat, vol, SR_ann,
               ex = ex_estimate, ex_t = ex_statistic, 
               capm = capm_estimate, capm_t = capm_statistic, 
               ff3 = ff3_estimate, ff3_t = ff3_statistic, 
               carhart = carhart_estimate, carhart_t = carhart_statistic, 
               ff5 = ff5_estimate, ff5_t = ff5_statistic, 
               ff5m = ff5m_estimate, ff5m_t = ff5m_statistic, 
               q = q_estimate, q_t = q_statistic,
               nmonth, yearm_start, yearm_end)
}

# Functions that conduct recursive ranking for annual frequency. 
perform_recursive_ranking_annual <- function(ret1) {
    # find t = stats each June ==============================
    # these t = stats are known before yearm_avail
    yearmlist = as.yearmon(paste0(1987:2018, '-06'))
    tstat = list()
    for (yearm in yearmlist){
        print(yearm)
        
        temp2 <- ret1 %>%
            filter(yearm <= yearm) %>% # Change to recursive one, which use past years' data for t-stat calculation. 
            group_by(signalid) %>%
            summarise(
                tstat = mean(ret) / sd(ret) * sqrt(n()),
                nmonth = n()
            ) %>%
            ungroup() %>% 
            mutate(yearm_avail = as.yearmon(yearm) + 1 / 12) # t-stat avail for next month ret
        
        tstat[[as.character(yearm)]] = temp2
    }
    
    tstat = rbindlist(tstat)

    # make portfolios
    # Sort into bins
    nbin = 10
    nmonthmin = 12 * 5 # drop signal if too little data
    
    # Filter, create bins, and apply grouping
    tstat2 <- tstat %>%
        filter(nmonth >= nmonthmin) %>%
        group_by(yearm_avail) %>%
        mutate(bin = ntile(tstat, nbin)) %>% 
        ungroup()
    
    # merge onto returns
    ret2 = merge(ret1, tstat2
                 , by.x = c('yearm', 'signalid')
                 , by.y = c('yearm_avail', 'signalid')
                 , all.x = TRUE)
    
    # arrange and fill bins with most recent
    ret3 <- ret2 %>%
        group_by(signalid) %>%
        arrange(signalid, yearm) %>%
        fill(bin, .direction = "down") %>%
        ungroup() %>% 
        filter(!is.na(bin)) %>%
        select(yearm, signalid, ret, bin, tstat, nmonth)
    
    port2 = ret3 %>% 
        group_by(yearm, bin) %>% 
        summarise(ret = mean(ret, na.rm = TRUE), 
                  .groups = 'drop') %>% 
        arrange(yearm, bin)
    
    # add long-short returns
    retLS = port2 %>% 
        filter (bin %in% c(1, nbin)) %>%
        pivot_wider(id_cols = 'yearm', 
                    names_from = 'bin', 
                    values_from = 'ret', 
                    names_prefix = 'bin') %>%
        mutate(ret = !!as.name(paste0('bin', nbin)) - !!as.name(paste0('bin', 1)))
    
    port3 = port2 %>% 
        rbind(retLS %>% transmute(yearm, bin = 'LS', ret)) %>%
        mutate(bin = str_pad(bin, 2, pad = '0')) %>% 
        arrange(yearm, bin)
    
    # summary stats to console
    output = calc_risk_adjusted_returns(port3)
}

# Read YZ's decile returns from SAS computation.
# You need to run the SAS code before reading the file below.  
decile0 <- read_sas('../../Data/01-YZ/decile0_19632019.sas7bdat')

# Preprocess the data, merge the signalname, generate date. 
ret0 = decile0 %>%
    mutate(signalid = paste0(anomalyvariable, "_", var), 
           year = year(DATE), 
           month = month(DATE), 
           yearm = as.yearmon(paste0(year, '-', month), '%Y-%m'),
           ddiff_ew = ddiff_ew * 100, 
           ddiff_vw = ddiff_vw * 100) %>% 
    filter(yearm >= 1963, yearm <= as.yearmon('2019-06')) 

# Use equal weighted returns 
ret_ew = select(ret0, signalid, yearm, year, month, ret = ddiff_ew) 
output_ew = perform_recursive_ranking_annual(ret_ew) %>% 
    mutate(weight = 'ew')

# Use value weighted returns
ret_vw = ret0 %>% select(signalid, yearm, year, month, ret = ddiff_vw) 
output_vw = perform_recursive_ranking_annual(ret_vw) %>% 
    mutate(weight = 'vw')

# Combine the two outputs and write the results to files. 
output = rbind(output_ew, output_vw)
write.csv(output,
          file = paste0(TABLE_PATH, "Table-06", "_", param_in$version, ".csv"),
          row.names = FALSE)
