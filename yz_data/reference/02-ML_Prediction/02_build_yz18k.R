##################################################################################################
# This code processes the YZ data
##################################################################################################
# set the path
setwd("")

source('01-common.R')

span_data <- function(file_input, file_output) {
  # Read the SAS code and span the data to cross sectional data
  fssignals <- read_sas(paste("../../Data/01-YZ/", file_input,".sas7bdat", sep=""))

  # Store the return data.
  data_ret <- fssignals %>%
    select(permno, form_date, crsp_mktcap_6, ret, nmonth, rf) %>%           
    mutate(form_md = md(form_date), exret = ret - rf) %>%                    # Generate month dummy and excess return. 
    distinct(permno, form_md, .keep_all = TRUE) %>%                          # Delete the duplicated observations using permno and form_md.
    select(permno, form_md, form_date, crsp_mktcap_6, exret, nmonth, ret, rf) %>%    # Select and sort variables
    arrange(permno, form_md)
  
  ### Extracting raw variables, merge with the annual return, and store in "fs_original.csv"
  data_fs <- fssignals %>%
    mutate(form_md = md(form_date)) %>%                                      # Generate month dummy
    select(permno, form_md, fsvariable, var) %>%                             # 
    distinct(permno, form_md, fsvariable, .keep_all = TRUE) %>%              # remove the duplicated observations
    spread(key = fsvariable, value = var) %>%                                # spread short table to long table.
    inner_join(data_ret, by=c("permno", "form_md"))                          # merge with the annual return
  
  rm(list = c('data_fs')) # Save space, otherwise not enough memory

  var_fssignals <- colnames(fssignals)[2:77]
  data_fs3 <- fssignals %>%
    mutate(form_md = md(form_date)) %>%
    select(permno, form_md, all_of(var_fssignals), fsvariable, var) %>%
    distinct(permno, form_md, fsvariable, .keep_all = TRUE)  %>% # Some variables are not unique, I delete them.
    arrange(permno, form_md, fsvariable)
  
  rm(list = c('fssignals')) # Save space, otherwise not enough memory
  
  data_fs4 <- dcast(setDT(data_fs3), permno+form_md ~ fsvariable, value.var = var_fssignals)
  
  data_fs4 <- inner_join(data_fs4, data_ret, by=c("permno", "form_md")) 
  
  write.csv(data_fs4, file = paste("../../Data/01-YZ/", file_output, ".csv",sep=""), row.names = FALSE)
}

impute_data <- function(file_input, file_output) {
  # 1. Rank the cross section; 
  # 2. range to [-1, +1]; 
  # 3. fill the remaining missing values with 0. 
  registerDoParallel(cores = detectCores() - 1)
  
  data = vroom(file = paste0("../../Data/01-YZ/", file_input, ".csv")) %>%
    rename(aexret = exret) %>%
    mutate(counter = (form_md - min(form_md))/12 + 1) %>%
    select(-form_date) %>%
    mutate_all(as.double)
  
  # We split these data sets according to their columns, so as to make the computation feasible.
  # When we start the project in 2020, the dataset was too large to be processed in a standard desktop. 
  data1 = select(data, -(3:18242))
  print("data1 split finished!")
  
  data2 = select(data, counter, permno, (3:6000))
  print("data2 split finished!")
  
  data3 = select(data, counter, permno, (6001:12000))
  print("data3 split finished!")
  
  data4 = select(data, counter, permno, (12001:18242))
  print("data4 split finished!")
  
  rm(data)
  gc()
  print("data split finished!")
  
  # We pre-process the data.
  data2 = data2 %>%
    group_by(counter) %>%
    mutate_at(vars(-c("counter", "permno")), rank_range) %>%
    mutate_at(vars(-c("counter", "permno")), ~replace_na(., 0)) %>%
    ungroup() %>%
    select(-c("counter", "permno"))
  print("data2 finished!")
  
  data3 = data3 %>%
    group_by(counter) %>%
    mutate_at(vars(-c("counter", "permno")), rank_range) %>%
    mutate_at(vars(-c("counter", "permno")), ~replace_na(., 0)) %>%
    ungroup() %>%
    select(-c("counter", "permno"))
  print("data3 finished!")
  
  data4 = data4 %>%
    group_by(counter) %>%
    mutate_at(vars(-c("counter", "permno")), rank_range) %>%
    mutate_at(vars(-c("counter", "permno")), ~replace_na(., 0))%>%
    ungroup() %>%
    select(-c("counter", "permno"))
  print("data4 finished!")
  
  # and merge the processed datasets into results
  results <- bind_cols(data1, data2, data3, data4)

  print("Process finished. Start writing files.")
  vroom_write(results, 
              path = paste0("../../Data/01-YZ/", file_output, ".csv"), 
              delim = ",")
  print("Writing finished.")
}

remove_redundant <- function(file_input, file_output) {
  data_folder <- "../../Data/01-YZ/"
  data_ext <- ".csv"
  
  # The common covariates for the YZ dataset.
  regs_common <- c("permno", "form_md", "crsp_mktcap_6", "aexret", "nmonth", "ret", "rf", "counter")
  
  # The filtered list of signals. 
  covariates = read.csv(file = paste0("../../Data/01-YZ/", "signallist_18113", ".csv"))$signals
  
  data <- vroom(file = paste0(data_folder, file_input, data_ext)) %>%
    select(all_of(regs_common), all_of(covariates))
  
  vroom_write(data, 
              path = paste0("../../Data/01-YZ/", file_output, ".csv"), 
              delim = ",")
}

# The temp files can be removed. Span the YZ signals from SAS codes.
span_data(file_input = "fssignals", file_output = "final_perm_tmp1")

# Pre-process the data
impute_data(file_input = "final_perm_tmp1", file_output = "final_perm_tmp2")

# Remove the redundant signals, from 18240 to 18113
remove_redundant(file_input = "final_perm_tmp2", file_output = "final_perm") 

# Remove temporaray files and clean the memory. 
file.remove(paste0("../../Data/01-YZ/", c("final_perm_tmp1", "final_perm_tmp2"), ".csv"))

gc()
