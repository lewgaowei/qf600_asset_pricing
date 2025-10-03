## Folders for the output. 
TABLE_PATH = "../../Output/Table/"   # Folder for the generated tables. 
OUTPUT_PATH = "../../Output/"        # Folder for the output.

## This is used to identify the unique version for each iteration. 
# For BRT, we use one batch. 
# For NN, We run 10 batches and take their average predictions. the following is the versions used in the 10 batches.
BASELINE_VERSION = '20240221'  # Baseline version for BRT
BASELINE_BATCH = c('20240221', '20240315', '20240320', '20240325', '20240327', '20240401', '20240403', '20240406', '20240408', '20240411')

## Some baseline parameters for different datasets. 
# For the YZ dataset, with 18k signals. 
param_baseline = list(
    n_cores = 1,
    method = "brt",
    hidden_layers = c(0),
    sample = "perm",
    dep_var = "aexret",
    cov = "funda",
    cov_num = 240,
    cov_rank = "miss",
    window = "recursive",
    cv_train = 12,
    cv_validation = 12,
    port_num = 10,
    begin = 24,
    end = 55,
    version = BASELINE_VERSION
)

# Parameter for the GHZ94 dataset, i.e., the GHZ dataset with 94 signals. 
param_baseline_ghz94 = list(
    n_cores = 1,
    method = "brt",
    hidden_layers = c(0),
    sample = "ghz94_original",
    dep_var = "mexret",
    cov = "ghz94",
    cov_num = 240,
    cov_rank = "miss",
    window = "recursive",
    param = "cv",
    cv_train = 144,
    cv_validation = 144,
    port_num = 10,
    begin = 299,
    end = 688,
    version = BASELINE_VERSION
)


# Parameter for the CZ207 dataset, i.e., the CZ dataset with 207 signals. 
param_baseline_cz207 = list(
    n_cores = 1,
    method = "brt",
    hidden_layers = c(0),
    sample = "cz207_63",
    dep_var = "mexret",
    cov = "cz207",
    cov_num = 240,
    cov_rank = "miss",
    window = "recursive",
    param = "cv",
    cv_train = 144,
    cv_validation = 144,
    port_num = 10,
    begin = 732,
    end = 1121,
    version = BASELINE_VERSION
)


# Parameter for the PR119, i.e., past-return with 119 signals. 
param_baseline_tech119 = list(
    n_cores = 1,
    method = "brt",
    hidden_layers = c(0),
    sample = "tech_lryz_63",
    dep_var = "mexret",
    cov = "tech_ret",
    cov_num = 240,
    cov_rank = "miss",
    window = "recursive",
    param = "cv",
    cv_train = 144,
    cv_validation = 144,
    port_num = 10,
    begin = 739,
    end = 1128,
    version = BASELINE_VERSION
)

# Parameter for the PR120, i.e., past-return with 120 signals. 
param_baseline_tech120 = list(
    n_cores = 1,
    method = "brt",
    hidden_layers = c(0),
    sample = "tech_lryz_st_63",
    dep_var = "mexret",
    cov = "tech_ret",
    cov_num = 240,
    cov_rank = "miss",
    window = "recursive",
    param = "cv",
    cv_train = 144,
    cv_validation = 144,
    port_num = 10,
    begin = 739,
    end = 1128,
    version = BASELINE_VERSION
)

