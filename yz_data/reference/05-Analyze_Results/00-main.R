## Set the path ##
# setwd("")

## You need to change the path before running any the program ###

source('01-common.R')
source('02-table-common.R')
source('03-common_settings.R')

# Before running the steps below, you need to run ML training and predictions in Python
# The python codes generate separate files containing predicted returns for each period. Predictions are computed at the annual and monthly frequencies

# Step 1: Merge the predictions from ML in Python to a single file
# This needs to be run only once. 
source('06-merge_pred.R')

# Step 2: Analyze the predictions and tabulate the tables. 
source('11-Table-01-to-03.R')
source('12-Table-04.R')
source('13-Table-05.R')
source('14-Table-06.R')
source('15-Table-07.R')
