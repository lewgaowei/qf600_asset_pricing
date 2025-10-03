### Main Entry ###
import sys
import os

# Change the folder
os.chdir("set your working path")
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

from analyze_data_V2 import ML_cross_validation, ML_Computation
                     
if __name__ == "__main__":
    param_in = dict({'method':          str(sys.argv[1]), 
                     'sample':          str(sys.argv[2]), 
                     'dep_var':         str(sys.argv[3]), # 'aexret', 'mexret'
                     'cov_num':         int(sys.argv[4]), # 240, 
                     'window' :         str(sys.argv[5]), # 'recursive', 'rolling'
                     'cv_train':        int(sys.argv[6]), # 12 if year; 144 if month
                     'cv_validation':   int(sys.argv[7]), # 12 if year; 144 if month
                     'port_num':        10, 
                     'begin':           int(sys.argv[8]), # 24 if year; 
                     'end':             int(sys.argv[9]), # 56 if year;
                     'cov':             str(sys.argv[10]), #'perm', 'tech_lryz', 'tech_mn'
                     #'job_name':        str(sys.argv[10]), # 'tech' or 'funda'
                     'cov_rank':        str(sys.argv[11]), # 'full', 'test', 'fixed'
                     'hidden_layers':   int(sys.argv[12]),  # the number of hidden layers for NN
                     'version':         str(sys.argv[13])   # A version file used to indicate the running version: Typically I use the dates attached on the pbs files
                     })
    
    # If "full" parameter, conduct the cross-validation first. 
    if (param_in['cov_rank'] == 'full'):
        ML_cross_validation(param_in)
        ML_Computation(param_in)
    elif (param_in['cov_rank'] == 'cv'):
        ML_cross_validation(param_in)
    elif (param_in['cov_rank'] == 'test'):
        ML_Computation(param_in)
    
