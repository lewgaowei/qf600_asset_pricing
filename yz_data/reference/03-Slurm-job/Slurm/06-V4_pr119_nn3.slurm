#!/bin/bash

### Job name
### This job works for the FAML project: running NN3 on the PR-119 sample. 
### This is the main file, and we also contain two sub-files for different number of paramters. 
#SBATCH --job-name=pr119+brt

### Files for the outputs. .out for the common output, .err for the errors
#SBATCH --output=output/%j.out 
#SBATCH --error=output/%j.err

### Number of Nodes to be used; and the number of task per node
###SBATCH --account=libin
###SBATCH --partition=sd530
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=28
###SBATCH --nodelist=m002

### Time the job needs to execute.
#SBATCH --time=7200:00:00

# source activate venv_faml
# cd ~/project/Workshop/2-FundaML/Code/Python/
cd /home2/ems034libin/Workshop/2-FundaML/
source venv/venv_faml/bin/activate

cd /home2/ems034libin/Workshop/JFE_Data_Code/FAML/Code/02-ML_Prediction/

python3 3_main_V4.py 'nn' tech_lryz_63 mexret 240 recursive 144 144 739 1129 tech_ret full 3 '20240221'
