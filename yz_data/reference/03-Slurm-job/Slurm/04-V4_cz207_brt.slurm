#!/bin/bash

### Job name
### This job works for the FAML project: running BRT on CZ-207 sample. 
### This is the main file, and we also contain two sub-files for different number of paramters. 
#SBATCH --job-name=cz207+brt

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

python3 3_main_V4.py 'brt' cz207_63 mexret 240 recursive 144 144 732 1149 cz207 full 0 '20240221'
