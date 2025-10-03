#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd

from sklearn.ensemble import GradientBoostingRegressor
from sklearn.ensemble import RandomForestRegressor

from sklearn.linear_model import LinearRegression
from sklearn.linear_model import ElasticNet
from sklearn.metrics import r2_score
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import ParameterGrid

import lightgbm as lgb
import pickle

from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, BatchNormalization
from tensorflow.keras.optimizers import SGD, Adam
from tensorflow.keras.regularizers import l1
from tensorflow.keras.callbacks import EarlyStopping

############### Variable definitions ###############
# YZ denotes the data from Yan and Zheng (2017)
# GHZ denotes the data retrieved from Green, Hand, and Zhang (2017): GHZ94 and GHZ93
# CZ denotes the data from Chen and Zimmermann (2022): CZ207 and CZ206
# TECH denotes the sampe using past return signals, TECH120 and TECH119.
# --------------

## YZ's signals. The signals contain 18,113 signals, combining 240 financial items and 76 permutations (15*5+1), excluding meaningless and redundant signals. 
# 240 fundamental variables (X)
regs_240x = [
    "at", "act", "invt", "ppent", "lt", "lct", "dltt", "ceq", "seq", "icapt", "sale", "cogs","xsga", 
    "pstkl", "ib", "ibadj", "ibcom", "pstkrv", "cstke", "pi", "txt", "ao", "dvp", "xido", "ni", "oiadp",
    "gp", "xopr",  "nopio", "nopi", "dvc", "dvt", "ebitda", "dp", "pstk", "dpact", "ppegt",  "dlc", 
    "che", "lo", "aco", "np", "capxv", "rect", "ebit", "pstkn", "xi", "ceql", "ceqt", "cstk", 
    "wcap", "re", "caps", "acox", "capx", "aox",  "niadj", "dcpstk", "txdb", "txditc", "recco",
    "itcb", "do", "spi", "xint", "lco", "ap", "dd1", "pstkc", "txp", "dcvt", "lcox", "mib",
    "ivao", "txdi", "ibc", "fopo", "ivaeq", "dv", "dpc", "sstk", "xidoc", "dclo", "dcvsr", "ds",
    "dlto", "dltr", "mii", "intan", "rea", "txr", "txdc", "dd", "dn", "dltis", "esub",
    "dcvsub", "ch", "ivst", "prstkc", "aqc", "ivch", "xrent", "ppeveb", "siv", "intc", "txc", "txfed",
    "tstk", "txs", "dvpa", "rectr", "msa", "xacc", "txfo", "dpvieb", "pstkr", "esubc", "lifr", "tlcf",
    "dd4", "dd2", "dd3", "dd5", "txw", "xpr", "tstkp", "tstkc", "aqs", "aqi", "sppe", "dc",
    "mrc1", "recd", "mrc2", "mrc3", "mrc4", "cstkcv", "chech", "mrct", "dudd", "reuna", "mrc5", "reajo",
    "recta", "am", "txo", "dm", "itci", "dltp", "sppiv", "invrm", "fincf", "ivncf", "fiao", "ivaco",
    "oancf", "aoloch", "exre", "idit", "acchg", "invfg", "xpp", "esopct", "esopt", "recch", "invwip", "esopdlt",
    "invch", "intpn", "xrd", "cld5", "cld4", "txpd", "cld3", "cld2", "txdfo", "fatc", "fatn", "ivstch",
    "gdwl", "txdfed", "dxd4", "txds", "dxd3", "dxd2", "dxd5", "fatp", "fatb", "ppevbb", "fato", "invo",
    "apalch", "fate", "nieci", "txach", "dvpibb", "fatl", "ppevr", "ppevo", "dlcch", "dfxa", "dpvio", "dpvir",
    "ppenc", "ob", "xad", "xdepl", "ppeno", "diladj", "dilavx", "ppennr", "fopt", "ppenme", "ppenli", "ppenb",
    "fsrco", "fuseo", "fsrct", "fuset", "rdip", "wcapc", "bast", "wcapch", "txndb", "donr", "txndbr", "dcom",
    "txndba", "txndbl", "lcoxdr", "pidom", "txdbca", "pifo", "drc", "dfs", "txdba", "txdbcl", "fopox", "txbco"]

regs_raw = ["_" + x for x in regs_240x]     # We add '_' to eliminate ambiguities in variable names. 

# 76 financial configurations, 15*5 + 1, based on 15 base variables (Y)
regs_15y = [
    "pd_var", 
    "var_a", "var_b", "var_c", "var_d", "var_e", "var_f", "var_g", "var_h",
    "var_i", "var_j", "var_k", "var_l", "var_m", "var_n", "var_o",
    "d_var_a", "d_var_b", "d_var_c", "d_var_d", "d_var_e", "d_var_f", "d_var_g", "d_var_h",
    "d_var_i", "d_var_j", "d_var_k", "d_var_l", "d_var_m", "d_var_n", "d_var_o",
    "pd_var_a", "pd_var_b", "pd_var_c", "pd_var_d", "pd_var_e", "pd_var_f", "pd_var_g", "pd_var_h",
    "pd_var_i", "pd_var_j", "pd_var_k", "pd_var_l", "pd_var_m", "pd_var_n", "pd_var_o",
    "d_var_at", "d_var_sale", "d_var_act", "d_var_invt", "d_var_ppent", "d_var_lt", "d_var_lct",
    "d_var_dltt", "d_var_ceq", "d_var_seq", "d_var_icapt", "d_var_cogs", "d_var_xsga", "d_var_emp", "d_var_mktcap",
    "pd_var_at", "pd_var_sale", "pd_var_act", "pd_var_invt", "pd_var_ppent", "pd_var_lt", "pd_var_lct",
    "pd_var_dltt", "pd_var_ceq", "pd_var_seq", "pd_var_icapt", "pd_var_cogs", "pd_var_xsga", "pd_var_emp", "pd_var_mktcap"]

# Raw 94 stock-level characteristics from Green, Hand, and Zhang (2017)
regs_ghz94 = [
    "absacc", "acc", "aeavol", "age", "agr", "baspread", "beta", "betasq", "bm", "bm_ia", 
    "cash", "cashdebt", "cashpr", "cfp", "cfp_ia", "chatoia", "chcsho", "chempia", "chinv", "chmom", 
    "chpmia", "chtx", "cinvest", "convind", "currat", "depr", "divi", "divo", "dolvol", "dy", 
    "ear", "egr", "ep", "gma", "grcapx", "grltnoa", "herf", "hire", "idiovol", "ill", 
    "indmom", "invest", "lev", "lgr", "maxret", "mom12m", "mom36m", "mom1m", "mom6m", "ms", 
    "mve_ia", "mvel1", "nincr", "operprof", "orgcap", "pchcapx_ia", "pchcurrat", "pchdepr", "pchgm_pchsale", "pchquick", 
    "pchsale_pchinvt", "pchsale_pchrect", "pchsale_pchxsga", "pchsaleinv", "pctacc", "pricedelay", "ps", "quick", "rd", "rd_mve", 
    "rd_sale", "realestate", "retvol", "roaq", "roavol", "roeq", "roic", "rsup", "salecash", "saleinv", 
    "salerec", "secured", "securedind", "sgr", "sin", "sp", "std_dolvol", "std_turn", "stdacc", "stdcf", 
    "tang", "tb", "turn", "zerotrade"]

# The 93 characteristics without short-term reversal (mom1m)
regs_ghz93 = list(regs_ghz94)
regs_ghz93.remove('mom1m')

## For CZ dataset and variables, downloaded from CZ's github sources.
# CZ207 used the 207 out of the 326 signals in the SignalDoc.xls. The filter is Cat.Signal = Predictor. 
regs_cz207 = [
    "AbnormalAccruals", "Accruals", "AccrualsBM", "Activism1", "AM", 
    "AnalystRevision", "AnnouncementReturn", "AssetGrowth", "BetaLiquidityPS", "BetaTailRisk",
    "betaVIX", "BM", "BMdec", "BookLeverage", "BPEBM", "Cash", "CashProd", "CBOperProf", "CF", "cfp",
    "ChangeInRecommendation", "ChAssetTurnover", "ChEQ", "ChForecastAccrual", "ChInv", 
    "ChInvIA", "ChNAnalyst", "ChNNCOA", "ChNWC", "ChTax",
    "CitationsRD", "CompEquIss", "CompositeDebtIssuance", "ConvDebt", "CoskewACX", 
    "CredRatDG", "CustomerMomentum", "DelBreadth", "DelCOA", "DelCOL",
    "DelEqu", "DelFINL", "DelLTI", "DelNetFin", "DivInit", "DivOmit", "DivSeason", "DivYieldST", "dNoa", "DolVol",
    "EarningsConsistency", "EarningsForecastDisparity", "EarningsStreak", "EarningsSurprise", "EarnSupBig", "EBM", "EntMult", "EP", "EquityDuration", "ExchSwitch",
    "ExclExp", "FEPS", "fgr5yrLag", "FirmAgeMom", "ForecastDispersion", "Frontier", "Governance", "GP", "GrAdExp", "grcapx",
    "grcapx3y", "Herf", "HerfBE", "hire", "IdioRisk", "IdioVol3F", "IdioVolAHT", "Illiquidity", "IndIPO", "IndMom",
    "IndRetBig", "IntanBM", "IntanCFP", "IntanEP", "IntanSP", "IntMom", "Investment", "InvestPPEInv", "InvGrowth", "iomom_cust",
    "iomom_supp", "Leverage", "LRreversal", "MaxRet", "MeanRankRevGrowth", "Mom12m", "Mom12mOffSeason", "Mom6m", "Mom6mJunk", "MomOffSeason",
    "MomOffSeason06YrPlus", "MomOffSeason16YrPlus", "MomRev", "MomSeason", "MomSeason06YrPlus", "MomSeason11YrPlus", "MomSeason16YrPlus", "MomSeasonShort", "MomVol", "MS",
    "NetDebtFinance", "NetDebtPrice", "NetEquityFinance", "NetPayoutYield", "NOA", "OPLeverage", "OptionVolume1", "OrderBacklog", "OrderBacklogChg", "OrgCap",
    "OScore", "PatentsRD", "PayoutYield", "PctAcc", "PctTotAcc", "Price", "PriceDelayRsq", "PS", "RD", "RDAbility",
    "RDcap", "RDIPO", "RDS", "Recomm_ShortInterest", "ResidualMomentum", "retConglomerate", "ReturnSkew", "ReturnSkew3F", "REV6", "RevenueSurprise",
    "RIO_Disp", "RIO_MB", "RIO_Turnover", "RIO_Volatility", "roaq", "sfe", "ShareIss1Y", "ShareIss5Y", "ShareVol", "Size",
    "SmileSlope", "std_turn", "STreversal", "SurpriseRD", "tang", "Tax", "TotalAccruals", "TrendFactor", "VolSD", "XFIN",
    "zerotrade", "zerotradeAlt1", "zerotradeAlt12", "Activism2", "AdExp", "AgeIPO", "AnalystValue", "AOP", "Beta", "BetaFP",
    "BidAskSpread", "BrandInvest", "ConsRecomm", "Coskewness", "DebtIssuance", "DelDRC", "DownRecomm", "FirmAge", "FR", "GrLTNOA",
    "GrSaleToGrInv", "GrSaleToGrOverhead", "HerfAsset", "High52", "IO_ShortInterest", "MomOffSeason11YrPlus", "MRreversal", "NumEarnIncrease", "OperProf", "OperProfRD",
    "OptionVolume2", "PredictedFE", "PriceDelaySlope", "PriceDelayTstat", "ProbInformedTrading", "realestate", "RoE", "ShareRepurchase", "ShortInterest", "sinAlgo",
    "skew1", "SP", "Spinoff", "UpRecomm", "VarCF", "VolMkt", "VolumeTrend"]

# Excluding short-term reversal (STreversal), resulting in 206 firm characteristics (CZ206)
regs_cz206 = list(regs_cz207)
regs_cz206.remove('STreversal')

########## Key variable for the different datasets
# Key var for the YZ signals.
key_var = ['counter', 'permno', 'form_md', 'aexret']

# Key var for technical, MMX and MZ
key_var_tech = ['counter', 'permno', 'mindex', 'mexret']

# Key variables for the cz207
key_var_cz207 = ['counter', 'permno', 'md', 'yyyymm', 'mexret']

## For GHZ dataset and variables
key_var_ghz94 = ['counter', 'permno', 'md', 'yyyymm', 'mexret']

############### Functions ###############
# This function reads the dataset into the memory. 
# We distinguish the dataset using the string in param_in['sample']. 
def read_data(param_in):
    data = pd.read_csv("../../Data/final_" + param_in['sample'] + ".csv")
    return(data)

# Format the output filename
def output_filename(param_in, mode = 'pred', path_pre = "../../Output/"):
    if (mode == 'sum'):
        folder = f"{path_pre}"
    elif (mode == 'pred'):
        folder = f"{path_pre}Pred/"
    elif (mode == 'imp'):
        folder = f"{path_pre}Imp/"
    elif (mode == 'plot'):
        folder = f"{path_pre}Plot/"
    elif (mode == 'cv'):
        folder = f"{path_pre}CV/"
    
    if (param_in['method'] == 'nn') or (param_in['method'] == 'enn'):
        method_adj = param_in['method'] + str(param_in['hidden_layers'])
    else:
        method_adj = param_in['method']    
    
    filename = folder + method_adj + '_' + \
        param_in['sample']+ '_' + \
        param_in['window'] + \
        '_dep_' + param_in['dep_var'] + \
        '_cov_' + str(param_in['cov']) + \
        '_num_' + str(param_in['cov_num']) + \
        '_t_' + str(param_in['cv_train']) + \
        '_v_' + str(param_in['cv_validation']) + \
        '_p' + str(param_in['port_num']) + \
        '_ver_' + str(param_in['version'])
    
    return(filename)

def cv_grid(method):
    # Define the grid for each method
    if (method == 'brt'):
        grid = {'n_iter':[100, 250, 500, 750, 1000],
                'n_rate': [0.01, 0.05, 0.1], 
                'depth': [-1]}
    elif (method == 'nn') or (method == 'mlp') or (method == 'enn'):
        grid = {'lambda':[0.00001, 0.001],
                'lr': [0.001,  0.01]}
        
    tunegrid = ParameterGrid(grid)    # Expand the parameter grid
    return(tunegrid) 

def get_dep_covariates(data, param_in):
    # obtain the dependent variable and covariates. 
    # The covariates need to end with cov_str, cov_num is used to control the number of covariates.
    dep_var = param_in['dep_var']
    if (param_in['cov'] == 'funda'):    # used for yz 18k signals
        cov_num = param_in['cov_num']
        cov_str = regs_raw[0:cov_num]
        covariates = data.columns.str.endswith(tuple(cov_str))
    elif (param_in['cov'] == 'tech'):   # used for past-return signals (with returns and squared returns)
        covariates = data.columns.str.startswith('lag_') # Caution: using lag will include lagmktcap and lagprice, so I change to lag_
    elif (param_in['cov'] == 'tech_ret'): # used for past-return signals (with returns only, PR119 or PR120)
        covariates = data.columns.str.endswith('_ret') 
    elif (param_in['cov'] == 'ghz94'):
        covariates = regs_ghz94
    elif (param_in['cov'] == 'cz207'):
        covariates = regs_cz207

    return dep_var, covariates

def get_key_test(data, param_in, k):
    '''
    # This function returns the key variables for the test sample.
    :param data: the whole data
    :param param_in: parameters
    :param k: k+1's data is the test data
    :return: key variables for the test sample.
    '''
    
    if (param_in['cov'] == 'funda'):
        key_test = data.loc[(data.counter == (k+1)), key_var].reset_index(drop = True)
    elif (param_in['cov'] == 'tech') or \
            (param_in['cov'] == 'tech_ret'):
        key_test = data.loc[(data.counter == (k+1)), key_var_tech].reset_index(drop = True)
    elif (param_in['cov'] == 'ghz94') or \
            (param_in['cov'] == 'ghz93'):
        key_test = data.loc[(data.counter == (k+1)), key_var_ghz94].reset_index(drop = True)
    elif (param_in['cov'] == 'cz207') or \
            (param_in['cov'] == 'cz206'):
        key_test = data.loc[(data.counter == (k+1)), key_var_cz207].reset_index(drop = True)
    else:  # default is the fundamental variables
        key_test = data.loc[(data.counter == (k+1)), key_var].reset_index(drop = True)

    return key_test

def train_validation_data(data, param_in, k):
    # recursive: test year: k+1; training years: [1, k-v];       validation years: [k-v+1, k]
    # rolling:   test year: k+1; training years: [k-v-t+1, k-v]; validation years: [k-v+1, k]
    # job_name:  'tech': startswith('lag'); 'funda': endswith regs_raw
    
    # obtain the dependent variable and covariates. 
    dep_var, covariates = get_dep_covariates(data, param_in)

    # obtain the training data
    if param_in['window'] == 'rolling':
        train_range = (data.counter <= (k-param_in['cv_validation'])) & (data.counter >=(k-param_in['cv_validation']-param_in['cv_train']+1))
    elif param_in['window'] == 'recursive':
        train_range = (data.counter <= (k-param_in['cv_validation']))
    
    X_train = data.loc[train_range, covariates]
    y_train = data.loc[train_range, dep_var]
    
    # NA in dep_var is not allowed in training, but allowed in testing
    X_train = X_train[y_train.notna()]
    y_train = y_train[y_train.notna()]
            
    # Obtain the validation sets
    validation_range = ((data.counter >= (k-param_in['cv_validation']+1))) & (data.counter <= k)
    X_validation = data.loc[validation_range, covariates]
    y_validation = data.loc[validation_range, dep_var]

    # NA is now allowed in training, but allowed in testing
    X_validation = X_validation[y_validation.notna()]
    y_validation = y_validation[y_validation.notna()]
    
    # Return the training and validation datasets
    return X_train, y_train, X_validation, y_validation

def train_test_data(data, param_in, k):
    # recursive: test year: k+1; training years: [1, k]
    # rolling:   test year: k+1; training years: [k-v-t+1, k]  

    # Obtain the list of dependent variable and covariates
    dep_var, covariates = get_dep_covariates(data, param_in)

    # get the training data
    if param_in['window'] == 'rolling':
        train_range = ((data.counter <= k) & (data.counter >=(k-param_in['cv_validation']-param_in['cv_train']+1)))
    elif param_in['window'] == 'recursive':
        train_range = (data.counter <= k)

    X_train = data.loc[train_range, covariates]
    y_train = data.loc[train_range, dep_var]

    # NA in dep_var is not allowed in training, but allowed in test
    X_train = X_train[y_train.notna()]
    y_train = y_train[y_train.notna()]
    
    # Obtain the test dataset using X_test, and keep the test variables to compare
    X_test = data.loc[(data.counter == (k+1)), covariates]
    key_test = get_key_test(data, param_in, k)

    return X_train, y_train, X_test, key_test

def get_test_data(data, param_in, k):
    # recursive: test year: k+1; training years: [1, k]
    # rolling:   test year: k+1; training years: [k-v-t+1, k]  

    # Obtain the list of dependent variable and covariates
    dep_var, covariates = get_dep_covariates(data, param_in)
    
    # Obtain the test dataset using X_test, and keep the test variables to compare
    X_test = data.loc[(data.counter == (k+1)), covariates]
    key_test = get_key_test(data, param_in, k)

    return X_test, key_test

# This function builds the nn structure
def build_nn(layer_dim, input_dim, param_lambda, param_lr):
    model_fit = Sequential()

    model_fit.add(Dense(32,
                        input_dim=input_dim,
                        activation='relu',
                        kernel_regularizer=l1(param_lambda)))
    model_fit.add(BatchNormalization())  # Batch Normalization after Dense layer

    if layer_dim >= 2:
        model_fit.add(Dense(16, 
                            activation='relu',
                            kernel_regularizer=l1(param_lambda)))
        model_fit.add(BatchNormalization())

    if layer_dim >= 3:
        model_fit.add(Dense(8, 
                            activation='relu',
                            kernel_regularizer=l1(param_lambda)))
        model_fit.add(BatchNormalization())

    if layer_dim >= 4:
        model_fit.add(Dense(4, 
                            activation='relu',
                            kernel_regularizer=l1(param_lambda)))
        model_fit.add(BatchNormalization())

    if layer_dim >= 5:
        model_fit.add(Dense(2, 
                            activation='relu',
                            kernel_regularizer=l1(param_lambda)))
        model_fit.add(BatchNormalization())

    model_fit.add(Dense(1, activation='linear'))  # final is a linear for regression

    # Define the Adam optimizer
    adam_optimizer = Adam(learning_rate=param_lr)

    # Compile the model with Adam optimizer and L1 regularization
    model_fit.compile(optimizer = adam_optimizer, 
                      loss = 'mean_squared_error',                       
                      metrics = ['mean_squared_error'])  # V1

    return model_fit

# This function conducts the cross-validation. 
def ML_cross_validation(param_in):
    data = read_data(param_in) # Read the dataset. It may be slow because of its huge size (32G)

    tunegrid = cv_grid(param_in['method'])  # Obtain grid for the grid search
    method = param_in['method']

    step = 1

    for k in range(param_in['begin'], param_in['end'], step):
        X_train, y_train, X_validation, y_validation = train_validation_data(data, param_in, k)
        
        cv_results = pd.DataFrame(list(tunegrid))
        cv_results['r2score'] = -100.00
        cv_results['mse'] = -100.00

        for i in range(len(tunegrid)):
            if method[0:3] == 'brt':
                model_fit = lgb.LGBMRegressor(learning_rate = tunegrid[i]['n_rate'],
                                            max_depth = int(tunegrid[i]['depth']),
                                            n_estimators = int(tunegrid[i]['n_iter']),
                                            metric = 'regression',
                                            force_col_wise = True,
                                            n_jobs = -1).fit(X_train, y_train)
            elif method == 'nn' or method == 'enn':
                model_fit = build_nn(layer_dim = param_in['hidden_layers'],
                                     input_dim = X_train.shape[1],
                                     param_lambda = tunegrid[i]['lambda'],
                                     param_lr = tunegrid[i]['lr'])
                
                # Early Stopping Callback
                early_stopping = EarlyStopping(monitor='loss', patience=5)

                model_fit.fit(X_train, y_train,
                              epochs=100,
                              batch_size=10000,
                              callbacks=[early_stopping],
                              use_multiprocessing=True,
                              verbose=False)

            y_pred = model_fit.predict(X_validation)

            r2score = r2_score(y_validation, y_pred)
            mse = mean_squared_error(y_validation, y_pred)
            cv_results.loc[i, 'r2score'] = r2score
            cv_results.loc[i, 'mse'] = mse

        for j in range(0, step):
            cv_results.sort_values(by=['r2score'], axis = 0, ascending = False, inplace = True)
            cv_results.to_csv(output_filename(param_in, mode = 'cv') + '_counter_' + str(k+1+j) + '_cv.csv', sep = ',', index = False)
            
            print(str(k+1+j) + " of " + str(param_in['end']))

# This function conducts the test step. 
def ML_Computation(param_in):
    data = read_data(param_in)
    
    method = param_in['method']
    max_counter = data['counter'].max()

    step = 1

    for k in range(param_in['begin'], param_in['end'], step):
        # Obtain the parameters obtained from cross validation or with fixed parameters (for debugging)
        if param_in['cov_rank'] == 'fixed':
            cv_param = cv_grid(param_in['method'])
            best_param = cv_param[0]
            best_param['depth'] = int(param_in['depth'])
            best_param['n_iter'] = int(param_in['n_iter'])
            best_param['n_rate'] = float(param_in['n_rate'])
        elif param_in['method'] == 'lr':
            best_param = 0
        elif param_in['method'] == 'enn':
            # mse with ascending = True; rscore with ascending = False
            tmp = param_in.copy()
            tmp['method'] = 'nn'  # enn has the same cross validation results as nn. 

            cv_param = pd.read_csv(output_filename(tmp, mode = 'cv') + "_counter_" + str(k+1) + "_cv.csv")
            cv_param.sort_values(by=['mse'], axis = 0, ascending = True, inplace = True)
            best_param = cv_param.iloc[0,]
        else:
            # mse with ascending = True; rscore with ascending = False
            cv_param = pd.read_csv(output_filename(param_in, mode = 'cv') + "_counter_" + str(k+1) + "_cv.csv")
            cv_param.sort_values(by=['mse'], axis = 0, ascending = True, inplace = True)

            best_param = cv_param.iloc[0,]

        # Obtain the training and test dataset
        X_train, y_train, X_test, key_test = train_test_data(data, param_in, k)
        
        # Train using the training datasets and specified parameters. 
        if method[0:3] == 'brt':
            model_fit = lgb.LGBMRegressor(learning_rate = float(best_param['n_rate']),
                                          max_depth     = int(best_param['depth']),
                                          n_estimators  = int(best_param['n_iter']),
                                          metric        = "regression",
                                          random_state  = 0,
                                          n_jobs        = -1).fit(X_train, y_train)
        elif method == 'nn':
            model_fit = build_nn(layer_dim = int(param_in['hidden_layers']),
                                 input_dim = X_train.shape[1],
                                 param_lambda = best_param['lambda'],
                                 param_lr = best_param['lr'])

            early_stopping = EarlyStopping(monitor = 'loss', patience = 5)
            model_fit.fit(X_train, y_train,
                          epochs = 100,
                          batch_size = 10000,
                          callbacks = [early_stopping],
                          use_multiprocessing = True,
                          verbose = False)
        elif method == 'enn':

            early_stopping = EarlyStopping(monitor = 'loss', patience = 5, mode = 'min')

            model_fit = []
            num_models = 10
            for _ in range(num_models):
                model = build_nn(layer_dim = int(param_in['hidden_layers']),
                                 input_dim = X_train.shape[1],
                                 param_lambda = best_param['lambda'],
                                 param_lr = best_param['lr'])
                
                model.fit(X_train, y_train,
                          epochs = 100,
                          batch_size = 10000,
                          callbacks = [early_stopping],
                          use_multiprocessing = True,
                          verbose = False)
                model_fit.append(model)

        for j in range(0, step):
            if (k+j < max_counter):
                # Obtain the prediction for counter k
                X_test_j, key_test_j = get_test_data(data, param_in, k+j)

                if method == 'enn':
                    predictions = [model.predict(X_test_j) for model in model_fit]
                    model_fit_pred = np.mean(predictions, axis = 0)
                else:
                    model_fit_pred = model_fit.predict(X_test_j)
                
                model_fit_store = key_test_j.assign(pred_return_1 = model_fit_pred.flatten())
                            
                if param_in['cov_rank'] == 'fixed':
                    str_param = str(best_param['depth'])+'_'+str(best_param['n_iter'])+'_'+str(best_param['n_rate'])
                    model_fit_store.to_csv(output_filename(param_in) + '_counter_' + str(k+1+j) + '_pred_python_' + str_param +'.csv', sep = ',', index = False)
                else:
                    model_fit_store.to_csv(output_filename(param_in) + '_counter_' + str(k+1+j) + '_pred_python' + '.csv', sep = ',', index = False)
            
                print(str(k+1+j) + " of " + str(max_counter))

