#################################################################################
# Enviroment :
##   Script   :   Industry_processer.py
##
# Author     :   Henry Hsieh
##   Date       :   2022-11-15
# Description:   The function of processing Industry Definition and merging data with raw data
# Source     :
# Reference  :
# Target     :
# Outfile    :
# Requester  :
# Change History :
# --------------------------------------------------------------------------
# Date      Authors         Description/Ref. No.
# ----------   --------------- ---------------------------------------------
#################################################################################

import glob
import pandas as pd
from tqdm import tqdm
from config import INDUSTRY_FILE_SOURCE, INDUSTRY_TARGET_FILE_NAME_MRT_SEC, INDUSTRY_TARGET_FILE_NAME_IND_SEC

################### function ############################
# Author: Henry HSIEH
# Funciton: Finding file path in assigned document
#########################################################


def find_file_path_from_asigned_doc_by_glob(path, target):
    target_doc_path = path + "\\"+target
    file_list_path = glob.glob(target_doc_path)
    return file_list_path

################### function ############################
# Author: Henry HSIEH
# Funciton: loading industry excel file
#########################################################


def loading_IndutryData_from_excel_file(filePath, target_file, usedcols_list):
    f_path = find_file_path_from_asigned_doc_by_glob(filePath, target_file)
    print(f_path)
    # LOADING INDUSTRY EXCEL FILE
    ind_file = pd.read_excel(f_path[0],
                             usecols=usedcols_list,
                             dtype="str")
    return ind_file

################### function ############################
# Author: Henry HSIEH
# Funciton: transfer hscode to dictionary. This process is transfering data type
#########################################################


def transfer_IndustryData_to_Dict(ind_df):
    definelst = {i: '|'.join(["^"+j for j in ind_df.hscode.tolist()[ix].split(',')])
                 for ix, i in enumerate(ind_df.reports_version_2_ind_name.tolist())}
    return definelst

################### function ############################
# Author: Henry HSIEH
# Funciton: removing duplicated hscode in defined list
#########################################################


def remove_duplicated_hscode(hscode_def_txt):
    tmp = hscode_def_txt.split(",")
    tmp = sorted(list(set(tmp)))
    output = ",".join(tmp)
    return output

################### function ############################
# Author: Henry HSIEH
# Funciton: Getting Defined Industry List of Market Section
#########################################################


def get_market_sec_industry_define_dict():
    # loading data
    filePath = INDUSTRY_FILE_SOURCE
    target_file = INDUSTRY_TARGET_FILE_NAME_MRT_SEC
    usedcols_list = ['hscode', 'reports_version_2_order',
                     'reports_version_2_ind_name']
    ind_file = loading_IndutryData_from_excel_file(
        filePath, target_file, usedcols_list)

    # FILTER NOTNA RAW AND REARRANGE TABLE
    ind_file = ind_file.loc[ind_file["reports_version_2_ind_name"].notna(
    ), ['reports_version_2_ind_name', 'reports_version_2_order', 'hscode']].reset_index(drop=True)
    ind_file = ind_file.loc[ind_file["reports_version_2_order"].notna(
    ), ['reports_version_2_ind_name', 'reports_version_2_order', 'hscode']].reset_index(drop=True)

    # remove duplicated hscodde
    cleaned_hscode_list = []
    for item in ind_file.hscode.tolist():
        tmp = remove_duplicated_hscode(item)
        cleaned_hscode_list.append(tmp)

    ind_file['hscode'] = cleaned_hscode_list

    # transferDataToDictionary
    ind_define_dict = transfer_IndustryData_to_Dict(ind_file)

    return ind_define_dict

################### function ############################
# Author: Henry HSIEH
# Funciton: Getting Defined Industry List of Industry Section
#########################################################


def get_industry_sec_industry_define_dict():
    # loading data
    filePath = INDUSTRY_FILE_SOURCE
    target_file = INDUSTRY_TARGET_FILE_NAME_IND_SEC
    usedcols_list = ['hscode', 'reports_version_2_order',
                     'reports_version_2_ind_name']
    ind_file = loading_IndutryData_from_excel_file(
        filePath, target_file, usedcols_list)

    # FILTER NOTNA RAW AND REARRANGE TABLE
    ind_file = ind_file.loc[ind_file["reports_version_2_ind_name"].notna(
    ), ['reports_version_2_ind_name', 'reports_version_2_order', 'hscode']].reset_index(drop=True)
    ind_file = ind_file.loc[ind_file["reports_version_2_order"].notna(
    ), ['reports_version_2_ind_name', 'reports_version_2_order', 'hscode']].reset_index(drop=True)

    # remove duplicated hscodde
    cleaned_hscode_list = []
    for item in ind_file.hscode.tolist():
        tmp = remove_duplicated_hscode(item)
        cleaned_hscode_list.append(tmp)

    ind_file['hscode'] = cleaned_hscode_list

    # transferDataToDictionary
    ind_define_dict = transfer_IndustryData_to_Dict(ind_file)

    return ind_define_dict

################### function ############################
# Author: Henry HSIEH
# Funciton: Getting whole Defined Industry List
#########################################################

# def get_industry_sec_industry_define_dict():
#     # loading data
#     filePath = INDUSTRY_FILE_SOURCE
#     target_file = INDUSTRY_TARGET_FILE_NAME_IND_SEC
#     usedcols_list = ['hscode','reports_version_2_order','reports_version_2_ind_name']
#     ind_file = loading_IndutryData_from_excel_file(filePath, target_file, usedcols_list)

#     # FILTER NOTNA RAW AND REARRANGE TABLE
#     ind_file = ind_file.loc[ind_file["reports_version_2_ind_name"].notna(),['reports_version_2_ind_name','reports_version_2_order','hscode']].reset_index(drop=True)
#     ind_file = ind_file.loc[ind_file["reports_version_2_order"].notna(),['reports_version_2_ind_name','reports_version_2_order','hscode']].reset_index(drop=True)

#     # remove duplicated hscodde
#     cleaned_hscode_list = []
#     for item in ind_file.hscode.tolist():
#         tmp = remove_duplicated_hscode(item)
#         cleaned_hscode_list.append(tmp)

#     ind_file['hscode'] = cleaned_hscode_list

#     # transferDataToDictionary
#     ind_define_dict = transfer_IndustryData_to_Dict(ind_file)

#     return ind_define_dict

################### function ############################
# Author: Henry HSIEH
# Funciton: Base on Defined Industry List, tagging each record to asigned industry category
#########################################################

# prepare data, containing mof value and Industry Data


def convert_indRawData(data, indDefinelst):
    L = []
    for ix, i in tqdm(indDefinelst.items()):
        tmp = data[data.Hscode.str.contains(i, regex=True)].copy()
        tmp['Industry'] = str(ix)
        L.append(tmp)

    new_data = pd.concat(L)

    return new_data

################### function ############################
# Author: Henry HSIEH
# Funciton: loading Industrial List
#########################################################


def loading_Industry_list():
    filePath = INDUSTRY_FILE_SOURCE
    target_file = INDUSTRY_TARGET_FILE_NAME_MRT_SEC
    usedcols_list = ['hscode', 'reports_version_2_order',
                     'reports_version_2_ind_name']
    ind_file = loading_IndutryData_from_excel_file(
        filePath, target_file, usedcols_list)

    ind_list = ind_file.reports_version_2_ind_name.tolist()

    return ind_list
