######################################################
# Enviroment :
##   Script     :   mof_data_collector.py
##
# Author     :   Henry Hsieh
##   Date       :   2022-10-29
# Description:   The function of collecting mof files (tsv files/ txt files)
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
import re
import sys
from datetime import datetime
from Industry_processer import *
import pandas as pd
from tqdm import tqdm

from config import COUNTRY_SOURCE, EXCHANGE_RATE_SOURCE, LOCAL_TXT_SOURCE, LOCAL_TSV_SOURCE

################### function ############################
# Author: Henry HSIEH
# Funciton: Finding file path in assigned document
#########################################################


def find_file_path_from_asigned_doc_by_glob(path, target):
    target_doc_path = path + "\\"+target
    file_list_path = glob.glob(target_doc_path)
    return file_list_path

######################################################
# Author: Henry HSIEH
# funciton: collecting txt file path
#######################################################
# get txt file file path


def get_txt_file_path():
    txt_file_list = glob.glob(LOCAL_TXT_SOURCE)
    print(len(txt_file_list))
    return txt_file_list

######################################################
# Author: Henry HSIEH
# funciton:  function of text extraction (sub, left, right)
#######################################################


def str_sub(text, start, end):
    return(text[start:end])


def str_left(text, end):
    return(text[:end])


def str_right(text, start):
    return(text[start:])

######################################################
# Author: Henry HSIEH
# funciton: Getting Data from TXT Files
#######################################################

# txt data process : get specific string in txt file


def mof_data_processing(data, str_year, str_month):

    line = str(data)

    hscode = str_left(line, 11)
    country = str_sub(line, 11, 13)
    custom = int(str_sub(line, 13, 14))
    trans_type = int(str_sub(line, 14, 16))
    ex_im = int(str_sub(line, 16, 17))
    hs_ver = str_sub(line, 17, 19)
    quant_unit = str_sub(line, 19, 22)
    weight_unit = str_sub(line, 22, 25)
    country_digit = str_sub(line, 25, 28)
    quant_mon = int(str_sub(line, 28, 40))
    weight_mon = int(str_sub(line, 40, 52))
    value_mon = int(str_sub(line, 52, 64))
    quant_year = int(str_sub(line, 64, 78))
    weight_year = int(str_sub(line, 78, 92))
    value_year = int(str_sub(line, 92, 106))
    #mof_month = str_sub(line,106,107)
    year = str(str_year)
    month = str(str_month)

    return([hscode, country, custom, trans_type, ex_im, hs_ver, quant_unit, weight_unit, country_digit, quant_mon, weight_mon, value_mon, quant_year, weight_year, value_year, year, month])

######################################################
# Author: Henry HSIEH
# funciton: Getting Data from TXT Files
#######################################################


def process_mof_txt_file(file_path):

    # extract year and month
    data_year = str(int(re.findall(r'\d+', file_path)[0][:3]) + 1911)
    data_month = re.findall(r'\d+', file_path)[0][3:]

    file_content = []
    with open(file_path, 'r') as f:
        for line in f.readlines():
            file_content.append(line.strip())   # 把末尾的'\n'刪掉
        f.close()

    m_w_data = []

    for line in file_content:
        # print(str(line),len(str(line)))
        m_w_data.append(mof_data_processing(
            str(line), str(data_year), str(data_month)))

    column_name = ['hscode', 'country', 'custom', 'trans_type', 'ex_im', 'hs_ver', 'quant_unit', 'weight_unit',
                   'country_digit', 'quant_mon', 'weight_mon', 'value_mon', 'quant_year', 'weight_year', 'value_year', 'year', 'month']
    output_data = pd.DataFrame(m_w_data, columns=column_name)

    return output_data
######################################################
# Author: Henry HSIEH
# funciton: Getting exchange rate from file
#######################################################

# loading exchange rate data


def exchange_rate_list():

    ex_rate_df = pd.read_excel(EXCHANGE_RATE_SOURCE, header=1)
    ex_rate_list = ex_rate_df.出口.tolist()
    ex_rate_list
    return ex_rate_list

######################################################
# Author: Henry HSIEH
# funciton : collect export data, and transfer NTD to USD
#######################################################


def txt_monthly_data_process(txt_data, exchange_rate):
    # export: ex_im = 1 & 4; import: ex_im = 2 & 5
    export_df = txt_data.loc[(txt_data['ex_im'] == 1)
                             | (txt_data['ex_im'] == 4), ]
    export_df["exchange_rate"] = exchange_rate
    export_df["value_mon_usd"] = (
        export_df["value_mon"]*1000/export_df["exchange_rate"])/1000
    used_column = ['hscode', 'country', 'ex_im', 'value_mon',
                   'exchange_rate', 'value_mon_usd', 'month', 'year']
    export_df = export_df.loc[:, used_column]
    export_df = export_df.groupby(by=['hscode', 'country', 'exchange_rate', 'month', 'year'])[
        'value_mon', 'value_mon_usd'].sum().reset_index()

    return export_df

######################################################
# Author: Henry HSIEH
# funciton : collect import data, and transfer NTD to USD
#######################################################


def txt_monthly_import_data_process(txt_data, exchange_rate):
    # export: ex_im = 1 & 4; import: ex_im = 2 & 5
    export_df = txt_data.loc[(txt_data['ex_im'] == 2)
                             | (txt_data['ex_im'] == 5), ]
    export_df["exchange_rate"] = exchange_rate
    export_df["value_mon_usd"] = (
        export_df["value_mon"]*1000/export_df["exchange_rate"])/1000
    used_column = ['hscode', 'country', 'ex_im', 'value_mon',
                   'exchange_rate', 'value_mon_usd', 'month', 'year']
    export_df = export_df.loc[:, used_column]
    export_df = export_df.groupby(by=['hscode', 'country', 'exchange_rate', 'month', 'year'])[
        'value_mon', 'value_mon_usd'].sum().reset_index()

    return export_df

######################################################
# Author: Henry HSIEH
# funciton : collect export/import data, and transfer NTD to USD
#######################################################


def get_country_data():
    # loading country_data

    cty_file = glob.glob(COUNTRY_SOURCE)
    cty_df = pd.read_excel(cty_file[0], sheet_name="國家對應", usecols=[
        "國家中文名稱", "國碼"], na_filter=False)
    colname = {"國家中文名稱": "cty_zen", "國碼": "country"}
    cty_df = cty_df.rename(colname, axis="columns")
#    cty_df.head()

    return cty_df


######################################################
# Author: Henry HSIEH
# funciton : output txt data for caculation. This process has done three task as below:
#           1. collect whole txt data
#           2. transfer NTD to USD
#           3. transfer country code to country name
#           4. export data
#######################################################

def mof_txt_df():

    txt_file_list = get_txt_file_path()
    print("--- get txt_file_path done ---")
    ex_rate_list = exchange_rate_list()
    #print("--- get exhange rate done ---")
    cty_df = get_country_data()
    print("--- get country data done---")

    # collect whole txt data
    txt_df = []
    for i_path, ex_i in zip(txt_file_list, ex_rate_list):
        datatime_str = re.findall(r'\d+', i_path)
        tmp = process_mof_txt_file(i_path)
        tmp = txt_monthly_data_process(tmp, ex_i)
        txt_df.append(tmp)
        print("{} finished".format(datatime_str[0]))

    txt_df = pd.concat(txt_df)
    # txt_df.head()

    txt_df = txt_df.merge(cty_df, left_on="country",
                          right_on="country", how="left")
    used_cols = ['hscode', 'cty_zen', 'value_mon_usd', 'year', 'month']
    txt_df = txt_df.loc[:, used_cols]
    mof_data = txt_df.rename(columns={'hscode': "Hscode", 'cty_zen': "Country",
                                      'value_mon_usd': "Value", 'year': "year", 'month': "month"})
    return mof_data

######################################################
# Author: Henry HSIEH
# funciton : output txt data for caculation. This process has done three task as below:
#           1. collect whole txt data
#           2. transfer NTD to USD
#           3. transfer country code to country name
#           4. export data
#######################################################


def mof_import_txt_df():

    txt_file_list = get_txt_file_path()
    #print("--- get txt_file_path done ---")
    ex_rate_list = exchange_rate_list()
    #print("--- get exhange rate done ---")
    cty_df = get_country_data()
    #print("--- get country data done---")

    # collect whole txt data
    txt_df = []
    for i_path, ex_i in zip(txt_file_list, ex_rate_list):
        datatime_str = re.findall(r'\d+', i_path)
        tmp = process_mof_txt_file(i_path)
        tmp = txt_monthly_import_data_process(tmp, ex_i)
        txt_df.append(tmp)
        print("{} finished".format(datatime_str[0]))

    txt_df = pd.concat(txt_df)
    # txt_df.head()

    txt_df = txt_df.merge(cty_df, left_on="country",
                          right_on="country", how="left")
    used_cols = ['hscode', 'cty_zen', 'value_mon_usd', 'year', 'month']
    txt_df = txt_df.loc[:, used_cols]
    mof_data = txt_df.rename(columns={'hscode': "Hscode", 'cty_zen': "Country",
                                      'value_mon_usd': "Value", 'year': "year", 'month': "month"})
    return mof_data

################### function ############################
# Author: Henry HSIEH
# Funciton: reading mof tsv file
#########################################################


def read_MOF_File(file_path):
    df = pd.read_csv(file_path, encoding='utf-8', sep='\t', usecols=['貨品分類', '中文貨名', '國家', '價值'],
                     dtype={
        '貨品分類': str,
        '中文貨名': str,
        '國家': str,
        '價值': float})
    df.columns = ['Hscode', 'Hscode_Chinese', 'Country', 'Value']
    df['Date'] = file_path.split('\\')[-1][:7]
    df['year'] = df['Date'].str[:4]
    df['month'] = df['Date'].str[5:]

    return df

################### function ############################
# Author: Henry HSIEH
# Funciton: loading tsv file. Method 1
#########################################################

# # OUTPUT:
# # .\mof\mof-export-usd\2020-01.tsv
# # .\mof\mof-export-usd\2020-02.tsv
# # .\mof\mof-export-usd\2020-03.tsv
# # .\mof\mof-export-usd\2020-04.tsv
# # .\mof\mof-export-usd\2020-05.tsv
# # .\mof\mof-export-usd\2021-01.tsv
# # .\mof\mof-export-usd\2021-02.tsv
# # .\mof\mof-export-usd\2021-03.tsv
# # .\mof\mof-export-usd\2021-04.tsv
# # .\mof\mof-export-usd\2021-05.tsv


def select_Same_Period_MOF_df(target_year, trade_flow, currency):

    target_year = str(target_year)
    target_path = LOCAL_TSV_SOURCE + "\mof-" + trade_flow + "-" + currency + "\*.tsv"
    lst = sorted(glob.glob(target_path))
    last_Year = lst[-1].split("\\")[-1][:4]  # 現在年分
    #print(last_Year," ",type(last_Year))
    last_month = lst[-1].split("\\")[-1][5:7]  # 最新月份
    # print(last_month)

    lslist = [target_year, str(int(target_year)-1)]
    print(lslist)

    L = []
    for i_path in lst:
        ls = i_path.split("\\")[-1][:4]

        if target_year != last_Year:
            for y in lslist:
                if ls == y:
                    print(i_path)
                    df = read_MOF_File(i_path)
                    L.append(df)
        else:
            for y in lslist:
                if ls == y:
                    la = i_path.split("\\")[-1][5:7]
                    if la <= last_month:  # if only run one month  set code la <= last_month as la == last_month
                        print(i_path)
                        df = read_MOF_File(i_path)
                        L.append(df)

    p1 = pd.concat(L)
    return p1

################### function ############################
# Author: Henry HSIEH
# Funciton: loading tsv file. Method 2
#########################################################

# OUTPUT:
# .\mof\mof-export-usd\2020-01.tsv
# .\mof\mof-export-usd\2020-02.tsv
# .\mof\mof-export-usd\2020-03.tsv
# .\mof\mof-export-usd\2020-04.tsv
# .\mof\mof-export-usd\2020-05.tsv
# .\mof\mof-export-usd\2020-06.tsv
# .\mof\mof-export-usd\2020-07.tsv
# .\mof\mof-export-usd\2020-08.tsv
# .\mof\mof-export-usd\2020-09.tsv
# .\mof\mof-export-usd\2020-10.tsv
# .\mof\mof-export-usd\2020-11.tsv
# .\mof\mof-export-usd\2020-12.tsv
# .\mof\mof-export-usd\2021-01.tsv
# .\mof\mof-export-usd\2021-02.tsv
# .\mof\mof-export-usd\2021-03.tsv
# .\mof\mof-export-usd\2021-04.tsv
# .\mof\mof-export-usd\2021-05.tsv


def select_MOF_df(start_year, end_year, trade_flow, currency):
    target_path = LOCAL_TSV_SOURCE +"\mof-" + trade_flow + "-" + currency + "\*.tsv"
    print(target_path)
    lst = sorted(glob.glob(target_path))
    print(lst[:3])

    lslist = list(range(int(start_year), int(end_year)+1))
    lslist = [str(e) for e in lslist]
    print("Selected Year:", lslist)

    L = []
    for i_path in lst:
        ls = i_path.split("\\")[-1][:4]
        for y in lslist:
            if ls == y:
                print(i_path)
                df = read_MOF_File(i_path)
                L.append(df)

    p1 = pd.concat(L)

    return p1

################### function ############################
# Author: Henry HSIEH
# Funciton: filter country
#########################################################


def filterCountry(raw_data, target_country_list):
    output = raw_data.loc[raw_data.Country.isin(target_country_list), :]
    return output

################### function ############################
# Author: Henry HSIEH
# Funciton: caculate each industry total value
#########################################################


def caculate_each_industry_summary(data, search_Industry):
    sumValue = data.loc[data["Industry"] == search_Industry, :]['Value'].sum()
    # sumValue unit is thousand USD; sumValue/1000 is Million USD
    newList = [search_Industry, round(sumValue/1000, 2)]
    return newList

################### function ############################
# Author: Henry HSIEH
# Funciton: caculate mof data by using tsv data (<2019)
#########################################################


def caculate_year_data(year, country_list, ind_define_dict):

    year = str(year)
    print("="*50)
    print("loading_raw_data")

    # from mof-export-usd document, getting mof tsv data and time period is from 2018 to 2021
    mof_data = select_MOF_df(year, year, "export", "usd")
    # remain the used columns for caculate
    mof_data = mof_data.loc[:, ["Hscode", "Country", "Value", "year", "month"]]

    print("="*50)
    print("filter Country List")

    caculate_df = filterCountry(mof_data, country_list)

    print("="*50)
    print("Caculate Industry data")
    # caculate_Industry_data
    raw_industry = convert_indRawData(caculate_df, ind_define_dict)

    # create a industry_list
    industry_list = list(ind_define_dict.keys())

    industry_sumValue_collector = []
    for e in industry_list:
        tmp = caculate_each_industry_summary(raw_industry, e)
        industry_sumValue_collector.append(tmp)

    output_df = pd.DataFrame(industry_sumValue_collector,
                             columns=["Industry", year])

    return output_df

################### function ############################
# Author: Henry HSIEH
# Funciton: caculate mof data by using txt data (>2019)
#########################################################


def caculate_txt_year_data(year, txt_data, country_list, ind_define_dict):

    if year >= 2019:
        year = str(year)
        print("="*50)
        print("loading_raw_data")

        # from mof-export-usd document, getting mof tsv data and time period is from 2018 to 2021
        mof_data = txt_data.loc[txt_data.year.isin([year]), :]

        print("="*50)
        print("filter Country List")

        caculate_df = filterCountry(mof_data, country_list)

        print("="*50)
        print("Caculate Industry data")
        # caculate_Industry_data
        raw_industry = convert_indRawData(caculate_df, ind_define_dict)

        # create a industry_list
        industry_list = list(ind_define_dict.keys())

        industry_sumValue_collector = []
        for e in industry_list:
            tmp = caculate_each_industry_summary(raw_industry, e)
            industry_sumValue_collector.append(tmp)

        output_df = pd.DataFrame(
            industry_sumValue_collector, columns=["Industry", year])

        return output_df
    else:
        return "{} support failed".format(year)

################### function ############################
# Author: Henry HSIEH
# Funciton: caculate_years_industry_sumdata_and_output_dataframe_v2
#########################################################


def caculate_years_industry_sumdata_and_output_dataframe_v2(data, target_year, month_list, ind_define_dict):
    # prepare industry list
    industry_list = list(ind_define_dict.keys())
    # extract start month and end month

    start_month = int(month_list[0])
    latest_month = int(month_list[-1])

    # prepare target year data
    caculate_df = data.loc[(data.year == target_year) &
                           (data.month.isin(month_list)), :]

    raw_industry = convert_indRawData(caculate_df, ind_define_dict)
    print(raw_industry)

    industry_sumValue_collector = []
    for e in industry_list:
        tmp = caculate_each_industry_summary(raw_industry, e)
        industry_sumValue_collector.append(tmp)

    # list to dataframe
    col_name = "{}年{}-{}月".format(target_year, start_month, latest_month)
    output_df = pd.DataFrame(industry_sumValue_collector,
                             columns=["Industry", col_name])

    return output_df

################### function ############################
# Author: Henry HSIEH
# Funciton: careate month list
#########################################################

# 產出月份的
# start_month, start is 0
# end_month, end is 12


def output_month_list(start_month, end_month):
    if end_month < 13:
        month_list = []
        for item in [str(i+1) for i in range(start_month, end_month)]:
            month_list.append(item.zfill(2))

    else:
        return ("OutOfRange")

    return month_list

################### function ############################
# Author: Henry HSIEH
# Funciton:caculate_latest_txt_year_data
#########################################################


def caculate_latest_txt_year_data(latest_year, latest_month, txt_data, country_list, ind_define_dict):
    latest_year_list = [str(int(latest_year)-1), str(latest_year)]
    latest_year = str(latest_year)

    # create month_list
    start_manth = 0
    end_month = latest_month
    month_list = output_month_list(start_manth, end_month)
    print("parameter: month_list:{} year_list:{}".format(
        month_list, latest_year_list))

    print("="*50)
    print("loading_raw_data")

#     # from mof-export-usd document, getting mof tsv data and time period is from 2018 to 2021
#     mof_data = select_Same_Period_MOF_df(latest_year,"export","usd")
#     # remain the used columns for caculate
#     mof_data = mof_data.loc[:,["Hscode","Country","Value","year","month"]]
    mof_data = txt_data.loc[(txt_data.year.isin(
        latest_year_list)) & txt_data.month.isin(month_list), :]
    output_df = mof_data

    print("="*50)
    print("filter Country List")

    caculate_df = filterCountry(mof_data, country_list)

    print("="*50)
    print("Caculate Industry data")

    # create a industry_list
    industry_list = list(ind_define_dict.keys())

    # caculate_Industry_data
    output_df = pd.DataFrame(industry_list, columns=["Industry"])
    for yr in latest_year_list:

        # caculate_funciton
        tmp = caculate_years_industry_sumdata_and_output_dataframe_v2(
            caculate_df, yr, month_list, ind_define_dict)
        print(tmp)
        # data merge
        output_df = output_df.merge(tmp, on="Industry", how="outer")

    return output_df

################### function ############################
# Author: Henry HSIEH
# Funciton:caculate import value
#########################################################

def caculate_each_industry_import_summary(data, search_Industry):
    data = data.loc[~data.Hscode.str.contains('Z$')]
    sumValue = data.loc[data["Industry"]==search_Industry,:]['Value'].sum()
    # sumValue 單位千美元   => sumValue/1000 百萬美元
    newList = [search_Industry, round(sumValue/1000,2)] 
    return newList

################### function ############################
# Author: Henry HSIEH
# Funciton:caculate import year data for tsv files
#########################################################

def caculate_import_year_data(year,country_list, ind_define_dict):
    
    year = str(year)
    print("="*50)
    print("loading_raw_data")    
    
    # from mof-export-usd document, getting mof tsv data and time period is from 2018 to 2021
    mof_data = select_MOF_df(year,year,"import","usd")
    # remain the used columns for caculate
    mof_data = mof_data.loc[:,["Hscode","Country","Value","year","month"]]
    
    print("="*50)
    print("filter Country List")
    
    caculate_df = filterCountry(mof_data,country_list)
    
    print("="*50)
    print("Caculate Industry data")
    #caculate_Industry_data
    raw_industry = convert_indRawData(caculate_df ,ind_define_dict)
    
    # create a industry_list
    industry_list = list(ind_define_dict.keys())
    
    industry_sumValue_collector= [] 
    for e in industry_list:
        tmp = caculate_each_industry_import_summary(raw_industry,e)
        industry_sumValue_collector.append(tmp)

    output_df =pd.DataFrame(industry_sumValue_collector, columns= ["Industry",year])
    
    return output_df

################### function ############################
# Author: Henry HSIEH
# Funciton:caculate_txt_year_import_dat
#########################################################

def caculate_txt_year_import_data(year,txt_data,country_list, ind_define_dict):

    if year >= 2019:
        year = str(year)
        print("="*50)
        print("loading_raw_data")    
    
        # from mof-export-usd document, getting mof tsv data and time period is from 2018 to 2021
        mof_data = txt_data.loc[txt_data.year.isin([year]),:]
    
        print("="*50)
        print("filter Country List")
    
        caculate_df = filterCountry(mof_data,country_list)
    
        print("="*50)
        print("Caculate Industry data")
        #caculate_Industry_data
        raw_industry = convert_indRawData(caculate_df ,ind_define_dict)
    
        # create a industry_list
        industry_list = list(ind_define_dict.keys())
    
        industry_sumValue_collector= [] 
        for e in industry_list:
            tmp = caculate_each_industry_import_summary(raw_industry,e)
            industry_sumValue_collector.append(tmp)

        output_df =pd.DataFrame(industry_sumValue_collector, columns= ["Industry",year])
    
        return output_df
    else:
        return "{} support failed".format(year)
    