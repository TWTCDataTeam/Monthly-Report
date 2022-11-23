#################################################################################
# Enviroment :
##   Script   :   Industry_processer.py
##
# Author     :   Henry Hsieh
##   Date       :   2022-11-15
# Description:   table processing and output files
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

from datetime import datetime
import pandas as pd
from tqdm import tqdm
from config import FILE_OUTPUT_PATH

################### function ############################
# Author: Henry HSIEH
# Funciton: Finding file path in assigned document for 市場處
#########################################################


def caculate_each_industryData_and_summery_as_list(data, search_Industry, last_year_int, last_month_int):
    # parameter
    m_list = list(range(1, last_month_int+1))
    search_month = [str("%02d") % e for e in m_list]

    def grow_rate(numberator, denumberator):
        growRate = round(((numberator/denumberator)-1)*100, 2)
        return growRate

    def valueDivid1000(value):
        return (value/1000)

    def filterByIndYear(data, search_Industry, search_year):
        sumValue = data.loc[(data['Industry'] == search_Industry) & (
            data.year == search_year), :]['Value'].sum()
        return sumValue

    def filterByIndYearMonth(data, search_Industry, search_year, search_month):
        sumValue = data.loc[(data['Industry'] == search_Industry) & (
            data.year == search_year) & (data.month.isin(search_month)), :]['Value'].sum()
        return sumValue

    # caculate_each_ind_data
    industry_name = search_Industry
    value_2018 = filterByIndYear(data, search_Industry, str(last_year_int-3))
    value_2019 = filterByIndYear(data, search_Industry, str(last_year_int-2))
    grow_2019 = grow_rate(value_2019, value_2018)
    value_2020 = filterByIndYear(data, search_Industry, str(last_year_int-1))
    grow_2020 = grow_rate(value_2020, value_2019)
    value_last_Aggr_Month = filterByIndYearMonth(
        data, search_Industry, str(last_year_int-1), search_month)
    value_now_Aggr_Month = filterByIndYearMonth(
        data, search_Industry, str(last_year_int), search_month)
    grow_value_now_Aggr_Month = grow_rate(
        value_now_Aggr_Month, value_last_Aggr_Month)
    value_old_last_month = filterByIndYearMonth(
        data, search_Industry, str(last_year_int-1), [str("%02d") % last_month_int])
    value_last_month = filterByIndYearMonth(data, search_Industry, str(
        last_year_int), [str("%02d") % last_month_int])
    grow_last_month = grow_rate(value_last_month, value_old_last_month)
    # thousand to millions
    value_2019 = valueDivid1000(value_2019)
    value_2020 = valueDivid1000(value_2020)
    value_now_Aggr_Month = valueDivid1000(value_now_Aggr_Month)
    value_last_month = valueDivid1000(value_last_month)
    # output_list
    new_list = [industry_name, value_2019, grow_2019, value_2020, grow_2020,
                value_now_Aggr_Month, grow_value_now_Aggr_Month, value_last_month, grow_last_month]

    return new_list

################### function ############################
# Author: Henry HSIEH
# Funciton: Filtering country from raw data for 市場處
#########################################################


def filterCty(raw_data, target_cty_list):
    output = raw_data.loc[raw_data.Country.isin(target_cty_list), :]
    return output


################### function ############################
# Author: Henry HSIEH
# Funciton: time stamp of file for 市場處
#########################################################

# 標註檔案產出時間
def get_date():
    time = '%0.2d%0.2d%0.2d%0.2d%0.2d' % (datetime.now().year, datetime.now(
    ).month, datetime.now().day, datetime.now().hour, datetime.now().minute)
    return time

################### function ############################
# Author: Henry HSIEH
# Funciton: data output as file with for 市場處
#########################################################


def outputToExcel(df, file_name, sh_name, table_header):
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, sheet_name=sh_name, startrow=1, index=False)
    worksheet = writer.sheets[sh_name]
    worksheet.write_string(0, 0, table_header)
    raw_num = df.shape[0]+2
    worksheet.write_string(raw_num, 0, "資料來源：財政部關務署、全球貿易大數據平台(iTrade)")

    writer.save()
    print("{} output_complete".format(file_name))


################### function ############################
# Author: Henry HSIEH
# Funciton: data output as file  for 市場處
#########################################################

def main_process_outputFile_and_outputDf(raw_industry, industry_list, area_name, area_list, latest_year, latest_month):

    def createColumnName(latest_year, latest_month):
        col1 = "產業"
        col2 = str(latest_year-2)+"年出口值(百萬美元)"
        col3 = str(latest_year-2)+"年成長率(%)"
        col4 = str(latest_year-1)+"年出口值(百萬美元)"
        col5 = str(latest_year-1)+"年成長率(%)"
        col6 = str(latest_year)+"年1-"+str(latest_month)+"月出口值(百萬美元)"
        col7 = str(latest_year)+"年1-"+str(latest_month)+"月成長率(%)"
        col8 = str(latest_year)+"年"+str(latest_month)+"月出口值(百萬美元)"
        col9 = str(latest_year)+"年"+str(latest_month)+"月成長率(%)"

        return [col1, col2, col3, col4, col5, col6, col7, col8, col9]

    time = '%0.2d%0.2d%0.2d' % (
        datetime.now().year, datetime.now().month, datetime.now().day)

    str_time = datetime.now()
    if area_name != "全球":
        output_df = []
        for item in tqdm(industry_list):
            filtered_data = filterCty(raw_industry, area_list)
            tmp = caculate_each_industryData_and_summery_as_list(
                filtered_data, item, last_year_int=latest_year, last_month_int=latest_month)
            output_df.append(tmp)
    else:
        output_df = []
        for item in tqdm(industry_list):
            tmp = caculate_each_industryData_and_summery_as_list(
                raw_industry, item, last_year_int=latest_year, last_month_int=latest_month)
            output_df.append(tmp)

    column_names = createColumnName(latest_year, latest_month)
    output_df = pd.DataFrame(output_df, columns=column_names)
    # output_data
    end_time = datetime.now()
    total = end_time - str_time
    print("="*20)
    print("========== time {}==========".format(total))
    str_time = datetime.now()

    time = get_date()
    output_file_path = FILE_OUTPUT_PATH + \
        "\MARKET_DEPT_REPORT\各產業最新數據(市場處)_{}_{}.xlsx".format(area_name, time)
    file_header = "{}年1-{}月臺灣各產業對{}最新出口情勢".format(
        latest_year, latest_month, area_name)
    sheet_name = "{}".format(area_name)
    outputToExcel(output_df, output_file_path, sheet_name, file_header)
    #output_df.to_excel(output_file_path, index = False)

    output_df['market'] = area_name
    end_time = datetime.now()
    total = end_time-str_time
    print("============= output {} success time:{} =================".format(
        area_name, total))

    return output_df

################### function ############################
# Author: Henry HSIEH
# Funciton: 整合與計算產業數據 產業處
#########################################################


def industry_sec_caculate_each_industryData_and_summery_as_list(data, search_Industry, last_year_int, last_month_int):
    # parameter
    m_list = list(range(1, last_month_int+1))
    search_month = [str("%02d") % e for e in m_list]

    def grow_rate(numberator, denumberator):
        growRate = round(((numberator/denumberator)-1)*100, 2)
        return growRate
    # unit transform：thousand to 億

    def valueDivid1000(value):
        return (value/100000)

    def filterByIndYear(data, search_Industry, search_year):
        sumValue = data.loc[(data['Industry'] == search_Industry) & (
            data.year == search_year), :]['Value'].sum()
        return sumValue

    def filterByIndYearMonth(data, search_Industry, search_year, search_month):
        sumValue = data.loc[(data['Industry'] == search_Industry) & (
            data.year == search_year) & (data.month.isin(search_month)), :]['Value'].sum()
        return sumValue

    # caculate_each_ind_data
    industry_name = search_Industry
    value_2018 = filterByIndYear(data, search_Industry, str(last_year_int-3))
    value_2019 = filterByIndYear(data, search_Industry, str(last_year_int-2))
    grow_2019 = grow_rate(value_2019, value_2018)
    value_2020 = filterByIndYear(data, search_Industry, str(last_year_int-1))
    grow_2020 = grow_rate(value_2020, value_2019)
    # cal aggregate month export value in year = latest_year -2
    value_old_Aggr_Month = filterByIndYearMonth(
        data, search_Industry, str(last_year_int-2), search_month)
    # cal aggregate month export value in year = latest_year -1
    value_last_Aggr_Month = filterByIndYearMonth(
        data, search_Industry, str(last_year_int-1), search_month)
    # cal aggregate month export value in year = now
    value_now_Aggr_Month = filterByIndYearMonth(
        data, search_Industry, str(last_year_int), search_month)
    # cal last aggr month grow rate
    grow_value_last_Aggr_Month = grow_rate(
        value_last_Aggr_Month, value_old_Aggr_Month)
    # cal now growth rate
    grow_value_now_Aggr_Month = grow_rate(
        value_now_Aggr_Month, value_last_Aggr_Month)
    value_old_last_month = filterByIndYearMonth(
        data, search_Industry, str(last_year_int-1), [str("%02d") % last_month_int])
    value_last_month = filterByIndYearMonth(data, search_Industry, str(
        last_year_int), [str("%02d") % last_month_int])
    grow_last_month = grow_rate(value_last_month, value_old_last_month)
    # thousand to millions
    value_2019 = valueDivid1000(value_2019)
    value_2020 = valueDivid1000(value_2020)
    value_last_Aggr_Month = valueDivid1000(value_last_Aggr_Month)
    value_now_Aggr_Month = valueDivid1000(value_now_Aggr_Month)
    value_last_month = valueDivid1000(value_last_month)
    # output_list
    new_list = [industry_name, value_2019, grow_2019, value_2020, grow_2020, value_last_Aggr_Month,
                grow_value_last_Aggr_Month, value_now_Aggr_Month, grow_value_now_Aggr_Month, value_last_month, grow_last_month]

    return new_list

################### function ############################
# Author: Henry HSIEH
# Funciton: 產出產業處 產業處
#########################################################


def industry_sec_main_process_outputFile_and_outputDf(raw_industry, industry_list, area_name, area_list, latest_year, latest_month):

    def createColumnName(latest_year, latest_month):
        col1 = "產業"
        col2 = str(latest_year-2)+"年出口值(億美元)"
        col3 = str(latest_year-2)+"年成長率(%)"
        col4 = str(latest_year-1)+"年出口值(億美元)"
        col5 = str(latest_year-1)+"年成長率(%)"
        col6 = str(latest_year-1)+"年1-"+str(latest_month)+"月出口值(億美元)"
        col7 = str(latest_year-1)+"年1-"+str(latest_month)+"月成長率(%)"
        col8 = str(latest_year)+"年1-"+str(latest_month)+"月出口值(億美元)"
        col9 = str(latest_year)+"年1-"+str(latest_month)+"月成長率(%)"
        col10 = str(latest_year)+"年"+str(latest_month)+"月出口值(億美元)"
        col11 = str(latest_year)+"年"+str(latest_month)+"月成長率(%)"

        return [col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, col11]

    time = '%0.2d%0.2d%0.2d' % (
        datetime.now().year, datetime.now().month, datetime.now().day)

    str_time = datetime.now()
    if area_name != "全球":
        output_df = []
        for item in tqdm(industry_list):
            filtered_data = filterCty(raw_industry, area_list)
            tmp = industry_sec_caculate_each_industryData_and_summery_as_list(
                filtered_data, item, last_year_int=latest_year, last_month_int=latest_month)
            output_df.append(tmp)
    else:
        output_df = []
        for item in tqdm(industry_list):
            tmp = industry_sec_caculate_each_industryData_and_summery_as_list(
                raw_industry, item, last_year_int=latest_year, last_month_int=latest_month)
            output_df.append(tmp)

    column_names = createColumnName(latest_year, latest_month)
    output_df = pd.DataFrame(output_df, columns=column_names)
    # output_data
    end_time = datetime.now()
    total = end_time - str_time
    print("="*20)
    print("========== time {}==========".format(total))
    str_time = datetime.now()

    time = get_date()
    output_file_path = FILE_OUTPUT_PATH + \
        "\INDUSTRY_DEPT_REPORT\各產業最新數據(產業處)_{}_{}.xlsx".format(area_name, time)
    file_header = "{}年1-{}月臺灣各產業對{}最新出口情勢".format(
        latest_year, latest_month, area_name)
    sheet_name = "{}".format(area_name)
    outputToExcel(output_df, output_file_path, sheet_name, file_header)
    #output_df.to_excel(output_file_path, index = False)

    output_df['market'] = area_name
    end_time = datetime.now()
    total = end_time-str_time
    print("============= output {} success time:{} =================".format(
        area_name, total))

    return output_df
