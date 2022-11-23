import pandas as pd
import glob
import re
from datetime import datetime
from tqdm import tqdm
import sys

# suppress scientific notation
#pd.set_option('display.float_format', lambda x: '%.2f' % x)


# ## function libraary


def find_file_path_from_asigned_doc_by_glob(path, target):
    target_doc_path = path + "\\"+target
    file_list_path = glob.glob(target_doc_path)
    return file_list_path


# get txt file file path
target_path = r"..\mof財政部關務署光碟資料\custom_exim\*.txt"
txt_file_list = glob.glob(target_path)
print(len(txt_file_list))
txt_file_list


# txt extract function
def str_sub(text, start, end):
    return(text[start:end])


def str_left(text, end):
    return(text[:end])


def str_right(text, start):
    return(text[start:])


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
#     output_data = pd.concat(m_w_data)

    return output_data


# loading exchange rate data
ex_rate_df = pd.read_excel(r".\data\統計用美元換算率.xls", header=1)
ex_rate_list = ex_rate_df.出口.tolist()
ex_rate_list


def txt_monthly_data_process(txt_data, exchange_rate):
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


# 彙整2019 to 2021年資料
txt_df = []
for i_path, ex_i in zip(txt_file_list, ex_rate_list):
    datatime_str = re.findall(r'\d+', i_path)
    tmp = process_mof_txt_file(i_path)
    tmp = txt_monthly_data_process(tmp, ex_i)
    txt_df.append(tmp)
    print("{} finished".format(datatime_str[0]))

txt_df = pd.concat(txt_df)
txt_df.head()


# loading country_data
cty_file = glob.glob(".\data\API區域_國家對應表.xlsx")
cty_df = pd.read_excel(cty_file[0], sheet_name="國家對應", usecols=[
                       "國家中文名稱", "國碼"], na_filter=False)
colname = {"國家中文名稱": "cty_zen", "國碼": "country"}
cty_df = cty_df.rename(colname, axis="columns")
cty_df.head()


#cty_df.loc[cty_df['country'] == 'NA', :]


txt_df = txt_df.merge(cty_df, left_on="country",
                      right_on="country", how="left")
used_cols = ['hscode', 'cty_zen', 'value_mon_usd', 'year', 'month']
txt_df = txt_df.loc[:, used_cols]
mof_data = txt_df.rename(columns={'hscode': "Hscode", 'cty_zen': "Country",
                                  'value_mon_usd': "Value", 'year': "year", 'month': "month"})
mof_data.head()


def loading_IndutryData_from_excel_file(filePath, target_file, usedcols_list):
    f_path = find_file_path_from_asigned_doc_by_glob(filePath, target_file)
    print(f_path)
    # LOADING INDUSTRY EXCEL FILE
    ind_file = pd.read_excel(f_path[0],
                             usecols=usedcols_list,
                             dtype="str")
    return ind_file


def transfer_IndustryData_to_Dict(ind_df):
    definelst = {i: '|'.join(["^"+j for j in ind_df.hscode.tolist()[ix].split(',')])
                 for ix, i in enumerate(ind_df.reports_version_2_ind_name.tolist())}
    return definelst


def remove_duplicated_hscode(hscode_def_txt):
    tmp = hscode_def_txt.split(",")
    tmp = sorted(list(set(tmp)))
    output = ",".join(tmp)
    return output


filePath = r".\data"
target_file = "*(市場處)*.xlsx"
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
ind_define_dict


# prepare data, containing mof value and Industry Data
def convert_indRawData(data, indDefinelst):
    L = []
    for ix, i in tqdm(indDefinelst.items()):
        tmp = data[data.Hscode.str.contains(i, regex=True)].copy()
        tmp['Industry'] = str(ix)
        L.append(tmp)

    new_data = pd.concat(L)

    return new_data


raw_industry = convert_indRawData(mof_data, ind_define_dict)
raw_industry.head()


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


def filterCty(raw_data, target_cty_list):
    output = raw_data.loc[raw_data.Country.isin(target_cty_list), :]
    return output


# 標註檔案產出時間
def get_date():
    time = '%0.2d%0.2d%0.2d%0.2d%0.2d' % (datetime.now().year, datetime.now(
    ).month, datetime.now().day, datetime.now().hour, datetime.now().minute)
    return time


def outputToExcel(df, file_name, sh_name, table_header):
    writer = pd.ExcelWriter(file_name)
    df.to_excel(writer, sheet_name=sh_name, startrow=1, index=False)
    worksheet = writer.sheets[sh_name]
    worksheet.write_string(0, 0, table_header)
    raw_num = df.shape[0]+2
    worksheet.write_string(raw_num, 0, "資料來源：財政部關務署、全球貿易大數據平台(iTrade)")

    writer.save()
    print("{} output_complete".format(file_name))


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
    output_file_path = r".\output_file\各產業最新數據(市場處)_{}_{}.xlsx".format(
        area_name, time)
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


target_market_dict = {
    '全球': "",
    '東協': ["新加坡", "汶萊", "馬來西亞", "泰國", "菲律賓", "印尼", "越南", "寮國", "緬甸", "柬埔寨"],
    '東協5國': ["菲律賓", "越南", "印尼", "馬來西亞", "泰國"],
    '印尼': ['印尼'],
    '印度': ['印度'],
    '泰國': ['泰國'],
    '馬來西亞': ['馬來西亞'],
    '菲律賓': ['菲律賓'],
    '越南': ['越南']
}


# setting
latest_year = str(sys.argv[1])  # 參數1：year 2022
latest_month = str(sys.argv[2])  # 參數2：month
industry_list = ind_file.reports_version_2_ind_name.tolist()

aggr_df = []
for target_market_name, target_market_list in target_market_dict.items():
    print("======= {} processing  ======".format(target_market_name))
    tmp = main_process_outputFile_and_outputDf(
        raw_industry, industry_list, target_market_name, target_market_list, latest_year, latest_month)
    aggr_df.append(tmp)
