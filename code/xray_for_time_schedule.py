import pandas as pd
import os

semester_str = '2022-2023(1)'
excel_file_time_schedule = os.path.join('..','output',semester_str,'整理后excel.xlsx')
assert os.path.exists(excel_file_time_schedule)
excel_file_time_schedule = pd.ExcelFile(excel_file_time_schedule,engine='openpyxl')

# 读取特定班级的数据表
class_names_available = excel_file_time_schedule.sheet_names
class_name = '21口腔'
class_name2 = '19护理口腔母婴班'

def get_df_select_with_dates(class_name,start_from,end_with):
    df = excel_file_time_schedule.parse(sheet_name=class_name,parse_dates=['开始时间','结束时间'],dtype={'节数':str})
    # 找到3月份14号到18号所有课程安排
    df_select_with_dates:pd.DataFrame = df[(df['开始时间']>start_from)&(df['开始时间']<end_with)].copy()
    df_select_with_dates.sort_values('开始时间',inplace=True)
    return df_select_with_dates

def get_joined_df_with_dates_as_index(class_name1,class_name2,start_from_date,end_with_date):
    start_from_date = start_from_date+" 00:00:00"
    end_with_date = end_with_date+" 23:59:59"
    df1 = get_df_select_with_dates(class_name,start_from_date,end_with_date)
    df2 = get_df_select_with_dates(class_name2,start_from_date,end_with_date)
    df_join = df1.set_index('开始时间').join(df2.set_index('开始时间'),how='outer',lsuffix='.class1',rsuffix='.class2')
    return df_join

def get_normal_class_start_time(start_date,end_date):
    class_start_times = [
        '08:00:00',
        '09:40:00',
        '13:30:00',
        '15:10:00',
    ]
    time_series = pd.date_range(start_date,end_date,freq='B')
    start_times = list()
    for t0 in time_series:
        for time_start in class_start_times:
            start_times.append(t0+pd.to_timedelta(time_start))
    rst_df = pd.DataFrame(range(len(start_times)))
    rst_df.set_index(pd.Series(start_times),inplace=True)
    return rst_df

start_date = '2022-11-28'
end_date = '2022-12-20'
df_normal_start_time = get_normal_class_start_time(start_date,end_date)
# df_joined = get_joined_df_with_dates_as_index(class_name,class_name2,start_date,end_date)
df_joined = get_df_select_with_dates(class_name,start_date,end_date).set_index('开始时间')
df_normal_start_time_availability = df_normal_start_time.join(df_joined).drop(columns=[0])
df_normal_start_time_availability['available'] = df_normal_start_time_availability.isna().all(axis=1)
df_normal_start_time_availability['星期几'] = df_normal_start_time_availability.index.weekday+1
df_normal_start_time_availability.fillna('',inplace=True)

xlwriter = pd.ExcelWriter(engine='xlsxwriter',path=os.path.join('..','output',semester_str,'a_xray_11-28_to_12_20.xlsx'))
df_normal_start_time_availability.to_excel(excel_writer=xlwriter)
xlwriter.save()





