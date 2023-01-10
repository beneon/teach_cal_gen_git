import os
import numpy as np
import pandas as pd
from ics import Calendar, Event
import pytz
from datetime import date, datetime, timedelta
from yaml import safe_load

with open('config.yml',encoding='utf8') as _yaml_file:
    _config = safe_load(_yaml_file)

semester_date_str = _config['semester_date_str']
kouqiangxi_teacher_list = _config['kouqiangxi_teacher_list']
timezone_shanghai = pytz.timezone('Asia/Shanghai')


def generate_ics_file_wrapper(teacher_name):
    xl_path = os.path.join('..','output',semester_date_str,f'{teacher_name}负责课程.xlsx')
    ics_out_path = os.path.join('..','output',semester_date_str,f'{teacher_name}教学日历.ics')
    xlFile = pd.ExcelFile(xl_path)

    dfs = xlFile.sheet_names
    if dfs[0] == 'Sheet1':
        # data_cleaning输出的空白excel文件（当某教师无授课任务的时候会出现）
        print(f'{teacher_name}在{semester_date_str}学期无教学任务，请查证')
    else:
        dfs = [xlFile.parse(s) for s in dfs]
        df = pd.concat(dfs)
        c = Calendar()
        df.apply(lambda row: c.events.add(event_gen(row)), axis=1)

        with open(ics_out_path, 'w', encoding='utf-8') as f:
            f.writelines(c)


def event_gen(para_row):
    para_row.fillna('',inplace=True)
    begin_time = timezone_shanghai.localize(para_row['开始时间'])
    end_time = timezone_shanghai.localize(para_row['结束时间'])
    e = Event(
        name = para_row['课程名称'],
        begin=begin_time,
        end=end_time,
        attendees=para_row['负责人'].split(','),
        location=para_row['地点'],
        description=para_row['备注'],
    )
    return e

for teacher in kouqiangxi_teacher_list:
    generate_ics_file_wrapper(teacher)


