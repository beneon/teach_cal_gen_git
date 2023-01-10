import numpy as np
import pandas as pd
import os
import re
from datetime import date, datetime
from yaml import safe_load

with open('config.yml',encoding='utf8') as _yaml_file:
    _config = safe_load(_yaml_file)
semester_date_str = _config['semester_date_str']
xl_file_name = _config['xl_file_name']
kouqiangxi_teacher_list = _config['kouqiangxi_teacher_list']
weeks_yaml_file_name = _config['weeks_yaml_file_name']
# alert logger现在放在Cleaner 类的外面了，但是还是要在Cleaner里面去引用这货……
alert_logger = {'授课时间与放假冲突警告':[],'年级班级信息特殊格式':[],'课程信息特殊格式':[]}
# 读取课程表文件，将各个sheet按照sheetname转换成若干个df的dict
xl_path = os.path.join('..',
                       'datafile',
                       semester_date_str,
                       xl_file_name
                       )

xlFile = pd.ExcelFile(xl_path)
xl_df_col = xlFile.sheet_names
xl_df_col = [
    {'df_name':e, 'df':xlFile.parse(e, skiprows=2, usecols=[0,1], header=None, names=['wday','etc'])} for e in xl_df_col
]

xl_df_col = {
    e['df_name']:e['df'] for e in xl_df_col
}

# 表格第二行包括了班级名称和学生数量，这个在开始的时候被skip了，现在从中提取信息，形成
xl_df_aux_col = xlFile.sheet_names
xl_df_aux_col = [
{'df_name':e, 'df':xlFile.parse(e, skiprows=1, nrows=1, header=None)} for e in xl_df_aux_col
]
xl_df_aux_col = {
e['df_name']:e['df'].iloc[0,1] for e in xl_df_aux_col
}
re_xl_df_aux_col = re.compile(r'([^人]{3,}?)[（(](\d+)人[）)]')
# 这里定义的regex是假定所有的班级人员信息都是按照：班级名称（xx人）的形式填写的，如果格式不对就会短路返回，同时输出错误信息，但是不会跳出
def df_aux_data_extract(e:str) -> dict:
    mo = re_xl_df_aux_col.match(e)
    if mo is None:
        alert_logger['年级班级信息特殊格式'].append(f'{e}无法提取人员数据')
        return  {
        'classes':e,
        'student_num':[0],
    }
    all_found = re.findall(re_xl_df_aux_col,e)
    unzipped = list(zip(*all_found))
    rst = {
        'classes':[e.strip() for e in unzipped[0]],
        'student_num':unzipped[1]
    }

    return rst


xl_df_aux_col = {key:df_aux_data_extract(val) for key, val in xl_df_aux_col.items()}


class Cleaner:
    weeks_yaml_path = os.path.join('..','datafile',semester_date_str,weeks_yaml_file_name)
    # 关键设置，使用前必须要调整，指向xxx_weeks.yaml文件
    alert_logger = alert_logger
    # 对外部代码的alert_logger有依赖，迁移的时候请注意
    with open(weeks_yaml_path, encoding='utf8') as weeks_yaml:
        weeks_yaml_data = safe_load(weeks_yaml)
    semester_day1 = weeks_yaml_data['1st_day']
    semester_day1 = datetime.combine(semester_day1, datetime.min.time())
    date_adjustments = weeks_yaml_data['date_adjustments']
    date_adjustments = [e['adjustment'] for e in date_adjustments]
    date_adjustments = pd.DataFrame(date_adjustments).set_index('from')
    date_adjustments['to'] = date_adjustments['to'].apply(
        lambda e: np.nan if e == 'None' else e
    )



    # 这里主要放置各种regex
    re_etc = re.compile(r"""
    ([^\s]*?)\s+ #1. 课程负责人员
    ([^\s]*?)\s+ #2. 课程名称
    [（(]([^\s]*?)周[)）]\s+ #3. 周数
    ([^\s]*?\s|[^\s]*?$) #4. 教室
    """,re.VERBOSE)
    # 上面的regex停用，现在用姓名，周次和节次三个节点，配合位置信息去提取数据
    re_etc_name = re.compile("[\u4e00-\u9fa5,，]+")
    re_etc_week = re.compile("[（(]第?([^\s第周（）\(\)]*?)周[)）]")
    re_etc_jieci = re.compile(r"""
    节次[:：]\s?(\d*)
    """,re.VERBOSE)
    re_jieshu = re.compile(r"\d{2}")
    re_comma = re.compile(r'[,，、]')
    # 以及config dict，这些dict应该用yaml载入也行
    wday_num_dict = {'日': 0, '一': 1, '二': 2, '三': 3, '四': 4, '五': 5, '六': 6, }
    jieshu_time_dict = {
        '01': ('08:00:00', '08:40:00'),
        '02': ('08:45:00', '09:25:00'),
        '03': ('09:40:00', '10:20:00'),
        '04': ('10:25:00', '11:05:00'),
        '05': ('11:10:00', '11:50:00'),
        '06': ('13:30:00', '14:10:00'),
        '07': ('14:15:00', '14:55:00'),
        '08': ('15:10:00', '15:50:00'),
        '09': ('15:55:00', '16:35:00'),
        '10': ('16:40:00', '17:20:00'),
        '11': ('18:30:00', '19:10:00'),
        '12': ('19:15:00', '19:55:00'),
        '13': ('20:05:00', '20:45:00'),
        '14': ('20:50:00', '21:30:00'),
    }
    etc_wrong_linebreak_crit = 4


# 接下来先针对第一个df进行处理，然后推广到其他的df
class DataFrameCleaner(Cleaner):
    def __init__(self,df:pd.DataFrame, aux_data:dict):
        self.raw_df = df
        self.aux_data = aux_data
        # aux_data的classes和student_num要放到表格里面
        self.wday_colname = df.columns[0]
        self.etc_colname = df.columns[1]
        self.raw_df = self.raw_df.set_index(self.wday_colname ).dropna()
        self.memo = self.raw_df.loc['备注',self.etc_colname]
        self.raw_df = self.raw_df.drop(labels='备注')
        self.raw_df = self.raw_df.reset_index()
        # some initial cleaning above:
        # 1. get the aux data
        # 2. write memo data in 备注行，then remove 备注行
        # 3. dropna in etc column
        self.rst_df = pd.DataFrame()
        # 处理wday列
        # 1. 星期几转换成数值
        self.rst_df[['星期几','节数']] = self.wday_split()
        self.rst_df['星期几'] = self.rst_df['星期几'].map(self.wday_num_dict)
        # 2. 节数转换成开始时间和结束时间
        df_temp = pd.DataFrame(list(self.rst_df['节数'].apply(self.jieshu_split)))
        self.rst_df['block_start'] = df_temp['block_start']
        self.rst_df['block_end'] = df_temp['block_end']
        # self.rst_df = self.rst_df.drop(columns='节数')
        # ---处理etc列---
        self.rst_df['etc'] = self.raw_df['etc']
        # 1. 利用\n将etc列拆分成多列，然后用melt整理成长表格
        index_2keep = self.rst_df.columns.drop('etc').to_list()
        self.rst_df = self.rst_df.set_index(index_2keep)
        self.rst_df = self.rst_df['etc'].str.split('\n|\s{3,}',expand=True).reset_index()
        self.rst_df['ori_ind'] = self.rst_df.index
        self.rst_df = self.rst_df.melt(id_vars=index_2keep+['ori_ind'],value_name='etc',var_name='seq')
        self.rst_df['etc'] = self.rst_df['etc'].str.strip()
        self.rst_df.set_index(['ori_ind','seq'],inplace=True)
        # 去除etc为None的行
        self.rst_df.dropna(inplace=True)
        # 有些多余的\n，下面的代码进一步处理这个问题
        self.rst_df['etc']=self.rst_df.apply(lambda row: self.etc_line_merge(row, self.rst_df['etc']),axis=1)
        # 去除etc为None的行
        self.rst_df = self.rst_df.dropna().reset_index()
        # 2. 从各列提取数据
        temp_df = self.rst_df.apply(lambda row:self.etc_data_extract(row),axis=1)
    #                     'fuze':None,
    #                 'name':None,
    #                 'weeknums':None,
    #                 'loc':None,
    #                 'jieci':None,
    #                 'memo':etc,
        self.rst_df[['负责人','课程名称','周数','地点','节次','备注']] = temp_df[[
            'fuze',
            'name',
            'weeknums',
            'loc',
            'jieci',
            'memo',
        ]]
        self.rst_df.dropna(inplace=True)
        self.rst_df.drop(columns=['etc',],inplace=True)
        # 3. 周次数据转化
        self.rst_df['周数'] = self.rst_df['周数'].apply(lambda e:self.zhoushu_conv(e))
        index_2keep = self.rst_df.columns.drop('周数').to_list()
        self.rst_df = self.rst_df.set_index(index_2keep)
        self.rst_df = self.rst_df['周数'].str.split(',',expand=True).reset_index()
        self.rst_df = self.rst_df.melt(id_vars=index_2keep, value_name='周数',var_name='dropthis').drop(columns='dropthis')
        self.rst_df.dropna(inplace=True)
        self.rst_df[['开始时间','结束时间','假期调休']] = self.rst_df.apply(lambda row:self.time_calulation(row),axis=1)
        self.rst_df.drop(columns=['block_start','block_end','ori_ind','seq'],inplace=True)
        self.rst_df = self.rst_df.sort_values(['课程名称','开始时间'])
        self.rst_df['班级'] = ', '.join(self.aux_data['classes'])
        student_nums = [int(e) for e in self.aux_data['student_num']]
        student_num_sum = np.sum(student_nums)
        self.rst_df['学生数'] = student_num_sum

    def wday_split(self):
        srs = self.raw_df[self.wday_colname]
        assert srs.str.match(pat=r'.*/').sum() == srs.shape[0], f"原始表格第一列部分区域不包含分隔符，包含行{srs.str.match(pat=r'.*/').sum()}, 全体行数{srs.shape[0]}"
        return srs.str.split(pat=r'/',expand=True)

    def jieshu_split(self,e):
        # apply this function to '节数'列，生成block_start, block_end
        mo = self.re_jieshu.match(e)
        if mo is None:
            raise Exception(f"{e} can't be splitted in jieshu_split")
        all_found = self.re_jieshu.findall(e)
        block_start = self.jieshu_time_dict[all_found[0]][0]
        block_end = self.jieshu_time_dict[all_found[-1]][1]
        return {'block_start':block_start, 'block_end':block_end}

    def etc_line_merge(self,row:pd.Series,etc_srs:pd.Series):
        ori_ind, seq = row.name
        seq_next = seq+1
        next_etc_in_line = etc_srs.iloc[(etc_srs.index.get_level_values('ori_ind')==ori_ind) & (etc_srs.index.get_level_values('seq')==seq_next)]
        # snoop.pp(ori_ind,seq,seq_next,len(next_etc_in_line),row,row['etc'])
        #当前元素是否过短？
        if len(row['etc']) <= self.etc_wrong_linebreak_crit:
            return None
        elif len(next_etc_in_line) == 0:
            return row['etc']
        else:
            etc_next = next_etc_in_line.values[0]
            if len(etc_next) <= self.etc_wrong_linebreak_crit:
                row['etc'] = row['etc']+" "+etc_next
                return row['etc']
            else:
                return row['etc']

    def etc_data_extract_alert_logger(self,etc,type_str):
        self.alert_logger['课程信息特殊格式'].append(f"{etc}不能匹配正则:不匹配正则类型{type_str}")
        return pd.Series({
                'fuze':None,
                'name':None,
                'weeknums':None,
                'loc':None,
                'jieci':None,
                'memo':etc,
            })
    def etc_data_extract(self,row):
        # print('etc_data_extract entered')
        rst = {}
        etc = row['etc']
        mo_name = self.re_etc_name.match(etc)
        mo_week = self.re_etc_week.search(etc)
        mo_jieci = self.re_etc_jieci.search(etc)
        if mo_name is None:
            return self.etc_data_extract_alert_logger(etc,'name')
        if mo_week is None:
            return self.etc_data_extract_alert_logger(etc,'week')
        if mo_jieci is None:
            return self.etc_data_extract_alert_logger(etc,'jieci')
        #三点定位
        #节次信息
        rst['jieci'] = mo_jieci.group(1)
        #节次以后的数据纳入memo
        rst['memo']=etc[mo_jieci.end():]
        #name match到的内容作为fuze
        rst['fuze'] = mo_name.group(0)
        #week match到的内容group（1）作为weeknums
        rst['weeknums']=mo_week.group(1)
        #负责人到周次之间的信息都是课程名称
        rst['name'] = etc[mo_name.end():mo_week.start()]
        #周次与节次之间的信息都是授课地点
        rst['loc'] = etc[mo_week.end():mo_jieci.start()]
        #
        # mo = self.re_etc.match(etc)
        #
        # if mo is None:
        #      self.alert_logger['课程信息特殊格式'].append(f"{row['etc']}不能匹配正则")
        #
        # else:
        #     # do something with mo.groups
        #     # 1. 课程负责人员
        #     rst['fuze'] = mo.group(1)
        #     # 2. 课程名称
        #     rst['name'] = mo.group(2)
        #     # 3. 周数
        #     rst['weeknums'] = mo.group(3)
        #     # 4. 教室
        #     rst['loc'] = mo.group(4)
        #     # extract jieci
        #     etc_rest = etc[mo.span()[1]:]
        #     mo_jieci = self.re_etc_jieci.search(etc_rest)
        #     if mo_jieci is None:
        #         rst['jieci'] = ""
        #         # snoop.pp(etc_rest,row)
        #     else:
        #         rst['jieci'] = mo_jieci.group(1)
        #     rst['memo'] = etc
        return pd.Series(rst)

    def zhoushu_conv(self, w):
        # snoop.pp(w)
        # 逗号分隔，然后还有range分隔
        w_grps = self.re_comma.split(w)
        rst = []
        for w in w_grps:
            rst = rst + self.range2list(w)
        return ','.join(rst)

    def range2list(self,w):
        if '-' in w:
            endpoints = [int(d) for d in w.split('-')]
            rst_list = [str(d) for d in list(range(endpoints[0],endpoints[1]+1))]
        else:
            rst_list = [w]
        return rst_list

    def time_calulation(self,row):
        rst = {'开始时间':None, '结束时间':None, '假期调休':'无'}
        # 默认情况下，假期调休为无，date_intermediate在设置好以后也不需要再变动
        date_intermediate = self.semester_day1 + pd.Timedelta(
            # 注意周数以1开始，所以这里要减1
            weeks=int(row['周数'])-1,
            days=int(row['星期几']),
        )
        # 首先设置一个date_intermediate, 开始时间还是0点0分，结束时间也没有设置。
        # 等这里做完跳转判定以后才设置开始以及结束时间
        need_date_adjustment = self.date_adjustments.index.isin([date_intermediate.date()]).sum()
        if need_date_adjustment>0:
            date_to = self.date_adjustments.loc[date_intermediate.date(),'to']
            vacation_name = self.date_adjustments.loc[date_intermediate.date(),'vacation_name']
            if pd.isna(date_to):
                self.alert_logger['授课时间与放假冲突警告'].append(f"{date_intermediate.date()}{row['负责人']}的{row['课程名称']}时间与{vacation_name}假期冲突")
                rst['假期调休'] = f'冲突！'
            else:
                # print(f"原先排课在{date_intermediate.date()}的{row['负责人']}的{row['课程名称']}将按照假期调休调整到{date_to}")
                temp_time = date_intermediate.time()
                date_intermediate = datetime.combine(date_to,temp_time)
                rst['假期调休'] = f'调休'
        rst['开始时间'] = date_intermediate + pd.Timedelta(row['block_start'])
        rst['结束时间'] = date_intermediate + pd.Timedelta(row['block_end'])
        return pd.Series(rst)



# dfc = DataFrameCleaner(xl_df_col['16口腔'],xl_df_aux_col['16口腔'])
xl_df_processed_col = {}

for k in xl_df_col.keys():
    xl_df_processed_col[k] = DataFrameCleaner(xl_df_col[k],xl_df_aux_col[k])

# 到了上面这里，xl_df_processed_col是一个集合，原来excel各个工作表都交给DataFrameCleaner，由这个col综合
# 下面的代码要做一下修正，让pandas可以合成一个大表
# TODO：这个df_all_in_one还没有派上用场
list_all_in_one = [e.rst_df for e in xl_df_processed_col.values()]
df_all_in_one = pd.concat(list_all_in_one)


# 原先的这些输出代码还是应该保留，因为memo也是有参考价值的
output_target_path = os.path.join('..','output',semester_date_str)
if not os.path.exists(output_target_path):
    os.mkdir(output_target_path)
xl_writer = pd.ExcelWriter(os.path.join(
    '..',
    'output',
    semester_date_str,
    '整理后excel.xlsx'
), engine='xlsxwriter')
for k,v in xl_df_processed_col.items():
    v.rst_df.to_excel(xl_writer,sheet_name=k,index=False)
    memo = v.memo
    memo_pos_x = chr(ord('A')+(v.rst_df.shape[1]+1)-1)
    memo_tbox_options = {
        'width':300,
        'height':200,
    }
    xl_writer.sheets[k].set_column('G:H',20)
    xl_writer.sheets[k].set_column('B:B',20)
    xl_writer.sheets[k].set_column('E:E',20)
    xl_writer.sheets[k].insert_textbox(f'{memo_pos_x}2', memo, memo_tbox_options)



xl_writer.save()
# 所有sheet都能通过initial cleaning

# 输出负责人中包含我自己的课程
def generate_teacher_specific_xlsx(teacher_name='郑骏明'):
    xl_writer = pd.ExcelWriter(
        os.path.join(
            '..',
            'output',
            semester_date_str,
            f'{teacher_name}负责课程.xlsx'
        ), engine='xlsxwriter')
    for k,v in xl_df_processed_col.items():
        df = v.rst_df
        df = df[df['负责人'].str.find(teacher_name)>=0]
        if df.shape[0]==0:
            continue
        df.to_excel(xl_writer,sheet_name=k,index=False)
        memo = v.memo
        memo_pos_x = chr(ord('A')+(v.rst_df.shape[1]+1)-1)
        memo_tbox_options = {
            'width':300,
            'height':200,
        }
        xl_writer.sheets[k].set_column('G:H',20)
        xl_writer.sheets[k].set_column('B:B',20)
        xl_writer.sheets[k].set_column('E:E',20)
        xl_writer.sheets[k].insert_textbox(f'{memo_pos_x}2', memo, memo_tbox_options)

    xl_writer.save()

for teacher in kouqiangxi_teacher_list:
    generate_teacher_specific_xlsx(teacher)

# 输出alert_logger内容，并将其中的内容存储为log_{date of today}.txt
with open(os.path.join('..','output',semester_date_str,f'log_{date.today()}.txt'),'w') as logger_file:
    for alert_name,alert_list in alert_logger.items():
        logger_file.write(f"# {alert_name}\n")
        logger_file.writelines([f"{e}\n" for e in alert_list])