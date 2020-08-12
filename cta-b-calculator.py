from pathlib import *
from datetime import datetime as dt 

from dateutil.relativedelta import relativedelta
from datetime import date

import pandas as pd
import numpy as np
from typing import *

from lazytools.data.db import Query

conn = Query('ch_prd_reports',connection_engine='httphouse',verbose=True)

class ScannerOPKC():
    
    def __init__(self, import_path, old_files_path):
        
        self.import_path = import_path
        self.old_files_path = old_files_path
        
    def scanner(self) -> List:

        old_files = self.old_files_path.read_text().split()
        new_files = list(self.import_path.glob('CALC-*.json'))

        upload_files = []

        for i in new_files:
            if i.stem not in old_files:
                upload_files.append(i)
                # Добавляем имя нового файла в базу  
                with open(self.old_files_path, 'a') as file:
                    file.write(i.stem + ' ')
        
        return upload_files

transport = ScannerOPKC(Path('O:\Diode\DiodeOut\scta'), Path('P:\calculator_opkc\code\old_files.yml'))
scanner_result = transport.scanner()

class Upload():

    def __init__(self, json_file, yml_path):

        self.json_file = json_file
        self.yml_path = yml_path

    def uploading(self):

        data= pd.read_json(str(self.json_file))

        def make_temp_table(name: str, data: pd.DataFrame, dtypes=None, schema='tasks'):

            if not dtypes:
                dtypes = ['text' for i in range(len(data.columns))]
                
            columns = ', '.join([f'{col} {dtype}' for col, dtype in zip(data.columns, dtypes)])
            create_stmt = f'CREATE TABLE {schema}.{name} ({columns})'
            
            sql = [
                f'DROP TABLE IF EXISTS {schema}.{name}',
                create_stmt,
                f'GRANT ALL ON TABLE {schema}.{name} TO tech_dict_clickhouse'
            ]
            
            rds = Query('pg_uat_rds_rds')
            rds.select(sql)[0]
            rds.insert(data, schema, name, tuple(data.columns))

        GROUP_BY = data['group_by'][1]
        mid_tid_table = pd.DataFrame(data['attachment'][1]).T
        mid_tid_table.columns = ['MERCHANT_ID', 'TERMINAL_ID']
        make_temp_table('b_calculator', mid_tid_table)

        def mechanics(data):
            for x in data[['purchase_min', 'purchase_max', 'max_total_purchase', 'purchase_order']]:
                if data[x][0] != 0 and data[x][0] != 'Any':
                    return data[x][0], x
                
        ACTION = str(list(mechanics(data))[0])
        NAME_ACTION = list(mechanics(data))[1]

        # Расчет MCC

        sql_mcc = open(self.yml_path.joinpath('mcc_calculation.yml'), 'r').read()
        result_mcc = conn.select(sql_mcc)[0]

        MCC = str(result_mcc.loc[:,'MCC'].tolist()).replace('[', '(').replace(']', ')')

        # #### Поиск MCC, в котором Партнер имеет не менее 10% всего оборота

        sql_mcc_more_10 = open(self.yml_path.joinpath('mcc_more_10.yml'), 'r').read().replace('{MCC}', MCC)
        result_mcc_more_10 = conn.select(sql_mcc_more_10)[0]

        result_mcc_more_10['RATIO'] = result_mcc_more_10['SUM'] / result_mcc_more_10['SUM'].sum()
        result_mcc_more_10 = result_mcc_more_10.where(result_mcc_more_10['RATIO'] > 0.1).dropna()

        MCC = str(result_mcc_more_10.loc[:,'MCC'].tolist()).replace('[', '(').replace(']', ')')

        # #### Поиск Региона

        sql_region_rus = open(self.yml_path.joinpath('region_rus.yml'), 'r').read().replace('{MCC}', MCC)
        result_region_rus = conn.select(sql_region_rus)[0]

        result_region_rus = result_region_rus.where(result_region_rus['REGION_RUS'] != 'НЕОПРЕДЕЛЕН').dropna()
        REGION_RUS = str(result_region_rus.loc[:,'REGION_RUS'].tolist()).replace('[', '(').replace(']', ')')

        if GROUP_BY == 'TRAN_MONTH':
            
            dates = []
            start_date = date(2020,6, 1)

            while start_date < date(2020,6,13):
                tmp = (
                    start_date.strftime('%Y%m%d'),
                    (start_date + relativedelta(months=1, days=9)).strftime('%Y%m%d'),
                    (start_date + relativedelta(months=1) - relativedelta(days=1)).strftime('%Y%m%d')
                )

                dates.append(tmp)
                start_date += relativedelta(months=1)

        elif GROUP_BY == 'TRAN_WEEK':
            
            dates= []
            start_date = date(2020,5, 1)

            while start_date < date(2020,6,1):
                tmp = (
                    start_date.strftime('%Y%m%d'),
                    (start_date + relativedelta(weeks=1, days=5)).strftime('%Y%m%d'),
                    (start_date + relativedelta(weeks=1) - relativedelta(days=1)).strftime('%Y%m%d')
                )

                dates.append(tmp)
                start_date += relativedelta(weeks=1)

        # ### Шаблоны

        pattern_1 = open(self.yml_path.joinpath('pattern_1.yml'), 'r', encoding='utf-8').read().replace('{GROUP_BY}', GROUP_BY)

        pattern_2 = open(self.yml_path.joinpath('pattern_2.yml'), 'r', encoding='utf-8').read().replace('{GROUP_BY}', GROUP_BY)

        pattern_3 = open(self.yml_path.joinpath('pattern_3.yml'), 'r', encoding='utf-8').read().replace('{GROUP_BY}', GROUP_BY).replace('{MCC}', MCC).replace('{REGION_RUS}', REGION_RUS)

        # #### Расчет данных бизнес кейса

        if NAME_ACTION == 'purchase_min':

            sql =  open(self.yml_path.joinpath('min_sum_tran.yml'), 'r', encoding='utf-8').read().format(var = [pattern_1, ACTION, pattern_2, pattern_3 ] )
            table = pd.DataFrame()

            for x, y, z in dates:
                result = conn.select(sql.replace('{x}',str(x)).replace('{y}',str(y)).replace('{z}',str(z)))[0]

                table = table.append(result)    

            
        if NAME_ACTION == 'purchase_max':

            sql = open(self.yml_path.joinpath('max_sum_tran.yml'), 'r', encoding='utf-8').read().format(var = [pattern_1, ACTION, pattern_2, pattern_3 ] )
            table = pd.DataFrame()

            for x, y, z in dates:
                result = conn.select(sql.replace('{x}',str(x)).replace('{y}',str(y)).replace('{z}',str(z)))[0]

                table = table.append(result) 
                
        if NAME_ACTION == 'max_total_purchase':
                        
            sql = open(self.yml_path.joinpath('max_sum_total.yml'), 'r', encoding='utf-8').read().format(var = [pattern_1, ACTION, pattern_2, pattern_3 ] )
            table = pd.DataFrame()

            for x, y, z in dates:
                result = conn.select(sql.replace('{x}',str(x)).replace('{y}',str(y)).replace('{z}',str(z)))[0]

                table = table.append(result)
                
        if NAME_ACTION == 'purchase_order':
                        
            if ACTION == 'Even':

                sql =  open(self.yml_path.joinpath('even_tran.yml'), 'r', encoding='utf-8').read().format(var = [pattern_1, pattern_2, pattern_3 ])
                table = pd.DataFrame()

                for x, y, z in dates:
                    result = conn.select(sql.replace('{x}',str(x)).replace('{y}',str(y)).replace('{z}',str(z)))[0]

                    table = table.append(result)

            if ACTION == 'Odd':

                sql = open(self.yml_path.joinpath('odd_tran.yml'), 'r', encoding='utf-8').read().format(var = [pattern_1, pattern_2, pattern_3 ])
                table = pd.DataFrame()

                for x, y, z in dates:
                    result = conn.select(sql.replace('{x}',str(x)).replace('{y}',str(y)).replace('{z}',str(z)))[0]

                    table = table.append(result)

        
        file_name = 'PROCESSED_' + self.json_file.stem + dt.now().strftime('%Y%m%d%H%M%S') + '.xlsx'
        xlsx_file = self.json_file.parent.parent.parent / 'DiodeIn' / 'scta' / file_name
        
        return table.to_excel(xlsx_file)

if not scanner_result:
    print('Нет новых файлов json')
else:
    for json_file in scanner_result:
        load = Upload(json_file, Path('P:\calculator_opkc\code'))
        load.uploading()