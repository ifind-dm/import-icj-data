import os
import re
import base64
import json
from modules.bigquery_worker import *
from modules.gcs_worker import *
from modules.schema import *
from modules.utils import *
from modules.slack import *
import pandas as pd
from datetime import datetime, timedelta
import pytz
import functions_framework
import logging

# 日本のタイムゾーンを設定
japan_tz = pytz.timezone('Asia/Tokyo')
    
# 現在のUTC時間を取得し、日本時間に変換
today = datetime.now(pytz.utc).astimezone(japan_tz)
yesterday = today - timedelta(days=1)
today_date = today.strftime('%Y-%m-%d')
yesterday_date = yesterday.strftime('%Y-%m-%d')

bq = BigQueryWorker()
gcs = GcsWorker()
slack_bot = Slack(bot_name='import-icj-data')

project_id = os.environ.get('PROJECT_ID')
dataset_id = os.environ.get('BQ_DATASET_ID')
bucket_name = os.environ.get('BUCKET_NAME')  
    
vma0060_cols = ['EMP_CD', 'S_CD1', 'S_NM1', 'POSTCD', 'POSTNM', 'POST_RANK', 'POST_RANKNM', 'BUMON_CD', 'BU_CD', 'LEDKBN', 'EGYKBN', 'STAFF_FLG', 'BUMON_NM', 'GET_LCD']
tms010_cols = ['IDNO', 'MKD', 'UPD', 'MSDT', 'MSID', 'JKBN', 'PLANNG', 'JCONTENTS', 'RANK', 'DELF']
tms060_cols = ['SUID', 'IDNO', 'MSID', 'MSDT', 'REF_DF', 'REF_DL', 'MK_FLG', 'SV_FLG', 'POINT', 'POINT2', 'POINT3']
tlog_cols = []

vma0060_table_name = 'iris_lv0_mst_vma0060_user_master'
tms010_table_name = 'iris_lv0_mst_tms010_journal_master'
tms060_table_name = 'iris_lv0_tra_tms060_journal_action_log'
tlog_table_name = 'iris_lv0_tra_tlog_journal_action_log'



def process_import_bq(query_path, prefix, table_name, partition_key, cols, schema):
    logging.info(f'Process import bq: {table_name}')
    tbl_full = f"{project_id}.{dataset_id}.{table_name}"
    query = bq.get_sql_query(query_path, {'target_table':tbl_full})
    try:
        df_mst = bq.load(query)
        date_list = pd.to_datetime(df_mst[partition_key]).dt.strftime('%Y%m%d').tolist()
    except Exception as e:
        print(e)
        date_list = []
    
    # gcsから対象ファイルのリストを取得
    f_dic = gcs.list_files_to_process(bucket_name, prefix, date_list)
    if len(f_dic) == 0:
        print('No files to process')
        logging.info('No files to process')
        return
    
    for f_name, f_path in f_dic.items():
        try:
            destination_file_name = './input/' + f_name
            gcs.download_blob(bucket_name, f_path, destination_file_name)
            df = pd.read_csv(destination_file_name, sep='\t', encoding='utf-8', names=cols)
            df = preprocess_data(df, f_name)
            bq.insert_with_client(df, schema, tbl_full, partition_key)
            slack_bot.slack_notify_success(f'Imported {f_name} to {table_name}', additional_info=f'Imported {len(df)} rows')
        except Exception as e:
            print(e)
            slack_bot.slack_notify_error(f'Failed to import {f_name} to {table_name}', additional_info=str(e))
            continue


def preprocess_data(df, f_name):
    if 'vma0060' in f_name:
        df['DT'] = yesterday_date
        return df
    elif 'tms010' in f_name:
        df['MKD'] = df['MKD'].apply(lambda x: parse_and_format_date(x, 'datetime'))
        df['MSDT'] = df['MSDT'].apply(lambda x: parse_and_format_date(x, 'date'))
        df['UPD_DATE'] = df['UPD'].apply(lambda x: parse_and_format_date(x, 'date'))
        df['UPD'] = df['UPD'].apply(lambda x: parse_and_format_date(x, 'datetime'))
        return df
    elif 'tms060' in f_name:
        df['MSDT'] = df['MSDT'].apply(lambda x: parse_and_format_date(x, 'date'))
        df['REF_DF'] = df['REF_DF'].apply(lambda x: parse_and_format_date(x, 'datetime'))
        df['UPD_DATE'] = df['REF_DL'].apply(lambda x: parse_and_format_date(x, 'date'))
        df['REF_DL'] = df['REF_DL'].apply(lambda x: parse_and_format_date(x, 'datetime'))
        return df
    else:
        return df

if __name__ == '__main__':
    process_import_bq('./query/load_vma0060.sql', 'vma0060/', vma0060_table_name, 'DT', vma0060_cols, schema_vma0060)
    process_import_bq('./query/load_tms010.sql', 'tms010/', tms010_table_name, 'UPD_DATE', tms010_cols, schema_tms010)
    process_import_bq('./query/load_tms060.sql', 'tms060/', tms060_table_name, 'UPD_DATE', tms060_cols, schema_tms060)