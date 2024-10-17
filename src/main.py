import os
import re
from modules.bigquery_worker import *
from modules.gcs_worker import *
from modules.schema import *
import pandas as pd
from datetime import datetime, timedelta
import pytz

# 日本のタイムゾーンを設定
japan_tz = pytz.timezone('Asia/Tokyo')
    
# 現在のUTC時間を取得し、日本時間に変換
today = datetime.now(pytz.utc).astimezone(japan_tz)
yesterday = today - timedelta(days=1)
today_date = today.strftime('%Y-%m-%d')
yesterday_date = yesterday.strftime('%Y-%m-%d')

bq = BigQueryWorker()
gcs = GcsWorker()

project_id = os.environ.get('PROJECT_ID')
dataset_id = os.environ.get('BQ_DATASET_ID')    
    
vma0060_cols = ['EMP_CD', 'S_CD1', 'S_NM1', 'POSTCD', 'POSTNM', 'POST_RANK', 'POST_RANKNM', 'BUMON_CD', 'BU_CD', 'LEDKBN', 'EGYKBN', 'STAFF_FLG', 'BUMON_NM', 'GET_LCD']
tms010_cols = ['IDNO', 'MKD', 'UPD', 'MSDT', 'MSID', 'JKBN', 'PLANNG', 'JCONTENTS', 'RANK', 'DELF']
tms060_cols = ['SUID', 'IDNO', 'MSID', 'MSDT', 'REF_DF', 'REF_DL', 'MK_FLG', 'SV_FLG', 'POINT', 'POINT2', 'POINT3']
tlog_cols = []

vma0060_table_name = 'iris_lv0_mst_vma0060_user_master'
tms010_table_name = 'iris_lv0_mst_tms010_journal_master'
tms060_table_name = 'iris_lv0_tra_tms060_journal_action_log'
tlog_table_name = 'iris_lv0_tra_tlog_journal_action_log'

def process_vma0060(file_path, bucket_name, file_name):
    gcs.download_blob(bucket_name, file_name, file_path)
    df = pd.read_csv(file_path, sep='\t', encoding='utf-8', names=vma0060_cols)
    df['DT'] = yesterday_date
    tbl_full = f"{project_id}.{dataset_id}.{vma0060_table_name}"
    bq.insert_with_client(df, schema_vma0060, tbl_full, 'DT')

def process_tms010(file_path, bucket_name, file_name):
    gcs.download_blob(bucket_name, file_name, file_path)
    df = pd.read_csv(file_path, sep='\t', encoding='utf-8', names=tms010_cols)
    df['UPD_DATE'] = pd.to_datetime(df['UPD'], format="%Y-%m-%d %H:%M:%S.%f").dt.date
    tbl_full = f"{project_id}.{dataset_id}.{tms010_table_name}"
    bq.insert_with_client(df, schema_tms010, tbl_full, 'UPD_DATE')


def process_tms060(file_path, bucket_name, file_name):
    gcs.download_blob(bucket_name, file_name, file_path)
    df = pd.read_csv(file_path, sep='\t', encoding='utf-8', names=tms060_cols)
    df['UPD_DATE'] = pd.to_datetime(df['REF_DL'], format="%Y-%m-%d %H:%M:%S").dt.date
    tbl_full = f"{project_id}.{dataset_id}.{tms060_table_name}"
    bq.insert_with_client(df, schema_tms060, tbl_full, 'UPD_DATE')

def gcs_to_bq(event, context):
    """Background Cloud Function to be triggered by Cloud Storage."""
    file = event
    bucket_name = file['bucket']
    file_name = file['name']

    print(f"Processing file: gs://{bucket_name}/{file_name}")

    # ローカルpath
    temp_file_path = f"/tmp/{os.path.basename(file_name)}"
    
    # ファイル名に基づいて適切な処理を選択
    if 'vma060' in file_name:
        process_vma0060(temp_file_path, bucket_name, file_name)
    elif 'tms010' in file_name:
        process_tms010(temp_file_path, bucket_name, file_name)
    elif 'tms060' in file_name:
        process_tms060(temp_file_path, bucket_name, file_name)
    else:
        return

    # 一時ファイルの削除
    os.remove(temp_file_path)

    print(f"File {file_name} processed successfully.")

# Cloud Function のエントリーポイント
def main(event, context):
    gcs_to_bq(event, context)