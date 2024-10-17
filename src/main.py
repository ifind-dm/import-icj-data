import os
import re
import base64
import json
from modules.bigquery_worker import *
from modules.gcs_worker import *
from modules.schema import *
import pandas as pd
from datetime import datetime, timedelta
import pytz
import functions_framework

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

def parse_datetime(date_str):
    """
    様々な日付時刻フォーマットを解析する関数
    """
    formats = [
        "%Y-%m-%d %H:%M:%S.%f %z",  # ISO 8601 形式（マイクロ秒とタイムゾーン付き）
        "%Y-%m-%d %H:%M:%S.%f",     # マイクロ秒付き
        "%Y-%m-%d %H:%M:%S %z",     # タイムゾーン付き
        "%Y-%m-%d %H:%M:%S",        # 基本形式
        "%Y-%m-%d",                 # 日付のみ
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    
    try:
        # ISO 8601 形式を自動解析
        return pd.to_datetime(date_str, format='ISO8601')
    except ValueError:
        # 全ての解析が失敗した場合
        return None


def process_vma0060(file_path, bucket_name, file_name):
    gcs.download_blob(bucket_name, file_name, file_path)
    df = pd.read_csv(file_path, sep='\t', encoding='utf-8', names=vma0060_cols)
    df['DT'] = yesterday_date
    tbl_full = f"{project_id}.{dataset_id}.{vma0060_table_name}"
    bq.insert_with_client(df, schema_vma0060, tbl_full, 'DT')

def process_tms010(file_path, bucket_name, file_name):
    gcs.download_blob(bucket_name, file_name, file_path)
    df = pd.read_csv(file_path, sep='\t', encoding='utf-8', names=tms010_cols)
    try:
        df['UPD_DATE'] = df['UPD'].apply(parse_datetime).dt.date
    except Exception as e:
        df['UPD_DATE'] = yesterday_date
        print(e)
    
    tbl_full = f"{project_id}.{dataset_id}.{tms010_table_name}"
    bq.insert_with_client(df, schema_tms010, tbl_full, 'UPD_DATE')


def process_tms060(file_path, bucket_name, file_name):
    gcs.download_blob(bucket_name, file_name, file_path)
    df = pd.read_csv(file_path, sep='\t', encoding='utf-8', names=tms060_cols)
    try:
        df['UPD_DATE'] = df['REF_DL'].apply(parse_datetime).dt.date
    except Exception as e:
        df['UPD_DATE'] = yesterday_date
        print(e)
    tbl_full = f"{project_id}.{dataset_id}.{tms060_table_name}"
    bq.insert_with_client(df, schema_tms060, tbl_full, 'UPD_DATE')

def gcs_to_bq(bucket_name, file_name):
    """Process the GCS file and insert data into BigQuery."""
    print(f"Processing file: gs://{bucket_name}/{file_name}")

    # ローカルpath
    temp_file_path = f"/tmp/{os.path.basename(file_name)}"
    
    # ファイル名に基づいて適切な処理を選択
    if 'vma0060' in file_name:
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
@functions_framework.cloud_event
def main(cloud_event):
    """Cloud Function triggered by a Cloud Pub/Sub event."""
    # Pub/Sub メッセージのデータを取得
    pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode("utf-8")
    message_data = json.loads(pubsub_message)

    # ストレージイベントの詳細を取得
    bucket = message_data["bucket"]
    name = message_data["name"]

    # GCS to BigQuery 処理を呼び出す
    gcs_to_bq(bucket, name)