import sys
import time
import traceback
from datetime import timedelta, datetime
from google.cloud import storage as gcs
from google.cloud.exceptions import NotFound
import google.auth
from google.oauth2 import service_account
from os import getenv
import logging
import pandas as pd
from collections import defaultdict

# from .utils import date2str, str2date

class GcsWorker:
    def __init__(self):
        self.sql_root_path = "../query/"
        self.credentials, self.project_name = google.auth.default()
        self.client = gcs.Client(project=self.project_name, credentials=self.credentials)
    #     self.credentials = service_account.Credentials.from_service_account_file(
    # "../ifind-dm-compute-service-key.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],)

    #     self.client = gcs.Client(project=self.credentials.project_id, credentials=self.credentials)

    def upload(self, bucket_name, local_path, gcs_path):
        bucket = self.client.get_bucket(bucket_name)
        # 格納するGCSのPathを指定(/xxx/yyy.csv)的な
        blob_gcs = bucket.blob(gcs_path)
        # ローカルのファイルパスを指定
        blob_gcs.upload_from_filename(local_path)
    
    def download_blob(self, bucket_name, source_blob_name, destination_file_name):
        """Downloads a blob from the bucket."""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)

    def list_files_to_process(self, bucket_name, prefix, date_list=[],days_to_process=7):
        """
        処理すべきファイルのリストを取得し、テーブル名でグループ化する
        
        :param bucket_name: GCSバケット名
        :param days_to_process: 処理対象となる過去の日数
        :return: テーブル名をキー、ファイルリストを値とする辞書
        """
        
        blobs = self.client.list_blobs(bucket_name, prefix=prefix)

        end_date = datetime.now().date()
        start_date = end_date - timedelta(days=days_to_process)
        
        target_dic = {}
        for blob in blobs:
            # ファイルパスを分割
            path_parts = blob.name.split('/')
            if len(path_parts) == 2 and path_parts[1].endswith('.tsv'):
                table_name = path_parts[0]
                file_name = path_parts[1]
                
                # ファイル名から日付を抽出
                try:
                    file_date_str = file_name.split('_')[-1].split('.')[0]
                    file_date = datetime.strptime(file_date_str, '%Y%m%d').date()
                    
                    # 日付が指定範囲内かチェック
                    if (start_date <= file_date <= end_date )& (file_date_str not in date_list):
                        target_dic[file_name] = blob.name
                except ValueError:
                    # 日付の解析に失敗した場合はスキップ
                    print(f"Skipping file with invalid date format: {blob.name}")
        
        return target_dic

if __name__ == "__main__":
    gcs = GcsWorker()