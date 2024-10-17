import sys
import time
import traceback
import datetime
from datetime import timedelta
from google.cloud import storage as gcs
from google.cloud.exceptions import NotFound
import google.auth
from google.oauth2 import service_account
from os import getenv
import logging
import pandas as pd
# from .utils import date2str, str2date

class GcsWorker:
    def __init__(self):
        self.sql_root_path = "../query/"
        self.credentials, self.project_name = google.auth.default()
        self.client = gcs.Client(project=self.project_name, credentials=self.credentials)
    #     self.credentials = service_account.Credentials.from_service_account_file(
    # "../icj-recommend-dev-88df20cf0dd1.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],)

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

if __name__ == "__main__":
    gcs = GcsWorker()