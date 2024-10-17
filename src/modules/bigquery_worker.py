import sys
import time
import traceback
from datetime import timedelta
from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import google.auth
from google.oauth2 import service_account
from os import getenv
import logging
# from .modules.utils import date2str, str2date

class BigQueryWorker:
    def __init__(self):
        self.sql_root_path = "../query/"
        self.credentials, self.project_name = google.auth.default()
        self.client = bigquery.Client(project=self.project_name, credentials=self.credentials)
    #     self.credentials = service_account.Credentials.from_service_account_file(
    # "../icj-recommend-dev-88df20cf0dd1.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],)
    #     self.client = bigquery.Client(project=self.credentials.project_id, credentials=self.credentials)
    
    @staticmethod
    def get_sql_query(file_path, format_dct={}):
        """ Get the SQL query of the target file and
            set the "date" defined by the argument to the query.
        """
        with open(file_path, "r") as file:
            query_str = file.read()
            sql_query = query_str.format(**format_dct)
        return sql_query
    
    def load(self, query, retry_num=3):
        """ Load data from BigQuery and
            return the result by DataFrame format.
        """
        logging.info("Load From BigQuery...")

        result = self.client.query(query).result()
        return result.to_dataframe()
    
    def query(self, query):
        """ Load data from BigQuery and
            return the result by iterable format.
        """
        logging.info("Load From BigQuery...")
        query_job = self.client.query(query)
        return query_job
    
    def insert_with_sql(self, query, full_table_id):
        self.client.query(query).result()

    
    def insert_with_client(self, df, schema_dic, full_table_id, time_partition_key=None):
        schema = []
        for field_name, field_setting in schema_dic.items():
            if field_setting[0] == "STRING":
                schema.append(bigquery.SchemaField(field_name, bigquery.enums.SqlTypeNames.STRING, mode=field_setting[1]))
            elif field_setting[0] == "FLOAT64":
                schema.append(bigquery.SchemaField(field_name, bigquery.enums.SqlTypeNames.FLOAT64, mode=field_setting[1]))
            elif field_setting[0] == "INT64":
                schema.append(bigquery.SchemaField(field_name, bigquery.enums.SqlTypeNames.INT64, mode=field_setting[1]))
            elif field_setting[0] == "DATE":
                schema.append(bigquery.SchemaField(field_name, bigquery.enums.SqlTypeNames.DATE, mode=field_setting[1]))
            elif field_setting[0] == "DATETIME":
                schema.append(bigquery.SchemaField(field_name, bigquery.enums.SqlTypeNames.DATETIME, mode=field_setting[1]))
        if time_partition_key is None:
            job_config = bigquery.LoadJobConfig(schema=schema,
                                            autodetect=False,
                                            allow_quoted_newlines=True,
                                            write_disposition="WRITE_APPEND",
                                            source_format=bigquery.SourceFormat.CSV)
        else:
            job_config = bigquery.LoadJobConfig(schema=schema,
                                            autodetect=False,
                                            allow_quoted_newlines=True,
                                            write_disposition="WRITE_APPEND",
                                            source_format=bigquery.SourceFormat.CSV,
                                            time_partitioning=bigquery.TimePartitioning(
                                            type_=bigquery.TimePartitioningType.DAY,  # パーティションのタイプを指定
                                            field=time_partition_key  # パーティションキーとなるカラムを指定
                                            )
                                            )
        try:
            self.client.load_table_from_dataframe(df, destination=full_table_id, job_config=job_config).result()
        except Exception as e:
            print('Error:', e)
            logging.info(f"Error: Not insert data (Table={full_table_id})")
            sys.exit(1)