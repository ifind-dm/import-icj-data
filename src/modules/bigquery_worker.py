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
    # "../ifind-dm-compute-service-key.json", scopes=["https://www.googleapis.com/auth/cloud-platform"],)
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
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                autodetect=False,
                allow_quoted_newlines=True,
                write_disposition="WRITE_APPEND",
                source_format=bigquery.SourceFormat.CSV
            )
            try:
                self.client.load_table_from_dataframe(df, destination=full_table_id, job_config=job_config).result()
            except Exception as e:
                print('Error:', e)
                logging.info(f"Error: Not insert data (Table={full_table_id})")
                sys.exit(1)
        else:
            # テーブル情報をパースして、project_id, dataset_id, table_idを取得
            table_ref = bigquery.TableReference.from_string(full_table_id)
            project_id = table_ref.project
            dataset_id = table_ref.dataset_id
            table_id = table_ref.table_id

            # カラムの型を確認するクエリ
            schema_query = f"""
            SELECT column_name, data_type
            FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.COLUMNS`
            WHERE table_name = '{table_id}'
            AND column_name = '{time_partition_key}'
            """
            schema_result = list(self.client.query(schema_query).result())
            if not schema_result:
                raise ValueError(f"Column {time_partition_key} not found in table {full_table_id}")
            column_type = schema_result[0]['data_type'].upper()
            
            # パーティション対象の日付を取得
            partition_dates = df[time_partition_key].unique()
            
            try:
                for partition_date in partition_dates:
                    # パーティション日付のデータのみを抽出
                    partition_df = df[df[time_partition_key] == partition_date]
                    
                    # 日付文字列の形式を統一
                    if isinstance(partition_date, str):
                        try:
                            # まず'YYYY-MM-DD'形式として解析を試みる
                            parsed_date = datetime.strptime(partition_date, '%Y-%m-%d')
                        except ValueError:
                            try:
                                # 次に'YYYYMMDD'形式として解析を試みる
                                parsed_date = datetime.strptime(partition_date, '%Y%m%d')
                            except ValueError:
                                raise ValueError(f"Unsupported date format: {partition_date}")
                        partition_date_str = parsed_date.strftime('%Y%m%d')
                        formatted_date = parsed_date.strftime('%Y-%m-%d')
                    elif isinstance(partition_date, (datetime, date)):
                        # datetime/date型の場合、直接フォーマット
                        partition_date_str = partition_date.strftime('%Y%m%d')
                        formatted_date = partition_date.strftime('%Y-%m-%d')
                    else:
                        raise ValueError(f"Unsupported date type: {type(partition_date)}")
                    
                    # パーティションの存在確認クエリ
                    check_query = f"""
                    SELECT partition_id
                    FROM `{project_id}.{dataset_id}.INFORMATION_SCHEMA.PARTITIONS`
                    WHERE table_name = '{table_id}'
                    AND partition_id = '{partition_date_str}'
                    """
                    partition_exists = len(list(self.client.query(check_query).result())) > 0
                    
                    if partition_exists:
                        # カラムの型に応じてDELETEクエリを構築
                        if column_type == 'DATE':
                            delete_query = f"""
                            DELETE FROM `{full_table_id}`
                            WHERE {time_partition_key} = DATE('{formatted_date}')
                            """
                        elif column_type in ['TIMESTAMP', 'DATETIME']:
                            delete_query = f"""
                            DELETE FROM `{full_table_id}`
                            WHERE {time_partition_key} BETWEEN 
                            TIMESTAMP('{formatted_date} 00:00:00') 
                            AND TIMESTAMP('{formatted_date} 23:59:59.999999')
                            """
                        else:
                            raise ValueError(f"Unsupported column type for partitioning: {column_type}")

                        self.client.query(delete_query).result()
                        logging.info(f"Deleted existing partition for date: {formatted_date}")
                    
                    # 新しいデータをロード
                    job_config = bigquery.LoadJobConfig(
                        schema=schema,
                        autodetect=False,
                        allow_quoted_newlines=True,
                        write_disposition="WRITE_APPEND",
                        source_format=bigquery.SourceFormat.CSV,
                        time_partitioning=bigquery.TimePartitioning(
                            type_=bigquery.TimePartitioningType.DAY,
                            field=time_partition_key
                        )
                    )
                    
                    # データフレームをロード
                    self.client.load_table_from_dataframe(
                        partition_df,
                        destination=full_table_id,
                        job_config=job_config
                    ).result()
                    
                    logging.info(f"Successfully {'overwritten' if partition_exists else 'inserted'} partition for date: {formatted_date}")
                
            except Exception as e:
                print('Error:', e)
                logging.info(f"Error: Failed to insert data (Table={full_table_id})")
                sys.exit(1)