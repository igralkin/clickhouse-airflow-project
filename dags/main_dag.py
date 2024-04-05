import os
import glob
import lzma
import shutil
import clickhouse_connect
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from file_downloader import FileDownloader


source_archive_url = 'https://datasets.clickhouse.com/cell_towers.csv.xz'
local_xz_file_path = '/opt/airflow/data/cell_towers.csv.xz'
local_csv_file_path = '/opt/airflow/data/cell_towers.csv'


clickhouse_host = '<...>'
clickhouse_port = '<...>'
clickhouse_user = '<...>'
clickhouse_password = '<...>'

db_name = 'default'
table_name = 'cell_towers'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def get_clickhouse_client():
    try:
        client = clickhouse_connect.get_client(
            host=clickhouse_host,
            port=clickhouse_port,
            user=clickhouse_user,
            password=clickhouse_password,
            verify=False
        )
        return client
    except Exception as e:
        raise AirflowException(f"Failed to connect to ClickHouse: {str(e)}")


def download_file():
    downloader = FileDownloader(
        source_url=source_archive_url,
        destination_path=local_xz_file_path
    )
    downloader.download()


def unarchive_file():
    source_path = local_xz_file_path
    destination_path = local_csv_file_path
    
    with lzma.open(source_path) as f:
        with open(destination_path, 'wb') as dest:
            shutil.copyfileobj(f, dest)


def divide_csv(source_path=local_csv_file_path,
               output_directory='/opt/airflow/data/chunks/',
               chunk_size=1000000): 

    os.makedirs(output_directory, exist_ok=True)

    if not output_directory.endswith('/'):
        output_directory += '/'
        
    output_prefix = f"{output_directory}{table_name}-"
    output_suffix = '.csv'

    csv_iterator = pd.read_csv(source_path, chunksize=chunk_size)

    for i, chunk in enumerate(csv_iterator):
        chunk_num = i+1
        chunk_file_path = f"{output_prefix}{chunk_num}{output_suffix}"
        chunk.to_csv(chunk_file_path, index=False)
        print(f"Chunk saved: {chunk_file_path}")


def load_to_clickhouse():
    clickhouse_client = get_clickhouse_client()
    
    csv_files = glob.glob('/opt/airflow/data/chunks/*.csv')
    
    # Загрузка микробатчами, можно поизменять max_partitions_per_insert_block для ускорения
    batch_size = 1000
    
    for csv_file in csv_files:
        for chunk in pd.read_csv(csv_file, chunksize=batch_size):
            try:
                filtered_chunk = chunk[chunk['mcc'].isin([262, 460, 310, 208, 510, 404, 250, 724, 234, 311])].copy()
                
                if 'created' in filtered_chunk.columns and 'updated' in filtered_chunk.columns:
                    filtered_chunk.loc[:, 'created'] = pd.to_datetime(filtered_chunk['created'])
                    filtered_chunk.loc[:, 'updated'] = pd.to_datetime(filtered_chunk['updated'])

                clickhouse_client.insert_df(f'{db_name}.{table_name}', filtered_chunk)
                
            except KeyError as e:
                print(f"KeyError encountered in file {csv_file} during processing chunk: {str(e)}")
                continue
            except Exception as e:
                raise AirflowException(f"Error during the loading process for file {csv_file}: {str(e)}")

            
with DAG(
    'main_dag',
    default_args=default_args,
    description='A DAG for loading celular data to Clickhouse',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    download_task = PythonOperator(
        task_id='download_file',
        python_callable=download_file,
    )

    unarchive_task = PythonOperator(
        task_id='unarchive_file',
        python_callable=unarchive_file,
    )

    split_csv_task = PythonOperator(
        task_id='divide_csv',
        python_callable=divide_csv,
    )

    load_to_clickhouse_task = PythonOperator(
        task_id='filter_and_load_to_clickhouse',
        python_callable=load_to_clickhouse,
        dag=dag,
    )

    download_task >> unarchive_task >> split_csv_task >> load_to_clickhouse_task
