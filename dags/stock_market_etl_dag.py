from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append('/opt/airflow/scripts')

from preprocess import clean_stock_data

default_args ={
    'owner': 'Salman',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'stock_market_etl_dagV1',
    default_args = default_args,
    start_date = datetime(2025,7,10),
    schedule = '5 4 * * wed,mon',
    catchup= True
) as dag:
    
    preprocess_task = PythonOperator(
        task_id = 'clean_stock_data_task' ,
        python_callable = clean_stock_data   
    )

    preprocess_task