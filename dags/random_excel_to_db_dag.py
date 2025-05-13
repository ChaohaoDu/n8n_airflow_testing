from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Function to read Excel and load to database

def load_data_to_db(excel_file, sheet_name, table_name):
    # Read data from Excel sheet
    df = pd.read_excel(excel_file, sheet_name=sheet_name)
    hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    engine = hook.get_sqlalchemy_engine()
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)

# Define default_args

def get_default_args():
    return {
        'owner': 'airflow',
        'depend_on_past': False,
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    }

# Define DAG

dag = DAG(
    'random_excel_to_db_dag',
    default_args=get_default_args(),
    description='A simple DAG to load data from Excel to Postgres',
    schedule_interval=None,
)

# Define tasks

task_load_data = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_db,
    op_kwargs={
        'excel_file': '/path/to/your/excel.xlsx',
        'sheet_name': 'Sheet1',
        'table_name': 'your_table_name',
    },
    dag=dag,
)

# Set task dependencies


