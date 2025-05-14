from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd

# Define a function to read from Excel and load into the database

def load_data_to_db(**kwargs):
    # Read Excel file
    df = pd.read_excel('path/to/your/file.xlsx', sheet_name='Sheet1')
    
    # Connection details
    pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection_id')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # Load DataFrame to SQL
    df.to_sql('your_table_name', con=engine, if_exists='replace', index=False)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Define the DAG
with DAG(dag_id='random_excel_to_db_dag',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    load_data_task = PythonOperator(
        task_id='load_data_to_db_task',
        python_callable=load_data_to_db,
        provide_context=True,
    )

    load_data_task