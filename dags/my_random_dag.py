from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd


# Function to extract data from Excel
def extract_data_from_excel(**kwargs):
    file_path = '/path/to/excel_file.xlsx'
    sheet_name = 'Sheet1'
    df = pd.read_excel(file_path, sheet_name=sheet_name)
    return df


# Function to load data into Postgres
def load_data_to_postgres(**kwargs):
    df = kwargs['task_instance'].xcom_pull(task_ids='extract_data')
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_conn')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for index, row in df.iterrows():
        cursor.execute("INSERT INTO my_table (column1, column2) VALUES (%s, %s)", (row['column1'], row['column2']))

    connection.commit()
    cursor.close()
    connection.close()


# Define the DAG
with DAG(
    'my_random_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 10, 1),
        'retries': 1,
    },
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_from_excel,
        provide_context=True,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_postgres,
        provide_context=True,
    )

    extract_data >> load_data