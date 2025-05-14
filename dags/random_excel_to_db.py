from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depend_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Instantiate the DAG
with DAG('random_excel_to_db', default_args=default_args, schedule_interval='@daily') as dag:

    # Function to read Excel and load to database
    def load_excel_to_db():
        # Read the data from Excel file
        df = pd.read_excel('/path/to/excel/file.xlsx', sheet_name='Sheet1')
        
        # Using PostgresHook to connect to database
        pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Iterate over DataFrame and insert into database
        for index, row in df.iterrows():
            cursor.execute('INSERT INTO my_table (column1, column2) VALUES (%s, %s)', (row['Column1'], row['Column2']))
        
        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()

    # Python operator task to execute the function
    load_task = PythonOperator(
        task_id='load_excel_to_db',
        python_callable=load_excel_to_db,
    )

    load_task