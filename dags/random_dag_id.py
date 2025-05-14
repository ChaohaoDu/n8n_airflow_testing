from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook


def read_excel_to_df(file_path):
    df = pd.read_excel(file_path)
    return df


def load_data_to_db(**kwargs):
    df = kwargs['ti'].xcom_pull(task_ids='read_excel')
    pg_hook = PostgresHook(postgres_conn_id='my_postgres_connection')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    for index, row in df.iterrows():
        cursor.execute("INSERT INTO my_table (column1, column2) VALUES (%s, %s)", (row['column1'], row['column2']))
    conn.commit()
    cursor.close()
    conn.close()


def create_dag(dag_id, schedule):
    dag = DAG(dag_id, schedule_interval=schedule, start_date=datetime(2023, 1, 1), catchup=False)

    read_excel = PythonOperator(
        task_id='read_excel',
        python_callable=read_excel_to_df,
        op_kwargs={'file_path': 'path/to/excel/file.xlsx'},
        dag=dag,
    )

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_db,
        dag=dag,
    )

    read_excel >> load_data
    return dag


my_dag = create_dag('random_dag_id', '@daily')