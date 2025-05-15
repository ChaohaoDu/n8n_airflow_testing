from datetime import datetime
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook

# Function to load data into the database
def load_data_into_db(**kwargs):
    # Transform input data into a DataFrame
    data = [
        {"company_name": "Your Company", "address": "123 Your Street", "city": "Your City, ST 12345", "phone": "(123) 456-7890", "invoice_date": "2000-01-01", "invoice_number": 123456, "total_price": 500},
        # Additional items can be added similarly
    ]
    df = pd.DataFrame(data)

    pg_hook = PostgresHook(postgres_conn_id='your_postgres_connection')
    connection = pg_hook.get_conn()
    cursor = connection.cursor()

    for index, row in df.iterrows():
        cursor.execute("INSERT INTO invoices (company_name, address, city, phone, invoice_date, invoice_number, total_price) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                       (row['company_name'], row['address'], row['city'], row['phone'], row['invoice_date'], row['invoice_number'], row['total_price']))

    connection.commit()
    cursor.close()
    connection.close()

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# Initialize the DAG
dag = DAG(
    'load_invoice_data',
    default_args=default_args,
    description='A DAG to load invoice data into the database',
    schedule_interval='@once',
)

# Define the task to load data into the database
load_data_task = PythonOperator(
    task_id='load_data_into_db',
    python_callable=load_data_into_db,
    dag=dag,
)

load_data_task