from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task
import os
import pandas as pd
import pdfplumber
from sqlalchemy import create_engine

# Database connection settings
DB_URI = 'postgresql://airflow:airflow@localhost:5432/airflow'
engine = create_engine(DB_URI)

# Define the default target table name
DEFAULT_TABLE_NAME = 'invoice_data'

def get_pdf_files(directory):
    return [f for f in os.listdir(directory) if f.endswith('.pdf')]

@task
def extract_data_from_pdf(pdf_file):
    with pdfplumber.open(pdf_file) as pdf:
        table = pdf.pages[0].extract_table()
    return pd.DataFrame(table[1:], columns=table[0])

@task
def save_as_csv(dataframe, file_path):
    dataframe.to_csv(file_path, index=False)

@task
def load_to_postgres(csv_file, table_name):
    df = pd.read_csv(csv_file)
    df.to_sql(table_name, engine, if_exists='append', index=False)

@task
def process_pdf_files(directory, table_name):
    pdf_files = get_pdf_files(directory)
    for pdf_file in pdf_files:
        pdf_file_path = os.path.join(directory, pdf_file)
        dataframe = extract_data_from_pdf(pdf_file_path)  
        temp_csv_path = f'/tmp/{pdf_file}.csv'
        save_as_csv(dataframe, temp_csv_path)
        load_to_postgres(temp_csv_path, table_name)

with DAG(
    dag_id='invoice_processing_dag',
    default_args={'owner': 'airflow', 'start_date': datetime(2023, 10, 1)},
    schedule_interval='@daily',
    catchup=False,
) as dag:
    directory = '/Users/ryan/Downloads/invoice'
    target_table_name = '{{ dag_run.conf.get('target_table_name', DEFAULT_TABLE_NAME) }}'
    pdf_file_name = '{{ dag_run.conf.get('pdf_file_name', None) }}'

    if pdf_file_name:
        process_pdf_files(directory, target_table_name)
    else:
        for pdf in get_pdf_files(directory):
            pdf_file = os.path.join(directory, pdf)
            extracted_data = extract_data_from_pdf(pdf_file)
            temp_csv_path = f'/tmp/{pdf}.csv'
            save_as_csv(extracted_data, temp_csv_path)
            load_to_postgres(temp_csv_path, target_table_name)
