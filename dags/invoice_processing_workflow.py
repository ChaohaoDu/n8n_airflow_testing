from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator
import os
import pandas as pd
import pdfplumber
from sqlalchemy import create_engine

# PostgreSQL connection string
POSTGRES_CONNECTION_STRING = 'postgresql://airflow:airflow@localhost:5432/airflow'

def get_pdf_files(dir_path: str):
    return [f for f in os.listdir(dir_path) if f.endswith('.pdf')]

@task
def extract_data_from_pdf(pdf_file_path):
    with pdfplumber.open(pdf_file_path) as pdf:
        # Assuming the first page has the needed table
        table = pdf.pages[0].extract_table()
        df = pd.DataFrame(table[1:], columns=table[0])  # Convert to DataFrame
    return df

@task
def save_df_to_csv(df: pd.DataFrame, filename: str):
    tmp_csv_path = f'/tmp/{filename}.csv'
    df.to_csv(tmp_csv_path, index=False)
    return tmp_csv_path

@task
def load_csv_to_postgres(csv_file_path: str, target_table_name: str):
    engine = create_engine(POSTGRES_CONNECTION_STRING)
    df = pd.read_csv(csv_file_path)
    df.to_sql(target_table_name, con=engine, if_exists='replace', index=False)

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'invoice_processing_workflow',
    default_args=default_args,
    description='Process invoice PDFs and load into PostgreSQL',
    schedule_interval=None,
)

dir_path = '/Users/ryan/Downloads/invoice'

@task
def process_invoices(target_table_name: str, pdf_file_name: str = None):
    pdf_files = []
    if pdf_file_name:
        pdf_files.append(pdf_file_name)
    else:
        pdf_files = get_pdf_files(dir_path)

    for pdf_file in pdf_files:
        pdf_file_path = os.path.join(dir_path, pdf_file)
        df = extract_data_from_pdf(pdf_file_path)
        csv_file_path = save_df_to_csv(df, pdf_file)
        load_csv_to_postgres(csv_file_path, target_table_name)

process_invoices_task = process_invoices(
    target_table_name='{{ dag_run.conf.get("target_table_name", "invoice_data") }}',
    pdf_file_name='{{ dag_run.conf.get("pdf_file_name", None) }}'
)