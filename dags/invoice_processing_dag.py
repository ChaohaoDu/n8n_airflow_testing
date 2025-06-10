from datetime import datetime
from os import listdir
from os.path import isfile, join
import pandas as pd
import pdfplumber
from sqlalchemy import create_engine
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.models import Variable

# Database connection string
postgresql_connection_string = 'postgresql://airflow:airflow@localhost:5432/airflow'

def get_pdf_files(directory):
    return [f for f in listdir(directory) if isfile(join(directory, f)) and f.endswith('.pdf')]

@task
def process_invoice(pdf_file, target_table_name):
    # Read the PDF file and extract data
    with pdfplumber.open(pdf_file) as pdf:
        table = pdf.pages[0].extract_table()  # Adjust page index if necessary

    # Convert to DataFrame
    df = pd.DataFrame(table[1:], columns=table[0])

    # Save to temporary CSV
    csv_file = f'/tmp/{pdf_file.split('/')[-1].replace(".pdf", ".csv")}'
    df.to_csv(csv_file, index=False)
    return csv_file

@task
def load_to_postgres(csv_file, target_table_name):
    # Create SQLAlchemy engine
    engine = create_engine(postgresql_connection_string)
    df = pd.read_csv(csv_file)
    # Load DataFrame to PostgreSQL
    df.to_sql(target_table_name, con=engine, index=False, if_exists='append')

# Define the DAG
with DAG(
    'invoice_processing_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2023, 1, 1),
        'retries': 1,
    },
    schedule_interval=None,
    catchup=False,
) as dag:

    pdf_directory = '/Users/ryan/Downloads/invoice'
    target_table_name = '{{ dag_run.conf.get("target_table_name", "invoice_data") }}'
    specific_file = '{{ dag_run.conf.get("pdf_file_name") }}'

    # Logic to define which files to process
    if specific_file:
        pdf_files = [specific_file]
    else:
        pdf_files = get_pdf_files(pdf_directory)

    for pdf_file in pdf_files:
        pdf_file_path = join(pdf_directory, pdf_file)
        csv_file = process_invoice(pdf_file_path, target_table_name)
        load_to_postgres(csv_file, target_table_name)
