"""
ETL Pipeline DAG for Apache Airflow
This DAG extracts data from a CSV file, transforms it, and loads it into a database
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlite3
import os

# Default arguments for the DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email': ['your_email@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define file paths
DATA_DIR = '/tmp/airflow_data'
RAW_DATA_PATH = f'{DATA_DIR}/raw_data.csv'
TRANSFORMED_DATA_PATH = f'{DATA_DIR}/transformed_data.csv'
DB_PATH = f'{DATA_DIR}/etl_database.db'


def extract_data(**context):
    """
    Extract: Read data from source (simulating with sample data)
    In production, this could be an API call, database query, or file read
    """
    print("Starting data extraction...")
    
    # Create data directory if it doesn't exist
    os.makedirs(DATA_DIR, exist_ok=True)
    
    # Sample data - in real scenario, this would come from an actual source
    sample_data = {
        'customer_id': [1, 2, 3, 4, 5],
        'customer_name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Brown', 'Charlie Wilson'],
        'purchase_amount': [150.50, 200.00, 75.25, 300.75, 125.00],
        'purchase_date': ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05'],
        'region': ['North', 'South', 'East', 'West', 'North']
    }
    
    df = pd.DataFrame(sample_data)
    df.to_csv(RAW_DATA_PATH, index=False)
    
    print(f"Extracted {len(df)} records to {RAW_DATA_PATH}")
    
    # Push metadata to XCom for next tasks
    context['ti'].xcom_push(key='extracted_records', value=len(df))


def transform_data(**context):
    """
    Transform: Clean and process the data
    - Convert dates to proper format
    - Add calculated fields
    - Filter invalid records
    """
    print("Starting data transformation...")
    
    # Read extracted data
    df = pd.read_csv(RAW_DATA_PATH)
    
    # Transform: Convert purchase_date to datetime
    df['purchase_date'] = pd.to_datetime(df['purchase_date'])
    
    # Transform: Add tax column (10% tax)
    df['tax_amount'] = df['purchase_amount'] * 0.10
    
    # Transform: Add total amount
    df['total_amount'] = df['purchase_amount'] + df['tax_amount']
    
    # Transform: Add month and year columns
    df['month'] = df['purchase_date'].dt.month
    df['year'] = df['purchase_date'].dt.year
    
    # Transform: Categorize purchase amounts
    df['purchase_category'] = df['purchase_amount'].apply(
        lambda x: 'High' if x > 200 else 'Medium' if x > 100 else 'Low'
    )
    
    # Save transformed data
    df.to_csv(TRANSFORMED_DATA_PATH, index=False)
    
    print(f"Transformed {len(df)} records to {TRANSFORMED_DATA_PATH}")
    
    # Push metadata to XCom
    context['ti'].xcom_push(key='transformed_records', value=len(df))


def load_data(**context):
    """
    Load: Insert transformed data into SQLite database
    In production, this could be PostgreSQL, MySQL, or a data warehouse
    """
    print("Starting data loading...")
    
    # Read transformed data
    df = pd.read_csv(TRANSFORMED_DATA_PATH)
    
    # Connect to SQLite database
    conn = sqlite3.connect(DB_PATH)
    
    # Load data into database
    df.to_sql('customer_purchases', conn, if_exists='replace', index=False)
    
    # Verify load
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM customer_purchases")
    count = cursor.fetchone()[0]
    
    conn.close()
    
    print(f"Loaded {count} records into database at {DB_PATH}")
    
    # Push metadata to XCom
    context['ti'].xcom_push(key='loaded_records', value=count)


def validate_pipeline(**context):
    """
    Validate: Check if the pipeline ran successfully
    Compare counts at each stage
    """
    print("Validating pipeline execution...")
    
    ti = context['ti']
    
    extracted = ti.xcom_pull(key='extracted_records', task_ids='extract')
    transformed = ti.xcom_pull(key='transformed_records', task_ids='transform')
    loaded = ti.xcom_pull(key='loaded_records', task_ids='load')
    
    print(f"Pipeline Summary:")
    print(f"  - Extracted: {extracted} records")
    print(f"  - Transformed: {transformed} records")
    print(f"  - Loaded: {loaded} records")
    
    if extracted == transformed == loaded:
        print("✓ Pipeline validation successful! All stages processed the same number of records.")
    else:
        raise ValueError(f"Pipeline validation failed! Record counts don't match.")


# Define the DAG
with DAG(
    'etl_pipeline_example',
    default_args=default_args,
    description='A simple ETL pipeline to demonstrate Airflow',
    schedule_interval=timedelta(days=1),  # Run daily
    start_date=datetime(2024, 1, 1),
    catchup=False,  # Don't run for past dates
    tags=['etl', 'example', 'tutorial'],
) as dag:
    
    # Define tasks
    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data,
        provide_context=True,
    )
    
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
        provide_context=True,
    )
    
    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data,
        provide_context=True,
    )
    
    validate_task = PythonOperator(
        task_id='validate',
        python_callable=validate_pipeline,
        provide_context=True,
    )
    
    # Define task dependencies (DAG structure)
    extract_task >> transform_task >> load_task >> validate_task