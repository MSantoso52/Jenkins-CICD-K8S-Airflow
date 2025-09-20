from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import json
import os

def elt_sales_data(**kwargs):
    # Extract: Read JSON file
    with open('sales_record.json', 'r') as f:
        data = json.load(f)
    
    # Load into DataFrame and flatten nested structures
    df = pd.json_normalize(data, sep='_')
    
    # Transform: Data cleansing
    # Remove duplicates based on order_id
    df = df.drop_duplicates(subset=['order_id'])
    
    # Data type correction
    # Quantity: some are strings, convert to int
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
    
    # Total_price: remove '$' if present and convert to float
    df['total_price'] = df['total_price'].astype(str).str.replace('$', '', regex=False)
    df['total_price'] = pd.to_numeric(df['total_price'], errors='coerce')
    
    # Price_per_unit: ensure float
    df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')
    
    # Order_date: convert to datetime
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
    # Handle null/missing values
    # Age: fill with median age
    median_age = df['customer_info_age'].median()
    df['customer_info_age'] = df['customer_info_age'].fillna(median_age)
    
    # Payment_method: fill nulls with 'Unknown'
    df['payment_method'] = df['payment_method'].fillna('Unknown')
    
    # Drop rows with null order_id or total_price (critical fields)
    df = df.dropna(subset=['order_id', 'total_price'])
    
    # Other nulls (e.g., notes): leave as is or fill with empty string
    df = df.fillna({'notes': ''})
    
    # Save cleaned data (Load transformed data)
    output_path = 'cleaned_sales_record.csv'
    df.to_csv(output_path, index=False)
    print(f"Cleaned data saved to {output_path}")

dag = DAG(
    'sales_elt_dag',
    default_args={
        'owner': 'airflow',
        'start_date': datetime(2025, 9, 20),
        'retries': 1,
    },
    schedule_interval='@daily',  # Adjust as needed
    catchup=False,
)

elt_task = PythonOperator(
    task_id='elt_sales_data',
    python_callable=elt_sales_data,
    provide_context=True,
    dag=dag,
)
