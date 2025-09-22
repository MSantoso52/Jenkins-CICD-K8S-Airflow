from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator  # Updated import
import json
import pandas as pd
from sqlalchemy import create_engine, text
import os

# Define the DAG first
dag = DAG(
    'sales_elt_dag',
    default_args={
        'owner': 'MSantoso52',
        'start_date': datetime(2025, 9, 21),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Sales ELT pipeline with data quality checks',
    schedule_interval='@daily',
    catchup=False,
    tags=['sales', 'elt', 'postgresql']
)

default_args = {
    'owner': 'MSantoso52',
    'start_date': datetime(2025, 9, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_and_transform():
    """Extract JSON and transform/clean the data"""
    print("Starting extraction and transformation...")
    
    # Check if file exists
    json_file_path = os.getenv('SALES_DATA_PATH', '/data/sales_record.json')
    if not os.path.exists(json_file_path):
        print(f"Warning: JSON file not found at {json_file_path}. Creating sample data for testing.")
        # Create sample data for testing
        sample_data = {
            "data": [
                {
                    "order_id": "TEST-001",
                    "item_name": "Test Item",
                    "quantity": 5,
                    "price_per_unit": 100.50,
                    "total_price": 502.50,
                    "order_date": "2025-01-01T00:00:00",
                    "region": "Test Region",
                    "payment_method": "Credit Card",
                    "customer_info": {
                        "customer_id": "CUST-TEST",
                        "email": "test@example.com",
                        "age": 30,
                        "address": {
                            "street": "123 Test St",
                            "city": "Test City",
                            "zip": "12345"
                        }
                    },
                    "status": "Completed"
                }
            ]
        }
    else:
        # Extract: Read JSON file
        with open(json_file_path, 'r') as f:
            data = json.load(f)
    
    if 'sample_data' not in locals():
        print(f"Extracted {len(data)} records from JSON")
        # Flatten: Make nested structures flat
        flattened_data = []
        for record in data:
            flat_record = record.copy()
            
            # Flatten customer_info
            if 'customer_info' in flat_record:
                customer = flat_record.pop('customer_info')
                flat_record.update(customer)
                
                # Flatten address
                if 'address' in customer:
                    address = customer['address']
                    flat_record.update(address)
                    # Remove address from customer since we flattened it
                    customer.pop('address', None)
            
            flattened_data.append(flat_record)
    else:
        flattened_data = sample_data["data"]
    
    # Convert to DataFrame for easier manipulation
    df = pd.DataFrame(flattened_data)
    print(f"Created DataFrame with {len(df)} rows and {len(df.columns)} columns")
    
    # Remove duplicates based on order_id
    initial_count = len(df)
    df = df.drop_duplicates(subset=['order_id'])
    print(f"Removed {initial_count - len(df)} duplicate records")
    
    # Data type corrections
    print("Applying data type corrections...")
    
    # Convert quantity (some are strings)
    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    
    # Convert price_per_unit
    df['price_per_unit'] = pd.to_numeric(df['price_per_unit'], errors='coerce')
    
    # Clean total_price (remove $ and convert to float)
    if 'total_price' in df.columns:
        df['total_price'] = df['total_price'].astype(str).str.replace(r'[\$,]', '', regex=True)
        df['total_price'] = pd.to_numeric(df['total_price'], errors='coerce')
    
    # Convert order_date to datetime
    df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
    # Convert age to numeric
    if 'age' in df.columns:
        df['age'] = pd.to_numeric(df['age'], errors='coerce')
    
    # Handle null/missing values
    print("Handling null values...")
    
    # Fill null ages with 0
    if 'age' in df.columns:
        df['age'] = df['age'].fillna(0)
    
    # Fill null payment methods with 'Unknown'
    if 'payment_method' in df.columns:
        df['payment_method'] = df['payment_method'].fillna('Unknown')
    
    # Fill null regions with 'Unknown'
    if 'region' in df.columns:
        df['region'] = df['region'].fillna('Unknown')
    
    # Drop rows where critical fields are missing
    critical_fields = ['order_id', 'item_name', 'quantity', 'price_per_unit']
    available_critical_fields = [field for field in critical_fields if field in df.columns]
    initial_rows = len(df)
    if available_critical_fields:
        df = df.dropna(subset=available_critical_fields)
        print(f"Dropped {initial_rows - len(df)} rows with missing critical fields")
    
    # Remove rows with negative quantities or prices (data quality)
    valid_quantity = df['quantity'] > 0 if 'quantity' in df.columns else True
    valid_price = df['price_per_unit'] > 0 if 'price_per_unit' in df.columns else True
    df = df[valid_quantity & valid_price]
    print(f"Removed rows with invalid quantities/prices")
    
    # Save cleaned data temporarily for loading task
    df.to_csv('/tmp/cleaned_sales.csv', index=False)
    print(f"Transformation complete. Saved {len(df)} cleaned records to temporary file.")
    
    return len(df)

def load_to_postgresql():
    """Load cleaned data to PostgreSQL database"""
    print("Starting PostgreSQL load...")
    
    try:
        # Get database connection details from environment variables
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'airflow')
        db_user = os.getenv('DB_USER', 'airflow')
        db_password = os.getenv('DB_PASSWORD', 'airflow')
        
        # Create connection string
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        
        # Read cleaned data
        df = pd.read_csv('/tmp/cleaned_sales.csv')
        print(f"Loading {len(df)} records to PostgreSQL...")
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS cleaned_sales_records (
            order_id VARCHAR(50) PRIMARY KEY,
            item_name VARCHAR(100),
            quantity INTEGER,
            price_per_unit DECIMAL(10,2),
            total_price DECIMAL(10,2),
            order_date TIMESTAMP,
            region VARCHAR(50),
            payment_method VARCHAR(50),
            customer_id VARCHAR(50),
            email VARCHAR(100),
            age INTEGER,
            street VARCHAR(200),
            city VARCHAR(100),
            zip VARCHAR(20),
            status VARCHAR(50)
        );
        """
        
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        
        # Load data to PostgreSQL (truncate and insert fresh data)
        with engine.connect() as conn:
            # Truncate existing data for this run
            conn.execute(text("TRUNCATE TABLE cleaned_sales_records;"))
            conn.commit()
            
            # Insert new data
            df.to_sql('cleaned_sales_records', engine, if_exists='append', index=False, method='multi')
            conn.commit()
        
        print(f"Successfully loaded {len(df)} records to PostgreSQL table 'cleaned_sales_records'")
        
        # Clean up temp file
        if os.path.exists('/tmp/cleaned_sales.csv'):
            os.remove('/tmp/cleaned_sales.csv')
            
    except Exception as e:
        print(f"Error loading to PostgreSQL: {str(e)}")
        raise

def validate_load():
    """Validate that data was loaded correctly"""
    print("Validating PostgreSQL load...")
    
    try:
        # Same connection setup
        db_host = os.getenv('DB_HOST', 'localhost')
        db_port = os.getenv('DB_PORT', '5432')
        db_name = os.getenv('DB_NAME', 'airflow')
        db_user = os.getenv('DB_USER', 'airflow')
        db_password = os.getenv('DB_PASSWORD', 'airflow')
        
        connection_string = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
        engine = create_engine(connection_string)
        
        # Check row count
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM cleaned_sales_records;"))
            row_count = result.fetchone()[0]
            print(f"Validation: Found {row_count} rows in PostgreSQL table")
        
        # Sample validation queries
        with engine.connect() as conn:
            # Check for null values in critical fields
            null_check_sql = """
                SELECT COUNT(*) FROM cleaned_sales_records 
                WHERE order_id IS NULL OR item_name IS NULL OR quantity IS NULL
            """
            null_check = conn.execute(text(null_check_sql)).fetchone()[0]
            
            # Check for negative values
            negative_check_sql = """
                SELECT COUNT(*) FROM cleaned_sales_records 
                WHERE quantity < 0 OR price_per_unit < 0
            """
            negative_check = conn.execute(text(negative_check_sql)).fetchone()[0]
            
            print(f"Validation: {null_check} rows with null critical fields")
            print(f"Validation: {negative_check} rows with negative values")
            
            if null_check == 0 and negative_check == 0 and row_count > 0:
                print("✓ Data quality validation PASSED")
            else:
                print("✗ Data quality validation FAILED")
                raise ValueError(f"Data quality validation failed: nulls={null_check}, negatives={negative_check}, rows={row_count}")
                
    except Exception as e:
        print(f"Error during validation: {str(e)}")
        raise

# Define tasks
extract_transform_task = PythonOperator(
    task_id='extract_and_transform',
    python_callable=extract_and_transform,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_postgresql',
    python_callable=load_to_postgresql,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_load',
    python_callable=validate_load,
    dag=dag,
)

# Set task dependencies
extract_transform_task >> load_task >> validate_task
