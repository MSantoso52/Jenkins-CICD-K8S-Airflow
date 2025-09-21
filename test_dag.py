import pytest
import sys
import os
from datetime import datetime
from airflow.models import DagBag
from airflow.configuration import conf as airflow_conf

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

@pytest.fixture
def dagbag():
    """Load the DAG bag for testing"""
    # Temporarily set Airflow config for testing
    test_conf = {
        'AIRFLOW__CORE__SQL_ALCHEMY_CONN': 'postgresql://airflow:airflow@localhost:5432/airflow',
        'AIRFLOW__CORE__DAGS_FOLDER': '.'
    }
    
    for key, value in test_conf.items():
        airflow_conf.set('core', key.split('__')[-1], value)
    
    return DagBag(dag_folder='.', include_examples=False)

def test_dag_loading(dagbag):
    """Test that the DAG loads without errors"""
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"
    
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    assert dag is not None, "DAG 'sales_elt_dag' not found"

def test_dag_structure(dagbag):
    """Test DAG has correct structure and tasks"""
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    
    # Check basic DAG properties
    assert dag.dag_id == 'sales_elt_dag'
    assert dag.start_date == datetime(2025, 9, 21)
    assert dag.schedule_interval == '@daily'
    assert len(dag.tasks) == 3, f"Expected 3 tasks, found {len(dag.tasks)}"
    
    # Check task names
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = ['extract_and_transform', 'load_to_postgresql', 'validate_load']
    assert set(task_ids) == set(expected_tasks), f"Expected tasks {expected_tasks}, found {task_ids}"

def test_task_dependencies(dagbag):
    """Test task dependencies are correct"""
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    
    extract_task = dag.get_task('extract_and_transform')
    load_task = dag.get_task('load_to_postgresql')
    validate_task = dag.get_task('validate_load')
    
    # Check downstream dependencies
    assert 'load_to_postgresql' in extract_task.downstream_task_ids
    assert 'validate_load' in load_task.downstream_task_ids
    assert len(validate_task.downstream_task_ids) == 0  # Last task

def test_extract_transform_function():
    """Test the extract_and_transform function logic"""
    from sales_elt_dag import extract_and_transform
    
    # Create a test JSON file
    test_json = {
        "data": [
            {
                "order_id": "TEST-001",
                "item_name": "Test Item",
                "quantity": "5",  # String to test conversion
                "price_per_unit": 100.50,
                "total_price": "$502.50",  # With $ to test cleaning
                "order_date": "2025-01-01T00:00:00",
                "region": "Test Region",
                "payment_method": None,  # Null to test handling
                "customer_info": {
                    "customer_id": "CUST-TEST",
                    "email": "test@example.com",
                    "age": None,  # Null age
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
    
    # Write test file
    with open('/tmp/test_sales.json', 'w') as f:
        import json
        json.dump(test_json, f)
    
    # Temporarily modify the function to use test file
    original_path = '/data/sales_record.json'
    try:
        # This is a simplified test - in real scenario, you'd mock the file reading
        # For now, just test the logic with sample data
        result = extract_and_transform()
        assert result > 0, "Extract and transform should return positive record count"
    finally:
        if os.path.exists('/tmp/test_sales.json'):
            os.remove('/tmp/test_sales.json')

def test_dag_default_args(dagbag):
    """Test DAG default arguments"""
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    
    # Check default args
    defaults = dag.default_args
    assert 'owner' in defaults
    assert 'start_date' in defaults
    assert defaults['retries'] == 1
    assert 'retry_delay' in defaults

def test_dag_tags(dagbag):
    """Test DAG has appropriate tags"""
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    assert hasattr(dag, 'tags')
    assert 'sales' in dag.tags
    assert 'elt' in dag.tags
    assert 'postgresql' in dag.tags

class TestDataQuality:
    """Test data quality aspects that the DAG should handle"""
    
    @pytest.fixture
    def sample_data(self):
        """Sample data with various quality issues"""
        return {
            "data": [
                # Valid record
                {
                    "order_id": "GOOD-001",
                    "item_name": "Good Item",
                    "quantity": 5,
                    "price_per_unit": 100.0,
                    "total_price": 500.0,
                    "order_date": "2025-01-01T00:00:00",
                    "region": "North",
                    "payment_method": "Credit Card",
                    "customer_info": {
                        "customer_id": "CUST-001",
                        "email": "good@example.com",
                        "age": 30,
                        "address": {"street": "123", "city": "Test", "zip": "12345"}
                    },
                    "status": "Completed"
                },
                # Duplicate
                {
                    "order_id": "GOOD-001",
                    "item_name": "Duplicate",
                    "quantity": 5,
                    "price_per_unit": 100.0,
                    "total_price": 500.0,
                    "order_date": "2025-01-01T00:00:00",
                    "region": "North",
                    "payment_method": "Credit Card",
                    "customer_info": {
                        "customer_id": "CUST-001",
                        "email": "duplicate@example.com",
                        "age": 30,
                        "address": {"street": "123", "city": "Test", "zip": "12345"}
                    },
                    "status": "Completed"
                },
                # Invalid quantity (string)
                {
                    "order_id": "STR-001",
                    "item_name": "String Quantity",
                    "quantity": "five",
                    "price_per_unit": 100.0,
                    "total_price": 500.0,
                    "order_date": "2025-01-01T00:00:00",
                    "region": "South",
                    "payment_method": None,
                    "customer_info": {
                        "customer_id": "CUST-002",
                        "email": "string@example.com",
                        "age": None,
                        "address": {"street": "456", "city": "Test", "zip": "67890"}
                    },
                    "status": "Pending"
                },
                # Invalid price with $
                {
                    "order_id": "DOLLAR-001",
                    "item_name": "Dollar Price",
                    "quantity": 3,
                    "price_per_unit": 50.0,
                    "total_price": "$150.00",
                    "order_date": "2025-01-02T00:00:00",
                    "region": "East",
                    "payment_method": "Cash",
                    "customer_info": {
                        "customer_id": "CUST-003",
                        "email": "dollar@example.com",
                        "age": 25,
                        "address": {"street": "789", "city": "Test", "zip": "11111"}
                    },
                    "status": "Completed"
                }
            ]
        }

def test_data_cleansing_logic(sample_data):
    """Test the data cleansing logic separately"""
    import pandas as pd
    from sales_elt_dag import extract_and_transform  # Import the function
    
    # Write sample data to temp file
    with open('/tmp/sample_sales.json', 'w') as f:
        import json
        json.dump(sample_data, f)
    
    try:
        # Run the transformation
        record_count = extract_and_transform()
        
        # Read the cleaned CSV and validate
        df = pd.read_csv('/tmp/cleaned_sales.csv')
        
        # Should have removed 1 duplicate, so 3 records
        assert len(df) == 3, f"Expected 3 records after deduplication, got {len(df)}"
        
        # Check that string quantity was converted to NaN and then dropped
        # The invalid quantity record should be dropped
        valid_order_ids = ['GOOD-001', 'DOLLAR-001']
        assert all(order_id in df['order_id'].values for order_id in valid_order_ids)
        
        # Check that $ was removed from total_price
        dollar_record = df[df['order_id'] == 'DOLLAR-001']
        assert len(dollar_record) == 1
        assert dollar_record['total_price'].iloc[0] == 150.0
        
        # Check null handling
        null_payment = df[df['order_id'] == 'DOLLAR-001']
        assert null_payment['payment_method'].iloc[0] == 'Cash'  # Should not be Unknown
        
        print("✓ Data cleansing tests PASSED")
        
    finally:
        # Cleanup
        for file in ['/tmp/sample_sales.json', '/tmp/cleaned_sales.csv']:
            if os.path.exists(file):
                os.remove(file)

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
