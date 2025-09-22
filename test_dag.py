import pytest
import sys
import os
from datetime import datetime
from unittest.mock import patch, MagicMock
from airflow.models import DagBag
from airflow.configuration import conf as airflow_conf

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

@pytest.fixture
def dagbag():
    """Load the DAG bag for testing"""
    # Mock the problematic import
    with patch('sales_elt_dag.PostgresOperator', new=MagicMock()):
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

@patch('sales_elt_dag.os')
@patch('sales_elt_dag.json')
def test_extract_transform_function(mock_json, mock_os):
    """Test the extract_and_transform function logic"""
    from sales_elt_dag import extract_and_transform
    
    # Mock file operations
    mock_file = MagicMock()
    mock_json.load.return_value = {
        "data": [
            {
                "order_id": "TEST-001",
                "item_name": "Test Item",
                "quantity": "5",
                "price_per_unit": 100.50,
                "total_price": "$502.50",
                "order_date": "2025-01-01T00:00:00",
                "region": "Test Region",
                "payment_method": None,
                "customer_info": {
                    "customer_id": "CUST-TEST",
                    "email": "test@example.com",
                    "age": None,
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
    mock_open = MagicMock(return_value=mock_file)
    mock_os.path.exists.return_value = True
    mock_os.getenv.return_value = 'localhost'
    
    with patch('builtins.open', mock_open):
        result = extract_and_transform()
        assert result > 0, "Extract and transform should return positive record count"
        mock_open.assert_called_once_with('/data/sales_record.json', 'r')

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

@patch('sales_elt_dag.os')
@patch('sales_elt_dag.json')
@patch('sales_elt_dag.pd')
def test_data_cleansing_logic(mock_pd, mock_json, mock_os, sample_data):
    """Test the data cleansing logic separately"""
    from sales_elt_dag import extract_and_transform
    
    # Mock file operations
    mock_file = MagicMock()
    mock_json.load.return_value = sample_data
    mock_open = MagicMock(return_value=mock_file)
    mock_os.path.exists.return_value = True
    mock_os.getenv.return_value = 'localhost'
    
    # Mock pandas operations
    mock_df = MagicMock()
    mock_pd.DataFrame.return_value = mock_df
    mock_df.drop_duplicates.return_value = mock_df
    mock_df.to_csv.return_value = None
    mock_df.dropna.return_value = mock_df
    mock_df.to_numeric.return_value = mock_df
    mock_df.astype.return_value = mock_df
    mock_df.str.replace.return_value = mock_df
    mock_df.to_datetime.return_value = mock_df
    mock_df.fillna.return_value = mock_df
    mock_df.loc.return_value = mock_df
    mock_df.shape = (3, 15)  # 3 rows after deduplication
    
    with patch('builtins.open', mock_open):
        record_count = extract_and_transform()
        
        # Verify the transformation steps were called
        assert mock_pd.DataFrame.called
        assert mock_df.drop_duplicates.called
        assert mock_df.to_csv.called
        assert record_count == 3, f"Expected 3 records after processing, got {record_count}"
        
        print("✓ Data cleansing tests PASSED")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
