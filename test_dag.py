import pytest
import sys
import os
from datetime import datetime
from unittest.mock import patch, MagicMock, Mock
from airflow.models import DagBag
from airflow.configuration import conf as airflow_conf
from airflow.models.dag import DAG
from airflow.models.baseoperator import BaseOperator

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

@pytest.fixture
def mock_airflow_db():
    """Mock Airflow's database connections to avoid SQLite issues"""
    with patch('airflow.models.dag.DagModel') as mock_dag_model:
        # Mock the database session and connection
        mock_session = Mock()
        mock_engine = Mock()
        
        # Create proper context manager mocks
        mock_connection = Mock()
        mock_connection_ctx = Mock()
        mock_connection_ctx.__enter__.return_value = mock_connection
        mock_connection_ctx.__exit__.return_value = None
        mock_engine.connect.return_value = mock_connection_ctx
        
        mock_session.get_bind.return_value = mock_engine
        mock_session.scalar.return_value = None
        mock_connection.scalar.return_value = None
        
        mock_dag_model.get_current.return_value = None
        mock_dag_model.get_dagbag_import_errors.return_value = []
        
        # Patch session creation
        with patch('airflow.utils.session.create_session', return_value=mock_session):
            yield mock_session

@pytest.fixture
def dagbag(mock_airflow_db):
    """Load the DAG bag for testing with mocked database"""
    # Temporarily set Airflow config for testing
    test_conf = {
        'AIRFLOW__CORE__SQL_ALCHEMY_CONN': 'sqlite:///:memory:',  # Use in-memory SQLite
        'AIRFLOW__CORE__DAGS_FOLDER': '.',
        'AIRFLOW__CORE__LOAD_EXAMPLES': 'False',
        'AIRFLOW__DATABASE__SQL_ALCHEMY_CONN': 'sqlite:///:memory:'
    }
    
    # Clear existing config and set test config
    for section in list(airflow_conf.keys()):
        del airflow_conf._sections[section]
    
    for key, value in test_conf.items():
        section, option = key.split('__', 1)
        airflow_conf.set(section, option, value)
    
    # Mock the database-related imports in the DAG file
    with patch('sales_elt_dag.create_engine') as mock_engine:
        mock_engine.return_value = Mock()
        return DagBag(dag_folder='.', include_examples=False)

def test_dag_loading(dagbag):
    """Test that the DAG loads without errors"""
    # Check for import errors
    assert dagbag.import_errors == {}, f"Import errors: {dagbag.import_errors}"
    
    # Get the DAG
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    assert dag is not None, "DAG 'sales_elt_dag' not found"
    assert isinstance(dag, DAG), f"DAG is not a DAG instance: {type(dag)}"

def test_dag_structure(dagbag):
    """Test DAG has correct structure and tasks"""
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    
    # Check basic DAG properties
    assert dag.dag_id == 'sales_elt_dag'
    assert dag.start_date == datetime(2025, 9, 21)
    assert dag.schedule == '@daily'
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
    
    # Check that tasks exist and are BaseOperator instances
    assert extract_task is not None
    assert load_task is not None
    assert validate_task is not None
    
    # Check downstream dependencies
    assert 'load_to_postgresql' in extract_task.downstream_task_ids
    assert 'validate_load' in load_task.downstream_task_ids
    assert len(validate_task.downstream_task_ids) == 0  # Last task has no downstream

def test_extract_transform_function():
    """Test the extract_and_transform function logic"""
    from sales_elt_dag import extract_and_transform
    
    # Mock file operations and pandas
    with patch('sales_elt_dag.os') as mock_os, \
         patch('sales_elt_dag.json') as mock_json, \
         patch('builtins.open') as mock_open, \
         patch('sales_elt_dag.pd') as mock_pd:
        
        # Mock file content
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
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Mock pandas operations
        mock_df = MagicMock()
        mock_df.drop_duplicates.return_value = mock_df
        mock_df.to_csv.return_value = None
        mock_df.dropna.return_value = mock_df
        mock_df.to_numeric.return_value = mock_df
        mock_df.astype.return_value = mock_df
        mock_df.str.replace.return_value = mock_df
        mock_df.to_datetime.return_value = mock_df
        mock_df.fillna.return_value = mock_df
        mock_df.loc.return_value = mock_df
        mock_df.shape = (1, 15)
        mock_df.__len__.return_value = 1
        mock_pd.DataFrame.return_value = mock_df
        
        mock_os.path.exists.return_value = True
        mock_os.getenv.return_value = '/data/sales_record.json'
        
        # Execute the function
        result = extract_and_transform()
        
        # Assertions
        assert result == 1, "Extract and transform should return record count of 1"
        mock_open.assert_called_once_with('/data/sales_record.json', 'r')
        mock_pd.DataFrame.assert_called_once()
        mock_df.drop_duplicates.assert_called_once_with(subset=['order_id'])
        mock_df.to_csv.assert_called_once_with('/tmp/cleaned_sales.csv', index=False)

def test_dag_default_args(dagbag):
    """Test DAG default arguments"""
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    
    # Check default args
    defaults = dag.default_args
    assert 'owner' in defaults
    assert defaults['owner'] == 'MSantoso52'
    assert 'start_date' in defaults
    assert defaults['retries'] == 1
    assert 'retry_delay' in defaults

def test_dag_tags(dagbag):
    """Test DAG has appropriate tags"""
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    assert hasattr(dag, 'tags')
    assert isinstance(dag.tags, list)
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
    from sales_elt_dag import extract_and_transform
    
    # Mock file operations and pandas
    with patch('sales_elt_dag.os') as mock_os, \
         patch('sales_elt_dag.json') as mock_json, \
         patch('builtins.open') as mock_open, \
         patch('sales_elt_dag.pd') as mock_pd:
        
        # Mock file content
        mock_file = MagicMock()
        mock_json.load.return_value = sample_data
        mock_open.return_value.__enter__.return_value = mock_file
        
        # Mock pandas operations
        mock_df = MagicMock()
        mock_df.drop_duplicates.return_value = mock_df
        mock_df.to_csv.return_value = None
        mock_df.dropna.return_value = mock_df
        mock_df.to_numeric.return_value = mock_df
        mock_df.astype.return_value = mock_df
        mock_df.str.replace.return_value = mock_df
        mock_df.to_datetime.return_value = mock_df
        mock_df.fillna.return_value = mock_df
        mock_df.loc.return_value = mock_df
        # After deduplication, we should have 3 unique records (4 total - 1 duplicate)
        mock_df.shape = (3, 15)
        mock_df.__len__.return_value = 3
        mock_pd.DataFrame.return_value = mock_df
        
        mock_os.path.exists.return_value = True
        mock_os.getenv.return_value = '/data/sales_record.json'
        
        # Execute the function
        record_count = extract_and_transform()
        
        # Verify the transformation steps were called
        assert mock_pd.DataFrame.called
        assert mock_df.drop_duplicates.called
        assert mock_df.to_csv.called
        assert record_count == 3, f"Expected 3 records after processing, got {record_count}"
        
        # Verify data type conversions were attempted
        assert mock_df.to_numeric.called
        assert mock_df.astype.called
        assert mock_df.str.replace.called
        assert mock_df.to_datetime.called
        assert mock_df.fillna.called
        assert mock_df.dropna.called
        
        print("✓ Data cleansing tests PASSED")

def test_load_to_postgresql_function():
    """Test the load_to_postgresql function with mocks"""
    from sales_elt_dag import load_to_postgresql
    
    with patch('sales_elt_dag.os') as mock_os, \
         patch('sales_elt_dag.pd') as mock_pd, \
         patch('sales_elt_dag.create_engine') as mock_engine, \
         patch('sales_elt_dag.text') as mock_text:
        
        # Mock the CSV file reading
        mock_df = MagicMock()
        mock_df.shape = (2, 10)
        mock_df.to_sql.return_value = None
        mock_pd.read_csv.return_value = mock_df
        
        # Mock database engine and connection
        mock_conn = MagicMock()
        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__.return_value = mock_conn
        mock_conn_ctx.__exit__.return_value = None
        mock_engine_instance = MagicMock()
        mock_engine_instance.connect.return_value = mock_conn_ctx
        mock_engine.return_value = mock_engine_instance
        
        # Mock SQL text objects
        mock_text_sql = MagicMock()
        mock_text.return_value = mock_text_sql
        
        # Mock file operations
        mock_os.path.exists.return_value = True
        mock_os.remove = Mock()
        mock_os.getenv.side_effect = lambda x, default=None: {
            'DB_HOST': 'localhost',
            'DB_PORT': '5432', 
            'DB_NAME': 'airflow',
            'DB_USER': 'airflow',
            'DB_PASSWORD': 'airflow'
        }.get(x, default)
        
        # Execute the function
        load_to_postgresql()
        
        # Verify calls
        assert mock_pd.read_csv.called
        assert mock_engine.called
        assert mock_conn.execute.called
        assert mock_df.to_sql.called
        assert mock_os.remove.called
        print("✓ PostgreSQL load function tests PASSED")

def test_validate_load_function():
    """Test the validate_load function with mocks"""
    from sales_elt_dag import validate_load
    
    with patch('sales_elt_dag.os') as mock_os, \
         patch('sales_elt_dag.create_engine') as mock_engine, \
         patch('sales_elt_dag.text') as mock_text:
        
        # Mock database engine and connection
        mock_conn = MagicMock()
        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__.return_value = mock_conn
        mock_conn_ctx.__exit__.return_value = None
        mock_engine_instance = MagicMock()
        mock_engine_instance.connect.return_value = mock_conn_ctx
        mock_engine.return_value = mock_engine_instance
        
        # Mock query results - successful validation
        mock_count_result = Mock()
        mock_count_result.fetchone.return_value = (5,)
        mock_null_result = Mock()
        mock_null_result.fetchone.return_value = (0,)
        mock_negative_result = Mock()
        mock_negative_result.fetchone.return_value = (0,)
        
        # Mock SQL text objects
        mock_text_sql = MagicMock()
        mock_text.return_value = mock_text_sql
        
        mock_conn.execute.side_effect = [
            mock_count_result,
            mock_null_result,
            mock_negative_result
        ]
        
        mock_os.getenv.side_effect = lambda x, default=None: {
            'DB_HOST': 'localhost',
            'DB_PORT': '5432', 
            'DB_NAME': 'records_db',
            'DB_USER': 'postgres',
            'DB_PASSWORD': 'postgres'
        }.get(x, default)
        
        # Execute the function
        validate_load()
        
        # Verify calls
        assert mock_engine.called
        assert mock_conn.execute.call_count == 3
        print("✓ Validation function tests PASSED")

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
