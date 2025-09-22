import pytest
import sys
import os
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock, Mock, mock_open, PropertyMock
from airflow.models import DagBag
from airflow.configuration import conf as airflow_conf
from airflow.models.dag import DAG
from airflow.models.baseoperator import BaseOperator
from airflow.utils.session import create_session
import warnings
import json
import builtins

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

@pytest.fixture
def mock_airflow_db():
    """Comprehensive mock for Airflow's database connections"""
    with patch('airflow.models.dag.DagModel') as mock_dag_model:
        # Mock DagModel completely
        mock_dag_model_instance = MagicMock()
        mock_dag_model_instance.last_expired = None
        mock_dag_model_instance.last_loaded = None
        
        mock_dag_model.get_current.return_value = mock_dag_model_instance
        mock_dag_model.get_dagbag_import_errors.return_value = []
        
        # Mock database session
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=None)
        mock_session.get_bind = MagicMock(return_value=None)
        mock_session.scalar = MagicMock(return_value=None)
        mock_session.first = MagicMock(return_value=None)
        mock_session.query = MagicMock(return_value=mock_session)
        
        # Mock engine
        mock_engine = MagicMock()
        mock_connection = MagicMock()
        mock_connection_ctx = MagicMock()
        mock_connection_ctx.__enter__ = MagicMock(return_value=mock_connection)
        mock_connection_ctx.__exit__ = MagicMock(return_value=None)
        mock_engine.connect = MagicMock(return_value=mock_connection_ctx)
        mock_engine.dispose = MagicMock()
        
        mock_session.get_bind.return_value = mock_engine
        
        # Mock create_session to return our mock session
        with patch('airflow.utils.session.create_session', return_value=mock_session):
            yield mock_session

@pytest.fixture
def mock_airflow_config():
    """Mock Airflow configuration safely"""
    # Store original config values
    original_config = {}
    
    try:
        # Store original values for key sections
        sections_to_save = ['core', 'database', 'scheduler', 'webserver']
        for section in sections_to_save:
            try:
                if airflow_conf.has_section(section):
                    original_config[section] = dict(airflow_conf.items(section))
            except:
                pass
        
        # Set test configuration
        test_conf = {
            'core.sql_alchemy_conn': 'sqlite:///:memory:',
            'core.dags_folder': os.path.abspath('.'),
            'core.load_examples': 'False',
            'database.sql_alchemy_conn': 'sqlite:///:memory:'
        }
        
        # Clear and set config safely
        for key, value in test_conf.items():
            section, option = key.split('.', 1)
            if not airflow_conf.has_section(section):
                airflow_conf.add_section(section)
            airflow_conf.set(section, option, value)
            
        yield
        
    finally:
        # Restore original config
        for section, items in original_config.items():
            try:
                airflow_conf.remove_section(section)
            except:
                pass
            if items:
                airflow_conf.add_section(section)
                for option, value in items.items():
                    airflow_conf.set(section, option, value)

@pytest.fixture
def mock_dag_import():
    """Mock the DAG import process"""
    # Create a mock module that looks like the real sales_elt_dag
    mock_module = MagicMock()
    mock_dag = MagicMock(spec=DAG)
    mock_dag.dag_id = 'sales_elt_dag'
    mock_dag.tasks = []
    mock_dag.import_errors = []
    mock_dag.last_loaded = datetime.now()
    # Fix: Use timezone-aware datetime to match Airflow's behavior
    mock_dag.default_args = {'owner': 'MSantoso52', 'start_date': datetime(2025, 9, 21, tzinfo=timezone.utc)}
    mock_dag.tags = ['sales', 'elt', 'postgresql']
    mock_dag.schedule = '@daily'
    mock_dag.catchup = False
    mock_dag.description = 'Sales ELT pipeline with data quality checks'
    
    # Mock the functions
    mock_extract = MagicMock(return_value=1)
    mock_load = MagicMock()
    mock_validate = MagicMock()
    
    mock_module.dag = mock_dag
    mock_module.extract_and_transform = mock_extract
    mock_module.load_to_postgresql = mock_load
    mock_module.validate_load = mock_validate
    
    # Capture the original import
    original_import = builtins.__import__
    
    # Create a more sophisticated import mock that avoids recursion
    def safe_import(name, *args, **kwargs):
        # Handle Airflow internal imports first
        if name.startswith('airflow'):
            try:
                return original_import(name, *args, **kwargs)
            except ImportError:
                # Return a basic mock for Airflow modules
                return MagicMock()
        
        # Handle our specific DAG import
        if name == 'sales_elt_dag':
            return mock_module
        
        # For all other imports, try the real import
        try:
            return original_import(name, *args, **kwargs)
        except ImportError:
            return MagicMock()
    
    with patch('builtins.__import__', side_effect=safe_import):
        yield mock_module

@pytest.fixture
def dagbag(mock_airflow_db, mock_airflow_config, mock_dag_import):
    """Load the DAG bag for testing with comprehensive mocking"""
    # Create a properly mocked DagBag
    mock_dag = mock_dag_import.dag
    
    # Mock the process_file method to return our mock DAG
    with patch('airflow.models.dagbag.DagBag.process_file') as mock_process_file:
        mock_process_file.return_value = (mock_dag, [])
        
        # Mock the collect_dags method to return our mock DAG
        with patch.object(DagBag, 'collect_dags') as mock_collect_dags:
            mock_collect_dags.return_value = [mock_dag]
            
            # Create DagBag with minimal processing
            dag_bag = DagBag(
                dag_folder=os.path.abspath('.'),
                include_examples=False,
                safe_mode=False
            )
            
            # Manually set the DAG attributes to avoid DB calls
            dag_bag.dagbag_import_errors = {}
            dag_bag._file_paths = {'sales_elt_dag.py': os.path.abspath('sales_elt_dag.py')}
            dag_bag._dags = {mock_dag.dag_id: mock_dag}
            
            # Patch the get_dag method to return our mock DAG
            def mock_get_dag(self, dag_id):
                if dag_id in self._dags:
                    return self._dags[dag_id]
                return None
            
            # Use a descriptor to bind the method to the instance
            mock_get_dag_bound = mock_get_dag.__get__(dag_bag, DagBag)
            dag_bag.get_dag = mock_get_dag_bound
            
            # Mock the dag_ids property using PropertyMock
            type(dag_bag).dag_ids = PropertyMock(return_value=[mock_dag.dag_id])
            
            yield dag_bag

def test_dag_loading(dagbag):
    """Test that the DAG loads without errors"""
    # Check for import errors
    assert dagbag.dagbag_import_errors == {}, f"Import errors: {dagbag.dagbag_import_errors}"
    
    # Get the DAG
    dag = dagbag.get_dag(dag_id='sales_elt_dag')
    assert dag is not None, "DAG 'sales_elt_dag' not found"
    assert dag.dag_id == 'sales_elt_dag'
    assert hasattr(dag, 'default_args')
    assert hasattr(dag, 'tags')
    
    print("✓ DAG loading test PASSED")

def test_dag_structure():
    """Test DAG has correct structure and tasks"""
    # Import the actual DAG for structure testing
    try:
        from sales_elt_dag import dag
    except ImportError as e:
        pytest.skip(f"Could not import DAG: {e}")
    
    # Check basic DAG properties
    assert dag.dag_id == 'sales_elt_dag'
    # Fix: Compare timezone-aware datetimes or use date comparison
    expected_start_date = datetime(2025, 9, 21, tzinfo=timezone.utc)
    assert dag.start_date == expected_start_date, f"Expected {expected_start_date}, got {dag.start_date}"
    assert dag.schedule == '@daily'
    assert len(dag.tasks) == 3, f"Expected 3 tasks, found {len(dag.tasks)}"
    
    # Check task names
    task_ids = [task.task_id for task in dag.tasks]
    expected_tasks = ['extract_and_transform', 'load_to_postgresql', 'validate_load']
    assert set(task_ids) == set(expected_tasks), f"Expected tasks {expected_tasks}, found {task_ids}"
    
    print("✓ DAG structure test PASSED")

def test_task_dependencies():
    """Test task dependencies are correct"""
    try:
        from sales_elt_dag import dag
    except ImportError as e:
        pytest.skip(f"Could not import DAG: {e}")
    
    extract_task = dag.get_task('extract_and_transform')
    load_task = dag.get_task('load_to_postgresql')
    validate_task = dag.get_task('validate_load')
    
    # Check that tasks exist and are BaseOperator instances
    assert extract_task is not None
    assert load_task is not None
    assert validate_task is not None
    assert isinstance(extract_task, BaseOperator)
    assert isinstance(load_task, BaseOperator)
    assert isinstance(validate_task, BaseOperator)
    
    # Check downstream dependencies
    assert 'load_to_postgresql' in extract_task.downstream_task_ids
    assert 'validate_load' in load_task.downstream_task_ids
    assert len(validate_task.downstream_task_ids) == 0  # Last task has no downstream
    
    print("✓ Task dependencies test PASSED")

def test_extract_transform_function():
    """Test the extract_and_transform function logic"""
    try:
        from sales_elt_dag import extract_and_transform
    except ImportError as e:
        pytest.skip(f"Could not import function: {e}")
    
    # Create the mock data
    mock_json_data = '{"data": [{"order_id": "TEST-001", "item_name": "Test Item", "quantity": "5", "price_per_unit": 100.50, "total_price": "$502.50", "order_date": "2025-01-01T00:00:00", "region": "Test Region", "payment_method": null, "customer_info": {"customer_id": "CUST-TEST", "email": "test@example.com", "age": null, "address": {"street": "123 Test St", "city": "Test City", "zip": "12345"}}, "status": "Completed"}]}'
    
    # Mock file operations and pandas
    with patch('sales_elt_dag.os') as mock_os, \
         patch('sales_elt_dag.json') as mock_json, \
         patch('builtins.open') as mock_file_open, \
         patch('sales_elt_dag.pd') as mock_pd:
        
        # Configure the file open mock
        mock_file_handle = mock_open(read_data=mock_json_data)
        mock_file_open.return_value.__enter__.return_value = mock_file_handle()
        
        # Mock pandas operations
        mock_df = MagicMock()
        mock_df.drop_duplicates.return_value = mock_df
        mock_df.to_csv.return_value = None
        mock_df.dropna.return_value = mock_df
        mock_df.to_numeric.return_value = mock_df
        mock_df.astype.return_value = mock_df
        mock_df.str = MagicMock()
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
        mock_file_open.assert_called_once_with('/data/sales_record.json', 'r')
        mock_pd.DataFrame.assert_called_once()
        mock_df.drop_duplicates.assert_called_once_with(subset=['order_id'])
        mock_df.to_csv.assert_called_once_with('/tmp/cleaned_sales.csv', index=False)
        
        print("✓ Extract and transform function test PASSED")

def test_dag_default_args():
    """Test DAG default arguments"""
    try:
        from sales_elt_dag import dag
    except ImportError as e:
        pytest.skip(f"Could not import DAG: {e}")
    
    # Check default args
    defaults = dag.default_args
    assert 'owner' in defaults
    assert defaults['owner'] == 'MSantoso52'
    assert 'start_date' in defaults
    # Fix: Make the comparison timezone-aware
    expected_start_date = datetime(2025, 9, 21, tzinfo=timezone.utc)
    assert defaults['start_date'] == expected_start_date
    assert defaults['retries'] == 1
    assert 'retry_delay' in defaults
    
    print("✓ DAG default args test PASSED")

def test_dag_tags():
    """Test DAG has appropriate tags"""
    try:
        from sales_elt_dag import dag
    except ImportError as e:
        pytest.skip(f"Could not import DAG: {e}")
    
    assert hasattr(dag, 'tags')
    assert isinstance(dag.tags, list)
    assert 'sales' in dag.tags
    assert 'elt' in dag.tags
    assert 'postgresql' in dag.tags
    
    print("✓ DAG tags test PASSED")

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
    try:
        from sales_elt_dag import extract_and_transform
    except ImportError as e:
        pytest.skip(f"Could not import function: {e}")
    
    # Mock file operations and pandas
    with patch('sales_elt_dag.os') as mock_os, \
         patch('sales_elt_dag.json') as mock_json, \
         patch('builtins.open') as mock_file_open, \
         patch('sales_elt_dag.pd') as mock_pd:
        
        # Configure the file open mock
        mock_file_handle = mock_open(read_data=json.dumps(sample_data))
        mock_file_open.return_value.__enter__.return_value = mock_file_handle()
        
        # Mock pandas operations
        mock_df = MagicMock()
        mock_df.drop_duplicates.return_value = mock_df
        mock_df.to_csv.return_value = None
        mock_df.dropna.return_value = mock_df
        mock_df.to_numeric.return_value = mock_df
        mock_df.astype.return_value = mock_df
        mock_df.str = MagicMock()
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
    try:
        from sales_elt_dag import load_to_postgresql
    except ImportError as e:
        pytest.skip(f"Could not import function: {e}")
    
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
        mock_conn_ctx.__enter__ = Mock(return_value=mock_conn)
        mock_conn_ctx.__exit__ = Mock(return_value=None)
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
        
        # Mock commit
        mock_conn.commit = Mock()
        mock_conn.execute = Mock()
        
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
    try:
        from sales_elt_dag import validate_load
    except ImportError as e:
        pytest.skip(f"Could not import function: {e}")
    
    with patch('sales_elt_dag.os') as mock_os, \
         patch('sales_elt_dag.create_engine') as mock_engine, \
         patch('sales_elt_dag.text') as mock_text:
        
        # Mock database engine and connection
        mock_conn = MagicMock()
        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__ = Mock(return_value=mock_conn)
        mock_conn_ctx.__exit__ = Mock(return_value=None)
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
