import pytest
import sys
import os
from datetime import datetime, timedelta
from unittest.mock import patch, MagicMock, Mock, mock_open
from airflow.models import DagBag
from airflow.configuration import conf as airflow_conf
from airflow.models.dag import DAG
import json
import pandas as pd

# Add current directory to Python path for imports
sys.path.insert(0, os.path.dirname(__file__))

# Import the functions from the DAG file
from sales_elt_dag import extract_and_transform, load_to_postgresql, validate_load

@pytest.fixture
def mock_airflow_db():
    """Comprehensive mock for Airflow's database connections"""
    with patch('airflow.models.dag.DagModel') as mock_dag_model:
        mock_dag_model_instance = MagicMock()
        mock_dag_model_instance.last_expired = None
        mock_dag_model_instance.last_loaded = None
        
        mock_dag_model.get_current.return_value = mock_dag_model_instance
        mock_dag_model.get_dagbag_import_errors.return_value = []
        
        mock_session = MagicMock()
        mock_session.__enter__ = MagicMock(return_value=mock_session)
        mock_session.__exit__ = MagicMock(return_value=None)
        mock_session.get_bind = MagicMock(return_value=None)
        
        with patch('airflow.utils.session.create_session', return_value=mock_session):
            yield

@pytest.fixture(autouse=True)
def mock_env_vars():
    """Mock environment variables for database connection and file path"""
    with patch.dict(os.environ, {
        'DB_HOST': 'test-db-host',
        'DB_PORT': '5432',
        'DB_NAME': 'test-db-name',
        'DB_USER': 'test-user',
        'DB_PASSWORD': 'test-password',
        'SALES_DATA_PATH': '/tmp/test_sales_record.json'
    }):
        yield

# Helper function to get DAG
def get_dag(dag_id):
    """Retrieve a specific DAG from the DagBag"""
    dag_folder = os.path.dirname(os.path.abspath(__file__))
    dagbag = DagBag(dag_folder, include_examples=False)
    return dagbag.dags.get(dag_id)

class TestDagIntegrity:
    """Validate DAG loading and structure"""
    def test_dag_loading(self, mock_airflow_db):
        """Test that the DAG loads without any import errors"""
        dag_folder = os.path.dirname(os.path.abspath(__file__))
        dag_name = 'sales_elt_dag'
        sys.path.append(dag_folder)
        try:
            dagbag = DagBag(dag_folder, include_examples=False)
            assert dag_name in dagbag.dags
            assert len(dagbag.import_errors) == 0, f"Import errors: {dagbag.import_errors}"
        finally:
            sys.path.remove(dag_folder)

    def test_dag_structure(self, mock_airflow_db):
        """Test that the DAG has the expected number of tasks"""
        dag = get_dag('sales_elt_dag')
        assert dag is not None
        assert len(dag.tasks) == 3

    def test_task_dependencies(self, mock_airflow_db):
        """Test task dependencies are correctly set"""
        dag = get_dag('sales_elt_dag')
        extract_task = dag.get_task('extract_and_transform')
        load_task = dag.get_task('load_to_postgresql')
        validate_task = dag.get_task('validate_load')

        assert 'load_to_postgresql' in extract_task.downstream_task_ids
        assert 'validate_load' in load_task.downstream_task_ids
        assert 'validate_load' not in extract_task.downstream_task_ids
        assert not validate_task.downstream_task_ids

    def test_dag_default_args(self, mock_airflow_db):
        """Test that default args are correctly applied"""
        dag = get_dag('sales_elt_dag')
        assert dag.default_args['owner'] == 'MSantoso52'
        assert dag.default_args['retries'] == 1
        assert dag.default_args['retry_delay'] == timedelta(minutes=5)
    
    def test_dag_tags(self, mock_airflow_db):
        """Test that the DAG has the correct tags"""
        dag = get_dag('sales_elt_dag')
        assert 'sales' in dag.tags
        assert 'elt' in dag.tags
        assert 'postgresql' in dag.tags
        assert len(dag.tags) == 3

@patch('sales_elt_dag.create_engine')
@patch('sales_elt_dag.os.getenv')
@patch('sales_elt_dag.pd')
class TestETLFunctions:
    """Test ETL functions with mocks"""
    def test_extract_transform_function(self, mock_pd, mock_os, mock_engine):
        """Test that extract and transform function correctly reads a file"""
        mock_os.return_value = 'mock_path/sales_record.json'
        
        # Mock file reading
        mock_json_data = {
            "data": [
                {"id": 1, "quantity": 10},
                {"id": 2, "quantity": 20}
            ]
        }
        mock_file_content = json.dumps(mock_json_data)
        
        m = mock_open(read_data=mock_file_content)
        with patch('sales_elt_dag.open', m, create=True):
            with patch('os.path.exists', return_value=True):
                mock_df_instance = MagicMock()
                mock_pd.DataFrame.return_value = mock_df_instance
                mock_df_instance.drop_duplicates.return_value = mock_df_instance
                mock_df_instance.dropna.return_value = mock_df_instance
                mock_df_instance.__getitem__.return_value = mock_df_instance # To allow chaining `df[...]`
                
                result = extract_and_transform()
                
                # Verify open was called
                m.assert_called_once_with('mock_path/sales_record.json', 'r')
                
                # Verify that the function returns an integer
                assert isinstance(result, int)

@patch('sales_elt_dag.create_engine')
@patch('sales_elt_dag.os.getenv')
@patch('sales_elt_dag.text')
@patch('sales_elt_dag.pd.read_csv')
class TestLoadFunctions:
    """Test loading and validation functions with mocks"""
    
    def test_load_to_postgresql(self, mock_read_csv, mock_text, mock_os, mock_engine):
        """Test that the load function correctly pushes data to the database"""
        # Mock the dataframe read from the temp file
        mock_df = MagicMock()
        mock_read_csv.return_value = mock_df

        # Mock database engine and connection
        mock_conn = MagicMock()
        mock_conn_ctx = MagicMock()
        mock_conn_ctx.__enter__ = Mock(return_value=mock_conn)
        mock_conn_ctx.__exit__ = Mock(return_value=None)
        mock_engine_instance = MagicMock()
        mock_engine_instance.connect.return_value = mock_conn_ctx
        mock_engine.return_value = mock_engine_instance
        
        # Mock environment variables
        mock_os.getenv.side_effect = lambda x, default=None: {
            'DB_HOST': 'test-db-host',
            'DB_PORT': '5432',
            'DB_NAME': 'test-db-name',
            'DB_USER': 'test-user',
            'DB_PASSWORD': 'test-password',
            'SALES_DATA_PATH': '/tmp/test_sales_record.json'
        }.get(x, default)
        
        # Mock os.path.exists and os.remove
        with patch('sales_elt_dag.os.path.exists', return_value=True):
            with patch('sales_elt_dag.os.remove') as mock_remove:
                load_to_postgresql()
                
                # Verify calls
                mock_engine.assert_called_once()
                assert mock_conn.execute.call_count == 2
                mock_df.to_sql.assert_called_once()
                mock_remove.assert_called_once_with('/tmp/cleaned_sales.csv')

    def test_validate_load(self, mock_read_csv, mock_text, mock_os, mock_engine):
        """Test that data quality validation checks the database correctly"""
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
        
        # Test case for failed validation
        mock_conn.execute.reset_mock()
        mock_conn.execute.side_effect = [
            mock_count_result, # Return a row count
            mock_null_result, # Return a null count
            Mock(fetchone=Mock(return_value=(1,))) # Return 1 for negative count
        ]
        
        with pytest.raises(ValueError, match="Data quality validation failed"):
            validate_load()

