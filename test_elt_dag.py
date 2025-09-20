import pytest
from datetime import datetime
from airflow.models import DagBag
import pandas as pd
from unittest.mock import patch, mock_open

# Import the DAG file (adjust path if needed)
from sales_elt_dag import dag, elt_sales_data

@pytest.fixture
def dag_bag():
    return DagBag(include_examples=False)

def test_dag_loads(dag_bag):
    assert dag_bag.import_errors == {}, "DAG import errors found"
    assert 'sales_elt_dag' in dag_bag.dags, "DAG not found"

def test_dag_structure():
    assert len(dag.tasks) == 1, "Unexpected number of tasks"
    task = dag.get_task('elt_sales_data')
    assert task.upstream_task_ids == set(), "Unexpected dependencies"

@patch('builtins.open', mock_open(read_data='[{"order_id": "test"}]'))
@patch('pandas.DataFrame.to_csv')
def test_elt_function(mock_to_csv, mock_open):
    context = {}
    elt_sales_data(**context)
    pd.json_normalize.assert_called()  # Ensure flattening happens
    mock_to_csv.assert_called_once()  # Ensure output is saved
