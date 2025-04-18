"""Test DLT integration functionality."""
import pytest
from unittest.mock import MagicMock, patch
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from dlt_framework.core.dlt_integration import DLTIntegration
from dlt_framework.core.exceptions import DLTFrameworkError

@pytest.fixture
def mock_dlt():
    """Create a mock DLT module."""
    mock = MagicMock()
    mock.table = MagicMock()
    mock.table_property = MagicMock()
    mock.expect = MagicMock()
    mock.metrics = MagicMock()
    return mock

def test_dlt_integration_class(mock_dlt):
    """Test DLT integration class initialization and methods."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        integration = DLTIntegration()
        
        # Test table properties
        integration.set_table_property("key", "value")
        mock_dlt.table_property.assert_called_with("key", "value")
        
        # Test expectations
        expectation = {"name": "valid_id", "constraint": "id IS NOT NULL"}
        integration.add_expectation(expectation)
        mock_dlt.expect.assert_called_with("valid_id", "id IS NOT NULL")
        
        # Test metrics
        metric = {"name": "null_count", "value": "COUNT(CASE WHEN id IS NULL THEN 1 END)"}
        integration.add_metric(metric)
        mock_dlt.metrics.assert_called_with("null_count", "COUNT(CASE WHEN id IS NULL THEN 1 END)")

def test_dlt_integration_with_spark(spark_session, mock_dlt):
    """Test DLT integration with Spark DataFrame."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create a sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True)
        ])
        data = [(1, "test"), (2, "test2")]
        df = spark_session.createDataFrame(data, schema)
        
        integration = DLTIntegration()
        
        # Test invalid expectation
        with pytest.raises(DLTFrameworkError):
            integration.add_expectation({"invalid": "expectation"})
        
        # Test valid expectation
        integration.add_expectation({
            "name": "valid_id",
            "constraint": "id IS NOT NULL"
        })
        
        # Test applying expectations to DataFrame
        result_df = integration.apply_expectations(df)
        assert result_df.count() == df.count()
        
        # Test applying metrics to DataFrame
        integration.add_metric({
            "name": "total_rows",
            "value": "COUNT(*)"
        })
        result_df = integration.apply_metrics(df)
        assert result_df.count() == df.count() 