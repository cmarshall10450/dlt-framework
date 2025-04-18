"""Test layer-specific decorators functionality."""
import pytest
from unittest.mock import patch
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, DoubleType

from dlt_framework.decorators.layers import bronze, silver, gold
from dlt_framework.core.registry import DecoratorRegistry

@pytest.fixture(autouse=True)
def clear_registry():
    """Clear the decorator registry before each test."""
    DecoratorRegistry().clear()

def test_bronze_decorator(spark_session, mock_dlt):
    """Test bronze layer decorator functionality."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("raw_data", StringType(), True)
        ])
        data = [(1, "test"), (2, "test2")]
        df = spark_session.createDataFrame(data, schema)
        
        # Define expectations and metrics
        expectations = [
            {"name": "valid_id", "constraint": "id IS NOT NULL"},
            {"name": "valid_raw_data", "constraint": "raw_data IS NOT NULL"}
        ]
        metrics = [
            {"name": "null_id_count", "value": "COUNT(CASE WHEN id IS NULL THEN 1 END)"}
        ]
        
        @bronze(
            expectations=expectations,
            metrics=metrics,
            table_properties={"layer": "bronze"},
            comment="Test bronze layer"
        )
        def process_bronze(df):
            return df
        
        # Test decorated function
        result_df = process_bronze(df)
        assert result_df.count() == df.count()
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(process_bronze)
        assert metadata["layer"] == "bronze"
        assert len(metadata["expectations"]) == 2
        assert len(metadata["metrics"]) == 1

def test_silver_decorator(spark_session, mock_dlt):
    """Test silver layer decorator functionality."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("email", StringType(), True)
        ])
        data = [(1, "test@example.com"), (2, "invalid")]
        df = spark_session.createDataFrame(data, schema)
        
        # Define expectations and metrics
        expectations = [
            {"name": "valid_email", "constraint": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'"}
        ]
        metrics = [
            {"name": "invalid_email_count", "value": "COUNT(CASE WHEN NOT email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$' THEN 1 END)"}
        ]
        
        @silver(
            expectations=expectations,
            metrics=metrics,
            table_properties={"layer": "silver"},
            comment="Test silver layer"
        )
        def process_silver(df):
            return df
        
        # Test decorated function
        result_df = process_silver(df)
        assert result_df.count() == df.count()
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(process_silver)
        assert metadata["layer"] == "silver"
        assert len(metadata["expectations"]) == 1
        assert len(metadata["metrics"]) == 1

def test_gold_decorator(spark_session, mock_dlt):
    """Test gold layer decorator functionality."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("date", DateType(), True),
            StructField("amount", DoubleType(), True)
        ])
        data = [(None, 100.0), (None, 200.0)]
        df = spark_session.createDataFrame(data, schema)
        
        # Define expectations and metrics
        expectations = [
            {"name": "positive_amount", "constraint": "amount > 0"}
        ]
        metrics = [
            {"name": "total_amount", "value": "SUM(amount)"}
        ]
        
        @gold(
            expectations=expectations,
            metrics=metrics,
            table_properties={"layer": "gold"},
            comment="Test gold layer"
        )
        def process_gold(df):
            return df.groupBy("date").sum("amount")
        
        # Test decorated function
        result_df = process_gold(df)
        assert result_df.count() == 1  # Grouped by date (all NULL)
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(process_gold)
        assert metadata["layer"] == "gold"
        assert len(metadata["expectations"]) == 1
        assert len(metadata["metrics"]) == 1

def test_decorator_stacking(spark_session, mock_dlt):
    """Test stacking layer decorators with other decorators."""
    with patch('dlt_framework.core.dlt_integration.dlt', mock_dlt):
        # Create sample DataFrame
        schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("value", StringType(), True)
        ])
        data = [(1, "test"), (2, "test2")]
        df = spark_session.createDataFrame(data, schema)
        
        # Define expectations and metrics
        expectations = [{"name": "valid_id", "constraint": "id IS NOT NULL"}]
        metrics = [{"name": "row_count", "value": "COUNT(*)"}]
        
        @bronze(
            expectations=expectations,
            metrics=metrics,
            table_properties={"layer": "bronze"},
            comment="Test bronze layer"
        )
        def process_data(df):
            return df
        
        # Test decorated function
        result_df = process_data(df)
        assert result_df.count() == df.count()
        
        # Verify metadata in registry
        registry = DecoratorRegistry()
        metadata = registry.get_metadata(process_data)
        assert metadata["layer"] == "bronze"
        assert len(metadata["expectations"]) == 1
        assert len(metadata["metrics"]) == 1 