"""Tests for DLT integration functionality."""

import pytest
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from dlt_framework.core.dlt_integration import DLTIntegration
from dlt_framework.decorators.base import medallion


def test_medallion_decorator_with_dlt_integration(spark_session):
    """Test that the medallion decorator correctly integrates with DLT."""
    # Create a sample DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, "test"), (2, "test2")]
    df = spark_session.createDataFrame(data, schema)
    
    # Define DLT configurations
    expectations = [
        {"name": "valid_id", "constraint": "id IS NOT NULL"}
    ]
    metrics = [
        {"name": "null_count", "value": "COUNT(*) WHERE id IS NULL"}
    ]
    table_properties = {
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
    comment = "Test table for DLT integration"
    
    # Create a decorated function
    @medallion(
        layer="bronze",
        expectations=expectations,
        metrics=metrics,
        table_properties=table_properties,
        comment=comment
    )
    def test_table() -> DataFrame:
        return df
    
    # Execute the decorated function
    result = test_table()
    
    # Verify that the DataFrame was returned
    assert isinstance(result, DataFrame)
    assert result.count() == 2
    
    # Note: We can't directly test DLT functionality in unit tests
    # as it requires a Databricks runtime environment.
    # Instead, we verify that our integration layer is called correctly
    # by checking the decorator's metadata
    from dlt_framework.core.registry import DecoratorRegistry
    registry = DecoratorRegistry()
    metadata = registry.get_metadata("medallion_test_table")
    
    assert metadata["expectations"] == expectations
    assert metadata["metrics"] == metrics
    assert metadata["table_properties"] == table_properties
    assert metadata["comment"] == comment


def test_dlt_integration_class():
    """Test the DLTIntegration class methods."""
    dlt_integration = DLTIntegration()
    
    # Test applying table properties
    properties = {"key": "value"}
    dlt_integration.apply_table_properties(properties)
    
    # Test setting table comment
    comment = "Test comment"
    dlt_integration.set_table_comment(comment)
    
    # Test with empty/None values
    dlt_integration.apply_table_properties(None)
    dlt_integration.set_table_comment(None)


def test_dlt_integration_with_spark(spark_session):
    """Test DLT integration with actual Spark DataFrames."""
    dlt_integration = DLTIntegration()
    
    # Create a sample DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, "test"), (2, "test2")]
    df = spark_session.createDataFrame(data, schema)
    
    # Test adding expectations
    expectations = [
        {"name": "valid_id", "constraint": "id IS NOT NULL"}
    ]
    df_with_expectations = dlt_integration.add_expectations(df, expectations)
    assert isinstance(df_with_expectations, DataFrame)
    
    # Test adding metrics
    metrics = [
        {"name": "null_count", "value": "COUNT(*) WHERE id IS NULL"}
    ]
    df_with_metrics = dlt_integration.add_quality_metrics(df, metrics)
    assert isinstance(df_with_metrics, DataFrame) 