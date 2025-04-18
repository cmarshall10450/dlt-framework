"""Tests for core DLT Medallion Framework functionality."""

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from dlt_framework.core.config import ConfigurationManager, ConfigurationError
from dlt_framework.core.exceptions import DecoratorError
from dlt_framework.core.registry import DecoratorRegistry
from dlt_framework.decorators import medallion


@pytest.fixture
def sample_config():
    """Create a sample configuration for testing."""
    return {
        "table": {
            "name": "test_table",
            "database": "test_db",
            "schema": "test_schema"
        }
    }


def test_config_manager():
    """Test configuration manager functionality."""
    config = {
        "table": {
            "name": "test_table",
            "database": "test_db",
            "schema": "test_schema"
        }
    }
    
    config_manager = ConfigurationManager()
    config_manager.update_config(config)
    assert config_manager.get_config() == config


def test_decorator_registry():
    """Test decorator registry functionality."""
    registry = DecoratorRegistry()
    
    # Test registration
    registry.register("test_decorator", lambda x: x)
    assert "test_decorator" in registry._decorators
    
    # Test duplicate registration
    with pytest.raises(DecoratorError):
        registry.register("test_decorator", lambda x: x)
    
    # Test dependency resolution
    registry.register("dep1", lambda x: x, dependencies=["test_decorator"])
    ordered = registry.resolve_dependencies(["dep1"])
    assert ordered == ["test_decorator", "dep1"]


def test_medallion_decorator(spark_session):
    """Test medallion decorator functionality."""
    # Create a sample DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, "test"), (2, "test2")]
    df = spark_session.createDataFrame(data, schema)

    # Create a decorated function
    @medallion(layer="bronze")
    def test_function(df):
        return df

    # Test the decorated function
    result_df = test_function(df)
    assert result_df.count() == 2


def test_medallion_decorator_with_config(spark_session, sample_config):
    """Test medallion decorator with configuration."""
    # Create a sample DataFrame
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True)
    ])
    data = [(1, "test"), (2, "test2")]
    df = spark_session.createDataFrame(data, schema)

    # Create a decorated function with configuration
    @medallion(layer="bronze", options=sample_config["table"])
    def test_function(df):
        return df

    # Test the decorated function
    result_df = test_function(df)
    assert result_df.count() == 2


def test_invalid_layer():
    """Test medallion decorator with invalid layer."""
    with pytest.raises(ConfigurationError):
        @medallion(layer="invalid_layer")
        def test_function(df):
            return df 