"""Test configuration and fixtures for the DLT Framework."""

import os
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark_session():
    """Create a local Spark session for testing."""
    # Set SPARK_HOME environment variable
    os.environ["SPARK_HOME"] = "/opt/homebrew/opt/apache-spark/libexec"
    
    spark = (SparkSession.builder
            .master("local[*]")
            .appName("DLTFrameworkTest")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
            .getOrCreate())
    
    yield spark
    
    # Clean up
    spark.stop()

@pytest.fixture(autouse=True)
def cleanup_spark_tables(spark_session):
    """Clean up any tables created during tests."""
    yield
    # Drop all tables after each test
    for table in spark_session.catalog.listTables():
        spark_session.sql(f"DROP TABLE IF EXISTS {table.name}")

@pytest.fixture
def sample_config():
    """Provide a sample configuration for testing."""
    return {
        "table": {
            "name": "test_table",
            "layer": "bronze",
            "description": "Test table for unit tests",
        },
        "validation": {
            "rules": [
                {
                    "name": "test_rule",
                    "column": "test_column",
                    "type": "not_null",
                }
            ]
        },
    } 