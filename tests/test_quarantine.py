"""Tests for quarantine functionality in the DLT Medallion Framework."""

import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from dlt_framework.core.quarantine import QuarantineConfig, QuarantineManager


@pytest.fixture
def spark_session():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .appName("QuarantineTests")
            .master("local[1]")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())


@pytest.fixture
def quarantine_config():
    """Create a QuarantineConfig for testing."""
    return QuarantineConfig(
        table_name="catalog.schema.quarantine",
        error_column="_error",
        timestamp_column="_ts",
        batch_id_column="_batch",
        source_column="_source"
    )


@pytest.fixture
def sample_df(spark_session):
    """Create a sample DataFrame for testing."""
    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("value", IntegerType(), True)
    ])
    data = [
        (1, "test1", 100),
        (2, "test2", -50),
        (None, "test3", 75)
    ]
    return spark_session.createDataFrame(data, schema)


def test_quarantine_config():
    """Test QuarantineConfig initialization."""
    config = QuarantineConfig(table_name="catalog.schema.table")
    assert config.table_name == "catalog.schema.table"
    assert config.error_column == "_error_details"
    
    config = QuarantineConfig(
        table_name="catalog.schema.table",
        error_column="custom_error",
        timestamp_column="custom_ts"
    )
    assert config.table_name == "catalog.schema.table"
    assert config.error_column == "custom_error"


@patch('dlt.table')
def test_quarantine_manager_init(mock_dlt_table, quarantine_config):
    """Test QuarantineManager initialization."""
    manager = QuarantineManager(quarantine_config)
    
    # Verify DLT table decorator was called
    mock_dlt_table.assert_called_once_with(
        comment="Quarantine table for invalid records"
    )


def test_prepare_quarantine_records(spark_session, quarantine_config, sample_df):
    """Test preparing records for quarantine with different error detail types."""
    manager = QuarantineManager(quarantine_config)
    
    # Test with string error
    quarantine_df = manager.prepare_quarantine_records(
        sample_df,
        "Test error message",
        "test_source",
        "batch_1"
    )
    
    # Verify metadata columns
    assert quarantine_config.error_column in quarantine_df.columns
    assert quarantine_config.timestamp_column in quarantine_df.columns
    assert quarantine_config.batch_id_column in quarantine_df.columns
    assert quarantine_config.source_column in quarantine_df.columns
    
    # Verify values
    row = quarantine_df.select(
        quarantine_config.error_column,
        quarantine_config.source_column,
        quarantine_config.batch_id_column
    ).first()
    
    assert row[quarantine_config.error_column] == "Test error message"
    assert row[quarantine_config.source_column] == "test_source"
    assert row[quarantine_config.batch_id_column] == "batch_1"
    
    # Test with column error
    error_col = F.when(F.col("value") < 0, "Negative value").otherwise(None)
    quarantine_df = manager.prepare_quarantine_records(
        sample_df,
        error_col,
        "test_source",
        "batch_2"
    )
    
    # Verify column-based error
    negative_value_rows = quarantine_df.filter(F.col("value") < 0)
    assert negative_value_rows.count() == 1
    assert negative_value_rows.first()[quarantine_config.error_column] == "Negative value"


def test_get_quarantined_records_filters(spark_session, quarantine_config, sample_df):
    """Test retrieving quarantined records with various filters."""
    manager = QuarantineManager(quarantine_config)
    
    # Mock the quarantine table
    mock_table = sample_df.withColumn(
        quarantine_config.error_column,
        F.lit("Test error")
    ).withColumn(
        quarantine_config.timestamp_column,
        F.current_timestamp()
    ).withColumn(
        quarantine_config.source_column,
        F.lit("source1")
    ).withColumn(
        quarantine_config.batch_id_column,
        F.lit("batch1")
    )
    
    # Create a second batch
    mock_table2 = sample_df.withColumn(
        quarantine_config.error_column,
        F.lit("Test error")
    ).withColumn(
        quarantine_config.timestamp_column,
        F.current_timestamp()
    ).withColumn(
        quarantine_config.source_column,
        F.lit("source2")
    ).withColumn(
        quarantine_config.batch_id_column,
        F.lit("batch2")
    )
    
    # Union the tables
    mock_table = mock_table.union(mock_table2)
    
    # Mock spark.table() to return our mock table
    spark_session.table = MagicMock(return_value=mock_table)
    
    # Test source filter
    source1_records = manager.get_quarantined_records(
        spark_session,
        source_table="source1"
    )
    assert source1_records.count() == 3
    assert all(row[quarantine_config.source_column] == "source1" 
              for row in source1_records.collect())
    
    # Test batch filter
    batch2_records = manager.get_quarantined_records(
        spark_session,
        batch_id="batch2"
    )
    assert batch2_records.count() == 3
    assert all(row[quarantine_config.batch_id_column] == "batch2" 
              for row in batch2_records.collect())


def test_reprocess_records(spark_session, quarantine_config, sample_df):
    """Test reprocessing quarantined records."""
    manager = QuarantineManager(quarantine_config)
    
    # Prepare quarantined records
    quarantine_df = manager.prepare_quarantine_records(
        sample_df,
        "Initial error",
        "test_source"
    )
    
    # Define validation function
    def validate_records(df):
        return df.filter(
            (F.col("id").isNotNull()) & 
            (F.col("value") > 0)
        )
    
    # Define error handler
    error_records = []
    def error_handler(df, error=None):
        error_records.extend(df.collect())
    
    # Reprocess records
    valid, invalid = manager.reprocess_records(
        quarantine_df,
        validate_records,
        error_handler
    )
    
    # Verify results
    assert valid.count() == 1  # Only id=1 record is valid
    assert invalid.count() == 2  # id=2 (negative value) and id=None records
    assert len(error_records) == 2


def test_error_summary(spark_session, quarantine_config, sample_df):
    """Test generating error summaries."""
    manager = QuarantineManager(quarantine_config)
    
    # Prepare mock quarantine data
    mock_table = (
        sample_df.filter(F.col("id").isNull())
        .withColumn(quarantine_config.error_column, F.lit("Null ID"))
        .withColumn(quarantine_config.source_column, F.lit("source1"))
        .withColumn(quarantine_config.timestamp_column, F.current_timestamp())
    ).union(
        sample_df.filter(F.col("value") < 0)
        .withColumn(quarantine_config.error_column, F.lit("Negative value"))
        .withColumn(quarantine_config.source_column, F.lit("source2"))
        .withColumn(quarantine_config.timestamp_column, F.current_timestamp())
    )
    
    # Mock spark.table() to return our mock table
    spark_session.table = MagicMock(return_value=mock_table)
    
    # Get summary
    summary = manager.get_error_summary(spark_session)
    summary_rows = summary.collect()
    
    # Verify summary statistics
    assert len(summary_rows) == 2  # Two distinct error types
    
    # Check error counts
    null_id_error = next(row for row in summary_rows 
                        if row[quarantine_config.error_column] == "Null ID")
    assert null_id_error["error_count"] == 1
    
    negative_value_error = next(row for row in summary_rows 
                              if row[quarantine_config.error_column] == "Negative value")
    assert negative_value_error["error_count"] == 1


def test_prepare_quarantine_records_with_override_source(spark_session, sample_df):
    """Test preparing quarantine records with an overridden source table name."""
    config = QuarantineConfig(
        enabled=True,
        source_table_name="default_source"
    )
    manager = QuarantineManager(config)
    
    # Test with override source
    override_source = "override_source"
    result = manager.prepare_quarantine_records(
        sample_df,
        "test error",
        source_table_name=override_source
    )
    
    # Verify override source is used
    source_value = result.select(config.source_column).collect()[0][0]
    assert source_value == override_source


def test_prepare_quarantine_records_with_none_source(spark_session, sample_df):
    """Test preparing quarantine records with no source table name."""
    config = QuarantineConfig(
        enabled=True,
        source_table_name=None
    )
    manager = QuarantineManager(config)
    
    # Test with no source
    result = manager.prepare_quarantine_records(
        sample_df,
        "test error"
    )
    
    # Verify source is None
    source_value = result.select(config.source_column).collect()[0][0]
    assert source_value is None
    
    # Test with explicit None override
    result = manager.prepare_quarantine_records(
        sample_df,
        "test error",
        source_table_name=None
    )
    
    # Verify source remains None
    source_value = result.select(config.source_column).collect()[0][0]
    assert source_value is None 