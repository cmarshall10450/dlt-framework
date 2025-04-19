"""Tests for batch and metadata management functionality."""

import pytest
from datetime import datetime
from unittest.mock import MagicMock, patch
from dataclasses import asdict

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from dlt_framework.metadata.batch import BatchManager, SourceMetadata


@pytest.fixture
def spark_session():
    """Create a SparkSession for testing."""
    return (SparkSession.builder
            .appName("MetadataTests")
            .master("local[1]")
            .getOrCreate())


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
        (2, "test2", 200),
        (3, "test3", 300)
    ]
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def source_metadata():
    """Create sample source metadata."""
    return SourceMetadata(
        source_type="file",
        source_path="/data/raw/test.parquet",
        format="parquet",
        partition_values={"date": "2024-03-20"},
        watermark_column="timestamp",
        timestamp=datetime.now(),
        additional_info={"owner": "test_team"}
    )


def test_source_metadata():
    """Test SourceMetadata initialization and attributes."""
    metadata = SourceMetadata(
        source_type="file",
        source_path="/path/to/file",
        format="csv"
    )
    
    assert metadata.source_type == "file"
    assert metadata.source_path == "/path/to/file"
    assert metadata.format == "csv"
    assert metadata.partition_values is None
    assert metadata.watermark_column is None
    assert metadata.timestamp is None
    assert metadata.additional_info is None


@patch('dlt.get_pipeline_context')
def test_get_pipeline_batch_id(mock_get_context):
    """Test getting batch ID from DLT pipeline context."""
    # Mock DLT pipeline context
    mock_context = MagicMock()
    mock_context.pipeline_id = "123"
    mock_context.pipeline_run_id = "456"
    mock_get_context.return_value = mock_context
    
    batch_id = BatchManager.get_pipeline_batch_id()
    assert batch_id == "pipeline_123_run_456"
    
    # Test fallback when DLT context is not available
    mock_get_context.side_effect = Exception("No DLT context")
    batch_id = BatchManager.get_pipeline_batch_id()
    assert batch_id.startswith("batch_")
    assert len(batch_id) > 6


def test_apply_batch_metadata(spark_session, sample_df, source_metadata):
    """Test applying batch and source metadata to DataFrame."""
    # Apply metadata with specific batch ID
    df_with_meta = BatchManager.apply_batch_metadata(
        sample_df,
        source_metadata,
        "test_batch_001"
    )
    
    # Verify metadata columns exist
    assert BatchManager.BATCH_ID_COLUMN in df_with_meta.columns
    assert BatchManager.SOURCE_PATH_COLUMN in df_with_meta.columns
    assert BatchManager.SOURCE_TYPE_COLUMN in df_with_meta.columns
    assert BatchManager.SOURCE_FORMAT_COLUMN in df_with_meta.columns
    assert BatchManager.PROCESS_TIMESTAMP_COLUMN in df_with_meta.columns
    
    # Verify partition columns
    for col in source_metadata.partition_values.keys():
        assert f"_partition_{col}" in df_with_meta.columns
    
    # Verify additional info columns
    for key in source_metadata.additional_info.keys():
        assert f"_meta_{key}" in df_with_meta.columns
    
    # Check values
    row = df_with_meta.select(
        BatchManager.BATCH_ID_COLUMN,
        BatchManager.SOURCE_PATH_COLUMN,
        BatchManager.SOURCE_TYPE_COLUMN,
        BatchManager.SOURCE_FORMAT_COLUMN,
        "_partition_date",
        "_meta_owner"
    ).first()
    
    assert row[BatchManager.BATCH_ID_COLUMN] == "test_batch_001"
    assert row[BatchManager.SOURCE_PATH_COLUMN] == source_metadata.source_path
    assert row[BatchManager.SOURCE_TYPE_COLUMN] == source_metadata.source_type
    assert row[BatchManager.SOURCE_FORMAT_COLUMN] == source_metadata.format
    assert row["_partition_date"] == source_metadata.partition_values["date"]
    assert row["_meta_owner"] == source_metadata.additional_info["owner"]


def test_get_source_metadata(spark_session, sample_df, source_metadata):
    """Test extracting source metadata from DataFrame."""
    # Apply metadata first
    df_with_meta = BatchManager.apply_batch_metadata(
        sample_df,
        source_metadata,
        "test_batch_002"
    )
    
    # Extract metadata
    metadata = BatchManager.get_source_metadata(df_with_meta)
    
    # Verify extracted metadata
    assert metadata["batch_id"] == "test_batch_002"
    assert metadata["source_path"] == source_metadata.source_path
    assert metadata["source_type"] == source_metadata.source_type
    assert metadata["source_format"] == source_metadata.format
    assert "process_timestamp" in metadata
    
    # Verify partitions
    assert "partitions" in metadata
    assert metadata["partitions"]["date"] == source_metadata.partition_values["date"]
    
    # Verify additional info
    assert "additional_info" in metadata
    assert metadata["additional_info"]["owner"] == source_metadata.additional_info["owner"]


def test_empty_metadata(spark_session, sample_df):
    """Test handling of minimal metadata."""
    minimal_metadata = SourceMetadata(
        source_type="table",
        source_path="catalog.schema.table"
    )
    
    # Apply minimal metadata
    df_with_meta = BatchManager.apply_batch_metadata(
        sample_df,
        minimal_metadata
    )
    
    # Verify only required columns are added
    assert BatchManager.BATCH_ID_COLUMN in df_with_meta.columns
    assert BatchManager.SOURCE_PATH_COLUMN in df_with_meta.columns
    assert BatchManager.SOURCE_TYPE_COLUMN in df_with_meta.columns
    assert BatchManager.PROCESS_TIMESTAMP_COLUMN in df_with_meta.columns
    
    # Verify optional columns are not added
    assert BatchManager.SOURCE_FORMAT_COLUMN not in df_with_meta.columns
    assert not any(c.startswith("_partition_") for c in df_with_meta.columns)
    assert not any(c.startswith("_meta_") for c in df_with_meta.columns)
    
    # Extract and verify metadata
    metadata = BatchManager.get_source_metadata(df_with_meta)
    assert "source_format" not in metadata
    assert "partitions" not in metadata
    assert "additional_info" not in metadata 