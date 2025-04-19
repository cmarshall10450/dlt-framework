"""Batch and source metadata management for DLT pipelines.

This module provides functionality for:
1. Generating and applying batch IDs to processed records
2. Capturing source metadata (file paths, timestamps, etc.)
3. Tracking data lineage through the pipeline
"""

from typing import Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
import dlt

@dataclass
class SourceMetadata:
    """Metadata about the source of a DataFrame."""
    source_type: str  # e.g., 'file', 'table', 'stream'
    source_path: str  # Full path/name of the source
    format: Optional[str] = None  # e.g., 'delta', 'parquet', 'csv'
    partition_values: Optional[Dict[str, str]] = None  # Partition values if reading partitioned data
    watermark_column: Optional[str] = None  # Column name for streaming watermark
    timestamp: Optional[datetime] = None  # When the data was read
    additional_info: Optional[Dict[str, str]] = None  # Any additional metadata

class BatchManager:
    """Manages batch IDs and source metadata for DLT pipelines."""
    
    # Default column names
    BATCH_ID_COLUMN = "_batch_id"
    SOURCE_PATH_COLUMN = "_source_path"
    SOURCE_TYPE_COLUMN = "_source_type"
    SOURCE_FORMAT_COLUMN = "_source_format"
    PROCESS_TIMESTAMP_COLUMN = "_process_timestamp"
    
    @staticmethod
    def get_pipeline_batch_id() -> str:
        """Get the current DLT pipeline run ID or generate a timestamp-based ID.
        
        Returns:
            str: Batch ID for the current pipeline run
        """
        try:
            # Try to get DLT pipeline run ID
            context = dlt.get_pipeline_context()
            return f"pipeline_{context.pipeline_id}_run_{context.pipeline_run_id}"
        except:
            # Fallback to timestamp-based ID
            return f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    @staticmethod
    def apply_batch_metadata(
        df: DataFrame,
        source_metadata: SourceMetadata,
        batch_id: Optional[str] = None
    ) -> DataFrame:
        """Apply batch ID and source metadata columns to a DataFrame.
        
        Args:
            df: Input DataFrame
            source_metadata: Metadata about the source
            batch_id: Optional batch ID (will use pipeline run ID if not provided)
            
        Returns:
            DataFrame with added metadata columns
            
        Example:
            ```python
            source_meta = SourceMetadata(
                source_type='file',
                source_path='/data/raw/customers.parquet',
                format='parquet'
            )
            
            df_with_metadata = BatchManager.apply_batch_metadata(
                raw_df,
                source_meta
            )
            ```
        """
        # Use provided batch ID or get from pipeline
        batch_id = batch_id or BatchManager.get_pipeline_batch_id()
        
        # Start with batch ID
        result_df = df.withColumn(
            BatchManager.BATCH_ID_COLUMN,
            F.lit(batch_id)
        )
        
        # Add source metadata
        result_df = result_df.withColumn(
            BatchManager.SOURCE_PATH_COLUMN,
            F.lit(source_metadata.source_path)
        ).withColumn(
            BatchManager.SOURCE_TYPE_COLUMN,
            F.lit(source_metadata.source_type)
        ).withColumn(
            BatchManager.PROCESS_TIMESTAMP_COLUMN,
            F.current_timestamp()
        )
        
        # Add format if available
        if source_metadata.format:
            result_df = result_df.withColumn(
                BatchManager.SOURCE_FORMAT_COLUMN,
                F.lit(source_metadata.format)
            )
        
        # Add partition values if available
        if source_metadata.partition_values:
            for col, value in source_metadata.partition_values.items():
                result_df = result_df.withColumn(
                    f"_partition_{col}",
                    F.lit(value)
                )
        
        # Add additional info if available
        if source_metadata.additional_info:
            for key, value in source_metadata.additional_info.items():
                result_df = result_df.withColumn(
                    f"_meta_{key}",
                    F.lit(value)
                )
        
        return result_df
    
    @staticmethod
    def get_source_metadata(df: DataFrame) -> Dict[str, str]:
        """Extract source metadata from a DataFrame with metadata columns.
        
        Args:
            df: DataFrame with metadata columns
            
        Returns:
            Dictionary containing source metadata
        """
        # Get a sample row to extract metadata
        sample = df.select(
            BatchManager.BATCH_ID_COLUMN,
            BatchManager.SOURCE_PATH_COLUMN,
            BatchManager.SOURCE_TYPE_COLUMN,
            BatchManager.SOURCE_FORMAT_COLUMN,
            BatchManager.PROCESS_TIMESTAMP_COLUMN
        ).limit(1).collect()
        
        if not sample:
            return {}
            
        row = sample[0]
        
        metadata = {
            'batch_id': row[BatchManager.BATCH_ID_COLUMN],
            'source_path': row[BatchManager.SOURCE_PATH_COLUMN],
            'source_type': row[BatchManager.SOURCE_TYPE_COLUMN],
            'process_timestamp': str(row[BatchManager.PROCESS_TIMESTAMP_COLUMN])
        }
        
        # Add format if present
        if BatchManager.SOURCE_FORMAT_COLUMN in df.columns:
            metadata['source_format'] = row[BatchManager.SOURCE_FORMAT_COLUMN]
        
        # Add any partition columns
        partition_cols = [c for c in df.columns if c.startswith('_partition_')]
        if partition_cols:
            partition_sample = df.select(*partition_cols).limit(1).collect()[0]
            metadata['partitions'] = {
                c.replace('_partition_', ''): partition_sample[c]
                for c in partition_cols
            }
        
        # Add any additional metadata
        meta_cols = [c for c in df.columns if c.startswith('_meta_')]
        if meta_cols:
            meta_sample = df.select(*meta_cols).limit(1).collect()[0]
            metadata['additional_info'] = {
                c.replace('_meta_', ''): meta_sample[c]
                for c in meta_cols
            }
        
        return metadata 