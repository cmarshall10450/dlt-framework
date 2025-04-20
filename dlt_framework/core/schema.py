"""Schema utilities for the DLT Medallion Framework.

This module provides functions for working with DataFrame schemas,
including for quarantine tables and data validation.
"""

from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType

from ..config.models import QuarantineConfig


def get_quarantine_metadata_schema() -> StructType:
    """Get the schema for quarantine metadata.
    
    Returns:
        StructType schema for quarantine metadata
    """
    return StructType([
        StructField("quarantine_timestamp", TimestampType(), False),
        StructField("source_table", StringType(), False),
        StructField("failed_expectations", ArrayType(
            StructType([
                StructField("name", StringType(), False),
                StructField("constraint", StringType(), False)
            ])
        ), False),
        StructField("error_details", StringType(), True),
        StructField("batch_id", StringType(), True)
    ])


def get_quarantine_schema_from_config(config: QuarantineConfig) -> StructType:
    """Get the quarantine schema based on a configuration.
    
    Args:
        config: QuarantineConfig object
        
    Returns:
        StructType schema customized for the quarantine config
    """
    return StructType([
        StructField(config.timestamp_column, TimestampType(), False),
        StructField(config.source_column, StringType(), False),
        StructField(config.failed_expectations_column, ArrayType(
            StructType([
                StructField("name", StringType(), False),
                StructField("constraint", StringType(), False)
            ])
        ), False),
        StructField(config.error_column, StringType(), True),
        StructField(config.batch_id_column, StringType(), True)
    ])
