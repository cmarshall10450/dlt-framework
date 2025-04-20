"""Table operations for quarantine functionality.

This module provides functions for creating and managing quarantine tables.
"""

from typing import Dict, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame

from dlt_framework.config import QuarantineConfig
from .schema import get_quarantine_schema_from_config


def create_quarantine_table(
    table_name: str,
    config: Optional[QuarantineConfig] = None
) -> None:
    """Create a quarantine table if it doesn't exist.
    
    Args:
        table_name: Name of the quarantine table to create
        config: Optional configuration for custom column names
        
    Raises:
        RuntimeError: If no active Spark session is found
    """
    # Get active Spark session
    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active Spark session found")
    
    # Determine schema based on config
    if config:
        schema = get_quarantine_schema_from_config(config)
    else:
        # Use default schema
        from .schema import get_quarantine_metadata_schema
        schema = get_quarantine_metadata_schema()
    
    # Create empty DataFrame with the schema
    empty_df = spark.createDataFrame([], schema)
    
    # Create the quarantine table using dlt.create_table
    dlt.create_table(
        name=table_name,
        comment="Quarantined records that failed data quality expectations",
        path=None,  # Let DLT manage the path
        partition_cols=None,
        table_properties={
            "quality": "bronze",  # Mark as bronze since it's raw rejected data
            "purpose": "data_quality_quarantine"
        }
    )
    
    # Write initial empty schema
    sequence_by = config.timestamp_column if config else "quarantine_timestamp"
    dlt.apply_changes(
        target=table_name,
        source=empty_df,
        keys=[],
        sequence_by=sequence_by
    )


def write_to_quarantine_table(
    df: DataFrame,
    table_name: str,
    sequence_by: str = "quarantine_timestamp"
) -> None:
    """Write records to the quarantine table.
    
    Args:
        df: DataFrame containing records to quarantine
        table_name: Name of the quarantine table
        sequence_by: Column name to sequence by
    """
    if df.isEmpty():
        return  # Nothing to write
    
    dlt.apply_changes(
        target=table_name,
        source=df,
        keys=[],  # No natural keys for quarantine records
        sequence_by=sequence_by,
        ignore_null_updates=True
    )


def table_exists(table_name: str) -> bool:
    """Check if a table exists.
    
    Args:
        table_name: Name of the table to check
        
    Returns:
        True if the table exists, False otherwise
        
    Raises:
        RuntimeError: If no active Spark session is found
    """
    spark = SparkSession.getActiveSession()
    if not spark:
        raise RuntimeError("No active Spark session found")
    
    # Parse catalog and schema from table name
    parts = table_name.split('.')
    if len(parts) != 3:
        raise ValueError(f"table_name must be fully qualified (catalog.schema.table), got {table_name}")
    
    catalog, schema, table = parts
    
    # Check if table exists
    try:
        tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}")
        return tables.filter(tables.tableName == table).count() > 0
    except Exception:
        return False
