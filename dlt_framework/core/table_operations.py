"""Table operations for the DLT Medallion Framework."""

from typing import Optional, Dict, Any, List
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import dlt

from ..config import QuarantineConfig


def get_empty_quarantine_dataframe(
    spark: SparkSession,
    source_table: str,
    partition_cols: Optional[List[str]] = None
) -> DataFrame:
    """Create an empty DataFrame with the quarantine schema.
    
    Args:
        spark: Active SparkSession
        source_table: Source table name
        partition_cols: Optional partition columns
        
    Returns:
        Empty DataFrame with quarantine schema
    """
    # Create basic schema for quarantined data
    schema = T.StructType([
        T.StructField("source_record_id", T.StringType(), True),
        T.StructField("error_details", T.StringType(), True),
        T.StructField("quarantine_timestamp", T.TimestampType(), True),
        T.StructField("source_table", T.StringType(), True),
        T.StructField("failed_validations", T.ArrayType(
            T.StructType([
                T.StructField("name", T.StringType(), True),
                T.StructField("constraint", T.StringType(), True)
            ])
        ), True)
    ])
    
    # Add partition columns if specified
    if partition_cols:
        for col in partition_cols:
            if col not in schema.fieldNames():
                if col in ["year", "month", "day"]:
                    schema.add(T.StructField(col, T.IntegerType(), True))
                else:
                    schema.add(T.StructField(col, T.StringType(), True))
    
    # Create empty DataFrame
    empty_df = spark.createDataFrame([], schema)
    
    # Add partition timestamps if needed for time-based partitioning
    if partition_cols and any(col in ["year", "month", "day"] for col in partition_cols):
        empty_df = empty_df.withColumn("quarantine_timestamp", F.current_timestamp())
        if "year" in partition_cols:
            empty_df = empty_df.withColumn("year", F.year("quarantine_timestamp"))
        if "month" in partition_cols:
            empty_df = empty_df.withColumn("month", F.month("quarantine_timestamp"))
        if "day" in partition_cols:
            empty_df = empty_df.withColumn("day", F.dayofmonth("quarantine_timestamp"))
    
    return empty_df


def table_exists(table_name: str) -> bool:
    """Check if a table or view exists.
    
    Args:
        table_name: Name of the table or view to check
        
    Returns:
        True if the table exists, False otherwise
    """
    try:
        spark = SparkSession.getActiveSession()
        if not spark:
            return False
        spark.table(table_name)
        return True
    except Exception:
        return False


def write_to_quarantine_table(
    df: DataFrame,
    table_name: str,
    timestamp_col: str
) -> None:
    """Write records to a quarantine table using DLT.
    
    Args:
        df: DataFrame containing records to quarantine
        table_name: Name of the quarantine table
        timestamp_col: Name of the timestamp column for versioning
    """
    # Ensure we have a unique key for each record
    if "source_record_id" not in df.columns:
        df = df.withColumn("source_record_id", F.monotonically_increasing_id())

    # Use DLT apply_changes for proper streaming support and versioning
    dlt.apply_changes(
        target=table_name,
        source=df,
        keys=["source_record_id"],  # Use source_record_id as the merge key
        sequence_by=timestamp_col,   # Use timestamp for versioning
        stored_as_scd_type=2,       # Track history of changes
        apply_as_deletes=None,      # Don't treat any records as deletes
        ignore_null_updates=True,    # Ignore null values in updates
        column_list=None,           # Update all columns
        merge_schema=True           # Allow schema evolution
    )
