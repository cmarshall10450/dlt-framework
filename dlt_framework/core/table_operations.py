"""Table operations for the DLT Medallion Framework."""

from typing import Optional, Dict, Any, List
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import dlt

from ..config import QuarantineConfig


def table_exists(table_name: str) -> bool:
    """Check if a table exists.
    
    Args:
        table_name: Fully qualified table name
        
    Returns:
        True if table exists, False otherwise
    """
    try:
        dlt.read(table_name)
        return True
    except Exception:
        return False


def _get_quarantine_table_schema(config: QuarantineConfig) -> T.StructType:
    """Get the schema for the quarantine table.
    
    Args:
        config: Quarantine configuration
        
    Returns:
        StructType schema for quarantine table
    """
    return T.StructType([
        T.StructField("_quarantine_key", T.IntegerType(), False),  # Required key column
        T.StructField(config.error_column, T.StringType(), True),
        T.StructField(config.timestamp_column, T.TimestampType(), True),
        T.StructField(config.batch_id_column, T.StringType(), True),
        T.StructField(config.source_column, T.StringType(), True),
        T.StructField(
            config.failed_expectations_column,
            T.ArrayType(
                T.StructType([
                    T.StructField("name", T.StringType(), True),
                    T.StructField("constraint", T.StringType(), True)
                ])
            ),
            True
        )
    ])


@dlt.table(
    comment="Quarantine table for invalid records",
    table_properties={
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true",
        "delta.enableChangeDataFeed": "true",
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5",
        "quality": "bronze",
        "purpose": "data_quality_quarantine"
    }
)
def create_quarantine_table(table_name: str, config: QuarantineConfig) -> DataFrame:
    """Create a quarantine table with the required schema.
    
    Args:
        table_name: Fully qualified table name
        config: Quarantine configuration
        
    Returns:
        Empty DataFrame with quarantine schema
    """
    # Create empty DataFrame with quarantine metadata schema
    schema = _get_quarantine_table_schema(config)
    
    # Create empty DataFrame with a dummy key column for DLT
    spark = dlt.get_spark_session()
    empty_df = spark.createDataFrame(
        [(
            1,  # dummy_key
            None,  # error
            datetime.now(),  # timestamp
            None,  # batch_id
            None,  # source
            []  # failed_expectations
        )],
        schema
    )
    
    return empty_df


def write_to_quarantine_table(
    df: DataFrame,
    table_name: str,
    timestamp_column: str,
    batch_id: Optional[str] = None
) -> None:
    """Write records to a quarantine table.
    
    Args:
        df: DataFrame containing records to quarantine
        table_name: Fully qualified table name
        timestamp_column: Name of timestamp column for sequencing
        batch_id: Optional batch identifier
    """
    # Add dummy key column if not present
    if "_quarantine_key" not in df.columns:
        df = df.withColumn(
            "_quarantine_key",
            F.monotonically_increasing_id()
        )

    # Write to quarantine table
    dlt.apply_changes(
        target=table_name,
        source=df,
        keys=["_quarantine_key"],
        sequence_by=timestamp_column,
        stored_as_scd_type=2
    )
