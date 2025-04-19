"""Quarantine management for invalid records in the DLT Medallion Framework.

This module provides functionality for managing quarantined records that fail validation,
including storage, retrieval, and reprocessing capabilities within Databricks DLT pipelines.
"""

from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, TimestampType
import dlt

@dataclass
class QuarantineConfig:
    """Configuration for quarantine management within DLT pipelines."""
    source_table_name: str  # Full Unity Catalog table name (catalog.schema.table)
    error_column: str = "_error"
    timestamp_column: str = "_quarantine_timestamp"
    batch_id_column: str = "_batch_id"
    source_column: str = "_source"

    @property
    def quarantine_table_name(self) -> str:
        """Generate the quarantine table name based on source table.
        
        The quarantine table will be created in the same catalog and schema
        as the source table, with '_quarantine' suffix.
        
        Example:
            source_table_name: 'catalog.schema.customers'
            quarantine_table_name: 'catalog.schema.customers_quarantine'
        """
        # Split the fully qualified table name
        parts = self.source_table_name.split('.')
        if len(parts) != 3:
            raise ValueError(
                f"source_table_name must be fully qualified (catalog.schema.table), got {self.source_table_name}"
            )
        
        # Add _quarantine suffix to table name
        parts[-1] = f"{parts[-1]}_quarantine"
        return '.'.join(parts)

class QuarantineManager:
    """Manages quarantined records that fail validation within DLT pipelines."""
    
    def __init__(
        self,
        config: QuarantineConfig
    ):
        """Initialize the quarantine manager.
        
        Args:
            config: Quarantine configuration
        """
        self.config = config
    
    @staticmethod
    def create_quarantine_table_for(source_table: str) -> DataFrame:
        """Create a quarantine table for a specific source table.
        
        This method should be called for each source table that needs quarantine
        capability. It creates a corresponding quarantine table in the same
        catalog and schema as the source table.
        
        Args:
            source_table: Fully qualified name of the source table (catalog.schema.table)
            
        Returns:
            DataFrame: The quarantine table's schema and data
            
        Example:
            ```python
            @dlt.table(
                name="customers_quarantine",
                comment="Quarantine table for invalid customer records"
            )
            def create_customers_quarantine():
                return QuarantineManager.create_quarantine_table_for("catalog.schema.customers")
            ```
        """
        # Extract table name for the stream name
        table_name = source_table.split('.')[-1]
        stream_name = f"{table_name}_quarantine_records"
        
        return dlt.read_stream(stream_name)

    def prepare_quarantine_records(
        self,
        df: DataFrame,
        error_details: Union[str, Column],
        batch_id: Optional[str] = None
    ) -> DataFrame:
        """Prepare records for quarantine by adding metadata columns.
        
        This method adds the required metadata columns to records that failed validation.
        The resulting DataFrame can be written to the quarantine table using dlt.write().
        
        Args:
            df: DataFrame containing records to quarantine
            error_details: Error message or column containing error details
            batch_id: Optional batch identifier
            
        Returns:
            DataFrame ready for quarantine with added metadata columns
        
        Example:
            ```python
            # In your DLT pipeline:
            invalid_records = df.filter(~valid_condition)
            quarantine_df = quarantine_manager.prepare_quarantine_records(
                invalid_records,
                "Failed validation: invalid email format",
                "batch_20240101"
            )
            # Write to the table-specific quarantine stream
            table_name = "customers"
            dlt.write(quarantine_df, f"{table_name}_quarantine_records")
            ```
        """
        if isinstance(error_details, str):
            error_details = F.lit(error_details)
            
        quarantine_df = df.withColumn(
            self.config.error_column,
            error_details
        ).withColumn(
            self.config.timestamp_column,
            F.current_timestamp()
        ).withColumn(
            self.config.source_column,
            F.lit(self.config.source_table_name)
        )
        
        if batch_id:
            quarantine_df = quarantine_df.withColumn(
                self.config.batch_id_column,
                F.lit(batch_id)
            )
        
        return quarantine_df
    
    def get_quarantined_records(
        self,
        spark: SparkSession,
        batch_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> DataFrame:
        """Retrieve quarantined records based on filters.
        
        Note: This method should only be used outside of DLT pipeline execution.
        
        Args:
            spark: SparkSession for reading the quarantine table
            batch_id: Filter by batch ID
            start_time: Filter records quarantined after this time
            end_time: Filter records quarantined before this time
            
        Returns:
            DataFrame containing matching quarantined records
        """
        df = spark.table(self.config.quarantine_table_name)
        
        if batch_id:
            df = df.filter(F.col(self.config.batch_id_column) == batch_id)
        
        if start_time:
            df = df.filter(F.col(self.config.timestamp_column) >= start_time)
        
        if end_time:
            df = df.filter(F.col(self.config.timestamp_column) <= end_time)
            
        return df
    
    def reprocess_records(
        self,
        df: DataFrame,
        validation_func: callable,
        error_handler: Optional[callable] = None
    ) -> Tuple[DataFrame, DataFrame]:
        """Attempt to reprocess quarantined records.
        
        Note: This method should only be used outside of DLT pipeline execution.
        
        Args:
            df: DataFrame containing records to reprocess
            validation_func: Function that validates records
            error_handler: Optional function to handle validation errors
            
        Returns:
            Tuple of (valid_records, still_invalid_records)
        """
        # Remove quarantine metadata columns
        reprocess_df = df.drop(
            self.config.error_column,
            self.config.timestamp_column,
            self.config.batch_id_column,
            self.config.source_column
        )
        
        try:
            # Apply validation
            valid_records = validation_func(reprocess_df)
            
            # Get records that are still invalid
            still_invalid = reprocess_df.join(
                valid_records,
                reprocess_df.columns,
                "left_anti"
            )
            
            if error_handler and still_invalid.count() > 0:
                error_handler(still_invalid)
                
            return valid_records, still_invalid
            
        except Exception as e:
            if error_handler:
                error_handler(reprocess_df, error=str(e))
            raise
    
    def get_error_summary(
        self,
        spark: SparkSession,
        batch_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> DataFrame:
        """Get summary statistics of quarantined records.
        
        Note: This method should only be used outside of DLT pipeline execution.
        
        Args:
            spark: SparkSession for reading the quarantine table
            batch_id: Filter by batch ID
            start_time: Filter records quarantined after this time
            end_time: Filter records quarantined before this time
            
        Returns:
            DataFrame with error counts and statistics
        """
        df = self.get_quarantined_records(
            spark,
            batch_id,
            start_time,
            end_time
        )
        
        return df.groupBy(
            self.config.error_column
        ).agg(
            F.count("*").alias("error_count"),
            F.min(self.config.timestamp_column).alias("first_seen"),
            F.max(self.config.timestamp_column).alias("last_seen")
        ).orderBy(F.col("error_count").desc()) 