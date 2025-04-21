"""Quarantine management for the DLT Medallion Framework.

This module provides functionality for managing quarantined records that fail validation,
including storage, retrieval, and reprocessing capabilities within Databricks DLT pipelines.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from datetime import datetime
from functools import reduce
import json

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
import dlt
from pyspark.sql.functions import (
    array, struct, lit, current_timestamp, monotonically_increasing_id,
    to_json, col, expr, window, date_trunc
)

from .table_operations import get_empty_quarantine_dataframe, write_to_quarantine_table, table_exists
from dlt_framework.config import QuarantineConfig, Expectation


class QuarantineManager:
    """Manages quarantine functionality for invalid records with optimized performance."""

    def __init__(self, config: QuarantineConfig):
        """Initialize quarantine manager with performance settings."""
        self.config = config
        self._cached_reference_data = {}

    def create_invalid_records_view(
        self,
        df: DataFrame,
        expectations: List[Expectation],
        source_table: str,
        watermark_column: Optional[str] = None
    ) -> Tuple[DataFrame, str]:
        """Create an optimized streaming view of invalid records.
        
        Args:
            df: Input DataFrame
            expectations: List of expectations to validate
            source_table: Source table name
            watermark_column: Optional watermark column
            
        Returns:
            Tuple of (valid_records, invalid_records_view_name)
        """
        if not expectations:
            return df, ""

        # Combine all validation conditions into a single evaluation
        validation_conditions = []
        validation_metadata = []
        
        for exp in expectations:
            condition = expr(exp.constraint)
            validation_conditions.append(condition)
            validation_metadata.append(
                struct(
                    lit(exp.name).alias("name"),
                    lit(exp.constraint).alias("constraint"),
                    (~condition).alias("failed")
                )
            )

        # Combine conditions efficiently
        combined_condition = reduce(lambda x, y: x & y, validation_conditions)
        
        # Add validation metadata in a single pass
        df_with_meta = df.withColumn(
            "validation_meta",
            struct(
                current_timestamp().alias("quarantine_timestamp"),
                lit(source_table).alias("source_table"),
                array(validation_metadata).alias("failed_validations"),
                monotonically_increasing_id().alias("version_id"),
                # Add partition columns for better performance
                date_trunc("year", current_timestamp()).alias("year"),
                date_trunc("month", current_timestamp()).alias("month")
            )
        )

        # Add watermark if specified
        if watermark_column and watermark_column in df.columns:
            df_with_meta = df_with_meta.withWatermark(watermark_column, "1 hour")

        # Split valid and invalid records in a single pass
        valid_records = df_with_meta.filter(combined_condition).drop("validation_meta")
        invalid_records = df_with_meta.filter(~combined_condition)

        # Create optimized view name
        view_name = f"{source_table}_invalid_records"
        
        # Create DLT streaming view with optimizations
        dlt.view(
            name=view_name,
            comment=f"Invalid records from {source_table}",
            temporary=False,
            table_properties={
                "delta.autoOptimize.optimizeWrite": str(self.config.optimize_write).lower(),
                "delta.autoOptimize.autoCompact": str(self.config.auto_compact).lower(),
            }
        )(lambda: invalid_records)

        return valid_records, view_name

    def create_quarantine_table_function(
        self,
        source_table: str,
        table_name: Optional[str] = None,
        partition_columns: Optional[List[str]] = None
    ) -> Callable[[], DataFrame]:
        """Create an optimized DLT table function for quarantined records and return it.
        
        Args:
            source_table: Source table name
            table_name: Optional custom table name
            partition_columns: Optional partition columns
            
        Returns:
            A function decorated with @dlt.table that can be registered
        """
        invalid_records_view = f"{source_table}_invalid_records"
        quarantine_table = table_name or f"{source_table}_quarantine"

        # Define optimized table properties
        table_properties = {
            "quality": "quarantine",
            "source_table": source_table,
            "delta.enableChangeDataFeed": "true",
            "delta.autoOptimize.optimizeWrite": str(self.config.optimize_write).lower(),
            "delta.autoOptimize.autoCompact": str(self.config.auto_compact).lower(),
            "delta.tuneFileSizesForRewrites": "true",
            "delta.targetFileSize": str(128 * 1024 * 1024),  # 128MB target file size
            "delta.dataSkippingNumIndexedCols": "10",  # Index more columns for better pruning
        }

        # Use effective partition columns
        effective_partition_cols = partition_columns or self.config.partition_by
        
        # Create a clean config copy to avoid closure issues
        config_dict = {
            k: v for k, v in self.config.__dict__.items()
            if not k.startswith('_')
        }
        
        @dlt.table(
            name=quarantine_table,
            comment=f"Quarantined records from {source_table}",
            partition_cols=effective_partition_cols,
            table_properties=table_properties
        )
        def quarantine_table_func():
            """Read quarantined records from the invalid records view."""
            # First check if invalid records view exists
            if table_exists(invalid_records_view):
                return dlt.read(invalid_records_view)
            else:
                # Return empty DataFrame with correct schema
                spark = SparkSession.getActiveSession()
                return get_empty_quarantine_dataframe(
                    spark, 
                    source_table, 
                    effective_partition_cols
                )
        
        return quarantine_table_func  # Actually return the function

    def process_reference_data(self, df: DataFrame, ref_data: DataFrame) -> DataFrame:
        """Process reference data efficiently using broadcast joins when appropriate."""
        # Check if reference data is small enough for broadcasting
        ref_size = ref_data.count() * len(ref_data.columns)
        
        if ref_size <= self.config.broadcast_threshold:
            return df.join(F.broadcast(ref_data), ["join_key"])
        else:
            # For larger reference data, use regular join with optimized shuffle
            return df.join(
                ref_data.repartition(df.rdd.getNumPartitions(), "join_key"),
                ["join_key"]
            )

    def optimize_quarantine_table(self, table_name: str) -> None:
        """Optimize the quarantine table for better query performance."""
        @dlt.table(
            name=f"{table_name}_optimized",
            temporary=True
        )
        def optimize():
            # Optimize the table layout
            return (
                dlt.read(table_name)
                .repartition(*self.config.partition_by)
                .sortWithinPartitions(*self.config.z_order_by)
            )

        # Apply optimized layout
        dlt.apply_changes(
            target=table_name,
            source=dlt.read(f"{table_name}_optimized"),
            keys=["source_record_id"],
            sequence_by="validation_meta.version_id",
            stored_as_scd_type=2,
            apply_as_deletes=None,
            merge_schema=True,
            optimization={
                "zorder_by": self.config.z_order_by
            }
        )

    def cleanup_expired_records(self, table_name: str) -> None:
        """
        Clean up expired records based on retention policy.
        
        Args:
            table_name: Name of the quarantine table
        """
        if not self.config.retention_period:
            return

        @dlt.table(
            name=f"{table_name}_cleanup",
            temporary=True
        )
        def cleanup():
            df = dlt.read(table_name)
            cutoff_timestamp = F.current_timestamp() - F.expr(self.config.retention_period)
            return df.filter(
                col("validation_meta.quarantine_timestamp") > cutoff_timestamp
            )

        # The cleanup table will replace the original table
        dlt.apply_changes(
            target=table_name,
            source=dlt.read(f"{table_name}_cleanup"),
            keys=["source_record_id"],
            sequence_by="validation_meta.version_id",
            apply_as_deletes="true",  # Remove expired records
            stored_as_scd_type=1
        )

    def prepare_quarantine_records(
        self,
        df: DataFrame,
        error_details: Union[str, Column],
        batch_id: Optional[str] = None,
        failed_expectations: Optional[List[Expectation]] = None,
        source_table_name: Optional[str] = None
    ) -> DataFrame:
        """Prepare records for quarantine by adding metadata columns.
        
        Args:
            df: DataFrame containing records to quarantine
            error_details: Error message or column containing error details
            batch_id: Optional batch identifier
            failed_expectations: Optional list of failed expectations
            source_table_name: Optional source table name to override config
            
        Returns:
            DataFrame ready for quarantine with added metadata columns
        """
        if not self.config.enabled or df.isEmpty():
            return df
            
        # Convert string error_details to a column
        if isinstance(error_details, str):
            error_details = F.lit(error_details)
            
        # Create failed expectations array if provided
        expectations_array = None
        if failed_expectations:
            expectations_array = F.array([
                F.struct(
                    F.lit(exp.name).alias("name"),
                    F.lit(exp.constraint).alias("constraint")
                )
                for exp in failed_expectations
            ])
        
        # Use provided source table name or fall back to config
        effective_source = source_table_name or self.config.source_table_name
        
        # Build metadata columns
        metadata_cols = {
            self.config.error_column: error_details,
            self.config.timestamp_column: F.current_timestamp(),
            self.config.source_column: F.lit(effective_source) if effective_source else F.lit(None),
            self.config.failed_expectations_column: (
                expectations_array if expectations_array is not None 
                else F.array()
            )
        }
        
        if batch_id:
            metadata_cols[self.config.batch_id_column] = F.lit(batch_id)
        else:
            metadata_cols[self.config.batch_id_column] = F.lit(None)
            
        # Add metadata columns to DataFrame
        quarantine_df = df
        for col_name, col_value in metadata_cols.items():
            quarantine_df = quarantine_df.withColumn(col_name, col_value)
            
        return quarantine_df

    def quarantine_records_by_condition(
        self,
        df: DataFrame,
        condition: Column,
        error_message: str,
        batch_id: Optional[str] = None
    ) -> Tuple[DataFrame, DataFrame]:
        """Quarantine records based on a condition.
        
        Args:
            df: Input DataFrame
            condition: Column expression for valid records
            error_message: Error message for invalid records
            batch_id: Optional batch identifier
            
        Returns:
            Tuple of (valid DataFrame, invalid DataFrame)
        """
        if not self.config.enabled:
            return df, df.filter(F.lit(False))  # Return empty invalid DataFrame
            
        # Split data into valid and invalid
        valid_df = df.filter(condition)
        invalid_df = df.filter(~condition)
        
        # If there are invalid records, prepare and write to quarantine
        if not invalid_df.isEmpty():
            invalid_df = self.prepare_quarantine_records(
                invalid_df,
                error_message,
                batch_id=batch_id
            )
            
            # Write to quarantine table
            write_to_quarantine_table(
                invalid_df,
                self.config.get_quarantine_table_name(),
                self.config.timestamp_column
            )
                
        return valid_df, invalid_df

    def quarantine_records_by_expectations(
        self,
        df: DataFrame,
        expectations: List[Expectation],
        batch_id: Optional[str] = None
    ) -> Tuple[DataFrame, DataFrame]:
        """Quarantine records that fail expectations.
        
        Args:
            df: Input DataFrame
            expectations: List of expectations to validate
            batch_id: Optional batch identifier
            
        Returns:
            Tuple of (valid DataFrame, invalid DataFrame)
        """
        if not self.config.enabled:
            return df, df.filter(F.lit(False))  # Return empty invalid DataFrame
            
        # Combine all expectations into a single condition
        valid_condition = reduce(
            lambda x, y: x & y,
            [F.expr(exp.constraint) for exp in expectations]
        )
        
        # Split data into valid and invalid
        valid_df = df.filter(valid_condition)
        invalid_df = df.filter(~valid_condition)
        
        # If there are invalid records, prepare and write to quarantine
        if not invalid_df.isEmpty():
            # Find failed expectations for each record
            failed_exps = []
            for exp in expectations:
                failed_exps.append(
                    ~F.expr(exp.constraint)
                )
            
            # Prepare quarantine records
            invalid_df = self.prepare_quarantine_records(
                invalid_df,
                "Failed data quality expectations",
                batch_id=batch_id,
                failed_expectations=expectations
            )
            
            # Write to quarantine table
            write_to_quarantine_table(
                invalid_df,
                self.config.get_quarantine_table_name(),
                self.config.timestamp_column
            )
                
        return valid_df, invalid_df

    def get_quarantined_records(
        self,
        spark: Optional[SparkSession] = None,
        batch_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        filter_condition: Optional[Column] = None
    ) -> DataFrame:
        """Retrieve quarantined records based on filters.
        
        Note: This method should only be used outside of DLT pipeline execution.
        
        Args:
            spark: SparkSession for reading the quarantine table (optional if active session exists)
            batch_id: Filter by batch ID
            start_time: Filter records quarantined after this time
            end_time: Filter records quarantined before this time
            filter_condition: Additional filter condition
            
        Returns:
            DataFrame containing matching quarantined records
            
        Raises:
            RuntimeError: If no SparkSession is available
        """
        if not self.config.enabled:
            raise ValueError("Quarantine is not enabled")
            
        # Get SparkSession
        if not spark:
            spark = SparkSession.getActiveSession()
            if not spark:
                raise RuntimeError("No active Spark session found and no SparkSession provided")
        
        # Read quarantine table
        df = spark.table(self.config.get_quarantine_table_name())
        
        # Apply filters
        if batch_id:
            df = df.filter(F.col(self.config.batch_id_column) == batch_id)
        
        if start_time:
            df = df.filter(F.col(self.config.timestamp_column) >= start_time)
        
        if end_time:
            df = df.filter(F.col(self.config.timestamp_column) <= end_time)
            
        if filter_condition:
            df = df.filter(filter_condition)
            
        return df
    
    def reprocess_records(
        self,
        df: DataFrame,
        validation_func: Callable[[DataFrame], DataFrame],
        error_handler: Optional[Callable[[DataFrame, Exception], Tuple[DataFrame, DataFrame]]] = None
    ) -> Tuple[DataFrame, DataFrame]:
        """Attempt to reprocess quarantined records.
        
        Note: This method should only be used outside of DLT pipeline execution.
        
        Args:
            df: DataFrame containing records to reprocess
            validation_func: Function that validates records and returns valid records
            error_handler: Optional function to handle validation errors
            
        Returns:
            Tuple of (valid_records, still_invalid_records)
            
        Raises:
            Exception: If validation fails and no error handler is provided
        """
        if not self.config.enabled:
            raise ValueError("Quarantine is not enabled")
            
        # Remove quarantine metadata columns if they exist
        columns_to_drop = [
            col for col in [
                self.config.error_column,
                self.config.timestamp_column,
                self.config.batch_id_column,
                self.config.source_column,
                self.config.failed_expectations_column
            ] if col in df.columns
        ]
        
        reprocess_df = df.drop(*columns_to_drop) if columns_to_drop else df
        
        try:
            # Apply validation
            valid_df = validation_func(reprocess_df)
            return valid_df, df.filter(F.lit(False))  # Return empty invalid DataFrame
        except Exception as e:
            if error_handler:
                return error_handler(reprocess_df, e)
            raise

    def get_quarantine_summary(
        self,
        spark: Optional[SparkSession] = None,
        batch_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> DataFrame:
        """Get a summary of quarantined records and their errors.
        
        Args:
            spark: SparkSession for reading the quarantine table (optional if active session exists)
            batch_id: Filter by batch ID
            start_time: Filter records quarantined after this time
            end_time: Filter records quarantined before this time
            
        Returns:
            DataFrame with error counts and details
            
        Raises:
            RuntimeError: If no SparkSession is available
        """
        if not self.config.enabled:
            raise ValueError("Quarantine is not enabled")
            
        # Get quarantined records
        df = self.get_quarantined_records(spark, batch_id, start_time, end_time)
        
        # Return empty DataFrame if no quarantined records
        if df.isEmpty():
            return df.select(
                F.lit(None).alias("error_type"),
                F.lit(None).alias("source_table"),
                F.lit(None).alias("batch_id"),
                F.lit(0).alias("record_count"),
                F.lit(None).alias("first_occurrence"),
                F.lit(None).alias("last_occurrence")
            )
        
        # Group by error type and source
        return df.groupBy(
            self.config.error_column,
            self.config.source_column,
            self.config.batch_id_column
        ).agg(
            F.count("*").alias("record_count"),
            F.min(self.config.timestamp_column).alias("first_occurrence"),
            F.max(self.config.timestamp_column).alias("last_occurrence")
        ).orderBy(F.desc("record_count"))
