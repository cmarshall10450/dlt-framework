"""Quarantine management for the DLT Medallion Framework.

This module provides functionality for managing quarantined records that fail validation,
including storage, retrieval, and reprocessing capabilities within Databricks DLT pipelines.
"""

from typing import Any, Callable, Dict, List, Optional, Tuple, Union
from datetime import datetime
from functools import reduce

from pyspark.sql import DataFrame, SparkSession, Column
from pyspark.sql import functions as F
import dlt

from .table_operations import create_quarantine_table, write_to_quarantine_table, table_exists
from ..config.models import QuarantineConfig, Expectation


class QuarantineManager:
    """Manages quarantined records that fail validation within DLT pipelines."""
    
    def __init__(self, config: QuarantineConfig):
        """Initialize the quarantine manager.
        
        Args:
            config: Quarantine configuration
        """
        self.config = config
        
        # Initialize quarantine table if enabled
        if self.config.enabled:
            quarantine_table_name = self.config.get_quarantine_table_name()
            if not table_exists(quarantine_table_name):
                create_quarantine_table(quarantine_table_name, self.config)
    
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
