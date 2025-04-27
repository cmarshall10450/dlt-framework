"""Bronze layer decorator for the DLT Medallion Framework.

This decorator applies basic bronze layer functionality and ensures tables 
are properly discovered by DLT at module import time.
"""
from typing import Any, Callable, Optional, Dict
import inspect
import functools

import dlt
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F

from ..utils import register_dlt_table, register_dlt_view
from dlt_framework.config import BronzeConfig, DLTTableConfig, DLTViewConfig
from dlt_framework.core.pipeline_context import PipelineContext

class Bronze:
    """Bronze layer decorator class for the DLT Medallion Framework.
    
    This class-based decorator applies bronze layer functionality and ensures
    tables and views are properly discovered by DLT at module import time.
    """
    
    def __init__(self, config: BronzeConfig, pipeline_context: Optional[PipelineContext] = None):
        """Initialize the bronze decorator.
        
        Args:
            config: Bronze layer configuration
            pipeline_context: Optional pipeline context for run metadata
        """
        self.config = config
        self.context = pipeline_context or PipelineContext.create()
        self.enable_quarantine = getattr(self.config, 'enable_quarantine', False)
        
    def __call__(self, func: Callable) -> Callable:
        """Apply the decorator to the function.
        
        Args:
            func: The function to decorate
            
        Returns:
            The decorated function
        """
        # Store the original function
        self.func = func
        
        # Get the calling module to expose functions
        caller_frame = inspect.currentframe().f_back
        module = inspect.getmodule(caller_frame)
        
        # Create bronze transformer function
        def base_table_transformer(df: DataFrame) -> DataFrame:
            """Apply bronze layer transformations to a DataFrame."""
            df = df.withColumns({
                "_record_id": F.monotonically_increasing_id(),
                "_ingestion_ts": F.current_timestamp(),
                "_layer": F.lit("bronze"),
                "_run_id": F.lit(self.context.run_id),
                "_run_start_time": F.lit(self.context.start_time.isoformat())
            })
            
            # Add any custom metadata from context
            for key, value in self.context.metadata.items():
                df = df.withColumn(f"_meta_{key}", F.lit(value))
            
            return df
        
        # Create view config
        base_table_config = self.config.asset.copy(update={"is_temporary": True})
            
        # Note: register_dlt_view expects a function to decorate
        base_table_func = register_dlt_table(
            config=base_table_config,
            transformer=base_table_transformer
        )(func)  # Apply the decorator to the original function
        
        # Add view function to the module namespace
        base_table_name = f"{func.__name__}_base"
        globals()[base_table_name] = base_table_func
        
        # Function to get records from view
        def get_valid_records(df: DataFrame) -> DataFrame:
            """Get records from the registered view."""
            spark = SparkSession.getActiveSession()
            df = spark.read.table(f"live.{base_table_name}")
            
            if self.enable_quarantine:
                # Apply quarantine filter for good data
                return df.filter(df._quarantine_flag == True)
            else:
                # Return all data when quarantine is not enabled
                return df
        
        # Register the bronze table
        bronze_table_func = register_dlt_table(
            config=self.config.asset,
            transformer=lambda df: get_valid_records(df)
        )(func)
        
        # Add table function to the module namespace
        globals()[func.__name__] = bronze_table_func
        
        # If quarantine is enabled, create and expose bad data table
        if self.enable_quarantine:
            def quarantine_data_transformer(df: DataFrame) -> DataFrame:
                """Get records that failed validation."""
                spark = SparkSession.getActiveSession()
                view_name = f"{self.config.asset.get_full_name()}_v"
                df = spark.read.table(f"live.{view_name}")
                return df.filter(df._quarantine_flag == False)
            
            # Create bad data config
            quarantine_config = DLTTableConfig(
                catalog=self.config.asset.catalog,
                schema=self.config.asset.schema,
                name=f"{self.config.asset.name}_quarantine",
                comment=f"Quarantine records from {self.config.asset.comment}"
            )
            
            # Register quarantine data table
            quarantine_data_func = register_dlt_table(
                config=quarantine_config,
                transformer=quarantine_data_transformer
            )(func)  # Apply the decorator to the original function
            
            # Add bad data function to the module namespace
            quarantine_func_name = f"{func.__name__}_quarantine"
            globals()[quarantine_func_name] = quarantine_data_func
        
        # Return the main table function
        return table_func