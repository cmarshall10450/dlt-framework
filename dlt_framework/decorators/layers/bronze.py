"""Bronze layer decorator for the DLT Medallion Framework.

This decorator applies bronze layer-specific functionality including:
- Data quality expectations
- Quarantine capability for invalid data
- PII detection and masking
- Schema evolution

This implementation ensures tables are properly discovered by DLT at module import time.
"""
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union
import json
from datetime import datetime
import sys
from pydantic import ValidationError

import dlt
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, current_timestamp, monotonically_increasing_id

from dlt_framework.config import (
    BronzeConfig, Layer, ConfigurationManager, QuarantineConfig, UnityTableConfig
)
from dlt_framework.core import (
    DLTIntegration, QuarantineManager, DecoratorRegistry, PIIDetector
)
from dlt_framework.core.table_operations import get_empty_quarantine_dataframe, table_exists


def bronze(
    table_name: Optional[str] = None,
    config_path: Optional[str] = None,
    config: Optional[BronzeConfig] = None,
    pii_detector: Optional[PIIDetector] = None,
    watermark_column: Optional[str] = None,
    **kwargs
) -> Callable:
    """Bronze layer decorator with enhanced lineage tracking.
    
    This implementation ensures the decorated function is properly registered
    with DLT at module import time so it can be discovered by the DLT pipeline.
    
    Args:
        table_name: Optional table name (overrides name in config)
        config_path: Optional path to configuration file
        config: Optional configuration object
        pii_detector: Optional PII detector instance
        watermark_column: Optional watermark column for incremental loads
        **kwargs: Additional configuration overrides
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        # Get the original function's module
        module = sys.modules[func.__module__]
        
        # Process configuration options:
        # 1. Start with default config or empty config
        # 2. Override with config file if provided
        # 3. Override with config object if provided
        # 4. Apply any additional kwargs as overrides
        
        # Create table config from table_name if provided
        table_config = None
        if table_name:
            try:
                # Create minimum table config with defaults
                table_config = UnityTableConfig(
                    name=table_name,
                    catalog="main",  # Default catalog
                    schema_name="default"  # Default schema
                )
                # Add to kwargs for config resolution
                kwargs["table"] = table_config.dict()
            except ValidationError as e:
                print(f"Warning: Invalid table name '{table_name}': {e}")
        
        # Resolve final configuration
        try:
            bronze_config = ConfigurationManager.resolve_config(
                layer=Layer.BRONZE,
                config_path=config_path,
                config_obj=config,
                **kwargs
            )
        except Exception as e:
            print(f"Error resolving configuration: {e}")
            # Fallback to basic configuration
            bronze_config = BronzeConfig(
                table=table_config or UnityTableConfig(
                    name=func.__name__,
                    catalog="main",
                    schema_name="default"
                )
            )
        
        # Get actual table name (from config or function name)
        final_table_name = bronze_config.table.name or func.__name__
        
        # Initialize DLT integration with the resolved config
        dlt_integration = DLTIntegration(bronze_config)
        
        # Create fully qualified table name for reference
        full_table_name = f"{bronze_config.table.catalog}.{bronze_config.table.schema_name}.{final_table_name}"
        
        # Create quarantine table function if configured
        if bronze_config.quarantine:
            try:
                # Create a quarantine table function at module import time
                quarantine_manager = QuarantineManager(bronze_config.quarantine)
                quarantine_table_func = quarantine_manager.create_quarantine_table_function(
                    source_table=full_table_name,
                    partition_columns=[watermark_column] if watermark_column else None
                )
                
                # Add quarantine table to module's globals for discovery by DLT
                quarantine_table_func_name = f"{final_table_name}_quarantine"
                module.__dict__[quarantine_table_func_name] = quarantine_table_func
                
                print(f"Registered quarantine table: {full_table_name}_quarantine")
            except Exception as e:
                print(f"Error creating quarantine table: {e}")
        
        # Create a runtime wrapper that implements bronze layer functionality
        @wraps(func)
        def runtime_wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Get DataFrame from original function
            df = func(*args, **kwargs)
            
            # Add source_record_id if not present
            if "source_record_id" not in df.columns:
                df = df.withColumn("source_record_id", monotonically_increasing_id())
            
            # Handle quarantine if configured
            if bronze_config.quarantine:
                quarantine_manager = QuarantineManager(bronze_config.quarantine)
                
                # Get quarantine validations
                quarantine_validations = [
                    exp for exp in bronze_config.validations 
                    if exp.action == "quarantine"
                ]
                
                if quarantine_validations:
                    try:
                        # Create invalid records view and get valid records
                        df, invalid_records_view = quarantine_manager.create_invalid_records_view(
                            df=df,
                            expectations=quarantine_validations,
                            source_table=full_table_name,
                            watermark_column=watermark_column
                        )
                    except Exception as e:
                        print(f"Error processing quarantine: {e}")
            
            # Apply PII detection if configured
            if pii_detector and hasattr(pii_detector, 'detect_and_handle'):
                try:
                    df = pii_detector.detect_and_handle(df)
                except Exception as e:
                    print(f"Error in PII detection: {e}")
                
            return df
        
        # Apply expectations from DLT integration
        if bronze_config.validations:
            # Get non-quarantine validations 
            non_quarantine_validations = [
                exp for exp in bronze_config.validations 
                if exp.action != "quarantine"
            ]
            
            if non_quarantine_validations:
                # Apply expectations using integration method
                runtime_wrapper = dlt_integration.add_expectations(non_quarantine_validations)(runtime_wrapper)
        
        # Add quality metrics if configured
        runtime_wrapper = dlt_integration.add_quality_metrics()(runtime_wrapper)
        
        # CRITICAL: Apply DLT table decorator using the integration method
        # This registers the table with DLT at import time
        runtime_wrapper = dlt_integration.apply_table_properties(runtime_wrapper)
        
        # Log for debugging
        print(f"Registered bronze table: {full_table_name}")
        
        # Return the wrapper function which is now decorated with @dlt.table
        return runtime_wrapper
    
    return decorator