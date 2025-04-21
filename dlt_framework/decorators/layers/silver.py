"""Silver layer decorator for the DLT Medallion Framework.

This decorator applies silver layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII masking
- Reference data validation
- Slowly Changing Dimension support

This implementation ensures tables are properly discovered by DLT at module import time.
"""
from functools import wraps
from typing import Any, Callable, Optional, Dict, List, Union
import json
from datetime import datetime
import sys
from pydantic import ValidationError

from pyspark.sql import DataFrame
import dlt

from dlt_framework.core import DLTIntegration, DecoratorRegistry, ReferenceManager
from dlt_framework.config import (
    SilverConfig, ConfigurationManager, Layer, Expectation, 
    ExpectationAction, UnityTableConfig
)


def silver(
    table_name: Optional[str] = None,
    config_path: Optional[str] = None,
    config: Optional[SilverConfig] = None,
    **kwargs
) -> Callable:
    """Silver layer decorator with business rule validation.
    
    This implementation ensures the decorated function is properly registered
    with DLT at module import time so it can be discovered by the DLT pipeline.
    
    Args:
        table_name: Optional table name (overrides name in config)
        config_path: Optional path to configuration file
        config: Optional configuration object
        **kwargs: Additional configuration overrides
        
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        # Get the original function's module
        module = sys.modules[func.__module__]
        
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
            silver_config = ConfigurationManager.resolve_config(
                layer=Layer.SILVER,
                config_path=config_path,
                config_obj=config,
                **kwargs
            )
        except Exception as e:
            print(f"Error resolving configuration: {e}")
            # Fallback to basic configuration
            silver_config = SilverConfig(
                table=table_config or UnityTableConfig(
                    name=func.__name__,
                    catalog="main",
                    schema_name="default"
                )
            )
        
        # Get actual table name (from config or function name)
        final_table_name = silver_config.table.name or func.__name__
        
        # Initialize DLT integration with the resolved config
        dlt_integration = DLTIntegration(silver_config)
        
        # Create fully qualified table name for reference
        full_table_name = f"{silver_config.table.catalog}.{silver_config.table.schema_name}.{final_table_name}"
        
        # Create a runtime wrapper that implements silver layer functionality
        @wraps(func)
        def runtime_wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Get DataFrame from original function
            df = func(*args, **kwargs)
            
            try:
                # Apply deduplication if configured
                if silver_config.deduplication:
                    df = df.dropDuplicates()
                
                # Apply normalization if configured
                if silver_config.normalization:
                    df = dlt_integration.normalize_dataframe(df)
                
                # Apply SCD logic if configured
                if silver_config.scd:
                    df = dlt_integration.apply_scd(df, silver_config.scd)
                
                # Apply reference data joins if configured
                if silver_config.references:
                    df = dlt_integration.apply_references(df, silver_config.references)
            except Exception as e:
                print(f"Error applying silver layer transformations: {e}")
                # Continue with the original dataframe
            
            return df
        
        # Apply expectations from DLT integration
        if silver_config.validations:
            # Apply expectations using integration method
            runtime_wrapper = dlt_integration.add_expectations(silver_config.validations)(runtime_wrapper)
        
        # Add quality metrics if configured
        runtime_wrapper = dlt_integration.add_quality_metrics()(runtime_wrapper)
        
        # CRITICAL: Apply DLT table decorator using the integration method
        # This registers the table with DLT at import time
        runtime_wrapper = dlt_integration.apply_table_properties(runtime_wrapper)
        
        # Log for debugging
        print(f"Registered silver table: {full_table_name}")
        
        # Return the wrapper function which is now decorated with @dlt.table
        return runtime_wrapper
    
    return decorator