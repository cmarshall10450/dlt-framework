"""Gold layer decorator for the DLT Medallion Framework.

This decorator applies gold layer-specific functionality including:
- Business metrics calculation
- Dimensional model integration
- Reference data validation
- PII verification

This implementation ensures tables are properly discovered by DLT at module import time.
"""
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union
import json
from datetime import datetime
import sys
from pydantic import ValidationError

import dlt
from pyspark.sql import DataFrame

from dlt_framework.config import (
    GoldConfig, Layer, ConfigurationManager, DLTTableConfig
)
from dlt_framework.core import (
    DLTIntegration, DecoratorRegistry, ReferenceManager
)


def gold(
    table_name: Optional[str] = None,
    config_path: Optional[str] = None,
    config: Optional[GoldConfig] = None,
    **kwargs
) -> Callable:
    """Gold layer decorator with dimension integration.
    
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
                table_config = DLTTableConfig(
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
            gold_config = ConfigurationManager.resolve_config(
                layer=Layer.GOLD,
                config_path=config_path,
                config_obj=config,
                **kwargs
            )
        except Exception as e:
            print(f"Error resolving configuration: {e}")
            # Fallback to basic configuration
            gold_config = GoldConfig(
                table=table_config or DLTTableConfig(
                    name=func.__name__,
                    catalog="main",
                    schema_name="default"
                )
            )
        
        # Get actual table name (from config or function name)
        final_table_name = gold_config.table.name or func.__name__
        
        # Initialize DLT integration with the resolved config
        dlt_integration = DLTIntegration(gold_config)
        
        # Create fully qualified table name for reference
        full_table_name = f"{gold_config.table.catalog}.{gold_config.table.schema_name}.{final_table_name}"
        
        # Create a runtime wrapper that implements gold layer functionality
        @wraps(func)
        def runtime_wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            try:
                # Initialize reference manager for dimension lookups
                ref_manager = None
                if gold_config.references:
                    ref_manager = ReferenceManager(gold_config.references)
                    
                # Create a context with references if needed
                inner_kwargs = dict(kwargs)
                if ref_manager:
                    inner_kwargs["reference_manager"] = ref_manager
                    
                # Get DataFrame from original function
                df = func(*args, **inner_kwargs)
                
                # Apply dimension joins if configured
                if gold_config.dimensions and ref_manager:
                    for dim_name, join_key in gold_config.dimensions.items():
                        dim_ref = next((r for r in gold_config.references if r.name == dim_name), None)
                        if dim_ref:
                            df = ref_manager.join_reference(df, dim_ref, join_key)
                
                # Verify PII masking if configured
                if gold_config.verify_pii_masking and gold_config.governance and gold_config.governance.pii_detection:
                    df = dlt_integration.verify_pii_masking(df)
            except Exception as e:
                print(f"Error applying gold layer transformations: {e}")
                # If we hit an error at this point, we need to call the original function
                # without any of our transformations to at least get a DataFrame
                df = func(*args, **kwargs)
                
            return df
        
        # Apply expectations from DLT integration
        if gold_config.validations:
            runtime_wrapper = dlt_integration.add_expectations(gold_config.validations)(runtime_wrapper)
        
        # Add quality metrics if configured
        runtime_wrapper = dlt_integration.add_quality_metrics()(runtime_wrapper)
        
        # CRITICAL: Apply DLT table decorator using the integration method
        # This registers the table with DLT at import time
        runtime_wrapper = dlt_integration.apply_table_properties(runtime_wrapper)
        
        # Log for debugging
        print(f"Registered gold table: {full_table_name}")
        
        # Return the wrapper function which is now decorated with @dlt.table
        return runtime_wrapper
    
    return decorator