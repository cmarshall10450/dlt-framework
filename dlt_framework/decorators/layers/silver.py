"""Silver layer decorator for the DLT Medallion Framework.

This decorator applies silver layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII masking
- Reference data validation

This implementation ensures tables are properly discovered by DLT at module import time.
"""
from functools import wraps
from typing import Any, Callable, Optional, Dict, List, Union
import json
from datetime import datetime
import sys

from pyspark.sql import DataFrame
import dlt

from dlt_framework.core import DLTIntegration, DecoratorRegistry, ReferenceManager
from dlt_framework.config import SilverConfig, ConfigurationManager, Layer, Expectation, ExpectationAction


def silver(
    config_path: Optional[str] = None,
    config: Optional[SilverConfig] = None
) -> Callable:
    """Silver layer decorator with business rule validation.
    
    This implementation ensures the decorated function is properly registered
    with DLT at module import time so it can be discovered by the DLT pipeline.
    """
    def decorator(func: Callable) -> Callable:
        # Get the original function's module
        module = sys.modules[func.__module__]
        
        # Resolve configuration immediately (at decorator application time)
        config_obj = ConfigurationManager.resolve_config(
            layer=Layer.SILVER,
            config_path=config_path,
            config_obj=config
        )
        
        # Initialize DLT integration
        dlt_integration = DLTIntegration(config_obj)
        
        # Get table name and properties
        table_name = config_obj.table.name or func.__name__
        full_table_name = f"{config_obj.table.catalog}.{config_obj.table.schema_name}.{table_name}"
        
        table_props = {
            "layer": "silver",
            "pipelines.autoOptimize.managed": "true",
            "delta.columnMapping.mode": "name",
            "pipelines.metadata.createdBy": "dlt_framework",
            "pipelines.metadata.createdTimestamp": datetime.now().isoformat(),
            "comment": json.dumps({
                "description": func.__doc__ or f"Silver table for {table_name}",
                "config": config_obj.dict(),
                "features": {
                    "deduplication": config_obj.deduplication,
                    "normalization": config_obj.normalization,
                    "scd": bool(config_obj.scd),
                    "references": bool(config_obj.references)
                }
            })
        }
        
        # Create a runtime wrapper that implements silver layer functionality
        @wraps(func)
        def runtime_wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Get DataFrame from original function
            df = func(*args, **kwargs)
            
            # Apply deduplication if configured
            if config_obj.deduplication:
                df = df.dropDuplicates()
                
            # Apply normalization if configured
            if config_obj.normalization:
                df = dlt_integration.normalize_dataframe(df)
                
            # Apply SCD logic if configured
            if config_obj.scd:
                df = dlt_integration.apply_scd(df, config_obj.scd)
                
            # Apply reference data joins if configured
            if config_obj.references:
                df = dlt_integration.apply_references(df, config_obj.references)
                
            return df
        
        # CRITICAL: Apply DLT expectations DIRECTLY to the runtime wrapper
        # at module import time
        if config_obj.validations:
            for expectation in config_obj.validations:
                if expectation.action != ExpectationAction.QUARANTINE:
                    dlt.expect(
                        name=expectation.name,
                        constraint=expectation.constraint,
                        action=expectation.action.value,  # Will be 'fail' or 'drop'
                        description=expectation.description
                    )(runtime_wrapper)
        
        # Add quality metrics if monitoring is configured
        if config_obj.monitoring_config and config_obj.monitoring_config.metrics:
            dlt_integration.add_quality_metrics()(runtime_wrapper)
        
        # CRITICAL: Register with DLT table decorator DIRECTLY at module import time
        # This is what makes the table discoverable by DLT
        dlt.table(
            name=full_table_name,
            comment=f"Silver layer table for {table_name}",
            table_properties=table_props,
            temporary=False,
            path=f"{config_obj.table.storage_location}/{table_name}" if config_obj.table.storage_location else None
        )(runtime_wrapper)
        
        # Add to DecoratorRegistry for metadata tracking
        DecoratorRegistry.register(
            func=func,
            layer=Layer.SILVER,
            features={
                "deduplication": config_obj.deduplication,
                "normalization": config_obj.normalization,
                "scd": bool(config_obj.scd),
                "references": bool(config_obj.references)
            },
            config=config_obj
        )
        
        # Log for debugging
        print(f"Registered silver table: {full_table_name}")
        
        # Return the wrapper function which is now directly decorated with @dlt.table
        return runtime_wrapper
    
    return decorator