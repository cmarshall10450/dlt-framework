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

import dlt
from pyspark.sql import DataFrame

from dlt_framework.config import (
    GoldConfig, Layer, ConfigurationManager
)
from dlt_framework.core import (
    DLTIntegration, DecoratorRegistry, ReferenceManager
)


def gold(
    config_path: Optional[str] = None,
    config: Optional[GoldConfig] = None
) -> Callable:
    """Gold layer decorator with dimension integration.
    
    This implementation ensures the decorated function is properly registered
    with DLT at module import time so it can be discovered by the DLT pipeline.
    """
    def decorator(func: Callable) -> Callable:
        # Get the original function's module
        module = sys.modules[func.__module__]
        
        # Resolve configuration immediately (at decorator application time)
        gold_config = ConfigurationManager.resolve_config(
            layer=Layer.GOLD,
            config_path=config_path,
            config_obj=config
        )
        
        # Initialize DLT integration
        dlt_integration = DLTIntegration()
        
        # Get table name and properties
        table_name = gold_config.table.name or func.__name__
        full_table_name = f"{gold_config.table.catalog}.{gold_config.table.schema_name}.{table_name}"
        
        table_properties = {
            "layer": "gold",
            "pipelines.autoOptimize.managed": "true",
            "delta.columnMapping.mode": "name",
            "pipelines.metadata.createdBy": "dlt_framework",
            "pipelines.metadata.createdTimestamp": datetime.now().isoformat(),
            "comment": json.dumps({
                "description": func.__doc__ or f"Gold table for {table_name}",
                "config": gold_config.dict(),
                "features": {
                    "references": bool(gold_config.references),
                    "dimensions": bool(gold_config.dimensions),
                    "verify_pii_masking": gold_config.verify_pii_masking
                }
            })
        }
        
        # Create a runtime wrapper that implements gold layer functionality
        @wraps(func)
        def runtime_wrapper(*args: Any, **kwargs: Any) -> DataFrame:
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
                
            return df
        
        # Apply expectations directly to the runtime wrapper at module import time
        if gold_config.validations:
            for expectation in gold_config.validations:
                dlt.expect(
                    name=expectation.name,
                    constraint=expectation.constraint,
                    action="fail" if expectation.action == "fail" else "drop",
                    description=expectation.description
                )(runtime_wrapper)
        
        # Add quality metrics if monitoring is configured
        if gold_config.monitoring_config and gold_config.monitoring_config.metrics:
            dlt_integration.add_quality_metrics()(runtime_wrapper)
        
        # CRITICAL: Register with DLT table decorator DIRECTLY at module import time
        dlt.table(
            name=full_table_name,
            comment=f"Gold layer table for {table_name}",
            table_properties=table_properties,
            temporary=False,
            path=f"{gold_config.table.storage_location}/{table_name}" if gold_config.table.storage_location else None
        )(runtime_wrapper)
        
        # Add to DecoratorRegistry for metadata tracking
        DecoratorRegistry.register(
            func=func,
            layer=Layer.GOLD,
            features={
                "references": bool(gold_config.references),
                "dimensions": bool(gold_config.dimensions),
                "verify_pii_masking": gold_config.verify_pii_masking
            },
            config=gold_config
        )
        
        # Log for debugging
        print(f"Registered gold table: {full_table_name}")
        
        # Return the wrapper function which is now directly decorated with @dlt.table
        return runtime_wrapper
    
    return decorator