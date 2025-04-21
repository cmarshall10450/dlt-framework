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

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp, monotonically_increasing_id

from dlt_framework.config import (
    BronzeConfig, Layer, ConfigurationManager, QuarantineConfig
)
from dlt_framework.core import (
    DLTIntegration, QuarantineManager, DecoratorRegistry, PIIDetector
)
from dlt_framework.core.table_operations import get_empty_quarantine_dataframe, table_exists


def bronze(
    config_path: Optional[str] = None,
    config: Optional[BronzeConfig] = None,
    pii_detector: Optional[PIIDetector] = None,
    watermark_column: Optional[str] = None
) -> Callable:
    """Bronze layer decorator with enhanced lineage tracking.
    
    This implementation ensures the decorated function is properly registered
    with DLT at module import time so it can be discovered by the DLT pipeline.
    """
    def decorator(func: Callable) -> Callable:
        # Get the original function's module
        module = sys.modules[func.__module__]
        
        # Resolve configuration immediately (at decorator application time)
        bronze_config = ConfigurationManager.resolve_config(
            layer=Layer.BRONZE,
            config_path=config_path,
            config_obj=config
        )
        
        # Initialize DLT integration
        dlt_integration = DLTIntegration(bronze_config)
        
        # Get table name and properties
        table_name = bronze_config.table.name or func.__name__
        full_table_name = f"{bronze_config.table.catalog}.{bronze_config.table.schema_name}.{table_name}"
        
        table_properties = {
            "layer": "bronze",
            "pipelines.autoOptimize.managed": "true",
            "delta.enableChangeDataFeed": "true" if bronze_config.quarantine else "false",
            "delta.columnMapping.mode": "name",
            "pipelines.metadata.createdBy": "dlt_framework",
            "pipelines.metadata.createdTimestamp": datetime.now().isoformat(),
            "comment": json.dumps({
                "description": func.__doc__ or f"Bronze table for {table_name}",
                "config": bronze_config.dict(),
                "features": {
                    "quarantine": bool(bronze_config.quarantine),
                    "pii_detection": bool(pii_detector),
                    "schema_evolution": bronze_config.schema_evolution,
                    "incremental": bool(watermark_column)
                }
            })
        }
        
        # Create quarantine table function if configured
        if bronze_config.quarantine:
            # Create a quarantine table function at module import time
            quarantine_manager = QuarantineManager(bronze_config.quarantine)
            quarantine_table_func = quarantine_manager.create_quarantine_table_function(
                source_table=full_table_name,
                partition_columns=[watermark_column] if watermark_column else None
            )
            
            # Add quarantine table to module's globals for discovery
            quarantine_table_func_name = f"{table_name}_quarantine"
            module.__dict__[quarantine_table_func_name] = quarantine_table_func
            
            print(f"Registered quarantine table: {full_table_name}_quarantine")
        
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
                
                # Create invalid records view and get valid records
                df, invalid_records_view = quarantine_manager.create_invalid_records_view(
                    df=df,
                    expectations=quarantine_validations,
                    source_table=full_table_name,
                    watermark_column=watermark_column
                )
                
                # Register relationship in DecoratorRegistry
                DecoratorRegistry.register_relationship(
                    source=full_table_name,
                    target=f"{full_table_name}_quarantine",
                    relationship_type="quarantine",
                    metadata={
                        "invalid_records_view": invalid_records_view,
                        "watermark_column": watermark_column,
                        "quarantine_config": bronze_config.quarantine.dict()
                    }
                )
            
            # Apply PII detection if configured
            if pii_detector:
                df = pii_detector.detect_and_handle(df)
                
            return df
        
        # Get non-quarantine validations 
        if bronze_config.validations:
            non_quarantine_validations = [
                exp for exp in bronze_config.validations 
                if exp.action != "quarantine"
            ]
            
            # Apply expectations directly to the runtime wrapper at module import time
            for expectation in non_quarantine_validations:
                dlt.expect(
                    name=expectation.name,
                    constraint=expectation.constraint,
                    action="fail" if expectation.action == "fail" else "drop",
                    description=expectation.description
                )(runtime_wrapper)
        
        # Add quality metrics if monitoring is configured
        if bronze_config.monitoring_config and bronze_config.monitoring_config.metrics:
            dlt_integration.add_quality_metrics()(runtime_wrapper)
        
        # CRITICAL: Register with DLT table decorator DIRECTLY at module import time
        dlt.table(
            name=full_table_name,
            comment=f"Bronze layer table for {table_name}",
            table_properties=table_properties,
            temporary=False,
            path=f"{bronze_config.table.storage_location}/{table_name}" if bronze_config.table.storage_location else None
        )(runtime_wrapper)
        
        # Add to DecoratorRegistry for metadata tracking
        DecoratorRegistry.register(
            func=func,
            layer=Layer.BRONZE,
            features={
                "quarantine": bool(bronze_config.quarantine),
                "pii_detection": bool(pii_detector),
                "schema_evolution": bronze_config.governance.schema_evolution,
                "incremental": bool(watermark_column)
            },
            config=bronze_config
        )
        
        # Log for debugging
        print(f"Registered bronze table: {full_table_name}")
        
        # Return the wrapper function which is now directly decorated with @dlt.table
        return runtime_wrapper
    
    return decorator