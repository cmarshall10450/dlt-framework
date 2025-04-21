# dlt_framework/decorators/layers/bronze.py

from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Union
import json
from datetime import datetime
from functools import reduce
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


def bronze(
    config_path: Optional[str] = None,
    config: Optional[BronzeConfig] = None,
    pii_detector: Optional[PIIDetector] = None,
    watermark_column: Optional[str] = None
) -> Callable:
    """Bronze layer decorator with enhanced lineage tracking."""
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Resolve configuration
            bronze_config = ConfigurationManager.resolve_config(
                layer=Layer.BRONZE,
                config_path=config_path,
                config_obj=config
            )

            # Initialize DLT integration
            dlt_integration = DLTIntegration(bronze_config)

            # Prepare table properties with enhanced lineage
            table_name = bronze_config.table.name or f.__name__
            table_properties = {
                "layer": "bronze",
                "pipelines.autoOptimize.managed": "true",
                "delta.enableChangeDataFeed": "true" if bronze_config.quarantine else "false",
                "delta.columnMapping.mode": "name",
                "pipelines.metadata.createdBy": "dlt_framework",
                "pipelines.metadata.createdTimestamp": datetime.now().isoformat(),
                "comment": json.dumps({
                    "description": f.__doc__ or f"Bronze table for {table_name}",
                    "config": bronze_config.dict(),
                    "features": {
                        "quarantine": bool(bronze_config.quarantine),
                        "pii_detection": bool(pii_detector),
                        "schema_evolution": bronze_config.schema_evolution,
                        "incremental": bool(watermark_column)
                    }
                })
            }

            # Get DLT expectation decorators for non-quarantine expectations
            expectation_decorators = []
            if bronze_config.validations:
                non_quarantine_validations = [
                    exp for exp in bronze_config.validations 
                    if exp.action != "quarantine"
                ]
                expectation_decorators.extend(
                    dlt_integration.get_expectation_decorators(non_quarantine_validations)
                )

            # Add quality metrics if monitoring is configured
            if bronze_config.monitoring_config:
                metrics_decorator = dlt_integration.add_quality_metrics()
                expectation_decorators.append(metrics_decorator)

            # Add DLT table decorator with full table path
            full_table_name = f"{bronze_config.table.catalog}.{bronze_config.table.schema_name}.{table_name}"
            table_decorator = dlt.table(
                name=full_table_name,
                comment=f"Bronze layer table for {table_name}",
                table_properties=table_properties,
                temporary=False,
                path=f"{bronze_config.table.storage_location}/{table_name}" if bronze_config.table.storage_location else None
            )
            expectation_decorators.append(table_decorator)

            # Create quarantine table function if configured
            if bronze_config.quarantine:
                # Add to the module's global namespace for import-time discovery
                module_globals = sys.modules[f.__module__].__dict__
                quarantine_table_func_name = f"{table_name}_quarantine"
                quarantine_manager = QuarantineManager(bronze_config.quarantine)
                module_globals[quarantine_table_func_name] = quarantine_manager.create_quarantine_table_function(
                    source_table=full_table_name,
                    partition_columns=[watermark_column] if watermark_column else None
                )

            # Apply all decorators to the function
            decorated_func = reduce(lambda x, y: y(x), expectation_decorators, f)

            # Get DataFrame from decorated function
            df = decorated_func(*args, **kwargs)

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

            # Register decorated function in DecoratorRegistry
            DecoratorRegistry.register(
                func=f,
                layer=Layer.BRONZE,
                features={
                    "quarantine": bool(bronze_config.quarantine),
                    "pii_detection": bool(pii_detector),
                    "schema_evolution": bronze_config.schema_evolution,
                    "incremental": bool(watermark_column)
                },
                config=bronze_config
            )

            return df

        return wrapper

    return decorator