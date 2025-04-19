"""Layer-specific decorators for the DLT Medallion Framework."""

from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, cast

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from ..core.config import (
    BronzeConfig,
    ConfigurationManager,
    Layer,
    TableConfig,
)
from ..core.dlt_integration import DLTIntegration
from ..core.registry import DecoratorRegistry
from ..validation.gdpr import GDPRValidator, GDPRField
from .base import medallion

T = TypeVar("T", bound=Callable[..., DataFrame])


def _add_column_tags(dlt_integration: DLTIntegration, column_name: str, tags: Dict[str, str]) -> None:
    """Helper function to add tags to a column."""
    for tag_name, tag_value in tags.items():
        dlt_integration.add_column_tag(column_name, tag_name, tag_value)


def bronze(
    config_path: Optional[Union[str, Path]] = None,
    source: Optional[Dict[str, Any]] = None,
    table: Optional[Dict[str, Any]] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    metrics: Optional[List[Dict[str, Any]]] = None,
    pii_detection: bool = True,
    **options: Any,
) -> Callable[[T], T]:
    """
    Bronze layer decorator for raw data ingestion and validation.

    Args:
        config_path: Optional path to configuration file
        source: Optional source configuration (Auto Loader or JDBC)
        table: Optional Unity Catalog table configuration
        expectations: Optional list of data quality expectations
        metrics: Optional list of quality metrics
        pii_detection: Whether to perform PII detection (default: True)
        **options: Additional configuration options

    Example:
        @bronze(
            source={
                "type": "auto_loader",
                "path": "s3://bucket/raw",
                "format": "cloudFiles",
                "options": {"cloudFiles.format": "json"}
            },
            table={
                "name": "raw_data",
                "catalog": "main",
                "schema": "bronze",
                "tags": {"sensitivity": "public"}
            }
        )
        def ingest_raw_data() -> DataFrame:
            # Function implementation
            pass
    """
    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Load configuration
            config_manager = ConfigurationManager()
            if config_path:
                config = config_manager.load_config(config_path, layer=Layer.BRONZE)
            else:
                # Create config from parameters
                config_dict = {
                    "source": source or {},
                    "table": table or {},
                    "version": "1.0"
                }
                if expectations:
                    config_dict["table"]["expectations"] = expectations
                if metrics:
                    config_dict["table"]["metrics"] = metrics
                config = BronzeConfig(**config_dict)

            # Get DLT integration instance
            dlt_integration = DLTIntegration()

            # Configure source if using Auto Loader
            if config.source.type == "auto_loader":
                source_config = dlt_integration.configure_auto_loader(
                    source_path=config.source.path,
                    format=config.source.format,
                    **config.source.options
                )
                # Add source config to table properties
                config.table.properties.update(source_config)

            # Apply base medallion decorator with bronze-specific configuration
            decorated_func = medallion(
                layer="bronze",
                config_path=config_path,
                expectations=config.table.expectations,
                metrics=config.table.metrics,
                table_properties=config.table.properties,
                decorator_type="bronze_layer",  # Use a different decorator type
                **options
            )(func)

            # Get the DataFrame from the decorated function
            df = decorated_func(*args, **kwargs)

            # Perform PII detection if enabled
            if pii_detection:
                gdpr_validator = GDPRValidator([])  # Empty field list for detection only
                pii_columns = gdpr_validator.detect_pii(df)
                
                # Add PII detection results as table properties
                pii_properties = {
                    "pii_columns": ",".join(
                        f"{col}:{pii_type}" 
                        for pii_type, cols in pii_columns.items() 
                        for col in cols
                    )
                }
                dlt_integration.apply_table_properties(pii_properties)
                
                # Add column-level tags for PII detection
                for pii_type, columns in pii_columns.items():
                    for column in columns:
                        _add_column_tags(dlt_integration, column, {
                            "pii": "true",
                            "pii_type": pii_type,
                            "pii_status": "detected",
                            "layer": "bronze",
                            "detection_timestamp": "CURRENT_TIMESTAMP"
                        })
                
                # Tag non-PII columns explicitly
                all_pii_columns = {
                    col for cols in pii_columns.values() 
                    for col in cols
                }
                for column in df.columns:
                    if column not in all_pii_columns:
                        _add_column_tags(dlt_integration, column, {
                            "pii": "false",
                            "layer": "bronze"
                        })

            return df

        return cast(T, wrapper)

    return decorator


def silver(
    config_path: Optional[Union[str, Path]] = None,
    table: Optional[Dict[str, Any]] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    metrics: Optional[List[Dict[str, Any]]] = None,
    masking_enabled: bool = True,
    masking_overrides: Optional[Dict[str, str]] = None,
    **options: Any,
) -> Callable[[T], T]:
    """
    Silver layer decorator for data normalization and cleansing.

    Args:
        config_path: Optional path to configuration file
        table: Optional Unity Catalog table configuration
        expectations: Optional list of data quality expectations
        metrics: Optional list of quality metrics
        masking_enabled: Whether to apply PII masking (default: True)
        masking_overrides: Optional dictionary to override default masking strategies
                         for specific columns, e.g. {"email": "hash", "phone": "redact"}
        **options: Additional configuration options

    Example:
        @silver(
            table={
                "name": "cleaned_data",
                "catalog": "main",
                "schema": "silver",
                "tags": {"data_quality": "validated"}
            },
            masking_overrides={
                "email": "hash",  # Override default masking for email
                "phone": "redact"  # Override default masking for phone
            }
        )
        def clean_data() -> DataFrame:
            return spark.table("LIVE.bronze.raw_data").transform(normalize_data)
    """
    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Load configuration
            config_manager = ConfigurationManager()
            if config_path:
                config = config_manager.load_config(config_path)
            else:
                # Create config from parameters
                config_dict = {
                    "table": table or {},
                    "version": "1.0"
                }
                if expectations:
                    config_dict["table"]["expectations"] = expectations
                if metrics:
                    config_dict["table"]["metrics"] = metrics
                config = TableConfig(**config_dict)

            # Apply base medallion decorator
            decorated_func = medallion(
                layer="silver",
                config_path=config_path,
                expectations=config.table.expectations,
                metrics=config.table.metrics,
                table_properties=config.table.properties,
                **options
            )(func)

            # Get the DataFrame from the decorated function
            df = decorated_func(*args, **kwargs)

            # Apply PII masking if enabled
            if masking_enabled:
                # Get PII detection results from bronze layer properties
                dlt_integration = DLTIntegration()
                bronze_properties = dlt_integration.get_table_properties(df)
                pii_columns_str = bronze_properties.get("pii_columns", "")
                
                if pii_columns_str:
                    # Parse PII columns from bronze layer
                    pii_fields = []
                    for col_info in pii_columns_str.split(","):
                        col, pii_type = col_info.split(":")
                        # Create GDPRField with default or overridden masking strategy
                        masking_strategy = None
                        if masking_overrides and col in masking_overrides:
                            masking_strategy = masking_overrides[col]
                        
                        pii_fields.append(GDPRField(
                            name=col,
                            pii_type=pii_type,
                            masking_strategy=masking_strategy
                        ))
                    
                    # Apply masking based on detected fields
                    if pii_fields:
                        gdpr_validator = GDPRValidator(pii_fields)
                        df = gdpr_validator.mask_pii(df)
                        
                        # Add masking metadata
                        masking_properties = {
                            "masked_columns": pii_columns_str,
                            "masking_timestamp": "CURRENT_TIMESTAMP",
                            "masking_overrides": str(masking_overrides) if masking_overrides else ""
                        }
                        dlt_integration.apply_table_properties(masking_properties)
                        
                        # Update column-level tags for masked PII
                        for field in pii_fields:
                            _add_column_tags(dlt_integration, field.name, {
                                "pii": "true",
                                "pii_type": field.pii_type,
                                "pii_status": "masked",
                                "masking_strategy": field.masking_strategy or "default",
                                "layer": "silver",
                                "masking_timestamp": "CURRENT_TIMESTAMP"
                            })
                
                # Tag non-PII columns
                pii_column_names = {field.name for field in pii_fields}
                for column in df.columns:
                    if column not in pii_column_names:
                        _add_column_tags(dlt_integration, column, {
                            "pii": "false",
                            "layer": "silver"
                        })

            return df

        return cast(T, wrapper)

    return decorator


def gold(
    config_path: Optional[Union[str, Path]] = None,
    table: Optional[Dict[str, Any]] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    metrics: Optional[List[Dict[str, Any]]] = None,
    verify_pii_masking: bool = True,
    **options: Any,
) -> Callable[[T], T]:
    """
    Gold layer decorator for business-level aggregations and metrics.

    Args:
        config_path: Optional path to configuration file
        table: Optional Unity Catalog table configuration
        expectations: Optional list of data quality expectations
        metrics: Optional list of quality metrics
        verify_pii_masking: Whether to verify PII masking (default: True)
        **options: Additional configuration options

    Example:
        @gold(
            table={
                "name": "customer_metrics",
                "catalog": "main",
                "schema": "gold",
                "tags": {"domain": "customer"}
            }
        )
        def calculate_metrics() -> DataFrame:
            return spark.table("LIVE.silver.cleaned_data").groupBy("date").agg(...)
    """
    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Load configuration
            config_manager = ConfigurationManager()
            if config_path:
                config = config_manager.load_config(config_path)
            else:
                # Create config from parameters
                config_dict = {
                    "table": table or {},
                    "version": "1.0"
                }
                if expectations:
                    config_dict["table"]["expectations"] = expectations
                if metrics:
                    config_dict["table"]["metrics"] = metrics
                config = TableConfig(**config_dict)

            # Apply base medallion decorator
            decorated_func = medallion(
                layer="gold",
                config_path=config_path,
                expectations=config.table.expectations,
                metrics=config.table.metrics,
                table_properties=config.table.properties,
                **options
            )(func)

            # Get the DataFrame from the decorated function
            df = decorated_func(*args, **kwargs)

            # Verify PII masking if enabled
            if verify_pii_masking:
                # Create an empty validator just for detection
                gdpr_validator = GDPRValidator([])
                pii_columns = gdpr_validator.detect_pii(df)
                
                if any(cols for cols in pii_columns.values()):
                    # Found unmasked PII data in gold layer
                    raise ValueError(
                        "Detected unmasked PII data in gold layer. Ensure all PII is "
                        "properly masked in the silver layer. Detected columns: " +
                        ", ".join(
                            f"{col} ({pii_type})"
                            for pii_type, cols in pii_columns.items()
                            for col in cols
                        )
                    )
                
                # Add column-level tags for verified columns
                dlt_integration = DLTIntegration()
                for column in df.columns:
                    _add_column_tags(dlt_integration, column, {
                        "pii": "false",
                        "pii_status": "verified",
                        "layer": "gold",
                        "verification_timestamp": "CURRENT_TIMESTAMP"
                    })

            return df

        return cast(T, wrapper)

    return decorator 