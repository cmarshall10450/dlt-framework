"""Layer-specific decorators for the DLT Medallion Framework."""

from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, TypeVar, Union, cast

from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from ..core.config_manager import ConfigurationManager
from ..core.config_models import (
    BronzeConfig,
    SilverConfig,
    GoldConfig,
    Expectation,
    Metric,
    MonitoringConfig,
    GovernanceConfig,
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
    config: Optional[BronzeConfig] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Bronze layer decorator.
    
    This decorator applies bronze layer-specific functionality:
    - Data quality expectations
    - PII detection and tagging
    - Raw data metrics collection
    - Quarantine handling for invalid records
    
    Args:
        config_path: Optional path to YAML configuration file
        config: Optional BronzeConfig object
        **kwargs: Additional configuration parameters
        
    Example:
        >>> @dlt.table
        >>> @bronze(
        ...     expectations=[
        ...         Expectation(name="valid_id", constraint="id IS NOT NULL"),
        ...         Expectation(name="valid_email", constraint="email LIKE '%@%'")
        ...     ],
        ...     pii_detection=True,
        ...     metrics=["record_count", "null_count"]
        ... )
        >>> def raw_transactions():
        ...     return spark.read.table("raw_transactions")
    """
    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Resolve configuration
            config_obj = ConfigurationManager.resolve_config(
                layer="bronze",
                config_path=config_path,
                config_obj=config,
                **kwargs
            )

            # Apply medallion decorator with resolved configuration
            decorated_func = medallion(
                layer="bronze",
                config=config_obj
            )(func)

            # Get the DataFrame from the decorated function
            df = decorated_func(*args, **inner_kwargs)

            # Perform PII detection if enabled
            if config_obj.pii_detection:
                gdpr_validator = GDPRValidator([])  # Empty field list for detection only
                pii_columns = gdpr_validator.detect_pii(df)
                
                # Add PII detection results as column tags
                for pii_type, columns in pii_columns.items():
                    for column in columns:
                        _add_column_tags(DLTIntegration(), column, {
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
                        _add_column_tags(DLTIntegration(), column, {
                            "pii": "false",
                            "layer": "bronze"
                        })

            return df

        return cast(T, wrapper)

    return decorator


def silver(
    config_path: Optional[Union[str, Path]] = None,
    config: Optional[SilverConfig] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Silver layer decorator.
    
    This decorator applies silver layer-specific functionality:
    - Data quality expectations
    - PII masking and encryption
    - Data standardization and cleansing
    - SCD handling for dimension tables
    
    Args:
        config_path: Optional path to YAML configuration file
        config: Optional SilverConfig object
        **kwargs: Additional configuration parameters
        
    Example:
        >>> @dlt.table
        >>> @silver(
        ...     masking_enabled=True,
        ...     masking_overrides={"email": "hash"},
        ...     scd_config=SCDConfig(type=2, key_columns=["id"]),
        ...     expectations=ConfigurationManager.required_columns("id", "email")
        ... )
        >>> def cleaned_transactions():
        ...     return spark.read.table("raw_transactions")
    """
    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Resolve configuration
            config_obj = ConfigurationManager.resolve_config(
                layer="silver",
                config_path=config_path,
                config_obj=config,
                **kwargs
            )

            # Apply medallion decorator with resolved configuration
            decorated_func = medallion(
                layer="silver",
                config=config_obj
            )(func)

            # Get the DataFrame from the decorated function
            df = decorated_func(*args, **inner_kwargs)

            # Apply PII masking if enabled
            if config_obj.masking_enabled:
                # Get PII detection results from bronze layer
                dlt_integration = DLTIntegration()
                
                # Get column tags to identify PII columns
                pii_fields = []
                for column in df.columns:
                    tags = dlt_integration.get_column_tags(column)
                    if tags.get("pii") == "true":
                        pii_fields.append(GDPRField(
                            name=column,
                            pii_type=tags.get("pii_type", "unknown"),
                            masking_strategy=config_obj.masking_overrides.get(column) if config_obj.masking_overrides else None
                        ))
                
                # Apply masking based on detected fields
                if pii_fields:
                    gdpr_validator = GDPRValidator(pii_fields)
                    df = gdpr_validator.mask_pii(df)
                    
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

            return df

        return cast(T, wrapper)

    return decorator


def gold(
    config_path: Optional[Union[str, Path]] = None,
    config: Optional[GoldConfig] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Gold layer decorator.
    
    This decorator applies gold layer-specific functionality:
    - Data quality expectations
    - PII masking verification
    - Aggregation and metric computation
    - Business rule validation
    
    Args:
        config_path: Optional path to YAML configuration file
        config: Optional GoldConfig object
        **kwargs: Additional configuration parameters
        
    Example:
        >>> @dlt.table
        >>> @gold(
        ...     verify_pii_masking=True,
        ...     expectations=ConfigurationManager.reference_check({
        ...         "customer_id": "dim_customers.id"
        ...     }),
        ...     monitoring=ConfigurationManager.monitor(
        ...         metrics=["daily_revenue", "customer_count"],
        ...         alerts=["revenue_drop_alert"]
        ...     )
        ... )
        >>> def customer_metrics():
        ...     return spark.read.table("cleaned_transactions")
    """
    def decorator(func: T) -> T:
        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Resolve configuration
            config_obj = ConfigurationManager.resolve_config(
                layer="gold",
                config_path=config_path,
                config_obj=config,
                **kwargs
            )

            # Apply medallion decorator with resolved configuration
            decorated_func = medallion(
                layer="gold",
                config=config_obj
            )(func)

            # Get the DataFrame from the decorated function
            df = decorated_func(*args, **inner_kwargs)

            # Verify PII masking if enabled
            if config_obj.verify_pii_masking:
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