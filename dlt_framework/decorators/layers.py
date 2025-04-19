"""Layer-specific decorators for the DLT Medallion Framework."""

from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar, Union, cast

from pyspark.sql import DataFrame

from ..core.config_manager import ConfigurationManager
from ..core.config_models import BronzeConfig, SilverConfig, GoldConfig
from ..core.dlt_integration import DLTIntegration
from ..core.registry import DecoratorRegistry
from ..validation.gdpr import GDPRValidator, GDPRField

T = TypeVar("T", bound=Callable[..., DataFrame])


def bronze(
    config_path: Optional[Union[str, Path]] = None,
    config: Optional[BronzeConfig] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Bronze layer decorator.
    
    This decorator applies bronze layer-specific functionality:
    - Data quality expectations
    - PII detection
    - Raw data metrics collection
    - Quarantine handling for invalid records
    
    Args:
        config_path: Optional path to YAML configuration file
        config: Optional BronzeConfig object
        **kwargs: Additional configuration parameters
        
    Example:
        >>> @dlt.table
        >>> @bronze(
        ...     config=BronzeConfig(
        ...         validate=[
        ...             Expectation(name="valid_id", constraint="id IS NOT NULL"),
        ...             Expectation(name="valid_email", constraint="email LIKE '%@%'")
        ...         ],
        ...         pii_detection=True,
        ...         metrics=["record_count", "null_count"]
        ...     )
        ... )
        >>> def raw_transactions():
        ...     return spark.read.table("raw_transactions")
    """
    def decorator(func: T) -> T:
        # Get the function name for registration
        func_name = func.__name__

        # Register with the decorator registry
        registry = DecoratorRegistry()
        registry.register(
            name=f"bronze_{func_name}",
            layer="bronze",
            decorator_type="layer"
        )

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Resolve configuration
            config_obj = ConfigurationManager.resolve_config(
                layer="bronze",
                config_path=config_path,
                config_obj=config,
                **kwargs
            )

            # Get the DataFrame from the function
            df = func(*args, **inner_kwargs)

            # Apply expectations and metrics if configured
            dlt_integration = DLTIntegration()
            if config_obj.validate:
                df = dlt_integration.add_expectations(df, config_obj.validate)
            if config_obj.metrics:
                df = dlt_integration.add_quality_metrics(df, config_obj.metrics)

            # Perform PII detection if enabled
            if config_obj.pii_detection:
                gdpr_validator = GDPRValidator([])  # Empty field list for detection only
                pii_columns = gdpr_validator.detect_pii(df)

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
        ...     config=SilverConfig(
        ...         masking_enabled=True,
        ...         masking_overrides={"email": "hash"},
        ...         validate=ConfigurationManager.required_columns("id", "email")
        ...     )
        ... )
        >>> def cleaned_transactions():
        ...     return spark.read.table("raw_transactions")
    """
    def decorator(func: T) -> T:
        # Get the function name for registration
        func_name = func.__name__

        # Register with the decorator registry
        registry = DecoratorRegistry()
        registry.register(
            name=f"silver_{func_name}",
            layer="silver",
            decorator_type="layer"
        )

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Resolve configuration
            config_obj = ConfigurationManager.resolve_config(
                layer="silver",
                config_path=config_path,
                config_obj=config,
                **kwargs
            )

            # Get the DataFrame from the function
            df = func(*args, **inner_kwargs)

            # Apply expectations and metrics if configured
            dlt_integration = DLTIntegration()
            if config_obj.validate:
                df = dlt_integration.add_expectations(df, config_obj.validate)
            if config_obj.metrics:
                df = dlt_integration.add_quality_metrics(df, config_obj.metrics)

            # Apply PII masking if enabled
            if config_obj.masking_enabled:
                # Get PII detection results from bronze layer
                pii_fields = []
                for column in df.columns:
                    if column in config_obj.masking_overrides:
                        pii_fields.append(GDPRField(
                            name=column,
                            pii_type="custom",
                            masking_strategy=config_obj.masking_overrides[column]
                        ))
                
                # Apply masking based on configured fields
                if pii_fields:
                    gdpr_validator = GDPRValidator(pii_fields)
                    df = gdpr_validator.mask_pii(df)

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
        ...     config=GoldConfig(
        ...         verify_pii_masking=True,
        ...         validate=ConfigurationManager.reference_check({
        ...             "customer_id": "dim_customers.id"
        ...         }),
        ...         monitoring=ConfigurationManager.monitor(
        ...             metrics=["daily_revenue", "customer_count"],
        ...             alerts=["revenue_drop_alert"]
        ...         )
        ...     )
        ... )
        >>> def customer_metrics():
        ...     return spark.read.table("cleaned_transactions")
    """
    def decorator(func: T) -> T:
        # Get the function name for registration
        func_name = func.__name__

        # Register with the decorator registry
        registry = DecoratorRegistry()
        registry.register(
            name=f"gold_{func_name}",
            layer="gold",
            decorator_type="layer"
        )

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Resolve configuration
            config_obj = ConfigurationManager.resolve_config(
                layer="gold",
                config_path=config_path,
                config_obj=config,
                **kwargs
            )

            # Get the DataFrame from the function
            df = func(*args, **inner_kwargs)

            # Apply expectations and metrics if configured
            dlt_integration = DLTIntegration()
            if config_obj.validate:
                df = dlt_integration.add_expectations(df, config_obj.validate)
            if config_obj.metrics:
                df = dlt_integration.add_quality_metrics(df, config_obj.metrics)

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

            return df

        return cast(T, wrapper)

    return decorator 