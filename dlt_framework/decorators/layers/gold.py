"""Gold layer decorator implementation."""

from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, Protocol, TypeVar, Union, cast

from pyspark.sql import DataFrame

from ...core.config_manager import ConfigurationManager
from ...core.config_models import GoldConfig
from ...core.dlt_integration import DLTIntegration
from ...core.registry import DecoratorRegistry
from ...validation.gdpr import GDPRValidator


class PIIDetector(Protocol):
    """Protocol for PII detection implementations."""
    
    def detect_pii(self, df: DataFrame) -> dict[str, list[str]]:
        """
        Detect PII columns in the DataFrame.
        
        Args:
            df: DataFrame to analyze
            
        Returns:
            Dictionary mapping PII types to lists of column names
        """
        ...


T = TypeVar("T", bound=Callable[..., DataFrame])


def gold(
    config_path: Optional[Union[str, Path]] = None,
    config: Optional[GoldConfig] = None,
    pii_detector: Optional[PIIDetector] = None,
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
        pii_detector: Optional custom PII detector implementation
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
                # Use provided detector or default to GDPRValidator
                detector = pii_detector or GDPRValidator([])
                pii_columns = detector.detect_pii(df)
                
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