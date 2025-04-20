"""Gold layer decorator for the DLT Medallion Framework.

This decorator applies gold layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII masking verification
- Aggregation
- Business rule validation
"""
from functools import wraps
from typing import Any, Callable, Optional, Protocol, TypeVar

from pyspark.sql import DataFrame
import dlt

from dlt_framework.core import DLTIntegration, DecoratorRegistry
from dlt_framework.config import GoldConfig, ConfigurationManager, Layer


# Get singleton registry instance
registry = DecoratorRegistry()


class PIIDetector(Protocol):
    """Protocol for PII detection implementations."""
    def detect(self, df: DataFrame) -> DataFrame:
        """Detect PII in DataFrame."""
        ...


# Type variable for functions that return a DataFrame
T = TypeVar("T", bound=Callable[..., DataFrame])


def gold(
    config: Optional[GoldConfig] = None,
    config_path: Optional[str] = None,
    pii_detector: Optional[PIIDetector] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Gold layer decorator.
    
    Args:
        config: Gold layer configuration object
        config_path: Path to configuration file
        pii_detector: PII detection implementation
        **kwargs: Additional configuration options
        
    Returns:
        Decorated function that applies gold layer functionality
    """
    def decorator(func: T) -> T:
        """Inner decorator function."""
        # Get function name for registration
        func_name = func.__name__

        # Resolve configuration
        config_obj = ConfigurationManager.resolve_config(
            layer="gold",
            config_path=config_path,
            config_obj=config,
            **kwargs
        )

        # Get table properties from DLTIntegration
        dlt_integration = DLTIntegration()
        table_props = dlt_integration.prepare_table_properties(
            table_config=config_obj.table,
            layer=Layer.GOLD,
            governance=config_obj.governance
        )

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Get DataFrame from decorated function
            df = func(*args, **inner_kwargs)

            # Apply PII verification if configured
            if config_obj.pii_verification and pii_detector:
                df = pii_detector.detect(df)

            # Apply aggregation if configured
            if config_obj.aggregation:
                # TODO: Implement aggregation logic
                pass

            # Apply business rules if configured
            if config_obj.business_rules:
                # TODO: Implement business rule validation
                pass

            # Apply expectations if configured
            if config_obj.validations:
                df = dlt_integration.add_expectations(df, config_obj.validations)

            # Apply metrics if configured
            if config_obj.metrics:
                df = dlt_integration.add_quality_metrics(df, config_obj.metrics)

            return df

        # Apply DLT table decorator with proper configuration
        decorated = dlt.table(**table_props)(wrapper)

        # Register the decorated function
        registry.register(
            name=f"gold_{func_name}",
            decorator=decorated,
            metadata={
                "layer": "gold",
                "layer_type": "dlt_layer",
                "config_class": GoldConfig.__name__,
                "features": [
                    "data_quality",
                    "metrics",
                    "pii_verification",
                    "aggregation",
                    "business_rules"
                ]
            },
            decorator_type="dlt_layer"
        )

        return decorated

    return decorator 