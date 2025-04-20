"""Bronze layer decorator for the DLT Medallion Framework.

This decorator applies bronze layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII detection
- Schema evolution handling
- Raw data quarantine
"""
from functools import wraps
from typing import Any, Callable, Optional, Protocol, TypeVar

from pyspark.sql import DataFrame
import dlt

from dlt_framework.core import DLTIntegration, DecoratorRegistry
from dlt_framework.config import BronzeConfig, ConfigurationManager, Layer


# Get singleton registry instance
registry = DecoratorRegistry()


class PIIDetector(Protocol):
    """Protocol for PII detection implementations."""
    def detect(self, df: DataFrame) -> DataFrame:
        """Detect PII in DataFrame."""
        ...


# Type variable for functions that return a DataFrame
T = TypeVar("T", bound=Callable[..., DataFrame])


def bronze(
    config: Optional[BronzeConfig] = None,
    config_path: Optional[str] = None,
    pii_detector: Optional[PIIDetector] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Bronze layer decorator.
    
    Args:
        config: Bronze layer configuration object
        config_path: Path to configuration file
        pii_detector: PII detection implementation
        **kwargs: Additional configuration options
        
    Returns:
        Decorated function that applies bronze layer functionality
    """
    def decorator(func: T) -> T:
        """Inner decorator function."""
        # Get function name for registration
        func_name = func.__name__

        # Resolve configuration
        config_obj = ConfigurationManager.resolve_config(
            layer="bronze",
            config_path=config_path,
            config_obj=config,
            **kwargs
        )

        # Get table properties from DLTIntegration
        dlt_integration = DLTIntegration()
        table_props = dlt_integration.prepare_table_properties(
            table_config=config_obj.table,
            layer=Layer.BRONZE,
            governance=config_obj.governance
        )

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Get DataFrame from decorated function
            df = func(*args, **inner_kwargs)

            # Apply PII detection if configured
            if config_obj.governance and config_obj.governance.pii_detection and pii_detector:
                df = pii_detector.detect(df)

            # Apply schema evolution if configured
            if config_obj.governance and config_obj.governance.schema_evolution:
                # TODO: Implement schema evolution logic
                pass

            # Apply quarantine if configured
            if config_obj.quarantine:
                # TODO: Implement quarantine logic
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
            name=f"bronze_{func_name}",
            decorator=decorated,
            metadata={
                "layer": "bronze",
                "layer_type": "dlt_layer",
                "config_class": BronzeConfig.__name__,
                "features": [
                    "data_quality",
                    "metrics",
                    "pii_detection",
                    "schema_evolution",
                    "quarantine"
                ]
            },
            decorator_type="dlt_layer"
        )

        return decorated

    return decorator 