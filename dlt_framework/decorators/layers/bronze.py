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

from core import DLTIntegration, DecoratorRegistry
from config import BronzeConfig, ConfigurationManager


# Get singleton registry instance
registry = DecoratorRegistry()


class PIIDetector(Protocol):
    """Protocol for PII detection implementations."""
    def detect_pii(self, df: DataFrame) -> dict[str, list[str]]:
        """Detect PII in the DataFrame.
        
        Args:
            df: The DataFrame to analyze.
            
        Returns:
            A dictionary mapping PII types to lists of column names containing that type.
        """
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
        config: Bronze layer configuration object.
        config_path: Path to configuration file.
        pii_detector: Optional PII detector implementation.
        **kwargs: Additional configuration options.
        
    Returns:
        Decorated function that applies bronze layer functionality.
    """
    def decorator(func: T) -> T:
        """Inner decorator function."""
        # Get function name for registration
        func_name = func.__name__

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            """Wrapper function that applies bronze layer functionality."""
            # Resolve configuration
            resolved_config = ConfigurationManager.resolve_config(
                layer="bronze",
                config_path=config_path,
                config_obj=config,
                **kwargs
            )

            # Get DataFrame from decorated function
            df = func(*args, **inner_kwargs)

            # Apply expectations if configured
            dlt_integration = DLTIntegration()
            if resolved_config.validate:
                df = dlt_integration.add_expectations(df, resolved_config.validate)

            # Apply metrics if configured
            if resolved_config.metrics:
                df = dlt_integration.add_quality_metrics(df, resolved_config.metrics)

            # Detect PII if enabled
            if resolved_config.pii_detection and pii_detector:
                pii_detector.detect_pii(df)

            # Handle quarantine if enabled
            if resolved_config.quarantine:
                # TODO: Implement quarantine logic
                pass

            # Handle schema evolution if enabled
            if resolved_config.schema_evolution:
                # TODO: Implement schema evolution logic
                pass

            return df

        # Register the decorated function
        registry.register(
            name=f"bronze_{func_name}",
            decorator=wrapper,
            metadata={
                "layer": "bronze",
                "layer_type": "dlt_layer",
                "config_class": BronzeConfig.__name__,
                "features": [
                    "data_quality",
                    "metrics",
                    "pii_detection",
                    "quarantine",
                    "schema_evolution"
                ]
            },
            decorator_type="dlt_layer"
        )

        return wrapper

    return decorator 