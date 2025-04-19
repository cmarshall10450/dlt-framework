"""Bronze layer decorator for the DLT Medallion Framework.

This decorator applies bronze layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII detection
- Schema evolution handling
- Raw data quarantine
"""
from functools import wraps
from typing import Callable, Optional, Protocol, TypeVar
from pyspark.sql import DataFrame

from dlt_framework.core.config_models import BronzeConfig
from dlt_framework.core.dlt_integration import DLTIntegration
from dlt_framework.core.registry import DecoratorRegistry


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
) -> Callable[[T], T]:
    """Bronze layer decorator.
    
    Args:
        config: Bronze layer configuration object.
        config_path: Path to configuration file.
        pii_detector: Optional PII detector implementation.
        
    Returns:
        Decorated function that applies bronze layer functionality.
    """
    def decorator(func: T) -> T:
        """Inner decorator function."""
        # Get function name for registration
        func_name = func.__name__

        @wraps(func)
        def wrapper(*args, **kwargs) -> DataFrame:
            """Wrapper function that applies bronze layer functionality."""
            # Resolve configuration
            resolved_config = DLTIntegration.resolve_config(
                config=config,
                config_path=config_path,
                config_class=BronzeConfig
            )

            # Get DataFrame from decorated function
            df = func(*args, **kwargs)

            # Apply expectations if configured
            if resolved_config.validate:
                for expectation in resolved_config.validate:
                    DLTIntegration.add_expectation(
                        name=expectation.name,
                        constraint=expectation.constraint
                    )

            # Apply metrics if configured
            if resolved_config.metrics:
                for metric in resolved_config.metrics:
                    DLTIntegration.add_metric(
                        name=metric.name,
                        value=metric.value
                    )

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