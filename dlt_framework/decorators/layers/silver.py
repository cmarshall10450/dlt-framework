"""Silver layer decorator for the DLT Medallion Framework.

This decorator applies silver layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII masking
- Reference data validation
"""
from functools import wraps
from typing import Any, Callable, Optional, TypeVar

from pyspark.sql import DataFrame
import dlt

from dlt_framework.core import DLTIntegration, DecoratorRegistry, ReferenceManager
from dlt_framework.config import SilverConfig, ConfigurationManager, Layer


# Get singleton registry instance
registry = DecoratorRegistry()


# Type variable for functions that return a DataFrame
T = TypeVar("T", bound=Callable[..., DataFrame])


def silver(
    config: Optional[SilverConfig] = None,
    config_path: Optional[str] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Silver layer decorator.
    
    Args:
        config: Silver layer configuration object
        config_path: Path to configuration file
        **kwargs: Additional configuration options
        
    Returns:
        Decorated function that applies silver layer functionality
    """
    def decorator(func: T) -> T:
        """Inner decorator function."""
        # Get function name for registration
        func_name = func.__name__

        # Resolve configuration
        config_obj = ConfigurationManager.resolve_config(
            layer="silver",
            config_path=config_path,
            config_obj=config,
            **kwargs
        )

        # Get table properties from DLTIntegration
        dlt_integration = DLTIntegration()
        table_props = dlt_integration.prepare_table_properties(
            table_config=config_obj.table,
            layer=Layer.SILVER,
            governance=config_obj.governance
        )

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Initialize reference manager
            ref_manager = ReferenceManager(config_obj)

            # Get DataFrame from decorated function
            df = func(*args, **inner_kwargs)

            # Apply reference data validation if configured
            if config_obj.references:
                df = ref_manager.validate_references(df)

            # Apply deduplication if enabled
            if config_obj.deduplication:
                # TODO: Implement deduplication logic
                pass

            # Apply normalization if enabled
            if config_obj.normalization:
                # TODO: Implement normalization logic
                pass

            # Apply SCD logic if configured
            if config_obj.scd:
                # TODO: Implement SCD logic
                pass

            # Apply expectations if configured
            if config_obj.validations:
                df = dlt_integration.apply_expectations_to_dataframe(df, config_obj.validations)

            # Apply metrics if configured
            if config_obj.metrics:
                df = dlt_integration.add_quality_metrics(df, config_obj.metrics)

            return df

        # Apply DLT table decorator with proper configuration
        decorated = dlt.table(**table_props)(wrapper)

        # Register the decorated function
        registry.register(
            name=f"silver_{func_name}",
            decorator=decorated,
            metadata={
                "layer": "silver",
                "layer_type": "dlt_layer",
                "config_class": SilverConfig.__name__,
                "features": [
                    "data_quality",
                    "metrics",
                    "reference_validation",
                    "deduplication",
                    "normalization",
                    "scd"
                ]
            },
            decorator_type="dlt_layer"
        )

        return decorated

    return decorator 