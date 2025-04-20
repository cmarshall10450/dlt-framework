"""Gold layer decorator for the DLT Medallion Framework.

This decorator applies gold layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII masking verification
- Aggregation and metric computation
- Business rule validation
- Reference data validation and lookups
"""
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, Protocol, TypeVar, Union, cast

from pyspark.sql import DataFrame

from dlt_framework.config import ConfigurationManager, GoldConfig
from dlt_framework.core import DLTIntegration, DecoratorRegistry, ReferenceManager
from dlt_framework.validation import GDPRValidator


# Get singleton registry instance
registry = DecoratorRegistry()


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


# Type variable for functions that return a DataFrame
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
    - Reference data validation and lookups
    
    Args:
        config_path: Path to configuration file.
        config: Gold layer configuration object.
        pii_detector: Optional PII detector implementation.
        **kwargs: Additional configuration options.
    """
    def decorator(func: T) -> T:
        # Get function name for registration
        func_name = func.__name__

        # Resolve configuration early to get table name
        config_obj = ConfigurationManager.resolve_config(
            layer="gold",
            config_path=config_path,
            config_obj=config,
            **kwargs
        )

        # Get table properties from DLTIntegration
        dlt_integration = DLTIntegration()
        table_props = dlt_integration.prepare_table_properties(
            catalog=config_obj.table.catalog,
            schema=config_obj.table.schema_name,
            table_name=config_obj.table.name,
            comment=config_obj.table.description,
            properties=config_obj.table.properties,
            tags=config_obj.table.tags,
            column_comments=config_obj.table.column_comments
        )

        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            # Initialize reference manager
            ref_manager = ReferenceManager(config_obj)
            
            # Add reference manager to function context
            inner_kwargs["ref_manager"] = ref_manager

            # Get the DataFrame from the function
            df = func(*args, **inner_kwargs)

            # Apply expectations and metrics if configured
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
                    "business_rules",
                    "reference_data"
                ]
            },
            decorator_type="dlt_layer"
        )

        return decorated

    return decorator 