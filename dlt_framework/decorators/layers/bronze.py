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
    config_path: Optional[str] = None,
    config: Optional[BronzeConfig] = None,
    pii_detector: Optional[PIIDetector] = None
) -> Callable:
    """Bronze layer decorator for the DLT Medallion Framework.
    
    This decorator applies bronze layer functionality including:
    - Data quality expectations
    - Metrics computation
    - PII detection
    - Schema evolution
    - Quarantine handling
    
    Args:
        config_path: Path to YAML configuration file
        config: BronzeConfig object (alternative to config_path)
        pii_detector: Optional PII detector implementation
        
    Returns:
        Decorated function that processes a DataFrame through the bronze layer
    """
    def decorator(f: TransformFunc) -> TransformFunc:
        # Get configuration
        bronze_config = ConfigurationManager.get_bronze_config(config_path, config)
        
        # Initialize DLT integration
        dlt_integration = DLTIntegration()
        if bronze_config.quarantine:
            dlt_integration.initialize_quarantine(bronze_config.quarantine)

        # Get standard DLT expectation decorators (non-quarantine)
        decorators = []
        if bronze_config.validations:
            decorators.extend(
                dlt_integration.get_expectation_decorators(bronze_config.validations)
            )

        # Add quality metrics as decorators if configured
        if bronze_config.monitoring and bronze_config.monitoring.metrics:
            decorators.append(
                dlt_integration.add_quality_metrics(bronze_config.monitoring.metrics)
            )

        # Prepare table properties
        table_props = dlt_integration.prepare_table_properties(
            bronze_config.table,
            Layer.BRONZE,
            bronze_config.governance
        )

        # Add DLT table decorator last
        decorators.append(dlt.table(**table_props))

        def wrapper(*args, **kwargs) -> DataFrame:
            # Get DataFrame from original function
            df = f(*args, **kwargs)

            # Apply quarantine expectations directly to DataFrame if configured
            if bronze_config.validations:
                df = dlt_integration.apply_expectations_to_dataframe(
                    df,
                    bronze_config.validations,
                    source_table=bronze_config.table.get_full_table_name()
                )

            # Apply PII detection if configured
            if bronze_config.pii_detection and pii_detector:
                df = pii_detector.detect(df)

            return df

        # Apply all decorators in sequence
        decorated = wrapper
        for decorator in decorators:
            decorated = decorator(decorated)

        return decorated

    return decorator 