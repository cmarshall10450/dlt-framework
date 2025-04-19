"""Silver layer decorator implementation."""

from functools import wraps
from pathlib import Path
from typing import Any, Callable, Optional, Protocol, TypeVar, Union, cast

from pyspark.sql import DataFrame

from ...core.config_manager import ConfigurationManager
from ...core.config_models import SilverConfig
from ...core.dlt_integration import DLTIntegration
from ...core.registry import DecoratorRegistry
from ...validation.gdpr import GDPRValidator, GDPRField


class PIIMasker(Protocol):
    """Protocol for PII masking implementations."""
    
    def mask_pii(self, df: DataFrame) -> DataFrame:
        """
        Mask PII columns in the DataFrame.
        
        Args:
            df: DataFrame to mask
            
        Returns:
            DataFrame with masked PII columns
        """
        ...


T = TypeVar("T", bound=Callable[..., DataFrame])


def silver(
    config_path: Optional[Union[str, Path]] = None,
    config: Optional[SilverConfig] = None,
    pii_masker: Optional[PIIMasker] = None,
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
        pii_masker: Optional custom PII masker implementation
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
                    # Use provided masker or default to GDPRValidator
                    masker = pii_masker or GDPRValidator(pii_fields)
                    df = masker.mask_pii(df)

            return df

        return cast(T, wrapper)

    return decorator 