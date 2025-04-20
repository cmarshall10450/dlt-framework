# dlt_framework/decorators/layers/bronze.py

from functools import wraps
from typing import Any, Callable, Optional, Protocol, TypeVar

from pyspark.sql import DataFrame, SparkSession
import dlt

from dlt_framework.core import DLTIntegration, DecoratorRegistry
from dlt_framework.config import BronzeConfig, ConfigurationManager, Layer, ExpectationAction
from dlt_framework.quarantine.definition import create_quarantine_table_function
from dlt_framework.quarantine.processor import process_quarantine_records


# Get singleton registry instance
registry = DecoratorRegistry()


# Type variable for functions that return a DataFrame
T = TypeVar("T", bound=Callable[..., DataFrame])


def bronze(
    config: Optional[BronzeConfig] = None,
    config_path: Optional[str] = None,
    **kwargs: Any,
) -> Callable[[T], T]:
    """Bronze layer decorator with proper quarantine handling.
    
    Args:
        config: Bronze layer configuration object
        config_path: Path to configuration file
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
        
        # Extract quarantine expectations
        quarantine_expectations = [
            exp for exp in config_obj.validations 
            if exp.action == ExpectationAction.QUARANTINE
        ]
        
        # Extract standard DLT expectations (non-quarantine)
        standard_expectations = [
            exp for exp in config_obj.validations 
            if exp.action != ExpectationAction.QUARANTINE
        ]

        # Define quarantine data generation function
        quarantine_data = None
        
        @wraps(func)
        def wrapper(*args: Any, **inner_kwargs: Any) -> DataFrame:
            """Wrapper that applies bronze functionality."""
            nonlocal quarantine_data
            
            # Get DataFrame from decorated function
            df = func(*args, **inner_kwargs)
            
            # Apply custom functionality (PII detection, schema evolution)
            # ...other processing...
            
            # Handle quarantine if configured
            if config_obj.quarantine and config_obj.quarantine.enabled and quarantine_expectations:
                valid_df, invalid_df = process_quarantine_records(
                    df,
                    quarantine_expectations,
                    config_obj.quarantine,
                    config_obj.table.get_full_table_name(),
                    None  # batch_id
                )
                
                # Store invalid records for quarantine table function
                quarantine_data = invalid_df
                
                # Continue with valid records only
                df = valid_df
            
            return df
        
        # Apply standard DLT expectations as decorators
        decorated_function = wrapper
        
        for exp in standard_expectations:
            if exp.action == ExpectationAction.DROP:
                decorated_function = dlt.expect_or_drop(
                    exp.name, exp.constraint
                )(decorated_function)
            elif exp.action == ExpectationAction.FAIL:
                decorated_function = dlt.expect_or_fail(
                    exp.name, exp.constraint
                )(decorated_function)
            elif exp.action == ExpectationAction.WARN:
                decorated_function = dlt.expect(
                    exp.name, exp.constraint
                )(decorated_function)
        
        # Apply DLT table decorator
        dlt_decorated = dlt.table(**table_props)(decorated_function)
        
        # Register with the framework
        registry.register(
            name=f"bronze_{func_name}",
            decorator=dlt_decorated,
            metadata={
                "layer": "bronze",
                "config_class": BronzeConfig.__name__,
                "features": [
                    "data_quality",
                    "metrics",
                    "pii_detection",
                    "schema_evolution",
                    "quarantine"
                ]
            }
        )
        
        # Create and register quarantine table if needed
        if config_obj.quarantine and config_obj.quarantine.enabled:
            # Function to supply quarantine data
            def get_quarantine_data() -> DataFrame:
                if quarantine_data is None or quarantine_data.isEmpty():
                    # Return empty DataFrame with correct schema if no quarantine data
                    return SparkSession.getActiveSession().createDataFrame(
                        [], 
                        schema=quarantine_data.schema if quarantine_data else None
                    )
                return quarantine_data
            
            # Create quarantine table function
            quarantine_function = create_quarantine_table_function(
                config_obj.table.get_full_table_name(),
                config_obj.quarantine,
                get_quarantine_data
            )
            
            # Register quarantine table
            registry.register(
                name=f"quarantine_{func_name}",
                decorator=quarantine_function,
                metadata={
                    "layer": "bronze",
                    "parent": f"bronze_{func_name}",
                    "type": "quarantine",
                    "config_class": "QuarantineConfig"
                }
            )
        
        return dlt_decorated

    return decorator