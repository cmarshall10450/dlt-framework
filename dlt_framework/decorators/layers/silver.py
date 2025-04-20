"""Silver layer decorator for the DLT Medallion Framework.

This decorator applies silver layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII masking
- Reference data validation
"""
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, Dict, List
from collections import defaultdict

from pyspark.sql import DataFrame
import dlt

from dlt_framework.core import DLTIntegration, DecoratorRegistry, ReferenceManager
from dlt_framework.config import SilverConfig, ConfigurationManager, Layer, Expectation, ExpectationAction


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
            layer=Layer.SILVER,
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

        # Initialize quarantine if configured
        if config_obj.quarantine:
            dlt_integration.initialize_quarantine(config_obj.quarantine)

        def create_expectation_decorators(expectations: List[Expectation]) -> List[Callable]:
            """Create DLT expectation decorators grouped by action."""
            if not expectations:
                return []

            # Group expectations by action
            action_groups: Dict[ExpectationAction, Dict[str, str]] = defaultdict(dict)
            for exp in expectations:
                action = ExpectationAction(exp.action) if isinstance(exp.action, str) else exp.action
                if action != ExpectationAction.QUARANTINE:
                    action_groups[action][exp.name] = exp.constraint

            decorators = []
            
            # Create decorators for each action type
            if ExpectationAction.DROP in action_groups:
                decorators.append(dlt.expect_all_or_drop(action_groups[ExpectationAction.DROP]))
            
            if ExpectationAction.FAIL in action_groups:
                decorators.append(dlt.expect_all_or_fail(action_groups[ExpectationAction.FAIL]))
            
            # Handle warn-level expectations
            warn_exps = {}
            for action, exps in action_groups.items():
                if action not in (ExpectationAction.DROP, ExpectationAction.FAIL, ExpectationAction.QUARANTINE):
                    warn_exps.update(exps)
            
            if warn_exps:
                decorators.append(dlt.expect_all(warn_exps))
            
            return decorators

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

            # Handle quarantine expectations if configured
            if config_obj.validations and dlt_integration.quarantine_manager:
                quarantine_exps = [
                    exp for exp in config_obj.validations 
                    if exp.action == ExpectationAction.QUARANTINE
                ]
                if quarantine_exps:
                    df, _ = dlt_integration.quarantine_manager.quarantine_records_by_expectations(
                        df, 
                        quarantine_exps,
                        batch_id=None
                    )

            return df

        # Create decorated function with all necessary decorators
        decorated = func

        # Apply expectation decorators if configured
        if config_obj.validations:
            for decorator_func in create_expectation_decorators(config_obj.validations):
                decorated = decorator_func(decorated)

        # Apply quality metrics decorator if configured
        if config_obj.metrics:
            metrics_decorator = dlt_integration.create_quality_metrics_decorator(config_obj.metrics)
            decorated = metrics_decorator(decorated)

        # Apply DLT table decorator last
        decorated = dlt.table(**table_props)(decorated)

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