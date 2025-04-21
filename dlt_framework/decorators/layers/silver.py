"""Silver layer decorator for the DLT Medallion Framework.

This decorator applies silver layer-specific functionality including:
- Data quality expectations
- Metrics computation
- PII masking
- Reference data validation
"""
from functools import wraps, reduce
from typing import Any, Callable, Optional, TypeVar, Dict, List, Union
from collections import defaultdict
import json
from datetime import datetime

from pyspark.sql import DataFrame
import dlt
from pyspark.sql.functions import col, current_timestamp

from dlt_framework.core import DLTIntegration, DecoratorRegistry, ReferenceManager
from dlt_framework.config import SilverConfig, ConfigurationManager, Layer, Expectation, ExpectationAction


# Get singleton registry instance
registry = DecoratorRegistry()


# Type variable for functions that return a DataFrame
T = TypeVar("T", bound=Callable[..., DataFrame])


def silver(
    config_path: Optional[str] = None,
    config: Optional[SilverConfig] = None,
    reference_manager: Optional[ReferenceManager] = None
) -> Callable:
    """Silver layer decorator with enhanced data quality and lineage."""
    def decorator(f: Callable) -> Callable:
        @wraps(f)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Resolve configuration
            silver_config = ConfigurationManager.resolve_config(
                layer=Layer.SILVER,
                config_path=config_path,
                config_obj=config
            )

            # Initialize DLT integration
            dlt_integration = DLTIntegration(silver_config)

            # Initialize reference manager if needed
            ref_manager = reference_manager or ReferenceManager()

            # Prepare table properties with enhanced lineage
            table_name = silver_config.table.name or f.__name__
            full_table_name = f"{silver_config.table.catalog}.{silver_config.table.schema_name}.{table_name}"
            
            # Log table registration for debugging
            print(f"Registering silver table with name: {full_table_name}")
            
            table_properties = {
                "layer": "silver",
                "pipelines.autoOptimize.managed": "true",
                "delta.enableChangeDataFeed": "true" if silver_config.scd else "false",
                "delta.columnMapping.mode": "name",
                "pipelines.metadata.createdBy": "dlt_framework",
                "pipelines.metadata.createdTimestamp": datetime.now().isoformat(),
                "comment": json.dumps({
                    "description": f.__doc__ or f"Silver table for {table_name}",
                    "config": silver_config.dict(),
                    "features": {
                        "deduplication": silver_config.deduplication,
                        "normalization": silver_config.normalization,
                        "scd": bool(silver_config.scd),
                        "references": bool(silver_config.references)
                    }
                })
            }

            # Get DLT expectation decorators
            expectation_decorators = []
            if silver_config.validations:
                expectation_decorators.extend(
                    dlt_integration.get_expectation_decorators(silver_config.validations)
                )

            # Add quality metrics if monitoring is configured
            if silver_config.monitoring_config:
                metrics_decorator = dlt_integration.add_quality_metrics()
                expectation_decorators.append(metrics_decorator)

            # Add DLT table decorator
            table_decorator = dlt.table(
                name=full_table_name,
                comment=f"Silver layer table for {table_name}",
                table_properties=table_properties,
                temporary=False,
                path=f"{silver_config.table.storage_location}/{table_name}" if silver_config.table.storage_location else None
            )
            expectation_decorators.append(table_decorator)

            # Apply all decorators to the function
            decorated_func = reduce(lambda x, y: y(x), expectation_decorators, f)

            # Get DataFrame from decorated function
            df = decorated_func(*args, **kwargs)

            # Apply normalization if configured
            if silver_config.normalization:
                df = dlt_integration.apply_normalization(df)

            # Apply deduplication if configured
            if silver_config.deduplication:
                df = dlt_integration.apply_deduplication(df)

            # Apply SCD if configured
            if silver_config.scd:
                df = dlt_integration.apply_scd(df)

            # Apply reference data if configured
            if silver_config.references:
                df = ref_manager.apply_references(df)

            # Register decorated function in DecoratorRegistry
            DecoratorRegistry.register(
                func=f,
                layer=Layer.SILVER,
                features={
                    "deduplication": silver_config.deduplication,
                    "normalization": silver_config.normalization,
                    "scd": bool(silver_config.scd),
                    "references": bool(silver_config.references)
                },
                config=silver_config
            )

            return df

        return wrapper

    return decorator


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


def silver_old(
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