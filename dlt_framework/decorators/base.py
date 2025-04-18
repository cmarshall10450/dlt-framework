"""Base decorators for the DLT Medallion Framework."""

import functools
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar, Union, cast, List

import dlt
from pyspark.sql import DataFrame

from ..core.config import ConfigurationManager
from ..core.dlt_integration import DLTIntegration
from ..core.exceptions import DecoratorError
from ..core.registry import DecoratorRegistry
from ..metadata.batch import BatchManager, SourceMetadata
from ..utils.spark import get_spark_session

F = TypeVar("F", bound=Callable[..., DataFrame])


def medallion(
    layer: str,
    config_path: Optional[Union[str, Path]] = None,
    expectations: Optional[List[Dict[str, Any]]] = None,
    metrics: Optional[List[Dict[str, Any]]] = None,
    table_properties: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None,
    source_metadata: Optional[SourceMetadata] = None,
    batch_id: Optional[str] = None,
    **options: Any,
) -> Callable[[F], F]:
    """
    Main decorator for DLT Medallion Framework tables.

    This decorator should be the outermost decorator in the stack and handles:
    - Configuration loading and validation
    - Layer validation
    - Integration with DLT
    - Decorator dependency resolution
    - DLT expectations and quality metrics
    - Table properties and comments
    - Batch and source metadata tracking

    Args:
        layer: The layer this table belongs to (bronze, silver, or gold)
        config_path: Optional path to a YAML configuration file
        expectations: Optional list of DLT expectations
        metrics: Optional list of DLT quality metrics
        table_properties: Optional dictionary of table properties
        comment: Optional table comment
        source_metadata: Optional metadata about the source data
        batch_id: Optional batch ID for tracking (uses DLT pipeline run ID if not provided)
        **options: Additional configuration options that override file-based config

    Returns:
        Decorated function that returns a DataFrame

    Raises:
        DecoratorError: If there are issues with the decorator setup
    """
    def decorator(func: F) -> F:
        # Get the registry instance
        registry = DecoratorRegistry()
        
        # Register this decorator instance
        decorator_name = f"medallion_{func.__name__}"
        registry.register(
            decorator_name,
            medallion,
            metadata={
                "layer": layer,
                "options": options,
                "expectations": expectations or [],
                "metrics": metrics or [],
                "table_properties": table_properties or {},
                "comment": comment,
                "source_metadata": source_metadata,
                "batch_id": batch_id,
            },
        )
        
        # Load and validate configuration
        config_manager = ConfigurationManager(config_path)
        if options:
            config_manager.update_config({"table": options})
        
        @dlt.table
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> DataFrame:
            # Get all decorators applied to this function in correct order
            decorators = registry.get_function_decorators(func.__name__)
            ordered_decorators = registry.resolve_dependencies(decorators)
            
            # Apply DLT table properties and comment
            dlt_integration = DLTIntegration()
            dlt_integration.apply_table_properties(table_properties)
            dlt_integration.set_table_comment(comment)
            
            # Execute the decorated function
            result = func(*args, **kwargs)
            
            # Apply batch and source metadata if provided or if this is a bronze layer
            if source_metadata or layer == "bronze":
                # Use provided source metadata or create default for bronze layer
                meta = source_metadata or SourceMetadata(
                    source_type="table",
                    source_path=func.__name__,  # Use function name as default source path
                    format="delta"  # Default format for DLT tables
                )
                result = BatchManager.apply_batch_metadata(result, meta, batch_id)
            
            # Apply DLT expectations and metrics
            if expectations:
                result = dlt_integration.add_expectations(result, expectations)
            if metrics:
                result = dlt_integration.add_quality_metrics(result, metrics)
            
            # Apply decorators in order
            for decorator_name in ordered_decorators:
                decorator_func = registry.get_decorator(decorator_name)
                metadata = registry.get_metadata(decorator_name)
                # Apply decorator-specific transformations here
                # This will be implemented as we add more specific decorators
            
            return result
        
        # Register that this function has been decorated
        registry.register_decorated_function(func.__name__, decorator_name)
        
        return cast(F, wrapper)
    
    return decorator 