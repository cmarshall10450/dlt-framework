"""Layer-specific decorators for the DLT Medallion Framework."""

from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, Optional, TypeVar, Union, cast

from pyspark.sql import DataFrame

from .base import medallion
from ..core.registry import DecoratorRegistry

F = TypeVar("F", bound=Callable[..., DataFrame])


def bronze(
    config_path: Optional[Union[str, Path]] = None,
    expectations: Optional[list[Dict[str, Any]]] = None,
    metrics: Optional[list[Dict[str, Any]]] = None,
    table_properties: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None,
    **options: Any,
) -> Callable[[F], F]:
    """
    Bronze layer decorator for raw data ingestion and validation.

    This decorator is specifically designed for bronze layer tables which focus on:
    - Raw data ingestion
    - Schema validation
    - Data quality checks
    - Invalid record quarantining
    - Source metadata preservation

    Args:
        config_path: Optional path to a YAML configuration file
        expectations: Optional list of DLT expectations
        metrics: Optional list of DLT quality metrics
        table_properties: Optional dictionary of table properties
        comment: Optional table comment
        **options: Additional configuration options that override file-based config

    Returns:
        Decorated function that returns a DataFrame

    Example:
        @bronze(
            expectations=[{"name": "valid_id", "constraint": "id IS NOT NULL"}],
            metrics=[{"name": "null_count", "value": "COUNT(*) WHERE id IS NULL"}]
        )
        def ingest_raw_data() -> DataFrame:
            return spark.read.format("json").load("/path/to/data")
    """
    def decorator(func: F) -> F:
        # Get registry instance
        registry = DecoratorRegistry()
        
        # Register this decorator with type information
        decorator_name = f"bronze_{func.__name__}"
        registry.register(
            decorator_name,
            bronze,
            metadata={
                "layer": "bronze",
                "expectations": expectations or [],
                "metrics": metrics or [],
                "table_properties": table_properties or {},
                "comment": comment,
                "options": options,
            },
            decorator_type="layer"
        )
        
        # Apply the base medallion decorator
        return cast(
            F,
            medallion(
                layer="bronze",
                config_path=config_path,
                expectations=expectations,
                metrics=metrics,
                table_properties=table_properties,
                comment=comment,
                **options
            )(func)
        )
    return decorator


def silver(
    config_path: Optional[Union[str, Path]] = None,
    expectations: Optional[list[Dict[str, Any]]] = None,
    metrics: Optional[list[Dict[str, Any]]] = None,
    table_properties: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None,
    **options: Any,
) -> Callable[[F], F]:
    """
    Silver layer decorator for data normalization and cleansing.

    This decorator is specifically designed for silver layer tables which focus on:
    - Data normalization and standardization
    - Deduplication
    - Data type enforcement
    - Business rule validation
    - Slowly Changing Dimension (SCD) handling
    - Change Data Capture (CDC) processing

    Args:
        config_path: Optional path to a YAML configuration file
        expectations: Optional list of DLT expectations
        metrics: Optional list of DLT quality metrics
        table_properties: Optional dictionary of table properties
        comment: Optional table comment
        **options: Additional configuration options that override file-based config

    Returns:
        Decorated function that returns a DataFrame

    Example:
        @silver(
            expectations=[
                {"name": "valid_email", "constraint": "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'"}
            ]
        )
        def normalize_customer_data() -> DataFrame:
            return spark.table("LIVE.bronze_customers").transform(normalize_email)
    """
    def decorator(func: F) -> F:
        # Get registry instance
        registry = DecoratorRegistry()
        
        # Register this decorator with type information
        decorator_name = f"silver_{func.__name__}"
        registry.register(
            decorator_name,
            silver,
            metadata={
                "layer": "silver",
                "expectations": expectations or [],
                "metrics": metrics or [],
                "table_properties": table_properties or {},
                "comment": comment,
                "options": options,
            },
            decorator_type="layer"
        )
        
        # Apply the base medallion decorator
        return cast(
            F,
            medallion(
                layer="silver",
                config_path=config_path,
                expectations=expectations,
                metrics=metrics,
                table_properties=table_properties,
                comment=comment,
                **options
            )(func)
        )
    return decorator


def gold(
    config_path: Optional[Union[str, Path]] = None,
    expectations: Optional[list[Dict[str, Any]]] = None,
    metrics: Optional[list[Dict[str, Any]]] = None,
    table_properties: Optional[Dict[str, Any]] = None,
    comment: Optional[str] = None,
    **options: Any,
) -> Callable[[F], F]:
    """
    Gold layer decorator for business-level aggregations and metrics.

    This decorator is specifically designed for gold layer tables which focus on:
    - Business metrics computation
    - Dimensional modeling
    - Aggregations and rollups
    - Reference data management
    - Business rule enforcement
    - Data mart preparation

    Args:
        config_path: Optional path to a YAML configuration file
        expectations: Optional list of DLT expectations
        metrics: Optional list of DLT quality metrics
        table_properties: Optional dictionary of table properties
        comment: Optional table comment
        **options: Additional configuration options that override file-based config

    Returns:
        Decorated function that returns a DataFrame

    Example:
        @gold(
            expectations=[
                {"name": "total_check", "constraint": "total_amount >= 0"}
            ],
            metrics=[
                {"name": "daily_sales", "value": "SUM(total_amount)"}
            ]
        )
        def calculate_daily_sales() -> DataFrame:
            return spark.table("LIVE.silver_orders").groupBy("date").sum("amount")
    """
    def decorator(func: F) -> F:
        # Get registry instance
        registry = DecoratorRegistry()
        
        # Register this decorator with type information
        decorator_name = f"gold_{func.__name__}"
        registry.register(
            decorator_name,
            gold,
            metadata={
                "layer": "gold",
                "expectations": expectations or [],
                "metrics": metrics or [],
                "table_properties": table_properties or {},
                "comment": comment,
                "options": options,
            },
            decorator_type="layer"
        )
        
        # Apply the base medallion decorator
        return cast(
            F,
            medallion(
                layer="gold",
                config_path=config_path,
                expectations=expectations,
                metrics=metrics,
                table_properties=table_properties,
                comment=comment,
                **options
            )(func)
        )
    return decorator 