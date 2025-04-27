from functools import wraps
from typing import Any, Callable, Optional, Dict, Union

import dlt
from pyspark.sql import DataFrame

from dlt_framework.config import DLTTableConfig, DLTViewConfig

DataFrameTransformer = Callable[[DataFrame], DataFrame]

def _register_dlt_asset(
    asset_config: Dict[str, Any],
    asset_fn: Callable,
    transformer: Optional[DataFrameTransformer] = None
) -> Callable:
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            # Get the DataFrame from the original function
            df = func(*args, **kwargs)
            
            # Apply transformer if provided
            if transformer:
                df = transformer(df)
                
            return df

        return asset_fn(**asset_config)(wrapper)
    
    return decorator

def register_dlt_table(
    config: DLTTableConfig,
    transformer: Optional[DataFrameTransformer] = None
) -> Callable:
    """
    Decorator to register a function as a Databricks Delta Table.
    """
    asset_config = {
        "name": config.get_full_name(),
        "partition_cols": config.partition_cols,
        "cluster_by": config.cluster_by,
        "path": config.path,
        "table_properties": config.table_properties,
        "comment": config.comment,
        "schema": config.data_schema,
        "row_filter": config.row_filter,
        "spark_conf": config.spark_conf,
        "temporary": config.is_temporary
    }

    return _register_dlt_asset(asset_config, dlt.table, transformer)
    
def register_dlt_view(
    config: DLTViewConfig,
    transformer: Optional[DataFrameTransformer] = None
) -> Callable:
    asset_config = {
        "name": config.get_full_name() + "_v",
        "comment": config.comment
    }

    return _register_dlt_asset(asset_config, dlt.view, transformer)