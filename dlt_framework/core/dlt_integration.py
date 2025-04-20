"""Delta Live Tables integration module for the DLT Medallion Framework."""

from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from collections import defaultdict

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col, struct, array, lit

from ..config.models import (
    Expectation, 
    Metric, 
    ExpectationAction, 
    Layer, 
    UnityTableConfig, 
    GovernanceConfig,
    QuarantineConfig
)
from .exceptions import DLTFrameworkError
from .quarantine_manager import QuarantineManager


class DLTIntegration:
    """Handles integration with Delta Live Tables functionality."""

    def __init__(self):
        """Initialize the DLT integration."""
        self._quarantine_manager = None

    @property
    def quarantine_manager(self) -> Optional[QuarantineManager]:
        """Get the quarantine manager instance."""
        return self._quarantine_manager

    def initialize_quarantine(self, config: QuarantineConfig) -> None:
        """Initialize the quarantine manager with configuration.
        
        Args:
            config: Quarantine configuration
        """
        if config and config.enabled:
            self._quarantine_manager = QuarantineManager(config)

    @staticmethod
    def prepare_table_properties(
        table_config: UnityTableConfig,
        layer: Layer,
        governance: Optional[GovernanceConfig] = None,
        partition_cols: Optional[List[str]] = None,
        cluster_by: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Prepare properties for @dlt.table decorator.

        Args:
            table_config: Unity Catalog table configuration
            layer: The layer this table belongs to
            governance: Optional governance configuration
            partition_cols: Optional list of partition columns
            cluster_by: Optional list of clustering columns

        Returns:
            Dictionary of properties for @dlt.table decorator
        """
        table_props = {}

        # Set basic table properties
        table_props["name"] = table_config.name
        if table_config.description:
            table_props["comment"] = table_config.description
        if partition_cols:
            table_props["partition_cols"] = partition_cols
        if cluster_by:
            table_props["cluster_by"] = cluster_by

        # Prepare table properties
        table_properties = {}

        # Add Unity Catalog metadata
        table_properties["target"] = f"{table_config.catalog}.{table_config.schema_name}.{table_config.name}"

        # Add column comments
        if table_config.column_comments:
            for column, comment in table_config.column_comments.items():
                table_properties[f"column_comment.{column}"] = comment

        # Generate and add governance tags
        governance_tags = table_config.get_governance_tags(layer, governance)
        for key, value in governance_tags.items():
            table_properties[f"tag.{key}"] = str(value)

        # Add Delta properties
        if table_config.properties:
            table_properties.update({key: str(value) for key, value in table_config.properties.items()})

        if table_properties:
            table_props["table_properties"] = table_properties

        return table_props

    @staticmethod
    def create_quality_metrics_decorator(metrics: List[Metric]) -> Callable:
        """
        Create a DLT decorator for quality metrics.
        
        Args:
            metrics: List of metrics to apply
            
        Returns:
            Decorator function that applies the metrics
        """
        if not metrics:
            return lambda f: f

        metric_dict = {
            metric.name: metric.value 
            for metric in metrics 
            if metric.name and metric.value
        }
        
        return dlt.expect_all(metric_dict) if metric_dict else lambda f: f 