"""Delta Live Tables integration module for the DLT Medallion Framework."""

from typing import Any, Dict, List, Optional, Union, Tuple
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

    def add_expectations(
        self,
        expectations: List[Expectation],
        source_table: Optional[str] = None,
        quarantine_config: Optional[QuarantineConfig] = None
    ) -> Any:
        """
        Create DLT expectation decorators to be applied to a transformation function.

        Args:
            expectations: List of Expectation objects
            source_table: Optional source table name for quarantine
            quarantine_config: Optional quarantine configuration

        Returns:
            Function decorator that applies DLT expectations

        Example:
            @DLTIntegration.add_expectations([
                Expectation(
                    name="valid_id",
                    constraint="id IS NOT NULL",
                    action="fail"
                ),
                Expectation(
                    name="valid_amount",
                    constraint="amount > 0",
                    action="quarantine"
                )
            ], source_table="catalog.schema.table")
            def transform_data(df: DataFrame) -> DataFrame:
                return df
        """
        if not expectations:
            return lambda f: f

        # Initialize quarantine if config provided
        if quarantine_config:
            self.initialize_quarantine(quarantine_config)

        def decorator(f):
            def wrapper(df: DataFrame) -> DataFrame:
                # Group expectations by action
                action_groups = defaultdict(list)
                for exp in expectations:
                    action_groups[exp.action].append(exp)

                result_df = df

                # Handle non-quarantine expectations first using DLT native decorators
                for action, exps in action_groups.items():
                    if action == ExpectationAction.QUARANTINE:
                        continue

                    # Create expectation dictionary for expect_all decorators
                    exp_dict = {exp.name: exp.constraint for exp in exps}
                    
                    if action == ExpectationAction.DROP:
                        result_df = dlt.expect_all_or_drop(exp_dict)(lambda df: df)(result_df)
                    elif action == ExpectationAction.FAIL:
                        result_df = dlt.expect_all_or_fail(exp_dict)(lambda df: df)(result_df)
                    else:  # Default to warn
                        result_df = dlt.expect_all(exp_dict)(lambda df: df)(result_df)

                # Handle quarantine expectations if configured
                quarantine_exps = action_groups.get(ExpectationAction.QUARANTINE, [])
                if quarantine_exps and self._quarantine_manager:
                    # Use quarantine manager to handle invalid records
                    valid_df, _ = self._quarantine_manager.quarantine_records_by_expectations(
                        result_df,
                        quarantine_exps,
                        batch_id=None  # Could be added as a parameter if needed
                    )
                    result_df = valid_df

                return result_df

            return wrapper
        return decorator

    @staticmethod
    def add_quality_metrics(metrics: List[Metric]) -> Any:
        """
        Create DLT quality metric decorators to be applied to a transformation function.

        Args:
            metrics: List of Metric objects

        Returns:
            Function decorator that applies DLT quality metrics

        Example:
            @DLTIntegration.add_quality_metrics([
                Metric(name="null_count", value="COUNT(*) WHERE id IS NULL"),
                Metric(name="total_revenue", value="SUM(amount)")
            ])
            def transform_data(df: DataFrame) -> DataFrame:
                return df
        """
        if not metrics:
            return lambda f: f

        def decorator(f):
            decorated = f
            # Group metrics into a single expect_all call for efficiency
            metric_dict = {
                metric.name: metric.value 
                for metric in metrics 
                if metric.name and metric.value
            }
            if metric_dict:
                decorated = dlt.expect_all(metric_dict)(decorated)
            return decorated
        return decorator 