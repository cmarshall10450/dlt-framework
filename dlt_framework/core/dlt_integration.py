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

    def apply_expectations_to_dataframe(
        self,
        df: DataFrame,
        expectations: List[Expectation],
        source_table: Optional[str] = None
    ) -> DataFrame:
        """
        Directly apply expectations to a DataFrame without using decorators.
        This is used for custom handling within layer wrapper functions.

        Args:
            df: Input DataFrame to process
            expectations: List of expectations to apply
            source_table: Optional source table name for quarantine

        Returns:
            Processed DataFrame with expectations applied
        """
        if not expectations:
            return df

        # Group expectations by action
        action_groups = defaultdict(list)
        for exp in expectations:
            action_groups[exp.action].append(exp)

        result_df = df

        # Handle quarantine expectations first if configured
        quarantine_exps = action_groups.get(ExpectationAction.QUARANTINE, [])
        if quarantine_exps and self._quarantine_manager:
            valid_df, _ = self._quarantine_manager.quarantine_records_by_expectations(
                result_df,
                quarantine_exps,
                batch_id=None
            )
            result_df = valid_df

        # Apply non-quarantine expectations using DLT functions
        for action, exps in action_groups.items():
            if action == ExpectationAction.QUARANTINE:
                continue

            # Create expectation dictionary
            exp_dict = {exp.name: exp.constraint for exp in exps}
            
            # Apply expectations based on action
            if action == ExpectationAction.DROP:
                result_df = dlt.expect_or_drop(exp.name, exp.constraint)(lambda df: df)(result_df)
            elif action == ExpectationAction.FAIL:
                result_df = dlt.expect_or_fail(exp.name, exp.constraint)(lambda df: df)(result_df)
            else:  # Default to warn
                result_df = dlt.expect(exp.name, exp.constraint)(lambda df: df)(result_df)

        return result_df

    def get_expectation_decorators(
        self,
        expectations: List[Expectation]
    ) -> List[Callable]:
        """
        Get a list of DLT expectation decorators for standard expectations (non-quarantine).
        
        Args:
            expectations: List of expectations to convert to decorators
            
        Returns:
            List of decorator functions to apply
        """
        if not expectations:
            return []

        decorators = []
        non_quarantine_exps = defaultdict(dict)
        
        # Group non-quarantine expectations by action
        for exp in expectations:
            if exp.action != ExpectationAction.QUARANTINE:
                non_quarantine_exps[exp.action][exp.name] = exp.constraint

        # Create decorators for each action type
        if ExpectationAction.DROP in non_quarantine_exps:
            decorators.append(
                dlt.expect_all_or_drop(non_quarantine_exps[ExpectationAction.DROP])
            )

        if ExpectationAction.FAIL in non_quarantine_exps:
            decorators.append(
                dlt.expect_all_or_fail(non_quarantine_exps[ExpectationAction.FAIL])
            )

        # For any other action type, use dlt.expect_all (warning-level expectations)
        other_exps = {}
        for action, exps in non_quarantine_exps.items():
            if action not in (ExpectationAction.DROP, ExpectationAction.FAIL, ExpectationAction.QUARANTINE):
                other_exps.update(exps)
        
        if other_exps:
            decorators.append(dlt.expect_all(other_exps))

        return decorators

    def add_expectations(
        self,
        expectations: List[Expectation],
        source_table: Optional[str] = None,
        quarantine_config: Optional[QuarantineConfig] = None
    ) -> Any:
        """
        Create DLT expectation decorators to be applied to a transformation function.
        This method is maintained for backward compatibility but prefer using
        get_expectation_decorators() and apply_expectations_to_dataframe() for more control.

        Args:
            expectations: List of expectations to apply
            source_table: Optional source table name for quarantine
            quarantine_config: Optional quarantine configuration

        Returns:
            Function decorator that applies DLT expectations
        """
        if not expectations:
            return lambda f: f

        # Initialize quarantine if config provided
        if quarantine_config:
            self.initialize_quarantine(quarantine_config)

        # Get standard expectation decorators
        decorators = self.get_expectation_decorators(expectations)

        def decorator(f):
            # Apply all decorators in sequence
            decorated = f
            for dec in decorators:
                decorated = dec(decorated)
            return decorated

        return decorator

    @staticmethod
    def add_quality_metrics(metrics: List[Metric]) -> Any:
        """
        Create DLT quality metric decorators to be applied to a transformation function.

        Args:
            metrics: List of Metric objects

        Returns:
            Function decorator that applies DLT quality metrics
        """
        if not metrics:
            return lambda f: f

        # Create metrics dictionary
        metric_dict = {
            metric.name: metric.value 
            for metric in metrics 
            if metric.name and metric.value
        }

        # Apply all metrics in a single decorator
        return lambda f: dlt.expect_all(metric_dict)(f) if metric_dict else f 