"""Delta Live Tables integration module for the DLT Medallion Framework."""

from typing import Any, Dict, List, Optional, Union

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr

from ..config.models import (
    Expectation, 
    Metric, 
    ExpectationAction, 
    Layer, 
    UnityTableConfig, 
    GovernanceConfig
)
from .exceptions import DLTFrameworkError
from .quarantine_manager import QuarantineManager


class DLTIntegration:
    """Handles integration with Delta Live Tables functionality."""

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
    def add_expectations(
        df: DataFrame, 
        expectations: List[Expectation],
        source_table: Optional[str] = None
    ) -> DataFrame:
        """
        Add DLT expectations to a DataFrame.

        Args:
            df: The input DataFrame
            expectations: List of Expectation objects with actions
            source_table: Optional source table name for quarantine metadata

        Returns:
            DataFrame with applied expectations

        Example:
            df = DLTIntegration.add_expectations(
                df,
                [
                    Expectation(
                        name="valid_id", 
                        constraint="id IS NOT NULL",
                        action=ExpectationAction.DROP
                    ),
                    Expectation(
                        name="valid_email", 
                        constraint="email LIKE '%@%'",
                        action=ExpectationAction.FAIL
                    )
                ]
            )
        """
        if not expectations:
            return df

        # Group expectations by action
        action_groups: Dict[ExpectationAction, List[Expectation]] = {}
        for exp in expectations:
            if not exp.name or not exp.constraint:
                raise DLTFrameworkError(
                    f"Invalid expectation configuration: {exp}"
                )
            action_groups.setdefault(exp.action, []).append(exp)

        result_df = df

        # Handle quarantine expectations first if any
        quarantine_expectations = action_groups.get(ExpectationAction.QUARANTINE, [])
        if quarantine_expectations and source_table:
            quarantine_manager = QuarantineManager(source_table)
            quarantine_table = quarantine_manager.get_quarantine_table_name(source_table)
            result_df, _ = quarantine_manager.quarantine_records(
                result_df, 
                quarantine_expectations,
                quarantine_table
            )

        # Handle other expectations
        for action, exps in action_groups.items():
            if action == ExpectationAction.QUARANTINE:
                continue

            for exp in exps:
                if action == ExpectationAction.DROP:
                    dlt.expect_or_drop(result_df, exp.name, exp.constraint)
                elif action == ExpectationAction.FAIL:
                    dlt.expect_or_fail(result_df, exp.name, exp.constraint)
                else:
                    raise DLTFrameworkError(
                        f"Invalid expectation action '{action}'. Must be one of: "
                        f"{', '.join(a.value for a in ExpectationAction)}"
                    )

        return result_df

    @staticmethod
    def add_quality_metrics(df: DataFrame, metrics: List[Metric]) -> DataFrame:
        """
        Add DLT quality metrics to a DataFrame.

        Args:
            df: The input DataFrame
            metrics: List of Metric objects

        Returns:
            DataFrame with applied quality metrics

        Example:
            df = DLTIntegration.add_quality_metrics(
                df,
                [
                    Metric(name="null_count", value="COUNT(*) WHERE id IS NULL"),
                    Metric(name="total_revenue", value="SUM(amount)")
                ]
            )
        """
        for metric in metrics:
            if not metric.name or not metric.value:
                raise DLTFrameworkError(f"Invalid metric configuration: {metric}")

            dlt.expect(df, metric.name, metric.value)

        return df 