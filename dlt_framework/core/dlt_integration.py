"""Delta Live Tables integration module for the DLT Medallion Framework."""

from typing import Any, Dict, List, Optional

import dlt
from pyspark.sql import DataFrame

from .exceptions import DLTFrameworkError


class DLTIntegration:
    """Handles integration with Delta Live Tables functionality."""

    @staticmethod
    def add_expectations(df: DataFrame, expectations: List[Dict[str, Any]]) -> DataFrame:
        """
        Add DLT expectations to a DataFrame.

        Args:
            df: The input DataFrame
            expectations: List of expectation configurations

        Returns:
            DataFrame with applied expectations

        Example expectation format:
        {
            "name": "valid_id",
            "condition": "id IS NOT NULL",
            "action": "DROP"
        }
        """
        for expectation in expectations:
            name = expectation.get("name")
            condition = expectation.get("condition")
            action = expectation.get("action", "DROP")

            if not name or not condition:
                raise DLTFrameworkError(
                    f"Invalid expectation configuration: {expectation}"
                )

            if action.upper() == "DROP":
                dlt.expect(df, name, condition)
            elif action.upper() == "QUARANTINE":
                dlt.expect_or_drop(df, name, condition)
            elif action.upper() == "FAIL":
                dlt.expect_or_fail(df, name, condition)
            else:
                raise DLTFrameworkError(f"Invalid expectation action: {action}")

        return df

    @staticmethod
    def add_quality_metrics(df: DataFrame, metrics: List[Dict[str, Any]]) -> DataFrame:
        """
        Add DLT quality metrics to a DataFrame.

        Args:
            df: The input DataFrame
            metrics: List of metric configurations

        Returns:
            DataFrame with applied quality metrics

        Example metric format:
        {
            "name": "null_count",
            "column": "id",
            "metric_type": "count",
            "condition": "id IS NULL"
        }
        """
        for metric in metrics:
            name = metric.get("name")
            condition = metric.get("condition")

            if not name or not condition:
                raise DLTFrameworkError(f"Invalid metric configuration: {metric}")

            dlt.expect(df, name, condition)

        return df

    @staticmethod
    def apply_table_properties(properties: Optional[Dict[str, Any]] = None) -> None:
        """
        Apply DLT table properties.

        Args:
            properties: Dictionary of table properties

        Example properties:
        {
            "pipelines.reset": "false",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.autoOptimize.autoCompact": "true"
        }
        """
        if not properties:
            return

        for key, value in properties.items():
            dlt.table_property(key, str(value))

    @staticmethod
    def set_table_comment(comment: Optional[str] = None) -> None:
        """
        Set the table comment.

        Args:
            comment: Table comment string
        """
        if comment:
            dlt.table_property("comment", comment) 