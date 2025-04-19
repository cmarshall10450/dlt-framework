"""Delta Live Tables integration module for the DLT Medallion Framework."""

from typing import Any, Dict, List, Optional, Union

import dlt
from pyspark.sql import DataFrame

from .config_models import Expectation, Metric, ExpectationAction
from .exceptions import DLTFrameworkError


class DLTIntegration:
    """Handles integration with Delta Live Tables functionality."""

    @staticmethod
    def configure_auto_loader(
        source_path: str,
        format: str = "cloudFiles",
        schema_location: Optional[str] = None,
        schema_evolution: bool = True,
        rescue_data: bool = True,
        **options: Any
    ) -> Dict[str, Any]:
        """
        Configure Auto Loader for streaming ingestion.

        Args:
            source_path: Path to source data
            format: Source format (default: cloudFiles)
            schema_location: Optional path to store schema information
            schema_evolution: Whether to enable schema evolution
            rescue_data: Whether to rescue malformed records
            **options: Additional Auto Loader options

        Returns:
            Dictionary of Auto Loader configuration options

        Example:
            config = DLTIntegration.configure_auto_loader(
                source_path="s3://bucket/path",
                format="cloudFiles",
                cloudFiles={"format": "json"},
                schema_evolution=True
            )
        """
        auto_loader_config = {
            "format": format,
            "path": source_path,
            "cloudFiles.schemaEvolution.enabled": str(schema_evolution).lower(),
            "cloudFiles.rescueDataOnError": str(rescue_data).lower(),
        }

        if schema_location:
            auto_loader_config["cloudFiles.schemaLocation"] = schema_location

        # Add any additional options
        for key, value in options.items():
            if isinstance(value, dict):
                # Handle nested options like cloudFiles.format
                for nested_key, nested_value in value.items():
                    full_key = f"{key}.{nested_key}"
                    auto_loader_config[full_key] = str(nested_value)
            else:
                auto_loader_config[key] = str(value)

        return auto_loader_config

    @staticmethod
    def prepare_table_properties(
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        column_comments: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None,
        comment: Optional[str] = None,
        properties: Optional[Dict[str, Any]] = None,
        partition_cols: Optional[List[str]] = None,
        cluster_by: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """
        Prepare properties for @dlt.table decorator.

        Args:
            catalog: Optional catalog name
            schema: Optional schema name
            table_name: Optional table name
            column_comments: Optional dictionary of column comments
            tags: Optional dictionary of Unity Catalog tags
            comment: Optional table comment
            properties: Optional dictionary of additional table properties
            partition_cols: Optional list of partition columns
            cluster_by: Optional list of clustering columns

        Returns:
            Dictionary of properties for @dlt.table decorator
        """
        table_config = {}

        # Set basic table properties
        if table_name:
            table_config["name"] = table_name
        if comment:
            table_config["comment"] = comment
        if partition_cols:
            table_config["partition_cols"] = partition_cols
        if cluster_by:
            table_config["cluster_by"] = cluster_by

        # Prepare table properties
        table_properties = {}

        # Add Unity Catalog metadata
        if catalog and schema:
            table_properties["target"] = f"{catalog}.{schema}.{table_name}"

        # Add column comments
        if column_comments:
            for column, comment in column_comments.items():
                table_properties[f"column_comment.{column}"] = comment

        # Add tags
        if tags:
            for key, value in tags.items():
                table_properties[f"tag.{key}"] = value

        # Add additional properties
        if properties:
            table_properties.update({key: str(value) for key, value in properties.items()})

        if table_properties:
            table_config["table_properties"] = table_properties

        return table_config

    @staticmethod
    def add_expectations(df: DataFrame, expectations: List[Expectation]) -> DataFrame:
        """
        Add DLT expectations to a DataFrame.

        Args:
            df: The input DataFrame
            expectations: List of Expectation objects with actions

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
        for expectation in expectations:
            if not expectation.name or not expectation.constraint:
                raise DLTFrameworkError(
                    f"Invalid expectation configuration: {expectation}"
                )

            # Get the action from the expectation
            action = expectation.action

            if action == ExpectationAction.DROP:
                dlt.expect_or_drop(df, expectation.name, expectation.constraint)
            elif action == ExpectationAction.FAIL:
                dlt.expect_or_fail(df, expectation.name, expectation.constraint)
            elif action == ExpectationAction.QUARANTINE:
                dlt.expect_or_quarantine(df, expectation.name, expectation.constraint)
            else:
                raise DLTFrameworkError(
                    f"Invalid expectation action '{action}'. Must be one of: "
                    f"{', '.join(a.value for a in ExpectationAction)}"
                )

        return df

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