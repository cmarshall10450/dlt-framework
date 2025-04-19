"""Delta Live Tables integration module for the DLT Medallion Framework."""

from typing import Any, Dict, List, Optional, Union

import dlt
from pyspark.sql import DataFrame

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
    def add_unity_catalog_metadata(
        df: DataFrame,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        table_name: Optional[str] = None,
        column_comments: Optional[Dict[str, str]] = None,
        tags: Optional[Dict[str, str]] = None
    ) -> DataFrame:
        """
        Add Unity Catalog metadata to a DataFrame.

        Args:
            df: The input DataFrame
            catalog: Optional catalog name
            schema: Optional schema name
            table_name: Optional table name
            column_comments: Optional dictionary of column comments
            tags: Optional dictionary of Unity Catalog tags

        Returns:
            DataFrame with Unity Catalog metadata
        """
        properties = {}
        
        if catalog and schema and table_name:
            properties["target"] = f"{catalog}.{schema}.{table_name}"

        if column_comments:
            for column, comment in column_comments.items():
                if column in df.columns:
                    properties[f"column_comment.{column}"] = comment

        if tags:
            for key, value in tags.items():
                properties[f"tag.{key}"] = value

        if properties:
            dlt.properties.update(properties)

        return df

    @staticmethod
    def add_column_tag(column_name: str, tag_name: str, tag_value: str) -> None:
        """
        Add a tag to a specific column.

        Args:
            column_name: Name of the column
            tag_name: Name of the tag
            tag_value: Value of the tag
        """
        dlt.properties[f"column_tag.{column_name}.{tag_name}"] = tag_value

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

        dlt.properties.update({key: str(value) for key, value in properties.items()})

    @staticmethod
    def set_table_comment(comment: Optional[str] = None) -> None:
        """
        Set the table comment.

        Args:
            comment: Table comment string
        """
        if comment:
            dlt.properties["comment"] = comment 