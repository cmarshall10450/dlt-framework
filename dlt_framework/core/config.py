"""Configuration management for the DLT Medallion Framework."""

import json
import os
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Union

import yaml
from pydantic import BaseModel, Field, validator

from .exceptions import ConfigurationError


class Layer(str, Enum):
    """Valid medallion layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class Expectation(BaseModel):
    """Data quality expectation configuration."""
    name: str = Field(..., description="Name of the expectation")
    constraint: str = Field(..., description="SQL constraint expression")
    description: Optional[str] = Field(None, description="Description of what this expectation validates")
    severity: str = Field("error", description="Severity level of expectation failure")

    @validator("severity")
    def validate_severity(cls, v):
        """Validate severity level."""
        valid_severities = ["error", "warning", "info"]
        if v.lower() not in valid_severities:
            raise ValueError(f"Severity must be one of {valid_severities}")
        return v.lower()


class Metric(BaseModel):
    """Data quality metric configuration."""
    name: str = Field(..., description="Name of the metric")
    value: str = Field(..., description="SQL expression for computing the metric")
    description: Optional[str] = Field(None, description="Description of what this metric measures")


class UnityTableConfig(BaseModel):
    """Unity Catalog table configuration used by all layers."""
    name: str = Field(..., description="Name of the table")
    catalog: str = Field(..., description="Unity Catalog name")
    schema: str = Field(..., description="Schema name")
    description: Optional[str] = Field(None, description="Description of the table's purpose")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Delta table properties")
    expectations: List[Expectation] = Field(default_factory=list, description="Data quality expectations")
    metrics: List[Metric] = Field(default_factory=list, description="Data quality metrics")
    tags: Dict[str, str] = Field(default_factory=dict, description="Unity Catalog tags")
    column_comments: Dict[str, str] = Field(default_factory=dict, description="Column-level comments")


class AutoLoaderSource(BaseModel):
    """Auto Loader source configuration for bronze layer."""
    type: Literal["auto_loader"] = Field("auto_loader", description="Source type identifier")
    path: str = Field(..., description="Path to source data")
    format: str = Field("cloudFiles", description="Source format (default: cloudFiles)")
    options: Dict[str, Any] = Field(default_factory=dict, description="Additional Auto Loader options")


class JDBCSource(BaseModel):
    """JDBC source configuration for bronze layer."""
    type: Literal["jdbc"] = Field("jdbc", description="Source type identifier")
    url: str = Field(..., description="JDBC connection URL")
    table: str = Field(..., description="Source table name")
    options: Dict[str, Any] = Field(default_factory=dict, description="Additional JDBC options")


class TableConfig(BaseModel):
    """Base table configuration for all layers."""
    table: UnityTableConfig = Field(..., description="Unity Catalog table configuration")
    dependencies: List[str] = Field(default_factory=list, description="List of dependent table names")
    version: str = Field("1.0", description="Configuration schema version")


class BronzeConfig(TableConfig):
    """Bronze layer configuration with source configuration."""
    source: Union[AutoLoaderSource, JDBCSource] = Field(..., description="Source configuration")


class ConfigurationManager:
    """Manages configuration loading and validation for the framework."""

    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """
        Initialize the configuration manager.

        Args:
            config_path: Optional path to a YAML/JSON configuration file
        """
        self._config: Optional[Union[TableConfig, BronzeConfig]] = None
        if config_path:
            self.load_config(config_path)

    def load_config(self, path: Union[str, Path], layer: Optional[Layer] = None) -> Union[TableConfig, BronzeConfig]:
        """
        Load configuration from a YAML or JSON file.

        Args:
            path: Path to the configuration file (must end in .yaml, .yml, or .json)
            layer: Optional layer to validate against. If bronze, uses BronzeConfig

        Returns:
            Config object containing the loaded configuration

        Raises:
            ConfigurationError: If the file cannot be loaded or is invalid
        """
        try:
            path = Path(path)
            with open(path, "r") as f:
                if path.suffix.lower() in [".yaml", ".yml"]:
                    config_dict = yaml.safe_load(f)
                elif path.suffix.lower() == ".json":
                    config_dict = json.load(f)
                else:
                    raise ConfigurationError(
                        f"Unsupported file format: {path.suffix}. Use .yaml, .yml, or .json"
                    )
            
            # Use BronzeConfig for bronze layer, TableConfig for others
            config_class = BronzeConfig if layer == Layer.BRONZE else TableConfig
            self._config = config_class(**config_dict)
            return self._config
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration from {path}: {str(e)}")

    def get_config(self) -> Union[TableConfig, BronzeConfig]:
        """
        Get the current configuration.

        Returns:
            Config object containing the current configuration

        Raises:
            ConfigurationError: If no configuration has been loaded
        """
        if self._config is None:
            raise ConfigurationError("No configuration has been loaded")
        return self._config

    def merge_configs(self, base: Union[TableConfig, BronzeConfig], override: Dict[str, Any]) -> Union[TableConfig, BronzeConfig]:
        """
        Merge a base configuration with override values.

        Args:
            base: Base configuration object
            override: Override configuration dictionary

        Returns:
            Config object containing the merged configuration
        """
        # Convert base config to dict for merging
        base_dict = base.dict()
        
        # Merge dictionaries
        merged = base_dict.copy()
        for key, value in override.items():
            if (
                key in merged
                and isinstance(merged[key], dict)
                and isinstance(value, dict)
            ):
                merged[key].update(value)
            else:
                merged[key] = value
        
        # Create new config from merged dict
        config_class = type(base)  # Preserve the original config type
        return config_class(**merged)

    def update_config(self, updates: Dict[str, Any]) -> None:
        """
        Update the current configuration with new values.

        Args:
            updates: Dictionary containing configuration updates

        Raises:
            ConfigurationError: If no configuration has been loaded
        """
        if self._config is None:
            raise ConfigurationError("No configuration has been loaded")
        self._config = self.merge_configs(self._config, updates)

    def save_config(self, path: Union[str, Path]) -> None:
        """
        Save the current configuration to a file.

        Args:
            path: Path to save the configuration (must end in .yaml, .yml, or .json)

        Raises:
            ConfigurationError: If no configuration has been loaded or the file format is unsupported
        """
        if self._config is None:
            raise ConfigurationError("No configuration has been loaded")

        try:
            path = Path(path)
            config_dict = self._config.dict(exclude_none=True)
            
            with open(path, "w") as f:
                if path.suffix.lower() in [".yaml", ".yml"]:
                    yaml.dump(config_dict, f, sort_keys=False)
                elif path.suffix.lower() == ".json":
                    json.dump(config_dict, f, indent=2)
                else:
                    raise ConfigurationError(
                        f"Unsupported file format: {path.suffix}. Use .yaml, .yml, or .json"
                    )
        except Exception as e:
            raise ConfigurationError(f"Failed to save configuration to {path}: {str(e)}") 