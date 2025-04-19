"""Configuration management for the DLT Medallion Framework."""

from pathlib import Path
from typing import Any, Dict, Optional, Type, Union

import yaml

from .config_models import (
    BaseLayerConfig,
    BronzeConfig,
    SilverConfig,
    GoldConfig,
    Expectation,
    Metric,
    MonitoringConfig,
    GovernanceConfig,
    SCDConfig,
)


class ConfigurationManager:
    """Manages configuration loading and resolution for the framework."""

    LAYER_CONFIGS = {
        "bronze": BronzeConfig,
        "silver": SilverConfig,
        "gold": GoldConfig,
    }

    @classmethod
    def load_yaml_config(cls, path: Union[str, Path]) -> Dict[str, Any]:
        """Load configuration from a YAML file.
        
        Args:
            path: Path to the YAML configuration file
            
        Returns:
            Dictionary containing the configuration
            
        Raises:
            FileNotFoundError: If the configuration file doesn't exist
            yaml.YAMLError: If the YAML file is invalid
        """
        path = Path(path)
        if not path.exists():
            raise FileNotFoundError(f"Configuration file not found: {path}")

        with path.open("r") as f:
            try:
                return yaml.safe_load(f)
            except yaml.YAMLError as e:
                raise yaml.YAMLError(f"Invalid YAML configuration in {path}: {e}")

    @classmethod
    def get_config_class(cls, layer: str) -> Type[BaseLayerConfig]:
        """Get the configuration class for a layer.
        
        Args:
            layer: Layer name (bronze, silver, or gold)
            
        Returns:
            Configuration class for the layer
            
        Raises:
            ValueError: If the layer is invalid
        """
        config_class = cls.LAYER_CONFIGS.get(layer.lower())
        if not config_class:
            raise ValueError(
                f"Invalid layer: {layer}. Must be one of: "
                f"{', '.join(cls.LAYER_CONFIGS.keys())}"
            )
        return config_class

    @classmethod
    def resolve_config(
        cls,
        layer: str,
        config_path: Optional[Union[str, Path]] = None,
        config_obj: Optional[BaseLayerConfig] = None,
        **kwargs: Any,
    ) -> BaseLayerConfig:
        """Resolve configuration from multiple sources.
        
        Resolution order (highest to lowest priority):
        1. Direct parameters (kwargs)
        2. Configuration object
        3. YAML configuration file
        4. Default values
        
        Args:
            layer: Layer name (bronze, silver, or gold)
            config_path: Optional path to YAML configuration file
            config_obj: Optional configuration object
            **kwargs: Additional configuration parameters
            
        Returns:
            Resolved configuration object
            
        Raises:
            ValueError: If the layer is invalid or configuration is invalid
        """
        config_class = cls.get_config_class(layer)
        config_dict: Dict[str, Any] = {}

        # Load from YAML file if provided
        if config_path:
            yaml_config = cls.load_yaml_config(config_path)
            # Get layer-specific config if present
            layer_config = yaml_config.get(layer, {})
            config_dict.update(layer_config)

            # Add cross-cutting concerns if present
            for key in ["monitoring", "governance"]:
                if key in yaml_config:
                    config_dict[key] = yaml_config[key]

        # Update with configuration object if provided
        if config_obj:
            if not isinstance(config_obj, config_class):
                raise ValueError(
                    f"Invalid configuration object type. Expected {config_class.__name__}, "
                    f"got {type(config_obj).__name__}"
                )
            config_dict.update(config_obj.__dict__)

        # Update with direct parameters
        config_dict.update(kwargs)

        # Create and validate the configuration object
        try:
            return config_class(**config_dict)
        except (ValueError, TypeError) as e:
            raise ValueError(f"Invalid {layer} layer configuration: {e}")

    @staticmethod
    def create_expectations(*constraints: str) -> list[Expectation]:
        """Create NOT NULL expectations for a list of columns.
        
        Args:
            *constraints: Column names or custom constraints
            
        Returns:
            List of Expectation objects
            
        Example:
            >>> create_expectations("id IS NOT NULL", "email LIKE '%@%'")
            [Expectation(name="check_1", constraint="id IS NOT NULL"),
             Expectation(name="check_2", constraint="email LIKE '%@%'")]
        """
        return [
            Expectation(
                name=f"check_{i}",
                constraint=constraint
            )
            for i, constraint in enumerate(constraints, 1)
        ]

    @staticmethod
    def required_columns(*columns: str) -> list[Expectation]:
        """Create NOT NULL expectations for a list of columns.
        
        Args:
            *columns: Column names
            
        Returns:
            List of Expectation objects
            
        Example:
            >>> required_columns("id", "email", "name")
            [Expectation(name="valid_id", constraint="id IS NOT NULL"),
             Expectation(name="valid_email", constraint="email IS NOT NULL"),
             Expectation(name="valid_name", constraint="name IS NOT NULL")]
        """
        return [
            Expectation(
                name=f"valid_{col}",
                constraint=f"{col} IS NOT NULL"
            )
            for col in columns
        ]

    @staticmethod
    def reference_check(mapping: Dict[str, str]) -> list[Expectation]:
        """Create referential integrity expectations.
        
        Args:
            mapping: Dictionary mapping columns to referenced table.column
            
        Returns:
            List of Expectation objects
            
        Example:
            >>> reference_check({"customer_id": "dim_customers.id"})
            [Expectation(
                name="ref_customer_id",
                constraint="customer_id IN (SELECT id FROM dim_customers)"
            )]
        """
        return [
            Expectation(
                name=f"ref_{col}",
                constraint=f"{col} IN (SELECT {ref.split('.')[-1]} FROM {ref.rsplit('.', 1)[0]})"
            )
            for col, ref in mapping.items()
        ]

    @staticmethod
    def monitor(
        metrics: Optional[list[str]] = None,
        alerts: Optional[list[str]] = None,
        dashboard: Optional[str] = None
    ) -> MonitoringConfig:
        """Create a monitoring configuration with sensible defaults.
        
        Args:
            metrics: List of metric names
            alerts: List of alert names
            dashboard: Optional dashboard name
            
        Returns:
            MonitoringConfig object
        """
        return MonitoringConfig(
            metrics=metrics or [],
            alerts=alerts or [],
            dashboard=dashboard
        ) 