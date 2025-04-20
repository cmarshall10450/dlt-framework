"""Configuration management for the DLT Medallion Framework.

This module provides a unified way to load, validate, and manage configuration
for the DLT Medallion Framework.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Type, TypeVar, Union

import yaml

from .models import (
    BaseLayerConfig, 
    BronzeConfig, 
    Expectation,
    GoldConfig, 
    Layer, 
    MonitoringConfig,
    SilverConfig
)
from .utils import load_yaml_file, merge_configs, validate_file_path, convert_keys_case

# Type variable for generic configuration types
T = TypeVar('T', bound=BaseLayerConfig)


class ConfigurationManager:
    """Manages configuration loading and resolution for the framework."""

    LAYER_CONFIGS = {
        Layer.BRONZE: BronzeConfig,
        Layer.SILVER: SilverConfig,
        Layer.GOLD: GoldConfig,
    }

    def __init__(self, config_path: Optional[Union[str, Path]] = None):
        """Initialize the configuration manager.
        
        Args:
            config_path: Optional path to a configuration file
        """
        self._config: Optional[BaseLayerConfig] = None
        self._config_path: Optional[Path] = None
        
        if config_path:
            self.load_config(config_path)

    @classmethod
    def get_config_class(cls, layer: Union[str, Layer]) -> Type[BaseLayerConfig]:
        """Get the configuration class for a layer.
        
        Args:
            layer: Layer name or enum
            
        Returns:
            Configuration class for the layer
            
        Raises:
            ValueError: If the layer is invalid
        """
        if isinstance(layer, str):
            try:
                layer_enum = Layer(layer.lower())
            except ValueError:
                raise ValueError(
                    f"Invalid layer: {layer}. Must be one of: {', '.join(l.value for l in Layer)}"
                )
        else:
            layer_enum = layer
            
        config_class = cls.LAYER_CONFIGS.get(layer_enum)
        if not config_class:
            raise ValueError(
                f"Invalid layer: {layer_enum}. Must be one of: {', '.join(l.value for l in Layer)}"
            )
        return config_class

    def load_config(
        self,
        path: Union[str, Path],
        layer: Optional[Union[str, Layer]] = None,
        environment: Optional[str] = None,
    ) -> BaseLayerConfig:
        """Load configuration from a file.
        
        Args:
            path: Path to the configuration file
            layer: Optional layer to validate against
            environment: Optional environment name (default: from DLT_ENV)
            
        Returns:
            Loaded configuration object
            
        Raises:
            ValueError: If configuration loading fails
        """
        # Validate file path and format
        file_path = validate_file_path(path)
        self._config_path = file_path
        
        # Load base configuration
        config_dict = load_yaml_file(file_path)
        
        # Load environment-specific config if it exists
        env = environment or os.getenv("DLT_ENV", "development")
        env_config = config_dict.get("environments", {}).get(env, {})
        if env_config:
            config_dict = merge_configs(config_dict, env_config)
            
        # Remove environments section to avoid pydantic validation errors
        if "environments" in config_dict:
            del config_dict["environments"]
            
        # Determine the configuration class to use
        config_class: Type[BaseLayerConfig] = BaseLayerConfig
        
        if layer:
            # If layer is specified, get the corresponding config class
            config_class = self.get_config_class(layer)
            
            # Extract layer-specific configuration if present
            layer_value = layer.value if isinstance(layer, Layer) else layer.lower()
            if layer_value in config_dict:
                config_dict = config_dict[layer_value]
        else:
            # Try to determine layer from configuration
            for layer_enum, layer_class in self.LAYER_CONFIGS.items():
                if layer_enum.value in config_dict:
                    config_class = layer_class
                    config_dict = config_dict[layer_enum.value]
                    break
                    
        # Create configuration object
        try:
            self._config = config_class.parse_obj(config_dict)
            return self._config
        except Exception as e:
            raise ValueError(f"Failed to load configuration: {str(e)}")

    def get_config(self) -> BaseLayerConfig:
        """Get the current configuration.
        
        Returns:
            Current configuration object
            
        Raises:
            ValueError: If no configuration has been loaded
        """
        if self._config is None:
            raise ValueError("No configuration has been loaded")
        return self._config

    def update_config(self, updates: Dict[str, Any]) -> BaseLayerConfig:
        """Update the current configuration with new values.
        
        Args:
            updates: Dictionary containing configuration updates
            
        Returns:
            Updated configuration object
            
        Raises:
            ValueError: If no configuration has been loaded
        """
        if self._config is None:
            raise ValueError("No configuration has been loaded")
        
        # Create a dict representation of the current config
        current_dict = self._config.dict()
        
        # Merge the updates with current config
        merged_dict = merge_configs(current_dict, updates)
        
        # Create new config instance with merged values
        config_class = type(self._config)
        self._config = config_class.parse_obj(merged_dict)
        return self._config

    def save_config(self, path: Optional[Union[str, Path]] = None, use_camel_case: bool = True) -> None:
        """Save the current configuration to a file.
        
        Args:
            path: Path to save the configuration (defaults to original path)
            use_camel_case: Whether to use camelCase keys in the output file
            
        Raises:
            ValueError: If no configuration has been loaded or save fails
        """
        if self._config is None:
            raise ValueError("No configuration has been loaded")

        save_path = path if path is not None else self._config_path
        if save_path is None:
            raise ValueError("No path specified and no original path available")
            
        # Validate file path and format
        file_path = validate_file_path(save_path)
        
        # Convert config to dict excluding None values
        config_dict = self._config.dict(exclude_none=True)
        
        # Convert to camelCase if requested
        if use_camel_case:
            config_dict = convert_keys_case(config_dict, to_camel=True)
        
        try:
            with open(file_path, "w") as f:
                if file_path.suffix.lower() in [".yaml", ".yml"]:
                    yaml.dump(config_dict, f, sort_keys=False, default_flow_style=False)
                else:  # .json
                    import json
                    json.dump(config_dict, f, indent=2)
        except Exception as e:
            raise ValueError(f"Failed to save configuration: {str(e)}")

    @staticmethod
    def create_expectations(*constraints: str) -> List[Expectation]:
        """Create expectations from constraints.
        
        Args:
            *constraints: SQL constraint expressions
            
        Returns:
            List of Expectation objects
        """
        return [
            Expectation(
                name=f"check_{i}",
                constraint=constraint
            )
            for i, constraint in enumerate(constraints, 1)
        ]

    @staticmethod
    def required_columns(*columns: str) -> List[Expectation]:
        """Create NOT NULL expectations for columns.
        
        Args:
            *columns: Column names
            
        Returns:
            List of Expectation objects
        """
        return [
            Expectation(
                name=f"valid_{col}",
                constraint=f"{col} IS NOT NULL"
            )
            for col in columns
        ]

    @staticmethod
    def reference_check(mapping: Dict[str, str]) -> List[Expectation]:
        """Create referential integrity expectations.
        
        Args:
            mapping: Dictionary mapping columns to referenced table.column
            
        Returns:
            List of Expectation objects
        """
        return [
            Expectation(
                name=f"ref_{col}",
                constraint=f"{col} IN (SELECT {ref.split('.')[-1]} FROM {ref.rsplit('.', 1)[0]})"
            )
            for col, ref in mapping.items()
        ]

    @classmethod
    def from_env_var(cls, env_var: str = "DLT_CONFIG_PATH") -> 'ConfigurationManager':
        """Create a configuration manager from an environment variable.
        
        Args:
            env_var: Environment variable containing the config path
            
        Returns:
            ConfigurationManager instance
            
        Raises:
            ValueError: If the environment variable is not set
        """
        config_path = os.getenv(env_var)
        if not config_path:
            raise ValueError(f"Environment variable {env_var} not set")
        return cls(config_path)

    @classmethod
    def from_layer(cls, layer: Union[str, Layer], config_path: Optional[Union[str, Path]] = None) -> 'ConfigurationManager':
        """Create a configuration manager for a specific layer.
        
        Args:
            layer: Layer name or enum
            config_path: Optional path to a configuration file
            
        Returns:
            ConfigurationManager instance
        """
        manager = cls(config_path)
        if config_path:
            manager.load_config(config_path, layer=layer)
        return manager

    @classmethod
    def resolve_config(
        cls,
        layer: Union[str, Layer],
        config_path: Optional[Union[str, Path]] = None,
        config_obj: Optional[BaseLayerConfig] = None,
        **kwargs: Any
    ) -> BaseLayerConfig:
        """Resolve configuration from either a path or object with optional overrides.
        
        This method handles multiple configuration sources in the following order:
        1. If config_obj is provided, use it as the base
        2. If config_path is provided, load and merge with base
        3. Apply any additional kwargs as overrides
        
        Args:
            layer: Layer name or enum
            config_path: Optional path to configuration file
            config_obj: Optional configuration object
            **kwargs: Additional configuration overrides
            
        Returns:
            Resolved configuration object
            
        Raises:
            ValueError: If neither config_path nor config_obj is provided
        """
        # Get the appropriate config class for the layer
        config_class = cls.get_config_class(layer)
        
        # Start with empty config if nothing provided
        if not config_obj and not config_path:
            config_obj = config_class()
            
        # If config object provided, validate it's the correct type
        if config_obj:
            if not isinstance(config_obj, config_class):
                raise ValueError(
                    f"Configuration object must be an instance of {config_class.__name__}, "
                    f"got {type(config_obj).__name__}"
                )
            base_config = config_obj
        else:
            base_config = config_class()
            
        # If config path provided, load and merge
        if config_path:
            manager = cls(config_path)
            file_config = manager.load_config(config_path, layer=layer)
            
            # Merge with base config
            base_dict = base_config.dict()
            file_dict = file_config.dict()
            merged_dict = merge_configs(base_dict, file_dict)
            base_config = config_class.parse_obj(merged_dict)
            
        # Apply any additional kwargs as overrides
        if kwargs:
            base_dict = base_config.dict()
            merged_dict = merge_configs(base_dict, kwargs)
            base_config = config_class.parse_obj(merged_dict)
            
        return base_config
