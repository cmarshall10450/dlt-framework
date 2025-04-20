"""Utility functions for configuration management.

This module provides helper functions for loading, validating, and manipulating
configuration files and objects.
"""

import re
import os
from pathlib import Path
from typing import Any, Dict, Union

import yaml


def to_camel_case(snake_str: str) -> str:
    """Convert snake_case to camelCase.
    
    Args:
        snake_str: String in snake_case format
        
    Returns:
        String in camelCase format
    
    Example:
        >>> to_camel_case('this_is_snake_case')
        'thisIsSnakeCase'
    """
    components = snake_str.split('_')
    return components[0] + ''.join(x.title() for x in components[1:])


def to_snake_case(camel_str: str) -> str:
    """Convert camelCase to snake_case.
    
    Args:
        camel_str: String in camelCase format
        
    Returns:
        String in snake_case format
        
    Example:
        >>> to_snake_case('thisIsCamelCase')
        'this_is_camel_case'
    """
    # Add underscore before capital letters and convert to lowercase
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', camel_str)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def convert_keys_case(d: Dict[str, Any], to_camel: bool = False) -> Dict[str, Any]:
    """Convert all dictionary keys between snake_case and camelCase.
    
    Args:
        d: Dictionary whose keys need to be converted
        to_camel: If True, convert from snake_case to camelCase;
                 if False, convert from camelCase to snake_case
        
    Returns:
        Dictionary with converted keys
    """
    converter = to_camel_case if to_camel else to_snake_case
    result = {}
    
    for key, value in d.items():
        # Convert the current key
        new_key = converter(key)
        
        # Recursively convert nested dictionaries
        if isinstance(value, dict):
            value = convert_keys_case(value, to_camel)
        # Handle lists that might contain dictionaries
        elif isinstance(value, list):
            value = [
                convert_keys_case(item, to_camel) if isinstance(item, dict) else item 
                for item in value
            ]
            
        result[new_key] = value
        
    return result


def validate_file_path(path: Union[str, Path]) -> Path:
    """Validate that the file path exists and has a supported format.
    
    Args:
        path: Path to the configuration file
        
    Returns:
        Path object for the validated file path
        
    Raises:
        ValueError: If the file doesn't exist or has an unsupported format
    """
    file_path = Path(path)
    
    # Check if the path exists or can be created
    if not file_path.exists() and not file_path.parent.exists():
        raise ValueError(f"Path does not exist: {file_path}")
        
    # Validate file format
    if file_path.suffix.lower() not in [".yaml", ".yml", ".json"]:
        raise ValueError(
            f"Unsupported file format: {file_path.suffix}. Use .yaml, .yml, or .json"
        )
        
    return file_path


def load_yaml_file(path: Union[str, Path]) -> Dict[str, Any]:
    """Load and parse a YAML or JSON file.
    
    Args:
        path: Path to the configuration file
        
    Returns:
        Dictionary containing the configuration
        
    Raises:
        ValueError: If the file doesn't exist or contains invalid content
    """
    file_path = Path(path)
    
    # Check if the file exists
    if not file_path.exists():
        raise ValueError(f"Configuration file not found: {file_path}")

    try:
        with open(file_path, "r") as f:
            if file_path.suffix.lower() in [".yaml", ".yml"]:
                config_dict = yaml.safe_load(f) or {}
            elif file_path.suffix.lower() == ".json":
                import json
                config_dict = json.load(f)
                
        # Convert camelCase keys to snake_case for internal use
        return convert_keys_case(config_dict, to_camel=False)
    except Exception as e:
        raise ValueError(f"Failed to load configuration file {file_path}: {str(e)}")


def merge_configs(base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
    """Deep merge two configuration dictionaries.
    
    Args:
        base: Base configuration dictionary
        override: Override configuration dictionary
        
    Returns:
        Merged configuration dictionary
        
    Example:
        >>> base = {"a": 1, "b": {"c": 2}}
        >>> override = {"b": {"d": 3}}
        >>> merge_configs(base, override)
        {"a": 1, "b": {"c": 2, "d": 3}}
    """
    merged = base.copy()
    
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = merge_configs(merged[key], value)
        else:
            merged[key] = value
            
    return merged


def get_environment() -> str:
    """Get the current environment name from environment variables.
    
    Returns:
        Environment name (default: "development")
    """
    return os.getenv("DLT_ENV", "development")


def get_config_path() -> Path:
    """Get the configuration path from environment variable.
    
    Returns:
        Path to the configuration file
        
    Raises:
        ValueError: If the environment variable is not set
    """
    config_path = os.getenv("DLT_CONFIG_PATH")
    if not config_path:
        raise ValueError("DLT_CONFIG_PATH environment variable not set")
        
    return Path(config_path)


def get_layers_from_config(config_dict: Dict[str, Any]) -> list:
    """Extract layer names from a configuration dictionary.
    
    Args:
        config_dict: Configuration dictionary
        
    Returns:
        List of layer names found in the configuration
    """
    from .models import Layer
    
    return [
        layer.value for layer in Layer
        if layer.value in config_dict
    ]
