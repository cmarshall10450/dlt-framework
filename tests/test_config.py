"""Test configuration management functionality."""

import json
import os
from pathlib import Path

import pytest
import yaml
from pydantic import ValidationError

from dlt_framework.core.config import (
    Config,
    ConfigurationError,
    ConfigurationManager,
    Expectation,
    Layer,
    Metric,
    TableConfig,
)


@pytest.fixture
def sample_config_dict():
    """Create a sample configuration dictionary."""
    return {
        "table": {
            "name": "test_table",
            "layer": "bronze",
            "description": "Test table description",
            "properties": {"delta.autoOptimize.optimizeWrite": "true"},
            "comment": "Test table comment",
            "expectations": [
                {
                    "name": "valid_id",
                    "constraint": "id IS NOT NULL",
                    "description": "Ensure ID is not null",
                    "severity": "error"
                }
            ],
            "metrics": [
                {
                    "name": "null_count",
                    "value": "COUNT(CASE WHEN id IS NULL THEN 1 END)",
                    "description": "Count of null IDs"
                }
            ]
        },
        "dependencies": ["source_table"],
        "version": "1.0"
    }


@pytest.fixture
def config_file(tmp_path, sample_config_dict):
    """Create a temporary configuration file."""
    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(sample_config_dict, f)
    return config_path


def test_layer_enum():
    """Test Layer enum functionality."""
    assert Layer.BRONZE == "bronze"
    assert Layer.SILVER == "silver"
    assert Layer.GOLD == "gold"
    
    with pytest.raises(ValueError):
        Layer("invalid")


def test_expectation_model():
    """Test Expectation model validation."""
    # Valid expectation
    exp = Expectation(
        name="test",
        constraint="col IS NOT NULL",
        severity="warning"
    )
    assert exp.severity == "warning"
    
    # Invalid severity
    with pytest.raises(ValidationError):
        Expectation(
            name="test",
            constraint="col IS NOT NULL",
            severity="invalid"
        )


def test_metric_model():
    """Test Metric model validation."""
    metric = Metric(
        name="test_metric",
        value="COUNT(*)",
        description="Test metric"
    )
    assert metric.name == "test_metric"
    assert metric.value == "COUNT(*)"
    assert metric.description == "Test metric"


def test_table_config_model():
    """Test TableConfig model validation."""
    table = TableConfig(
        name="test_table",
        layer=Layer.BRONZE,
        expectations=[
            Expectation(name="test", constraint="col IS NOT NULL")
        ],
        metrics=[
            Metric(name="count", value="COUNT(*)")
        ]
    )
    assert table.name == "test_table"
    assert table.layer == Layer.BRONZE
    assert len(table.expectations) == 1
    assert len(table.metrics) == 1


def test_config_model():
    """Test Config model validation."""
    config = Config(
        table=TableConfig(
            name="test_table",
            layer=Layer.BRONZE
        ),
        dependencies=["source_table"]
    )
    assert config.table.name == "test_table"
    assert config.version == "1.0"  # Default value
    assert len(config.dependencies) == 1


def test_load_yaml_config(config_file):
    """Test loading configuration from YAML file."""
    manager = ConfigurationManager(config_file)
    config = manager.get_config()
    
    assert config.table.name == "test_table"
    assert config.table.layer == Layer.BRONZE
    assert len(config.table.expectations) == 1
    assert len(config.table.metrics) == 1
    assert config.dependencies == ["source_table"]


def test_load_json_config(tmp_path, sample_config_dict):
    """Test loading configuration from JSON file."""
    config_path = tmp_path / "config.json"
    with open(config_path, "w") as f:
        json.dump(sample_config_dict, f)
    
    manager = ConfigurationManager(config_path)
    config = manager.get_config()
    
    assert config.table.name == "test_table"
    assert config.table.layer == Layer.BRONZE


def test_save_config(tmp_path, config_file):
    """Test saving configuration to file."""
    # Load existing config
    manager = ConfigurationManager(config_file)
    
    # Save as YAML
    yaml_path = tmp_path / "saved.yaml"
    manager.save_config(yaml_path)
    assert yaml_path.exists()
    
    # Save as JSON
    json_path = tmp_path / "saved.json"
    manager.save_config(json_path)
    assert json_path.exists()
    
    # Verify saved configs can be loaded
    yaml_manager = ConfigurationManager(yaml_path)
    json_manager = ConfigurationManager(json_path)
    assert yaml_manager.get_config().table.name == "test_table"
    assert json_manager.get_config().table.name == "test_table"


def test_merge_configs(sample_config_dict):
    """Test merging configurations."""
    manager = ConfigurationManager()
    base_config = Config(**sample_config_dict)
    
    updates = {
        "table": {
            "description": "Updated description",
            "expectations": [
                {
                    "name": "new_check",
                    "constraint": "value > 0",
                    "severity": "warning"
                }
            ]
        }
    }
    
    merged = manager.merge_configs(base_config, updates)
    assert merged.table.description == "Updated description"
    assert len(merged.table.expectations) == 1
    assert merged.table.expectations[0].name == "new_check"


def test_invalid_config():
    """Test invalid configuration handling."""
    # Missing required field
    with pytest.raises(ValidationError):
        Config(table={"layer": "bronze"})  # Missing name
    
    # Invalid layer
    with pytest.raises(ValidationError):
        Config(table={"name": "test", "layer": "invalid"})
    
    # Invalid file format
    with pytest.raises(ConfigurationError):
        manager = ConfigurationManager()
        manager.load_config("config.txt")


def test_update_config(config_file):
    """Test updating configuration."""
    manager = ConfigurationManager(config_file)
    
    updates = {
        "table": {
            "description": "New description",
            "properties": {"new.property": "value"}
        }
    }
    
    manager.update_config(updates)
    config = manager.get_config()
    
    assert config.table.description == "New description"
    assert config.table.properties["new.property"] == "value"
    # Original values should be preserved
    assert config.table.name == "test_table"
    assert len(config.table.expectations) == 1 