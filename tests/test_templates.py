"""Test configuration template functionality."""

import pytest
import yaml
from pydantic import ValidationError

from dlt_framework.core.config import Config, Expectation, Layer, Metric
from dlt_framework.core.exceptions import TemplateError
from dlt_framework.core.templates import Template, TemplateManager, TemplateRef


@pytest.fixture
def base_template_dict():
    """Create a base template dictionary."""
    return {
        "name": "base_template",
        "description": "Base template for testing",
        "table": {
            "name": "test_table",
            "layer": "bronze",
            "properties": {
                "delta.autoOptimize.optimizeWrite": "true"
            },
            "expectations": [
                {
                    "name": "valid_id",
                    "constraint": "id IS NOT NULL",
                    "severity": "error"
                }
            ],
            "metrics": [
                {
                    "name": "row_count",
                    "value": "COUNT(*)"
                }
            ]
        }
    }


@pytest.fixture
def template_file(tmp_path, base_template_dict):
    """Create a temporary template file."""
    template_path = tmp_path / "templates.yaml"
    with open(template_path, "w") as f:
        yaml.dump([base_template_dict], f)
    return template_path


def test_template_model():
    """Test Template model validation."""
    # Valid template with table config
    template = Template(
        name="test",
        table={
            "name": "test_table",
            "layer": "bronze"
        }
    )
    assert template.name == "test"
    assert template.table.name == "test_table"
    
    # Valid template with individual components
    template = Template(
        name="test",
        expectations=[
            Expectation(name="test", constraint="col IS NOT NULL")
        ]
    )
    assert len(template.expectations) == 1
    
    # Invalid template (no components)
    with pytest.raises(ValidationError):
        Template(name="test")


def test_template_inheritance(tmp_path):
    """Test template inheritance functionality."""
    # Create parent template
    parent = {
        "name": "parent",
        "table": {
            "name": "parent_table",
            "layer": "bronze",
            "properties": {"key1": "value1"}
        }
    }
    
    # Create child template
    child = {
        "name": "child",
        "extends": "parent",
        "table": {
            "name": "child_table",
            "properties": {"key2": "value2"}
        }
    }
    
    # Save templates
    template_path = tmp_path / "templates.yaml"
    with open(template_path, "w") as f:
        yaml.dump([parent, child], f)
    
    # Load and apply templates
    manager = TemplateManager(template_path)
    config = manager.apply_template("child")
    
    # Verify inheritance
    assert config.table.name == "child_table"  # Overridden
    assert config.table.layer == "bronze"  # Inherited
    assert config.table.properties == {
        "key1": "value1",  # From parent
        "key2": "value2"   # From child
    }


def test_template_overrides(template_file, base_template_dict):
    """Test template override functionality."""
    manager = TemplateManager(template_file)
    
    # Apply template with overrides
    config = manager.apply_template(TemplateRef(
        name="base_template",
        overrides={
            "table": {
                "name": "override_table",
                "expectations": [
                    {
                        "name": "new_check",
                        "constraint": "value > 0",
                        "severity": "warning"
                    }
                ]
            }
        }
    ))
    
    # Verify overrides
    assert config.table.name == "override_table"
    assert len(config.table.expectations) == 2  # Original + new
    assert config.table.expectations[1].name == "new_check"


def test_template_manager_operations(template_file):
    """Test TemplateManager operations."""
    manager = TemplateManager()
    
    # Test loading templates
    manager.load_templates(template_file)
    template = manager.get_template("base_template")
    assert template.name == "base_template"
    
    # Test non-existent template
    with pytest.raises(TemplateError):
        manager.get_template("non_existent")
    
    # Test invalid template file
    with pytest.raises(TemplateError):
        manager.load_templates("non_existent.yaml")


def test_template_merging(template_file):
    """Test merging templates with existing config."""
    manager = TemplateManager(template_file)
    
    # Create base config
    base_config = Config(
        table={
            "name": "existing_table",
            "layer": "silver",
            "metrics": [
                {
                    "name": "existing_metric",
                    "value": "SUM(value)"
                }
            ]
        }
    )
    
    # Apply template
    config = manager.apply_template("base_template", base_config)
    
    # Verify merge
    assert config.table.name == "existing_table"  # Kept from base
    assert config.table.layer == "silver"  # Kept from base
    assert len(config.table.metrics) == 2  # Combined metrics
    assert config.table.properties["delta.autoOptimize.optimizeWrite"] == "true"  # From template


def test_multiple_templates_in_file(tmp_path):
    """Test loading multiple templates from a single file."""
    templates = [
        {
            "name": "template1",
            "table": {
                "name": "table1",
                "layer": "bronze"
            }
        },
        {
            "name": "template2",
            "table": {
                "name": "table2",
                "layer": "silver"
            }
        }
    ]
    
    # Save templates
    template_path = tmp_path / "templates.yaml"
    with open(template_path, "w") as f:
        yaml.dump(templates, f)
    
    # Load templates
    manager = TemplateManager(template_path)
    
    # Verify both templates loaded
    template1 = manager.get_template("template1")
    template2 = manager.get_template("template2")
    assert template1.table.name == "table1"
    assert template2.table.name == "table2"


def test_template_directory_loading(tmp_path):
    """Test loading templates from a directory."""
    template_dir = tmp_path / "templates"
    template_dir.mkdir()
    
    # Create multiple template files
    templates1 = {
        "name": "template1",
        "table": {"name": "table1", "layer": "bronze"}
    }
    templates2 = {
        "name": "template2",
        "table": {"name": "table2", "layer": "silver"}
    }
    
    with open(template_dir / "template1.yaml", "w") as f:
        yaml.dump(templates1, f)
    with open(template_dir / "template2.yaml", "w") as f:
        yaml.dump(templates2, f)
    
    # Load templates from directory
    manager = TemplateManager(template_dir)
    
    # Verify all templates loaded
    assert "template1" in manager._templates
    assert "template2" in manager._templates 