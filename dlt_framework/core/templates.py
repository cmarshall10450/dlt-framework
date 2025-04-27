"""Configuration template management for the DLT Medallion Framework."""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, root_validator

from ..config.models import (
    Layer, Expectation, Metric, DLTTableConfig as TableConfig,
    BronzeConfig, SilverConfig, GoldConfig
)
from .exceptions import TemplateError


class TemplateRef(BaseModel):
    """Reference to a template with optional overrides."""
    name: str = Field(..., description="Name of the template to use")
    overrides: Dict[str, Any] = Field(default_factory=dict, description="Values to override from template")


class Template(BaseModel):
    """Configuration template model."""
    name: str = Field(..., description="Template name")
    description: Optional[str] = Field(None, description="Template description")
    extends: Optional[str] = Field(None, description="Name of template to extend")
    table: Optional[TableConfig] = Field(None, description="Table configuration")
    expectations: List[Expectation] = Field(default_factory=list, description="Default expectations")
    metrics: List[Metric] = Field(default_factory=list, description="Default metrics")
    properties: Dict[str, Any] = Field(default_factory=dict, description="Default table properties")

    @root_validator
    def validate_template(cls, values):
        """Ensure template has either table config or individual components."""
        if values.get("table") is None and not any([
            values.get("expectations"),
            values.get("metrics"),
            values.get("properties")
        ]):
            raise ValueError("Template must define either table config or individual components")
        return values


class TemplateManager:
    """Manages configuration templates."""

    def __init__(self, template_path: Optional[Union[str, Path]] = None):
        """
        Initialize the template manager.

        Args:
            template_path: Optional path to template directory or file
        """
        self._templates: Dict[str, Template] = {}
        if template_path:
            self.load_templates(template_path)

    def load_templates(self, path: Union[str, Path]) -> None:
        """
        Load templates from a file or directory.

        Args:
            path: Path to template file or directory

        Raises:
            TemplateError: If templates cannot be loaded
        """
        path = Path(path)
        try:
            if path.is_dir():
                for file in path.glob("*.yaml"):
                    self._load_template_file(file)
            else:
                self._load_template_file(path)
        except Exception as e:
            raise TemplateError(f"Failed to load templates from {path}: {str(e)}")

    def _load_template_file(self, path: Path) -> None:
        """Load templates from a single file."""
        try:
            with open(path, "r") as f:
                content = yaml.safe_load(f)
                
            if isinstance(content, dict):
                # Single template
                template = Template(**content)
                self._templates[template.name] = template
            elif isinstance(content, list):
                # Multiple templates
                for item in content:
                    template = Template(**item)
                    self._templates[template.name] = template
            else:
                raise TemplateError(f"Invalid template format in {path}")
        except Exception as e:
            raise TemplateError(f"Failed to load template from {path}: {str(e)}")

    def get_template(self, name: str) -> Template:
        """
        Get a template by name.

        Args:
            name: Template name

        Returns:
            Template object

        Raises:
            TemplateError: If template does not exist
        """
        if name not in self._templates:
            raise TemplateError(f"Template not found: {name}")
        return self._templates[name]

    def apply_template(
        self, 
        template_ref: Union[str, TemplateRef], 
        layer: Layer,
        base_config: Optional[Union[BronzeConfig, SilverConfig, GoldConfig]] = None
    ) -> Union[BronzeConfig, SilverConfig, GoldConfig]:
        """
        Apply a template to create or update a configuration.

        Args:
            template_ref: Template name or TemplateRef object
            layer: Layer type (bronze, silver, gold)
            base_config: Optional base configuration to update

        Returns:
            Config object with template applied

        Raises:
            TemplateError: If template cannot be applied
        """
        # Convert string to TemplateRef
        if isinstance(template_ref, str):
            template_ref = TemplateRef(name=template_ref)

        try:
            # Get template
            template = self.get_template(template_ref.name)
            
            # Start with base config or create new based on layer
            if base_config:
                config_dict = base_config.dict()
            else:
                config_dict = {"table": {}}
                if layer == Layer.BRONZE:
                    config_dict = BronzeConfig(**config_dict).dict()
                elif layer == Layer.SILVER:
                    config_dict = SilverConfig(**config_dict).dict()
                else:  # GOLD
                    config_dict = GoldConfig(**config_dict).dict()
            
            # Apply parent template if exists
            if template.extends:
                parent_template = self.get_template(template.extends)
                config_dict = self._apply_template_dict(parent_template, config_dict)
            
            # Apply current template
            config_dict = self._apply_template_dict(template, config_dict)
            
            # Apply overrides
            if template_ref.overrides:
                config_dict = self._merge_dicts(config_dict, template_ref.overrides)
            
            # Create appropriate config object based on layer
            if layer == Layer.BRONZE:
                return BronzeConfig(**config_dict)
            elif layer == Layer.SILVER:
                return SilverConfig(**config_dict)
            else:  # GOLD
                return GoldConfig(**config_dict)

        except Exception as e:
            raise TemplateError(f"Failed to apply template {template_ref.name}: {str(e)}")

    def _apply_template_dict(self, template: Template, config_dict: Dict[str, Any]) -> Dict[str, Any]:
        """Apply template to configuration dictionary."""
        if template.table:
            # If template has full table config, use it as base
            if "table" not in config_dict:
                config_dict["table"] = {}
            config_dict["table"] = self._merge_dicts(
                template.table.dict(exclude_none=True),
                config_dict["table"]
            )
        else:
            # Apply individual components
            if template.expectations:
                if "expectations" not in config_dict["table"]:
                    config_dict["table"]["expectations"] = []
                config_dict["table"]["expectations"].extend(
                    exp.dict() for exp in template.expectations
                )
            
            if template.metrics:
                if "metrics" not in config_dict["table"]:
                    config_dict["table"]["metrics"] = []
                config_dict["table"]["metrics"].extend(
                    metric.dict() for metric in template.metrics
                )
            
            if template.properties:
                if "properties" not in config_dict["table"]:
                    config_dict["table"]["properties"] = {}
                config_dict["table"]["properties"].update(template.properties)
        
        return config_dict

    def _merge_dicts(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries."""
        result = base.copy()
        for key, value in override.items():
            if (
                key in result and 
                isinstance(result[key], dict) and 
                isinstance(value, dict)
            ):
                result[key] = self._merge_dicts(result[key], value)
            else:
                result[key] = value
        return result 