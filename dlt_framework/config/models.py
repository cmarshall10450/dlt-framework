"""
This module contains all the Pydantic models used for configuration in the framework.
"""

from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union
from pydantic import BaseModel, Field, validator, root_validator, Extra
import re
from datetime import datetime


class ConfigBaseModel(BaseModel):
    """Base model for all configuration classes with support for CamelCase."""
    
    class Config:
        # Allow extra fields in the input data (they'll be ignored)
        extra = Extra.ignore
        
        # Allow field population by alias (CamelCase)
        allow_population_by_field_name = True
        
        # Function to generate aliases for snake_case field names
        @staticmethod
        def alias_generator(field_name: str) -> str:
            """Generate CamelCase aliases for snake_case field names."""
            parts = field_name.split('_')
            return parts[0] + ''.join(word.capitalize() for word in parts[1:])


class Layer(str, Enum):
    """Valid medallion layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class ExpectationAction(str, Enum):
    """Actions to take when expectations fail."""
    QUARANTINE = "quarantine"
    DROP = "drop"
    FAIL = "fail"


class SeverityLevel(str, Enum):
    """Severity levels for expectation failures."""
    ERROR = "error"
    WARNING = "warning"
    INFO = "info"


class DataClassification(str, Enum):
    """Data classification levels."""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"


class MaskingStrategy(str, Enum):
    """Available masking strategies."""
    HASH = "hash"
    TRUNCATE = "truncate"
    REDACT = "redact"


class RetentionPeriod(str, Enum):
    """Time units for retention periods."""
    DAYS = "days"
    WEEKS = "weeks"
    MONTHS = "months"
    YEARS = "years"


class SourceFormat(str, Enum):
    """Valid source data formats."""
    CLOUD_FILES = "cloudFiles"
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    DELTA = "delta"
    AVRO = "avro"


class DeltaProperty(str, Enum):
    """Valid Delta table properties."""
    OPTIMIZE_WRITE = "delta.autoOptimize.optimizeWrite"
    AUTO_COMPACT = "delta.autoOptimize.autoCompact"
    COLUMN_MAPPING = "delta.columnMapping.mode"
    COMPATIBILITY_VERSION = "delta.compatibility.version"
    CHANGE_DATA_FEED = "delta.enableChangeDataFeed"
    MIN_READER_VERSION = "delta.minReaderVersion"
    MIN_WRITER_VERSION = "delta.minWriterVersion"
    RANDOMIZE_PREFIXES = "delta.randomizeFilePrefixes"
    PREFIX_LENGTH = "delta.randomPrefixLength"


class SCDType(int, Enum):
    """Valid SCD types."""
    TYPE_1 = 1
    TYPE_2 = 2


class Expectation(ConfigBaseModel):
    """Data quality expectation configuration."""
    name: str = Field(..., description="Name of the expectation")
    constraint: str = Field(..., description="SQL constraint expression")
    description: Optional[str] = Field(None, description="Description of what this expectation validates")
    action: ExpectationAction = Field(ExpectationAction.FAIL, description="Action to take on failure")
    severity: SeverityLevel = Field(SeverityLevel.ERROR, description="Severity level of expectation failure")

    @validator("name")
    def validate_name(cls, v):
        """Validate expectation name format."""
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', v):
            raise ValueError("Name must be a valid identifier (start with letter, contain only letters, numbers, underscores)")
        return v

    @validator("constraint")
    def validate_constraint(cls, v):
        """Validate SQL constraint format."""
        if not v.strip():
            raise ValueError("Constraint cannot be empty")
        # Basic SQL validation - should contain at least one comparison operator
        if not any(op in v for op in ['=', '>', '<', '>=', '<=', '!=', 'IN', 'LIKE', 'IS']):
            raise ValueError("Constraint must contain at least one comparison operator")
        return v


class Metric(ConfigBaseModel):
    """Data quality metric configuration."""
    name: str = Field(..., description="Name of the metric")
    value: str = Field(..., description="SQL expression for computing the metric")
    description: Optional[str] = Field(None, description="Description of what this metric measures")

    @validator("name")
    def validate_metric_name(cls, v):
        """Validate metric name format."""
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', v):
            raise ValueError("Metric name must be a valid identifier (start with letter, contain only letters, numbers, underscores)")
        return v

    @validator("value")
    def validate_metric_value(cls, v):
        """Validate metric SQL expression."""
        if not v.strip():
            raise ValueError("Metric value cannot be empty")
        # Basic SQL validation - should contain at least one aggregation function
        agg_functions = ['COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'STDDEV', 'VARIANCE']
        if not any(func in v.upper() for func in agg_functions):
            raise ValueError(f"Metric value should contain at least one aggregation function: {', '.join(agg_functions)}")
        return v


class QuarantineConfig(ConfigBaseModel):
    """Configuration for quarantine management within DLT pipelines."""
    enabled: bool = Field(True, description="Whether quarantine is enabled")
    source_table_name: Optional[str] = Field(None, description="Full Unity Catalog table name (catalog.schema.table)")
    error_column: str = Field("error_details", description="Column name for error details")
    timestamp_column: str = Field("quarantine_timestamp", description="Column name for quarantine timestamp")
    batch_id_column: str = Field("batch_id", description="Column name for batch ID")
    source_column: str = Field("source_table", description="Column name for source table")
    failed_expectations_column: str = Field("failed_expectations", description="Column name for failed expectations")

    @validator("source_table_name")
    def validate_table_name(cls, v):
        """Validate fully qualified table name format if provided."""
        if v is not None and not re.match(r'^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$', v):
            raise ValueError("source_table_name must be in format: catalog.schema.table")
        return v

    @validator("error_column", "timestamp_column", "batch_id_column", "source_column", "failed_expectations_column")
    def validate_column_names(cls, v, field):
        """Validate column name format."""
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', v):
            raise ValueError(f"{field.name} must be a valid column name (start with letter, contain only letters, numbers, underscores)")
        return v

    def get_quarantine_table_name(self, source_table_name: Optional[str] = None) -> str:
        """Generate the quarantine table name based on source table.
        
        Args:
            source_table_name: Optional source table name to override the config
            
        Returns:
            Fully qualified quarantine table name
            
        Raises:
            ValueError: If no source table name is available
        """
        table_name = source_table_name or self.source_table_name
        if not table_name:
            raise ValueError("source_table_name must be provided either in config or at runtime")
            
        # Validate the table name format
        if not re.match(r'^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$', table_name):
            raise ValueError("source_table_name must be in format: catalog.schema.table")
        
        # Split the fully qualified table name
        parts = table_name.split('.')
        
        # Add _quarantine suffix to table name
        parts[-1] = f"{parts[-1]}_quarantine"
        return '.'.join(parts)


class MonitoringConfig(ConfigBaseModel):
    """Monitoring configuration."""
    expectations: List[Expectation] = Field(default_factory=list, description="Data quality expectations")
    metrics: List[Metric] = Field(default_factory=list, description="Data quality metrics")
    alerts: List[str] = Field(default_factory=list, description="List of alert configurations")
    dashboard: Optional[str] = Field(None, description="Dashboard configuration name")
    
    @validator("alerts")
    def validate_alerts(cls, v):
        """Validate alert names."""
        for alert in v:
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', alert):
                raise ValueError(f"Invalid alert name: {alert}. Must start with letter and contain only letters, numbers, underscores")
        return v

    @validator("dashboard")
    def validate_dashboard(cls, v):
        """Validate dashboard name."""
        if v is not None and not re.match(r'^[a-zA-Z][a-zA-Z0-9_-]*$', v):
            raise ValueError("Dashboard name must start with letter and contain only letters, numbers, underscores, or hyphens")
        return v

    @root_validator(pre=True)
    def process_expectations_and_metrics(cls, values):
        """Process expectations and metrics if provided as dicts."""
        if "expectations" in values and isinstance(values["expectations"], list):
            values["expectations"] = [
                exp if isinstance(exp, Expectation) else Expectation(**exp)
                for exp in values["expectations"]
            ]
        
        if "metrics" in values and isinstance(values["metrics"], list):
            values["metrics"] = [
                metric if isinstance(metric, Metric) else Metric(**metric)
                for metric in values["metrics"]
            ]
        
        return values


class SCDConfig(ConfigBaseModel):
    """Slowly Changing Dimension configuration."""
    type: SCDType = Field(SCDType.TYPE_2, description="SCD type (1, 2)")
    key_columns: List[str] = Field(..., description="Natural key columns")
    track_columns: List[str] = Field(..., description="Columns to track changes")
    effective_from: str = Field("effective_date", description="Effective from column name")
    effective_to: Optional[str] = Field("end_date", description="Effective to column name")
    current_flag: Optional[str] = Field("is_current", description="Current flag column name")

    @validator("key_columns", "track_columns")
    def validate_column_lists(cls, v):
        """Validate column name formats."""
        if not v:
            raise ValueError("Column list cannot be empty")
        for col in v:
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', col):
                raise ValueError(f"Invalid column name: {col}. Must start with letter and contain only letters, numbers, underscores")
        return v

    @root_validator
    def validate_scd_config(cls, values):
        """Validate SCD configuration consistency."""
        scd_type = values.get("type")
        if scd_type == SCDType.TYPE_2:
            if not values.get("effective_to"):
                raise ValueError("SCD Type 2 requires effective_to column")
            if not values.get("current_flag"):
                raise ValueError("SCD Type 2 requires current_flag column")
        return values


class GovernanceConfig(ConfigBaseModel):
    """Governance configuration."""
    owner: Optional[str] = Field(None, description="Table owner")
    steward: Optional[str] = Field(None, description="Data steward")
    schema_evolution: bool = Field(True, description="Whether schema evolution is allowed")
    pii_detection: bool = Field(False, description="Whether PII detection is enabled")
    masking_enabled: bool = Field(False, description="Whether data masking is enabled") 
    masking_overrides: Dict[str, MaskingStrategy] = Field(default_factory=dict, description="Column-specific masking strategies")
    classification: Dict[str, DataClassification] = Field(default_factory=dict, description="Column classifications")
    retention: Optional[str] = Field(None, description="Data retention policy")
    tags: Dict[str, str] = Field(default_factory=dict, description="Governance tags")
    
    @validator("owner", "steward")
    def validate_email(cls, v):
        """Validate email format."""
        if v is not None and not re.match(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', v):
            raise ValueError("Invalid email format")
        return v

    @validator("retention")
    def validate_retention(cls, v):
        """Validate retention policy format."""
        if v is not None:
            pattern = rf'^(\d+)\s+({"|".join(period.value for period in RetentionPeriod)})$'
            if not re.match(pattern, v.lower()):
                raise ValueError(f"Retention must be in format: 'X {' or '.join(period.value for period in RetentionPeriod)}'")
        return v


class UnityTableConfig(ConfigBaseModel):
    """Unity Catalog table configuration."""
    name: str = Field(..., description="Name of the table")
    catalog: str = Field(..., description="Unity Catalog name")
    schema_name: str = Field(..., description="Schema name", alias="schema")
    description: Optional[str] = Field(None, description="Description of the table's purpose")
    properties: Dict[DeltaProperty, Any] = Field(default_factory=dict, description="Delta table properties")
    column_comments: Dict[str, str] = Field(default_factory=dict, description="Column-level comments")
    tags: Dict[str, str] = Field(default_factory=dict, description="Table tags")

    @validator("name", "catalog", "schema_name")
    def validate_identifiers(cls, v, field):
        """Validate Unity Catalog identifiers."""
        if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', v):
            raise ValueError(f"{field.name} must start with letter and contain only letters, numbers, underscores")
        if len(v) > 255:
            raise ValueError(f"{field.name} must not exceed 255 characters")
        return v

    @validator("description")
    def validate_description(cls, v):
        """Validate description length."""
        if v is not None and len(v) > 1000:
            raise ValueError("Description must not exceed 1000 characters")
        return v

    def get_full_table_name(self) -> str:
        """Get the fully qualified table name."""
        return f"{self.catalog}.{self.schema_name}.{self.name}"

    def get_governance_tags(self, layer: Layer, governance: Optional[GovernanceConfig] = None) -> Dict[str, str]:
        """Generate governance tags based on configuration.
        
        Args:
            layer: The layer this table belongs to
            governance: Optional governance configuration
            
        Returns:
            Dictionary of tags for governance and discoverability
        """
        # Start with user-defined tags
        all_tags = dict(self.tags)
        
        # Add basic table metadata tags
        all_tags.update({
            "layer": layer.value,
            "catalog": self.catalog,
            "schema": self.schema_name,
            "table": self.name,
            "full_name": self.get_full_table_name()
        })
        
        # Add governance-related tags if configuration is provided
        if governance:
            if governance.owner:
                all_tags["owner"] = governance.owner
            if governance.steward:
                all_tags["steward"] = governance.steward
            if governance.pii_detection:
                all_tags["pii_detection"] = "enabled"
            if governance.masking_enabled:
                all_tags["data_masking"] = "enabled"
            if governance.retention:
                all_tags["retention_policy"] = governance.retention
            
            # Add data classification tags
            if governance.classification:
                all_tags["highest_classification"] = max(
                    (c.value for c in governance.classification.values()),
                    default="unclassified"
                )
                
            # Add governance tags
            all_tags.update(governance.tags)
        
        # Add schema evolution tag
        if governance and governance.schema_evolution:
            all_tags["schema_evolution"] = "enabled"
        
        # Add Delta table property indicators
        for prop in self.properties:
            if isinstance(prop, DeltaProperty):
                all_tags[f"delta_{prop.name.lower()}"] = "enabled"
        
        return all_tags


class ReferenceConfig(ConfigBaseModel):
    """Reference data configuration."""
    name: str = Field(..., description="Logical name for the reference")
    table_name: str = Field(..., description="Physical table name (catalog.schema.table)")
    join_keys: Dict[str, str] = Field(..., description="Mapping of source columns to reference columns")
    lookup_columns: List[str] = Field(..., description="Columns needed for lookup operations")
    cache_ttl: Optional[int] = Field(None, description="Cache duration in seconds")

    @validator("table_name")
    def validate_table_name(cls, v):
        """Validate fully qualified table name format."""
        if not re.match(r'^[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$', v):
            raise ValueError("table_name must be in format: catalog.schema.table")
        return v

    @validator("lookup_columns")
    def validate_lookup_columns(cls, v):
        """Validate lookup column names."""
        if not v:
            raise ValueError("lookup_columns cannot be empty")
        for col in v:
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', col):
                raise ValueError(f"Invalid column name: {col}")
        return v

    @validator("cache_ttl")
    def validate_cache_ttl(cls, v):
        """Validate cache TTL if provided."""
        if v is not None and v <= 0:
            raise ValueError("cache_ttl must be positive")
        return v


class BaseLayerConfig(ConfigBaseModel):
    """Base configuration for all layers."""
    table: UnityTableConfig = Field(..., description="Unity Catalog table configuration")
    version: Optional[str] = Field(None, description="Configuration version")
    monitoring: Optional[MonitoringConfig] = Field(None, description="Monitoring configuration")
    governance: Optional[GovernanceConfig] = Field(None, description="Governance configuration")
    validations: List[Expectation] = Field(default_factory=list, description="Data quality expectations")
    metrics: List[Metric] = Field(default_factory=list, description="Quality metrics to compute")

    @validator("version")
    def validate_version(cls, v):
        """Validate version format if provided."""
        if v is not None and not re.match(r'^\d+\.\d+\.\d+$', v):
            raise ValueError("version must be in format: MAJOR.MINOR.PATCH")
        return v


class BronzeConfig(BaseLayerConfig):
    """Bronze layer specific configuration."""
    quarantine: Optional[QuarantineConfig] = Field(None, description="Quarantine configuration")
    
    @root_validator(pre=True)
    def process_quarantine(cls, values):
        """Process quarantine configuration if provided as dict."""
        if "quarantine" in values and isinstance(values["quarantine"], dict):
            values["quarantine"] = QuarantineConfig(**values["quarantine"])
        return values


class SilverConfig(BaseLayerConfig):
    """Silver layer specific configuration."""
    deduplication: bool = Field(True, description="Whether deduplication is enabled")
    normalization: bool = Field(True, description="Whether data normalization is enabled")
    scd: Optional[SCDConfig] = Field(None, description="SCD configuration")
    references: List[ReferenceConfig] = Field(default_factory=list, description="Reference data configurations")
    
    @root_validator(pre=True)
    def process_scd(cls, values):
        """Process SCD configuration if provided as dict."""
        if "scd" in values and isinstance(values["scd"], dict):
            values["scd"] = SCDConfig(**values["scd"])
        return values

    @root_validator(pre=True)
    def process_references(cls, values):
        """Process reference configurations if provided as dicts."""
        if "references" in values and isinstance(values["references"], list):
            values["references"] = [
                ref if isinstance(ref, ReferenceConfig) else ReferenceConfig(**ref)
                for ref in values["references"]
            ]
        return values


class GoldConfig(BaseLayerConfig):
    """Gold layer specific configuration."""
    references: List[ReferenceConfig] = Field(default_factory=list, description="Reference data configurations")
    dimensions: Dict[str, str] = Field(default_factory=dict, description="Dimension to join key mapping")
    verify_pii_masking: bool = Field(True, description="Whether to verify PII masking")

    @root_validator(pre=True)
    def process_references(cls, values):
        """Process reference configurations if provided as dicts."""
        if "references" in values and isinstance(values["references"], list):
            values["references"] = [
                ref if isinstance(ref, ReferenceConfig) else ReferenceConfig(**ref)
                for ref in values["references"]
            ]
        return values

    @validator("dimensions")
    def validate_dimensions(cls, v):
        """Validate dimension mappings."""
        for key, value in v.items():
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', key):
                raise ValueError(f"Invalid dimension name: {key}")
            if not re.match(r'^[a-zA-Z][a-zA-Z0-9_]*$', value):
                raise ValueError(f"Invalid join column: {value}")
        return v

    @validator("references")
    def validate_references(cls, v):
        """Validate reference configurations."""
        if not isinstance(v, list):
            raise ValueError("References must be a list")
        return v

    @root_validator
    def validate_gold_config(cls, values):
        """Validate Gold layer configuration."""
        verify_pii_masking = values.get("verify_pii_masking", False)
        governance = values.get("governance")
        
        if verify_pii_masking and (not governance or not governance.pii_detection):
            raise ValueError("PII detection must be enabled in governance config when verify_pii_masking is True")
        return values
