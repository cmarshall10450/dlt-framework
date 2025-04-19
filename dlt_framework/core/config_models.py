"""Configuration models for the DLT Medallion Framework."""

from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Union


class ExpectationAction(str, Enum):
    """Valid actions for expectations."""
    QUARANTINE = "quarantine"
    DROP = "drop"
    FAIL = "fail"


@dataclass
class Expectation:
    """Data quality expectation configuration."""
    name: str
    constraint: str
    action: ExpectationAction = ExpectationAction.QUARANTINE

    def __post_init__(self):
        """Validate and convert action to enum."""
        if isinstance(self.action, str):
            try:
                self.action = ExpectationAction(self.action.lower())
            except ValueError:
                raise ValueError(
                    f"Invalid action '{self.action}'. Must be one of: "
                    f"{', '.join(a.value for a in ExpectationAction)}"
                )


@dataclass
class Metric:
    """Data quality metric configuration."""
    name: str
    value: Optional[str] = None


@dataclass
class MonitoringConfig:
    """Monitoring and observability configuration."""
    metrics: List[Union[str, Metric]] = field(default_factory=list)
    alerts: List[str] = field(default_factory=list)
    dashboard: Optional[str] = None

    def __post_init__(self):
        """Convert string metrics to Metric objects."""
        processed_metrics = []
        for metric in self.metrics:
            if isinstance(metric, str):
                processed_metrics.append(Metric(name=metric))
            elif isinstance(metric, dict):
                processed_metrics.append(Metric(**metric))
            else:
                processed_metrics.append(metric)
        self.metrics = processed_metrics


@dataclass
class GovernanceConfig:
    """Data governance configuration."""
    owner: Optional[str] = None
    steward: Optional[str] = None
    classification: Optional[Dict[str, str]] = None
    retention: Optional[str] = None
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class SCDConfig:
    """Slowly Changing Dimension configuration."""
    type: int = 2
    key_columns: List[str] = field(default_factory=list)
    track_columns: Optional[List[str]] = None  # None means all columns
    timestamp_col: str = "updated_at"
    current_flag: Optional[str] = "is_current"

    def __post_init__(self):
        """Validate SCD type."""
        if self.type not in [1, 2]:
            raise ValueError("SCD type must be 1 or 2")


@dataclass
class BaseLayerConfig:
    """Base configuration for all layers."""
    validate: List[Union[Expectation, Dict]] = field(default_factory=list)
    metrics: List[Union[str, Metric, Dict]] = field(default_factory=list)
    monitoring: Optional[Union[MonitoringConfig, Dict]] = None
    governance: Optional[Union[GovernanceConfig, Dict]] = None

    def __post_init__(self):
        """Process and validate configuration."""
        # Process expectations
        processed_expectations = []
        for exp in self.validate:
            if isinstance(exp, dict):
                processed_expectations.append(Expectation(**exp))
            else:
                processed_expectations.append(exp)
        self.validate = processed_expectations

        # Process metrics
        processed_metrics = []
        for metric in self.metrics:
            if isinstance(metric, str):
                processed_metrics.append(Metric(name=metric))
            elif isinstance(metric, dict):
                processed_metrics.append(Metric(**metric))
            else:
                processed_metrics.append(metric)
        self.metrics = processed_metrics

        # Process monitoring config
        if isinstance(self.monitoring, dict):
            self.monitoring = MonitoringConfig(**self.monitoring)

        # Process governance config
        if isinstance(self.governance, dict):
            self.governance = GovernanceConfig(**self.governance)


@dataclass
class BronzeConfig(BaseLayerConfig):
    """Bronze layer configuration."""
    quarantine: bool = True
    pii_detection: bool = False
    schema_evolution: bool = True


@dataclass
class SilverConfig(BaseLayerConfig):
    """Silver layer configuration."""
    deduplication: bool = False
    scd: Optional[Union[SCDConfig, Dict]] = None
    normalization: Dict[str, str] = field(default_factory=dict)
    masking_enabled: bool = False
    masking_overrides: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self):
        """Process and validate configuration."""
        super().__post_init__()
        if isinstance(self.scd, dict):
            self.scd = SCDConfig(**self.scd)
        
        # Validate masking strategies
        valid_strategies = {"hash", "truncate", "redact"}
        invalid_strategies = set(self.masking_overrides.values()) - valid_strategies
        if invalid_strategies:
            raise ValueError(
                f"Invalid masking strategies: {invalid_strategies}. "
                f"Must be one of: {', '.join(valid_strategies)}"
            )


@dataclass
class GoldConfig(BaseLayerConfig):
    """Gold layer configuration."""
    references: Dict[str, str] = field(default_factory=dict)
    dimensions: List[str] = field(default_factory=list) 