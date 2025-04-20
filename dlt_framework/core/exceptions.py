"""Custom exceptions for the DLT Medallion Framework."""


class DLTFrameworkError(Exception):
    """Base exception for all framework errors."""
    pass


class ConfigurationError(DLTFrameworkError):
    """Raised when there is an error in configuration loading or validation."""
    pass


class ValidationError(DLTFrameworkError):
    """Raised when data validation fails."""
    pass


class LayerError(DLTFrameworkError):
    """Raised when there is an error specific to a medallion layer."""
    pass


class MonitoringError(DLTFrameworkError):
    """Raised when there is an error in monitoring or metrics collection."""
    pass


class GovernanceError(DLTFrameworkError):
    """Raised when there is an error in governance or compliance checks."""
    pass 