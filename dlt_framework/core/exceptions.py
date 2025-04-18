"""Core exceptions for the DLT Medallion Framework."""

class DLTFrameworkError(Exception):
    """Base exception for all framework errors."""

    pass


class ConfigurationError(DLTFrameworkError):
    """Raised when there is an error in configuration."""

    pass


class ValidationError(DLTFrameworkError):
    """Raised when data validation fails."""

    pass


class DecoratorError(DLTFrameworkError):
    """Raised when there is an error in decorator processing."""

    pass


class LayerError(DLTFrameworkError):
    """Raised when there is an error in layer processing."""

    pass


class GovernanceError(DLTFrameworkError):
    """Raised when there is an error in governance processing."""

    pass


class MonitoringError(DLTFrameworkError):
    """Raised when there is an error in monitoring."""

    pass 