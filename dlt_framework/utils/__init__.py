"""Utility functions for the DLT Medallion Framework."""

from .spark import get_spark_session, SparkSessionNotFoundError

__all__ = ["get_spark_session", "SparkSessionNotFoundError"] 