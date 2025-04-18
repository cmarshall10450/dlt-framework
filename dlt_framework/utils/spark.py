"""Spark session utilities for the DLT Medallion Framework."""

from typing import Optional

from pyspark.sql import SparkSession

class SparkSessionNotFoundError(Exception):
    """Raised when no active SparkSession is found."""

    pass


def get_spark_session() -> SparkSession:
    """
    Get the active SparkSession.
    
    This function should be used in Python files instead of directly accessing the 'spark' variable,
    which is only available in Databricks notebooks.
    
    Returns:
        SparkSession: The active SparkSession

    Raises:
        SparkSessionNotFoundError: If no active SparkSession is found
    """
    spark: Optional[SparkSession] = SparkSession.getActiveSession()
    
    if spark is None:
        raise SparkSessionNotFoundError(
            "No active SparkSession found. "
            "This framework must be run in a Databricks environment with Runtime >= 15.4 LTS"
        )
    
    return spark 