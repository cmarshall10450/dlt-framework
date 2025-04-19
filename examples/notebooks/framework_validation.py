# Databricks notebook source
# MAGIC %md
# MAGIC # DLT Medallion Framework Validation
# MAGIC 
# MAGIC This notebook demonstrates the usage of the DLT Medallion Framework with both YAML and object-based configuration.

# COMMAND ----------
# MAGIC %pip install pyspark pyyaml

# COMMAND ----------

from datetime import datetime
from pathlib import Path
from typing import Dict, List

import dlt
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType

from dlt_framework.core.config_models import (
    BronzeConfig,
    SilverConfig,
    GoldConfig,
    Expectation,
    SCDConfig,
    MonitoringConfig,
)
from dlt_framework.decorators import bronze, silver, gold
from dlt_framework.validation.quarantine import QuarantineConfig

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Sample Data

# COMMAND ----------

def generate_sample_data(spark) -> DataFrame:
    """Generate sample transaction data."""
    data = [
        (1, "PENDING", 100.50, "john@email.com", "2024-01-01"),
        (2, "completed", 200.75, "jane@email.com", "2024-01-01"),
        (3, "FAILED", -50.25, "invalid_email", "2024-01-02"),
        (None, "unknown", 300.00, "bob@email.com", "2024-01-02"),
        (4, "PENDING", 150.00, "sarah@email.com", "2024-01-03"),
        (4, "COMPLETED", 150.00, "sarah@email.com", "2024-01-03"),  # Duplicate
    ]
    
    # Define schema with proper types
    schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("status", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("email", StringType(), True),
        StructField("date", StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Method 1: Object-Based Configuration

# COMMAND ----------

# Bronze layer configuration
bronze_config = BronzeConfig(
    quarantine=True,
    pii_detection=True,
    schema_evolution=True,
    validate=[
        Expectation(
            name="valid_transaction_id",
            constraint="transaction_id IS NOT NULL",
            action="quarantine"
        ),
        Expectation(
            name="valid_amount",
            constraint="amount > 0",
            action="quarantine"
        )
    ],
    metrics=["raw_record_count", "invalid_record_count"],
    monitoring=MonitoringConfig(
        metrics=["record_count", "null_count"],
        alerts=["data_quality_alert"]
    )
)

# Silver layer configuration
silver_config = SilverConfig(
    deduplication=True,
    scd=SCDConfig(
        type=2,
        key_columns=["transaction_id"],
        track_columns=["amount", "status"]
    ),
    normalization={
        "status": "UPPER(status)",
        "amount": "ROUND(amount, 2)"
    },
    validate=[
        Expectation(
            name="unique_transaction",
            constraint="COUNT(*) OVER (PARTITION BY transaction_id) = 1",
            action="fail"
        )
    ]
)

# Gold layer configuration
gold_config = GoldConfig(
    references={
        "customer_id": "dim_customers.id",
        "product_id": "dim_products.id"
    },
    dimensions=["customer", "product", "date"],
    validate=[
        Expectation(
            name="complete_dimensions",
            constraint="customer_id IS NOT NULL AND product_id IS NOT NULL",
            action="fail"
        )
    ]
)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Method 2: YAML Configuration
# MAGIC The YAML configuration is loaded from 'config.yaml' in the same directory.

# COMMAND ----------

# Get the current notebook's directory
notebook_dir = Path().absolute()
config_path = notebook_dir / "config.yaml"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Pipeline Implementation
# MAGIC Demonstrating both configuration methods

# COMMAND ----------

# Using object-based configuration
@dlt.table
@bronze(config=bronze_config)
def raw_transactions() -> DataFrame:
    """Ingest raw transaction data."""
    return generate_sample_data(spark)

# Using YAML configuration
@dlt.table
@silver(config_path=config_path)
def cleaned_transactions() -> DataFrame:
    """Clean and standardize transaction data."""
    return dlt.read("raw_transactions")

# Using object-based configuration
@dlt.table
@gold(config=gold_config)
def transaction_metrics() -> DataFrame:
    """Calculate transaction metrics."""
    df = dlt.read("cleaned_transactions")
    return df.groupBy("date").agg(
        F.sum("amount").alias("daily_revenue"),
        F.countDistinct("transaction_id").alias("transaction_count")
    )

# COMMAND ----------
# MAGIC %md
# MAGIC ## View Results

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View raw transactions including quarantined records
# MAGIC SELECT * FROM raw_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View cleaned transactions
# MAGIC SELECT * FROM cleaned_transactions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View transaction metrics
# MAGIC SELECT * FROM transaction_metrics 