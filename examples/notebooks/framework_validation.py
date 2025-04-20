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
import pyspark.sql.types as T

from dlt_framework.config import (
    BronzeConfig,
    SilverConfig,
    GoldConfig,
    Expectation,
    SCDConfig,
    MonitoringConfig,
    Metric,
    UnityTableConfig,
    QuarantineConfig,
    GovernanceConfig
)
from dlt_framework.decorators import bronze, silver, gold

# COMMAND ----------
# MAGIC %md
# MAGIC ## Generate Sample Data

# COMMAND ----------

def generate_sample_data(spark) -> DataFrame:
    """Generate sample transaction data with various data quality issues."""
    data = [
        # Valid records
        (1, "PENDING", 100.50, "john@email.com", "2024-01-01"),
        (2, "completed", 200.75, "jane@email.com", "2024-01-01"),
        # Invalid records for quarantine
        (3, "FAILED", -50.25, "invalid_email", "2024-01-02"),  # Negative amount
        (None, "unknown", 300.00, "bob@email.com", "2024-01-02"),  # Null ID
        (5, "PENDING", -75.50, None, "2024-01-02"),  # Negative amount & null email
        # Duplicates for testing
        (4, "PENDING", 150.00, "sarah@email.com", "2024-01-03"),
        (4, "COMPLETED", 150.00, "sarah@email.com", "2024-01-03"),
    ]
    
    # Define schema with proper types
    schema = T.StructType([
        T.StructField("transaction_id", T.LongType(), True),
        T.StructField("status", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("email", T.StringType(), True),
        T.StructField("date", T.StringType(), True)
    ])
    
    return spark.createDataFrame(data, schema)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Method 1: Object-Based Configuration

# COMMAND ----------

# Bronze layer configuration with quarantine expectations
bronze_config = BronzeConfig(
    # Unity Catalog table configuration (required)
    table=UnityTableConfig(
        name="raw_transactions",
        catalog="demo",
        schemaName="bronze",
        description="Raw transaction data with quality checks and quarantine"
    ),
    # Quarantine configuration
    quarantine=QuarantineConfig(
        enabled=True,
        source_table_name="demo.bronze.raw_transactions"
    ),
    pii_detection=True,
    schema_evolution=True,
    validations=[
        Expectation(
            name="valid_transaction_id",
            constraint="transaction_id IS NOT NULL",
            action="quarantine"  # Records with null IDs will be quarantined
        ),
        Expectation(
            name="valid_amount",
            constraint="amount > 0",
            action="quarantine"  # Records with negative amounts will be quarantined
        ),
        Expectation(
            name="valid_email",
            constraint="email IS NOT NULL AND email LIKE '%@%.%'",
            action="quarantine"  # Records with invalid emails will be quarantined
        )
    ],
    monitoring=MonitoringConfig(
        metrics=[
            Metric(
                name="raw_record_count",
                value="COUNT(*)",
                description="Total number of raw records"
            ),
            Metric(
                name="quarantined_record_count",
                value="COUNT(*) WHERE quarantined = true",
                description="Number of quarantined records"
            ),
            Metric(
                name="record_count",
                value="COUNT(*)",
                description="Total number of records"
            ),
            Metric(
                name="null_count",
                value="COUNT(*) WHERE value IS NULL",
                description="Number of null values"
            )
        ],
        alerts=["data_quality_alert"]
    )
) 

# Silver layer configuration
silver_config = SilverConfig(
    table=UnityTableConfig(
        name="cleaned_transactions",
        catalog="demo",
        schemaName="silver",
        description="Cleaned and standardized transaction data"
    ),
    deduplication=True,
    normalization=True,  # Enable normalization
    scd=SCDConfig(
        type=2,
        key_columns=["transaction_id"],
        track_columns=["amount", "status"]
    ),
    validations=[
        Expectation(
            name="unique_transaction",
            constraint="COUNT(*) OVER (PARTITION BY transaction_id) = 1",
            action="fail"
        )
    ]
)

# Gold layer configuration
gold_config = GoldConfig(
    table=UnityTableConfig(
        name="transaction_metrics",
        catalog="demo",
        schemaName="gold",
        description="Aggregated transaction metrics with dimension references"
    ),
    references={
        "customer_id": "dim_customers.id",
        "product_id": "dim_products.id"
    },
    dimensions=["customer", "product", "date"],
    validations=[
        Expectation(
            name="complete_dimensions",
            constraint="customer_id IS NOT NULL AND product_id IS NOT NULL",
            action="fail"
        )
    ],
    governance=GovernanceConfig(
        pii_detection=True  # Enable PII detection since verify_pii_masking is True
    )
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
# MAGIC ## Pipeline Implementation with Quarantine Testing

# COMMAND ----------

# Using object-based configuration with quarantine
@bronze(config=bronze_config)
def raw_transactions() -> DataFrame:
    """Ingest raw transaction data with quarantine handling."""
    return generate_sample_data(spark)

# Using YAML configuration
@silver(config_path=config_path)
def cleaned_transactions() -> DataFrame:
    """Clean and standardize transaction data.
    
    Dependencies are managed automatically by DLT based on dlt.read() calls.
    This ensures proper execution order without needing explicit configuration.
    """
    return dlt.read("raw_transactions")

# Using object-based configuration
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
# MAGIC ## View Results and Validate Quarantine

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View raw transactions (should only include valid records)
# MAGIC SELECT * FROM raw_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View quarantined records with their failure reasons
# MAGIC SELECT 
# MAGIC   *,
# MAGIC   quarantine_metadata.quarantine_timestamp,
# MAGIC   quarantine_metadata.source_table,
# MAGIC   explode(quarantine_metadata.failed_expectations) as failed_expectation
# MAGIC FROM raw_transactions_quarantine
# MAGIC ORDER BY quarantine_metadata.quarantine_timestamp;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze quarantine reasons
# MAGIC SELECT 
# MAGIC   failed_expectation.name as failed_rule,
# MAGIC   COUNT(*) as failure_count
# MAGIC FROM raw_transactions_quarantine
# MAGIC LATERAL VIEW explode(quarantine_metadata.failed_expectations) exp AS failed_expectation
# MAGIC GROUP BY failed_expectation.name
# MAGIC ORDER BY failure_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View cleaned transactions (should only include valid, deduplicated records)
# MAGIC SELECT * FROM cleaned_transactions;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View transaction metrics
# MAGIC SELECT * FROM transaction_metrics; 