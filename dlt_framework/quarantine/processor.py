from typing import List, Tuple, Optional
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from ..config import Expectation, QuarantineConfig


def process_quarantine_records(
    df: DataFrame,
    expectations: List[Expectation],
    config: QuarantineConfig,
    source_table: str,
    batch_id: Optional[str] = None
) -> Tuple[DataFrame, DataFrame]:
    """
    Process records for quarantine based on expectations.
    
    Args:
        df: Input DataFrame
        expectations: List of expectations to validate
        config: Quarantine configuration
        source_table: Source table name
        batch_id: Optional batch identifier
        
    Returns:
        Tuple of (valid_df, quarantined_df)
    """
    # Skip if no expectations or quarantine not enabled
    if not expectations or not config.enabled:
        return df, df.filter(F.lit(False))  # Return empty quarantine DataFrame
        
    # Create combined condition for all expectations
    valid_conditions = []
    for exp in expectations:
        valid_conditions.append(F.expr(exp.constraint))
    
    combined_condition = valid_conditions[0]
    for condition in valid_conditions[1:]:
        combined_condition = combined_condition & condition
    
    # Split data into valid and invalid
    valid_df = df.filter(combined_condition)
    invalid_df = df.filter(~combined_condition)
    
    # Prepare invalid records with quarantine metadata
    if not invalid_df.isEmpty():
        # Create failed expectations array
        expectations_array = F.array([
            F.struct(
                F.lit(exp.name).alias("name"),
                F.lit(exp.constraint).alias("constraint")
            )
            for exp in expectations
        ])
        
        # Add quarantine metadata columns
        invalid_df = invalid_df.withColumn(
            "quarantine_metadata",
            F.struct(
                F.current_timestamp().alias(config.timestamp_column),
                F.lit(source_table).alias(config.source_column),
                F.lit(batch_id).alias(config.batch_id_column),
                F.lit("Failed validation expectations").alias(config.error_column),
                expectations_array.alias(config.failed_expectations_column)
            )
        )
    
    return valid_df, invalid_df