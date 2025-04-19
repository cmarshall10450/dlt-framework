"""Schema validation engine for the DLT Medallion Framework.

This module provides schema validation capabilities with support for:
- Schema validation against expected schemas
- Schema evolution handling
- Quarantine mechanisms for invalid records
- Validation metrics tracking
"""

from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from datetime import datetime

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
import pyspark.sql.functions as F

@dataclass
class ValidationResult:
    """Contains the results of a schema validation operation."""
    valid_records: DataFrame
    invalid_records: Optional[DataFrame] = None
    validation_time: datetime = datetime.now()
    error_counts: Dict[str, int] = None
    metrics: Dict[str, float] = None

class SchemaValidator:
    """Schema validation engine with quarantine support."""
    
    def __init__(
        self,
        expected_schema: StructType,
        evolution_mode: str = "additive",
        quarantine_path: Optional[str] = None,
        track_metrics: bool = True
    ):
        """Initialize the schema validator.
        
        Args:
            expected_schema: The expected PySpark schema
            evolution_mode: How to handle schema changes ('strict', 'additive', 'all')
            quarantine_path: Optional path to store quarantined records
            track_metrics: Whether to track validation metrics
        """
        self.expected_schema = expected_schema
        self.evolution_mode = evolution_mode
        self.quarantine_path = quarantine_path
        self.track_metrics = track_metrics
        
        # Validate evolution mode
        valid_modes = ["strict", "additive", "all"]
        if evolution_mode not in valid_modes:
            raise ValueError(f"Evolution mode must be one of {valid_modes}")
    
    def _validate_schema_evolution(
        self, 
        current_schema: StructType
    ) -> Tuple[bool, List[str]]:
        """Validate if schema evolution is allowed based on evolution mode.
        
        Args:
            current_schema: The schema of the incoming data
            
        Returns:
            Tuple of (is_valid, list of validation messages)
        """
        messages = []
        is_valid = True
        
        # Get field names and types
        expected_fields = {f.name: f.dataType for f in self.expected_schema.fields}
        current_fields = {f.name: f.dataType for f in current_schema.fields}
        
        # Check for missing required fields
        missing_fields = set(expected_fields.keys()) - set(current_fields.keys())
        if missing_fields:
            messages.append(f"Missing required fields: {missing_fields}")
            is_valid = False
        
        # Check for new fields
        new_fields = set(current_fields.keys()) - set(expected_fields.keys())
        if new_fields:
            if self.evolution_mode == "strict":
                messages.append(f"New fields not allowed in strict mode: {new_fields}")
                is_valid = False
            else:
                messages.append(f"New fields detected: {new_fields}")
        
        # Check for type mismatches
        for field_name in expected_fields.keys() & current_fields.keys():
            if expected_fields[field_name] != current_fields[field_name]:
                messages.append(
                    f"Type mismatch for {field_name}: "
                    f"expected {expected_fields[field_name]}, "
                    f"got {current_fields[field_name]}"
                )
                is_valid = False
        
        return is_valid, messages

    def _prepare_error_column(self) -> str:
        """Create the error column name with timestamp to avoid conflicts."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"_error_details_{timestamp}"

    def validate(
        self, 
        df: DataFrame,
        drop_invalid: bool = False
    ) -> ValidationResult:
        """Validate the input DataFrame against the expected schema.
        
        Args:
            df: Input DataFrame to validate
            drop_invalid: Whether to drop invalid records or quarantine them
            
        Returns:
            ValidationResult containing valid and invalid records
        """
        start_time = datetime.now()
        error_counts = {}
        metrics = {}
        
        # Validate schema evolution
        schema_valid, messages = self._validate_schema_evolution(df.schema)
        
        if not schema_valid and self.evolution_mode == "strict":
            raise ValueError(f"Schema validation failed: {messages}")
        
        # Create error column for tracking validation issues
        error_col = self._prepare_error_column()
        
        # Initialize validation DataFrame with error column
        validation_df = df.withColumn(error_col, F.lit(None).cast(StringType()))
        
        # Validate each field
        for field in self.expected_schema.fields:
            field_errors = []
            
            # Check if field exists
            if field.name not in df.columns:
                if not field.nullable:
                    validation_df = validation_df.withColumn(
                        error_col,
                        F.concat(
                            F.coalesce(F.col(error_col), F.lit("")),
                            F.lit(f"Missing required field: {field.name}; ")
                        )
                    )
                continue
            
            # Check for nulls in non-nullable fields
            if not field.nullable:
                validation_df = validation_df.withColumn(
                    error_col,
                    F.when(
                        F.col(field.name).isNull(),
                        F.concat(
                            F.coalesce(F.col(error_col), F.lit("")),
                            F.lit(f"Null value in non-nullable field: {field.name}; ")
                        )
                    ).otherwise(F.col(error_col))
                )
            
            # Type validation
            if field.name in df.columns:
                current_type = df.schema[field.name].dataType
                if current_type != field.dataType:
                    validation_df = validation_df.withColumn(
                        error_col,
                        F.concat(
                            F.coalesce(F.col(error_col), F.lit("")),
                            F.lit(
                                f"Type mismatch for {field.name}: "
                                f"expected {field.dataType}, got {current_type}; "
                            )
                        )
                    )
        
        # Split into valid and invalid records
        valid_records = validation_df.filter(F.col(error_col).isNull())
        invalid_records = validation_df.filter(F.col(error_col).isNotNull())
        
        # Drop the error column from valid records
        valid_records = valid_records.drop(error_col)
        
        # Calculate metrics if enabled
        if self.track_metrics:
            total_count = df.count()
            valid_count = valid_records.count()
            invalid_count = total_count - valid_count
            
            metrics = {
                "total_records": total_count,
                "valid_records": valid_count,
                "invalid_records": invalid_count,
                "validity_rate": valid_count / total_count if total_count > 0 else 1.0
            }
            
            # Count specific error types
            if invalid_records.count() > 0:
                error_counts = (
                    invalid_records
                    .select(F.explode(F.split(error_col, "; ")).alias("error"))
                    .groupBy("error")
                    .count()
                    .collect()
                )
                error_counts = {row["error"]: row["count"] for row in error_counts}
        
        # Handle quarantine if path is specified
        if self.quarantine_path and invalid_records.count() > 0:
            (
                invalid_records
                .write
                .mode("append")
                .format("delta")
                .save(self.quarantine_path)
            )
        
        # Return validation result
        return ValidationResult(
            valid_records=valid_records,
            invalid_records=None if drop_invalid else invalid_records,
            validation_time=start_time,
            error_counts=error_counts,
            metrics=metrics
        )

    def get_quarantined_records(
        self,
        spark,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Optional[DataFrame]:
        """Retrieve quarantined records for a given time range.
        
        Args:
            spark: SparkSession
            start_time: Optional start time filter
            end_time: Optional end time filter
            
        Returns:
            DataFrame of quarantined records or None if quarantine is not enabled
        """
        if not self.quarantine_path:
            return None
        
        df = spark.read.format("delta").load(self.quarantine_path)
        
        if start_time:
            df = df.filter(F.col("_quarantine_timestamp") >= start_time)
        if end_time:
            df = df.filter(F.col("_quarantine_timestamp") <= end_time)
            
        return df 