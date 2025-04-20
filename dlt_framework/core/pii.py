"""PII detection and handling for the DLT Medallion Framework."""

from typing import Dict, List, Optional, Protocol
from abc import ABC, abstractmethod

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from dlt_framework.config import MaskingStrategy
from dlt_framework.validation.gdpr import GDPRValidator, GDPRField, create_gdpr_validator


class PIIDetector(ABC):
    """Abstract base class for PII detection and handling."""

    @abstractmethod
    def detect_and_handle(self, df: DataFrame) -> DataFrame:
        """Detect and handle PII in the DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with PII handled according to configuration
        """
        pass

    @abstractmethod
    def get_pii_columns(self, df: DataFrame) -> List[str]:
        """Get list of columns containing PII.
        
        Args:
            df: Input DataFrame
            
        Returns:
            List of column names containing PII
        """
        pass

    @abstractmethod
    def apply_masking(self, df: DataFrame, columns: List[str], strategy: MaskingStrategy) -> DataFrame:
        """Apply masking to specified columns.
        
        Args:
            df: Input DataFrame
            columns: List of columns to mask
            strategy: Masking strategy to apply
            
        Returns:
            DataFrame with masked columns
        """
        pass


class DefaultPIIDetector(PIIDetector):
    """Default implementation of PII detection and handling using GDPRValidator."""

    def __init__(
        self,
        pii_columns: Optional[List[str]] = None,
        masking_strategies: Optional[Dict[str, MaskingStrategy]] = None,
        patterns_file: Optional[str] = None
    ):
        """Initialize the PII detector.
        
        Args:
            pii_columns: Optional list of known PII columns
            masking_strategies: Optional mapping of columns to masking strategies
            patterns_file: Optional path to custom PII patterns YAML file
        """
        self.pii_columns = pii_columns or []
        self.masking_strategies = masking_strategies or {}
        
        # Convert masking strategies to GDPR field configurations
        pii_fields = []
        for col in self.pii_columns:
            strategy = self.masking_strategies.get(col, MaskingStrategy.HASH)
            pii_fields.append({
                "name": col,
                "pii_type": "generic_pii",  # Default type for pre-configured columns
                "requires_consent": False,  # Default to not requiring consent
                "masking_strategy": strategy.value
            })
            
        # Initialize GDPR validator
        self.validator = create_gdpr_validator(
            pii_fields=pii_fields,
            patterns_file=patterns_file
        )

    def detect_and_handle(self, df: DataFrame) -> DataFrame:
        """Detect and handle PII in the DataFrame using GDPR validation."""
        # Detect additional PII columns
        pii_detection = self.validator.detect_pii(df)
        
        # Get all columns that need masking
        columns_to_mask = set(self.pii_columns)
        for detected_cols in pii_detection.values():
            columns_to_mask.update(detected_cols)
            
        # Apply masking using GDPR validator
        return self.validator.mask_pii(df, list(columns_to_mask))

    def get_pii_columns(self, df: DataFrame) -> List[str]:
        """Get list of columns containing PII using GDPR detection."""
        # Start with configured PII columns
        all_pii_columns = set(self.pii_columns)
        
        # Add detected PII columns
        pii_detection = self.validator.detect_pii(df)
        for detected_cols in pii_detection.values():
            all_pii_columns.update(detected_cols)
                
        return list(all_pii_columns)

    def apply_masking(self, df: DataFrame, columns: List[str], strategy: MaskingStrategy) -> DataFrame:
        """Apply masking to specified columns using GDPR masking."""
        # Create temporary GDPR fields for the columns
        temp_fields = []
        for col in columns:
            temp_fields.append({
                "name": col,
                "pii_type": "generic_pii",
                "masking_strategy": strategy.value
            })
            
        # Create temporary validator for these columns
        temp_validator = create_gdpr_validator(pii_fields=temp_fields)
        
        # Apply masking
        return temp_validator.mask_pii(df, columns) 