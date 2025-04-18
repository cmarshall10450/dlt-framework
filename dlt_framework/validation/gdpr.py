"""GDPR compliance functionality for the DLT Medallion Framework.

This module provides tools for:
1. PII detection and classification
2. GDPR-specific validation rules
3. Data masking and anonymization
4. Consent tracking and validation
"""

from typing import Dict, List, Optional, Set, Union, Any
from dataclasses import dataclass
from datetime import datetime
import os
from pathlib import Path

import yaml
import pyspark.sql.functions as F
from pyspark.sql import Column, DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from .rules import ValidationRule, RegexRule, RuleSet

@dataclass
class PIIPattern:
    """Configuration for a PII pattern."""
    pattern: str
    description: str
    requires_consent: bool
    default_masking: str

@dataclass
class GDPRField:
    """Configuration for a GDPR-protected field."""
    name: str
    pii_type: str
    requires_consent: bool = True
    retention_period: Optional[int] = None  # in days
    masking_strategy: Optional[str] = None  # if None, uses default from pattern config

class GDPRValidator:
    """Validates GDPR compliance for DataFrame columns."""
    
    def __init__(self, fields: List[GDPRField], patterns_file: Optional[str] = None):
        """Initialize with GDPR field configurations.
        
        Args:
            fields: List of GDPRField configurations
            patterns_file: Optional path to patterns YAML file. If None, uses default UK/EU patterns
        """
        self.fields = {field.name: field for field in fields}
        self.patterns = self._load_patterns(patterns_file)
        self._build_rules()
    
    def _load_patterns(self, patterns_file: Optional[str] = None) -> Dict[str, PIIPattern]:
        """Load PII patterns from YAML configuration.
        
        Args:
            patterns_file: Path to patterns YAML file. If None, uses default UK/EU patterns
            
        Returns:
            Dictionary mapping pattern names to PIIPattern objects
        """
        if patterns_file is None:
            patterns_file = os.path.join(
                os.path.dirname(__file__),
                'patterns',
                'uk_eu_pii_patterns.yaml'
            )
            
        with open(patterns_file, 'r') as f:
            config = yaml.safe_load(f)
            
        patterns = {}
        for category in config.values():
            for name, details in category.items():
                patterns[name] = PIIPattern(
                    pattern=details['pattern'],
                    description=details['description'],
                    requires_consent=details['requires_consent'],
                    default_masking=details['default_masking']
                )
                
        return patterns
    
    def _build_rules(self) -> None:
        """Build validation rules for each GDPR field."""
        self.rules = {}
        for name, field in self.fields.items():
            rule_set = RuleSet()
            if field.pii_type in self.patterns:
                pattern = self.patterns[field.pii_type]
                rule_set.add_rule(RegexRule(
                    pattern.pattern,
                    f"valid_{field.pii_type}"
                ))
                # Use pattern's consent requirement if not specified in field
                if field.requires_consent is None:
                    field.requires_consent = pattern.requires_consent
                # Use pattern's default masking strategy if not specified in field
                if field.masking_strategy is None:
                    field.masking_strategy = pattern.default_masking
            self.rules[name] = rule_set
    
    def validate_consent(self, df: DataFrame, consent_column: str) -> DataFrame:
        """Validate that consent exists for PII fields.
        
        Args:
            df: Input DataFrame
            consent_column: Column containing consent information
            
        Returns:
            DataFrame with consent validation results
        """
        consent_fields = [
            name for name, field in self.fields.items()
            if field.requires_consent
        ]
        
        # Add validation columns
        for field in consent_fields:
            df = df.withColumn(
                f"{field}_has_consent",
                F.col(consent_column).isNotNull() & 
                (F.col(consent_column) == True)
            )
        
        return df
    
    def mask_pii(self, df: DataFrame, columns: Optional[List[str]] = None) -> DataFrame:
        """Apply masking to PII fields based on their masking strategy.
        
        Args:
            df: Input DataFrame
            columns: Optional list of columns to mask. If None, masks all PII fields.
            
        Returns:
            DataFrame with masked PII fields
        """
        if columns is None:
            columns = list(self.fields.keys())
            
        for col in columns:
            if col not in self.fields:
                continue
                
            field = self.fields[col]
            strategy = field.masking_strategy
            if strategy == 'hash':
                df = df.withColumn(col, F.sha2(F.col(col), 256))
            elif strategy == 'truncate':
                df = df.withColumn(col, F.substring(F.col(col), 1, 4))
            elif strategy == 'redact':
                df = df.withColumn(col, F.lit('REDACTED'))
                
        return df
    
    def detect_pii(self, df: DataFrame) -> Dict[str, Set[str]]:
        """Detect potential PII fields in a DataFrame.
        
        Args:
            df: Input DataFrame
            
        Returns:
            Dictionary mapping PII types to column names that may contain that type of PII
        """
        pii_columns = {pii_type: set() for pii_type in self.patterns}
        
        for col in df.columns:
            # Skip already classified columns
            if col in self.fields:
                continue
                
            # Check each column against PII patterns
            for pii_type, pattern in self.patterns.items():
                # Sample the column to check for matches
                matches = df.select(
                    F.sum(F.col(col).rlike(pattern.pattern)).alias('matches')
                ).collect()[0]['matches']
                
                if matches > 0:
                    pii_columns[pii_type].add(col)
                    
        return pii_columns
    
    def add_gdpr_metadata(self, df: DataFrame) -> DataFrame:
        """Add GDPR-related metadata to the DataFrame.
        
        This includes:
        - PII classification
        - Required consent flags
        - Retention periods
        - Processing purposes
        
        Args:
            df: Input DataFrame
            
        Returns:
            DataFrame with GDPR metadata columns
        """
        metadata_df = df
        
        # Add PII type indicators
        for col in df.columns:
            if col in self.fields:
                field = self.fields[col]
                pattern = self.patterns.get(field.pii_type)
                
                metadata_df = metadata_df.withColumn(
                    f"{col}_pii_type",
                    F.lit(field.pii_type)
                )
                
                if pattern:
                    metadata_df = metadata_df.withColumn(
                        f"{col}_requires_consent",
                        F.lit(field.requires_consent)
                    )
                    metadata_df = metadata_df.withColumn(
                        f"{col}_masking_strategy",
                        F.lit(field.masking_strategy or pattern.default_masking)
                    )
                
                if field.retention_period:
                    metadata_df = metadata_df.withColumn(
                        f"{col}_retention_date",
                        F.date_add(
                            F.current_date(),
                            field.retention_period
                        )
                    )
        
        return metadata_df

def create_gdpr_validator(
    pii_fields: List[Dict[str, Any]],
    patterns_file: Optional[str] = None
) -> GDPRValidator:
    """Create a GDPRValidator from a configuration dictionary.
    
    Args:
        pii_fields: List of dictionaries containing PII field configurations
        patterns_file: Optional path to patterns YAML file
        
    Returns:
        Configured GDPRValidator instance
        
    Example:
        ```python
        validator = create_gdpr_validator([
            {
                "name": "email",
                "pii_type": "email",
                "requires_consent": True,
                "retention_period": 365,
                "masking_strategy": "hash"
            },
            {
                "name": "phone",
                "pii_type": "phone_uk",
                "requires_consent": True,
                "masking_strategy": "truncate"
            }
        ])
        ```
    """
    fields = [GDPRField(**field_config) for field_config in pii_fields]
    return GDPRValidator(fields, patterns_file) 