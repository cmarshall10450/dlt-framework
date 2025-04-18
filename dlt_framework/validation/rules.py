"""Data quality validation rules for the DLT Medallion Framework.

This module provides common validation rules that can be used with the schema validator
to enforce data quality standards at the Bronze layer.
"""

from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql import Column
from pyspark.sql.types import DataType, StringType, NumericType, TimestampType, DateType

@dataclass
class ValidationRule:
    """Base class for validation rules."""
    name: str
    description: str
    
    def validate(self, column: Column) -> Column:
        """Apply the validation rule to a column.
        
        Args:
            column: The PySpark Column to validate
            
        Returns:
            A Column expression that evaluates to True for valid values
        """
        raise NotImplementedError("Subclasses must implement validate()")

@dataclass
class NotNullRule(ValidationRule):
    """Validates that a column does not contain null values."""
    
    def __init__(self):
        super().__init__(
            name="not_null",
            description="Validates that a column does not contain null values"
        )
    
    def validate(self, column: Column) -> Column:
        return column.isNotNull()

@dataclass
class NumericRangeRule(ValidationRule):
    """Validates that numeric values fall within a specified range."""
    min_value: Optional[Union[int, float]] = None
    max_value: Optional[Union[int, float]] = None
    
    def __init__(
        self,
        min_value: Optional[Union[int, float]] = None,
        max_value: Optional[Union[int, float]] = None
    ):
        super().__init__(
            name="numeric_range",
            description=f"Validates numeric values between {min_value} and {max_value}"
        )
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, column: Column) -> Column:
        conditions = []
        
        if self.min_value is not None:
            conditions.append(column >= self.min_value)
        if self.max_value is not None:
            conditions.append(column <= self.max_value)
            
        if not conditions:
            return F.lit(True)
            
        return F.reduce(lambda x, y: x & y, conditions)

@dataclass
class StringLengthRule(ValidationRule):
    """Validates string length is within specified bounds."""
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    
    def __init__(
        self,
        min_length: Optional[int] = None,
        max_length: Optional[int] = None
    ):
        super().__init__(
            name="string_length",
            description=f"Validates string length between {min_length} and {max_length}"
        )
        self.min_length = min_length
        self.max_length = max_length
    
    def validate(self, column: Column) -> Column:
        conditions = []
        length = F.length(column)
        
        if self.min_length is not None:
            conditions.append(length >= self.min_length)
        if self.max_length is not None:
            conditions.append(length <= self.max_length)
            
        if not conditions:
            return F.lit(True)
            
        return F.reduce(lambda x, y: x & y, conditions)

@dataclass
class RegexRule(ValidationRule):
    """Validates string values match a regex pattern."""
    pattern: str
    
    def __init__(self, pattern: str, name: Optional[str] = None):
        super().__init__(
            name=name or "regex",
            description=f"Validates string values match pattern: {pattern}"
        )
        self.pattern = pattern
    
    def validate(self, column: Column) -> Column:
        return column.rlike(self.pattern)

@dataclass
class DateTimeRangeRule(ValidationRule):
    """Validates datetime values fall within a specified range."""
    min_datetime: Optional[datetime] = None
    max_datetime: Optional[datetime] = None
    
    def __init__(
        self,
        min_datetime: Optional[datetime] = None,
        max_datetime: Optional[datetime] = None
    ):
        super().__init__(
            name="datetime_range",
            description=f"Validates datetime between {min_datetime} and {max_datetime}"
        )
        self.min_datetime = min_datetime
        self.max_datetime = max_datetime
    
    def validate(self, column: Column) -> Column:
        conditions = []
        
        if self.min_datetime is not None:
            conditions.append(column >= self.min_datetime)
        if self.max_datetime is not None:
            conditions.append(column <= self.max_datetime)
            
        if not conditions:
            return F.lit(True)
            
        return F.reduce(lambda x, y: x & y, conditions)

# Common regex patterns
EMAIL_PATTERN = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
PHONE_PATTERN = r'^\+?1?\d{9,15}$'
URL_PATTERN = r'^(http|https)://[^\s/$.?#].[^\s]*$'
IPV4_PATTERN = r'^(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$'

# Pre-built common rules
EMAIL_RULE = RegexRule(EMAIL_PATTERN, "email")
PHONE_RULE = RegexRule(PHONE_PATTERN, "phone")
URL_RULE = RegexRule(URL_PATTERN, "url")
IPV4_RULE = RegexRule(IPV4_PATTERN, "ipv4")

class RuleSet:
    """A collection of validation rules to be applied together."""
    
    def __init__(self, rules: Optional[List[ValidationRule]] = None):
        """Initialize a rule set.
        
        Args:
            rules: Optional list of validation rules
        """
        self.rules = rules or []
    
    def add_rule(self, rule: ValidationRule) -> None:
        """Add a rule to the set."""
        self.rules.append(rule)
    
    def validate(self, column: Column) -> Column:
        """Apply all rules in the set to a column.
        
        Returns a Column expression that is True only if all rules pass.
        """
        if not self.rules:
            return F.lit(True)
            
        conditions = [rule.validate(column) for rule in self.rules]
        return F.reduce(lambda x, y: x & y, conditions)

def create_rule_set(
    not_null: bool = False,
    min_value: Optional[Union[int, float]] = None,
    max_value: Optional[Union[int, float]] = None,
    min_length: Optional[int] = None,
    max_length: Optional[int] = None,
    regex_pattern: Optional[str] = None,
    min_datetime: Optional[datetime] = None,
    max_datetime: Optional[datetime] = None
) -> RuleSet:
    """Create a RuleSet with common validation rules.
    
    Args:
        not_null: Whether to include a NotNullRule
        min_value: Minimum numeric value
        max_value: Maximum numeric value
        min_length: Minimum string length
        max_length: Maximum string length
        regex_pattern: Regex pattern for string validation
        min_datetime: Minimum datetime value
        max_datetime: Maximum datetime value
        
    Returns:
        A RuleSet containing the specified rules
    """
    rules = []
    
    if not_null:
        rules.append(NotNullRule())
    
    if min_value is not None or max_value is not None:
        rules.append(NumericRangeRule(min_value, max_value))
    
    if min_length is not None or max_length is not None:
        rules.append(StringLengthRule(min_length, max_length))
    
    if regex_pattern is not None:
        rules.append(RegexRule(regex_pattern))
    
    if min_datetime is not None or max_datetime is not None:
        rules.append(DateTimeRangeRule(min_datetime, max_datetime))
    
    return RuleSet(rules) 