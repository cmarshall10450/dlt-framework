"""Delta Live Tables integration module for the DLT Medallion Framework."""

from typing import Any, Dict, List, Optional, Union, Tuple, Callable
from collections import defaultdict
from datetime import datetime
import json

import dlt
from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col, struct, array, lit

from ..config.models import (
    BaseLayerConfig,
    BronzeConfig,
    SilverConfig,
    GoldConfig,
    Expectation, 
    Metric, 
    ExpectationAction, 
    Layer, 
    DLTTableConfig, 
    GovernanceConfig,
    QuarantineConfig,
    ReferenceConfig,
    SCDConfig
)
from .exceptions import DLTFrameworkError
from .quarantine_manager import QuarantineManager


class DLTIntegration:
    """Handles integration with Delta Live Tables functionality."""

    def __init__(self, config: Optional[BaseLayerConfig] = None):
        """Initialize the DLT integration.
        
        Args:
            config: Layer configuration
        """
        self._config = config
        self._quarantine_manager = None
        
        # Initialize quarantine if config is provided and has quarantine settings
        if config and hasattr(config, 'quarantine') and config.quarantine:
            self.initialize_quarantine(config.quarantine)

    @property
    def config(self) -> Optional[BaseLayerConfig]:
        """Get the current configuration."""
        return self._config
    
    @property
    def quarantine_manager(self) -> Optional[QuarantineManager]:
        """Get the quarantine manager instance."""
        return self._quarantine_manager

    def initialize_quarantine(self, config: QuarantineConfig) -> None:
        """Initialize the quarantine manager with configuration.
        
        Args:
            config: Quarantine configuration
        """
        if config and config.enabled:
            self._quarantine_manager = QuarantineManager(config)
    
    def prepare_table_properties(
        self,
        table_config: DLTTableConfig,
        layer: Layer,
        governance: Optional[GovernanceConfig] = None,
        partition_cols: Optional[List[str]] = None,
        cluster_by: Optional[List[str]] = None,
    ) -> Dict[str, str]:
        """
        Prepare properties for @dlt.table decorator.

        Args:
            table_config: DLT Catalog table configuration
            layer: The layer this table belongs to
            governance: Optional governance configuration
            partition_cols: Optional list of partition columns
            cluster_by: Optional list of clustering columns

        Returns:
            Dictionary of table properties for @dlt.table decorator
        """
        # Basic properties that all layers should have
        table_properties = {
            "layer": layer.value,
            "pipelines.autoOptimize.managed": "true",
            "delta.columnMapping.mode": "name",
            "pipelines.metadata.createdBy": "dlt_framework",
            "pipelines.metadata.createdTimestamp": datetime.now().isoformat(),
        }
        
        # Add Delta properties if specified
        if table_config.properties:
            for prop, value in table_config.properties.items():
                table_properties[str(prop)] = str(value)
                
        # Add governance tags
        if governance:
            if governance.owner:
                table_properties["tag.owner"] = governance.owner
            if governance.steward:
                table_properties["tag.steward"] = governance.steward
            if governance.schema_evolution:
                table_properties["tag.schema_evolution"] = "enabled"
            
            # Add data classification tags
            for column, classification in governance.classification.items():
                table_properties[f"tag.classification.{column}"] = classification.value
                
            # Add all governance tags
            for key, value in governance.tags.items():
                table_properties[f"tag.{key}"] = str(value)
        
        # Layer-specific properties
        if layer == Layer.BRONZE:
            table_properties["delta.enableChangeDataFeed"] = "true"
            
        # Add a JSON comment with the full configuration
        if self._config:
            table_properties["comment"] = json.dumps({
                "description": table_config.description or f"{layer.value.capitalize()} table for {table_config.name}",
                "layer": layer.value,
                "features": self._get_features_for_layer(layer)
            })
        
        return table_properties
    
    def _get_features_for_layer(self, layer: Layer) -> Dict[str, bool]:
        """Get enabled features for a specific layer."""
        features = {}
        
        if layer == Layer.BRONZE and isinstance(self._config, BronzeConfig):
            features = {
                "quarantine": bool(self._config.quarantine),
                "schema_evolution": self._config.schema_evolution,
                "pii_detection": self._config.governance.pii_detection if self._config.governance else False
            }
        elif layer == Layer.SILVER and isinstance(self._config, SilverConfig):
            features = {
                "deduplication": self._config.deduplication,
                "normalization": self._config.normalization,
                "scd": bool(self._config.scd),
                "references": bool(self._config.references)
            }
        elif layer == Layer.GOLD and isinstance(self._config, GoldConfig):
            features = {
                "references": bool(self._config.references),
                "dimensions": bool(self._config.dimensions),
                "verify_pii_masking": self._config.verify_pii_masking
            }
        
        return features
    
    def apply_table_properties(self, table_func: Callable) -> Callable:
        """Apply table properties to a function using dlt.table decorator.
        
        Args:
            table_func: Function to decorate
            
        Returns:
            Decorated function
        """
        if not self._config or not self._config.table:
            return table_func
            
        table_name = self._config.table.name
        storage_location = getattr(self._config.table, 'storage_location', None)
        
        # Determine layer
        layer = None
        if isinstance(self._config, BronzeConfig):
            layer = Layer.BRONZE
        elif isinstance(self._config, SilverConfig):
            layer = Layer.SILVER
        elif isinstance(self._config, GoldConfig):
            layer = Layer.GOLD
            
        if not layer:
            return table_func
            
        # Get fully qualified table name
        full_table_name = f"{self._config.table.catalog}.{self._config.table.schema_name}.{table_name}"
        
        # Get table properties
        table_properties = self.prepare_table_properties(
            self._config.table, 
            layer,
            self._config.governance if hasattr(self._config, 'governance') else None
        )
        
        # Apply DLT table decorator
        return dlt.table(
            name=full_table_name,
            comment=f"{layer.value.capitalize()} layer table for {table_name}",
            table_properties=table_properties,
            temporary=False,
            path=f"{storage_location}/{table_name}" if storage_location else None
        )(table_func)
    
    def add_expectations(self, validations: List[Expectation]) -> Callable:
        """Add DLT expectations to a function.
        
        Args:
            validations: List of expectations to apply
            
        Returns:
            Decorator function that applies the expectations
        """
        if not validations:
            return lambda f: f
            
        def decorator(func: Callable) -> Callable:
            decorated_func = func
            for expectation in validations:
                # Skip quarantine validations as they're handled separately
                if expectation.action != ExpectationAction.QUARANTINE:
                    decorated_func = dlt.expect(
                        name=expectation.name,
                        constraint=expectation.constraint,
                        action="fail" if expectation.action == ExpectationAction.FAIL else "drop",
                        description=expectation.description or f"Validation: {expectation.name}"
                    )(decorated_func)
            return decorated_func
            
        return decorator
    
    def add_quality_metrics(self) -> Callable:
        """Add quality metrics to a function.
        
        Returns:
            Decorator function that applies quality metrics
        """
        metrics = []
        
        # Get metrics from config
        if self._config and hasattr(self._config, 'metrics') and self._config.metrics:
            metrics.extend(self._config.metrics)
            
        # Get metrics from monitoring config
        if self._config and hasattr(self._config, 'monitoring') and self._config.monitoring and self._config.monitoring.metrics:
            metrics.extend(self._config.monitoring.metrics)
            
        if not metrics:
            return lambda f: f
        
        # Create a dictionary of metrics for dlt.expect_all
        metric_dict = {
            metric.name: metric.value 
            for metric in metrics 
            if metric.name and metric.value
        }
        
        return dlt.expect_all(metric_dict) if metric_dict else lambda f: f
        
    def normalize_dataframe(self, df: DataFrame) -> DataFrame:
        """Apply normalization to a dataframe based on config.
        
        Args:
            df: DataFrame to normalize
            
        Returns:
            Normalized DataFrame
        """
        # For now, implement basic normalization logic
        # This would be enhanced with more sophisticated normalization rules
        if not (isinstance(self._config, SilverConfig) and self._config.normalization):
            return df
            
        # Example implementation - convert string columns to lowercase
        for field in df.schema.fields:
            if field.dataType.typeName() == "string":
                df = df.withColumn(field.name, expr(f"lower({field.name})"))
                
        return df
        
    def apply_scd(self, df: DataFrame, scd_config: SCDConfig) -> DataFrame:
        """Apply Slowly Changing Dimension logic.
        
        Args:
            df: DataFrame to apply SCD to
            scd_config: SCD configuration
            
        Returns:
            DataFrame with SCD logic applied
        """
        # This is a placeholder for SCD implementation
        # In a real implementation, this would handle Type 1 and Type 2 SCD logic
        if not scd_config:
            return df
            
        # Simple placeholder - this would need a full implementation
        if scd_config.type == 1:  # Type 1 SCD (overwrite)
            return df
        elif scd_config.type == 2:  # Type 2 SCD (track history)
            # Add effective date if not present
            if scd_config.effective_from not in df.columns:
                df = df.withColumn(scd_config.effective_from, expr("current_timestamp()"))
                
            # Add end date if not present
            if scd_config.effective_to and scd_config.effective_to not in df.columns:
                df = df.withColumn(scd_config.effective_to, lit(None))
                
            # Add current flag if not present
            if scd_config.current_flag and scd_config.current_flag not in df.columns:
                df = df.withColumn(scd_config.current_flag, lit(True))
        
        return df
        
    def apply_references(self, df: DataFrame, references: List[ReferenceConfig]) -> DataFrame:
        """Apply reference data joins.
        
        Args:
            df: DataFrame to apply references to
            references: List of reference configurations
            
        Returns:
            DataFrame with references applied
        """
        # Basic implementation for reference data joins
        if not references:
            return df
            
        # Process each reference configuration
        for ref_config in references:
            # Get reference table
            try:
                ref_table = dlt.read(ref_config.table_name)
                
                # Filter to only needed columns
                ref_columns = list(ref_config.join_keys.values()) + ref_config.lookup_columns
                ref_table = ref_table.select(*ref_columns)
                
                # Prepare join conditions
                join_conditions = []
                for source_col, ref_col in ref_config.join_keys.items():
                    join_conditions.append(df[source_col] == ref_table[ref_col])
                
                # Join with reference table
                join_condition = join_conditions[0]
                for condition in join_conditions[1:]:
                    join_condition = join_condition & condition
                    
                df = df.join(ref_table, join_condition, "left")
            except Exception as e:
                # Log error but continue processing
                print(f"Error joining reference {ref_config.name}: {str(e)}")
                
        return df
        
    def verify_pii_masking(self, df: DataFrame) -> DataFrame:
        """Verify PII fields are properly masked.
        
        Args:
            df: DataFrame to verify
            
        Returns:
            Original DataFrame (potentially with validation errors)
        """
        if not isinstance(self._config, GoldConfig) or not self._config.verify_pii_masking:
            return df
            
        # This would verify that PII fields have been properly masked
        # For now, just return the original DataFrame
        return df 