"""
This module provides reference data management functionality for the DLT framework.
"""

from typing import Any, Dict, List, Optional, Union
from datetime import datetime, timedelta

import dlt
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

from dlt_framework.config import SilverConfig, GoldConfig, ReferenceConfig


class ReferenceManager:
    """Manages access to reference data for transformations."""
    
    def __init__(self, config: Union[SilverConfig, GoldConfig]):
        """Initialize the reference manager.
        
        Args:
            config: Layer configuration containing reference definitions
        """
        self.config = config
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._cache_timestamps: Dict[str, datetime] = {}
    
    def _get_config(self, name: str) -> ReferenceConfig:
        """Get reference configuration by name.
        
        Args:
            name: Logical name of the reference
            
        Returns:
            Reference configuration
            
        Raises:
            ValueError: If reference not found
        """
        for ref in self.config.references:
            if ref.name == name:
                return ref
        raise ValueError(f"Reference '{name}' not found in configuration")
    
    def _should_refresh_cache(self, name: str) -> bool:
        """Check if cache needs refreshing.
        
        Args:
            name: Reference name
            
        Returns:
            True if cache should be refreshed
        """
        ref_config = self._get_config(name)
        if ref_config.cache_ttl is None:
            return True
            
        last_refresh = self._cache_timestamps.get(name)
        if last_refresh is None:
            return True
            
        ttl = timedelta(seconds=ref_config.cache_ttl)
        return datetime.now() - last_refresh > ttl
    
    def _load_lookup_cache(self, name: str) -> Dict[str, Any]:
        """Load reference data into lookup cache.
        
        Args:
            name: Reference name
            
        Returns:
            Dictionary mapping keys to reference data
        """
        ref_config = self._get_config(name)
        df = dlt.read(ref_config.table_name)
        
        # Convert DataFrame to dictionary for fast lookups
        lookup_dict = {}
        for row in df.select(ref_config.lookup_columns).collect():
            key = tuple(row[k] for k in ref_config.join_keys.values())
            lookup_dict[key] = row.asDict()
        
        self._cache[name] = lookup_dict
        self._cache_timestamps[name] = datetime.now()
        return lookup_dict
    
    def get_reference_df(self, name: str) -> DataFrame:
        """Get reference data as a DataFrame.
        
        Args:
            name: Reference name
            
        Returns:
            Reference DataFrame
            
        Raises:
            ValueError: If reference not found
        """
        ref_config = self._get_config(name)
        return dlt.read(ref_config.table_name)
    
    def lookup(self, name: str, key_values: List[Any]) -> Optional[Dict[str, Any]]:
        """Look up reference data by key values.
        
        Args:
            name: Reference name
            key_values: List of key values matching join keys order
            
        Returns:
            Dictionary of reference values or None if not found
            
        Raises:
            ValueError: If reference not found
        """
        if name not in self._cache or self._should_refresh_cache(name):
            self._load_lookup_cache(name)
        
        key_tuple = tuple(key_values)
        return self._cache[name].get(key_tuple)
    
    def broadcast(self, name: str) -> DataFrame:
        """Get reference data as a broadcast variable.
        
        Args:
            name: Reference name
            
        Returns:
            Broadcast reference DataFrame
            
        Raises:
            ValueError: If reference not found
        """
        ref_config = self._get_config(name)
        df = dlt.read(ref_config.table_name)
        return F.broadcast(df) 