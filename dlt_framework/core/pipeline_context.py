"""Pipeline context management for DLT Framework.

This module provides context management for pipeline executions, ensuring consistent
metadata across all operations within a pipeline run.
"""
from datetime import datetime
import uuid
from typing import Optional, Dict, Any
from dataclasses import dataclass, field


@dataclass
class PipelineContext:
    """Manages context information for a pipeline execution.
    
    This class ensures consistent run IDs and metadata across all operations
    within a single pipeline execution.
    
    Attributes:
        run_id: Unique identifier for this pipeline run
        start_time: Timestamp when the pipeline run started
        metadata: Additional metadata about the pipeline run
    """
    run_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    start_time: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def create(cls, metadata: Optional[Dict[str, Any]] = None) -> 'PipelineContext':
        """Creates a new pipeline context with optional metadata.
        
        Args:
            metadata: Optional dictionary of metadata to associate with this run
            
        Returns:
            A new PipelineContext instance
        """
        return cls(metadata=metadata or {})
    
    def get_run_metadata(self) -> Dict[str, Any]:
        """Gets all metadata associated with this pipeline run.
        
        Returns:
            Dictionary containing run_id, start_time, and any custom metadata
        """
        return {
            "run_id": self.run_id,
            "start_time": self.start_time.isoformat(),
            **self.metadata
        }
    
    def add_metadata(self, key: str, value: Any) -> None:
        """Adds or updates metadata for this pipeline run.
        
        Args:
            key: Metadata key
            value: Metadata value
        """
        self.metadata[key] = value 