# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Chunk, Embed. Healthcare Data, AI-Ready with RAG.
# https://github.com/PulsePipe/pulsepipe
#
# Copyright (C) 2025 Amir Abrams
#
# This file is part of PulsePipe and is licensed under the GNU Affero General 
# Public License v3.0 (AGPL-3.0). A full copy of this license can be found in 
# the LICENSE file at the root of this repository or online at:
# https://www.gnu.org/licenses/agpl-3.0.html
#
# PulsePipe is distributed WITHOUT ANY WARRANTY; without even the implied 
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# We welcome community contributions — if you make it better, 
# share it back. The whole healthcare ecosystem wins.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

# src/pulsepipe/persistence/database/dialect.py

"""
Database SQL dialect abstraction interface.

Provides database-specific SQL generation and data type mapping.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple, Union
from datetime import datetime


class DatabaseDialect(ABC):
    """
    Abstract base class for database SQL dialects.
    
    Handles database-specific SQL syntax, data types, and query generation.
    This allows the same business logic to work across different SQL databases.
    """
    
    @abstractmethod
    def get_pipeline_run_insert(self) -> str:
        """Get database operation for inserting a pipeline run record."""
        pass
    
    @abstractmethod
    def get_pipeline_run_update(self) -> str:
        """Get database operation for updating a pipeline run record."""
        pass
    
    @abstractmethod
    def get_pipeline_run_select(self) -> str:
        """Get database operation for selecting a pipeline run by ID."""
        pass
    
    @abstractmethod
    def get_pipeline_runs_list(self) -> str:
        """Get database operation for listing recent pipeline runs."""
        pass
    
    @abstractmethod
    def get_pipeline_run_count_update(self) -> str:
        """Get database operation for updating pipeline run counts."""
        pass
    
    @abstractmethod
    def get_recent_pipeline_runs(self, limit: int = 10) -> str:
        """Get database operation for recent pipeline runs."""
        pass
    
    @abstractmethod
    def get_ingestion_stat_insert(self) -> str:
        """Get database operation for inserting an ingestion statistic."""
        pass
    
    @abstractmethod
    def get_failed_record_insert(self) -> str:
        """Get database operation for inserting a failed record."""
        pass
    
    @abstractmethod
    def get_audit_event_insert(self) -> str:
        """Get database operation for inserting an audit event."""
        pass
    
    @abstractmethod
    def get_quality_metric_insert(self) -> str:
        """Get database operation for inserting a quality metric."""
        pass
    
    @abstractmethod
    def get_performance_metric_insert(self) -> str:
        """Get database operation for inserting a performance metric."""
        pass
    
    @abstractmethod
    def get_chunking_stat_insert(self) -> str:
        """Get database operation for inserting a chunking statistic."""
        pass
    
    @abstractmethod
    def get_deid_stat_insert(self) -> str:
        """Get database operation for inserting a de-identification statistic."""
        pass
    
    @abstractmethod
    def get_embedding_stat_insert(self) -> str:
        """Get database operation for inserting an embedding statistic."""
        pass
    
    @abstractmethod
    def get_vector_db_stat_insert(self) -> str:
        """Get database operation for inserting a vector database statistic."""
        pass
    
    @abstractmethod
    def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                 start_date: Optional[datetime] = None,
                                 end_date: Optional[datetime] = None) -> Tuple[str, List[Any]]:
        """
        Get database operation for ingestion summary with optional filters.
        
        Returns:
            Tuple of (DB Operation string, parameters list)
        """
        pass
    
    @abstractmethod
    def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Tuple[str, List[Any]]:
        """
        Get database operation for quality summary with optional filters.
        
        Returns:
            Tuple of (DB Operation string, parameters list)
        """
        pass
    
    @abstractmethod
    def get_cleanup(self, cutoff_date: datetime) -> List[Tuple[str, List[Any]]]:
        """
        Get database operation statements for cleaning up old data.
        
        Returns:
            List of (DB Operation string, parameters) tuples in execution order
        """
        pass
    
    @abstractmethod
    def format_datetime(self, dt: datetime) -> str:
        """
        Format datetime for database storage.
        
        Args:
            dt: Datetime to format
            
        Returns:
            Database-specific datetime string
        """
        pass
    
    @abstractmethod
    def parse_datetime(self, dt_str: str) -> datetime:
        """
        Parse datetime from database storage format.
        
        Args:
            dt_str: Database datetime string
            
        Returns:
            Parsed datetime object
        """
        pass
    
    @abstractmethod
    def get_auto_increment_syntax(self) -> str:
        """
        Get database-specific auto-increment syntax.
        
        Returns:
            Auto-increment column definition (e.g., "AUTOINCREMENT", "SERIAL")
        """
        pass
    
    @abstractmethod
    def get_json_column_type(self) -> str:
        """
        Get database-specific JSON column type.
        
        Returns:
            JSON column type definition (e.g., "TEXT", "JSONB")
        """
        pass
    
    @abstractmethod
    def serialize_json(self, data: Any) -> str:
        """
        Serialize data to JSON for database storage.
        
        Args:
            data: Data to serialize
            
        Returns:
            JSON string for database storage
        """
        pass
    
    @abstractmethod
    def deserialize_json(self, json_str: Optional[str]) -> Any:
        """
        Deserialize JSON from database storage.
        
        Args:
            json_str: JSON string from database
            
        Returns:
            Deserialized data or None
        """
        pass
    
    @abstractmethod
    def escape_identifier(self, identifier: str) -> str:
        """
        Escape database identifier (table name, column name).
        
        Args:
            identifier: Identifier to escape
            
        Returns:
            Escaped identifier
        """
        pass
    
    @abstractmethod
    def get_limit_syntax(self, limit: int, offset: Optional[int] = None) -> str:
        """
        Get database-specific LIMIT/OFFSET syntax.
        
        Args:
            limit: Maximum number of rows
            offset: Optional row offset
            
        Returns:
            LIMIT clause string
        """
        pass
    
    def get_database_type(self) -> str:
        """
        Get the database type name.
        
        Returns:
            Database type (e.g., "sqlite", "postgresql", "mongodb")
        """
        return self.__class__.__name__.lower().replace('dialect', '').replace('adapter', '')
    
    def supports_feature(self, feature: str) -> bool:
        """
        Check if database supports a specific feature.
        
        Args:
            feature: Feature name (e.g., "transactions", "json", "full_text_search")
            
        Returns:
            True if feature is supported
        """
        # Default implementation - subclasses can override
        common_features = {"transactions"}
        return feature in common_features