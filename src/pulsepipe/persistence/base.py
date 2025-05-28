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

# src/pulsepipe/persistence/base.py

"""
Abstract base classes for persistence layer.

Defines the interfaces for multi-database persistence support including
SQL and NoSQL databases for healthcare data tracking and analytics.
"""

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, Any, Optional, List
from dataclasses import dataclass

from .models import ProcessingStatus, ErrorCategory


@dataclass
class PipelineRunSummary:
    """Summary data for a pipeline run."""
    id: str
    name: str
    started_at: datetime
    completed_at: Optional[datetime]
    status: str
    total_records: int
    successful_records: int
    failed_records: int
    skipped_records: int
    error_message: Optional[str] = None


@dataclass
class IngestionStat:
    """Individual ingestion statistics record."""
    id: Optional[str]
    pipeline_run_id: str
    stage_name: str
    file_path: Optional[str]
    record_id: Optional[str]
    record_type: Optional[str]
    status: ProcessingStatus
    error_category: Optional[ErrorCategory]
    error_message: Optional[str]
    error_details: Optional[Dict[str, Any]]
    processing_time_ms: Optional[int]
    record_size_bytes: Optional[int]
    data_source: Optional[str]
    timestamp: datetime


@dataclass
class QualityMetric:
    """Data quality metrics for a record or batch."""
    id: Optional[str]
    pipeline_run_id: str
    record_id: Optional[str]
    record_type: Optional[str]
    completeness_score: Optional[float]
    consistency_score: Optional[float]
    validity_score: Optional[float]
    accuracy_score: Optional[float]
    overall_score: Optional[float]
    missing_fields: Optional[List[str]]
    invalid_fields: Optional[List[str]]
    outlier_fields: Optional[List[str]]
    quality_issues: Optional[List[str]]
    metrics_details: Optional[Dict[str, Any]]
    sampled: bool = False
    timestamp: Optional[datetime] = None


class BasePersistenceProvider(ABC):
    """
    Abstract base class for persistence providers.
    
    Defines the interface that all persistence implementations must follow,
    supporting both SQL and NoSQL databases for healthcare data tracking.
    """
    
    @abstractmethod
    async def connect(self) -> None:
        """Establish connection to the database."""
        pass
    
    @abstractmethod
    async def disconnect(self) -> None:
        """Close connection to the database."""
        pass
    
    @abstractmethod
    async def initialize_schema(self) -> None:
        """Initialize database schema/collections."""
        pass
    
    @abstractmethod
    async def health_check(self) -> bool:
        """Check if the database connection is healthy."""
        pass
    
    # Pipeline Run Management
    
    @abstractmethod
    async def start_pipeline_run(self, run_id: str, name: str, 
                               config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        """Record the start of a pipeline run."""
        pass
    
    @abstractmethod
    async def complete_pipeline_run(self, run_id: str, status: str = "completed", 
                                  error_message: Optional[str] = None) -> None:
        """Mark a pipeline run as completed."""
        pass
    
    @abstractmethod
    async def update_pipeline_run_counts(self, run_id: str, total: int = 0, 
                                       successful: int = 0, failed: int = 0, 
                                       skipped: int = 0) -> None:
        """Update record counts for a pipeline run."""
        pass
    
    @abstractmethod
    async def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        """Get pipeline run summary by ID."""
        pass
    
    # Ingestion Statistics
    
    @abstractmethod
    async def record_ingestion_stat(self, stat: IngestionStat) -> str:
        """Record an ingestion statistic. Returns the record ID."""
        pass
    
    @abstractmethod
    async def record_failed_record(self, ingestion_stat_id: str, original_data: str,
                                 failure_reason: str, normalized_data: Optional[str] = None,
                                 stack_trace: Optional[str] = None) -> str:
        """Store a complete failed record for analysis. Returns the record ID."""
        pass
    
    # Quality Metrics
    
    @abstractmethod
    async def record_quality_metric(self, metric: QualityMetric) -> str:
        """Record a quality metric. Returns the record ID."""
        pass
    
    # Audit Events
    
    @abstractmethod
    async def record_audit_event(self, pipeline_run_id: str, event_type: str, 
                                stage_name: str, message: str, event_level: str = "INFO",
                                record_id: Optional[str] = None, 
                                details: Optional[Dict[str, Any]] = None,
                                correlation_id: Optional[str] = None) -> str:
        """Record an audit event. Returns the record ID."""
        pass
    
    # Performance Metrics
    
    @abstractmethod
    async def record_performance_metric(self, pipeline_run_id: str, stage_name: str,
                                      started_at: datetime, completed_at: datetime,
                                      records_processed: int = 0, 
                                      memory_usage_mb: Optional[float] = None,
                                      cpu_usage_percent: Optional[float] = None,
                                      bottleneck_indicator: Optional[str] = None) -> str:
        """Record performance metrics for a pipeline stage. Returns the record ID."""
        pass
    
    # System Metrics
    
    @abstractmethod
    async def record_system_metric(self, pipeline_run_id: str, hostname: Optional[str] = None,
                                  os_name: Optional[str] = None, os_version: Optional[str] = None,
                                  python_version: Optional[str] = None, 
                                  cpu_model: Optional[str] = None,
                                  cpu_cores: Optional[int] = None, 
                                  memory_total_gb: Optional[float] = None,
                                  gpu_available: bool = False, 
                                  gpu_model: Optional[str] = None,
                                  additional_info: Optional[Dict[str, Any]] = None) -> str:
        """Record system metrics. Returns the record ID."""
        pass
    
    # Analytics and Reporting
    
    @abstractmethod
    async def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get ingestion statistics summary."""
        pass
    
    @abstractmethod
    async def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Dict[str, Any]:
        """Get quality metrics summary."""
        pass
    
    @abstractmethod
    async def get_recent_pipeline_runs(self, limit: int = 10) -> List[PipelineRunSummary]:
        """Get recent pipeline runs."""
        pass
    
    @abstractmethod
    async def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Clean up old tracking data. Returns number of records deleted."""
        pass


class BaseTrackingRepository(ABC):
    """
    Abstract base class for tracking repositories.
    
    Provides a high-level interface for data intelligence operations
    while abstracting away the specific database implementation.
    """
    
    def __init__(self, provider: BasePersistenceProvider):
        """
        Initialize tracking repository.
        
        Args:
            provider: Persistence provider implementation
        """
        self.provider = provider
    
    async def connect(self) -> None:
        """Establish connection to the database."""
        await self.provider.connect()
    
    async def disconnect(self) -> None:
        """Close connection to the database."""
        await self.provider.disconnect()
    
    async def initialize(self) -> None:
        """Initialize the repository (connect and setup schema)."""
        await self.provider.connect()
        await self.provider.initialize_schema()
    
    async def health_check(self) -> bool:
        """Check if the repository is healthy."""
        return await self.provider.health_check()
    
    # Forward all operations to the provider
    
    async def start_pipeline_run(self, run_id: str, name: str, 
                               config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        """Record the start of a pipeline run."""
        return await self.provider.start_pipeline_run(run_id, name, config_snapshot)
    
    async def complete_pipeline_run(self, run_id: str, status: str = "completed", 
                                  error_message: Optional[str] = None) -> None:
        """Mark a pipeline run as completed."""
        return await self.provider.complete_pipeline_run(run_id, status, error_message)
    
    async def update_pipeline_run_counts(self, run_id: str, total: int = 0, 
                                       successful: int = 0, failed: int = 0, 
                                       skipped: int = 0) -> None:
        """Update record counts for a pipeline run."""
        return await self.provider.update_pipeline_run_counts(run_id, total, successful, failed, skipped)
    
    async def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        """Get pipeline run summary by ID."""
        return await self.provider.get_pipeline_run(run_id)
    
    async def record_ingestion_stat(self, stat: IngestionStat) -> str:
        """Record an ingestion statistic."""
        return await self.provider.record_ingestion_stat(stat)
    
    async def record_failed_record(self, ingestion_stat_id: str, original_data: str,
                                 failure_reason: str, normalized_data: Optional[str] = None,
                                 stack_trace: Optional[str] = None) -> str:
        """Store a complete failed record for analysis."""
        return await self.provider.record_failed_record(
            ingestion_stat_id, original_data, failure_reason, normalized_data, stack_trace
        )
    
    async def record_quality_metric(self, metric: QualityMetric) -> str:
        """Record a quality metric."""
        return await self.provider.record_quality_metric(metric)
    
    async def record_audit_event(self, pipeline_run_id: str, event_type: str, 
                               stage_name: str, message: str, event_level: str = "INFO",
                               record_id: Optional[str] = None, 
                               details: Optional[Dict[str, Any]] = None,
                               correlation_id: Optional[str] = None) -> str:
        """Record an audit event."""
        return await self.provider.record_audit_event(
            pipeline_run_id, event_type, stage_name, message, event_level, 
            record_id, details, correlation_id
        )
    
    async def record_performance_metric(self, pipeline_run_id: str, stage_name: str,
                                      started_at: datetime, completed_at: datetime,
                                      records_processed: int = 0, 
                                      memory_usage_mb: Optional[float] = None,
                                      cpu_usage_percent: Optional[float] = None,
                                      bottleneck_indicator: Optional[str] = None) -> str:
        """Record performance metrics for a pipeline stage."""
        return await self.provider.record_performance_metric(
            pipeline_run_id, stage_name, started_at, completed_at, 
            records_processed, memory_usage_mb, cpu_usage_percent, bottleneck_indicator
        )
    
    async def record_system_metric(self, pipeline_run_id: str, hostname: Optional[str] = None,
                                  os_name: Optional[str] = None, os_version: Optional[str] = None,
                                  python_version: Optional[str] = None, 
                                  cpu_model: Optional[str] = None,
                                  cpu_cores: Optional[int] = None, 
                                  memory_total_gb: Optional[float] = None,
                                  gpu_available: bool = False, 
                                  gpu_model: Optional[str] = None,
                                  additional_info: Optional[Dict[str, Any]] = None) -> str:
        """Record system metrics."""
        return await self.provider.record_system_metric(
            pipeline_run_id, hostname, os_name, os_version, python_version,
            cpu_model, cpu_cores, memory_total_gb, gpu_available, gpu_model, additional_info
        )
    
    async def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """Get ingestion statistics summary."""
        return await self.provider.get_ingestion_summary(pipeline_run_id, start_date, end_date)
    
    async def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Dict[str, Any]:
        """Get quality metrics summary."""
        return await self.provider.get_quality_summary(pipeline_run_id)
    
    async def get_recent_pipeline_runs(self, limit: int = 10) -> List[PipelineRunSummary]:
        """Get recent pipeline runs."""
        return await self.provider.get_recent_pipeline_runs(limit)
    
    async def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """Clean up old tracking data."""
        return await self.provider.cleanup_old_data(days_to_keep)