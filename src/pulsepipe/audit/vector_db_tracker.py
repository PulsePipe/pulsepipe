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

# src/pulsepipe/audit/vector_db_tracker.py

"""
Vector database success/failure tracking system for PulsePipe.

Provides comprehensive tracking of vector database operations with detailed metrics
and export capabilities for analysis.
"""

import json
import csv
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Union
from dataclasses import dataclass, field, asdict
from pathlib import Path
from enum import Enum
from contextlib import contextmanager

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.persistence import TrackingRepository, ProcessingStatus, ErrorCategory, VectorDbStat
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig

logger = LogFactory.get_logger(__name__)


class VectorDbOutcome(str, Enum):
    """Outcome of a vector database operation."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL_SUCCESS = "partial_success"
    SKIPPED = "skipped"


class VectorDbStage(str, Enum):
    """Stage where vector database outcome occurred."""
    CONNECTION = "connection"
    INDEX_CREATION = "index_creation"
    COLLECTION_SETUP = "collection_setup"
    VECTOR_INSERTION = "vector_insertion"
    METADATA_INSERTION = "metadata_insertion"
    INDEXING = "indexing"
    VALIDATION = "validation"
    OPTIMIZATION = "optimization"


@dataclass
class VectorDbRecord:
    """Individual vector database record tracking information."""
    record_id: str
    source_id: Optional[str] = None
    content_type: Optional[str] = None  # clinical, operational, narrative
    outcome: Optional[VectorDbOutcome] = None
    stage: Optional[VectorDbStage] = None
    error_category: Optional[ErrorCategory] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    processing_time_ms: Optional[int] = None
    vector_count: Optional[int] = None
    index_name: Optional[str] = None
    collection_name: Optional[str] = None
    vector_store_type: Optional[str] = None
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class VectorDbBatchMetrics:
    """Metrics for a batch of vector database operations."""
    batch_id: str
    pipeline_run_id: str
    stage_name: str
    started_at: datetime
    completed_at: Optional[datetime] = None
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    partial_success_records: int = 0
    total_processing_time_ms: int = 0
    total_vectors_stored: int = 0
    avg_processing_time_ms: float = 0.0
    records_per_second: float = 0.0
    vectors_per_second: float = 0.0
    success_rate: float = 0.0
    failure_rate: float = 0.0
    avg_vectors_per_record: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    vector_store_types: List[str] = field(default_factory=list)
    index_names: List[str] = field(default_factory=list)
    collection_names: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_metrics(self) -> None:
        """Calculate derived metrics."""
        if self.completed_at:
            duration_seconds = (self.completed_at - self.started_at).total_seconds()
            if duration_seconds > 0:
                self.records_per_second = self.total_records / duration_seconds
                self.vectors_per_second = self.total_vectors_stored / duration_seconds
        
        if self.total_records > 0:
            self.success_rate = (self.successful_records / self.total_records) * 100
            self.failure_rate = (self.failed_records / self.total_records) * 100
            self.avg_processing_time_ms = self.total_processing_time_ms / self.total_records
            self.avg_vectors_per_record = self.total_vectors_stored / self.total_records
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['started_at'] = self.started_at.isoformat()
        data['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        return data


@dataclass
class VectorDbSummary:
    """Summary of vector database operations across multiple batches."""
    pipeline_run_id: str
    generated_at: datetime
    time_range_start: Optional[datetime] = None
    time_range_end: Optional[datetime] = None
    total_batches: int = 0
    total_records: int = 0
    successful_records: int = 0
    failed_records: int = 0
    skipped_records: int = 0
    partial_success_records: int = 0
    success_rate: float = 0.0
    failure_rate: float = 0.0
    avg_processing_time_ms: float = 0.0
    total_processing_time_ms: int = 0
    total_vectors_stored: int = 0
    records_per_second: float = 0.0
    vectors_per_second: float = 0.0
    avg_vectors_per_record: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    most_common_errors: List[Dict[str, Any]] = field(default_factory=list)
    performance_trends: List[Dict[str, Any]] = field(default_factory=list)
    vector_store_types: List[str] = field(default_factory=list)
    index_names: List[str] = field(default_factory=list)
    collection_names: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_batches(cls, pipeline_run_id: str, batches: List[VectorDbBatchMetrics]) -> 'VectorDbSummary':
        """Create summary from list of batch metrics."""
        summary = cls(
            pipeline_run_id=pipeline_run_id,
            generated_at=datetime.now(),
            total_batches=len(batches)
        )
        
        if not batches:
            summary.recommendations = summary._generate_recommendations()
            return summary
        
        # Set time range
        start_times = [b.started_at for b in batches]
        end_times = [b.completed_at for b in batches if b.completed_at]
        
        summary.time_range_start = min(start_times) if start_times else None
        summary.time_range_end = max(end_times) if end_times else None
        
        # Aggregate totals
        for batch in batches:
            summary.total_records += batch.total_records
            summary.successful_records += batch.successful_records
            summary.failed_records += batch.failed_records
            summary.skipped_records += batch.skipped_records
            summary.partial_success_records += batch.partial_success_records
            summary.total_processing_time_ms += batch.total_processing_time_ms
            summary.total_vectors_stored += batch.total_vectors_stored
            
            # Aggregate error categories
            for category, count in batch.errors_by_category.items():
                summary.errors_by_category[category] = summary.errors_by_category.get(category, 0) + count
            
            # Aggregate error stages
            for stage, count in batch.errors_by_stage.items():
                summary.errors_by_stage[stage] = summary.errors_by_stage.get(stage, 0) + count
            
            # Collect unique vector store types
            for store_type in batch.vector_store_types:
                if store_type not in summary.vector_store_types:
                    summary.vector_store_types.append(store_type)
            
            # Collect unique index names
            for index_name in batch.index_names:
                if index_name not in summary.index_names:
                    summary.index_names.append(index_name)
            
            # Collect unique collection names
            for collection_name in batch.collection_names:
                if collection_name not in summary.collection_names:
                    summary.collection_names.append(collection_name)
            
            # Collect unique content types
            for content_type in batch.content_types:
                if content_type not in summary.content_types:
                    summary.content_types.append(content_type)
        
        # Calculate derived metrics
        if summary.total_records > 0:
            summary.success_rate = (summary.successful_records / summary.total_records) * 100
            summary.failure_rate = (summary.failed_records / summary.total_records) * 100
            summary.avg_processing_time_ms = summary.total_processing_time_ms / summary.total_records
            summary.avg_vectors_per_record = summary.total_vectors_stored / summary.total_records
        
        if summary.time_range_start and summary.time_range_end:
            duration_seconds = (summary.time_range_end - summary.time_range_start).total_seconds()
            if duration_seconds > 0:
                summary.records_per_second = summary.total_records / duration_seconds
                summary.vectors_per_second = summary.total_vectors_stored / duration_seconds
        
        # Generate most common errors
        summary.most_common_errors = [
            {"category": cat, "count": count, "percentage": round((count / summary.failed_records) * 100, 1)}
            for cat, count in sorted(summary.errors_by_category.items(), key=lambda x: x[1], reverse=True)[:5]
        ] if summary.failed_records > 0 else []
        
        # Generate recommendations
        summary.recommendations = summary._generate_recommendations()
        
        return summary
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on vector database metrics."""
        recommendations = []
        
        # Handle case with no data processed
        if self.total_records == 0:
            recommendations.append(
                "No records were processed for vector storage. Verify input data and vector database configuration."
            )
            return recommendations
        
        # High failure rate recommendations
        if self.failure_rate > 20:
            recommendations.append(
                f"High failure rate ({self.failure_rate:.1f}%) detected in vector storage. "
                "Review database connectivity and index configuration."
            )
        elif self.failure_rate > 10:
            recommendations.append(
                f"Moderate failure rate ({self.failure_rate:.1f}%) detected. "
                "Monitor vector database performance and capacity."
            )
        
        # Performance recommendations
        if self.avg_processing_time_ms > 3000:
            recommendations.append(
                f"Average vector storage time is high ({self.avg_processing_time_ms:.0f}ms per record). "
                "Consider optimizing database configuration or implementing batch inserts."
            )
        
        if self.vectors_per_second < 50 and self.total_vectors_stored > 100:
            recommendations.append(
                f"Low vector storage throughput ({self.vectors_per_second:.1f} vectors/sec). "
                "Consider implementing bulk operations or increasing batch sizes."
            )
        
        # Vector storage recommendations
        if self.avg_vectors_per_record > 50:
            recommendations.append(
                f"High number of vectors per record ({self.avg_vectors_per_record:.1f}). "
                "Consider optimizing chunking strategy or filtering less relevant vectors."
            )
        elif self.avg_vectors_per_record < 1:
            recommendations.append(
                f"Low vectors per record ({self.avg_vectors_per_record:.1f}). "
                "Verify embedding stage is properly generating vectors for storage."
            )
        
        # Database configuration recommendations
        if len(self.vector_store_types) > 2:
            recommendations.append(
                f"Multiple vector database types in use ({len(self.vector_store_types)}). "
                "Consider standardizing on a single vector store for consistency."
            )
        
        if len(self.index_names) > 5:
            recommendations.append(
                f"Many indexes in use ({len(self.index_names)}). "
                "Consider consolidating indexes or implementing index rotation strategies."
            )
        
        # Capacity and scaling recommendations
        if self.total_vectors_stored > 1000000:  # 1 million vectors
            recommendations.append(
                f"Large number of vectors stored ({self.total_vectors_stored:,}). "
                "Consider implementing index optimization and capacity planning."
            )
        
        # Error pattern recommendations
        if self.most_common_errors:
            top_error = self.most_common_errors[0]
            if top_error['percentage'] > 50:
                recommendations.append(
                    f"Most errors ({top_error['percentage']}%) are '{top_error['category']}'. "
                    "Focus on addressing this specific vector database error pattern."
                )
        
        # Stage-specific recommendations
        connection_errors = self.errors_by_stage.get('connection', 0)
        if connection_errors > self.failed_records * 0.3:  # 30% of failures are connection issues
            recommendations.append(
                "High rate of connection errors detected. "
                "Review database connectivity, timeouts, and connection pooling configuration."
            )
        
        indexing_errors = self.errors_by_stage.get('indexing', 0)
        if indexing_errors > self.failed_records * 0.2:  # 20% of failures are indexing issues
            recommendations.append(
                "High rate of indexing errors detected. "
                "Review index configuration, capacity, and maintenance schedules."
            )
        
        if not recommendations:
            recommendations.append("Vector database performance appears healthy with no major issues identified.")
        
        return recommendations
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['generated_at'] = self.generated_at.isoformat()
        data['time_range_start'] = self.time_range_start.isoformat() if self.time_range_start else None
        data['time_range_end'] = self.time_range_end.isoformat() if self.time_range_end else None
        return data


class VectorDbTracker:
    """
    Comprehensive vector database tracking system.
    
    Tracks success/failure rates, performance metrics, and error patterns
    for vector database operations with export capabilities.
    """
    
    def __init__(self, pipeline_run_id: str, stage_name: str, 
                 config: DataIntelligenceConfig,
                 repository: Optional[TrackingRepository] = None):
        """
        Initialize vector database tracker.
        
        Args:
            pipeline_run_id: Unique identifier for the pipeline run
            stage_name: Name of the vector database stage
            config: Data intelligence configuration
            repository: Optional tracking repository for persistence
        """
        self.pipeline_run_id = pipeline_run_id
        self.stage_name = stage_name
        self.config = config
        self.repository = repository
        
        # Current batch being tracked
        self.current_batch: Optional[VectorDbBatchMetrics] = None
        self.batch_records: List[VectorDbRecord] = []
        
        # Completed batches
        self.completed_batches: List[VectorDbBatchMetrics] = []
        
        # Configuration
        self.enabled = config.is_feature_enabled('vector_db_tracking')
        self.detailed_tracking = config.is_feature_enabled('vector_db_tracking', 'detailed_tracking')
        self.auto_persist = config.is_feature_enabled('vector_db_tracking', 'auto_persist')
        
        if self.enabled:
            logger.info(f"Vector database tracker initialized for pipeline: {pipeline_run_id}, stage: {stage_name}")
    
    def is_enabled(self) -> bool:
        """Check if vector database tracking is enabled."""
        return self.enabled
    
    @contextmanager
    def track_batch(self, batch_id: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Context manager for tracking a batch of vector database operations.
        
        Args:
            batch_id: Unique identifier for the batch
            metadata: Optional batch metadata
        """
        if not self.enabled:
            yield
            return
        
        self.start_batch(batch_id, metadata)
        try:
            yield
        finally:
            self.finish_batch()
    
    def start_batch(self, batch_id: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Start tracking a new batch of vector database operations.
        
        Args:
            batch_id: Unique identifier for the batch
            metadata: Optional batch metadata
        """
        if not self.enabled:
            return
        
        # Finish previous batch if it exists
        if self.current_batch:
            logger.warning(f"Previous batch '{self.current_batch.batch_id}' was not properly finished")
            self.finish_batch()
        
        self.current_batch = VectorDbBatchMetrics(
            batch_id=batch_id,
            pipeline_run_id=self.pipeline_run_id,
            stage_name=self.stage_name,
            started_at=datetime.now(),
            metadata=metadata or {}
        )
        self.batch_records = []
        
        logger.debug(f"Started tracking vector database batch: {batch_id}")
    
    def record_success(self, record_id: str, source_id: Optional[str] = None,
                      content_type: Optional[str] = None, processing_time_ms: Optional[int] = None,
                      vector_count: Optional[int] = None, index_name: Optional[str] = None,
                      collection_name: Optional[str] = None, vector_store_type: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a successful vector database operation.
        
        Args:
            record_id: Unique identifier for the record
            source_id: Source record identifier
            content_type: Type of content (clinical, operational, narrative)
            processing_time_ms: Time taken to process the record
            vector_count: Number of vectors stored
            index_name: Name of the index used
            collection_name: Name of the collection used
            vector_store_type: Type of vector store (e.g., 'qdrant', 'weaviate')
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = VectorDbRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=VectorDbOutcome.SUCCESS,
            processing_time_ms=processing_time_ms,
            vector_count=vector_count,
            index_name=index_name,
            collection_name=collection_name,
            vector_store_type=vector_store_type,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_failure(self, record_id: str, error: Exception, stage: VectorDbStage,
                      error_category: Optional[ErrorCategory] = None,
                      source_id: Optional[str] = None, content_type: Optional[str] = None,
                      processing_time_ms: Optional[int] = None, index_name: Optional[str] = None,
                      collection_name: Optional[str] = None, vector_store_type: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a failed vector database operation.
        
        Args:
            record_id: Unique identifier for the record
            error: Exception that caused the failure
            stage: Stage where the failure occurred
            error_category: Category of the error
            source_id: Source record identifier
            content_type: Type of content being processed
            processing_time_ms: Time taken before failure
            index_name: Name of the index being used
            collection_name: Name of the collection being used
            vector_store_type: Type of vector store being used
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = VectorDbRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=VectorDbOutcome.FAILURE,
            stage=stage,
            error_category=error_category,
            error_message=str(error),
            error_details={
                "error_type": type(error).__name__,
                "error_message": str(error)
            },
            processing_time_ms=processing_time_ms,
            index_name=index_name,
            collection_name=collection_name,
            vector_store_type=vector_store_type,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_skip(self, record_id: str, reason: str, source_id: Optional[str] = None,
                   content_type: Optional[str] = None, index_name: Optional[str] = None,
                   collection_name: Optional[str] = None, vector_store_type: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a skipped vector database operation.
        
        Args:
            record_id: Unique identifier for the record
            reason: Reason for skipping
            source_id: Source record identifier
            content_type: Type of content
            index_name: Name of the index
            collection_name: Name of the collection
            vector_store_type: Type of vector store
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = VectorDbRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=VectorDbOutcome.SKIPPED,
            error_message=reason,
            index_name=index_name,
            collection_name=collection_name,
            vector_store_type=vector_store_type,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_partial_success(self, record_id: str, issues: List[str], stage: VectorDbStage,
                              source_id: Optional[str] = None, content_type: Optional[str] = None,
                              processing_time_ms: Optional[int] = None, vector_count: Optional[int] = None,
                              index_name: Optional[str] = None, collection_name: Optional[str] = None,
                              vector_store_type: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a partially successful vector database operation.
        
        Args:
            record_id: Unique identifier for the record
            issues: List of issues encountered
            stage: Stage where issues occurred
            source_id: Source record identifier
            content_type: Type of content
            processing_time_ms: Time taken to process
            vector_count: Number of vectors stored
            index_name: Name of the index used
            collection_name: Name of the collection used
            vector_store_type: Type of vector store used
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = VectorDbRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=VectorDbOutcome.PARTIAL_SUCCESS,
            stage=stage,
            error_message="; ".join(issues),
            error_details={"issues": issues},
            processing_time_ms=processing_time_ms,
            vector_count=vector_count,
            index_name=index_name,
            collection_name=collection_name,
            vector_store_type=vector_store_type,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def _add_record(self, record: VectorDbRecord) -> None:
        """Add record to current batch and update metrics."""
        if not self.current_batch:
            # Create a default batch if none exists
            self.start_batch(f"auto_batch_{int(time.time())}")
        
        # Add to detailed records if enabled
        if self.detailed_tracking:
            self.batch_records.append(record)
        
        # Update batch metrics
        batch = self.current_batch
        batch.total_records += 1
        
        if record.outcome == VectorDbOutcome.SUCCESS:
            batch.successful_records += 1
        elif record.outcome == VectorDbOutcome.FAILURE:
            batch.failed_records += 1
            
            # Track error categories and stages
            if record.error_category:
                cat = record.error_category.value
                batch.errors_by_category[cat] = batch.errors_by_category.get(cat, 0) + 1
            
            if record.stage:
                stage = record.stage.value
                batch.errors_by_stage[stage] = batch.errors_by_stage.get(stage, 0) + 1
                
        elif record.outcome == VectorDbOutcome.SKIPPED:
            batch.skipped_records += 1
        elif record.outcome == VectorDbOutcome.PARTIAL_SUCCESS:
            batch.partial_success_records += 1
        
        # Update totals
        if record.processing_time_ms:
            batch.total_processing_time_ms += record.processing_time_ms
        
        if record.vector_count:
            batch.total_vectors_stored += record.vector_count
        
        # Track vector store types, index names, collection names, and content types
        if record.vector_store_type and record.vector_store_type not in batch.vector_store_types:
            batch.vector_store_types.append(record.vector_store_type)
        
        if record.index_name and record.index_name not in batch.index_names:
            batch.index_names.append(record.index_name)
        
        if record.collection_name and record.collection_name not in batch.collection_names:
            batch.collection_names.append(record.collection_name)
        
        if record.content_type and record.content_type not in batch.content_types:
            batch.content_types.append(record.content_type)
    
    def _persist_record(self, record: VectorDbRecord) -> None:
        """Persist record to repository."""
        if not self.repository:
            return
        
        try:
            # Convert to VectorDbStat
            stat = VectorDbStat(
                id=None,
                pipeline_run_id=self.pipeline_run_id,
                stage_name=self.stage_name,
                source_id=record.source_id,
                record_id=record.record_id,
                content_type=record.content_type,
                status=ProcessingStatus.SUCCESS if record.outcome == VectorDbOutcome.SUCCESS else ProcessingStatus.FAILURE,
                error_category=record.error_category,
                error_message=record.error_message,
                error_details=record.error_details,
                processing_time_ms=record.processing_time_ms,
                vector_count=record.vector_count,
                index_name=record.index_name,
                collection_name=record.collection_name,
                vector_store_type=record.vector_store_type,
                timestamp=record.timestamp
            )
            self.repository.record_vector_db_stat(stat)
        except Exception as e:
            logger.error(f"Failed to persist vector database record: {e}")
    
    def finish_batch(self) -> Optional[VectorDbBatchMetrics]:
        """Finish the current batch and calculate final metrics."""
        if not self.enabled or not self.current_batch:
            return None
        
        # Mark batch as completed
        self.current_batch.completed_at = datetime.now()
        
        # Calculate final metrics
        self.current_batch.calculate_metrics()
        
        # Add to completed batches
        self.completed_batches.append(self.current_batch)
        
        logger.debug(f"Finished vector database batch: {self.current_batch.batch_id} "
                    f"({self.current_batch.total_records} records, "
                    f"{self.current_batch.total_vectors_stored} vectors stored, "
                    f"{self.current_batch.success_rate:.1f}% success rate)")
        
        finished_batch = self.current_batch
        self.current_batch = None
        self.batch_records = []
        
        return finished_batch
    
    def get_current_batch_summary(self) -> Optional[Dict[str, Any]]:
        """Get summary of current batch being tracked."""
        if not self.current_batch:
            return None
        
        # Calculate success rate on the fly for current batch
        batch = self.current_batch
        success_rate = (batch.successful_records / batch.total_records * 100) if batch.total_records > 0 else 0.0
        
        return {
            "batch_id": batch.batch_id,
            "total_records": batch.total_records,
            "successful_records": batch.successful_records,
            "failed_records": batch.failed_records,
            "skipped_records": batch.skipped_records,
            "total_vectors_stored": batch.total_vectors_stored,
            "success_rate": success_rate,
            "duration_seconds": (datetime.now() - batch.started_at).total_seconds()
        }
    
    def get_summary(self) -> VectorDbSummary:
        """Get comprehensive summary of all vector database operations."""
        all_batches = self.completed_batches.copy()
        
        # Include current batch if it exists
        if self.current_batch:
            temp_batch = VectorDbBatchMetrics(
                batch_id=self.current_batch.batch_id,
                pipeline_run_id=self.current_batch.pipeline_run_id,
                stage_name=self.current_batch.stage_name,
                started_at=self.current_batch.started_at,
                completed_at=datetime.now(),
                total_records=self.current_batch.total_records,
                successful_records=self.current_batch.successful_records,
                failed_records=self.current_batch.failed_records,
                skipped_records=self.current_batch.skipped_records,
                partial_success_records=self.current_batch.partial_success_records,
                total_processing_time_ms=self.current_batch.total_processing_time_ms,
                total_vectors_stored=self.current_batch.total_vectors_stored,
                errors_by_category=self.current_batch.errors_by_category.copy(),
                errors_by_stage=self.current_batch.errors_by_stage.copy(),
                vector_store_types=self.current_batch.vector_store_types.copy(),
                index_names=self.current_batch.index_names.copy(),
                collection_names=self.current_batch.collection_names.copy(),
                content_types=self.current_batch.content_types.copy()
            )
            temp_batch.calculate_metrics()
            all_batches.append(temp_batch)
        
        return VectorDbSummary.from_batches(self.pipeline_run_id, all_batches)
    
    def export_metrics(self, file_path: str, format: str = "json", 
                      include_details: bool = False) -> None:
        """
        Export vector database metrics to file.
        
        Args:
            file_path: Path to export file
            format: Export format (json, csv)
            include_details: Whether to include detailed record information
        """
        if not self.enabled:
            logger.warning("Vector database tracking is disabled, no metrics to export")
            return
        
        summary = self.get_summary()
        
        # Normalize file path for cross-platform compatibility
        import os
        import sys
        
        try:
            output_path = Path(file_path)
        except (ValueError, OSError) as e:
            # Handle Windows path issues
            if sys.platform == "win32" and "PYTEST_CURRENT_TEST" in os.environ:
                output_path = Path(file_path)
            else:
                raise e
        
        output_path.parent.mkdir(parents=True, exist_ok=True)
        
        if format.lower() == "json":
            self._export_json(summary, output_path, include_details)
        elif format.lower() == "csv":
            self._export_csv(summary, output_path, include_details)
        else:
            raise ValueError(f"Unsupported export format: {format}")
        
        logger.info(f"Exported vector database metrics to: {file_path}")
    
    def _export_json(self, summary: VectorDbSummary, file_path: Path, 
                    include_details: bool) -> None:
        """Export metrics to JSON format."""
        export_data = {
            "summary": summary.to_dict(),
            "completed_batches": [batch.to_dict() for batch in self.completed_batches]
        }
        
        if include_details and self.detailed_tracking:
            export_data["batch_details"] = {}
            for i, batch in enumerate(self.completed_batches):
                export_data["batch_details"][batch.batch_id] = {
                    "records_count": batch.total_records,
                    "vectors_stored": batch.total_vectors_stored,
                    "metadata": batch.metadata
                }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
    
    def _export_csv(self, summary: VectorDbSummary, file_path: Path, 
                   include_details: bool) -> None:
        """Export metrics to CSV format."""
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write summary header
            writer.writerow(["Vector Database Summary Report"])
            writer.writerow(["Pipeline Run ID", summary.pipeline_run_id])
            writer.writerow(["Generated At", summary.generated_at.isoformat()])
            writer.writerow([])
            
            # Write summary metrics
            writer.writerow(["Metric", "Value"])
            writer.writerow(["Total Records", summary.total_records])
            writer.writerow(["Successful Records", summary.successful_records])
            writer.writerow(["Failed Records", summary.failed_records])
            writer.writerow(["Skipped Records", summary.skipped_records])
            writer.writerow(["Success Rate (%)", f"{summary.success_rate:.2f}"])
            writer.writerow(["Failure Rate (%)", f"{summary.failure_rate:.2f}"])
            writer.writerow(["Total Vectors Stored", summary.total_vectors_stored])
            writer.writerow(["Avg Vectors Per Record", f"{summary.avg_vectors_per_record:.2f}"])
            writer.writerow(["Vectors Per Second", f"{summary.vectors_per_second:.2f}"])
            writer.writerow([])
            
            # Write vector store information
            if summary.vector_store_types:
                writer.writerow(["Vector Store Types"])
                for store_type in summary.vector_store_types:
                    writer.writerow([store_type])
                writer.writerow([])
            
            # Write error breakdown
            if summary.errors_by_category:
                writer.writerow(["Error Breakdown by Category"])
                writer.writerow(["Category", "Count", "Percentage"])
                total_errors = sum(summary.errors_by_category.values())
                for category, count in summary.errors_by_category.items():
                    percentage = (count / total_errors) * 100 if total_errors > 0 else 0
                    writer.writerow([category, count, f"{percentage:.1f}%"])
                writer.writerow([])
            
            # Write batch details if requested
            if include_details:
                writer.writerow(["Batch Details"])
                writer.writerow(["Batch ID", "Total Records", "Vectors Stored", "Success Rate (%)", "Duration (ms)"])
                for batch in self.completed_batches:
                    duration_ms = 0
                    if batch.completed_at:
                        duration_ms = (batch.completed_at - batch.started_at).total_seconds() * 1000
                    writer.writerow([
                        batch.batch_id,
                        batch.total_records,
                        batch.total_vectors_stored,
                        f"{batch.success_rate:.2f}",
                        f"{duration_ms:.0f}"
                    ])
    
    def clear_history(self) -> None:
        """Clear completed batch history (keeps current batch)."""
        self.completed_batches.clear()
        logger.debug("Cleared vector database tracking history")