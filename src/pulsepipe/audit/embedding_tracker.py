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

# src/pulsepipe/audit/embedding_tracker.py

"""
Embedding success/failure tracking system for PulsePipe.

Provides comprehensive tracking of embedding operations with detailed metrics
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
from pulsepipe.persistence import TrackingRepository, ProcessingStatus, ErrorCategory, EmbeddingStat
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig

logger = LogFactory.get_logger(__name__)


class EmbeddingOutcome(str, Enum):
    """Outcome of an embedding operation."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL_SUCCESS = "partial_success"
    SKIPPED = "skipped"


class EmbeddingStage(str, Enum):
    """Stage where embedding outcome occurred."""
    MODEL_LOADING = "model_loading"
    TEXT_PREPROCESSING = "text_preprocessing"
    EMBEDDING_GENERATION = "embedding_generation"
    VECTOR_VALIDATION = "vector_validation"
    BATCH_PROCESSING = "batch_processing"
    POST_PROCESSING = "post_processing"


@dataclass
class EmbeddingRecord:
    """Individual embedding record tracking information."""
    record_id: str
    source_id: Optional[str] = None
    content_type: Optional[str] = None  # clinical, operational, narrative
    outcome: Optional[EmbeddingOutcome] = None
    stage: Optional[EmbeddingStage] = None
    error_category: Optional[ErrorCategory] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    processing_time_ms: Optional[int] = None
    chunk_count: Optional[int] = None
    embedding_dimensions: Optional[int] = None
    model_name: Optional[str] = None
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class EmbeddingBatchMetrics:
    """Metrics for a batch of embedding operations."""
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
    total_chunks_embedded: int = 0
    total_vectors_generated: int = 0
    avg_processing_time_ms: float = 0.0
    records_per_second: float = 0.0
    chunks_per_second: float = 0.0
    vectors_per_second: float = 0.0
    success_rate: float = 0.0
    failure_rate: float = 0.0
    avg_chunks_per_record: float = 0.0
    avg_embedding_dimensions: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    model_names: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_metrics(self) -> None:
        """Calculate derived metrics."""
        if self.completed_at:
            duration_seconds = (self.completed_at - self.started_at).total_seconds()
            if duration_seconds > 0:
                self.records_per_second = self.total_records / duration_seconds
                self.chunks_per_second = self.total_chunks_embedded / duration_seconds
                self.vectors_per_second = self.total_vectors_generated / duration_seconds
        
        if self.total_records > 0:
            self.success_rate = (self.successful_records / self.total_records) * 100
            self.failure_rate = (self.failed_records / self.total_records) * 100
            self.avg_processing_time_ms = self.total_processing_time_ms / self.total_records
            self.avg_chunks_per_record = self.total_chunks_embedded / self.total_records
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['started_at'] = self.started_at.isoformat()
        data['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        return data


@dataclass
class EmbeddingSummary:
    """Summary of embedding operations across multiple batches."""
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
    total_chunks_embedded: int = 0
    total_vectors_generated: int = 0
    records_per_second: float = 0.0
    chunks_per_second: float = 0.0
    vectors_per_second: float = 0.0
    avg_chunks_per_record: float = 0.0
    avg_embedding_dimensions: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    most_common_errors: List[Dict[str, Any]] = field(default_factory=list)
    performance_trends: List[Dict[str, Any]] = field(default_factory=list)
    model_names: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_batches(cls, pipeline_run_id: str, batches: List[EmbeddingBatchMetrics]) -> 'EmbeddingSummary':
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
        embedding_dimensions = []
        for batch in batches:
            summary.total_records += batch.total_records
            summary.successful_records += batch.successful_records
            summary.failed_records += batch.failed_records
            summary.skipped_records += batch.skipped_records
            summary.partial_success_records += batch.partial_success_records
            summary.total_processing_time_ms += batch.total_processing_time_ms
            summary.total_chunks_embedded += batch.total_chunks_embedded
            summary.total_vectors_generated += batch.total_vectors_generated
            
            if batch.avg_embedding_dimensions > 0:
                embedding_dimensions.append(batch.avg_embedding_dimensions)
            
            # Aggregate error categories
            for category, count in batch.errors_by_category.items():
                summary.errors_by_category[category] = summary.errors_by_category.get(category, 0) + count
            
            # Aggregate error stages
            for stage, count in batch.errors_by_stage.items():
                summary.errors_by_stage[stage] = summary.errors_by_stage.get(stage, 0) + count
            
            # Collect unique model names
            for model_name in batch.model_names:
                if model_name not in summary.model_names:
                    summary.model_names.append(model_name)
            
            # Collect unique content types
            for content_type in batch.content_types:
                if content_type not in summary.content_types:
                    summary.content_types.append(content_type)
        
        # Calculate derived metrics
        if summary.total_records > 0:
            summary.success_rate = (summary.successful_records / summary.total_records) * 100
            summary.failure_rate = (summary.failed_records / summary.total_records) * 100
            summary.avg_processing_time_ms = summary.total_processing_time_ms / summary.total_records
            summary.avg_chunks_per_record = summary.total_chunks_embedded / summary.total_records
        
        if embedding_dimensions:
            summary.avg_embedding_dimensions = sum(embedding_dimensions) / len(embedding_dimensions)
        
        if summary.time_range_start and summary.time_range_end:
            duration_seconds = (summary.time_range_end - summary.time_range_start).total_seconds()
            if duration_seconds > 0:
                summary.records_per_second = summary.total_records / duration_seconds
                summary.chunks_per_second = summary.total_chunks_embedded / duration_seconds
                summary.vectors_per_second = summary.total_vectors_generated / duration_seconds
        
        # Generate most common errors
        summary.most_common_errors = [
            {"category": cat, "count": count, "percentage": round((count / summary.failed_records) * 100, 1)}
            for cat, count in sorted(summary.errors_by_category.items(), key=lambda x: x[1], reverse=True)[:5]
        ] if summary.failed_records > 0 else []
        
        # Generate recommendations
        summary.recommendations = summary._generate_recommendations()
        
        return summary
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on embedding metrics."""
        recommendations = []
        
        # Handle case with no data processed
        if self.total_records == 0:
            recommendations.append(
                "No records were processed for embedding. Verify input data and pipeline configuration."
            )
            return recommendations
        
        # High failure rate recommendations
        if self.failure_rate > 20:
            recommendations.append(
                f"High failure rate ({self.failure_rate:.1f}%) detected in embedding generation. "
                "Review model configuration and input text preprocessing."
            )
        elif self.failure_rate > 10:
            recommendations.append(
                f"Moderate failure rate ({self.failure_rate:.1f}%) detected. "
                "Monitor embedding model performance and batch sizes."
            )
        
        # Performance recommendations
        if self.avg_processing_time_ms > 5000:
            recommendations.append(
                f"Average embedding time is high ({self.avg_processing_time_ms:.0f}ms per record). "
                "Consider optimizing model inference or implementing GPU acceleration."
            )
        
        if self.chunks_per_second < 20 and self.total_chunks_embedded > 100:
            recommendations.append(
                f"Low embedding throughput ({self.chunks_per_second:.1f} chunks/sec). "
                "Consider implementing batch processing or model optimization."
            )
        
        # Vector generation recommendations
        if self.vectors_per_second < 10 and self.total_vectors_generated > 50:
            recommendations.append(
                f"Low vector generation rate ({self.vectors_per_second:.1f} vectors/sec). "
                "Consider increasing batch size or using more efficient embedding models."
            )
        
        # Chunks per record recommendations
        if self.avg_chunks_per_record > 100:
            recommendations.append(
                f"High number of chunks per record ({self.avg_chunks_per_record:.1f}). "
                "Consider filtering chunks or increasing chunk size to reduce processing overhead."
            )
        elif self.avg_chunks_per_record < 1:
            recommendations.append(
                f"Low chunks per record ({self.avg_chunks_per_record:.1f}). "
                "Verify chunking stage is properly generating content for embedding."
            )
        
        # Model and dimensions recommendations
        if len(self.model_names) > 3:
            recommendations.append(
                f"Multiple embedding models in use ({len(self.model_names)}). "
                "Consider standardizing on fewer models for consistency and performance."
            )
        
        if self.avg_embedding_dimensions > 1536:
            recommendations.append(
                f"High embedding dimensions ({self.avg_embedding_dimensions:.0f}). "
                "Consider using lower-dimensional models for better storage and query performance."
            )
        elif self.avg_embedding_dimensions < 256:
            recommendations.append(
                f"Low embedding dimensions ({self.avg_embedding_dimensions:.0f}). "
                "Verify model configuration and consider higher-dimensional models for better accuracy."
            )
        
        # Error pattern recommendations
        if self.most_common_errors:
            top_error = self.most_common_errors[0]
            if top_error['percentage'] > 50:
                recommendations.append(
                    f"Most errors ({top_error['percentage']}%) are '{top_error['category']}'. "
                    "Focus on addressing this specific embedding error pattern."
                )
        
        if not recommendations:
            recommendations.append("Embedding performance appears healthy with no major issues identified.")
        
        return recommendations
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['generated_at'] = self.generated_at.isoformat()
        data['time_range_start'] = self.time_range_start.isoformat() if self.time_range_start else None
        data['time_range_end'] = self.time_range_end.isoformat() if self.time_range_end else None
        return data


class EmbeddingTracker:
    """
    Comprehensive embedding tracking system.
    
    Tracks success/failure rates, performance metrics, and error patterns
    for embedding operations with export capabilities.
    """
    
    def __init__(self, pipeline_run_id: str, stage_name: str, 
                 config: DataIntelligenceConfig,
                 repository: Optional[TrackingRepository] = None):
        """
        Initialize embedding tracker.
        
        Args:
            pipeline_run_id: Unique identifier for the pipeline run
            stage_name: Name of the embedding stage
            config: Data intelligence configuration
            repository: Optional tracking repository for persistence
        """
        self.pipeline_run_id = pipeline_run_id
        self.stage_name = stage_name
        self.config = config
        self.repository = repository
        
        # Current batch being tracked
        self.current_batch: Optional[EmbeddingBatchMetrics] = None
        self.batch_records: List[EmbeddingRecord] = []
        
        # Completed batches
        self.completed_batches: List[EmbeddingBatchMetrics] = []
        
        # Configuration
        self.enabled = config.is_feature_enabled('embedding_tracking')
        self.detailed_tracking = config.is_feature_enabled('embedding_tracking', 'detailed_tracking')
        self.auto_persist = config.is_feature_enabled('embedding_tracking', 'auto_persist')
        
        if self.enabled:
            logger.info(f"Embedding tracker initialized for pipeline: {pipeline_run_id}, stage: {stage_name}")
    
    def is_enabled(self) -> bool:
        """Check if embedding tracking is enabled."""
        return self.enabled
    
    @contextmanager
    def track_batch(self, batch_id: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Context manager for tracking a batch of embedding operations.
        
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
        Start tracking a new batch of embedding operations.
        
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
        
        self.current_batch = EmbeddingBatchMetrics(
            batch_id=batch_id,
            pipeline_run_id=self.pipeline_run_id,
            stage_name=self.stage_name,
            started_at=datetime.now(),
            metadata=metadata or {}
        )
        self.batch_records = []
        
        logger.debug(f"Started tracking embedding batch: {batch_id}")
    
    def record_success(self, record_id: str, source_id: Optional[str] = None,
                      content_type: Optional[str] = None, processing_time_ms: Optional[int] = None,
                      chunk_count: Optional[int] = None, embedding_dimensions: Optional[int] = None,
                      model_name: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a successful embedding operation.
        
        Args:
            record_id: Unique identifier for the record
            source_id: Source record identifier
            content_type: Type of content (clinical, operational, narrative)
            processing_time_ms: Time taken to process the record
            chunk_count: Number of chunks embedded
            embedding_dimensions: Dimensionality of embeddings
            model_name: Name of embedding model used
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = EmbeddingRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=EmbeddingOutcome.SUCCESS,
            processing_time_ms=processing_time_ms,
            chunk_count=chunk_count,
            embedding_dimensions=embedding_dimensions,
            model_name=model_name,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_failure(self, record_id: str, error: Exception, stage: EmbeddingStage,
                      error_category: Optional[ErrorCategory] = None,
                      source_id: Optional[str] = None, content_type: Optional[str] = None,
                      processing_time_ms: Optional[int] = None, model_name: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a failed embedding operation.
        
        Args:
            record_id: Unique identifier for the record
            error: Exception that caused the failure
            stage: Stage where the failure occurred
            error_category: Category of the error
            source_id: Source record identifier
            content_type: Type of content being processed
            processing_time_ms: Time taken before failure
            model_name: Name of embedding model being used
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = EmbeddingRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=EmbeddingOutcome.FAILURE,
            stage=stage,
            error_category=error_category,
            error_message=str(error),
            error_details={
                "error_type": type(error).__name__,
                "error_message": str(error)
            },
            processing_time_ms=processing_time_ms,
            model_name=model_name,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_skip(self, record_id: str, reason: str, source_id: Optional[str] = None,
                   content_type: Optional[str] = None, model_name: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a skipped embedding operation.
        
        Args:
            record_id: Unique identifier for the record
            reason: Reason for skipping
            source_id: Source record identifier
            content_type: Type of content
            model_name: Name of embedding model
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = EmbeddingRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=EmbeddingOutcome.SKIPPED,
            error_message=reason,
            model_name=model_name,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_partial_success(self, record_id: str, issues: List[str], stage: EmbeddingStage,
                              source_id: Optional[str] = None, content_type: Optional[str] = None,
                              processing_time_ms: Optional[int] = None, chunk_count: Optional[int] = None,
                              embedding_dimensions: Optional[int] = None, model_name: Optional[str] = None,
                              metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a partially successful embedding operation.
        
        Args:
            record_id: Unique identifier for the record
            issues: List of issues encountered
            stage: Stage where issues occurred
            source_id: Source record identifier
            content_type: Type of content
            processing_time_ms: Time taken to process
            chunk_count: Number of chunks embedded
            embedding_dimensions: Dimensionality of embeddings
            model_name: Name of embedding model used
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = EmbeddingRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=EmbeddingOutcome.PARTIAL_SUCCESS,
            stage=stage,
            error_message="; ".join(issues),
            error_details={"issues": issues},
            processing_time_ms=processing_time_ms,
            chunk_count=chunk_count,
            embedding_dimensions=embedding_dimensions,
            model_name=model_name,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def _add_record(self, record: EmbeddingRecord) -> None:
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
        
        if record.outcome == EmbeddingOutcome.SUCCESS:
            batch.successful_records += 1
        elif record.outcome == EmbeddingOutcome.FAILURE:
            batch.failed_records += 1
            
            # Track error categories and stages
            if record.error_category:
                cat = record.error_category.value
                batch.errors_by_category[cat] = batch.errors_by_category.get(cat, 0) + 1
            
            if record.stage:
                stage = record.stage.value
                batch.errors_by_stage[stage] = batch.errors_by_stage.get(stage, 0) + 1
                
        elif record.outcome == EmbeddingOutcome.SKIPPED:
            batch.skipped_records += 1
        elif record.outcome == EmbeddingOutcome.PARTIAL_SUCCESS:
            batch.partial_success_records += 1
        
        # Update totals
        if record.processing_time_ms:
            batch.total_processing_time_ms += record.processing_time_ms
        
        if record.chunk_count:
            batch.total_chunks_embedded += record.chunk_count
            # Assume one vector per chunk
            batch.total_vectors_generated += record.chunk_count
        
        # Track model names and content types
        if record.model_name and record.model_name not in batch.model_names:
            batch.model_names.append(record.model_name)
        
        if record.content_type and record.content_type not in batch.content_types:
            batch.content_types.append(record.content_type)
    
    def _persist_record(self, record: EmbeddingRecord) -> None:
        """Persist record to repository."""
        if not self.repository:
            return
        
        try:
            # Convert to EmbeddingStat
            stat = EmbeddingStat(
                id=None,
                pipeline_run_id=self.pipeline_run_id,
                stage_name=self.stage_name,
                source_id=record.source_id,
                record_id=record.record_id,
                content_type=record.content_type,
                status=ProcessingStatus.SUCCESS if record.outcome == EmbeddingOutcome.SUCCESS else ProcessingStatus.FAILURE,
                error_category=record.error_category,
                error_message=record.error_message,
                error_details=record.error_details,
                processing_time_ms=record.processing_time_ms,
                chunk_count=record.chunk_count,
                embedding_dimensions=record.embedding_dimensions,
                model_name=record.model_name,
                timestamp=record.timestamp
            )
            self.repository.record_embedding_stat(stat)
        except Exception as e:
            logger.error(f"Failed to persist embedding record: {e}")
    
    def finish_batch(self) -> Optional[EmbeddingBatchMetrics]:
        """Finish the current batch and calculate final metrics."""
        if not self.enabled or not self.current_batch:
            return None
        
        # Mark batch as completed
        self.current_batch.completed_at = datetime.now()
        
        # Calculate final metrics
        self.current_batch.calculate_metrics()
        
        # Add to completed batches
        self.completed_batches.append(self.current_batch)
        
        logger.debug(f"Finished embedding batch: {self.current_batch.batch_id} "
                    f"({self.current_batch.total_records} records, "
                    f"{self.current_batch.total_chunks_embedded} chunks embedded, "
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
            "total_chunks_embedded": batch.total_chunks_embedded,
            "total_vectors_generated": batch.total_vectors_generated,
            "success_rate": success_rate,
            "duration_seconds": (datetime.now() - batch.started_at).total_seconds()
        }
    
    def get_summary(self) -> EmbeddingSummary:
        """Get comprehensive summary of all embedding operations."""
        all_batches = self.completed_batches.copy()
        
        # Include current batch if it exists
        if self.current_batch:
            temp_batch = EmbeddingBatchMetrics(
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
                total_chunks_embedded=self.current_batch.total_chunks_embedded,
                total_vectors_generated=self.current_batch.total_vectors_generated,
                errors_by_category=self.current_batch.errors_by_category.copy(),
                errors_by_stage=self.current_batch.errors_by_stage.copy(),
                model_names=self.current_batch.model_names.copy(),
                content_types=self.current_batch.content_types.copy()
            )
            temp_batch.calculate_metrics()
            all_batches.append(temp_batch)
        
        return EmbeddingSummary.from_batches(self.pipeline_run_id, all_batches)
    
    def export_metrics(self, file_path: str, format: str = "json", 
                      include_details: bool = False) -> None:
        """
        Export embedding metrics to file.
        
        Args:
            file_path: Path to export file
            format: Export format (json, csv)
            include_details: Whether to include detailed record information
        """
        if not self.enabled:
            logger.warning("Embedding tracking is disabled, no metrics to export")
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
        
        logger.info(f"Exported embedding metrics to: {file_path}")
    
    def _export_json(self, summary: EmbeddingSummary, file_path: Path, 
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
                    "chunks_embedded": batch.total_chunks_embedded,
                    "vectors_generated": batch.total_vectors_generated,
                    "metadata": batch.metadata
                }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
    
    def _export_csv(self, summary: EmbeddingSummary, file_path: Path, 
                   include_details: bool) -> None:
        """Export metrics to CSV format."""
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write summary header
            writer.writerow(["Embedding Summary Report"])
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
            writer.writerow(["Total Chunks Embedded", summary.total_chunks_embedded])
            writer.writerow(["Total Vectors Generated", summary.total_vectors_generated])
            writer.writerow(["Avg Chunks Per Record", f"{summary.avg_chunks_per_record:.2f}"])
            writer.writerow(["Avg Embedding Dimensions", f"{summary.avg_embedding_dimensions:.0f}"])
            writer.writerow(["Chunks Per Second", f"{summary.chunks_per_second:.2f}"])
            writer.writerow(["Vectors Per Second", f"{summary.vectors_per_second:.2f}"])
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
                writer.writerow(["Batch ID", "Total Records", "Chunks Embedded", "Vectors Generated", "Success Rate (%)", "Duration (ms)"])
                for batch in self.completed_batches:
                    duration_ms = 0
                    if batch.completed_at:
                        duration_ms = (batch.completed_at - batch.started_at).total_seconds() * 1000
                    writer.writerow([
                        batch.batch_id,
                        batch.total_records,
                        batch.total_chunks_embedded,
                        batch.total_vectors_generated,
                        f"{batch.success_rate:.2f}",
                        f"{duration_ms:.0f}"
                    ])
    
    def clear_history(self) -> None:
        """Clear completed batch history (keeps current batch)."""
        self.completed_batches.clear()
        logger.debug("Cleared embedding tracking history")