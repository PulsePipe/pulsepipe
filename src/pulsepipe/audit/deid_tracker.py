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

# src/pulsepipe/audit/deid_tracker.py

"""
De-identification success/failure tracking system for PulsePipe.

Provides comprehensive tracking of de-identification operations with detailed metrics
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
from pulsepipe.persistence import TrackingRepository, ProcessingStatus, ErrorCategory, DeidStat
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig

logger = LogFactory.get_logger(__name__)


class DeidOutcome(str, Enum):
    """Outcome of a de-identification operation."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL_SUCCESS = "partial_success"
    SKIPPED = "skipped"


class DeidStage(str, Enum):
    """Stage where de-identification outcome occurred."""
    PHI_DETECTION = "phi_detection"
    ENTITY_RECOGNITION = "entity_recognition"
    CONFIDENCE_SCORING = "confidence_scoring"
    PHI_REMOVAL = "phi_removal"
    VALIDATION = "validation"
    POST_PROCESSING = "post_processing"


@dataclass
class DeidRecord:
    """Individual de-identification record tracking information."""
    record_id: str
    source_id: Optional[str] = None
    content_type: Optional[str] = None  # clinical, operational, narrative
    outcome: Optional[DeidOutcome] = None
    stage: Optional[DeidStage] = None
    error_category: Optional[ErrorCategory] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    processing_time_ms: Optional[int] = None
    phi_entities_detected: Optional[int] = None
    phi_entities_removed: Optional[int] = None
    confidence_scores: Optional[Dict[str, float]] = None
    deid_method: Optional[str] = None
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class DeidBatchMetrics:
    """Metrics for a batch of de-identification operations."""
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
    total_phi_detected: int = 0
    total_phi_removed: int = 0
    avg_processing_time_ms: float = 0.0
    records_per_second: float = 0.0
    phi_per_second: float = 0.0
    success_rate: float = 0.0
    failure_rate: float = 0.0
    phi_removal_rate: float = 0.0
    avg_phi_per_record: float = 0.0
    avg_confidence_score: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    deid_methods: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_metrics(self) -> None:
        """Calculate derived metrics."""
        if self.completed_at:
            duration_seconds = (self.completed_at - self.started_at).total_seconds()
            if duration_seconds > 0:
                self.records_per_second = self.total_records / duration_seconds
                self.phi_per_second = self.total_phi_detected / duration_seconds
        
        if self.total_records > 0:
            self.success_rate = (self.successful_records / self.total_records) * 100
            self.failure_rate = (self.failed_records / self.total_records) * 100
            self.avg_processing_time_ms = self.total_processing_time_ms / self.total_records
            self.avg_phi_per_record = self.total_phi_detected / self.total_records
        
        if self.total_phi_detected > 0:
            self.phi_removal_rate = (self.total_phi_removed / self.total_phi_detected) * 100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['started_at'] = self.started_at.isoformat()
        data['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        return data


@dataclass
class DeidSummary:
    """Summary of de-identification operations across multiple batches."""
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
    total_phi_detected: int = 0
    total_phi_removed: int = 0
    records_per_second: float = 0.0
    phi_per_second: float = 0.0
    phi_removal_rate: float = 0.0
    avg_phi_per_record: float = 0.0
    avg_confidence_score: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    most_common_errors: List[Dict[str, Any]] = field(default_factory=list)
    performance_trends: List[Dict[str, Any]] = field(default_factory=list)
    deid_methods: List[str] = field(default_factory=list)
    content_types: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_batches(cls, pipeline_run_id: str, batches: List[DeidBatchMetrics]) -> 'DeidSummary':
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
        confidence_scores = []
        for batch in batches:
            summary.total_records += batch.total_records
            summary.successful_records += batch.successful_records
            summary.failed_records += batch.failed_records
            summary.skipped_records += batch.skipped_records
            summary.partial_success_records += batch.partial_success_records
            summary.total_processing_time_ms += batch.total_processing_time_ms
            summary.total_phi_detected += batch.total_phi_detected
            summary.total_phi_removed += batch.total_phi_removed
            
            if batch.avg_confidence_score > 0:
                confidence_scores.append(batch.avg_confidence_score)
            
            # Aggregate error categories
            for category, count in batch.errors_by_category.items():
                summary.errors_by_category[category] = summary.errors_by_category.get(category, 0) + count
            
            # Aggregate error stages
            for stage, count in batch.errors_by_stage.items():
                summary.errors_by_stage[stage] = summary.errors_by_stage.get(stage, 0) + count
            
            # Collect unique deid methods
            for deid_method in batch.deid_methods:
                if deid_method not in summary.deid_methods:
                    summary.deid_methods.append(deid_method)
            
            # Collect unique content types
            for content_type in batch.content_types:
                if content_type not in summary.content_types:
                    summary.content_types.append(content_type)
        
        # Calculate derived metrics
        if summary.total_records > 0:
            summary.success_rate = (summary.successful_records / summary.total_records) * 100
            summary.failure_rate = (summary.failed_records / summary.total_records) * 100
            summary.avg_processing_time_ms = summary.total_processing_time_ms / summary.total_records
            summary.avg_phi_per_record = summary.total_phi_detected / summary.total_records
        
        if summary.total_phi_detected > 0:
            summary.phi_removal_rate = (summary.total_phi_removed / summary.total_phi_detected) * 100
        
        if confidence_scores:
            summary.avg_confidence_score = sum(confidence_scores) / len(confidence_scores)
        
        if summary.time_range_start and summary.time_range_end:
            duration_seconds = (summary.time_range_end - summary.time_range_start).total_seconds()
            if duration_seconds > 0:
                summary.records_per_second = summary.total_records / duration_seconds
                summary.phi_per_second = summary.total_phi_detected / duration_seconds
        
        # Generate most common errors
        summary.most_common_errors = [
            {"category": cat, "count": count, "percentage": round((count / summary.failed_records) * 100, 1)}
            for cat, count in sorted(summary.errors_by_category.items(), key=lambda x: x[1], reverse=True)[:5]
        ] if summary.failed_records > 0 else []
        
        # Generate recommendations
        summary.recommendations = summary._generate_recommendations()
        
        return summary
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on de-identification metrics."""
        recommendations = []
        
        # Handle case with no data processed
        if self.total_records == 0:
            recommendations.append(
                "No records were processed for de-identification. Verify input data and pipeline configuration."
            )
            return recommendations
        
        # High failure rate recommendations
        if self.failure_rate > 20:
            recommendations.append(
                f"High failure rate ({self.failure_rate:.1f}%) detected in de-identification. "
                "Review PHI detection models and entity recognition patterns."
            )
        elif self.failure_rate > 10:
            recommendations.append(
                f"Moderate failure rate ({self.failure_rate:.1f}%) detected. "
                "Monitor de-identification parameters and confidence thresholds."
            )
        
        # Performance recommendations
        if self.avg_processing_time_ms > 2000:
            recommendations.append(
                f"Average de-identification time is high ({self.avg_processing_time_ms:.0f}ms per record). "
                "Consider optimizing NER models or implementing caching."
            )
        
        if self.phi_per_second < 10 and self.total_phi_detected > 50:
            recommendations.append(
                f"Low PHI processing throughput ({self.phi_per_second:.1f} entities/sec). "
                "Consider implementing batch processing for entity recognition."
            )
        
        # PHI detection and removal recommendations
        if self.phi_removal_rate < 95 and self.total_phi_detected > 0:
            recommendations.append(
                f"PHI removal rate is low ({self.phi_removal_rate:.1f}%). "
                "Review confidence thresholds and validation rules."
            )
        elif self.phi_removal_rate > 99.5:
            recommendations.append(
                f"PHI removal rate is very high ({self.phi_removal_rate:.1f}%). "
                "Verify that legitimate medical terms are not being over-removed."
            )
        
        # Confidence score recommendations
        if self.avg_confidence_score < 0.7 and self.total_phi_detected > 0:
            recommendations.append(
                f"Average confidence score is low ({self.avg_confidence_score:.2f}). "
                "Consider retraining PHI detection models or adjusting thresholds."
            )
        
        # PHI detection rate recommendations
        if self.avg_phi_per_record > 20:
            recommendations.append(
                f"High PHI detection rate ({self.avg_phi_per_record:.1f} entities per record). "
                "Verify detection accuracy and consider adjusting sensitivity."
            )
        elif self.avg_phi_per_record < 1 and len(self.content_types) > 0:
            # Only warn if we're processing clinical content
            clinical_content = any(ct in ['clinical', 'narrative'] for ct in self.content_types)
            if clinical_content:
                recommendations.append(
                    f"Low PHI detection rate ({self.avg_phi_per_record:.1f} entities per record). "
                    "Verify PHI detection models are properly configured for clinical content."
                )
        
        # Error pattern recommendations
        if self.most_common_errors:
            top_error = self.most_common_errors[0]
            if top_error['percentage'] > 50:
                recommendations.append(
                    f"Most errors ({top_error['percentage']}%) are '{top_error['category']}'. "
                    "Focus on addressing this specific de-identification error pattern."
                )
        
        if not recommendations:
            recommendations.append("De-identification performance appears healthy with no major issues identified.")
        
        return recommendations
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['generated_at'] = self.generated_at.isoformat()
        data['time_range_start'] = self.time_range_start.isoformat() if self.time_range_start else None
        data['time_range_end'] = self.time_range_end.isoformat() if self.time_range_end else None
        return data


class DeidTracker:
    """
    Comprehensive de-identification tracking system.
    
    Tracks success/failure rates, performance metrics, and error patterns
    for de-identification operations with export capabilities.
    """
    
    def __init__(self, pipeline_run_id: str, stage_name: str, 
                 config: DataIntelligenceConfig,
                 repository: Optional[TrackingRepository] = None):
        """
        Initialize de-identification tracker.
        
        Args:
            pipeline_run_id: Unique identifier for the pipeline run
            stage_name: Name of the de-identification stage
            config: Data intelligence configuration
            repository: Optional tracking repository for persistence
        """
        self.pipeline_run_id = pipeline_run_id
        self.stage_name = stage_name
        self.config = config
        self.repository = repository
        
        # Current batch being tracked
        self.current_batch: Optional[DeidBatchMetrics] = None
        self.batch_records: List[DeidRecord] = []
        
        # Completed batches
        self.completed_batches: List[DeidBatchMetrics] = []
        
        # Configuration
        self.enabled = config.is_feature_enabled('deid_tracking')
        self.detailed_tracking = config.is_feature_enabled('deid_tracking', 'detailed_tracking')
        self.auto_persist = config.is_feature_enabled('deid_tracking', 'auto_persist')
        
        if self.enabled:
            logger.info(f"De-identification tracker initialized for pipeline: {pipeline_run_id}, stage: {stage_name}")
    
    def is_enabled(self) -> bool:
        """Check if de-identification tracking is enabled."""
        return self.enabled
    
    @contextmanager
    def track_batch(self, batch_id: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Context manager for tracking a batch of de-identification operations.
        
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
        Start tracking a new batch of de-identification operations.
        
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
        
        self.current_batch = DeidBatchMetrics(
            batch_id=batch_id,
            pipeline_run_id=self.pipeline_run_id,
            stage_name=self.stage_name,
            started_at=datetime.now(),
            metadata=metadata or {}
        )
        self.batch_records = []
        
        logger.debug(f"Started tracking de-identification batch: {batch_id}")
    
    def record_success(self, record_id: str, source_id: Optional[str] = None,
                      content_type: Optional[str] = None, processing_time_ms: Optional[int] = None,
                      phi_entities_detected: Optional[int] = None, phi_entities_removed: Optional[int] = None,
                      confidence_scores: Optional[Dict[str, float]] = None, deid_method: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a successful de-identification operation.
        
        Args:
            record_id: Unique identifier for the record
            source_id: Source record identifier
            content_type: Type of content (clinical, operational, narrative)
            processing_time_ms: Time taken to process the record
            phi_entities_detected: Number of PHI entities detected
            phi_entities_removed: Number of PHI entities removed
            confidence_scores: Confidence scores for detected entities
            deid_method: Method used for de-identification
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = DeidRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=DeidOutcome.SUCCESS,
            processing_time_ms=processing_time_ms,
            phi_entities_detected=phi_entities_detected,
            phi_entities_removed=phi_entities_removed,
            confidence_scores=confidence_scores,
            deid_method=deid_method,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_failure(self, record_id: str, error: Exception, stage: DeidStage,
                      error_category: Optional[ErrorCategory] = None,
                      source_id: Optional[str] = None, content_type: Optional[str] = None,
                      processing_time_ms: Optional[int] = None, deid_method: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a failed de-identification operation.
        
        Args:
            record_id: Unique identifier for the record
            error: Exception that caused the failure
            stage: Stage where the failure occurred
            error_category: Category of the error
            source_id: Source record identifier
            content_type: Type of content being processed
            processing_time_ms: Time taken before failure
            deid_method: Method being used for de-identification
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = DeidRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=DeidOutcome.FAILURE,
            stage=stage,
            error_category=error_category,
            error_message=str(error),
            error_details={
                "error_type": type(error).__name__,
                "error_message": str(error)
            },
            processing_time_ms=processing_time_ms,
            deid_method=deid_method,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_skip(self, record_id: str, reason: str, source_id: Optional[str] = None,
                   content_type: Optional[str] = None, deid_method: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a skipped de-identification operation.
        
        Args:
            record_id: Unique identifier for the record
            reason: Reason for skipping
            source_id: Source record identifier
            content_type: Type of content
            deid_method: Method being used for de-identification
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = DeidRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=DeidOutcome.SKIPPED,
            error_message=reason,
            deid_method=deid_method,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_partial_success(self, record_id: str, issues: List[str], stage: DeidStage,
                              source_id: Optional[str] = None, content_type: Optional[str] = None,
                              processing_time_ms: Optional[int] = None, phi_entities_detected: Optional[int] = None,
                              phi_entities_removed: Optional[int] = None, confidence_scores: Optional[Dict[str, float]] = None,
                              deid_method: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a partially successful de-identification operation.
        
        Args:
            record_id: Unique identifier for the record
            issues: List of issues encountered
            stage: Stage where issues occurred
            source_id: Source record identifier
            content_type: Type of content
            processing_time_ms: Time taken to process
            phi_entities_detected: Number of PHI entities detected
            phi_entities_removed: Number of PHI entities removed
            confidence_scores: Confidence scores for detected entities
            deid_method: Method used for de-identification
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = DeidRecord(
            record_id=record_id,
            source_id=source_id,
            content_type=content_type,
            outcome=DeidOutcome.PARTIAL_SUCCESS,
            stage=stage,
            error_message="; ".join(issues),
            error_details={"issues": issues},
            processing_time_ms=processing_time_ms,
            phi_entities_detected=phi_entities_detected,
            phi_entities_removed=phi_entities_removed,
            confidence_scores=confidence_scores,
            deid_method=deid_method,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def _add_record(self, record: DeidRecord) -> None:
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
        
        if record.outcome == DeidOutcome.SUCCESS:
            batch.successful_records += 1
        elif record.outcome == DeidOutcome.FAILURE:
            batch.failed_records += 1
            
            # Track error categories and stages
            if record.error_category:
                cat = record.error_category.value
                batch.errors_by_category[cat] = batch.errors_by_category.get(cat, 0) + 1
            
            if record.stage:
                stage = record.stage.value
                batch.errors_by_stage[stage] = batch.errors_by_stage.get(stage, 0) + 1
                
        elif record.outcome == DeidOutcome.SKIPPED:
            batch.skipped_records += 1
        elif record.outcome == DeidOutcome.PARTIAL_SUCCESS:
            batch.partial_success_records += 1
        
        # Update totals
        if record.processing_time_ms:
            batch.total_processing_time_ms += record.processing_time_ms
        
        if record.phi_entities_detected:
            batch.total_phi_detected += record.phi_entities_detected
        
        if record.phi_entities_removed:
            batch.total_phi_removed += record.phi_entities_removed
        
        # Track deid methods and content types
        if record.deid_method and record.deid_method not in batch.deid_methods:
            batch.deid_methods.append(record.deid_method)
        
        if record.content_type and record.content_type not in batch.content_types:
            batch.content_types.append(record.content_type)
    
    def _persist_record(self, record: DeidRecord) -> None:
        """Persist record to repository."""
        if not self.repository:
            return
        
        try:
            # Convert to DeidStat
            stat = DeidStat(
                id=None,
                pipeline_run_id=self.pipeline_run_id,
                stage_name=self.stage_name,
                source_id=record.source_id,
                record_id=record.record_id,
                content_type=record.content_type,
                status=ProcessingStatus.SUCCESS if record.outcome == DeidOutcome.SUCCESS else ProcessingStatus.FAILURE,
                error_category=record.error_category,
                error_message=record.error_message,
                error_details=record.error_details,
                processing_time_ms=record.processing_time_ms,
                phi_entities_detected=record.phi_entities_detected,
                phi_entities_removed=record.phi_entities_removed,
                confidence_scores=record.confidence_scores,
                deid_method=record.deid_method,
                timestamp=record.timestamp
            )
            self.repository.record_deid_stat(stat)
        except Exception as e:
            logger.error(f"Failed to persist de-identification record: {e}")
    
    def finish_batch(self) -> Optional[DeidBatchMetrics]:
        """Finish the current batch and calculate final metrics."""
        if not self.enabled or not self.current_batch:
            return None
        
        # Mark batch as completed
        self.current_batch.completed_at = datetime.now()
        
        # Calculate final metrics
        self.current_batch.calculate_metrics()
        
        # Add to completed batches
        self.completed_batches.append(self.current_batch)
        
        logger.debug(f"Finished de-identification batch: {self.current_batch.batch_id} "
                    f"({self.current_batch.total_records} records, "
                    f"{self.current_batch.total_phi_detected} PHI detected, "
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
            "total_phi_detected": batch.total_phi_detected,
            "total_phi_removed": batch.total_phi_removed,
            "success_rate": success_rate,
            "duration_seconds": (datetime.now() - batch.started_at).total_seconds()
        }
    
    def get_summary(self) -> DeidSummary:
        """Get comprehensive summary of all de-identification operations."""
        all_batches = self.completed_batches.copy()
        
        # Include current batch if it exists
        if self.current_batch:
            temp_batch = DeidBatchMetrics(
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
                total_phi_detected=self.current_batch.total_phi_detected,
                total_phi_removed=self.current_batch.total_phi_removed,
                errors_by_category=self.current_batch.errors_by_category.copy(),
                errors_by_stage=self.current_batch.errors_by_stage.copy(),
                deid_methods=self.current_batch.deid_methods.copy(),
                content_types=self.current_batch.content_types.copy()
            )
            temp_batch.calculate_metrics()
            all_batches.append(temp_batch)
        
        return DeidSummary.from_batches(self.pipeline_run_id, all_batches)
    
    def export_metrics(self, file_path: str, format: str = "json", 
                      include_details: bool = False) -> None:
        """
        Export de-identification metrics to file.
        
        Args:
            file_path: Path to export file
            format: Export format (json, csv)
            include_details: Whether to include detailed record information
        """
        if not self.enabled:
            logger.warning("De-identification tracking is disabled, no metrics to export")
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
        
        logger.info(f"Exported de-identification metrics to: {file_path}")
    
    def _export_json(self, summary: DeidSummary, file_path: Path, 
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
                    "phi_detected": batch.total_phi_detected,
                    "phi_removed": batch.total_phi_removed,
                    "metadata": batch.metadata
                }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
    
    def _export_csv(self, summary: DeidSummary, file_path: Path, 
                   include_details: bool) -> None:
        """Export metrics to CSV format."""
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write summary header
            writer.writerow(["De-identification Summary Report"])
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
            writer.writerow(["Total PHI Detected", summary.total_phi_detected])
            writer.writerow(["Total PHI Removed", summary.total_phi_removed])
            writer.writerow(["PHI Removal Rate (%)", f"{summary.phi_removal_rate:.2f}"])
            writer.writerow(["Avg PHI Per Record", f"{summary.avg_phi_per_record:.2f}"])
            writer.writerow(["Avg Confidence Score", f"{summary.avg_confidence_score:.2f}"])
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
                writer.writerow(["Batch ID", "Total Records", "PHI Detected", "PHI Removed", "Success Rate (%)", "Duration (ms)"])
                for batch in self.completed_batches:
                    duration_ms = 0
                    if batch.completed_at:
                        duration_ms = (batch.completed_at - batch.started_at).total_seconds() * 1000
                    writer.writerow([
                        batch.batch_id,
                        batch.total_records,
                        batch.total_phi_detected,
                        batch.total_phi_removed,
                        f"{batch.success_rate:.2f}",
                        f"{duration_ms:.0f}"
                    ])
    
    def clear_history(self) -> None:
        """Clear completed batch history (keeps current batch)."""
        self.completed_batches.clear()
        logger.debug("Cleared de-identification tracking history")