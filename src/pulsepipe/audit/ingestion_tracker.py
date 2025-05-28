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

# src/pulsepipe/audit/ingestion_tracker.py

"""
Ingestion success/failure tracking system for PulsePipe.

Provides comprehensive tracking of ingestion operations with detailed metrics
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
from pulsepipe.persistence import ProcessingStatus, ErrorCategory, IngestionStat
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig

logger = LogFactory.get_logger(__name__)


class IngestionOutcome(str, Enum):
    """Outcome of an ingestion operation."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL_SUCCESS = "partial_success"
    SKIPPED = "skipped"


class IngestionStage(str, Enum):
    """Stage where ingestion outcome occurred."""
    PARSING = "parsing"
    VALIDATION = "validation"
    TRANSFORMATION = "transformation"
    PERSISTENCE = "persistence"
    POST_PROCESSING = "post_processing"


@dataclass
class IngestionRecord:
    """Individual record tracking information."""
    record_id: str
    record_type: Optional[str] = None
    file_path: Optional[str] = None
    outcome: Optional[IngestionOutcome] = None
    stage: Optional[IngestionStage] = None
    error_category: Optional[ErrorCategory] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    processing_time_ms: Optional[int] = None
    record_size_bytes: Optional[int] = None
    data_source: Optional[str] = None
    timestamp: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            self.timestamp = datetime.now()


@dataclass
class IngestionBatchMetrics:
    """Metrics for a batch of ingestion operations."""
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
    total_bytes_processed: int = 0
    avg_processing_time_ms: float = 0.0
    records_per_second: float = 0.0
    bytes_per_second: float = 0.0
    success_rate: float = 0.0
    failure_rate: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    data_sources: List[str] = field(default_factory=list)
    file_paths: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def calculate_metrics(self) -> None:
        """Calculate derived metrics."""
        if self.completed_at:
            duration_seconds = (self.completed_at - self.started_at).total_seconds()
            if duration_seconds > 0:
                self.records_per_second = self.total_records / duration_seconds
                self.bytes_per_second = self.total_bytes_processed / duration_seconds
        
        if self.total_records > 0:
            self.success_rate = (self.successful_records / self.total_records) * 100
            self.failure_rate = (self.failed_records / self.total_records) * 100
        
        if self.total_records > 0:
            self.avg_processing_time_ms = self.total_processing_time_ms / self.total_records
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['started_at'] = self.started_at.isoformat()
        data['completed_at'] = self.completed_at.isoformat() if self.completed_at else None
        return data


@dataclass
class IngestionSummary:
    """Summary of ingestion operations across multiple batches."""
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
    total_bytes_processed: int = 0
    records_per_second: float = 0.0
    bytes_per_second: float = 0.0
    errors_by_category: Dict[str, int] = field(default_factory=dict)
    errors_by_stage: Dict[str, int] = field(default_factory=dict)
    most_common_errors: List[Dict[str, Any]] = field(default_factory=list)
    performance_trends: List[Dict[str, Any]] = field(default_factory=list)
    data_sources: List[str] = field(default_factory=list)
    recommendations: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def from_batches(cls, pipeline_run_id: str, batches: List[IngestionBatchMetrics]) -> 'IngestionSummary':
        """Create summary from list of batch metrics."""
        summary = cls(
            pipeline_run_id=pipeline_run_id,
            generated_at=datetime.now(),
            total_batches=len(batches)
        )
        
        if not batches:
            # Generate recommendations even for empty batches
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
            summary.total_bytes_processed += batch.total_bytes_processed
            
            # Aggregate error categories
            for category, count in batch.errors_by_category.items():
                summary.errors_by_category[category] = summary.errors_by_category.get(category, 0) + count
            
            # Aggregate error stages
            for stage, count in batch.errors_by_stage.items():
                summary.errors_by_stage[stage] = summary.errors_by_stage.get(stage, 0) + count
            
            # Collect unique data sources
            for source in batch.data_sources:
                if source not in summary.data_sources:
                    summary.data_sources.append(source)
        
        # Calculate derived metrics
        if summary.total_records > 0:
            summary.success_rate = (summary.successful_records / summary.total_records) * 100
            summary.failure_rate = (summary.failed_records / summary.total_records) * 100
            summary.avg_processing_time_ms = summary.total_processing_time_ms / summary.total_records
        
        if summary.time_range_start and summary.time_range_end:
            duration_seconds = (summary.time_range_end - summary.time_range_start).total_seconds()
            if duration_seconds > 0:
                summary.records_per_second = summary.total_records / duration_seconds
                summary.bytes_per_second = summary.total_bytes_processed / duration_seconds
        
        # Generate most common errors
        summary.most_common_errors = [
            {"category": cat, "count": count, "percentage": round((count / summary.failed_records) * 100, 1)}
            for cat, count in sorted(summary.errors_by_category.items(), key=lambda x: x[1], reverse=True)[:5]
        ] if summary.failed_records > 0 else []
        
        # Generate recommendations
        summary.recommendations = summary._generate_recommendations()
        
        return summary
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on ingestion metrics."""
        recommendations = []
        
        # Handle case with no data processed
        if self.total_records == 0:
            recommendations.append(
                "No records were processed. Verify data sources and pipeline configuration."
            )
            return recommendations
        
        # High failure rate recommendations
        if self.failure_rate > 20:
            recommendations.append(
                f"High failure rate ({self.failure_rate:.1f}%) detected. "
                "Review data quality and implement better validation."
            )
        elif self.failure_rate > 10:
            recommendations.append(
                f"Moderate failure rate ({self.failure_rate:.1f}%) detected. "
                "Monitor data sources and consider implementing retry mechanisms."
            )
        
        # Performance recommendations
        if self.avg_processing_time_ms > 500:
            recommendations.append(
                f"Average processing time is high ({self.avg_processing_time_ms:.0f}ms per record). "
                "Consider optimizing parsers and data transformations."
            )
        
        if self.records_per_second < 10 and self.total_records > 100:
            recommendations.append(
                f"Low throughput detected ({self.records_per_second:.1f} records/sec). "
                "Consider implementing batch processing and parallel execution."
            )
        
        # Error pattern recommendations
        if self.most_common_errors:
            top_error = self.most_common_errors[0]
            if top_error['percentage'] > 50:
                recommendations.append(
                    f"Most errors ({top_error['percentage']}%) are '{top_error['category']}'. "
                    "Focus on addressing this specific error pattern."
                )
        
        # Data source diversity
        if len(self.data_sources) > 5:
            recommendations.append(
                f"Processing data from {len(self.data_sources)} different sources. "
                "Consider implementing source-specific validation rules."
            )
        
        if not recommendations:
            recommendations.append("Ingestion performance appears healthy with no major issues identified.")
        
        return recommendations
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        data = asdict(self)
        # Convert datetime objects to ISO strings
        data['generated_at'] = self.generated_at.isoformat()
        data['time_range_start'] = self.time_range_start.isoformat() if self.time_range_start else None
        data['time_range_end'] = self.time_range_end.isoformat() if self.time_range_end else None
        return data


class IngestionTracker:
    """
    Comprehensive ingestion tracking system.
    
    Tracks success/failure rates, performance metrics, and error patterns
    for ingestion operations with export capabilities.
    """
    
    def __init__(self, pipeline_run_id: str, stage_name: str, 
                 config: DataIntelligenceConfig,
                 repository: Optional[Any] = None):
        """
        Initialize ingestion tracker.
        
        Args:
            pipeline_run_id: Unique identifier for the pipeline run
            stage_name: Name of the ingestion stage
            config: Data intelligence configuration
            repository: Optional tracking repository for persistence (legacy)
        """
        self.pipeline_run_id = pipeline_run_id
        self.stage_name = stage_name
        self.config = config
        self.repository = repository
        
        # Current batch being tracked
        self.current_batch: Optional[IngestionBatchMetrics] = None
        self.batch_records: List[IngestionRecord] = []
        
        # Completed batches
        self.completed_batches: List[IngestionBatchMetrics] = []
        
        # Configuration
        self.enabled = config.is_feature_enabled('ingestion_tracking')
        self.detailed_tracking = config.is_feature_enabled('ingestion_tracking', 'detailed_tracking')
        self.auto_persist = config.is_feature_enabled('ingestion_tracking', 'auto_persist')
        
        if self.enabled:
            logger.info(f"Ingestion tracker initialized for pipeline: {pipeline_run_id}, stage: {stage_name}")
    
    def is_enabled(self) -> bool:
        """Check if ingestion tracking is enabled."""
        return self.enabled
    
    @contextmanager
    def track_batch(self, batch_id: str, metadata: Optional[Dict[str, Any]] = None):
        """
        Context manager for tracking a batch of ingestion operations.
        
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
        Start tracking a new batch of ingestion operations.
        
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
        
        self.current_batch = IngestionBatchMetrics(
            batch_id=batch_id,
            pipeline_run_id=self.pipeline_run_id,
            stage_name=self.stage_name,
            started_at=datetime.now(),
            metadata=metadata or {}
        )
        self.batch_records = []
        
        logger.debug(f"Started tracking batch: {batch_id}")
    
    def record_success(self, record_id: str, record_type: Optional[str] = None,
                      file_path: Optional[str] = None, processing_time_ms: Optional[int] = None,
                      record_size_bytes: Optional[int] = None, data_source: Optional[str] = None,
                      metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a successful ingestion operation.
        
        Args:
            record_id: Unique identifier for the record
            record_type: Type of the record (e.g., 'Patient', 'Observation')
            file_path: Source file path
            processing_time_ms: Time taken to process the record
            record_size_bytes: Size of the record in bytes
            data_source: Source system or format
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = IngestionRecord(
            record_id=record_id,
            record_type=record_type,
            file_path=file_path,
            outcome=IngestionOutcome.SUCCESS,
            processing_time_ms=processing_time_ms,
            record_size_bytes=record_size_bytes,
            data_source=data_source,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_failure(self, record_id: str, error: Exception, stage: IngestionStage,
                      error_category: Optional[ErrorCategory] = None,
                      record_type: Optional[str] = None, file_path: Optional[str] = None,
                      processing_time_ms: Optional[int] = None, record_size_bytes: Optional[int] = None,
                      data_source: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a failed ingestion operation.
        
        Args:
            record_id: Unique identifier for the record
            error: Exception that caused the failure
            stage: Stage where the failure occurred
            error_category: Category of the error
            record_type: Type of the record
            file_path: Source file path
            processing_time_ms: Time taken before failure
            record_size_bytes: Size of the record in bytes
            data_source: Source system or format
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = IngestionRecord(
            record_id=record_id,
            record_type=record_type,
            file_path=file_path,
            outcome=IngestionOutcome.FAILURE,
            stage=stage,
            error_category=error_category,
            error_message=str(error),
            error_details={
                "error_type": type(error).__name__,
                "error_message": str(error)
            },
            processing_time_ms=processing_time_ms,
            record_size_bytes=record_size_bytes,
            data_source=data_source,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_skip(self, record_id: str, reason: str, record_type: Optional[str] = None,
                   file_path: Optional[str] = None, data_source: Optional[str] = None,
                   metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a skipped ingestion operation.
        
        Args:
            record_id: Unique identifier for the record
            reason: Reason for skipping
            record_type: Type of the record
            file_path: Source file path
            data_source: Source system or format
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = IngestionRecord(
            record_id=record_id,
            record_type=record_type,
            file_path=file_path,
            outcome=IngestionOutcome.SKIPPED,
            error_message=reason,
            data_source=data_source,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def record_partial_success(self, record_id: str, issues: List[str], stage: IngestionStage,
                              record_type: Optional[str] = None, file_path: Optional[str] = None,
                              processing_time_ms: Optional[int] = None, record_size_bytes: Optional[int] = None,
                              data_source: Optional[str] = None, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Record a partially successful ingestion operation.
        
        Args:
            record_id: Unique identifier for the record
            issues: List of issues encountered
            stage: Stage where issues occurred
            record_type: Type of the record
            file_path: Source file path
            processing_time_ms: Time taken to process
            record_size_bytes: Size of the record in bytes
            data_source: Source system or format
            metadata: Additional record metadata
        """
        if not self.enabled:
            return
        
        record = IngestionRecord(
            record_id=record_id,
            record_type=record_type,
            file_path=file_path,
            outcome=IngestionOutcome.PARTIAL_SUCCESS,
            stage=stage,
            error_message="; ".join(issues),
            error_details={"issues": issues},
            processing_time_ms=processing_time_ms,
            record_size_bytes=record_size_bytes,
            data_source=data_source,
            metadata=metadata or {}
        )
        
        self._add_record(record)
        
        # Persist to repository if enabled and available
        if self.auto_persist and self.repository:
            self._persist_record(record)
    
    def _add_record(self, record: IngestionRecord) -> None:
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
        
        if record.outcome == IngestionOutcome.SUCCESS:
            batch.successful_records += 1
        elif record.outcome == IngestionOutcome.FAILURE:
            batch.failed_records += 1
            
            # Track error categories and stages
            if record.error_category:
                cat = record.error_category.value
                batch.errors_by_category[cat] = batch.errors_by_category.get(cat, 0) + 1
            
            if record.stage:
                stage = record.stage.value
                batch.errors_by_stage[stage] = batch.errors_by_stage.get(stage, 0) + 1
                
        elif record.outcome == IngestionOutcome.SKIPPED:
            batch.skipped_records += 1
        elif record.outcome == IngestionOutcome.PARTIAL_SUCCESS:
            batch.partial_success_records += 1
        
        # Update totals
        if record.processing_time_ms:
            batch.total_processing_time_ms += record.processing_time_ms
        
        if record.record_size_bytes:
            batch.total_bytes_processed += record.record_size_bytes
        
        # Track data sources and file paths
        if record.data_source and record.data_source not in batch.data_sources:
            batch.data_sources.append(record.data_source)
        
        if record.file_path and record.file_path not in batch.file_paths:
            batch.file_paths.append(record.file_path)
    
    def _persist_record(self, record: IngestionRecord) -> None:
        """Persist record to repository."""
        if not self.repository:
            return
        
        try:
            # Convert to IngestionStat
            stat = IngestionStat(
                id=None,
                pipeline_run_id=self.pipeline_run_id,
                stage_name=self.stage_name,
                file_path=record.file_path,
                record_id=record.record_id,
                record_type=record.record_type,
                status=ProcessingStatus.SUCCESS if record.outcome == IngestionOutcome.SUCCESS else ProcessingStatus.FAILURE,
                error_category=record.error_category,
                error_message=record.error_message,
                error_details=record.error_details,
                processing_time_ms=record.processing_time_ms,
                record_size_bytes=record.record_size_bytes,
                data_source=record.data_source,
                timestamp=record.timestamp
            )
            
            self.repository.record_ingestion_stat(stat)
        except Exception as e:
            logger.error(f"Failed to persist ingestion record: {e}")
    
    def finish_batch(self) -> Optional[IngestionBatchMetrics]:
        """Finish the current batch and calculate final metrics."""
        if not self.enabled or not self.current_batch:
            return None
        
        # Mark batch as completed
        self.current_batch.completed_at = datetime.now()
        
        # Calculate final metrics
        self.current_batch.calculate_metrics()
        
        # Add to completed batches
        self.completed_batches.append(self.current_batch)
        
        logger.debug(f"Finished batch: {self.current_batch.batch_id} "
                    f"({self.current_batch.total_records} records, "
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
            "success_rate": success_rate,
            "duration_seconds": (datetime.now() - batch.started_at).total_seconds()
        }
    
    def get_summary(self) -> IngestionSummary:
        """Get comprehensive summary of all ingestion operations."""
        all_batches = self.completed_batches.copy()
        
        # Include current batch if it exists
        if self.current_batch:
            temp_batch = IngestionBatchMetrics(
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
                total_bytes_processed=self.current_batch.total_bytes_processed,
                errors_by_category=self.current_batch.errors_by_category.copy(),
                errors_by_stage=self.current_batch.errors_by_stage.copy(),
                data_sources=self.current_batch.data_sources.copy(),
                file_paths=self.current_batch.file_paths.copy()
            )
            temp_batch.calculate_metrics()
            all_batches.append(temp_batch)
        
        return IngestionSummary.from_batches(self.pipeline_run_id, all_batches)
    
    def export_metrics(self, file_path: str, format: str = "json", 
                      include_details: bool = False) -> None:
        """
        Export ingestion metrics to file.
        
        Args:
            file_path: Path to export file
            format: Export format (json, csv)
            include_details: Whether to include detailed record information
        """
        if not self.enabled:
            logger.warning("Ingestion tracking is disabled, no metrics to export")
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
        
        logger.info(f"Exported ingestion metrics to: {file_path}")
    
    def _export_json(self, summary: IngestionSummary, file_path: Path, 
                    include_details: bool) -> None:
        """Export metrics to JSON format."""
        export_data = {
            "summary": summary.to_dict(),
            "completed_batches": [batch.to_dict() for batch in self.completed_batches]
        }
        
        if include_details and self.detailed_tracking:
            export_data["batch_details"] = {}
            for i, batch in enumerate(self.completed_batches):
                batch_records = []
                # Note: In a real implementation, you'd store batch records separately
                # For now, we'll include available metadata
                export_data["batch_details"][batch.batch_id] = {
                    "records_count": batch.total_records,
                    "metadata": batch.metadata
                }
        
        with open(file_path, 'w') as f:
            json.dump(export_data, f, indent=2, default=str)
    
    def _export_csv(self, summary: IngestionSummary, file_path: Path, 
                   include_details: bool) -> None:
        """Export metrics to CSV format."""
        with open(file_path, 'w', newline='') as f:
            writer = csv.writer(f)
            
            # Write summary header
            writer.writerow(["Ingestion Summary Report"])
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
            writer.writerow(["Avg Processing Time (ms)", f"{summary.avg_processing_time_ms:.2f}"])
            writer.writerow(["Records Per Second", f"{summary.records_per_second:.2f}"])
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
                writer.writerow(["Batch ID", "Total Records", "Success Rate (%)", "Failure Rate (%)", "Duration (ms)"])
                for batch in self.completed_batches:
                    duration_ms = 0
                    if batch.completed_at:
                        duration_ms = (batch.completed_at - batch.started_at).total_seconds() * 1000
                    writer.writerow([
                        batch.batch_id,
                        batch.total_records,
                        f"{batch.success_rate:.2f}",
                        f"{batch.failure_rate:.2f}",
                        f"{duration_ms:.0f}"
                    ])
    
    def clear_history(self) -> None:
        """Clear completed batch history (keeps current batch)."""
        self.completed_batches.clear()
        logger.debug("Cleared ingestion tracking history")