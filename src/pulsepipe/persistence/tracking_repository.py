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

# src/pulsepipe/persistence/tracking_repository.py

"""
Data access layer for tracking and audit data.

Provides high-level methods for storing and retrieving ingestion statistics,
audit events, quality metrics, and performance data.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass

from pulsepipe.utils.log_factory import LogFactory
from .models import ProcessingStatus, ErrorCategory
from .database import DatabaseConnection, DatabaseDialect

logger = LogFactory.get_logger(__name__)


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
    id: Optional[int]
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
    id: Optional[int]
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


@dataclass
class ChunkingStat:
    """Individual chunking statistics record."""
    id: Optional[int]
    pipeline_run_id: str
    stage_name: str
    source_id: Optional[str]
    record_id: Optional[str]
    chunk_type: Optional[str]
    status: ProcessingStatus
    error_category: Optional[ErrorCategory]
    error_message: Optional[str]
    error_details: Optional[Dict[str, Any]]
    processing_time_ms: Optional[int]
    chunk_count: Optional[int]
    total_chars: Optional[int]
    avg_chunk_size: Optional[int]
    overlap_chars: Optional[int]
    chunker_type: Optional[str]
    timestamp: datetime


@dataclass
class DeidStat:
    """Individual de-identification statistics record."""
    id: Optional[int]
    pipeline_run_id: str
    stage_name: str
    source_id: Optional[str]
    record_id: Optional[str]
    content_type: Optional[str]
    status: ProcessingStatus
    error_category: Optional[ErrorCategory]
    error_message: Optional[str]
    error_details: Optional[Dict[str, Any]]
    processing_time_ms: Optional[int]
    phi_entities_detected: Optional[int]
    phi_entities_removed: Optional[int]
    confidence_scores: Optional[Dict[str, float]]
    deid_method: Optional[str]
    timestamp: datetime


@dataclass
class EmbeddingStat:
    """Individual embedding statistics record."""
    id: Optional[int]
    pipeline_run_id: str
    stage_name: str
    source_id: Optional[str]
    record_id: Optional[str]
    content_type: Optional[str]
    status: ProcessingStatus
    error_category: Optional[ErrorCategory]
    error_message: Optional[str]
    error_details: Optional[Dict[str, Any]]
    processing_time_ms: Optional[int]
    chunk_count: Optional[int]
    embedding_dimensions: Optional[int]
    model_name: Optional[str]
    timestamp: datetime


@dataclass
class VectorDbStat:
    """Individual vector database statistics record."""
    id: Optional[int]
    pipeline_run_id: str
    stage_name: str
    source_id: Optional[str]
    record_id: Optional[str]
    content_type: Optional[str]
    status: ProcessingStatus
    error_category: Optional[ErrorCategory]
    error_message: Optional[str]
    error_details: Optional[Dict[str, Any]]
    processing_time_ms: Optional[int]
    vector_count: Optional[int]
    index_name: Optional[str]
    collection_name: Optional[str]
    vector_store_type: Optional[str]
    timestamp: datetime


class TrackingRepository:
    """
    Repository for storing and retrieving tracking and audit data.
    
    Provides a high-level interface for data intelligence operations
    while abstracting away the database implementation details.
    """
    
    def __init__(self, connection: DatabaseConnection, dialect: DatabaseDialect):
        """
        Initialize tracking repository.
        
        Args:
            connection: Database connection (abstracted)
            dialect: SQL dialect for database-specific operations
        """
        self.conn = connection
        self.dialect = dialect
        
        # Initialize schema if the connection supports it
        if hasattr(self.conn, 'init_schema'):
            try:
                self.conn.init_schema()
                logger.debug("Database schema initialized successfully")
            except Exception as e:
                logger.warning(f"Could not initialize database schema: {e}")
                # Don't fail - the schema might already exist
    
    # Pipeline Run Management
    
    def start_pipeline_run(self, run_id: str, name: str, config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        """
        Record the start of a pipeline run.
        
        Args:
            run_id: Unique identifier for the pipeline run
            name: Pipeline name
            config_snapshot: Optional snapshot of configuration used
        """
        config_data = self.dialect.serialize_json(config_snapshot)
        
        sql = self.dialect.get_pipeline_run_insert()
        params = (run_id, name, self.dialect.format_datetime(datetime.now()), "running", config_data)
        
        self.conn.execute(sql, params)
        self.conn.commit()
        logger.debug(f"Started tracking pipeline run: {run_id}")
    
    def complete_pipeline_run(self, run_id: str, status: str = "completed", 
                            error_message: Optional[str] = None) -> None:
        """
        Mark a pipeline run as completed.
        
        Args:
            run_id: Pipeline run identifier
            status: Final status (completed, failed, cancelled)
            error_message: Optional error message if failed
        """
        sql = self.dialect.get_pipeline_run_update()
        now = self.dialect.format_datetime(datetime.now())
        params = (now, status, error_message, now, run_id)
        
        self.conn.execute(sql, params)
        self.conn.commit()
        logger.debug(f"Completed pipeline run: {run_id} with status: {status}")
    
    def update_pipeline_run_counts(self, run_id: str, total: int = 0, 
                                 successful: int = 0, failed: int = 0, skipped: int = 0) -> None:
        """
        Update record counts for a pipeline run.
        
        Args:
            run_id: Pipeline run identifier
            total: Total records processed
            successful: Successfully processed records
            failed: Failed records
            skipped: Skipped records
        """
        sql = self.dialect.get_pipeline_run_count_update()
        params = (total, successful, failed, skipped, self.dialect.format_datetime(datetime.now()), run_id)
        
        self.conn.execute(sql, params)
        self.conn.commit()
    
    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        """
        Get pipeline run summary by ID.
        
        Args:
            run_id: Pipeline run identifier
            
        Returns:
            PipelineRunSummary or None if not found
        """
        sql = self.dialect.get_pipeline_run_select()
        result = self.conn.execute(sql, (run_id,))
        
        row = result.fetchone()
        if not row:
            return None
        
        return PipelineRunSummary(
            id=row['id'],
            name=row['name'],
            started_at=self.dialect.parse_datetime(row['started_at']),
            completed_at=self.dialect.parse_datetime(row['completed_at']) if row['completed_at'] else None,
            status=row['status'],
            total_records=row['total_records'],
            successful_records=row['successful_records'],
            failed_records=row['failed_records'],
            skipped_records=row['skipped_records'],
            error_message=row['error_message']
        )
    
    # Ingestion Statistics
    
    def record_ingestion_stat(self, stat: IngestionStat) -> int:
        """
        Record an ingestion statistic.
        
        Args:
            stat: IngestionStat object to record
            
        Returns:
            ID of the inserted record
        """
        error_details_data = self.dialect.serialize_json(stat.error_details)
        
        sql = self.dialect.get_ingestion_stat_insert()
        params = (
            stat.pipeline_run_id, stat.stage_name, stat.file_path, stat.record_id,
            stat.record_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, error_details_data, stat.processing_time_ms,
            stat.record_size_bytes, stat.data_source, 
            self.dialect.format_datetime(stat.timestamp) if stat.timestamp else self.dialect.format_datetime(datetime.now())
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    def record_failed_record(self, ingestion_stat_id: int, original_data: str,
                           failure_reason: str, normalized_data: Optional[str] = None,
                           stack_trace: Optional[str] = None) -> int:
        """
        Store a complete failed record for analysis.
        
        Args:
            ingestion_stat_id: ID of related ingestion stat
            original_data: Original data that failed to process
            failure_reason: Reason for failure
            normalized_data: Partially processed data (if any)
            stack_trace: Full stack trace of the error
            
        Returns:
            ID of the inserted failed record
        """
        sql = self.dialect.get_failed_record_insert()
        params = (ingestion_stat_id, original_data, normalized_data, failure_reason, stack_trace)
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    # Quality Metrics
    
    def record_quality_metric(self, metric: QualityMetric) -> int:
        """
        Record a quality metric.
        
        Args:
            metric: QualityMetric object to record
            
        Returns:
            ID of the inserted record
        """
        missing_fields_json = self.dialect.serialize_json(metric.missing_fields) if metric.missing_fields else None
        invalid_fields_json = self.dialect.serialize_json(metric.invalid_fields) if metric.invalid_fields else None
        outlier_fields_json = self.dialect.serialize_json(metric.outlier_fields) if metric.outlier_fields else None
        quality_issues_json = self.dialect.serialize_json(metric.quality_issues) if metric.quality_issues else None
        metrics_details_json = self.dialect.serialize_json(metric.metrics_details) if metric.metrics_details else None
        
        sql = self.dialect.get_quality_metric_insert()
        params = (
            metric.pipeline_run_id, metric.record_id, metric.record_type,
            metric.completeness_score, metric.consistency_score, metric.validity_score,
            metric.accuracy_score, metric.overall_score, missing_fields_json,
            invalid_fields_json, outlier_fields_json, quality_issues_json,
            metrics_details_json, metric.sampled, 
            self.dialect.format_datetime(metric.timestamp) if metric.timestamp else self.dialect.format_datetime(datetime.now())
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    # Audit Events
    
    def record_audit_event(self, pipeline_run_id: str, event_type: str, stage_name: str,
                         message: str, event_level: str = "INFO", record_id: Optional[str] = None,
                         details: Optional[Dict[str, Any]] = None, correlation_id: Optional[str] = None) -> int:
        """
        Record an audit event.
        
        Args:
            pipeline_run_id: Pipeline run identifier
            event_type: Type of event (e.g., 'record_processed', 'validation_failed')
            stage_name: Pipeline stage where event occurred
            message: Human-readable event message
            event_level: Event level (DEBUG, INFO, WARNING, ERROR)
            record_id: Optional record identifier
            details: Optional additional event details
            correlation_id: Optional correlation identifier for tracking related events
            
        Returns:
            ID of the inserted audit event
        """
        details_json = self.dialect.serialize_json(details) if details else None
        
        sql = self.dialect.get_audit_event_insert()
        params = (
            pipeline_run_id, event_type, stage_name, record_id,
            event_level, message, details_json, correlation_id
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    # Performance Metrics
    
    def record_performance_metric(self, pipeline_run_id: str, stage_name: str,
                                started_at: datetime, completed_at: datetime,
                                records_processed: int = 0, memory_usage_mb: Optional[float] = None,
                                cpu_usage_percent: Optional[float] = None,
                                bottleneck_indicator: Optional[str] = None) -> int:
        """
        Record performance metrics for a pipeline stage.
        
        Args:
            pipeline_run_id: Pipeline run identifier
            stage_name: Name of the pipeline stage
            started_at: Stage start time
            completed_at: Stage completion time
            records_processed: Number of records processed
            memory_usage_mb: Peak memory usage in MB
            cpu_usage_percent: Average CPU usage percentage
            bottleneck_indicator: Description of any bottlenecks identified
            
        Returns:
            ID of the inserted performance metric
        """
        duration_ms = int((completed_at - started_at).total_seconds() * 1000)
        records_per_second = records_processed / (duration_ms / 1000) if duration_ms > 0 else 0
        
        sql = self.dialect.get_performance_metric_insert()
        params = (
            pipeline_run_id, stage_name, self.dialect.format_datetime(started_at), self.dialect.format_datetime(completed_at),
            duration_ms, records_processed, records_per_second,
            memory_usage_mb, cpu_usage_percent, bottleneck_indicator
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    def record_chunking_stat(self, stat: ChunkingStat) -> int:
        """
        Record a chunking statistic.
        
        Args:
            stat: ChunkingStat object to record
            
        Returns:
            ID of the inserted record
        """
        error_details_data = self.dialect.serialize_json(stat.error_details)
        confidence_scores_data = self.dialect.serialize_json(stat.confidence_scores if hasattr(stat, 'confidence_scores') else None)
        
        sql = self.dialect.get_chunking_stat_insert()
        params = (
            stat.pipeline_run_id, stat.stage_name, stat.source_id, stat.record_id,
            stat.chunk_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, error_details_data, stat.processing_time_ms,
            stat.chunk_count, stat.total_chars, stat.avg_chunk_size,
            stat.overlap_chars, stat.chunker_type,
            self.dialect.format_datetime(stat.timestamp) if stat.timestamp else self.dialect.format_datetime(datetime.now())
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    def record_deid_stat(self, stat: DeidStat) -> int:
        """
        Record a de-identification statistic.
        
        Args:
            stat: DeidStat object to record
            
        Returns:
            ID of the inserted record
        """
        error_details_data = self.dialect.serialize_json(stat.error_details)
        confidence_scores_data = self.dialect.serialize_json(stat.confidence_scores)
        
        sql = self.dialect.get_deid_stat_insert()
        params = (
            stat.pipeline_run_id, stat.stage_name, stat.source_id, stat.record_id,
            stat.content_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, error_details_data, stat.processing_time_ms,
            stat.phi_entities_detected, stat.phi_entities_removed, confidence_scores_data,
            stat.deid_method,
            self.dialect.format_datetime(stat.timestamp) if stat.timestamp else self.dialect.format_datetime(datetime.now())
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    def record_embedding_stat(self, stat: EmbeddingStat) -> int:
        """
        Record an embedding statistic.
        
        Args:
            stat: EmbeddingStat object to record
            
        Returns:
            ID of the inserted record
        """
        error_details_data = self.dialect.serialize_json(stat.error_details)
        
        sql = self.dialect.get_embedding_stat_insert()
        params = (
            stat.pipeline_run_id, stat.stage_name, stat.source_id, stat.record_id,
            stat.content_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, error_details_data, stat.processing_time_ms,
            stat.chunk_count, stat.embedding_dimensions, stat.model_name,
            self.dialect.format_datetime(stat.timestamp) if stat.timestamp else self.dialect.format_datetime(datetime.now())
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    def record_vector_db_stat(self, stat: VectorDbStat) -> int:
        """
        Record a vector database statistic.
        
        Args:
            stat: VectorDbStat object to record
            
        Returns:
            ID of the inserted record
        """
        error_details_data = self.dialect.serialize_json(stat.error_details)
        
        sql = self.dialect.get_vector_db_stat_insert()
        params = (
            stat.pipeline_run_id, stat.stage_name, stat.source_id, stat.record_id,
            stat.content_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, error_details_data, stat.processing_time_ms,
            stat.vector_count, stat.index_name, stat.collection_name,
            stat.vector_store_type,
            self.dialect.format_datetime(stat.timestamp) if stat.timestamp else self.dialect.format_datetime(datetime.now())
        )
        
        result = self.conn.execute(sql, params)
        self.conn.commit()
        return result.lastrowid
    
    # Analytics and Reporting
    
    def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                            start_date: Optional[datetime] = None,
                            end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get ingestion statistics summary.
        
        Args:
            pipeline_run_id: Optional pipeline run to filter by
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Dictionary with ingestion summary statistics
        """
        sql, params = self.dialect.get_ingestion_summary(pipeline_run_id, start_date, end_date)
        result = self.conn.execute(sql, params)
        results = result.fetchall()
        
        summary = {
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
            "error_breakdown": {},
            "avg_processing_time_ms": 0,
            "total_bytes_processed": 0
        }
        
        total_processing_time = 0
        total_records = 0
        
        for row in results:
            count = row['count']
            total_records += count
            summary["total_records"] += count
            
            if row.get('avg_processing_time'):
                total_processing_time += row['avg_processing_time'] * count
            
            if row.get('total_bytes'):
                summary["total_bytes_processed"] += row['total_bytes']
            
            status = row['status']
            if status == ProcessingStatus.SUCCESS.value:
                summary["successful_records"] += count
            elif status == ProcessingStatus.FAILURE.value:
                summary["failed_records"] += count
                error_category = row.get('error_category') or 'unknown'
                summary["error_breakdown"][error_category] = summary["error_breakdown"].get(error_category, 0) + count
            elif status == ProcessingStatus.SKIPPED.value:
                summary["skipped_records"] += count
        
        if total_records > 0:
            summary["avg_processing_time_ms"] = total_processing_time / total_records
        
        return summary
    
    def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get quality metrics summary.
        
        Args:
            pipeline_run_id: Optional pipeline run to filter by
            
        Returns:
            Dictionary with quality summary statistics
        """
        sql, params = self.dialect.get_quality_summary(pipeline_run_id)
        result = self.conn.execute(sql, params)
        results = result.fetchall()
        
        if not results:
            return {
                "total_records": 0,
                "avg_completeness_score": None,
                "avg_consistency_score": None, 
                "avg_validity_score": None,
                "avg_accuracy_score": None,
                "avg_overall_score": None,
                "min_overall_score": None,
                "max_overall_score": None
            }
        
        row = results[0]
        return {
            "total_records": row.get('total_records', 0),
            "avg_completeness_score": row.get('avg_completeness'),
            "avg_consistency_score": row.get('avg_consistency'), 
            "avg_validity_score": row.get('avg_validity'),
            "avg_accuracy_score": row.get('avg_accuracy'),
            "avg_overall_score": row.get('avg_overall_score'),
            "min_overall_score": row.get('min_score'),
            "max_overall_score": row.get('max_score')
        }
    
    def get_recent_pipeline_runs(self, limit: int = 10) -> List[PipelineRunSummary]:
        """
        Get recent pipeline runs.
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            List of PipelineRunSummary objects
        """
        sql = self.dialect.get_recent_pipeline_runs(limit)
        result = self.conn.execute(sql, ())
        results = result.fetchall()
        
        return [
            PipelineRunSummary(
                id=row['id'],
                name=row['name'],
                started_at=self.dialect.parse_datetime(row['started_at']) if row['started_at'] else datetime.now(),
                completed_at=self.dialect.parse_datetime(row['completed_at']) if row['completed_at'] else None,
                status=row['status'],
                total_records=row['total_records'] or 0,
                successful_records=row['successful_records'] or 0,
                failed_records=row['failed_records'] or 0,
                skipped_records=row['skipped_records'] or 0,
                error_message=row['error_message']
            )
            for row in results
        ]
    
    def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        """
        Clean up old tracking data.
        
        Args:
            days_to_keep: Number of days of data to keep
            
        Returns:
            Number of records deleted
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # Use dialect-specific cleanup SQL
        cleanup_statements = self.dialect.get_cleanup(cutoff_date)
        
        total_deleted = 0
        
        # Execute each cleanup statement in order
        for sql, params in cleanup_statements:
            try:
                result = self.conn.execute(sql, params)
                # Only count positive rowcounts to avoid negative totals from failed operations
                if result.rowcount > 0:
                    total_deleted += result.rowcount
                    logger.debug(f"Deleted {result.rowcount} rows from cleanup SQL: {sql[:50]}...")
            except Exception as e:
                logger.error(f"Error executing cleanup SQL: {sql[:100]}... Error: {e}")
                # Continue with other statements even if one fails
                continue
        
        self.conn.commit()
        logger.info(f"Cleaned up {total_deleted} old tracking records older than {days_to_keep} days")
        
        return total_deleted