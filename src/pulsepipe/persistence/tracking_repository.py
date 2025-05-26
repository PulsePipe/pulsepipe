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

import sqlite3
import json
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass

from pulsepipe.utils.log_factory import LogFactory
from .models import ProcessingStatus, ErrorCategory

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


class TrackingRepository:
    """
    Repository for storing and retrieving tracking and audit data.
    
    Provides a high-level interface for data intelligence operations
    while abstracting away the database implementation details.
    """
    
    def __init__(self, connection: sqlite3.Connection):
        """
        Initialize tracking repository.
        
        Args:
            connection: SQLite database connection
        """
        self.conn = connection
        self.conn.row_factory = sqlite3.Row  # Enable column access by name
    
    # Pipeline Run Management
    
    def start_pipeline_run(self, run_id: str, name: str, config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        """
        Record the start of a pipeline run.
        
        Args:
            run_id: Unique identifier for the pipeline run
            name: Pipeline name
            config_snapshot: Optional snapshot of configuration used
        """
        config_json = json.dumps(config_snapshot) if config_snapshot else None
        
        self.conn.execute("""
            INSERT INTO pipeline_runs (
                id, name, started_at, status, config_snapshot
            ) VALUES (?, ?, ?, ?, ?)
        """, (run_id, name, datetime.now(), "running", config_json))
        
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
        self.conn.execute("""
            UPDATE pipeline_runs 
            SET completed_at = ?, status = ?, error_message = ?, updated_at = ?
            WHERE id = ?
        """, (datetime.now(), status, error_message, datetime.now(), run_id))
        
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
        self.conn.execute("""
            UPDATE pipeline_runs 
            SET total_records = total_records + ?,
                successful_records = successful_records + ?,
                failed_records = failed_records + ?,
                skipped_records = skipped_records + ?,
                updated_at = ?
            WHERE id = ?
        """, (total, successful, failed, skipped, datetime.now(), run_id))
        
        self.conn.commit()
    
    def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        """
        Get pipeline run summary by ID.
        
        Args:
            run_id: Pipeline run identifier
            
        Returns:
            PipelineRunSummary or None if not found
        """
        cursor = self.conn.execute("""
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records, 
                   skipped_records, error_message
            FROM pipeline_runs 
            WHERE id = ?
        """, (run_id,))
        
        row = cursor.fetchone()
        if not row:
            return None
        
        return PipelineRunSummary(
            id=row['id'],
            name=row['name'],
            started_at=datetime.fromisoformat(row['started_at']),
            completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
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
        error_details_json = json.dumps(stat.error_details) if stat.error_details else None
        
        cursor = self.conn.execute("""
            INSERT INTO ingestion_stats (
                pipeline_run_id, stage_name, file_path, record_id, record_type,
                status, error_category, error_message, error_details,
                processing_time_ms, record_size_bytes, data_source, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            stat.pipeline_run_id, stat.stage_name, stat.file_path, stat.record_id,
            stat.record_type, stat.status.value if stat.status else None,
            stat.error_category.value if stat.error_category else None,
            stat.error_message, error_details_json, stat.processing_time_ms,
            stat.record_size_bytes, stat.data_source, stat.timestamp
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
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
        cursor = self.conn.execute("""
            INSERT INTO failed_records (
                ingestion_stat_id, original_data, normalized_data,
                failure_reason, stack_trace
            ) VALUES (?, ?, ?, ?, ?)
        """, (ingestion_stat_id, original_data, normalized_data, failure_reason, stack_trace))
        
        self.conn.commit()
        return cursor.lastrowid
    
    # Quality Metrics
    
    def record_quality_metric(self, metric: QualityMetric) -> int:
        """
        Record a quality metric.
        
        Args:
            metric: QualityMetric object to record
            
        Returns:
            ID of the inserted record
        """
        missing_fields_json = json.dumps(metric.missing_fields) if metric.missing_fields else None
        invalid_fields_json = json.dumps(metric.invalid_fields) if metric.invalid_fields else None
        outlier_fields_json = json.dumps(metric.outlier_fields) if metric.outlier_fields else None
        quality_issues_json = json.dumps(metric.quality_issues) if metric.quality_issues else None
        metrics_details_json = json.dumps(metric.metrics_details) if metric.metrics_details else None
        
        cursor = self.conn.execute("""
            INSERT INTO quality_metrics (
                pipeline_run_id, record_id, record_type,
                completeness_score, consistency_score, validity_score,
                accuracy_score, overall_score, missing_fields,
                invalid_fields, outlier_fields, quality_issues,
                metrics_details, sampled, timestamp
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            metric.pipeline_run_id, metric.record_id, metric.record_type,
            metric.completeness_score, metric.consistency_score, metric.validity_score,
            metric.accuracy_score, metric.overall_score, missing_fields_json,
            invalid_fields_json, outlier_fields_json, quality_issues_json,
            metrics_details_json, metric.sampled, metric.timestamp or datetime.now()
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
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
        details_json = json.dumps(details) if details else None
        
        cursor = self.conn.execute("""
            INSERT INTO audit_events (
                pipeline_run_id, event_type, stage_name, record_id,
                event_level, message, details, correlation_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline_run_id, event_type, stage_name, record_id,
            event_level, message, details_json, correlation_id
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
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
        
        cursor = self.conn.execute("""
            INSERT INTO performance_metrics (
                pipeline_run_id, stage_name, started_at, completed_at,
                duration_ms, records_processed, records_per_second,
                memory_usage_mb, cpu_usage_percent, bottleneck_indicator
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pipeline_run_id, stage_name, started_at, completed_at,
            duration_ms, records_processed, records_per_second,
            memory_usage_mb, cpu_usage_percent, bottleneck_indicator
        ))
        
        self.conn.commit()
        return cursor.lastrowid
    
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
        where_conditions = []
        params = []
        
        if pipeline_run_id:
            where_conditions.append("pipeline_run_id = ?")
            params.append(pipeline_run_id)
        
        if start_date:
            where_conditions.append("timestamp >= ?")
            params.append(start_date)
        
        if end_date:
            where_conditions.append("timestamp <= ?")
            params.append(end_date)
        
        where_clause = "WHERE " + " AND ".join(where_conditions) if where_conditions else ""
        
        cursor = self.conn.execute(f"""
            SELECT 
                status,
                error_category,
                COUNT(*) as count,
                AVG(processing_time_ms) as avg_processing_time,
                SUM(record_size_bytes) as total_bytes
            FROM ingestion_stats 
            {where_clause}
            GROUP BY status, error_category
        """, params)
        
        results = cursor.fetchall()
        
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
            
            if row['avg_processing_time']:
                total_processing_time += row['avg_processing_time'] * count
            
            if row['total_bytes']:
                summary["total_bytes_processed"] += row['total_bytes']
            
            status = row['status']
            if status == ProcessingStatus.SUCCESS.value:
                summary["successful_records"] += count
            elif status == ProcessingStatus.FAILURE.value:
                summary["failed_records"] += count
                error_category = row['error_category'] or 'unknown'
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
        where_clause = "WHERE pipeline_run_id = ?" if pipeline_run_id else ""
        params = [pipeline_run_id] if pipeline_run_id else []
        
        cursor = self.conn.execute(f"""
            SELECT 
                COUNT(*) as total_records,
                AVG(completeness_score) as avg_completeness,
                AVG(consistency_score) as avg_consistency,
                AVG(validity_score) as avg_validity,
                AVG(accuracy_score) as avg_accuracy,
                AVG(overall_score) as avg_overall_score,
                MIN(overall_score) as min_score,
                MAX(overall_score) as max_score
            FROM quality_metrics 
            {where_clause}
        """, params)
        
        row = cursor.fetchone()
        
        return {
            "total_records": row['total_records'] or 0,
            "avg_completeness_score": row['avg_completeness'],
            "avg_consistency_score": row['avg_consistency'], 
            "avg_validity_score": row['avg_validity'],
            "avg_accuracy_score": row['avg_accuracy'],
            "avg_overall_score": row['avg_overall_score'],
            "min_overall_score": row['min_score'],
            "max_overall_score": row['max_score']
        }
    
    def get_recent_pipeline_runs(self, limit: int = 10) -> List[PipelineRunSummary]:
        """
        Get recent pipeline runs.
        
        Args:
            limit: Maximum number of runs to return
            
        Returns:
            List of PipelineRunSummary objects
        """
        cursor = self.conn.execute("""
            SELECT id, name, started_at, completed_at, status,
                   total_records, successful_records, failed_records,
                   skipped_records, error_message
            FROM pipeline_runs 
            ORDER BY started_at DESC 
            LIMIT ?
        """, (limit,))
        
        return [
            PipelineRunSummary(
                id=row['id'],
                name=row['name'],
                started_at=datetime.fromisoformat(row['started_at']),
                completed_at=datetime.fromisoformat(row['completed_at']) if row['completed_at'] else None,
                status=row['status'],
                total_records=row['total_records'],
                successful_records=row['successful_records'],
                failed_records=row['failed_records'],
                skipped_records=row['skipped_records'],
                error_message=row['error_message']
            )
            for row in cursor.fetchall()
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
        
        # Get pipeline runs to delete
        cursor = self.conn.execute(
            "SELECT id FROM pipeline_runs WHERE started_at < ?",
            (cutoff_date,)
        )
        old_run_ids = [row[0] for row in cursor.fetchall()]
        
        if not old_run_ids:
            return 0
        
        total_deleted = 0
        
        # Delete failed_records first (they reference ingestion_stats)
        if old_run_ids:
            placeholders = ','.join('?' * len(old_run_ids))
            cursor = self.conn.execute(f"""
                DELETE FROM failed_records 
                WHERE ingestion_stat_id IN (
                    SELECT id FROM ingestion_stats 
                    WHERE pipeline_run_id IN ({placeholders})
                )
            """, old_run_ids)
            total_deleted += cursor.rowcount
        
        # Delete other related data (due to foreign key constraints)
        for table in ["system_metrics", "performance_metrics", "quality_metrics", 
                     "audit_events", "ingestion_stats"]:
            placeholders = ','.join('?' * len(old_run_ids))
            cursor = self.conn.execute(
                f"DELETE FROM {table} WHERE pipeline_run_id IN ({placeholders})",
                old_run_ids
            )
            total_deleted += cursor.rowcount
        
        # Delete pipeline runs
        placeholders = ','.join('?' * len(old_run_ids))
        cursor = self.conn.execute(
            f"DELETE FROM pipeline_runs WHERE id IN ({placeholders})",
            old_run_ids
        )
        total_deleted += cursor.rowcount
        
        self.conn.commit()
        logger.info(f"Cleaned up {total_deleted} old tracking records older than {days_to_keep} days")
        
        return total_deleted