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

# tests/test_tracking_repository.py

"""
Unit tests for tracking repository and data access layer.

Tests high-level repository operations for storing and retrieving
tracking data, analytics, and reporting functionality.
"""

import pytest
import sqlite3
import tempfile
import os
import json
from datetime import datetime, timedelta
from typing import Dict, Any

from pulsepipe.persistence.models import init_data_intelligence_db, ProcessingStatus, ErrorCategory
from pulsepipe.persistence.tracking_repository import (
    TrackingRepository,
    PipelineRunSummary,
    IngestionStat,
    QualityMetric
)


class TestPipelineRunSummary:
    """Test PipelineRunSummary dataclass."""
    
    def test_basic_creation(self):
        """Test basic PipelineRunSummary creation."""
        summary = PipelineRunSummary(
            id="test-123",
            name="test_pipeline",
            started_at=datetime.now(),
            completed_at=None,
            status="running",
            total_records=100,
            successful_records=95,
            failed_records=5,
            skipped_records=0
        )
        
        assert summary.id == "test-123"
        assert summary.name == "test_pipeline"
        assert summary.status == "running"
        assert summary.total_records == 100
        assert summary.successful_records == 95
        assert summary.failed_records == 5
        assert summary.skipped_records == 0
        assert summary.completed_at is None
        assert summary.error_message is None
    
    def test_with_error_message(self):
        """Test PipelineRunSummary with error message."""
        summary = PipelineRunSummary(
            id="test-123",
            name="test_pipeline",
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status="failed",
            total_records=100,
            successful_records=0,
            failed_records=100,
            skipped_records=0,
            error_message="Test error"
        )
        
        assert summary.status == "failed"
        assert summary.error_message == "Test error"
        assert summary.completed_at is not None


class TestIngestionStat:
    """Test IngestionStat dataclass."""
    
    def test_basic_creation(self):
        """Test basic IngestionStat creation."""
        stat = IngestionStat(
            id=None,
            pipeline_run_id="run-123",
            stage_name="ingestion",
            file_path="/path/to/file.json",
            record_id="record-456",
            record_type="Patient",
            status=ProcessingStatus.SUCCESS,
            error_category=None,
            error_message=None,
            error_details=None,
            processing_time_ms=150,
            record_size_bytes=1024,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        assert stat.pipeline_run_id == "run-123"
        assert stat.stage_name == "ingestion"
        assert stat.status == ProcessingStatus.SUCCESS
        assert stat.processing_time_ms == 150
        assert stat.record_size_bytes == 1024
        assert stat.error_category is None
    
    def test_with_error_details(self):
        """Test IngestionStat with error information."""
        error_details = {"field": "patient_id", "value": "invalid"}
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id="run-123",
            stage_name="validation",
            file_path=None,
            record_id="record-456",
            record_type="Patient",
            status=ProcessingStatus.FAILURE,
            error_category=ErrorCategory.VALIDATION_ERROR,
            error_message="Invalid patient ID format",
            error_details=error_details,
            processing_time_ms=50,
            record_size_bytes=512,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        assert stat.status == ProcessingStatus.FAILURE
        assert stat.error_category == ErrorCategory.VALIDATION_ERROR
        assert stat.error_message == "Invalid patient ID format"
        assert stat.error_details == error_details


class TestQualityMetric:
    """Test QualityMetric dataclass."""
    
    def test_basic_creation(self):
        """Test basic QualityMetric creation."""
        metric = QualityMetric(
            id=None,
            pipeline_run_id="run-123",
            record_id="record-456",
            record_type="Patient",
            completeness_score=0.95,
            consistency_score=0.88,
            validity_score=0.92,
            accuracy_score=0.90,
            overall_score=0.91,
            missing_fields=["phone"],
            invalid_fields=[],
            outlier_fields=["age"],
            quality_issues=["Missing phone number"],
            metrics_details={"details": "test"},
            sampled=False
        )
        
        assert metric.pipeline_run_id == "run-123"
        assert metric.completeness_score == 0.95
        assert metric.overall_score == 0.91
        assert metric.missing_fields == ["phone"]
        assert metric.sampled is False
    
    def test_with_defaults(self):
        """Test QualityMetric with default values."""
        metric = QualityMetric(
            id=None,
            pipeline_run_id="run-123",
            record_id=None,
            record_type=None,
            completeness_score=None,
            consistency_score=None,
            validity_score=None,
            accuracy_score=None,
            overall_score=None,
            missing_fields=None,
            invalid_fields=None,
            outlier_fields=None,
            quality_issues=None,
            metrics_details=None
        )
        
        assert metric.sampled is False
        assert metric.timestamp is None
        assert metric.completeness_score is None


class TestTrackingRepository:
    """Test TrackingRepository class."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database with schema."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        conn = sqlite3.connect(path)
        init_data_intelligence_db(conn)
        
        yield conn
        
        conn.close()
        os.unlink(path)
    
    @pytest.fixture
    def repository(self, temp_db):
        """Create a TrackingRepository instance."""
        return TrackingRepository(temp_db)
    
    def test_init(self, temp_db):
        """Test repository initialization."""
        repo = TrackingRepository(temp_db)
        assert repo.conn == temp_db
        assert temp_db.row_factory == sqlite3.Row
    
    # Pipeline Run Management Tests
    
    def test_start_pipeline_run(self, repository, temp_db):
        """Test starting a pipeline run."""
        run_id = "test-run-123"
        name = "test_pipeline"
        config = {"param1": "value1"}
        
        repository.start_pipeline_run(run_id, name, config)
        
        # Verify the run was recorded
        cursor = temp_db.execute(
            "SELECT id, name, status, config_snapshot FROM pipeline_runs WHERE id = ?",
            (run_id,)
        )
        row = cursor.fetchone()
        
        assert row['id'] == run_id
        assert row['name'] == name
        assert row['status'] == "running"
        assert json.loads(row['config_snapshot']) == config
    
    def test_start_pipeline_run_without_config(self, repository, temp_db):
        """Test starting a pipeline run without config snapshot."""
        run_id = "test-run-124"
        name = "test_pipeline"
        
        repository.start_pipeline_run(run_id, name)
        
        cursor = temp_db.execute(
            "SELECT config_snapshot FROM pipeline_runs WHERE id = ?",
            (run_id,)
        )
        row = cursor.fetchone()
        assert row['config_snapshot'] is None
    
    def test_complete_pipeline_run_success(self, repository, temp_db):
        """Test completing a pipeline run successfully."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        repository.complete_pipeline_run(run_id, "completed")
        
        cursor = temp_db.execute(
            "SELECT status, completed_at, error_message FROM pipeline_runs WHERE id = ?",
            (run_id,)
        )
        row = cursor.fetchone()
        
        assert row['status'] == "completed"
        assert row['completed_at'] is not None
        assert row['error_message'] is None
    
    def test_complete_pipeline_run_failure(self, repository, temp_db):
        """Test completing a pipeline run with failure."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        error_msg = "Test error occurred"
        repository.complete_pipeline_run(run_id, "failed", error_msg)
        
        cursor = temp_db.execute(
            "SELECT status, error_message FROM pipeline_runs WHERE id = ?",
            (run_id,)
        )
        row = cursor.fetchone()
        
        assert row['status'] == "failed"
        assert row['error_message'] == error_msg
    
    def test_update_pipeline_run_counts(self, repository, temp_db):
        """Test updating pipeline run record counts."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        repository.update_pipeline_run_counts(run_id, 100, 95, 5, 0)
        
        cursor = temp_db.execute(
            "SELECT total_records, successful_records, failed_records, skipped_records FROM pipeline_runs WHERE id = ?",
            (run_id,)
        )
        row = cursor.fetchone()
        
        assert row['total_records'] == 100
        assert row['successful_records'] == 95
        assert row['failed_records'] == 5
        assert row['skipped_records'] == 0
    
    def test_update_pipeline_run_counts_incremental(self, repository, temp_db):
        """Test incremental updates to pipeline run counts."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        repository.update_pipeline_run_counts(run_id, 50, 45, 5, 0)
        repository.update_pipeline_run_counts(run_id, 50, 50, 0, 0)
        
        cursor = temp_db.execute(
            "SELECT total_records, successful_records, failed_records FROM pipeline_runs WHERE id = ?",
            (run_id,)
        )
        row = cursor.fetchone()
        
        assert row['total_records'] == 100
        assert row['successful_records'] == 95
        assert row['failed_records'] == 5
    
    def test_get_pipeline_run_exists(self, repository, temp_db):
        """Test getting an existing pipeline run."""
        run_id = "test-run-123"
        name = "test_pipeline"
        repository.start_pipeline_run(run_id, name)
        repository.update_pipeline_run_counts(run_id, 100, 95, 5, 0)
        repository.complete_pipeline_run(run_id, "completed")
        
        summary = repository.get_pipeline_run(run_id)
        
        assert summary is not None
        assert isinstance(summary, PipelineRunSummary)
        assert summary.id == run_id
        assert summary.name == name
        assert summary.status == "completed"
        assert summary.total_records == 100
        assert summary.successful_records == 95
        assert summary.failed_records == 5
        assert summary.completed_at is not None
    
    def test_get_pipeline_run_not_exists(self, repository):
        """Test getting a non-existent pipeline run."""
        summary = repository.get_pipeline_run("nonexistent")
        assert summary is None
    
    # Ingestion Statistics Tests
    
    def test_record_ingestion_stat_success(self, repository, temp_db):
        """Test recording a successful ingestion stat."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id=run_id,
            stage_name="ingestion",
            file_path="/test/file.json",
            record_id="record-123",
            record_type="Patient",
            status=ProcessingStatus.SUCCESS,
            error_category=None,
            error_message=None,
            error_details=None,
            processing_time_ms=150,
            record_size_bytes=1024,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        stat_id = repository.record_ingestion_stat(stat)
        
        assert isinstance(stat_id, int)
        assert stat_id > 0
        
        # Verify the record was stored
        cursor = temp_db.execute(
            "SELECT pipeline_run_id, stage_name, status, processing_time_ms FROM ingestion_stats WHERE id = ?",
            (stat_id,)
        )
        row = cursor.fetchone()
        
        assert row['pipeline_run_id'] == run_id
        assert row['stage_name'] == "ingestion"
        assert row['status'] == ProcessingStatus.SUCCESS.value
        assert row['processing_time_ms'] == 150
    
    def test_record_ingestion_stat_with_error(self, repository, temp_db):
        """Test recording an ingestion stat with error details."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        error_details = {"field": "patient_id", "value": "invalid"}
        stat = IngestionStat(
            id=None,
            pipeline_run_id=run_id,
            stage_name="validation",
            file_path=None,
            record_id="record-123",
            record_type="Patient",
            status=ProcessingStatus.FAILURE,
            error_category=ErrorCategory.VALIDATION_ERROR,
            error_message="Invalid patient ID",
            error_details=error_details,
            processing_time_ms=50,
            record_size_bytes=512,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        stat_id = repository.record_ingestion_stat(stat)
        
        # Verify error information was stored
        cursor = temp_db.execute(
            "SELECT status, error_category, error_message, error_details FROM ingestion_stats WHERE id = ?",
            (stat_id,)
        )
        row = cursor.fetchone()
        
        assert row['status'] == ProcessingStatus.FAILURE.value
        assert row['error_category'] == ErrorCategory.VALIDATION_ERROR.value
        assert row['error_message'] == "Invalid patient ID"
        assert json.loads(row['error_details']) == error_details
    
    def test_record_failed_record(self, repository, temp_db):
        """Test recording a failed record."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id=run_id,
            stage_name="ingestion",
            file_path=None,
            record_id=None,
            record_type=None,
            status=ProcessingStatus.FAILURE,
            error_category=ErrorCategory.PARSE_ERROR,
            error_message=None,
            error_details=None,
            processing_time_ms=None,
            record_size_bytes=None,
            data_source=None,
            timestamp=datetime.now()
        )
        
        stat_id = repository.record_ingestion_stat(stat)
        
        original_data = '{"invalid": "json"'
        failure_reason = "Malformed JSON"
        stack_trace = "Traceback (most recent call last)..."
        
        failed_id = repository.record_failed_record(
            stat_id, original_data, failure_reason, None, stack_trace
        )
        
        assert isinstance(failed_id, int)
        assert failed_id > 0
        
        # Verify the failed record was stored
        cursor = temp_db.execute(
            "SELECT ingestion_stat_id, original_data, failure_reason, stack_trace FROM failed_records WHERE id = ?",
            (failed_id,)
        )
        row = cursor.fetchone()
        
        assert row['ingestion_stat_id'] == stat_id
        assert row['original_data'] == original_data
        assert row['failure_reason'] == failure_reason
        assert row['stack_trace'] == stack_trace
    
    # Quality Metrics Tests
    
    def test_record_quality_metric(self, repository, temp_db):
        """Test recording a quality metric."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        metric = QualityMetric(
            id=None,
            pipeline_run_id=run_id,
            record_id="record-123",
            record_type="Patient",
            completeness_score=0.95,
            consistency_score=0.88,
            validity_score=0.92,
            accuracy_score=0.90,
            overall_score=0.91,
            missing_fields=["phone"],
            invalid_fields=[],
            outlier_fields=["age"],
            quality_issues=["Missing phone number"],
            metrics_details={"test": "data"},
            sampled=True,
            timestamp=datetime.now()
        )
        
        metric_id = repository.record_quality_metric(metric)
        
        assert isinstance(metric_id, int)
        assert metric_id > 0
        
        # Verify the metric was stored
        cursor = temp_db.execute(
            "SELECT pipeline_run_id, overall_score, missing_fields, sampled FROM quality_metrics WHERE id = ?",
            (metric_id,)
        )
        row = cursor.fetchone()
        
        assert row['pipeline_run_id'] == run_id
        assert row['overall_score'] == 0.91
        assert json.loads(row['missing_fields']) == ["phone"]
        assert row['sampled'] == 1  # SQLite stores boolean as integer
    
    # Audit Events Tests
    
    def test_record_audit_event(self, repository, temp_db):
        """Test recording an audit event."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        details = {"record_count": 100, "file_size": 1024}
        correlation_id = "corr-456"
        
        event_id = repository.record_audit_event(
            pipeline_run_id=run_id,
            event_type="batch_processed",
            stage_name="ingestion",
            message="Processed batch of 100 records",
            event_level="INFO",
            record_id=None,
            details=details,
            correlation_id=correlation_id
        )
        
        assert isinstance(event_id, int)
        assert event_id > 0
        
        # Verify the event was stored
        cursor = temp_db.execute(
            "SELECT event_type, message, event_level, details, correlation_id FROM audit_events WHERE id = ?",
            (event_id,)
        )
        row = cursor.fetchone()
        
        assert row['event_type'] == "batch_processed"
        assert row['message'] == "Processed batch of 100 records"
        assert row['event_level'] == "INFO"
        assert json.loads(row['details']) == details
        assert row['correlation_id'] == correlation_id
    
    def test_record_audit_event_minimal(self, repository, temp_db):
        """Test recording an audit event with minimal information."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        event_id = repository.record_audit_event(
            pipeline_run_id=run_id,
            event_type="stage_started",
            stage_name="ingestion",
            message="Starting ingestion stage"
        )
        
        assert isinstance(event_id, int)
        
        cursor = temp_db.execute(
            "SELECT event_level, details FROM audit_events WHERE id = ?",
            (event_id,)
        )
        row = cursor.fetchone()
        
        assert row['event_level'] == "INFO"  # Default value
        assert row['details'] is None
    
    # Performance Metrics Tests
    
    def test_record_performance_metric(self, repository, temp_db):
        """Test recording a performance metric."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=5)
        
        perf_id = repository.record_performance_metric(
            pipeline_run_id=run_id,
            stage_name="ingestion",
            started_at=started_at,
            completed_at=completed_at,
            records_processed=1000,
            memory_usage_mb=256.5,
            cpu_usage_percent=75.2,
            bottleneck_indicator="CPU bound"
        )
        
        assert isinstance(perf_id, int)
        assert perf_id > 0
        
        # Verify the metric was stored
        cursor = temp_db.execute(
            "SELECT stage_name, duration_ms, records_processed, records_per_second, memory_usage_mb FROM performance_metrics WHERE id = ?",
            (perf_id,)
        )
        row = cursor.fetchone()
        
        assert row['stage_name'] == "ingestion"
        assert row['duration_ms'] == 5000  # 5 seconds
        assert row['records_processed'] == 1000
        assert row['records_per_second'] == 200.0  # 1000 records / 5 seconds
        assert row['memory_usage_mb'] == 256.5
    
    def test_record_performance_metric_zero_duration(self, repository, temp_db):
        """Test recording performance metric with zero duration."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        started_at = datetime.now()
        completed_at = started_at  # Same time
        
        perf_id = repository.record_performance_metric(
            pipeline_run_id=run_id,
            stage_name="test",
            started_at=started_at,
            completed_at=completed_at,
            records_processed=100
        )
        
        cursor = temp_db.execute(
            "SELECT duration_ms, records_per_second FROM performance_metrics WHERE id = ?",
            (perf_id,)
        )
        row = cursor.fetchone()
        
        assert row['duration_ms'] == 0
        assert row['records_per_second'] == 0
    
    # Analytics and Reporting Tests
    
    def test_get_ingestion_summary_empty(self, repository):
        """Test getting ingestion summary with no data."""
        summary = repository.get_ingestion_summary()
        
        assert summary['total_records'] == 0
        assert summary['successful_records'] == 0
        assert summary['failed_records'] == 0
        assert summary['skipped_records'] == 0
        assert summary['error_breakdown'] == {}
        assert summary['avg_processing_time_ms'] == 0
        assert summary['total_bytes_processed'] == 0
    
    def test_get_ingestion_summary_with_data(self, repository, temp_db):
        """Test getting ingestion summary with actual data."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        # Add some test stats
        stats = [
            IngestionStat(None, run_id, "ingestion", None, "rec1", "Patient", 
                         ProcessingStatus.SUCCESS, None, None, None, 100, 1024, None, datetime.now()),
            IngestionStat(None, run_id, "ingestion", None, "rec2", "Patient",
                         ProcessingStatus.SUCCESS, None, None, None, 150, 2048, None, datetime.now()),
            IngestionStat(None, run_id, "validation", None, "rec3", "Patient",
                         ProcessingStatus.FAILURE, ErrorCategory.VALIDATION_ERROR, "Invalid", None, 50, 512, None, datetime.now())
        ]
        
        for stat in stats:
            repository.record_ingestion_stat(stat)
        
        summary = repository.get_ingestion_summary(run_id)
        
        assert summary['total_records'] == 3
        assert summary['successful_records'] == 2
        assert summary['failed_records'] == 1
        assert summary['skipped_records'] == 0
        assert summary['error_breakdown']['validation_error'] == 1
        assert summary['avg_processing_time_ms'] == 100.0  # (100 + 150 + 50) / 3
        assert summary['total_bytes_processed'] == 3584  # 1024 + 2048 + 512
    
    def test_get_quality_summary_empty(self, repository):
        """Test getting quality summary with no data."""
        summary = repository.get_quality_summary()
        
        assert summary['total_records'] == 0
        assert summary['avg_completeness_score'] is None
        assert summary['avg_overall_score'] is None
        assert summary['min_overall_score'] is None
        assert summary['max_overall_score'] is None
    
    def test_get_quality_summary_with_data(self, repository, temp_db):
        """Test getting quality summary with actual data."""
        run_id = "test-run-123"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        # Add quality metrics
        metrics = [
            QualityMetric(None, run_id, "rec1", "Patient", 0.9, 0.8, 0.85, 0.88, 0.86, None, None, None, None, None),
            QualityMetric(None, run_id, "rec2", "Patient", 0.95, 0.9, 0.92, 0.93, 0.925, None, None, None, None, None),
            QualityMetric(None, run_id, "rec3", "Patient", 0.85, 0.75, 0.80, 0.82, 0.805, None, None, None, None, None)
        ]
        
        for metric in metrics:
            repository.record_quality_metric(metric)
        
        summary = repository.get_quality_summary(run_id)
        
        assert summary['total_records'] == 3
        assert abs(summary['avg_completeness_score'] - 0.9) < 0.01
        assert abs(summary['avg_overall_score'] - 0.863) < 0.01  # (0.86 + 0.925 + 0.805) / 3
        assert summary['min_overall_score'] == 0.805
        assert summary['max_overall_score'] == 0.925
    
    def test_get_recent_pipeline_runs_empty(self, repository):
        """Test getting recent pipeline runs with no data."""
        runs = repository.get_recent_pipeline_runs()
        assert runs == []
    
    def test_get_recent_pipeline_runs_with_data(self, repository, temp_db):
        """Test getting recent pipeline runs with data."""
        # Create multiple runs
        run_ids = ["run-1", "run-2", "run-3"]
        for i, run_id in enumerate(run_ids):
            repository.start_pipeline_run(run_id, f"pipeline_{i}")
            repository.update_pipeline_run_counts(run_id, 100 + i * 10, 90 + i * 5, 10 - i, 0)
            repository.complete_pipeline_run(run_id, "completed")
        
        runs = repository.get_recent_pipeline_runs(limit=2)
        
        assert len(runs) == 2
        assert all(isinstance(run, PipelineRunSummary) for run in runs)
        
        # Should be in reverse chronological order (most recent first)
        assert runs[0].id == "run-3"
        assert runs[1].id == "run-2"
    
    def test_cleanup_old_data_no_data(self, repository):
        """Test cleanup with no data to clean."""
        deleted_count = repository.cleanup_old_data(30)
        assert deleted_count == 0
    
    def test_cleanup_old_data_with_old_runs(self, repository, temp_db):
        """Test cleanup of old data."""
        # Create old and new runs
        old_time = datetime.now() - timedelta(days=45)
        new_time = datetime.now() - timedelta(days=15)
        
        # Insert old run manually to control timestamp
        temp_db.execute("""
            INSERT INTO pipeline_runs (id, name, started_at, status)
            VALUES (?, ?, ?, ?)
        """, ("old-run", "old_pipeline", old_time, "completed"))
        
        repository.start_pipeline_run("new-run", "new_pipeline")
        
        # Add related data for old run
        repository.record_audit_event("old-run", "test_event", "test_stage", "Test message")
        
        temp_db.commit()
        
        # Cleanup data older than 30 days
        deleted_count = repository.cleanup_old_data(30)
        
        assert deleted_count > 0
        
        # Verify old run is gone
        old_run = repository.get_pipeline_run("old-run")
        assert old_run is None
        
        # Verify new run still exists
        new_run = repository.get_pipeline_run("new-run")
        assert new_run is not None


class TestRepositoryIntegration:
    """Test repository integration scenarios."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database with schema."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        conn = sqlite3.connect(path)
        init_data_intelligence_db(conn)
        
        yield conn
        
        conn.close()
        os.unlink(path)
    
    @pytest.fixture
    def repository(self, temp_db):
        """Create a TrackingRepository instance."""
        return TrackingRepository(temp_db)
    
    def test_complete_pipeline_workflow(self, repository, temp_db):
        """Test a complete pipeline workflow with all tracking components."""
        run_id = "complete-test-run"
        
        # Start pipeline
        config = {"batch_size": 100, "validation": True}
        repository.start_pipeline_run(run_id, "complete_test_pipeline", config)
        
        # Record ingestion stats
        for i in range(5):
            stat = IngestionStat(
                None, run_id, "ingestion", f"/file_{i}.json", f"record_{i}", "Patient",
                ProcessingStatus.SUCCESS, None, None, None, 100 + i * 10, 1024 + i * 100, "FHIR", datetime.now()
            )
            repository.record_ingestion_stat(stat)
        
        # Record one failure
        failed_stat = IngestionStat(
            None, run_id, "validation", "/file_bad.json", "record_bad", "Patient",
            ProcessingStatus.FAILURE, ErrorCategory.VALIDATION_ERROR, "Invalid format", 
            {"field": "id", "issue": "missing"}, 50, 512, "FHIR", datetime.now()
        )
        failed_stat_id = repository.record_ingestion_stat(failed_stat)
        
        # Record the failed record
        repository.record_failed_record(
            failed_stat_id, '{"incomplete": "data"}', "Missing required field", None, "Stack trace here"
        )
        
        # Record quality metrics
        for i in range(3):
            metric = QualityMetric(
                None, run_id, f"record_{i}", "Patient", 0.9 + i * 0.02, 0.85 + i * 0.01, 
                0.88 + i * 0.01, 0.87 + i * 0.01, 0.875 + i * 0.01, [], [], [], [], None
            )
            repository.record_quality_metric(metric)
        
        # Record audit events
        repository.record_audit_event(run_id, "pipeline_started", "ingestion", "Pipeline started successfully")
        repository.record_audit_event(run_id, "validation_failed", "validation", "Record validation failed", "WARNING")
        
        # Record performance metrics
        start_time = datetime.now() - timedelta(seconds=10)
        end_time = datetime.now()
        repository.record_performance_metric(run_id, "ingestion", start_time, end_time, 6, 128.5, 45.2)
        
        # Update counts and complete
        repository.update_pipeline_run_counts(run_id, 6, 5, 1, 0)
        repository.complete_pipeline_run(run_id, "completed")
        
        # Verify complete workflow
        run_summary = repository.get_pipeline_run(run_id)
        assert run_summary.status == "completed"
        assert run_summary.total_records == 6
        assert run_summary.successful_records == 5
        assert run_summary.failed_records == 1
        
        ingestion_summary = repository.get_ingestion_summary(run_id)
        assert ingestion_summary['total_records'] == 6
        assert ingestion_summary['successful_records'] == 5
        assert ingestion_summary['failed_records'] == 1
        assert 'validation_error' in ingestion_summary['error_breakdown']
        
        quality_summary = repository.get_quality_summary(run_id)
        assert quality_summary['total_records'] == 3
        assert quality_summary['avg_overall_score'] > 0.8
    
    def test_date_range_filtering(self, repository, temp_db):
        """Test date range filtering in analytics queries."""
        run_id = "date-filter-test"
        repository.start_pipeline_run(run_id, "test_pipeline")
        
        # Create stats with different timestamps
        old_date = datetime.now() - timedelta(days=10)
        recent_date = datetime.now() - timedelta(days=2)
        
        # Insert old stat manually
        temp_db.execute("""
            INSERT INTO ingestion_stats (pipeline_run_id, stage_name, status, timestamp)
            VALUES (?, ?, ?, ?)
        """, (run_id, "ingestion", "success", old_date))
        
        # Insert recent stat using repository
        recent_stat = IngestionStat(
            None, run_id, "ingestion", None, None, None, ProcessingStatus.SUCCESS,
            None, None, None, None, None, None, recent_date
        )
        repository.record_ingestion_stat(recent_stat)
        
        temp_db.commit()
        
        # Filter by date range
        start_date = datetime.now() - timedelta(days=5)
        end_date = datetime.now()
        
        summary = repository.get_ingestion_summary(run_id, start_date, end_date)
        
        # Should only include the recent record
        assert summary['total_records'] == 1
        assert summary['successful_records'] == 1
        
        # Without date filter, should include both
        summary_all = repository.get_ingestion_summary(run_id)
        assert summary_all['total_records'] == 2