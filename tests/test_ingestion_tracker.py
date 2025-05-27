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

# tests/test_ingestion_tracker.py

"""
Unit tests for the ingestion tracking system.

Tests cover ingestion success/failure tracking, metrics calculation,
export functionality, and integration with the persistence layer.
"""

import pytest
import json
import csv
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

from pulsepipe.audit.ingestion_tracker import (
    IngestionTracker,
    IngestionRecord,
    IngestionBatchMetrics,
    IngestionSummary,
    IngestionOutcome,
    IngestionStage
)
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig
from pulsepipe.persistence import ErrorCategory, ProcessingStatus


@pytest.fixture
def mock_config():
    """Create a mock configuration with ingestion tracking enabled."""
    config = Mock(spec=DataIntelligenceConfig)
    config.is_feature_enabled.return_value = True
    return config


@pytest.fixture
def mock_repository():
    """Create a mock tracking repository."""
    repository = Mock()
    repository.record_ingestion_stat.return_value = 1
    return repository


@pytest.fixture
def tracker(mock_config, mock_repository):
    """Create an ingestion tracker for testing."""
    return IngestionTracker(
        pipeline_run_id="test_pipeline_123",
        stage_name="ingestion",
        config=mock_config,
        repository=mock_repository
    )


@pytest.fixture
def disabled_tracker():
    """Create a disabled ingestion tracker for testing."""
    config = Mock(spec=DataIntelligenceConfig)
    config.is_feature_enabled.return_value = False
    
    return IngestionTracker(
        pipeline_run_id="test_pipeline_123",
        stage_name="ingestion",
        config=config
    )


class TestIngestionRecord:
    """Test IngestionRecord data class."""
    
    def test_create_record_with_defaults(self):
        """Test creating a record with default values."""
        record = IngestionRecord(record_id="test_123")
        
        assert record.record_id == "test_123"
        assert record.record_type is None
        assert record.outcome is None
        assert record.timestamp is not None
        assert isinstance(record.timestamp, datetime)
        assert record.metadata == {}
    
    def test_create_record_with_all_fields(self):
        """Test creating a record with all fields populated."""
        timestamp = datetime.now()
        metadata = {"source": "test"}
        
        record = IngestionRecord(
            record_id="test_123",
            record_type="Patient",
            file_path="/data/test.json",
            outcome=IngestionOutcome.SUCCESS,
            stage=IngestionStage.PARSING,
            error_category=ErrorCategory.VALIDATION_ERROR,
            error_message="Test error",
            error_details={"field": "name"},
            processing_time_ms=100,
            record_size_bytes=1024,
            data_source="FHIR",
            timestamp=timestamp,
            metadata=metadata
        )
        
        assert record.record_id == "test_123"
        assert record.record_type == "Patient"
        assert record.file_path == "/data/test.json"
        assert record.outcome == IngestionOutcome.SUCCESS
        assert record.stage == IngestionStage.PARSING
        assert record.error_category == ErrorCategory.VALIDATION_ERROR
        assert record.error_message == "Test error"
        assert record.error_details == {"field": "name"}
        assert record.processing_time_ms == 100
        assert record.record_size_bytes == 1024
        assert record.data_source == "FHIR"
        assert record.timestamp == timestamp
        assert record.metadata == metadata


class TestIngestionBatchMetrics:
    """Test IngestionBatchMetrics data class."""
    
    def test_create_batch_metrics(self):
        """Test creating batch metrics."""
        started_at = datetime.now()
        
        batch = IngestionBatchMetrics(
            batch_id="batch_123",
            pipeline_run_id="pipeline_123",
            stage_name="ingestion",
            started_at=started_at
        )
        
        assert batch.batch_id == "batch_123"
        assert batch.pipeline_run_id == "pipeline_123"
        assert batch.stage_name == "ingestion"
        assert batch.started_at == started_at
        assert batch.total_records == 0
        assert batch.successful_records == 0
        assert batch.failed_records == 0
        assert batch.success_rate == 0.0
    
    def test_calculate_metrics(self):
        """Test metric calculations."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=10)
        
        batch = IngestionBatchMetrics(
            batch_id="batch_123",
            pipeline_run_id="pipeline_123",
            stage_name="ingestion",
            started_at=started_at,
            completed_at=completed_at,
            total_records=100,
            successful_records=80,
            failed_records=20,
            total_processing_time_ms=5000,
            total_bytes_processed=10240
        )
        
        batch.calculate_metrics()
        
        assert batch.success_rate == 80.0
        assert batch.failure_rate == 20.0
        assert batch.avg_processing_time_ms == 50.0
        assert batch.records_per_second == 10.0  # 100 records / 10 seconds
        assert batch.bytes_per_second == 1024.0  # 10240 bytes / 10 seconds
    
    def test_calculate_metrics_no_completion_time(self):
        """Test metric calculations without completion time."""
        batch = IngestionBatchMetrics(
            batch_id="batch_123",
            pipeline_run_id="pipeline_123",
            stage_name="ingestion",
            started_at=datetime.now(),
            total_records=100,
            successful_records=80,
            failed_records=20
        )
        
        batch.calculate_metrics()
        
        assert batch.success_rate == 80.0
        assert batch.failure_rate == 20.0
        assert batch.records_per_second == 0.0
        assert batch.bytes_per_second == 0.0
    
    def test_to_dict_conversion(self):
        """Test converting batch metrics to dictionary."""
        started_at = datetime.now()
        
        batch = IngestionBatchMetrics(
            batch_id="batch_123",
            pipeline_run_id="pipeline_123",
            stage_name="ingestion",
            started_at=started_at,
            total_records=100
        )
        
        data = batch.to_dict()
        
        assert data["batch_id"] == "batch_123"
        assert data["pipeline_run_id"] == "pipeline_123"
        assert data["total_records"] == 100
        assert data["started_at"] == started_at.isoformat()
        assert data["completed_at"] is None


class TestIngestionSummary:
    """Test IngestionSummary data class and creation methods."""
    
    def test_create_summary_from_empty_batches(self):
        """Test creating summary from empty batch list."""
        summary = IngestionSummary.from_batches("pipeline_123", [])
        
        assert summary.pipeline_run_id == "pipeline_123"
        assert summary.total_batches == 0
        assert summary.total_records == 0
        assert summary.success_rate == 0.0
        assert len(summary.recommendations) > 0
    
    def test_create_summary_from_batches(self):
        """Test creating summary from multiple batches."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=10)
        
        batch1 = IngestionBatchMetrics(
            batch_id="batch_1",
            pipeline_run_id="pipeline_123",
            stage_name="ingestion",
            started_at=started_at,
            completed_at=completed_at,
            total_records=100,
            successful_records=90,
            failed_records=10,
            total_processing_time_ms=2000,
            total_bytes_processed=5120,
            errors_by_category={"validation_error": 8, "parse_error": 2},
            errors_by_stage={"parsing": 5, "validation": 5},
            data_sources=["FHIR", "HL7"]
        )
        
        batch2 = IngestionBatchMetrics(
            batch_id="batch_2",
            pipeline_run_id="pipeline_123",
            stage_name="ingestion",
            started_at=started_at + timedelta(seconds=15),
            completed_at=started_at + timedelta(seconds=25),
            total_records=50,
            successful_records=45,
            failed_records=5,
            total_processing_time_ms=1000,
            total_bytes_processed=2560,
            errors_by_category={"validation_error": 3, "network_error": 2},
            errors_by_stage={"validation": 5},
            data_sources=["FHIR", "X12"]
        )
        
        summary = IngestionSummary.from_batches("pipeline_123", [batch1, batch2])
        
        assert summary.pipeline_run_id == "pipeline_123"
        assert summary.total_batches == 2
        assert summary.total_records == 150
        assert summary.successful_records == 135
        assert summary.failed_records == 15
        assert summary.success_rate == 90.0
        assert summary.failure_rate == 10.0
        assert summary.avg_processing_time_ms == 20.0  # 3000ms / 150 records
        assert summary.total_bytes_processed == 7680
        
        # Check error aggregation
        assert summary.errors_by_category["validation_error"] == 11
        assert summary.errors_by_category["parse_error"] == 2
        assert summary.errors_by_category["network_error"] == 2
        assert summary.errors_by_stage["parsing"] == 5
        assert summary.errors_by_stage["validation"] == 10
        
        # Check data sources
        assert set(summary.data_sources) == {"FHIR", "HL7", "X12"}
        
        # Check most common errors
        assert len(summary.most_common_errors) > 0
        assert summary.most_common_errors[0]["category"] == "validation_error"
        assert summary.most_common_errors[0]["count"] == 11
    
    def test_generate_recommendations_high_failure_rate(self):
        """Test recommendation generation for high failure rate."""
        summary = IngestionSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            successful_records=70,
            failed_records=30,
            failure_rate=30.0
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("High failure rate" in rec for rec in recommendations)
    
    def test_generate_recommendations_high_processing_time(self):
        """Test recommendation generation for high processing time."""
        summary = IngestionSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,  # Need records > 0 to avoid "no records" recommendation
            avg_processing_time_ms=1000.0
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("processing time is high" in rec for rec in recommendations)
    
    def test_generate_recommendations_low_throughput(self):
        """Test recommendation generation for low throughput."""
        summary = IngestionSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=1000,
            records_per_second=5.0
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("Low throughput" in rec for rec in recommendations)


class TestIngestionTracker:
    """Test IngestionTracker class."""
    
    def test_create_tracker_enabled(self, tracker):
        """Test creating an enabled tracker."""
        assert tracker.pipeline_run_id == "test_pipeline_123"
        assert tracker.stage_name == "ingestion"
        assert tracker.enabled is True
        assert tracker.current_batch is None
        assert len(tracker.completed_batches) == 0
    
    def test_create_tracker_disabled(self, disabled_tracker):
        """Test creating a disabled tracker."""
        assert disabled_tracker.enabled is False
    
    def test_start_batch(self, tracker):
        """Test starting a new batch."""
        metadata = {"source": "test"}
        tracker.start_batch("batch_123", metadata)
        
        assert tracker.current_batch is not None
        assert tracker.current_batch.batch_id == "batch_123"
        assert tracker.current_batch.pipeline_run_id == "test_pipeline_123"
        assert tracker.current_batch.stage_name == "ingestion"
        assert tracker.current_batch.metadata == metadata
        assert len(tracker.batch_records) == 0
    
    def test_start_batch_when_disabled(self, disabled_tracker):
        """Test starting batch when tracking is disabled."""
        disabled_tracker.start_batch("batch_123")
        
        assert disabled_tracker.current_batch is None
    
    def test_start_batch_replaces_previous(self, tracker):
        """Test starting a new batch replaces the previous one."""
        tracker.start_batch("batch_1")
        tracker.start_batch("batch_2")
        
        assert tracker.current_batch.batch_id == "batch_2"
        assert len(tracker.completed_batches) == 1
        assert tracker.completed_batches[0].batch_id == "batch_1"
    
    def test_record_success(self, tracker):
        """Test recording a successful operation."""
        tracker.start_batch("batch_123")
        
        tracker.record_success(
            record_id="record_1",
            record_type="Patient",
            file_path="/data/test.json",
            processing_time_ms=50,
            record_size_bytes=1024,
            data_source="FHIR",
            metadata={"test": "value"}
        )
        
        batch = tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 1
        assert batch.failed_records == 0
        assert batch.total_processing_time_ms == 50
        assert batch.total_bytes_processed == 1024
        assert "FHIR" in batch.data_sources
        assert "/data/test.json" in batch.file_paths
        
        # Check detailed tracking
        assert len(tracker.batch_records) == 1
        record = tracker.batch_records[0]
        assert record.record_id == "record_1"
        assert record.outcome == IngestionOutcome.SUCCESS
    
    def test_record_success_when_disabled(self, disabled_tracker):
        """Test recording success when tracking is disabled."""
        disabled_tracker.record_success("record_1")
        
        # Should not create any batch
        assert disabled_tracker.current_batch is None
    
    def test_record_failure(self, tracker):
        """Test recording a failed operation."""
        tracker.start_batch("batch_123")
        
        error = ValueError("Test error")
        tracker.record_failure(
            record_id="record_1",
            error=error,
            stage=IngestionStage.VALIDATION,
            error_category=ErrorCategory.VALIDATION_ERROR,
            record_type="Patient",
            processing_time_ms=25
        )
        
        batch = tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 0
        assert batch.failed_records == 1
        assert batch.errors_by_category[ErrorCategory.VALIDATION_ERROR.value] == 1
        assert batch.errors_by_stage[IngestionStage.VALIDATION.value] == 1
        
        # Check detailed tracking
        record = tracker.batch_records[0]
        assert record.record_id == "record_1"
        assert record.outcome == IngestionOutcome.FAILURE
        assert record.stage == IngestionStage.VALIDATION
        assert record.error_message == "Test error"
        assert record.error_details["error_type"] == "ValueError"
    
    def test_record_skip(self, tracker):
        """Test recording a skipped operation."""
        tracker.start_batch("batch_123")
        
        tracker.record_skip(
            record_id="record_1",
            reason="Already processed",
            record_type="Patient"
        )
        
        batch = tracker.current_batch
        assert batch.total_records == 1
        assert batch.skipped_records == 1
        
        # Check detailed tracking
        record = tracker.batch_records[0]
        assert record.record_id == "record_1"
        assert record.outcome == IngestionOutcome.SKIPPED
        assert record.error_message == "Already processed"
    
    def test_record_partial_success(self, tracker):
        """Test recording a partially successful operation."""
        tracker.start_batch("batch_123")
        
        issues = ["Missing field", "Invalid format"]
        tracker.record_partial_success(
            record_id="record_1",
            issues=issues,
            stage=IngestionStage.TRANSFORMATION,
            processing_time_ms=75
        )
        
        batch = tracker.current_batch
        assert batch.total_records == 1
        assert batch.partial_success_records == 1
        
        # Check detailed tracking
        record = tracker.batch_records[0]
        assert record.record_id == "record_1"
        assert record.outcome == IngestionOutcome.PARTIAL_SUCCESS
        assert record.stage == IngestionStage.TRANSFORMATION
        assert record.error_details["issues"] == issues
    
    def test_auto_batch_creation(self, tracker):
        """Test automatic batch creation when none exists."""
        # Record without starting a batch
        tracker.record_success("record_1")
        
        # Should auto-create a batch
        assert tracker.current_batch is not None
        assert tracker.current_batch.batch_id.startswith("auto_batch_")
        assert tracker.current_batch.total_records == 1
    
    def test_finish_batch(self, tracker):
        """Test finishing a batch."""
        tracker.start_batch("batch_123")
        tracker.record_success("record_1", processing_time_ms=50)
        tracker.record_failure("record_2", ValueError("error"), IngestionStage.PARSING)
        
        finished_batch = tracker.finish_batch()
        
        assert finished_batch is not None
        assert finished_batch.batch_id == "batch_123"
        assert finished_batch.completed_at is not None
        assert finished_batch.total_records == 2
        
        # Check metrics were calculated
        assert finished_batch.success_rate == 50.0
        assert finished_batch.failure_rate == 50.0
        
        # Check batch was moved to completed
        assert tracker.current_batch is None
        assert len(tracker.completed_batches) == 1
        assert tracker.completed_batches[0] == finished_batch
        assert len(tracker.batch_records) == 0
    
    def test_finish_batch_when_none_active(self, tracker):
        """Test finishing when no batch is active."""
        result = tracker.finish_batch()
        
        assert result is None
    
    def test_track_batch_context_manager(self, tracker):
        """Test using track_batch as context manager."""
        with tracker.track_batch("batch_123", {"test": "value"}):
            tracker.record_success("record_1")
            tracker.record_failure("record_2", ValueError("error"), IngestionStage.PARSING)
        
        # Batch should be automatically finished
        assert tracker.current_batch is None
        assert len(tracker.completed_batches) == 1
        
        batch = tracker.completed_batches[0]
        assert batch.batch_id == "batch_123"
        assert batch.total_records == 2
        assert batch.metadata == {"test": "value"}
    
    def test_track_batch_context_manager_with_exception(self, tracker):
        """Test context manager handles exceptions properly."""
        with pytest.raises(ValueError):
            with tracker.track_batch("batch_123"):
                tracker.record_success("record_1")
                raise ValueError("Test exception")
        
        # Batch should still be finished despite exception
        assert tracker.current_batch is None
        assert len(tracker.completed_batches) == 1
    
    def test_get_current_batch_summary(self, tracker):
        """Test getting current batch summary."""
        # No active batch
        summary = tracker.get_current_batch_summary()
        assert summary is None
        
        # With active batch
        tracker.start_batch("batch_123")
        tracker.record_success("record_1")
        tracker.record_failure("record_2", ValueError("error"), IngestionStage.PARSING)
        
        summary = tracker.get_current_batch_summary()
        
        assert summary["batch_id"] == "batch_123"
        assert summary["total_records"] == 2
        assert summary["successful_records"] == 1
        assert summary["failed_records"] == 1
        assert summary["success_rate"] == 50.0
        assert "duration_seconds" in summary
    
    def test_get_summary(self, tracker):
        """Test getting comprehensive summary."""
        # Complete one batch
        tracker.start_batch("batch_1")
        tracker.record_success("record_1", processing_time_ms=50)
        tracker.record_failure("record_2", ValueError("error"), IngestionStage.PARSING)
        tracker.finish_batch()
        
        # Start another batch
        tracker.start_batch("batch_2")
        tracker.record_success("record_3", processing_time_ms=25)
        
        summary = tracker.get_summary()
        
        assert summary.pipeline_run_id == "test_pipeline_123"
        assert summary.total_batches == 2  # One completed + one current
        assert summary.total_records == 3
        assert summary.successful_records == 2
        assert summary.failed_records == 1
        assert summary.success_rate == pytest.approx(66.67, rel=1e-2)
    
    def test_clear_history(self, tracker):
        """Test clearing completed batch history."""
        tracker.start_batch("batch_1")
        tracker.record_success("record_1")
        tracker.finish_batch()
        
        tracker.start_batch("batch_2")
        tracker.record_success("record_2")
        
        assert len(tracker.completed_batches) == 1
        
        tracker.clear_history()
        
        assert len(tracker.completed_batches) == 0
        assert tracker.current_batch is not None  # Current batch preserved
    
    def test_persistence_integration(self, tracker, mock_repository):
        """Test integration with tracking repository."""
        tracker.record_success("record_1", record_type="Patient")
        
        # Should have called repository
        mock_repository.record_ingestion_stat.assert_called_once()
        
        # Check the call arguments
        call_args = mock_repository.record_ingestion_stat.call_args[0]
        stat = call_args[0]
        assert stat.record_id == "record_1"
        assert stat.record_type == "Patient"
        assert stat.status == ProcessingStatus.SUCCESS
    
    def test_persistence_integration_failure(self, tracker, mock_repository):
        """Test persistence integration for failures."""
        error = ValueError("Test error")
        tracker.record_failure(
            "record_1", 
            error, 
            IngestionStage.VALIDATION,
            ErrorCategory.VALIDATION_ERROR
        )
        
        # Check the repository call
        call_args = mock_repository.record_ingestion_stat.call_args[0]
        stat = call_args[0]
        assert stat.record_id == "record_1"
        assert stat.status == ProcessingStatus.FAILURE
        assert stat.error_category == ErrorCategory.VALIDATION_ERROR
        assert stat.error_message == "Test error"
    
    def test_persistence_error_handling(self, tracker, mock_repository):
        """Test handling of persistence errors."""
        mock_repository.record_ingestion_stat.side_effect = Exception("DB error")
        
        # Should not raise exception
        tracker.record_success("record_1")
        
        # Tracking should still work
        assert tracker.current_batch.total_records == 1


class TestIngestionTrackerExport:
    """Test export functionality of IngestionTracker."""
    
    def test_export_metrics_json(self, tracker):
        """Test exporting metrics to JSON format."""
        # Create test data
        tracker.start_batch("batch_1")
        tracker.record_success("record_1", record_type="Patient", processing_time_ms=50)
        tracker.record_failure("record_2", ValueError("error"), IngestionStage.PARSING)
        tracker.finish_batch()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            temp_path = f.name
        
        try:
            tracker.export_metrics(temp_path, format="json", include_details=True)
            
            # Verify file was created and contains expected data
            assert os.path.exists(temp_path)
            
            with open(temp_path, 'r') as f:
                data = json.load(f)
            
            assert "summary" in data
            assert "completed_batches" in data
            assert data["summary"]["total_records"] == 2
            assert data["summary"]["successful_records"] == 1
            assert data["summary"]["failed_records"] == 1
            assert len(data["completed_batches"]) == 1
            
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_export_metrics_csv(self, tracker):
        """Test exporting metrics to CSV format."""
        # Create test data
        tracker.start_batch("batch_1")
        tracker.record_success("record_1", processing_time_ms=50)
        tracker.record_failure("record_2", ValueError("error"), IngestionStage.PARSING)
        tracker.finish_batch()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            temp_path = f.name
        
        try:
            tracker.export_metrics(temp_path, format="csv")
            
            # Verify file was created and contains expected data
            assert os.path.exists(temp_path)
            
            with open(temp_path, 'r') as f:
                content = f.read()
            
            assert "Ingestion Summary Report" in content
            assert "Total Records,2" in content
            assert "Successful Records,1" in content
            assert "Failed Records,1" in content
            assert "Success Rate (%),50.00" in content
            
        finally:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
    
    def test_export_metrics_unsupported_format(self, tracker):
        """Test exporting with unsupported format."""
        with tempfile.NamedTemporaryFile() as f:
            with pytest.raises(ValueError, match="Unsupported export format"):
                tracker.export_metrics(f.name, format="xml")
    
    def test_export_metrics_when_disabled(self, disabled_tracker):
        """Test export warning when tracking is disabled."""
        with tempfile.NamedTemporaryFile() as f:
            # Should not raise exception but warn
            disabled_tracker.export_metrics(f.name)
    
    def test_export_creates_directory(self, tracker):
        """Test that export creates parent directories."""
        tracker.start_batch("batch_1")
        tracker.record_success("record_1")
        tracker.finish_batch()
        
        temp_dir = tempfile.mkdtemp()
        export_path = os.path.join(temp_dir, "subdir", "metrics.json")
        
        try:
            tracker.export_metrics(export_path)
            
            # Verify file was created (which means directory was created)
            assert os.path.exists(export_path)
            
            # Verify the parent directory was created
            assert os.path.exists(os.path.dirname(export_path))
            
        finally:
            # Clean up
            if os.path.exists(temp_dir):
                import shutil
                shutil.rmtree(temp_dir)


@pytest.mark.parametrize("outcome,expected_field", [
    (IngestionOutcome.SUCCESS, "successful_records"),
    (IngestionOutcome.FAILURE, "failed_records"),
    (IngestionOutcome.SKIPPED, "skipped_records"),
    (IngestionOutcome.PARTIAL_SUCCESS, "partial_success_records")
])
def test_record_outcome_updates_correct_field(tracker, outcome, expected_field):
    """Test that different outcomes update the correct metric fields."""
    tracker.start_batch("batch_123")
    
    if outcome == IngestionOutcome.SUCCESS:
        tracker.record_success("record_1")
    elif outcome == IngestionOutcome.FAILURE:
        tracker.record_failure("record_1", ValueError("error"), IngestionStage.PARSING)
    elif outcome == IngestionOutcome.SKIPPED:
        tracker.record_skip("record_1", "reason")
    elif outcome == IngestionOutcome.PARTIAL_SUCCESS:
        tracker.record_partial_success("record_1", ["issue"], IngestionStage.PARSING)
    
    batch = tracker.current_batch
    assert getattr(batch, expected_field) == 1
    assert batch.total_records == 1


class TestIngestionTrackerConfiguration:
    """Test configuration-dependent behavior."""
    
    def test_detailed_tracking_disabled(self):
        """Test behavior when detailed tracking is disabled."""
        config = Mock(spec=DataIntelligenceConfig)
        config.is_feature_enabled.side_effect = lambda feature, subfeature=None: {
            'ingestion_tracking': True,
            ('ingestion_tracking', 'detailed_tracking'): False,
            ('ingestion_tracking', 'auto_persist'): True
        }.get((feature, subfeature) if subfeature else feature, False)
        
        tracker = IngestionTracker(
            pipeline_run_id="test",
            stage_name="ingestion",
            config=config
        )
        
        tracker.start_batch("batch_1")
        tracker.record_success("record_1")
        
        # Should not store detailed records
        assert len(tracker.batch_records) == 0
        # But should still update batch metrics
        assert tracker.current_batch.total_records == 1
    
    def test_auto_persist_disabled(self):
        """Test behavior when auto-persist is disabled."""
        config = Mock(spec=DataIntelligenceConfig)
        config.is_feature_enabled.side_effect = lambda feature, subfeature=None: {
            'ingestion_tracking': True,
            ('ingestion_tracking', 'detailed_tracking'): True,
            ('ingestion_tracking', 'auto_persist'): False
        }.get((feature, subfeature) if subfeature else feature, False)
        
        repository = Mock()
        
        tracker = IngestionTracker(
            pipeline_run_id="test",
            stage_name="ingestion",
            config=config,
            repository=repository
        )
        
        tracker.record_success("record_1")
        
        # Should not call repository
        repository.record_ingestion_stat.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__])