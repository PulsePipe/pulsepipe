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

"""
Unit tests for chunking tracker system.

Tests comprehensive chunking tracking with metrics, error handling, 
and export capabilities.
"""

import pytest
import tempfile
import json
import csv
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path

from pulsepipe.audit.chunking_tracker import (
    ChunkingTracker, ChunkingRecord, ChunkingBatchMetrics, ChunkingSummary,
    ChunkingOutcome, ChunkingStage
)
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig
from pulsepipe.persistence import ErrorCategory, ProcessingStatus, ChunkingStat


@pytest.fixture
def mock_config():
    """Create mock data intelligence configuration."""
    config = Mock(spec=DataIntelligenceConfig)
    config.is_feature_enabled = Mock(return_value=True)
    return config


@pytest.fixture
def mock_repository():
    """Create mock tracking repository."""
    repository = Mock()
    repository.record_chunking_stat = Mock()
    return repository


@pytest.fixture
def chunking_tracker(mock_config, mock_repository):
    """Create chunking tracker instance."""
    return ChunkingTracker(
        pipeline_run_id="test-pipeline-123",
        stage_name="chunking",
        config=mock_config,
        repository=mock_repository
    )


@pytest.fixture
def disabled_chunking_tracker():
    """Create disabled chunking tracker."""
    config = Mock(spec=DataIntelligenceConfig)
    config.is_feature_enabled = Mock(return_value=False)
    return ChunkingTracker(
        pipeline_run_id="test-pipeline-123",
        stage_name="chunking", 
        config=config
    )


class TestChunkingRecord:
    """Test ChunkingRecord dataclass."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        record = ChunkingRecord(record_id="chunk-123")
        
        assert record.record_id == "chunk-123"
        assert record.source_id is None
        assert record.chunk_type is None
        assert record.outcome is None
        assert record.stage is None
        assert record.processing_time_ms is None
        assert record.chunk_count is None
        assert record.total_chars is None
        assert record.avg_chunk_size is None
        assert record.overlap_chars is None
        assert record.chunker_type is None
        assert record.metadata == {}
        assert isinstance(record.timestamp, datetime)
    
    def test_init_with_all_fields(self):
        """Test initialization with all fields."""
        timestamp = datetime.now()
        metadata = {"test": "value"}
        
        record = ChunkingRecord(
            record_id="chunk-123",
            source_id="source-456",
            chunk_type="clinical",
            outcome=ChunkingOutcome.SUCCESS,
            stage=ChunkingStage.SEGMENTATION,
            error_category=ErrorCategory.VALIDATION_ERROR,
            error_message="Test error",
            error_details={"detail": "test"},
            processing_time_ms=1500,
            chunk_count=5,
            total_chars=1000,
            avg_chunk_size=200,
            overlap_chars=50,
            chunker_type="clinical_chunker",
            timestamp=timestamp,
            metadata=metadata
        )
        
        assert record.record_id == "chunk-123"
        assert record.source_id == "source-456"
        assert record.chunk_type == "clinical"
        assert record.outcome == ChunkingOutcome.SUCCESS
        assert record.stage == ChunkingStage.SEGMENTATION
        assert record.error_category == ErrorCategory.VALIDATION_ERROR
        assert record.error_message == "Test error"
        assert record.error_details == {"detail": "test"}
        assert record.processing_time_ms == 1500
        assert record.chunk_count == 5
        assert record.total_chars == 1000
        assert record.avg_chunk_size == 200
        assert record.overlap_chars == 50
        assert record.chunker_type == "clinical_chunker"
        assert record.timestamp == timestamp
        assert record.metadata == metadata


class TestChunkingBatchMetrics:
    """Test ChunkingBatchMetrics dataclass."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        started_at = datetime.now()
        metrics = ChunkingBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="chunking",
            started_at=started_at
        )
        
        assert metrics.batch_id == "batch-123"
        assert metrics.pipeline_run_id == "pipeline-456"
        assert metrics.stage_name == "chunking"
        assert metrics.started_at == started_at
        assert metrics.completed_at is None
        assert metrics.total_records == 0
        assert metrics.successful_records == 0
        assert metrics.failed_records == 0
        assert metrics.skipped_records == 0
        assert metrics.partial_success_records == 0
        assert metrics.total_processing_time_ms == 0
        assert metrics.total_chunks_created == 0
        assert metrics.total_chars_processed == 0
        assert metrics.success_rate == 0.0
        assert metrics.failure_rate == 0.0
        assert metrics.errors_by_category == {}
        assert metrics.errors_by_stage == {}
        assert metrics.chunker_types == []
        assert metrics.chunk_types == []
        assert metrics.metadata == {}
    
    def test_calculate_metrics(self):
        """Test metric calculations."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=10)
        
        metrics = ChunkingBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="chunking",
            started_at=started_at,
            completed_at=completed_at,
            total_records=100,
            successful_records=80,
            failed_records=20,
            total_processing_time_ms=50000,
            total_chunks_created=400,
            total_chars_processed=8000
        )
        
        metrics.calculate_metrics()
        
        assert metrics.success_rate == 80.0
        assert metrics.failure_rate == 20.0
        assert metrics.avg_processing_time_ms == 500.0
        assert metrics.records_per_second == 10.0
        assert metrics.chunks_per_second == 40.0
        assert metrics.chars_per_second == 800.0
        assert metrics.avg_chunks_per_record == 4.0
        assert metrics.avg_chunk_size == 20.0
    
    def test_calculate_metrics_zero_division(self):
        """Test metric calculations with zero values."""
        started_at = datetime.now()
        
        metrics = ChunkingBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="chunking",
            started_at=started_at,
            total_records=0,
            total_chunks_created=0
        )
        
        metrics.calculate_metrics()
        
        assert metrics.success_rate == 0.0
        assert metrics.failure_rate == 0.0
        assert metrics.avg_processing_time_ms == 0.0
        assert metrics.avg_chunks_per_record == 0.0
        assert metrics.avg_chunk_size == 0.0
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=5)
        
        metrics = ChunkingBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="chunking",
            started_at=started_at,
            completed_at=completed_at,
            total_records=10
        )
        
        data = metrics.to_dict()
        
        assert data["batch_id"] == "batch-123"
        assert data["pipeline_run_id"] == "pipeline-456"
        assert data["stage_name"] == "chunking"
        assert data["started_at"] == started_at.isoformat()
        assert data["completed_at"] == completed_at.isoformat()
        assert data["total_records"] == 10


class TestChunkingSummary:
    """Test ChunkingSummary dataclass."""
    
    def test_from_batches_empty(self):
        """Test summary creation from empty batch list."""
        summary = ChunkingSummary.from_batches("pipeline-123", [])
        
        assert summary.pipeline_run_id == "pipeline-123"
        assert summary.total_batches == 0
        assert summary.total_records == 0
        assert summary.recommendations != []
        assert "No records were processed" in summary.recommendations[0]
    
    def test_from_batches_with_data(self):
        """Test summary creation from batch list with data."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=10)
        
        batch1 = ChunkingBatchMetrics(
            batch_id="batch-1",
            pipeline_run_id="pipeline-123",
            stage_name="chunking",
            started_at=started_at,
            completed_at=completed_at,
            total_records=50,
            successful_records=45,
            failed_records=5,
            total_processing_time_ms=25000,
            total_chunks_created=200,
            total_chars_processed=4000,
            errors_by_category={"validation": 3, "processing": 2},
            errors_by_stage={"segmentation": 5},
            chunker_types=["clinical"],
            chunk_types=["clinical"]
        )
        batch1.calculate_metrics()
        
        batch2 = ChunkingBatchMetrics(
            batch_id="batch-2",
            pipeline_run_id="pipeline-123",
            stage_name="chunking",
            started_at=started_at,
            completed_at=completed_at,
            total_records=30,
            successful_records=28,
            failed_records=2,
            total_processing_time_ms=15000,
            total_chunks_created=120,
            total_chars_processed=2400,
            errors_by_category={"validation": 1, "timeout": 1},
            errors_by_stage={"segmentation": 2},
            chunker_types=["operational"],
            chunk_types=["operational"]
        )
        batch2.calculate_metrics()
        
        summary = ChunkingSummary.from_batches("pipeline-123", [batch1, batch2])
        
        assert summary.pipeline_run_id == "pipeline-123"
        assert summary.total_batches == 2
        assert summary.total_records == 80
        assert summary.successful_records == 73
        assert summary.failed_records == 7
        assert summary.success_rate == 91.25
        assert summary.failure_rate == 8.75
        assert summary.total_chunks_created == 320
        assert summary.total_chars_processed == 6400
        assert summary.avg_chunks_per_record == 4.0
        assert summary.avg_chunk_size == 20.0
        assert summary.errors_by_category == {"validation": 4, "processing": 2, "timeout": 1}
        assert summary.errors_by_stage == {"segmentation": 7}
        assert summary.chunker_types == ["clinical", "operational"]
        assert summary.chunk_types == ["clinical", "operational"]
        assert len(summary.most_common_errors) > 0
        assert summary.most_common_errors[0]["category"] == "validation"
    
    def test_generate_recommendations_high_failure_rate(self):
        """Test recommendation generation for high failure rate."""
        summary = ChunkingSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            failed_records=25,
            failure_rate=25.0
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("High failure rate" in rec for rec in recommendations)
    
    def test_generate_recommendations_high_processing_time(self):
        """Test recommendation generation for high processing time."""
        summary = ChunkingSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            avg_processing_time_ms=1500
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("Average chunking time is high" in rec for rec in recommendations)
    
    def test_generate_recommendations_large_chunks(self):
        """Test recommendation generation for large chunks."""
        summary = ChunkingSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            avg_chunk_size=2500
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("Average chunk size is large" in rec for rec in recommendations)
    
    def test_generate_recommendations_small_chunks(self):
        """Test recommendation generation for small chunks."""
        summary = ChunkingSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            avg_chunk_size=150
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("Average chunk size is small" in rec for rec in recommendations)
    
    def test_generate_recommendations_healthy(self):
        """Test recommendation generation for healthy metrics."""
        summary = ChunkingSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            successful_records=95,
            failed_records=5,
            success_rate=95.0,
            failure_rate=5.0,
            avg_processing_time_ms=500,
            avg_chunk_size=800,
            avg_chunks_per_record=3.0
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("appears healthy" in rec for rec in recommendations)
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        generated_at = datetime.now()
        
        summary = ChunkingSummary(
            pipeline_run_id="pipeline-123",
            generated_at=generated_at,
            total_records=100
        )
        
        data = summary.to_dict()
        
        assert data["pipeline_run_id"] == "pipeline-123"
        assert data["generated_at"] == generated_at.isoformat()
        assert data["total_records"] == 100


class TestChunkingTracker:
    """Test ChunkingTracker main class."""
    
    def test_init_enabled(self, mock_config, mock_repository):
        """Test initialization when tracking is enabled."""
        tracker = ChunkingTracker(
            pipeline_run_id="test-pipeline-123",
            stage_name="chunking",
            config=mock_config,
            repository=mock_repository
        )
        
        assert tracker.pipeline_run_id == "test-pipeline-123"
        assert tracker.stage_name == "chunking"
        assert tracker.config == mock_config
        assert tracker.repository == mock_repository
        assert tracker.enabled is True
        assert tracker.current_batch is None
        assert tracker.batch_records == []
        assert tracker.completed_batches == []
    
    def test_init_disabled(self, disabled_chunking_tracker):
        """Test initialization when tracking is disabled."""
        tracker = disabled_chunking_tracker
        
        assert tracker.enabled is False
    
    def test_is_enabled(self, chunking_tracker, disabled_chunking_tracker):
        """Test is_enabled method."""
        assert chunking_tracker.is_enabled() is True
        assert disabled_chunking_tracker.is_enabled() is False
    
    def test_start_batch(self, chunking_tracker):
        """Test starting a tracking batch."""
        metadata = {"test": "value"}
        
        chunking_tracker.start_batch("batch-123", metadata)
        
        assert chunking_tracker.current_batch is not None
        assert chunking_tracker.current_batch.batch_id == "batch-123"
        assert chunking_tracker.current_batch.pipeline_run_id == "test-pipeline-123"
        assert chunking_tracker.current_batch.stage_name == "chunking"
        assert chunking_tracker.current_batch.metadata == metadata
        assert chunking_tracker.batch_records == []
    
    def test_start_batch_disabled(self, disabled_chunking_tracker):
        """Test starting batch when tracking is disabled."""
        disabled_chunking_tracker.start_batch("batch-123")
        
        assert disabled_chunking_tracker.current_batch is None
    
    def test_start_batch_replaces_existing(self, chunking_tracker):
        """Test starting a new batch replaces existing one."""
        chunking_tracker.start_batch("batch-1")
        assert chunking_tracker.current_batch.batch_id == "batch-1"
        
        chunking_tracker.start_batch("batch-2")
        assert chunking_tracker.current_batch.batch_id == "batch-2"
        assert len(chunking_tracker.completed_batches) == 1
        assert chunking_tracker.completed_batches[0].batch_id == "batch-1"
    
    def test_record_success(self, chunking_tracker):
        """Test recording a successful chunking operation."""
        chunking_tracker.start_batch("batch-123")
        
        chunking_tracker.record_success(
            record_id="chunk-456",
            source_id="source-789",
            chunk_type="clinical",
            processing_time_ms=500,
            chunk_count=3,
            total_chars=600,
            overlap_chars=50,
            chunker_type="clinical_chunker",
            metadata={"test": "value"}
        )
        
        batch = chunking_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 1
        assert batch.failed_records == 0
        assert batch.total_processing_time_ms == 500
        assert batch.total_chunks_created == 3
        assert batch.total_chars_processed == 600
        assert "clinical_chunker" in batch.chunker_types
        assert "clinical" in batch.chunk_types
    
    def test_record_success_disabled(self, disabled_chunking_tracker):
        """Test recording success when tracking is disabled."""
        disabled_chunking_tracker.record_success("chunk-456")
        
        assert disabled_chunking_tracker.current_batch is None
    
    def test_record_success_with_persistence(self, chunking_tracker, mock_repository):
        """Test recording success with repository persistence."""
        chunking_tracker.auto_persist = True
        chunking_tracker.start_batch("batch-123")
        
        chunking_tracker.record_success(
            record_id="chunk-456",
            source_id="source-789",
            chunk_type="clinical",
            chunk_count=3
        )
        
        mock_repository.record_chunking_stat.assert_called_once()
        call_args = mock_repository.record_chunking_stat.call_args[0][0]
        assert isinstance(call_args, ChunkingStat)
        assert call_args.record_id == "chunk-456"
        assert call_args.source_id == "source-789"
        assert call_args.chunk_type == "clinical"
        assert call_args.status == ProcessingStatus.SUCCESS
    
    def test_record_failure(self, chunking_tracker):
        """Test recording a failed chunking operation."""
        chunking_tracker.start_batch("batch-123")
        
        error = ValueError("Test error")
        chunking_tracker.record_failure(
            record_id="chunk-456",
            error=error,
            stage=ChunkingStage.SEGMENTATION,
            error_category=ErrorCategory.VALIDATION_ERROR,
            source_id="source-789",
            chunk_type="clinical",
            processing_time_ms=300,
            chunker_type="clinical_chunker"
        )
        
        batch = chunking_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 0
        assert batch.failed_records == 1
        assert batch.total_processing_time_ms == 300
        assert batch.errors_by_category["validation_error"] == 1
        assert batch.errors_by_stage["segmentation"] == 1
        assert "clinical_chunker" in batch.chunker_types
        assert "clinical" in batch.chunk_types
    
    def test_record_skip(self, chunking_tracker):
        """Test recording a skipped chunking operation."""
        chunking_tracker.start_batch("batch-123")
        
        chunking_tracker.record_skip(
            record_id="chunk-456",
            reason="Empty content",
            source_id="source-789",
            chunk_type="clinical"
        )
        
        batch = chunking_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 0
        assert batch.failed_records == 0
        assert batch.skipped_records == 1
        assert "clinical" in batch.chunk_types
    
    def test_record_partial_success(self, chunking_tracker):
        """Test recording a partially successful chunking operation."""
        chunking_tracker.start_batch("batch-123")
        
        issues = ["Some segments too small", "Overlap adjustment needed"]
        chunking_tracker.record_partial_success(
            record_id="chunk-456",
            issues=issues,
            stage=ChunkingStage.OVERLAP_PROCESSING,
            source_id="source-789",
            chunk_type="clinical",
            processing_time_ms=800,
            chunk_count=2,
            total_chars=400
        )
        
        batch = chunking_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 0
        assert batch.failed_records == 0
        assert batch.partial_success_records == 1
        assert batch.total_processing_time_ms == 800
        assert batch.total_chunks_created == 2
        assert batch.total_chars_processed == 400
    
    def test_finish_batch(self, chunking_tracker):
        """Test finishing a tracking batch."""
        chunking_tracker.start_batch("batch-123")
        chunking_tracker.record_success("chunk-1", chunk_count=5, total_chars=1000)
        chunking_tracker.record_success("chunk-2", chunk_count=3, total_chars=600)
        
        finished_batch = chunking_tracker.finish_batch()
        
        assert finished_batch is not None
        assert finished_batch.batch_id == "batch-123"
        assert finished_batch.total_records == 2
        assert finished_batch.total_chunks_created == 8
        assert finished_batch.completed_at is not None
        assert finished_batch.success_rate == 100.0
        
        assert chunking_tracker.current_batch is None
        assert chunking_tracker.batch_records == []
        assert len(chunking_tracker.completed_batches) == 1
        assert chunking_tracker.completed_batches[0] == finished_batch
    
    def test_finish_batch_disabled(self, disabled_chunking_tracker):
        """Test finishing batch when tracking is disabled."""
        result = disabled_chunking_tracker.finish_batch()
        
        assert result is None
    
    def test_track_batch_context_manager(self, chunking_tracker):
        """Test track_batch context manager."""
        metadata = {"test": "value"}
        
        with chunking_tracker.track_batch("batch-123", metadata):
            assert chunking_tracker.current_batch is not None
            assert chunking_tracker.current_batch.batch_id == "batch-123"
            
            chunking_tracker.record_success("chunk-1", chunk_count=2)
        
        # After context, batch should be finished
        assert chunking_tracker.current_batch is None
        assert len(chunking_tracker.completed_batches) == 1
        assert chunking_tracker.completed_batches[0].batch_id == "batch-123"
        assert chunking_tracker.completed_batches[0].total_records == 1
    
    def test_track_batch_context_manager_disabled(self, disabled_chunking_tracker):
        """Test context manager when tracking is disabled."""
        with disabled_chunking_tracker.track_batch("batch-123"):
            assert disabled_chunking_tracker.current_batch is None
    
    def test_get_current_batch_summary(self, chunking_tracker):
        """Test getting current batch summary."""
        assert chunking_tracker.get_current_batch_summary() is None
        
        chunking_tracker.start_batch("batch-123")
        chunking_tracker.record_success("chunk-1", chunk_count=5)
        chunking_tracker.record_failure("chunk-2", ValueError("error"), ChunkingStage.SEGMENTATION)
        
        summary = chunking_tracker.get_current_batch_summary()
        
        assert summary is not None
        assert summary["batch_id"] == "batch-123"
        assert summary["total_records"] == 2
        assert summary["successful_records"] == 1
        assert summary["failed_records"] == 1
        assert summary["success_rate"] == 50.0
        assert summary["total_chunks_created"] == 5
        assert "duration_seconds" in summary
    
    def test_get_summary(self, chunking_tracker):
        """Test getting comprehensive summary."""
        # Create and finish first batch
        chunking_tracker.start_batch("batch-1")
        chunking_tracker.record_success("chunk-1", chunk_count=3, total_chars=600)
        chunking_tracker.record_success("chunk-2", chunk_count=2, total_chars=400)
        chunking_tracker.finish_batch()
        
        # Create second batch (current)
        chunking_tracker.start_batch("batch-2")
        chunking_tracker.record_success("chunk-3", chunk_count=4, total_chars=800)
        
        summary = chunking_tracker.get_summary()
        
        assert summary.pipeline_run_id == "test-pipeline-123"
        assert summary.total_batches == 2  # 1 completed + 1 current
        assert summary.total_records == 3
        assert summary.successful_records == 3
        assert summary.total_chunks_created == 9
        assert summary.total_chars_processed == 1800
        assert len(summary.recommendations) > 0
    
    def test_export_metrics_json(self, chunking_tracker):
        """Test exporting metrics to JSON format."""
        chunking_tracker.start_batch("batch-123")
        chunking_tracker.record_success("chunk-1", chunk_count=3, total_chars=600)
        chunking_tracker.finish_batch()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            export_path = f.name
        
        try:
            chunking_tracker.export_metrics(export_path, format="json", include_details=True)
            
            with open(export_path, 'r') as f:
                data = json.load(f)
            
            assert "summary" in data
            assert "completed_batches" in data
            assert data["summary"]["pipeline_run_id"] == "test-pipeline-123"
            assert data["summary"]["total_records"] == 1
            assert len(data["completed_batches"]) == 1
            assert data["completed_batches"][0]["batch_id"] == "batch-123"
            
        finally:
            Path(export_path).unlink(missing_ok=True)
    
    def test_export_metrics_csv(self, chunking_tracker):
        """Test exporting metrics to CSV format."""
        chunking_tracker.start_batch("batch-123")
        chunking_tracker.record_success("chunk-1", chunk_count=3, total_chars=600)
        chunking_tracker.finish_batch()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            export_path = f.name
        
        try:
            chunking_tracker.export_metrics(export_path, format="csv", include_details=True)
            
            with open(export_path, 'r') as f:
                content = f.read()
            
            assert "Chunking Summary Report" in content
            assert "test-pipeline-123" in content
            assert "Total Records" in content
            assert "Batch Details" in content
            assert "batch-123" in content
            
        finally:
            Path(export_path).unlink(missing_ok=True)
    
    def test_export_metrics_disabled(self, disabled_chunking_tracker):
        """Test exporting metrics when tracking is disabled."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as f:
            # Should not raise an error, but should log warning
            disabled_chunking_tracker.export_metrics(f.name)
    
    def test_export_metrics_unsupported_format(self, chunking_tracker):
        """Test exporting metrics with unsupported format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml') as f:
            with pytest.raises(ValueError, match="Unsupported export format"):
                chunking_tracker.export_metrics(f.name, format="xml")
    
    def test_clear_history(self, chunking_tracker):
        """Test clearing completed batch history."""
        # Create and finish some batches
        chunking_tracker.start_batch("batch-1")
        chunking_tracker.record_success("chunk-1")
        chunking_tracker.finish_batch()
        
        chunking_tracker.start_batch("batch-2")
        chunking_tracker.record_success("chunk-2")
        chunking_tracker.finish_batch()
        
        assert len(chunking_tracker.completed_batches) == 2
        
        # Start a new current batch
        chunking_tracker.start_batch("batch-3")
        chunking_tracker.record_success("chunk-3")
        
        # Clear history
        chunking_tracker.clear_history()
        
        assert len(chunking_tracker.completed_batches) == 0
        assert chunking_tracker.current_batch is not None  # Current batch preserved
        assert chunking_tracker.current_batch.batch_id == "batch-3"
    
    def test_auto_batch_creation(self, chunking_tracker):
        """Test automatic batch creation when recording without explicit batch."""
        chunking_tracker.record_success("chunk-1", chunk_count=2)
        
        assert chunking_tracker.current_batch is not None
        assert chunking_tracker.current_batch.batch_id.startswith("auto_batch_")
        assert chunking_tracker.current_batch.total_records == 1
    
    def test_detailed_tracking_enabled(self, mock_config, mock_repository):
        """Test detailed tracking when enabled."""
        mock_config.is_feature_enabled = Mock(side_effect=lambda feature, *args: {
            'chunking_tracking': True,
            ('chunking_tracking', 'detailed_tracking'): True
        }.get((feature,) + args, feature == 'chunking_tracking'))
        
        tracker = ChunkingTracker(
            pipeline_run_id="test-pipeline-123",
            stage_name="chunking",
            config=mock_config,
            repository=mock_repository
        )
        
        tracker.start_batch("batch-123")
        tracker.record_success("chunk-1", chunk_count=2)
        tracker.record_success("chunk-2", chunk_count=3)
        
        assert tracker.detailed_tracking is True
        assert len(tracker.batch_records) == 2
        assert tracker.batch_records[0].record_id == "chunk-1"
        assert tracker.batch_records[1].record_id == "chunk-2"
    
    def test_detailed_tracking_disabled(self, mock_config, mock_repository):
        """Test detailed tracking when disabled."""
        mock_config.is_feature_enabled = Mock(side_effect=lambda feature, *args: {
            'chunking_tracking': True,
            ('chunking_tracking', 'detailed_tracking'): False
        }.get((feature,) + args, feature == 'chunking_tracking'))
        
        tracker = ChunkingTracker(
            pipeline_run_id="test-pipeline-123",
            stage_name="chunking",
            config=mock_config,
            repository=mock_repository
        )
        
        tracker.start_batch("batch-123")
        tracker.record_success("chunk-1", chunk_count=2)
        tracker.record_success("chunk-2", chunk_count=3)
        
        assert tracker.detailed_tracking is False
        assert len(tracker.batch_records) == 0  # Records not stored when detailed tracking disabled
    
    def test_persistence_failure_handling(self, chunking_tracker, mock_repository):
        """Test handling of persistence failures."""
        chunking_tracker.auto_persist = True
        mock_repository.record_chunking_stat.side_effect = Exception("Database error")
        
        chunking_tracker.start_batch("batch-123")
        
        # Should not raise exception, but should log error
        chunking_tracker.record_success("chunk-1", chunk_count=2)
        
        # Verify the record was still added to batch despite persistence failure
        assert chunking_tracker.current_batch.total_records == 1
        assert chunking_tracker.current_batch.successful_records == 1


class TestChunkingOutcome:
    """Test ChunkingOutcome enum."""
    
    def test_enum_values(self):
        """Test enum values are correct."""
        assert ChunkingOutcome.SUCCESS.value == "success"
        assert ChunkingOutcome.FAILURE.value == "failure"
        assert ChunkingOutcome.PARTIAL_SUCCESS.value == "partial_success"
        assert ChunkingOutcome.SKIPPED.value == "skipped"


class TestChunkingStage:
    """Test ChunkingStage enum."""
    
    def test_enum_values(self):
        """Test enum values are correct."""
        assert ChunkingStage.TEXT_EXTRACTION.value == "text_extraction"
        assert ChunkingStage.SEGMENTATION.value == "segmentation"
        assert ChunkingStage.OVERLAP_PROCESSING.value == "overlap_processing"
        assert ChunkingStage.VALIDATION.value == "validation"
        assert ChunkingStage.METADATA_ASSIGNMENT.value == "metadata_assignment"


if __name__ == "__main__":
    pytest.main([__file__])