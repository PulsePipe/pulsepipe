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
Unit tests for vector database tracker system.

Tests comprehensive vector database tracking with metrics, error handling, 
and export capabilities.
"""

import pytest
import tempfile
import json
import csv
from datetime import datetime, timedelta
from unittest.mock import Mock, MagicMock, patch
from pathlib import Path

from pulsepipe.audit.vector_db_tracker import (
    VectorDbTracker, VectorDbRecord, VectorDbBatchMetrics, VectorDbSummary,
    VectorDbOutcome, VectorDbStage
)
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig
from pulsepipe.persistence import ErrorCategory, ProcessingStatus, VectorDbStat


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
    repository.record_vector_db_stat = Mock()
    return repository


@pytest.fixture
def vector_db_tracker(mock_config, mock_repository):
    """Create vector db tracker instance."""
    return VectorDbTracker(
        pipeline_run_id="test-pipeline-123",
        stage_name="vectorstore",
        config=mock_config,
        repository=mock_repository
    )


@pytest.fixture
def disabled_vector_db_tracker():
    """Create disabled vector db tracker."""
    config = Mock(spec=DataIntelligenceConfig)
    config.is_feature_enabled = Mock(return_value=False)
    return VectorDbTracker(
        pipeline_run_id="test-pipeline-123",
        stage_name="vectorstore", 
        config=config
    )


class TestVectorDbRecord:
    """Test VectorDbRecord dataclass."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        record = VectorDbRecord(record_id="vector-123")
        
        assert record.record_id == "vector-123"
        assert record.source_id is None
        assert record.content_type is None
        assert record.outcome is None
        assert record.stage is None
        assert record.processing_time_ms is None
        assert record.vector_count is None
        assert record.index_name is None
        assert record.collection_name is None
        assert record.vector_store_type is None
        assert record.metadata == {}
        assert isinstance(record.timestamp, datetime)
    
    def test_init_with_all_fields(self):
        """Test initialization with all fields."""
        timestamp = datetime.now()
        metadata = {"test": "value"}
        
        record = VectorDbRecord(
            record_id="vector-123",
            source_id="source-456",
            content_type="clinical",
            outcome=VectorDbOutcome.SUCCESS,
            stage=VectorDbStage.VECTOR_INSERTION,
            error_category=ErrorCategory.NETWORK_ERROR,
            error_message="Test error",
            error_details={"detail": "test"},
            processing_time_ms=1500,
            vector_count=100,
            index_name="clinical_index",
            collection_name="clinical_collection",
            vector_store_type="qdrant",
            timestamp=timestamp,
            metadata=metadata
        )
        
        assert record.record_id == "vector-123"
        assert record.source_id == "source-456"
        assert record.content_type == "clinical"
        assert record.outcome == VectorDbOutcome.SUCCESS
        assert record.stage == VectorDbStage.VECTOR_INSERTION
        assert record.error_category == ErrorCategory.NETWORK_ERROR
        assert record.error_message == "Test error"
        assert record.error_details == {"detail": "test"}
        assert record.processing_time_ms == 1500
        assert record.vector_count == 100
        assert record.index_name == "clinical_index"
        assert record.collection_name == "clinical_collection"
        assert record.vector_store_type == "qdrant"
        assert record.timestamp == timestamp
        assert record.metadata == metadata


class TestVectorDbBatchMetrics:
    """Test VectorDbBatchMetrics dataclass."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        started_at = datetime.now()
        metrics = VectorDbBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="vectorstore",
            started_at=started_at
        )
        
        assert metrics.batch_id == "batch-123"
        assert metrics.pipeline_run_id == "pipeline-456"
        assert metrics.stage_name == "vectorstore"
        assert metrics.started_at == started_at
        assert metrics.completed_at is None
        assert metrics.total_records == 0
        assert metrics.successful_records == 0
        assert metrics.failed_records == 0
        assert metrics.skipped_records == 0
        assert metrics.partial_success_records == 0
        assert metrics.total_processing_time_ms == 0
        assert metrics.total_vectors_stored == 0
        assert metrics.success_rate == 0.0
        assert metrics.failure_rate == 0.0
        assert metrics.errors_by_category == {}
        assert metrics.errors_by_stage == {}
        assert metrics.vector_store_types == []
        assert metrics.index_names == []
        assert metrics.collection_names == []
        assert metrics.content_types == []
        assert metrics.metadata == {}
    
    def test_calculate_metrics(self):
        """Test metric calculations."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=10)
        
        metrics = VectorDbBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="vectorstore",
            started_at=started_at,
            completed_at=completed_at,
            total_records=100,
            successful_records=80,
            failed_records=20,
            total_processing_time_ms=50000,
            total_vectors_stored=8000
        )
        
        metrics.calculate_metrics()
        
        assert metrics.success_rate == 80.0
        assert metrics.failure_rate == 20.0
        assert metrics.avg_processing_time_ms == 500.0
        assert metrics.records_per_second == 10.0
        assert metrics.vectors_per_second == 800.0
        assert metrics.avg_vectors_per_record == 80.0
    
    def test_calculate_metrics_zero_division(self):
        """Test metric calculations with zero values."""
        started_at = datetime.now()
        
        metrics = VectorDbBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="vectorstore",
            started_at=started_at,
            total_records=0,
            total_vectors_stored=0
        )
        
        metrics.calculate_metrics()
        
        assert metrics.success_rate == 0.0
        assert metrics.failure_rate == 0.0
        assert metrics.avg_processing_time_ms == 0.0
        assert metrics.avg_vectors_per_record == 0.0
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=5)
        
        metrics = VectorDbBatchMetrics(
            batch_id="batch-123",
            pipeline_run_id="pipeline-456",
            stage_name="vectorstore",
            started_at=started_at,
            completed_at=completed_at,
            total_records=10
        )
        
        data = metrics.to_dict()
        
        assert data["batch_id"] == "batch-123"
        assert data["pipeline_run_id"] == "pipeline-456"
        assert data["stage_name"] == "vectorstore"
        assert data["started_at"] == started_at.isoformat()
        assert data["completed_at"] == completed_at.isoformat()
        assert data["total_records"] == 10


class TestVectorDbSummary:
    """Test VectorDbSummary dataclass."""
    
    def test_from_batches_empty(self):
        """Test summary creation from empty batch list."""
        summary = VectorDbSummary.from_batches("pipeline-123", [])
        
        assert summary.pipeline_run_id == "pipeline-123"
        assert summary.total_batches == 0
        assert summary.total_records == 0
        assert summary.recommendations != []
        assert "No records were processed" in summary.recommendations[0]
    
    def test_from_batches_with_data(self):
        """Test summary creation from batch list with data."""
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=10)
        
        batch1 = VectorDbBatchMetrics(
            batch_id="batch-1",
            pipeline_run_id="pipeline-123",
            stage_name="vectorstore",
            started_at=started_at,
            completed_at=completed_at,
            total_records=50,
            successful_records=45,
            failed_records=5,
            total_processing_time_ms=25000,
            total_vectors_stored=4500,
            errors_by_category={"connection": 3, "timeout": 2},
            errors_by_stage={"vector_insertion": 5},
            vector_store_types=["qdrant"],
            index_names=["clinical_index"],
            collection_names=["clinical_collection"],
            content_types=["clinical"]
        )
        batch1.calculate_metrics()
        
        batch2 = VectorDbBatchMetrics(
            batch_id="batch-2",
            pipeline_run_id="pipeline-123",
            stage_name="vectorstore",
            started_at=started_at,
            completed_at=completed_at,
            total_records=30,
            successful_records=28,
            failed_records=2,
            total_processing_time_ms=15000,
            total_vectors_stored=2800,
            errors_by_category={"connection": 1, "validation": 1},
            errors_by_stage={"indexing": 2},
            vector_store_types=["weaviate"],
            index_names=["operational_index"],
            collection_names=["operational_collection"],
            content_types=["operational"]
        )
        batch2.calculate_metrics()
        
        summary = VectorDbSummary.from_batches("pipeline-123", [batch1, batch2])
        
        assert summary.pipeline_run_id == "pipeline-123"
        assert summary.total_batches == 2
        assert summary.total_records == 80
        assert summary.successful_records == 73
        assert summary.failed_records == 7
        assert summary.success_rate == 91.25
        assert summary.failure_rate == 8.75
        assert summary.total_vectors_stored == 7300
        assert summary.avg_vectors_per_record == 91.25
        assert summary.errors_by_category == {"connection": 4, "timeout": 2, "validation": 1}
        assert summary.errors_by_stage == {"vector_insertion": 5, "indexing": 2}
        assert summary.vector_store_types == ["qdrant", "weaviate"]
        assert summary.index_names == ["clinical_index", "operational_index"]
        assert summary.collection_names == ["clinical_collection", "operational_collection"]
        assert summary.content_types == ["clinical", "operational"]
        assert len(summary.most_common_errors) > 0
        assert summary.most_common_errors[0]["category"] == "connection"
    
    def test_generate_recommendations_high_failure_rate(self):
        """Test recommendation generation for high failure rate."""
        summary = VectorDbSummary(
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
        summary = VectorDbSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            avg_processing_time_ms=3500
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("Average vector storage time is high" in rec for rec in recommendations)
    
    def test_generate_recommendations_low_throughput(self):
        """Test recommendation generation for low throughput."""
        summary = VectorDbSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            total_vectors_stored=200,
            vectors_per_second=20
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("Low vector storage throughput" in rec for rec in recommendations)
    
    def test_generate_recommendations_high_vectors_per_record(self):
        """Test recommendation generation for high vectors per record."""
        summary = VectorDbSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            total_vectors_stored=6000,
            avg_vectors_per_record=60
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("High number of vectors per record" in rec for rec in recommendations)
    
    def test_generate_recommendations_healthy(self):
        """Test recommendation generation for healthy metrics."""
        summary = VectorDbSummary(
            pipeline_run_id="test",
            generated_at=datetime.now(),
            total_records=100,
            successful_records=95,
            failed_records=5,
            success_rate=95.0,
            failure_rate=5.0,
            avg_processing_time_ms=1000,
            vectors_per_second=100,
            avg_vectors_per_record=10,
            total_vectors_stored=1000
        )
        
        recommendations = summary._generate_recommendations()
        
        assert any("appears healthy" in rec for rec in recommendations)
    
    def test_to_dict(self):
        """Test conversion to dictionary."""
        generated_at = datetime.now()
        
        summary = VectorDbSummary(
            pipeline_run_id="pipeline-123",
            generated_at=generated_at,
            total_records=100
        )
        
        data = summary.to_dict()
        
        assert data["pipeline_run_id"] == "pipeline-123"
        assert data["generated_at"] == generated_at.isoformat()
        assert data["total_records"] == 100


class TestVectorDbTracker:
    """Test VectorDbTracker main class."""
    
    def test_init_enabled(self, mock_config, mock_repository):
        """Test initialization when tracking is enabled."""
        tracker = VectorDbTracker(
            pipeline_run_id="test-pipeline-123",
            stage_name="vectorstore",
            config=mock_config,
            repository=mock_repository
        )
        
        assert tracker.pipeline_run_id == "test-pipeline-123"
        assert tracker.stage_name == "vectorstore"
        assert tracker.config == mock_config
        assert tracker.repository == mock_repository
        assert tracker.enabled is True
        assert tracker.current_batch is None
        assert tracker.batch_records == []
        assert tracker.completed_batches == []
    
    def test_init_disabled(self, disabled_vector_db_tracker):
        """Test initialization when tracking is disabled."""
        tracker = disabled_vector_db_tracker
        
        assert tracker.enabled is False
    
    def test_is_enabled(self, vector_db_tracker, disabled_vector_db_tracker):
        """Test is_enabled method."""
        assert vector_db_tracker.is_enabled() is True
        assert disabled_vector_db_tracker.is_enabled() is False
    
    def test_start_batch(self, vector_db_tracker):
        """Test starting a tracking batch."""
        metadata = {"test": "value"}
        
        vector_db_tracker.start_batch("batch-123", metadata)
        
        assert vector_db_tracker.current_batch is not None
        assert vector_db_tracker.current_batch.batch_id == "batch-123"
        assert vector_db_tracker.current_batch.pipeline_run_id == "test-pipeline-123"
        assert vector_db_tracker.current_batch.stage_name == "vectorstore"
        assert vector_db_tracker.current_batch.metadata == metadata
        assert vector_db_tracker.batch_records == []
    
    def test_start_batch_disabled(self, disabled_vector_db_tracker):
        """Test starting batch when tracking is disabled."""
        disabled_vector_db_tracker.start_batch("batch-123")
        
        assert disabled_vector_db_tracker.current_batch is None
    
    def test_start_batch_replaces_existing(self, vector_db_tracker):
        """Test starting a new batch replaces existing one."""
        vector_db_tracker.start_batch("batch-1")
        assert vector_db_tracker.current_batch.batch_id == "batch-1"
        
        vector_db_tracker.start_batch("batch-2")
        assert vector_db_tracker.current_batch.batch_id == "batch-2"
        assert len(vector_db_tracker.completed_batches) == 1
        assert vector_db_tracker.completed_batches[0].batch_id == "batch-1"
    
    def test_record_success(self, vector_db_tracker):
        """Test recording a successful vector database operation."""
        vector_db_tracker.start_batch("batch-123")
        
        vector_db_tracker.record_success(
            record_id="vector-456",
            source_id="source-789",
            content_type="clinical",
            processing_time_ms=800,
            vector_count=100,
            index_name="clinical_index",
            collection_name="clinical_collection",
            vector_store_type="qdrant",
            metadata={"test": "value"}
        )
        
        batch = vector_db_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 1
        assert batch.failed_records == 0
        assert batch.total_processing_time_ms == 800
        assert batch.total_vectors_stored == 100
        assert "qdrant" in batch.vector_store_types
        assert "clinical_index" in batch.index_names
        assert "clinical_collection" in batch.collection_names
        assert "clinical" in batch.content_types
    
    def test_record_success_disabled(self, disabled_vector_db_tracker):
        """Test recording success when tracking is disabled."""
        disabled_vector_db_tracker.record_success("vector-456")
        
        assert disabled_vector_db_tracker.current_batch is None
    
    def test_record_success_with_persistence(self, vector_db_tracker, mock_repository):
        """Test recording success with repository persistence."""
        vector_db_tracker.auto_persist = True
        vector_db_tracker.start_batch("batch-123")
        
        vector_db_tracker.record_success(
            record_id="vector-456",
            source_id="source-789",
            content_type="clinical",
            vector_count=100
        )
        
        mock_repository.record_vector_db_stat.assert_called_once()
        call_args = mock_repository.record_vector_db_stat.call_args[0][0]
        assert isinstance(call_args, VectorDbStat)
        assert call_args.record_id == "vector-456"
        assert call_args.source_id == "source-789"
        assert call_args.content_type == "clinical"
        assert call_args.status == ProcessingStatus.SUCCESS
    
    def test_record_failure(self, vector_db_tracker):
        """Test recording a failed vector database operation."""
        vector_db_tracker.start_batch("batch-123")
        
        error = ConnectionError("Connection failed")
        vector_db_tracker.record_failure(
            record_id="vector-456",
            error=error,
            stage=VectorDbStage.CONNECTION,
            error_category=ErrorCategory.NETWORK_ERROR,
            source_id="source-789",
            content_type="clinical",
            processing_time_ms=300,
            vector_store_type="qdrant"
        )
        
        batch = vector_db_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 0
        assert batch.failed_records == 1
        assert batch.total_processing_time_ms == 300
        assert batch.errors_by_category["network_error"] == 1
        assert batch.errors_by_stage["connection"] == 1
        assert "qdrant" in batch.vector_store_types
        assert "clinical" in batch.content_types
    
    def test_record_skip(self, vector_db_tracker):
        """Test recording a skipped vector database operation."""
        vector_db_tracker.start_batch("batch-123")
        
        vector_db_tracker.record_skip(
            record_id="vector-456",
            reason="No vectors to store",
            source_id="source-789",
            content_type="operational"
        )
        
        batch = vector_db_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 0
        assert batch.failed_records == 0
        assert batch.skipped_records == 1
        assert "operational" in batch.content_types
    
    def test_record_partial_success(self, vector_db_tracker):
        """Test recording a partially successful vector database operation."""
        vector_db_tracker.start_batch("batch-123")
        
        issues = ["Some vectors failed validation", "Index update partially failed"]
        vector_db_tracker.record_partial_success(
            record_id="vector-456",
            issues=issues,
            stage=VectorDbStage.VALIDATION,
            source_id="source-789",
            content_type="clinical",
            processing_time_ms=1200,
            vector_count=80,
            index_name="clinical_index",
            collection_name="clinical_collection",
            vector_store_type="weaviate"
        )
        
        batch = vector_db_tracker.current_batch
        assert batch.total_records == 1
        assert batch.successful_records == 0
        assert batch.failed_records == 0
        assert batch.partial_success_records == 1
        assert batch.total_processing_time_ms == 1200
        assert batch.total_vectors_stored == 80
    
    def test_finish_batch(self, vector_db_tracker):
        """Test finishing a tracking batch."""
        vector_db_tracker.start_batch("batch-123")
        vector_db_tracker.record_success("vector-1", vector_count=100)
        vector_db_tracker.record_success("vector-2", vector_count=150)
        
        finished_batch = vector_db_tracker.finish_batch()
        
        assert finished_batch is not None
        assert finished_batch.batch_id == "batch-123"
        assert finished_batch.total_records == 2
        assert finished_batch.total_vectors_stored == 250
        assert finished_batch.completed_at is not None
        assert finished_batch.success_rate == 100.0
        
        assert vector_db_tracker.current_batch is None
        assert vector_db_tracker.batch_records == []
        assert len(vector_db_tracker.completed_batches) == 1
        assert vector_db_tracker.completed_batches[0] == finished_batch
    
    def test_finish_batch_disabled(self, disabled_vector_db_tracker):
        """Test finishing batch when tracking is disabled."""
        result = disabled_vector_db_tracker.finish_batch()
        
        assert result is None
    
    def test_track_batch_context_manager(self, vector_db_tracker):
        """Test track_batch context manager."""
        metadata = {"test": "value"}
        
        with vector_db_tracker.track_batch("batch-123", metadata):
            assert vector_db_tracker.current_batch is not None
            assert vector_db_tracker.current_batch.batch_id == "batch-123"
            
            vector_db_tracker.record_success("vector-1", vector_count=50)
        
        # After context, batch should be finished
        assert vector_db_tracker.current_batch is None
        assert len(vector_db_tracker.completed_batches) == 1
        assert vector_db_tracker.completed_batches[0].batch_id == "batch-123"
        assert vector_db_tracker.completed_batches[0].total_records == 1
    
    def test_track_batch_context_manager_disabled(self, disabled_vector_db_tracker):
        """Test context manager when tracking is disabled."""
        with disabled_vector_db_tracker.track_batch("batch-123"):
            assert disabled_vector_db_tracker.current_batch is None
    
    def test_get_current_batch_summary(self, vector_db_tracker):
        """Test getting current batch summary."""
        assert vector_db_tracker.get_current_batch_summary() is None
        
        vector_db_tracker.start_batch("batch-123")
        vector_db_tracker.record_success("vector-1", vector_count=100)
        vector_db_tracker.record_failure("vector-2", ValueError("error"), VectorDbStage.VECTOR_INSERTION)
        
        summary = vector_db_tracker.get_current_batch_summary()
        
        assert summary is not None
        assert summary["batch_id"] == "batch-123"
        assert summary["total_records"] == 2
        assert summary["successful_records"] == 1
        assert summary["failed_records"] == 1
        assert summary["success_rate"] == 50.0
        assert summary["total_vectors_stored"] == 100
        assert "duration_seconds" in summary
    
    def test_get_summary(self, vector_db_tracker):
        """Test getting comprehensive summary."""
        # Create and finish first batch
        vector_db_tracker.start_batch("batch-1")
        vector_db_tracker.record_success("vector-1", vector_count=100)
        vector_db_tracker.record_success("vector-2", vector_count=150)
        vector_db_tracker.finish_batch()
        
        # Create second batch (current)
        vector_db_tracker.start_batch("batch-2")
        vector_db_tracker.record_success("vector-3", vector_count=200)
        
        summary = vector_db_tracker.get_summary()
        
        assert summary.pipeline_run_id == "test-pipeline-123"
        assert summary.total_batches == 2  # 1 completed + 1 current
        assert summary.total_records == 3
        assert summary.successful_records == 3
        assert summary.total_vectors_stored == 450
        assert len(summary.recommendations) > 0
    
    def test_export_metrics_json(self, vector_db_tracker):
        """Test exporting metrics to JSON format."""
        vector_db_tracker.start_batch("batch-123")
        vector_db_tracker.record_success("vector-1", vector_count=100)
        vector_db_tracker.finish_batch()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            export_path = f.name
        
        try:
            vector_db_tracker.export_metrics(export_path, format="json", include_details=True)
            
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
    
    def test_export_metrics_csv(self, vector_db_tracker):
        """Test exporting metrics to CSV format."""
        vector_db_tracker.start_batch("batch-123")
        vector_db_tracker.record_success("vector-1", vector_count=100)
        vector_db_tracker.finish_batch()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            export_path = f.name
        
        try:
            vector_db_tracker.export_metrics(export_path, format="csv", include_details=True)
            
            with open(export_path, 'r') as f:
                content = f.read()
            
            assert "Vector Database Summary Report" in content
            assert "test-pipeline-123" in content
            assert "Total Records" in content
            assert "Total Vectors Stored" in content
            assert "Batch Details" in content
            assert "batch-123" in content
            
        finally:
            Path(export_path).unlink(missing_ok=True)
    
    def test_export_metrics_disabled(self, disabled_vector_db_tracker):
        """Test exporting metrics when tracking is disabled."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json') as f:
            # Should not raise an error, but should log warning
            disabled_vector_db_tracker.export_metrics(f.name)
    
    def test_export_metrics_unsupported_format(self, vector_db_tracker):
        """Test exporting metrics with unsupported format."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml') as f:
            with pytest.raises(ValueError, match="Unsupported export format"):
                vector_db_tracker.export_metrics(f.name, format="xml")
    
    def test_clear_history(self, vector_db_tracker):
        """Test clearing completed batch history."""
        # Create and finish some batches
        vector_db_tracker.start_batch("batch-1")
        vector_db_tracker.record_success("vector-1")
        vector_db_tracker.finish_batch()
        
        vector_db_tracker.start_batch("batch-2")
        vector_db_tracker.record_success("vector-2")
        vector_db_tracker.finish_batch()
        
        assert len(vector_db_tracker.completed_batches) == 2
        
        # Start a new current batch
        vector_db_tracker.start_batch("batch-3")
        vector_db_tracker.record_success("vector-3")
        
        # Clear history
        vector_db_tracker.clear_history()
        
        assert len(vector_db_tracker.completed_batches) == 0
        assert vector_db_tracker.current_batch is not None  # Current batch preserved
        assert vector_db_tracker.current_batch.batch_id == "batch-3"
    
    def test_auto_batch_creation(self, vector_db_tracker):
        """Test automatic batch creation when recording without explicit batch."""
        vector_db_tracker.record_success("vector-1", vector_count=100)
        
        assert vector_db_tracker.current_batch is not None
        assert vector_db_tracker.current_batch.batch_id.startswith("auto_batch_")
        assert vector_db_tracker.current_batch.total_records == 1
    
    def test_detailed_tracking_enabled(self, mock_config, mock_repository):
        """Test detailed tracking when enabled."""
        mock_config.is_feature_enabled = Mock(side_effect=lambda feature, *args: {
            'vector_db_tracking': True,
            ('vector_db_tracking', 'detailed_tracking'): True
        }.get((feature,) + args, feature == 'vector_db_tracking'))
        
        tracker = VectorDbTracker(
            pipeline_run_id="test-pipeline-123",
            stage_name="vectorstore",
            config=mock_config,
            repository=mock_repository
        )
        
        tracker.start_batch("batch-123")
        tracker.record_success("vector-1", vector_count=100)
        tracker.record_success("vector-2", vector_count=150)
        
        assert tracker.detailed_tracking is True
        assert len(tracker.batch_records) == 2
        assert tracker.batch_records[0].record_id == "vector-1"
        assert tracker.batch_records[1].record_id == "vector-2"
    
    def test_detailed_tracking_disabled(self, mock_config, mock_repository):
        """Test detailed tracking when disabled."""
        mock_config.is_feature_enabled = Mock(side_effect=lambda feature, *args: {
            'vector_db_tracking': True,
            ('vector_db_tracking', 'detailed_tracking'): False
        }.get((feature,) + args, feature == 'vector_db_tracking'))
        
        tracker = VectorDbTracker(
            pipeline_run_id="test-pipeline-123",
            stage_name="vectorstore",
            config=mock_config,
            repository=mock_repository
        )
        
        tracker.start_batch("batch-123")
        tracker.record_success("vector-1", vector_count=100)
        tracker.record_success("vector-2", vector_count=150)
        
        assert tracker.detailed_tracking is False
        assert len(tracker.batch_records) == 0  # Records not stored when detailed tracking disabled
    
    def test_persistence_failure_handling(self, vector_db_tracker, mock_repository):
        """Test handling of persistence failures."""
        vector_db_tracker.auto_persist = True
        mock_repository.record_vector_db_stat.side_effect = Exception("Database error")
        
        vector_db_tracker.start_batch("batch-123")
        
        # Should not raise exception, but should log error
        vector_db_tracker.record_success("vector-1", vector_count=100)
        
        # Verify the record was still added to batch despite persistence failure
        assert vector_db_tracker.current_batch.total_records == 1
        assert vector_db_tracker.current_batch.successful_records == 1


class TestVectorDbOutcome:
    """Test VectorDbOutcome enum."""
    
    def test_enum_values(self):
        """Test enum values are correct."""
        assert VectorDbOutcome.SUCCESS.value == "success"
        assert VectorDbOutcome.FAILURE.value == "failure"
        assert VectorDbOutcome.PARTIAL_SUCCESS.value == "partial_success"
        assert VectorDbOutcome.SKIPPED.value == "skipped"


class TestVectorDbStage:
    """Test VectorDbStage enum."""
    
    def test_enum_values(self):
        """Test enum values are correct."""
        assert VectorDbStage.CONNECTION.value == "connection"
        assert VectorDbStage.INDEX_CREATION.value == "index_creation"
        assert VectorDbStage.COLLECTION_SETUP.value == "collection_setup"
        assert VectorDbStage.VECTOR_INSERTION.value == "vector_insertion"
        assert VectorDbStage.METADATA_INSERTION.value == "metadata_insertion"
        assert VectorDbStage.INDEXING.value == "indexing"
        assert VectorDbStage.VALIDATION.value == "validation"
        assert VectorDbStage.OPTIMIZATION.value == "optimization"


if __name__ == "__main__":
    pytest.main([__file__])