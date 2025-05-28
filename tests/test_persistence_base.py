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

# tests/test_persistence_base.py

"""
Unit tests for persistence base classes.

Tests the abstract base classes and data structures used across
all persistence provider implementations.
"""

import pytest
from datetime import datetime
from typing import Dict, Any, Optional

from pulsepipe.persistence.base import (
    BasePersistenceProvider,
    BaseTrackingRepository,
    PipelineRunSummary,
    IngestionStat,
    QualityMetric
)
from pulsepipe.persistence.models import ProcessingStatus, ErrorCategory


class MockPersistenceProvider(BasePersistenceProvider):
    """Mock implementation for testing base functionality."""
    
    def __init__(self):
        self.connected = False
        self.schema_initialized = False
        self.data = {
            "pipeline_runs": {},
            "ingestion_stats": {},
            "quality_metrics": {},
            "audit_events": {},
            "performance_metrics": {},
            "system_metrics": {},
            "failed_records": {}
        }
        self.next_id = 1
    
    async def connect(self) -> None:
        self.connected = True
    
    async def disconnect(self) -> None:
        self.connected = False
    
    async def initialize_schema(self) -> None:
        self.schema_initialized = True
    
    async def health_check(self) -> bool:
        return self.connected
    
    async def start_pipeline_run(self, run_id: str, name: str, 
                               config_snapshot: Optional[Dict[str, Any]] = None) -> None:
        self.data["pipeline_runs"][run_id] = {
            "id": run_id,
            "name": name,
            "started_at": datetime.now(),
            "completed_at": None,
            "status": "running",
            "total_records": 0,
            "successful_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
            "config_snapshot": config_snapshot,
            "error_message": None
        }
    
    async def complete_pipeline_run(self, run_id: str, status: str = "completed", 
                                  error_message: Optional[str] = None) -> None:
        if run_id in self.data["pipeline_runs"]:
            self.data["pipeline_runs"][run_id].update({
                "completed_at": datetime.now(),
                "status": status,
                "error_message": error_message
            })
    
    async def update_pipeline_run_counts(self, run_id: str, total: int = 0, 
                                       successful: int = 0, failed: int = 0, 
                                       skipped: int = 0) -> None:
        if run_id in self.data["pipeline_runs"]:
            run = self.data["pipeline_runs"][run_id]
            run["total_records"] += total
            run["successful_records"] += successful
            run["failed_records"] += failed
            run["skipped_records"] += skipped
    
    async def get_pipeline_run(self, run_id: str) -> Optional[PipelineRunSummary]:
        run_data = self.data["pipeline_runs"].get(run_id)
        if not run_data:
            return None
        
        return PipelineRunSummary(
            id=run_data["id"],
            name=run_data["name"],
            started_at=run_data["started_at"],
            completed_at=run_data["completed_at"],
            status=run_data["status"],
            total_records=run_data["total_records"],
            successful_records=run_data["successful_records"],
            failed_records=run_data["failed_records"],
            skipped_records=run_data["skipped_records"],
            error_message=run_data["error_message"]
        )
    
    async def record_ingestion_stat(self, stat: IngestionStat) -> str:
        stat_id = str(self.next_id)
        self.next_id += 1
        self.data["ingestion_stats"][stat_id] = stat
        return stat_id
    
    async def record_failed_record(self, ingestion_stat_id: str, original_data: str,
                                 failure_reason: str, normalized_data: Optional[str] = None,
                                 stack_trace: Optional[str] = None) -> str:
        record_id = str(self.next_id)
        self.next_id += 1
        self.data["failed_records"][record_id] = {
            "ingestion_stat_id": ingestion_stat_id,
            "original_data": original_data,
            "failure_reason": failure_reason,
            "normalized_data": normalized_data,
            "stack_trace": stack_trace
        }
        return record_id
    
    async def record_quality_metric(self, metric: QualityMetric) -> str:
        metric_id = str(self.next_id)
        self.next_id += 1
        self.data["quality_metrics"][metric_id] = metric
        return metric_id
    
    async def record_audit_event(self, pipeline_run_id: str, event_type: str, 
                               stage_name: str, message: str, event_level: str = "INFO",
                               record_id: Optional[str] = None, 
                               details: Optional[Dict[str, Any]] = None,
                               correlation_id: Optional[str] = None) -> str:
        event_id = str(self.next_id)
        self.next_id += 1
        self.data["audit_events"][event_id] = {
            "pipeline_run_id": pipeline_run_id,
            "event_type": event_type,
            "stage_name": stage_name,
            "message": message,
            "event_level": event_level,
            "record_id": record_id,
            "details": details,
            "correlation_id": correlation_id
        }
        return event_id
    
    async def record_performance_metric(self, pipeline_run_id: str, stage_name: str,
                                      started_at: datetime, completed_at: datetime,
                                      records_processed: int = 0, 
                                      memory_usage_mb: Optional[float] = None,
                                      cpu_usage_percent: Optional[float] = None,
                                      bottleneck_indicator: Optional[str] = None) -> str:
        metric_id = str(self.next_id)
        self.next_id += 1
        self.data["performance_metrics"][metric_id] = {
            "pipeline_run_id": pipeline_run_id,
            "stage_name": stage_name,
            "started_at": started_at,
            "completed_at": completed_at,
            "records_processed": records_processed,
            "memory_usage_mb": memory_usage_mb,
            "cpu_usage_percent": cpu_usage_percent,
            "bottleneck_indicator": bottleneck_indicator
        }
        return metric_id
    
    async def record_system_metric(self, pipeline_run_id: str, hostname: Optional[str] = None,
                                  os_name: Optional[str] = None, os_version: Optional[str] = None,
                                  python_version: Optional[str] = None, 
                                  cpu_model: Optional[str] = None,
                                  cpu_cores: Optional[int] = None, 
                                  memory_total_gb: Optional[float] = None,
                                  gpu_available: bool = False, 
                                  gpu_model: Optional[str] = None,
                                  additional_info: Optional[Dict[str, Any]] = None) -> str:
        metric_id = str(self.next_id)
        self.next_id += 1
        self.data["system_metrics"][metric_id] = {
            "pipeline_run_id": pipeline_run_id,
            "hostname": hostname,
            "os_name": os_name,
            "os_version": os_version,
            "python_version": python_version,
            "cpu_model": cpu_model,
            "cpu_cores": cpu_cores,
            "memory_total_gb": memory_total_gb,
            "gpu_available": gpu_available,
            "gpu_model": gpu_model,
            "additional_info": additional_info
        }
        return metric_id
    
    async def get_ingestion_summary(self, pipeline_run_id: Optional[str] = None,
                                  start_date: Optional[datetime] = None,
                                  end_date: Optional[datetime] = None) -> Dict[str, Any]:
        return {
            "total_records": len(self.data["ingestion_stats"]),
            "successful_records": 0,
            "failed_records": 0,
            "skipped_records": 0,
            "error_breakdown": {},
            "avg_processing_time_ms": 0,
            "total_bytes_processed": 0
        }
    
    async def get_quality_summary(self, pipeline_run_id: Optional[str] = None) -> Dict[str, Any]:
        return {
            "total_records": len(self.data["quality_metrics"]),
            "avg_completeness_score": None,
            "avg_consistency_score": None,
            "avg_validity_score": None,
            "avg_accuracy_score": None,
            "avg_overall_score": None,
            "min_overall_score": None,
            "max_overall_score": None
        }
    
    async def get_recent_pipeline_runs(self, limit: int = 10) -> list:
        runs = []
        for run_data in list(self.data["pipeline_runs"].values())[:limit]:
            runs.append(PipelineRunSummary(
                id=run_data["id"],
                name=run_data["name"],
                started_at=run_data["started_at"],
                completed_at=run_data["completed_at"],
                status=run_data["status"],
                total_records=run_data["total_records"],
                successful_records=run_data["successful_records"],
                failed_records=run_data["failed_records"],
                skipped_records=run_data["skipped_records"],
                error_message=run_data["error_message"]
            ))
        return runs
    
    async def cleanup_old_data(self, days_to_keep: int = 30) -> int:
        return 0


class TestPipelineRunSummary:
    """Test PipelineRunSummary data class."""
    
    def test_pipeline_run_summary_creation(self):
        """Test creating a PipelineRunSummary instance."""
        started_at = datetime.now()
        summary = PipelineRunSummary(
            id="test-run-1",
            name="test-pipeline",
            started_at=started_at,
            completed_at=None,
            status="running",
            total_records=0,
            successful_records=0,
            failed_records=0,
            skipped_records=0
        )
        
        assert summary.id == "test-run-1"
        assert summary.name == "test-pipeline"
        assert summary.started_at == started_at
        assert summary.completed_at is None
        assert summary.status == "running"
        assert summary.total_records == 0
        assert summary.error_message is None
    
    def test_pipeline_run_summary_with_error(self):
        """Test PipelineRunSummary with error message."""
        summary = PipelineRunSummary(
            id="test-run-2",
            name="failed-pipeline",
            started_at=datetime.now(),
            completed_at=datetime.now(),
            status="failed",
            total_records=10,
            successful_records=5,
            failed_records=5,
            skipped_records=0,
            error_message="Database connection failed"
        )
        
        assert summary.status == "failed"
        assert summary.error_message == "Database connection failed"
        assert summary.failed_records == 5


class TestIngestionStat:
    """Test IngestionStat data class."""
    
    def test_ingestion_stat_creation(self):
        """Test creating an IngestionStat instance."""
        timestamp = datetime.now()
        stat = IngestionStat(
            id="stat-1",
            pipeline_run_id="run-1",
            stage_name="ingestion",
            file_path="/data/test.json",
            record_id="record-123",
            record_type="patient",
            status=ProcessingStatus.SUCCESS,
            error_category=None,
            error_message=None,
            error_details=None,
            processing_time_ms=150,
            record_size_bytes=2048,
            data_source="FHIR",
            timestamp=timestamp
        )
        
        assert stat.id == "stat-1"
        assert stat.pipeline_run_id == "run-1"
        assert stat.stage_name == "ingestion"
        assert stat.status == ProcessingStatus.SUCCESS
        assert stat.processing_time_ms == 150
        assert stat.record_size_bytes == 2048
        assert stat.timestamp == timestamp
    
    def test_ingestion_stat_with_error(self):
        """Test IngestionStat with error information."""
        stat = IngestionStat(
            id="stat-2",
            pipeline_run_id="run-1",
            stage_name="validation",
            file_path="/data/invalid.json",
            record_id="record-456",
            record_type="encounter",
            status=ProcessingStatus.FAILURE,
            error_category=ErrorCategory.VALIDATION_ERROR,
            error_message="Missing required field: patient_id",
            error_details={"missing_fields": ["patient_id"]},
            processing_time_ms=50,
            record_size_bytes=1024,
            data_source="HL7",
            timestamp=datetime.now()
        )
        
        assert stat.status == ProcessingStatus.FAILURE
        assert stat.error_category == ErrorCategory.VALIDATION_ERROR
        assert stat.error_message == "Missing required field: patient_id"
        assert stat.error_details == {"missing_fields": ["patient_id"]}


class TestQualityMetric:
    """Test QualityMetric data class."""
    
    def test_quality_metric_creation(self):
        """Test creating a QualityMetric instance."""
        metric = QualityMetric(
            id="metric-1",
            pipeline_run_id="run-1",
            record_id="record-123",
            record_type="patient",
            completeness_score=0.95,
            consistency_score=0.88,
            validity_score=0.92,
            accuracy_score=0.90,
            overall_score=0.91,
            missing_fields=["phone"],
            invalid_fields=[],
            outlier_fields=["age"],
            quality_issues=["Phone number missing"],
            metrics_details={"algorithm": "v2.1"},
            sampled=False
        )
        
        assert metric.id == "metric-1"
        assert metric.pipeline_run_id == "run-1"
        assert metric.completeness_score == 0.95
        assert metric.overall_score == 0.91
        assert metric.missing_fields == ["phone"]
        assert metric.quality_issues == ["Phone number missing"]
        assert not metric.sampled
    
    def test_quality_metric_minimal(self):
        """Test QualityMetric with minimal data."""
        metric = QualityMetric(
            id=None,
            pipeline_run_id="run-2",
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
        
        assert metric.pipeline_run_id == "run-2"
        assert metric.completeness_score is None
        assert metric.missing_fields is None
        assert metric.sampled is False  # Default value


@pytest.mark.asyncio
class TestBaseTrackingRepository:
    """Test BaseTrackingRepository functionality."""
    
    async def test_repository_initialization(self):
        """Test repository initialization with provider."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        
        await repository.initialize()
        
        assert provider.connected
        assert provider.schema_initialized
    
    async def test_health_check(self):
        """Test repository health check."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        
        # Before connection
        assert not await repository.health_check()
        
        # After connection
        await repository.connect()
        assert await repository.health_check()
        
        # After disconnection
        await repository.disconnect()
        assert not await repository.health_check()
    
    async def test_pipeline_run_lifecycle(self):
        """Test complete pipeline run lifecycle."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Start pipeline run
        run_id = "test-run-123"
        config = {"profile": "test", "concurrent": True}
        await repository.start_pipeline_run(run_id, "test-pipeline", config)
        
        # Verify run was created
        run_summary = await repository.get_pipeline_run(run_id)
        assert run_summary is not None
        assert run_summary.id == run_id
        assert run_summary.name == "test-pipeline"
        assert run_summary.status == "running"
        assert run_summary.total_records == 0
        
        # Update counts
        await repository.update_pipeline_run_counts(run_id, 10, 8, 2, 0)
        
        # Verify counts updated
        run_summary = await repository.get_pipeline_run(run_id)
        assert run_summary.total_records == 10
        assert run_summary.successful_records == 8
        assert run_summary.failed_records == 2
        assert run_summary.skipped_records == 0
        
        # Complete pipeline run
        await repository.complete_pipeline_run(run_id, "completed")
        
        # Verify completion
        run_summary = await repository.get_pipeline_run(run_id)
        assert run_summary.status == "completed"
        assert run_summary.completed_at is not None
        assert run_summary.error_message is None
    
    async def test_pipeline_run_with_error(self):
        """Test pipeline run completion with error."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        run_id = "failed-run-456"
        await repository.start_pipeline_run(run_id, "failing-pipeline")
        
        # Complete with error
        error_msg = "Database connection timeout"
        await repository.complete_pipeline_run(run_id, "failed", error_msg)
        
        # Verify error recorded
        run_summary = await repository.get_pipeline_run(run_id)
        assert run_summary.status == "failed"
        assert run_summary.error_message == error_msg
    
    async def test_ingestion_statistics(self):
        """Test recording ingestion statistics."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Create ingestion stat
        stat = IngestionStat(
            id=None,
            pipeline_run_id="run-1",
            stage_name="ingestion",
            file_path="/data/patient.json",
            record_id="patient-123",
            record_type="Patient",
            status=ProcessingStatus.SUCCESS,
            error_category=None,
            error_message=None,
            error_details=None,
            processing_time_ms=200,
            record_size_bytes=4096,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        # Record stat
        stat_id = await repository.record_ingestion_stat(stat)
        assert stat_id is not None
        assert stat_id in provider.data["ingestion_stats"]
    
    async def test_failed_record_tracking(self):
        """Test recording failed records."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Record failed record
        original_data = '{"invalid": "json"'
        failure_reason = "Invalid JSON syntax"
        stack_trace = "ValueError: Invalid JSON at line 1"
        
        record_id = await repository.record_failed_record(
            "stat-123",
            original_data,
            failure_reason,
            None,
            stack_trace
        )
        
        assert record_id is not None
        assert record_id in provider.data["failed_records"]
        
        failed_record = provider.data["failed_records"][record_id]
        assert failed_record["original_data"] == original_data
        assert failed_record["failure_reason"] == failure_reason
        assert failed_record["stack_trace"] == stack_trace
    
    async def test_quality_metrics(self):
        """Test recording quality metrics."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Create quality metric
        metric = QualityMetric(
            id=None,
            pipeline_run_id="run-1",
            record_id="patient-456",
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
            metrics_details={"version": "2.1"},
            sampled=True
        )
        
        # Record metric
        metric_id = await repository.record_quality_metric(metric)
        assert metric_id is not None
        assert metric_id in provider.data["quality_metrics"]
    
    async def test_audit_events(self):
        """Test recording audit events."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Record audit event
        event_id = await repository.record_audit_event(
            pipeline_run_id="run-1",
            event_type="record_processed",
            stage_name="ingestion",
            message="Successfully processed patient record",
            event_level="INFO",
            record_id="patient-789",
            details={"processing_time": 150},
            correlation_id="corr-123"
        )
        
        assert event_id is not None
        assert event_id in provider.data["audit_events"]
        
        event = provider.data["audit_events"][event_id]
        assert event["event_type"] == "record_processed"
        assert event["message"] == "Successfully processed patient record"
        assert event["details"] == {"processing_time": 150}
    
    async def test_performance_metrics(self):
        """Test recording performance metrics."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Record performance metric
        started_at = datetime.now()
        completed_at = datetime.now()
        
        metric_id = await repository.record_performance_metric(
            pipeline_run_id="run-1",
            stage_name="ingestion",
            started_at=started_at,
            completed_at=completed_at,
            records_processed=100,
            memory_usage_mb=256.5,
            cpu_usage_percent=75.2,
            bottleneck_indicator="disk_io"
        )
        
        assert metric_id is not None
        assert metric_id in provider.data["performance_metrics"]
        
        metric = provider.data["performance_metrics"][metric_id]
        assert metric["records_processed"] == 100
        assert metric["memory_usage_mb"] == 256.5
        assert metric["bottleneck_indicator"] == "disk_io"
    
    async def test_system_metrics(self):
        """Test recording system metrics."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Record system metric
        additional_info = {
            "cpu_threads": 8,
            "memory_available_gb": 14.5,
            "disk_total_gb": 500.0,
            "disk_free_gb": 250.0
        }
        
        metric_id = await repository.record_system_metric(
            pipeline_run_id="run-1",
            hostname="test-server",
            os_name="Linux",
            os_version="Ubuntu 22.04",
            python_version="3.11.2",
            cpu_model="Intel Core i7",
            cpu_cores=4,
            memory_total_gb=16.0,
            gpu_available=True,
            gpu_model="NVIDIA RTX 4080",
            additional_info=additional_info
        )
        
        assert metric_id is not None
        assert metric_id in provider.data["system_metrics"]
        
        metric = provider.data["system_metrics"][metric_id]
        assert metric["hostname"] == "test-server"
        assert metric["os_name"] == "Linux"
        assert metric["gpu_available"] is True
        assert metric["additional_info"] == additional_info
    
    async def test_analytics_methods(self):
        """Test analytics and reporting methods."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        # Test ingestion summary
        summary = await repository.get_ingestion_summary("run-1")
        assert "total_records" in summary
        assert "successful_records" in summary
        assert "failed_records" in summary
        
        # Test quality summary
        quality_summary = await repository.get_quality_summary("run-1")
        assert "total_records" in quality_summary
        assert "avg_overall_score" in quality_summary
        
        # Test recent pipeline runs
        recent_runs = await repository.get_recent_pipeline_runs(5)
        assert isinstance(recent_runs, list)
        
        # Test cleanup
        deleted_count = await repository.cleanup_old_data(30)
        assert isinstance(deleted_count, int)
    
    async def test_nonexistent_pipeline_run(self):
        """Test retrieving non-existent pipeline run."""
        provider = MockPersistenceProvider()
        repository = BaseTrackingRepository(provider)
        await repository.initialize()
        
        run_summary = await repository.get_pipeline_run("nonexistent-run")
        assert run_summary is None