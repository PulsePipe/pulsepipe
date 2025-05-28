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

# tests/test_persistence_sqlite.py

"""
Unit tests for SQLite persistence provider.

Tests the SQLite implementation of the persistence provider
with comprehensive coverage of all operations.
"""

from unittest.mock import patch
import pytest
import pytest_asyncio
import tempfile
import os
from datetime import datetime, timedelta
from pathlib import Path

from pulsepipe.persistence.sqlite_provider import SQLitePersistenceProvider
from pulsepipe.persistence.base import (
    PipelineRunSummary,
    IngestionStat,
    QualityMetric
)
from pulsepipe.persistence.models import ProcessingStatus, ErrorCategory
from pulsepipe.utils.path_normalizer import PlatformPath


@pytest_asyncio.fixture
async def temp_db_provider():
    """Create a temporary SQLite database for testing."""
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
        db_path = tmp_file.name
    
    config = {
        "db_path": db_path,
        "timeout": 5.0,
        "enable_wal": False,  # Disable WAL for testing
        "enable_foreign_keys": True,
        "cache_size": -1000  # 1MB cache for testing
    }
    
    provider = SQLitePersistenceProvider(config)
    await provider.connect()
    await provider.initialize_schema()
    
    try:
        yield provider
    finally:
        await provider.disconnect()
        
        # Clean up
        try:
            os.unlink(db_path)
        except OSError:
            pass  # File might already be deleted


@pytest.mark.asyncio
class TestSQLitePersistenceProvider:
    """Test SQLite persistence provider functionality."""
    
    async def test_connection_lifecycle(self):
        """Test connection and disconnection."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
            db_path = tmp_file.name
        
        config = {"db_path": db_path}
        provider = SQLitePersistenceProvider(config)
        
        # Test connection
        await provider.connect()
        assert await provider.health_check()
        
        # Test disconnection
        await provider.disconnect()
        assert not await provider.health_check()
        
        # Clean up
        os.unlink(db_path)
    
    async def test_schema_initialization(self, temp_db_provider):
        """Test database schema initialization."""
        provider = temp_db_provider
        
        # Schema should already be initialized
        assert await provider.health_check()
        
        # Test that we can reinitialize without errors
        await provider.initialize_schema()
        assert await provider.health_check()
    
    async def test_pipeline_run_operations(self, temp_db_provider):
        """Test pipeline run CRUD operations."""
        provider = temp_db_provider
        
        # Start pipeline run
        run_id = "test-run-001"
        run_name = "test-pipeline"
        config_snapshot = {"profile": "test", "concurrent": True}
        
        await provider.start_pipeline_run(run_id, run_name, config_snapshot)
        
        # Retrieve pipeline run
        run_summary = await provider.get_pipeline_run(run_id)
        assert run_summary is not None
        assert run_summary.id == run_id
        assert run_summary.name == run_name
        assert run_summary.status == "running"
        assert run_summary.total_records == 0
        assert run_summary.completed_at is None
        
        # Update counts
        await provider.update_pipeline_run_counts(run_id, 100, 95, 3, 2)
        
        run_summary = await provider.get_pipeline_run(run_id)
        assert run_summary.total_records == 100
        assert run_summary.successful_records == 95
        assert run_summary.failed_records == 3
        assert run_summary.skipped_records == 2
        
        # Complete pipeline run
        await provider.complete_pipeline_run(run_id, "completed")
        
        run_summary = await provider.get_pipeline_run(run_id)
        assert run_summary.status == "completed"
        assert run_summary.completed_at is not None
        assert run_summary.error_message is None
    
    async def test_pipeline_run_with_error(self, temp_db_provider):
        """Test pipeline run completion with error."""
        provider = temp_db_provider
        
        run_id = "failed-run-002"
        await provider.start_pipeline_run(run_id, "failing-pipeline")
        
        error_message = "Database connection timeout"
        await provider.complete_pipeline_run(run_id, "failed", error_message)
        
        run_summary = await provider.get_pipeline_run(run_id)
        assert run_summary.status == "failed"
        assert run_summary.error_message == error_message
    
    async def test_nonexistent_pipeline_run(self, temp_db_provider):
        """Test retrieving non-existent pipeline run."""
        provider = temp_db_provider
        
        run_summary = await provider.get_pipeline_run("nonexistent-run")
        assert run_summary is None
    
    async def test_ingestion_statistics(self, temp_db_provider):
        """Test recording ingestion statistics."""
        provider = temp_db_provider
        
        # First create a pipeline run (required for foreign key)
        pipeline_run_id = "run-003"
        await provider.start_pipeline_run(pipeline_run_id, "test-pipeline")
        
        # Create ingestion statistic
        stat = IngestionStat(
            id=None,
            pipeline_run_id=pipeline_run_id,
            stage_name="ingestion",
            file_path="/data/patients.json",
            record_id="patient-001",
            record_type="Patient",
            status=ProcessingStatus.SUCCESS,
            error_category=None,
            error_message=None,
            error_details=None,
            processing_time_ms=250,
            record_size_bytes=8192,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        # Record statistic
        stat_id = await provider.record_ingestion_stat(stat)
        assert stat_id is not None
        assert stat_id.isdigit()
    
    async def test_ingestion_statistics_with_error(self, temp_db_provider):
        """Test recording ingestion statistics with error details."""
        provider = temp_db_provider
        
        # First create a pipeline run (required for foreign key)
        pipeline_run_id = "run-004"
        await provider.start_pipeline_run(pipeline_run_id, "test-pipeline")
        
        error_details = {
            "validation_errors": ["Missing patient ID", "Invalid date format"],
            "field_errors": {"birthDate": "Invalid format"}
        }
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id=pipeline_run_id,
            stage_name="validation",
            file_path="/data/invalid.json",
            record_id="patient-002",
            record_type="Patient",
            status=ProcessingStatus.FAILURE,
            error_category=ErrorCategory.VALIDATION_ERROR,
            error_message="Patient validation failed",
            error_details=error_details,
            processing_time_ms=50,
            record_size_bytes=1024,
            data_source="HL7",
            timestamp=datetime.now()
        )
        
        stat_id = await provider.record_ingestion_stat(stat)
        assert stat_id is not None
    
    async def test_failed_record_tracking(self, temp_db_provider):
        """Test recording failed records."""
        provider = temp_db_provider
        
        # First create a pipeline run (required for foreign key)
        pipeline_run_id = "run-005"
        await provider.start_pipeline_run(pipeline_run_id, "test-pipeline")
        
        # First record an ingestion stat
        stat = IngestionStat(
            id=None,
            pipeline_run_id=pipeline_run_id,
            stage_name="parsing",
            file_path="/data/corrupt.json",
            record_id="record-003",
            record_type="Encounter",
            status=ProcessingStatus.FAILURE,
            error_category=ErrorCategory.PARSE_ERROR,
            error_message="JSON parsing failed",
            error_details=None,
            processing_time_ms=10,
            record_size_bytes=512,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        ingestion_stat_id = await provider.record_ingestion_stat(stat)
        
        # Record failed record
        original_data = '{"resourceType": "Encounter", "id": "enc-123", invalid}'
        failure_reason = "Invalid JSON syntax at character 56"
        stack_trace = "JSONDecodeError: Expecting ',' delimiter"
        
        failed_record_id = await provider.record_failed_record(
            ingestion_stat_id,
            original_data,
            failure_reason,
            None,
            stack_trace
        )
        
        assert failed_record_id is not None
        assert failed_record_id.isdigit()
    
    async def test_quality_metrics(self, temp_db_provider):
        """Test recording quality metrics."""
        provider = temp_db_provider
        
        # First create a pipeline run (required for foreign key)
        pipeline_run_id = "run-006"
        await provider.start_pipeline_run(pipeline_run_id, "test-pipeline")
        
        metric = QualityMetric(
            id=None,
            pipeline_run_id=pipeline_run_id,
            record_id="patient-004",
            record_type="Patient",
            completeness_score=0.92,
            consistency_score=0.88,
            validity_score=0.95,
            accuracy_score=0.90,
            overall_score=0.91,
            missing_fields=["phoneNumber", "email"],
            invalid_fields=["birthDate"],
            outlier_fields=["height"],
            quality_issues=["Missing contact information", "Invalid birth date format"],
            metrics_details={"algorithm_version": "2.1", "confidence": 0.85},
            sampled=True,
            timestamp=datetime.now()
        )
        
        metric_id = await provider.record_quality_metric(metric)
        assert metric_id is not None
        assert metric_id.isdigit()
    
    async def test_audit_events(self, temp_db_provider):
        """Test recording audit events."""
        provider = temp_db_provider
        
        # First create a pipeline run (required for foreign key)
        pipeline_run_id = "run-007"
        await provider.start_pipeline_run(pipeline_run_id, "test-pipeline")
        
        details = {
            "processing_time_ms": 150,
            "record_size": 2048,
            "validation_rules_applied": ["patient_id_required", "valid_birth_date"]
        }
        
        event_id = await provider.record_audit_event(
            pipeline_run_id=pipeline_run_id,
            event_type="record_validation",
            stage_name="validation",
            message="Patient record validated successfully",
            event_level="INFO",
            record_id="patient-005",
            details=details,
            correlation_id="corr-abc-123"
        )
        
        assert event_id is not None
        assert event_id.isdigit()
    
    async def test_performance_metrics(self, temp_db_provider):
        """Test recording performance metrics."""
        provider = temp_db_provider
        
        # First create a pipeline run (required for foreign key)
        pipeline_run_id = "run-008"
        await provider.start_pipeline_run(pipeline_run_id, "test-pipeline")
        
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=30)
        
        metric_id = await provider.record_performance_metric(
            pipeline_run_id=pipeline_run_id,
            stage_name="embedding",
            started_at=started_at,
            completed_at=completed_at,
            records_processed=500,
            memory_usage_mb=512.75,
            cpu_usage_percent=85.2,
            bottleneck_indicator="cpu_bound"
        )
        
        assert metric_id is not None
        assert metric_id.isdigit()
    
    async def test_system_metrics(self, temp_db_provider):
        """Test recording system metrics."""
        provider = temp_db_provider
        
        # First create a pipeline run (required for foreign key)
        pipeline_run_id = "run-009"
        await provider.start_pipeline_run(pipeline_run_id, "test-pipeline")
        
        additional_info = {
            "cpu_threads": 16,
            "memory_available_gb": 28.5,
            "disk_total_gb": 1000.0,
            "disk_free_gb": 750.0,
            "gpu_memory_gb": 12.0,
            "network_interfaces": ["eth0", "lo"],
            "environment_variables": {"PYTHONPATH": "/app"},
            "package_versions": {"numpy": "1.24.0", "pandas": "2.0.0"}
        }
        
        metric_id = await provider.record_system_metric(
            pipeline_run_id=pipeline_run_id,
            hostname="worker-node-01",
            os_name="Linux",
            os_version="Ubuntu 22.04.3 LTS",
            python_version="3.11.5",
            cpu_model="AMD EPYC 7742",
            cpu_cores=8,
            memory_total_gb=32.0,
            gpu_available=True,
            gpu_model="NVIDIA A100",
            additional_info=additional_info
        )
        
        assert metric_id is not None
        assert metric_id.isdigit()
    
    async def test_ingestion_summary(self, temp_db_provider):
        """Test ingestion summary analytics."""
        provider = temp_db_provider
        
        run_id = "run-010"
        
        # First create a pipeline run (required for foreign key)
        await provider.start_pipeline_run(run_id, "test-pipeline")
        
        # Create multiple ingestion stats with different statuses
        stats = [
            IngestionStat(
                id=None, pipeline_run_id=run_id, stage_name="ingestion",
                file_path="/data/file1.json", record_id="rec-1", record_type="Patient",
                status=ProcessingStatus.SUCCESS, error_category=None, error_message=None,
                error_details=None, processing_time_ms=100, record_size_bytes=1024,
                data_source="FHIR", timestamp=datetime.now()
            ),
            IngestionStat(
                id=None, pipeline_run_id=run_id, stage_name="ingestion",
                file_path="/data/file2.json", record_id="rec-2", record_type="Patient",
                status=ProcessingStatus.SUCCESS, error_category=None, error_message=None,
                error_details=None, processing_time_ms=150, record_size_bytes=2048,
                data_source="FHIR", timestamp=datetime.now()
            ),
            IngestionStat(
                id=None, pipeline_run_id=run_id, stage_name="validation",
                file_path="/data/file3.json", record_id="rec-3", record_type="Patient",
                status=ProcessingStatus.FAILURE, error_category=ErrorCategory.VALIDATION_ERROR,
                error_message="Validation failed", error_details=None,
                processing_time_ms=50, record_size_bytes=512, data_source="FHIR",
                timestamp=datetime.now()
            )
        ]
        
        # Record all stats
        for stat in stats:
            await provider.record_ingestion_stat(stat)
        
        # Get summary
        summary = await provider.get_ingestion_summary(run_id)
        
        assert summary["total_records"] == 3
        assert summary["successful_records"] == 2
        assert summary["failed_records"] == 1
        assert summary["skipped_records"] == 0
        assert "validation_error" in summary["error_breakdown"]
        assert summary["error_breakdown"]["validation_error"] == 1
        assert summary["avg_processing_time_ms"] == 100.0  # (100 + 150 + 50) / 3
        assert summary["total_bytes_processed"] == 3584  # 1024 + 2048 + 512
    
    async def test_quality_summary(self, temp_db_provider):
        """Test quality summary analytics."""
        provider = temp_db_provider
        
        run_id = "run-011"
        
        # First create a pipeline run (required for foreign key)
        await provider.start_pipeline_run(run_id, "test-pipeline")
        
        # Create multiple quality metrics
        metrics = [
            QualityMetric(
                id=None, pipeline_run_id=run_id, record_id="rec-1", record_type="Patient",
                completeness_score=0.95, consistency_score=0.88, validity_score=0.92,
                accuracy_score=0.90, overall_score=0.91, missing_fields=None,
                invalid_fields=None, outlier_fields=None, quality_issues=None,
                metrics_details=None, sampled=False
            ),
            QualityMetric(
                id=None, pipeline_run_id=run_id, record_id="rec-2", record_type="Patient",
                completeness_score=0.88, consistency_score=0.85, validity_score=0.90,
                accuracy_score=0.87, overall_score=0.87, missing_fields=None,
                invalid_fields=None, outlier_fields=None, quality_issues=None,
                metrics_details=None, sampled=False
            )
        ]
        
        # Record all metrics
        for metric in metrics:
            await provider.record_quality_metric(metric)
        
        # Get summary
        summary = await provider.get_quality_summary(run_id)
        
        assert summary["total_records"] == 2
        assert abs(summary["avg_completeness_score"] - 0.915) < 0.001  # (0.95 + 0.88) / 2
        assert abs(summary["avg_overall_score"] - 0.89) < 0.001  # (0.91 + 0.87) / 2
        assert summary["min_overall_score"] == 0.87
        assert summary["max_overall_score"] == 0.91
    
    async def test_recent_pipeline_runs(self, temp_db_provider):
        """Test retrieving recent pipeline runs."""
        provider = temp_db_provider
        
        # Create multiple pipeline runs
        run_ids = ["recent-run-1", "recent-run-2", "recent-run-3"]
        
        for i, run_id in enumerate(run_ids):
            await provider.start_pipeline_run(run_id, f"pipeline-{i}")
            await provider.complete_pipeline_run(run_id, "completed")
        
        # Get recent runs
        recent_runs = await provider.get_recent_pipeline_runs(2)
        
        assert len(recent_runs) == 2
        assert all(isinstance(run, PipelineRunSummary) for run in recent_runs)
        
        # Check that runs are ordered by started_at DESC
        assert recent_runs[0].id == "recent-run-3"  # Most recent
        assert recent_runs[1].id == "recent-run-2"
    
    async def test_cleanup_old_data(self, temp_db_provider):
        """Test cleanup of old tracking data."""
        provider = temp_db_provider
        
        # Create old pipeline run (simulated)
        old_run_id = "old-run-cleanup"
        await provider.start_pipeline_run(old_run_id, "old-pipeline")
        
        # Manually update the started_at to be old
        # Note: In real implementation, this would use actual old data
        await provider.connection.execute(
            "UPDATE pipeline_runs SET started_at = ? WHERE id = ?",
            ((datetime.now() - timedelta(days=40)).isoformat(), old_run_id)
        )
        await provider.connection.commit()
        
        # Run cleanup (keep 30 days)
        deleted_count = await provider.cleanup_old_data(30)
        
        assert deleted_count >= 1  # At least our old run should be deleted
        
        # Verify old run is gone
        run_summary = await provider.get_pipeline_run(old_run_id)
        assert run_summary is None
    
    async def test_database_path_creation(self):
        """Test that database parent directories are created."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "nested", "subdir", "test.db")
            config = {"db_path": db_path}
            
            provider = SQLitePersistenceProvider(config)
            await provider.connect()
            
            # Verify database file was created
            assert os.path.exists(db_path)
            assert os.path.isfile(db_path)
            
            await provider.disconnect()
    
    async def test_connection_configuration(self):
        """Test various connection configurations."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
            db_path = tmp_file.name
        
        config = {
            "db_path": db_path,
            "timeout": 10.0,
            "enable_wal": True,
            "enable_foreign_keys": False,
            "cache_size": -2000
        }
        
        provider = SQLitePersistenceProvider(config)
        await provider.connect()
        
        # Test that connection works with custom config
        assert await provider.health_check()
        
        await provider.disconnect()
        os.unlink(db_path)
    
    async def test_concurrent_operations(self, temp_db_provider):
        """Test concurrent database operations."""
        provider = temp_db_provider
        
        # This test ensures that our async implementation handles
        # concurrent operations properly
        import asyncio
        
        async def create_run(run_id: str):
            await provider.start_pipeline_run(run_id, f"concurrent-pipeline-{run_id}")
            await provider.complete_pipeline_run(run_id, "completed")
            return await provider.get_pipeline_run(run_id)
        
        # Run multiple operations concurrently
        tasks = [create_run(f"concurrent-{i}") for i in range(5)]
        results = await asyncio.gather(*tasks)
        
        # Verify all operations completed successfully
        assert len(results) == 5
        assert all(result is not None for result in results)
        assert all(result.status == "completed" for result in results)
    
    @pytest.mark.asyncio
    async def test_error_handling(self):
        """Simulate permission error during SQLite connect (cross-platform safe)."""
        config = {"db_path": "C:/fake/readonly/test.db"}
        provider = SQLitePersistenceProvider(config)

        with patch("sqlite3.connect", side_effect=PermissionError("Access denied")):
            with pytest.raises(PermissionError):
                await provider.connect()
    
    async def test_health_check_without_connection(self):
        """Test health check on unconnected provider."""
        config = {"db_path": ":memory:"}
        provider = SQLitePersistenceProvider(config)
        
        # Health check should fail before connection
        assert not await provider.health_check()
    
    async def test_transactions_and_rollback(self, temp_db_provider):
        """Test that operations are properly committed."""
        provider = temp_db_provider
        
        # Start a pipeline run
        run_id = "transaction-test"
        await provider.start_pipeline_run(run_id, "transaction-pipeline")
        
        # Verify it's immediately visible (auto-commit)
        run_summary = await provider.get_pipeline_run(run_id)
        assert run_summary is not None
        assert run_summary.id == run_id