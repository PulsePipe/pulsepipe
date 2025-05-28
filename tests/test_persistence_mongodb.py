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

# tests/test_persistence_mongodb.py

"""
Unit tests for MongoDB persistence provider.

Tests the MongoDB implementation with mocked connections since
we don't require a running MongoDB instance for unit tests.
"""

import pytest
import pytest_asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch
from bson import ObjectId

from pulsepipe.persistence.mongodb_provider import MongoDBPersistenceProvider
from pulsepipe.persistence.base import (
    PipelineRunSummary,
    IngestionStat,
    QualityMetric
)
from pulsepipe.persistence.models import ProcessingStatus, ErrorCategory


class TestMongoDBPersistenceProvider:
    """Test MongoDB persistence provider functionality."""
    
    def test_initialization(self):
        """Test MongoDB provider initialization."""
        config = {
            "host": "localhost",
            "port": 27017,
            "database": "test_db",
            "username": "test_user",
            "password": "test_pass",
            "tls": True,
            "replica_set": "rs0"
        }
        
        provider = MongoDBPersistenceProvider(config)
        
        assert provider.host == "localhost"
        assert provider.port == 27017
        assert provider.database_name == "test_db"
        assert provider.username == "test_user"
        assert provider.password == "test_pass"
        assert provider.tls_enabled is True
        assert provider.replica_set == "rs0"
    
    def test_initialization_defaults(self):
        """Test MongoDB provider initialization with defaults."""
        config = {}
        
        provider = MongoDBPersistenceProvider(config)
        
        assert provider.host == "localhost"
        assert provider.port == 27017
        assert provider.database_name == "pulsepipe_intelligence"
        assert provider.username is None
        assert provider.password is None
        assert provider.tls_enabled is True
    
    @pytest.mark.asyncio
    @patch('pulsepipe.persistence.mongodb_provider.MongoClient')
    @pytest.mark.asyncio
    async def test_connection_without_auth(self, mock_client_class):
        """Test connection without authentication."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = True
        mock_client_class.return_value = mock_client
        
        config = {
            "host": "localhost",
            "port": 27017,
            "database": "test_db",
            "tls": False
        }
        
        provider = MongoDBPersistenceProvider(config)
        await provider.connect()
        
        # Verify client was created with correct URI
        mock_client_class.assert_called_once()
        call_args = mock_client_class.call_args
        assert "mongodb://localhost:27017/test_db" in call_args[0][0]
        
        # Verify connection test was performed
        mock_client.admin.command.assert_called_once_with('ismaster')
    
    @patch('pulsepipe.persistence.mongodb_provider.MongoClient')
    @pytest.mark.asyncio
    async def test_connection_with_auth(self, mock_client_class):
        """Test connection with authentication."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = True
        mock_client_class.return_value = mock_client
        
        config = {
            "host": "localhost",
            "port": 27017,
            "database": "test_db",
            "username": "testuser",
            "password": "testpass",
            "auth_source": "admin"
        }
        
        provider = MongoDBPersistenceProvider(config)
        await provider.connect()
        
        # Verify client was created with auth URI
        mock_client_class.assert_called_once()
        call_args = mock_client_class.call_args
        assert "testuser:testpass@localhost:27017" in call_args[0][0]
        assert "authSource=admin" in call_args[0][0]
    
    @patch('pulsepipe.persistence.mongodb_provider.MongoClient')
    @pytest.mark.asyncio
    async def test_connection_with_tls(self, mock_client_class):
        """Test connection with TLS configuration."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = True
        mock_client_class.return_value = mock_client
        
        config = {
            "host": "secure.mongodb.com",
            "database": "secure_db",
            "tls": True,
            "tls_ca_file": "/path/to/ca.pem",
            "tls_cert_file": "/path/to/client.pem"
        }
        
        provider = MongoDBPersistenceProvider(config)
        await provider.connect()
        
        # Verify TLS options were set
        call_kwargs = mock_client_class.call_args[1]
        assert call_kwargs["tls"] is True
        assert call_kwargs["tlsCAFile"] == "/path/to/ca.pem"
        assert call_kwargs["tlsCertificateKeyFile"] == "/path/to/client.pem"
    
    @patch('pulsepipe.persistence.mongodb_provider.MongoClient')
    @pytest.mark.asyncio
    async def test_connection_failure(self, mock_client_class):
        """Test connection failure handling."""
        mock_client = MagicMock()
        mock_client.admin.command.side_effect = Exception("Connection failed")
        mock_client_class.return_value = mock_client
        
        config = {"host": "nonexistent.host", "database": "test_db"}
        provider = MongoDBPersistenceProvider(config)
        
        with pytest.raises(Exception):  # Allow any exception
            await provider.connect()
    
    @patch('pulsepipe.persistence.mongodb_provider.MongoClient')
    @pytest.mark.asyncio
    async def test_disconnect(self, mock_client_class):
        """Test disconnection."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = True
        mock_client_class.return_value = mock_client
        
        config = {"host": "localhost", "database": "test_db"}
        provider = MongoDBPersistenceProvider(config)
        
        await provider.connect()
        await provider.disconnect()
        
        mock_client.close.assert_called_once()
        assert provider.client is None
    
    @patch('pulsepipe.persistence.mongodb_provider.MongoClient')
    @pytest.mark.asyncio
    async def test_health_check(self, mock_client_class):
        """Test health check functionality."""
        mock_client = MagicMock()
        mock_client.admin.command.return_value = True
        mock_client_class.return_value = mock_client
        
        config = {"host": "localhost", "database": "test_db"}
        provider = MongoDBPersistenceProvider(config)
        
        # Health check without connection
        assert not await provider.health_check()
        
        # Health check with connection
        await provider.connect()
        assert await provider.health_check()
        
        # Health check with connection failure
        mock_client.admin.command.side_effect = Exception("Ping failed")
        assert not await provider.health_check()
    
    @pytest_asyncio.fixture
    async def mock_provider(self):
        """Create a mocked MongoDB provider for testing."""
        with patch('pulsepipe.persistence.mongodb_provider.MongoClient') as mock_client_class:
            mock_client = MagicMock()
            mock_client.admin.command.return_value = True
            mock_client_class.return_value = mock_client
            
            # Mock database and collections
            mock_database = MagicMock()
            mock_client.__getitem__.return_value = mock_database
            
            mock_collections = {}
            for collection_name in ["pipeline_runs", "ingestion_stats", "failed_records",
                                  "audit_events", "quality_metrics", "performance_metrics",
                                  "system_metrics"]:
                mock_collection = MagicMock()
                mock_collections[collection_name] = mock_collection
                mock_database.__getitem__.return_value = mock_collection
            
            config = {"host": "localhost", "database": "test_db"}
            provider = MongoDBPersistenceProvider(config)
            await provider.connect()
            await provider.initialize_schema()
            
            # Store mock objects for test access
            provider._mock_client = mock_client
            provider._mock_database = mock_database
            provider._mock_collections = mock_collections
            
            yield provider
            
            await provider.disconnect()
    
    @pytest.mark.asyncio
    async def test_initialize_schema(self, mock_provider):
        """Test schema initialization."""
        provider = mock_provider
        
        # Schema initialization should create indexes
        # We can't easily test the exact create_index calls due to mocking complexity,
        # but we can verify the method completes without error
        await provider.initialize_schema()
        assert provider.database is not None
    
    @pytest.mark.asyncio
    async def test_pipeline_run_operations(self, mock_provider):
        """Test pipeline run CRUD operations."""
        provider = mock_provider
        
        # Mock the collection for pipeline runs
        mock_collection = MagicMock()
        provider.collections["pipeline_runs"] = mock_collection
        
        # Test start pipeline run
        run_id = "test-run-001"
        run_name = "test-pipeline"
        config_snapshot = {"profile": "test"}
        
        await provider.start_pipeline_run(run_id, run_name, config_snapshot)
        
        # Verify insert was called
        mock_collection.insert_one.assert_called_once()
        insert_call = mock_collection.insert_one.call_args[0][0]
        assert insert_call["_id"] == run_id
        assert insert_call["name"] == run_name
        assert insert_call["config_snapshot"] == config_snapshot
        assert insert_call["status"] == "running"
        
        # Test complete pipeline run
        mock_collection.reset_mock()
        await provider.complete_pipeline_run(run_id, "completed")
        
        mock_collection.update_one.assert_called_once()
        update_call = mock_collection.update_one.call_args
        assert update_call[0][0] == {"_id": run_id}
        assert update_call[0][1]["$set"]["status"] == "completed"
        
        # Test update counts
        mock_collection.reset_mock()
        await provider.update_pipeline_run_counts(run_id, 10, 8, 2, 0)
        
        mock_collection.update_one.assert_called_once()
        update_call = mock_collection.update_one.call_args
        assert update_call[0][1]["$inc"]["total_records"] == 10
        assert update_call[0][1]["$inc"]["successful_records"] == 8
        
        # Test get pipeline run
        mock_collection.reset_mock()
        mock_document = {
            "_id": run_id,
            "name": run_name,
            "started_at": datetime.now(),
            "completed_at": datetime.now(),
            "status": "completed",
            "total_records": 10,
            "successful_records": 8,
            "failed_records": 2,
            "skipped_records": 0,
            "error_message": None
        }
        mock_collection.find_one.return_value = mock_document
        
        result = await provider.get_pipeline_run(run_id)
        
        assert isinstance(result, PipelineRunSummary)
        assert result.id == run_id
        assert result.name == run_name
        assert result.status == "completed"
        assert result.total_records == 10
        
        # Test non-existent pipeline run
        mock_collection.find_one.return_value = None
        result = await provider.get_pipeline_run("nonexistent")
        assert result is None
    
    @pytest.mark.asyncio
    async def test_ingestion_statistics(self, mock_provider):
        """Test recording ingestion statistics."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439011")
        mock_collection.insert_one.return_value = mock_result
        provider.collections["ingestion_stats"] = mock_collection
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id="run-001",
            stage_name="ingestion",
            file_path="/data/test.json",
            record_id="record-123",
            record_type="Patient",
            status=ProcessingStatus.SUCCESS,
            error_category=None,
            error_message=None,
            error_details=None,
            processing_time_ms=150,
            record_size_bytes=2048,
            data_source="FHIR",
            timestamp=datetime.now()
        )
        
        result_id = await provider.record_ingestion_stat(stat)
        
        assert result_id == "507f1f77bcf86cd799439011"
        mock_collection.insert_one.assert_called_once()
        
        insert_call = mock_collection.insert_one.call_args[0][0]
        assert insert_call["pipeline_run_id"] == "run-001"
        assert insert_call["stage_name"] == "ingestion"
        assert insert_call["status"] == ProcessingStatus.SUCCESS.value
    
    @pytest.mark.asyncio
    async def test_failed_record_tracking(self, mock_provider):
        """Test recording failed records."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439012")
        mock_collection.insert_one.return_value = mock_result
        provider.collections["failed_records"] = mock_collection
        
        result_id = await provider.record_failed_record(
            "507f1f77bcf86cd799439011",
            '{"invalid": "json"',
            "Invalid JSON syntax",
            None,
            "JSONDecodeError: Invalid syntax"
        )
        
        assert result_id == "507f1f77bcf86cd799439012"
        mock_collection.insert_one.assert_called_once()
        
        insert_call = mock_collection.insert_one.call_args[0][0]
        assert insert_call["original_data"] == '{"invalid": "json"'
        assert insert_call["failure_reason"] == "Invalid JSON syntax"
    
    @pytest.mark.asyncio
    async def test_quality_metrics(self, mock_provider):
        """Test recording quality metrics."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439013")
        mock_collection.insert_one.return_value = mock_result
        provider.collections["quality_metrics"] = mock_collection
        
        metric = QualityMetric(
            id=None,
            pipeline_run_id="run-001",
            record_id="patient-123",
            record_type="Patient",
            completeness_score=0.95,
            consistency_score=0.88,
            validity_score=0.92,
            accuracy_score=0.90,
            overall_score=0.91,
            missing_fields=["phone"],
            invalid_fields=[],
            outlier_fields=["age"],
            quality_issues=["Missing phone"],
            metrics_details={"version": "2.1"},
            sampled=True
        )
        
        result_id = await provider.record_quality_metric(metric)
        
        assert result_id == "507f1f77bcf86cd799439013"
        mock_collection.insert_one.assert_called_once()
        
        insert_call = mock_collection.insert_one.call_args[0][0]
        assert insert_call["completeness_score"] == 0.95
        assert insert_call["missing_fields"] == ["phone"]
        assert insert_call["sampled"] is True
    
    @pytest.mark.asyncio
    async def test_audit_events(self, mock_provider):
        """Test recording audit events."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439014")
        mock_collection.insert_one.return_value = mock_result
        provider.collections["audit_events"] = mock_collection
        
        result_id = await provider.record_audit_event(
            pipeline_run_id="run-001",
            event_type="record_processed",
            stage_name="ingestion",
            message="Record processed successfully",
            event_level="INFO",
            record_id="patient-123",
            details={"processing_time": 150},
            correlation_id="corr-123"
        )
        
        assert result_id == "507f1f77bcf86cd799439014"
        mock_collection.insert_one.assert_called_once()
        
        insert_call = mock_collection.insert_one.call_args[0][0]
        assert insert_call["event_type"] == "record_processed"
        assert insert_call["message"] == "Record processed successfully"
        assert insert_call["details"] == {"processing_time": 150}
    
    @pytest.mark.asyncio
    async def test_performance_metrics(self, mock_provider):
        """Test recording performance metrics."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439015")
        mock_collection.insert_one.return_value = mock_result
        provider.collections["performance_metrics"] = mock_collection
        
        started_at = datetime.now()
        completed_at = started_at + timedelta(seconds=30)
        
        result_id = await provider.record_performance_metric(
            pipeline_run_id="run-001",
            stage_name="embedding",
            started_at=started_at,
            completed_at=completed_at,
            records_processed=100,
            memory_usage_mb=256.5,
            cpu_usage_percent=75.2,
            bottleneck_indicator="cpu_bound"
        )
        
        assert result_id == "507f1f77bcf86cd799439015"
        mock_collection.insert_one.assert_called_once()
        
        insert_call = mock_collection.insert_one.call_args[0][0]
        assert insert_call["records_processed"] == 100
        assert insert_call["memory_usage_mb"] == 256.5
        assert insert_call["bottleneck_indicator"] == "cpu_bound"
        assert insert_call["duration_ms"] == 30000  # 30 seconds
    
    @pytest.mark.asyncio
    async def test_system_metrics(self, mock_provider):
        """Test recording system metrics."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        mock_result = MagicMock()
        mock_result.inserted_id = ObjectId("507f1f77bcf86cd799439016")
        mock_collection.insert_one.return_value = mock_result
        provider.collections["system_metrics"] = mock_collection
        
        additional_info = {
            "cpu_threads": 8,
            "memory_available_gb": 14.5,
            "disk_total_gb": 500.0
        }
        
        result_id = await provider.record_system_metric(
            pipeline_run_id="run-001",
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
        
        assert result_id == "507f1f77bcf86cd799439016"
        mock_collection.insert_one.assert_called_once()
        
        insert_call = mock_collection.insert_one.call_args[0][0]
        assert insert_call["hostname"] == "test-server"
        assert insert_call["os_name"] == "Linux"
        assert insert_call["gpu_available"] is True
        assert insert_call["cpu_threads"] == 8
    
    @pytest.mark.asyncio
    async def test_analytics_ingestion_summary(self, mock_provider):
        """Test ingestion summary analytics."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        provider.collections["ingestion_stats"] = mock_collection
        
        # Mock aggregation results
        mock_results = [
            {
                "_id": {"status": "success", "error_category": None},
                "count": 10,
                "avg_processing_time": 150.0,
                "total_bytes": 20480
            },
            {
                "_id": {"status": "failure", "error_category": "validation_error"},
                "count": 2,
                "avg_processing_time": 50.0,
                "total_bytes": 1024
            }
        ]
        mock_collection.aggregate.return_value = mock_results
        
        summary = await provider.get_ingestion_summary("run-001")
        
        assert summary["total_records"] == 12
        assert summary["successful_records"] == 10
        assert summary["failed_records"] == 2
        assert summary["error_breakdown"]["validation_error"] == 2
        # Calculate expected average: (150*10 + 50*2) / 12 = 133.33...
        expected_avg = (150.0*10 + 50.0*2) / 12
        assert abs(summary["avg_processing_time_ms"] - expected_avg) < 0.01
        assert summary["total_bytes_processed"] == 21504
    
    @pytest.mark.asyncio
    async def test_analytics_quality_summary(self, mock_provider):
        """Test quality summary analytics."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        provider.collections["quality_metrics"] = mock_collection
        
        # Mock aggregation results
        mock_results = [
            {
                "_id": None,
                "total_records": 5,
                "avg_completeness": 0.92,
                "avg_consistency": 0.88,
                "avg_validity": 0.94,
                "avg_accuracy": 0.90,
                "avg_overall_score": 0.91,
                "min_score": 0.85,
                "max_score": 0.95
            }
        ]
        mock_collection.aggregate.return_value = mock_results
        
        summary = await provider.get_quality_summary("run-001")
        
        assert summary["total_records"] == 5
        assert summary["avg_completeness_score"] == 0.92
        assert summary["avg_overall_score"] == 0.91
        assert summary["min_overall_score"] == 0.85
        assert summary["max_overall_score"] == 0.95
    
    @pytest.mark.asyncio
    async def test_recent_pipeline_runs(self, mock_provider):
        """Test retrieving recent pipeline runs."""
        provider = mock_provider
        
        mock_collection = MagicMock()
        provider.collections["pipeline_runs"] = mock_collection
        
        # Mock cursor and results
        mock_documents = [
            {
                "_id": "run-001",
                "name": "pipeline-1",
                "started_at": datetime.now(),
                "completed_at": datetime.now(),
                "status": "completed",
                "total_records": 100,
                "successful_records": 95,
                "failed_records": 5,
                "skipped_records": 0,
                "error_message": None
            },
            {
                "_id": "run-002",
                "name": "pipeline-2",
                "started_at": datetime.now(),
                "completed_at": None,
                "status": "running",
                "total_records": 50,
                "successful_records": 45,
                "failed_records": 0,
                "skipped_records": 5,
                "error_message": None
            }
        ]
        
        mock_cursor = MagicMock()
        mock_cursor.sort.return_value.limit.return_value = mock_documents
        mock_collection.find.return_value = mock_cursor
        
        recent_runs = await provider.get_recent_pipeline_runs(2)
        
        assert len(recent_runs) == 2
        assert all(isinstance(run, PipelineRunSummary) for run in recent_runs)
        assert recent_runs[0].id == "run-001"
        assert recent_runs[1].id == "run-002"
        
        # Verify sort and limit were called
        mock_cursor.sort.assert_called_once_with("started_at", -1)
        mock_cursor.sort.return_value.limit.assert_called_once_with(2)
    
    @pytest.mark.asyncio
    async def test_cleanup_old_data(self, mock_provider):
        """Test cleanup of old tracking data."""
        provider = mock_provider
        
        # Mock pipeline runs collection
        mock_runs_collection = MagicMock()
        provider.collections["pipeline_runs"] = mock_runs_collection
        
        # Mock old runs
        mock_old_runs = [{"_id": "old-run-1"}, {"_id": "old-run-2"}]
        mock_runs_cursor = MagicMock()
        mock_runs_cursor.__iter__ = lambda x: iter(mock_old_runs)
        mock_runs_collection.find.return_value = mock_runs_cursor
        
        # Mock other collections
        for collection_name in ["ingestion_stats", "quality_metrics", "audit_events"]:
            mock_collection = MagicMock()
            mock_result = MagicMock()
            mock_result.deleted_count = 5
            mock_collection.delete_many.return_value = mock_result
            provider.collections[collection_name] = mock_collection
        
        # Mock failed_records with special handling
        mock_failed_collection = MagicMock()
        mock_failed_result = MagicMock()
        mock_failed_result.deleted_count = 3
        mock_failed_collection.delete_many.return_value = mock_failed_result
        provider.collections["failed_records"] = mock_failed_collection
        
        # Mock ingestion stats for failed records lookup
        mock_ingestion_collection = MagicMock()
        mock_ingestion_cursor = MagicMock()
        mock_ingestion_cursor.__iter__ = lambda x: iter([{"_id": "stat-1"}, {"_id": "stat-2"}])
        mock_ingestion_collection.find.return_value = mock_ingestion_cursor
        provider.collections["ingestion_stats"] = mock_ingestion_collection
        
        # Mock final pipeline runs deletion
        mock_runs_result = MagicMock()
        mock_runs_result.deleted_count = 2
        mock_runs_collection.delete_many.return_value = mock_runs_result
        
        deleted_count = await provider.cleanup_old_data(30)
        
        # Mock the return value properly
        # Verify deletions occurred (just check it's returned)
        assert deleted_count is not None
        mock_runs_collection.find.assert_called()
        mock_failed_collection.delete_many.assert_called()
        mock_runs_collection.delete_many.assert_called()
    
    @pytest.mark.asyncio
    async def test_collection_names_configuration(self, mock_provider):
        """Test that collection names are properly configured."""
        provider = mock_provider
        
        expected_collections = [
            "pipeline_runs",
            "ingestion_stats", 
            "failed_records",
            "audit_events",
            "quality_metrics",
            "performance_metrics",
            "system_metrics"
        ]
        
        for collection_name in expected_collections:
            assert collection_name in provider.collection_names
        
        # Verify all expected collections are initialized
        assert len(provider.collections) == len(expected_collections)