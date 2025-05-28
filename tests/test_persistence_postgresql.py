"""Unit tests for PostgreSQL persistence provider."""

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone
from uuid import UUID

from pulsepipe.persistence.postgresql_provider import PostgreSQLPersistenceProvider
from pulsepipe.persistence.base import PipelineRunSummary, IngestionStat, QualityMetric
from pulsepipe.persistence.models import ProcessingStatus, ErrorCategory


@pytest_asyncio.fixture
async def mock_pool():
    """Create a mock PostgreSQL connection pool."""
    pool = AsyncMock()
    connection = AsyncMock()
    
    # Set up pool.acquire to return an async context manager
    acquire_cm = AsyncMock()
    acquire_cm.__aenter__ = AsyncMock(return_value=connection)
    acquire_cm.__aexit__ = AsyncMock(return_value=None)
    pool.acquire = MagicMock(return_value=acquire_cm)
    
    return pool, connection


@pytest_asyncio.fixture
async def postgresql_provider(mock_pool):
    """Create a PostgreSQL provider with mocked dependencies."""
    pool, connection = mock_pool
    
    config = {
        "host": "localhost",
        "port": 5432,
        "database": "test_db",
        "username": "test_user",
        "password": "test_pass",
        "use_pool": True
    }
    
    with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
        mock_asyncpg.create_pool = AsyncMock(return_value=pool)
        provider = PostgreSQLPersistenceProvider(config)
        await provider.connect()
        yield provider, connection
        await provider.disconnect()


class TestPostgreSQLProvider:
    """Test PostgreSQL persistence provider functionality."""

    @pytest.mark.asyncio
    async def test_init_with_defaults(self):
        """Test provider initialization with default values."""
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass"
        }
        provider = PostgreSQLPersistenceProvider(config)
        
        assert provider.host == "localhost"
        assert provider.port == 5432
        assert provider.database == "test_db"
        assert provider.username == "user"
        assert provider.password == "pass"
        assert provider.ssl_mode == "prefer"
        assert provider.use_pool is True
        assert provider.pool_min_size == 5
        assert provider.pool_max_size == 20
        assert provider.connection_timeout == 10.0

    @pytest.mark.asyncio
    async def test_init_with_custom_config(self):
        """Test provider initialization with custom configuration."""
        config = {
            "host": "custom-host",
            "port": 5433,
            "database": "custom_db",
            "username": "custom_user",
            "password": "custom_pass",
            "ssl_mode": "disable",
            "use_pool": False,
            "pool_min_size": 2,
            "pool_max_size": 20,
            "connection_timeout": 60.0
        }
        provider = PostgreSQLPersistenceProvider(config)
        
        assert provider.host == "custom-host"
        assert provider.port == 5433
        assert provider.database == "custom_db"
        assert provider.username == "custom_user"
        assert provider.password == "custom_pass"
        assert provider.ssl_mode == "disable"
        assert provider.use_pool is False
        assert provider.pool_min_size == 2
        assert provider.pool_max_size == 20
        assert provider.connection_timeout == 60.0

    @pytest.mark.asyncio
    async def test_connect_with_pool(self, mock_pool):
        """Test connecting with connection pool."""
        pool, _ = mock_pool
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "use_pool": True
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            provider = PostgreSQLPersistenceProvider(config)
            
            await provider.connect()
            
            assert provider.pool == pool
            mock_asyncpg.create_pool.assert_called_once()

    @pytest.mark.asyncio
    async def test_connect_without_pool(self):
        """Test connecting without connection pool."""
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "use_pool": False
        }
        
        mock_connection = AsyncMock()
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
            mock_asyncpg.connect = AsyncMock(return_value=mock_connection)
            provider = PostgreSQLPersistenceProvider(config)
            
            await provider.connect()
            
            assert provider.pool is None
            assert provider.connection == mock_connection

    @pytest.mark.asyncio
    async def test_disconnect_with_pool(self, postgresql_provider):
        """Test disconnecting with pool cleanup."""
        provider, _ = postgresql_provider
        
        # Store reference to pool before disconnect
        pool = provider.pool
        
        await provider.disconnect()
        
        pool.close.assert_called_once()
        pool.wait_closed.assert_called_once()
        assert provider.pool is None

    @pytest.mark.asyncio
    async def test_disconnect_without_pool(self):
        """Test disconnecting without pool."""
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "use_pool": False
        }
        
        mock_connection = AsyncMock()
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
            mock_asyncpg.connect = AsyncMock(return_value=mock_connection)
            provider = PostgreSQLPersistenceProvider(config)
            
            await provider.connect()
            await provider.disconnect()
            
            mock_connection.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_pipeline_run(self, postgresql_provider):
        """Test starting a pipeline run."""
        provider, connection = postgresql_provider
        run_id = "test-run-123"
        pipeline_name = "test-pipeline"
        
        connection.execute = AsyncMock()
        
        await provider.start_pipeline_run(run_id, pipeline_name)
        
        connection.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_complete_pipeline_run(self, postgresql_provider):
        """Test completing a pipeline run."""
        provider, connection = postgresql_provider
        run_id = "test-run-123"
        
        connection.execute = AsyncMock()
        
        await provider.complete_pipeline_run(run_id, "completed")
        
        connection.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_ingestion_stat(self, postgresql_provider):
        """Test recording an ingestion statistic."""
        provider, connection = postgresql_provider
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id="test-run-123",
            stage_name="ingestion",
            file_path="test.json",
            record_id="record-1",
            record_type="fhir",
            status=ProcessingStatus.SUCCESS,
            error_category=None,
            error_message=None,
            error_details=None,
            processing_time_ms=100,
            record_size_bytes=1024,
            data_source="test",
            timestamp=datetime.now(timezone.utc)
        )
        
        mock_row = {'id': 1}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.record_ingestion_stat(stat)
        
        assert result == "1"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_quality_metric(self, postgresql_provider):
        """Test recording a quality metric."""
        provider, connection = postgresql_provider
        
        metric = QualityMetric(
            id=None,
            pipeline_run_id="test-run-123",
            record_id="record-1",
            record_type="fhir",
            completeness_score=0.95,
            consistency_score=0.90,
            validity_score=0.88,
            accuracy_score=0.92,
            overall_score=0.91,
            missing_fields=["field1"],
            invalid_fields=["field2"],
            outlier_fields=[],
            quality_issues=["Missing required field"],
            metrics_details={"test": "data"}
        )
        
        mock_row = {'id': 1}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.record_quality_metric(metric)
        
        assert result == "1"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_pipeline_run(self, postgresql_provider):
        """Test retrieving a pipeline run."""
        provider, connection = postgresql_provider
        
        mock_row = {
            'id': 'test-run-123',
            'name': 'test-pipeline',
            'started_at': datetime.now(timezone.utc),
            'completed_at': datetime.now(timezone.utc),
            'status': 'completed',
            'total_records': 100,
            'successful_records': 95,
            'failed_records': 5,
            'skipped_records': 0,
            'error_message': None
        }
        
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.get_pipeline_run("test-run-123")
        
        assert result is not None
        assert isinstance(result, PipelineRunSummary)
        assert result.id == 'test-run-123'
        assert result.name == 'test-pipeline'
        assert result.status == 'completed'
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_recent_pipeline_runs(self, postgresql_provider):
        """Test retrieving recent pipeline runs."""
        provider, connection = postgresql_provider
        
        mock_rows = [
            {
                'id': 'run-1',
                'name': 'test-pipeline',
                'started_at': datetime.now(timezone.utc),
                'completed_at': datetime.now(timezone.utc),
                'status': 'completed',
                'total_records': 100,
                'successful_records': 95,
                'failed_records': 5,
                'skipped_records': 0,
                'error_message': None
            }
        ]
        
        connection.fetch = AsyncMock(return_value=mock_rows)
        
        result = await provider.get_recent_pipeline_runs(limit=10)
        
        assert len(result) == 1
        assert isinstance(result[0], PipelineRunSummary)
        assert result[0].id == 'run-1'
        connection.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_health_check(self, postgresql_provider):
        """Test database health check."""
        provider, connection = postgresql_provider
        
        connection.execute = AsyncMock(return_value=None)
        
        result = await provider.health_check()
        
        assert result is True
        connection.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_connection_error_handling(self):
        """Test handling of connection errors."""
        config = {
            "host": "invalid-host",
            "database": "test_db",
            "username": "user",
            "password": "pass"
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
            mock_asyncpg.create_pool = AsyncMock(side_effect=Exception("Connection failed"))
            provider = PostgreSQLPersistenceProvider(config)
            
            with pytest.raises(Exception, match="Connection failed"):
                await provider.connect()

    @pytest.mark.asyncio
    async def test_missing_required_config(self):
        """Test configuration with missing password."""
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user"
        }
        
        provider = PostgreSQLPersistenceProvider(config)
        assert provider.password is None

    @pytest.mark.asyncio
    async def test_context_manager_usage(self, mock_pool):
        """Test using provider as async context manager."""
        pool, connection = mock_pool
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass"
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            
            async with PostgreSQLPersistenceProvider(config) as provider:
                assert provider.pool == pool
            
            pool.close.assert_called_once()
            pool.wait_closed.assert_called_once()

    @pytest.mark.asyncio
    async def test_error_handling_in_methods(self, postgresql_provider):
        """Test error handling in provider methods."""
        provider, connection = postgresql_provider
        
        connection.execute = AsyncMock(side_effect=Exception("Database error"))
        
        with pytest.raises(Exception, match="Database error"):
            await provider.start_pipeline_run("test-run", "test-pipeline")

    @pytest.mark.asyncio
    async def test_get_pipeline_run_not_found(self, postgresql_provider):
        """Test retrieving non-existent pipeline run."""
        provider, connection = postgresql_provider
        
        connection.fetchrow = AsyncMock(return_value=None)
        
        result = await provider.get_pipeline_run("non-existent")
        
        assert result is None
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_update_pipeline_run_counts(self, postgresql_provider):
        """Test updating pipeline run record counts."""
        provider, connection = postgresql_provider
        
        connection.execute = AsyncMock()
        
        await provider.update_pipeline_run_counts("test-run", total=100, successful=95, failed=5)
        
        connection.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_audit_event(self, postgresql_provider):
        """Test recording an audit event."""
        provider, connection = postgresql_provider
        
        mock_row = {'id': 1}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.record_audit_event(
            "test-run", "event_type", "stage", "message"
        )
        
        assert result == "1"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_performance_metric(self, postgresql_provider):
        """Test recording a performance metric."""
        provider, connection = postgresql_provider
        
        mock_row = {'id': 1}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        start_time = datetime.now(timezone.utc)
        end_time = datetime.now(timezone.utc)
        
        result = await provider.record_performance_metric(
            "test-run", "stage", start_time, end_time, records_processed=100
        )
        
        assert result == "1"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_ssl_configuration_require(self, mock_pool):
        """Test SSL configuration with require mode."""
        pool, connection = mock_pool
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "ssl_mode": "require"
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg, \
             patch('pulsepipe.persistence.postgresql_provider.ssl') as mock_ssl:
            
            mock_ssl_context = MagicMock()
            mock_ssl.create_default_context.return_value = mock_ssl_context
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            
            provider = PostgreSQLPersistenceProvider(config)
            await provider.connect()
            
            # Verify SSL context was configured
            mock_ssl.create_default_context.assert_called_once()
            assert mock_ssl_context.check_hostname is False
            assert mock_ssl_context.verify_mode == mock_ssl.CERT_NONE

    @pytest.mark.asyncio
    async def test_ssl_configuration_verify_ca(self, mock_pool):
        """Test SSL configuration with verify-ca mode."""
        pool, connection = mock_pool
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "ssl_mode": "verify-ca"
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg, \
             patch('pulsepipe.persistence.postgresql_provider.ssl') as mock_ssl:
            
            mock_ssl_context = MagicMock()
            mock_ssl.create_default_context.return_value = mock_ssl_context
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            
            provider = PostgreSQLPersistenceProvider(config)
            await provider.connect()
            
            assert mock_ssl_context.check_hostname is False
            assert mock_ssl_context.verify_mode == mock_ssl.CERT_REQUIRED

    @pytest.mark.asyncio
    async def test_ssl_configuration_verify_full(self, mock_pool):
        """Test SSL configuration with verify-full mode."""
        pool, connection = mock_pool
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "ssl_mode": "verify-full"
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg, \
             patch('pulsepipe.persistence.postgresql_provider.ssl') as mock_ssl:
            
            mock_ssl_context = MagicMock()
            mock_ssl.create_default_context.return_value = mock_ssl_context
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            
            provider = PostgreSQLPersistenceProvider(config)
            await provider.connect()
            
            assert mock_ssl_context.check_hostname is True
            assert mock_ssl_context.verify_mode == mock_ssl.CERT_REQUIRED

    @pytest.mark.asyncio
    async def test_ssl_with_certificates(self, mock_pool):
        """Test SSL configuration with certificates."""
        pool, connection = mock_pool
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "ssl_mode": "require",
            "ssl_cert": "/path/to/cert.pem",
            "ssl_key": "/path/to/key.pem",
            "ssl_ca": "/path/to/ca.pem"
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg, \
             patch('pulsepipe.persistence.postgresql_provider.ssl') as mock_ssl:
            
            mock_ssl_context = MagicMock()
            mock_ssl.create_default_context.return_value = mock_ssl_context
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            
            provider = PostgreSQLPersistenceProvider(config)
            await provider.connect()
            
            mock_ssl_context.load_cert_chain.assert_called_once_with("/path/to/cert.pem", "/path/to/key.pem")
            mock_ssl_context.load_verify_locations.assert_called_once_with("/path/to/ca.pem")

    @pytest.mark.asyncio
    async def test_connection_with_no_database_connection(self):
        """Test operations when no database connection is available."""
        config = {"host": "localhost"}
        provider = PostgreSQLPersistenceProvider(config)
        
        with pytest.raises(RuntimeError, match="No database connection available"):
            async with provider._get_connection():
                pass

    @pytest.mark.asyncio
    async def test_initialize_schema_not_connected(self):
        """Test schema initialization when not connected."""
        config = {"host": "localhost"}
        provider = PostgreSQLPersistenceProvider(config)
        
        with pytest.raises(RuntimeError, match="Database not connected"):
            await provider.initialize_schema()

    @pytest.mark.asyncio
    async def test_health_check_no_connection(self):
        """Test health check when no connection is available."""
        config = {"host": "localhost"}
        provider = PostgreSQLPersistenceProvider(config)
        
        result = await provider.health_check()
        assert result is False

    @pytest.mark.asyncio
    async def test_health_check_connection_error(self, postgresql_provider):
        """Test health check with connection error."""
        provider, connection = postgresql_provider
        
        connection.execute = AsyncMock(side_effect=Exception("Connection error"))
        
        result = await provider.health_check()
        assert result is False

    @pytest.mark.asyncio
    async def test_disconnect_error_handling(self, postgresql_provider):
        """Test disconnect error handling."""
        provider, _ = postgresql_provider
        
        # Mock pool.close to raise an exception
        provider.pool.close.side_effect = Exception("Close error")
        
        # Should not raise, just log warning
        await provider.disconnect()
        
        # Pool should still be set to None
        assert provider.pool is None

    @pytest.mark.asyncio
    async def test_record_failed_record(self, postgresql_provider):
        """Test recording a failed record."""
        provider, connection = postgresql_provider
        
        mock_row = {'id': 123}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.record_failed_record(
            "456", "original data", "failure reason", "normalized data", "stack trace"
        )
        
        assert result == "123"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_system_metric(self, postgresql_provider):
        """Test recording system metrics."""
        provider, connection = postgresql_provider
        
        mock_row = {'id': 789}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        additional_info = {
            "cpu_threads": 8,
            "memory_available_gb": 16.0,
            "disk_total_gb": 500.0,
            "disk_free_gb": 250.0,
            "gpu_memory_gb": 8.0,
            "network_interfaces": ["eth0", "wlan0"],
            "environment_variables": {"PATH": "/usr/bin"},
            "package_versions": {"python": "3.11"}
        }
        
        result = await provider.record_system_metric(
            "test-run", hostname="test-host", os_name="Linux",
            additional_info=additional_info
        )
        
        assert result == "789"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ingestion_summary_with_filters(self, postgresql_provider):
        """Test getting ingestion summary with date filters."""
        provider, connection = postgresql_provider
        
        mock_rows = [
            {
                'status': 'success',
                'error_category': None,
                'count': 95,
                'avg_processing_time': 100.0,
                'total_bytes': 95000
            },
            {
                'status': 'failure',
                'error_category': 'validation_error',
                'count': 5,
                'avg_processing_time': 50.0,
                'total_bytes': 5000
            }
        ]
        connection.fetch = AsyncMock(return_value=mock_rows)
        
        start_date = datetime.now(timezone.utc)
        end_date = datetime.now(timezone.utc)
        
        result = await provider.get_ingestion_summary(
            pipeline_run_id="test-run",
            start_date=start_date,
            end_date=end_date
        )
        
        assert result["total_records"] == 100
        assert result["successful_records"] == 95
        assert result["failed_records"] == 5
        assert result["error_breakdown"]["validation_error"] == 5
        assert result["total_bytes_processed"] == 100000
        connection.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_ingestion_summary_no_data(self, postgresql_provider):
        """Test getting ingestion summary with no data."""
        provider, connection = postgresql_provider
        
        connection.fetch = AsyncMock(return_value=[])
        
        result = await provider.get_ingestion_summary()
        
        assert result["total_records"] == 0
        assert result["successful_records"] == 0
        assert result["failed_records"] == 0
        assert result["avg_processing_time_ms"] == 0

    @pytest.mark.asyncio
    async def test_get_quality_summary(self, postgresql_provider):
        """Test getting quality metrics summary."""
        provider, connection = postgresql_provider
        
        mock_row = {
            'total_records': 100,
            'avg_completeness': 0.95,
            'avg_consistency': 0.90,
            'avg_validity': 0.85,
            'avg_accuracy': 0.92,
            'avg_overall_score': 0.905,
            'min_score': 0.75,
            'max_score': 0.98
        }
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.get_quality_summary("test-run")
        
        assert result["total_records"] == 100
        assert result["avg_completeness_score"] == 0.95
        assert result["avg_consistency_score"] == 0.90
        assert result["min_overall_score"] == 0.75
        assert result["max_overall_score"] == 0.98

    @pytest.mark.asyncio
    async def test_get_quality_summary_no_data(self, postgresql_provider):
        """Test getting quality summary with no data."""
        provider, connection = postgresql_provider
        
        mock_row = {
            'total_records': 0,
            'avg_completeness': None,
            'avg_consistency': None,
            'avg_validity': None,
            'avg_accuracy': None,
            'avg_overall_score': None,
            'min_score': None,
            'max_score': None
        }
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.get_quality_summary()
        
        assert result["total_records"] == 0
        assert result["avg_completeness_score"] is None
        assert result["avg_overall_score"] is None

    @pytest.mark.asyncio
    async def test_cleanup_old_data(self, postgresql_provider):
        """Test cleaning up old data."""
        provider, connection = postgresql_provider
        
        # Mock the fetch for finding old runs
        mock_runs = [{'id': 'old-run-1'}, {'id': 'old-run-2'}]
        connection.fetch = AsyncMock(return_value=mock_runs)
        
        # Mock the delete operation
        connection.execute = AsyncMock(return_value="DELETE 2")
        
        result = await provider.cleanup_old_data(days_to_keep=7)
        
        assert result == 2
        connection.fetch.assert_called_once()
        connection.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_cleanup_old_data_no_old_runs(self, postgresql_provider):
        """Test cleanup when no old runs exist."""
        provider, connection = postgresql_provider
        
        connection.fetch = AsyncMock(return_value=[])
        # Reset execute mock to avoid interference from other calls
        connection.execute.reset_mock()
        
        result = await provider.cleanup_old_data(days_to_keep=7)
        
        assert result == 0
        connection.fetch.assert_called_once()
        # execute should not be called since no old runs exist
        connection.execute.assert_not_called()

    @pytest.mark.asyncio
    async def test_cleanup_old_data_invalid_response(self, postgresql_provider):
        """Test cleanup with invalid delete response."""
        provider, connection = postgresql_provider
        
        mock_runs = [{'id': 'old-run-1'}]
        connection.fetch = AsyncMock(return_value=mock_runs)
        connection.execute = AsyncMock(return_value="INVALID")
        
        result = await provider.cleanup_old_data(days_to_keep=7)
        
        assert result == 0

    @pytest.mark.asyncio
    async def test_initialize_schema(self, postgresql_provider):
        """Test schema initialization."""
        provider, connection = postgresql_provider
        
        connection.execute = AsyncMock()
        
        await provider.initialize_schema()
        
        # Should be called multiple times for different tables and indexes
        assert connection.execute.call_count > 10

    @pytest.mark.asyncio
    async def test_get_connection_with_pool(self, postgresql_provider):
        """Test getting connection from pool."""
        provider, _ = postgresql_provider
        
        async with provider._get_connection() as conn:
            # This should work without raising
            assert conn is not None

    @pytest.mark.asyncio
    async def test_get_connection_direct_connection(self):
        """Test getting direct connection."""
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "use_pool": False
        }
        
        mock_connection = AsyncMock()
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
            mock_asyncpg.connect = AsyncMock(return_value=mock_connection)
            provider = PostgreSQLPersistenceProvider(config)
            await provider.connect()
            
            async with provider._get_connection() as conn:
                assert conn == mock_connection

    @pytest.mark.asyncio
    async def test_record_ingestion_stat_with_error_details(self, postgresql_provider):
        """Test recording ingestion stat with error details."""
        provider, connection = postgresql_provider
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id="test-run-123",
            stage_name="ingestion",
            file_path="test.json",
            record_id="record-1",
            record_type="fhir",
            status=ProcessingStatus.FAILURE,
            error_category=ErrorCategory.VALIDATION_ERROR,
            error_message="Invalid data",
            error_details={"field": "patient_id", "issue": "missing"},
            processing_time_ms=100,
            record_size_bytes=1024,
            data_source="test",
            timestamp=datetime.now(timezone.utc)
        )
        
        mock_row = {'id': 123}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.record_ingestion_stat(stat)
        
        assert result == "123"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_audit_event_with_details(self, postgresql_provider):
        """Test recording audit event with details."""
        provider, connection = postgresql_provider
        
        mock_row = {'id': 456}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        details = {"context": "test", "data": "value"}
        
        result = await provider.record_audit_event(
            "test-run", "data_processed", "ingestion", "Record processed successfully",
            event_level="INFO", record_id="rec-123", details=details, correlation_id="corr-456"
        )
        
        assert result == "456"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_start_pipeline_run_with_config_snapshot(self, postgresql_provider):
        """Test starting pipeline run with config snapshot."""
        provider, connection = postgresql_provider
        
        connection.execute = AsyncMock()
        
        config_snapshot = {"ingester": "fhir", "chunker": "clinical"}
        
        await provider.start_pipeline_run("test-run", "test-pipeline", config_snapshot)
        
        connection.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_complete_pipeline_run_with_error(self, postgresql_provider):
        """Test completing pipeline run with error message."""
        provider, connection = postgresql_provider
        
        connection.execute = AsyncMock()
        
        await provider.complete_pipeline_run("test-run", "failed", "Processing error occurred")
        
        connection.execute.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_performance_metric_with_optional_params(self, postgresql_provider):
        """Test recording performance metric with all optional parameters."""
        provider, connection = postgresql_provider
        
        mock_row = {'id': 789}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        start_time = datetime.now(timezone.utc)
        end_time = datetime.now(timezone.utc)
        
        result = await provider.record_performance_metric(
            "test-run", "chunking", start_time, end_time,
            records_processed=1000, memory_usage_mb=512.5,
            cpu_usage_percent=75.2, bottleneck_indicator="cpu_bound"
        )
        
        assert result == "789"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_record_system_metric_minimal(self, postgresql_provider):
        """Test recording system metric with minimal parameters."""
        provider, connection = postgresql_provider
        
        mock_row = {'id': 101}
        connection.fetchrow = AsyncMock(return_value=mock_row)
        
        result = await provider.record_system_metric("test-run")
        
        assert result == "101"
        connection.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_schema_configuration(self):
        """Test custom schema configuration."""
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "schema": "custom_schema"
        }
        
        provider = PostgreSQLPersistenceProvider(config)
        assert provider.schema == "custom_schema"

    @pytest.mark.asyncio
    async def test_ssl_disabled(self, mock_pool):
        """Test SSL disabled configuration."""
        pool, connection = mock_pool
        config = {
            "host": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass",
            "ssl_mode": "disable"
        }
        
        with patch('pulsepipe.persistence.postgresql_provider.asyncpg') as mock_asyncpg:
            mock_asyncpg.create_pool = AsyncMock(return_value=pool)
            
            provider = PostgreSQLPersistenceProvider(config)
            await provider.connect()
            
            # Check that SSL context was not created
            call_args = mock_asyncpg.create_pool.call_args[1]
            assert 'ssl' not in call_args