"""Unit tests for SQL Server persistence provider."""

import pytest
import pytest_asyncio
from datetime import datetime, timezone

from pulsepipe.persistence.sqlserver_provider import SQLServerPersistenceProvider
from pulsepipe.persistence.base import IngestionStat, QualityMetric
from pulsepipe.persistence.models import ProcessingStatus, ErrorCategory


class TestSQLServerProvider:
    """Test SQL Server persistence provider functionality."""

    @pytest.mark.asyncio
    async def test_init_with_defaults(self):
        """Test provider initialization with default values."""
        config = {
            "server": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass"
        }
        provider = SQLServerPersistenceProvider(config)
        
        assert provider.server == "localhost"
        assert provider.port == 1433
        assert provider.database == "test_db"
        assert provider.username == "user"
        assert provider.password == "pass"
        assert provider.driver == "ODBC Driver 18 for SQL Server"
        assert provider.encrypt is True
        assert provider.trust_server_certificate is False
        assert provider.connection_timeout == 30
        assert provider.command_timeout == 30
        assert provider.use_windows_auth is False
        assert provider.application_name == "PulsePipe"

    @pytest.mark.asyncio
    async def test_init_with_custom_config(self):
        """Test provider initialization with custom configuration."""
        config = {
            "server": "custom-server",
            "port": 1434,
            "database": "custom_db",
            "username": "custom_user",
            "password": "custom_pass",
            "driver": "Custom Driver",
            "encrypt": False,
            "trust_server_certificate": True,
            "connection_timeout": 60,
            "command_timeout": 120,
            "use_windows_auth": True,
            "application_name": "CustomApp"
        }
        provider = SQLServerPersistenceProvider(config)
        
        assert provider.server == "custom-server"
        assert provider.port == 1434
        assert provider.database == "custom_db"
        assert provider.username == "custom_user"
        assert provider.password == "custom_pass"
        assert provider.driver == "Custom Driver"
        assert provider.encrypt is False
        assert provider.trust_server_certificate is True
        assert provider.connection_timeout == 60
        assert provider.command_timeout == 120
        assert provider.use_windows_auth is True
        assert provider.application_name == "CustomApp"

    @pytest.mark.asyncio
    async def test_connect_raises_enterprise_error(self):
        """Test that connect raises enterprise edition error."""
        config = {
            "server": "localhost",
            "database": "test_db",
            "username": "user",
            "password": "pass"
        }
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.connect()
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message
        assert "abramsamir@gmail.com" in error_message
        assert "SQLite, PostgreSQL, and MongoDB" in error_message

    @pytest.mark.asyncio
    async def test_disconnect_raises_enterprise_error(self):
        """Test that disconnect raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.disconnect()
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_initialize_schema_raises_enterprise_error(self):
        """Test that initialize_schema raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.initialize_schema()
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_health_check_raises_enterprise_error(self):
        """Test that health_check raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.health_check()
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_start_pipeline_run_raises_enterprise_error(self):
        """Test that start_pipeline_run raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.start_pipeline_run("run-1", "test-pipeline")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_complete_pipeline_run_raises_enterprise_error(self):
        """Test that complete_pipeline_run raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.complete_pipeline_run("run-1")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_update_pipeline_run_counts_raises_enterprise_error(self):
        """Test that update_pipeline_run_counts raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.update_pipeline_run_counts("run-1", total=100)
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_get_pipeline_run_raises_enterprise_error(self):
        """Test that get_pipeline_run raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.get_pipeline_run("run-1")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_record_ingestion_stat_raises_enterprise_error(self):
        """Test that record_ingestion_stat raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        stat = IngestionStat(
            id=None,
            pipeline_run_id="run-1",
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
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.record_ingestion_stat(stat)
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_record_failed_record_raises_enterprise_error(self):
        """Test that record_failed_record raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.record_failed_record("stat-1", "original_data", "failure_reason")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_record_quality_metric_raises_enterprise_error(self):
        """Test that record_quality_metric raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        metric = QualityMetric(
            id=None,
            pipeline_run_id="run-1",
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
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.record_quality_metric(metric)
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_record_audit_event_raises_enterprise_error(self):
        """Test that record_audit_event raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.record_audit_event("run-1", "event_type", "stage", "message")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_record_performance_metric_raises_enterprise_error(self):
        """Test that record_performance_metric raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        start_time = datetime.now(timezone.utc)
        end_time = datetime.now(timezone.utc)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.record_performance_metric("run-1", "stage", start_time, end_time)
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_record_system_metric_raises_enterprise_error(self):
        """Test that record_system_metric raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.record_system_metric("run-1", hostname="test-host")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_get_ingestion_summary_raises_enterprise_error(self):
        """Test that get_ingestion_summary raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.get_ingestion_summary("run-1")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_get_quality_summary_raises_enterprise_error(self):
        """Test that get_quality_summary raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.get_quality_summary("run-1")
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_get_recent_pipeline_runs_raises_enterprise_error(self):
        """Test that get_recent_pipeline_runs raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.get_recent_pipeline_runs(limit=10)
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_cleanup_old_data_raises_enterprise_error(self):
        """Test that cleanup_old_data raises enterprise edition error."""
        config = {"server": "localhost"}
        provider = SQLServerPersistenceProvider(config)
        
        with pytest.raises(NotImplementedError) as exc_info:
            await provider.cleanup_old_data(days_to_keep=30)
        
        error_message = str(exc_info.value)
        assert "SQL Server persistence is available in PulsePipe Enterprise Edition only" in error_message

    @pytest.mark.asyncio
    async def test_provider_stores_config_properly(self):
        """Test that provider stores configuration properly."""
        config = {
            "server": "test-server",
            "port": 1435,
            "database": "test_database",
            "username": "test_user",
            "password": "test_password",
            "driver": "Test Driver",
            "encrypt": False,
            "trust_server_certificate": True,
            "connection_timeout": 45,
            "command_timeout": 90,
            "use_windows_auth": True,
            "application_name": "TestApp"
        }
        
        provider = SQLServerPersistenceProvider(config)
        
        # Verify all config values are stored
        assert provider.config == config
        assert provider.connection is None  # Should start as None
        
        # Verify individual properties
        for key, expected_value in config.items():
            actual_value = getattr(provider, key)
            assert actual_value == expected_value, f"Property {key} should be {expected_value}, got {actual_value}"

    @pytest.mark.asyncio
    async def test_provider_handles_missing_config_values(self):
        """Test that provider handles missing configuration values with defaults."""
        minimal_config = {"server": "minimal-server"}
        provider = SQLServerPersistenceProvider(minimal_config)
        
        # Check that defaults are applied
        assert provider.server == "minimal-server"
        assert provider.port == 1433  # Default
        assert provider.database == "pulsepipe_intelligence"  # Default
        assert provider.username is None  # Not provided
        assert provider.password is None  # Not provided
        assert provider.driver == "ODBC Driver 18 for SQL Server"  # Default
        assert provider.encrypt is True  # Default
        assert provider.trust_server_certificate is False  # Default
        assert provider.connection_timeout == 30  # Default
        assert provider.command_timeout == 30  # Default
        assert provider.use_windows_auth is False  # Default
        assert provider.application_name == "PulsePipe"  # Default