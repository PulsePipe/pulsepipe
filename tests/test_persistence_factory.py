# tests/test_persistence_factory.py

import pytest
import tempfile
import os
from unittest.mock import patch, MagicMock

from pulsepipe.persistence.factory import (
    create_persistence_provider,
    get_async_tracking_repository,
    validate_persistence_config
)
from pulsepipe.persistence.sqlite_provider import SQLitePersistenceProvider
from pulsepipe.persistence.mongodb_provider import MongoDBPersistenceProvider
from pulsepipe.persistence.postgresql_provider import PostgreSQLPersistenceProvider
from pulsepipe.persistence.sqlserver_provider import SQLServerPersistenceProvider
from pulsepipe.persistence.base import BaseTrackingRepository


class TestCreatePersistenceProvider:
    """Test persistence provider creation."""
    
    def test_create_sqlite_provider(self):
        """Test creating SQLite provider."""
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": ":memory:",
                    "enable_wal": False
                }
            }
        }
        
        provider = create_persistence_provider(config)
        
        assert isinstance(provider, SQLitePersistenceProvider)
        assert provider.db_path == ":memory:"
        assert not provider.enable_wal
    
    def test_create_provider_no_config(self):
        """Test creating provider with no persistence configuration."""
        config = {}
        
        # Should default to SQLite
        provider = create_persistence_provider(config)
        assert isinstance(provider, SQLitePersistenceProvider)
    
    def test_create_provider_invalid_type(self):
        """Test creating provider with invalid type."""
        config = {
            "persistence": {
                "type": "invalid_db_type"
            }
        }
        
        with pytest.raises(ValueError) as exc_info:
            create_persistence_provider(config)
        
        assert "Unsupported persistence provider type: invalid_db_type" in str(exc_info.value)
    
    @patch('pulsepipe.persistence.factory.MongoDBPersistenceProvider')
    def test_create_mongodb_provider_import_error(self, mock_mongodb):
        """Test creating MongoDB provider with ImportError."""
        mock_mongodb.side_effect = ImportError("pymongo not found")
        
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {"host": "localhost", "database": "test"}
            }
        }
        
        with pytest.raises(ImportError) as exc_info:
            create_persistence_provider(config)
        
        assert "MongoDB dependencies not installed" in str(exc_info.value)
        assert "poetry add pymongo" in str(exc_info.value)
    
    @patch('pulsepipe.persistence.factory.PostgreSQLPersistenceProvider')
    def test_create_postgresql_provider_import_error(self, mock_postgresql):
        """Test creating PostgreSQL provider with ImportError."""
        mock_postgresql.side_effect = ImportError("asyncpg not found")
        
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {"host": "localhost", "database": "test"}
            }
        }
        
        with pytest.raises(ImportError) as exc_info:
            create_persistence_provider(config)
        
        assert "PostgreSQL dependencies not installed" in str(exc_info.value)
        assert "poetry add asyncpg" in str(exc_info.value)
    
    @patch('pulsepipe.persistence.factory.SQLServerPersistenceProvider')
    def test_create_sqlserver_provider_import_error(self, mock_sqlserver):
        """Test creating SQL Server provider with ImportError."""
        mock_sqlserver.side_effect = ImportError("pyodbc not found")
        
        config = {
            "persistence": {
                "type": "sqlserver",
                "sqlserver": {"server": "localhost", "database": "test"}
            }
        }
        
        with pytest.raises(ImportError) as exc_info:
            create_persistence_provider(config)
        
        assert "SQL Server dependencies not installed" in str(exc_info.value)
        assert "poetry add pyodbc sqlalchemy" in str(exc_info.value)


@pytest.mark.asyncio
class TestGetAsyncTrackingRepository:
    """Test async tracking repository creation."""
    
    async def test_get_sqlite_repository(self):
        """Test getting async repository with SQLite provider."""
        with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
            db_path = tmp_file.name
        
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {"db_path": db_path}
            }
        }
        
        repository = await get_async_tracking_repository(config)
        
        assert isinstance(repository, BaseTrackingRepository)
        assert await repository.health_check()
        
        await repository.disconnect()
        os.unlink(db_path)


class TestValidatePersistenceConfig:
    """Test persistence configuration validation."""
    
    def test_validate_sqlite_config(self):
        """Test validating SQLite configuration."""
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": ":memory:"
                }
            }
        }
        
        assert validate_persistence_config(config) is True
    
    def test_validate_invalid_provider_type(self):
        """Test validating configuration with invalid provider type."""
        config = {
            "persistence": {
                "type": "invalid_type"
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_default_config(self):
        """Test validating empty configuration (defaults to SQLite)."""
        config = {}
        assert validate_persistence_config(config) is True
    
    @patch('pathlib.Path.mkdir')
    def test_validate_sqlite_config_invalid_path(self, mock_mkdir):
        """Test validating SQLite configuration with invalid path."""
        mock_mkdir.side_effect = OSError("Permission denied")
        
        config = {
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": "/invalid/path/to/db.sqlite"
                }
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_mongodb_config_missing_host(self):
        """Test validating MongoDB configuration with missing host."""
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {
                    "database": "test_db"
                }
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_mongodb_config_missing_database(self):
        """Test validating MongoDB configuration with missing database."""
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {
                    "host": "localhost"
                }
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_postgresql_config_missing_host(self):
        """Test validating PostgreSQL configuration with missing host."""
        config = {
            "persistence": {
                "type": "postgresql", 
                "postgresql": {
                    "database": "test_db"
                }
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_postgresql_config_missing_database(self):
        """Test validating PostgreSQL configuration with missing database."""
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {
                    "host": "localhost"
                }
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_sqlserver_config_missing_server(self):
        """Test validating SQL Server configuration with missing server."""
        config = {
            "persistence": {
                "type": "sqlserver",
                "sqlserver": {
                    "database": "test_db"
                }
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_sqlserver_config_missing_database(self):
        """Test validating SQL Server configuration with missing database."""
        config = {
            "persistence": {
                "type": "sqlserver",
                "sqlserver": {
                    "server": "localhost"
                }
            }
        }
        
        assert validate_persistence_config(config) is False
    
    def test_validate_mongodb_config_valid(self):
        """Test validating valid MongoDB configuration."""
        config = {
            "persistence": {
                "type": "mongodb",
                "mongodb": {
                    "host": "localhost",
                    "database": "test_db"
                }
            }
        }
        
        assert validate_persistence_config(config) is True
    
    def test_validate_postgresql_config_valid(self):
        """Test validating valid PostgreSQL configuration."""
        config = {
            "persistence": {
                "type": "postgresql",
                "postgresql": {
                    "host": "localhost",
                    "database": "test_db"
                }
            }
        }
        
        assert validate_persistence_config(config) is True
    
    def test_validate_sqlserver_config_valid(self):
        """Test validating valid SQL Server configuration."""
        config = {
            "persistence": {
                "type": "sqlserver",
                "sqlserver": {
                    "server": "localhost",
                    "database": "test_db"
                }
            }
        }
        
        assert validate_persistence_config(config) is True


@pytest.mark.asyncio
async def test_end_to_end_sqlite():
    """Test end-to-end workflow with SQLite."""
    config = {
        "persistence": {
            "type": "sqlite",
            "sqlite": {"db_path": ":memory:"}
        }
    }
    
    # Validate config
    assert validate_persistence_config(config)
    
    # Create repository
    repository = await get_async_tracking_repository(config)
    
    # Use repository
    await repository.start_pipeline_run("integration-test", "test-pipeline")
    run_summary = await repository.get_pipeline_run("integration-test")
    
    assert run_summary is not None
    assert run_summary.name == "test-pipeline"
    
    await repository.disconnect()