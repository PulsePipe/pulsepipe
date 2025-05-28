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

# src/pulsepipe/persistence/factory.py

"""
Persistence factory for creating database providers and repositories.

Supports multiple database backends including SQLite, MongoDB, and SQL Server
with configurable connection parameters and security settings.
"""

from pathlib import Path
from typing import Dict, Any

from pulsepipe.utils.log_factory import LogFactory
from .base import BasePersistenceProvider, BaseTrackingRepository
from .sqlite_provider import SQLitePersistenceProvider

# Conditional imports for optional database providers
try:
    from .mongodb_provider import MongoDBPersistenceProvider
    MONGODB_AVAILABLE = True
except ImportError:
    MongoDBPersistenceProvider = None
    MONGODB_AVAILABLE = False

try:
    from .postgresql_provider import PostgreSQLPersistenceProvider
    POSTGRESQL_AVAILABLE = True
except ImportError:
    PostgreSQLPersistenceProvider = None
    POSTGRESQL_AVAILABLE = False

try:
    from .sqlserver_provider import SQLServerPersistenceProvider
    SQLSERVER_AVAILABLE = True
except ImportError:
    SQLServerPersistenceProvider = None
    SQLSERVER_AVAILABLE = False

logger = LogFactory.get_logger(__name__)

def create_persistence_provider(config: Dict[str, Any]) -> BasePersistenceProvider:
    """
    Create a persistence provider based on configuration.
    
    Args:
        config: Configuration dictionary with persistence settings
        
    Returns:
        BasePersistenceProvider instance
        
    Raises:
        ValueError: If provider type is unsupported
        ImportError: If required dependencies are missing
    """
    persistence_config = config.get("persistence", {})
    provider_type = persistence_config.get("type", "sqlite").lower()
    
    logger.info(f"Creating persistence provider: {provider_type}")
    
    if provider_type == "sqlite":
        return SQLitePersistenceProvider(persistence_config.get("sqlite", {}))
    
    elif provider_type == "mongodb":
        if not MONGODB_AVAILABLE:
            raise ImportError(
                "MongoDB dependencies not installed. Install with: poetry install --extras mongodb"
            )
        try:
            return MongoDBPersistenceProvider(persistence_config.get("mongodb", {}))
        except ImportError as e:
            raise ImportError(
                f"MongoDB dependencies not installed. Original error: {e}. "
                f"Install with: poetry add pymongo"
            )
    
    elif provider_type == "postgresql":
        if not POSTGRESQL_AVAILABLE:
            raise ImportError(
                "PostgreSQL dependencies not installed. Install with: poetry install --extras postgresql"
            )
        try:
            return PostgreSQLPersistenceProvider(persistence_config.get("postgresql", {}))
        except ImportError as e:
            raise ImportError(
                f"PostgreSQL dependencies not installed. Original error: {e}. "
                f"Install with: poetry add asyncpg"
            )
    
    elif provider_type == "sqlserver":
        if not SQLSERVER_AVAILABLE:
            raise ImportError(
                "SQL Server dependencies not installed. Install with: poetry install --extras sqlserver"
            )
        try:
            return SQLServerPersistenceProvider(persistence_config.get("sqlserver", {}))
        except ImportError as e:
            raise ImportError(
                f"SQL Server dependencies not installed. Original error: {e}. "
                f"Install with: poetry add pyodbc sqlalchemy"
            )
    
    else:
        raise ValueError(
            f"Unsupported persistence provider type: {provider_type}. "
            f"Supported types: sqlite, mongodb, postgresql, sqlserver"
        )


async def get_async_tracking_repository(config: Dict[str, Any]) -> BaseTrackingRepository:
    """
    Get an async tracking repository instance with initialized schema.
    
    Args:
        config: Configuration dictionary with persistence settings
        
    Returns:
        BaseTrackingRepository instance ready for use
    """
    provider = create_persistence_provider(config)
    repository = BaseTrackingRepository(provider)
    
    # Initialize the repository (connect and setup schema)
    await repository.initialize()
    
    logger.info(f"Async tracking repository initialized with {type(provider).__name__}")
    return repository



# Configuration validation

def validate_persistence_config(config: Dict[str, Any]) -> bool:
    """
    Validate persistence configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        True if valid, False otherwise
    """
    persistence_config = config.get("persistence", {})
    provider_type = persistence_config.get("type", "sqlite").lower()
    
    if provider_type not in ["sqlite", "mongodb", "postgresql", "sqlserver"]:
        logger.error(f"Invalid persistence provider type: {provider_type}")
        return False
    
    # Provider-specific validation
    if provider_type == "sqlite":
        sqlite_config = persistence_config.get("sqlite", {})
        db_path = sqlite_config.get("db_path")
        if db_path:
            try:
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(f"Invalid SQLite database path: {e}")
                return False
    
    elif provider_type == "mongodb":
        mongodb_config = persistence_config.get("mongodb", {})
        if not mongodb_config.get("host"):
            logger.error("MongoDB host is required")
            return False
        if not mongodb_config.get("database"):
            logger.error("MongoDB database name is required")
            return False
    
    elif provider_type == "postgresql":
        postgresql_config = persistence_config.get("postgresql", {})
        if not postgresql_config.get("host"):
            logger.error("PostgreSQL host is required")
            return False
        if not postgresql_config.get("database"):
            logger.error("PostgreSQL database name is required")
            return False
    
    elif provider_type == "sqlserver":
        sqlserver_config = persistence_config.get("sqlserver", {})
        if not sqlserver_config.get("server"):
            logger.error("SQL Server host is required")
            return False
        if not sqlserver_config.get("database"):
            logger.error("SQL Server database name is required")
            return False
    
    return True
