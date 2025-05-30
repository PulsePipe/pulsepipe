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

from typing import Optional

from .models import ProcessingStatus, ErrorCategory
from .tracking_repository import TrackingRepository
from .database import (
    DatabaseConnection,
    SQLDialect,
    ConfigurationError
)
from .database.sqlite_impl import SQLiteConnection, SQLiteDialect
from .database.postgresql_impl import PostgreSQLConnection, PostgreSQLDialect
from .database.mongodb_impl import MongoDBConnection, MongoDBAdapter


def get_database_connection(config: dict) -> DatabaseConnection:
    """
    Create a database connection based on configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        DatabaseConnection instance for the configured database type
        
    Raises:
        ConfigurationError: If database configuration is invalid
    """
    persistence_config = config.get("persistence", {})
    db_type = persistence_config.get("type", persistence_config.get("db_type", "sqlite"))
    
    if db_type == "sqlite":
        sqlite_config = persistence_config.get("sqlite", {})
        db_path = sqlite_config.get("db_path", ".pulsepipe/state/ingestion.sqlite3")
        timeout = sqlite_config.get("timeout", 30.0)
        
        return SQLiteConnection(db_path=db_path, timeout=timeout)
    
    elif db_type == "postgresql":
        pg_config = persistence_config.get("postgresql", {})
        
        required_fields = ["host", "port", "database", "username", "password"]
        missing_fields = [field for field in required_fields if field not in pg_config]
        
        if missing_fields:
            raise ConfigurationError(
                f"PostgreSQL configuration missing required fields: {missing_fields}"
            )
        
        return PostgreSQLConnection(
            host=pg_config["host"],
            port=pg_config["port"],
            database=pg_config["database"],
            username=pg_config["username"],
            password=pg_config["password"],
            pool_size=pg_config.get("pool_size", 5),
            max_overflow=pg_config.get("max_overflow", 10)
        )
    
    elif db_type == "mongodb":
        mongo_config = persistence_config.get("mongodb", {})
        
        connection_string = mongo_config.get("connection_string")
        database = mongo_config.get("database")
        
        if not connection_string or not database:
            raise ConfigurationError(
                "MongoDB configuration requires 'connection_string' and 'database'"
            )
        
        return MongoDBConnection(
            connection_string=connection_string,
            database=database,
            collection_prefix=mongo_config.get("collection_prefix", "audit_"),
            username=mongo_config.get("username"),
            password=mongo_config.get("password"),
            replica_set=mongo_config.get("replica_set"),
            read_preference=mongo_config.get("read_preference")
        )
    
    else:
        raise ConfigurationError(f"Unsupported database type: {db_type}")


def get_sql_dialect(config: dict) -> SQLDialect:
    """
    Create a SQL dialect based on configuration.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        SQLDialect instance for the configured database type
        
    Raises:
        ConfigurationError: If database configuration is invalid
    """
    persistence_config = config.get("persistence", {})
    db_type = persistence_config.get("type", persistence_config.get("db_type", "sqlite"))
    
    if db_type == "sqlite":
        return SQLiteDialect()
    
    elif db_type == "postgresql":
        return PostgreSQLDialect()
    
    elif db_type == "mongodb":
        mongo_config = persistence_config.get("mongodb", {})
        collection_prefix = mongo_config.get("collection_prefix", "audit_")
        return MongoDBAdapter(collection_prefix=collection_prefix)
    
    else:
        raise ConfigurationError(f"Unsupported database type: {db_type}")




def get_tracking_repository(config: dict, connection: Optional[DatabaseConnection] = None) -> TrackingRepository:
    """
    Get a tracking repository instance.
    
    Args:
        config: Configuration dictionary
        connection: Optional existing connection, creates new one if None
        
    Returns:
        TrackingRepository instance ready for use
    """
    if connection is None:
        # Create new connection using the database abstraction
        db_connection = get_database_connection(config)
        dialect = get_sql_dialect(config)
        return TrackingRepository(db_connection, dialect)
    else:
        # Use provided connection
        dialect = get_sql_dialect(config)
        return TrackingRepository(connection, dialect)


