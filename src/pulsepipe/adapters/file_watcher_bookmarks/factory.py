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

from .sqlite_store import SQLiteBookmarkStore
from .common_store import CommonBookmarkStore
from pulsepipe.persistence.factory import get_database_connection, get_sql_dialect
from pulsepipe.persistence.database.exceptions import ConfigurationError
from pulsepipe.utils.database_diagnostics import raise_database_diagnostic_error, DatabaseDiagnosticError
import time


def create_bookmark_store(config: dict):
    """
    Create a bookmark store based on configuration.
    
    Now supports all database backends through the unified adapter system.
    """
    # Check if we should use the new common store
    persistence_config = config.get("persistence", {})
    
    # If persistence config exists, use the common store with database adapters  
    if persistence_config:
        start_time = time.time()
        try:
            connection = get_database_connection(config)
            dialect = get_sql_dialect(config)
            elapsed = time.time() - start_time
            
            # Warn about slow connections
            if elapsed > 2.0:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"⚠️ Slow bookmark store connection: {elapsed:.2f}s")
            
            return CommonBookmarkStore(connection, dialect)
        except Exception as e:
            elapsed = time.time() - start_time
            # Get database type from either persistence.type or persistence.database.type
            db_type = (
                persistence_config.get("type") or 
                persistence_config.get("database", {}).get("type", "unknown")
            )
            
            # Use comprehensive diagnostics for better error reporting
            try:
                raise_database_diagnostic_error(config)
            except DatabaseDiagnosticError as diag_error:
                # Re-raise with bookmark store context
                enhanced_message = (
                    f"🔒 {db_type.title()} bookmark store initialization failed\n"
                    f"Connection attempt duration: {elapsed:.2f}s\n\n"
                    + str(diag_error)
                )
                raise ConfigurationError(
                    enhanced_message,
                    details={"database_type": db_type, "connection_time": elapsed, "original_error": str(e)},
                    original_error=e
                )
            
            # Fallback error handling if diagnostics fail
            raise ConfigurationError(
                f"🔒 {db_type.title()} bookmark store configuration error: {e}\n"
                f"Connection attempt duration: {elapsed:.2f}s\n\n"
                f"👉 Check your persistence.database configuration\n"
                f"💡 Verify connectivity, credentials, and database server availability\n"
                f"🛠️  For troubleshooting, see https://github.com/PulsePipe/pulsepipe/wiki/persistence/{db_type}\n\n"
                f"Fix the database connection to continue.",
                details={"database_type": db_type, "connection_time": elapsed, "original_error": str(e)},
                original_error=e
            )
    
    # Legacy store creation for backward compatibility
    store_type = config.get("type", "sqlite")

    if store_type == "sqlite":
        db_path = config.get("db_path", "bookmarks.db")
        
        # Create SQLite connection and dialect directly for legacy configs
        from pulsepipe.persistence.database.sqlite_impl import SQLiteConnection, SQLiteDialect
        connection = SQLiteConnection(db_path)
        dialect = SQLiteDialect()
        return CommonBookmarkStore(connection, dialect)

    elif store_type == "postgres" or store_type == "postgresql":
        # Now supported through the common store with enhanced diagnostics
        start_time = time.time()
        try:
            connection = get_database_connection(config)
            dialect = get_sql_dialect(config)
            elapsed = time.time() - start_time
            
            if elapsed > 2.0:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"⚠️ Slow PostgreSQL bookmark store connection: {elapsed:.2f}s")
            
            return CommonBookmarkStore(connection, dialect)
        except Exception as e:
            elapsed = time.time() - start_time
            
            # Use comprehensive diagnostics 
            try:
                raise_database_diagnostic_error(config)
            except DatabaseDiagnosticError as diag_error:
                enhanced_message = (
                    f"🔒 PostgreSQL bookmark store initialization failed\n"
                    f"Connection attempt duration: {elapsed:.2f}s\n\n"
                    + str(diag_error)
                )
                raise ConfigurationError(
                    enhanced_message,
                    details={"database_type": "postgresql", "connection_time": elapsed, "original_error": str(e)},
                    original_error=e
                )
            
            # Fallback error handling
            raise ConfigurationError(
                f"🔒 PostgreSQL bookmark store configuration error: {e}\n"
                f"Connection attempt duration: {elapsed:.2f}s\n\n"
                f"👉 Check your PostgreSQL configuration and connectivity\n"
                f"💡 Verify database server is running, credentials are correct, and network is accessible\n"
                f"🛠️  For troubleshooting, see https://github.com/PulsePipe/pulsepipe/wiki/persistence/postgresql\n\n"
                f"Fix the database connection to continue.",
                details={"database_type": "postgresql", "connection_time": elapsed, "original_error": str(e)},
                original_error=e
            )

    elif store_type == "mongodb":
        # Now supported through the common store with enhanced diagnostics
        start_time = time.time()
        try:
            connection = get_database_connection(config)
            dialect = get_sql_dialect(config)
            elapsed = time.time() - start_time
            
            if elapsed > 2.0:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"⚠️ Slow MongoDB bookmark store connection: {elapsed:.2f}s")
            
            return CommonBookmarkStore(connection, dialect)
        except Exception as e:
            elapsed = time.time() - start_time
            
            # Use comprehensive diagnostics
            try:
                raise_database_diagnostic_error(config)
            except DatabaseDiagnosticError as diag_error:
                enhanced_message = (
                    f"🔒 MongoDB bookmark store initialization failed\n"
                    f"Connection attempt duration: {elapsed:.2f}s\n\n"
                    + str(diag_error)
                )
                raise ConfigurationError(
                    enhanced_message,
                    details={"database_type": "mongodb", "connection_time": elapsed, "original_error": str(e)},
                    original_error=e
                )
            
            # Fallback error handling
            raise ConfigurationError(
                f"🔒 MongoDB bookmark store configuration error: {e}\n"
                f"Connection attempt duration: {elapsed:.2f}s\n\n"
                f"👉 Check your MongoDB configuration and connectivity\n"
                f"💡 Verify MongoDB server is running, credentials are correct, and network is accessible\n"
                f"🛠️  For troubleshooting, see https://github.com/PulsePipe/pulsepipe/wiki/persistence/mongodb\n\n"
                f"Fix the database connection to continue.",
                details={"database_type": "mongodb", "connection_time": elapsed, "original_error": str(e)},
                original_error=e
            )

    elif store_type == "mssql":
        raise NotImplementedError(
            "🔒 MS SQL Server bookmark tracking is available in PulsePipe Enterprise"
            "\n👉 Upgrade at https://github.com/PulsePipe/pulsepipe/wiki/enterprise"
        )

    elif store_type == "s3":
        raise NotImplementedError(
            "🔒 S3 + DynamoDB scalable bookmark store is available in PulsePilot Enterprise."
            "\n👉 Get enterprise ingestion at https://github.com/PulsePipe/pulsepipe/wiki/enterprise"
        )

    else:
        raise ValueError(f"❌ Unsupported bookmark store type: {store_type}")
