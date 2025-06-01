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


def create_bookmark_store(config: dict):
    """
    Create a bookmark store based on configuration.
    
    Now supports all database backends through the unified adapter system.
    """
    # Check if we should use the new common store
    persistence_config = config.get("persistence", {})
    
    # If persistence config exists, use the common store with database adapters
    if persistence_config:
        try:
            connection = get_database_connection(config)
            dialect = get_sql_dialect(config)
            return CommonBookmarkStore(connection, dialect)
        except Exception:
            # Fall back to legacy SQLite store if there's an issue
            pass
    
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
        # Now supported through the common store
        try:
            connection = get_database_connection(config)
            dialect = get_sql_dialect(config)
            return CommonBookmarkStore(connection, dialect)
        except Exception as e:
            raise NotImplementedError(
                f"🔒 PostgreSQL bookmark store configuration error: {e}"
                "\n👉 Check your persistence configuration"
            )

    elif store_type == "mongodb":
        # Now supported through the common store
        try:
            connection = get_database_connection(config)
            dialect = get_sql_dialect(config)
            return CommonBookmarkStore(connection, dialect)
        except Exception as e:
            raise NotImplementedError(
                f"🔒 MongoDB bookmark store configuration error: {e}"
                "\n👉 Check your persistence configuration"
            )

    elif store_type == "mssql":
        raise NotImplementedError(
            "🔒 MS SQL Server bookmark tracking is available in PulsePipe Enterprise"
            "\n👉 Upgrade at https://pulsepipe.io/pilot"
        )

    elif store_type == "s3":
        raise NotImplementedError(
            "🔒 S3 + DynamoDB scalable bookmark store is available in PulsePilot Enterprise."
            "\n👉 Get enterprise ingestion at https://pulsepipe.io/pilot"
        )

    else:
        raise ValueError(f"❌ Unsupported bookmark store type: {store_type}")
