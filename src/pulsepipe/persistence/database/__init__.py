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

# src/pulsepipe/persistence/database/__init__.py

"""
Database abstraction layer for multi-backend persistence support.

This module provides database-agnostic interfaces and implementations
for SQLite, PostgreSQL, and MongoDB backends.
"""

from .connection import DatabaseConnection
from .dialect import DatabaseDialect
from .exceptions import (
    DatabaseError,
    ConnectionError,
    QueryError,
    ConfigurationError,
    TransactionError,
    SchemaError
)
from .sqlite_impl import SQLiteConnection


def init_data_intelligence_db(connection: DatabaseConnection) -> None:
    """
    Initialize the data intelligence database schema.
    
    Creates all necessary tables for pipeline tracking, ingestion statistics,
    audit events, quality metrics, and performance data.
    
    Args:
        connection: Database connection to initialize
    """
    if isinstance(connection, SQLiteConnection):
        connection.init_schema()
    else:
        # For other database types, could implement similar schema init
        # For now, only SQLite is supported for schema initialization
        raise NotImplementedError(f"Schema initialization not implemented for {type(connection).__name__}")


__all__ = [
    "DatabaseConnection",
    "DatabaseDialect", 
    "DatabaseError",
    "ConnectionError",
    "QueryError",
    "ConfigurationError",
    "TransactionError",
    "SchemaError",
    "init_data_intelligence_db"
]