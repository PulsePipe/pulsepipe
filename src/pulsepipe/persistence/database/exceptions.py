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

# src/pulsepipe/persistence/database/exceptions.py

"""
Database abstraction layer exception hierarchy.

Provides common exception types for database operations across all backends.
"""

from typing import Optional, Dict, Any


class DatabaseError(Exception):
    """Base exception for all database-related errors."""
    
    def __init__(self, message: str, details: Optional[Dict[str, Any]] = None, 
                 original_error: Optional[Exception] = None):
        """
        Initialize database error.
        
        Args:
            message: Human-readable error message
            details: Optional additional error details
            original_error: Original exception that caused this error
        """
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.original_error = original_error


class ConnectionError(DatabaseError):
    """Raised when database connection fails or is lost."""
    pass


class QueryError(DatabaseError):
    """Raised when a database query fails."""
    pass


class TransactionError(DatabaseError):
    """Raised when transaction operations fail."""
    pass


class SchemaError(DatabaseError):
    """Raised when schema operations fail."""
    pass


class ConfigurationError(DatabaseError):
    """Raised when database configuration is invalid."""
    pass


class NotSupportedError(DatabaseError):
    """Raised when an operation is not supported by the database backend."""
    pass


def wrap_database_error(original_error: Exception, message: str, 
                       details: Optional[Dict[str, Any]] = None) -> DatabaseError:
    """
    Wrap a database-specific exception into our common exception hierarchy.
    
    Args:
        original_error: The original database-specific exception
        message: Human-readable error message
        details: Optional additional error details
        
    Returns:
        Appropriate DatabaseError subclass
    """
    error_type = type(original_error).__name__.lower()
    
    # Map common database errors to our exception types
    if any(term in error_type for term in ['connection', 'network', 'timeout']):
        return ConnectionError(message, details, original_error)
    elif any(term in error_type for term in ['transaction', 'commit', 'rollback']):
        return TransactionError(message, details, original_error)
    elif any(term in error_type for term in ['schema', 'table', 'column']):
        return SchemaError(message, details, original_error)
    elif any(term in error_type for term in ['syntax', 'query', 'sql']):
        return QueryError(message, details, original_error)
    else:
        return DatabaseError(message, details, original_error)