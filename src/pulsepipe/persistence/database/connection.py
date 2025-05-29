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

# src/pulsepipe/persistence/database/connection.py

"""
Database connection abstraction interface.

Provides a common interface for database connections across different backends.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Tuple
from contextlib import contextmanager


class DatabaseResult:
    """
    Represents the result of a database query.
    
    Provides a common interface for accessing query results regardless
    of the underlying database implementation.
    """
    
    def __init__(self, rows: List[Dict[str, Any]], lastrowid: Optional[int] = None, 
                 rowcount: Optional[int] = None):
        """
        Initialize query result.
        
        Args:
            rows: List of result rows as dictionaries
            lastrowid: ID of last inserted row (for INSERT operations)
            rowcount: Number of affected rows (for UPDATE/DELETE operations)
        """
        self.rows = rows
        self.lastrowid = lastrowid
        self.rowcount = rowcount
    
    def fetchone(self) -> Optional[Dict[str, Any]]:
        """Fetch the next row from the result set."""
        return self.rows[0] if self.rows else None
    
    def fetchall(self) -> List[Dict[str, Any]]:
        """Fetch all remaining rows from the result set."""
        return self.rows
    
    def fetchmany(self, size: int) -> List[Dict[str, Any]]:
        """Fetch a limited number of rows from the result set."""
        return self.rows[:size]
    
    def __iter__(self):
        """Make result iterable."""
        return iter(self.rows)
    
    def __len__(self):
        """Return number of rows in result."""
        return len(self.rows)


class DatabaseConnection(ABC):
    """
    Abstract base class for database connections.
    
    Defines the common interface that all database implementations must support.
    This abstraction allows the TrackingRepository to work with any database backend.
    """
    
    @abstractmethod
    def execute(self, query: str, params: Optional[Union[Tuple, Dict]] = None) -> DatabaseResult:
        """
        Execute a single database query.
        
        Args:
            query: SQL query or database-specific query string
            params: Query parameters (tuple for positional, dict for named)
            
        Returns:
            DatabaseResult containing query results
            
        Raises:
            QueryError: If query execution fails
        """
        pass
    
    @abstractmethod
    def executemany(self, query: str, params_list: List[Union[Tuple, Dict]]) -> DatabaseResult:
        """
        Execute a query multiple times with different parameters.
        
        Args:
            query: SQL query or database-specific query string
            params_list: List of parameter sets to execute
            
        Returns:
            DatabaseResult containing aggregated results
            
        Raises:
            QueryError: If query execution fails
        """
        pass
    
    @abstractmethod
    def commit(self) -> None:
        """
        Commit the current transaction.
        
        Raises:
            TransactionError: If commit fails
        """
        pass
    
    @abstractmethod
    def rollback(self) -> None:
        """
        Rollback the current transaction.
        
        Raises:
            TransactionError: If rollback fails
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Close the database connection.
        
        Raises:
            ConnectionError: If close operation fails
        """
        pass
    
    @abstractmethod
    def is_connected(self) -> bool:
        """
        Check if the connection is still active.
        
        Returns:
            True if connection is active, False otherwise
        """
        pass
    
    @abstractmethod
    def get_connection_info(self) -> Dict[str, Any]:
        """
        Get information about the current connection.
        
        Returns:
            Dictionary with connection details (host, database, etc.)
        """
        pass
    
    @contextmanager
    @abstractmethod
    def transaction(self):
        """
        Context manager for database transactions.
        
        Automatically commits on success or rolls back on exception.
        
        Usage:
            with connection.transaction():
                connection.execute("INSERT INTO ...", params)
                connection.execute("UPDATE ...", params)
        """
        pass
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - close connection."""
        self.close()