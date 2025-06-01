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

# tests/test_database_connection_abc.py

"""
Unit tests for database connection abstract base classes.

Tests the core database abstraction interfaces and result classes.
"""

import pytest
from unittest.mock import Mock, MagicMock
from typing import Dict, Any, List

from pulsepipe.persistence.database.connection import DatabaseConnection, DatabaseResult
from pulsepipe.persistence.database.dialect import DatabaseDialect


class TestDatabaseResult:
    """Test DatabaseResult class."""
    
    def test_init_basic(self):
        """Test basic initialization."""
        rows = [{"id": 1, "name": "test"}]
        result = DatabaseResult(rows=rows, lastrowid=123, rowcount=1)
        
        assert result.rows == rows
        assert result.lastrowid == 123
        assert result.rowcount == 1
    
    def test_init_minimal(self):
        """Test initialization with minimal parameters."""
        rows = []
        result = DatabaseResult(rows=rows)
        
        assert result.rows == []
        assert result.lastrowid is None
        assert result.rowcount is None
    
    def test_fetchone_with_data(self):
        """Test fetchone with data available."""
        rows = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        result = DatabaseResult(rows=rows)
        
        first_row = result.fetchone()
        assert first_row == {"id": 1, "name": "test"}
    
    def test_fetchone_empty(self):
        """Test fetchone with no data."""
        result = DatabaseResult(rows=[])
        
        first_row = result.fetchone()
        assert first_row is None
    
    def test_fetchall(self):
        """Test fetchall method."""
        rows = [{"id": 1, "name": "test"}, {"id": 2, "name": "test2"}]
        result = DatabaseResult(rows=rows)
        
        all_rows = result.fetchall()
        assert all_rows == rows
    
    def test_fetchmany(self):
        """Test fetchmany method."""
        rows = [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]
        result = DatabaseResult(rows=rows)
        
        limited_rows = result.fetchmany(2)
        assert limited_rows == [{"id": 1}, {"id": 2}]
    
    def test_fetchmany_more_than_available(self):
        """Test fetchmany requesting more rows than available."""
        rows = [{"id": 1}, {"id": 2}]
        result = DatabaseResult(rows=rows)
        
        limited_rows = result.fetchmany(5)
        assert limited_rows == rows
    
    def test_iter(self):
        """Test result is iterable."""
        rows = [{"id": 1}, {"id": 2}]
        result = DatabaseResult(rows=rows)
        
        iterated_rows = list(result)
        assert iterated_rows == rows
    
    def test_len(self):
        """Test len() works on result."""
        rows = [{"id": 1}, {"id": 2}, {"id": 3}]
        result = DatabaseResult(rows=rows)
        
        assert len(result) == 3
    
    def test_len_empty(self):
        """Test len() on empty result."""
        result = DatabaseResult(rows=[])
        assert len(result) == 0


class TestDatabaseConnectionABC:
    """Test DatabaseConnection abstract base class."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that DatabaseConnection cannot be instantiated directly."""
        with pytest.raises(TypeError):
            DatabaseConnection()
    
    def test_abstract_methods_defined(self):
        """Test that all required abstract methods are defined."""
        required_methods = [
            'execute',
            'executemany', 
            'commit',
            'rollback',
            'close',
            'is_connected',
            'get_connection_info',
            'transaction'
        ]
        
        for method in required_methods:
            assert hasattr(DatabaseConnection, method)
            assert callable(getattr(DatabaseConnection, method))
    
    def test_context_manager_protocol(self):
        """Test that DatabaseConnection implements context manager protocol."""
        assert hasattr(DatabaseConnection, '__enter__')
        assert hasattr(DatabaseConnection, '__exit__')


class TestDatabaseDialectABC:
    """Test DatabaseDialect abstract base class."""
    
    def test_cannot_instantiate_abstract_class(self):
        """Test that DatabaseDialect cannot be instantiated directly."""
        with pytest.raises(TypeError):
            DatabaseDialect()
    
    def test_abstract_methods_defined(self):
        """Test that all required abstract methods are defined."""
        required_methods = [
            'get_pipeline_run_insert',
            'get_pipeline_run_update',
            'get_pipeline_run_select',
            'get_pipeline_runs_list',
            'get_ingestion_stat_insert',
            'get_failed_record_insert',
            'get_audit_event_insert',
            'get_quality_metric_insert',
            'get_performance_metric_insert',
            'get_ingestion_summary',
            'get_quality_summary',
            'get_cleanup',
            'format_datetime',
            'parse_datetime',
            'get_auto_increment_syntax',
            'get_json_column_type',
            'serialize_json',
            'deserialize_json',
            'escape_identifier',
            'get_limit_syntax'
        ]
        
        for method in required_methods:
            assert hasattr(DatabaseDialect, method)
            assert callable(getattr(DatabaseDialect, method))
    
    def test_concrete_methods(self):
        """Test concrete methods that have default implementations."""
        # Create a mock subclass to test concrete methods
        class MockDialect(DatabaseDialect):
            def get_pipeline_run_insert(self): return ""
            def get_pipeline_run_update(self): return ""
            def get_pipeline_run_select(self): return ""
            def get_pipeline_runs_list(self): return ""
            def get_pipeline_run_count_update(self): return ""  # ADDED: Missing method
            def get_recent_pipeline_runs(self, limit: int = 10): return ""  # ADDED: Missing method
            def get_ingestion_stat_insert(self): return ""
            def get_failed_record_insert(self): return ""
            def get_audit_event_insert(self): return ""
            def get_quality_metric_insert(self): return ""
            def get_performance_metric_insert(self): return ""
            def get_ingestion_summary(self, *args): return "", []
            def get_quality_summary(self, *args): return "", []
            def get_cleanup(self, *args): return []
            def format_datetime(self, *args): return ""
            def parse_datetime(self, *args): return None
            def get_auto_increment_syntax(self): return ""
            def get_json_column_type(self): return ""
            def serialize_json(self, *args): return ""
            def deserialize_json(self, *args): return None
            def escape_identifier(self, *args): return ""
            def get_limit_syntax(self, *args): return ""

        dialect = MockDialect()

        # Test get_database_type
        db_type = dialect.get_database_type()
        assert db_type == "mock"

        # Test supports_feature
        assert dialect.supports_feature("transactions") is True
        assert dialect.supports_feature("nonexistent_feature") is False


class ConcreteDatabaseConnection(DatabaseConnection):
    """Concrete implementation for testing."""
    
    def __init__(self):
        self.executed_queries = []
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.connected = True
    
    def execute(self, query, params=None):
        self.executed_queries.append((query, params))
        return DatabaseResult([{"test": "result"}], 123, 1)
    
    def executemany(self, query, params_list):
        for params in params_list:
            self.executed_queries.append((query, params))
        return DatabaseResult([], None, len(params_list))
    
    def commit(self):
        self.committed = True
    
    def rollback(self):
        self.rolled_back = True
    
    def close(self):
        self.closed = True
        self.connected = False
    
    def is_connected(self):
        return self.connected
    
    def get_connection_info(self):
        return {"database_type": "test", "connected": self.connected}
    
    def transaction(self):
        return self


class TestConcreteDatabaseConnection:
    """Test concrete database connection implementation."""
    
    def test_concrete_implementation(self):
        """Test that concrete implementation works."""
        conn = ConcreteDatabaseConnection()
        
        # Test execute
        result = conn.execute("SELECT 1", (1, 2))
        assert isinstance(result, DatabaseResult)
        assert len(conn.executed_queries) == 1
        assert conn.executed_queries[0] == ("SELECT 1", (1, 2))
        
        # Test executemany
        result = conn.executemany("INSERT INTO test VALUES (?)", [(1,), (2,), (3,)])
        assert result.rowcount == 3
        assert len(conn.executed_queries) == 4  # 1 from execute + 3 from executemany
        
        # Test commit
        conn.commit()
        assert conn.committed is True
        
        # Test rollback
        conn.rollback()
        assert conn.rolled_back is True
        
        # Test is_connected
        assert conn.is_connected() is True
        
        # Test get_connection_info
        info = conn.get_connection_info()
        assert info["database_type"] == "test"
        assert info["connected"] is True
        
        # Test close
        conn.close()
        assert conn.closed is True
        assert conn.is_connected() is False
    
    def test_context_manager(self):
        """Test context manager functionality."""
        conn = ConcreteDatabaseConnection()
        
        with conn as context_conn:
            assert context_conn is conn
            assert conn.closed is False
        
        # After exiting context, connection should be closed
        assert conn.closed is True