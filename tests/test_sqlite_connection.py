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

# tests/test_sqlite_connection.py

"""
Unit tests for SQLite database connection implementation.

Tests SQLiteConnection and SQLiteDialect classes.
"""

import pytest
import sqlite3
import tempfile
import os
import json
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch, MagicMock

from pulsepipe.persistence.database.sqlite_impl import SQLiteConnection, SQLiteDialect
from pulsepipe.persistence.database.connection import DatabaseResult
from pulsepipe.persistence.database.exceptions import (
    ConnectionError,
    QueryError,
    TransactionError
)


class TestSQLiteConnection:
    """Test SQLiteConnection class."""
    
    @pytest.fixture
    def temp_db_path(self):
        """Create a temporary database file path."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        os.unlink(path)  # Remove the file, we just want the path
        yield path
        # Cleanup
        if os.path.exists(path):
            os.unlink(path)
    
    @pytest.fixture
    def sqlite_conn(self, temp_db_path):
        """Create a SQLiteConnection instance."""
        conn = SQLiteConnection(db_path=temp_db_path)
        yield conn
        conn.close()
    
    def test_init_creates_directory(self):
        """Test that initialization creates parent directories."""
        with tempfile.TemporaryDirectory() as temp_dir:
            db_path = os.path.join(temp_dir, "subdir", "test.db")
            
            conn = SQLiteConnection(db_path=db_path)
            
            assert os.path.exists(db_path)
            assert conn.is_connected()
            
            conn.close()
    
    def test_init_with_timeout(self, temp_db_path):
        """Test initialization with custom timeout."""
        conn = SQLiteConnection(db_path=temp_db_path, timeout=60.0)
        
        assert conn.timeout == 60.0
        assert conn.is_connected()
        
        conn.close()
    
    def test_init_connection_error(self):
        """Test initialization with database connection error."""
        # Mock both path creation and sqlite3.connect to raise errors
        with patch('pathlib.Path.mkdir') as mock_mkdir, \
             patch('sqlite3.connect', side_effect=sqlite3.Error("Connection failed")):
            with pytest.raises(ConnectionError):
                SQLiteConnection(db_path="/invalid/path/test.db")
    
    def test_execute_simple_query(self, sqlite_conn):
        """Test executing a simple query."""
        result = sqlite_conn.execute("SELECT 1 as value")
        
        assert isinstance(result, DatabaseResult)
        assert len(result.rows) == 1
        assert result.rows[0]["value"] == 1
    
    def test_execute_with_params_tuple(self, sqlite_conn):
        """Test executing query with tuple parameters."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        sqlite_conn.execute("INSERT INTO test (id, name) VALUES (?, ?)", (1, "test"))
        
        result = sqlite_conn.execute("SELECT * FROM test WHERE id = ?", (1,))
        
        assert len(result.rows) == 1
        assert result.rows[0]["id"] == 1
        assert result.rows[0]["name"] == "test"
    
    def test_execute_with_params_dict(self, sqlite_conn):
        """Test executing query with dict parameters."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        sqlite_conn.execute(
            "INSERT INTO test (id, name) VALUES (:id, :name)", 
            {"id": 1, "name": "test"}
        )
        
        result = sqlite_conn.execute("SELECT * FROM test WHERE id = :id", {"id": 1})
        
        assert len(result.rows) == 1
        assert result.rows[0]["name"] == "test"
    
    def test_execute_insert_returns_lastrowid(self, sqlite_conn):
        """Test that INSERT operations return lastrowid."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER PRIMARY KEY, name TEXT)")
        result = sqlite_conn.execute("INSERT INTO test (name) VALUES (?)", ("test",))
        
        assert result.lastrowid is not None
        assert result.lastrowid > 0
        assert result.rowcount == 1
    
    def test_execute_invalid_sql(self, sqlite_conn):
        """Test executing invalid SQL raises QueryError."""
        with pytest.raises(QueryError):
            sqlite_conn.execute("INVALID SQL STATEMENT")
    
    def test_execute_no_connection(self, temp_db_path):
        """Test executing query without connection raises error."""
        conn = SQLiteConnection(db_path=temp_db_path)
        conn.close()
        
        with pytest.raises(ConnectionError):
            conn.execute("SELECT 1")
    
    def test_executemany(self, sqlite_conn):
        """Test executemany method."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER, name TEXT)")
        
        params_list = [(1, "test1"), (2, "test2"), (3, "test3")]
        result = sqlite_conn.executemany(
            "INSERT INTO test (id, name) VALUES (?, ?)", 
            params_list
        )
        
        assert result.rowcount == 3
        
        # Verify all records were inserted
        check_result = sqlite_conn.execute("SELECT COUNT(*) as count FROM test")
        assert check_result.rows[0]["count"] == 3
    
    def test_executemany_error(self, sqlite_conn):
        """Test executemany with invalid SQL."""
        with pytest.raises(QueryError):
            sqlite_conn.executemany("INVALID SQL", [(1,), (2,)])
    
    def test_commit(self, sqlite_conn):
        """Test commit method."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER)")
        sqlite_conn.execute("INSERT INTO test (id) VALUES (1)")
        
        # Should not raise an exception
        sqlite_conn.commit()
    
    def test_commit_no_connection(self, temp_db_path):
        """Test commit without connection raises error."""
        conn = SQLiteConnection(db_path=temp_db_path)
        conn.close()
        
        with pytest.raises(ConnectionError):
            conn.commit()
    
    def test_rollback(self, sqlite_conn):
        """Test rollback method."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER)")
        sqlite_conn.execute("INSERT INTO test (id) VALUES (1)")
        
        # Should not raise an exception
        sqlite_conn.rollback()
    
    def test_rollback_no_connection(self, temp_db_path):
        """Test rollback without connection raises error."""
        conn = SQLiteConnection(db_path=temp_db_path)
        conn.close()
        
        with pytest.raises(ConnectionError):
            conn.rollback()
    
    def test_close(self, sqlite_conn):
        """Test close method."""
        assert sqlite_conn.is_connected()
        
        sqlite_conn.close()
        
        assert not sqlite_conn.is_connected()
    
    def test_close_error_handling(self, temp_db_path):
        """Test close with connection error."""
        conn = SQLiteConnection(db_path=temp_db_path)
        
        # Store reference to real connection before mocking
        real_connection = conn._connection
        
        # Mock the connection to raise an error on close
        mock_connection = MagicMock()
        mock_connection.close.side_effect = sqlite3.Error("Close error")
        conn._connection = mock_connection
        
        try:
            with pytest.raises(ConnectionError):
                conn.close()
        finally:
            # Ensure real connection is closed for Windows
            try:
                real_connection.close()
            except:
                pass
    
    def test_is_connected_true(self, sqlite_conn):
        """Test is_connected returns True for active connection."""
        assert sqlite_conn.is_connected() is True
    
    def test_is_connected_false_no_connection(self, temp_db_path):
        """Test is_connected returns False when no connection."""
        conn = SQLiteConnection(db_path=temp_db_path)
        
        # Store reference to real connection before nullifying it
        real_connection = conn._connection
        
        try:
            conn._connection = None
            assert conn.is_connected() is False
        finally:
            # Ensure real connection is closed for Windows
            try:
                if real_connection:
                    real_connection.close()
            except:
                pass
    
    def test_is_connected_false_broken_connection(self, sqlite_conn):
        """Test is_connected returns False for broken connection."""
        # Close the underlying connection but don't update the wrapper
        sqlite_conn._connection.close()
        
        assert sqlite_conn.is_connected() is False
    
    def test_get_connection_info(self, sqlite_conn):
        """Test get_connection_info method."""
        info = sqlite_conn.get_connection_info()
        
        assert isinstance(info, dict)
        assert info["database_type"] == "sqlite"
        assert "db_path" in info
        assert "timeout" in info
        assert "is_connected" in info
        assert info["is_connected"] is True
    
    def test_transaction_success(self, sqlite_conn):
        """Test successful transaction context manager."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER)")
        
        with sqlite_conn.transaction():
            sqlite_conn.execute("INSERT INTO test (id) VALUES (1)")
            sqlite_conn.execute("INSERT INTO test (id) VALUES (2)")
        
        # Verify transaction was committed
        result = sqlite_conn.execute("SELECT COUNT(*) as count FROM test")
        assert result.rows[0]["count"] == 2
    
    def test_transaction_rollback_on_exception(self, sqlite_conn):
        """Test transaction rollback on exception."""
        sqlite_conn.execute("CREATE TABLE test (id INTEGER)")
        sqlite_conn.execute("INSERT INTO test (id) VALUES (1)")
        sqlite_conn.commit()
        
        with pytest.raises(ValueError):
            with sqlite_conn.transaction():
                sqlite_conn.execute("INSERT INTO test (id) VALUES (2)")
                raise ValueError("Test exception")
        
        # Verify transaction was rolled back
        result = sqlite_conn.execute("SELECT COUNT(*) as count FROM test")
        assert result.rows[0]["count"] == 1
    
    def test_transaction_no_connection(self, temp_db_path):
        """Test transaction context manager without connection."""
        conn = SQLiteConnection(db_path=temp_db_path)
        conn.close()
        
        with pytest.raises(ConnectionError):
            with conn.transaction():
                pass
    
    def test_get_raw_connection(self, sqlite_conn):
        """Test get_raw_connection method."""
        raw_conn = sqlite_conn.get_raw_connection()
        
        assert isinstance(raw_conn, sqlite3.Connection)
        assert raw_conn is sqlite_conn._connection
    
    def test_get_raw_connection_no_connection(self, temp_db_path):
        """Test get_raw_connection without connection."""
        conn = SQLiteConnection(db_path=temp_db_path)
        conn.close()
        
        with pytest.raises(ConnectionError):
            conn.get_raw_connection()
    
    def test_context_manager(self, temp_db_path):
        """Test SQLiteConnection as context manager."""
        with SQLiteConnection(db_path=temp_db_path) as conn:
            assert conn.is_connected()
            result = conn.execute("SELECT 1")
            assert len(result.rows) == 1
        
        # Connection should be closed after exiting context
        assert not conn.is_connected()


class TestSQLiteDialect:
    """Test SQLiteDialect class."""
    
    @pytest.fixture
    def dialect(self):
        """Create a SQLiteDialect instance."""
        return SQLiteDialect()
    
    def test_get_pipeline_run_insert(self, dialect):
        """Test pipeline run insert SQL generation."""
        sql = dialect.get_pipeline_run_insert()
        
        assert "INSERT INTO pipeline_runs" in sql
        assert "id, name, started_at, status, config_snapshot" in sql
        assert "VALUES (?, ?, ?, ?, ?)" in sql
    
    def test_get_pipeline_run_update(self, dialect):
        """Test pipeline run update SQL generation."""
        sql = dialect.get_pipeline_run_update()
        
        assert "UPDATE pipeline_runs" in sql
        assert "completed_at = ?" in sql
        assert "WHERE id = ?" in sql
    
    def test_get_pipeline_run_select(self, dialect):
        """Test pipeline run select SQL generation."""
        sql = dialect.get_pipeline_run_select()
        
        assert "SELECT" in sql
        assert "FROM pipeline_runs" in sql
        assert "WHERE id = ?" in sql
    
    def test_get_pipeline_runs_list(self, dialect):
        """Test pipeline runs list SQL generation."""
        sql = dialect.get_pipeline_runs_list()
        
        assert "SELECT" in sql
        assert "FROM pipeline_runs" in sql
        assert "ORDER BY started_at DESC" in sql
        assert "LIMIT ?" in sql
    
    def test_get_ingestion_stat_insert(self, dialect):
        """Test ingestion stat insert SQL generation."""
        sql = dialect.get_ingestion_stat_insert()
        
        assert "INSERT INTO ingestion_stats" in sql
        assert "pipeline_run_id" in sql
        assert "stage_name" in sql
    
    def test_get_failed_record_insert(self, dialect):
        """Test failed record insert SQL generation."""
        sql = dialect.get_failed_record_insert()
        
        assert "INSERT INTO failed_records" in sql
        assert "ingestion_stat_id" in sql
        assert "original_data" in sql
    
    def test_get_audit_event_insert(self, dialect):
        """Test audit event insert SQL generation."""
        sql = dialect.get_audit_event_insert()
        
        assert "INSERT INTO audit_events" in sql
        assert "pipeline_run_id" in sql
        assert "event_type" in sql
    
    def test_get_quality_metric_insert(self, dialect):
        """Test quality metric insert SQL generation."""
        sql = dialect.get_quality_metric_insert()
        
        assert "INSERT INTO quality_metrics" in sql
        assert "pipeline_run_id" in sql
        assert "completeness_score" in sql
    
    def test_get_performance_metric_insert(self, dialect):
        """Test performance metric insert SQL generation."""
        sql = dialect.get_performance_metric_insert()
        
        assert "INSERT INTO performance_metrics" in sql
        assert "pipeline_run_id" in sql
        assert "stage_name" in sql
    
    def test_get_ingestion_summary_no_filters(self, dialect):
        """Test ingestion summary SQL without filters."""
        sql, params = dialect.get_ingestion_summary()
        
        assert "SELECT" in sql
        assert "FROM ingestion_stats" in sql
        assert "GROUP BY status, error_category" in sql
        assert len(params) == 0
    
    def test_get_ingestion_summary_with_filters(self, dialect):
        """Test ingestion summary SQL with filters."""
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
        
        sql, params = dialect.get_ingestion_summary(
            pipeline_run_id="test-123",
            start_date=start_date,
            end_date=end_date
        )
        
        assert "WHERE" in sql
        assert "pipeline_run_id = ?" in sql
        assert "timestamp >= ?" in sql
        assert "timestamp <= ?" in sql
        assert len(params) == 3
        assert params[0] == "test-123"
    
    def test_get_quality_summary_no_filter(self, dialect):
        """Test quality summary SQL without filter."""
        sql, params = dialect.get_quality_summary()
        
        assert "SELECT" in sql
        assert "FROM quality_metrics" in sql
        assert "AVG(completeness_score)" in sql
        assert len(params) == 0
    
    def test_get_quality_summary_with_filter(self, dialect):
        """Test quality summary SQL with filter."""
        sql, params = dialect.get_quality_summary(pipeline_run_id="test-123")
        
        assert "WHERE pipeline_run_id = ?" in sql
        assert len(params) == 1
        assert params[0] == "test-123"
    
    def test_get_cleanup(self, dialect):
        """Test cleanup SQL generation."""
        cutoff_date = datetime.now() - timedelta(days=30)
        statements = dialect.get_cleanup(cutoff_date)
        
        assert isinstance(statements, list)
        assert len(statements) > 0
        
        # Check that statements are in correct order (dependencies first)
        sql_texts = [stmt[0] for stmt in statements]
        failed_records_idx = next(i for i, sql in enumerate(sql_texts) if "failed_records" in sql)
        pipeline_runs_idx = next(i for i, sql in enumerate(sql_texts) if "DELETE FROM pipeline_runs" in sql)
        
        assert failed_records_idx < pipeline_runs_idx
    
    def test_format_datetime(self, dialect):
        """Test datetime formatting."""
        dt = datetime(2023, 1, 15, 10, 30, 45)
        formatted = dialect.format_datetime(dt)
        
        assert isinstance(formatted, str)
        assert "2023-01-15" in formatted
        assert "10:30:45" in formatted
    
    def test_parse_datetime(self, dialect):
        """Test datetime parsing."""
        dt_str = "2023-01-15T10:30:45"
        parsed = dialect.parse_datetime(dt_str)
        
        assert isinstance(parsed, datetime)
        assert parsed.year == 2023
        assert parsed.month == 1
        assert parsed.day == 15
        assert parsed.hour == 10
        assert parsed.minute == 30
        assert parsed.second == 45
    
    def test_get_auto_increment_syntax(self, dialect):
        """Test auto-increment syntax."""
        syntax = dialect.get_auto_increment_syntax()
        assert "INTEGER PRIMARY KEY AUTOINCREMENT" in syntax
    
    def test_get_json_column_type(self, dialect):
        """Test JSON column type."""
        col_type = dialect.get_json_column_type()
        assert col_type == "TEXT"
    
    def test_serialize_json(self, dialect):
        """Test JSON serialization."""
        data = {"key": "value", "number": 123}
        serialized = dialect.serialize_json(data)
        
        assert isinstance(serialized, str)
        assert "key" in serialized
        assert "value" in serialized
    
    def test_serialize_json_none(self, dialect):
        """Test JSON serialization with None."""
        serialized = dialect.serialize_json(None)
        assert serialized is None
    
    def test_deserialize_json(self, dialect):
        """Test JSON deserialization."""
        json_str = '{"key": "value", "number": 123}'
        data = dialect.deserialize_json(json_str)
        
        assert isinstance(data, dict)
        assert data["key"] == "value"
        assert data["number"] == 123
    
    def test_deserialize_json_none(self, dialect):
        """Test JSON deserialization with None."""
        data = dialect.deserialize_json(None)
        assert data is None
    
    def test_escape_identifier(self, dialect):
        """Test identifier escaping."""
        escaped = dialect.escape_identifier("table_name")
        assert escaped == '"table_name"'
    
    def test_get_limit_syntax_limit_only(self, dialect):
        """Test LIMIT syntax with limit only."""
        syntax = dialect.get_limit_syntax(10)
        assert syntax == "LIMIT 10"
    
    def test_get_limit_syntax_with_offset(self, dialect):
        """Test LIMIT syntax with offset."""
        syntax = dialect.get_limit_syntax(10, 20)
        assert syntax == "LIMIT 10 OFFSET 20"
    
    def test_supports_feature(self, dialect):
        """Test feature support checking."""
        assert dialect.supports_feature("transactions") is True
        assert dialect.supports_feature("basic_sql") is True
        assert dialect.supports_feature("foreign_keys") is True
        assert dialect.supports_feature("json_extract") is True
        assert dialect.supports_feature("full_text_search") is True
        assert dialect.supports_feature("nonexistent_feature") is False
    
    def test_get_database_type(self, dialect):
        """Test database type detection."""
        db_type = dialect.get_database_type()
        assert db_type == "sqlite"