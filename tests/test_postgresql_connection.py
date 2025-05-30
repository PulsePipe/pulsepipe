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

# tests/test_postgresql_connection_simple.py

"""
Simple unit tests for PostgreSQL database connection implementation.

Tests PostgreSQLDialect only since connection requires psycopg2.
"""

import pytest
import json
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock

from pulsepipe.persistence.database.postgresql_impl import PostgreSQLDialect
from pulsepipe.persistence.database.exceptions import ConfigurationError


class TestPostgreSQLDialect:
    """Test PostgreSQLDialect class."""
    
    @pytest.fixture
    def dialect(self):
        """Create a PostgreSQLDialect instance."""
        return PostgreSQLDialect()
    
    def test_get_pipeline_run_insert_sql(self, dialect):
        """Test pipeline run insert SQL uses PostgreSQL parameters."""
        sql = dialect.get_pipeline_run_insert_sql()
        
        assert "INSERT INTO pipeline_runs" in sql
        assert "%s" in sql  # PostgreSQL parameter style
        assert "?" not in sql  # SQLite parameter style
    
    def test_get_pipeline_run_update_sql(self, dialect):
        """Test pipeline run update SQL uses PostgreSQL parameters."""
        sql = dialect.get_pipeline_run_update_sql()
        
        assert "UPDATE pipeline_runs" in sql
        assert "%s" in sql
    
    def test_get_ingestion_stat_insert_sql(self, dialect):
        """Test ingestion stat insert SQL has RETURNING clause."""
        sql = dialect.get_ingestion_stat_insert_sql()
        
        assert "INSERT INTO ingestion_stats" in sql
        assert "RETURNING id" in sql
    
    def test_get_failed_record_insert_sql(self, dialect):
        """Test failed record insert SQL has RETURNING clause."""
        sql = dialect.get_failed_record_insert_sql()
        
        assert "INSERT INTO failed_records" in sql
        assert "RETURNING id" in sql
    
    def test_get_ingestion_summary_sql_with_filters(self, dialect):
        """Test ingestion summary SQL with PostgreSQL parameters."""
        start_date = datetime.now() - timedelta(days=7)
        end_date = datetime.now()
        
        sql, params = dialect.get_ingestion_summary_sql(
            pipeline_run_id="test-123",
            start_date=start_date,
            end_date=end_date
        )
        
        assert "%s" in sql
        assert len(params) == 3
    
    def test_get_auto_increment_syntax(self, dialect):
        """Test PostgreSQL auto-increment syntax."""
        syntax = dialect.get_auto_increment_syntax()
        assert "SERIAL PRIMARY KEY" in syntax
    
    def test_get_json_column_type(self, dialect):
        """Test PostgreSQL JSON column type."""
        col_type = dialect.get_json_column_type()
        assert col_type == "JSONB"
    
    def test_serialize_json(self, dialect):
        """Test JSON serialization."""
        data = {"key": "value", "number": 123}
        serialized = dialect.serialize_json(data)
        
        assert isinstance(serialized, str)
        assert "key" in serialized
    
    def test_deserialize_json_dict(self, dialect):
        """Test JSON deserialization when already a dict."""
        data = {"key": "value", "number": 123}
        result = dialect.deserialize_json(data)
        
        assert result is data  # Should return the same dict
    
    def test_escape_identifier(self, dialect):
        """Test PostgreSQL identifier escaping."""
        escaped = dialect.escape_identifier("table_name")
        assert escaped == '"table_name"'
    
    def test_supports_feature(self, dialect):
        """Test PostgreSQL feature support."""
        assert dialect.supports_feature("transactions") is True
        assert dialect.supports_feature("jsonb") is True
        assert dialect.supports_feature("arrays") is True
        assert dialect.supports_feature("connection_pooling") is True
    
    def test_get_database_type(self, dialect):
        """Test database type detection."""
        db_type = dialect.get_database_type()
        assert db_type == "postgresql"
    
    def test_get_pipeline_run_select_sql(self, dialect):
        """Test pipeline run select SQL."""
        sql = dialect.get_pipeline_run_select_sql()
        assert "SELECT" in sql
        assert "FROM pipeline_runs" in sql
        assert "WHERE id = %s" in sql
    
    def test_get_pipeline_runs_list_sql(self, dialect):
        """Test pipeline runs list SQL."""
        sql = dialect.get_pipeline_runs_list_sql()
        assert "SELECT" in sql
        assert "ORDER BY started_at DESC" in sql
        assert "LIMIT %s" in sql
    
    def test_get_audit_event_insert_sql(self, dialect):
        """Test audit event insert SQL."""
        sql = dialect.get_audit_event_insert_sql()
        assert "INSERT INTO audit_events" in sql
        assert "RETURNING id" in sql
    
    def test_get_quality_metric_insert_sql(self, dialect):
        """Test quality metric insert SQL."""
        sql = dialect.get_quality_metric_insert_sql()
        assert "INSERT INTO quality_metrics" in sql
        assert "RETURNING id" in sql
    
    def test_get_performance_metric_insert_sql(self, dialect):
        """Test performance metric insert SQL."""
        sql = dialect.get_performance_metric_insert_sql()
        assert "INSERT INTO performance_metrics" in sql
        assert "RETURNING id" in sql
    
    def test_get_ingestion_summary_sql_no_filters(self, dialect):
        """Test ingestion summary SQL without filters."""
        sql, params = dialect.get_ingestion_summary_sql()
        assert "SELECT" in sql
        assert "GROUP BY status, error_category" in sql
        assert len(params) == 0
        assert "WHERE" not in sql
    
    def test_get_ingestion_summary_sql_single_filter(self, dialect):
        """Test ingestion summary SQL with single filter."""
        sql, params = dialect.get_ingestion_summary_sql(pipeline_run_id="test-123")
        assert "WHERE pipeline_run_id = %s" in sql
        assert len(params) == 1
        assert params[0] == "test-123"
    
    def test_get_quality_summary_sql_with_filter(self, dialect):
        """Test quality summary SQL with filter."""
        sql, params = dialect.get_quality_summary_sql(pipeline_run_id="test-123")
        assert "WHERE pipeline_run_id = %s" in sql
        assert len(params) == 1
        assert params[0] == "test-123"
    
    def test_get_quality_summary_sql_no_filter(self, dialect):
        """Test quality summary SQL without filter."""
        sql, params = dialect.get_quality_summary_sql()
        assert "SELECT" in sql
        assert "AVG(" in sql
        assert len(params) == 0
        assert "WHERE" not in sql
    
    def test_get_cleanup_sql(self, dialect):
        """Test cleanup SQL statements."""
        cutoff_date = datetime.now() - timedelta(days=30)
        statements = dialect.get_cleanup_sql(cutoff_date)
        
        assert len(statements) == 7  # Should have 7 delete statements
        assert all(isinstance(stmt, tuple) and len(stmt) == 2 for stmt in statements)
        
        # Check that failed_records is deleted first
        assert "DELETE FROM failed_records" in statements[0][0]
        # Check that pipeline_runs is deleted last
        assert "DELETE FROM pipeline_runs" in statements[-1][0]
    
    def test_format_datetime(self, dialect):
        """Test datetime formatting."""
        dt = datetime(2023, 1, 15, 10, 30, 45)
        formatted = dialect.format_datetime(dt)
        assert isinstance(formatted, str)
        assert "2023-01-15" in formatted
    
    def test_parse_datetime(self, dialect):
        """Test datetime parsing."""
        dt_str = "2023-01-15T10:30:45"
        parsed = dialect.parse_datetime(dt_str)
        assert isinstance(parsed, datetime)
        assert parsed.year == 2023
        assert parsed.month == 1
        assert parsed.day == 15
    
    def test_parse_datetime_with_z(self, dialect):
        """Test datetime parsing with Z timezone."""
        dt_str = "2023-01-15T10:30:45Z"
        parsed = dialect.parse_datetime(dt_str)
        assert isinstance(parsed, datetime)
    
    def test_serialize_json_none(self, dialect):
        """Test JSON serialization with None."""
        result = dialect.serialize_json(None)
        assert result is None
    
    def test_deserialize_json_string(self, dialect):
        """Test JSON deserialization from string."""
        json_str = '{"key": "value"}'
        result = dialect.deserialize_json(json_str)
        assert result == {"key": "value"}
    
    def test_deserialize_json_none(self, dialect):
        """Test JSON deserialization with None."""
        result = dialect.deserialize_json(None)
        assert result is None
    
    def test_get_limit_syntax_with_offset(self, dialect):
        """Test LIMIT syntax with offset."""
        syntax = dialect.get_limit_syntax(10, 5)
        assert syntax == "LIMIT 10 OFFSET 5"
    
    def test_get_limit_syntax_without_offset(self, dialect):
        """Test LIMIT syntax without offset."""
        syntax = dialect.get_limit_syntax(10)
        assert syntax == "LIMIT 10"
    
    def test_supports_feature_unsupported(self, dialect):
        """Test unsupported feature."""
        assert dialect.supports_feature("unsupported_feature") is False


