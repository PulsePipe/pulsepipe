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

# tests/test_persistence_models.py

"""
Unit tests for persistence models and database schema.

Tests database schema creation, validation, and initialization
for the data intelligence tracking system.
"""

import pytest
import sqlite3
import tempfile
import os
from datetime import datetime
from typing import Dict, Any

from pulsepipe.persistence.models import (
    DataIntelligenceSchema,
    ProcessingStatus,
    ErrorCategory,
    init_data_intelligence_db
)


class TestProcessingStatus:
    """Test ProcessingStatus enum."""
    
    def test_status_values(self):
        """Test all status values are defined correctly."""
        assert ProcessingStatus.SUCCESS == "success"
        assert ProcessingStatus.FAILURE == "failure"
        assert ProcessingStatus.PARTIAL == "partial"
        assert ProcessingStatus.SKIPPED == "skipped"
    
    def test_status_enum_membership(self):
        """Test status values are proper enum members."""
        statuses = list(ProcessingStatus)
        assert len(statuses) == 4
        assert ProcessingStatus.SUCCESS in statuses
        assert ProcessingStatus.FAILURE in statuses
        assert ProcessingStatus.PARTIAL in statuses
        assert ProcessingStatus.SKIPPED in statuses


class TestErrorCategory:
    """Test ErrorCategory enum."""
    
    def test_error_category_values(self):
        """Test all error category values are defined correctly."""
        assert ErrorCategory.SCHEMA_ERROR == "schema_error"
        assert ErrorCategory.VALIDATION_ERROR == "validation_error"
        assert ErrorCategory.PARSE_ERROR == "parse_error"
        assert ErrorCategory.TRANSFORMATION_ERROR == "transformation_error"
        assert ErrorCategory.SYSTEM_ERROR == "system_error"
        assert ErrorCategory.DATA_QUALITY_ERROR == "data_quality_error"
        assert ErrorCategory.NETWORK_ERROR == "network_error"
        assert ErrorCategory.PERMISSION_ERROR == "permission_error"
    
    def test_error_category_enum_membership(self):
        """Test error categories are proper enum members."""
        categories = list(ErrorCategory)
        assert len(categories) == 8
        assert ErrorCategory.SCHEMA_ERROR in categories
        assert ErrorCategory.VALIDATION_ERROR in categories
        assert ErrorCategory.PARSE_ERROR in categories


class TestDataIntelligenceSchema:
    """Test DataIntelligenceSchema class."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        conn = sqlite3.connect(path)
        yield conn
        
        conn.close()
        os.unlink(path)
    
    @pytest.fixture
    def schema(self, temp_db):
        """Create a DataIntelligenceSchema instance."""
        return DataIntelligenceSchema(temp_db)
    
    def test_init(self, temp_db):
        """Test schema initialization."""
        schema = DataIntelligenceSchema(temp_db)
        assert schema.conn == temp_db
        
        # Check foreign keys are enabled
        cursor = temp_db.execute("PRAGMA foreign_keys")
        result = cursor.fetchone()
        assert result[0] == 1  # Foreign keys enabled
    
    def test_create_tables(self, schema, temp_db):
        """Test table creation."""
        schema.create_tables()
        
        # Check all tables were created
        cursor = temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        )
        tables = {row[0] for row in cursor.fetchall()}
        
        expected_tables = {
            'pipeline_runs',
            'ingestion_stats',
            'failed_records',
            'audit_events',
            'quality_metrics',
            'performance_metrics',
            'system_metrics'
        }
        
        assert expected_tables.issubset(tables)
    
    def test_create_tables_idempotent(self, schema, temp_db):
        """Test that create_tables can be called multiple times safely."""
        schema.create_tables()
        schema.create_tables()  # Should not raise an error
        
        # Verify tables still exist
        cursor = temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        assert 'pipeline_runs' in tables
        assert 'ingestion_stats' in tables
    
    def test_pipeline_runs_table_structure(self, schema, temp_db):
        """Test pipeline_runs table structure."""
        schema.create_tables()
        
        cursor = temp_db.execute("PRAGMA table_info(pipeline_runs)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}  # name: type
        
        expected_columns = {
            'id': 'TEXT',
            'name': 'TEXT',
            'started_at': 'TIMESTAMP',
            'completed_at': 'TIMESTAMP',
            'status': 'TEXT',
            'total_records': 'INTEGER',
            'successful_records': 'INTEGER',
            'failed_records': 'INTEGER',
            'skipped_records': 'INTEGER',
            'config_snapshot': 'TEXT',
            'error_message': 'TEXT',
            'created_at': 'TIMESTAMP',
            'updated_at': 'TIMESTAMP'
        }
        
        for col, col_type in expected_columns.items():
            assert col in columns
            assert columns[col] == col_type
    
    def test_ingestion_stats_table_structure(self, schema, temp_db):
        """Test ingestion_stats table structure."""
        schema.create_tables()
        
        cursor = temp_db.execute("PRAGMA table_info(ingestion_stats)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        expected_columns = {
            'id': 'INTEGER',
            'pipeline_run_id': 'TEXT',
            'stage_name': 'TEXT',
            'file_path': 'TEXT',
            'record_id': 'TEXT',
            'record_type': 'TEXT',
            'status': 'TEXT',
            'error_category': 'TEXT',
            'error_message': 'TEXT',
            'error_details': 'TEXT',
            'processing_time_ms': 'INTEGER',
            'record_size_bytes': 'INTEGER',
            'data_source': 'TEXT',
            'timestamp': 'TIMESTAMP'
        }
        
        for col, col_type in expected_columns.items():
            assert col in columns
            assert columns[col] == col_type
    
    def test_failed_records_table_structure(self, schema, temp_db):
        """Test failed_records table structure."""
        schema.create_tables()
        
        cursor = temp_db.execute("PRAGMA table_info(failed_records)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        expected_columns = {
            'id': 'INTEGER',
            'ingestion_stat_id': 'INTEGER',
            'original_data': 'TEXT',
            'normalized_data': 'TEXT',
            'failure_reason': 'TEXT',
            'stack_trace': 'TEXT',
            'retry_count': 'INTEGER',
            'last_retry_at': 'TIMESTAMP',
            'resolved_at': 'TIMESTAMP',
            'resolution_notes': 'TEXT',
            'created_at': 'TIMESTAMP'
        }
        
        for col, col_type in expected_columns.items():
            assert col in columns
            assert columns[col] == col_type
    
    def test_audit_events_table_structure(self, schema, temp_db):
        """Test audit_events table structure."""
        schema.create_tables()
        
        cursor = temp_db.execute("PRAGMA table_info(audit_events)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        expected_columns = {
            'id': 'INTEGER',
            'pipeline_run_id': 'TEXT',
            'event_type': 'TEXT',
            'stage_name': 'TEXT',
            'record_id': 'TEXT',
            'event_level': 'TEXT',
            'message': 'TEXT',
            'details': 'TEXT',
            'user_context': 'TEXT',
            'system_context': 'TEXT',
            'correlation_id': 'TEXT',
            'timestamp': 'TIMESTAMP'
        }
        
        for col, col_type in expected_columns.items():
            assert col in columns
            assert columns[col] == col_type
    
    def test_quality_metrics_table_structure(self, schema, temp_db):
        """Test quality_metrics table structure."""
        schema.create_tables()
        
        cursor = temp_db.execute("PRAGMA table_info(quality_metrics)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        expected_columns = {
            'id': 'INTEGER',
            'pipeline_run_id': 'TEXT',
            'record_id': 'TEXT',
            'record_type': 'TEXT',
            'completeness_score': 'REAL',
            'consistency_score': 'REAL',
            'validity_score': 'REAL',
            'accuracy_score': 'REAL',
            'overall_score': 'REAL',
            'missing_fields': 'TEXT',
            'invalid_fields': 'TEXT',
            'outlier_fields': 'TEXT',
            'quality_issues': 'TEXT',
            'metrics_details': 'TEXT',
            'sampled': 'BOOLEAN',
            'timestamp': 'TIMESTAMP'
        }
        
        for col, col_type in expected_columns.items():
            assert col in columns
            assert columns[col] == col_type
    
    def test_performance_metrics_table_structure(self, schema, temp_db):
        """Test performance_metrics table structure."""
        schema.create_tables()
        
        cursor = temp_db.execute("PRAGMA table_info(performance_metrics)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        expected_columns = {
            'id': 'INTEGER',
            'pipeline_run_id': 'TEXT',
            'stage_name': 'TEXT',
            'started_at': 'TIMESTAMP',
            'completed_at': 'TIMESTAMP',
            'duration_ms': 'INTEGER',
            'records_processed': 'INTEGER',
            'records_per_second': 'REAL',
            'memory_usage_mb': 'REAL',
            'cpu_usage_percent': 'REAL',
            'disk_io_bytes': 'INTEGER',
            'network_io_bytes': 'INTEGER',
            'bottleneck_indicator': 'TEXT',
            'optimization_suggestions': 'TEXT'
        }
        
        for col, col_type in expected_columns.items():
            assert col in columns
            assert columns[col] == col_type
    
    def test_system_metrics_table_structure(self, schema, temp_db):
        """Test system_metrics table structure."""
        schema.create_tables()
        
        cursor = temp_db.execute("PRAGMA table_info(system_metrics)")
        columns = {row[1]: row[2] for row in cursor.fetchall()}
        
        expected_columns = {
            'id': 'INTEGER',
            'pipeline_run_id': 'TEXT',
            'hostname': 'TEXT',
            'os_name': 'TEXT',
            'os_version': 'TEXT',
            'python_version': 'TEXT',
            'cpu_model': 'TEXT',
            'cpu_cores': 'INTEGER',
            'cpu_threads': 'INTEGER',
            'memory_total_gb': 'REAL',
            'memory_available_gb': 'REAL',
            'disk_total_gb': 'REAL',
            'disk_free_gb': 'REAL',
            'gpu_available': 'BOOLEAN',
            'gpu_model': 'TEXT',
            'gpu_memory_gb': 'REAL',
            'network_interfaces': 'TEXT',
            'environment_variables': 'TEXT',
            'package_versions': 'TEXT',
            'timestamp': 'TIMESTAMP'
        }
        
        for col, col_type in expected_columns.items():
            assert col in columns
            assert columns[col] == col_type
    
    def test_indexes_created(self, schema, temp_db):
        """Test that indexes are created properly."""
        schema.create_tables()
        
        # Get all indexes
        cursor = temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_%'"
        )
        indexes = {row[0] for row in cursor.fetchall()}
        
        # Check some key indexes exist
        expected_indexes = {
            'idx_pipeline_runs_name',
            'idx_pipeline_runs_status',
            'idx_ingestion_stats_pipeline_run',
            'idx_ingestion_stats_status',
            'idx_audit_events_pipeline_run',
            'idx_quality_metrics_pipeline_run',
            'idx_performance_metrics_pipeline_run'
        }
        
        assert expected_indexes.issubset(indexes)
    
    def test_foreign_key_constraints(self, schema, temp_db):
        """Test foreign key constraints work."""
        schema.create_tables()
        
        # Try to insert into child table without parent - should fail
        with pytest.raises(sqlite3.IntegrityError):
            temp_db.execute("""
                INSERT INTO ingestion_stats (pipeline_run_id, stage_name, status)
                VALUES ('nonexistent', 'test', 'success')
            """)
            temp_db.commit()
    
    def test_drop_tables(self, schema, temp_db):
        """Test dropping all tables."""
        schema.create_tables()
        
        # Verify tables exist
        cursor = temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables_before = {row[0] for row in cursor.fetchall()}
        assert 'pipeline_runs' in tables_before
        
        schema.drop_tables()
        
        # Verify tables are gone
        cursor = temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('pipeline_runs', 'ingestion_stats')"
        )
        tables_after = {row[0] for row in cursor.fetchall()}
        assert len(tables_after) == 0
    
    def test_get_table_info(self, schema, temp_db):
        """Test getting table information."""
        schema.create_tables()
        
        table_info = schema.get_table_info()
        
        assert isinstance(table_info, dict)
        assert 'pipeline_runs' in table_info
        assert 'ingestion_stats' in table_info
        
        # Check pipeline_runs table info
        pipeline_cols = table_info['pipeline_runs']
        assert isinstance(pipeline_cols, list)
        assert len(pipeline_cols) > 0
        
        # Check column structure
        id_col = next(col for col in pipeline_cols if col['name'] == 'id')
        assert id_col['type'] == 'TEXT'
        assert id_col['primary_key'] is True
    
    def test_validate_schema_valid(self, schema, temp_db):
        """Test schema validation with valid schema."""
        schema.create_tables()
        
        assert schema.validate_schema() is True
    
    def test_validate_schema_missing_tables(self, schema, temp_db):
        """Test schema validation with missing tables."""
        # Don't create tables
        assert schema.validate_schema() is False
    
    def test_validate_schema_incomplete_table(self, schema, temp_db):
        """Test schema validation with incomplete table structure."""
        # Create an incomplete pipeline_runs table
        temp_db.execute("""
            CREATE TABLE pipeline_runs (
                id TEXT PRIMARY KEY,
                name TEXT
            )
        """)
        temp_db.commit()
        
        assert schema.validate_schema() is False
    
    def test_validate_schema_exception_handling(self, schema, temp_db):
        """Test schema validation handles exceptions gracefully."""
        # Close the connection to cause an exception
        temp_db.close()
        
        assert schema.validate_schema() is False


class TestInitDataIntelligenceDb:
    """Test init_data_intelligence_db function."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database for testing."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        conn = sqlite3.connect(path)
        yield conn
        
        conn.close()
        os.unlink(path)
    
    def test_init_new_database(self, temp_db):
        """Test initializing a new database."""
        schema = init_data_intelligence_db(temp_db)
        
        assert isinstance(schema, DataIntelligenceSchema)
        assert schema.conn == temp_db
        
        # Check tables were created
        cursor = temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        assert 'pipeline_runs' in tables
        assert 'ingestion_stats' in tables
    
    def test_init_existing_database(self, temp_db):
        """Test initializing database that already has schema."""
        # Initialize once
        schema1 = init_data_intelligence_db(temp_db)
        
        # Initialize again - should not fail
        schema2 = init_data_intelligence_db(temp_db)
        
        assert isinstance(schema2, DataIntelligenceSchema)
        
        # Tables should still exist
        cursor = temp_db.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row[0] for row in cursor.fetchall()}
        assert 'pipeline_runs' in tables


class TestDataIntegrity:
    """Test data integrity and constraints."""
    
    @pytest.fixture
    def temp_db(self):
        """Create a temporary database with schema."""
        fd, path = tempfile.mkstemp(suffix='.db')
        os.close(fd)
        
        conn = sqlite3.connect(path)
        schema = init_data_intelligence_db(conn)
        
        yield conn
        
        conn.close()
        os.unlink(path)
    
    def test_pipeline_run_insertion(self, temp_db):
        """Test basic pipeline run insertion."""
        run_id = "test-run-123"
        temp_db.execute("""
            INSERT INTO pipeline_runs (id, name, started_at, status)
            VALUES (?, ?, ?, ?)
        """, (run_id, "test_pipeline", datetime.now(), "running"))
        temp_db.commit()
        
        # Verify insertion
        cursor = temp_db.execute("SELECT id, name FROM pipeline_runs WHERE id = ?", (run_id,))
        row = cursor.fetchone()
        assert row[0] == run_id
        assert row[1] == "test_pipeline"
    
    def test_ingestion_stats_with_foreign_key(self, temp_db):
        """Test ingestion stats insertion with valid foreign key."""
        run_id = "test-run-123"
        
        # Insert parent record
        temp_db.execute("""
            INSERT INTO pipeline_runs (id, name, started_at, status)
            VALUES (?, ?, ?, ?)
        """, (run_id, "test_pipeline", datetime.now(), "running"))
        
        # Insert child record
        temp_db.execute("""
            INSERT INTO ingestion_stats (pipeline_run_id, stage_name, status, timestamp)
            VALUES (?, ?, ?, ?)
        """, (run_id, "ingestion", "success", datetime.now()))
        temp_db.commit()
        
        # Verify both records exist
        cursor = temp_db.execute("""
            SELECT pr.name, ist.stage_name 
            FROM pipeline_runs pr 
            JOIN ingestion_stats ist ON pr.id = ist.pipeline_run_id
            WHERE pr.id = ?
        """, (run_id,))
        row = cursor.fetchone()
        assert row[0] == "test_pipeline"
        assert row[1] == "ingestion"
    
    def test_cascade_deletion_behavior(self, temp_db):
        """Test that foreign key constraints work as expected."""
        run_id = "test-run-123"
        
        # Insert parent and child records
        temp_db.execute("""
            INSERT INTO pipeline_runs (id, name, started_at, status)
            VALUES (?, ?, ?, ?)
        """, (run_id, "test_pipeline", datetime.now(), "running"))
        
        temp_db.execute("""
            INSERT INTO ingestion_stats (pipeline_run_id, stage_name, status, timestamp)
            VALUES (?, ?, ?, ?)
        """, (run_id, "ingestion", "success", datetime.now()))
        temp_db.commit()
        
        # Verify child record exists
        cursor = temp_db.execute("SELECT COUNT(*) FROM ingestion_stats WHERE pipeline_run_id = ?", (run_id,))
        assert cursor.fetchone()[0] == 1
        
        # Try to delete parent - should fail due to foreign key constraint
        with pytest.raises(sqlite3.IntegrityError):
            temp_db.execute("DELETE FROM pipeline_runs WHERE id = ?", (run_id,))
            temp_db.commit()
    
    def test_json_data_storage(self, temp_db):
        """Test storing and retrieving JSON data."""
        run_id = "test-run-123"
        config_data = {"param1": "value1", "param2": {"nested": "value"}}
        
        temp_db.execute("""
            INSERT INTO pipeline_runs (id, name, started_at, status, config_snapshot)
            VALUES (?, ?, ?, ?, ?)
        """, (run_id, "test_pipeline", datetime.now(), "running", str(config_data)))
        temp_db.commit()
        
        # Retrieve and verify
        cursor = temp_db.execute("SELECT config_snapshot FROM pipeline_runs WHERE id = ?", (run_id,))
        stored_config = cursor.fetchone()[0]
        assert stored_config == str(config_data)
    
    def test_timestamp_handling(self, temp_db):
        """Test timestamp storage and retrieval."""
        run_id = "test-run-123"
        start_time = datetime.now()
        
        temp_db.execute("""
            INSERT INTO pipeline_runs (id, name, started_at, status)
            VALUES (?, ?, ?, ?)
        """, (run_id, "test_pipeline", start_time, "running"))
        temp_db.commit()
        
        # Retrieve and verify timestamp
        cursor = temp_db.execute("SELECT started_at FROM pipeline_runs WHERE id = ?", (run_id,))
        stored_time = cursor.fetchone()[0]
        
        # SQLite stores timestamps as strings, so compare string representations
        assert str(start_time) == stored_time or start_time.isoformat() in stored_time