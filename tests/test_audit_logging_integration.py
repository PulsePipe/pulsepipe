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

# tests/test_audit_logging_integration.py

"""
Tests for audit logging integration in pipeline execution.

Verifies that audit logging is properly initialized and records audit events
when pipelines are executed with auditing enabled.
"""

import pytest
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, MagicMock

from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.persistence.database.sqlite_impl import SQLiteConnection
from pulsepipe.persistence.tracking_repository import TrackingRepository


@pytest.fixture
def temp_db_path():
    """Create a temporary database file for testing."""
    with tempfile.NamedTemporaryFile(suffix='.sqlite3', delete=False) as f:
        temp_path = f.name
    yield temp_path
    # Cleanup
    if os.path.exists(temp_path):
        os.unlink(temp_path)


@pytest.fixture
def test_config_with_audit():
    """Provide a test configuration with audit logging enabled."""
    return {
        "adapter": {
            "type": "file_watcher",
            "watch_path": "incoming/test"
        },
        "ingester": {
            "type": "fhir",
            "version": "R4"
        },
        "persistence": {
            "type": "sqlite",
            "sqlite": {
                "db_path": ":memory:",
                "timeout": 30.0
            }
        },
        "data_intelligence": {
            "enabled": True,
            "performance_mode": "standard",
            "features": {
                "audit_trail": {
                    "enabled": True,
                    "detail_level": "standard",
                    "record_level_tracking": True,
                    "structured_errors": True
                },
                "ingestion_tracking": {
                    "enabled": True,
                    "store_failed_records": True
                }
            }
        }
    }


@pytest.fixture
def test_config_without_audit():
    """Provide a test configuration with audit logging disabled."""
    return {
        "adapter": {
            "type": "file_watcher",
            "watch_path": "incoming/test"
        },
        "ingester": {
            "type": "fhir",
            "version": "R4"
        },
        "persistence": {
            "type": "sqlite",
            "sqlite": {
                "db_path": ":memory:",
                "timeout": 30.0
            }
        },
        "data_intelligence": {
            "enabled": False,
            "features": {
                "audit_trail": {
                    "enabled": False
                }
            }
        }
    }


class TestAuditLoggingIntegration:
    """Test suite for audit logging integration."""
    
    def test_pipeline_context_initializes_audit_logging_when_enabled(self, test_config_with_audit):
        """Test that PipelineContext properly initializes audit logging when enabled."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Verify that audit logging components were initialized
        assert context.audit_logger is not None, "AuditLogger should be initialized when enabled"
        assert context.tracking_repository is not None, "TrackingRepository should be initialized when enabled"
        assert context.data_intelligence_config is not None, "DataIntelligenceConfig should be stored when enabled"
        
        # Verify database connection is established
        assert context.tracking_repository.conn is not None
        assert context.tracking_repository.conn.is_connected()
    
    def test_pipeline_context_skips_audit_logging_when_disabled(self, test_config_without_audit):
        """Test that PipelineContext skips audit logging when disabled."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_without_audit,
            summary=True
        )
        
        # Verify that audit logging components were not initialized
        assert context.audit_logger is None, "AuditLogger should be None when disabled"
        assert context.tracking_repository is None, "TrackingRepository should be None when disabled"
    
    def test_stage_lifecycle_logging(self, test_config_with_audit):
        """Test that stage lifecycle events are logged to audit trail."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Mock the audit logger to verify calls
        with patch.object(context.audit_logger, 'log_stage_started') as mock_started, \
             patch.object(context.audit_logger, 'log_stage_completed') as mock_completed:
            
            # Test stage lifecycle
            context.start_stage("ingestion")
            context.end_stage("ingestion", result=["test_result"])
            
            # Verify audit logging calls
            mock_started.assert_called_once_with("ingestion")
            mock_completed.assert_called_once()
    
    def test_error_logging(self, test_config_with_audit):
        """Test that errors are logged to audit trail."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Mock the audit logger to verify calls
        with patch.object(context.audit_logger, 'log_error') as mock_error:
            
            # Add an error
            context.add_error("ingestion", "Test error message", {"detail": "test"})
            
            # Verify audit logging call
            mock_error.assert_called_once_with("ingestion", "Test error message", {"detail": "test"})
    
    def test_warning_logging(self, test_config_with_audit):
        """Test that warnings are logged to audit trail."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Mock the audit logger to verify calls
        with patch.object(context.audit_logger, 'log_warning') as mock_warning:
            
            # Add a warning
            context.add_warning("ingestion", "Test warning message", {"detail": "test"})
            
            # Verify audit logging call
            mock_warning.assert_called_once_with("ingestion", "Test warning message", {"detail": "test"})
    
    def test_pipeline_completion_logging(self, test_config_with_audit):
        """Test that pipeline completion is logged to audit trail."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Mock the audit logger and tracking repository
        with patch.object(context.audit_logger, 'log_pipeline_completed') as mock_completed, \
             patch.object(context.tracking_repository, 'complete_pipeline_run') as mock_complete_run:
            
            # Get summary (which triggers completion logging)
            summary = context.get_summary()
            
            # Verify audit logging calls
            mock_completed.assert_called_once()
            mock_complete_run.assert_called_once()
            
            # Verify summary structure
            assert "pipeline_id" in summary
            assert "total_duration" in summary
            assert summary["error_count"] == 0
    
    def test_pipeline_failure_logging(self, test_config_with_audit):
        """Test that pipeline failure is logged to audit trail."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Add an error to make pipeline fail
        context.add_error("ingestion", "Test failure")
        
        # Mock the audit logger and tracking repository
        with patch.object(context.audit_logger, 'log_pipeline_failed') as mock_failed, \
             patch.object(context.tracking_repository, 'complete_pipeline_run') as mock_complete_run:
            
            # Get summary (which triggers completion logging)
            summary = context.get_summary()
            
            # Verify failure logging calls
            mock_failed.assert_called_once()
            mock_complete_run.assert_called_once_with(
                run_id=context.pipeline_id,
                status="failed",
                error_message="Pipeline completed with 1 errors"
            )
            
            # Verify summary structure
            assert summary["error_count"] == 1
    
    def test_audit_logging_graceful_failure(self, temp_db_path):
        """Test that pipeline continues gracefully if audit logging fails to initialize."""
        # Create a config with invalid database path to force failure
        invalid_config = {
            "adapter": {"type": "file_watcher"},
            "ingester": {"type": "fhir"},
            "persistence": {
                "type": "sqlite",
                "sqlite": {
                    "db_path": "/invalid/path/database.sqlite3",
                    "timeout": 30.0
                }
            },
            "data_intelligence": {
                "enabled": True,
                "features": {
                    "audit_trail": {"enabled": True}
                }
            }
        }
        
        # This should not raise an exception, but should log a warning
        context = PipelineContext(
            name="test_pipeline",
            config=invalid_config,
            summary=True
        )
        
        # Verify that audit logging was disabled due to initialization failure
        assert context.audit_logger is None
        assert context.tracking_repository is None
    
    def test_database_schema_initialization(self, test_config_with_audit):
        """Test that database schema is properly initialized."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Verify that the tracking repository has a working database connection
        assert context.tracking_repository is not None
        assert context.tracking_repository.conn.is_connected()
        
        # Test that we can query the database (schema should be initialized)
        result = context.tracking_repository.conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        table_names = [row['name'] for row in result.rows]
        
        # Verify that audit tables exist
        expected_tables = ['pipeline_runs', 'audit_events', 'ingestion_stats', 'quality_metrics']
        for table in expected_tables:
            assert table in table_names, f"Table {table} should exist in database schema"
    
    def test_pipeline_run_tracking(self, test_config_with_audit):
        """Test that pipeline runs are properly tracked in the database."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Verify that a pipeline run was started
        pipeline_run = context.tracking_repository.get_pipeline_run(context.pipeline_id)
        assert pipeline_run is not None
        assert pipeline_run.name == "test_pipeline"
        assert pipeline_run.status == "running"
        
        # Complete the pipeline
        summary = context.get_summary()
        
        # Verify that the pipeline run was completed
        pipeline_run = context.tracking_repository.get_pipeline_run(context.pipeline_id)
        assert pipeline_run.status == "completed"
        assert pipeline_run.completed_at is not None
    
    def test_ingestion_tracker_creation(self, test_config_with_audit):
        """Test that ingestion tracker is created when ingestion stage starts."""
        context = PipelineContext(
            name="test_pipeline",
            config=test_config_with_audit,
            summary=True
        )
        
        # Initially no ingestion tracker should exist
        assert context.get_ingestion_tracker("ingestion") is None
        
        # Start ingestion stage
        context.start_stage("ingestion")
        
        # Now ingestion tracker should be created
        ingestion_tracker = context.get_ingestion_tracker("ingestion")
        assert ingestion_tracker is not None
        assert ingestion_tracker.stage_name == "ingestion"
        assert ingestion_tracker.pipeline_run_id == context.pipeline_id