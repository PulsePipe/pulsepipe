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

# tests/test_audit_logger.py

"""
Unit tests for audit logging infrastructure.

Tests audit logger functionality, event handling,
correlation tracking, and persistence integration.
"""

import pytest
import uuid
import json
from datetime import datetime
from unittest.mock import Mock, MagicMock

from pulsepipe.audit.audit_logger import (
    AuditLogger,
    AuditEvent,
    AuditLevel,
    EventType
)
from pulsepipe.config.data_intelligence_config import DataIntelligenceConfig
from pulsepipe.persistence.models import ErrorCategory


class TestAuditLevel:
    """Test AuditLevel enum."""
    
    def test_audit_levels(self):
        """Test all audit level values."""
        assert AuditLevel.DEBUG == "DEBUG"
        assert AuditLevel.INFO == "INFO"
        assert AuditLevel.WARNING == "WARNING"
        assert AuditLevel.ERROR == "ERROR"
        assert AuditLevel.CRITICAL == "CRITICAL"
    
    def test_audit_level_membership(self):
        """Test audit level enum membership."""
        levels = list(AuditLevel)
        assert len(levels) == 5
        assert AuditLevel.DEBUG in levels
        assert AuditLevel.CRITICAL in levels


class TestEventType:
    """Test EventType enum."""
    
    def test_event_types(self):
        """Test key event type values."""
        assert EventType.PIPELINE_STARTED == "pipeline_started"
        assert EventType.RECORD_PROCESSED == "record_processed"
        assert EventType.ERROR_OCCURRED == "error_occurred"
        assert EventType.VALIDATION_FAILED == "validation_failed"
    
    def test_event_type_completeness(self):
        """Test that all expected event types are defined."""
        expected_types = [
            "pipeline_started", "pipeline_completed", "pipeline_failed",
            "stage_started", "stage_completed", "stage_failed",
            "record_processed", "record_failed", "record_skipped",
            "validation_passed", "validation_failed",
            "error_occurred", "warning_issued"
        ]
        
        actual_types = [et.value for et in EventType]
        for expected in expected_types:
            assert expected in actual_types


class TestAuditEvent:
    """Test AuditEvent dataclass."""
    
    def test_basic_creation(self):
        """Test basic AuditEvent creation."""
        event = AuditEvent(
            event_type=EventType.RECORD_PROCESSED,
            stage_name="ingestion",
            message="Record processed successfully"
        )
        
        assert event.event_type == EventType.RECORD_PROCESSED
        assert event.stage_name == "ingestion"
        assert event.message == "Record processed successfully"
        assert event.level == AuditLevel.INFO  # Default
        assert event.record_id is None
        assert event.correlation_id is None
        assert event.details is None
        assert event.timestamp is not None  # Auto-generated
    
    def test_full_creation(self):
        """Test AuditEvent with all fields."""
        details = {"processing_time": 150, "record_size": 1024}
        user_context = {"user_id": "test_user"}
        system_context = {"hostname": "test_host"}
        timestamp = datetime.now()
        
        event = AuditEvent(
            event_type=EventType.RECORD_FAILED,
            stage_name="validation",
            message="Record validation failed",
            level=AuditLevel.ERROR,
            record_id="rec_123",
            correlation_id="corr_456",
            details=details,
            user_context=user_context,
            system_context=system_context,
            timestamp=timestamp
        )
        
        assert event.event_type == EventType.RECORD_FAILED
        assert event.level == AuditLevel.ERROR
        assert event.record_id == "rec_123"
        assert event.correlation_id == "corr_456"
        assert event.details == details
        assert event.user_context == user_context
        assert event.system_context == system_context
        assert event.timestamp == timestamp
    
    def test_to_dict(self):
        """Test AuditEvent to_dict conversion."""
        event = AuditEvent(
            event_type=EventType.PIPELINE_STARTED,
            stage_name="pipeline",
            message="Pipeline started",
            details={"config_version": "1.0"}
        )
        
        event_dict = event.to_dict()
        
        assert isinstance(event_dict, dict)
        assert event_dict['event_type'] == EventType.PIPELINE_STARTED
        assert event_dict['stage_name'] == "pipeline"
        assert event_dict['message'] == "Pipeline started"
        assert event_dict['level'] == AuditLevel.INFO
        assert event_dict['details'] == {"config_version": "1.0"}
    
    def test_to_json(self):
        """Test AuditEvent to_json conversion."""
        event = AuditEvent(
            event_type=EventType.ERROR_OCCURRED,
            stage_name="processing",
            message="An error occurred"
        )
        
        json_str = event.to_json()
        
        assert isinstance(json_str, str)
        parsed = json.loads(json_str)
        assert parsed['event_type'] == EventType.ERROR_OCCURRED.value
        assert parsed['stage_name'] == "processing"
        assert parsed['message'] == "An error occurred"
        assert 'timestamp' in parsed
    
    def test_timestamp_auto_generation(self):
        """Test automatic timestamp generation."""
        before = datetime.now()
        event = AuditEvent(
            event_type=EventType.RECORD_PROCESSED,
            stage_name="test",
            message="Test message"
        )
        after = datetime.now()
        
        assert before <= event.timestamp <= after
    
    def test_timestamp_preservation(self):
        """Test that provided timestamp is preserved."""
        custom_time = datetime(2023, 1, 1, 12, 0, 0)
        event = AuditEvent(
            event_type=EventType.RECORD_PROCESSED,
            stage_name="test",
            message="Test message",
            timestamp=custom_time
        )
        
        assert event.timestamp == custom_time


class TestAuditLogger:
    """Test AuditLogger class."""
    
    @pytest.fixture
    def mock_config(self):
        """Create mock configuration."""
        config = Mock(spec=DataIntelligenceConfig)
        config.is_feature_enabled.return_value = True
        return config
    
    @pytest.fixture
    def mock_repository(self):
        """Create mock tracking repository."""
        return Mock()
    
    @pytest.fixture
    def audit_logger(self, mock_config, mock_repository):
        """Create AuditLogger instance."""
        return AuditLogger("test_run_123", mock_config, mock_repository)
    
    def test_initialization(self, mock_config, mock_repository):
        """Test AuditLogger initialization."""
        logger = AuditLogger("test_run_123", mock_config, mock_repository)
        
        assert logger.pipeline_run_id == "test_run_123"
        assert logger.config == mock_config
        assert logger.repository == mock_repository
        assert logger.correlation_stack == []
        assert logger.event_buffer == []
        assert logger.auto_flush_threshold == 100
    
    def test_enabled_configuration(self, mock_config, mock_repository):
        """Test logger behavior when audit trail is enabled."""
        mock_config.is_feature_enabled.side_effect = lambda feature, sub_feature=None: {
            ('audit_trail', None): True,
            ('audit_trail', 'record_level_tracking'): True,
            ('audit_trail', 'structured_errors'): True
        }.get((feature, sub_feature), False)
        
        logger = AuditLogger("test_run", mock_config, mock_repository)
        
        assert logger.enabled is True
        assert logger.record_level_tracking is True
        assert logger.structured_errors is True
    
    def test_disabled_configuration(self, mock_repository):
        """Test logger behavior when audit trail is disabled."""
        mock_config = Mock(spec=DataIntelligenceConfig)
        mock_config.is_feature_enabled.return_value = False
        
        logger = AuditLogger("test_run", mock_config, mock_repository)
        
        assert logger.enabled is False
        assert logger.record_level_tracking is False
        assert logger.structured_errors is False
    
    def test_is_enabled(self, audit_logger):
        """Test is_enabled method."""
        assert audit_logger.is_enabled() is True
        
        audit_logger.enabled = False
        assert audit_logger.is_enabled() is False
    
    def test_correlation_context(self, audit_logger):
        """Test correlation context manager."""
        assert audit_logger.get_current_correlation_id() is None
        
        with audit_logger.correlation_context("test_corr_123") as corr_id:
            assert corr_id == "test_corr_123"
            assert audit_logger.get_current_correlation_id() == "test_corr_123"
        
        assert audit_logger.get_current_correlation_id() is None
    
    def test_correlation_context_auto_generation(self, audit_logger):
        """Test correlation context with auto-generated ID."""
        with audit_logger.correlation_context() as corr_id:
            assert corr_id is not None
            assert len(corr_id) == 8  # UUID prefix
            assert audit_logger.get_current_correlation_id() == corr_id
    
    def test_nested_correlation_context(self, audit_logger):
        """Test nested correlation contexts."""
        with audit_logger.correlation_context("outer") as outer_id:
            assert audit_logger.get_current_correlation_id() == "outer"
            
            with audit_logger.correlation_context("inner") as inner_id:
                assert audit_logger.get_current_correlation_id() == "inner"
            
            assert audit_logger.get_current_correlation_id() == "outer"
        
        assert audit_logger.get_current_correlation_id() is None
    
    def test_log_event_enabled(self, audit_logger, mock_repository):
        """Test log_event when logging is enabled."""
        event = AuditEvent(
            event_type=EventType.RECORD_PROCESSED,
            stage_name="test",
            message="Test event"
        )
        
        audit_logger.log_event(event)
        
        # Check event was added to buffer
        assert len(audit_logger.event_buffer) == 1
        assert audit_logger.event_buffer[0] == event
        
        # Check repository was called
        mock_repository.record_audit_event.assert_called_once()
    
    def test_log_event_disabled(self, mock_config, mock_repository):
        """Test log_event when logging is disabled."""
        mock_config.is_feature_enabled.return_value = False
        logger = AuditLogger("test_run", mock_config, mock_repository)
        
        event = AuditEvent(
            event_type=EventType.RECORD_PROCESSED,
            stage_name="test",
            message="Test event"
        )
        
        logger.log_event(event)
        
        # Check event was not added to buffer
        assert len(logger.event_buffer) == 0
        
        # Check repository was not called
        mock_repository.record_audit_event.assert_not_called()
    
    def test_log_event_with_correlation(self, audit_logger):
        """Test log_event with correlation context."""
        event = AuditEvent(
            event_type=EventType.RECORD_PROCESSED,
            stage_name="test",
            message="Test event"
        )
        
        with audit_logger.correlation_context("test_corr"):
            audit_logger.log_event(event)
        
        # Check correlation ID was set
        logged_event = audit_logger.event_buffer[0]
        assert logged_event.correlation_id == "test_corr"
    
    def test_log_event_repository_error(self, audit_logger, mock_repository):
        """Test log_event handles repository errors gracefully."""
        mock_repository.record_audit_event.side_effect = Exception("Database error")
        
        event = AuditEvent(
            event_type=EventType.RECORD_PROCESSED,
            stage_name="test",
            message="Test event"
        )
        
        # Should not raise exception
        audit_logger.log_event(event)
        
        # Event should still be in buffer
        assert len(audit_logger.event_buffer) == 1
    
    def test_auto_flush_buffer(self, audit_logger):
        """Test automatic buffer flushing."""
        audit_logger.auto_flush_threshold = 3
        
        # Add events up to threshold
        for i in range(3):
            event = AuditEvent(
                event_type=EventType.RECORD_PROCESSED,
                stage_name="test",
                message=f"Event {i}"
            )
            audit_logger.log_event(event)
        
        # Buffer should be flushed
        assert len(audit_logger.event_buffer) == 0
    
    def test_log_pipeline_started(self, audit_logger):
        """Test log_pipeline_started method."""
        details = {"config_version": "1.0"}
        audit_logger.log_pipeline_started("pipeline", details)
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.PIPELINE_STARTED
        assert event.stage_name == "pipeline"
        assert event.level == AuditLevel.INFO
        assert event.details == details
        assert "Pipeline started" in event.message
    
    def test_log_pipeline_completed(self, audit_logger):
        """Test log_pipeline_completed method."""
        audit_logger.log_pipeline_completed("pipeline")
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.PIPELINE_COMPLETED
        assert event.level == AuditLevel.INFO
        assert "completed successfully" in event.message
    
    def test_log_pipeline_failed(self, audit_logger):
        """Test log_pipeline_failed method."""
        error = ValueError("Test error")
        audit_logger.log_pipeline_failed("pipeline", error)
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.PIPELINE_FAILED
        assert event.level == AuditLevel.ERROR
        assert "Test error" in event.message
        assert event.details["error_type"] == "ValueError"
        assert event.details["error_message"] == "Test error"
    
    def test_log_stage_started(self, audit_logger):
        """Test log_stage_started method."""
        audit_logger.log_stage_started("ingestion")
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.STAGE_STARTED
        assert event.stage_name == "ingestion"
        assert event.level == AuditLevel.INFO
    
    def test_log_stage_completed(self, audit_logger):
        """Test log_stage_completed method."""
        audit_logger.log_stage_completed("ingestion")
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.STAGE_COMPLETED
        assert event.stage_name == "ingestion"
        assert event.level == AuditLevel.INFO
    
    def test_log_stage_failed(self, audit_logger):
        """Test log_stage_failed method."""
        error = RuntimeError("Stage error")
        audit_logger.log_stage_failed("validation", error)
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.STAGE_FAILED
        assert event.stage_name == "validation"
        assert event.level == AuditLevel.ERROR
        assert "Stage error" in event.message
    
    def test_log_record_processed(self, audit_logger):
        """Test log_record_processed method."""
        audit_logger.log_record_processed(
            "ingestion", "rec_123", "Patient", 150, {"field_count": 25}
        )
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.RECORD_PROCESSED
        assert event.record_id == "rec_123"
        assert event.level == AuditLevel.DEBUG
        assert event.details["record_type"] == "Patient"
        assert event.details["processing_time_ms"] == 150
    
    def test_log_record_processed_disabled(self, mock_config, mock_repository):
        """Test log_record_processed when record-level tracking is disabled."""
        mock_config.is_feature_enabled.side_effect = lambda feature, sub_feature=None: {
            ('audit_trail', None): True,
            ('audit_trail', 'record_level_tracking'): False
        }.get((feature, sub_feature), False)
        
        logger = AuditLogger("test_run", mock_config, mock_repository)
        logger.log_record_processed("ingestion", "rec_123")
        
        # Should not log anything
        assert len(logger.event_buffer) == 0
    
    def test_log_record_failed(self, audit_logger):
        """Test log_record_failed method."""
        error = ValueError("Invalid data")
        audit_logger.log_record_failed(
            "validation", "rec_123", error, ErrorCategory.VALIDATION_ERROR
        )
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.RECORD_FAILED
        assert event.record_id == "rec_123"
        assert event.level == AuditLevel.WARNING
        assert event.details["error_category"] == ErrorCategory.VALIDATION_ERROR.value
    
    def test_log_record_skipped(self, audit_logger):
        """Test log_record_skipped method."""
        audit_logger.log_record_skipped("filtering", "rec_123", "Duplicate record")
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.RECORD_SKIPPED
        assert event.record_id == "rec_123"
        assert event.level == AuditLevel.INFO
        assert event.details["skip_reason"] == "Duplicate record"
    
    def test_log_validation_failed(self, audit_logger):
        """Test log_validation_failed method."""
        errors = ["Missing patient ID", "Invalid date format"]
        audit_logger.log_validation_failed("validation", "rec_123", errors)
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.VALIDATION_FAILED
        assert event.record_id == "rec_123"
        assert event.level == AuditLevel.WARNING
        assert event.details["validation_errors"] == errors
        assert event.details["error_count"] == 2
    
    def test_log_data_quality_check(self, audit_logger):
        """Test log_data_quality_check method."""
        issues = ["Missing phone number"]
        audit_logger.log_data_quality_check("quality", "rec_123", 0.65, issues)
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.DATA_QUALITY_CHECK
        assert event.record_id == "rec_123"
        assert event.level == AuditLevel.WARNING  # Low score triggers warning
        assert event.details["quality_score"] == 0.65
        assert event.details["quality_issues"] == issues
    
    def test_log_data_quality_check_high_score(self, audit_logger):
        """Test log_data_quality_check with high score."""
        audit_logger.log_data_quality_check("quality", "rec_123", 0.85, [])
        
        event = audit_logger.event_buffer[0]
        assert event.level == AuditLevel.INFO  # High score is info level
    
    def test_log_performance_metric(self, audit_logger):
        """Test log_performance_metric method."""
        audit_logger.log_performance_metric("ingestion", "records_per_second", 125.5, "rec/sec")
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.PERFORMANCE_METRIC
        assert event.level == AuditLevel.DEBUG
        assert event.details["metric_name"] == "records_per_second"
        assert event.details["metric_value"] == 125.5
        assert event.details["unit"] == "rec/sec"
    
    def test_log_warning(self, audit_logger):
        """Test log_warning method."""
        audit_logger.log_warning("processing", "Data quality below threshold", "rec_123")
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.WARNING_ISSUED
        assert event.level == AuditLevel.WARNING
        assert event.record_id == "rec_123"
        assert "Data quality below threshold" in event.message
    
    def test_log_error(self, audit_logger):
        """Test log_error method."""
        error = RuntimeError("Processing error")
        audit_logger.log_error("processing", error, "rec_123")
        
        event = audit_logger.event_buffer[0]
        assert event.event_type == EventType.ERROR_OCCURRED
        assert event.level == AuditLevel.ERROR
        assert event.record_id == "rec_123"
        assert event.details["error_type"] == "RuntimeError"
    
    def test_get_events_no_filter(self, audit_logger):
        """Test get_events without filters."""
        # Add different types of events
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_record_processed("ingestion", "rec_1")
        audit_logger.log_warning("processing", "Warning message")
        
        events = audit_logger.get_events()
        assert len(events) == 3
    
    def test_get_events_with_filters(self, audit_logger):
        """Test get_events with filters."""
        # Add different types of events
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_record_processed("ingestion", "rec_1")
        audit_logger.log_warning("processing", "Warning message")
        audit_logger.log_error("processing", ValueError("Error"))
        
        # Filter by event type
        warning_events = audit_logger.get_events(event_type=EventType.WARNING_ISSUED)
        assert len(warning_events) == 1
        assert warning_events[0].event_type == EventType.WARNING_ISSUED
        
        # Filter by level
        error_events = audit_logger.get_events(level=AuditLevel.ERROR)
        assert len(error_events) == 1
        assert error_events[0].level == AuditLevel.ERROR
        
        # Filter by stage
        processing_events = audit_logger.get_events(stage_name="processing")
        assert len(processing_events) == 2
    
    def test_get_event_count(self, audit_logger):
        """Test get_event_count method."""
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_warning("processing", "Warning 1")
        audit_logger.log_warning("processing", "Warning 2")
        
        assert audit_logger.get_event_count() == 3
        assert audit_logger.get_event_count(level=AuditLevel.WARNING) == 2
        assert audit_logger.get_event_count(event_type=EventType.PIPELINE_STARTED) == 1
    
    def test_flush_buffer(self, audit_logger):
        """Test flush_buffer method."""
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_record_processed("ingestion", "rec_1")
        
        assert len(audit_logger.event_buffer) == 2
        
        audit_logger.flush_buffer()
        
        assert len(audit_logger.event_buffer) == 0
    
    def test_export_events_json(self, audit_logger, safe_tmp_path):
        """Test export_events in JSON format."""
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_record_processed("ingestion", "rec_1")
        
        export_path = safe_tmp_path / "events.json"
        audit_logger.export_events(str(export_path), format="json")
        
        assert export_path.exists()
        
        # Verify content
        with open(export_path) as f:
            data = json.load(f)
        
        assert len(data) == 2
        assert data[0]["event_type"] == EventType.PIPELINE_STARTED.value
    
    def test_export_events_csv(self, audit_logger, safe_tmp_path):
        """Test export_events in CSV format."""
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_record_processed("ingestion", "rec_1")
        
        export_path = safe_tmp_path / "events.csv"
        audit_logger.export_events(str(export_path), format="csv")
        
        assert export_path.exists()
        
        # Verify CSV has content
        with open(export_path) as f:
            content = f.read()
        
        assert "event_type" in content
        assert "pipeline_started" in content
    
    def test_export_events_invalid_format(self, audit_logger, safe_tmp_path):
        """Test export_events with invalid format."""
        audit_logger.log_pipeline_started("pipeline")
        
        export_path = safe_tmp_path / "events.xml"
        
        with pytest.raises(ValueError, match="Unsupported export format"):
            audit_logger.export_events(str(export_path), format="xml")
    
    def test_export_events_with_filter(self, audit_logger, safe_tmp_path):
        """Test export_events with event type filter."""
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_record_processed("ingestion", "rec_1")
        audit_logger.log_warning("processing", "Warning")
        
        export_path = safe_tmp_path / "warnings.json"
        audit_logger.export_events(str(export_path), event_type=EventType.WARNING_ISSUED)
        
        with open(export_path) as f:
            data = json.load(f)
        
        assert len(data) == 1
        assert data[0]["event_type"] == EventType.WARNING_ISSUED.value
    
    def test_get_summary(self, audit_logger):
        """Test get_summary method."""
        # Add various events
        audit_logger.log_pipeline_started("pipeline")
        audit_logger.log_record_processed("ingestion", "rec_1")
        audit_logger.log_warning("processing", "Warning")
        audit_logger.log_error("processing", ValueError("Error"))
        
        with audit_logger.correlation_context("corr_1"):
            audit_logger.log_record_processed("ingestion", "rec_2")
        
        with audit_logger.correlation_context("corr_2"):
            audit_logger.log_record_processed("ingestion", "rec_3")
        
        summary = audit_logger.get_summary()
        
        assert summary["pipeline_run_id"] == "test_run_123"
        assert summary["total_events"] == 6
        assert summary["level_counts"]["INFO"] == 1  # pipeline_started only
        assert summary["level_counts"]["WARNING"] == 1
        assert summary["level_counts"]["ERROR"] == 1
        assert summary["level_counts"]["DEBUG"] == 3  # All record_processed events use DEBUG
        assert summary["event_type_counts"]["record_processed"] == 3
        assert summary["unique_correlation_ids"] == 2
        assert summary["audit_enabled"] is True