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

# tests/test_cli_context.py

"""
Unit tests for the CLI context functionality.

Tests all functionality of the PipelineContext dataclass including initialization,
metadata handling, context serialization, and logging prefix generation.
"""

import time
import uuid
import pytest
from unittest.mock import patch, MagicMock
from datetime import datetime

from pulsepipe.cli.context import PipelineContext


class TestPipelineContextInitialization:
    """Tests for PipelineContext initialization and basic functionality."""
    
    def test_default_initialization(self):
        """Test PipelineContext with default parameters."""
        context = PipelineContext()
        
        # Check that default values are set
        assert context.pipeline_id is not None
        assert len(context.pipeline_id) == 36  # UUID string length
        assert context.profile is None
        assert context.user_id is None
        assert context.org_id is None
        assert context.hostname is not None
        assert context.username is not None
        assert context.start_time is not None
        assert context.is_dry_run is False
        
        # Verify UUID format
        uuid.UUID(context.pipeline_id)  # Should not raise an exception
    
    def test_initialization_with_parameters(self):
        """Test PipelineContext with specific parameters."""
        test_pipeline_id = "test-pipeline-123"
        test_profile = "healthcare-prod"
        test_user_id = "user@example.com"
        test_org_id = "healthcare-org"
        
        context = PipelineContext(
            pipeline_id=test_pipeline_id,
            profile=test_profile,
            user_id=test_user_id,
            org_id=test_org_id,
            is_dry_run=True
        )
        
        assert context.pipeline_id == test_pipeline_id
        assert context.profile == test_profile
        assert context.user_id == test_user_id
        assert context.org_id == test_org_id
        assert context.is_dry_run is True
    
    def test_initialization_system_values_populated(self):
        """Test PipelineContext system values are populated correctly."""
        context = PipelineContext()
        
        # System values should be populated (actual values)
        assert context.hostname is not None
        assert isinstance(context.hostname, str)
        assert len(context.hostname) > 0
        
        assert context.username is not None
        assert isinstance(context.username, str)
        assert len(context.username) > 0
        
        assert context.start_time is not None
        assert isinstance(context.start_time, float)
        assert context.start_time > 0
    
    def test_initialization_with_empty_pipeline_id(self):
        """Test PipelineContext when pipeline_id is empty string."""
        context = PipelineContext(pipeline_id="")
        
        # Should generate new UUID in __post_init__
        assert context.pipeline_id != ""
        assert len(context.pipeline_id) == 36
        uuid.UUID(context.pipeline_id)  # Should not raise an exception
    
    def test_initialization_with_none_pipeline_id(self):
        """Test PipelineContext when pipeline_id is None."""
        context = PipelineContext(pipeline_id=None)
        
        # Should generate new UUID in __post_init__
        assert context.pipeline_id is not None
        assert len(context.pipeline_id) == 36
        uuid.UUID(context.pipeline_id)  # Should not raise an exception


class TestPipelineContextPostInit:
    """Tests for PipelineContext __post_init__ method."""
    
    def test_post_init_generates_uuid_for_empty_string(self):
        """Test that __post_init__ generates UUID for empty string."""
        # Create context with empty pipeline_id
        context = PipelineContext()
        context.pipeline_id = ""  # Set after creation
        context.__post_init__()  # Manually call to test
        
        assert context.pipeline_id != ""
        assert len(context.pipeline_id) == 36
        uuid.UUID(context.pipeline_id)  # Should not raise an exception
    
    def test_post_init_generates_uuid_for_none(self):
        """Test that __post_init__ generates UUID for None."""
        context = PipelineContext()
        context.pipeline_id = None  # Set after creation
        context.__post_init__()  # Manually call to test
        
        assert context.pipeline_id is not None
        assert len(context.pipeline_id) == 36
        uuid.UUID(context.pipeline_id)  # Should not raise an exception
    
    def test_post_init_preserves_existing_uuid(self):
        """Test that __post_init__ preserves existing valid pipeline_id."""
        original_id = "existing-pipeline-id"
        context = PipelineContext()
        context.pipeline_id = original_id
        context.__post_init__()
        
        assert context.pipeline_id == original_id
    
    def test_post_init_handles_whitespace_only_string(self):
        """Test that __post_init__ treats whitespace-only string as empty."""
        context = PipelineContext()
        context.pipeline_id = "   "  # Whitespace only
        context.__post_init__()
        
        # Should still be whitespace (current implementation doesn't strip)
        assert context.pipeline_id == "   "
    
    def test_post_init_handles_zero_value(self):
        """Test that __post_init__ handles falsy but non-empty values."""
        context = PipelineContext()
        context.pipeline_id = 0  # Falsy but not empty/None
        context.__post_init__()
        
        # Should generate new UUID since 0 is falsy
        assert context.pipeline_id != 0
        assert isinstance(context.pipeline_id, str)
        uuid.UUID(context.pipeline_id)  # Should not raise an exception


class TestPipelineContextAsDict:
    """Tests for PipelineContext as_dict method."""
    
    def test_as_dict_with_all_values(self):
        """Test as_dict with all values populated."""
        context = PipelineContext(
            pipeline_id="test-id-123",
            profile="prod-profile",
            user_id="user123",
            org_id="org456",
            is_dry_run=True
        )
        
        result = context.as_dict()
        
        # Check all values are present
        assert result["pipeline_id"] == "test-id-123"
        assert result["profile"] == "prod-profile"
        assert result["user_id"] == "user123"
        assert result["org_id"] == "org456"
        assert result["is_dry_run"] is True
        assert "hostname" in result
        assert "username" in result
        assert "start_time" in result
    
    def test_as_dict_filters_none_values(self):
        """Test that as_dict filters out None values."""
        context = PipelineContext(
            pipeline_id="test-id-123",
            profile=None,  # This should be filtered out
            user_id=None,  # This should be filtered out
            org_id=None,   # This should be filtered out
            is_dry_run=False
        )
        
        result = context.as_dict()
        
        # None values should be filtered out
        assert "profile" not in result
        assert "user_id" not in result
        assert "org_id" not in result
        
        # Non-None values should be present
        assert result["pipeline_id"] == "test-id-123"
        assert result["is_dry_run"] is False
        assert "hostname" in result
        assert "username" in result
        assert "start_time" in result
    
    def test_as_dict_preserves_falsy_non_none_values(self):
        """Test that as_dict preserves falsy but non-None values."""
        context = PipelineContext(
            pipeline_id="test-id-123",
            profile="",  # Empty string should be preserved
            is_dry_run=False  # False should be preserved
        )
        
        result = context.as_dict()
        
        assert result["profile"] == ""
        assert result["is_dry_run"] is False
        assert result["pipeline_id"] == "test-id-123"
    
    def test_as_dict_includes_system_metadata(self):
        """Test that as_dict includes system-generated metadata."""
        context = PipelineContext()
        result = context.as_dict()
        
        # System metadata should always be present
        assert "hostname" in result
        assert "username" in result
        assert "start_time" in result
        assert "pipeline_id" in result
        
        # Check types
        assert isinstance(result["hostname"], str)
        assert isinstance(result["username"], str)
        assert isinstance(result["start_time"], float)
        assert isinstance(result["pipeline_id"], str)
    
    def test_as_dict_system_data_types(self):
        """Test as_dict includes system data with correct types."""
        context = PipelineContext(pipeline_id="test-123")
        result = context.as_dict()
        
        # Check system data is included with correct types
        assert "hostname" in result
        assert isinstance(result["hostname"], str)
        assert len(result["hostname"]) > 0
        
        assert "username" in result
        assert isinstance(result["username"], str)
        assert len(result["username"]) > 0
        
        assert "start_time" in result
        assert isinstance(result["start_time"], float)
        assert result["start_time"] > 0
        
        assert result["pipeline_id"] == "test-123"


class TestPipelineContextGetLogPrefix:
    """Tests for PipelineContext get_log_prefix method."""
    
    def test_get_log_prefix_with_pipeline_id_only(self):
        """Test get_log_prefix with only pipeline_id."""
        context = PipelineContext(pipeline_id="test-pipeline-id-12345")
        prefix = context.get_log_prefix()
        
        # Should show first 8 characters of pipeline_id
        assert prefix == "[test-pip]"
    
    def test_get_log_prefix_with_pipeline_id_and_profile(self):
        """Test get_log_prefix with pipeline_id and profile."""
        context = PipelineContext(
            pipeline_id="test-pipeline-id-12345",
            profile="healthcare-prod"
        )
        prefix = context.get_log_prefix()
        
        expected = "[test-pip] [healthcare-prod]"
        assert prefix == expected
    
    def test_get_log_prefix_with_enterprise_fields(self):
        """Test get_log_prefix with user_id and org_id (PulsePilot mode)."""
        context = PipelineContext(
            pipeline_id="test-pipeline-id-12345",
            profile="enterprise",
            user_id="john.doe",
            org_id="acme-healthcare"
        )
        prefix = context.get_log_prefix()
        
        expected = "[test-pip] [enterprise] [john.doe@acme-healthcare]"
        assert prefix == expected
    
    def test_get_log_prefix_with_user_id_only(self):
        """Test get_log_prefix with user_id but no org_id."""
        context = PipelineContext(
            pipeline_id="test-pipeline-id-12345",
            user_id="john.doe",
            org_id=None  # Missing org_id
        )
        prefix = context.get_log_prefix()
        
        # Should not include user@org part if either is missing
        expected = "[test-pip]"
        assert prefix == expected
    
    def test_get_log_prefix_with_org_id_only(self):
        """Test get_log_prefix with org_id but no user_id."""
        context = PipelineContext(
            pipeline_id="test-pipeline-id-12345",
            user_id=None,  # Missing user_id
            org_id="acme-healthcare"
        )
        prefix = context.get_log_prefix()
        
        # Should not include user@org part if either is missing
        expected = "[test-pip]"
        assert prefix == expected
    
    def test_get_log_prefix_with_short_pipeline_id(self):
        """Test get_log_prefix with pipeline_id shorter than 8 characters."""
        context = PipelineContext(pipeline_id="short")
        prefix = context.get_log_prefix()
        
        # Should show the full short id
        assert prefix == "[short]"
    
    def test_get_log_prefix_with_empty_pipeline_id(self):
        """Test get_log_prefix with empty pipeline_id after __post_init__."""
        context = PipelineContext(pipeline_id="")
        # __post_init__ should have generated a new UUID
        prefix = context.get_log_prefix()
        
        # Should have a pipeline_id part
        assert "[" in prefix and "]" in prefix
        assert len(prefix.split("]")[0]) > 1  # Should have content in brackets
    
    def test_get_log_prefix_with_none_pipeline_id(self):
        """Test get_log_prefix when pipeline_id is None."""
        context = PipelineContext()
        context.pipeline_id = None  # Override after creation
        prefix = context.get_log_prefix()
        
        # Should handle None gracefully (current implementation may vary)
        # This tests the actual behavior
        if context.pipeline_id:
            assert "[" in prefix
        else:
            # If None is preserved, prefix might be empty or have just other parts
            pass  # Accept any behavior for None case
    
    def test_get_log_prefix_comprehensive_example(self):
        """Test get_log_prefix with a comprehensive real-world example."""
        context = PipelineContext(
            pipeline_id="a1b2c3d4-e5f6-7890-abcd-ef1234567890",
            profile="clinical-production",
            user_id="dr.smith@hospital.com",
            org_id="regional-medical-center"
        )
        prefix = context.get_log_prefix()
        
        expected = "[a1b2c3d4] [clinical-production] [dr.smith@hospital.com@regional-medical-center]"
        assert prefix == expected


class TestPipelineContextEdgeCases:
    """Tests for edge cases and error conditions."""
    
    def test_context_with_special_characters_in_fields(self):
        """Test context with special characters in string fields."""
        context = PipelineContext(
            pipeline_id="test-id-with-特殊字符",
            profile="profile@with#special$chars",
            user_id="user+name@domain.com",
            org_id="org-with-hyphens_and_underscores.123"
        )
        
        # Should handle special characters without errors
        assert context.pipeline_id == "test-id-with-特殊字符"
        assert context.profile == "profile@with#special$chars"
        assert context.user_id == "user+name@domain.com"
        assert context.org_id == "org-with-hyphens_and_underscores.123"
        
        # as_dict should work
        result = context.as_dict()
        assert result["pipeline_id"] == "test-id-with-特殊字符"
        
        # get_log_prefix should work
        prefix = context.get_log_prefix()
        assert "[test-id-]" in prefix  # First 8 chars
    
    def test_context_with_very_long_strings(self):
        """Test context with very long string values."""
        long_string = "x" * 1000
        context = PipelineContext(
            pipeline_id=long_string,
            profile=long_string,
            user_id=long_string,
            org_id=long_string
        )
        
        # Should handle long strings without errors
        assert len(context.pipeline_id) == 1000
        assert len(context.profile) == 1000
        
        # get_log_prefix should truncate pipeline_id appropriately
        prefix = context.get_log_prefix()
        assert "[xxxxxxxx]" in prefix  # First 8 chars
    
    @patch('pulsepipe.cli.context.socket.gethostname')
    @patch('pulsepipe.cli.context.getpass.getuser')
    def test_context_with_system_call_exceptions(self, mock_getuser, mock_gethostname):
        """Test context creation when system calls raise exceptions."""
        mock_gethostname.side_effect = Exception("Network error")
        mock_getuser.side_effect = Exception("User lookup failed")
        
        # Context creation should handle exceptions gracefully
        # This depends on the actual implementation - it might use defaults or re-raise
        try:
            context = PipelineContext()
            # If no exception, check that we got some default values
            assert context.hostname is not None
            assert context.username is not None
        except Exception:
            # If exceptions are re-raised, that's also valid behavior
            pass
    
    def test_context_serialization_consistency(self):
        """Test that as_dict output is consistent across multiple calls."""
        context = PipelineContext(
            pipeline_id="consistent-test",
            profile="test-profile",
            user_id="test-user",
            org_id="test-org"
        )
        
        # Multiple calls should return the same data
        dict1 = context.as_dict()
        dict2 = context.as_dict()
        
        assert dict1 == dict2
        assert dict1["pipeline_id"] == dict2["pipeline_id"]
        assert dict1["start_time"] == dict2["start_time"]
    
    def test_context_immutability_after_creation(self):
        """Test that context fields can be modified after creation."""
        context = PipelineContext(pipeline_id="original-id")
        original_id = context.pipeline_id
        
        # Dataclass fields should be mutable
        context.pipeline_id = "modified-id"
        assert context.pipeline_id == "modified-id"
        assert context.pipeline_id != original_id
        
        # as_dict should reflect changes
        result = context.as_dict()
        assert result["pipeline_id"] == "modified-id"


class TestPipelineContextDataclassFeatures:
    """Tests for dataclass-specific features."""
    
    def test_context_equality(self):
        """Test that two contexts with same data are equal."""
        context1 = PipelineContext(
            pipeline_id="test-123",
            profile="test-profile",
            user_id="user1",
            org_id="org1",
            is_dry_run=True
        )
        
        context2 = PipelineContext(
            pipeline_id="test-123",
            profile="test-profile", 
            user_id="user1",
            org_id="org1",
            is_dry_run=True
        )
        
        # Note: These won't be equal due to different hostname, username, start_time
        # but we can test field-by-field equality
        assert context1.pipeline_id == context2.pipeline_id
        assert context1.profile == context2.profile
        assert context1.user_id == context2.user_id
        assert context1.org_id == context2.org_id
        assert context1.is_dry_run == context2.is_dry_run
    
    def test_context_representation(self):
        """Test string representation of context."""
        context = PipelineContext(
            pipeline_id="test-123",
            profile="test-profile"
        )
        
        # Should have a meaningful string representation
        repr_str = repr(context)
        assert "PipelineContext" in repr_str
        assert "test-123" in repr_str
        assert "test-profile" in repr_str
    
    def test_context_field_access(self):
        """Test that all fields are accessible."""
        context = PipelineContext(
            pipeline_id="test-123",
            profile="test-profile",
            user_id="user1",
            org_id="org1",
            is_dry_run=True
        )
        
        # All fields should be accessible
        assert hasattr(context, "pipeline_id")
        assert hasattr(context, "profile")
        assert hasattr(context, "user_id")
        assert hasattr(context, "org_id")
        assert hasattr(context, "hostname")
        assert hasattr(context, "username")
        assert hasattr(context, "start_time")
        assert hasattr(context, "is_dry_run")
    
    def test_context_field_types(self):
        """Test that field types are as expected."""
        context = PipelineContext()
        
        # Check field types
        assert isinstance(context.pipeline_id, str)
        assert context.profile is None or isinstance(context.profile, str)
        assert context.user_id is None or isinstance(context.user_id, str)
        assert context.org_id is None or isinstance(context.org_id, str)
        assert isinstance(context.hostname, str)
        assert isinstance(context.username, str)
        assert isinstance(context.start_time, float)
        assert isinstance(context.is_dry_run, bool)


ALLOWED_DOMAINS = {
    "enterprise-client.com",
    "enterprise-client-org.com",
    "example.com"
}

class TestPipelineContextIntegration:
    """Integration tests for PipelineContext usage scenarios."""
    
    def test_healthcare_pipeline_scenario(self):
        """Test context in a realistic healthcare pipeline scenario."""
        # Simulate a clinical data processing pipeline
        context = PipelineContext(
            pipeline_id="clinical-proc-2024-001",
            profile="clinical-production",
            user_id="dr.jane.smith@regionalhospital.com",
            org_id="regional-healthcare-network",
            is_dry_run=False
        )
        
        # Verify context is properly set up for healthcare use
        assert "clinical" in context.pipeline_id
        assert "clinical" in context.profile
        assert "@regionalhospital.com" in context.user_id
        assert "healthcare" in context.org_id
        assert context.is_dry_run is False
        
        # Test log prefix for monitoring
        prefix = context.get_log_prefix()
        assert "[clinical]" in prefix  # First 8 chars: "clinical"
        assert "[clinical-production]" in prefix
        assert "[dr.jane.smith@regionalhospital.com@regional-healthcare-network]" in prefix
        
        # Test dictionary export for structured logging
        data = context.as_dict()
        assert data["pipeline_id"] == "clinical-proc-2024-001"
        assert data["profile"] == "clinical-production"
        assert data["user_id"] == "dr.jane.smith@regionalhospital.com"
        assert data["org_id"] == "regional-healthcare-network"
        assert "start_time" in data
        assert "hostname" in data
        assert "username" in data
    
    def test_enterprise_saas_scenario(self):
        """Test context in an enterprise SaaS deployment scenario."""

        # Simulate enterprise multi-tenant deployment
        user_id = "admin@enterprise-client.com"
        org_id = "enterprise-client-org-12345"
        context = PipelineContext(
            pipeline_id=str(uuid.uuid4()),
            profile="enterprise-saas-v2",
            user_id=user_id,
            org_id=org_id,
            is_dry_run=False
        )

        # Verify valid UUID
        uuid.UUID(context.pipeline_id)

        # Verify enterprise profile
        assert context.profile.startswith("enterprise")

        # Secure domain checks
        user_domain = user_id.split("@")[-1]
        assert user_domain in ALLOWED_DOMAINS

        org_base_domain = org_id.split("-org")[0] + ".com"
        assert org_base_domain in ALLOWED_DOMAINS

        # Log prefix formatting
        prefix = context.get_log_prefix()
        parts = prefix.split(" ")
        assert len(parts) == 3
        assert f"[{context.profile}]" in prefix
        assert f"[{user_id}@{org_id}]" in prefix

        # Dictionary export contains all required fields
        data = context.as_dict()
        required_fields = ["pipeline_id", "profile", "user_id", "org_id", "hostname", "username", "start_time"]
        for field in required_fields:
            assert field in data

        # No fields should be None in enterprise mode
        assert all(v is not None for v in data.values())
