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
Unit tests for persistence models.

Tests enum definitions for status and error categorization.
"""

import pytest
from pulsepipe.persistence.models import (
    ProcessingStatus,
    ErrorCategory
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
        assert ErrorCategory.TRANSFORMATION_ERROR in categories
        assert ErrorCategory.SYSTEM_ERROR in categories
        assert ErrorCategory.DATA_QUALITY_ERROR in categories
        assert ErrorCategory.NETWORK_ERROR in categories
        assert ErrorCategory.PERMISSION_ERROR in categories