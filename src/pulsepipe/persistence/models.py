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

# src/pulsepipe/persistence/models.py

"""
Database models for data intelligence and audit tracking.

Defines enums and status types for tracking ingestion statistics,
audit trails, quality metrics, and performance data.
"""

from enum import Enum


class ProcessingStatus(str, Enum):
    """Status values for processing records."""
    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL = "partial"
    SKIPPED = "skipped"


class ErrorCategory(str, Enum):
    """Categories for error classification."""
    SCHEMA_ERROR = "schema_error"
    VALIDATION_ERROR = "validation_error"
    PARSE_ERROR = "parse_error"
    TRANSFORMATION_ERROR = "transformation_error"
    SYSTEM_ERROR = "system_error"
    DATA_QUALITY_ERROR = "data_quality_error"
    NETWORK_ERROR = "network_error"
    PERMISSION_ERROR = "permission_error"