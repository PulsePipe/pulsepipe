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

# src/pulsepipe/pipelines/quality/__init__.py

"""
Data Quality Scoring Engine for PulsePipe.

Provides comprehensive data quality assessment including completeness,
consistency, outlier detection, and aggregate scoring.
"""

from .scoring_engine import (
    DataQualityScorer,
    QualityScore,
    CompletenessScorer,
    ConsistencyScorer,
    OutlierDetector,
    DataUsageAnalyzer,
    QualityDimension,
    Severity,
    QualityIssue
)
from .integration import (
    QualityAssessmentService,
    QualityAssessmentPipeline
)

__all__ = [
    "DataQualityScorer",
    "QualityScore", 
    "CompletenessScorer",
    "ConsistencyScorer",
    "OutlierDetector",
    "DataUsageAnalyzer",
    "QualityDimension",
    "Severity",
    "QualityIssue",
    "QualityAssessmentService",
    "QualityAssessmentPipeline"
]