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

# src/pulsepipe/models/pathology.py

from typing import Optional, List
from pydantic import BaseModel

class PathologyFinding(BaseModel):
    code: Optional[str]                   # SNOMED, local code, or free-text
    coding_method: Optional[str]
    description: Optional[str]            # e.g., "Invasive ductal carcinoma"
    comment: Optional[str]                # Additional notes on the finding

class PathologyReport(BaseModel):
    """
    Represents a pathology examination report on tissue specimens.
    
    Pathology reports document the examination of cells and tissues by a 
    pathologist to diagnose disease. They're crucial for cancer diagnosis,
    staging, and treatment planning, as well as for diagnosing other
    conditions that require tissue analysis.
    
    A complete report typically includes specimen information, gross and 
    microscopic descriptions, and a diagnostic interpretation with additional
    information like cancer staging and grading where applicable.
    """
    report_id: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]
    test_name: Optional[str] = None
    specimen: Optional[str]               # e.g., "Breast biopsy", "Lung nodule"
    procedure: Optional[str]              # e.g., "Needle biopsy", "Surgical resection"
    collection_date: Optional[str]
    report_date: Optional[str]
    performing_lab: Optional[str]
    ordering_provider_id: Optional[str]
    findings: List[PathologyFinding] = []  # Pathologist's structured findings
    diagnosis: Optional[str]               # Final diagnosis / summary impression
    staging: Optional[str]                 # e.g., "pT2N1M0", optional
    grade: Optional[str]                   # e.g., "Grade II"
    narrative: Optional[str]               # Full gross + microscopic narrative
    note: Optional[str]                    # Any additional comments
