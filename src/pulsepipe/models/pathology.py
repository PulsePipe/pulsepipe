# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
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

from typing import Optional, List
from pydantic import BaseModel

class PathologyFinding(BaseModel):
    code: Optional[str]                   # SNOMED, local code, or free-text
    coding_method: Optional[str]
    description: Optional[str]            # e.g., "Invasive ductal carcinoma"
    comment: Optional[str]                # Additional notes on the finding

class PathologyReport(BaseModel):
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
