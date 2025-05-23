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

# src/pulsepipe/models/blood_bank.py

from typing import List, Optional
from pydantic import BaseModel


class BloodBankFinding(BaseModel):
    """
    Represents a single blood bank test result, such as ABO typing, Rh factor,
    antibody screening, or crossmatch result.
    """
    code: Optional[str]
    coding_method: Optional[str]        
    test_name: Optional[str]                    # ABO, Rh, Antibody Screen, Crossmatch
    result: Optional[str]
    interpretation: Optional[str]               # e.g., Compatible, Incompatible, Positive
    comment: Optional[str]


class BloodBankReport(BaseModel):
    """
    Represents a structured blood bank report containing one or more findings.
    This may include blood type testing, antibody screening, and compatibility checks,
    associated with a specific patient and encounter.
    """
    report_id: Optional[str]
    collection_date: Optional[str]
    result_date: Optional[str]
    findings: List[BloodBankFinding] = []
    comment: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]
