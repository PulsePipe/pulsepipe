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

# src/pulsepipe/models/diagnostic_test.py

from typing import Optional
from pydantic import BaseModel

class DiagnosticTest(BaseModel):
    test_id: Optional[str]
    ordering_provider_id: Optional[str]
    performing_facility: Optional[str]
    test_type: Optional[str]                      # e.g., EKG, Spirometry, EEG
    code: Optional[str]
    coding_method: Optional[str]
    result_date: Optional[str]
    result_summary: Optional[str]                 # Narrative interpretation
    findings: Optional[str]                       # Textual or structured findings
    patient_id: Optional[str]
    encounter_id: Optional[str]
