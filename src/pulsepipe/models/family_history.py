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

# src/pulsepipe/models/family_history.py

from typing import Optional
from pydantic import BaseModel

class FamilyHistory(BaseModel):
    """
    Represents a clinical family history entry, capturing known conditions affecting the patient's relatives.
    This information is often used to assess genetic risk and guide preventive care.
    """
    condition: Optional[str]                     # "Diabetes Mellitus", "Breast Cancer"
    code: Optional[str]                          # ICD, SNOMED, etc.
    coding_method: Optional[str]
    relative: Optional[str]                      # mother, father, sibling, child
    status: Optional[str]                        # e.g., affected, carrier, unknown
    age_of_onset: Optional[str]
    notes: Optional[str]
