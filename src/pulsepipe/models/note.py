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

# src/pulsepipe/models/note.py

from typing import Optional
from pydantic import BaseModel

class Note(BaseModel):
    """
    Represents a clinical note documenting patient care.
    
    Clinical notes are narrative text documents created by healthcare providers
    to record observations, assessments, plans, and other aspects of patient care.
    They may include admission notes, progress notes, consultation notes,
    discharge summaries, procedure notes, or other documentation types.
    
    Notes provide crucial context and details that may not be captured in
    structured data elements and often contain the rationale for clinical decisions.
    """
    note_type_code: Optional[str] = None     # e.g., "DS" = Discharge Summary, "PN" = Progress Note
    text: Optional[str] = None               # The actual note content
    timestamp: Optional[str] = None          # ISO 8601 datetime (required)
    author_id: Optional[str] = None          # EHR or system-level user ID
    author_name: Optional[str] = None        # Displayable name (optional)
    patient_id: Optional[str] = None                  
    encounter_id: Optional[str] = None