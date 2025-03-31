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
from typing import Optional
from pydantic import BaseModel

class Note(BaseModel):
    note_type_code: Optional[str] = None     # e.g., "DS" = Discharge Summary, "PN" = Progress Note
    text: Optional[str] = None               # The actual note content
    timestamp: Optional[str] = None          # ISO 8601 datetime (required)
    author_id: Optional[str] = None          # EHR or system-level user ID
    author_name: Optional[str] = None        # Displayable name (optional)
    patient_id: Optional[str] = None                  
    encounter_id: Optional[str] = None