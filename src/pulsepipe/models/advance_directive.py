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

class AdvanceDirective(BaseModel):
    directive_type: Optional[str]             # e.g., "DNR", "Full Code", "Advance Directive on file"
    code: Optional[str]                       # Local or SNOMED code
    coding_method: Optional[str]
    status: Optional[str]                     # e.g., "Active", "Rescinded"
    effective_date: Optional[str]
    expiration_date: Optional[str]
    notes: Optional[str]                      # Free-text detail, e.g., location of paper copy
