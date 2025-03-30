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

class Order(BaseModel):
    order_id: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]
    order_type: Optional[str]                 # e.g., Lab, Imaging, Procedure, Medication
    code: Optional[str]                       # CPT, LOINC, internal code
    coding_method: Optional[str]
    description: Optional[str]
    ordering_provider_id: Optional[str]
    order_date: Optional[str]
    status: Optional[str]                      # ordered, canceled, completed
    notes: Optional[str]
