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

class ImagingFinding(BaseModel):
    code: Optional[str]                      # SNOMED CT, RadLex, etc.
    coding_method: Optional[str]
    description: Optional[str]
    impression: Optional[str]                # Radiologist's impression
    abnormal_flag: Optional[str]             # Optional, depending on source
    result_date: Optional[str]

class ImagingReport(BaseModel):
    report_id: Optional[str]
    image_type: Optional[str]               # Chest X-Ray, Lower Lumbar MRI
    coding_method: Optional[str]
    ordering_provider_id: Optional[str]
    performing_facility: Optional[str]
    modality: Optional[str]                  # CT, MRI, X-ray, Ultrasound, etc.
    acquisition_date: Optional[str]
    findings: List[ImagingFinding] = []
    narrative: Optional[str]                 # Full report narrative
    patient_id: Optional[str]
    encounter_id: Optional[str]