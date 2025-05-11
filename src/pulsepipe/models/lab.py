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

# src/pulsepipe/models/lab.py

from typing import Optional, List
from pydantic import BaseModel
from enum import Enum

class LabType(str, Enum):
    """
    Enumeration of standard laboratory test categories.
    
    Used to classify laboratory reports by their medical specialty
    or testing methodology.
    """
    chemistry = "Chemistry"
    hematology = "Hematology"
    coagulation = "Coagulation"
    urinalysis = "Urinalysis"
    toxicology = "Toxicology"
    serology = "Serology"
    immunology = "Immunology"
    molecular = "Molecular Diagnostics"
    virology = "Virology"

class LabObservation(BaseModel):
    code: Optional[str]                      # LOINC or local code
    coding_method: Optional[str]             # e.g., LOINC
    name: Optional[str]                      # e.g., WBC, RBC, HgA1C
    description: Optional[str]
    value: Optional[str]                     # Could be numeric or text (e.g., "Positive")
    unit: Optional[str]
    reference_range: Optional[str]
    abnormal_flag: Optional[str]             # e.g., H = High, L = Low, N = Normal
    result_date: Optional[str]               # When result was observed

class LabReport(BaseModel):
    """
    Represents a comprehensive laboratory report containing multiple observations.
    
    Laboratory reports typically include one or more individual test results (observations)
    along with metadata about the specimen collection, ordering provider, and performing
    laboratory. Reports may represent a single test or a panel of related tests (e.g.,
    Complete Blood Count, Basic Metabolic Panel).
    
    This model captures both structured test results and any narrative interpretations
    provided by laboratory professionals.
    """
    report_id: Optional[str]
    lab_type: Optional[LabType]                     # Chemistry, Hematology, Pathology, etc.
    code: Optional[str]                             # LOINC or local code for the report or panel
    coding_method: Optional[str]                    # LOINC, local
    panel_name: Optional[str]                       # "Basic Metabolic Panel"
    panel_code: Optional[str]                       # CPT or local panel code
    panel_code_method: Optional[str]                
    is_panel: Optional[bool]                        # True if this report represents a panel
    ordering_provider_id: Optional[str]
    performing_lab: Optional[str]
    report_type: Optional[str]                      # Narrative description
    collection_date: Optional[str]
    observations: List[LabObservation] = []
    note: Optional[str]                             # Free-text impression
    patient_id: Optional[str]
    encounter_id: Optional[str]
