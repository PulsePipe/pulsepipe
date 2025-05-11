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

# src/pulsepipe/models/microbiology.py

from typing import List, Optional
from pydantic import BaseModel

class MicrobiologySensitivity(BaseModel):
    antibiotic_code: Optional[str]
    coding_method: Optional[str]
    antibiotic_name: Optional[str]
    mic: Optional[str]                # Minimum Inhibitory Concentration
    interpretation: Optional[str]    # S, I, R, etc.

class MicrobiologyOrganism(BaseModel):
    organism_code: Optional[str]
    coding_method: Optional[str]
    organism_name: Optional[str]
    colony_count: Optional[str]
    sensitivities: List[MicrobiologySensitivity] = []

class MicrobiologyReport(BaseModel):
    """
    Represents a clinical microbiology test report.
    
    Microbiology reports document the results of laboratory tests performed
    to identify infectious organisms and their susceptibility to antimicrobial
    agents. These reports are critical for diagnosing infections and selecting
    appropriate antibiotic therapy.
    
    A complete report includes specimen information, identified organisms, 
    and antimicrobial susceptibility test results (antibiogram).
    """
    report_id: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]
    collection_date: Optional[str] = "Unknown"
    result_date: Optional[str]  = "Unknown"
    source_site: Optional[str]  = "Unknown"     # blood, urine, sputum
    organisms: List[MicrobiologyOrganism] = []
    comment: Optional[str]
