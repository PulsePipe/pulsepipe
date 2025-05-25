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

# src/pulsepipe/ingesters/cda_utils/problem_mapper.py

from typing import Dict, Any
from pulsepipe.models import Problem
from .base_mapper import BaseCDAMapper

class ProblemMapper(BaseCDAMapper):
    """Maps CDA problem data to Problem model."""
    
    def map(self, data: Dict[str, Any]) -> Problem:
        """Map CDA problem data to Problem."""
        
        # Extract problem information
        problem_data = self._safe_get(data, 'problem', {})
        problem_name = problem_data.get('display')
        
        # Parse onset date and convert to string
        onset_date_str = self._safe_get(data, 'onset_date')
        onset_date = None
        if onset_date_str:
            parsed_date = self._parse_date(onset_date_str)
            if parsed_date:
                onset_date = parsed_date.strftime('%Y-%m-%d')
        
        # Extract patient and encounter IDs
        patient_id = self._safe_get(data, 'patient_id')
        encounter_id = self._safe_get(data, 'encounter_id')
        
        return Problem(
            code=problem_data.get('code'),
            coding_method=problem_data.get('system'),
            description=problem_name,
            onset_date=onset_date,
            patient_id=patient_id,
            encounter_id=encounter_id
        )