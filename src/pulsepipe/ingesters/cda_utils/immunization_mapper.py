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

# src/pulsepipe/ingesters/cda_utils/immunization_mapper.py

from typing import Dict, Any
from pulsepipe.models import Immunization
from .base_mapper import BaseCDAMapper

class ImmunizationMapper(BaseCDAMapper):
    """Maps CDA immunization data to Immunization model."""
    
    def map(self, data: Dict[str, Any]) -> Immunization:
        """Map CDA immunization data to Immunization."""
        
        # Extract vaccine information
        vaccine_data = self._safe_get(data, 'vaccine', {})
        vaccine_name = vaccine_data.get('display')
        
        # Parse administration date
        administration_date_raw = self._safe_get(data, 'administration_date')
        administration_date = None
        if administration_date_raw:
            parsed_date = self._parse_date(administration_date_raw)
            if parsed_date:
                administration_date = parsed_date.strftime('%Y-%m-%d')
        
        # Extract status and lot number
        status = self._safe_get(data, 'status', 'completed')
        lot_number = self._safe_get(data, 'lot_number')
        
        # Extract patient and encounter IDs
        patient_id = self._safe_get(data, 'patient_id')
        encounter_id = self._safe_get(data, 'encounter_id')
        
        return Immunization(
            vaccine_code=vaccine_data.get('code'),
            coding_method=vaccine_data.get('system'),
            description=vaccine_name,
            date_administered=administration_date,
            status=status,
            lot_number=lot_number,
            patient_id=patient_id,
            encounter_id=encounter_id
        )