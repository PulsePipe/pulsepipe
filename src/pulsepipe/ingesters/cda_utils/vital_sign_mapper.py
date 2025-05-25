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

# src/pulsepipe/ingesters/cda_utils/vital_sign_mapper.py

from typing import Dict, Any
from pulsepipe.models import VitalSign
from .base_mapper import BaseCDAMapper

class VitalSignMapper(BaseCDAMapper):
    """Maps CDA vital sign data to VitalSign model."""
    
    def map(self, data: Dict[str, Any]) -> VitalSign:
        """Map CDA vital sign data to VitalSign."""
        
        # Extract vital sign information
        vital_data = self._safe_get(data, 'vital_sign', {})
        vital_name = vital_data.get('display')
        
        # Extract value information
        value_data = self._safe_get(data, 'value', {})
        value = value_data.get('value')
        unit = value_data.get('unit')
        
        # Parse recorded date
        recorded_date_raw = self._safe_get(data, 'recorded_date')
        recorded_date = None
        if recorded_date_raw:
            parsed_date = self._parse_date(recorded_date_raw)
            if parsed_date:
                recorded_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract patient and encounter IDs
        patient_id = self._safe_get(data, 'patient_id')
        encounter_id = self._safe_get(data, 'encounter_id')
        
        # Convert value to float if possible, otherwise keep as string
        # VitalSign.value is required, so provide a default if None
        processed_value = value
        if value is not None:
            try:
                processed_value = float(value)
            except (ValueError, TypeError):
                processed_value = str(value)
        else:
            # Provide default value for required field
            processed_value = "N/A"
        
        return VitalSign(
            code=vital_data.get('code'),
            coding_method=vital_data.get('system'),
            display=vital_name,
            value=processed_value,
            unit=unit,
            timestamp=recorded_date,
            patient_id=patient_id,
            encounter_id=encounter_id
        )