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

# src/pulsepipe/ingesters/cda_utils/medication_mapper.py

from typing import Dict, Any
from pulsepipe.models import Medication
from .base_mapper import BaseCDAMapper

class MedicationMapper(BaseCDAMapper):
    """Maps CDA medication data to Medication model."""
    
    def map(self, data: Dict[str, Any]) -> Medication:
        """Map CDA medication data to Medication."""
        
        # Extract medication information
        medication_data = self._safe_get(data, 'medication', {})
        medication_name = self._safe_get(data, 'medication_name') or medication_data.get('display')
        
        # Extract dosage information
        dosage_data = self._safe_get(data, 'dosage', {})
        dose_amount = dosage_data.get('value')
        dose_unit = dosage_data.get('unit')
        
        # Build dose string
        dose = None
        if dose_amount and dose_unit:
            dose = f"{dose_amount} {dose_unit}"
        elif dose_amount:
            dose = str(dose_amount)
        
        # Parse start date - convert to string for model
        start_date_str = self._safe_get(data, 'start_date')
        start_date = None
        if start_date_str:
            parsed_date = self._parse_date(start_date_str)
            if parsed_date:
                start_date = parsed_date.strftime('%Y-%m-%d')
        
        return Medication(
            code=medication_data.get('code'),
            coding_method=medication_data.get('system'),
            name=medication_name,
            dose=dose,
            route=None,  # Could be extracted if present in CDA
            frequency=None,  # Could be extracted if present in CDA
            start_date=start_date,
            end_date=None,  # Could be extracted if present in CDA
            status='active',  # Default status
            patient_id=None,  # Will be set by ingester if needed
            encounter_id=None,  # Will be set by ingester if needed
            notes=None  # CDA medications rarely have notes
        )