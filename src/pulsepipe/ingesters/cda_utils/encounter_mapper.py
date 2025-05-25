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

# src/pulsepipe/ingesters/cda_utils/encounter_mapper.py

from typing import Dict, Any, Optional
from pulsepipe.models import EncounterInfo, EncounterProvider
from .base_mapper import BaseCDAMapper

class EncounterMapper(BaseCDAMapper):
    """Maps CDA encounter data to EncounterInfo model."""
    
    def map(self, data: Dict[str, Any]) -> EncounterInfo:
        """
        Map CDA encounter data to EncounterInfo.
        
        CDA encounters can be found in:
        - componentOf/encompassingEncounter (for the overall encounter)
        - Encounters section (2.16.840.1.113883.10.20.22.2.22.1)
        - Individual entry encounters in various sections
        """
        
        # Extract encounter identifiers
        encounter_id = self._safe_get(data, 'id')
        
        # Extract encounter class/type
        encounter_class = self._safe_get(data, 'class_code', 'unknown')
        encounter_type = self._safe_get(data, 'type_code', 'unknown')
        
        # Map encounter class codes to readable values
        class_mapping = {
            'AMB': 'ambulatory',
            'EMER': 'emergency',
            'IMP': 'inpatient', 
            'OBSENC': 'observation',
            'PRENC': 'pre-admission',
            'SS': 'short_stay'
        }
        encounter_class = class_mapping.get(encounter_class, encounter_class)
        
        # Extract dates and convert to strings
        start_date_raw = self._safe_get(data, 'start_date')
        start_date = None
        if start_date_raw:
            parsed_date = self._parse_date(start_date_raw)
            if parsed_date:
                start_date = parsed_date.strftime('%Y-%m-%d')
        
        end_date_raw = self._safe_get(data, 'end_date')
        end_date = None
        if end_date_raw:
            parsed_date = self._parse_date(end_date_raw)
            if parsed_date:
                end_date = parsed_date.strftime('%Y-%m-%d')
        
        # Extract location information
        location_data = self._safe_get(data, 'location', {})
        location_name = location_data.get('name')
        service_line = location_data.get('service_line')
        department = location_data.get('department')
        facility = location_data.get('facility')
        
        # Extract provider information
        providers = []
        for provider_data in self._safe_get(data, 'providers', []):
            provider = EncounterProvider(
                id=provider_data.get('id'),
                type_code=provider_data.get('type'),
                coding_method='ProviderRole',
                name=provider_data.get('name'),
                specialty=provider_data.get('specialty')
            )
            providers.append(provider)
        
        # Extract admission/discharge information
        admission_source = self._safe_get(data, 'admission_source')
        discharge_disposition = self._safe_get(data, 'discharge_disposition')
        
        # Extract diagnosis information (primary/secondary)
        primary_diagnosis = None
        secondary_diagnoses = []
        
        for diag_data in self._safe_get(data, 'diagnoses', []):
            diagnosis_info = {
                'code': diag_data.get('code'),
                'display': diag_data.get('display'),
                'system': diag_data.get('system')
            }
            
            if diag_data.get('type') == 'primary' or diag_data.get('sequence') == '1':
                primary_diagnosis = diagnosis_info
            else:
                secondary_diagnoses.append(diagnosis_info)
        
        # Extract DRG information
        drg_code = self._safe_get(data, 'drg', {}).get('code')
        drg_description = self._safe_get(data, 'drg', {}).get('description')
        
        # Extract financial class/payor
        financial_class = self._safe_get(data, 'financial_class')
        primary_insurance = self._safe_get(data, 'primary_insurance')
        
        # Determine status
        status = 'finished'  # Most CDA documents represent completed encounters
        if end_date is None and start_date is not None:
            status = 'in-progress'
        elif start_date is None:
            status = 'planned'
        
        # Extract patient ID
        patient_id = self._safe_get(data, 'patient_id')
        
        return EncounterInfo(
            id=encounter_id,
            admit_date=start_date,
            discharge_date=end_date,
            encounter_type=encounter_class or 'unknown',
            type_coding_method='HL7ActCode',
            location=location_name,
            reason_code=primary_diagnosis.get('code') if primary_diagnosis else None,
            reason_coding_method=primary_diagnosis.get('system') if primary_diagnosis else None,
            providers=providers if providers else [],
            visit_type=encounter_type or 'unknown',
            patient_id=patient_id
        )