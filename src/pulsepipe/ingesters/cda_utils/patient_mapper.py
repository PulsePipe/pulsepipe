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

# src/pulsepipe/ingesters/cda_utils/patient_mapper.py

from typing import Dict, Any, Optional, List
from datetime import datetime
from pulsepipe.models import PatientInfo, PatientPreferences
from .base_mapper import BaseCDAMapper

class PatientMapper(BaseCDAMapper):
    """Maps CDA patient data to PatientInfo model."""
    
    def map(self, data: Dict[str, Any]) -> PatientInfo:
        """Map CDA patient data to PatientInfo."""
        
        # Extract identifiers and use first one as main ID
        identifiers = {}
        patient_id = None
        for id_data in self._safe_get(data, 'identifiers', []):
            if id_data.get('extension'):
                key = id_data.get('root', 'unknown')
                value = id_data.get('extension', '')
                identifiers[key] = value
                if patient_id is None:
                    patient_id = value
        
        # Extract gender
        gender_data = self._safe_get(data, 'gender', {})
        gender = gender_data.get('code', '').lower() if gender_data.get('code') else None
        
        # Parse birth date and calculate age for over-90 determination
        birth_date_str = self._safe_get(data, 'birth_date')
        dob_year = None
        over_90 = False
        
        if birth_date_str:
            try:
                # Handle different date formats
                if len(birth_date_str) >= 4:
                    year = int(birth_date_str[:4])
                    age = datetime.now().year - year
                    if age >= 90:
                        over_90 = True
                    else:
                        dob_year = year
            except (ValueError, TypeError):
                pass
        
        # Extract geographic area (state level for HIPAA compliance)
        address_data = self._safe_get(data, 'address', {})
        geographic_area = address_data.get('state')
        
        # Build preferences from available data
        preferences = []
        
        return PatientInfo(
            id=patient_id,
            dob_year=dob_year,
            over_90=over_90,
            gender=gender,
            geographic_area=geographic_area,
            identifiers=identifiers if identifiers else {},
            preferences=preferences if preferences else None
        )