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

# src/pulsepipe/ingesters/cda_utils/procedure_mapper.py

from typing import Dict, Any
from pulsepipe.models import Procedure
from pulsepipe.models.procedure import ProcedureProvider
from .base_mapper import BaseCDAMapper

class ProcedureMapper(BaseCDAMapper):
    """Maps CDA procedure data to Procedure model."""
    
    def map(self, data: Dict[str, Any]) -> Procedure:
        """Map CDA procedure data to Procedure."""
        
        # Extract procedure information
        procedure_data = self._safe_get(data, 'procedure', {})
        procedure_name = procedure_data.get('display')
        
        # Parse performed date
        performed_date_raw = self._safe_get(data, 'performed_date')
        performed_date = None
        if performed_date_raw:
            parsed_date = self._parse_date(performed_date_raw)
            if parsed_date:
                performed_date = parsed_date.strftime('%Y-%m-%d')
        
        # Extract status
        status = self._safe_get(data, 'status', 'completed')
        
        # Extract patient and encounter IDs
        patient_id = self._safe_get(data, 'patient_id')
        encounter_id = self._safe_get(data, 'encounter_id')
        
        # Extract providers if available
        providers = []
        for provider_data in self._safe_get(data, 'providers', []):
            provider = ProcedureProvider(
                provider_id=provider_data.get('id'),
                role=provider_data.get('role')
            )
            providers.append(provider)
        
        return Procedure(
            code=procedure_data.get('code'),
            coding_method=procedure_data.get('system'),
            description=procedure_name,
            performed_date=performed_date,
            status=status,
            providers=providers,
            patient_id=patient_id,
            encounter_id=encounter_id
        )