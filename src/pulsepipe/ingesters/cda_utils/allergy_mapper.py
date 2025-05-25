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

# src/pulsepipe/ingesters/cda_utils/allergy_mapper.py

from typing import Dict, Any
from pulsepipe.models import Allergy
from .base_mapper import BaseCDAMapper

class AllergyMapper(BaseCDAMapper):
    """Maps CDA allergy data to Allergy model."""
    
    def map(self, data: Dict[str, Any]) -> Allergy:
        """Map CDA allergy data to Allergy."""
        
        # Extract substance information
        substance_data = self._safe_get(data, 'substance', {})
        substance_name = self._safe_get(data, 'substance_name') or substance_data.get('display')
        
        # Extract coding system
        coding_system = substance_data.get('system')
        
        # Parse onset date - convert to string for model
        onset_date = self._safe_get(data, 'onset_date')
        onset_str = None
        if onset_date:
            parsed_date = self._parse_date(onset_date)
            if parsed_date:
                onset_str = parsed_date.strftime('%Y-%m-%d')
        
        return Allergy(
            substance=substance_name,
            coding_method=coding_system,
            reaction=None,  # Not typically in CDA allergy sections
            severity=None,  # Could be extracted if present
            onset=onset_str,
            patient_id=None  # Will be set by the ingester if needed
        )