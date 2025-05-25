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

# src/pulsepipe/ingesters/cda_utils/base_mapper.py

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from datetime import datetime

class BaseCDAMapper(ABC):
    """Base class for CDA data mappers."""
    
    @abstractmethod
    def map(self, data: Dict[str, Any]) -> Any:
        """Map CDA data to PulsePipe model."""
        pass
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[datetime]:
        """Parse CDA date format to datetime object."""
        if not date_str:
            return None
        
        # Remove timezone info for basic parsing
        clean_date = date_str
        if '+' in clean_date:
            clean_date = clean_date.split('+')[0]
        if '-' in clean_date and len(clean_date) > 10:  # Don't split on date separators
            clean_date = clean_date.split('-')[0] if clean_date.count('-') > 2 else clean_date
        if '.' in clean_date:
            clean_date = clean_date.split('.')[0]
        
        # Try different date formats
        formats = [
            '%Y%m%d%H%M%S',
            '%Y%m%d%H%M',
            '%Y%m%d',
            '%Y-%m-%d',
            '%Y-%m-%dT%H:%M:%S',
            '%Y-%m-%d %H:%M:%S'
        ]
        
        for fmt in formats:
            try:
                parsed_date = datetime.strptime(clean_date, fmt)
                # Return datetime object
                return parsed_date
            except ValueError:
                continue
        
        # If parsing fails, return None
        return None
    
    def _safe_get(self, data: Dict[str, Any], key: str, default: Any = None) -> Any:
        """Safely get value from dictionary."""
        return data.get(key, default)


class CDAMapperRegistry:
    """Registry for CDA mappers."""
    
    def __init__(self):
        self._mappers = {}
        self._initialize_mappers()
    
    def _initialize_mappers(self):
        """Initialize all available mappers."""
        from .patient_mapper import PatientMapper
        from .encounter_mapper import EncounterMapper
        from .allergy_mapper import AllergyMapper
        from .medication_mapper import MedicationMapper
        from .problem_mapper import ProblemMapper
        from .procedure_mapper import ProcedureMapper
        from .vital_sign_mapper import VitalSignMapper
        from .immunization_mapper import ImmunizationMapper
        from .lab_report_mapper import LabReportMapper
        
        self._mappers = {
            'patient': PatientMapper(),
            'encounter': EncounterMapper(),
            'allergy': AllergyMapper(),
            'medication': MedicationMapper(),
            'problem': ProblemMapper(),
            'procedure': ProcedureMapper(),
            'vital_sign': VitalSignMapper(),
            'immunization': ImmunizationMapper(),
            'lab_report': LabReportMapper(),
        }
    
    def get_mapper(self, mapper_type: str) -> BaseCDAMapper:
        """Get mapper by type."""
        if mapper_type not in self._mappers:
            raise ValueError(f"Unknown mapper type: {mapper_type}")
        return self._mappers[mapper_type]
    
    def register_mapper(self, mapper_type: str, mapper: BaseCDAMapper):
        """Register a new mapper."""
        self._mappers[mapper_type] = mapper