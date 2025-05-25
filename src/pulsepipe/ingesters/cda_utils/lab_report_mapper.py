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

# src/pulsepipe/ingesters/cda_utils/lab_report_mapper.py

from typing import Dict, Any
from pulsepipe.models import LabReport, LabObservation
from .base_mapper import BaseCDAMapper

class LabReportMapper(BaseCDAMapper):
    """Maps CDA lab result data to LabReport model."""
    
    def map(self, data: Dict[str, Any]) -> LabReport:
        """Map CDA lab result data to LabReport."""
        
        # Extract report-level information
        report_data = self._safe_get(data, 'report', {})
        report_id = self._safe_get(data, 'report_id')
        
        # Extract test information for creating observations
        test_data = self._safe_get(data, 'test', {})
        test_name = test_data.get('display')
        
        # Extract result information
        result_data = self._safe_get(data, 'result', {})
        result_value = result_data.get('value')
        result_unit = result_data.get('unit')
        
        # Parse collected date
        collected_date_raw = self._safe_get(data, 'collected_date')
        collected_date = None
        if collected_date_raw:
            parsed_date = self._parse_date(collected_date_raw)
            if parsed_date:
                collected_date = parsed_date.strftime('%Y-%m-%d %H:%M:%S')
        
        # Extract reference range
        ref_range_data = self._safe_get(data, 'reference_range', {})
        reference_range = None
        if ref_range_data.get('low') or ref_range_data.get('high'):
            low = ref_range_data.get('low', '')
            high = ref_range_data.get('high', '')
            unit = ref_range_data.get('unit', '')
            reference_range = f"{low}-{high} {unit}".strip()
        
        # Extract abnormal flag
        abnormal_flag = self._safe_get(data, 'abnormal_flag')
        
        # Create the lab observation
        observation = LabObservation(
            code=test_data.get('code'),
            coding_method=test_data.get('system'),
            name=test_name,
            description=test_data.get('description'),
            value=result_value,
            unit=result_unit,
            reference_range=reference_range,
            abnormal_flag=abnormal_flag,
            result_date=collected_date
        )
        
        # Extract patient and encounter IDs
        patient_id = self._safe_get(data, 'patient_id')
        encounter_id = self._safe_get(data, 'encounter_id')
        
        # Create the lab report
        return LabReport(
            report_id=report_id,
            lab_type=self._safe_get(data, 'lab_type'),
            code=report_data.get('code'),
            coding_method=report_data.get('system'),
            panel_name=report_data.get('panel_name'),
            panel_code=report_data.get('panel_code'),
            panel_code_method=report_data.get('panel_code_method'),
            is_panel=self._safe_get(data, 'is_panel', False),
            ordering_provider_id=self._safe_get(data, 'ordering_provider_id'),
            performing_lab=self._safe_get(data, 'performing_lab'),
            report_type=self._safe_get(data, 'report_type'),
            collection_date=collected_date,
            observations=[observation] if observation.name or observation.value else [],
            note=self._safe_get(data, 'note'),
            patient_id=patient_id,
            encounter_id=encounter_id
        )