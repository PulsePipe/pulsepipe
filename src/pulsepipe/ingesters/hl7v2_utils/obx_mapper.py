# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
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

# src/pulsepipe/ingesters/hl7v2_utils/obx_mapper.py


from typing import Dict, Any
from pulsepipe.utils.log_factory import LogFactory
from .message import Segment
from .base_mapper import HL7v2Mapper, register_mapper
from pulsepipe.models import VitalSign, LabObservation, LabReport
from pulsepipe.models.clinical_content import PulseClinicalContent

class OBXMapper(HL7v2Mapper):
    def __init__(self):
        self.segment = "OBX"
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing HL7v2 OBXMapper")

    def accepts(self, seg: Segment) -> bool:
        return (seg.id == self.segment)

    def map(self, seg: Segment, content: PulseClinicalContent, cache: Dict[str, Any]):
        self.logger.debug("{self.segment} Segment: {seg}")
        try:
            # The segment is already an OBX segment, so we don't need to get "OBX" from it
            # Instead, directly access the fields we need
            
            # Get values using direct field access
            set_id = seg.get(1)  # OBX-1: Set ID
            value_type = seg.get(2)  # OBX-2: Value Type
            
            # OBX-3: Observation Identifier
            code = seg.get(3, 1)  # Code
            code_text = seg.get(3, 2)  # Text description
            code_system = seg.get(3, 3)  # Coding system
            
            # OBX-5: Observation Value
            value = seg.get(5)
            
            # OBX-6: Units
            units = seg.get(6, 1)
            
            # OBX-7: Reference Range
            ref_range = seg.get(7)
            
            # OBX-8: Abnormal Flags
            abnormal_flag = seg.get(8)
            
            # OBX-14: Date/Time of the Observation
            observation_date = seg.get(14) or cache.get("current_observation_date")
            
            self.logger.info(f"DEBUG OBX: code={code}, text={code_text}, value={value}, unit={units}")
            
            # Determine category
            category = self._determine_observation_category(code, code_text, cache)
            
            if category == 'vital_signs':
                self._map_vital_sign(code, code_system, code_text, value, units, observation_date, content, cache)
            else:
                self._map_lab_observation(code, code_system, code_text, value, units, ref_range, abnormal_flag, observation_date, content, cache)
            
            self.logger.info(f"Mapped OBX: {code} - {value}")
            
        except Exception as e:
            self.logger.exception(f"Error mapping OBX segment: {e}")

    def _determine_observation_category(self, code, code_text, cache):
        context = cache.get("context", "")
        if code and code.upper() in {"BP", "TEMP", "HR", "RR", "O2SAT"}:
            return "vital_signs"
        if code_text and any(term in code_text.upper() for term in ["BLOOD PRESSURE", "TEMPERATURE", "HEART RATE", "RESPIRATORY", "OXYGEN"]):
            return "vital_signs"
        if context.startswith("vital"):
            return "vital_signs"
        return "lab"

    def _map_vital_sign(self, code, system, text, value, unit, ts, content, cache):
        try:
            value_num = float(value) if value else None
        except:
            value_num = value

        vital = VitalSign(
            code=code,
            coding_method=system,
            display=text,
            value=value_num,
            unit=unit,
            timestamp=ts,
            patient_id=cache.get('patient_id'),
            encounter_id=cache.get('encounter_id')
        )
        content.vital_signs.append(vital)

    def _map_lab_observation(self, code, system, text, value, unit, ref, abnormal, ts, content, cache):
        obs = LabObservation(
            observation_id=None,
            code=code,
            coding_method=system,
            name=text,
            description=text,
            value=value,
            unit=unit,
            reference_range=ref,
            abnormal_flag=abnormal,
            result_date=ts,
            interpretation=None,
            status="final"
        )

        # Try to attach to an existing report
        report = next((r for r in content.lab if r.panel_code == cache.get("current_panel_code")), None)
        if report:
            report.observations.append(obs)
        else:
            content.lab.append(LabReport(
                report_id=None,
                lab_type=None,
                code=code,
                coding_method=system,
                panel_name=cache.get("current_panel_name", "LABORATORY PANEL"),
                panel_code=cache.get("current_panel_code", "LAB-PANEL"),
                panel_code_method=system or "L",
                is_panel=True,
                ordering_provider_id=cache.get("ordering_provider_id"),
                performing_lab="Lab",
                report_type="Laboratory",
                collection_date=ts,
                observations=[obs],
                note="",
                patient_id=cache.get("patient_id"),
                encounter_id=cache.get("encounter_id")
            ))

register_mapper(OBXMapper())
