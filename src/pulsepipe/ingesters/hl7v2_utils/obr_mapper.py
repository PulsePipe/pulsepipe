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

# src/pulsepipe/ingesters/hl7v2_utils/obr_mapper.py

from typing import Dict, Any
from pulsepipe.utils.log_factory import LogFactory
from .message import Segment
from .base_mapper import HL7v2Mapper, register_mapper
from pulsepipe.models import LabReport, LabObservation
from pulsepipe.models.clinical_content import PulseClinicalContent


class OBRMapper(HL7v2Mapper):
    def __init__(self):
        self.segment = "OBR"
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing HL7v2 OBRMapper")

    def accepts(self, seg: Segment) -> bool:
        return (seg.id == self.segment)

    def map(self, seg: Segment, content: PulseClinicalContent, cache: Dict[str, Any]):
        self.logger.debug("{self.segment} Segment: {seg}")
        try:
            get = lambda f, c=1, s=1: seg.get(f"{f}.{c}.{s}")

            observation_id = get(3)
            panel_code = get(4, 1)
            panel_text = get(4, 2)
            panel_system = get(4, 3)
            collection_date = get(7)

            # Save to cache for OBXMapper
            cache["current_panel_code"] = panel_code
            cache["current_panel_name"] = panel_text
            cache["current_observation_date"] = collection_date
            cache["context"] = panel_text or ""

            report = LabReport(
                report_id=None,
                lab_type=None,
                code=panel_code,
                coding_method=panel_system or "L",
                panel_name=panel_text,
                panel_code=panel_code,
                panel_code_method=panel_system or "L",
                is_panel=True,
                ordering_provider_id=cache.get("ordering_provider_id"),
                performing_lab="Lab",
                report_type="Laboratory",
                collection_date=collection_date,
                observations=[],
                note="",
                patient_id=cache.get("patient_id"),
                encounter_id=cache.get("encounter_id")
            )
            content.lab.append(report)
            self.logger.info(f"Mapped OBR {panel_code} - {panel_text}")

        except Exception as e:
            self.logger.exception("Error mapping OBR segment")

register_mapper(OBRMapper())
