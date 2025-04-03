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

import logging
from hl7apy.parser import parse_message
from pulsepipe.models import PulseClinicalContent, MessageCache

# Explicitly import all mappers here to trigger their registration
from .hl7v2_utils import base_mapper
from .hl7v2_utils import pid_mapper  # Always include
# from .hl7v2_utils import obx_mapper  # Uncomment when you implement it
# from .hl7v2_utils import al1_mapper
# from .hl7v2_utils import dg1_mapper
# ... add others as needed

logger = logging.getLogger(__name__)

class HL7v2Ingester:
    def parse(self, raw_data: str) -> PulseClinicalContent:
        if not raw_data.strip():
            raise ValueError("Empty HL7v2 data received")

        cache: MessageCache = {"patient_id": None, "encounter_id": None}
        
        try:
            message = parse_message(raw_data, validation_level="T")
            pid_segment = next(s for s in message.children if s.name == "PID")

            # ✅ Diagnostic prints (put these temporarily)
            print(message.version)
            print([field.name for field in pid_segment.children])

        except Exception as e:
            logger.exception("HL7v2 parsing error")
            raise ValueError("Failed to parse HL7v2 message") from e

        content = PulseClinicalContent(
            patient=None,
            encounter=None,
            vital_signs=[],
            allergies=[],
            immunizations=[],
            diagnoses=[],
            problem_list=[],
            procedures=[],
            medications=[],
            payors=[],
            mar=[],
            notes=[],
            imaging=[],
            lab=[],
            pathology=[],
            diagnostic_test=[],
            microbiology=[],
            blood_bank=[],
            family_history=[],
            social_history=[],
            advance_directives=[],
            functional_status=[],
            order=[],
            implant=[],
        )

        # Iterate over all segments
        for segment in message.children:
            self._map_segment(segment, content, cache)

        return content

    def _map_segment(self, segment, content: PulseClinicalContent, cache: dict):
        # Go through the registry and apply the first matching mapper
        for mapper in base_mapper.MAPPER_REGISTRY:
            if mapper.accepts(segment):
                try:
                    mapper.map(segment, content, cache)
                    logger.debug(f"Mapped segment {segment.name} using {mapper.__class__.__name__}")
                except Exception as e:
                    logger.exception(f"Error mapping segment {segment.name} with {mapper.__class__.__name__}")
                break  # Only one mapper should handle a segment
