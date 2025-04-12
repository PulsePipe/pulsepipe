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

# src/pulsepipe/ingesters/hl7v2_ingester.py

import re
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models import PulseClinicalContent, MessageCache

# Import mappers to ensure registration
from .hl7v2_utils.parser import HL7Message
from .hl7v2_utils.base_mapper import HL7v2Mapper
from .hl7v2_utils.msh_mapper import MSHMapper
from .hl7v2_utils.pid_mapper import PIDMapper
from .hl7v2_utils.obr_mapper import OBRMapper
from .hl7v2_utils.obx_mapper import OBXMapper

class HL7v2Ingester:
    def __init__(self):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing HL7v2Ingester")


    def parse(self, hl7_blob: str) -> list:
        """
        Parse multiple HL7 messages from a single input string.
        
        Args:
            raw_data (str): Raw HL7 message(s)
        
        Returns:
            List of parsed PulseClinicalContent objects
        """

        if not hl7_blob.strip():
            raise ValueError("Empty HL7v2 data received")

        # Normalize all line endings
        normalized = hl7_blob.replace('\r\n', '\r').replace('\n', '\r')

        messages = []

        # Use regex to split on MSH boundaries
        messages = re.split(r'(?=MSH\|)', normalized.strip())
        messages = [msg for msg in messages if msg.strip()]

        # Check that we have at least one valid HL7 message
        if not any(msg.startswith("MSH|") for msg in messages):
            raise ValueError("This is not an HL7 message")

        # Create a list for results
        parsed_contents = []

        for i, msg in enumerate(messages):
            try:
                parsed_msg = HL7Message(msg)
                content = self.parseImp(parsed_msg)
                parsed_contents.append(content)
            except Exception as e:
                self.logger.info(f"❌ Failed parsing or mapping message {i}: {e}")

        return parsed_contents


    def parseImp(self, hl7_message: HL7Message) -> PulseClinicalContent:
        """
        Parse a single HL7v2 message.
        
        Args:
            raw_data (str): Raw HL7 message
        
        Returns:
            PulseClinicalContent object
        """

        try:     
            # Create content template
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

            # Create message cache for tracking context
            cache: MessageCache = {
                "patient_id": None, 
                "encounter_id": None, 
                "current_observation_type": None,
                "current_observation_date": None,
                "resource_index": {}
            }

            self.logger.info(f"\nHL7 Message: {hl7_message}\n")
            # important, implement cache
            for segment in hl7_message.segments:
                segment_id = segment.id
                if segment_id == "MSH":
                    MSHMapper().map(segment, content, cache)
                elif segment_id == "PID":
                    PIDMapper().map(segment, content, cache)
                elif segment_id == "OBR":
                    OBRMapper().map(segment, content, cache)
                elif segment_id == "OBX":
                    OBXMapper().map(segment, content, cache)

            self.logger.debug(f"Finished parsing. Patient: {content.patient}")
            return content

        except Exception as e:
            self.logger.exception("HL7v2 parsing error")
            raise ValueError(f"Failed to parse HL7v2 message: {str(e)}") from e
