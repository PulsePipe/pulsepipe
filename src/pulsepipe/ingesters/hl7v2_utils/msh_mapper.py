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

# src/pulsepipe/ingesters/hl7v2_utils/msh_mapper.py


from typing import Dict, Any
from pulsepipe.utils.log_factory import LogFactory
from .message import Segment
from pulsepipe.models.clinical_content import PulseClinicalContent
#from pulsepipe.models import MessageMetadata
from .base_mapper import HL7v2Mapper, register_mapper


class MSHMapper(HL7v2Mapper):
    def __init__(self):
        self.segment = "MSH"
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing HL7v2 MSHMapper")

    def accepts(self, seg: Segment) -> bool:
        return seg.id == self.segment

    def map(self, seg: Segment, content: PulseClinicalContent, cache: Dict[str, Any]):
        self.logger.debug("{self.segment} Segment: {seg}")
        try:
            #ToDo: Parse for cache like org id, etc..
            #msh = msh_segments[0]
            #get = lambda f, c=1, s=1: msg.get(f"MSH.{f}.{c}.{s}")

            # content.metadata = MessageMetadata(
            #     sending_application=get(3),
            #     sending_facility=get(4),
            #     receiving_application=get(5),
            #     receiving_facility=get(6),
            #     message_datetime=get(7),
            #     message_type=get(9),
            #     message_control_id=get(10),
            #     processing_id=get(11),
            #     version=get(12)
            # )
            seg.id == "MSH"
            #self.logger.info(f"Mapped message metadata: {content.metadata}")

        except Exception as e:
            self.logger.exception(f"Error mapping MSH segment: {e}")
            raise

register_mapper(MSHMapper())
