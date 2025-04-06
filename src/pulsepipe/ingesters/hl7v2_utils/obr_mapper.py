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
import logging
from typing import Dict, Any

from hl7apy.core import Segment
from .base_mapper import HL7v2Mapper, register_mapper

logger = logging.getLogger(__name__)

def safe_get_value(field, default=None):
    """
    Safely extract value from an HL7 field with multiple extraction strategies
    """
    if not field:
        return default
    
    try:
        # Try multiple extraction methods
        extractors = [
            lambda: field.value,
            lambda: field.ce_1.value if hasattr(field, 'ce_1') and field.ce_1 else None,
        ]
        
        for extractor in extractors:
            try:
                value = extractor()
                if value:
                    return value
            except Exception:
                continue
        
        return default
    except Exception as e:
        logger.warning(f"Error extracting field value: {e}")
        return default

class OBRMapper(HL7v2Mapper):
    def accepts(self, segment: Segment) -> bool:
        return segment.name == "OBR"

    def map(self, segment: Segment, content, cache: Dict[str, Any]):
        try:
            # Log detailed segment information
            logger.debug(f"Mapping OBR segment")

            # Extract observation details
            code = safe_get_value(segment.obr_4.ce_1) if hasattr(segment, 'obr_4') else None
            code_text = safe_get_value(segment.obr_4.ce_2) if hasattr(segment, 'obr_4') else None
            code_system = safe_get_value(segment.obr_4.ce_3) if hasattr(segment, 'obr_4') else None

            # Observation date
            observation_date = safe_get_value(segment.obr_7) if hasattr(segment, 'obr_7') else None

            # Update context in cache
            if code:
                cache['current_observation_type'] = code
                cache['current_observation_date'] = observation_date

            logger.debug(f"Successfully mapped OBR: {code}")

        except Exception as e:
            logger.exception(f"Error mapping OBR segment: {e}")
            raise

# Register the mapper
register_mapper(OBRMapper())