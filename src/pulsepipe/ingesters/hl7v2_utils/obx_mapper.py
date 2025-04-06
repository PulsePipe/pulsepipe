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
import logging
from typing import Dict, Any

from hl7apy.core import Segment
from pulsepipe.models import VitalSign, LabReport, LabObservation
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

class OBXMapper(HL7v2Mapper):
    def accepts(self, segment: Segment) -> bool:
        return segment.name == "OBX"

    def map(self, segment: Segment, content, cache: Dict[str, Any]):
        try:
            # Log detailed segment information
            logger.debug(f"Mapping OBX segment")

            # Extract observation details
            observation_type = safe_get_value(segment.obx_2)  # Value type
            code = safe_get_value(segment.obx_3.ce_1) if hasattr(segment, 'obx_3') else None
            code_text = safe_get_value(segment.obx_3.ce_2) if hasattr(segment, 'obx_3') else None
            code_system = safe_get_value(segment.obx_3.ce_3) if hasattr(segment, 'obx_3') else None
            
            # Extract value
            value = safe_get_value(segment.obx_5)
            units = safe_get_value(segment.obx_6.ce_1) if hasattr(segment, 'obx_6') else None
            
            # Reference range
            ref_range = None
            if hasattr(segment, 'obx_7'):
                ref_range = safe_get_value(segment.obx_7)
            
            # Abnormal flags
            abnormal_flag = safe_get_value(segment.obx_8) if hasattr(segment, 'obx_8') else None

            # Determine observation category based on code
            category_map = {
                'BP': 'vital_signs',
                'TEMP': 'vital_signs',
                'HR': 'vital_signs',
                'WBC': 'lab',
                'RBC': 'lab',
                'HGB': 'lab'
            }
            category = category_map.get(code, 'unknown')

            # Create appropriate model based on category
            if category == 'vital_signs':
                vital_sign = VitalSign(
                    code=code,
                    coding_method=code_system,
                    display=code_text,
                    value=value,
                    unit=units,
                    timestamp=None,  # Consider extracting from OBR segment
                    patient_id=cache.get('patient_id'),
                    encounter_id=None  # You might want to track this from OBR
                )
                content.vital_signs.append(vital_sign)
            
            elif category == 'lab':
                lab_obs = LabObservation(
                    code=code,
                    coding_method=code_system,
                    name=code_text,
                    description=code_text,
                    value=str(value),
                    unit=units,
                    reference_range=ref_range,
                    abnormal_flag=abnormal_flag,
                    result_date=None,  # Consider extracting from OBR segment
                )
                
                # Check if a LabReport already exists, if not create one
                if not content.lab:
                    content.lab.append(LabReport(
                        report_id=None,
                        lab_type=None,
                        code=None,
                        coding_method=None,
                        panel_name=None,
                        observations=[lab_obs],
                        patient_id=cache.get('patient_id')
                    ))
                else:
                    content.lab[0].observations.append(lab_obs)

            logger.debug(f"Successfully mapped OBX: {code} - {value}")

        except Exception as e:
            logger.exception(f"Error mapping OBX segment: {e}")
            raise

# Register the mapper
register_mapper(OBXMapper())