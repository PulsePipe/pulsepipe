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

# src/pulsepipe/ingesters/hl7v2_utils/pid_mapper.py

import logging
from datetime import datetime
from typing import Optional, Dict, Any

from hl7apy.core import Segment
from pulsepipe.models import PatientInfo, PatientPreferences
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
            lambda: field.xpn_1.value if hasattr(field, 'xpn_1') and field.xpn_1 else None,
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

class PIDMapper(HL7v2Mapper):
    def accepts(self, segment: Segment) -> bool:
        return segment.name == "PID"

    def map(self, segment: Segment, content, cache: Dict[str, Any]):
        try:
            # Log detailed segment information
            logger.debug(f"Mapping PID segment")
            
            # Identifiers
            identifiers = {}
            
            # PID-3: Patient Identifiers
            if hasattr(segment, 'pid_3') and segment.pid_3:
                for cx in segment.pid_3:
                    id_value = safe_get_value(cx.cx_1) if hasattr(cx, 'cx_1') else None
                    id_type = safe_get_value(cx.cx_5) if hasattr(cx, 'cx_5') else "UNKNOWN"
                    
                    if id_value:
                        identifiers[id_type] = id_value

            # Birth Date
            dob_year = None
            over_90 = False
            if hasattr(segment, 'pid_7') and segment.pid_7:
                dob_str = safe_get_value(segment.pid_7)
                if dob_str:
                    try:
                        dob = datetime.strptime(dob_str, "%Y%m%d")
                        dob_year = dob.year
                        age = datetime.now().year - dob.year
                        over_90 = age >= 90
                    except ValueError:
                        logger.warning(f"Could not parse date: {dob_str}")

            # Gender
            gender = safe_get_value(segment.pid_8) if hasattr(segment, 'pid_8') else None

            # Geographic Area (PID-11)
            geographic_area = None
            if hasattr(segment, 'pid_11') and segment.pid_11:
                addr = segment.pid_11[0]
                city = safe_get_value(addr.xad_3) if hasattr(addr, 'xad_3') else None
                state = safe_get_value(addr.xad_4) if hasattr(addr, 'xad_4') else None
                zip_code = safe_get_value(addr.xad_6) if hasattr(addr, 'xad_6') else None
                
                # Construct geographic area string
                area_parts = [p for p in [city, state, zip_code] if p]
                geographic_area = " ".join(area_parts) if area_parts else None

            # Patient Preferences
            preferred_language = safe_get_value(segment.pid_15) if hasattr(segment, 'pid_15') else None
            marital_status = safe_get_value(segment.pid_16) if hasattr(segment, 'pid_16') else None
            religion = safe_get_value(segment.pid_17) if hasattr(segment, 'pid_17') else None

            preferences = []
            if preferred_language or marital_status or religion:
                preferences.append(PatientPreferences(
                    preferred_language=preferred_language,
                    communication_method=None,
                    requires_interpreter=None,
                    preferred_contact_time=None,
                    notes=f"Marital Status: {marital_status}, Religion: {religion}"
                ))

            # Create PatientInfo
            content.patient = PatientInfo(
                id=identifiers.get("MR") or identifiers.get("UNIQUE"),
                dob_year=dob_year,
                over_90=over_90,
                gender=gender,
                geographic_area=geographic_area,
                identifiers=identifiers if identifiers else None,
                preferences=preferences if preferences else None
            )

            # Update cache with patient ID
            if content.patient.id:
                cache['patient_id'] = content.patient.id

            logger.debug(f"Successfully mapped patient: {content.patient}")

        except Exception as e:
            logger.exception(f"Error mapping PID segment: {e}")
            raise

# Register the mapper
register_mapper(PIDMapper())