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

from hl7apy.core import Segment
from pulsepipe.models import PatientInfo, PatientPreferences
from .base_mapper import HL7v2Mapper, register_mapper
from datetime import datetime

def get_field_value(field):
    if not field:
        return None
    if hasattr(field, "ce_1") and field.ce_1:
        return field.ce_1.value
    return field.value


class PIDMapper(HL7v2Mapper):
    def accepts(self, segment: Segment) -> bool:
        return segment.name == "PID"

    def map(self, segment: Segment, content):
        identifiers = {}

        # PID-3 - Identifiers
        if hasattr(segment, "pid_3") and segment.pid_3:
            for cx in segment.pid_3:
                id_value = cx.cx_1.value if cx.cx_1 else None
                id_type = cx.cx_5.value if cx.cx_5 else "UNKNOWN"
                if id_value:
                    identifiers[id_type] = id_value

        # PID-7 - DOB
        dob_year = None
        over_90 = False
        if hasattr(segment, "pid_7") and segment.pid_7 and segment.pid_7.ts_1:
            dob_str = segment.pid_7.ts_1.value
            if dob_str:
                try:
                    dob = datetime.strptime(dob_str[:8], "%Y%m%d")
                    dob_year = dob.year
                    age = datetime.now().year - dob.year
                    over_90 = age >= 90
                except Exception:
                    dob_year = None
                    over_90 = False

        # PID-8 - Gender
        gender = segment.pid_8.value if hasattr(segment, "pid_8") else None

        # PID-11 - Geographic area
        geographic_area = None
        if hasattr(segment, "pid_11") and segment.pid_11:
            addr = segment.pid_11[0]
            zip_code = addr.xad_6.value if addr.xad_6 else None
            state = addr.xad_5.value if addr.xad_5 else None
            if zip_code and state:
                geographic_area = f"{zip_code} {state}"
            elif zip_code:
                geographic_area = zip_code
            elif state:
                geographic_area = state

        # --- PatientPreferences ---
        preferences = PatientPreferences(
            preferred_language=segment.pid_15.value if segment.pid_15 else None,
            communication_method="Phone" if (hasattr(segment, "pid_13") and segment.pid_13) else None,
            requires_interpreter=None,
            preferred_contact_time=None,
            notes=None
        )

        content.patient = PatientInfo(
            id=identifiers.get("MR") or identifiers.get("UNKNOWN") or None,
            dob_year=dob_year,
            over_90=over_90,
            gender=gender,
            geographic_area=geographic_area,
            identifiers=identifiers if identifiers else None,
            preferences=[preferences] if any(vars(preferences).values()) else None
        )

register_mapper(PIDMapper())
