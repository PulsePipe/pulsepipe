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

from datetime import datetime
from typing import Dict, Any

from .message import Segment
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models import PatientInfo, PatientPreferences
from pulsepipe.models.clinical_content import PulseClinicalContent
from .base_mapper import HL7v2Mapper, register_mapper


class PIDMapper(HL7v2Mapper):
    def __init__(self):
        self.segment = "PID"
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing HL7v2 PIDMapper")

    def accepts(self, seg: Segment) -> bool:
        return (seg.id == self.segment)
# - PID (Patient Identification)
#   - PID-1: Set ID
#   - PID-2: Patient ID (External)
#   - PID-3: Patient Identifier List
#   - PID-4: Alternate Patient ID
#   - PID-5: Patient Name
#   - PID-6: Mother's Maiden Name
#   - PID-7: Date of Birth
#   - PID-8: Sex
#   - PID-9: Patient Alias
#   - PID-10: Race
#   - PID-11: Patient Address
#   - PID-12: County Code
#   - PID-13: Phone Number - Home
#   - PID-14: Phone Number - Business
#   - PID-15: Primary Language
#   - PID-16: Marital Status
#   - PID-17: Religion
#   - PID-18: Patient Account Number
#   - PID-19: SSN Number
#   - PID-20: Driver's License Number
#   - PID-21: Mother's Identifier
#   - PID-22: Ethnic Group
#   - PID-23: Birth Place
#   - PID-24: Multiple Birth Indicator
#   - PID-25: Birth Order
#   - PID-26: Citizenship
#   - PID-27: Veterans Military Status
#   - PID-28: Nationality Code
#   - PID-29: Patient Death Date and Time
#   - PID-30: Patient Death Indicator
    def map(self, seg: Segment, content: PulseClinicalContent, cache: Dict[str, Any]):
        self.logger.debug("{self.segment} Segment: {seg}")
        try:
            pid = seg
            n = 0
            for field in pid.fields:
                n = n + 1

            get = lambda f, c=1, s=1: seg.get(f"{f}.{c}.{s}")
            # Identifiers (PID-3)
            id = None
            identifiers = {}
            reps = pid.fields[3] if len(pid.fields) > 3 else []
            for rep in reps:
                if len(rep) == 1:
                    id = str(rep)
                if rep and len(rep) >= 5:
                    id_val = rep[0][0] if rep[0] else None
                    id_type = rep[4][0] if len(rep) > 4 and rep[4] else "UNKNOWN"
                    if id_val:
                        identifiers[id_type] = id_val
                
            if id == None:
                id = identifiers.get("MR") or identifiers.get("UNIQUE")
                if id == None:
                    id = pid.fields[3] 

            # Birth date (PID-7)
            dob_str = get(7)
            dob_year = None
            over_90 = False
            if dob_str:
                try:
                    dob = datetime.strptime(dob_str, "%Y%m%d")
                    dob_year = dob.year
                    over_90 = datetime.now().year - dob_year >= 90
                except Exception as e:
                    self.logger.exception(f"Could not parse DOB: {dob_str} ({e})")

            # Gender (PID-8)
            gender = None
            if len(seg.fields) > 8:
                gender_field = seg.fields[8]
                if gender_field and gender_field.repetitions:
                    gender = str(gender_field.repetitions[0])
                    # Clean up any extra characters if needed
                    if gender:
                        gender = gender.strip()

            # Geographic area (PID-11)
            geo = pid.fields[11][0] if len(pid) > 11 and pid[11] else []
            geo_parts = [geo[2][0] if len(geo) > 2 and geo[2] else None,
                         geo[3][0] if len(geo) > 3 and geo[3] else None,
                         geo[5][0] if len(geo) > 5 and geo[5] else None]
            geographic_area = " ".join([p for p in geo_parts if p]) or None

            preferred_language = pid.get(15)
            marital_status     = pid.get(16)
            religion           = pid.get(17)

            preferences = []
            if preferred_language or marital_status or religion:
                preferences.append(PatientPreferences(
                    preferred_language=preferred_language,
                    communication_method=None,
                    requires_interpreter=None,
                    preferred_contact_time=None,
                    notes=f"Marital Status: {marital_status}, Religion: {religion}"
                ))

            content.patient = PatientInfo(
                id=id,
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

            self.logger.info(f"Mapped patient: id={content.patient.id}, preferences={content.patient.preferences}")

        except Exception as e:
            self.logger.exception(f"Error mapping PID segment: {e}")
            raise

# Register the mapper
register_mapper(PIDMapper())
