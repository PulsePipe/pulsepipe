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

# src/pulsepipe/ingesters/hl7v2_utils/pv1_mapper.py

from typing import Dict, Any, List
from pulsepipe.utils.log_factory import LogFactory
from .message import Segment
from .base_mapper import HL7v2Mapper, register_mapper
from pulsepipe.models.encounter import EncounterInfo, EncounterProvider
from pulsepipe.models.clinical_content import PulseClinicalContent

# - PV1 (Patient Visit)
#   - PV1-1: Set ID
#   - PV1-2: Patient Class
#   - PV1-3: Assigned Patient Location
#   - PV1-4: Admission Type
#   - PV1-5: Preadmit Number
#   - PV1-6: Prior Patient Location
#   - PV1-7: Attending Doctor
#   - PV1-8: Referring Doctor
#   - PV1-9: Consulting Doctor
#   - PV1-10: Hospital Service
#   - PV1-11: Temporary Location
#   - PV1-12: Preadmit Test Indicator
#   - PV1-13: Re-admission Indicator
#   - PV1-14: Admit Source
#   - PV1-15: Ambulatory Status
#   - PV1-16: VIP Indicator
#   - PV1-17: Admitting Doctor
#   - PV1-18: Patient Type
#   - PV1-19: Visit Number
#   - PV1-20: Financial Class
#   - PV1-21: Charge Price Indicator
#   - PV1-22: Courtesy Code
#   - PV1-23: Credit Rating
#   - PV1-24: Contract Code
#   - PV1-25: Contract Effective Date
#   - PV1-26: Contract Amount
#   - PV1-27: Contract Period
#   - PV1-28: Interest Code
#   - PV1-29: Transfer to Bad Debt Code
#   - PV1-30: Transfer to Bad Debt Date
#   - PV1-31: Bad Debt Agency Code
#   - PV1-32: Bad Debt Transfer Amount
#   - PV1-33: Bad Debt Recovery Amount
#   - PV1-34: Delete Account Indicator
#   - PV1-35: Delete Account Date
#   - PV1-36: Discharge Disposition
#   - PV1-37: Discharged to Location
#   - PV1-38: Diet Type
#   - PV1-39: Servicing Facility
#   - PV1-40: Bed Status
#   - PV1-41: Account Status
#   - PV1-42: Pending Location
#   - PV1-43: Prior Temporary Location
#   - PV1-44: Admit Date/Time
#   - PV1-45: Discharge Date/Time
#   - PV1-46: Current Patient Balance
#   - PV1-47: Total Charges
#   - PV1-48: Total Adjustments
#   - PV1-49: Total Payments
#   - PV1-50: Alternate Visit ID
#   - PV1-51: Visit Indicator
#   - PV1-52: Other Healthcare Provider
class PV1Mapper(HL7v2Mapper):
    def __init__(self):
        self.segment = "PV1"
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing HL7v2 PV1Mapper")

    def accepts(self, seg: Segment) -> bool:
        return (seg.id == self.segment)

    def map(self, seg: Segment, content: PulseClinicalContent, cache: Dict[str, Any]):
        self.logger.debug("{self.segment} Segment: {seg}")
        try:
            pv1 = seg
            enc: EncounterInfo = None
            providers: List[EncounterProvider]

            enc.id = pv1.get(19)

            self.logger.info(f"Mapped PV1: {enc.id} - {providers}")
            content.encounter = enc
    
        except Exception as e:
            self.logger.exception(f"Error mapping PV1 segment: {e}")

register_mapper(PV1Mapper())

# class EncounterProvider(BaseModel):
#     """
#     Represents a provider involved in a clinical encounter.
#     """
#     id: Optional[str]
#     type_code: Optional[str]
#     coding_method: Optional[str]
#     name: Optional[str]
#     specialty: Optional[str]

# class EncounterInfo(BaseModel):
#     """
#     Represents a clinical encounter, such as an inpatient admission, outpatient visit,
#     or emergency room encounter.
#     """
#     id: Optional[str]
#     admit_date: Optional[str]
#     discharge_date: Optional[str]
#     encounter_type: Optional[str]
#     type_coding_method: Optional[str]
#     location: Optional[str]
#     reason_code: Optional[str]
#     reason_coding_method: Optional[str]
#     providers: Optional[List[EncounterProvider]] = []
#     visit_type: Optional[str]
#     patient_id: Optional[str]