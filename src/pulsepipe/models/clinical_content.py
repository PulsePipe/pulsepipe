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

from typing import List, Optional
from pydantic import BaseModel

from .patient import PatientInfo
from .encounter import EncounterInfo
from .vital_sign import VitalSign
from .allergy import Allergy
from .immunization import Immunization
from .diagnosis import Diagnosis
from .problem import Problem
from .procedure import Procedure
from .medication import Medication
from .mar import MAR
from .payor import Payor
from .note import Note
from .imaging import ImagingReport
from .lab import LabReport
from .microbiology import MicrobiologyReport
from .blood_bank import BloodBankReport
from .family_history import FamilyHistory
from .social_history import SocialHistory
from .diagnostic_test import DiagnosticTest
from .pathology import PathologyReport
from .advance_directive import AdvanceDirective
from .functional_status import FunctionalStatus
from .order import Order
from .implant import Implant

class PulseClinicalContent(BaseModel):
    patient: Optional[PatientInfo]
    encounter: Optional[EncounterInfo]
    vital_signs: List[VitalSign] = []
    allergies: List[Allergy] = []
    immunizations: List[Immunization] = []
    diagnoses: List[Diagnosis] = []
    problem_list: List[Problem] = []
    procedures: List[Procedure] = []
    medications: List[Medication] = []
    payors: List[Payor] = []
    mar: List[MAR] = []
    notes: List[Note] = []
    imaging: List[ImagingReport] = []
    lab: List[LabReport] = []
    pathology: List[PathologyReport] = []
    diagnostic_test: List[DiagnosticTest] = []
    microbiology: List[MicrobiologyReport] = []
    blood_bank: List[BloodBankReport] = []
    family_history: List[FamilyHistory] = []
    social_history: List[SocialHistory] = []
    advance_directives: List[AdvanceDirective] = []
    functional_status: List[FunctionalStatus] = []
    order: List[Order] = []
    implant: List[Implant] = []