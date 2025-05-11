# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Chunk, Embed. Healthcare Data, AI-Ready with RAG.
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

# src/pulsepipe/models/clinical_content.py

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
from .device import Device
from .document_reference import DocumentReference
from .supply_delivery import SupplyDelivery
from .care_team import CareTeam
from .care_plan import CarePlan
from .provenance import Provenance
from .location import Location
from .organization import Organization
from .practitioner import Practitioner
from .practitioner_role import PractitionerRole
# Note: Claim and ExplanationOfBenefit are in the billing.py model file

class PulseClinicalContent(BaseModel):
    """
    Comprehensive container for all clinical content related to a patient.
    
    This model serves as the core clinical data structure in PulsePipe,
    aggregating patient demographic information, encounter details, and
    all clinical data elements that comprise a patient's medical record.
    
    It provides a standardized format for normalized healthcare data
    regardless of the original source format (HL7, FHIR, CDA, etc.),
    making it ideal for downstream AI processing and analytics.
    """
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
    # New model fields
    devices: List[Device] = []
    document_references: List[DocumentReference] = []
    supply_deliveries: List[SupplyDelivery] = []
    care_teams: List[CareTeam] = []
    care_plans: List[CarePlan] = []
    provenances: List[Provenance] = []
    locations: List[Location] = []
    organizations: List[Organization] = []
    practitioners: List[Practitioner] = []
    practitioner_roles: List[PractitionerRole] = []
    claims: List = []  # Type will be resolved at runtime


    """
    PulseClinicalContent summary method implementation

    This should be added to your existing PulseClinicalContent model class.
    """
    def summary(self) -> str:
        """
        Generate a human-friendly summary of the clinical content.
        
        Returns:
            str: A formatted summary string with emoji indicators
        """
        # Domain-specific emoji mapping - expanded for your complete model
        domain_emoji = {
            # Core clinical entities
            "patient": "🧑‍⚕️",
            "encounter": "🏥",
            "vital_signs": "❤️‍🔥",
            "allergies": "🚫🌿",
            "immunizations": "💉",
            "diagnoses": "🩺",
            "problem_list": "📝",
            "procedures": "🛠️",
            "medications": "💊",
            "payors": "💰",
            "mar": "⏱️",  # Medication Administration Record
            "notes": "🗒️",

            # Diagnostic reports
            "imaging": "📷",
            "lab": "🧪",
            "pathology": "🔬",
            "diagnostic_test": "📊",
            "microbiology": "🦠",
            "blood_bank": "🩸",

            # History and status
            "family_history": "👨‍👩‍👧‍👦",
            "social_history": "🏠",
            "advance_directives": "📜",
            "functional_status": "🚶",

            # Other
            "order": "✅",
            "implant": "🦿",
            "devices": "🔌",
            "document_references": "📄",
            "supply_deliveries": "📦",
            "care_teams": "👥",
            "care_plans": "📋",
            "provenances": "🔍",
            "locations": "📍",
            "organizations": "🏢",
            "practitioners": "⚕️",
            "practitioner_roles": "👨‍⚕️",
        }

        summary_parts = []
        
        # Patient information - adapted for PatientInfo model structure
        if self.patient:
            patient = self.patient
            patient_id = getattr(patient, "id", None)
            
            # Determine age representation
            age_info = ""
            if getattr(patient, "over_90", False):
                age_info = "90+ years"
            elif hasattr(patient, "dob_year") and patient.dob_year:
                import datetime
                current_year = datetime.datetime.now().year
                age = current_year - patient.dob_year
                age_info = f"{age} years"
            
            # Get gender if available
            gender = getattr(patient, "gender", "")
            
            # Geographic area
            geo_area = getattr(patient, "geographic_area", "")
            
            # Build patient description
            patient_parts = []
            if patient_id:
                patient_parts.append(f"ID: {patient_id}")
            if gender:
                patient_parts.append(gender)
            if age_info:
                patient_parts.append(age_info)
            if geo_area:
                patient_parts.append(f"from {geo_area}")
            
            if patient_parts:
                summary_parts.append(f"👤 Patient: {', '.join(patient_parts)}")
            else:
                summary_parts.append("👤 Patient: [Limited Info]")
        
        # Encounter information
        if self.encounter:
            encounter = self.encounter
            encounter_type = getattr(encounter, "type", "")
            status = getattr(encounter, "status", "")
            
            encounter_desc = "Encounter"
            if encounter_type:
                encounter_desc += f": {encounter_type}"
            if status:
                encounter_desc += f" ({status})"
                
            summary_parts.append(f"🏥 {encounter_desc}")
        
        # Count and summarize all clinical content lists
        for attr_name, attr_value in self.__dict__.items():
            # Skip patient and encounter (already processed)
            if attr_name in ["patient", "encounter"]:
                continue
                
            # Process lists with items
            if isinstance(attr_value, list) and attr_value:
                count = len(attr_value)
                if count > 0:
                    # Get emoji for this domain
                    emoji = domain_emoji.get(attr_name, "📄")
                    
                    # Format attribute name for display (convert snake_case to Title Case)
                    display_name = " ".join(word.capitalize() for word in attr_name.split("_"))
                    
                    summary_parts.append(f"{emoji} {count} {display_name}")
        
        # If no content, provide default message
        if not summary_parts:
            return "❌ No clinical content found"
        
        return "✅ " + " | ".join(summary_parts)
