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

from datetime import datetime
from pulsepipe.models import PatientInfo, PatientPreferences, PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper

@fhir_mapper("Patient")
class PatientMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Patient" 
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        patient_id = resource.get("id")
        gender = resource.get("gender")

        # Date of Birth & Over-90 check
        birth_date = resource.get("birthDate")
        dob_year = None
        over_90 = False
        geographic_area = None
    
        if birth_date:
            try:
                year = int(birth_date.split("-")[0])
                age = datetime.now().year - year
                if age >= 90:
                    over_90 = True
                else:
                    dob_year = year
            except Exception:
                pass

        # Identifiers
        identifiers = {}
        for identifier in resource.get("identifier", []):
            system = identifier.get("system")
            value = identifier.get("value")
            if system and value:
                identifiers[system] = value

        # Preferences (FHIR Communication)
        preferences = []
        for comm in resource.get("communication", []):
            preferred_language = None
            if "language" in comm:
                preferred_language = comm["language"].get("text") or \
                                     comm["language"].get("coding", [{}])[0].get("display")

            requires_interpreter = comm.get("preferred", False)

            preferences.append(PatientPreferences(
                preferred_language=preferred_language,
                communication_method=None,  # Extend later if needed
                requires_interpreter=requires_interpreter,
                preferred_contact_time=None,
                notes=None
            ))

        # Assign into content
        content.patient = PatientInfo(
            id=patient_id,
            gender=gender,
            dob_year=dob_year,
            over_90=over_90,
            identifiers=identifiers,
            preferences=preferences or None,
            geographic_area=geographic_area,
        )
