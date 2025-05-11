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

# src/pulsepipe/ingesters/fhir_utils/procedure_mapper.py

"""
PulsePipe — Procedure Mapper for FHIR Resources
"""

from pulsepipe.models.procedure import Procedure, ProcedureProvider
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference, get_code, get_system, get_display, extract_effective_date

@fhir_mapper("Procedure")
class ProcedureMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Procedure"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        procedure = self.parse_procedure(resource, cache)
        content.procedures.append(procedure)
    
    def parse_procedure(self, resource: dict, cache: MessageCache) -> Procedure:
        # Extract basic identifiers
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract procedure code and description
        code = get_code(resource)
        coding_method = get_system(resource)
        description = get_display(resource)
        
        # If no description found in code.coding[].display, try code.text
        if not description and resource.get("code", {}).get("text"):
            description = resource["code"]["text"]
        
        # Extract performance date
        performed_date = None
        if resource.get("performedDateTime"):
            performed_date = resource["performedDateTime"]
        elif resource.get("performedPeriod", {}).get("start"):
            performed_date = resource["performedPeriod"]["start"]
        
        # Extract status
        status = resource.get("status")  # preparation, in-progress, completed, etc.
        
        # Extract performers/providers
        providers = []
        for performer in resource.get("performer", []):
            provider_id = None
            provider_role = None
            
            # Extract provider ID from reference
            if performer.get("actor", {}).get("reference"):
                provider_ref = performer["actor"]["reference"]
                if "Practitioner" in provider_ref:
                    provider_id = provider_ref.split("/")[-1]
            
            # Extract provider role
            if performer.get("function", {}).get("coding"):
                for coding in performer["function"]["coding"]:
                    if coding.get("display"):
                        provider_role = coding["display"]
                        break
                    elif coding.get("code"):
                        provider_role = coding["code"]
                        break
            elif performer.get("function", {}).get("text"):
                provider_role = performer["function"]["text"]
            
            # Only add provider if we have an ID
            if provider_id:
                providers.append(ProcedureProvider(
                    provider_id=provider_id,
                    role=provider_role
                ))
        
        # Create and return the Procedure
        return Procedure(
            code=code,
            coding_method=coding_method,
            description=description,
            performed_date=performed_date,
            status=status,
            providers=providers,
            patient_id=patient_id,
            encounter_id=encounter_id
        )