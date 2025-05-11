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

# src/pulsepipe/ingesters/fhir_utils/care_team_mapper.py

from datetime import datetime
from pulsepipe.models.care_team import CareTeam, CareTeamParticipant
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("CareTeam")
class CareTeamMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "CareTeam"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR CareTeam resource to the PulsePipe CareTeam model.
        
        Args:
            resource: The FHIR CareTeam resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        care_team_id = resource.get("id")
        status = resource.get("status")
        team_name = resource.get("name")
        
        # Extract patient reference
        patient_id = None
        subject = resource.get("subject", {})
        if subject:
            patient_id = extract_patient_reference(resource)
        
        # Extract encounter reference
        encounter_id = None
        encounter = resource.get("encounter", {})
        if encounter:
            encounter_id = extract_encounter_reference(resource)
        
        # Extract period
        period_start = None
        period_end = None
        period = resource.get("period", {})
        if period:
            start_str = period.get("start")
            end_str = period.get("end")
            if start_str:
                try:
                    period_start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
            if end_str:
                try:
                    period_end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
        
        # Extract category
        category = None
        category_code = None
        category_system = None
        if resource.get("category"):
            for cat in resource.get("category", []):
                codings = cat.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    category = cat.get("text") or coding.get("display")
                    category_code = coding.get("code")
                    category_system = coding.get("system")
                    break
        
        # Extract reason
        reason = None
        reason_code = None
        reason_system = None
        if resource.get("reasonCode"):
            for reason_obj in resource.get("reasonCode", []):
                codings = reason_obj.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    reason = reason_obj.get("text") or coding.get("display")
                    reason_code = coding.get("code")
                    reason_system = coding.get("system")
                    break
        
        # Extract managing organization
        managing_organization = None
        if resource.get("managingOrganization"):
            for org in resource.get("managingOrganization", []):
                ref = org.get("reference")
                if ref:
                    managing_organization = ref.split("/")[-1]
                    break
        
        # Extract notes
        notes = None
        if resource.get("note"):
            notes_list = []
            for note in resource.get("note", []):
                if note.get("text"):
                    notes_list.append(note.get("text"))
            if notes_list:
                notes = "\n".join(notes_list)
        
        # Extract identifiers
        identifiers = {}
        for identifier in resource.get("identifier", []):
            system = identifier.get("system")
            value = identifier.get("value")
            if system and value:
                identifiers[system] = value
        
        # Process participants
        participants = []
        for participant in resource.get("participant", []):
            member_ref = participant.get("member", {}).get("reference")
            member_type = None
            member_id = None
            
            if member_ref:
                parts = member_ref.split("/")
                if len(parts) >= 2:
                    member_type = parts[-2]
                    member_id = parts[-1]
            
            # Extract role
            role = None
            role_code = None
            role_system = None
            if participant.get("role"):
                for role_obj in participant.get("role", []):
                    codings = role_obj.get("coding", [])
                    if codings and len(codings) > 0:
                        coding = codings[0]
                        role = role_obj.get("text") or coding.get("display")
                        role_code = coding.get("code")
                        role_system = coding.get("system")
                        break
            
            # Extract member name - use display name from member reference
            name = participant.get("member", {}).get("display")
            
            # Extract onBehalfOf
            on_behalf_of = None
            on_behalf_ref = participant.get("onBehalfOf", {}).get("reference")
            if on_behalf_ref:
                on_behalf_of = on_behalf_ref.split("/")[-1]
            
            # Extract period
            part_period_start = None
            part_period_end = None
            part_period = participant.get("period", {})
            if part_period:
                start_str = part_period.get("start")
                end_str = part_period.get("end")
                if start_str:
                    try:
                        part_period_start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        pass
                if end_str:
                    try:
                        part_period_end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        pass
            
            participants.append(CareTeamParticipant(
                id=member_id,
                role=role,
                role_code=role_code,
                role_system=role_system,
                name=name,
                member_type=member_type,
                period_start=part_period_start,
                period_end=part_period_end,
                onBehalfOf=on_behalf_of
            ))
        
        # Create CareTeam object
        care_team = CareTeam(
            id=care_team_id,
            status=status,
            name=team_name,
            patient_id=patient_id,
            encounter_id=encounter_id,
            period_start=period_start,
            period_end=period_end,
            category=category,
            category_code=category_code,
            category_system=category_system,
            reason=reason,
            reason_code=reason_code,
            reason_system=reason_system,
            managing_organization=managing_organization,
            participants=participants,
            notes=notes,
            identifiers=identifiers
        )
        
        # Add to content
        if not hasattr(content, 'care_teams'):
            content.care_teams = []
        content.care_teams.append(care_team)
