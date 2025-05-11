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

# src/pulsepipe/ingesters/fhir_utils/provenance_mapper.py

from datetime import datetime
from pulsepipe.models.provenance import Provenance, ProvenanceAgent, ProvenanceEntity
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper

@fhir_mapper("Provenance")
class ProvenanceMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "Provenance"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR Provenance resource to the PulsePipe Provenance model.
        
        Args:
            resource: The FHIR Provenance resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        provenance_id = resource.get("id")
        
        # Extract target (what the provenance is about)
        target_id = None
        target_type = None
        if resource.get("target") and len(resource.get("target", [])) > 0:
            target_ref = resource.get("target", [])[0].get("reference")
            if target_ref:
                parts = target_ref.split("/")
                if len(parts) >= 2:
                    target_type = parts[-2]
                    target_id = parts[-1]
        
        # Extract occurred period
        occurred_start = None
        occurred_end = None
        
        # Process either occurrenceDateTime or occurrencePeriod
        if resource.get("occurredDateTime"):
            time_str = resource.get("occurredDateTime")
            if time_str:
                try:
                    occurred_start = datetime.fromisoformat(time_str.replace('Z', '+00:00'))
                    occurred_end = occurred_start  # Same time for point-in-time
                except (ValueError, TypeError):
                    pass
        elif resource.get("occurredPeriod"):
            period = resource.get("occurredPeriod", {})
            start_str = period.get("start")
            end_str = period.get("end")
            if start_str:
                try:
                    occurred_start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
            if end_str:
                try:
                    occurred_end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                except (ValueError, TypeError):
                    pass
        
        # Extract recorded time
        recorded = None
        recorded_str = resource.get("recorded")
        if recorded_str:
            try:
                recorded = datetime.fromisoformat(recorded_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        # Extract policy
        policy = None
        if resource.get("policy") and len(resource.get("policy", [])) > 0:
            policy = resource.get("policy", [])[0]
        
        # Extract location
        location = None
        location_ref = resource.get("location", {}).get("reference")
        if location_ref:
            location = location_ref.split("/")[-1]
        
        # Extract reason
        reason = None
        reason_code = None
        reason_system = None
        if resource.get("reason") and len(resource.get("reason", [])) > 0:
            reason_obj = resource.get("reason", [])[0]
            codings = reason_obj.get("coding", [])
            if codings and len(codings) > 0:
                coding = codings[0]
                reason = reason_obj.get("text") or coding.get("display")
                reason_code = coding.get("code")
                reason_system = coding.get("system")
        
        # Extract activity
        activity = None
        activity_code = None
        activity_system = None
        if resource.get("activity"):
            activity_obj = resource.get("activity", {})
            codings = activity_obj.get("coding", [])
            if codings and len(codings) > 0:
                coding = codings[0]
                activity = activity_obj.get("text") or coding.get("display")
                activity_code = coding.get("code")
                activity_system = coding.get("system")
        
        # Process agents
        agents = []
        for agent in resource.get("agent", []):
            # Extract agent who (reference)
            agent_id = None
            agent_type = None
            who_ref = agent.get("who", {}).get("reference")
            if who_ref:
                parts = who_ref.split("/")
                if len(parts) >= 2:
                    agent_type = parts[-2]
                    agent_id = parts[-1]
            
            # Extract agent display name
            name = agent.get("who", {}).get("display")
            
            # Extract role
            role = None
            role_code = None
            role_system = None
            if agent.get("role") and len(agent.get("role", [])) > 0:
                role_obj = agent.get("role", [])[0]
                codings = role_obj.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    role = role_obj.get("text") or coding.get("display")
                    role_code = coding.get("code")
                    role_system = coding.get("system")
            
            # Extract onBehalfOf
            on_behalf_of = None
            on_behalf_of_type = None
            on_behalf_ref = agent.get("onBehalfOf", {}).get("reference")
            if on_behalf_ref:
                parts = on_behalf_ref.split("/")
                if len(parts) >= 2:
                    on_behalf_of_type = parts[-2]
                    on_behalf_of = parts[-1]
            
            agents.append(ProvenanceAgent(
                id=agent_id,
                type=agent_type,
                role=role,
                role_code=role_code,
                role_system=role_system,
                name=name,
                onBehalfOf=on_behalf_of,
                onBehalfOf_type=on_behalf_of_type
            ))
        
        # Process entities
        entities = []
        for entity in resource.get("entity", []):
            # Extract entity what (reference)
            entity_id = None
            entity_type = None
            what_ref = entity.get("what", {}).get("reference")
            if what_ref:
                parts = what_ref.split("/")
                if len(parts) >= 2:
                    entity_type = parts[-2]
                    entity_id = parts[-1]
            
            # Extract role
            role = entity.get("role")
            
            # Description or display name
            description = entity.get("what", {}).get("display")
            
            entities.append(ProvenanceEntity(
                id=entity_id,
                role=role,
                type=entity_type,
                reference=what_ref,
                description=description
            ))
        
        # Extract signatures
        signatures = []
        for signature in resource.get("signature", []):
            signatures.append({
                "type": signature.get("type", [{}])[0].get("code"),
                "when": signature.get("when"),
                "who": signature.get("who", {}).get("reference"),
                "targetFormat": signature.get("targetFormat"),
                "sigFormat": signature.get("sigFormat"),
                # Not storing actual data as it's large and typically not needed for search
            })
        
        # Create Provenance object
        provenance = Provenance(
            id=provenance_id,
            target_id=target_id,
            target_type=target_type,
            occurred_start=occurred_start,
            occurred_end=occurred_end,
            recorded=recorded,
            policy=policy,
            location=location,
            reason=reason,
            reason_code=reason_code,
            reason_system=reason_system,
            activity=activity,
            activity_code=activity_code,
            activity_system=activity_system,
            agents=agents,
            entities=entities,
            signature=signatures
        )
        
        # Add to content
        if not hasattr(content, 'provenances'):
            content.provenances = []
        content.provenances.append(provenance)
