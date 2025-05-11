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

# src/pulsepipe/ingesters/fhir_utils/care_plan_mapper.py

from datetime import datetime
from pulsepipe.models.care_plan import CarePlan, CarePlanActivity
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("CarePlan")
class CarePlanMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "CarePlan"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        """
        Maps a FHIR CarePlan resource to the PulsePipe CarePlan model.
        
        Args:
            resource: The FHIR CarePlan resource
            content: The PulseClinicalContent instance to update
            cache: The MessageCache for reference resolution
        """
        # Extract core data
        care_plan_id = resource.get("id")
        status = resource.get("status")
        intent = resource.get("intent")
        title = resource.get("title")
        description = resource.get("description")
        
        # Extract patient reference
        patient_id = extract_patient_reference(resource)
        
        # Extract encounter reference
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
        
        # Extract created date
        created = None
        created_str = resource.get("created")
        if created_str:
            try:
                created = datetime.fromisoformat(created_str.replace('Z', '+00:00'))
            except (ValueError, TypeError):
                pass
        
        # Extract author
        author = None
        author_type = None
        author_ref = resource.get("author", {}).get("reference")
        if author_ref:
            parts = author_ref.split("/")
            if len(parts) >= 2:
                author_type = parts[-2]
                author = parts[-1]
        
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
        
        # Extract care team reference
        care_team_id = None
        care_team_ref = None
        if resource.get("careTeam"):
            for team in resource.get("careTeam", []):
                ref = team.get("reference")
                if ref and "CareTeam" in ref:
                    care_team_id = ref.split("/")[-1]
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
        
        # Extract addresses (conditions/problems)
        addresses = []
        for address in resource.get("addresses", []):
            ref = address.get("reference")
            if ref:
                addresses.append(ref.split("/")[-1])
        
        # Extract supporting plans
        supports = []
        for supporting in resource.get("basedOn", []) + resource.get("replaces", []):
            ref = supporting.get("reference")
            if ref:
                supports.append(ref.split("/")[-1])
        
        # Extract goals
        goals = []
        for goal in resource.get("goal", []):
            ref = goal.get("reference")
            if ref:
                goals.append(ref.split("/")[-1])
        
        # Process activities
        activities = []
        for activity in resource.get("activity", []):
            # Extract activity attributes
            activity_id = None  # Not explicitly in FHIR resource
            activity_status = activity.get("status")
            
            # Extract activity reference (if available)
            reference = None
            reference_desc = None
            reference_code = None
            reference_code_system = None
            
            if activity.get("reference"):
                ref = activity.get("reference", {}).get("reference")
                if ref:
                    reference = ref.split("/")[-1]
                    
                    # Try to get description from reference display
                    reference_desc = activity.get("reference", {}).get("display")
            
            # Extract activity detail
            detail = activity.get("detail", {})
            
            # Detail status
            detail_status = detail.get("status")
            
            # Detail description
            detail_description = detail.get("description")
            
            # Detail code
            detail_code = None
            detail_code_system = None
            if detail.get("code"):
                code = detail.get("code", {})
                detail_description = detail_description or code.get("text")
                codings = code.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    detail_description = detail_description or coding.get("display")
                    detail_code = coding.get("code")
                    detail_code_system = coding.get("system")
            
            # Detail category
            detail_category = None
            detail_category_code = None
            detail_category_system = None
            if detail.get("category"):
                category = detail.get("category", {})
                codings = category.get("coding", [])
                if codings and len(codings) > 0:
                    coding = codings[0]
                    detail_category = category.get("text") or coding.get("display")
                    detail_category_code = coding.get("code")
                    detail_category_system = coding.get("system")
            
            # Detail scheduled period
            detail_period_start = None
            detail_period_end = None
            if detail.get("scheduledPeriod"):
                period = detail.get("scheduledPeriod", {})
                start_str = period.get("start")
                end_str = period.get("end")
                if start_str:
                    try:
                        detail_period_start = datetime.fromisoformat(start_str.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        pass
                if end_str:
                    try:
                        detail_period_end = datetime.fromisoformat(end_str.replace('Z', '+00:00'))
                    except (ValueError, TypeError):
                        pass
            
            # Detail location
            detail_location = None
            location_ref = detail.get("location", {}).get("reference")
            if location_ref:
                detail_location = location_ref.split("/")[-1]
            
            # Detail performer
            detail_performer = None
            detail_performer_type = None
            if detail.get("performer"):
                for performer in detail.get("performer", []):
                    ref = performer.get("reference")
                    if ref:
                        parts = ref.split("/")
                        if len(parts) >= 2:
                            detail_performer_type = parts[-2]
                            detail_performer = parts[-1]
                            break
            
            # Build activity object
            activities.append(CarePlanActivity(
                id=activity_id,
                status=activity_status,
                description=reference_desc,
                code=reference_code,
                code_system=reference_code_system,
                detail_status=detail_status,
                detail_description=detail_description,
                detail_code=detail_code,
                detail_code_system=detail_code_system,
                category=detail_category,
                category_code=detail_category_code,
                category_system=detail_category_system,
                period_start=detail_period_start,
                period_end=detail_period_end,
                location=detail_location,
                performer=detail_performer,
                performer_type=detail_performer_type
            ))
        
        # Create CarePlan object
        care_plan = CarePlan(
            id=care_plan_id,
            status=status,
            intent=intent,
            title=title,
            description=description,
            patient_id=patient_id,
            encounter_id=encounter_id,
            period_start=period_start,
            period_end=period_end,
            created=created,
            author=author,
            author_type=author_type,
            category=category,
            category_code=category_code,
            category_system=category_system,
            care_team_id=care_team_id,
            addresses=addresses,
            supports=supports,
            goals=goals,
            activities=activities,
            notes=notes,
            identifiers=identifiers
        )
        
        # Add to content
        if not hasattr(content, 'care_plans'):
            content.care_plans = []
        content.care_plans.append(care_plan)