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

# src/pulsepipe/ingesters/fhir_ingester.py

import json
import logging
from typing import List, Union, Dict, Any
from pulsepipe.models import PulseClinicalContent, MessageCache
from .fhir_utils.base_mapper import MAPPER_REGISTRY
from pulsepipe.utils.xml_to_json import xml_to_json
from pulsepipe.canonical.builder import CanonicalBuilder

logger = logging.getLogger(__name__)

class FHIRIngester:
    def parse(self, raw_data: str) -> Union[PulseClinicalContent, List[PulseClinicalContent]]:
        """
        Parse FHIR data - supports single resources, Bundles, and arrays of FHIR resources.
        
        Args:
            raw_data: String containing FHIR data in JSON or XML format
            
        Returns:
            Either a single PulseClinicalContent or a list of PulseClinicalContent objects
            depending on the input format
        """
        if not raw_data.strip():
            raise ValueError("Empty data received")
        
        # Convert raw data to JSON if it's in XML format
        try:
            # First try to parse as JSON
            data = json.loads(raw_data)
        except json.JSONDecodeError:
            # If fails, try to parse as XML
            data = xml_to_json(raw_data)
        
        # Check if we have an array of FHIR resources
        if isinstance(data, list):
            logger.info(f"Detected array of FHIR resources with {len(data)} items")
            results = []
            for item in data:
                # Process each item as a separate FHIR resource
                results.append(self._parse_single_resource(item))
            return results
        else:
            # Single FHIR resource or Bundle
            return self._parse_single_resource(data)
    
    def _parse_single_resource(self, data: Dict[str, Any]) -> PulseClinicalContent:
        """
        Parse a single FHIR resource or Bundle
        
        Args:
            data: Dictionary representing a FHIR resource
            
        Returns:
            PulseClinicalContent object
        """
        if "resourceType" not in data:
            raise ValueError("Missing resourceType, not a valid FHIR resource.")

        cache: MessageCache = {"patient_id": None, "encounter_id": None, "reference_id": None}

        content = PulseClinicalContent(
            patient=None,
            encounter=None,
            vital_signs=[],
            allergies=[],
            immunizations=[],
            diagnoses=[],
            problem_list=[],
            procedures=[],
            medications=[],
            payors=[],
            mar=[],
            notes=[],
            imaging=[],
            lab=[],
            pathology=[],
            diagnostic_test=[],
            microbiology=[],
            blood_bank=[],
            family_history=[],
            social_history=[],
            advance_directives=[],
            functional_status=[],
            order=[],
            implant=[],
        )

        # 🔵 First Pass - Cache Important References + Build Index
        if data["resourceType"] == "Bundle":
            for entry in data.get("entry", []):
                res = entry.get("resource", {})
                if not res: continue

                rtype = res.get("resourceType")
                rid = res.get("id")

                # cache first found patient, encounter, order
                if rtype == "Patient" and not cache["patient_id"]:
                    cache["patient_id"] = rid
                if rtype == "Encounter" and not cache["encounter_id"]:
                    cache["encounter_id"] = rid
                if rtype in {"ServiceRequest", "Order"} and not cache.get("order_id"):
                    cache["order_id"] = rid

        # 🟣 Second Pass - Map normally
        if data["resourceType"] == "Bundle":
            for entry in data.get("entry", []):
                self._map_resource(entry.get("resource", {}), content, cache)
        else:
            self._map_resource(data, content, cache)

        # 🟢 Optional Third Pass - Fix up resources (cross-references)
        self._link_missing_references(content, cache)

        return content

    def _map_resource(self, resource: dict, content: PulseClinicalContent, cache: dict):
        """Map a FHIR resource to the canonical model using registered mappers"""
        for mapper in MAPPER_REGISTRY:
            if mapper.accepts(resource):
                mapper.map(resource, content, cache)
                break

    def _link_missing_references(self, content: PulseClinicalContent, cache: dict):
        """Fix up any missing references between resources"""
        # For example, link imaging reports to orders, or missing patient_id to labs
        # Not required unless you want advanced reference fixing
        pass
