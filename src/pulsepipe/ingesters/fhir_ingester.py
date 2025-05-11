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

# src/pulsepipe/ingesters/fhir_ingester.py

import json
from pulsepipe.utils.log_factory import LogFactory
from typing import List, Union, Dict, Any
from pulsepipe.models import PulseClinicalContent, MessageCache
from .fhir_utils.base_mapper import MAPPER_REGISTRY
from pulsepipe.utils.xml_to_json import xml_to_json
from pulsepipe.canonical.builder import CanonicalBuilder
from pulsepipe.utils.errors import FHIRError, ValidationError, SchemaValidationError

class FHIRIngester:
    """
    FHIR data ingester that parses FHIR resources into PulseClinicalContent.
    
    This ingester supports:
    - Single FHIR resources
    - FHIR bundles
    - Arrays of FHIR resources
    - Both JSON and XML formats
    """
    
    def __init__(self):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing FHIRIngester")

    def parse(self, raw_data: str) -> Union[PulseClinicalContent, List[PulseClinicalContent]]:
        """
        Parse FHIR data - supports single resources, Bundles, and arrays of FHIR resources.
        
        Args:
            raw_data: String containing FHIR data in JSON or XML format
            
        Returns:
            Either a single PulseClinicalContent or a list of PulseClinicalContent objects
            depending on the input format
            
        Raises:
            FHIRError: If there's an error processing the FHIR data
            ValidationError: If the data fails validation
        """
        try:
            if not raw_data or not raw_data.strip():
                raise FHIRError("Empty or blank data received")
            
            # Convert raw data to JSON if it's in XML format
            try:
                # First try to parse as JSON
                data = json.loads(raw_data)
            except json.JSONDecodeError:
                try:
                    # If that fails, try to parse as XML
                    data = xml_to_json(raw_data)
                except Exception as e:
                    raise FHIRError(
                        "Failed to parse input as JSON or XML",
                        details={"data_snippet": raw_data[:100] + "..." if len(raw_data) > 100 else raw_data},
                        cause=e
                    ) from e
            
            # Check if we have an array of FHIR resources
            if isinstance(data, list):
                self.logger.info(f"Detected array of FHIR resources with {len(data)} items")
                results = []
                errors = []
                
                for i, item in enumerate(data):
                    try:
                        # Process each item as a separate FHIR resource
                        results.append(self._parse_single_resource(item))
                    except Exception as e:
                        error_info = {
                            "index": i,
                            "resource_type": item.get("resourceType", "unknown"),
                            "error": str(e)
                        }
                        errors.append(error_info)
                        self.logger.error(f"Error processing resource at index {i}: {str(e)}")
                
                if not results and errors:
                    # If all resources failed, raise an error
                    raise FHIRError(
                        f"Failed to process any FHIR resources. {len(errors)} errors encountered.",
                        details={"errors": errors}
                    )
                elif errors:
                    # If some resources failed but others succeeded, log a warning
                    self.logger.warning(
                        f"Processed {len(results)} resources successfully, but encountered {len(errors)} errors"
                    )
                
                return results
            else:
                # Single FHIR resource or Bundle
                return self._parse_single_resource(data)
                
        except (FHIRError, ValidationError):
            # Re-raise these specific exceptions
            raise
        except Exception as e:
            # Wrap other exceptions in FHIRError
            raise FHIRError(
                f"Unexpected error processing FHIR data: {str(e)}",
                cause=e
            ) from e
    
    def _parse_single_resource(self, data: Dict[str, Any]) -> PulseClinicalContent:
        """
        Parse a single FHIR resource or Bundle
        
        Args:
            data: Dictionary representing a FHIR resource
            
        Returns:
            PulseClinicalContent object
            
        Raises:
            FHIRError: If there's an error processing the FHIR resource
            ValidationError: If the resource fails validation
        """
        try:
            if "resourceType" not in data:
                raise FHIRError(
                    "Missing resourceType, not a valid FHIR resource",
                    details={"data_keys": list(data.keys())}
                )

            resource_type = data.get("resourceType")
            self.logger.info(f"Processing FHIR resource type: {resource_type}")

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
                bundle_entries = data.get("entry", [])
                self.logger.info(f"Processing Bundle with {len(bundle_entries)} entries")
                
                for entry in bundle_entries:
                    res = entry.get("resource", {})
                    if not res: 
                        continue

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
                mapped_resources = 0
                for entry in data.get("entry", []):
                    try:
                        resource = entry.get("resource", {})
                        if resource:
                            self._map_resource(resource, content, cache)
                            mapped_resources += 1
                    except Exception as e:
                        resource_type = entry.get("resource", {}).get("resourceType", "unknown")
                        self.logger.warning(
                            f"Error mapping resource of type {resource_type}: {str(e)}"
                        )
                
                if mapped_resources == 0:
                    raise FHIRError(
                        "Failed to map any resources from the Bundle",
                        details={"bundle_type": data.get("type", "unknown")}
                    )
            else:
                self._map_resource(data, content, cache)

            # 🟢 Optional Third Pass - Fix up resources (cross-references)
            self._link_missing_references(content, cache)

            return content
            
        except FHIRError:
            # Re-raise FHIRError
            raise
        except ValidationError:
            # Re-raise ValidationError
            raise
        except Exception as e:
            # Wrap other exceptions
            resource_type = data.get("resourceType", "unknown")
            raise FHIRError(
                f"Error parsing FHIR resource of type {resource_type}",
                details={"resource_type": resource_type},
                cause=e
            ) from e

    def _map_resource(self, resource: dict, content: PulseClinicalContent, cache: dict):
        """Map a FHIR resource to the canonical model using registered mappers"""
        resource_type = resource.get("resourceType", "unknown")
        
        try:
            mapper_found = False
            for mapper in MAPPER_REGISTRY:
                if mapper.accepts(resource):
                    mapper.map(resource, content, cache)
                    mapper_found = True
                    break
                    
            if not mapper_found:
                self.logger.warning(f"No mapper found for resource type: {resource_type}")
                
        except Exception as e:
            # Log error but continue with other resources
            self.logger.error(f"Error mapping resource type {resource_type}: {str(e)}")
            # Don't re-raise to allow processing of other resources in the bundle

    def _link_missing_references(self, content: PulseClinicalContent, cache: dict):
        """Fix up any missing references between resources"""
        # For example, link imaging reports to orders, or missing patient_id to labs
        # Not required unless you want advanced reference fixing
        pass
