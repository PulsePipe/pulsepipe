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

# src/pulsepipe/ingesters/fhir_utils/imaging_study_mapper.py

"""
PulsePipe — ImagingStudy Mapper for FHIR Resources
"""

from pulsepipe.models.imaging import ImagingReport, ImagingFinding
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference

@fhir_mapper("ImagingStudy")
class ImagingStudyMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "ImagingStudy"
    
    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        report = self.parse_imaging_study(resource, cache)
        content.imaging.append(report)
    
    def parse_imaging_study(self, resource: dict, cache: MessageCache) -> ImagingReport:
        # Extract basic information
        report_id = resource.get("id")
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        encounter_id = extract_encounter_reference(resource) or cache.get("encounter_id")
        
        # Extract study date
        acquisition_date = resource.get("started")
        
        # Extract modality information from the first series
        modality = None
        if resource.get("series") and len(resource["series"]) > 0:
            for series in resource["series"]:
                if series.get("modality", {}).get("code"):
                    modality = series["modality"]["code"]
                    break
        
        # Extract study description
        image_type = None
        if resource.get("description"):
            image_type = resource["description"]
        elif resource.get("procedureCode") and len(resource["procedureCode"]) > 0:
            for coding in resource["procedureCode"][0].get("coding", []):
                if coding.get("display"):
                    image_type = coding["display"]
                    break
        
        # Extract performing facility
        performing_facility = None
        if resource.get("location", {}).get("reference"):
            performing_facility = resource["location"]["reference"].split("/")[-1]
        
        # Extract ordering provider
        ordering_provider_id = None
        if resource.get("referrer", {}).get("reference"):
            ordering_provider_id = resource["referrer"]["reference"].split("/")[-1]
        
        # Initialize findings array
        findings = []
        
        # For ImagingStudy, we typically wouldn't have findings directly here.
        # Findings would usually be in a DiagnosticReport that references this ImagingStudy.
        # However, we can extract some basic information from the series/instances if available.
        
        # Extract interpretation/conclusion if it exists
        narrative = resource.get("note", [{}])[0].get("text") if resource.get("note") else None
        
        # Create and return ImagingReport
        return ImagingReport(
            report_id=report_id,
            image_type=image_type,
            coding_method="DICOM", # ImagingStudy typically uses DICOM
            ordering_provider_id=ordering_provider_id,
            performing_facility=performing_facility,
            modality=modality,
            acquisition_date=acquisition_date,
            findings=findings,  # Usually empty for ImagingStudy
            narrative=narrative,
            patient_id=patient_id,
            encounter_id=encounter_id
        )