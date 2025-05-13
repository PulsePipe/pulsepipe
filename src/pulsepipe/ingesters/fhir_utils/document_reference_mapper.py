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

# src/pulsepipe/ingesters/fhir_utils/document_reference_mapper.py

"""
PulsePipe — DocumentReference Mapper for FHIR Resources
"""

import base64
from typing import List, Optional

from pulsepipe.models.document_reference import DocumentReference, DocumentAuthor
from pulsepipe.models import PulseClinicalContent, MessageCache
from .base_mapper import BaseFHIRMapper, fhir_mapper
from .extractors import extract_patient_reference, extract_encounter_reference
from pulsepipe.utils.narrative_decoder import decode_narrative

@fhir_mapper("DocumentReference")
class DocumentReferenceMapper(BaseFHIRMapper):
    RESOURCE_TYPE = "DocumentReference"

    def map(self, resource: dict, content: PulseClinicalContent, cache: MessageCache) -> None:
        if not hasattr(content, "document_references"):
            setattr(content, "document_references", [])

        content.document_references.append(self.parse_document_reference(resource, cache))

    def _decode_attachment_content(self, attachment: dict) -> str:
        """
        Helper method to decode attachment content that might be encoded

        Args:
            attachment: FHIR attachment object with potential encoded data

        Returns:
            str: Decoded content or None if not decodable
        """
        if not attachment.get("data"):
            return None

        # Use the narrative decoder utility
        return decode_narrative(attachment["data"])
    
    def parse_document_reference(self, resource: dict, cache: MessageCache) -> DocumentReference:
        document_id = resource.get("id")
        
        # Extract patient reference
        patient_id = extract_patient_reference(resource) or cache.get("patient_id")
        
        # Extract document metadata
        title = resource.get("description")
        status = resource.get("status")
        
        # Extract document type
        document_type = None
        if resource.get("type", {}).get("coding"):
            for coding in resource["type"]["coding"]:
                document_type = coding.get("display") or coding.get("code")
                if document_type:
                    break
        
        # Extract document class
        document_class = None
        if resource.get("category", [{}])[0].get("coding"):
            for coding in resource["category"][0]["coding"]:
                document_class = coding.get("display") or coding.get("code")
                if document_class:
                    break
        
        # Extract dates
        creation_date = resource.get("date")
        # Could also use resource.get("content", [{}])[0].get("attachment", {}).get("creation") if available
        
        # Extract content information
        content_type = None
        content_text = None
        content_url = None
        
        if resource.get("content"):
            for content_entry in resource["content"]:
                attachment = content_entry.get("attachment", {})
                content_type = attachment.get("contentType")
                
                # Handle inline content (base64 or hex encoded)
                if attachment.get("data"):
                    # Use the helper method to decode attachment content
                    content_text = self._decode_attachment_content(attachment)
                
                # Handle URL reference
                if attachment.get("url"):
                    content_url = attachment["url"]
                
                # First content entry is enough
                if content_type or content_text or content_url:
                    break
        
        # Extract security label
        security_label = None
        if resource.get("securityLabel", [{}])[0].get("coding"):
            for coding in resource["securityLabel"][0]["coding"]:
                security_label = coding.get("display") or coding.get("code")
                if security_label:
                    break
        
        # Extract facility information
        facility = None
        if resource.get("context", {}).get("facilityType", {}).get("coding"):
            for coding in resource["context"]["facilityType"]["coding"]:
                facility = coding.get("display") or coding.get("code")
                if facility:
                    break
        
        # Extract department/practice setting
        department = None
        if resource.get("context", {}).get("practiceSetting", {}).get("coding"):
            for coding in resource["context"]["practiceSetting"]["coding"]:
                department = coding.get("display") or coding.get("code")
                if department:
                    break
        
        # Extract related encounters
        related_encounters: List[str] = []
        encounter_field = resource.get("context", {}).get("encounter")
        
        # Handle encounter field which can be a reference object or a list of references
        if encounter_field:
            if isinstance(encounter_field, dict) and encounter_field.get("reference"):
                # It's a direct reference object
                encounter_id = encounter_field["reference"].split("/")[-1]
                related_encounters.append(encounter_id)
            elif isinstance(encounter_field, list):
                # It's a list of references
                for encounter_ref in encounter_field:
                    if isinstance(encounter_ref, dict) and encounter_ref.get("reference"):
                        encounter_id = encounter_ref["reference"].split("/")[-1]
                        related_encounters.append(encounter_id)
        
        # Extract author information
        authors: List[DocumentAuthor] = []
        for author_ref in resource.get("author", []):
            author_id = None
            author_name = None
            author_role = None
            
            if isinstance(author_ref, dict) and author_ref.get("reference"):
                author_id = author_ref["reference"].split("/")[-1]
                
                # Try to get author display name if available
                author_name = author_ref.get("display")
                
                # If we have access to the referenced Practitioner resource,
                # we could extract more information, but for now we'll use what we have
                
                authors.append(DocumentAuthor(
                    author_id=author_id,
                    author_name=author_name,
                    author_role=author_role
                ))
        
        # Create and return the DocumentReference
        return DocumentReference(
            document_id=document_id,
            title=title,
            document_type=document_type,
            document_class=document_class,
            status=status,
            format=content_type,  # Using MIME type as format
            creation_date=creation_date,
            last_modified_date=None,  # FHIR doesn't have a direct equivalent
            security_label=security_label,
            content_type=content_type,
            content=content_text,
            content_url=content_url,
            authors=authors,
            related_encounters=related_encounters,
            patient_id=patient_id,
            facility=facility,
            department=department
        )