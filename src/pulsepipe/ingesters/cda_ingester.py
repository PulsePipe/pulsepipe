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

# src/pulsepipe/ingesters/cda_ingester.py

import xml.etree.ElementTree as ET
from typing import List, Union, Dict, Any, Optional
from datetime import datetime

from .base import Ingester
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.models import PulseClinicalContent
from pulsepipe.canonical.builder import CanonicalBuilder
from pulsepipe.utils.errors import ValidationError, SchemaValidationError
from .cda_utils.base_mapper import CDAMapperRegistry
from .cda_utils.document_parser import CDADocumentParser

class CDAIngester(Ingester):
    """
    CDA/CCDA/CCD ingester that parses Clinical Document Architecture XML
    into PulseClinicalContent.
    
    This ingester supports:
    - CDA documents (Clinical Document Architecture)
    - CCDA documents (Consolidated CDA)
    - CCD documents (Continuity of Care Documents)
    """
    
    def __init__(self):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing CDAIngester")
        self.parser = CDADocumentParser()
        self.mapper_registry = CDAMapperRegistry()

    def parse(self, raw_data: str) -> Union[PulseClinicalContent, List[PulseClinicalContent]]:
        """
        Parse CDA XML data and convert to PulseClinicalContent.
        
        Args:
            raw_data: XML string containing CDA document
            
        Returns:
            PulseClinicalContent object or list of objects
            
        Raises:
            ValidationError: If XML is malformed or required elements are missing
            SchemaValidationError: If CDA structure doesn't conform to expected format
        """
        try:
            self.logger.info("🔍 Parsing CDA document")
            
            # Parse XML
            try:
                root = ET.fromstring(raw_data)
            except ET.ParseError as e:
                raise ValidationError(f"Invalid XML: {str(e)}")
            
            # Validate it's a CDA document
            if not self._is_cda_document(root):
                raise SchemaValidationError("Document is not a valid CDA document")
            
            # Parse the document structure
            parsed_data = self.parser.parse_document(root)
            
            # Convert to clinical content
            clinical_content = self._convert_to_clinical_content(parsed_data)
            
            self.logger.info(f"✅ Successfully parsed CDA document with {'encounter' if clinical_content.encounter else 'no encounter'}")
            return clinical_content
            
        except Exception as e:
            self.logger.error(f"❌ Error parsing CDA document: {str(e)}")
            raise ValidationError(f"Failed to parse CDA document: {str(e)}")
    
    def _is_cda_document(self, root: ET.Element) -> bool:
        """
        Check if the XML root element represents a valid CDA document.
        """
        # Check namespace and root element name
        if root.tag not in ['{urn:hl7-org:v3}ClinicalDocument', 'ClinicalDocument']:
            return False
            
        # Check for required templateId indicating CDA compliance
        template_ids = self._find_elements(root, './templateId')
        cda_template_found = any(
            elem.get('root') in [
                '2.16.840.1.113883.10.20.22.1.1',  # US General Header Template
                '2.16.840.1.113883.10.20.22.1.2',  # CCD Template
            ] for elem in template_ids
        )
        
        return cda_template_found
    
    def _find_elements(self, root: ET.Element, xpath: str) -> List[ET.Element]:
        """
        Find elements using xpath, handling both namespaced and non-namespaced XML.
        """
        # First try original xpath
        elements = root.findall(xpath)
        
        # If no elements found and xpath doesn't have namespace, try with namespace
        if not elements and '{urn:hl7-org:v3}' not in xpath:
            # Convert xpath to include namespace
            parts = xpath.split('/')
            namespaced_parts = []
            for part in parts:
                if part == '' or part == '.' or part == '..' or part == '*':
                    namespaced_parts.append(part)
                elif part.startswith('@'):
                    namespaced_parts.append(part)
                else:
                    namespaced_parts.append(f'{{{self.parser.namespaces["cda"]}}}{part}')
            namespaced_xpath = '/'.join(namespaced_parts)
            elements = root.findall(namespaced_xpath)
        
        return elements
    
    def _convert_to_clinical_content(self, parsed_data: Dict[str, Any]) -> PulseClinicalContent:
        """
        Convert parsed CDA data to PulseClinicalContent object.
        """
        # Create empty clinical content first
        clinical_content = PulseClinicalContent(patient=None, encounter=None)
        
        # Map patient information
        if 'patient' in parsed_data and parsed_data['patient']:
            clinical_content.patient = self.mapper_registry.get_mapper('patient').map(parsed_data['patient'])
        
        # Map primary encounter
        if 'encounters' in parsed_data and parsed_data['encounters']:
            clinical_content.encounter = self.mapper_registry.get_mapper('encounter').map(parsed_data['encounters'][0])
        
        # Map allergies
        if 'allergies' in parsed_data:
            clinical_content.allergies = [
                self.mapper_registry.get_mapper('allergy').map(allergy) 
                for allergy in parsed_data['allergies']
            ]
        
        # Map medications
        if 'medications' in parsed_data:
            clinical_content.medications = [
                self.mapper_registry.get_mapper('medication').map(med) 
                for med in parsed_data['medications']
            ]
        
        # Map problems/diagnoses
        if 'problems' in parsed_data:
            clinical_content.problem_list = [
                self.mapper_registry.get_mapper('problem').map(problem) 
                for problem in parsed_data['problems']
            ]
        
        # Map procedures
        if 'procedures' in parsed_data:
            clinical_content.procedures = [
                self.mapper_registry.get_mapper('procedure').map(proc) 
                for proc in parsed_data['procedures']
            ]
        
        # Map vital signs
        if 'vital_signs' in parsed_data:
            clinical_content.vital_signs = [
                self.mapper_registry.get_mapper('vital_sign').map(vital) 
                for vital in parsed_data['vital_signs']
            ]
        
        # Map immunizations
        if 'immunizations' in parsed_data:
            clinical_content.immunizations = [
                self.mapper_registry.get_mapper('immunization').map(imm) 
                for imm in parsed_data['immunizations']
            ]
        
        # Map lab results
        if 'lab_results' in parsed_data:
            clinical_content.lab = [
                self.mapper_registry.get_mapper('lab_report').map(lab) 
                for lab in parsed_data['lab_results']
            ]
        
        return clinical_content