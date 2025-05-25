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

# src/pulsepipe/ingesters/cda_utils/document_parser.py

import xml.etree.ElementTree as ET
from typing import Dict, List, Any, Optional
from datetime import datetime

class CDADocumentParser:
    """
    Parser for CDA document structure that extracts relevant sections
    into a structured format for mapping to PulseClinicalContent.
    """
    
    def __init__(self):
        self.namespaces = {
            'cda': 'urn:hl7-org:v3',
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
        }
    
    def parse_document(self, root: ET.Element) -> Dict[str, Any]:
        """
        Parse a CDA document and extract structured data.
        
        Args:
            root: Root element of the CDA document
            
        Returns:
            Dictionary containing parsed sections
        """
        result = {}
        
        # Parse patient information from recordTarget
        result['patient'] = self._parse_patient_info(root)
        
        # Parse encounter information from header
        result['encounters'] = self._parse_encounter_info(root)
        
        # Parse structured body sections
        body = self._find_element(root, './/component/structuredBody')
        if body is not None:
            result.update(self._parse_structured_body(body))
        
        return result
    
    def _parse_patient_info(self, root: ET.Element) -> Dict[str, Any]:
        """Extract patient information from recordTarget section."""
        patient_data = {}
        
        record_target = self._find_element(root, './/recordTarget/patientRole')
        if record_target is None:
            return patient_data
        
        # Patient identifiers
        ids = self._find_elements(record_target, './id')
        patient_data['identifiers'] = [
            {
                'root': id_elem.get('root', ''),
                'extension': id_elem.get('extension', ''),
                'assigning_authority': id_elem.get('assigningAuthorityName', '')
            }
            for id_elem in ids
        ]
        
        # Patient demographics
        patient = self._find_element(record_target, './patient')
        if patient is not None:
            # Name
            name_elem = self._find_element(patient, './name')
            if name_elem is not None:
                patient_data['name'] = {
                    'given': [elem.text for elem in self._find_elements(name_elem, './given') if elem.text],
                    'family': [elem.text for elem in self._find_elements(name_elem, './family') if elem.text],
                    'prefix': [elem.text for elem in self._find_elements(name_elem, './prefix') if elem.text],
                    'suffix': [elem.text for elem in self._find_elements(name_elem, './suffix') if elem.text]
                }
            
            # Gender
            gender_elem = self._find_element(patient, './administrativeGenderCode')
            if gender_elem is not None:
                patient_data['gender'] = {
                    'code': gender_elem.get('code', ''),
                    'display': gender_elem.get('displayName', '')
                }
            
            # Birth date
            birth_elem = self._find_element(patient, './birthTime')
            if birth_elem is not None:
                patient_data['birth_date'] = birth_elem.get('value', '')
            
            # Race
            race_elem = self._find_element(patient, './raceCode')
            if race_elem is not None:
                patient_data['race'] = {
                    'code': race_elem.get('code', ''),
                    'display': race_elem.get('displayName', ''),
                    'system': race_elem.get('codeSystem', '')
                }
            
            # Ethnicity
            ethnicity_elem = self._find_element(patient, './ethnicGroupCode')
            if ethnicity_elem is not None:
                patient_data['ethnicity'] = {
                    'code': ethnicity_elem.get('code', ''),
                    'display': ethnicity_elem.get('displayName', ''),
                    'system': ethnicity_elem.get('codeSystem', '')
                }
        
        # Address
        addr_elem = self._find_element(record_target, './addr')
        if addr_elem is not None:
            patient_data['address'] = {
                'street': [elem.text for elem in self._find_elements(addr_elem, './streetAddressLine') if elem.text],
                'city': self._get_text(self._find_element(addr_elem, './city')),
                'state': self._get_text(self._find_element(addr_elem, './state')),
                'postal_code': self._get_text(self._find_element(addr_elem, './postalCode')),
                'country': self._get_text(self._find_element(addr_elem, './country')),
                'use': addr_elem.get('use', '')
            }
        
        # Telecom
        telecom_elems = self._find_elements(record_target, './telecom')
        patient_data['telecom'] = [
            {
                'value': elem.get('value', ''),
                'use': elem.get('use', '')
            }
            for elem in telecom_elems
        ]
        
        return patient_data
    
    def _parse_encounter_info(self, root: ET.Element) -> List[Dict[str, Any]]:
        """Extract encounter information from CDA header and sections."""
        encounters = []
        
        # Parse encompassing encounter from header
        encompassing_encounter = self._find_element(root, './/componentOf/encompassingEncounter')
        if encompassing_encounter is not None:
            encounter_data = {}
            
            # Encounter ID
            id_elem = self._find_element(encompassing_encounter, './id')
            if id_elem is not None:
                encounter_data['id'] = id_elem.get('extension', id_elem.get('root', ''))
            
            # Encounter class
            code_elem = self._find_element(encompassing_encounter, './code')
            if code_elem is not None:
                encounter_data['class_code'] = code_elem.get('code', '')
                encounter_data['type_code'] = code_elem.get('displayName', '')
            
            # Effective time (encounter period)
            time_elem = self._find_element(encompassing_encounter, './effectiveTime')
            if time_elem is not None:
                low_elem = self._find_element(time_elem, './low')
                high_elem = self._find_element(time_elem, './high')
                
                if low_elem is not None:
                    encounter_data['start_date'] = low_elem.get('value', '')
                if high_elem is not None:
                    encounter_data['end_date'] = high_elem.get('value', '')
                
                # Handle single value time
                if time_elem.get('value'):
                    encounter_data['start_date'] = time_elem.get('value')
            
            # Location information
            location_elem = self._find_element(encompassing_encounter, './location/healthCareFacility')
            if location_elem is not None:
                location_data = {}
                
                # Facility ID and name
                id_elem = self._find_element(location_elem, './id')
                if id_elem is not None:
                    location_data['facility_id'] = id_elem.get('extension', '')
                
                # Facility code (service line indicator)
                code_elem = self._find_element(location_elem, './code')
                if code_elem is not None:
                    location_data['service_line'] = code_elem.get('displayName', '')
                    location_data['department'] = code_elem.get('code', '')
                
                # Facility name
                name_elem = self._find_element(location_elem, './location/name')
                if name_elem is not None:
                    location_data['name'] = name_elem.text
                
                # Facility organization
                org_elem = self._find_element(location_elem, './serviceProviderOrganization')
                if org_elem is not None:
                    org_name_elem = self._find_element(org_elem, './name')
                    if org_name_elem is not None:
                        location_data['facility'] = org_name_elem.text
                
                encounter_data['location'] = location_data
            
            # Responsible party (attending physician)
            responsible_party = self._find_element(encompassing_encounter, './responsibleParty/assignedEntity')
            if responsible_party is not None:
                providers = []
                provider_data = {}
                
                # Provider ID
                id_elem = self._find_element(responsible_party, './id')
                if id_elem is not None:
                    provider_data['id'] = id_elem.get('extension', '')
                
                # Provider name
                name_elem = self._find_element(responsible_party, './assignedPerson/name')
                if name_elem is not None:
                    given_names = [elem.text for elem in self._find_elements(name_elem, './given') if elem.text]
                    family_names = [elem.text for elem in self._find_elements(name_elem, './family') if elem.text]
                    provider_name = ' '.join(given_names + family_names)
                    provider_data['name'] = provider_name
                
                provider_data['role'] = 'attending'
                provider_data['type'] = 'physician'
                providers.append(provider_data)
                encounter_data['providers'] = providers
            
            encounters.append(encounter_data)
        
        # Also check for encounters section
        # This would be in the structured body
        
        return encounters
    
    def _parse_structured_body(self, body: ET.Element) -> Dict[str, Any]:
        """Parse the structured body sections."""
        result = {}
        
        # Find all components/sections
        sections = self._find_elements(body, './/component/section')
        
        for section in sections:
            # Get template ID to determine section type
            template_elem = self._find_element(section, './templateId')
            if template_elem is None:
                continue
                
            template_id = template_elem.get('root', '')
            
            # Parse based on section type
            if template_id == '2.16.840.1.113883.10.20.22.2.6.1':  # Allergies
                result['allergies'] = self._parse_allergies_section(section)
            elif template_id == '2.16.840.1.113883.10.20.22.2.1.1':  # Medications
                result['medications'] = self._parse_medications_section(section)
            elif template_id == '2.16.840.1.113883.10.20.22.2.5.1':  # Problems
                result['problems'] = self._parse_problems_section(section)
            elif template_id == '2.16.840.1.113883.10.20.22.2.7.1':  # Procedures
                result['procedures'] = self._parse_procedures_section(section)
            elif template_id == '2.16.840.1.113883.10.20.22.2.4.1':  # Vital Signs
                result['vital_signs'] = self._parse_vital_signs_section(section)
            elif template_id == '2.16.840.1.113883.10.20.22.2.2.1':  # Immunizations
                result['immunizations'] = self._parse_immunizations_section(section)
            elif template_id == '2.16.840.1.113883.10.20.22.2.3.1':  # Results/Labs
                result['lab_results'] = self._parse_lab_results_section(section)
        
        return result
    
    def _parse_allergies_section(self, section: ET.Element) -> List[Dict[str, Any]]:
        """Parse allergies section."""
        allergies = []
        entries = self._find_elements(section, './/entry')
        
        for entry in entries:
            allergy_data = {}
            
            # Find allergy observation
            obs = self._find_element(entry, './/observation')
            if obs is None:
                continue
            
            # Allergy substance
            participant = self._find_element(obs, './/participant/participantRole/playingEntity')
            if participant is not None:
                code_elem = self._find_element(participant, './code')
                if code_elem is not None:
                    allergy_data['substance'] = {
                        'code': code_elem.get('code', ''),
                        'display': code_elem.get('displayName', ''),
                        'system': code_elem.get('codeSystem', '')
                    }
                
                name_elem = self._find_element(participant, './name')
                if name_elem is not None:
                    allergy_data['substance_name'] = name_elem.text
            
            # Allergy status and criticality
            status_elem = self._find_element(obs, './statusCode')
            if status_elem is not None:
                allergy_data['status'] = status_elem.get('code', '')
            
            # Effective time
            time_elem = self._find_element(obs, './effectiveTime')
            if time_elem is not None:
                allergy_data['onset_date'] = time_elem.get('value', '')
            
            allergies.append(allergy_data)
        
        return allergies
    
    def _parse_medications_section(self, section: ET.Element) -> List[Dict[str, Any]]:
        """Parse medications section."""
        medications = []
        entries = self._find_elements(section, './/entry')
        
        for entry in entries:
            med_data = {}
            
            # Find medication activity
            subst_admin = self._find_element(entry, './/substanceAdministration')
            if subst_admin is None:
                continue
            
            # Medication information
            consumable = self._find_element(subst_admin, './/consumable/manufacturedProduct/manufacturedMaterial')
            if consumable is not None:
                code_elem = self._find_element(consumable, './code')
                if code_elem is not None:
                    med_data['medication'] = {
                        'code': code_elem.get('code', ''),
                        'display': code_elem.get('displayName', ''),
                        'system': code_elem.get('codeSystem', '')
                    }
                
                name_elem = self._find_element(consumable, './name')
                if name_elem is not None:
                    med_data['medication_name'] = name_elem.text
            
            # Dosage
            dose_elem = self._find_element(subst_admin, './/doseQuantity')
            if dose_elem is not None:
                med_data['dosage'] = {
                    'value': dose_elem.get('value', ''),
                    'unit': dose_elem.get('unit', '')
                }
            
            # Effective time
            time_elem = self._find_element(subst_admin, './effectiveTime')
            if time_elem is not None:
                med_data['start_date'] = time_elem.get('value', '')
            
            medications.append(med_data)
        
        return medications
    
    def _parse_problems_section(self, section: ET.Element) -> List[Dict[str, Any]]:
        """Parse problems section."""
        problems = []
        entries = self._find_elements(section, './/entry')
        
        for entry in entries:
            problem_data = {}
            
            # Find problem observation
            obs = self._find_element(entry, './/observation')
            if obs is None:
                continue
            
            # Problem code
            code_elem = self._find_element(obs, './value')
            if code_elem is not None:
                problem_data['problem'] = {
                    'code': code_elem.get('code', ''),
                    'display': code_elem.get('displayName', ''),
                    'system': code_elem.get('codeSystem', '')
                }
            
            # Status
            status_elem = self._find_element(obs, './statusCode')
            if status_elem is not None:
                problem_data['status'] = status_elem.get('code', '')
            
            # Effective time
            time_elem = self._find_element(obs, './effectiveTime')
            if time_elem is not None:
                problem_data['onset_date'] = time_elem.get('value', '')
            
            problems.append(problem_data)
        
        return problems
    
    def _parse_procedures_section(self, section: ET.Element) -> List[Dict[str, Any]]:
        """Parse procedures section."""
        procedures = []
        entries = self._find_elements(section, './/entry')
        
        for entry in entries:
            proc_data = {}
            
            # Find procedure activity
            proc = self._find_element(entry, './/procedure')
            if proc is None:
                continue
            
            # Procedure code
            code_elem = self._find_element(proc, './code')
            if code_elem is not None:
                proc_data['procedure'] = {
                    'code': code_elem.get('code', ''),
                    'display': code_elem.get('displayName', ''),
                    'system': code_elem.get('codeSystem', '')
                }
            
            # Effective time
            time_elem = self._find_element(proc, './effectiveTime')
            if time_elem is not None:
                proc_data['performed_date'] = time_elem.get('value', '')
            
            procedures.append(proc_data)
        
        return procedures
    
    def _parse_vital_signs_section(self, section: ET.Element) -> List[Dict[str, Any]]:
        """Parse vital signs section."""
        vital_signs = []
        entries = self._find_elements(section, './/entry')
        
        for entry in entries:
            # Find vital signs organizer
            organizer = self._find_element(entry, './/organizer')
            if organizer is None:
                continue
            
            # Get individual vital sign observations
            observations = self._find_elements(organizer, './/observation')
            for obs in observations:
                vital_data = {}
                
                # Vital sign type
                code_elem = self._find_element(obs, './code')
                if code_elem is not None:
                    vital_data['vital_sign'] = {
                        'code': code_elem.get('code', ''),
                        'display': code_elem.get('displayName', ''),
                        'system': code_elem.get('codeSystem', '')
                    }
                
                # Value
                value_elem = self._find_element(obs, './value')
                if value_elem is not None:
                    vital_data['value'] = {
                        'value': value_elem.get('value', ''),
                        'unit': value_elem.get('unit', '')
                    }
                
                # Effective time
                time_elem = self._find_element(obs, './effectiveTime')
                if time_elem is not None:
                    vital_data['recorded_date'] = time_elem.get('value', '')
                
                vital_signs.append(vital_data)
        
        return vital_signs
    
    def _parse_immunizations_section(self, section: ET.Element) -> List[Dict[str, Any]]:
        """Parse immunizations section."""
        immunizations = []
        entries = self._find_elements(section, './/entry')
        
        for entry in entries:
            imm_data = {}
            
            # Find immunization activity
            subst_admin = self._find_element(entry, './/substanceAdministration')
            if subst_admin is None:
                continue
            
            # Vaccine information
            consumable = self._find_element(subst_admin, './/consumable/manufacturedProduct/manufacturedMaterial')
            if consumable is not None:
                code_elem = self._find_element(consumable, './code')
                if code_elem is not None:
                    imm_data['vaccine'] = {
                        'code': code_elem.get('code', ''),
                        'display': code_elem.get('displayName', ''),
                        'system': code_elem.get('codeSystem', '')
                    }
            
            # Administration date
            time_elem = self._find_element(subst_admin, './effectiveTime')
            if time_elem is not None:
                imm_data['administration_date'] = time_elem.get('value', '')
            
            immunizations.append(imm_data)
        
        return immunizations
    
    def _parse_lab_results_section(self, section: ET.Element) -> List[Dict[str, Any]]:
        """Parse lab results section."""
        lab_results = []
        entries = self._find_elements(section, './/entry')
        
        for entry in entries:
            # Find results organizer
            organizer = self._find_element(entry, './/organizer')
            if organizer is None:
                continue
            
            # Get individual lab observations
            observations = self._find_elements(organizer, './/observation')
            for obs in observations:
                lab_data = {}
                
                # Lab test
                code_elem = self._find_element(obs, './code')
                if code_elem is not None:
                    lab_data['test'] = {
                        'code': code_elem.get('code', ''),
                        'display': code_elem.get('displayName', ''),
                        'system': code_elem.get('codeSystem', '')
                    }
                
                # Result value
                value_elem = self._find_element(obs, './value')
                if value_elem is not None:
                    lab_data['result'] = {
                        'value': value_elem.get('value', ''),
                        'unit': value_elem.get('unit', ''),
                        'type': value_elem.get('xsi:type', '').replace('PQ', 'Quantity').replace('ST', 'String')
                    }
                
                # Reference range
                ref_range = self._find_element(obs, './referenceRange/observationRange/value')
                if ref_range is not None:
                    lab_data['reference_range'] = {
                        'low': ref_range.get('low', ''),
                        'high': ref_range.get('high', ''),
                        'unit': ref_range.get('unit', '')
                    }
                
                # Effective time
                time_elem = self._find_element(obs, './effectiveTime')
                if time_elem is not None:
                    lab_data['collected_date'] = time_elem.get('value', '')
                
                lab_results.append(lab_data)
        
        return lab_results
    
    def _find_element(self, parent: ET.Element, path: str) -> Optional[ET.Element]:
        """Find single element, handling namespaces."""
        # Try without namespace first
        element = parent.find(path)
        if element is None:
            # Add namespace to each element in the path
            namespaced_path = self._add_namespace_to_path(path)
            element = parent.find(namespaced_path)
        return element
    
    def _find_elements(self, parent: ET.Element, path: str) -> List[ET.Element]:
        """Find multiple elements, handling namespaces."""
        # Try without namespace first
        elements = parent.findall(path)
        if not elements:
            # Add namespace to each element in the path
            namespaced_path = self._add_namespace_to_path(path)
            elements = parent.findall(namespaced_path)
        return elements
    
    def _add_namespace_to_path(self, path: str) -> str:
        """Add namespace to each element in an XPath."""
        if '{urn:hl7-org:v3}' in path:
            return path  # Already has namespace
        
        # Split path and add namespace to each element
        parts = path.split('/')
        namespaced_parts = []
        
        for part in parts:
            if part in ['', '.', '..']:
                namespaced_parts.append(part)
            elif part == '*':
                namespaced_parts.append(part)
            elif part.startswith('@'):
                namespaced_parts.append(part)  # Attributes don't need namespace
            else:
                namespaced_parts.append(f'{{{self.namespaces["cda"]}}}{part}')
        
        return '/'.join(namespaced_parts)
    
    def _get_text(self, element: Optional[ET.Element]) -> Optional[str]:
        """Safely get text from element."""
        return element.text if element is not None else None