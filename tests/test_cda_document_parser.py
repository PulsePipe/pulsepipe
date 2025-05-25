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

# tests/test_cda_document_parser.py

import pytest
import xml.etree.ElementTree as ET

from pulsepipe.ingesters.cda_utils.document_parser import CDADocumentParser


class TestCDADocumentParser:
    """Test suite for CDADocumentParser."""
    
    @pytest.fixture
    def parser(self):
        return CDADocumentParser()
    
    @pytest.fixture
    def sample_cda_root(self):
        """Create a sample CDA root element for testing."""
        xml_content = '''
        <ClinicalDocument xmlns="urn:hl7-org:v3">
            <recordTarget>
                <patientRole>
                    <id extension="12345" root="2.16.840.1.113883.19.5.99999.2"/>
                    <addr use="HP">
                        <streetAddressLine>123 Main St</streetAddressLine>
                        <city>Anytown</city>
                        <state>NY</state>
                        <postalCode>12345</postalCode>
                    </addr>
                    <telecom value="tel:+15551234567" use="HP"/>
                    <patient>
                        <name use="L">
                            <given>John</given>
                            <family>Doe</family>
                        </name>
                        <administrativeGenderCode code="M" displayName="Male"/>
                        <birthTime value="19800101"/>
                    </patient>
                </patientRole>
            </recordTarget>
            
            <componentOf>
                <encompassingEncounter>
                    <id extension="ENC123" root="2.16.840.1.113883.19.5.99999.19"/>
                    <code code="AMB" displayName="Ambulatory"/>
                    <effectiveTime>
                        <low value="20230101080000"/>
                        <high value="20230101100000"/>
                    </effectiveTime>
                    <location>
                        <healthCareFacility>
                            <code code="CARD" displayName="Cardiology"/>
                            <location>
                                <name>Cardiology Clinic</name>
                            </location>
                        </healthCareFacility>
                    </location>
                </encompassingEncounter>
            </componentOf>
            
            <component>
                <structuredBody>
                    <component>
                        <section>
                            <templateId root="2.16.840.1.113883.10.20.22.2.6.1"/>
                            <entry>
                                <observation>
                                    <statusCode code="completed"/>
                                    <effectiveTime value="20220101"/>
                                    <participant typeCode="CSM">
                                        <participantRole>
                                            <playingEntity>
                                                <code code="1191" displayName="Aspirin"/>
                                                <name>Aspirin</name>
                                            </playingEntity>
                                        </participantRole>
                                    </participant>
                                </observation>
                            </entry>
                        </section>
                    </component>
                </structuredBody>
            </component>
        </ClinicalDocument>
        '''
        return ET.fromstring(xml_content)
    
    def test_parser_initialization(self, parser):
        """Test parser initializes correctly."""
        assert parser is not None
        assert hasattr(parser, 'namespaces')
        assert 'cda' in parser.namespaces
        assert 'xsi' in parser.namespaces
    
    def test_parse_document_structure(self, parser, sample_cda_root):
        """Test parsing complete document structure."""
        result = parser.parse_document(sample_cda_root)
        
        assert isinstance(result, dict)
        assert 'patient' in result
        assert 'encounters' in result
        assert 'allergies' in result
    
    def test_parse_patient_info(self, parser, sample_cda_root):
        """Test parsing patient information."""
        patient_data = parser._parse_patient_info(sample_cda_root)
        
        assert patient_data['identifiers'][0]['extension'] == '12345'
        assert patient_data['name']['given'] == ['John']
        assert patient_data['name']['family'] == ['Doe']
        assert patient_data['gender']['code'] == 'M'
        assert patient_data['birth_date'] == '19800101'
        assert patient_data['address']['street'] == ['123 Main St']
        assert patient_data['address']['city'] == 'Anytown'
        assert patient_data['telecom'][0]['value'] == 'tel:+15551234567'
    
    def test_parse_encounter_info(self, parser, sample_cda_root):
        """Test parsing encounter information."""
        encounters = parser._parse_encounter_info(sample_cda_root)
        
        assert len(encounters) == 1
        encounter = encounters[0]
        assert encounter['id'] == 'ENC123'
        assert encounter['class_code'] == 'AMB'
        assert encounter['start_date'] == '20230101080000'
        assert encounter['end_date'] == '20230101100000'
        assert encounter['location']['service_line'] == 'Cardiology'
        assert encounter['location']['name'] == 'Cardiology Clinic'
    
    def test_parse_allergies_section(self, parser, sample_cda_root):
        """Test parsing allergies section."""
        # Get the structured body
        body = parser._find_element(sample_cda_root, './/component/structuredBody')
        result = parser._parse_structured_body(body)
        
        assert 'allergies' in result
        assert len(result['allergies']) == 1
        allergy = result['allergies'][0]
        assert allergy['substance']['code'] == '1191'
        assert allergy['substance']['display'] == 'Aspirin'
        assert allergy['substance_name'] == 'Aspirin'
    
    def test_find_element_with_namespace(self, parser):
        """Test finding elements with namespace."""
        xml = '<root xmlns="urn:hl7-org:v3"><child>value</child></root>'
        root = ET.fromstring(xml)
        
        element = parser._find_element(root, './child')
        assert element is not None
        assert element.text == 'value'
    
    def test_find_element_without_namespace(self, parser):
        """Test finding elements without namespace."""
        xml = '<root><child>value</child></root>'
        root = ET.fromstring(xml)
        
        element = parser._find_element(root, './child')
        assert element is not None
        assert element.text == 'value'
    
    def test_find_elements_multiple(self, parser):
        """Test finding multiple elements."""
        xml = '''
        <root xmlns="urn:hl7-org:v3">
            <child>value1</child>
            <child>value2</child>
        </root>
        '''
        root = ET.fromstring(xml)
        
        elements = parser._find_elements(root, './child')
        assert len(elements) == 2
        assert elements[0].text == 'value1'
        assert elements[1].text == 'value2'
    
    def test_get_text_safe(self, parser):
        """Test safely getting text from elements."""
        xml = '<root><child>value</child><empty></empty></root>'
        root = ET.fromstring(xml)
        
        child = root.find('./child')
        empty = root.find('./empty')
        missing = root.find('./missing')
        
        assert parser._get_text(child) == 'value'
        assert parser._get_text(empty) is None
        assert parser._get_text(missing) is None
    
    def test_parse_empty_sections(self, parser):
        """Test parsing with empty sections."""
        xml = '''
        <ClinicalDocument xmlns="urn:hl7-org:v3">
            <recordTarget>
                <patientRole>
                    <patient>
                        <name><given>Test</given></name>
                    </patient>
                </patientRole>
            </recordTarget>
            <component>
                <structuredBody>
                </structuredBody>
            </component>
        </ClinicalDocument>
        '''
        root = ET.fromstring(xml)
        result = parser.parse_document(root)
        
        assert 'patient' in result
        assert result['patient']['name']['given'] == ['Test']
    
    def test_parse_medications_section(self, parser):
        """Test parsing medications section."""
        xml = '''
        <section xmlns="urn:hl7-org:v3">
            <templateId root="2.16.840.1.113883.10.20.22.2.1.1"/>
            <entry>
                <substanceAdministration>
                    <statusCode code="completed"/>
                    <effectiveTime value="20230101"/>
                    <doseQuantity value="10" unit="mg"/>
                    <consumable>
                        <manufacturedProduct>
                            <manufacturedMaterial>
                                <code code="197361" displayName="Lisinopril"/>
                                <name>Lisinopril 10mg</name>
                            </manufacturedMaterial>
                        </manufacturedProduct>
                    </consumable>
                </substanceAdministration>
            </entry>
        </section>
        '''
        section = ET.fromstring(xml)
        medications = parser._parse_medications_section(section)
        
        assert len(medications) == 1
        med = medications[0]
        assert med['medication']['code'] == '197361'
        assert med['medication']['display'] == 'Lisinopril'
        assert med['medication_name'] == 'Lisinopril 10mg'
        assert med['dosage']['value'] == '10'
        assert med['dosage']['unit'] == 'mg'
    
    def test_parse_vital_signs_section(self, parser):
        """Test parsing vital signs section."""
        xml = '''
        <section xmlns="urn:hl7-org:v3">
            <templateId root="2.16.840.1.113883.10.20.22.2.4.1"/>
            <entry>
                <organizer>
                    <observation>
                        <code code="8480-6" displayName="Systolic blood pressure"/>
                        <value value="120" unit="mmHg"/>
                        <effectiveTime value="20230101"/>
                    </observation>
                </organizer>
            </entry>
        </section>
        '''
        section = ET.fromstring(xml)
        vitals = parser._parse_vital_signs_section(section)
        
        assert len(vitals) == 1
        vital = vitals[0]
        assert vital['vital_sign']['code'] == '8480-6'
        assert vital['vital_sign']['display'] == 'Systolic blood pressure'
        assert vital['value']['value'] == '120'
        assert vital['value']['unit'] == 'mmHg'
    
    def test_parse_lab_results_section(self, parser):
        """Test parsing lab results section."""
        xml = '''
        <section xmlns="urn:hl7-org:v3">
            <templateId root="2.16.840.1.113883.10.20.22.2.3.1"/>
            <entry>
                <organizer>
                    <observation>
                        <code code="2093-3" displayName="Cholesterol"/>
                        <value value="180" unit="mg/dL"/>
                        <effectiveTime value="20230101"/>
                        <referenceRange>
                            <observationRange>
                                <value low="100" high="199" unit="mg/dL"/>
                            </observationRange>
                        </referenceRange>
                    </observation>
                </organizer>
            </entry>
        </section>
        '''
        section = ET.fromstring(xml)
        labs = parser._parse_lab_results_section(section)
        
        assert len(labs) == 1
        lab = labs[0]
        assert lab['test']['code'] == '2093-3'
        assert lab['test']['display'] == 'Cholesterol'
        assert lab['result']['value'] == '180'
        assert lab['result']['unit'] == 'mg/dL'
        assert lab['reference_range']['low'] == '100'
        assert lab['reference_range']['high'] == '199'