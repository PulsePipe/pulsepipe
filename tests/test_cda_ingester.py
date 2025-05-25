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

# tests/test_cda_ingester.py

import pytest
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path

from pulsepipe.ingesters.cda_ingester import CDAIngester
from pulsepipe.models import PulseClinicalContent, PatientInfo
from pulsepipe.utils.errors import ValidationError, SchemaValidationError


class TestCDAIngester:
    """Test suite for CDA ingester."""
    
    @pytest.fixture
    def ingester(self):
        """Create CDA ingester instance."""
        return CDAIngester()
    
    @pytest.fixture
    def sample_cda_xml(self):
        """Create a sample CDA document for testing."""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
    <realmCode code="US"/>
    <typeId root="2.16.840.1.113883.1.3" extension="POCD_HD000040"/>
    <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
    <templateId root="2.16.840.1.113883.10.20.22.1.2"/>
    <id root="2.16.840.1.113883.19.5.99999.1" extension="TT988"/>
    <code code="34133-9" codeSystem="2.16.840.1.113883.6.1" displayName="Summarization of Episode Note"/>
    <title>Health Summary</title>
    <effectiveTime value="20230101120000"/>
    <confidentialityCode code="N" codeSystem="2.16.840.1.113883.5.25"/>
    <languageCode code="en-US"/>
    
    <recordTarget>
        <patientRole>
            <id extension="12345" root="2.16.840.1.113883.19.5.99999.2"/>
            <addr use="HP">
                <streetAddressLine>123 Main St</streetAddressLine>
                <city>Anytown</city>
                <state>NY</state>
                <postalCode>12345</postalCode>
                <country>US</country>
            </addr>
            <telecom value="tel:+15551234567" use="HP"/>
            <patient>
                <name use="L">
                    <given>Jane</given>
                    <family>Bloggs</family>
                </name>
                <administrativeGenderCode code="F" displayName="Female"/>
                <birthTime value="19750315"/>
                <raceCode code="2106-3" displayName="White" codeSystem="2.16.840.1.113883.6.238"/>
                <ethnicGroupCode code="2186-5" displayName="Not Hispanic or Latino" codeSystem="2.16.840.1.113883.6.238"/>
            </patient>
        </patientRole>
    </recordTarget>
    
    <author>
        <time value="20230101120000"/>
        <assignedAuthor>
            <id extension="999999999" root="2.16.840.1.113883.4.6"/>
            <assignedPerson>
                <name>
                    <given>Jane</given>
                    <family>Smith</family>
                </name>
            </assignedPerson>
        </assignedAuthor>
    </author>
    
    <custodian>
        <assignedCustodian>
            <representedCustodianOrganization>
                <id extension="99999999" root="2.16.840.1.113883.4.6"/>
                <name>Good Health Hospital</name>
            </representedCustodianOrganization>
        </assignedCustodian>
    </custodian>
    
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
                    <id extension="FAC001" root="2.16.840.1.113883.19.5.99999.1"/>
                    <code code="CARD" displayName="Cardiology"/>
                    <location>
                        <name>Cardiology Clinic</name>
                    </location>
                    <serviceProviderOrganization>
                        <name>Good Health Hospital</name>
                    </serviceProviderOrganization>
                </healthCareFacility>
            </location>
            <responsibleParty>
                <assignedEntity>
                    <id extension="DOC123" root="2.16.840.1.113883.4.6"/>
                    <assignedPerson>
                        <name>
                            <given>Robert</given>
                            <family>Johnson</family>
                        </name>
                    </assignedPerson>
                </assignedEntity>
            </responsibleParty>
        </encompassingEncounter>
    </componentOf>
    
    <component>
        <structuredBody>
            <component>
                <section>
                    <templateId root="2.16.840.1.113883.10.20.22.2.6.1"/>
                    <code code="48765-2" codeSystem="2.16.840.1.113883.6.1" displayName="Allergies"/>
                    <title>Allergies</title>
                    <entry>
                        <observation classCode="OBS" moodCode="EVN">
                            <templateId root="2.16.840.1.113883.10.20.22.4.7"/>
                            <statusCode code="completed"/>
                            <effectiveTime value="20220101"/>
                            <participant typeCode="CSM">
                                <participantRole classCode="MANU">
                                    <playingEntity classCode="MMAT">
                                        <code code="1191" displayName="Aspirin" codeSystem="2.16.840.1.113883.6.88"/>
                                        <name>Aspirin</name>
                                    </playingEntity>
                                </participantRole>
                            </participant>
                        </observation>
                    </entry>
                </section>
            </component>
            
            <component>
                <section>
                    <templateId root="2.16.840.1.113883.10.20.22.2.1.1"/>
                    <code code="10160-0" codeSystem="2.16.840.1.113883.6.1" displayName="Medications"/>
                    <title>Medications</title>
                    <entry>
                        <substanceAdministration classCode="SBADM" moodCode="EVN">
                            <templateId root="2.16.840.1.113883.10.20.22.4.16"/>
                            <statusCode code="completed"/>
                            <effectiveTime value="20230101"/>
                            <doseQuantity value="10" unit="mg"/>
                            <consumable>
                                <manufacturedProduct>
                                    <manufacturedMaterial>
                                        <code code="197361" displayName="Lisinopril" codeSystem="2.16.840.1.113883.6.88"/>
                                        <name>Lisinopril 10mg</name>
                                    </manufacturedMaterial>
                                </manufacturedProduct>
                            </consumable>
                        </substanceAdministration>
                    </entry>
                </section>
            </component>
        </structuredBody>
    </component>
</ClinicalDocument>'''
    
    @pytest.fixture
    def invalid_xml(self):
        """Create invalid XML for testing error handling."""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<InvalidDocument>
    <content>This is not a CDA document</content>
</InvalidDocument>'''
    
    @pytest.fixture
    def malformed_xml(self):
        """Create malformed XML for testing error handling."""
        return '''<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument>
    <unclosed_tag>
</ClinicalDocument>'''
    
    def test_ingester_initialization(self, ingester):
        """Test CDA ingester initializes properly."""
        assert ingester is not None
        assert hasattr(ingester, 'parser')
        assert hasattr(ingester, 'mapper_registry')
        assert hasattr(ingester, 'logger')
    
    def test_parse_valid_cda_document(self, ingester, sample_cda_xml):
        """Test parsing a valid CDA document."""
        result = ingester.parse(sample_cda_xml)
        
        assert isinstance(result, PulseClinicalContent)
        assert result.patient is not None
        assert result.patient.id == "12345"
        assert result.patient.gender == "f"
        
    def test_parse_patient_information(self, ingester, sample_cda_xml):
        """Test parsing patient information from CDA."""
        result = ingester.parse(sample_cda_xml)
        patient = result.patient
        
        assert patient.id == "12345"
        assert patient.gender == "f"
        assert patient.geographic_area == "NY"
        assert patient.dob_year == 1975  # Based on birthTime 19750315
    
    def test_parse_encounter_information(self, ingester, sample_cda_xml):
        """Test parsing encounter information from CDA."""
        result = ingester.parse(sample_cda_xml)
        
        assert result.encounter is not None
        
        encounter = result.encounter
        assert encounter.id == "ENC123"
        assert encounter.encounter_type == "ambulatory"
        assert encounter.location == "Cardiology Clinic"
        
        # Check providers
        assert encounter.providers is not None
        assert len(encounter.providers) == 1
        provider = encounter.providers[0]
        assert provider.name == "Robert Johnson"
        assert provider.id == "DOC123"
        assert provider.type_code == "physician"
    
    def test_parse_allergies_section(self, ingester, sample_cda_xml):
        """Test parsing allergies section from CDA."""
        result = ingester.parse(sample_cda_xml)
        
        assert result.allergies is not None
        assert len(result.allergies) == 1
        
        allergy = result.allergies[0]
        assert allergy.substance == "Aspirin"
        assert allergy.coding_method == "2.16.840.1.113883.6.88"
    
    def test_parse_medications_section(self, ingester, sample_cda_xml):
        """Test parsing medications section from CDA."""
        result = ingester.parse(sample_cda_xml)
        
        assert result.medications is not None
        assert len(result.medications) == 1
        
        medication = result.medications[0]
        assert medication.name == "Lisinopril 10mg"
        assert medication.code == "197361"
        assert medication.coding_method == "2.16.840.1.113883.6.88"
        assert medication.dose == "10 mg"
        assert medication.status == "active"
    
    def test_is_cda_document_valid(self, ingester):
        """Test CDA document validation."""
        root = ET.fromstring('''
        <ClinicalDocument xmlns="urn:hl7-org:v3">
            <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
        </ClinicalDocument>
        ''')
        assert ingester._is_cda_document(root) is True
    
    def test_is_cda_document_invalid(self, ingester):
        """Test CDA document validation with invalid document."""
        root = ET.fromstring('''
        <SomeOtherDocument xmlns="urn:hl7-org:v3">
            <content>Not a CDA</content>
        </SomeOtherDocument>
        ''')
        assert ingester._is_cda_document(root) is False
    
    def test_find_elements_with_namespace(self, ingester):
        """Test finding elements with namespace handling."""
        xml = '''
        <root xmlns="urn:hl7-org:v3">
            <child>value1</child>
            <child>value2</child>
        </root>
        '''
        root = ET.fromstring(xml)
        elements = ingester._find_elements(root, './/child')
        assert len(elements) == 2
    
    def test_find_elements_without_namespace(self, ingester):
        """Test finding elements without namespace."""
        xml = '''
        <root>
            <child>value1</child>
            <child>value2</child>
        </root>
        '''
        root = ET.fromstring(xml)
        elements = ingester._find_elements(root, './/child')
        assert len(elements) == 2
    
    def test_parse_malformed_xml_raises_validation_error(self, ingester, malformed_xml):
        """Test that malformed XML raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ingester.parse(malformed_xml)
        assert "Invalid XML" in str(exc_info.value)
    
    def test_parse_invalid_cda_raises_schema_validation_error(self, ingester, invalid_xml):
        """Test that invalid CDA raises SchemaValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            ingester.parse(invalid_xml)
        assert "not a valid CDA document" in str(exc_info.value)
    
    def test_parse_empty_sections(self, ingester):
        """Test parsing CDA with empty sections."""
        minimal_cda = '''<?xml version="1.0" encoding="UTF-8"?>
<ClinicalDocument xmlns="urn:hl7-org:v3">
    <templateId root="2.16.840.1.113883.10.20.22.1.1"/>
    <templateId root="2.16.840.1.113883.10.20.22.1.2"/>
    <recordTarget>
        <patientRole>
            <id extension="TEST123" root="2.16.840.1.113883.19.5"/>
            <patient>
                <name><given>Test</given><family>Patient</family></name>
            </patient>
        </patientRole>
    </recordTarget>
    <component>
        <structuredBody>
        </structuredBody>
    </component>
</ClinicalDocument>'''
        
        result = ingester.parse(minimal_cda)
        assert isinstance(result, PulseClinicalContent)
        assert result.patient.id is not None
    
    def test_convert_to_clinical_content_empty_data(self, ingester):
        """Test converting empty parsed data to clinical content."""
        empty_data = {}
        result = ingester._convert_to_clinical_content(empty_data)
        assert isinstance(result, PulseClinicalContent)
        assert result.patient is None
        assert result.encounter is None
        assert result.allergies == []


class TestCDAIngesterIntegration:
    """Integration tests with real CDA files."""
    
    @pytest.fixture
    def ingester(self):
        """Create CDA ingester instance."""
        return CDAIngester()
    
    def test_parse_emerge_patient_file(self, ingester):
        """Test parsing a real EMERGE patient file."""
        test_file = Path("test_data/sample_ccdas/EMERGE/Patient-0.xml")
        if test_file.exists():
            with open(test_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            result = ingester.parse(content)
            assert isinstance(result, PulseClinicalContent)
            assert result.patient is not None
            # Verify patient has meaningful data
            assert result.patient.id is not None
    
    def test_parse_cerner_sample_file(self, ingester):
        """Test parsing a Cerner sample file."""
        test_file = Path("test_data/sample_ccdas/Cerner Samples/problems-and-medications.xml")
        if test_file.exists():
            with open(test_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            result = ingester.parse(content)
            assert isinstance(result, PulseClinicalContent)
            assert result.patient is not None
    
    def test_parse_hl7_sample_file(self, ingester):
        """Test parsing an HL7 sample file."""
        test_file = Path("test_data/sample_ccdas/HL7 Samples/CCD.sample.xml")
        if test_file.exists():
            with open(test_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            result = ingester.parse(content)
            assert isinstance(result, PulseClinicalContent)
            assert result.patient is not None
