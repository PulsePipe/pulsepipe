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

# tests/test_cda_mappers.py

import pytest
from datetime import datetime

from pulsepipe.ingesters.cda_utils.patient_mapper import PatientMapper
from pulsepipe.ingesters.cda_utils.allergy_mapper import AllergyMapper
from pulsepipe.ingesters.cda_utils.medication_mapper import MedicationMapper
from pulsepipe.ingesters.cda_utils.problem_mapper import ProblemMapper
from pulsepipe.ingesters.cda_utils.procedure_mapper import ProcedureMapper
from pulsepipe.ingesters.cda_utils.vital_sign_mapper import VitalSignMapper
from pulsepipe.ingesters.cda_utils.immunization_mapper import ImmunizationMapper
from pulsepipe.ingesters.cda_utils.lab_report_mapper import LabReportMapper
from pulsepipe.ingesters.cda_utils.encounter_mapper import EncounterMapper
from pulsepipe.ingesters.cda_utils.base_mapper import CDAMapperRegistry

from pulsepipe.models import (
    PatientInfo, Allergy, Medication, Problem, Procedure, 
    VitalSign, Immunization, LabObservation, LabReport, EncounterInfo
)


class TestPatientMapper:
    """Test suite for PatientMapper."""
    
    @pytest.fixture
    def mapper(self):
        return PatientMapper()
    
    @pytest.fixture
    def patient_data(self):
        return {
            'identifiers': [
                {'root': '2.16.840.1.113883.19.5.99999.2', 'extension': '12345', 'assigning_authority': 'Hospital'},
                {'root': '2.16.840.1.113883.4.1', 'extension': '555-12-3456', 'assigning_authority': 'SSN'}
            ],
            'name': {
                'given': ['John', 'Michael'],
                'family': ['Doe'],
                'prefix': ['Mr.'],
                'suffix': ['Jr.']
            },
            'gender': {'code': 'M', 'display': 'Male'},
            'birth_date': '19800115',
            'race': {'code': '2106-3', 'display': 'White', 'system': '2.16.840.1.113883.6.238'},
            'ethnicity': {'code': '2186-5', 'display': 'Not Hispanic or Latino', 'system': '2.16.840.1.113883.6.238'},
            'address': {
                'street': ['123 Main St', 'Apt 4B'],
                'city': 'Anytown',
                'state': 'NY',
                'postal_code': '12345',
                'country': 'US',
                'use': 'HP'
            },
            'telecom': [
                {'value': 'tel:+15551234567', 'use': 'HP'},
                {'value': 'email:john.doe@example.com', 'use': 'WP'}
            ]
        }
    
    def test_map_complete_patient_data(self, mapper, patient_data):
        """Test mapping complete patient data."""
        result = mapper.map(patient_data)
        
        assert isinstance(result, PatientInfo)
        assert result.id == "12345"
        assert result.gender == "m"
        assert result.geographic_area == "NY"
        assert result.dob_year == 1980
        assert result.over_90 == False
        assert "2.16.840.1.113883.19.5.99999.2" in result.identifiers
    
    def test_map_minimal_patient_data(self, mapper):
        """Test mapping minimal patient data."""
        minimal_data = {
            'identifiers': [{'extension': 'TEST123', 'root': 'test.system'}]
        }
        
        result = mapper.map(minimal_data)
        assert isinstance(result, PatientInfo)
        assert result.id == "TEST123"
    
    def test_parse_date_valid(self, mapper):
        """Test parsing valid date formats."""
        assert mapper._parse_date('20230101') is not None
        assert mapper._parse_date('20230101120000') is not None
        assert mapper._parse_date('2023-01-01') is not None
    
    def test_parse_date_invalid(self, mapper):
        """Test parsing invalid date formats."""
        assert mapper._parse_date('invalid') is None
        assert mapper._parse_date(None) is None
        assert mapper._parse_date('') is None


class TestAllergyMapper:
    """Test suite for AllergyMapper."""
    
    @pytest.fixture
    def mapper(self):
        return AllergyMapper()
    
    @pytest.fixture
    def allergy_data(self):
        return {
            'substance': {'code': '1191', 'display': 'Aspirin', 'system': '2.16.840.1.113883.6.88'},
            'substance_name': 'Aspirin 325mg',
            'onset_date': '20220101',
            'status': 'active'
        }
    
    def test_map_complete_allergy_data(self, mapper, allergy_data):
        """Test mapping complete allergy data."""
        result = mapper.map(allergy_data)
        
        assert isinstance(result, Allergy)
        assert result.substance == "Aspirin 325mg"
        assert result.coding_method == "2.16.840.1.113883.6.88"
    
    def test_map_minimal_allergy_data(self, mapper):
        """Test mapping minimal allergy data."""
        minimal_data = {
            'substance': {'display': 'Penicillin'}
        }
        
        result = mapper.map(minimal_data)
        assert isinstance(result, Allergy)
        assert result.substance == "Penicillin"


class TestMedicationMapper:
    """Test suite for MedicationMapper."""
    
    @pytest.fixture
    def mapper(self):
        return MedicationMapper()
    
    @pytest.fixture
    def medication_data(self):
        return {
            'medication': {'code': '197361', 'display': 'Lisinopril', 'system': '2.16.840.1.113883.6.88'},
            'medication_name': 'Lisinopril 10mg tablets',
            'dosage': {'value': '10', 'unit': 'mg'},
            'start_date': '20230101'
        }
    
    def test_map_complete_medication_data(self, mapper, medication_data):
        """Test mapping complete medication data."""
        result = mapper.map(medication_data)
        
        assert isinstance(result, Medication)
        assert result.name == "Lisinopril 10mg tablets"
        assert result.code == "197361"
        assert result.coding_method == "2.16.840.1.113883.6.88"
        assert result.dose == "10 mg"
        assert result.status == "active"


class TestProblemMapper:
    """Test suite for ProblemMapper."""
    
    @pytest.fixture
    def mapper(self):
        return ProblemMapper()
    
    @pytest.fixture
    def problem_data(self):
        return {
            'problem': {'code': 'I10', 'display': 'Essential hypertension', 'system': '2.16.840.1.113883.6.90'},
            'onset_date': '20220601',
            'status': 'active'
        }
    
    def test_map_complete_problem_data(self, mapper, problem_data):
        """Test mapping complete problem data."""
        result = mapper.map(problem_data)
        
        assert isinstance(result, Problem)
        assert result.description == "Essential hypertension"
        assert result.code == "I10"
        assert result.coding_method == "2.16.840.1.113883.6.90"


class TestProcedureMapper:
    """Test suite for ProcedureMapper."""
    
    @pytest.fixture
    def mapper(self):
        return ProcedureMapper()
    
    @pytest.fixture
    def procedure_data(self):
        return {
            'procedure': {'code': '80146002', 'display': 'Excision of appendix', 'system': '2.16.840.1.113883.6.96'},
            'performed_date': '20230115'
        }
    
    def test_map_complete_procedure_data(self, mapper, procedure_data):
        """Test mapping complete procedure data."""
        result = mapper.map(procedure_data)
        
        assert isinstance(result, Procedure)
        assert result.description == "Excision of appendix"
        assert result.code == "80146002"
        assert result.coding_method == "2.16.840.1.113883.6.96"
        assert result.status == "completed"


class TestVitalSignMapper:
    """Test suite for VitalSignMapper."""
    
    @pytest.fixture
    def mapper(self):
        return VitalSignMapper()
    
    @pytest.fixture
    def vital_sign_data(self):
        return {
            'vital_sign': {'code': '8480-6', 'display': 'Systolic blood pressure', 'system': '2.16.840.1.113883.6.1'},
            'value': {'value': '120', 'unit': 'mmHg'},
            'recorded_date': '20230101120000'
        }
    
    def test_map_complete_vital_sign_data(self, mapper, vital_sign_data):
        """Test mapping complete vital sign data."""
        result = mapper.map(vital_sign_data)
        
        assert isinstance(result, VitalSign)
        assert result.display == "Systolic blood pressure"
        assert result.code == "8480-6"
        assert result.coding_method == "2.16.840.1.113883.6.1"
        assert result.value == 120.0
        assert result.unit == "mmHg"
    
    def test_map_non_numeric_value(self, mapper):
        """Test mapping vital sign with non-numeric value."""
        data = {
            'vital_sign': {'display': 'General appearance'},
            'value': {'value': 'alert', 'unit': ''}
        }
        
        result = mapper.map(data)
        assert isinstance(result, VitalSign)
        assert result.value == "alert"  # Non-numeric value kept as string


class TestImmunizationMapper:
    """Test suite for ImmunizationMapper."""
    
    @pytest.fixture
    def mapper(self):
        return ImmunizationMapper()
    
    @pytest.fixture
    def immunization_data(self):
        return {
            'vaccine': {'code': '08', 'display': 'Hep B, adolescent or pediatric', 'system': '2.16.840.1.113883.12.292'},
            'administration_date': '20220301'
        }
    
    def test_map_complete_immunization_data(self, mapper, immunization_data):
        """Test mapping complete immunization data."""
        result = mapper.map(immunization_data)
        
        assert isinstance(result, Immunization)
        assert result.description == "Hep B, adolescent or pediatric"
        assert result.vaccine_code == "08"
        assert result.coding_method == "2.16.840.1.113883.12.292"
        assert result.status == "completed"


class TestLabReportMapper:
    """Test suite for LabReportMapper."""
    
    @pytest.fixture
    def mapper(self):
        return LabReportMapper()
    
    @pytest.fixture
    def lab_result_data(self):
        return {
            'test': {'code': '2093-3', 'display': 'Cholesterol [Mass/volume] in Serum or Plasma', 'system': '2.16.840.1.113883.6.1'},
            'result': {'value': '180', 'unit': 'mg/dL'},
            'reference_range': {'low': '100', 'high': '199', 'unit': 'mg/dL'},
            'collected_date': '20230101080000'
        }
    
    def test_map_complete_lab_result_data(self, mapper, lab_result_data):
        """Test mapping complete lab result data."""
        result = mapper.map(lab_result_data)
        
        assert isinstance(result, LabReport)
        assert len(result.observations) == 1
        obs = result.observations[0]
        assert obs.name == "Cholesterol [Mass/volume] in Serum or Plasma"
        assert obs.code == "2093-3"
        assert obs.coding_method == "2.16.840.1.113883.6.1"
        assert obs.value == "180"
        assert obs.unit == "mg/dL"
        assert obs.reference_range == "100-199 mg/dL"


class TestEncounterMapper:
    """Test suite for EncounterMapper."""
    
    @pytest.fixture
    def mapper(self):
        return EncounterMapper()
    
    @pytest.fixture
    def encounter_data(self):
        return {
            'id': 'ENC123',
            'class_code': 'AMB',
            'type_code': 'Ambulatory visit',
            'start_date': '20230101080000',
            'end_date': '20230101100000',
            'location': {
                'name': 'Cardiology Clinic',
                'service_line': 'Cardiology',
                'department': 'CARD',
                'facility': 'Good Health Hospital'
            },
            'providers': [
                {
                    'name': 'Dr. John Smith',
                    'id': 'DOC123',
                    'role': 'attending',
                    'type': 'physician'
                }
            ],
            'diagnoses': [
                {
                    'code': 'I10',
                    'display': 'Essential hypertension',
                    'system': '2.16.840.1.113883.6.90',
                    'type': 'primary'
                }
            ],
            'drg': {
                'code': '194',
                'description': 'Simple pneumonia & pleurisy w CC'
            }
        }
    
    def test_map_complete_encounter_data(self, mapper, encounter_data):
        """Test mapping complete encounter data."""
        result = mapper.map(encounter_data)
        
        assert isinstance(result, EncounterInfo)
        assert result.id == "ENC123"
        assert result.encounter_type == "ambulatory"
        assert result.visit_type == "Ambulatory visit"
        assert result.location == "Cardiology Clinic"
        assert result.admit_date == "2023-01-01"
        assert result.discharge_date == "2023-01-01"
        
        # Check providers
        assert result.providers is not None
        assert len(result.providers) == 1
        assert result.providers[0].name == "Dr. John Smith"
        assert result.providers[0].id == "DOC123"
        assert result.providers[0].type_code == "physician"


class TestCDAMapperRegistry:
    """Test suite for CDAMapperRegistry."""
    
    @pytest.fixture
    def registry(self):
        return CDAMapperRegistry()
    
    def test_registry_initialization(self, registry):
        """Test registry initializes with all mappers."""
        expected_mappers = [
            'patient', 'encounter', 'allergy', 'medication', 
            'problem', 'procedure', 'vital_sign', 'immunization', 
            'lab_report'
        ]
        
        for mapper_type in expected_mappers:
            mapper = registry.get_mapper(mapper_type)
            assert mapper is not None
    
    def test_get_unknown_mapper_raises_error(self, registry):
        """Test getting unknown mapper raises ValueError."""
        with pytest.raises(ValueError) as exc_info:
            registry.get_mapper('unknown_mapper')
        assert "Unknown mapper type" in str(exc_info.value)
    
    def test_register_new_mapper(self, registry):
        """Test registering a new mapper."""
        class CustomMapper:
            def map(self, data):
                return "custom"
        
        custom_mapper = CustomMapper()
        registry.register_mapper('custom', custom_mapper)
        
        retrieved_mapper = registry.get_mapper('custom')
        assert retrieved_mapper == custom_mapper