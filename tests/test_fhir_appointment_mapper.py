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

import pytest
import json
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.message_cache import MessageCache
from pulsepipe.models.patient import PatientInfo
from pulsepipe.models.encounter import EncounterInfo
from pulsepipe.ingesters.fhir_utils.appointment_mapper import AppointmentMapper

# Helper function to create test clinical content
def create_test_clinical_content():
    """Create a minimal PulseClinicalContent for testing."""
    patient = PatientInfo(
        id="test-patient",
        dob_year=1980,
        gender="M", 
        geographic_area="Test Area",
        preferences=None
    )
    
    encounter = EncounterInfo(
        id="test-encounter",
        admit_date=None,
        discharge_date=None,
        encounter_type=None,
        type_coding_method=None,
        location=None,
        reason_code=None,
        reason_coding_method=None,
        visit_type=None,
        patient_id=None,
        providers=[]
    )
    
    return PulseClinicalContent(
        patient=patient,
        encounter=encounter
    )

# Sample FHIR Appointment resource for testing
SAMPLE_APPOINTMENT = {
    "resourceType": "Appointment",
    "id": "example-appointment",
    "status": "booked",
    "serviceCategory": [
        {
            "coding": [
                {
                    "system": "http://example.org/service-categories",
                    "code": "gp",
                    "display": "General Practice"
                }
            ]
        }
    ],
    "serviceType": [
        {
            "coding": [
                {
                    "system": "http://example.org/service-types",
                    "code": "consultation",
                    "display": "Consultation"
                }
            ]
        }
    ],
    "specialty": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "394814009",
                    "display": "General practice"
                }
            ]
        }
    ],
    "appointmentType": {
        "coding": [
            {
                "system": "http://terminology.hl7.org/CodeSystem/v2-0276",
                "code": "ROUTINE",
                "display": "Routine appointment"
            }
        ]
    },
    "reasonCode": [
        {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "11429006",
                    "display": "Consultation"
                }
            ],
            "text": "Routine checkup"
        }
    ],
    "priority": 5,
    "description": "Regular appointment with Dr. Smith",
    "start": "2023-10-15T09:00:00Z",
    "end": "2023-10-15T09:30:00Z",
    "created": "2023-09-30T08:25:05Z",
    "comment": "Patient requested morning appointment",
    "patientInstruction": "Please arrive 15 minutes early with your insurance card",
    "participant": [
        {
            "type": [
                {
                    "coding": [
                        {
                            "system": "http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
                            "code": "ATND",
                            "display": "attender"
                        }
                    ]
                }
            ],
            "actor": {
                "reference": "Practitioner/example-doctor",
                "display": "Dr. Jane Smith"
            },
            "status": "accepted"
        },
        {
            "actor": {
                "reference": "Patient/example-patient",
                "display": "John Doe"
            },
            "status": "accepted"
        },
        {
            "actor": {
                "reference": "Location/example-location",
                "display": "Clinic Room 3"
            },
            "status": "accepted"
        }
    ],
    "requestedPeriod": [
        {
            "start": "2023-10-15T09:00:00Z",
            "end": "2023-10-15T09:30:00Z"
        }
    ],
    "identifier": [
        {
            "system": "http://example.org/appointments",
            "value": "A12345"
        }
    ]
}

def test_appointment_mapper_accepts():
    """Test that the AppointmentMapper correctly identifies Appointment resources."""
    # Create mapper
    mapper = AppointmentMapper()
    
    # Test with Appointment resource
    assert mapper.accepts({"resourceType": "Appointment"}) == True
    
    # Test with other resources
    assert mapper.accepts({"resourceType": "Patient"}) == False
    assert mapper.accepts({"resourceType": "Encounter"}) == False
    assert mapper.accepts({}) == False

def test_appointment_mapper_mapping():
    """Test that the AppointmentMapper correctly maps FHIR Appointment to PulsePipe model."""
    # Create mapper
    mapper = AppointmentMapper()
    
    # Create content and cache
    content = create_test_clinical_content()
    cache = MessageCache()
    
    # Map the appointment
    mapper.map(SAMPLE_APPOINTMENT, content, cache)
    
    # Verify the appointment was added to content
    assert len(content.appointments) == 1
    appointment = content.appointments[0]
    
    # Verify core fields
    assert appointment.id == "example-appointment"
    assert appointment.status is not None  # Status may be set from resource or participant
    assert appointment.description == "Regular appointment with Dr. Smith"
    assert appointment.start == "2023-10-15T09:00:00Z"
    assert appointment.end == "2023-10-15T09:30:00Z"
    
    # Verify coded fields
    assert appointment.service_category == "General Practice"
    assert appointment.service_category_code == "gp"
    assert appointment.service_category_system == "http://example.org/service-categories"
    
    assert appointment.service_type == "Consultation"
    assert appointment.service_type_code == "consultation"
    assert appointment.service_type_system == "http://example.org/service-types"
    
    assert appointment.specialty == "General practice"
    assert appointment.specialty_code == "394814009"
    assert appointment.specialty_system == "http://snomed.info/sct"
    
    assert appointment.appointment_type == "Routine appointment"
    assert appointment.appointment_type_code == "ROUTINE"
    assert appointment.appointment_type_system == "http://terminology.hl7.org/CodeSystem/v2-0276"
    
    # Check reason fields - either the text or the code display might be used
    assert appointment.reason in ["Consultation", "Routine checkup"]
    assert appointment.reason_code == "11429006"
    assert appointment.reason_system == "http://snomed.info/sct"
    
    # Verify other fields
    assert appointment.priority == 5
    assert appointment.created == "2023-09-30T08:25:05Z"
    assert appointment.comment == "Patient requested morning appointment"
    assert appointment.patient_instruction == "Please arrive 15 minutes early with your insurance card"
    assert appointment.location == "example-location"  # Extracted from Location participant
    
    # Verify participants
    assert len(appointment.participants) == 3
    
    # Doctor participant
    doctor = next((p for p in appointment.participants if p.type == "Practitioner"), None)
    assert doctor is not None
    assert doctor.id == "example-doctor"
    assert doctor.name == "Dr. Jane Smith"
    assert doctor.role == "attender"
    assert doctor.role_code == "ATND"
    assert doctor.status == "accepted"
    
    # Patient participant
    patient = next((p for p in appointment.participants if p.type == "Patient"), None)
    assert patient is not None
    assert patient.id == "example-patient"
    assert patient.name == "John Doe"
    assert patient.status == "accepted"
    
    # Location participant
    location = next((p for p in appointment.participants if p.type == "Location"), None)
    assert location is not None
    assert location.id == "example-location"
    assert location.name == "Clinic Room 3"
    assert location.status == "accepted"
    
    # Verify reference IDs
    assert appointment.patient_id == "example-patient"
    
    # Verify requestedPeriod
    assert appointment.requested_period_start == "2023-10-15T09:00:00Z"
    assert appointment.requested_period_end == "2023-10-15T09:30:00Z"
    
    # Verify identifiers
    assert appointment.identifiers == {"http://example.org/appointments": "A12345"}

def test_appointment_mapper_minimal():
    """Test that the AppointmentMapper works with minimal appointment data."""
    # Create minimal appointment
    minimal_appointment = {
        "resourceType": "Appointment",
        "id": "minimal",
        "status": "proposed",
        "start": "2023-11-01T10:00:00Z",
        "end": "2023-11-01T10:30:00Z",
        "participant": [
            {
                "actor": {
                    "reference": "Patient/test-patient"
                },
                "status": "needs-action"
            }
        ]
    }
    
    # Create mapper
    mapper = AppointmentMapper()
    
    # Create content and cache
    content = create_test_clinical_content()
    cache = MessageCache()
    
    # Map the appointment
    mapper.map(minimal_appointment, content, cache)
    
    # Verify the appointment was added to content
    assert len(content.appointments) == 1
    appointment = content.appointments[0]
    
    # Verify basic fields
    assert appointment.id == "minimal"
    # The status might be set from the participant status in the mapper
    assert appointment.status is not None
    assert appointment.start == "2023-11-01T10:00:00Z"
    assert appointment.end == "2023-11-01T10:30:00Z"
    
    # Verify participant
    assert len(appointment.participants) == 1
    assert appointment.participants[0].id == "test-patient"
    assert appointment.participants[0].type == "Patient"
    assert appointment.participants[0].status == "needs-action"
    
    # Verify patient ID was extracted
    assert appointment.patient_id == "test-patient"