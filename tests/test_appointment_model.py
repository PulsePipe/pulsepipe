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
from datetime import datetime
from pulsepipe.models.appointment import AppointmentInfo, AppointmentParticipant
from pulsepipe.models.clinical_content import PulseClinicalContent

def test_appointment_participant_model():
    """Test that the AppointmentParticipant model can be created and accessed correctly."""
    # Create an appointment participant
    participant = AppointmentParticipant(
        id="p123",
        type="Practitioner",
        type_code="doctor",
        type_system="http://terminology.hl7.org/CodeSystem/v3-ParticipationType",
        name="Dr. Jane Smith",
        role="attending",
        role_code="ATND",
        role_system="http://terminology.hl7.org/CodeSystem/v3-ParticipationRole",
        status="accepted",
        period_start="2023-10-01T09:00:00",
        period_end="2023-10-01T09:30:00"
    )
    
    # Check fields
    assert participant.id == "p123"
    assert participant.type == "Practitioner"
    assert participant.type_code == "doctor"
    assert participant.type_system == "http://terminology.hl7.org/CodeSystem/v3-ParticipationType"
    assert participant.name == "Dr. Jane Smith"
    assert participant.role == "attending"
    assert participant.role_code == "ATND"
    assert participant.role_system == "http://terminology.hl7.org/CodeSystem/v3-ParticipationRole"
    assert participant.status == "accepted"
    assert participant.period_start == "2023-10-01T09:00:00"
    assert participant.period_end == "2023-10-01T09:30:00"

def test_appointment_info_model():
    """Test that the AppointmentInfo model can be created and accessed correctly."""
    # Create participants for the appointment
    doctor = AppointmentParticipant(
        id="d456",
        type="Practitioner",
        name="Dr. Jane Smith",
        role="attending",
        status="accepted"
    )
    
    patient = AppointmentParticipant(
        id="p789",
        type="Patient",
        name="John Doe",
        role="patient",
        status="accepted"
    )
    
    # Create an appointment
    appointment = AppointmentInfo(
        id="appt123",
        status="booked",
        service_category="Cardiology",
        service_category_code="394579002",
        service_category_system="http://snomed.info/sct",
        service_type="Initial consultation",
        service_type_code="11429006",
        service_type_system="http://snomed.info/sct",
        specialty="Cardiology",
        specialty_code="394579002",
        specialty_system="http://snomed.info/sct",
        appointment_type="CHECKUP",
        appointment_type_code="CHECKUP",
        appointment_type_system="http://terminology.hl7.org/CodeSystem/v2-0276",
        reason="Annual checkup",
        reason_code="encounter-reason",
        reason_system="http://example.org/fhir/code-systems/encounter-reason",
        priority=1,
        description="Annual cardiology checkup",
        start="2023-10-01T09:00:00",
        end="2023-10-01T09:30:00",
        created="2023-09-15T14:30:00",
        comment="Patient requested morning appointment",
        patient_instruction="Please arrive 15 minutes early to complete paperwork",
        canceled_reason=None,
        location="RM102",
        participants=[doctor, patient],
        requested_period_start="2023-10-01T09:00:00",
        requested_period_end="2023-10-01T09:30:00",
        patient_id="p789",
        encounter_id=None,
        identifiers={"http://hospital.example.org/identifiers/appointments": "A123456"}
    )
    
    # Check fields
    assert appointment.id == "appt123"
    assert appointment.status == "booked"
    assert appointment.service_category == "Cardiology"
    assert appointment.service_category_code == "394579002"
    assert appointment.service_category_system == "http://snomed.info/sct"
    assert appointment.service_type == "Initial consultation"
    assert appointment.service_type_code == "11429006"
    assert appointment.service_type_system == "http://snomed.info/sct"
    assert appointment.specialty == "Cardiology"
    assert appointment.specialty_code == "394579002"
    assert appointment.specialty_system == "http://snomed.info/sct"
    assert appointment.appointment_type == "CHECKUP"
    assert appointment.appointment_type_code == "CHECKUP"
    assert appointment.appointment_type_system == "http://terminology.hl7.org/CodeSystem/v2-0276"
    assert appointment.reason == "Annual checkup"
    assert appointment.reason_code == "encounter-reason"
    assert appointment.reason_system == "http://example.org/fhir/code-systems/encounter-reason"
    assert appointment.priority == 1
    assert appointment.description == "Annual cardiology checkup"
    assert appointment.start == "2023-10-01T09:00:00"
    assert appointment.end == "2023-10-01T09:30:00"
    assert appointment.created == "2023-09-15T14:30:00"
    assert appointment.comment == "Patient requested morning appointment"
    assert appointment.patient_instruction == "Please arrive 15 minutes early to complete paperwork"
    assert appointment.canceled_reason is None
    assert appointment.location == "RM102"
    assert len(appointment.participants) == 2
    assert appointment.requested_period_start == "2023-10-01T09:00:00"
    assert appointment.requested_period_end == "2023-10-01T09:30:00"
    assert appointment.patient_id == "p789"
    assert appointment.encounter_id is None
    assert appointment.identifiers == {"http://hospital.example.org/identifiers/appointments": "A123456"}
    
    # Check participants
    assert appointment.participants[0].id == "d456"
    assert appointment.participants[0].name == "Dr. Jane Smith"
    assert appointment.participants[1].id == "p789"
    assert appointment.participants[1].name == "John Doe"

def test_appointments_in_clinical_content():
    """Test that appointments can be added to PulseClinicalContent."""
    # Create a simple appointment
    appointment = AppointmentInfo(
        id="appt123",
        status="booked",
        description="Annual checkup",
        start="2023-10-01T09:00:00",
        end="2023-10-01T09:30:00"
    )

    # Create patient and encounter to satisfy validation
    from pulsepipe.models.patient import PatientInfo
    from pulsepipe.models.encounter import EncounterInfo

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

    # Create clinical content with the appointment
    content = PulseClinicalContent(
        patient=patient,
        encounter=encounter,
        appointments=[appointment]
    )
    
    # Check that the appointment is in the clinical content
    assert len(content.appointments) == 1
    assert content.appointments[0].id == "appt123"
    assert content.appointments[0].status == "booked"
    
    # Test adding another appointment
    content.appointments.append(AppointmentInfo(
        id="appt456",
        status="cancelled",
        description="Follow-up visit",
        start="2023-10-15T14:00:00",
        end="2023-10-15T14:30:00"
    ))
    
    # Check that both appointments are in the content
    assert len(content.appointments) == 2
    assert content.appointments[1].id == "appt456"
    assert content.appointments[1].status == "cancelled"
    
    # Test summary method includes appointments
    summary = content.summary()
    assert "📅" in summary  # Emoji for appointments should be in summary