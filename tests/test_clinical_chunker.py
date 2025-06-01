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

import pytest
from unittest.mock import Mock, patch
from typing import List, Dict, Any

from pulsepipe.pipelines.chunkers.clinical_chunker import ClinicalSectionChunker
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.patient import PatientInfo
from pulsepipe.models.encounter import EncounterInfo
from pulsepipe.models.vital_sign import VitalSign
from pulsepipe.models.allergy import Allergy
from pulsepipe.models.medication import Medication
from pulsepipe.models.note import Note


class TestClinicalSectionChunker:
    
    @pytest.fixture
    def chunker_with_metadata(self):
        """Create a chunker with metadata enabled."""
        return ClinicalSectionChunker(include_metadata=True)
    
    @pytest.fixture
    def chunker_without_metadata(self):
        """Create a chunker with metadata disabled."""
        return ClinicalSectionChunker(include_metadata=False)
    
    @pytest.fixture
    def sample_patient(self):
        """Create a sample patient for testing."""
        return PatientInfo(
            id="patient-123",
            dob_year=1990,
            over_90=False,
            gender="male",
            geographic_area="CA",
            identifiers={},
            preferences=[]
        )
    
    @pytest.fixture
    def sample_encounter(self):
        """Create a sample encounter for testing."""
        return EncounterInfo(
            id="encounter-456",
            admit_date=None,
            discharge_date=None,
            encounter_type="outpatient",
            type_coding_method=None,
            location=None,
            reason_code=None,
            reason_coding_method=None,
            providers=[],
            visit_type="routine",
            patient_id=None
        )
    
    @pytest.fixture
    def sample_vital_signs(self):
        """Create sample vital signs for testing."""
        return [
            VitalSign(
                code="bp",
                coding_method=None,
                display="blood_pressure",
                value="120/80",
                unit="mmHg",
                timestamp=None,
                patient_id=None,
                encounter_id=None
            ),
            VitalSign(
                code="hr",
                coding_method=None,
                display="heart_rate",
                value=72,
                unit="bpm",
                timestamp=None,
                patient_id=None,
                encounter_id=None
            )
        ]
    
    @pytest.fixture
    def sample_allergies(self):
        """Create sample allergies for testing."""
        return [
            Allergy(
                substance="Penicillin",
                coding_method=None,
                reaction="Rash",
                severity="Moderate",
                onset=None,
                patient_id=None
            )
        ]
    
    @pytest.fixture
    def sample_medications(self):
        """Create sample medications for testing."""
        return [
            Medication(
                code=None,
                coding_method=None,
                name="Lisinopril",
                dose="10mg",
                route=None,
                frequency="daily",
                start_date=None,
                end_date=None,
                status=None,
                patient_id=None,
                encounter_id=None,
                notes=None
            )
        ]
    
    @pytest.fixture
    def sample_notes(self):
        """Create sample notes for testing."""
        return [
            Note(
                note_type_code="PN",
                text="Patient is doing well",
                timestamp=None,
                author_id=None,
                author_name="Dr. Smith",
                patient_id=None,
                encounter_id=None
            )
        ]
    
    @pytest.fixture
    def empty_clinical_content(self):
        """Create empty clinical content for testing."""
        return PulseClinicalContent(patient=None, encounter=None)
    
    @pytest.fixture
    def clinical_content_with_patient_encounter(self, sample_patient, sample_encounter):
        """Create clinical content with patient and encounter."""
        return PulseClinicalContent(
            patient=sample_patient,
            encounter=sample_encounter
        )
    
    @pytest.fixture
    def full_clinical_content(self, sample_patient, sample_encounter, sample_vital_signs, 
                             sample_allergies, sample_medications, sample_notes):
        """Create full clinical content with all data types."""
        return PulseClinicalContent(
            patient=sample_patient,
            encounter=sample_encounter,
            vital_signs=sample_vital_signs,
            allergies=sample_allergies,
            medications=sample_medications,
            notes=sample_notes
        )

    def test_init_with_metadata_enabled(self):
        """Test chunker initialization with metadata enabled."""
        chunker = ClinicalSectionChunker(include_metadata=True)
        assert chunker.include_metadata is True
        assert chunker.logger is not None

    def test_init_with_metadata_disabled(self):
        """Test chunker initialization with metadata disabled."""
        chunker = ClinicalSectionChunker(include_metadata=False)
        assert chunker.include_metadata is False
        assert chunker.logger is not None

    def test_init_default_metadata_setting(self):
        """Test chunker initialization with default metadata setting."""
        chunker = ClinicalSectionChunker()
        assert chunker.include_metadata is True

    @patch('pulsepipe.utils.log_factory.LogFactory.get_logger')
    def test_init_logging(self, mock_get_logger):
        """Test that logger is properly initialized."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        ClinicalSectionChunker()
        
        mock_get_logger.assert_called_once_with('pulsepipe.pipelines.chunkers.clinical_chunker')
        mock_logger.info.assert_called_once_with("📁 Initializing ClinicalSectionChunker")

    def test_chunk_empty_content(self, chunker_with_metadata, empty_clinical_content):
        """Test chunking empty clinical content."""
        chunks = chunker_with_metadata.chunk(empty_clinical_content)
        assert chunks == []

    def test_chunk_content_without_lists(self, chunker_with_metadata, clinical_content_with_patient_encounter):
        """Test chunking content that has no list fields with data."""
        chunks = chunker_with_metadata.chunk(clinical_content_with_patient_encounter)
        assert chunks == []

    def test_chunk_content_with_vital_signs_metadata_enabled(self, chunker_with_metadata, sample_patient, 
                                                           sample_encounter, sample_vital_signs):
        """Test chunking content with vital signs and metadata enabled."""
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=sample_encounter,
            vital_signs=sample_vital_signs
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["type"] == "vital_signs"
        assert len(chunk["content"]) == 2
        assert chunk["metadata"]["patient_id"] == "patient-123"
        assert chunk["metadata"]["encounter_id"] == "encounter-456"
        
        # Verify vital signs data is properly serialized
        assert chunk["content"][0]["display"] == "blood_pressure"
        assert chunk["content"][1]["display"] == "heart_rate"

    def test_chunk_content_with_vital_signs_metadata_disabled(self, chunker_without_metadata, sample_patient, 
                                                            sample_encounter, sample_vital_signs):
        """Test chunking content with vital signs and metadata disabled."""
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=sample_encounter,
            vital_signs=sample_vital_signs
        )
        
        chunks = chunker_without_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["type"] == "vital_signs"
        assert len(chunk["content"]) == 2
        assert "metadata" not in chunk

    def test_chunk_content_with_multiple_sections(self, chunker_with_metadata, full_clinical_content):
        """Test chunking content with multiple populated sections."""
        chunks = chunker_with_metadata.chunk(full_clinical_content)
        
        # Should have 4 chunks: vital_signs, allergies, medications, notes
        assert len(chunks) == 4
        
        chunk_types = [chunk["type"] for chunk in chunks]
        assert "vital_signs" in chunk_types
        assert "allergies" in chunk_types
        assert "medications" in chunk_types
        assert "notes" in chunk_types
        
        # All chunks should have metadata
        for chunk in chunks:
            assert chunk["metadata"]["patient_id"] == "patient-123"
            assert chunk["metadata"]["encounter_id"] == "encounter-456"

    def test_chunk_content_missing_patient(self, chunker_with_metadata, sample_encounter, sample_vital_signs):
        """Test chunking content without patient information."""
        content = PulseClinicalContent(
            patient=None,
            encounter=sample_encounter,
            vital_signs=sample_vital_signs
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["metadata"]["patient_id"] is None
        assert chunk["metadata"]["encounter_id"] == "encounter-456"

    def test_chunk_content_missing_encounter(self, chunker_with_metadata, sample_patient, sample_vital_signs):
        """Test chunking content without encounter information."""
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=None,
            vital_signs=sample_vital_signs
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["metadata"]["patient_id"] == "patient-123"
        assert chunk["metadata"]["encounter_id"] is None

    def test_chunk_content_missing_patient_and_encounter(self, chunker_with_metadata, sample_vital_signs):
        """Test chunking content without patient or encounter information."""
        content = PulseClinicalContent(patient=None, encounter=None, vital_signs=sample_vital_signs)
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["metadata"]["patient_id"] is None
        assert chunk["metadata"]["encounter_id"] is None

    def test_chunk_content_patient_without_id(self, chunker_with_metadata, sample_encounter, sample_vital_signs):
        """Test chunking content with patient that has no ID."""
        patient_no_id = PatientInfo(
            id=None,
            dob_year=None,
            over_90=False,
            gender="female",
            geographic_area="NY",
            identifiers={},
            preferences=[]
        )
        
        content = PulseClinicalContent(
            patient=patient_no_id,
            encounter=sample_encounter,
            vital_signs=sample_vital_signs
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["metadata"]["patient_id"] is None
        assert chunk["metadata"]["encounter_id"] == "encounter-456"

    def test_chunk_content_encounter_without_id(self, chunker_with_metadata, sample_patient, sample_vital_signs):
        """Test chunking content with encounter that has no ID."""
        encounter_no_id = EncounterInfo(
            id=None,
            admit_date=None,
            discharge_date=None,
            encounter_type="inpatient",
            type_coding_method=None,
            location=None,
            reason_code=None,
            reason_coding_method=None,
            providers=[],
            visit_type="admission",
            patient_id=None
        )
        
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=encounter_no_id,
            vital_signs=sample_vital_signs
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["metadata"]["patient_id"] == "patient-123"
        assert chunk["metadata"]["encounter_id"] is None

    def test_chunk_content_with_empty_lists(self, chunker_with_metadata, sample_patient, sample_encounter):
        """Test chunking content with explicitly empty lists."""
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=sample_encounter,
            vital_signs=[],  # Explicitly empty
            allergies=[],   # Explicitly empty
            medications=[], # Explicitly empty
        )
        
        chunks = chunker_with_metadata.chunk(content)
        assert chunks == []

    def test_chunk_handles_model_dump_correctly(self, chunker_with_metadata, sample_patient, sample_allergies):
        """Test that chunk properly calls model_dump on list items."""
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=None,
            allergies=sample_allergies
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        
        # Verify the content is properly serialized (model_dump was called)
        assert isinstance(chunk["content"], list)
        assert len(chunk["content"]) == 1
        assert isinstance(chunk["content"][0], dict)
        assert chunk["content"][0]["substance"] == "Penicillin"

    @patch('pulsepipe.utils.log_factory.LogFactory.get_logger')
    def test_chunk_logging_with_patient_encounter(self, mock_get_logger, sample_patient, 
                                                 sample_encounter, sample_vital_signs):
        """Test that chunk logs correctly with patient and encounter IDs."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        chunker = ClinicalSectionChunker()
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=sample_encounter,
            vital_signs=sample_vital_signs
        )
        
        chunker.chunk(content)
        
        # Verify logging calls
        assert mock_logger.info.call_count == 2  # Init + chunk logging
        chunk_log_call = mock_logger.info.call_args_list[1]
        log_message = chunk_log_call[0][0]
        
        assert "[PIPE] 🧩 Created 1 clinical chunks from" in log_message
        assert "patient_id=patient-123" in log_message
        assert "encounter_id=encounter-456" in log_message

    @patch('pulsepipe.utils.log_factory.LogFactory.get_logger')
    def test_chunk_logging_without_patient_encounter(self, mock_get_logger, sample_vital_signs):
        """Test that chunk logs correctly without patient or encounter IDs."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        chunker = ClinicalSectionChunker()
        content = PulseClinicalContent(patient=None, encounter=None, vital_signs=sample_vital_signs)
        
        chunker.chunk(content)
        
        # Verify logging calls
        assert mock_logger.info.call_count == 2  # Init + chunk logging
        chunk_log_call = mock_logger.info.call_args_list[1]
        log_message = chunk_log_call[0][0]
        
        assert "[PIPE] 🧩 Created 1 clinical chunks from" in log_message
        assert "patient_id=None" in log_message
        assert "encounter_id=None" in log_message

    @patch('pulsepipe.utils.log_factory.LogFactory.get_logger')
    def test_chunk_logging_no_chunks_produced(self, mock_get_logger, empty_clinical_content):
        """Test that chunk logs correctly when no chunks are produced."""
        mock_logger = Mock()
        mock_get_logger.return_value = mock_logger
        
        chunker = ClinicalSectionChunker()
        chunker.chunk(empty_clinical_content)
        
        # Verify logging calls
        assert mock_logger.info.call_count == 2  # Init + chunk logging
        chunk_log_call = mock_logger.info.call_args_list[1]
        log_message = chunk_log_call[0][0]
        
        assert "[PIPE] 🧩 Created 0 clinical chunks from" in log_message
        assert "patient_id=None" in log_message
        assert "encounter_id=None" in log_message

    def test_chunk_preserves_order_of_fields(self, chunker_with_metadata, full_clinical_content):
        """Test that chunks are created in the order fields are defined in the model."""
        chunks = chunker_with_metadata.chunk(full_clinical_content)
        
        # The order should match the order of fields in PulseClinicalContent model
        chunk_types = [chunk["type"] for chunk in chunks]
        
        # Based on the model definition, these should appear in this order
        expected_types = ["vital_signs", "allergies", "medications", "notes"]
        assert chunk_types == expected_types

    def test_chunk_content_data_integrity(self, chunker_with_metadata, sample_patient, sample_medications):
        """Test that all data is preserved correctly in chunks."""
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=None,
            medications=sample_medications
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        
        # Verify all medication data is preserved
        medication_data = chunk["content"][0]
        assert medication_data["name"] == "Lisinopril"
        assert medication_data["dose"] == "10mg"
        assert medication_data["frequency"] == "daily"

    def test_chunk_multiple_items_in_same_section(self, chunker_with_metadata, sample_patient):
        """Test chunking when a section has multiple items."""
        multiple_allergies = [
            Allergy(substance="Penicillin", coding_method=None, reaction="Rash", severity="Moderate", onset=None, patient_id=None),
            Allergy(substance="Shellfish", coding_method=None, reaction="Swelling", severity="Severe", onset=None, patient_id=None),
            Allergy(substance="Pollen", coding_method=None, reaction="Sneezing", severity="Mild", onset=None, patient_id=None)
        ]
        
        content = PulseClinicalContent(
            patient=sample_patient,
            encounter=None,
            allergies=multiple_allergies
        )
        
        chunks = chunker_with_metadata.chunk(content)
        
        assert len(chunks) == 1
        chunk = chunks[0]
        assert chunk["type"] == "allergies"
        assert len(chunk["content"]) == 3
        
        # Verify all allergies are included
        allergens = [allergy["substance"] for allergy in chunk["content"]]
        assert "Penicillin" in allergens
        assert "Shellfish" in allergens
        assert "Pollen" in allergens