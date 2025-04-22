# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
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

"""Unit tests for the ClinicalSectionChunker."""

import pytest
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock, Mock

from pulsepipe.pipelines.chunkers.clinical_chunker import ClinicalSectionChunker
from pulsepipe.models.clinical_content import PulseClinicalContent


class TestClinicalSectionChunker:
    """Tests for the ClinicalSectionChunker class."""
    
    @pytest.fixture
    def chunker(self):
        """Create a ClinicalSectionChunker instance."""
        return ClinicalSectionChunker()
    
    @pytest.fixture
    def chunker_no_metadata(self):
        """Create a ClinicalSectionChunker instance with metadata disabled."""
        return ClinicalSectionChunker(include_metadata=False)
    
    @pytest.fixture
    def mock_patient(self):
        """Create a mock patient."""
        patient = Mock()
        patient.id = "P12345"
        return patient
    
    @pytest.fixture
    def mock_encounter(self):
        """Create a mock encounter."""
        encounter = Mock()
        encounter.id = "E67890"
        return encounter
    
    @pytest.fixture
    def mock_vital_signs(self):
        """Create mock vital signs."""
        vs1 = Mock()
        vs1.model_dump.return_value = {"id": "VS1", "name": "Blood Pressure", "value": "120/80"}
        vs2 = Mock()
        vs2.model_dump.return_value = {"id": "VS2", "name": "Heart Rate", "value": "72"}
        return [vs1, vs2]
    
    @pytest.fixture
    def mock_allergies(self):
        """Create mock allergies."""
        allergy = Mock()
        allergy.model_dump.return_value = {"id": "A1", "substance": "Penicillin", "reaction": "Rash"}
        return [allergy]
    
    @pytest.fixture
    def mock_content(self, mock_patient, mock_encounter, mock_vital_signs, mock_allergies):
        """Create a mock clinical content object."""
        content = Mock(spec=PulseClinicalContent)
        content.patient = mock_patient
        content.encounter = mock_encounter
        content.vital_signs = mock_vital_signs
        content.allergies = mock_allergies
        content.medications = []
        content.diagnoses = []
        
        # Set up __dict__ to simulate a real PulseClinicalContent object
        content.__dict__ = {
            "patient": mock_patient,
            "encounter": mock_encounter,
            "vital_signs": mock_vital_signs,
            "allergies": mock_allergies,
            "medications": [],
            "diagnoses": []
        }
        
        return content
    
    @pytest.fixture
    def mock_empty_content(self):
        """Create empty clinical content."""
        content = Mock(spec=PulseClinicalContent)
        content.patient = None
        content.encounter = None
        content.__dict__ = {
            "patient": None,
            "encounter": None,
            "vital_signs": [],
            "allergies": [],
            "medications": [],
            "diagnoses": []
        }
        return content
    
    def test_initialization(self, chunker):
        """Test chunker initialization."""
        assert chunker.include_metadata is True
        assert chunker.logger is not None
    
    def test_initialization_no_metadata(self, chunker_no_metadata):
        """Test initialization with metadata disabled."""
        assert chunker_no_metadata.include_metadata is False
        assert chunker_no_metadata.logger is not None
    
    def test_chunk_with_content(self, chunker, mock_content):
        """Test chunking with content that has multiple sections."""
        chunks = chunker.chunk(mock_content)
        
        # Should have 2 chunks for: vital_signs, allergies (the only ones with data)
        assert len(chunks) == 2
        
        # Check that each chunk has the right structure
        for chunk in chunks:
            assert "type" in chunk
            assert "content" in chunk
            assert "metadata" in chunk
            assert chunk["metadata"]["patient_id"] == "P12345"
            assert chunk["metadata"]["encounter_id"] == "E67890"
        
        # Verify each section type is present
        chunk_types = [chunk["type"] for chunk in chunks]
        assert "vital_signs" in chunk_types
        assert "allergies" in chunk_types
        
        # Check content of a specific chunk
        vital_signs_chunk = next(c for c in chunks if c["type"] == "vital_signs")
        assert len(vital_signs_chunk["content"]) == 2
        assert vital_signs_chunk["content"][0]["id"] == "VS1"
    
    def test_chunk_without_metadata(self, chunker_no_metadata, mock_content):
        """Test chunking without including metadata."""
        chunks = chunker_no_metadata.chunk(mock_content)
        
        # Should have 2 chunks for sections with data
        assert len(chunks) == 2
        
        # Check that chunks don't have metadata
        for chunk in chunks:
            assert "type" in chunk
            assert "content" in chunk
            assert "metadata" not in chunk
    
    def test_chunk_empty_content(self, chunker, mock_empty_content):
        """Test chunking with empty content."""
        chunks = chunker.chunk(mock_empty_content)
        
        # Should have 0 chunks since there are no lists with items
        assert len(chunks) == 0
    
    def test_chunk_with_missing_patient_id(self, chunker, mock_content, mock_patient):
        """Test chunking when patient ID is missing."""
        # Remove patient ID
        mock_patient.id = None
        
        chunks = chunker.chunk(mock_content)
        
        # Should still produce chunks
        assert len(chunks) > 0
        # Check patient ID is None in metadata
        assert chunks[0]["metadata"]["patient_id"] is None
        assert chunks[0]["metadata"]["encounter_id"] == "E67890"
    
    def test_chunk_with_missing_encounter_id(self, chunker, mock_content, mock_encounter):
        """Test chunking when encounter ID is missing."""
        # Remove encounter ID
        mock_encounter.id = None
        
        chunks = chunker.chunk(mock_content)
        
        # Should still produce chunks
        assert len(chunks) > 0
        # Check encounter ID is None in metadata
        assert chunks[0]["metadata"]["patient_id"] == "P12345"
        assert chunks[0]["metadata"]["encounter_id"] is None
    
    def test_chunk_with_null_patient_and_encounter(self, chunker, mock_vital_signs):
        """Test chunking when patient and encounter are null."""
        # Create a new content object with null patient and encounter
        content = Mock(spec=PulseClinicalContent)
        content.patient = None
        content.encounter = None
        content.vital_signs = mock_vital_signs
        content.allergies = []
        
        # Set up __dict__ to simulate a real PulseClinicalContent object
        content.__dict__ = {
            "patient": None,
            "encounter": None,
            "vital_signs": mock_vital_signs,
            "allergies": []
        }
        
        chunks = chunker.chunk(content)
        
        # Should still produce chunks
        assert len(chunks) > 0
        # Check patient and encounter ID are None in metadata
        assert chunks[0]["metadata"]["patient_id"] is None
        assert chunks[0]["metadata"]["encounter_id"] is None
    
    def test_logging(self, chunker, mock_content):
        """Test that logging is done properly."""
        with patch.object(chunker.logger, 'info') as mock_info:
            chunks = chunker.chunk(mock_content)
            
            # Verify log message contains the right information
            mock_info.assert_called_once()
            log_message = mock_info.call_args[0][0]
            assert "ClinicalSectionChunker produced" in log_message
            assert "patient_id=P12345" in log_message
            assert "encounter_id=E67890" in log_message