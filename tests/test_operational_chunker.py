import pytest
from unittest.mock import Mock, patch

from pulsepipe.pipelines.chunkers.operational_chunker import OperationalEntityChunker
from pulsepipe.models.operational_content import PulseOperationalContent
from pulsepipe.models.clinical_content import PulseClinicalContent


class MockPydanticModel:
    """Mock Pydantic model for testing."""
    def __init__(self, data):
        self.data = data
    
    def model_dump(self):
        return self.data


@pytest.fixture
def chunker():
    return OperationalEntityChunker()


@pytest.fixture
def chunker_no_metadata():
    return OperationalEntityChunker(include_metadata=False)


class TestOperationalEntityChunker:
    
    def test_init_default(self):
        chunker = OperationalEntityChunker()
        assert chunker.include_metadata is True
        assert chunker.logger is not None

    def test_init_with_metadata_false(self):
        chunker = OperationalEntityChunker(include_metadata=False)
        assert chunker.include_metadata is False

    @patch('pulsepipe.pipelines.chunkers.operational_chunker.LogFactory')
    def test_init_logger_setup(self, mock_log_factory):
        mock_logger = Mock()
        mock_log_factory.get_logger.return_value = mock_logger
        
        chunker = OperationalEntityChunker()
        
        mock_log_factory.get_logger.assert_called_once_with('pulsepipe.pipelines.chunkers.operational_chunker')
        mock_logger.info.assert_called_once_with("📁 Initializing OperationalEntityChunker")

    def test_chunk_none_content(self, chunker):
        result = chunker.chunk(None)
        assert result == []

    def test_chunk_none_content_warning(self, chunker):
        with patch.object(chunker.logger, 'warning') as mock_warning:
            chunker.chunk(None)
            mock_warning.assert_called_once_with("Received None content in chunker, skipping")

    def test_chunk_unexpected_content_type(self, chunker):
        # Test with a string instead of expected content types
        result = chunker.chunk("invalid_content")
        assert result == []

    def test_chunk_unexpected_content_type_warning(self, chunker):
        with patch.object(chunker.logger, 'warning') as mock_warning:
            chunker.chunk("invalid_content")
            mock_warning.assert_called_once_with("Unexpected content type in chunker: <class 'str'>")

    def test_chunk_operational_content_basic(self, chunker):
        # Create a proper PulseOperationalContent object
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ORG123"
        )
        
        # Add test data after creation
        content.claims = [MockPydanticModel({"claim_id": "C001", "amount": 100.0})]
        content.payments = [MockPydanticModel({"payment_id": "P001", "amount": 50.0})]
        
        result = chunker.chunk(content)
        
        # Should have chunks for 'claims' and 'payments'
        assert len(result) == 2
        
        # Find chunks by type
        claims_chunk = next((chunk for chunk in result if chunk["type"] == "claims"), None)
        payments_chunk = next((chunk for chunk in result if chunk["type"] == "payments"), None)
        
        assert claims_chunk is not None
        assert payments_chunk is not None
        
        # Check content
        assert claims_chunk["content"][0] == {"claim_id": "C001", "amount": 100.0}
        assert payments_chunk["content"][0] == {"payment_id": "P001", "amount": 50.0}
        
        # Check metadata
        assert claims_chunk["metadata"]["transaction_type"] == "835"
        assert claims_chunk["metadata"]["organization_id"] == "ORG123"

    def test_chunk_operational_content_no_metadata(self, chunker_no_metadata):
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ORG123"
        )
        
        content.claims = [MockPydanticModel({"claim_id": "C001", "amount": 100.0})]
        
        result = chunker_no_metadata.chunk(content)
        
        assert len(result) == 1
        assert "metadata" not in result[0]
        assert "type" in result[0]
        assert "content" in result[0]

    def test_chunk_empty_lists_ignored(self, chunker):
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ORG123"
        )
        
        # All lists are empty by default
        result = chunker.chunk(content)
        
        # Should have no chunks since no non-empty lists
        assert len(result) == 0

    def test_chunk_unknown_transaction_type(self, chunker):
        content = PulseOperationalContent(
            transaction_type=None,
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ORG123"
        )
        
        content.claims = [MockPydanticModel({"id": "1"})]
        
        result = chunker.chunk(content)
        
        assert len(result) == 1
        assert result[0]["metadata"]["transaction_type"] == "unknown"

    def test_chunk_unknown_organization_id(self, chunker):
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id=None
        )
        
        content.claims = [MockPydanticModel({"id": "1"})]
        
        result = chunker.chunk(content)
        
        assert len(result) == 1
        assert result[0]["metadata"]["organization_id"] == "unknown"

    def test_chunk_model_dump_called(self, chunker):
        """Test that model_dump is called on list items."""
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="TEST123"
        )
        
        # Create mock objects that track model_dump calls
        mock_item1 = Mock()
        mock_item1.model_dump.return_value = {"id": "1", "value": "test1"}
        mock_item2 = Mock()
        mock_item2.model_dump.return_value = {"id": "2", "value": "test2"}
        
        content.claims = [mock_item1, mock_item2]
        
        result = chunker.chunk(content)
        
        assert len(result) == 1
        chunk = result[0]
        
        # Verify model_dump was called on each item
        mock_item1.model_dump.assert_called_once()
        mock_item2.model_dump.assert_called_once()
        
        # Verify the dumped data is in the chunk
        assert chunk["content"] == [
            {"id": "1", "value": "test1"},
            {"id": "2", "value": "test2"}
        ]

    # TODO: Fix clinical content mocking
    # def test_chunk_clinical_content(self, chunker):
    #     """Test chunking with clinical content - simplified test."""
    #     # Create a simple mock object that will pass isinstance check for clinical content
    #     content = Mock()
    #     content.__class__ = type('MockClinicalContent', (PulseClinicalContent,), {})
    #     content.transaction_type = None
    #     content.organization_id = "CLINIC456"
    #     content.patients = [MockPydanticModel({"patient_id": "P001", "name": "John Doe"})]
    #     content.__dict__ = {
    #         "transaction_type": None,
    #         "organization_id": "CLINIC456",
    #         "patients": [MockPydanticModel({"patient_id": "P001", "name": "John Doe"})]
    #     }
    #     
    #     result = chunker.chunk(content)
    #     
    #     assert len(result) == 1
    #     patients_chunk = result[0]
    #     assert patients_chunk["type"] == "patients"
    #     assert patients_chunk["content"][0] == {"patient_id": "P001", "name": "John Doe"}
    #     assert patients_chunk["metadata"]["transaction_type"] == "unknown"
    #     assert patients_chunk["metadata"]["organization_id"] == "CLINIC456"

    def test_chunk_logging_info(self, chunker):
        content = PulseOperationalContent(
            transaction_type="835",
            interchange_control_number="12345",
            functional_group_control_number="67890",
            organization_id="ORG123"
        )
        
        content.claims = [MockPydanticModel({"claim_id": "C001", "amount": 100.0})]
        content.payments = [MockPydanticModel({"payment_id": "P001", "amount": 50.0})]
        
        with patch.object(chunker.logger, 'info') as mock_info:
            chunker.chunk(content)
            
            # Verify final info log
            mock_info.assert_called_with(
                "🧩 OperationalEntityChunker produced 2 chunks 🧠 (transaction_type=835, org_id=ORG123)"
            )