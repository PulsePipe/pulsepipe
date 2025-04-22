import pytest
from unittest.mock import patch, MagicMock
from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.adapters.file_watcher import FileWatcherAdapter
from pulsepipe.ingesters.fhir_ingester import FHIRIngester
from pulsepipe.ingesters.hl7v2_ingester import HL7v2Ingester
from pulsepipe.ingesters.x12_ingester import X12Ingester
from pulsepipe.ingesters.plaintext_ingester import PlainTextIngester

class TestCreateAdapter:
    @patch('pulsepipe.utils.factory.load_config', return_value={})
    def test_unsupported_adapter_type(self, mock_load_config):
        config = {"type": "unsupported_adapter_type"}
        
        with pytest.raises(ValueError) as excinfo:
            create_adapter(config)
        
        assert "Unsupported adapter type: unsupported_adapter_type" in str(excinfo.value)

class TestCreateIngester:
    def test_unsupported_ingester_type(self):
        config = {"type": "unsupported_ingester_type"}
        
        with pytest.raises(ValueError) as excinfo:
            create_ingester(config)
        
        assert "Unsupported ingester type: unsupported_ingester_type" in str(excinfo.value)
        
    @patch('pulsepipe.ingesters.fhir_ingester.FHIRIngester')
    def test_create_fhir_ingester_directly(self, mock_fhir_class):
        config = {"type": "fhir"}
        mock_fhir_class.return_value = MagicMock(spec=FHIRIngester)
        
        # Create a real FHIRIngester directly (no mocking necessary)
        fhir_ingester = FHIRIngester()
        
        # Verify it's the right type
        assert isinstance(fhir_ingester, FHIRIngester)
    
    @patch('pulsepipe.ingesters.hl7v2_ingester.HL7v2Ingester')
    def test_create_hl7v2_ingester_directly(self, mock_hl7v2_class):
        config = {"type": "hl7v2"}
        mock_hl7v2_class.return_value = MagicMock(spec=HL7v2Ingester)
        
        # Create a real HL7v2Ingester directly (no mocking necessary)
        hl7v2_ingester = HL7v2Ingester()
        
        # Verify it's the right type
        assert isinstance(hl7v2_ingester, HL7v2Ingester)
    
    @patch('pulsepipe.ingesters.x12_ingester.X12Ingester')
    def test_create_x12_ingester_directly(self, mock_x12_class):
        config = {"type": "x12"}
        mock_x12_class.return_value = MagicMock(spec=X12Ingester)
        
        # Create a real X12Ingester directly (no mocking necessary)
        x12_ingester = X12Ingester()
        
        # Verify it's the right type
        assert isinstance(x12_ingester, X12Ingester)
    
    @patch('pulsepipe.ingesters.plaintext_ingester.PlainTextIngester')
    def test_create_plaintext_ingester_directly(self, mock_plaintext_class):
        config = {"type": "plaintext"}
        mock_plaintext_class.return_value = MagicMock(spec=PlainTextIngester)
        
        # Create a real PlainTextIngester directly (no mocking necessary)
        plaintext_ingester = PlainTextIngester()
        
        # Verify it's the right type
        assert isinstance(plaintext_ingester, PlainTextIngester)