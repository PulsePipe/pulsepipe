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
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

import pytest
from unittest.mock import patch, MagicMock

from pulsepipe.utils.factory import create_adapter, create_ingester
from pulsepipe.adapters.file_watcher import FileWatcherAdapter
from pulsepipe.ingesters.fhir_ingester import FHIRIngester
from pulsepipe.ingesters.hl7v2_ingester import HL7v2Ingester
from pulsepipe.ingesters.x12_ingester import X12Ingester
from pulsepipe.ingesters.plaintext_ingester import PlainTextIngester


class TestFactory:
    @pytest.fixture
    def file_watcher_config(self):
        return {
            "type": "file_watcher", 
            "watch_path": "./incoming/test", 
            "extensions": [".txt", ".json"],
            "continuous": True
        }
    
    @patch('pulsepipe.utils.factory.load_config')
    def test_create_adapter_file_watcher(self, mock_load_config, file_watcher_config):
        mock_load_config.return_value = {"logging": {"level": "INFO"}}
        
        adapter = create_adapter(file_watcher_config)
        
        assert isinstance(adapter, FileWatcherAdapter)
        assert getattr(adapter, 'single_scan_mode', False) is False
        
        # Test with single_scan flag
        adapter = create_adapter(file_watcher_config, single_scan=True)
        
        assert isinstance(adapter, FileWatcherAdapter)
        assert getattr(adapter, 'single_scan_mode', False) is True
    
    def test_create_adapter_unsupported(self):
        with pytest.raises(ValueError) as excinfo:
            create_adapter({"type": "unsupported_adapter"})
        
        assert "Unsupported adapter type: unsupported_adapter" in str(excinfo.value)
    
    def test_create_ingester_fhir(self):
        ingester = create_ingester({"type": "fhir"})
        assert isinstance(ingester, FHIRIngester)
    
    def test_create_ingester_hl7v2(self):
        ingester = create_ingester({"type": "hl7v2"})
        assert isinstance(ingester, HL7v2Ingester)
    
    def test_create_ingester_x12(self):
        ingester = create_ingester({"type": "x12"})
        assert isinstance(ingester, X12Ingester)
    
    def test_create_ingester_plaintext(self):
        ingester = create_ingester({"type": "plaintext"})
        assert isinstance(ingester, PlainTextIngester)
    
    def test_create_ingester_unsupported(self):
        with pytest.raises(ValueError) as excinfo:
            create_ingester({"type": "unsupported_ingester"})
        
        assert "Unsupported ingester type: unsupported_ingester" in str(excinfo.value)