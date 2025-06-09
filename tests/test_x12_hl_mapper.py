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

# tests/test_x12_hl_mapper.py

import pytest
from unittest.mock import Mock, patch
from pulsepipe.ingesters.x12_utils.hl_mapper import HLMapper


class TestHLMapper:
    """Test suite for X12 HL (Hierarchical Level) segment mapper"""

    def setup_method(self):
        """Set up test fixtures"""
        self.mapper = HLMapper()
        self.mock_content = Mock()
        self.cache = {}

    def test_initialization(self):
        """Test HLMapper initialization"""
        assert self.mapper.typeCode == "HL"
        assert self.mapper.logger is not None

    def test_accepts_hl_segment(self):
        """Test that mapper accepts HL segment ID"""
        assert self.mapper.accepts("HL") is True

    def test_accepts_rejects_non_hl_segments(self):
        """Test that mapper rejects non-HL segment IDs"""
        assert self.mapper.accepts("ST") is False
        assert self.mapper.accepts("SE") is False
        assert self.mapper.accepts("CLP") is False
        assert self.mapper.accepts("SVC") is False
        assert self.mapper.accepts("") is False
        assert self.mapper.accepts(None) is False

    def test_map_complete_hl_segment(self):
        """Test mapping complete HL segment with all elements"""
        elements = ["1", "0", "20"]  # HL ID, Parent ID, Level Code
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify cache values
        assert self.cache["hl_id"] == "1"
        assert self.cache["hl_parent"] == "0"
        assert self.cache["hl_code"] == "20"
        
        # Verify hierarchy tracking
        assert "hl_hierarchy" in self.cache
        assert "1" in self.cache["hl_hierarchy"]
        assert self.cache["hl_hierarchy"]["1"]["parent"] == "0"
        assert self.cache["hl_hierarchy"]["1"]["code"] == "20"

    def test_map_hl_segment_minimal_elements(self):
        """Test mapping HL segment with only ID element"""
        elements = ["2"]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify cache values
        assert self.cache["hl_id"] == "2"
        assert self.cache["hl_parent"] is None
        assert self.cache["hl_code"] is None
        
        # Verify hierarchy tracking
        assert "hl_hierarchy" in self.cache
        assert "2" in self.cache["hl_hierarchy"]
        assert self.cache["hl_hierarchy"]["2"]["parent"] is None
        assert self.cache["hl_hierarchy"]["2"]["code"] is None

    def test_map_hl_segment_with_id_and_parent(self):
        """Test mapping HL segment with ID and parent only"""
        elements = ["3", "1"]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify cache values
        assert self.cache["hl_id"] == "3"
        assert self.cache["hl_parent"] == "1"
        assert self.cache["hl_code"] is None
        
        # Verify hierarchy tracking
        assert "hl_hierarchy" in self.cache
        assert "3" in self.cache["hl_hierarchy"]
        assert self.cache["hl_hierarchy"]["3"]["parent"] == "1"
        assert self.cache["hl_hierarchy"]["3"]["code"] is None

    def test_map_empty_elements(self):
        """Test mapping with empty elements list"""
        elements = []
        
        # This should raise IndexError as expected
        with pytest.raises(IndexError):
            self.mapper.map("HL", elements, self.mock_content, self.cache)

    def test_map_with_extra_elements(self):
        """Test mapping with more than 3 elements (should use first 3)"""
        elements = ["4", "2", "22", "extra1", "extra2"]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify only first 3 elements are used
        assert self.cache["hl_id"] == "4"
        assert self.cache["hl_parent"] == "2"
        assert self.cache["hl_code"] == "22"

    def test_map_multiple_hl_segments_hierarchy_building(self):
        """Test mapping multiple HL segments builds hierarchy correctly"""
        # First HL segment - top level
        elements1 = ["1", "", "20"]  # Empty parent for top level
        self.mapper.map("HL", elements1, self.mock_content, self.cache)
        
        # Second HL segment - child of first
        elements2 = ["2", "1", "21"]
        self.mapper.map("HL", elements2, self.mock_content, self.cache)
        
        # Third HL segment - child of second
        elements3 = ["3", "2", "22"]
        self.mapper.map("HL", elements3, self.mock_content, self.cache)
        
        # Verify final cache state reflects last segment
        assert self.cache["hl_id"] == "3"
        assert self.cache["hl_parent"] == "2"
        assert self.cache["hl_code"] == "22"
        
        # Verify complete hierarchy is maintained
        hierarchy = self.cache["hl_hierarchy"]
        assert len(hierarchy) == 3
        
        assert hierarchy["1"]["parent"] == ""
        assert hierarchy["1"]["code"] == "20"
        
        assert hierarchy["2"]["parent"] == "1"
        assert hierarchy["2"]["code"] == "21"
        
        assert hierarchy["3"]["parent"] == "2"
        assert hierarchy["3"]["code"] == "22"

    def test_map_overwrite_existing_cache_values(self):
        """Test that mapping overwrites existing cache values"""
        # Pre-populate cache
        self.cache["hl_id"] = "old_id"
        self.cache["hl_parent"] = "old_parent"
        self.cache["hl_code"] = "old_code"
        
        elements = ["new_id", "new_parent", "new_code"]
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify values are overwritten
        assert self.cache["hl_id"] == "new_id"
        assert self.cache["hl_parent"] == "new_parent"
        assert self.cache["hl_code"] == "new_code"

    def test_map_preserve_existing_hierarchy(self):
        """Test that existing hierarchy is preserved when adding new entries"""
        # Pre-populate hierarchy
        self.cache["hl_hierarchy"] = {
            "existing_id": {
                "parent": "existing_parent",
                "code": "existing_code"
            }
        }
        
        elements = ["new_id", "new_parent", "new_code"]
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify existing hierarchy is preserved and new entry added
        hierarchy = self.cache["hl_hierarchy"]
        assert len(hierarchy) == 2
        assert "existing_id" in hierarchy
        assert "new_id" in hierarchy
        
        assert hierarchy["existing_id"]["parent"] == "existing_parent"
        assert hierarchy["existing_id"]["code"] == "existing_code"
        assert hierarchy["new_id"]["parent"] == "new_parent"
        assert hierarchy["new_id"]["code"] == "new_code"

    def test_map_with_none_elements(self):
        """Test mapping with None in elements list"""
        elements = [None, None, None]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify None values are preserved
        assert self.cache["hl_id"] is None
        assert self.cache["hl_parent"] is None
        assert self.cache["hl_code"] is None
        
        # Verify hierarchy tracking works with None values
        assert "hl_hierarchy" in self.cache
        assert None in self.cache["hl_hierarchy"]
        assert self.cache["hl_hierarchy"][None]["parent"] is None
        assert self.cache["hl_hierarchy"][None]["code"] is None

    def test_map_with_numeric_strings(self):
        """Test mapping with numeric string values (typical X12 format)"""
        elements = ["001", "000", "020"]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify numeric strings are preserved as strings
        assert self.cache["hl_id"] == "001"
        assert self.cache["hl_parent"] == "000"
        assert self.cache["hl_code"] == "020"

    def test_map_with_alphanumeric_values(self):
        """Test mapping with alphanumeric values"""
        elements = ["A1", "B2", "C3"]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify alphanumeric values are preserved
        assert self.cache["hl_id"] == "A1"
        assert self.cache["hl_parent"] == "B2"
        assert self.cache["hl_code"] == "C3"

    @patch('pulsepipe.utils.log_factory.LogFactory.get_logger')
    def test_initialization_with_logger_mock(self, mock_logger_factory):
        """Test initialization with mocked logger"""
        mock_logger = Mock()
        mock_logger_factory.return_value = mock_logger
        
        mapper = HLMapper()
        
        # Verify logger setup
        mock_logger_factory.assert_called_once_with('pulsepipe.ingesters.x12_utils.hl_mapper')
        assert mapper.logger == mock_logger
        mock_logger.info.assert_called_once_with("📁 Initializing X12 HLMapper")

    @patch('pulsepipe.utils.log_factory.LogFactory.get_logger')
    def test_map_with_debug_logging(self, mock_logger_factory):
        """Test that debug logging is called during mapping"""
        mock_logger = Mock()
        mock_logger_factory.return_value = mock_logger
        
        mapper = HLMapper()
        elements = ["1", "0", "20"]
        
        mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Note: The debug call has a string formatting issue in the original code
        # This test verifies the debug method is called
        mock_logger.debug.assert_called_once()

    def test_map_empty_string_elements(self):
        """Test mapping with empty string elements"""
        elements = ["", "", ""]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify empty strings are preserved
        assert self.cache["hl_id"] == ""
        assert self.cache["hl_parent"] == ""
        assert self.cache["hl_code"] == ""
        
        # Verify hierarchy tracking with empty strings
        assert "hl_hierarchy" in self.cache
        assert "" in self.cache["hl_hierarchy"]
        assert self.cache["hl_hierarchy"][""]["parent"] == ""
        assert self.cache["hl_hierarchy"][""]["code"] == ""

    def test_map_whitespace_elements(self):
        """Test mapping with whitespace-only elements"""
        elements = [" ", "  ", "\t"]
        
        self.mapper.map("HL", elements, self.mock_content, self.cache)
        
        # Verify whitespace is preserved (no trimming)
        assert self.cache["hl_id"] == " "
        assert self.cache["hl_parent"] == "  "
        assert self.cache["hl_code"] == "\t"

    def test_typical_healthcare_hierarchy_codes(self):
        """Test mapping with typical healthcare HL codes"""
        # Test common X12 HL codes used in healthcare
        test_cases = [
            ("1", "", "20"),    # Information Source (Payer)
            ("2", "1", "21"),   # Information Receiver (Provider)
            ("3", "2", "22"),   # Subscriber
            ("4", "3", "23"),   # Patient (if different from subscriber)
        ]
        
        for hl_id, parent, code in test_cases:
            cache = {}
            elements = [hl_id, parent, code]
            
            self.mapper.map("HL", elements, self.mock_content, cache)
            
            assert cache["hl_id"] == hl_id
            assert cache["hl_parent"] == parent
            assert cache["hl_code"] == code
            assert cache["hl_hierarchy"][hl_id]["parent"] == parent
            assert cache["hl_hierarchy"][hl_id]["code"] == code