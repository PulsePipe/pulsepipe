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

"""Unit tests for the OperationalEntityChunker."""

import pytest
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock, Mock

from pulsepipe.pipelines.chunkers.operational_chunker import OperationalEntityChunker


class TestOperationalEntityChunker:
    """Tests for the OperationalEntityChunker class."""
    
    def test_initialization(self):
        """Test chunker initialization."""
        chunker = OperationalEntityChunker()
        assert chunker.include_metadata is True
        assert chunker.logger is not None
    
    def test_initialization_no_metadata(self):
        """Test initialization with metadata disabled."""
        chunker = OperationalEntityChunker(include_metadata=False)
        assert chunker.include_metadata is False
        assert chunker.logger is not None
    
    def test_chunk_with_none_content(self):
        """Test chunking when content is None."""
        chunker = OperationalEntityChunker()
        
        with patch.object(chunker.logger, 'warning') as mock_warning:
            chunks = chunker.chunk(None)
            
            # Verify results
            assert len(chunks) == 0
            mock_warning.assert_called_once()
            assert "Received None content in chunker" in mock_warning.call_args[0][0]
    
    def test_chunk_with_incorrect_content_type(self):
        """Test chunking with incorrect content type."""
        chunker = OperationalEntityChunker()
        # Use a dictionary instead of the proper model
        content = {"not": "a valid model"}
        
        with patch.object(chunker.logger, 'warning') as mock_warning:
            chunks = chunker.chunk(content)
            
            # Verify results
            assert len(chunks) == 0
            mock_warning.assert_called_once()
            assert "Unexpected content type in chunker" in mock_warning.call_args[0][0]