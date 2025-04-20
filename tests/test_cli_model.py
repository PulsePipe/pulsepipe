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

# tests/test_cli_model.py

import pytest
from unittest.mock import patch, MagicMock
from click.testing import CliRunner

from pulsepipe.cli.main import cli


class TestCliModel:
    """Tests for the CLI model command."""
    
    @pytest.fixture
    def mock_config_loader(self):
        """Mock for the config_loader function."""
        with patch('pulsepipe.cli.main.load_config') as mock:
            mock.return_value = {"logging": {"show_banner": False}}
            yield mock
    
    def test_model_list_command(self, mock_config_loader):
        """Test the model list command."""
        runner = CliRunner()
        
        # Run the model list command with the --all flag
        result = runner.invoke(cli, ["model", "list", "--all"])
        
        # Check the command execution - it should at least run without errors
        assert result.exit_code == 0
        
        # The CLI returns help text when no flag is provided
        result_no_flag = runner.invoke(cli, ["model", "list"])
        assert result_no_flag.exit_code == 0
        assert "--all" in result_no_flag.output
        assert "--clinical" in result_no_flag.output
        assert "--operational" in result_no_flag.output
        
    def test_model_schema_command(self, mock_config_loader):
        """Test the model schema command."""
        runner = CliRunner()
        
        # Run the model schema command with fully qualified name
        result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.patient.Patient"])
        
        # Check the command execution
        assert result.exit_code == 0