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

# tests/test_cli_model_extended.py

import os
import json
import pytest
from unittest.mock import patch, MagicMock, mock_open
from click.testing import CliRunner
from pydantic import BaseModel, Field

from pulsepipe.cli.main import cli
from pulsepipe.cli.command.model import (
    model, schema, validate, list as model_list, example,
    generate_example_from_schema
)


# Create test model classes for mocking (prefix 'Mock' to avoid pytest collecting as test class)
class MockPatient(BaseModel):
    id: str = Field(..., description="Patient identifier")
    name: str = Field(..., description="Patient name")
    birth_date: str = Field(None, description="Patient date of birth")
    
    @classmethod
    def model_json_schema(cls):
        return {
            "title": "Patient",
            "description": "Patient demographic information",
            "type": "object",
            "properties": {
                "id": {"type": "string", "description": "Patient identifier"},
                "name": {"type": "string", "description": "Patient name"},
                "birth_date": {"type": "string", "description": "Patient date of birth"}
            },
            "required": ["id", "name"]
        }
    
    @classmethod
    def get_example(cls):
        return {
            "id": "patient123",
            "name": "John Doe",
            "birth_date": "1980-01-01"
        }


class TestCliModelExtended:
    """Extended tests for the CLI model command."""
    
    @pytest.fixture
    def mock_config_loader(self):
        """Mock for the config_loader function."""
        with patch('pulsepipe.cli.main.load_config') as mock:
            mock.return_value = {"logging": {"show_banner": False}}
            yield mock
    
    def test_model_help_command(self, mock_config_loader):
        """Test the model command shows help text."""
        runner = CliRunner()
        
        # Check basic help text instead of trying complex mocking
        result = runner.invoke(cli, ["model", "--help"])
        
        # Check the command execution
        assert result.exit_code == 0
        assert "Manage and explore data models" in result.output

    def test_model_schema_help_command(self, mock_config_loader):
        """Test the model schema command help text."""
        runner = CliRunner()
        
        # Check schema help text
        result = runner.invoke(cli, ["model", "schema", "--help"])
        
        # Check the command execution
        assert result.exit_code == 0
        assert "Display schema for a specified model" in result.output

    def test_model_validate_help_command(self, mock_config_loader):
        """Test the model validate command help text."""
        runner = CliRunner()
        
        # Check validate help text
        result = runner.invoke(cli, ["model", "validate", "--help"])
        
        # Check the command execution
        assert result.exit_code == 0
        assert "Validate JSON data against a model schema" in result.output

    def test_model_list_help_command(self, mock_config_loader):
        """Test the model list command help text."""
        runner = CliRunner()
        
        # Check list help text
        result = runner.invoke(cli, ["model", "list", "--help"])
        
        # Check the command execution
        assert result.exit_code == 0
        assert "List available models in the pulsepipe package" in result.output
    
    def test_model_example_help_command(self, mock_config_loader):
        """Test the model example command help text."""
        runner = CliRunner()
        
        # Check example help text
        result = runner.invoke(cli, ["model", "example", "--help"])
        
        # Check the command execution
        assert result.exit_code == 0
        assert "Generate example JSON for a model" in result.output

    def test_generate_example_from_schema(self):
        """Test the generate_example_from_schema function."""
        # Test object schema
        object_schema = {
            "type": "object",
            "properties": {
                "id": {"type": "string"},
                "age": {"type": "integer"},
                "active": {"type": "boolean"}
            },
            "required": ["id", "age"]
        }
        
        result = generate_example_from_schema(object_schema)
        assert result["id"] == "example"
        assert result["age"] == 0
        assert "active" not in result  # Not required, so not included
        
        # Test array schema
        array_schema = {
            "type": "array",
            "items": {
                "type": "string"
            }
        }
        
        result = generate_example_from_schema(array_schema)
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0] == "example"
        
        # Test enum schema
        enum_schema = {
            "type": "string",
            "enum": ["option1", "option2", "option3"]
        }
        
        result = generate_example_from_schema(enum_schema)
        assert result == "option1"
        
        # Test date format schema
        date_schema = {
            "type": "string",
            "format": "date"
        }
        
        result = generate_example_from_schema(date_schema)
        assert result == "2023-01-01"

    def test_model_example_command(self, mock_config_loader):
        """Test the model example command."""
        runner = CliRunner()
        
        # Mock importlib.import_module to return a module with our test model
        with patch('importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.Patient = MockPatient
            mock_import.return_value = mock_module
            
            # Run the model example command with UTF-8 output to avoid Windows issues
            # when parsing output with json.loads
            result = runner.invoke(cli, ["model", "example", "pulsepipe.models.patient.Patient"])
            
            # Check the command execution
            assert result.exit_code == 0
            
            # Make sure we have output
            assert result.output.strip()
            
            # First strip any potential BOM or whitespace
            clean_output = result.output.strip().lstrip('\ufeff')
            
            # Verify output is valid JSON - using Windows-safe parsing
            try:
                example_json = json.loads(clean_output)
                assert example_json["id"] == "patient123"
                assert example_json["name"] == "John Doe"
                assert example_json["birth_date"] == "1980-01-01"
            except json.JSONDecodeError as e:
                # If we hit a decode error, print diagnostic info
                print(f"JSON decode error: {str(e)}")
                print(f"Output content (repr): {repr(clean_output)}")
                print(f"Output length: {len(clean_output)}")
                
                # Try parsing character by character to identify the issue
                for i, char in enumerate(clean_output):
                    print(f"Char {i}: {repr(char)} (ord: {ord(char)})")
                    if i > 10:  # Just show first few characters
                        break
                        
                # Force the test to fail with the original error
                raise

    def test_model_example_fallback_to_schema(self, mock_config_loader):
        """Test the model example command falling back to schema when get_example not available."""
        runner = CliRunner()
        
        # Create a model class without get_example method
        class ModelWithoutExample(BaseModel):
            name: str
            value: int
            
            @classmethod
            def model_json_schema(cls):
                return {
                    "type": "object",
                    "properties": {
                        "name": {"type": "string"},
                        "value": {"type": "integer"}
                    },
                    "required": ["name", "value"]
                }
        
        # Mock importlib.import_module to return a module with our test model
        with patch('importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.ModelNoExample = ModelWithoutExample
            mock_import.return_value = mock_module
            
            # Run the model example command
            result = runner.invoke(cli, ["model", "example", "pulsepipe.models.test.ModelNoExample"])
            
            # Check the command execution
            assert result.exit_code == 0
            
            # Make sure we have output
            assert result.output.strip()
            
            # Clean output for Windows compatibility
            clean_output = result.output.strip().lstrip('\ufeff')
            
            # Verify output is valid JSON based on schema
            try:
                example_json = json.loads(clean_output)
                assert example_json["name"] == "example"
                assert example_json["value"] == 0
            except json.JSONDecodeError as e:
                # If we hit a decode error, print diagnostic info
                print(f"JSON decode error: {str(e)}")
                print(f"Output content (repr): {repr(clean_output)}")
                print(f"Output length: {len(clean_output)}")
                
                # Try parsing character by character to identify the issue
                for i, char in enumerate(clean_output):
                    print(f"Char {i}: {repr(char)} (ord: {ord(char)})")
                    if i > 10:  # Just show first few characters
                        break
                        
                # Force the test to fail with the original error
                raise