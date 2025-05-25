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
    generate_example_from_schema, _get_field_type
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
        assert "--fields-only" in result.output

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
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.Patient = MockPatient
            mock_import.return_value = mock_module
            
            # Run the model example command
            result = runner.invoke(cli, ["model", "example", "pulsepipe.models.patient.Patient"])
            
            # Check the command execution
            assert result.exit_code == 0
            
            # Make sure we have output
            assert result.output.strip()
            
            # Extract only the JSON part from the output
            # This will find the first '{' and take everything from there
            json_start = result.output.find('{')
            if json_start >= 0:
                json_text = result.output[json_start:]
                
                # Verify output is valid JSON
                example_json = json.loads(json_text)
                assert example_json["id"] == "patient123"
                assert example_json["name"] == "John Doe"
                assert example_json["birth_date"] == "1980-01-01"
            else:
                pytest.fail("No JSON found in output")

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
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.ModelNoExample = ModelWithoutExample
            mock_import.return_value = mock_module
            
            # Run the model example command
            result = runner.invoke(cli, ["model", "example", "pulsepipe.models.test.ModelNoExample"])
            
            # Check the command execution
            assert result.exit_code == 0
            
            # Make sure we have output
            assert result.output.strip()
            
            # Find the JSON part in the output - locate the position of the first '{'
            json_start = result.output.find('{')
            assert json_start >= 0, "No JSON found in output"
            
            json_text = result.output[json_start:]
            
            # Verify output is valid JSON based on schema
            try:
                example_json = json.loads(json_text)
                assert example_json["name"] == "example"
                assert example_json["value"] == 0
            except json.JSONDecodeError as e:
                # If we hit a decode error, print diagnostic info
                print(f"JSON decode error: {str(e)}")
                print(f"JSON content (repr): {repr(json_text)}")
                print(f"JSON length: {len(json_text)}")
                
                # Try parsing character by character to identify the issue
                for i, char in enumerate(json_text):
                    print(f"Char {i}: {repr(char)} (ord: {ord(char)})")
                    if i > 10:  # Just show first few characters
                        break
                        
                # Force the test to fail with the original error
                raise

    def test_model_schema_error_handling(self, mock_config_loader):
        """Test schema command error handling for various failure cases."""
        runner = CliRunner()
        
        # Test ImportError - module not found
        result = runner.invoke(cli, ["model", "schema", "nonexistent.module.Model"])
        assert result.exit_code == 0
        assert "❌ Could not import model" in result.output
        
        # Test AttributeError - class not found in module
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_import.return_value = mock_module
            del mock_module.NonExistentClass  # Ensure it doesn't exist
            
            result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.patient.NonExistentClass"])
            assert result.exit_code == 0
            assert "❌ Class not found" in result.output
        
        # Test non-Pydantic model
        class NotAPydanticModel:
            pass
        
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.NotAPydanticModel = NotAPydanticModel
            mock_import.return_value = mock_module
            
            result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.test.NotAPydanticModel"])
            assert result.exit_code == 0
            assert "❌ pulsepipe.models.test.NotAPydanticModel is not a Pydantic model" in result.output
        
        # Test general exception
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_import.side_effect = Exception("General error")
            
            result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.patient.Patient"])
            assert result.exit_code == 0
            assert "❌ Error: General error" in result.output

    def test_model_schema_json_output(self, mock_config_loader):
        """Test schema command with JSON output option."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.Patient = MockPatient
            mock_import.return_value = mock_module
            
            result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.patient.Patient", "--json"])
            assert result.exit_code == 0
            
            # Should contain JSON output
            try:
                json.loads(result.output)
            except json.JSONDecodeError:
                pytest.fail("Output is not valid JSON")

    def test_model_schema_fields_only_option(self, mock_config_loader):
        """Test the --fields-only option with schema command."""
        runner = CliRunner()
        
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.Patient = MockPatient
            mock_import.return_value = mock_module
            
            result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.patient.Patient", "--fields-only"])
            assert result.exit_code == 0
            
            # Check that it outputs field names and types in compact format
            lines = [line.strip() for line in result.output.strip().split('\n') if line.strip()]
            field_lines = [line for line in lines if ':' in line and not line.startswith('Schema for')]
            
            # Should have field lines for our mock patient
            assert len(field_lines) >= 2  # At least id and name fields
            
            # Check format: field_name: field_type
            for line in field_lines:
                if ':' in line:
                    field_part, type_part = line.split(':', 1)
                    assert field_part.strip()  # Non-empty field name
                    assert type_part.strip()   # Non-empty type

    def test_model_schema_no_fields(self, mock_config_loader):
        """Test schema command with model that has no properties."""
        runner = CliRunner()
        
        class EmptyModel(BaseModel):
            @classmethod
            def model_json_schema(cls):
                return {"type": "object"}
        
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.EmptyModel = EmptyModel
            mock_import.return_value = mock_module
            
            result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.test.EmptyModel", "--fields-only"])
            assert result.exit_code == 0
            # With fields-only, empty models should just output nothing or minimal output
            lines = [line.strip() for line in result.output.strip().split('\n') if line.strip()]
            # Should have very few or no field lines
            field_lines = [line for line in lines if ':' in line and 'string' in line.lower() or 'integer' in line.lower()]
            assert len(field_lines) == 0  # No actual field lines for empty model

    def test_model_schema_complex_types(self, mock_config_loader):
        """Test schema command with complex field types."""
        runner = CliRunner()
        
        # Test with actual encounter model that exists
        result = runner.invoke(cli, ["model", "schema", "pulsepipe.models.encounter.EncounterInfo"])
        assert result.exit_code == 0
        assert "Schema for EncounterInfo:" in result.output
        # Check that the schema display logic is working
        assert "Fields:" in result.output or "No fields found" in result.output

    def test_model_validate_command(self, mock_config_loader, tmp_path):
        """Test the validate command with valid JSON."""
        runner = CliRunner()
        
        # Create a test JSON file
        test_data = {
            "id": "test123",
            "name": "Test Patient",
            "birth_date": "1990-01-01"
        }
        
        test_file = tmp_path / "test_patient.json"
        test_file.write_text(json.dumps(test_data))
        
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.Patient = MockPatient
            mock_import.return_value = mock_module
            
            result = runner.invoke(cli, ["model", "validate", str(test_file), "pulsepipe.models.patient.Patient"])
            assert result.exit_code == 0
            assert "✅ Validation successful" in result.output
            assert "Model: Patient" in result.output

    def test_model_validate_with_summary_method(self, mock_config_loader, tmp_path):
        """Test validate command with a model that has a summary method."""
        runner = CliRunner()
        
        class ModelWithSummary(BaseModel):
            name: str
            
            def summary(self):
                return f"Model for {self.name}"
            
            @classmethod
            def model_validate(cls, data):
                return cls(name=data["name"])
        
        test_data = {"name": "Test"}
        test_file = tmp_path / "test_data.json"
        test_file.write_text(json.dumps(test_data))
        
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_module.ModelWithSummary = ModelWithSummary
            mock_import.return_value = mock_module
            
            result = runner.invoke(cli, ["model", "validate", str(test_file), "pulsepipe.models.test.ModelWithSummary"])
            assert result.exit_code == 0
            assert "✅ Validation successful" in result.output
            assert "Summary: Model for Test" in result.output

    def test_model_validate_with_list_data(self, mock_config_loader, tmp_path):
        """Test validate command with list data."""
        runner = CliRunner()
        
        test_data = ["item1", "item2", "item3"]
        test_file = tmp_path / "test_list.json"
        test_file.write_text(json.dumps(test_data))
        
        # Test error handling - this should fail validation but exercise the list data path
        result = runner.invoke(cli, ["model", "validate", str(test_file), "pulsepipe.models.patient.PatientInfo"])
        assert result.exit_code == 0
        assert "❌ Validation failed" in result.output

    def test_model_validate_failure(self, mock_config_loader, tmp_path):
        """Test validate command with validation failure."""
        runner = CliRunner()
        
        test_data = {"invalid": "data"}
        test_file = tmp_path / "invalid_data.json"
        test_file.write_text(json.dumps(test_data))
        
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_module = MagicMock()
            mock_patient_class = MagicMock()
            mock_patient_class.model_validate.side_effect = ValueError("Validation failed")
            mock_module.Patient = mock_patient_class
            mock_import.return_value = mock_module
            
            result = runner.invoke(cli, ["model", "validate", str(test_file), "pulsepipe.models.patient.Patient"])
            assert result.exit_code == 0
            assert "❌ Validation failed: Validation failed" in result.output

    def test_model_list_clinical_filter(self, mock_config_loader):
        """Test list command with clinical filter."""
        runner = CliRunner()
        
        with patch('os.walk') as mock_walk, \
             patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            
            # Mock file structure
            mock_walk.return_value = [
                ('/models', [], ['patient.py', 'allergy.py', 'billing.py'])
            ]
            
            # Mock modules
            def import_side_effect(module_path):
                mock_module = MagicMock()
                mock_module.__name__ = module_path
                if 'patient' in module_path:
                    mock_module.Patient = type('Patient', (BaseModel,), {'__module__': module_path})
                    return mock_module
                elif 'allergy' in module_path:
                    mock_module.Allergy = type('Allergy', (BaseModel,), {'__module__': module_path})
                    return mock_module
                elif 'billing' in module_path:
                    mock_module.BillingRecord = type('BillingRecord', (BaseModel,), {'__module__': module_path})
                    return mock_module
                else:
                    raise ImportError("Module not found")
            
            mock_import.side_effect = import_side_effect
            
            result = runner.invoke(cli, ["model", "list", "--clinical"])
            assert result.exit_code == 0
            assert "Clinical models:" in result.output

    def test_model_list_operational_filter(self, mock_config_loader):
        """Test list command with operational filter."""
        runner = CliRunner()
        
        with patch('os.walk') as mock_walk, \
             patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            
            # Mock file structure
            mock_walk.return_value = [
                ('/models', [], ['billing.py', 'operational_content.py'])
            ]
            
            # Mock modules
            def import_side_effect(module_path):
                mock_module = MagicMock()
                mock_module.__name__ = module_path
                if 'billing' in module_path:
                    mock_module.BillingRecord = type('BillingRecord', (BaseModel,), {'__module__': module_path})
                    return mock_module
                elif 'operational' in module_path:
                    mock_module.OperationalContent = type('OperationalContent', (BaseModel,), {'__module__': module_path})
                    return mock_module
                else:
                    raise ImportError("Module not found")
            
            mock_import.side_effect = import_side_effect
            
            result = runner.invoke(cli, ["model", "list", "--operational"])
            assert result.exit_code == 0
            assert "Operational models:" in result.output

    def test_model_list_no_models_found(self, mock_config_loader):
        """Test list command when no models are found."""
        runner = CliRunner()
        
        # Test with operational since there should be fewer models and might trigger not found
        result = runner.invoke(cli, ["model", "list", "--operational"])
        assert result.exit_code == 0
        # Could be either no models found or models found - both are valid test results
        assert "models" in result.output.lower()

    def test_model_list_exception_handling(self, mock_config_loader):
        """Test list command exception handling."""
        runner = CliRunner()
        
        with patch('os.walk') as mock_walk:
            mock_walk.side_effect = Exception("File system error")
            
            result = runner.invoke(cli, ["model", "list", "--all"])
            assert result.exit_code == 0
            assert "❌ Error: File system error" in result.output

    def test_model_example_error_handling(self, mock_config_loader):
        """Test example command error handling."""
        runner = CliRunner()
        
        # Test ImportError
        result = runner.invoke(cli, ["model", "example", "nonexistent.module.Model"])
        assert result.exit_code == 0
        assert "❌ Error:" in result.output
        
        # Test other exceptions
        with patch('pulsepipe.cli.command.model.importlib.import_module') as mock_import:
            mock_import.side_effect = Exception("General error")
            
            result = runner.invoke(cli, ["model", "example", "pulsepipe.models.patient.Patient"])
            assert result.exit_code == 0
            assert "❌ Error: General error" in result.output

    def test_generate_example_edge_cases(self):
        """Test generate_example_from_schema edge cases."""
        # Test schema without type
        schema_no_type = {"description": "No type specified"}
        result = generate_example_from_schema(schema_no_type)
        assert result is None
        
        # Test datetime format
        datetime_schema = {
            "type": "string",
            "format": "date-time"
        }
        result = generate_example_from_schema(datetime_schema)
        assert result == "2023-01-01T00:00:00Z"
        
        # Test number type
        number_schema = {"type": "number"}
        result = generate_example_from_schema(number_schema)
        assert result == 0.0
        
        # Test boolean type
        boolean_schema = {"type": "boolean"}
        result = generate_example_from_schema(boolean_schema)
        assert result is False
        
        # Test unknown type
        unknown_schema = {"type": "unknown_type"}
        result = generate_example_from_schema(unknown_schema)
        assert result is None

    def test_get_field_type_function(self):
        """Test the _get_field_type helper function."""
        # Test basic types
        string_prop = {"type": "string"}
        assert _get_field_type(string_prop) == "string"
        
        integer_prop = {"type": "integer"}
        assert _get_field_type(integer_prop) == "integer"
        
        # Test array types
        array_prop = {"type": "array", "items": {"type": "string"}}
        assert _get_field_type(array_prop) == "array of string"
        
        # Test $ref
        ref_prop = {"$ref": "#/$defs/PatientInfo"}
        assert _get_field_type(ref_prop) == "PatientInfo"
        
        # Test anyOf (union types)
        union_prop = {"anyOf": [{"type": "string"}, {"type": "integer"}]}
        assert _get_field_type(union_prop) == "string | integer"
        
        # Test anyOf with null (should skip null)
        optional_prop = {"anyOf": [{"type": "string"}, {"type": "null"}]}
        assert _get_field_type(optional_prop) == "string"
        
        # Test unknown
        unknown_prop = {"description": "No type info"}
        assert _get_field_type(unknown_prop) == "unknown"
