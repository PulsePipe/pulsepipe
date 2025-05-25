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
# 
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

# src/pulsepipe/cli/commands/model.py

"""
Model inspection and management commands for PulsePipe CLI.
"""
import json
import click
import importlib
import warnings
import os
from typing import Dict, Any, List, Type
from pydantic import BaseModel

# Suppress spaCy warnings
warnings.filterwarnings("ignore", category=FutureWarning, module="spacy")

# Delay import of LogFactory to avoid potential issues
# from pulsepipe.utils.log_factory import LogFactory


def _get_field_type(prop: Dict[str, Any], defs: Dict[str, Any] = None) -> str:
    """Extract field type from property schema."""
    if defs is None:
        defs = {}
    
    # Handle direct type
    if 'type' in prop:
        field_type = prop['type']
        
        if field_type == 'array' and 'items' in prop:
            items = prop['items']
            if '$ref' in items:
                ref_name = items['$ref'].split('/')[-1]
                return f"array of {ref_name}"
            elif 'type' in items:
                return f"array of {items['type']}"
            else:
                return "array"
        
        return field_type
    
    # Handle $ref
    elif '$ref' in prop:
        ref_name = prop['$ref'].split('/')[-1]
        return ref_name
    
    # Handle anyOf/oneOf/allOf
    elif 'anyOf' in prop:
        types = []
        for item in prop['anyOf']:
            if item.get('type') == 'null':
                continue  # Skip null types
            types.append(_get_field_type(item, defs))
        return ' | '.join(types) if types else 'any'
    
    elif 'oneOf' in prop:
        types = [_get_field_type(item, defs) for item in prop['oneOf']]
        return ' | '.join(types)
    
    elif 'allOf' in prop:
        types = [_get_field_type(item, defs) for item in prop['allOf']]
        return ' & '.join(types)
    
    return 'unknown'


@click.group()
def model():
    """Manage and explore data models.
    
    Inspect model schema, debug model transformations, and handle
    model validation tasks.
    """
    pass


@model.command()
@click.argument('model_path', required=True)
@click.option('--json', 'output_json', is_flag=True, help='Output schema as JSON')
@click.option('--fields-only', 'fields_only', is_flag=True, help='Output only field names and types')
def schema(model_path, output_json, fields_only):
    """Display schema for a specified model.
    
    MODEL_PATH should be the dotted path to the model class
    (e.g. pulsepipe.models.clinical.Patient)
    
    Examples:
        pulsepipe model schema pulsepipe.models.patient.PatientInfo
        pulsepipe model schema pulsepipe.models.allergy.Allergy --json
        pulsepipe model schema pulsepipe.models.patient.PatientInfo --fields-only
    """
    try:
        # Suppress logging setup messages for cleaner output
        old_stdout = os.dup(1)
        old_stderr = os.dup(2)
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        
        from pulsepipe.utils.log_factory import LogFactory
        logger = LogFactory.get_logger("model.schema")
        
        # Restore stdout/stderr
        os.dup2(old_stdout, 1)
        os.dup2(old_stderr, 2)
        os.close(devnull)
        os.close(old_stdout)
        os.close(old_stderr)
    except Exception:
        # If logger fails, continue without it
        logger = None
    
    try:
        # Dynamically import the model
        module_path, class_name = model_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        model_class = getattr(module, class_name)
        
        # Ensure it's a Pydantic model
        if not issubclass(model_class, BaseModel):
            click.echo(f"❌ {model_path} is not a Pydantic model", err=True)
            return
        
        # Get schema
        schema = model_class.model_json_schema()
        
        if output_json:
            # Output raw JSON schema
            click.echo(json.dumps(schema, indent=2))
        elif fields_only:
            # Output only field names and types
            if 'properties' in schema:
                for name, prop in schema['properties'].items():
                    field_type = _get_field_type(prop, schema.get('$defs', {}))
                    required = name in schema.get('required', [])
                    req_marker = "*" if required else ""
                    
                    click.echo(f"{name}{req_marker}: {field_type}")
        else:
            # Output formatted schema info
            click.echo(f"Schema for {class_name}:")
            click.echo(f"  Description: {schema.get('description', 'No description')}")
            
            if 'properties' in schema:
                click.echo("\nFields:")
                for name, prop in schema['properties'].items():
                    field_type = _get_field_type(prop, schema.get('$defs', {}))
                    description = prop.get('description', '')
                    required = name in schema.get('required', [])
                    req_marker = "*" if required else ""
                    
                    click.echo(f"  • {name}{req_marker}: {field_type}")
                    if description:
                        click.echo(f"    {description}")
            
            if 'required' in schema:
                click.echo(f"\n* Required fields")
                
    except ImportError:
        click.echo(f"❌ Could not import model: {model_path}", err=True)
    except AttributeError:
        click.echo(f"❌ Class not found: {class_name} in {module_path}", err=True)
    except Exception as e:
        click.echo(f"❌ Error: {str(e)}", err=True)


@model.command()
@click.argument('json_file', type=click.Path(exists=True))
@click.argument('model_path', required=True)
def validate(json_file, model_path):
    """Validate JSON data against a model schema.
    
    Examples:
        pulsepipe model validate patient_data.json pulsepipe.models.clinical.Patient
    """
    try:
        from pulsepipe.utils.log_factory import LogFactory
        logger = LogFactory.get_logger("model.validate")
    except Exception:
        logger = None
    
    try:
        # Load the JSON data
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Import the model class
        module_path, class_name = model_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        model_class = getattr(module, class_name)
        
        # Validate
        model_instance = model_class.model_validate(data)
        
        click.echo(f"✅ Validation successful for {json_file}")
        click.echo(f"Model: {class_name}")
        
        # Print basic info about validated instance
        if hasattr(model_instance, 'summary'):
            click.echo(f"\nSummary: {model_instance.summary()}")
        else:
            # Create a simple summary
            if isinstance(data, dict):
                fields = len(data)
                click.echo(f"\nFields: {fields}")
            elif isinstance(data, list):
                items = len(data)
                click.echo(f"\nItems: {items}")
        
    except Exception as e:
        click.echo(f"❌ Validation failed: {str(e)}", err=True)


@model.command()
@click.option('-a', '--all', 'show_all', is_flag=True, help='Show all available models')
@click.option('-c', '--clinical', is_flag=True, help='Show clinical models')
@click.option('-o', '--operational', is_flag=True, help='Show operational models')
def list(show_all, clinical, operational):
    """List available models in the pulsepipe package.
    
    Examples:
        pulsepipe model list --all
        pulsepipe model list --clinical
        pulsepipe model list --operational
        pulsepipe model list -a
        pulsepipe model list -c
        pulsepipe model list -o
        pulsepipe model list show_all
    """
    import os
    import importlib
    import inspect
    import pulsepipe.models
    from pydantic import BaseModel
    
    try:
        from pulsepipe.utils.log_factory import LogFactory
        logger = LogFactory.get_logger("model.list")
    except Exception:
        logger = None
    
    # If no filter options provided, show usage help
    if not any([show_all, clinical, operational]):
        click.echo("Please specify one of the following options:")
        click.echo("  --all            Show all available models")
        click.echo("  --clinical       Show clinical data models")
        click.echo("  --operational    Show operational data models")
        click.echo("\nExample:")
        click.echo("  pulsepipe model list --clinical")
        click.echo("\nOr to see details of a specific model:")
        click.echo("  pulsepipe model schema pulsepipe.models.clinical_content.PulseClinicalContent")
        return
    
    # Define model categories
    clinical_prefixes = [
                        "advance_directive", "allergy", "blood_bank", "clinical_content",
                        "diagnosis", "diagnostic_test", "encounter", "family_history",
                        "functional_status", "imaging", "immunization", "implant", "lab",
                        "mar", "medication", "microbiology", "note", "order", "pathology",
                        "patient", "payor", "prior_authorization", "problem", "procedure",
                        "social_history", "vital_sign"
                        ]
    operational_prefixes = ["operational", "claim", "billing", "payment", "adjustment"]
    
    # Find all models
    try:
        all_models = []
        models_dir = os.path.dirname(pulsepipe.models.__file__)
        
        # Manual scan of Python files in models directory
        for root, dirs, files in os.walk(models_dir):
            for file in files:
                if file.endswith('.py') and not file.startswith('__'):
                    # Get relative path to make import path
                    rel_path = os.path.relpath(os.path.join(root, file), os.path.dirname(models_dir))
                    module_path = f"pulsepipe.{os.path.splitext(rel_path)[0].replace(os.sep, '.')}"
                    
                    try:
                        module = importlib.import_module(module_path)
                        # Find all Pydantic models in this module
                        for name, obj in inspect.getmembers(module):
                            if (inspect.isclass(obj) and 
                                issubclass(obj, BaseModel) and 
                                obj.__module__ == module.__name__ and
                                obj != BaseModel):
                                
                                full_name = f"{module.__name__}.{name}"
                                all_models.append((full_name, obj))
                    except (ImportError, AttributeError) as e:
                        logger.debug(f"Couldn't import {module_path}: {str(e)}")
                        continue
        
        # Filter models based on options
        filtered_models = []
        
        if show_all:
            filtered_models = all_models
        else:
            for model_path, model_class in all_models:
                model_lower = model_path.lower()
                
                if clinical and any(prefix in model_lower for prefix in clinical_prefixes):
                    filtered_models.append((model_path, model_class))
                elif operational and any(prefix in model_lower for prefix in operational_prefixes):
                    filtered_models.append((model_path, model_class))
        
        # Sort models by name
        filtered_models.sort(key=lambda x: x[0])
        
        # Handle special cases for known models
        if clinical and not any("clinicalcontent" in model_path.lower() for model_path, _ in filtered_models):
            try:
                from pulsepipe.models.clinical_content import PulseClinicalContent
                filtered_models.append(("pulsepipe.models.clinical_content.PulseClinicalContent", PulseClinicalContent))
            except ImportError:
                pass
                
        if operational and not any("operationalcontent" in model_path.lower() for model_path, _ in filtered_models):
            try:
                # Try to import operational content model if it exists
                from pulsepipe.models.operational_content import PulseOperationalContent
                filtered_models.append(("pulsepipe.models.operational_content.PulseOperationalContent", PulseOperationalContent))
            except ImportError:
                pass
        
        if filtered_models:
            # Determine what we're showing
            if show_all:
                title = "All models"
            elif clinical and operational:
                title = "Clinical and operational models"
            elif clinical:
                title = "Clinical models"
            elif operational:
                title = "Operational models"
            else:
                title = "Models"
                
            click.echo(f"{title}:")
            for model_path, model_class in filtered_models:
                # Get a basic description if available
                description = getattr(model_class, '__doc__', '')
                if description:
                    description = description.split('\n')[0].strip()
                else:
                    description = ''
                
                click.echo(f"  • {model_path}")
                if description:
                    click.echo(f"    {description}")
            
            click.echo(f"\nTotal: {len(filtered_models)} models")
            click.echo("\nTo view details for a specific model:")
            click.echo("  pulsepipe model schema <model_path>")
        else:
            if clinical:
                click.echo("No clinical models found.")
            elif operational:
                click.echo("No operational models found.")
            else:
                click.echo("No models found.")
    
    except Exception as e:
        logger.error(f"Error listing models: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)


@model.command()
@click.argument('model_path', required=True)
def example(model_path):
    """Generate example JSON for a model.
    
    Examples:
        pulsepipe model example pulsepipe.models.clinical.Patient
    """
    try:
        from pulsepipe.utils.log_factory import LogFactory
        logger = LogFactory.get_logger("model.example")
    except Exception:
        logger = None
    
    try:
        # Import the model class
        module_path, class_name = model_path.rsplit('.', 1)
        module = importlib.import_module(module_path)
        model_class = getattr(module, class_name)
        
        # Check if the model has an example method
        if hasattr(model_class, 'get_example'):
            example_data = model_class.get_example()
            click.echo(json.dumps(example_data, indent=2))
        else:
            # Create a minimal example from schema
            schema = model_class.model_json_schema()
            example_data = generate_example_from_schema(schema)
            click.echo(json.dumps(example_data, indent=2))
            
    except Exception as e:
        logger.error(f"Error generating example: {str(e)}", exc_info=True)
        click.echo(f"❌ Error: {str(e)}", err=True)



def generate_example_from_schema(schema):
    """Generate a minimal example instance from a JSON schema."""
    if 'type' not in schema:
        return None
    
    if schema['type'] == 'object':
        result = {}
        properties = schema.get('properties', {})
        required = schema.get('required', [])
        
        for prop_name, prop_schema in properties.items():
            if prop_name in required:
                result[prop_name] = generate_example_from_schema(prop_schema)
        
        return result
    
    elif schema['type'] == 'array':
        items_schema = schema.get('items', {})
        return [generate_example_from_schema(items_schema)]
    
    elif schema['type'] == 'string':
        if 'enum' in schema:
            return schema['enum'][0]
        elif schema.get('format') == 'date-time':
            return "2023-01-01T00:00:00Z"
        elif schema.get('format') == 'date':
            return "2023-01-01"
        else:
            return "example"
    
    elif schema['type'] == 'integer':
        return 0
    
    elif schema['type'] == 'number':
        return 0.0
    
    elif schema['type'] == 'boolean':
        return False
    
    return None