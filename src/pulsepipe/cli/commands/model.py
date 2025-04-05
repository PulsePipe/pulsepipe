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
from typing import Dict, Any, List, Type
from pydantic import BaseModel

from pulsepipe.utils.log_factory import LogFactory


@click.group()
def model():
    """Manage and explore data models.
    
    Inspect model schema, debug model transformations, and handle
    model validation tasks.
    """
    pass


@model.command()
@click.argument('model_path', required=True)
@click.option('--fields-only', is_flag=True, help='Show only field names without details')
@click.option('--json', 'output_json', is_flag=True, help='Output schema as JSON')
def schema(model_path, fields_only, output_json):
    """Display schema for a specified model.
    
    MODEL_PATH should be the dotted path to the model class
    (e.g. pulsepipe.models.clinical.Patient)
    
    Examples:
        pulsepipe model schema pulsepipe.models.clinical.Patient
        pulsepipe model schema pulsepipe.models.PulseClinicalContent --fields-only
    """
    logger = LogFactory.get_logger("model.schema")
    
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
            # Output just field names
            if 'properties' in schema:
                fields = list(schema['properties'].keys())
                click.echo(f"Fields for {class_name}:")
                for field in sorted(fields):
                    click.echo(f"  • {field}")
                click.echo(f"\nTotal: {len(fields)} fields")
            else:
                click.echo(f"No fields found in {class_name}")
        else:
            # Output formatted schema info
            click.echo(f"Schema for {class_name}:")
            click.echo(f"  Description: {schema.get('description', 'No description')}")
            
            if 'properties' in schema:
                click.echo("\nFields:")
                for name, prop in schema['properties'].items():
                    field_type = prop.get('type', 'unknown')
                    if field_type == 'array' and 'items' in prop:
                        items_type = prop['items'].get('type', 'unknown')
                        if items_type == 'object' and '$ref' in prop['items']:
                            ref = prop['items']['$ref'].split('/')[-1]
                            field_type = f"array of {ref}"
                        else:
                            field_type = f"array of {items_type}"
                    elif '$ref' in prop:
                        ref = prop['$ref'].split('/')[-1]
                        field_type = ref
                        
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
    logger = LogFactory.get_logger("model.validate")
    
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
@click.option('--all', 'show_all', is_flag=True, help='Show all available models')
def list(show_all):
    """List available models in the pulsepipe package.
    
    Examples:
        pulsepipe model list
        pulsepipe model list --all
    """
    import pkgutil
    import inspect
    import pulsepipe.models
    from pulsepipe.models import PulseClinicalContent
    
    logger = LogFactory.get_logger("model.list")
    
    def get_models_from_module(module, prefix=''):
        """Recursively get all Pydantic models from a module."""
        models = []
        
        # Get all models directly in this module
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and issubclass(obj, BaseModel) and obj.__module__ == module.__name__:
                models.append((f"{prefix}.{name}" if prefix else name, obj))
        
        # Recursively check submodules
        if show_all:
            for _, submodule_name, is_pkg in pkgutil.iter_modules(module.__path__):
                full_submodule_name = f"{module.__name__}.{submodule_name}"
                try:
                    submodule = importlib.import_module(full_submodule_name)
                    if is_pkg:
                        # This is a package
                        new_prefix = f"{prefix}.{submodule_name}" if prefix else submodule_name
                        models.extend(get_models_from_module(submodule, new_prefix))
                    else:
                        # This is a module
                        for name, obj in inspect.getmembers(submodule):
                            if inspect.isclass(obj) and issubclass(obj, BaseModel) and obj.__module__ == full_submodule_name:
                                full_name = f"{module.__name__}.{submodule_name}.{name}"
                                models.append((full_name, obj))
                except (ImportError, AttributeError):
                    pass
        
        return models
    
    try:
        # Start with the main models
        models = get_models_from_module(pulsepipe.models, 'pulsepipe.models')
        
        # Sort models by name
        models.sort(key=lambda x: x[0])
        
        if models:
            click.echo("Available models:")
            for model_path, model_class in models:
                # Get a basic description if available
                description = getattr(model_class, '__doc__', '')
                if description:
                    description = description.split('\n')[0].strip()
                else:
                    description = ''
                
                click.echo(f"  • {model_path}")
                if description:
                    click.echo(f"    {description}")
            
            click.echo(f"\nTotal: {len(models)} models")
            click.echo("\nTo view details for a specific model:")
            click.echo("  pulsepipe model schema <model_path>")
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
    logger = LogFactory.get_logger("model.example")
    
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