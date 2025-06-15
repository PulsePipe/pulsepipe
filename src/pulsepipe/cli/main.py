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

# src/pulsepipe/cli/main.py

"""
PulsePipe CLI - Healthcare data pipeline tool
"""

import os
import sys
import warnings

# Fast help path - exit early for main help to avoid loading heavy modules
if len(sys.argv) > 1 and (sys.argv[1] == '--help' or sys.argv[1] == '-h'):
    print("""Usage: pulsepipe [OPTIONS] COMMAND [ARGS]...

  PulsePipe: Healthcare data pipeline tool.
  Prepare healthcare data for AI through configurable adapters, ingesters,
  normalizers, chunkers, embedders, and vector database loaders.

Options:
  --version                                            Show the version and
                                                       exit.
  --config                 -c  FILE                    Path to pulsepipe.yaml
                                                       configuration file
  --profile                -p  TEXT                    Config profile name
                                                       (e.g., patient_fhir,
                                                       lab_hl7)
  --pipeline-id                TEXT                    Unique identifier for
                                                       this pipeline run
  --log-level              -l  [DEBUG|INFO|WARNING|ER  Set the logging level
                               ROR|CRITICAL]
  --json-logs/--no-json-l                              Output logs in JSON
  ogs                                                  format (for machine
                                                       consumption)
  --quiet                  -q                          Suppress non-essential
                                                       output
  --help                                               Show this message and
                                                       exit.

Commands:
  config        Configuration management commands.
  database      Database connectivity and health check commands.
  metrics       Manage and export ingestion metrics.
  model         Model inspection and management commands.
  run           Run a data processing pipeline.""")
    sys.exit(0)

import rich_click as click

# Fast path for any help command - handle all help scenarios quickly
if '--help' in sys.argv or '-h' in sys.argv:
    if len(sys.argv) >= 2:
        if sys.argv[1] == 'config':
            if len(sys.argv) >= 3 and sys.argv[2] == 'filewatcher':
                print("""Usage: pulsepipe config filewatcher [OPTIONS] COMMAND [ARGS]...

  🗂️  File Watcher bookmark and file management.

  Manage file watcher features like bookmark cache and file management

Options:
  --help  Show this message and exit.

Commands:
  archive  📦 Move processed files to an archive directory.
  delete   🗑️ Delete processed files from disk.
  list     📋 List all processed files (successes and errors).
  reset    🧹 Reset (clear) the bookmark cache.""")
                sys.exit(0)
            elif len(sys.argv) >= 3 and sys.argv[2] == 'show':
                print("""Usage: pulsepipe config show [OPTIONS]

  Show the current active configuration settings.

Options:
  -f, --format [yaml|table]  Output format for configuration display
                             [default: yaml]
  --help                     Show this message and exit.

Examples:
  pulsepipe config show
  pulsepipe config show --format table""")
                sys.exit(0)
            elif len(sys.argv) >= 3 and sys.argv[2] == 'list':
                print("""Usage: pulsepipe config list [OPTIONS]

  List available configuration profiles.

Options:
  --config-dir DIRECTORY  Configuration directory
  --help                  Show this message and exit.

Examples:
  pulsepipe config list""")
                sys.exit(0)
            elif len(sys.argv) >= 3 and sys.argv[2] == 'validate':
                print("""Usage: pulsepipe config validate [OPTIONS]

  Validate configuration files.

Options:
  -p, --profile TEXT  Profile name to validate
  --all               Validate all profiles in config directory
  --help              Show this message and exit.

Examples:
  pulsepipe config validate --profile patient_fhir
  pulsepipe config validate --all""")
                sys.exit(0)
            elif len(sys.argv) >= 3 and sys.argv[2] == 'create-profile':
                print("""Usage: pulsepipe config create-profile [OPTIONS]

  Create a unified profile from separate config files.

Options:
  -b, --base TEXT        Base config file (default: pulsepipe.yaml)
  -a, --adapter TEXT     Adapter config file  [required]
  -i, --ingester TEXT    Ingester config file  [required]
  -c, --chunker TEXT     Chunker config file
  -e, --embedding TEXT   Embedding config file
  -vs, --vectorstore TEXT  Vectorstore config file
  -n, --name TEXT        Profile name to create  [required]
  -d, --description TEXT Profile description
  -f, --force            Overwrite existing profile
  --help                 Show this message and exit.

Examples:
  pulsepipe config create-profile --adapter fhir.yaml --ingester json.yaml --name patient_fhir
  pulsepipe config create-profile --adapter fhir.yaml --ingester json.yaml --chunker chunker.yaml \\
    --embedding embedding.yaml --vectorstore vectorstore.yaml --name complete_fhir""")
                sys.exit(0)
            elif len(sys.argv) >= 3 and sys.argv[2] == 'delete-profile':
                print("""Usage: pulsepipe config delete-profile [OPTIONS]

  Delete a configuration profile.

Options:
  -n, --name TEXT  Profile name to delete  [required]
  -f, --force      Delete without confirmation
  --help           Show this message and exit.

Examples:
  pulsepipe config delete-profile --name old_profile
  pulsepipe config delete-profile --name unused_profile --force""")
                sys.exit(0)
            else:
                print("""Usage: pulsepipe config [OPTIONS] COMMAND [ARGS]...

  Configuration management commands.

Options:
  --help      Show this message and exit.

Commands:
  validate        Validate configuration files.
  create-profile  Create a unified profile from separate config files.
  list            List available configuration profiles.
  delete-profile  Delete a configuration profile.
  filewatcher     File Watcher bookmark and file management.
  show            Show the current active configuration settings.

Examples:
  pulsepipe config list
  pulsepipe config validate --profile patient_fhir""")
                sys.exit(0)
        elif sys.argv[1] == 'run':
            print("""Usage: pulsepipe run [OPTIONS]

  Run a data processing pipeline.

Options:
  --concurrent    Enable concurrent execution of pipeline stages
  --help          Show this message and exit.""")
            sys.exit(0)
        elif sys.argv[1] == 'metrics':
            if len(sys.argv) == 3:
                print("""Usage: pulsepipe metrics [OPTIONS] COMMAND [ARGS]...

  Manage and export ingestion metrics. 

Options:
  --help      Show this message and exit.

Commands:
  export    Export ingestion metrics to file.
  analyze   Analyze ingestion metrics and show insights.
  cleanup   Clean up old ingestion metrics data.
  status    Show current ingestion metrics status.

Examples:
  pulsepipe metrics export --format json
  pulsepipe metrics analyze --days 7
  pulsepipe metrics status""")
                sys.exit(0)

# Fast path for model commands - detect early and use minimal CLI
if len(sys.argv) > 1 and sys.argv[1] == 'model':
    # Import minimal dependencies for model commands
    import importlib
    import json
    from typing import Dict, Any
    from pydantic import BaseModel
    
    def get_field_type(prop: Dict[str, Any], defs: Dict[str, Any] = None) -> str:
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
                types.append(get_field_type(item, defs))
            return ' | '.join(types) if types else 'any'
        
        elif 'oneOf' in prop:
            types = [get_field_type(item, defs) for item in prop['oneOf']]
            return ' | '.join(types)
        
        elif 'allOf' in prop:
            types = [get_field_type(item, defs) for item in prop['allOf']]
            return ' & '.join(types)
        
        return 'unknown'
    
    @click.command()
    @click.argument('model_path', required=True)
    @click.option('--json', 'output_json', is_flag=True, help='Output schema as JSON')
    @click.option('--fields-only', 'fields_only', is_flag=True, help='Output only field names and types')
    def schema(model_path, output_json, fields_only):
        """Display schema for a specified model."""
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
            schema_data = model_class.model_json_schema()
            
            if output_json:
                # Output raw JSON schema
                click.echo(json.dumps(schema_data, indent=2))
            elif fields_only:
                # Output only field names and types
                if 'properties' in schema_data:
                    for name, prop in schema_data['properties'].items():
                        field_type = get_field_type(prop, schema_data.get('$defs', {}))
                        required = name in schema_data.get('required', [])
                        req_marker = "*" if required else ""
                        
                        click.echo(f"{name}{req_marker}: {field_type}")
            else:
                # Output formatted schema info
                click.echo(f"Schema for {class_name}:")
                click.echo(f"  Description: {schema_data.get('description', 'No description')}")
                
                if 'properties' in schema_data:
                    click.echo("\nFields:")
                    for name, prop in schema_data['properties'].items():
                        field_type = get_field_type(prop, schema_data.get('$defs', {}))
                        description = prop.get('description', '')
                        required = name in schema_data.get('required', [])
                        req_marker = "*" if required else ""
                        
                        click.echo(f"  • {name}{req_marker}: {field_type}")
                        if description:
                            click.echo(f"    {description}")
                
                if 'required' in schema_data:
                    click.echo(f"\n* Required fields")
                    
        except ImportError:
            click.echo(f"❌ Could not import model: {model_path}", err=True)
        except AttributeError:
            click.echo(f"❌ Class not found: {class_name} in {module_path}", err=True)
        except Exception as e:
            click.echo(f"❌ Error: {str(e)}", err=True)
    
    @click.command()
    @click.argument('json_file', type=click.Path(exists=True))
    @click.argument('model_path', required=True)
    def validate_model(json_file, model_path):
        """Validate JSON data against a model schema."""
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

    def _generate_realistic_string_value(field_name):
        """Generate realistic string values based on field name patterns."""
        field_lower = field_name.lower() if field_name else ""
        
        # Patient/Person identifiers
        if any(x in field_lower for x in ['patient_id', 'patientid', 'mrn', 'medical_record']):
            return "MRN123456789"
        elif any(x in field_lower for x in ['id', 'identifier']):
            return "12345-67890-ABCDE"
        
        # Names
        elif any(x in field_lower for x in ['first_name', 'given', 'firstname']):
            return "John"
        elif any(x in field_lower for x in ['last_name', 'family', 'lastname', 'surname']):
            return "Smith"
        elif 'name' in field_lower:
            return "John Smith"
        
        # Contact information
        elif any(x in field_lower for x in ['phone', 'telephone']):
            return "(555) 123-4567"
        elif 'email' in field_lower:
            return "john.smith@example.com"
        elif any(x in field_lower for x in ['address', 'street']):
            return "123 Main Street"
        elif 'city' in field_lower:
            return "Boston"
        elif any(x in field_lower for x in ['state', 'province']):
            return "MA"
        elif any(x in field_lower for x in ['zip', 'postal']):
            return "02101"
        
        # Healthcare-specific fields
        elif any(x in field_lower for x in ['diagnosis', 'condition']):
            return "Hypertension"
        elif any(x in field_lower for x in ['medication', 'drug']):
            return "Lisinopril 10mg"
        elif any(x in field_lower for x in ['allergy', 'allergen', 'substance']):
            return "Penicillin"
        elif any(x in field_lower for x in ['procedure', 'treatment']):
            return "Blood pressure check"
        elif any(x in field_lower for x in ['lab', 'test', 'result']):
            return "Complete Blood Count"
        elif any(x in field_lower for x in ['vital', 'sign']):
            return "Blood Pressure"
        elif any(x in field_lower for x in ['provider', 'physician', 'doctor']):
            return "Dr. Jane Wilson, MD"
        elif any(x in field_lower for x in ['facility', 'hospital']):
            return "General Hospital"
        elif any(x in field_lower for x in ['department', 'unit']):
            return "Cardiology"
        
        # Social history specific fields
        elif any(x in field_lower for x in ['smoking', 'tobacco']):
            return "Former smoker, quit 5 years ago"
        elif any(x in field_lower for x in ['alcohol', 'drinking']):
            return "Social drinker, 1-2 drinks per week"
        elif any(x in field_lower for x in ['exercise', 'activity']):
            return "Moderate exercise 3 times per week"
        elif any(x in field_lower for x in ['occupation', 'job', 'work']):
            return "Software Engineer"
        elif any(x in field_lower for x in ['marital', 'marriage']):
            return "Married"
        elif any(x in field_lower for x in ['education']):
            return "College graduate"
        elif any(x in field_lower for x in ['social', 'history']):
            return "Non-smoker, occasional alcohol use, regular exercise"
        
        # Clinical codes
        elif any(x in field_lower for x in ['icd', 'code']):
            return "I10"
        elif any(x in field_lower for x in ['snomed', 'sct']):
            return "38341003"
        elif any(x in field_lower for x in ['loinc']):
            return "8480-6"
        elif any(x in field_lower for x in ['cpt']):
            return "99213"
        
        # Status/Category fields
        elif any(x in field_lower for x in ['status', 'state']):
            return "active"
        elif any(x in field_lower for x in ['type', 'category']):
            return "primary"
        elif any(x in field_lower for x in ['gender', 'sex']):
            return "male"
        elif any(x in field_lower for x in ['race', 'ethnicity']):
            return "White"
        elif any(x in field_lower for x in ['severity']):
            return "moderate"
        elif any(x in field_lower for x in ['reaction']):
            return "skin rash"
        elif any(x in field_lower for x in ['onset']):
            return "2020-01-15"
        
        # Notes and descriptions
        elif any(x in field_lower for x in ['note', 'comment', 'description', 'narrative']):
            return "Patient presents with elevated blood pressure. Recommend lifestyle modifications and medication adherence."
        
        # Default fallback
        return "Sample Healthcare Data"

    def _generate_realistic_integer_value(field_name):
        """Generate realistic integer values based on field name patterns."""
        field_lower = field_name.lower() if field_name else ""
        
        if any(x in field_lower for x in ['age', 'years']):
            return 45
        elif any(x in field_lower for x in ['weight', 'kg']):
            return 75
        elif any(x in field_lower for x in ['height', 'cm']):
            return 175
        elif any(x in field_lower for x in ['systolic', 'sbp']):
            return 120
        elif any(x in field_lower for x in ['diastolic', 'dbp']):
            return 80
        elif any(x in field_lower for x in ['heart_rate', 'pulse', 'bpm']):
            return 72
        elif any(x in field_lower for x in ['glucose', 'sugar']):
            return 95
        elif any(x in field_lower for x in ['count', 'num', 'quantity']):
            return 2
        elif any(x in field_lower for x in ['dose', 'dosage']):
            return 10
        
        return 123

    def _generate_realistic_number_value(field_name):
        """Generate realistic number values based on field name patterns."""
        field_lower = field_name.lower() if field_name else ""
        
        if any(x in field_lower for x in ['temperature', 'temp']):
            return 98.6
        elif any(x in field_lower for x in ['bmi']):
            return 24.5
        elif any(x in field_lower for x in ['cholesterol']):
            return 185.5
        elif any(x in field_lower for x in ['hemoglobin', 'hgb']):
            return 14.2
        elif any(x in field_lower for x in ['creatinine']):
            return 1.1
        elif any(x in field_lower for x in ['cost', 'amount', 'price']):
            return 125.50
        elif any(x in field_lower for x in ['percentage', 'percent']):
            return 85.5
        
        return 42.5

    def generate_example_from_schema(schema, field_name=""):
        """Generate a realistic healthcare example instance from a JSON schema."""
        # Handle anyOf first - common pattern in Pydantic schemas for optional fields
        if 'anyOf' in schema:
            non_null_types = [item for item in schema['anyOf'] if item.get('type') != 'null']
            if non_null_types:
                # Use the first non-null type for the example
                return generate_example_from_schema(non_null_types[0], field_name)
            return None
        
        if 'type' not in schema:
            return None
        
        if schema['type'] == 'object':
            result = {}
            properties = schema.get('properties', {})
            
            # Generate examples for all properties, not just required ones
            for prop_name, prop_schema in properties.items():
                # Skip if the property allows only null
                if prop_schema.get('type') == 'null':
                    continue
                
                result[prop_name] = generate_example_from_schema(prop_schema, prop_name)
            
            return result
        
        elif schema['type'] == 'array':
            items_schema = schema.get('items', {})
            return [generate_example_from_schema(items_schema, field_name)]
        
        elif schema['type'] == 'string':
            if 'enum' in schema:
                return schema['enum'][0]
            elif schema.get('format') == 'date-time':
                return "2024-01-15T10:30:00Z"
            elif schema.get('format') == 'date':
                return "1985-03-22"
            else:
                return _generate_realistic_string_value(field_name or schema.get('title', ''))
        
        elif schema['type'] == 'integer':
            return _generate_realistic_integer_value(field_name or schema.get('title', ''))
        
        elif schema['type'] == 'number':
            return _generate_realistic_number_value(field_name or schema.get('title', ''))
        
        elif schema['type'] == 'boolean':
            return True
        
        return None

    @click.command()
    @click.argument('model_path', required=True)
    def example_model(model_path):
        """Generate example JSON for a model."""
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
            click.echo(f"❌ Error: {str(e)}", err=True)

    @click.command()
    @click.option('-a', '--all', 'show_all', is_flag=True, help='Show all available models')
    @click.option('-c', '--clinical', is_flag=True, help='Show clinical models')
    @click.option('-o', '--operational', is_flag=True, help='Show operational models')
    def list_models(show_all, clinical, operational):
        """List available models in the pulsepipe package."""
        import os
        import inspect
        
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
        operational_prefixes = ["operational", "claim", "billing", "payment", "adjustment", "drg", "prior_authorization"]
        
        # Find all models
        try:
            import pulsepipe.models  # Import only when actually needed
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
                        except (ImportError, AttributeError):
                            # Skip modules that can't be imported
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
            click.echo(f"❌ Error: {str(e)}", err=True)

    # If we're here, handle model commands directly
    if len(sys.argv) >= 3:
        if sys.argv[2] == 'schema':
            # Remove 'model' and 'schema' from argv, keeping the rest
            sys.argv = [sys.argv[0]] + sys.argv[3:]
            schema()
            sys.exit(0)
        elif sys.argv[2] == 'list':
            # Remove 'model' and 'list' from argv, keeping the rest
            sys.argv = [sys.argv[0]] + sys.argv[3:]
            list_models()
            sys.exit(0)
        elif sys.argv[2] == 'validate':
            # Remove 'model' and 'validate' from argv, keeping the rest
            sys.argv = [sys.argv[0]] + sys.argv[3:]
            validate_model()
            sys.exit(0)
        elif sys.argv[2] == 'example':
            # Remove 'model' and 'example' from argv, keeping the rest
            sys.argv = [sys.argv[0]] + sys.argv[3:]
            example_model()
            sys.exit(0)
        elif sys.argv[2] == '--help' or sys.argv[2] == '-h':
            # Handle help flag - show fast help without loading heavy imports
            click.echo("Usage: pulsepipe model [OPTIONS] COMMAND [ARGS]...")
            click.echo("")
            click.echo("  Model inspection and management commands.")
            click.echo("")
            click.echo("Options:")
            click.echo("  --help      Show this message and exit.")
            click.echo("")
            click.echo("Commands:")
            click.echo("  list      List available models in the pulsepipe package.")
            click.echo("  schema    Display schema for a specified model.")
            click.echo("  validate  Validate JSON data against a model schema.")
            click.echo("  example   Generate example JSON for a model.")
            click.echo("")
            click.echo("Examples:")
            click.echo("  pulsepipe model list --clinical")
            click.echo("  pulsepipe model schema pulsepipe.models.patient.PatientInfo")
            sys.exit(0)
        elif sys.argv[2].startswith('-'):
            # Handle unknown flags/options quickly without loading heavy imports
            click.echo("Usage: pulsepipe model [OPTIONS] COMMAND [ARGS]...")
            click.echo("")
            click.echo("Try 'pulsepipe model --help' for help")
            click.echo(f"Error: No such option: {sys.argv[2]}", err=True)
            sys.exit(2)
        else:
            # Handle unrecognized subcommands quickly without loading heavy imports
            click.echo("Usage: pulsepipe model [OPTIONS] COMMAND [ARGS]...")
            click.echo("")
            click.echo("Try 'pulsepipe model --help' for help")
            click.echo(f"Error: No such command '{sys.argv[2]}'.", err=True)
            sys.exit(2)
    elif len(sys.argv) == 2:
        # Handle bare 'pulsepipe model' command - show help
        click.echo("Usage: pulsepipe model [OPTIONS] COMMAND [ARGS]...")
        click.echo("")
        click.echo("  Model inspection and management commands.")
        click.echo("")
        click.echo("Commands:")
        click.echo("  list      List available models in the pulsepipe package.")
        click.echo("  schema    Display schema for a specified model.")
        click.echo("  validate  Validate JSON data against a model schema.")
        click.echo("  example   Generate example JSON for a model.")
        click.echo("")
        click.echo("Examples:")
        click.echo("  pulsepipe model list --clinical")
        click.echo("  pulsepipe model schema pulsepipe.models.patient.PatientInfo")
        sys.exit(0)
    
    # For other model commands or help, fall through to normal CLI

# Suppress common warnings for cleaner CLI output
warnings.filterwarnings("ignore", category=FutureWarning, module="spacy")
warnings.filterwarnings("ignore", category=UserWarning, module="torch")
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.config_loader import load_config
from pulsepipe.cli.banner import get_banner
from rich.pretty import pretty_repr
from rich.console import Console

console = Console()

click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.SHOW_METAVARS_COLUMN = True
click.rich_click.STYLE_USAGE = "bold cyan"
click.rich_click.STYLE_COMMANDS = "bold white"
click.rich_click.STYLE_OPTIONS = "bold yellow"
click.rich_click.STYLE_HELPTEXT_FIRST_LINE = "green"
click.rich_click.STYLE_HELPTEXT = ""
click.rich_click.STYLE_OPTION_DEFAULT = "dim cyan"
click.rich_click.HELP_WIDTH = 100

class PipelineContext:
    """Context information for a pipeline run."""
    
    def __init__(self, pipeline_id=None, profile=None, user_id=None, org_id=None, is_dry_run=False):
        import uuid
        import time
        import socket
        import getpass
        
        # Primary identifiers
        self.pipeline_id = pipeline_id or str(uuid.uuid4())
        self.profile = profile
        
        # Enterprise fields
        self.user_id = user_id
        self.org_id = org_id
        
        # Execution metadata
        self.hostname = socket.gethostname()
        self.username = getpass.getuser()
        self.start_time = time.time()
        
        # Runtime flags
        self.is_dry_run = is_dry_run
    
    def as_dict(self):
        """Convert context to dictionary for logging."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
    def get_log_prefix(self):
        """Get a prefix string for log messages."""
        parts = []
        if self.pipeline_id:
            parts.append(f"[{self.pipeline_id[:8]}]")
        if self.profile:
            parts.append(f"[{self.profile}]")
        if self.user_id and self.org_id:
            parts.append(f"[{self.user_id}@{self.org_id}]")
        return " ".join(parts)


# Import CLI options
from pulsepipe.cli.options import common_options, logging_options, output_options

@click.group(invoke_without_command=True)
@click.version_option(package_name="pulsepipe")
@common_options
@logging_options
@click.pass_context
def cli(ctx, config_path, profile, pipeline_id, log_level, json_logs, quiet):
    """PulsePipe: Healthcare data pipeline tool.
    

    Prepare healthcare data for AI through configurable adapters, ingesters, normalizers, chunkers, embedders, and vector database loaders.
    """
    ctx.ensure_object(dict)

    # Create pipeline context
    pipeline_context = PipelineContext(
        pipeline_id=pipeline_id,
        profile=profile
    )
    ctx.obj['context'] = pipeline_context

    # Handle config loading
    try:
        if profile:
            config_dir = os.path.dirname(config_path) if config_path else "config"
            profile_path = os.path.join(config_dir, f"{profile}.yaml")
            if not os.path.exists(profile_path):
                click.echo(f"❌ Profile not found: {profile_path}", err=True)
                sys.exit(1)
            config = load_config(profile_path)
            config_path = profile_path  # so we can print the actual path used
        elif config_path:
            config = load_config(config_path)
        else:
            config_path = "pulsepipe.yaml"
            config = load_config(config_path)
    except Exception as e:
        click.echo(f"❌ Failed to load configuration: {str(e)}", err=True)
        sys.exit(1)

    ctx.obj['config'] = config
    ctx.obj['config_path'] = config_path

    if config.get("logging", {}).get("show_banner", True):
        click.secho(get_banner(), fg='blue')

    # Show config and help if no subcommand is provided
    if ctx.invoked_subcommand is None:
        console.print(f"[bold cyan]📄 Loaded config from:[/bold cyan] {config_path}")
        console.print(pretty_repr(config))
        ctx.info_name = ""
        click.echo(cli.get_help(ctx))
        return

    # Setup logging
    log_config = config.get("logging", {})
    if log_level:
        log_config["level"] = log_level
    if json_logs:
        log_config["format"] = "json"

    LogFactory.init_from_config(
        log_config,
        context=pipeline_context.as_dict() if pipeline_context else None
    )


# Lazy load run command to avoid loading heavy pipeline imports
def lazy_run_invoke(ctx):
    """Lazy load run command only when needed."""
    from pulsepipe.cli.command.run import run as run_command
    return run_command.invoke(ctx)

# Import and add the run command directly - but only load heavy pipeline imports when actually running
from pulsepipe.cli.command.run import run as run_command
cli.add_command(run_command, 'run')

@cli.group()
def config():
    """Configuration management commands."""
    pass

# Force load config commands immediately at module level
from pulsepipe.cli.command.config import config as config_impl
for name, command in config_impl.commands.items():
    config.add_command(command, name)

@cli.group()
def model():
    """Model inspection and management commands."""
    pass

@cli.group()
def metrics():
    """Manage and export ingestion metrics."""
    pass

@cli.group()
def database():
    """Database connectivity and health check commands."""
    pass

# Store original invoke methods
_config_invoke = config.invoke
_model_invoke = model.invoke
_metrics_invoke = metrics.invoke
_database_invoke = database.invoke


def lazy_model_invoke(ctx):
    """Lazy load model commands only when needed."""
    if not model.commands:
        from pulsepipe.cli.command.model import model as model_impl
        for name, command in model_impl.commands.items():
            model.add_command(command, name)
    return _model_invoke(ctx)

def lazy_metrics_invoke(ctx):
    """Lazy load metrics commands only when needed."""
    if not metrics.commands:
        from pulsepipe.cli.command.metrics import metrics as metrics_impl
        for name, command in metrics_impl.commands.items():
            metrics.add_command(command, name)
    return _metrics_invoke(ctx)


def lazy_metrics_get_command(ctx, cmd_name):
    """Lazy load metrics commands for help and command resolution."""
    if not metrics.commands:
        from pulsepipe.cli.command.metrics import metrics as metrics_impl
        for name, command in metrics_impl.commands.items():
            metrics.add_command(command, name)
    return metrics.commands.get(cmd_name)

def lazy_database_invoke(ctx):
    """Lazy load database commands only when needed."""
    if not database.commands:
        _load_database_commands()
    return _database_invoke(ctx)

def _load_database_commands():
    """Load database commands."""
    from pulsepipe.utils.database_diagnostics import diagnose_database_connection, create_detailed_error_message
    from pulsepipe.utils.config_loader import load_config
    import click
    import time
    
    @click.command()
    @click.option("--config-path", default="pulsepipe.yaml", help="Path to the configuration file.")
    @click.option("--profile", default=None, help="Optional config profile to load.")
    @click.option("--timeout", default=10, help="Connection timeout in seconds.")
    @click.option("--verbose", "-v", is_flag=True, help="Show detailed diagnostic information.")
    def health_check(config_path, profile, timeout, verbose):
        """🔍 Comprehensive database connectivity health check."""
        
        try:
            # Load configuration
            if profile:
                import os
                config_dir = os.path.dirname(config_path)
                profile_path = os.path.join(config_dir, f"{profile}.yaml")
                config = load_config(profile_path)
            else:
                config = load_config(config_path)
            
            click.echo("🔍 Running database connectivity health check...")
            if verbose:
                click.echo(f"Configuration file: {config_path}")
                if profile:
                    click.echo(f"Profile: {profile}")
                click.echo(f"Timeout: {timeout}s")
                click.echo()
            
            # Run diagnostics
            start_time = time.time()
            issue_type, suggested_fixes, diagnostic_info = diagnose_database_connection(config, timeout)
            total_time = time.time() - start_time
            
            # Display results
            if issue_type in ["connection_working", "connection_slow_but_working"]:
                click.echo("✅ Database connection is healthy!")
                
                db_type = diagnostic_info.get("config_type", "unknown")
                connection_time = diagnostic_info.get("connection_timeout", total_time)
                
                click.echo(f"Database type: {db_type}")
                click.echo(f"Connection time: {connection_time:.2f}s")
                
                if issue_type == "connection_slow_but_working":
                    click.echo("⚠️ Connection is slow but functional")
                    click.echo("Consider optimizing database performance")
                
                if verbose:
                    click.echo("\nDiagnostic details:")
                    for key, value in diagnostic_info.items():
                        click.echo(f"  {key}: {value}")
            else:
                click.echo("❌ Database connection failed")
                error_message = create_detailed_error_message(issue_type, suggested_fixes, config, diagnostic_info)
                click.echo(error_message)
                
                if verbose:
                    click.echo("\nDiagnostic details:")
                    for key, value in diagnostic_info.items():
                        click.echo(f"  {key}: {value}")
                
                # Exit with error code
                raise click.ClickException("Database health check failed")
                
        except Exception as e:
            if isinstance(e, click.ClickException):
                raise
            click.echo(f"❌ Health check failed: {e}", err=True)
            raise click.ClickException("Health check encountered an error")
    
    database.add_command(health_check, "health-check")

# Replace invoke methods with lazy versions (config is loaded immediately now)
model.invoke = lazy_model_invoke
metrics.invoke = lazy_metrics_invoke
database.invoke = lazy_database_invoke

# Also replace get_command method for help support
_metrics_get_command = metrics.get_command
metrics.get_command = lazy_metrics_get_command

if __name__ == "__main__":
    cli()