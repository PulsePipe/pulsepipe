# PulsePipe CLI

A powerful command-line interface for PulsePipe healthcare data ingestion system.

## Installation

The CLI is automatically installed when you install the PulsePipe package:

```bash
poetry install
```

## Configuration

### Configuration Profiles

A configuration profile combines adapter, ingester, and logging settings into a single YAML file:

```yaml
# Example: config/patient_fhir.yaml
profile:
  name: patient_fhir
  description: "Patient data from FHIR R4 source"
  created_at: "2025-04-05"  # Optional

adapter:
  type: file_watcher
  watch_path: "./data/fhir"
  extensions: [".json", ".xml"]
  continuous: true  # Set to false for one-time processing

ingester:
  type: fhir
  version: "R4"
  resource_types:
    - Patient
    - Observation

logging:
  level: "INFO"  # debug | info | warning | error
  type: rich      # rich | json | none  
  destination: stdout  # stdout | file | both
  file_path: logs/pulsepipe.log  # Only used if destination includes 'file'
  show_banner: true  # Display the CLI banner
```

### Component-specific Config Files

You can also create separate config files for adapters and ingesters:

```yaml
# Example: config/fhir_adapter.yaml
adapter:
  type: file_watcher
  watch_path: "./data/fhir"
  extensions: [".json", ".xml"]
  continuous: true
```

```yaml
# Example: config/fhir_ingester.yaml
ingester:
  type: fhir
  version: "R4"
  resource_types:
    - Patient
    - Observation
```

These can be used directly with the CLI or combined into a profile using the `create-profile` command.

### Pipeline Configuration

For running multiple pipelines, create a pipeline configuration file:

```yaml
# Example: pipeline.yaml
pipelines:
  - name: fhir_clinical
    description: "Process FHIR clinical data files"
    active: true
    adapter:
      type: file_watcher
      watch_path: "./incoming/fhir"
      extensions: [".json", ".xml"]
      continuous: true  # Set to continuously watch for files
    ingester:
      type: fhir
      version: "R4"
        
  - name: hl7v2_lab_results
    description: "Process HL7v2 lab results"
    active: true
    adapter:
      type: file_watcher
      watch_path: "./incoming/hl7"
      extensions: [".hl7", ".txt"]
      continuous: true
    ingester:
      type: hl7v2
      message_types: ["ORU^R01"]
        
  - name: x12_billing
    description: "Process X12 billing and prior auth data"
    active: false  # Inactive pipeline, only runs with --all flag
    adapter:
      type: file_watcher
      watch_path: "./incoming/x12"
      extensions: [".x12", ".837", ".278", ".txt"]
      continuous: true
    ingester:
      type: x12
      transaction_types: ["837", "835", "278"]
```

### Managing Configurations

```bash
# List available profiles
pulsepipe config list

# Validate a specific profile
pulsepipe config validate --profile patient_fhir

# Validate all profiles
pulsepipe config validate --all

# Create a new profile from existing configs
pulsepipe config create-profile \
  --adapter fhir.yaml \
  --ingester json.yaml \
  --name patient_fhir \
  --description "Patient data from FHIR R4 source"
```

### Creating Profiles

Profiles can be created in several ways:

1. **Manually create profile YAML files:**
   Create a YAML file in the `config/` directory with your adapter, ingester, and logging settings:

   ```bash
   # Example: config/patient_fhir.yaml
   nano config/patient_fhir.yaml
   ```
   
   Use the [Configuration Profiles](#configuration-profiles) section above as a template.

2. **Use the create-profile command** to combine existing adapter and ingester configurations:

   ```bash
   # First create separate adapter and ingester configs
   nano config/fhir_adapter.yaml
   nano config/fhir_ingester.yaml
   
   # Then combine them into a profile
   pulsepipe config create-profile \
     --adapter config/fhir_adapter.yaml \
     --ingester config/fhir_ingester.yaml \
     --name patient_fhir \
     --description "Patient data from FHIR R4 source"
   ```

3. **Copy and modify an existing profile:**

   ```bash
   cp config/patient_fhir.yaml config/patient_fhir_custom.yaml
   nano config/patient_fhir_custom.yaml
   ```

## Usage

### Running Pipelines

Run a pipeline with a configuration profile:

```bash
# Run using a profile
pulsepipe run --profile patient_fhir

# Show a summary of processed data
pulsepipe run --profile patient_fhir --summary

# Print the full normalized model
pulsepipe run --profile lab_hl7 --print-model

# Save output to a file
pulsepipe run --profile patient_fhir --print-model --output patient_data.json

# Specify a custom pipeline ID
pulsepipe run --profile patient_fhir --pipeline-id my-pipeline-123

# Run with specific adapter and ingester configs
pulsepipe run --adapter adapter.yaml --ingester ingester.yaml

# Validate config without running the pipeline
pulsepipe run --dry-run --profile patient_fhir
```

### Running Multiple Pipelines

PulsePipe supports running multiple pipelines from a pipeline configuration file:

```bash
# Run all active pipelines defined in pipeline.yaml
pulsepipe run --pipeline-config pipeline.yaml

# Run specific pipelines from the config file
pulsepipe run --pipeline-config pipeline.yaml --pipeline fhir_clinical --pipeline x12_billing

# Run all pipelines including inactive ones
pulsepipe run --pipeline-config pipeline.yaml --all

# Run in one-time processing mode (overriding continuous setting)
pulsepipe run --pipeline-config pipeline.yaml --one-time

# Run in continuous watch mode (multiple pipelines run concurrently)
pulsepipe run --pipeline-config pipeline.yaml --continuous
```

### File Watcher Management

PulsePipe offers commands to manage the File Watcher adapter's bookmark database:

```bash
# List all processed files
pulsepipe config filewatcher list

# Reset file watcher bookmarks (reprocess all files)
pulsepipe config filewatcher reset

# Archive processed files to a backup directory
pulsepipe config filewatcher archive --archive-dir ./archived_files

# Delete processed files
pulsepipe config filewatcher delete
```

### Working with Models

```bash
# List available models
pulsepipe model list

# Show schema for a specific model
pulsepipe model schema pulsepipe.models.PulseClinicalContent

# Show only field names
pulsepipe model schema pulsepipe.models.PulseClinicalContent --fields-only

# Output schema as JSON
pulsepipe model schema pulsepipe.models.PulseClinicalContent --json

# Validate JSON data against a model
pulsepipe model validate patient_data.json pulsepipe.models.PulseClinicalContent

# Generate example JSON for a model
pulsepipe model example pulsepipe.models.PulseClinicalContent
```

## Environment Variables

The CLI supports the following environment variables:

- `PULSEPIPE_CONFIG`: Default path to the configuration file
- `PULSEPIPE_LOG_LEVEL`: Default log level (DEBUG, INFO, WARNING, ERROR)
- `PULSEPIPE_PROFILE`: Default profile to use

## Enterprise Features (PulsePilot)

For enterprise deployments, the CLI supports additional features:

```bash
# Use with organization and user context
pulsepipe run --profile patient_fhir --org-id healthcare-inc --user-id analyst-1

# Generate structured JSON logs
pulsepipe run --profile patient_fhir --json-logs

# Advanced logging and auditing
pulsepipe run --profile patient_fhir --log-level DEBUG
```