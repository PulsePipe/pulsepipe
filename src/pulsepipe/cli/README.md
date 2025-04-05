# PulsePipe CLI

A powerful command-line interface for PulsePipe healthcare data ingestion system.

## Installation

The CLI is automatically installed when you install the PulsePipe package:

```bash
poetry install
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

## Configuration Profiles

A configuration profile combines adapter, ingester, and logging settings into a single YAML file:

```yaml
profile:
  name: patient_fhir
  description: "Patient data from FHIR R4 source"

adapter:
  type: file_watcher
  config:
    directory: "./data/fhir"
    pattern: "*.json"
    recursive: true

ingester:
  type: fhir
  config:
    version: "R4"
    resource_types:
      - Patient
      - Observation

logging:
  level: "INFO"
  format: "rich"
  include_emoji: true
```

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