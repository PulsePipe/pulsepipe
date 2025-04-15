# PulsePipe CLI

A powerful command-line interface for PulsePipe healthcare data ingestion system.

## Installation

The CLI is automatically installed when you install the PulsePipe package:

```bash
poetry install
```

## Configuration

### Configuration Profiles

A configuration profile combines all pipeline stage configurations into a single YAML file:

```yaml
# Example: config/patient_fhir.yaml
profile:
  name: patient_fhir
  description: "Patient data from FHIR R4 source"
  created_at: "2025-04-05"

# Input stage - reads data from source
adapter:
  type: file_watcher
  watch_path: "./data/fhir"
  extensions: [".json", ".xml"]
  continuous: true  # Set to false for one-time processing

# Parsing stage - converts source data to canonical models
ingester:
  type: fhir
  version: "R4"
  resource_types:
    - Patient
    - Observation

# Chunking stage - splits content into smaller pieces
chunker:
  type: clinical
  export_chunks_to: "jsonl"  # Optional, to save chunks to file
  include_metadata: true

# Embedding stage - converts text chunks to vector embeddings
embedding:
  type: clinical
  model_name: "all-MiniLM-L6-v2"
  export_embeddings_to: "jsonl"  # Optional, to save embeddings to file

# Vector database stage - stores embeddings for retrieval
vectorstore:
  enabled: true
  engine: weaviate
  host: "http://localhost"
  port: 8080
  namespace_prefix: "pulsepipe_fhir"

# Logging configuration
logging:
  level: "INFO"  # debug | info | warning | error
  type: rich      # rich | json | none  
  destination: stdout  # stdout | file | both
  file_path: logs/pulsepipe.log  # Only used if destination includes 'file'
  show_banner: true  # Display the CLI banner
```

### Component-specific Config Files

You can also create separate config files for each pipeline stage:

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

```yaml
# Example: config/clinical_chunker.yaml
chunker:
  type: clinical
  export_chunks_to: "jsonl"
  include_metadata: true
```

```yaml
# Example: config/clinical_embedding.yaml
embedding:
  type: clinical
  model_name: "all-MiniLM-L6-v2"
  export_embeddings_to: "jsonl"
```

```yaml
# Example: config/vectorstore.yaml
vectorstore:
  enabled: true
  engine: weaviate
  host: "http://localhost"
  port: 8080
  namespace_prefix: "pulsepipe_fhir"
```

These can be used directly with the CLI or combined into a profile using the `create-profile` command.

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
  --adapter fhir_adapter.yaml \
  --ingester fhir_ingester.yaml \
  --name patient_fhir \
  --description "Patient data from FHIR R4 source"
```

### Creating Profiles

Profiles can be created in several ways:

1. **Manually create profile YAML files:**
   Create a YAML file in the `config/` directory with configurations for all pipeline stages:

   ```bash
   # Example: config/patient_fhir.yaml
   nano config/patient_fhir.yaml
   ```
   
   Use the [Configuration Profiles](#configuration-profiles) section above as a template.

2. **Use the create-profile command** to combine existing stage configurations:

   ```bash
   # First create separate stage configs
   nano config/fhir_adapter.yaml
   nano config/fhir_ingester.yaml
   nano config/clinical_chunker.yaml
   nano config/clinical_embedding.yaml
   nano config/vectorstore.yaml
   
   # Then combine them into a profile
   pulsepipe config create-profile \
    --adapter config/fhir_adapter.yaml \
    --ingester config/fhir_ingester.yaml \
    --chunker config/clinical_chunker.yaml \
    --embedding config/clinical_embedding.yaml \
    --vectorstore config/qdrant_vectorstore.yaml \
    --name patient_fhir \
    --description "Patient data from FHIR R4 source"
   ```

3. **Copy and modify an existing profile:**

   ```bash
   cp config/patient_fhir.yaml config/patient_fhir_custom.yaml
   nano config/patient_fhir_custom.yaml
   ```

## Usage

### Running Complete Pipelines

Run a complete pipeline with a configuration profile:

```bash
# Run using a profile (recommended approach for complete pipelines)
pulsepipe run --profile patient_fhir

# Show a summary of processed data
pulsepipe run --profile patient_fhir --summary

# Print the full normalized model
pulsepipe run --profile patient_fhir --print-model

# Save output to a file
pulsepipe run --profile patient_fhir --print-model --output patient_data.json
```

### Running with Explicit Component Configurations

You can also run the pipeline with explicitly specified component configurations:

```bash
# Run with specific component configs
pulsepipe run \
  --adapter adapter.yaml \
  --ingester ingester.yaml \
  --chunker chunker.yaml \
  --embedding embedding.yaml \
  --vectorstore vectorstore.yaml

# Run with minimal configuration (adapter and ingester only)
# Note: This will only perform ingestion without chunking, embedding, or vector storage
pulsepipe run --adapter adapter.yaml --ingester ingester.yaml
```

### Pipeline Execution Options

```bash
# Run in one-time processing mode (overriding continuous setting)
pulsepipe run --profile patient_fhir --one-time

# Run in continuous watch mode
pulsepipe run --profile patient_fhir --continuous

# Run with concurrent pipeline stages (improved performance)
pulsepipe run --profile patient_fhir --concurrent

# Set a timeout for pipeline execution (in seconds)
pulsepipe run --profile patient_fhir --timeout 300

# Enable verbose logging for debugging
pulsepipe run --profile patient_fhir --verbose
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
