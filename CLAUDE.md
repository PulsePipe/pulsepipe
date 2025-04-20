# CLAUDE.md

## Project Overview
PulsePipe is a modular, AI-native healthcare data pipeline that processes clinical data formats (HL7 v2, FHIR, CDA/CCDA, X12, plain text), de-identifies and normalizes it, and prepares it for LLM processing and vector embeddings.

## Core Architecture
- Pipeline stages: Adapter → Ingester → De-ID → Chunker → Embedder → VectorStore
- Each stage has a specific responsibility in the data transformation process
- Modular design allows each component to be swapped or extended
- Async/await pattern used throughout for I/O operations

## Development Environment
- Python 3.11.x
- Poetry 1.6.1+ for dependency management (no pip)
- `poetry install` to set up environment
- `poetry run pulsepipe` to run commands

## Key Components

### Adapters
- **FileWatcherAdapter**: Monitors directories for healthcare data files
  - Processes existing files first, then watches for changes
  - **Important**: For continuous processing, use the watch script: `./scripts/watch_directory.sh <profile> <interval>`
  - SQLite bookmark store tracks processed files to prevent duplicates

### Ingesters
- Parse and normalize specific healthcare formats:
  - **FHIR**: JSON/XML resources and bundles
  - **HL7v2**: The traditional pipe-delimited format
  - **X12**: EDI format for claims, payments, authorizations (837, 835, 278)
  - **PlainText**: Basic text processing

### Pipeline Processing
- **De-ID**: Currently a pass-through placeholder
- **Chunkers**: Break data into embedding-friendly segments
  - Clinical chunks based on medical sections
  - Operational chunks based on business entities (claims, payments)
- **Embedders**: Generate vector embeddings using various models
- **VectorStores**: Store embeddings (Weaviate, Qdrant implemented)

## Data Models
- `PulseClinicalContent`: For clinical/medical data (patients, encounters, observations)
- `PulseOperationalContent`: For billing/administrative data (claims, payments)
- Both use Pydantic models with validation

## Command-Line Interface

### Running Pipelines
```bash
# Run a pipeline with a named profile
poetry run pulsepipe run --profile <profile_name>

# Run a pipeline with concurrent execution (recommended)
poetry run pulsepipe run --profile <profile_name> --concurrent

# For continuous processing, use the watch script
./scripts/watch_directory.sh <profile_name> <interval_seconds>
```

### Configuration Commands
```bash
# List available configurations
poetry run pulsepipe config list

# View a specific model schema
poetry run pulsepipe model schema <model_name>
```

## Configuration

### YAML Configuration Files
- `config/<profile>.yaml`: Pipeline configuration profiles
- Key sections:
  - `adapter`: Data source configuration
  - `ingester`: Parser configuration
  - `chunker`: Chunking configuration
  - `embedding`: Embedding model configuration
  - `vectorstore`: Vector database configuration

### Example Configuration
```yaml
profile:
  name: billing_x12
  description: Process X12 billing files
adapter:
  type: file_watcher
  watch_path: ./incoming/x12
  extensions: [.txt, .x12, .835, .837, .278]
  continuous: true  # Watch for new files
ingester:
  type: x12
  transaction_types: [835, 837, 278]
chunker:
  type: operational
embedding:
  type: operational
  model_name: all-MiniLM-L6-v2
vectorstore:
  enabled: true
  engine: qdrant
  host: http://localhost:6333
```

## Common Issues and Solutions

### Continuous Pipeline Processing
- **Issue**: Pipeline gets stuck in ingestion stage
- **Solution**: Use the watch script for reliable operation
  ```bash
  ./scripts/watch_directory.sh billing_x12 30
  ```
  This script:
  - Processes all files in the directory
  - Waits for the specified interval (30 seconds)
  - Checks for new files and processes them
  - Repeats automatically

### Missing Dependencies
- **Issue**: Errors about missing packages
- **Solution**: Always use Poetry for dependency management
  ```bash
  poetry install
  poetry add <package_name>
  ```

### Vector Database Connectivity
- **Issue**: Cannot connect to vector database
- **Solution**: Ensure vector database is running and accessible
  ```bash
  # For Qdrant
  docker run -p 6333:6333 qdrant/qdrant
  ```

## Best Practices

### Extending the Pipeline
1. Create new component in appropriate directory
2. Implement required interface methods
3. Register in factory classes
4. Add configuration options in YAML

### Error Handling
- Catch and wrap specific errors in domain-specific PulsePipeError types
- Include helpful error messages and context in the details dict
- Propagate errors up the stack for proper logging and CLI display

### Performance Optimization
- Use the `--concurrent` flag for parallel stage execution
- For large datasets, consider chunking the input data
- Monitor memory usage with large vector embeddings

## API Usage Example
```python
from pulsepipe.pipelines.runner import PipelineRunner
from pulsepipe.utils.config_loader import load_config

async def process_healthcare_data():
    # Create a pipeline runner
    runner = PipelineRunner()
    
    # Load a configuration
    config = load_config("config/billing_x12.yaml")
    
    # Run the pipeline
    result = await runner.run_pipeline(
        config=config,
        name="billing_x12",
        concurrent=True
    )
    
    # Process results
    if result["success"]:
        data = result["result"]
        print(f"Processed {len(data) if isinstance(data, list) else 1} items")
    else:
        print(f"Pipeline failed: {result['errors']}")
```