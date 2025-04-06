# PulsePipe

**PulsePipe** is a modular, AI-native healthcare data pipeline. It ingests clinical data (HL7 v2, FHIR, CDA/CCDA, plain text, and custom templates), de-identifies and normalizes it, prepares it for LLM-friendly text processing, and generates vector representations for AI/ML tasks.

PulsePipe transforms healthcare data into embedding-ready chunks annotated with rich metadata (patient hashes, timestamps, note types, etc.). It supports generating multiple vector formats via configurable embedding engines (ClinicalBERT, GPT-4, DeepSeek, and others) for use in vector databases such as Pinecone, Weaviate, or FAISS.

PulsePipe is designed for AI-powered applications including:
- Clinical semantic search
- Clinical decision support
- Patient similarity matching
- Embedding-driven analytics
- Hybrid structured + unstructured data pipelines

---

## ✨ Features

- Modular multi-format ingestion (FHIR, HL7 v2.x, CDA/CCDA, Plain Text)
- Canonical Clinical Data Model (Pulse Clinical Content)
- De-identification Module using Presidio + Clinical NER models (optional)
- Embedding-ready Pipeline: Produces vector-ready chunks for AI/ML tasks
- Supports multiple embedding models (ClinicalBERT, GPT-family, DeepSeek, etc.)
- Supports generating multiple embedding formats per chunk
- Metadata-first design for context-rich, patient-safe vector chunks
- Flexible vector storage (Pinecone, Weaviate, FAISS, or custom backends)
- Built on `pydantic` for strict schema validation
- Compatible with modern Python toolchains (Poetry, Pytest, Mypy)
- Designed for AI/NLP enhanced ingestion pipelines

---

## ⚡ Supported Ingestors

| Ingestor | Description |
|----------|-------------|
| FHIR | Supports FHIR `JSON` or `XML` resources |
| HL7 | Supports HL7 v2.x message ingestion (ADT, ORU, ORM, etc.) |
| CDA | Supports CDA/CCDA XML documents (e.g., Discharge Summaries, AVS) |
| PlainText | Supports unstructured clinical notes |
| Custom Template | Allows you to define and extend your own ingestion logic for proprietary or domain-specific formats |

---

## ✅ End-to-End Pipeline

1. **Ingest**  
   Accepts HL7, FHIR, CDA, PlainText, or custom template input.

2. **Normalize**  
   Converts input into the Pulse Canonical Model — a standardized, AI/analytics-ready clinical representation.

3. **De-identify**  
   Removes PHI using Presidio and Clinical NER models such as BioClinicalBERT or medSpaCy.

4. **Chunk**  
   Splits de-identified clinical narratives into embedding-friendly chunks with metadata.

5. **Embed**  
   Generates one or more embedding vectors per chunk using:
   - ClinicalBERT (local)
   - OpenAI GPT-family (via API)
   - DeepSeek (optional)
   - Other pluggable embedding models

6. **Store**  
   Embeddings and metadata are persisted into:
   - Pinecone
   - Weaviate
   - FAISS
   - or a custom vector backend

7. **(Optional) PulsePilot UI**  
   Optional web-based UI (React + FastAPI) for search, similarity exploration, and analytics.

---

## 📜 License

PulsePipe is licensed under the **GNU Affero General Public License v3.0 (AGPL-3.0)** — and that's on purpose.

We believe that foundational healthcare infrastructure should:
- Be open
- Improve with community contributions
- Avoid proprietary lock-in

By using AGPL:
- We ensure that if you run PulsePipe as part of a distributed system or as a service, you must share back your improvements.
- This protects the broader healthcare community while allowing commercial use under clear terms.

Non-profits, hospitals, academic institutions, government organizations, and individual researchers may use, modify, and deploy PulsePipe for **non-commercial** purposes without triggering AGPL Section 13.

For full details, see the [LICENSE](./LICENSE.md) and [LICENSE-EXCEPTIONS](./LICENSE-EXCEPTIONS.md) files.

---

## 📦 Installation & Building

```bash
poetry install   # Installs dependencies and sets up the virtual environment

poetry build     # Builds project into dist/*.whl and dist/*.tar.gz

poetry publish --repository <repo>      # Publishes build artifacts to specified repository (optional)

poetry run python  # Opens a Python REPL using the managed virtualenv
```

---

## 🧪 Testing & 🐞 Debugging the Pipeline

### ✅ Unit Testing & QA

```bash
# Run unit tests
poetry run pytest

# Run verbose
poetry run pytest -s

# Run tests with coverage report
poetry run pytest --cov=src/ --cov-report=term-missing tests/

# Type checking (static analysis)
poetry run mypy src/ tests/

# Linting (PEP8 compliance)
poetry run black --check src/ tests/
poetry run isort --check-only src/ tests/

# Auto-fix formatting (format code in-place)
poetry run black src/ tests/
poetry run isort src/ tests/
```

---

## 📦 Running and Using

PulsePipe is built for CLI-first interaction. Once installed via Poetry, you can run pipelines using a simple command:

```bash
pulsepipe run --profile <your_profile>
```

This will load the specified YAML profile and start ingesting and processing data based on your configuration.

### Common Commands

```bash
# Run a pipeline using a profile
pulsepipe run --profile patient_fhir

# View a summary after run
pulsepipe run --profile patient_fhir --summary

# Print the normalized data model
pulsepipe run --profile patient_fhir --print-model

# Run using adapter + ingester configs directly
pulsepipe run --adapter adapter.yaml --ingester ingester.yaml

# Run all pipelines from a pipeline.yaml
pulsepipe run --pipeline-config pipeline.yaml
```

You can also manage configs, inspect models, and reset bookmarks using CLI subcommands:

```bash
# Validate a config
pulsepipe config validate --profile patient_fhir

# List processed files for file watcher
pulsepipe config filewatcher list

# View model schemas
pulsepipe model schema pulsepipe.models.PulseClinicalContent
```

For **full documentation**, see  
📄 [`src/pulsepipe/cli/README.md`](src/pulsepipe/cli/README.md)


---
## 📈 Architecture Diagram

![PulsePipe Architecture](docs/pulsepipe_architecture_layers.png)