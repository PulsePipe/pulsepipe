
# PulsePipe Roadmap

This document outlines the planned milestones and future direction of the PulsePipe project.

## ✅ MVP Scope [45% Complete]
## 🟣 Short-Term Goals [0% Complete]
## 🟠 Mid-Term Goals [0% Complete]
## 🟠 Long-Term Goals [0% Complete]

---

## ✅ MVP Scope

- [x] Canonical Clinical Content Model (Pulse Canonical Clinical Model)
- [x] Canonical Operational Content Model (Pulse Canonical Operational Model)
- [x] YAML-based configuration (ingestor registration, pipelines, vector dbs, deid)
- [x] Ingestion Adapter factory with file watcher
- [x] Global persistence for file watcher cache and other cache using SQLite
- [x] FHIR (JSON/XML) ingestion
- [x] X12 Claims and Prior Auth ingester
- [x] Command Line Interface (CLI) implementation for PulsePipe
    - [x] Multi pipeline execution
- [x] Contributor's guide
- [x] Python package with `pydantic` validation
- [x] Python developer tooling:
    - [x] Poetry
    - [x] Pytest
    - [x] Mypy
- [x] HL7 v2.x ingestion
    - [x] Custom HL7 parser that casts a wide net (parses everything in segments of interest)
- [ ] 🧬 Composable Chunking + Embedding Framework
    - [x] `Chunker` base class interfaces
    - [x] `Embedder` base class interfaces
    - [x] YAML configuration for chunkers and embedders in `pipeline.yaml`
    - [ ] `NarrativeChunker`: extracts and truncates text narrative fields
    - [ ] `FhirBundleSplitterChunker`: splits on `entry.resource`
    - [x] `ClinicalEmbedder`: calls local ClinicalBERT for embedding clinical canonical data model
    - [x] `OperationalEmbedder`: calls local MiniLM-L6-v2 for embedding operational canonical data model
    - [ ] CLI options to test chunking/embedding flows
    - [ ] Output of chunks and embeddings in vector-friendly format (e.g., JSONL)
    - [ ] Embedding pipeline supporting ClinicalBERT
    - [ ] Embedding pipeline supporting MiniLM-L6-v2
- [ ] Vector database integration:
    - [ ] Pinecone
    - [x] Weaviate
    - [x] Qdrant
- [ ] 📖 Pipeline Concurrent Step Execution
    - [ ] Add queues to relay messages between steps
- [ ] 📖 CLI Model Description
    - [ ] Write concise descriptions of each `PulseClinicalContent` and `PulseOperationalContent` models (now exposed in the CLI)
    - [ ] Ensure descriptions show up clearly in `pulsepipe model --help` and related views
- [ ] Robust error handling in the ingestion pipeline to account for messy healthcare data
    - [ ] Keep track of data ingested vs un-ingested for later AI feedback
- [ ] De-identification via Presidio + Clinical NER models
- [ ] 🧪 Synthetic Test Dataset
    - [ ] Integrate Synthia to generate high-fidelity synthetic FHIR data
    - [ ] Generate a small, high-quality synthetic dataset (FHIR, HL7v2, X12)
    - [ ] Include edge cases (multiple patients, empty fields, varied formats)
    - [ ] Use for unit/integration tests of mappers and embedding steps
- [ ] Complete Unit Tests:
    - [x] Banner Display
    - [ ] Filewatcher Adapter
    - [x] Ingestors (FHIR, HL7v2, X12)
    - [ ] CLI command parsing and context propagation
    - [ ] Canonical Models and Pydantic schemas
    - [ ] YAML configuration loading and validation
    - [ ] Logging setup and enrichment
    - [ ] Chunker framework and implementation coverage
    - [x] Embedder call mocking and fallback logic
    - [ ] Vector DB connectivity and document serialization

---

## 🟣 Short-Term Goals (v0.2.x - v0.3.x)

- [ ] Support for generating **multiple embedding formats** simultaneously (e.g., BERT + GPT vectors)
- [ ] Improved de-identification pipelines:
    - Incorporate clinical-specific regex library
    - BioClinicalBERT + Presidio joint recognizer (PulsePilot Exclusive)
- [ ] 🧬 Composable Chunking + Embedding Framework
    - [ ] `OpenAIEmbedder`: calls OpenAI for embedding with model selection
        - [ ] OpenAI GPT-3 / GPT-4
    - [ ] `Llama4Embedder`: calls custom, self hosted Llama v4 model in the cloud for embeddings
        - [ ] Llama 4 Maverick: https://www.llama.com/
- [ ] Optional SNOMED CT and ICD code extraction for terminology enrichment (PulsePilot Exclusive)
- [ ] CDA / CCDA document ingestion
- [ ] Custom template ingestion (extensible ingestor system) (PulsePilot Exclusive)
- [ ] Full X12 ingester
- [ ] Plain text ingestion
- [ ] Support for embedding metadata directly into vector stores
- [ ] Optional export of embeddings to offline formats (Parquet, Feather) (PulsePilot Exclusive)
- [ ] Global persistence for file watcher cache and other cache using PostgreSQL (PulsePilot Exclusive)
- [ ] Global persistence using cloud-native backends (e.g., AWS S3 + DynamoDB) (PulsePilot Exclusive)
- [ ] Add REST API Ingestion Adapter to the factory (PulsePilot Exclusive)
- [ ] 🔌 Plug-in System Marketplace (PulsePilot Exclusive)
    - [ ] Entry point-based plugin registry (`pulsepipe-epic`, `pulsepipe-cerner`, `pulsepipe-athena`, etc.)
    - [ ] `pulsepipe list-plugins` and `pulsepipe install` support
    - [ ] Marketplace for distributing:
        - [ ] Custom ingesters and normalization configs
        - [ ] Chunking strategies tuned to specific EHR exports
        - [ ] De-identification presets and PHI patterns
        - [ ] Embedding config bundles (e.g., vector model + chunk tuning)
        - [ ] Support for free and paid plugins (license metadata + vendor attribution)
        - [ ] YAML/JSON schema validation for plugin config injection
        - [ ] UI support for discovering and installing plugins via PulsePilot
        - [ ] Optional local or private plugin registries for enterprise use
- [ ] PulsePilot UI (React + FastAPI) (PulsePilot Exclusive)
    - [ ] View and manage active pipelines: Allows users to start, stop, and monitor pipeline activity (e.g., HL7 → FHIR → Vector store), with real-time logs and status.
    - [ ] UI for editing configuration (typically maintained via YAML files): Visual interface for editing pipeline and system configurations that are usually managed manually via YAML—includes validation, schemas, and save/publish controls.
    - [ ] Monitor ingestion performance and metrics: Dashboards to track ingestion throughput, processing time, and error rates for each stage of the pipeline.
    - [ ] Analyze de-identification processes: Insights into how identifiers are removed or replaced—includes PHI detection types, counts, and potential misses.
    - [ ] Visualize document chunking activity: UI for inspecting how documents are chunked for embedding—chunk size, overlap, structure warnings, etc.
    - [ ] Inspect embedding performance and statistics: Tracks which embedding models were used, token usage, embedding sizes, success/failure rates, and latency.
    - [ ] Explore metadata extraction and usage: Displays key extracted metadata (e.g., patient ID, doc type, timestamps), showing how it's used for indexing, filtering, and downstream querying.
    - [ ] Enable basic vector space visualization and inspection: Provides 2D/3D projections (e.g., t-SNE, UMAP) of embedded vectors to explore clustering and identify embedding anomalies or outliers.
- [ ] Vector database integration:
    - [ ] Milvus
    - [ ] pgvector/PostgreSQL
    - [ ] Chroma
    - [ ] FAISS
    - [ ] MongoDB Atlas Vector Search
    - [ ] Redis
    - [ ] Elasticsearch / OpenSearch

---

## 🟠 Mid-Term Goals (v0.4.x - v0.5.x)

- [ ] UMLS & SNOMED CT Concept Tagging - standardized clinical terminology mapping (PulsePilot Exclusive)
- [ ] Support for additional clinical embeddings:
    - GatorTron (if available)
    - Med-BERT
    - Custom user-provided models
- [ ] Multi-vector search strategies
- [ ] Custom ingesters for EHR vendor specific output (Epic, Cerner, Athena, etc.)
- [ ] Extend canonical data model to cover additional clinical and operational concepts beyond MVP
- [ ] PulsePilot UI:
    - **Full-featured Search / Similarity Explorer** (PulsePilot Exclusive)  
      Allows advanced filtering, comparison, and concept-based search.
    - **Visualization components** (embedding space plots, concept graph) (PulsePilot Exclusive)
- [ ] OpenAPI & GraphQL endpoints for programmatic access (PulsePilot Exclusive)
- [ ] AI-Assisted Data Profiling & Normalization Suggestion Engine (PulsePilot Exclusive)
    - Automatic detection of unmapped or inconsistent data fields.
    - AI-powered recommendations for data normalization and FHIR mapping.
    - Schema drift detection.
    - Interactive UI for reviewing and exporting recommendations.
    - Embedding-based similarity and terminology-assisted suggestions.
- [ ] Add FHIR webhook REST to the ingestion adapter factory (PulsePilot Exclusive)
- [ ] Add MLTP Socket for HL7 v2.x live feeds to the ingestion adapter factory (PulsePilot Exclusive)
- [ ] Snapshot-based testing of pipelines (`pulsepipe test --golden`)
- [ ] Studio/REPL mode for manual pipeline step-by-step execution
- [ ] Caching layer for chunked/embedded data to speed up dev cycles

---

## 🟡 Long-Term Goals (v1.x+)

- [ ] Official extension framework for custom: (PulsePilot Exclusive)
    - Ingestors
    - De-identification modules
    - Embedding engines
- [ ] 🔐 Security & Compliance (Planned)
    - [ ] 📜 HIPAA Awareness
        - [ ] Clear documentation on HIPAA boundaries and responsibilities
        - [ ] Recommendations for compliant deployment (VPCs, access controls, audit logs)
    - [ ] 🔐 Encryption
        - [ ] At-rest encryption for all stored outputs
        - [ ] In-transit encryption (HTTPS, secure WebSockets)
        - [ ] Support for envelope encryption using KMS or customer-managed keys
    - [ ] 🧾 Audit Trails
        - [ ] Per-pipeline audit log with timestamps, user/tenant metadata, success/failure
        - [ ] Optional integration with external SIEM or logging systems
        - [ ] Optional blockchain-backed audit logs (e.g., append-only logs using Bitcoin, SUI, or permissioned chains like Hyperledger) for tamper-evident compliance trails
    - [ ] 🔐 Role-Based Access Control (RBAC)
        - [ ] PulsePilot-level RBAC framework for managing users, roles, and permissions
        - [ ] Support for tenant isolation in multi-tenant setups
    - [ ] 🧪 Security Testing & Policies
        - [ ] Static code analysis and vulnerability scanning
        - [ ] Guidelines for extending PulsePipe without introducing data leaks
- [ ] Secure API authentication (JWT, OAuth2, or OIDC)
- [ ] Support for multi-tenant vector stores (e.g., namespace isolation, tenant-aware indexing) (PulsePilot Exclusive)
- [ ] Streaming data ingestion and dispatcher: (PulsePilot Exclusive)
    - Kafka / Pulsar consumer
    - Generic dispatcher for routing inbound events
- [ ] Integration with private or enterprise-scale LLMs: (PulsePilot Exclusive)
    - Private GPT instances
    - Llama 2 / Llama 3
    - Other on-prem or fine-tuned clinical models
- [ ] Support for healthcare-specific LLM fine-tuning: (PulsePilot Exclusive)
    - Transfer learning pipeline
    - Embedding + supervised task workflows
- [ ] SaaS Offering for PulsePilot (PulsePilot Exclusive)
- [ ] Distributed / parallelized embedding pipeline:
    - Embedding workers
    - Chunk-level parallelism
    - Optimized for serverless and containerized environments
- [ ] First-class support for open-source clinical models:
    - DeepSeek
    - SaaS based on Llama 4 Maverick (with healthcare adaptations)
    - SaaS based on BioBERT, ClinicalBERT, and Med-BERT family
- [ ] Production-ready **PulsePilot** UI:
    - End-user ready
    - Query builders and advanced search
    - Reporting and export (CSV, PDF, JSONL)
- [ ] Complete developer & operator documentation:
    - API documentation
    - Deployment guides (cloud + on-prem)
    - Example pipelines

---

## Notes

This roadmap is iterative. Contributions, feedback, and feature requests are welcome and encouraged.

PulsePipe aims to serve as a foundation for healthcare AI pipelines, combining modern embedding workflows with robust clinical data ingestion and de-identification.
