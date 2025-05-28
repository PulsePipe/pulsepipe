
# PulsePipe Roadmap

This document outlines the planned milestones and future direction of the PulsePipe project.

---
#### ✅ MVP Scope      [ 80% Complete ]                      🟣 Short-Term Goals [  0% Complete ]
#### 🟠 Mid-Term Goals [  0% Complete ]                      🟠 Long-Term Goals  [  0% Complete ]

#### 🧪 Unit Test Coverage: 80-85%

## ✅ MVP Scope

- [x] Canonical Clinical Content Model (Pulse Canonical Clinical Model)
- [x] Canonical Operational Content Model (Pulse Canonical Operational Model)
- [x] YAML-based configuration (ingestor registration, pipelines, vector dbs, deid)
- [x] Ingestion Adapter factory with file watcher
- [x] Global persistence for file watcher cache and other cache using SQLite
- [x] FHIR (JSON/XML) ingestion
- [x] X12 Claims and Prior Auth ingester
- [x] Command Line Interface (CLI) implementation for PulsePipe
    - [x] Core commands: run (pipeline execution), config (configuration management), model (data model exploration)
    - [x] Profile-based configuration system with YAML support
    - [x] Pipeline tracking, logging, and reporting capabilities
    - [x] Configuration validation and data model exploration tools
- [x] Contributor's guide
- [x] Python package with `pydantic` validation
- [x] Python developer tooling:
    - [x] Poetry
    - [x] Pytest
    - [x] Mypy
- [x] HL7 v2.x ingestion
    - [x] Custom HL7 parser that casts a wide net (parses everything in segments of interest)
- [x] Vector database integration:
    - [x] Weaviate
    - [x] Qdrant
- [x] 📖 Pipeline Concurrent Step Execution
    - [x] Add queues to relay messages between steps
- [x] 📖 CLI Model Description
    - [x] Write concise descriptions of each `PulseClinicalContent` and `PulseOperationalContent` models (now exposed in the CLI)
    - [x] Ensure descriptions show up clearly in `pulsepipe model --help` and related views
- [x] HL7 CDA/CCDA (XML) document ingestion
- [x] De-identification
    - [x] HIPAA's 18 Identifiers -- Safe harbor
    - [X] Presidio
    - [X] Clinical NER models
- [ ] 🧬 Composable Chunking + Embedding Framework
    - [x] `Chunker` base class interfaces
    - [x] `Embedder` base class interfaces
    - [x] YAML configuration for chunkers and embedders in `pipeline.yaml`
    - [x] `ClinicalEmbedder`: calls local ClinicalBERT for embedding clinical canonical data model
    - [x] `OperationalEmbedder`: calls local MiniLM-L6-v2 for embedding operational canonical data model
    - [x] Embedding pipeline supporting ClinicalBERT
    - [x] Embedding pipeline supporting MiniLM-L6-v2
    - [ ] `NarrativeChunker`: extracts and truncates text narrative fields
    - [ ] `FhirBundleSplitterChunker`: splits on `entry.resource`
    - [ ] CLI options to test chunking/embedding flows
    - [ ] Output of chunks and embeddings in vector-friendly format (e.g., JSONL)
- [ ] 🧪 Synthetic Test Dataset
    - [x] Integrate Synthea to generate high-fidelity synthetic FHIR data
    - [ ] Generate a small, high-quality synthetic dataset (FHIR, HL7v2, X12)
    - [ ] Include edge cases (multiple patients, empty fields, varied formats)
    - [ ] Use for unit/integration tests of mappers and embedding steps
- [ ] Review Canonical Content Models (CDM) by Medical Informatics SME
    - [ ] Medical Informatics SME 1
    - [ ] Medical Informatics SME 2
- [ ] 🏥 Healthcare Data Intelligence & Quality Assurance
    - [x] YAML Configuration Framework for Data Intelligence Features
        - [x] Add `data_intelligence` section to pipeline.yaml with granular on/off controls
        - [x] Implement performance mode settings (fast/standard/comprehensive)
        - [x] Add sampling configuration for high-volume processing
        - [x] Create feature-specific enable/disable flags
    - [x] Ingestion Success/Failure Tracking System
        - [x] Add `ingestion_stats` table to persistence layer
        - [x] Track per-file/per-record success/failure counts with error categorization
        - [x] Store failed records with error messages for AI training feedback loop
        - [x] Export ingestion metrics to JSON/CSV for analysis
        - [x] Add YAML config: `tracking.enabled`, `tracking.store_failed_records`
    - [x] Detailed Audit Trail Infrastructure
        - [x] Extend logging to capture record-level processing status (parsed/failed/skipped)
        - [x] Add structured error classification (schema_error, validation_error, parse_error, etc.)
        - [x] Create audit report generator that outputs processing statistics by data source
        - [x] Add YAML config: `audit_trail.enabled`, `audit_trail.detail_level`
        - [ ] Add CLI command: `pulsepipe audit --summary` and `pulsepipe audit --failures`
    - [x] Data Quality Scoring Engine
        - [x] Implement completeness scoring (% of required fields populated)
        - [x] Add data consistency checks (date ranges, code format validation)
        - [x] Create outlier detection for numeric values (vital signs, lab values)
        - [x] Generate quality score per record and aggregate scores per batch
        - [x] Add YAML config: `quality_scoring.enabled`, `quality_scoring.sampling_rate`
    - [ ] Clinical Content Analysis Module
        - [ ] Add text analysis to detect missing structured data in clinical notes using ClinicalBERT
        - [ ] Implement regex patterns to identify potential PHI leakage post-de-identification
        - [ ] Create medication/diagnosis extraction from free text for completeness checking
        - [ ] Add data standardization gap detection (non-standard date formats, naming conventions)
        - [ ] Add YAML config: `content_analysis.enabled`, `content_analysis.phi_detection_only`
    - [ ] Healthcare Terminology Validation Framework
        - [ ] Create code validation functions for ICD-10, SNOMED CT, RxNorm, LOINC
        - [ ] Add terminology coverage reporting (% of codes mapped to standard vocabularies)
        - [ ] Implement "unmapped terms" collector for manual review
        - [ ] Generate terminology compliance reports with remediation suggestions
        - [ ] Add YAML config: `terminology_validation.enabled`, `terminology_validation.code_systems`
- [ ] 🚀 Performance Metrics & Infrastructure Intelligence
    - [x] Step-Level Performance Tracking
        - [x] Add timing decorators to each pipeline step with records/second metrics
        - [x] Track configuration impact on performance (AI features vs speed trade-offs)
        - [x] Identify bottlenecks with detailed step breakdown analysis
        - [x] Generate optimization recommendations based on workload patterns
    - [ ] Environmental System Metrics
        - [x] Capture CPU type, core count, RAM, and storage type during pipeline execution
        - [ ] Track peak resource utilization (CPU %, memory usage, disk I/O) per step
        - [ ] Detect GPU availability and utilization for future acceleration features
        - [ ] Add infrastructure sizing recommendations for different workload scales
    - [ ] Performance CLI Integration
        - [ ] Add `--performance-report` flag to generate detailed performance summaries
        - [ ] Create `pulsepipe benchmark` command for infrastructure testing
        - [ ] Include system environment in audit logs for deployment optimization
        - [ ] Generate cloud instance sizing recommendations (AWS, Azure, GCP)
    - [ ] Performance Summary Display
        ```
        🚀 Performance Summary
        ├── Total Pipeline Time: 4m 23s
        ├── Records Processed: 1,247 patients
        ├── Overall Rate: 4.8 records/sec
        └── Bottleneck: Clinical Analysis (15.9 rec/sec)

        ⚡ Step Breakdown:
        ├── Ingestion     │ ████████████████████ │  12s │ 101.4/sec
        ├── Normalization │ ████████████████████ │  46s │  27.3/sec  
        ├── De-ID         │ ██████████           │  89s │  14.0/sec │ 🔍 NER enabled
        ├── Chunking      │ ████████████████████ │  23s │  54.0/sec
        ├── Embedding     │ ████████             │ 157s │  21.8/sec │ 🧠 ClinicalBERT
        └── Analysis      │ ██████               │  78s │  15.9/sec │ 🔬 AI insights

        💡 Optimization Tips:
        • Set clinical_analysis.sampling_rate=0.1 → ~3x faster
        • Increase embedding.batch_size=64 → ~15% faster  
        • For production loads, consider disabling AI features
        ```
- [ ] Complete Unit Tests:
    - [x] Banner Display
    - [x] Filewatcher Adapter
    - [x] Ingestors (FHIR, HL7v2, X12, CDA)
    - [x] CLI command parsing and context propagation
    - [x] Canonical Models and Pydantic schemas
    - [x] YAML configuration loading and validation
    - [x] Logging setup and enrichment
    - [x] Chunker framework and implementation coverage
    - [x] Embedder call mocking and fallback logic
    - [x] Vector DB connectivity and document serialization
    - [x] Integrate Pytests with Github Actions
    - [x] Add a Code Coverage Report
    - [x] Code Coverage >=85% (Current coverage at 86% on Ubuntu, 83% on Windows, 83% on AWS Linux Cloud b/c it skips all the vector dbs unit tests)
    - [ ] Review existing tests for superficial coverage (init-only tests without meaningful validation)
    - [ ] Add tests for error paths and boundary conditions (malformed data, connection failures, timeouts)
    - [ ] Expand coverage for complex logic branches in high-risk modules (parsers, config handlers, pipeline execution)
    - [ ] Manual testing with realistic but messy data

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
    - [ ] Pinecone
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
