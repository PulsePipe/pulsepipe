
# PulsePipe Roadmap

This document outlines the planned milestones and future direction of the PulsePipe project.

---

## ✅ MVP Scope

- [x] Canonical Clinical Content Model (Pulse Canonical Clinical Model)
- [x] Canonical Operational Content Model (Pulse Canonical Operational Model)
- [x] YAML-based configuration (ingestor registration, pipelines, vector dbs, deid)
- [x] Ingestion Adapter factory with file watcher
- [x] Global persistence for file watcher cache and other cache using SQLite
- [ ] HL7 v2.x ingestion
- [x] FHIR (JSON/XML) ingestion
- [x] X12 Claims and Prior Auth ingester
- [x] Command Line Interface (CLI) implementation for PulsePipe
    - [ ] Multi pipeline execution
- [x] Contributor's guide
- [x] Python package with `pydantic` validation
- [x] Python developer tooling:
    - [x] Poetry
    - [x] Pytest
    - [x] Mypy
- [ ] De-identification via Presidio + Clinical NER models
- [ ] Chunking engine for embedding preparation
- [ ] Embedding pipeline supporting:
    - ClinicalBERT
    - OpenAI GPT-3 / GPT-4
    - DeepSeek
- [ ] Vector database integration:
    - [ ] Pinecone
    - [ ] Weaviate
    - [ ] FAISS

---

## 🟣 Short-Term Goals (v0.2.x - v0.3.x)

- [ ] Support for generating **multiple embedding formats** simultaneously (e.g., BERT + GPT vectors)
- [ ] Improved de-identification pipelines:
    - Incorporate clinical-specific regex library
    - BioClinicalBERT + Presidio joint recognizer
- [ ] Optional SNOMED CT and ICD code extraction for terminology enrichment
- [ ] CDA / CCDA document ingestion
- [ ] Custom template ingestion (extensible ingestor system)
- [ ] Plain text ingestion
- [ ] Support for embedding metadata directly into vector stores
- [ ] Optional export of embeddings to offline formats (Parquet, Feather)
- [ ] Global persistence for file watcher cache and other cache using PostgreSQL (PulsePilot Exclusive)
- [ ] Global persistence using cloud-native backends (e.g., AWS S3 + DynamoDB) (PulsePilot Exclusive)
- [ ] Add REST API Ingestion Adapter to the factory
- [ ] PulsePilot UI (React + FastAPI)
    - View and manage active pipelines: Allows users to start, stop, and monitor pipeline activity (e.g., HL7 → FHIR → Vector store), with real-time logs and status.
    - UI for editing configuration (typically maintained via YAML files): Visual interface for editing pipeline and system configurations that are usually managed manually via YAML—includes validation, schemas, and save/publish controls.
    - Monitor ingestion performance and metrics: Dashboards to track ingestion throughput, processing time, and error rates for each stage of the pipeline.
    - Analyze de-identification processes: Insights into how identifiers are removed or replaced—includes PHI detection types, counts, and potential misses.
    - Visualize document chunking activity: UI for inspecting how documents are chunked for embedding—chunk size, overlap, structure warnings, etc.
    - Inspect embedding performance and statistics: Tracks which embedding models were used, token usage, embedding sizes, success/failure rates, and latency.
    - Explore metadata extraction and usage: Displays key extracted metadata (e.g., patient ID, doc type, timestamps), showing how it's used for indexing, filtering, and downstream querying.
    - Enable basic vector space visualization and inspection: Provides 2D/3D projections (e.g., t-SNE, UMAP) of embedded vectors to explore clustering and identify embedding anomalies or outliers.

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

---

## 🟡 Long-Term Goals (v1.x+)

- [ ] Official extension framework for custom: (PulsePilot Exclusive)
    - Ingestors
    - De-identification modules
    - Embedding engines
- [ ] Enterprise readiness: (PulsePilot Exclusive)
    - Full audit and provenance logging
    - Role-based access control (RBAC)
    - Secure API authentication (JWT, OAuth2, or OIDC)
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
    - Meta Llama (with healthcare adaptations)
    - BioBERT, ClinicalBERT, and Med-BERT family
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
