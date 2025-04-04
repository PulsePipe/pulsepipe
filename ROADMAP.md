# PulsePipe Roadmap

This document outlines the planned milestones and future direction of the PulsePipe project.

---

## ✅ MVP Scope

- [x] Canonical Clinical Content Model (Pulse Canonical Clinical Model)
- [ ] Canonical Operational Content Model (Pulse Canonical Operational Model)
- [x] YAML-based configuration (ingestor registration, pipelines, vector dbs, deid)
- [x] Ingestion Adapter factory with file watcher
- [x] Global persistence for file watcher cache and other cache using SQLite
- [ ] HL7 v2.x ingestion
- [x] FHIR (JSON/XML) ingestion
- [ ] CDA / CCDA document ingestion
- [x] X12 Claims and Prior Auth ingester
- [ ] Plain text ingestion
- [ ] Custom template ingestion (extensible ingestor system)
- [ ] De-identification via Presidio + Clinical NER models
- [ ] Chunking engine for embedding preparation
- [ ] Embedding pipeline supporting:
    - ClinicalBERT
    - OpenAI GPT-3 / GPT-4
    - DeepSeek
- [ ] Vector database integration:
    - Pinecone [ ]
    - Weaviate [ ]
    - FAISS [ ]
- [ ] Optional PulsePilot UI (React + FastAPI)
- [x] Python package with `pydantic` validation
- [ ] Python developer tooling:
    - Poetry [x]
    - Pytest [x]
    - Mypy [ ]

---

## 🟣 Short-Term Goals (v0.2.x - v0.3.x)

- [ ] Support for generating **multiple embedding formats** simultaneously (e.g., BERT + GPT vectors)
- [ ] Improved de-identification pipelines:
    - Incorporate clinical-specific regex library
    - BioClinicalBERT + Presidio joint recognizer
- [ ] Basic SNOMED CT and ICD code extraction (optional)
- [ ] PulsePilot UI:
    - Initial Search / Similarity Explorer
    - Basic vector space inspection
- [ ] Support for embedding metadata directly into vector stores
- [ ] Optional export of embeddings to offline formats (Parquet, Feather)
- [ ] Global persistence for file watcher cache and other cache using PostgreSQL (PulsePilot Exclusive)
- [ ] Global persistence for file watcher cache and other cache using like a cloud native AWS S2/DynamoDB (PulsePilot Exclusive)
- [ ] Add REST API Ingestion Adapter to the factory
- [ ] Global persistence for file watcher cache and other cache using SQLite

---

## 🟠 Mid-Term Goals (v0.4.x - v0.5.x)

- [ ] UMLS & SNOMED CT Concept Tagging (standardized clinical terminology mapping)
- [ ] Support for additional clinical embeddings:
    - GatorTron (if available)
    - Med-BERT
    - Custom user-provided models
- [ ] Multi-vector search strategies
- [ ] Improved canonical data model to include:
    - Problem Lists
    - Medications
    - Labs
    - Imaging Reports
- [ ] PulsePilot UI:
    - Enhanced search UX
    - Visualization components (embedding space plots, concept graph)
- [ ] OpenAPI & GraphQL endpoints for programmatic access
- [ ] AI-Assisted Data Profiling & Normalization Suggestion Engine (PulsePilot Exclusive)
    - Automatic detection of unmapped or inconsistent data fields.
    - AI-powered recommendations for data normalization and FHIR mapping.
    - Schema drift detection.
    - Interactive UI for reviewing and exporting recommendations.
    - Embedding-based similarity and terminology-assisted suggestions.
- [ ] Add FHIR webhook to the ingestion adapter factory (PulsePilot Exclusive)
- [ ] Add MLTP Socket for HL7 v2.x live feeds to the ingestion adapter factory (PulsePilot Exclusive)

---

## 🟡 Long-Term Goals (v1.x+)

- [ ] Official extension framework for custom:
    - Ingestors
    - De-identification modules
    - Embedding engines
- [ ] Enterprise readiness:
    - Full audit and provenance logging
    - Role-based access control (RBAC)
    - Secure API authentication (JWT, OAuth2, or OIDC)
- [ ] Support for multi-tenant vector stores (e.g., namespace isolation, tenant-aware indexing)
- [ ] Streaming data ingestion and dispatcher:
    - HL7 Listener (MLLP / TCP)
    - FHIR Webhook (REST)
    - Kafka / Pulsar consumer
    - Generic dispatcher for routing inbound events
- [ ] Integration with private or enterprise-scale LLMs:
    - Private GPT instances
    - Llama 2 / Llama 3
    - Other on-prem or fine-tuned clinical models
- [ ] Support for healthcare-specific LLM fine-tuning:
    - Transfer learning pipeline
    - Embedding + supervised task workflows
- [ ] SaaS Offering for PulsePilot.
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
    - Embedding visualization (optional)
- [ ] Complete developer & operator documentation:
    - Contributor's guide
    - API documentation
    - Deployment guides (cloud + on-prem)
    - Example pipelines

---

## Notes

This roadmap is iterative. Contributions, feedback, and feature requests are welcome and encouraged.

PulsePipe aims to serve as a foundation for healthcare AI pipelines, combining modern embedding workflows with robust clinical data ingestion and de-identification.
