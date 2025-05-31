# PulsePipe Healthcare Data Pipeline System

## System Overview

PulsePipe is a comprehensive **AI-native healthcare data processing system** designed to ingest, normalize, de-identify, chunk, and embed healthcare data for AI-ready RAG (Retrieval-Augmented Generation) applications. The system follows enterprise-grade patterns with robust audit trails, multi-database support, and comprehensive error handling, specifically tailored for **clinical and operational healthcare workflows**.

## 🧪 **CRITICAL: 85% Unit Test Coverage Requirement**

**ALL CODE CONTRIBUTIONS MUST MAINTAIN ≥85% UNIT TEST COVERAGE**

### Testing Standards
- **Meaningful Coverage**: No superficial init-only tests without validation
- **Error Path Testing**: Must test boundary conditions, malformed data, connection failures, timeouts
- **Complex Logic Coverage**: Expand coverage for high-risk modules (parsers, config handlers, pipeline execution)
- **Healthcare Data Edge Cases**: Test with realistic but messy clinical data scenarios

### Running Tests & Coverage
```bash
# Run all tests with coverage
poetry run pytest --cov=src/pulsepipe --cov-report=term-missing tests/ -s

# Generate HTML coverage report
poetry run pytest --cov=src/pulsepipe --cov-report=term-missing --cov-report=html tests/ -s

# Run specific healthcare data tests
poetry run pytest -s tests/test_hl7v2_ingester.py
poetry run pytest -s tests/test_fhir_ingester.py

# Current Coverage Status:
# ✅ Ubuntu: 88%
# ⚠️  Windows: 85% (vector DB tests skipped)
# ⚠️  AWS Linux: 85% (vector DB tests skipped)
```

## How Claude Can Help

As your AI coding assistant, I can help you with:

### 🎯 **Healthcare-Specific Architecture Understanding**
- Explain clinical data flow patterns (HL7, FHIR, X12, CDA/CCDA → Canonical Model)
- Break down HIPAA-compliant de-identification pipelines
- Understand embedding strategies for clinical vs operational data
- Navigate healthcare terminology integration (SNOMED, ICD-10, LOINC)

### 🧪 **Test-Driven Development**
- **Write comprehensive unit tests** that achieve ≥85% coverage
- Design tests for **healthcare data edge cases** (malformed HL7, incomplete FHIR bundles)
- Create **error path validation** for clinical data processing failures
- Implement **boundary condition tests** for PHI detection and de-identification

### 🔧 **Healthcare Data Pipeline Development**
- Implement new **clinical data ingestors** (Epic, Cerner, Athena formats)
- Extend the **canonical clinical/operational models**
- Add new **vector database integrations** for medical embeddings
- Create **custom chunking strategies** for clinical narratives

### 🚀 **AI/ML Healthcare Extensions**
- Integrate new **clinical embedding models** (ClinicalBERT, BioBERT, Med-BERT)
- Design **multi-vector search strategies** for clinical similarity
- Implement **terminology validation frameworks** (ICD-10, SNOMED CT, RxNorm, LOINC)
- Create **AI-assisted data profiling** for clinical data quality

---

## Healthcare Data Pipeline Architecture

```
🏥 Healthcare Data Processing Pipeline
┌─────────────────────────────────────────────────────────────────────────┐
│                     Clinical & Operational Data Flow                     │
├───────────────┬─────────────────┬─────────────────┬─────────────────────┤
│   Ingestion   │  Normalization  │ De-Identification│    AI Processing    │
│               │                 │                 │                     │
│ HL7 v2.x     ├─────────────────┤ Presidio +      │ Clinical Chunking   │
│ FHIR R4      │ Pulse Canonical │ Clinical NER    │ Medical Embeddings  │
│ X12 Claims   │ Clinical Model  │ HIPAA Safe      │ Vector Storage      │
│ CDA/CCDA     │ Pulse Canonical │ Harbor          │ RAG Applications    │
│ Plain Text   │ Operational     │ PHI Removal     │ Clinical Search     │
│ Custom       │ Model           │                 │ Similarity Matching │
└───────────────┴─────────────────┴─────────────────┴─────────────────────┘
```

## Core Healthcare Components Deep Dive

### 1. **Clinical Data Ingestion** (`src/pulsepipe/ingesters/`)
**Core Concept**: Multi-format healthcare data normalization to canonical models

```python
# Example: FHIR to Canonical Clinical Model
fhir_ingester = FHIRIngester(config)
clinical_content = fhir_ingester.ingest(fhir_bundle)

# Example: HL7 v2.x to Canonical Clinical Model  
hl7_ingester = HL7v2Ingester(config)
clinical_content = hl7_ingester.ingest(hl7_message)
```

**Healthcare-Specific Patterns**:
- **Wide-Net Parsing**: Casts wide net to capture all relevant clinical data
- **Canonical Model Mapping**: Transforms diverse formats to unified clinical representation
- **PHI-Aware Processing**: Maintains patient privacy throughout ingestion

### 2. **De-Identification Pipeline** (`src/pulsepipe/deid/`)
**Core Concept**: HIPAA-compliant removal of 18 PHI identifiers

```
De-ID Flow:
┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Presidio   │───▶│ Clinical    │───▶│ Safe Harbor │
│  Base       │    │ NER Models  │    │ Validation  │
│             │    │             │    │             │
│ • Names     │    │ • Medical   │    │ • 18 PHI    │
│ • Dates     │    │ • Locations │    │ • Compliance│
│ • IDs       │    │ • Clinical  │    │ • Audit     │
└─────────────┘    └─────────────┘    └─────────────┘
```

### 3. **AI-Native Embedding System** (`src/pulsepipe/embedders/`)
**Core Concept**: Clinical and operational embedding strategies for healthcare AI

```python
# Clinical embeddings for patient data
clinical_embedder = ClinicalEmbedder(model="clinical-bert-base")
clinical_vectors = clinical_embedder.embed(clinical_chunks)

# Operational embeddings for administrative data
operational_embedder = OperationalEmbedder(model="minilm-l6-v2")
operational_vectors = operational_embedder.embed(operational_chunks)
```

**Healthcare AI Features**:
- **Clinical Context Preservation**: Maintains medical meaning in embeddings
- **Multi-Modal Support**: Handles structured data + clinical narratives
- **Vector Database Integration**: Optimized for medical similarity search

---

## Healthcare-Specific Development Patterns

### **Clinical Data Validation**
```python
# Example: FHIR resource validation with healthcare-specific rules
def test_fhir_patient_validation():
    fhir_data = {
        "resourceType": "Patient",
        "identifier": [{"value": "12345"}],
        "name": [{"family": "Doe", "given": ["John"]}],
        "birthDate": "1990-01-01"
    }
    
    # Test canonical model conversion
    clinical_content = fhir_ingester.ingest(fhir_data)
    assert clinical_content.patient.birth_date == "1990-01-01"
    
    # Test PHI handling
    assert "Doe" not in str(clinical_content) after de-identification
```

### **Healthcare Error Handling Strategy**
```python
try:
    # Clinical data processing
    clinical_content = ingester.ingest(healthcare_data)
    quality_score = quality_engine.assess(clinical_content)
    
    # Track healthcare-specific metrics
    tracker.record_success(
        record_id=patient_id,
        record_type="Patient",
        processing_time_ms=elapsed,
        quality_score=quality_score
    )
except HL7ParsingError as e:
    # Healthcare-specific error classification
    error_analysis = classifier.classify_error(e, stage_name="hl7_parsing")
    tracker.record_failure(patient_id, e, IngestionStage.PARSING)
    audit_logger.log_clinical_error(stage_name, patient_id, e)
```

### **Clinical Test Coverage Requirements**
```python
# Example: Comprehensive clinical data testing
class TestHL7v2Ingester:
    def test_adt_message_complete(self):
        """Test complete ADT message processing"""
        # Test with realistic HL7 message
        
    def test_adt_message_missing_pid_segment(self):
        """Test error handling for missing patient segment"""
        # Test boundary condition
        
    def test_adt_message_malformed_datetime(self):
        """Test handling of malformed timestamps"""
        # Test error path
        
    def test_phi_detection_in_hl7_fields(self):
        """Test PHI detection across all HL7 segments"""
        # Test healthcare-specific validation
```

---

## Healthcare Data Quality Assurance

### **Clinical Data Intelligence Features**
```yaml
# pipeline.yaml - Healthcare-specific quality controls
data_intelligence:
  enabled: true
  performance_mode: "comprehensive"  # fast/standard/comprehensive
  
  quality_scoring:
    enabled: true
    sampling_rate: 1.0  # 100% for clinical data
    clinical_validation: true
    terminology_checks: true
    
  audit_trail:
    enabled: true
    record_level_tracking: true
    phi_audit: true  # Track de-identification effectiveness
    
  clinical_analysis:
    enabled: true
    missing_data_detection: true
    clinical_terminology_validation: true
    medication_interaction_checks: true
```

### **Healthcare Performance Monitoring**
```
🏥 Clinical Pipeline Performance Summary
├── Total Patients Processed: 1,247
├── HL7 Messages: 3,891 │ FHIR Bundles: 1,247 │ X12 Claims: 892
├── PHI Removal Rate: 99.97% │ Quality Score: 94.2%
└── Clinical Terminology Coverage: 87.3%

⚡ Healthcare-Specific Bottlenecks:
├── De-identification │ ██████████ │ 89s │ 14.0/sec │ 🔒 PHI scrubbing
├── Clinical NER      │ ████████   │ 78s │ 15.9/sec │ 🧠 Medical entities
├── Terminology Val.  │ ██████     │ 67s │ 18.6/sec │ 📚 SNOMED/ICD-10
└── Quality Scoring   │ ████       │ 45s │ 27.7/sec │ 📊 Clinical metrics

💡 Healthcare Optimization Tips:
• Enable clinical_analysis.sampling_rate=0.1 for faster processing
• Use terminology_validation.essential_only=true for core codes
• Consider phi_detection.confidence_threshold=0.85 for speed vs accuracy
```

---

## Healthcare-Specific Questions I Can Help Answer

### 🏛️ **Clinical Architecture Questions**
- "How does PulsePipe maintain HIPAA compliance during the embedding process?"
- "What's the best way to add support for Epic's proprietary HL7 segments?"
- "How should I structure a new clinical terminology validator?"

### 🛠️ **Healthcare Implementation Questions**
- "How do I create a custom clinical quality scorer?"
- "What's the pattern for adding new medical embedding models?"
- "How can I extend the canonical clinical model for cardiology data?"

### 🚀 **Medical AI Extension Questions**
- "How would I add support for DICOM medical imaging integration?"
- "What's the best way to implement clinical decision support rules?"
- "How can I add SNOMED CT concept mapping to the pipeline?"

### 🐛 **Clinical Data Debugging Questions**
- "How can I trace a patient record through de-identification?"
- "What's the best way to analyze PHI detection accuracy?"
- "How do I investigate clinical data quality scores?"

---

## Getting Started with Healthcare Development

1. **Understand Clinical Data Flow**: Start with healthcare format ingestion (HL7, FHIR)
2. **Test with Synthetic Data**: Use Synthea for generating realistic test datasets
3. **Focus on PHI Compliance**: Understand de-identification requirements
4. **Quality-First Approach**: Implement quality scoring for clinical data

**Example Healthcare Development Session**:
```bash
# 1. Generate synthetic clinical data
poetry run python scripts/generate_synthea_data.py

# 2. Test FHIR ingestion with coverage
poetry run pytest --cov=src/pulsepipe/ingesters tests/test_fhir_ingester.py -s

# 3. Run clinical pipeline with quality tracking
pulsepipe run --profile patient_fhir --summary

# 4. Analyze clinical data quality
pulsepipe metrics analyze --pipeline-run-id <id> --format table

# 5. Check test coverage (must be ≥85%)
poetry run pytest --cov=src/pulsepipe --cov-report=term-missing tests/ -s
```

The system is specifically designed for **healthcare AI applications** with enterprise-grade compliance, clinical data quality assurance, and medical terminology integration. Every component must maintain the 85% test coverage standard while handling the complexities of real-world clinical data processing.