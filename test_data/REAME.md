# Test Data Directory

This directory contains test datasets used for PulsePipe development and testing. The data is organized into Git submodules to keep the main repository lightweight while providing access to comprehensive healthcare data samples.

## 📁 Submodules Overview

### 1. Synthea (`synthea/`)
**Repository**: [SyntheaProject/synthea](https://github.com/syntheaproject/synthea)  
**Purpose**: Synthetic patient data generator for FHIR, HL7 v2, and other healthcare formats

Synthea generates realistic but synthetic patient records including:
- FHIR R4 resources (Patient, Encounter, Observation, etc.)
- HL7 v2 messages (ADT, ORU, ORM)
- C-CDA documents
- CSV export formats

### 2. Sample CCDAs (`sample_ccdas/`)
**Repository**: [jmandel/sample_ccdas](https://github.com/jmandel/sample_ccdas)  
**Purpose**: Collection of real-world anonymized C-CDA documents for testing

Contains various C-CDA document types:
- Continuity of Care Documents (CCD)
- Discharge Summaries
- Consultation Notes
- Progress Notes
- Various clinical document templates

## 🚀 Quick Setup

### Initialize All Test Data
```bash
# From the PulsePipe root directory
git submodule update --init --recursive test_data/
```

### Initialize Individual Submodules
```bash
# Initialize only Synthea
git submodule update --init test_data/synthea

# Initialize only sample CCDAs
git submodule update --init test_data/sample_ccdas
```

## 📋 Detailed Setup Instructions

### Fresh Clone Setup
If you're cloning PulsePipe for the first time:

```bash
# Clone PulsePipe with all submodules
git clone --recursive https://github.com/your-org/pulsepipe.git
cd pulsepipe

# Verify submodules are populated
ls -la test_data/synthea
ls -la test_data/sample_ccdas
```

### Existing Repository Setup
If you already have PulsePipe cloned:

```bash
cd pulsepipe

# Update to latest submodule configurations
git pull origin main

# Initialize the new test_data submodules
git submodule update --init --recursive test_data/
```

## 🔄 Updating Test Data

### Update All Submodules
```bash
# Update all submodules to their latest commits
git submodule update --remote test_data/
```

### Update Individual Submodules
```bash
# Update only Synthea
git submodule update --remote test_data/synthea

# Update only sample CCDAs  
git submodule update --remote test_data/sample_ccdas
```

### Commit Submodule Updates
```bash
# After updating submodules, commit the new references
git add test_data/
git commit -m "Update test data submodules to latest versions"
```

## 🧪 Using Test Data with PulsePipe

### Synthea Data
```bash
# Generate synthetic patient data (requires Java)
cd test_data/synthea
./gradlew build
./run_synthea -p 10  # Generate 10 patients

# Use generated FHIR data with PulsePipe
pulsepipe run --profile fhir_test --input test_data/synthea/output/fhir/
```

### Sample C-CDA Documents
```bash
# Process sample C-CDA documents
pulsepipe run --profile cda_test --input test_data/sample_ccdas/

# Process specific C-CDA types
pulsepipe run --profile cda_test --input test_data/sample_ccdas/CCDA_CCD_b1_*
```

## 🔍 Verification

Verify your test data setup:

```bash
# Check submodule status
git submodule status test_data/

# Verify Synthea installation
ls test_data/synthea/src/main/resources/

# Verify sample C-CDA availability
find test_data/sample_ccdas/ -name "*.xml" | wc -l
```

## ⚠️ Important Notes

- **Data Privacy**: All test data consists of synthetic or anonymized samples only
- **Storage**: Submodules are not included in the main repository size
- **Updates**: Test data updates independently from PulsePipe core
- **Requirements**: Synthea requires Java 11+ to generate new data
- **Performance**: Large datasets may impact initial clone time

## 🔧 Troubleshooting

### Submodule Not Initializing
```bash
# Force re-initialization
git submodule deinit test_data/synthea
git submodule update --init test_data/synthea
```

### Empty Directories
```bash
# Ensure you're in the correct directory
pwd  # Should show pulsepipe root

# Re-run recursive update
git submodule update --init --recursive
```

### Permission Issues
```bash
# On Unix systems, ensure execute permissions
chmod +x test_data/synthea/gradlew
```

## 📚 Additional Resources

- [Synthea Documentation](https://github.com/syntheaproject/synthea/wiki)
- [C-CDA Implementation Guide](https://www.hl7.org/implement/standards/product_brief.cfm?product_id=492)
- [PulsePipe Configuration Guide](../src/pulsepipe/cli/README.md)

For issues with test data setup, please check the individual submodule repositories or open an issue in the main PulsePipe repository.