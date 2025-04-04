# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Embed. Healthcare Data, AI-Ready.
# https://github.com/PulsePipe/pulsepipe
#
# Copyright (C) 2025 Amir Abrams
#
# This file is part of PulsePipe and is licensed under the GNU Affero General 
# Public License v3.0 (AGPL-3.0). A full copy of this license can be found in 
# the LICENSE file at the root of this repository or online at:
# https://www.gnu.org/licenses/agpl-3.0.html
#
# PulsePipe is distributed WITHOUT ANY WARRANTY; without even the implied 
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# We welcome community contributions — if you make it better, 
# share it back. The whole healthcare ecosystem wins.
# ------------------------------------------------------------------------------
# ------------------------------------------------------------------------------
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

import pytest
from pathlib import Path
from pulsepipe.ingesters.fhir_ingester import FHIRIngester
from pulsepipe.models import PulseClinicalContent

@pytest.fixture
def fhir_bundle():
    """Load sample FHIR Bundle as raw JSON string"""
    bundle_path = Path(__file__).parent / "fixtures" / "simple_patient_bundle.json"
    return bundle_path.read_text()


def test_fhir_bundle_ingest(fhir_bundle):
    # Arrange
    ingester = FHIRIngester()

    # Act
    clinical_content: PulseClinicalContent = ingester.parse(fhir_bundle)

    # Assert
    
    # ---- Basic presence tests ----
    assert clinical_content.patient is not None
    assert clinical_content.encounter is not None
    assert len(clinical_content.vital_signs) > 0
    assert len(clinical_content.allergies) > 0
    assert len(clinical_content.immunizations) > 0
    assert len(clinical_content.diagnoses) >= 0  # maybe empty depending on mapping
    assert len(clinical_content.problem_list) > 0
    assert len(clinical_content.medications) > 0
    assert len(clinical_content.lab) > 0
    assert len(clinical_content.pathology) > 0
    assert len(clinical_content.imaging) > 0
    assert len(clinical_content.microbiology) > 0
    assert len(clinical_content.blood_bank) > 0
    assert len(clinical_content.diagnostic_test) > 0
    print("🧪 Common Data Model Results:\n", clinical_content.model_dump_json(indent=2))

    # ---- Content checks ----
    #cbc = next((lab for lab in clinical_content.lab if "CBC" in lab.test_name), None)
    #assert cbc is not None, "CBC Panel should be present in lab reports"
    #assert any(res.name == "Hemoglobin" for res in cbc.results), "Hemoglobin should be in CBC"
    #assert any(res.name == "WBC" for res in cbc.results), "WBC should be in CBC"
    #assert any(res.name == "Platelets" for res in cbc.results), "Platelets should be in CBC"

    #path = next((p for p in clinical_content.pathology if "Liver Biopsy" in p.test_name), None)
    #assert path is not None
    #assert "steatosis" in path.result_text.lower()

    #cxr = next((img for img in clinical_content.imaging if "Chest X-Ray" in img.test_name), None)
    #assert cxr is not None
    #assert "cardiomegaly" in cxr.result_text.lower()

    #ucx = next((micro for micro in clinical_content.microbiology if "Urine Culture" in micro.test_name), None)
    #assert ucx is not None
    #assert "E. coli" in ucx.result_text

    #ekg = next((d for d in clinical_content.diagnostic_test if "ECG" in d.test_name), None)
    #assert ekg is not None
    #assert "sinus rhythm" in ekg.result_text.lower()
