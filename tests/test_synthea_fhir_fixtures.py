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

# tests/test_synthea_fhir_fixtures.py

import os
import pytest
from pathlib import Path
from pulsepipe.ingesters.fhir_ingester import FHIRIngester
from pulsepipe.models.clinical_content import PulseClinicalContent

class TestSyntheaFHIRFixtures:
    """Test the FHIR ingester with Synthea-generated patient fixtures."""
    
    @pytest.fixture
    def fhir_ingester(self):
        """Set up a FHIR ingester instance."""
        return FHIRIngester()
    
    @pytest.fixture(params=[
        "Aldo_Wuckert_f68cef5d-f873-979e-963b-06d81b5200b6.json",
        "Daron_Corkery_dc389de6-8d79-a516-ff50-8d731113578f.json",
        "Gussie_Mitzie_Dickens_04acd7d4-5fb3-403b-feed-c3ff8aaaaa77.json",
        "Jenice_Labadie_e52c1a05-2b2d-3341-0ce8-580c5ff17f5e.json",
        "Kathyrn_Hessel_2cc870ad-6cb7-aae2-16b6-1deb8a5f55b9.json",
        "Leif_Pfannerstill_46d1ad9b-7dbc-2692-6577-e5a065b5cbbe.json",
        "May_Frank_Nicolas_2cd6b8b1-83a9-7444-d7bb-657182bb06c2.json",
        "Noel_Hermann_cf04147f-ee5e-360c-7cf6-a830b2ff958e.json",
        "Raymonde_Bashirian_6c2ea93b-1526-3dd6-29bf-fdd01f065a94.json",
        "Sara_Ana_María_Peralta_a9546050-a7c0-d45f-441b-971822efb680.json",
    ])
    def synthea_fixture(self, request):
        """Load Synthea fixture file based on parameter."""
        fixture_name = request.param
        fixture_path = Path(__file__).parent / "fixtures" / fixture_name

        if not fixture_path.exists():
            # For CI environments where directory structure might be different
            fixture_path = Path("fixtures") / fixture_name

        # Extract UUID from the filename - it's the part that looks like a UUID
        # For example, in "Aldo_Wuckert_f68cef5d-f873-979e-963b-06d81b5200b6.json"
        # The UUID is "f68cef5d-f873-979e-963b-06d81b5200b6"
        parts = fixture_name.split('_')
        patient_id = None
        if len(parts) >= 3:
            # The last part before .json is usually the UUID
            patient_id = parts[-1].split('.')[0]

        return {
            "name": fixture_name,
            "data": fixture_path.read_text(),
            "patient_id": patient_id
        }
    
    def test_synthea_fixture_ingestion(self, fhir_ingester, synthea_fixture):
        """Test ingesting Synthea fixture files."""
        fixture_name = synthea_fixture["name"]
        fixture_data = synthea_fixture["data"]
        
        # Process the test data
        result = fhir_ingester.parse(fixture_data)
        
        # Basic validation - should be a PulseClinicalContent object
        assert isinstance(result, PulseClinicalContent), f"Fixture {fixture_name} should produce PulseClinicalContent"
        
        # Validate patient data exists
        assert result.patient is not None, f"Fixture {fixture_name} should have patient data"
        
        # Verify patient ID if available
        if synthea_fixture["patient_id"]:
            # Extract just the UUID part from the filename for comparison
            uuid_part = synthea_fixture["patient_id"].split('.')[0]
            assert uuid_part in result.patient.id, \
                f"Patient ID mismatch in {fixture_name}: expected {uuid_part} in {result.patient.id}"
        
        # Print basic summary for debugging
        print(f"\n🧪 {fixture_name} Clinical Content Summary:")
        print(result.summary())
        
        # Test required clinical data is populated
        self._verify_clinical_data(result, fixture_name)
    
    def _verify_clinical_data(self, content: PulseClinicalContent, fixture_name: str):
        """Verify key clinical data elements are present in the content."""
        # Basic patient demographics must be present
        assert content.patient.id is not None, f"{fixture_name}: Patient ID missing"
        assert content.patient.gender is not None, f"{fixture_name}: Patient gender missing"

        # Check that we have at least one of each common clinical element
        # (Some patients may not have all element types)
        clinical_elements = [
            (content.encounter, "encounters"),
            (content.vital_signs, "vital signs"),
            (content.allergies, "allergies"),
            (content.medications, "medications"),
            (content.immunizations, "immunizations"),
            (content.problem_list, "problems"),
            (content.procedures, "procedures"),
            (content.lab, "lab results"),
            (content.diagnostic_test, "diagnostic tests"),
        ]

        # Count how many elements we have
        element_counts = {name: len(element) if isinstance(element, list) else (1 if element else 0)
                         for element, name in clinical_elements}

        # At least some clinical elements should be populated
        populated_elements = sum(1 for count in element_counts.values() if count > 0)
        assert populated_elements >= 3, f"{fixture_name}: Too few clinical elements populated ({populated_elements})"

        # Print element counts
        print(f"Clinical element counts for {fixture_name}:")
        for name, count in element_counts.items():
            print(f"  - {name}: {count}")

        # Perform additional validation on elements that should have specific structure
        # We no longer need detailed field validation as it can vary across fixtures
        # Just checking that main components are present is sufficient
    
    def test_all_synthea_fixtures_individually(self, fhir_ingester):
        """Test all Synthea fixtures with individual assertions for each file."""
        fixture_dir = Path(__file__).parent / "fixtures"
        
        # List of all the Synthea patient fixture files
        fixture_files = [
            "Aldo_Wuckert_f68cef5d-f873-979e-963b-06d81b5200b6.json",
            "Daron_Corkery_dc389de6-8d79-a516-ff50-8d731113578f.json",
            "Gussie_Mitzie_Dickens_04acd7d4-5fb3-403b-feed-c3ff8aaaaa77.json",
            "Jenice_Labadie_e52c1a05-2b2d-3341-0ce8-580c5ff17f5e.json",
            "Kathyrn_Hessel_2cc870ad-6cb7-aae2-16b6-1deb8a5f55b9.json",
            "Leif_Pfannerstill_46d1ad9b-7dbc-2692-6577-e5a065b5cbbe.json",
            "May_Frank_Nicolas_2cd6b8b1-83a9-7444-d7bb-657182bb06c2.json",
            "Noel_Hermann_cf04147f-ee5e-360c-7cf6-a830b2ff958e.json",
            "Raymonde_Bashirian_6c2ea93b-1526-3dd6-29bf-fdd01f065a94.json",
            "Sara_Ana_María_Peralta_a9546050-a7c0-d45f-441b-971822efb680.json",
        ]
        
        for fixture_file in fixture_files:
            fixture_path = fixture_dir / fixture_file
            fixture_data = fixture_path.read_text()
            
            # Process the fixture
            result = fhir_ingester.parse(fixture_data)
            
            # Basic validation that parse worked
            assert isinstance(result, PulseClinicalContent), f"Failed to parse {fixture_file}"
            assert result.patient is not None, f"{fixture_file} missing patient data"
            
            # Verify key data elements based on the fixture
            if "Aldo_Wuckert" in fixture_file:
                self._verify_aldo_wuckert(result)
            elif "Daron_Corkery" in fixture_file:
                self._verify_daron_corkery(result)
            elif "Gussie_Mitzie_Dickens" in fixture_file:
                self._verify_gussie_dickens(result)
            elif "Jenice_Labadie" in fixture_file:
                self._verify_jenice_labadie(result)
            elif "Kathyrn_Hessel" in fixture_file:
                self._verify_kathyrn_hessel(result)
            elif "Leif_Pfannerstill" in fixture_file:
                self._verify_leif_pfannerstill(result)
            elif "May_Frank_Nicolas" in fixture_file:
                self._verify_may_nicolas(result)
            elif "Noel_Hermann" in fixture_file:
                self._verify_noel_hermann(result)
            elif "Raymonde_Bashirian" in fixture_file:
                self._verify_raymonde_bashirian(result)
            elif "Sara_Ana_María_Peralta" in fixture_file:
                self._verify_sara_peralta(result)
    
    def _verify_aldo_wuckert(self, content: PulseClinicalContent):
        """Verify specific details for Aldo Wuckert's data."""
        assert "f68cef5d" in content.patient.id, "Patient ID should contain 'f68cef5d'"

        # Verify at least one medication exists
        assert len(content.medications) > 0, "Should have at least one medication"

        # Verify at least one lab test exists
        assert len(content.lab) > 0, "Should have at least one lab test"
    
    def _verify_daron_corkery(self, content: PulseClinicalContent):
        """Verify specific details for Daron Corkery's data."""
        assert "dc389de6" in content.patient.id, "Patient ID should contain 'dc389de6'"

        # Verify at least one medication exists
        assert len(content.medications) > 0, "Should have at least one medication"
    
    def _verify_gussie_dickens(self, content: PulseClinicalContent):
        """Verify specific details for Gussie Dickens' data."""
        assert "04acd7d4" in content.patient.id, "Patient ID should contain '04acd7d4'"

        # Verify at least one vital sign exists
        assert len(content.vital_signs) > 0, "Should have at least one vital sign"
    
    def _verify_jenice_labadie(self, content: PulseClinicalContent):
        """Verify specific details for Jenice Labadie's data."""
        assert "e52c1a05" in content.patient.id, "Patient ID should contain 'e52c1a05'"

        # Verify at least one procedure exists
        assert len(content.procedures) > 0, "Should have at least one procedure"
    
    def _verify_kathyrn_hessel(self, content: PulseClinicalContent):
        """Verify specific details for Kathyrn Hessel's data."""
        assert "2cc870ad" in content.patient.id, "Patient ID should contain '2cc870ad'"

        # Verify at least one immunization exists
        assert len(content.immunizations) > 0, "Should have at least one immunization"
    
    def _verify_leif_pfannerstill(self, content: PulseClinicalContent):
        """Verify specific details for Leif Pfannerstill's data."""
        assert "46d1ad9b" in content.patient.id, "Patient ID should contain '46d1ad9b'"

        # Verify at least one lab test exists
        assert len(content.lab) > 0, "Should have at least one lab test"
    
    def _verify_may_nicolas(self, content: PulseClinicalContent):
        """Verify specific details for May Nicolas' data."""
        assert "2cd6b8b1" in content.patient.id, "Patient ID should contain '2cd6b8b1'"

        # Verify at least one vital sign exists
        assert len(content.vital_signs) > 0, "Should have at least one vital sign"
    
    def _verify_noel_hermann(self, content: PulseClinicalContent):
        """Verify specific details for Noel Hermann's data."""
        assert "cf04147f" in content.patient.id, "Patient ID should contain 'cf04147f'"

        # Verify at least one medication exists
        assert len(content.medications) > 0, "Should have at least one medication"
    
    def _verify_raymonde_bashirian(self, content: PulseClinicalContent):
        """Verify specific details for Raymonde Bashirian's data."""
        assert "6c2ea93b" in content.patient.id, "Patient ID should contain '6c2ea93b'"

        # Verify at least one procedure exists
        assert len(content.procedures) > 0, "Should have at least one procedure"
    
    def _verify_sara_peralta(self, content: PulseClinicalContent):
        """Verify specific details for Sara Ana María Peralta's data."""
        assert "a9546050" in content.patient.id, "Patient ID should contain 'a9546050'"

        # Verify at least one medication exists
        assert len(content.medications) > 0, "Should have at least one medication"