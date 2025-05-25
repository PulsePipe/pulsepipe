#!/usr/bin/env python3

# Quick test to see what's required for model creation

from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.patient import PatientInfo
from pulsepipe.models.encounter import EncounterInfo
from pulsepipe.models.vital_sign import VitalSign
from pulsepipe.models.allergy import Allergy

# Test minimal models
try:
    print("Testing empty PulseClinicalContent...")
    content = PulseClinicalContent()
    print("✓ Empty PulseClinicalContent created successfully")
except Exception as e:
    print(f"✗ Empty PulseClinicalContent failed: {e}")

try:
    print("\nTesting minimal PatientInfo...")
    patient = PatientInfo()
    print("✓ Empty PatientInfo created successfully")
except Exception as e:
    print(f"✗ Empty PatientInfo failed: {e}")

try:
    print("\nTesting minimal Allergy...")
    allergy = Allergy()
    print("✓ Empty Allergy created successfully")
except Exception as e:
    print(f"✗ Empty Allergy failed: {e}")

try:
    print("\nTesting minimal VitalSign...")
    vital = VitalSign(value="120")
    print("✓ Minimal VitalSign created successfully")
except Exception as e:
    print(f"✗ Minimal VitalSign failed: {e}")

try:
    print("\nTesting PulseClinicalContent with None values...")
    content = PulseClinicalContent(patient=None, encounter=None)
    print("✓ PulseClinicalContent with None values created successfully")
except Exception as e:
    print(f"✗ PulseClinicalContent with None values failed: {e}")