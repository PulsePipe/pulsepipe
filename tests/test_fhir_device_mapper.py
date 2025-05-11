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

# tests/test_fhir_device_mapper.py

import pytest
from pulsepipe.ingesters.fhir_utils.device_mapper import DeviceMapper
from pulsepipe.models import PulseClinicalContent, MessageCache
from pulsepipe.models.device import Device, DeviceProperty

def test_device_mapper_basic():
    mapper = DeviceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Basic device example
    device_res = {
        "resourceType": "Device",
        "id": "device-001",
        "type": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "468063009",
                    "display": "Pacemaker"
                }
            ]
        },
        "status": "active",
        "manufacturer": "CardiacDevices Inc.",
        "modelNumber": "PM-2000",
        "patient": {
            "reference": "Patient/patient123"
        }
    }
    
    content = PulseClinicalContent(
        patient=None,
        encounter=None,
        vital_signs=[],
        allergies=[],
        immunizations=[],
        diagnoses=[],
        problem_list=[],
        procedures=[],
        medications=[],
        payors=[],
        mar=[],
        notes=[],
        imaging=[],
        lab=[],
        pathology=[],
        diagnostic_test=[],
        microbiology=[],
        blood_bank=[],
        family_history=[],
        social_history=[],
        advance_directives=[],
        functional_status=[],
        order=[],
        implant=[],
        devices=[]
    )
    
    mapper.map(device_res, content, cache)
    
    assert len(content.devices) == 1
    device = content.devices[0]
    assert device.device_id == "device-001"
    assert device.type == "Pacemaker"
    assert device.manufacturer == "CardiacDevices Inc."
    assert device.model == "PM-2000"
    assert device.status == "active"
    assert device.patient_id == "patient123"
    assert device.encounter_id == "encounter456"

def test_device_mapper_with_identifiers():
    mapper = DeviceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Device with serial and lot numbers
    device_res = {
        "resourceType": "Device",
        "id": "device-002",
        "type": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "58938008",
                    "display": "Insulin Pump"
                }
            ]
        },
        "status": "active",
        "identifier": [
            {
                "type": {
                    "coding": [
                        {
                            "code": "SNO"
                        }
                    ]
                },
                "value": "SN12345"
            },
            {
                "type": {
                    "coding": [
                        {
                            "code": "LOT"
                        }
                    ]
                },
                "value": "LOT98765"
            }
        ],
        "expirationDate": "2028-01-15",
        "deviceName": [
            {
                "name": "InsulinPro X3"
            }
        ],
        "version": [
            {
                "value": "3.2.1"
            }
        ],
        "note": [
            {
                "text": "Monthly battery check required"
            }
        ]
    }
    
    content = PulseClinicalContent(
        patient=None,
        encounter=None,
        vital_signs=[],
        allergies=[],
        immunizations=[],
        diagnoses=[],
        problem_list=[],
        procedures=[],
        medications=[],
        payors=[],
        mar=[],
        notes=[],
        imaging=[],
        lab=[],
        pathology=[],
        diagnostic_test=[],
        microbiology=[],
        blood_bank=[],
        family_history=[],
        social_history=[],
        advance_directives=[],
        functional_status=[],
        order=[],
        implant=[],
        devices=[]
    )
    
    mapper.map(device_res, content, cache)
    
    assert len(content.devices) == 1
    device = content.devices[0]
    assert device.device_id == "device-002"
    assert device.type == "Insulin Pump"
    assert device.serial_number == "SN12345"
    assert device.lot_number == "LOT98765"
    assert device.expiration_date == "2028-01-15"
    assert device.name == "InsulinPro X3"
    assert device.version == "3.2.1"
    assert device.safety_info == "Monthly battery check required"

def test_device_mapper_with_properties():
    mapper = DeviceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Device with properties in different formats
    device_res = {
        "resourceType": "Device",
        "id": "device-003",
        "type": {
            "coding": [
                {
                    "system": "http://snomed.info/sct",
                    "code": "706689003",
                    "display": "Ventilator"
                }
            ]
        },
        "status": "active",
        "property": [
            {
                "type": {
                    "coding": [
                        {
                            "code": "FLOW",
                            "display": "Flow Rate"
                        }
                    ]
                },
                "valueQuantity": {
                    "value": 15,
                    "unit": "L/min"
                }
            },
            {
                "type": {
                    "coding": [
                        {
                            "code": "MODE",
                            "display": "Operation Mode"
                        }
                    ]
                },
                "valueCode": {
                    "coding": [
                        {
                            "code": "CPAP",
                            "display": "Continuous Positive Airway Pressure"
                        }
                    ]
                }
            },
            {
                "type": {
                    "coding": [
                        {
                            "code": "NOTES",
                            "display": "Configuration Notes"
                        }
                    ]
                },
                "valueString": "Patient-specific settings applied"
            }
        ]
    }
    
    content = PulseClinicalContent(
        patient=None,
        encounter=None,
        vital_signs=[],
        allergies=[],
        immunizations=[],
        diagnoses=[],
        problem_list=[],
        procedures=[],
        medications=[],
        payors=[],
        mar=[],
        notes=[],
        imaging=[],
        lab=[],
        pathology=[],
        diagnostic_test=[],
        microbiology=[],
        blood_bank=[],
        family_history=[],
        social_history=[],
        advance_directives=[],
        functional_status=[],
        order=[],
        implant=[],
        devices=[]
    )
    
    mapper.map(device_res, content, cache)
    
    assert len(content.devices) == 1
    device = content.devices[0]
    assert device.device_id == "device-003"
    assert device.type == "Ventilator"
    
    # Check properties
    assert len(device.properties) == 3
    
    # Check quantitative property
    flow_property = next((p for p in device.properties if p.property_type == "Flow Rate"), None)
    assert flow_property is not None
    assert flow_property.property_value == "15"
    assert flow_property.property_unit == "L/min"
    
    # Check coded property
    mode_property = next((p for p in device.properties if p.property_type == "Operation Mode"), None)
    assert mode_property is not None
    assert mode_property.property_value == "Continuous Positive Airway Pressure"
    
    # Check string property
    notes_property = next((p for p in device.properties if p.property_type == "Configuration Notes"), None)
    assert notes_property is not None
    assert notes_property.property_value == "Patient-specific settings applied"

def test_device_mapper_fallback_to_cache():
    mapper = DeviceMapper()
    
    cache: MessageCache = {
        "patient_id": "patient123",
        "encounter_id": "encounter456",
        "order_id": None,
        "resource_index": {},
    }
    
    # Device with no explicit patient reference
    device_res = {
        "resourceType": "Device",
        "id": "device-004",
        "type": {
            "coding": [
                {
                    "code": "706689003" 
                }
            ]
        },
        "status": "active"
    }
    
    content = PulseClinicalContent(
        patient=None,
        encounter=None,
        vital_signs=[],
        allergies=[],
        immunizations=[],
        diagnoses=[],
        problem_list=[],
        procedures=[],
        medications=[],
        payors=[],
        mar=[],
        notes=[],
        imaging=[],
        lab=[],
        pathology=[],
        diagnostic_test=[],
        microbiology=[],
        blood_bank=[],
        family_history=[],
        social_history=[],
        advance_directives=[],
        functional_status=[],
        order=[],
        implant=[],
        devices=[]
    )
    
    mapper.map(device_res, content, cache)
    
    assert len(content.devices) == 1
    device = content.devices[0]
    assert device.device_id == "device-004"
    assert device.type == "706689003"  # Falls back to code when display not available
    assert device.patient_id == "patient123"  # Should get this from cache
    assert device.encounter_id == "encounter456"  # Should get this from cache