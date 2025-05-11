# ------------------------------------------------------------------------------
# PulsePipe — Ingest, Normalize, De-ID, Chunk, Embed. Healthcare Data, AI-Ready with RAG.
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

# tests/test_supply_delivery_model.py

import pytest
from pydantic import ValidationError
from pulsepipe.models.supply_delivery import SupplyDelivery, SupplyDeliveryItem

def test_supply_delivery_item_creation():
    """Test creating a basic SupplyDeliveryItem"""
    
    item = SupplyDeliveryItem(
        item_code="A4253",
        item_name="Blood glucose test strips",
        coding_method="HCPCS",
        quantity="100",
        quantity_unit="strips",
        lot_number="LOT12345",
        expiration_date="2026-01-31"
    )
    
    assert item.item_code == "A4253"
    assert item.item_name == "Blood glucose test strips"
    assert item.coding_method == "HCPCS"
    assert item.quantity == "100"
    assert item.quantity_unit == "strips"
    assert item.lot_number == "LOT12345"
    assert item.expiration_date == "2026-01-31"

def test_supply_delivery_item_optional_fields():
    """Test that all fields in SupplyDeliveryItem are optional"""

    # Create with only some fields
    item = SupplyDeliveryItem(
        item_code="E0260",
        item_name="Hospital bed, semi-electric",
        coding_method=None,
        quantity=None,
        quantity_unit=None,
        lot_number=None,
        expiration_date=None
    )

    assert item.item_code == "E0260"
    assert item.item_name == "Hospital bed, semi-electric"
    assert item.coding_method is None
    assert item.quantity is None
    assert item.quantity_unit is None
    assert item.lot_number is None
    assert item.expiration_date is None

    # Create with no fields
    empty_item = SupplyDeliveryItem(
        item_code=None,
        item_name=None,
        coding_method=None,
        quantity=None,
        quantity_unit=None,
        lot_number=None,
        expiration_date=None
    )
    assert empty_item.item_code is None
    assert empty_item.item_name is None

def test_supply_delivery_creation():
    """Test creating a SupplyDelivery with items"""

    # Create items
    item1 = SupplyDeliveryItem(
        item_code="A4253",
        item_name="Blood glucose test strips",
        quantity="100",
        quantity_unit=None,
        coding_method=None,
        lot_number=None,
        expiration_date=None
    )

    item2 = SupplyDeliveryItem(
        item_code="A4233",
        item_name="Blood glucose test or calibrator",
        quantity="2",
        quantity_unit=None,
        coding_method=None,
        lot_number=None,
        expiration_date=None
    )

    # Create supply delivery
    delivery = SupplyDelivery(
        delivery_id="sd12345",
        status="completed",
        delivery_type="medical-supplies",
        delivered_on="2025-05-01T10:15:00Z",
        destination="Patient home",
        supplier="ABC Medical Supplies",
        items=[item1, item2],
        notes="Delivered to patient's spouse",
        patient_id="patient123",
        encounter_id="encounter456"
    )
    
    assert delivery.delivery_id == "sd12345"
    assert delivery.status == "completed"
    assert delivery.delivery_type == "medical-supplies"
    assert delivery.delivered_on == "2025-05-01T10:15:00Z"
    assert delivery.destination == "Patient home"
    assert delivery.supplier == "ABC Medical Supplies"
    assert len(delivery.items) == 2
    assert delivery.notes == "Delivered to patient's spouse"
    assert delivery.patient_id == "patient123"
    assert delivery.encounter_id == "encounter456"
    
    # Check that items were added correctly
    assert delivery.items[0].item_code == "A4253"
    assert delivery.items[1].item_code == "A4233"

def test_supply_delivery_empty_items():
    """Test that items defaults to an empty list"""

    delivery = SupplyDelivery(
        delivery_id="sd12345",
        status="completed",
        delivery_type=None,
        delivered_on=None,
        destination=None,
        supplier=None,
        notes=None,
        patient_id=None,
        encounter_id=None
    )

    assert delivery.items == []

def test_supply_delivery_optional_fields():
    """Test that all fields in SupplyDelivery are optional except items"""

    # Create with minimal fields
    delivery = SupplyDelivery(
        delivery_id="sd12345",
        status=None,
        delivery_type=None,
        delivered_on=None,
        destination=None,
        supplier=None,
        notes=None,
        patient_id=None,
        encounter_id=None
    )

    assert delivery.delivery_id == "sd12345"
    assert delivery.status is None
    assert delivery.delivery_type is None
    assert delivery.delivered_on is None
    assert delivery.destination is None
    assert delivery.supplier is None
    assert delivery.notes is None
    assert delivery.patient_id is None
    assert delivery.encounter_id is None
    assert delivery.items == []

    # Create with no fields
    empty_delivery = SupplyDelivery(
        delivery_id=None,
        status=None,
        delivery_type=None,
        delivered_on=None,
        destination=None,
        supplier=None,
        notes=None,
        patient_id=None,
        encounter_id=None
    )
    assert empty_delivery.delivery_id is None
    assert empty_delivery.items == []

def test_supply_delivery_with_dict_items():
    """Test creating a SupplyDelivery with items provided as dictionaries"""

    # Create using dict items instead of model instances
    delivery = SupplyDelivery(
        delivery_id="sd12345",
        status="in-progress",
        delivery_type=None,
        delivered_on=None,
        destination=None,
        supplier=None,
        notes=None,
        patient_id=None,
        encounter_id=None,
        items=[
            SupplyDeliveryItem(
                item_code="A4253",
                item_name="Blood glucose test strips",
                quantity="100",
                quantity_unit=None,
                coding_method=None,
                lot_number=None,
                expiration_date=None
            ),
            SupplyDeliveryItem(
                item_code="A4233",
                item_name="Blood glucose test or calibrator",
                quantity="2",
                quantity_unit=None,
                coding_method=None,
                lot_number=None,
                expiration_date=None
            )
        ]
    )

    assert delivery.delivery_id == "sd12345"
    assert len(delivery.items) == 2
    assert isinstance(delivery.items[0], SupplyDeliveryItem)
    assert isinstance(delivery.items[1], SupplyDeliveryItem)
    assert delivery.items[0].item_code == "A4253"
    assert delivery.items[1].item_code == "A4233"

def test_supply_delivery_to_dict():
    """Test converting SupplyDelivery to dict"""

    delivery = SupplyDelivery(
        delivery_id="sd12345",
        status="completed",
        delivery_type="medical-supplies",
        delivered_on=None,
        destination=None,
        supplier=None,
        items=[
            SupplyDeliveryItem(
                item_code="A4253",
                item_name="Blood glucose test strips",
                quantity=None,
                quantity_unit=None,
                coding_method=None,
                lot_number=None,
                expiration_date=None
            )
        ],
        notes=None,
        patient_id="patient123",
        encounter_id=None
    )

    # Convert to dict
    delivery_dict = delivery.model_dump()  # Using model_dump() as dict() is deprecated

    assert isinstance(delivery_dict, dict)
    assert delivery_dict["delivery_id"] == "sd12345"
    assert delivery_dict["status"] == "completed"
    assert delivery_dict["delivery_type"] == "medical-supplies"
    assert delivery_dict["patient_id"] == "patient123"
    assert isinstance(delivery_dict["items"], list)
    assert len(delivery_dict["items"]) == 1
    assert delivery_dict["items"][0]["item_code"] == "A4253"
    assert delivery_dict["items"][0]["item_name"] == "Blood glucose test strips"