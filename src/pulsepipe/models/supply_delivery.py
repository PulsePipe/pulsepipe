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

# src/pulsepipe/models/supply_delivery.py

from typing import Optional, List
from pydantic import BaseModel

class SupplyDeliveryItem(BaseModel):
    """
    Represents an individual item being delivered as part of a supply delivery.
    
    This model captures details about a specific supply item, including
    its identity, quantity, and any item-specific details.
    """
    item_code: Optional[str]
    item_name: Optional[str]
    coding_method: Optional[str]
    quantity: Optional[str]
    quantity_unit: Optional[str]
    lot_number: Optional[str]
    expiration_date: Optional[str]

class SupplyDelivery(BaseModel):
    """
    Represents the delivery of supplies to a patient or healthcare facility.
    
    This model tracks the movement of medical supplies, equipment, medication,
    or other materials. Supply deliveries are a critical component of healthcare
    logistics and inventory management, providing visibility into when and where
    supplies are delivered.
    
    Supply deliveries can include consumable supplies (e.g., dressings, catheters),
    durable medical equipment (e.g., wheelchairs, oxygen concentrators), or
    implantable devices that will later be used in procedures.
    """
    delivery_id: Optional[str]
    status: Optional[str]               # in-progress, completed, abandoned
    delivery_type: Optional[str]        # medication, device, biologics, etc.
    delivered_on: Optional[str]
    destination: Optional[str]          # Location or facility where supplies were delivered
    supplier: Optional[str]             # Entity providing the supplies
    items: List[SupplyDeliveryItem] = []
    notes: Optional[str]
    patient_id: Optional[str]
    encounter_id: Optional[str]