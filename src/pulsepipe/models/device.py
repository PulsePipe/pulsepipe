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

# src/pulsepipe/models/device.py

from typing import Optional, List
from pydantic import BaseModel


class DeviceProperty(BaseModel):
    """
    Represents a specific property or attribute of a healthcare device.
    
    Devices often have various configurable properties or measurements that
    describe their capabilities, settings, and operating parameters. This model
    captures these device-specific attributes to provide detailed device information.
    """
    property_type: Optional[str]    # Name/type of property
    property_value: Optional[str]   # Value of the property
    property_unit: Optional[str]    # Unit of measurement if applicable


class Device(BaseModel):
    """
    Represents a medical device used for patient care or monitoring.
    
    Medical devices encompass a wide range of equipment used in healthcare,
    including implantable devices (pacemakers, stents), durable medical equipment
    (ventilators, infusion pumps), and digital health technologies (continuous glucose
    monitors, remote monitoring devices).
    
    This model captures key details about the device, its status, and its association
    with patients or clinical encounters.
    """
    device_id: Optional[str]          # Unique identifier for the device
    type: Optional[str]               # Type of device
    manufacturer: Optional[str]       # Device manufacturer
    model: Optional[str]              # Model name/number
    serial_number: Optional[str]      # Serial number (may be partially masked)
    lot_number: Optional[str]         # Manufacturing lot number
    expiration_date: Optional[str]    # Date device expires if applicable
    status: Optional[str]             # Active, inactive, entered-in-error
    name: Optional[str]               # Human-friendly name
    version: Optional[str]            # Software/firmware version if applicable
    safety_info: Optional[str]        # Safety notices, recalls
    properties: List[DeviceProperty] = []  # Device-specific properties
    patient_id: Optional[str]         # Patient association if applicable
    encounter_id: Optional[str]       # Encounter association if applicable