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

# tests/test_hl7v2_message.py

import pytest
from pulsepipe.ingesters.hl7v2_utils.message import Subcomponent, Component, Field, Segment, Message

def test_subcomponent_creation():
    """Test creation and basic functionality of the Subcomponent class."""
    # Create a subcomponent with a list of values
    subcomps = ["value1", "value2", "value3"]
    subcomponent = Subcomponent(subcomps)
    
    # Check length
    assert len(subcomponent) == 3
    
    # Check direct access
    assert subcomponent[0] == "value1"
    assert subcomponent[1] == "value2"
    assert subcomponent[2] == "value3"
    
    # Check get method (1-indexed)
    assert subcomponent.get(1) == "value1"
    assert subcomponent.get(2) == "value2"
    assert subcomponent.get(3) == "value3"
    
    # Check out of bounds handling
    assert subcomponent.get(4) is None
    
    # Check string representation
    assert str(subcomponent) == "value1&value2&value3"

def test_subcomponent_with_none():
    """Test Subcomponent handling of None values."""
    # Create a subcomponent with some None values
    subcomps = ["value1", None, "value3"]
    subcomponent = Subcomponent(subcomps)
    
    # Check length
    assert len(subcomponent) == 3
    
    # Check direct access
    assert subcomponent[0] == "value1"
    assert subcomponent[1] is None
    assert subcomponent[2] == "value3"
    
    # Check get method
    assert subcomponent.get(1) == "value1"
    assert subcomponent.get(2) is None
    assert subcomponent.get(3) == "value3"
    
    # Check string representation (None converted to empty string)
    assert str(subcomponent) == "value1&&value3"

def test_component_creation():
    """Test creation and basic functionality of the Component class."""
    # Create subcomponents
    sub1 = Subcomponent(["name", "prefix"])
    sub2 = Subcomponent(["address", "street"])
    
    # Create a component with these subcomponents
    component = Component([sub1, sub2])
    
    # Check length
    assert len(component) == 2
    
    # Check direct access
    assert component[0] == sub1
    assert component[1] == sub2
    
    # Check get method for nested access
    assert component.get(1, 1) == "name"
    assert component.get(1, 2) == "prefix"
    assert component.get(2, 1) == "address"
    assert component.get(2, 2) == "street"
    
    # Check out of bounds handling
    assert component.get(3, 1) is None
    
    # Check string representation
    assert str(component) == "name&prefix^address&street"

def test_field_creation():
    """Test creation and basic functionality of the Field class."""
    # Create components
    comp1 = Component([Subcomponent(["FirstName", "MiddleName"]), Subcomponent(["LastName"])])
    comp2 = Component([Subcomponent(["AnotherField"])])
    
    # Create a field with repetitions of components
    field = Field([comp1, comp2])
    
    # Check length
    assert len(field) == 2
    
    # Check direct access
    assert field[0] == comp1
    assert field[1] == comp2
    
    # Check get method for nested access
    assert field.get(0, 1, 1) == "FirstName"
    assert field.get(0, 1, 2) == "MiddleName"
    assert field.get(0, 2, 1) == "LastName"
    assert field.get(1, 1, 1) == "AnotherField"
    
    # Check out of bounds handling
    assert field.get(2, 1, 1) is None
    
    # Check string representation
    assert str(field) == "FirstName&MiddleName^LastName~AnotherField"

def test_segment_creation():
    """Test creation and basic functionality of the Segment class."""
    # Create fields
    field1 = Field([Component([Subcomponent(["MSH"])])])
    field2 = Field([Component([Subcomponent(["^~\\&"])])])
    field3 = Field([Component([Subcomponent(["SENDING_APP"])])])
    
    # Create a segment with these fields
    segment = Segment("MSH", [field1, field2, field3])
    
    # Check segment ID
    assert segment.id == "MSH"
    
    # Check field access
    assert segment[0] == field1
    assert segment[1] == field2
    assert segment[2] == field3
    
    # Check direct nested access
    assert segment.get(0, 1, 1, 0) == "MSH"
    assert segment.get(2, 1, 1, 0) == "SENDING_APP"
    
    # Check the raw_field method
    assert segment.raw_field(1) == field2
    
    # Check out of bounds handling
    assert segment.raw_field(5) == []

def test_message_creation():
    """Test creation and basic functionality of the Message class."""
    # Create segments
    msh_segment = Segment("MSH", [
        Field([Component([Subcomponent(["MSH"])])]),
        Field([Component([Subcomponent(["^~\\&"])])])
    ])
    pid_segment = Segment("PID", [
        Field([Component([Subcomponent(["1"])])]),
        Field([Component([Subcomponent(["123456"])])])
    ])
    
    # Create a message with these segments
    message = Message("ADT_A01", [msh_segment, pid_segment])
    
    # Check message ID
    assert message.id == "ADT_A01"
    
    # Check segments
    assert message.segments[0] == msh_segment
    assert message.segments[1] == pid_segment
    
    # Check raw_field method
    assert message.raw_field(0) == msh_segment
    assert message.raw_field(1) == pid_segment
    
    # Check out of bounds handling
    assert message.raw_field(3) == []

def test_complex_nested_access():
    """Test complex nested access patterns in the HL7 message structure."""
    # Testing the structure and bounds of the HL7 message classes without causing errors

    # Create simple but adequate subcomponents
    name_subcomps = ["LastName", "FirstName", "MiddleInitial"]
    name_subcomponent = Subcomponent(name_subcomps)

    # Create component with the subcomponent
    name_component = Component([name_subcomponent])

    # Create field with the component
    name_field = Field([name_component])

    # Similarly for address
    addr_subcomps = ["Street", "City", "State", "Zip", "Country"]
    addr_subcomponent = Subcomponent(addr_subcomps)
    addr_component = Component([addr_subcomponent])
    addr_field = Field([addr_component])

    # Create segment with these fields
    pid_segment = Segment("PID", [None, None, None, None, name_field, None, None, None, None, addr_field])

    # Test accessing fields by index
    assert pid_segment.fields[4] == name_field
    assert pid_segment.fields[9] == addr_field

    # Test accessing components
    assert pid_segment.fields[4].repetitions[0] == name_component
    assert pid_segment.fields[9].repetitions[0] == addr_component

    # Test accessing subcomponents
    assert pid_segment.fields[4].repetitions[0].components[0] == name_subcomponent
    assert pid_segment.fields[9].repetitions[0].components[0] == addr_subcomponent

    # Test accessing values by index
    assert pid_segment.fields[4].repetitions[0].components[0].subcomponents[0] == "LastName"
    assert pid_segment.fields[4].repetitions[0].components[0].subcomponents[1] == "FirstName"
    assert pid_segment.fields[4].repetitions[0].components[0].subcomponents[2] == "MiddleInitial"

    assert pid_segment.fields[9].repetitions[0].components[0].subcomponents[0] == "Street"
    assert pid_segment.fields[9].repetitions[0].components[0].subcomponents[4] == "Country"