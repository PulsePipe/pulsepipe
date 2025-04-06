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

# src/pulsepipe/ingesters/hl7v2_ingester.py

import logging
import traceback
import re

from hl7apy.parser import parse_segment, parse_message
from pulsepipe.models import PulseClinicalContent, MessageCache

# Import mappers to ensure registration
from .hl7v2_utils import base_mapper
from .hl7v2_utils import pid_mapper  # Core mapper
from .hl7v2_utils import obx_mapper
from .hl7v2_utils import obr_mapper

logger = logging.getLogger(__name__)

class HL7v2Ingester:
    def parse(self, raw_data: str) -> list:
        """
        Parse multiple HL7 messages from a single input string.
        
        Args:
            raw_data (str): Raw HL7 message(s)
        
        Returns:
            List of parsed PulseClinicalContent objects
        """
        if not raw_data.strip():
            raise ValueError("Empty HL7v2 data received")

        # Use a more robust regex to split messages
        # This ensures we correctly separate complete MSH segments
        messages = re.split(r'(?=MSH\|)', raw_data.strip())
        
        if not messages or len(messages) < 2:
            raise ValueError("This is not an HL7 message")
    
        # Remove any empty strings from the list
        messages = [msg.strip() for msg in messages if msg.strip()]
        
        parsed_contents = []
        for i, message in enumerate(messages, 1):
            try:
                # Ensure the message starts with MSH
                if not message.startswith('MSH'):
                    message = 'MSH' + message
                
                parsed_content = self.parseImp(message)
                parsed_contents.append(parsed_content)
            except Exception as e:
                logger.error(f"Error parsing message {i}: {e}")
                logger.debug(f"Problematic message: {message}")
                logger.debug(traceback.format_exc())
        
        return parsed_contents

    def parseImp(self, raw_data: str) -> PulseClinicalContent:
        """
        Parse a single HL7v2 message.
        
        Args:
            raw_data (str): Raw HL7 message
        
        Returns:
            PulseClinicalContent object
        """

        try:
            # Clean the version in the MSH segment and normalize line endings
            cleaned_raw_data = self._clean_hl7_version(raw_data)
            
            # Create content template
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
            )

            # Create message cache for tracking context
            cache: MessageCache = {
                "patient_id": None, 
                "encounter_id": None, 
                "current_observation_type": None,
                "current_observation_date": None,
                "resource_index": {}
            }

            # Parsing arguments
            parse_args = {
                'validation_level': 2,  # Less strict validation
                'force_validation': False
            }
            
            # Parse message details (MSH)
            message = parse_message(cleaned_raw_data, **parse_args)

            # Split message into segments for individual parsing
            segments = cleaned_raw_data.split('\r\n')

            # Parse each segment individually
            for segment_str in segments:
                segment_str = segment_str.strip()
                if not segment_str:
                    continue
                
                try:
                    # Parse segment using parse_segment
                    parse_args = {
                        'validation_level': 2,  # Less strict validation
                        'force_validation': False
                    }
                    
                    # Extract segment type for debugging
                    segment_type = segment_str.split('|')[0] if '|' in segment_str else 'UNKNOWN'
                    
                    # Parse the segment
                    segment = parse_segment(segment_str, version=message.version, encoding_chars=message.encoding_chars, validation_level=2)
                    
                    logger.debug(f"Successfully parsed segment: {segment_type}")
                    
                    # Map segment using registered mappers
                    self._map_segment(segment, content, cache)
                    
                except Exception as e:
                    logger.error(f"Error parsing segment '{segment_str}': {e}")
                    logger.debug(traceback.format_exc())
            
            logger.debug(f"Finished parsing. Patient: {content.patient}")
            return content

        except Exception as e:
            logger.exception("HL7v2 parsing error")
            raise ValueError(f"Failed to parse HL7v2 message: {str(e)}") from e

    def _clean_hl7_version(self, raw_data: str) -> str:
        """
        Clean the HL7 version field to remove any newline or unexpected characters.
        Also ensures proper line endings.
        
        Args:
            raw_data (str): Raw HL7 message
        
        Returns:
            str: Cleaned HL7 message with proper version
        """
        # Normalize line endings
        raw_data = raw_data.replace('\r\n', '\n').replace('\r', '\n')
        
        # Convert back to HL7 standard \r\n
        raw_data = raw_data.replace('\n', '\r\n')
        
        # Split the raw data into lines
        lines = raw_data.split('\r\n')
        
        # Find and process the MSH line
        for i, line in enumerate(lines):
            if line.startswith('MSH|'):
                # Split MSH components
                msh_parts = line.split('|')
                
                # Ensure we have enough parts and the version field exists
                if len(msh_parts) >= 12:
                    # Extract version, removing any newline or unexpected characters
                    version = re.sub(r'[^\d.]', '', msh_parts[11])
                    
                    # Replace the version field
                    msh_parts[11] = version
                    
                    # Reconstruct the MSH line
                    lines[i] = '|'.join(msh_parts)
                
                break
        
        # Reconstruct the cleaned message
        return '\r\n'.join(lines)

    def _map_segment(self, segment, content: PulseClinicalContent, cache: dict):
        """
        Map a single segment using registered mappers
        
        Args:
            segment: HL7 segment to map
            content: PulseClinicalContent to populate
            cache: Shared context dictionary
        """
        # Log the segment details for debugging
        logger.debug(f"Attempting to map segment: {segment.name}")
        
        # Try to find a mapper for this segment
        mapped = False
        for mapper in base_mapper.MAPPER_REGISTRY:
            try:
                if mapper.accepts(segment):
                    try:
                        mapper.map(segment, content, cache)
                        mapped = True
                        logger.debug(f"Successfully mapped segment {segment.name} using {mapper.__class__.__name__}")
                        break
                    except Exception as e:
                        logger.error(f"Error mapping segment {segment.name} with {mapper.__class__.__name__}: {e}")
                        logger.debug(traceback.format_exc())
            except Exception as e:
                logger.error(f"Error in mapper {mapper.__class__.__name__}: {e}")
                logger.debug(traceback.format_exc())
        
        # Log if no mapper found
        if not mapped:
            logger.warning(f"No mapper found for segment {segment.name}")