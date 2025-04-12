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

# src/pulsepipe/config/errors.py

"""
Standardized error handling for PulsePipe.

This module defines a hierarchy of exception types used throughout the PulsePipe
system to provide consistent error handling and reporting.
"""

class PulsePipeError(Exception):
    """Base exception for all PulsePipe errors."""
    
    def __init__(self, message, details=None, cause=None):
        """
        Initialize a PulsePipe error.
        
        Args:
            message (str): Human-readable error message
            details (dict, optional): Additional error context details
            cause (Exception, optional): Original exception that caused this error
        """
        self.message = message
        self.details = details or {}
        self.cause = cause
        
        # Build the full error message
        full_message = message
        if cause:
            full_message += f" | Caused by: {str(cause)}"
        
        super().__init__(full_message)


# === Data Ingestion Errors ===

class AdapterError(PulsePipeError):
    """Error occurred in a data adapter component."""
    pass


class FileAdapterError(AdapterError):
    """Error with file-based adapters."""
    pass


class FileWatcherError(FileAdapterError):
    """Error in the FileWatcher adapter."""
    pass


class IngesterError(PulsePipeError):
    """Error occurred in a data ingester component."""
    pass


class FHIRError(IngesterError):
    """Error processing FHIR data."""
    pass


class HL7v2Error(IngesterError):
    """Error processing HL7v2 data."""
    pass


class X12Error(IngesterError):
    """Error processing X12 data."""
    pass


class CDAError(IngesterError):
    """Error processing CDA data."""
    pass


class PlainTextError(IngesterError):
    """Error processing plain text medical data."""
    pass


# === Processing Pipeline Errors ===

class PipelineError(PulsePipeError):
    """Error in the processing pipeline."""
    pass


class IngestionEngineError(PipelineError):
    """Error in the core ingestion engine."""
    pass


class NormalizationError(PipelineError):
    """Error during data normalization."""
    pass


class DeidentificationError(PipelineError):
    """Error during data de-identification."""
    pass


class ChunkerError(PipelineError):
    """Error during content chunking."""
    pass


class EmbedderError(PipelineError):
    """Error during text embedding."""
    pass


class VectorStoreError(PipelineError):
    """Error with the vector database."""
    pass


# === Validation Errors ===

class ValidationError(PulsePipeError):
    """Data validation error."""
    
    def __init__(self, message, field=None, constraints=None, value=None, details=None, cause=None):
        """
        Initialize a validation error.
        
        Args:
            message (str): Human-readable error message
            field (str, optional): Name of the field that failed validation
            constraints (dict, optional): Constraints that were violated
            value (any, optional): Value that failed validation
            details (dict, optional): Additional error details
            cause (Exception, optional): Original exception that caused this error
        """
        self.field = field
        self.constraints = constraints or {}
        self.value = value
        
        # Add validation-specific information to details
        full_details = details or {}
        if field:
            full_details["field"] = field
        if value is not None:
            full_details["value"] = str(value)
        if constraints:
            full_details["constraints"] = constraints
        
        super().__init__(message, details=full_details, cause=cause)


class SchemaValidationError(ValidationError):
    """Error validating data against a schema."""
    pass


class ContentValidationError(ValidationError):
    """Error validating healthcare content."""
    pass


# === Configuration and System Errors ===

class ConfigurationError(PulsePipeError):
    """Error in system configuration."""
    pass


class MissingConfigurationError(ConfigurationError):
    """Required configuration is missing."""
    pass


class InvalidConfigurationError(ConfigurationError):
    """Configuration is invalid or inconsistent."""
    pass


class ResourceError(PulsePipeError):
    """Error accessing a required resource."""
    pass


class DatabaseError(ResourceError):
    """Error with database operations."""
    pass


class FileSystemError(ResourceError):
    """Error with filesystem operations."""
    pass


class NetworkError(ResourceError):
    """Error with network operations."""
    pass


# === CLI Errors ===

class CLIError(PulsePipeError):
    """Error in the command-line interface."""
    pass
