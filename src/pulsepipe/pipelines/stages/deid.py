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

# src/pulsepipe/pipelines/stages/deid.py

"""
De-identification stage for PulsePipe pipeline.

Includes support for Microsoft Presidio for advanced PII/PHI detection using
NLP techniques, with fallback to traditional regex-based redaction.

Implements the HIPAA Safe Harbor method with configurable options.
"""

import re
import copy
import uuid
from typing import Any, Dict, List, Optional, Union, Tuple
from datetime import datetime, date

from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
from presidio_anonymizer import AnonymizerEngine
from presidio_analyzer.nlp_engine import SpacyNlpEngine
from pulsepipe.utils.errors import DeidentificationError, ConfigurationError
from pulsepipe.pipelines.context import PipelineContext
from pulsepipe.pipelines.stages import PipelineStage
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.operational_content import PulseOperationalContent
from pulsepipe.pipelines.deid.config import (
    DEFAULT_SALT, PATIENT_ID_HASH_LENGTH, MRN_HASH_LENGTH, 
    GENERAL_ID_HASH_LENGTH, ACCOUNT_HASH_LENGTH, REDACTION_MARKERS
)

class DeidentificationStage(PipelineStage):
    """
    Pipeline stage that performs de-identification of healthcare data.
    
    This stage implements HIPAA Safe Harbor method for de-identification:
    - Removing names
    - Truncating dates to year
    - Removing specific identifiers (MRNs, SSNs, etc.)
    - Generalizing geographic information
    - And more based on the 18 HIPAA identifiers
    
    Configuration options allow for customization of the de-identification process.
    """
    
    def __init__(self):
        """Initialize the de-identification stage."""
        super().__init__("deid")

        nlp_engine = SpacyNlpEngine(models=[{"lang_code": "en", "model_name": "en_core_web_lg"}])
        nlp_engine.load()

        # Create a clean registry for just English
        registry = RecognizerRegistry()
        registry.load_predefined_recognizers()

        self.analyzer = AnalyzerEngine(registry=registry, nlp_engine=nlp_engine, supported_languages=["en"])
        self.anonymizer = AnonymizerEngine()
        
        # These are the default PHI type handlers
        self.phi_handlers = {
            "names": self._redact_names,
            "dates": self._handle_dates,
            "ids": self._handle_identifiers,
            "geographic": self._handle_geographic_data,
            "contact": self._handle_contact_info,
            "biometrics": self._handle_biometric_identifiers,
            "accounts": self._handle_account_numbers
        }
        
        # Common PHI regular expressions
        self.phi_regex = {
            # Medical record numbers - various formats
            "mrn": re.compile(r'\b(MRN|Medical Record Number|Record Number|Chart Number)\s*:?\s*([A-Za-z0-9-]{4,14})\b', re.IGNORECASE),
            
            # Phone numbers
            "phone": re.compile(r'\b(\+\d{1,3}[-\s.]?)?\(?\d{3}\)?[-\s.]?\d{3}[-\s.]?\d{4}\b'),
            
            # Social Security Numbers (with or without dashes)
            "ssn": re.compile(r'\b\d{3}[-\s]?\d{2}[-\s]?\d{4}\b'),
            
            # Email addresses
            "email": re.compile(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'),
            
            # URLs
            "url": re.compile(r'\bhttps?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+(/[-\w%.]+)*\b'),
            
            # IP addresses
            "ip": re.compile(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b'),
            
            # Common financial account patterns 
            "account": re.compile(r'\b(?:Acct|Account)\s*(?:#|No|Number|:)?\s*[-#]?\s*(\d{4,17})\b', re.IGNORECASE),
            
            # Credit card-like patterns
            "cc": re.compile(r'\b(?:\d{4}[- ]?){3}\d{4}\b'),
            
            # License numbers (simple pattern, could be enhanced)
            "license": re.compile(r'\b[A-Z](?:\d[- ]?){6,8}[A-Z0-9]\b', re.IGNORECASE)
        }
    

    def _redact_phi_with_presidio(self, text: str) -> str:
        if not text:
            return text
        results = self.analyzer.analyze(text=text, language='en')
        return self.anonymizer.anonymize(text=text, analyzer_results=results).text


    def _redact_text(self, text: str, config: Dict[str, Any]) -> str:
        """
        Combined redaction: Presidio first (if enabled), fallback to regex.
        """
        if not text:
            return text
        try:
            if config.get("use_presidio_for_text", False):
                text = self._redact_phi_with_presidio(text)
        except Exception as e:
            self.logger.warning(f"Presidio redaction failed, using regex fallback: {str(e)}")
        return self._redact_phi_from_text(text, config)



    async def execute(self, context: PipelineContext, input_data: Any = None) -> Any:
        """
        Execute the de-identification process.
        
        Args:
            context: Pipeline execution context
            input_data: Data to de-identify (from previous stage)
            
        Returns:
            De-identified data
            
        Raises:
            DeidentificationError: If de-identification fails
            ConfigurationError: If de-identification configuration is invalid
        """
        # Get deid configuration
        config = self.get_stage_config(context)
        if not config:
            self.logger.warning(f"{context.log_prefix} Deid stage is enabled but no configuration provided, using defaults")
            config = {
                "method": "safe_harbor",
                "keep_year": True,
                "geographic_precision": "state",
                "over_90_handling": "flag",
                "patient_id_strategy": "hash"
            }
        
        # Check if we have input data (from previous stage or from context)
        if input_data is None:
            # Try to get data from context
            input_data = context.ingested_data
            
            self.logger.info(f"{context.log_prefix} Using data from ingestion stage")
            
            if input_data is None:
                self.logger.error(f"{context.log_prefix} No input data available for de-identification!")
                raise DeidentificationError(
                    "No input data available for de-identification",
                    details={
                        "pipeline": context.name,
                        "executed_stages": context.executed_stages
                    }
                )
        
        self.logger.info(f"{context.log_prefix} Starting de-identification process")
        
        # Process input based on data type
        try:
            if isinstance(input_data, list):
                # Handle a batch of items
                self.logger.info(f"{context.log_prefix} Processing batch of {len(input_data)} items")
                
                deid_results = []
                for i, item in enumerate(input_data):
                    self.logger.info(f"{context.log_prefix} De-identifying item {i+1} of type {type(item).__name__}")
                    deid_item = self._deid_item(item, config)
                    deid_results.append(deid_item)
                    
                self.logger.info(f"{context.log_prefix} Completed de-identification of {len(deid_results)} items")
                return deid_results
            else:
                # Handle a single item
                self.logger.info(f"{context.log_prefix} De-identifying single item of type {type(input_data).__name__}")
                result = self._deid_item(input_data, config)
                
                self.logger.info(f"{context.log_prefix} De-identification complete")
                return result
                
        except Exception as e:
            self.logger.error(f"{context.log_prefix} Error during de-identification: {str(e)}")
            raise DeidentificationError(
                f"Error during de-identification: {str(e)}",
                details={"deid_method": config.get("method", "safe_harbor")}
            )
    
    def _deid_item(self, item: Any, config: Dict[str, Any]) -> Any:
        """
        De-identify a single item based on its type.
        
        Args:
            item: Item to de-identify
            config: De-identification configuration
            
        Returns:
            De-identified item
        """
        # Make a deep copy to avoid modifying the original
        item_copy = copy.deepcopy(item)
        
        # Apply de-identification based on item type
        if isinstance(item_copy, PulseClinicalContent):
            return self._deid_clinical_content(item_copy, config)
        elif isinstance(item_copy, PulseOperationalContent):
            return self._deid_operational_content(item_copy, config)
        else:
            # For other types, just return as is with a warning
            self.logger.warning(f"Unsupported item type for de-identification: {type(item_copy).__name__}")
            return item_copy
    
    def _deid_clinical_content(self, content: PulseClinicalContent, config: Dict[str, Any]) -> PulseClinicalContent:
        """
        De-identify clinical content by handling each component.
        
        Args:
            content: Clinical content to de-identify
            config: De-identification configuration
            
        Returns:
            De-identified clinical content
        """
        # Keep track of original-to-deid ID mappings for consistency
        id_mapping = {}
        
        # Process patient information first
        if content.patient:
            content.patient = self._deid_patient(content.patient, config, id_mapping)
        
        # Process encounter information
        if content.encounter:
            content.encounter = self._deid_encounter(content.encounter, config, id_mapping)
        
        # Process allergies
        if content.allergies:
            content.allergies = [self._deid_allergy(a, config, id_mapping) for a in content.allergies]
        
        # Process immunizations
        if content.immunizations:
            content.immunizations = [self._deid_immunization(i, config, id_mapping) for i in content.immunizations]
        
        # Process diagnoses
        if content.diagnoses:
            content.diagnoses = [self._deid_diagnosis(d, config, id_mapping) for d in content.diagnoses]
        
        # Process problems
        if content.problem_list:
            content.problem_list = [self._deid_problem(p, config, id_mapping) for p in content.problem_list]
        
        # Process medications
        if content.medications:
            content.medications = [self._deid_medication(m, config, id_mapping) for m in content.medications]
        
        # Process labs
        if content.lab:
            content.lab = [self._deid_lab_report(l, config, id_mapping) for l in content.lab]
        
        # Process imaging reports
        if content.imaging:
            content.imaging = [self._deid_imaging_report(i, config, id_mapping) for i in content.imaging]
        
        # Process notes (need special text processing)
        if content.notes:
            content.notes = [self._deid_note(n, config, id_mapping) for n in content.notes]
        
        # Mark the content as de-identified
        content.deidentified = True
        
        return content
    
    def _deid_operational_content(self, content: PulseOperationalContent, config: Dict[str, Any]) -> PulseOperationalContent:
        """
        De-identify operational content by handling financial components.
        
        Args:
            content: Operational content to de-identify
            config: De-identification configuration
            
        Returns:
            De-identified operational content
        """
        # Keep track of original-to-deid ID mappings for consistency
        id_mapping = {}
        
        # Claims
        if content.claims:
            content.claims = [self._deid_claim(c, config, id_mapping) for c in content.claims]
        
        # Charges
        if content.charges:
            content.charges = [self._deid_charge(c, config, id_mapping) for c in content.charges]
        
        # Payments
        if content.payments:
            content.payments = [self._deid_payment(p, config, id_mapping) for p in content.payments]
        
        # Prior authorizations
        if content.prior_authorizations:
            content.prior_authorizations = [self._deid_prior_auth(a, config, id_mapping) for a in content.prior_authorizations]
        
        # Mark the content as de-identified
        content.deidentified = True
        
        return content
    
    def _deid_patient(self, patient, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify patient information."""
        # Use the handler dictionary to apply appropriate transformations
        patient = self._handle_dates(patient, config)
        patient = self._handle_identifiers(patient, config, id_mapping)
        patient = self._handle_geographic_data(patient, config)
        
        # Apply specific handling for patient IDs based on configuration
        patient_id_strategy = config.get("patient_id_strategy", "hash")
        original_id = getattr(patient, "id", None)
        
        if original_id:
            if patient_id_strategy == "hash":
                # Create a deterministic hash of the ID
                import hashlib
                # Get salt from configuration or use default
                salt = config.get("id_salt", DEFAULT_SALT)
                hashed_id = hashlib.sha256((original_id + salt).encode()).hexdigest()[:16]
                patient.id = f"DEID_{hashed_id}"
            elif patient_id_strategy == "random":
                # Generate a random UUID
                patient.id = f"DEID_{str(uuid.uuid4())[:8]}"
            elif patient_id_strategy == "prefix":
                # Just add a prefix
                patient.id = f"DEID_{original_id}"
            
            # Update the ID mapping
            id_mapping[original_id] = patient.id
        
        return patient
    
    def _deid_encounter(self, encounter, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify encounter information."""
        # Handle dates
        encounter = self._handle_dates(encounter, config)
        
        # Handle patient ID references
        if hasattr(encounter, "patient_id") and encounter.patient_id in id_mapping:
            encounter.patient_id = id_mapping[encounter.patient_id]
        
        # Handle provider information
        if hasattr(encounter, "providers") and encounter.providers:
            for provider in encounter.providers:
                # Redact specific provider identifiers
                if hasattr(provider, "id"):
                    provider.id = f"DEID_PROV_{str(uuid.uuid4())[:8]}"
                
                # Generalize provider names
                if hasattr(provider, "name"):
                    provider.name = f"Provider-{str(uuid.uuid4())[:4]}"
        
        return encounter
    
    def _deid_allergy(self, allergy, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify allergy information."""
        # Patient ID reference handling
        if hasattr(allergy, "patient_id") and allergy.patient_id in id_mapping:
            allergy.patient_id = id_mapping[allergy.patient_id]
        
        # Handle dates
        allergy = self._handle_dates(allergy, config)
        
        return allergy
    
    def _deid_immunization(self, immunization, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify immunization information."""
        # Patient ID reference handling
        if hasattr(immunization, "patient_id") and immunization.patient_id in id_mapping:
            immunization.patient_id = id_mapping[immunization.patient_id]
        
        # Handle dates
        immunization = self._handle_dates(immunization, config)
        
        # Handle lot number which could be identifying
        if hasattr(immunization, "lot_number") and immunization.lot_number:
            immunization.lot_number = f"LOT-{str(uuid.uuid4())[:6]}"
        
        return immunization
    
    def _deid_diagnosis(self, diagnosis, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify diagnosis information."""
        # Patient ID reference handling
        if hasattr(diagnosis, "patient_id") and diagnosis.patient_id in id_mapping:
            diagnosis.patient_id = id_mapping[diagnosis.patient_id]
        
        # Handle dates
        diagnosis = self._handle_dates(diagnosis, config)
        
        return diagnosis
    
    def _deid_problem(self, problem, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify problem information."""
        # Patient ID reference handling
        if hasattr(problem, "patient_id") and problem.patient_id in id_mapping:
            problem.patient_id = id_mapping[problem.patient_id]
        
        # Handle dates
        problem = self._handle_dates(problem, config)
        
        return problem
    
    def _deid_medication(self, medication, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify medication information."""
        # Patient ID reference handling
        if hasattr(medication, "patient_id") and medication.patient_id in id_mapping:
            medication.patient_id = id_mapping[medication.patient_id]
        
        # Handle dates
        medication = self._handle_dates(medication, config)
        
        return medication
    
    def _deid_lab_report(self, lab_report, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify lab report information."""
        # Patient ID reference handling
        if hasattr(lab_report, "patient_id") and lab_report.patient_id in id_mapping:
            lab_report.patient_id = id_mapping[lab_report.patient_id]
        
        # Handle dates
        lab_report = self._handle_dates(lab_report, config)
        
        # Handle observations
        if hasattr(lab_report, "observations") and lab_report.observations:
            for obs in lab_report.observations:
                obs = self._handle_dates(obs, config)
                
                # Patient ID reference handling
                if hasattr(obs, "patient_id") and obs.patient_id in id_mapping:
                    obs.patient_id = id_mapping[obs.patient_id]
        
        return lab_report
    
    def _deid_imaging_report(self, imaging_report, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify imaging report information."""
        # Patient ID reference handling
        if hasattr(imaging_report, "patient_id") and imaging_report.patient_id in id_mapping:
            imaging_report.patient_id = id_mapping[imaging_report.patient_id]
        
        # Handle dates
        imaging_report = self._handle_dates(imaging_report, config)
        
        # Handle narrative text which might contain PHI
        if hasattr(imaging_report, "narrative") and imaging_report.narrative:
            imaging_report.narrative = self._redact_text(imaging_report.narrative, config)
        
        return imaging_report
    
    def _deid_note(self, note, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify clinical note information."""
        # Patient ID reference handling
        if hasattr(note, "patient_id") and note.patient_id in id_mapping:
            note.patient_id = id_mapping[note.patient_id]
        
        # Handle dates
        note = self._handle_dates(note, config)
        
        # Handle text content which contains PHI
        if hasattr(note, "text") and note.text:
            note.text = self._redact_text(note.text, config)
        
        # Handle author information
        if hasattr(note, "author_id"):
            note.author_id = f"DEID_AUTHOR_{str(uuid.uuid4())[:6]}"
        
        if hasattr(note, "author_name"):
            note.author_name = f"Provider-{str(uuid.uuid4())[:4]}"
        
        return note
    
    def _deid_claim(self, claim, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify claim information."""
        # Patient ID reference handling
        if hasattr(claim, "patient_id") and claim.patient_id in id_mapping:
            claim.patient_id = id_mapping[claim.patient_id]
        
        # Handle dates
        claim = self._handle_dates(claim, config)
        
        # Handle account numbers
        claim = self._handle_account_numbers(claim, config)
        
        return claim
    
    def _deid_charge(self, charge, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify charge information."""
        # Patient ID reference handling
        if hasattr(charge, "patient_id") and charge.patient_id in id_mapping:
            charge.patient_id = id_mapping[charge.patient_id]
        
        # Handle dates
        charge = self._handle_dates(charge, config)
        
        return charge
    
    def _deid_payment(self, payment, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify payment information."""
        # Patient ID reference handling
        if hasattr(payment, "patient_id") and payment.patient_id in id_mapping:
            payment.patient_id = id_mapping[payment.patient_id]
        
        # Handle dates
        payment = self._handle_dates(payment, config)
        
        # Handle check numbers
        if hasattr(payment, "check_number") and payment.check_number:
            payment.check_number = f"CHKNUM-{str(uuid.uuid4())[:6]}"
        
        return payment
    
    def _deid_prior_auth(self, auth, config: Dict[str, Any], id_mapping: Dict[str, str]) -> Any:
        """De-identify prior authorization information."""
        # Patient ID reference handling
        if hasattr(auth, "patient_id") and auth.patient_id in id_mapping:
            auth.patient_id = id_mapping[auth.patient_id]
        
        # Handle dates
        auth = self._handle_dates(auth, config)
        
        return auth
    
    # === PHI Handler Methods ===
    
    def _redact_names(self, obj: Any, config: Dict[str, Any]) -> Any:
        """Redact name fields from an object."""
        # This is a stub - would need NER for real implementation
        return obj
    
    def _handle_dates(self, obj: Any, config: Dict[str, Any]) -> Any:
        """
        Handle dates according to Safe Harbor method.
        
        - Keep only year or generalize dates to month/year based on config
        - Special handling for dates of individuals over 90
        - Properly handles lists of dates (such as service_dates in PriorAuthorization)
        """
        keep_year = config.get("keep_year", True)
        over_90_handling = config.get("over_90_handling", "flag")
        
        # Check for over_90 flag if present
        is_over_90 = False
        if hasattr(obj, "over_90"):
            is_over_90 = bool(obj.over_90)
        
        # Process date attributes
        # Use model.__dict__ instead of iterating through dir(obj) to avoid
        # Pydantic deprecation warnings
        for attr_name, attr_value in obj.__dict__.items():
            # Skip private and special attributes
            if attr_name.startswith('_'):
                continue
            
            # Handle lists of dates (like service_dates in PriorAuthorization)
            if isinstance(attr_value, list):
                processed_list = []
                list_modified = False
                
                for item in attr_value:
                    if isinstance(item, (datetime, date)):
                        list_modified = True
                        # Apply Safe Harbor rules
                        if is_over_90 and over_90_handling == "redact":
                            # Skip this item (will be None)
                            processed_list.append(None)
                        elif keep_year:
                            # Convert to year only
                            try:
                                year = item.year
                                # For >90 patients, we might need to modify the year
                                if is_over_90 and over_90_handling == "adjust":
                                    year = max(year, datetime.now().year - 90)
                                
                                # Create a date with just the year (Jan 1)
                                if isinstance(item, datetime):
                                    new_date = datetime(year, 1, 1, 0, 0, 0)
                                else:
                                    new_date = date(year, 1, 1)
                                
                                processed_list.append(new_date)
                            except Exception:
                                # If date manipulation fails, set to None
                                processed_list.append(None)
                        else:
                            # Completely remove the date
                            processed_list.append(None)
                    else:
                        # Keep non-date items unchanged
                        processed_list.append(item)
                
                # Only update the attribute if we actually modified date objects
                if list_modified:
                    setattr(obj, attr_name, processed_list)
                
            # Handle individual date objects
            elif isinstance(attr_value, (datetime, date)):
                # Apply Safe Harbor rules
                if is_over_90 and over_90_handling == "redact":
                    # Completely redact dates for >90 patients
                    setattr(obj, attr_name, None)
                elif keep_year:
                    # Convert to year only
                    try:
                        year = attr_value.year
                        # For >90 patients, we might need to modify the year
                        if is_over_90 and over_90_handling == "adjust":
                            year = max(year, datetime.now().year - 90)
                        
                        # Create a date with just the year (Jan 1)
                        if isinstance(attr_value, datetime):
                            new_date = datetime(year, 1, 1, 0, 0, 0)
                        else:
                            new_date = date(year, 1, 1)
                        
                        setattr(obj, attr_name, new_date)
                    except Exception:
                        # If date manipulation fails, set to None
                        setattr(obj, attr_name, None)
                else:
                    # Completely remove the date
                    setattr(obj, attr_name, None)
            
            # Handle date strings
            elif isinstance(attr_value, str) and ("date" in attr_name.lower() or 
                                               "time" in attr_name.lower() or
                                               "birth" in attr_name.lower() or
                                               "admit" in attr_name.lower() or
                                               "discharge" in attr_name.lower()):
                try:
                    # Try to parse the date string
                    formats = [
                        "%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%d/%m/%Y",
                        "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                        "%Y%m%d"
                    ]
                    
                    parsed_date = None
                    for fmt in formats:
                        try:
                            parsed_date = datetime.strptime(attr_value, fmt)
                            break
                        except ValueError:
                            continue
                    
                    if parsed_date:
                        # Apply same rules as above
                        if is_over_90 and over_90_handling == "redact":
                            setattr(obj, attr_name, None)
                        elif keep_year:
                            year = parsed_date.year
                            if is_over_90 and over_90_handling == "adjust":
                                year = max(year, datetime.now().year - 90)
                                
                            setattr(obj, attr_name, f"{year}")
                        else:
                            setattr(obj, attr_name, None)
                except Exception:
                    # If parsing fails, apply text-based approach
                    if is_over_90 and over_90_handling == "redact":
                        setattr(obj, attr_name, None)
                    else:
                        # Try to extract and keep only the year
                        year_pattern = re.compile(r'\b(19|20)\d{2}\b')
                        year_match = year_pattern.search(attr_value)
                        
                        if year_match and keep_year:
                            year = year_match.group(0)
                            if is_over_90 and over_90_handling == "adjust":
                                year_int = int(year)
                                year = str(max(year_int, datetime.now().year - 90))
                                
                            setattr(obj, attr_name, year)
                        else:
                            setattr(obj, attr_name, None)
        
        return obj
    
    def _handle_identifiers(self, obj: Any, config: Dict[str, Any], id_mapping: Dict[str, str] = None) -> Any:
        """
        Handle identifiers like MRNs, SSNs, etc.
        
        Uses deterministic hashing to ensure consistency across different runs,
        allowing for traceability when needed.
        """
        if id_mapping is None:
            id_mapping = {}
        
        # Get the salt value from config or use the default from the config file
        salt = config.get("id_salt", DEFAULT_SALT)
        
        # Process attributes looking for identifier fields
        # Use model.__dict__ instead of iterating through dir(obj) to avoid
        # Pydantic deprecation warnings
        for attr_name, attr_value in obj.__dict__.items():
            # Skip private and special attributes
            if attr_name.startswith('_'):
                continue
            
            # Handle dictionaries (like identifiers dict)
            if isinstance(attr_value, dict) and "identifiers" in attr_name:
                # Create a new filtered dict
                new_dict = {}
                
                # Only keep allowed identifiers
                for k, v in attr_value.items():
                    if k.lower() in ["mrn", "ssn", "drivers_license", "passport", "phone", "email"]:
                        # These are PHI and should be removed or pseudonymized
                        if k.lower() == "mrn" and config.get("keep_mrn_hash", True):
                            # Create deterministic hash for MRN
                            import hashlib
                            hashed = hashlib.sha256((str(v) + salt).encode()).hexdigest()[:MRN_HASH_LENGTH]
                            new_dict["mrn_hash"] = f"DEID_{hashed}"
                        # Otherwise skip this identifier
                    else:
                        # Keep non-PHI identifiers but still hash if they contain sensitive info
                        import hashlib
                        hashed = hashlib.sha256((str(v) + salt).encode()).hexdigest()[:GENERAL_ID_HASH_LENGTH]
                        new_dict[k] = f"DEID_{hashed}"
                
                setattr(obj, attr_name, new_dict)
            
            # Handle string fields that appear to be identifiers
            elif isinstance(attr_value, str):
                # Check if it's a common identifier field
                if "mrn" in attr_name.lower():
                    # Use deterministic hashing for MRN
                    import hashlib
                    hashed = hashlib.sha256((attr_value + salt).encode()).hexdigest()[:MRN_HASH_LENGTH]
                    setattr(obj, attr_name, f"DEID_MRN_{hashed}")
                    
                    # Store in the mapping for reference
                    id_mapping[attr_value] = f"DEID_MRN_{hashed}"
                    
                elif "ssn" in attr_name.lower() or "license" in attr_name.lower():
                    # Use deterministic hashing for other identifiers
                    import hashlib
                    prefix = "SSN" if "ssn" in attr_name.lower() else "LIC"
                    hashed = hashlib.sha256((attr_value + salt).encode()).hexdigest()[:GENERAL_ID_HASH_LENGTH]
                    setattr(obj, attr_name, f"DEID_{prefix}_{hashed}")
                
                # Check for ID fields that are references
                elif attr_name.endswith("_id") and attr_value in id_mapping:
                    # Use consistent mapping
                    setattr(obj, attr_name, id_mapping[attr_value])
                elif attr_name.endswith("_id") and attr_value:
                    # Create new mapping for this ID
                    import hashlib
                    hashed = hashlib.sha256((attr_value + salt).encode()).hexdigest()[:GENERAL_ID_HASH_LENGTH]
                    deid_value = f"DEID_ID_{hashed}"
                    id_mapping[attr_value] = deid_value
                    setattr(obj, attr_name, deid_value)
        
        return obj
    
    def _handle_geographic_data(self, obj: Any, config: Dict[str, Any]) -> Any:
        """
        Handle geographic information.
        
        - Precision level: none, country, state, or city
        - Default is state-level (HIPAA-compliant)
        """
        precision = config.get("geographic_precision", "state")
        
        # Look for geographic attributes
        if hasattr(obj, "geographic_area") and obj.geographic_area:
            if precision == "none":
                obj.geographic_area = None
            elif precision == "country":
                # Extract just the country if possible
                parts = obj.geographic_area.split(",")
                if len(parts) > 1:
                    obj.geographic_area = parts[-1].strip()
                else:
                    # If can't identify country, use USA as default
                    obj.geographic_area = "USA"
            elif precision == "state":
                # Keep state if available, otherwise keep as is
                parts = obj.geographic_area.split(",")
                if len(parts) > 1:
                    # Try to extract state
                    state_part = parts[-2].strip() if len(parts) > 2 else parts[-1].strip()
                    if len(state_part) <= 3:  # State abbreviation
                        obj.geographic_area = state_part
                    else:
                        obj.geographic_area = state_part
            # If precision is city or higher, leave as is
        
        # Check address fields
        address_fields = ["address", "street", "city", "zip", "zipcode", "postal"]
        for attr_name in dir(obj):
            if any(addr_field in attr_name.lower() for addr_field in address_fields):
                # Address field found, handle based on precision
                if precision in ["none", "country", "state"]:
                    # Remove specific address
                    setattr(obj, attr_name, None)
        
        return obj
    
    def _handle_contact_info(self, obj: Any, config: Dict[str, Any]) -> Any:
        """Handle contact information like phone, email, etc."""
        # Look for common contact info attributes
        contact_fields = ["phone", "fax", "email", "contact"]
        
        for attr_name, attr_value in obj.__dict__.items():
            # Skip private and special attributes
            if attr_name.startswith('_'):
                continue
            
            # Check if attribute contains contact info
            if any(field in attr_name.lower() for field in contact_fields):
                if isinstance(attr_value, str):
                    # Redact the contact information
                    setattr(obj, attr_name, None)
        
        return obj
    
    def _handle_biometric_identifiers(self, obj: Any, config: Dict[str, Any]) -> Any:
        """Handle biometric identifiers."""
        # Look for biometric fields
        biometric_fields = ["biometric", "fingerprint", "iris", "retinal", "voice", "dna"]
        
        for attr_name, attr_value in obj.__dict__.items():
            # Skip private and special attributes
            if attr_name.startswith('_'):
                continue
                
            # Check if attribute contains biometric data
            if any(field in attr_name.lower() for field in biometric_fields):
                # Remove biometric data
                setattr(obj, attr_name, None)
        
        return obj
    
    def _handle_account_numbers(self, obj: Any, config: Dict[str, Any]) -> Any:
        """Handle account numbers and financial information."""
        # Look for account-related fields
        account_fields = ["account", "payment", "credit", "debit", "card", "bank", "financial"]
        
        for attr_name, attr_value in obj.__dict__.items():
            # Skip private and special attributes
            if attr_name.startswith('_'):
                continue
            
            # Check if attribute contains account info
            if any(field in attr_name.lower() for field in account_fields) and isinstance(attr_value, str):
                # Pseudonymize the account number
                import hashlib
                hashed = hashlib.sha256(attr_value.encode()).hexdigest()[:8]
                setattr(obj, attr_name, f"ACCT-{hashed}")
        
        return obj
    
    def _redact_phi_from_text(self, text: str, config: Dict[str, Any]) -> str:
        """
        Redact PHI from free text content.
        
        This is a simplified version that uses regex patterns to identify
        common PHI patterns. A production system would use more advanced
        NLP techniques and named entity recognition.
        """
        if not text:
            return text
            
        # Make a copy of the text
        redacted_text = text
        
        # Apply regex-based redaction for common patterns
        for phi_type, pattern in self.phi_regex.items():
            # Different replacement based on type
            if phi_type == "mrn":
                redacted_text = pattern.sub(r'\1: [REDACTED-MRN]', redacted_text)
            elif phi_type == "phone":
                redacted_text = pattern.sub('[REDACTED-PHONE]', redacted_text)
            elif phi_type == "ssn":
                redacted_text = pattern.sub('[REDACTED-SSN]', redacted_text)
            elif phi_type == "email":
                redacted_text = pattern.sub('[REDACTED-EMAIL]', redacted_text)
            elif phi_type == "url":
                redacted_text = pattern.sub('[REDACTED-URL]', redacted_text)
            elif phi_type == "ip":
                redacted_text = pattern.sub('[REDACTED-IP]', redacted_text)
            elif phi_type == "account":
                redacted_text = pattern.sub(r'\1: [REDACTED-ACCT]', redacted_text)
            elif phi_type == "cc":
                redacted_text = pattern.sub('[REDACTED-CC]', redacted_text)
            elif phi_type == "license":
                redacted_text = pattern.sub('[REDACTED-LICENSE]', redacted_text)
        
        # Redact dates
        date_patterns = [
            # YYYY-MM-DD
            re.compile(r'\b\d{4}[-/]\d{1,2}[-/]\d{1,2}\b'),
            # MM/DD/YYYY
            re.compile(r'\b\d{1,2}[-/]\d{1,2}[-/]\d{4}\b'),
            # DD-MON-YYYY
            re.compile(r'\b\d{1,2}[-\s][A-Za-z]{3,9}[-\s]\d{4}\b'),
            # MON DD, YYYY
            re.compile(r'\b[A-Za-z]{3,9}\s\d{1,2},?\s\d{4}\b')
        ]
        
        for pattern in date_patterns:
            redacted_text = pattern.sub('[REDACTED-DATE]', redacted_text)
        
        # Handle specific HIPAA concerns for over-90 patients
        if config.get("over_90_handling") == "redact" and getattr(self, "_patient_is_over_90", False):
            # More aggressive date redaction for over 90 patients
            # Remove all years
            year_pattern = re.compile(r'\b(19|20)\d{2}\b')
            redacted_text = year_pattern.sub('[REDACTED-YEAR]', redacted_text)
        
        # Since we don't have a proper NER, we use a simple name search
        # Note: This is a very basic approach that would need improvement
        name_patterns = [
            # Dr. Lastname
            re.compile(r'\b(Dr|Doctor)\.?\s+[A-Z][a-z]+\b'),
            # First Last
            re.compile(r'\b[A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b'),
            # Mr./Mrs./Ms. Last
            re.compile(r'\b(Mr|Mrs|Ms|Miss)\.?\s+[A-Z][a-z]+\b')
        ]
        
        for pattern in name_patterns:
            redacted_text = pattern.sub('[REDACTED-NAME]', redacted_text)
        
        # Geographic redaction based on configuration
        precision = config.get("geographic_precision", "state")
        
        if precision in ["none", "country"]:
            # Redact city and state names
            # This is a simplified approach - would need a gazetteer for production
            city_state_pattern = re.compile(r'\b[A-Z][a-z]+(?:,\s*[A-Z]{2})?\b')
            redacted_text = city_state_pattern.sub('[REDACTED-LOCATION]', redacted_text)
            
            # Redact ZIP codes
            zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
            redacted_text = zip_pattern.sub('[REDACTED-ZIP]', redacted_text)
        elif precision == "state":
            # Only redact specific addresses and ZIP codes
            address_pattern = re.compile(r'\b\d+\s+[A-Za-z\s]+(?:Street|St|Avenue|Ave|Road|Rd|Boulevard|Blvd|Drive|Dr|Lane|Ln|Court|Ct|Place|Pl|Terrace|Ter|Way)\b', re.IGNORECASE)
            redacted_text = address_pattern.sub('[REDACTED-ADDRESS]', redacted_text)
            
            # Redact ZIP codes
            zip_pattern = re.compile(r'\b\d{5}(?:-\d{4})?\b')
            redacted_text = zip_pattern.sub('[REDACTED-ZIP]', redacted_text)
        
        return redacted_text
