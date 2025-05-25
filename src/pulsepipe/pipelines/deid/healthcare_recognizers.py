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

# src/pulsepipe/pipelines/deid/healthcare_recognizers.py

"""
Healthcare-specific recognizers for PulsePipe de-identification.

This module provides enhanced NER capabilities for healthcare data using:
- Presidio's built-in recognizers
- SciSpacy biomedical models
- Custom healthcare entity patterns
"""

import re
import spacy
from typing import List, Optional, Dict, Any
from presidio_analyzer import EntityRecognizer, RecognizerResult
from presidio_analyzer.nlp_engine import SpacyNlpEngine


class HealthcareNerRecognizer(EntityRecognizer):
    """
    Healthcare-specific Named Entity Recognizer using biomedical models.
    
    This recognizer uses SciSpacy models trained on biomedical text to identify
    healthcare-specific entities that standard NLP models might miss.
    """
    
    ENTITIES = [
        "MEDICAL_CONDITION", 
        "MEDICATION", 
        "MEDICAL_PROCEDURE", 
        "BODY_PART",
        "MEDICAL_DEVICE",
        "LAB_VALUE",
        "DOSAGE"
    ]
    
    def __init__(self):
        super().__init__(
            supported_entities=self.ENTITIES,
            name="HealthcareNerRecognizer"
        )
        
        # Try to load biomedical model, fallback to general model
        self.nlp = None
        try:
            self.nlp = spacy.load("en_core_sci_sm")
            self.model_name = "en_core_sci_sm"
        except OSError:
            try:
                self.nlp = spacy.load("en_core_web_lg") 
                self.model_name = "en_core_web_lg"
            except OSError:
                raise RuntimeError("No suitable spaCy model found for healthcare NER")
    
    def load(self) -> None:
        """Load the NLP model."""
        pass  # Model is already loaded in __init__
    
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Analyze text for healthcare entities.
        
        Args:
            text: Text to analyze
            entities: List of entity types to look for
            nlp_artifacts: Pre-computed NLP artifacts (optional)
            
        Returns:
            List of RecognizerResult objects
        """
        results = []
        
        if not text or not self.nlp:
            return results
        
        # Process text with biomedical model
        doc = self.nlp(text)
        
        # Extract entities
        for ent in doc.ents:
            # Map spaCy entity labels to our entity types
            entity_type = self._map_entity_label(ent.label_)
            
            if entity_type and entity_type in entities:
                result = RecognizerResult(
                    entity_type=entity_type,
                    start=ent.start_char,
                    end=ent.end_char,
                    score=0.85,  # High confidence for biomedical model
                    analysis_explanation=f"Detected by {self.model_name} as {ent.label_}"
                )
                results.append(result)
        
        return results
    
    def _map_entity_label(self, spacy_label: str) -> Optional[str]:
        """
        Map spaCy entity labels to our healthcare entity types.
        
        Args:
            spacy_label: The label from spaCy NER
            
        Returns:
            Mapped entity type or None
        """
        # SciSpacy entity mappings
        label_mapping = {
            # SciSpacy biomedical entities
            "CHEMICAL": "MEDICATION",
            "DISEASE": "MEDICAL_CONDITION", 
            "SPECIES": None,  # Usually not PHI
            "CELL_TYPE": "BODY_PART",
            "CELL_LINE": None,
            "DNA": None,
            "RNA": None,
            "PROTEIN": None,
            
            # Standard spaCy entities that might be medical
            "ORG": None,  # Organizations not always medical
            "PERSON": None,  # Handled by main person recognizer
            "GPE": None,   # Geographic entities handled separately
            "DATE": None,  # Handled by date recognizer
            "TIME": None,
            "MONEY": None,
            "PERCENT": None,
            "QUANTITY": "LAB_VALUE",  # Lab values often have quantities
            "CARDINAL": "LAB_VALUE"   # Numeric lab values
        }
        
        return label_mapping.get(spacy_label)


class MedicalRecordNumberRecognizer(EntityRecognizer):
    """
    Enhanced Medical Record Number (MRN) recognizer.
    
    Uses improved patterns to catch various MRN formats used across 
    different healthcare systems.
    """
    
    ENTITIES = ["MEDICAL_RECORD_NUMBER"]
    
    # Enhanced MRN patterns for different healthcare systems
    MRN_PATTERNS = [
        # Standard MRN formats
        re.compile(r'\b(?:MRN|Medical\s+Record\s+Number|Chart\s+Number|Patient\s+ID)\s*[:#]?\s*([A-Za-z0-9\-]{4,15})\b', re.IGNORECASE),
        
        # Epic-style MRNs (often start with E)
        re.compile(r'\bE\d{7,10}\b'),
        
        # Cerner-style MRNs  
        re.compile(r'\b[A-Z]{2,3}\d{6,9}\b'),
        
        # Numeric MRNs with specific lengths
        re.compile(r'\b\d{6,12}\b(?=\s*(?:MRN|Chart|Patient|Record))', re.IGNORECASE),
        
        # Alphanumeric patterns common in healthcare
        re.compile(r'\b[A-Z]\d{2}[-]?\d{3}[-]?\d{3}\b'),
        re.compile(r'\b\d{3}[-]?[A-Z]{2}[-]?\d{4}\b'),
    ]
    
    def __init__(self):
        super().__init__(
            supported_entities=self.ENTITIES,
            name="MedicalRecordNumberRecognizer"
        )
    
    def load(self) -> None:
        """Load the recognizer."""
        pass
    
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Analyze text for MRN patterns.
        
        Args:
            text: Text to analyze
            entities: List of entity types to look for
            nlp_artifacts: Pre-computed NLP artifacts (optional)
            
        Returns:
            List of RecognizerResult objects
        """
        results = []
        
        if "MEDICAL_RECORD_NUMBER" not in entities or not text:
            return results
        
        for pattern in self.MRN_PATTERNS:
            matches = pattern.finditer(text)
            
            for match in matches:
                # Use the full match for patterns without groups, 
                # or the first group for patterns with groups
                if match.groups():
                    start, end = match.span(1)
                    matched_text = match.group(1)
                else:
                    start, end = match.span()
                    matched_text = match.group()
                
                # Skip if the matched text is too short or too long
                if len(matched_text) < 4 or len(matched_text) > 15:
                    continue
                
                # Calculate confidence based on context and pattern
                confidence = self._calculate_confidence(text, match, matched_text)
                
                if confidence > 0.5:  # Only include high-confidence matches
                    result = RecognizerResult(
                        entity_type="MEDICAL_RECORD_NUMBER",
                        start=start,
                        end=end,
                        score=confidence,
                        analysis_explanation=f"MRN pattern match: {pattern.pattern[:50]}..."
                    )
                    results.append(result)
        
        return results
    
    def _calculate_confidence(self, text: str, match: re.Match, matched_text: str) -> float:
        """
        Calculate confidence score for MRN match.
        
        Args:
            text: Full text being analyzed
            match: Regex match object
            matched_text: The matched MRN text
            
        Returns:
            Confidence score between 0 and 1
        """
        confidence = 0.6  # Base confidence
        
        # Context-based confidence boosters
        context_window = 20
        start_context = max(0, match.start() - context_window)
        end_context = min(len(text), match.end() + context_window)
        context = text[start_context:end_context].lower()
        
        # Healthcare context keywords
        healthcare_keywords = [
            'mrn', 'medical record', 'chart number', 'patient id', 
            'hospital', 'clinic', 'physician', 'doctor', 'nurse',
            'admission', 'discharge', 'encounter', 'visit'
        ]
        
        for keyword in healthcare_keywords:
            if keyword in context:
                confidence += 0.1
                break
        
        # Pattern-specific confidence adjustments
        if re.match(r'^E\d{7,10}$', matched_text):  # Epic pattern
            confidence += 0.2
        elif re.match(r'^[A-Z]{2,3}\d{6,9}$', matched_text):  # Cerner pattern
            confidence += 0.2
        elif re.match(r'^\d{6,12}$', matched_text):  # Pure numeric
            confidence += 0.1
        
        return min(confidence, 1.0)


class DrugNameRecognizer(EntityRecognizer):
    """
    Drug/Medication name recognizer using enhanced patterns.
    
    Recognizes both brand names and generic drug names that might
    contain patient-specific information or be considered PHI.
    """
    
    ENTITIES = ["MEDICATION"]
    
    def __init__(self):
        super().__init__(
            supported_entities=self.ENTITIES,
            name="DrugNameRecognizer"
        )
        
        # Common drug name patterns and suffixes
        self.drug_suffixes = [
            r'(ine|ide|ium|ate|ole|pam|lam|zole|pine|done|rone|mab|nib|stat|pril|tan|ide)$',
            r'(mycin|cillin|oxin|toin|sulfa|chlor|cort|pred|dexa|hydro).*',
        ]
        
        # Drug strength/dosage patterns
        self.dosage_patterns = [
            re.compile(r'\b\d+\s*(?:mg|mcg|g|ml|cc|units?|iu)\b', re.IGNORECASE),
            re.compile(r'\b\d+(?:\.\d+)?\s*%\b'),
            re.compile(r'\b\d+(?:\.\d+)?\s*(?:mg/ml|mcg/ml|units/ml)\b', re.IGNORECASE),
        ]
    
    def load(self) -> None:
        """Load the recognizer."""
        pass
    
    def analyze(self, text: str, entities: List[str], nlp_artifacts=None) -> List[RecognizerResult]:
        """
        Analyze text for medication names and dosages.
        
        Args:
            text: Text to analyze
            entities: List of entity types to look for
            nlp_artifacts: Pre-computed NLP artifacts (optional)
            
        Returns:
            List of RecognizerResult objects
        """
        results = []
        
        if "MEDICATION" not in entities or not text:
            return results
        
        # Find dosage patterns (often indicate medications nearby)
        for pattern in self.dosage_patterns:
            matches = pattern.finditer(text)
            
            for match in matches:
                result = RecognizerResult(
                    entity_type="MEDICATION",
                    start=match.start(),
                    end=match.end(),
                    score=0.7,
                    analysis_explanation="Drug dosage pattern detected"
                )
                results.append(result)
        
        # Look for drug name patterns
        words = re.findall(r'\b[A-Za-z]{3,}\b', text)
        
        for word in words:
            for suffix_pattern in self.drug_suffixes:
                if re.search(suffix_pattern, word.lower()):
                    # Find the word's position in the original text
                    start = text.lower().find(word.lower())
                    if start != -1:
                        result = RecognizerResult(
                            entity_type="MEDICATION", 
                            start=start,
                            end=start + len(word),
                            score=0.6,
                            analysis_explanation=f"Drug name pattern: {suffix_pattern}"
                        )
                        results.append(result)
                        break
        
        return results


def create_healthcare_analyzer() -> 'AnalyzerEngine':
    """
    Create an AnalyzerEngine with healthcare-specific recognizers.
    
    Returns:
        Configured AnalyzerEngine with healthcare recognizers
    """
    from presidio_analyzer import AnalyzerEngine, RecognizerRegistry
    
    # Create registry with default recognizers
    registry = RecognizerRegistry()
    registry.load_predefined_recognizers()
    
    # Add our healthcare-specific recognizers
    registry.add_recognizer(HealthcareNerRecognizer())
    registry.add_recognizer(MedicalRecordNumberRecognizer()) 
    registry.add_recognizer(DrugNameRecognizer())
    
    # Create NLP engine with biomedical model if available
    try:
        nlp_engine = SpacyNlpEngine(models=[
            {"lang_code": "en", "model_name": "en_core_sci_sm"},  # Biomedical model
            {"lang_code": "en", "model_name": "en_core_web_lg"}   # Fallback
        ])
    except Exception:
        # Fallback to standard model
        nlp_engine = SpacyNlpEngine(models=[
            {"lang_code": "en", "model_name": "en_core_web_lg"}
        ])
    
    nlp_engine.load()
    
    # Create analyzer with our enhanced registry
    analyzer = AnalyzerEngine(
        registry=registry,
        nlp_engine=nlp_engine,
        supported_languages=["en"]
    )
    
    return analyzer