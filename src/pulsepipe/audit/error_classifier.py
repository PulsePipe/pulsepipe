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

# src/pulsepipe/audit/error_classifier.py

"""
Error classification system for structured error analysis.

Provides automatic error categorization, pattern detection,
and actionable recommendations for error resolution.
"""

import re
import traceback
from typing import Dict, Any, Optional, List, Tuple
from dataclasses import dataclass
from enum import Enum

from pulsepipe.persistence.models import ErrorCategory
from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


class ErrorSeverity(str, Enum):
    """Error severity levels."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class ErrorPattern(str, Enum):
    """Common error patterns for classification."""
    JSON_PARSE_ERROR = "json_parse_error"
    XML_PARSE_ERROR = "xml_parse_error"
    MISSING_REQUIRED_FIELD = "missing_required_field"
    INVALID_DATA_TYPE = "invalid_data_type"
    INVALID_DATE_FORMAT = "invalid_date_format"
    INVALID_CODE_FORMAT = "invalid_code_format"
    SCHEMA_VALIDATION_ERROR = "schema_validation_error"
    NETWORK_TIMEOUT = "network_timeout"
    CONNECTION_ERROR = "connection_error"
    PERMISSION_DENIED = "permission_denied"
    FILE_NOT_FOUND = "file_not_found"
    MEMORY_ERROR = "memory_error"
    DISK_SPACE_ERROR = "disk_space_error"
    RATE_LIMIT_ERROR = "rate_limit_error"
    AUTHENTICATION_ERROR = "authentication_error"
    UNKNOWN_ERROR = "unknown_error"


@dataclass
class ErrorAnalysis:
    """
    Comprehensive error analysis result.
    """
    category: ErrorCategory
    pattern: ErrorPattern
    severity: ErrorSeverity
    description: str
    root_cause: Optional[str] = None
    recommendations: Optional[List[str]] = None
    technical_details: Optional[Dict[str, Any]] = None
    similar_errors_count: int = 0
    is_recoverable: bool = False
    confidence_score: float = 0.0


@dataclass
class ClassifiedError:
    """
    Error with classification and context information.
    """
    original_error: Exception
    analysis: ErrorAnalysis
    stage_name: str
    record_id: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    stack_trace: Optional[str] = None
    timestamp: Optional[str] = None


class ErrorClassifier:
    """
    Intelligent error classification system.
    
    Analyzes exceptions and error messages to provide structured
    classification, severity assessment, and actionable recommendations.
    """
    
    def __init__(self):
        """Initialize error classifier with pattern rules."""
        self.classification_rules = self._build_classification_rules()
        self.error_patterns = self._build_error_patterns()
        self.severity_rules = self._build_severity_rules()
        self.recommendation_rules = self._build_recommendation_rules()
    
    def classify_error(self, error: Exception, stage_name: str,
                      record_id: Optional[str] = None,
                      context: Optional[Dict[str, Any]] = None) -> ClassifiedError:
        """
        Classify an error and provide comprehensive analysis.
        
        Args:
            error: Exception to classify
            stage_name: Pipeline stage where error occurred
            record_id: Optional record identifier
            context: Optional additional context
            
        Returns:
            ClassifiedError with complete analysis
        """
        # Get stack trace
        stack_trace = traceback.format_exc()
        
        # Analyze the error
        analysis = self._analyze_error(error, context)
        
        # Create classified error
        classified_error = ClassifiedError(
            original_error=error,
            analysis=analysis,
            stage_name=stage_name,
            record_id=record_id,
            context=context,
            stack_trace=stack_trace
        )
        
        logger.debug(f"Classified error: {analysis.category.value} - {analysis.pattern.value}")
        return classified_error
    
    def _analyze_error(self, error: Exception, context: Optional[Dict[str, Any]] = None) -> ErrorAnalysis:
        """
        Perform detailed error analysis.
        
        Args:
            error: Exception to analyze
            context: Optional context information
            
        Returns:
            ErrorAnalysis with classification and recommendations
        """
        error_type = type(error).__name__
        error_message = str(error).lower()
        
        # Determine error category and pattern
        category = self._classify_category(error_type, error_message, context)
        pattern = self._detect_pattern(error_type, error_message)
        severity = self._assess_severity(category, pattern, error_type)
        
        # Generate description and recommendations
        description = self._generate_description(category, pattern, error)
        recommendations = self._generate_recommendations(category, pattern, error)
        root_cause = self._identify_root_cause(error_type, error_message, context)
        
        # Assess recoverability
        is_recoverable = self._assess_recoverability(category, pattern)
        
        # Calculate confidence score
        confidence_score = self._calculate_confidence(error_type, error_message, pattern)
        
        # Extract technical details
        technical_details = self._extract_technical_details(error, context)
        
        return ErrorAnalysis(
            category=category,
            pattern=pattern,
            severity=severity,
            description=description,
            root_cause=root_cause,
            recommendations=recommendations,
            technical_details=technical_details,
            is_recoverable=is_recoverable,
            confidence_score=confidence_score
        )
    
    def _classify_category(self, error_type: str, error_message: str,
                          context: Optional[Dict[str, Any]] = None) -> ErrorCategory:
        """Classify error into main category."""
        # Check specific error types first
        if error_type in ['JSONDecodeError', 'XMLSyntaxError', 'ParseError']:
            return ErrorCategory.PARSE_ERROR
        
        if error_type in ['ValidationError', 'SchemaError', 'ValueError']:
            return ErrorCategory.VALIDATION_ERROR
        
        if error_type in ['ConnectionError', 'TimeoutError', 'HTTPError']:
            return ErrorCategory.NETWORK_ERROR
        
        if error_type in ['PermissionError', 'UnauthorizedError', 'ForbiddenError']:
            return ErrorCategory.PERMISSION_ERROR
        
        if error_type in ['MemoryError', 'OSError', 'SystemError']:
            return ErrorCategory.SYSTEM_ERROR
        
        # Check error message patterns
        for pattern, category in self.classification_rules.items():
            if re.search(pattern, error_message, re.IGNORECASE):
                return category
        
        # Check context for additional clues
        if context:
            if 'schema' in context or 'validation' in context:
                return ErrorCategory.SCHEMA_ERROR
            if 'transform' in context or 'mapping' in context:
                return ErrorCategory.TRANSFORMATION_ERROR
        
        return ErrorCategory.SYSTEM_ERROR  # Default fallback
    
    def _detect_pattern(self, error_type: str, error_message: str) -> ErrorPattern:
        """Detect specific error pattern."""
        # Check error type patterns
        if error_type == 'JSONDecodeError':
            return ErrorPattern.JSON_PARSE_ERROR
        
        if error_type in ['XMLSyntaxError', 'XMLParseError']:
            return ErrorPattern.XML_PARSE_ERROR
        
        if error_type == 'MemoryError':
            return ErrorPattern.MEMORY_ERROR
        
        if error_type == 'PermissionError':
            return ErrorPattern.PERMISSION_DENIED
        
        if error_type == 'FileNotFoundError':
            return ErrorPattern.FILE_NOT_FOUND
        
        # Check message patterns
        for pattern_regex, pattern_type in self.error_patterns.items():
            if re.search(pattern_regex, error_message, re.IGNORECASE):
                return pattern_type
        
        return ErrorPattern.UNKNOWN_ERROR
    
    def _assess_severity(self, category: ErrorCategory, pattern: ErrorPattern,
                        error_type: str) -> ErrorSeverity:
        """Assess error severity."""
        # Critical errors
        if pattern in [ErrorPattern.MEMORY_ERROR, ErrorPattern.DISK_SPACE_ERROR]:
            return ErrorSeverity.CRITICAL
        
        if category == ErrorCategory.SYSTEM_ERROR and error_type in ['SystemError', 'OSError']:
            return ErrorSeverity.CRITICAL
        
        # High severity errors
        if category in [ErrorCategory.PERMISSION_ERROR, ErrorCategory.NETWORK_ERROR]:
            return ErrorSeverity.HIGH
        
        if pattern in [ErrorPattern.AUTHENTICATION_ERROR, ErrorPattern.CONNECTION_ERROR]:
            return ErrorSeverity.HIGH
        
        # Medium severity errors
        if category in [ErrorCategory.SCHEMA_ERROR, ErrorCategory.VALIDATION_ERROR]:
            return ErrorSeverity.MEDIUM
        
        if pattern in [ErrorPattern.MISSING_REQUIRED_FIELD, ErrorPattern.INVALID_DATA_TYPE]:
            return ErrorSeverity.MEDIUM
        
        # Low severity errors
        if category == ErrorCategory.DATA_QUALITY_ERROR:
            return ErrorSeverity.LOW
        
        if pattern in [ErrorPattern.INVALID_DATE_FORMAT, ErrorPattern.INVALID_CODE_FORMAT]:
            return ErrorSeverity.LOW
        
        return ErrorSeverity.MEDIUM  # Default
    
    def _generate_description(self, category: ErrorCategory, pattern: ErrorPattern,
                            error: Exception) -> str:
        """Generate human-readable error description."""
        descriptions = {
            ErrorPattern.JSON_PARSE_ERROR: "Invalid JSON format in input data",
            ErrorPattern.XML_PARSE_ERROR: "Invalid XML format in input data",
            ErrorPattern.MISSING_REQUIRED_FIELD: "Required field is missing from record",
            ErrorPattern.INVALID_DATA_TYPE: "Field value has incorrect data type",
            ErrorPattern.INVALID_DATE_FORMAT: "Date field has invalid format",
            ErrorPattern.SCHEMA_VALIDATION_ERROR: "Record does not conform to expected schema",
            ErrorPattern.NETWORK_TIMEOUT: "Network operation timed out",
            ErrorPattern.CONNECTION_ERROR: "Failed to establish network connection",
            ErrorPattern.PERMISSION_DENIED: "Insufficient permissions to access resource",
            ErrorPattern.FILE_NOT_FOUND: "Required file or resource not found",
            ErrorPattern.MEMORY_ERROR: "System ran out of available memory",
            ErrorPattern.AUTHENTICATION_ERROR: "Authentication failed",
        }
        
        description = descriptions.get(pattern, f"Error in {category.value}: {str(error)}")
        return description
    
    def _generate_recommendations(self, category: ErrorCategory, pattern: ErrorPattern,
                                error: Exception) -> List[str]:
        """Generate actionable recommendations."""
        recommendations = []
        
        # Pattern-specific recommendations
        if pattern == ErrorPattern.JSON_PARSE_ERROR:
            recommendations.extend([
                "Validate JSON syntax using a JSON validator",
                "Check for missing quotes, brackets, or commas",
                "Ensure proper escaping of special characters"
            ])
        
        elif pattern == ErrorPattern.XML_PARSE_ERROR:
            recommendations.extend([
                "Validate XML syntax using an XML validator", 
                "Check for unclosed tags or invalid characters",
                "Ensure proper XML declaration and encoding"
            ])
        
        elif pattern == ErrorPattern.MISSING_REQUIRED_FIELD:
            recommendations.extend([
                "Check data source for completeness",
                "Implement default values for missing fields",
                "Add data validation at ingestion point"
            ])
        
        elif pattern == ErrorPattern.NETWORK_TIMEOUT:
            recommendations.extend([
                "Increase timeout configuration",
                "Check network connectivity",
                "Implement retry logic with exponential backoff"
            ])
        
        elif pattern == ErrorPattern.MEMORY_ERROR:
            recommendations.extend([
                "Process data in smaller batches",
                "Increase available memory allocation",
                "Implement streaming processing for large datasets"
            ])
        
        elif pattern == ErrorPattern.PERMISSION_DENIED:
            recommendations.extend([
                "Check file and directory permissions",
                "Verify user account has necessary access rights",
                "Contact system administrator for permission updates"
            ])
        
        # Category-specific recommendations
        if category == ErrorCategory.VALIDATION_ERROR:
            recommendations.extend([
                "Review and update data validation rules",
                "Implement data cleansing pipeline",
                "Add pre-processing validation step"
            ])
        
        elif category == ErrorCategory.SCHEMA_ERROR:
            recommendations.extend([
                "Update schema definition to match data",
                "Implement schema evolution strategy",
                "Add schema validation at data source"
            ])
        
        # Remove duplicates and return
        return list(set(recommendations))
    
    def _identify_root_cause(self, error_type: str, error_message: str,
                           context: Optional[Dict[str, Any]] = None) -> Optional[str]:
        """Identify potential root cause of the error."""
        # Common root causes based on patterns
        if 'json' in error_message and 'decode' in error_message:
            return "Malformed JSON data from source system"
        
        if 'xml' in error_message and 'syntax' in error_message:
            return "Invalid XML structure from data provider"
        
        if 'required' in error_message or 'missing' in error_message:
            return "Incomplete data from upstream system"
        
        if 'timeout' in error_message or 'connection' in error_message:
            return "Network connectivity or service availability issue"
        
        if 'permission' in error_message or 'access' in error_message:
            return "Insufficient access privileges or security restrictions"
        
        if 'memory' in error_message:
            return "Insufficient system resources for data volume"
        
        if context and 'file_path' in context:
            return f"Issue with file: {context['file_path']}"
        
        return None
    
    def _assess_recoverability(self, category: ErrorCategory, pattern: ErrorPattern) -> bool:
        """Assess if the error is recoverable."""
        # Generally recoverable errors
        recoverable_patterns = [
            ErrorPattern.NETWORK_TIMEOUT,
            ErrorPattern.CONNECTION_ERROR,
            ErrorPattern.RATE_LIMIT_ERROR,
            ErrorPattern.INVALID_DATE_FORMAT,
            ErrorPattern.INVALID_CODE_FORMAT
        ]
        
        recoverable_categories = [
            ErrorCategory.DATA_QUALITY_ERROR,
            ErrorCategory.VALIDATION_ERROR
        ]
        
        if pattern in recoverable_patterns:
            return True
        
        if category in recoverable_categories:
            return True
        
        # Generally non-recoverable errors
        non_recoverable_patterns = [
            ErrorPattern.PERMISSION_DENIED,
            ErrorPattern.AUTHENTICATION_ERROR,
            ErrorPattern.FILE_NOT_FOUND,
            ErrorPattern.MEMORY_ERROR
        ]
        
        if pattern in non_recoverable_patterns:
            return False
        
        return True  # Default to recoverable
    
    def _calculate_confidence(self, error_type: str, error_message: str,
                            pattern: ErrorPattern) -> float:
        """Calculate confidence score for classification."""
        confidence = 0.5  # Base confidence
        
        # High confidence for specific error types
        if error_type in ['JSONDecodeError', 'XMLSyntaxError', 'MemoryError', 'PermissionError']:
            confidence += 0.4
        
        # Medium confidence for pattern matches
        if pattern != ErrorPattern.UNKNOWN_ERROR:
            confidence += 0.2
        
        # Boost confidence for clear error messages
        clear_indicators = ['json', 'xml', 'permission', 'memory', 'timeout', 'connection']
        for indicator in clear_indicators:
            if indicator in error_message.lower():
                confidence += 0.1
                break
        
        return min(1.0, confidence)
    
    def _extract_technical_details(self, error: Exception,
                                  context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Extract technical details for debugging."""
        details = {
            "error_type": type(error).__name__,
            "error_message": str(error),
            "error_args": error.args
        }
        
        # Add context information
        if context:
            details["context"] = context
        
        # Extract specific error attributes
        if hasattr(error, 'errno'):
            details["error_code"] = error.errno
        
        if hasattr(error, 'filename'):
            details["filename"] = error.filename
        
        if hasattr(error, 'lineno'):
            details["line_number"] = error.lineno
        
        if hasattr(error, 'colno'):
            details["column_number"] = error.colno
        
        return details
    
    def _build_classification_rules(self) -> Dict[str, ErrorCategory]:
        """Build regex patterns for error classification."""
        return {
            r'json.*decode|parse.*json|invalid.*json': ErrorCategory.PARSE_ERROR,
            r'xml.*syntax|parse.*xml|invalid.*xml': ErrorCategory.PARSE_ERROR,
            r'schema.*error|validation.*failed|invalid.*schema': ErrorCategory.SCHEMA_ERROR,
            r'required.*field|missing.*field|field.*required': ErrorCategory.VALIDATION_ERROR,
            r'invalid.*type|type.*error|wrong.*type': ErrorCategory.VALIDATION_ERROR,
            r'transform.*error|mapping.*error|conversion.*error': ErrorCategory.TRANSFORMATION_ERROR,
            r'network.*error|connection.*error|timeout': ErrorCategory.NETWORK_ERROR,
            r'permission.*denied|access.*denied|unauthorized': ErrorCategory.PERMISSION_ERROR,
            r'memory.*error|out.*of.*memory|insufficient.*memory': ErrorCategory.SYSTEM_ERROR,
            r'disk.*space|no.*space|storage.*full': ErrorCategory.SYSTEM_ERROR,
            r'quality.*check|data.*quality|quality.*score': ErrorCategory.DATA_QUALITY_ERROR,
        }
    
    def _build_error_patterns(self) -> Dict[str, ErrorPattern]:
        """Build regex patterns for error pattern detection."""
        return {
            r'json.*decode|parse.*json': ErrorPattern.JSON_PARSE_ERROR,
            r'xml.*syntax|parse.*xml': ErrorPattern.XML_PARSE_ERROR,
            r'required.*field|missing.*field': ErrorPattern.MISSING_REQUIRED_FIELD,
            r'invalid.*type|wrong.*type': ErrorPattern.INVALID_DATA_TYPE,
            r'date.*format|invalid.*date': ErrorPattern.INVALID_DATE_FORMAT,
            r'code.*format|invalid.*code': ErrorPattern.INVALID_CODE_FORMAT,
            r'schema.*validation|schema.*error': ErrorPattern.SCHEMA_VALIDATION_ERROR,
            r'timeout|timed.*out': ErrorPattern.NETWORK_TIMEOUT,
            r'connection.*error|failed.*connect': ErrorPattern.CONNECTION_ERROR,
            r'permission.*denied|access.*denied': ErrorPattern.PERMISSION_DENIED,
            r'file.*not.*found|no.*such.*file': ErrorPattern.FILE_NOT_FOUND,
            r'memory.*error|out.*of.*memory': ErrorPattern.MEMORY_ERROR,
            r'disk.*space|no.*space': ErrorPattern.DISK_SPACE_ERROR,
            r'rate.*limit|too.*many.*requests': ErrorPattern.RATE_LIMIT_ERROR,
            r'auth.*error|unauthorized|forbidden': ErrorPattern.AUTHENTICATION_ERROR,
        }
    
    def _build_severity_rules(self) -> Dict[str, ErrorSeverity]:
        """Build severity assessment rules."""
        return {
            'MemoryError': ErrorSeverity.CRITICAL,
            'SystemError': ErrorSeverity.CRITICAL,
            'OSError': ErrorSeverity.HIGH,
            'PermissionError': ErrorSeverity.HIGH,
            'ConnectionError': ErrorSeverity.HIGH,
            'ValidationError': ErrorSeverity.MEDIUM,
            'ValueError': ErrorSeverity.MEDIUM,
            'TypeError': ErrorSeverity.MEDIUM,
        }
    
    def _build_recommendation_rules(self) -> Dict[ErrorPattern, List[str]]:
        """Build recommendation rules for different error patterns."""
        return {
            ErrorPattern.JSON_PARSE_ERROR: [
                "Validate JSON syntax",
                "Check for missing quotes or brackets",
                "Implement JSON schema validation"
            ],
            ErrorPattern.NETWORK_TIMEOUT: [
                "Increase timeout values",
                "Implement retry logic",
                "Check network connectivity"
            ],
            ErrorPattern.MEMORY_ERROR: [
                "Process data in smaller batches",
                "Increase memory allocation",
                "Implement streaming processing"
            ]
        }
    
    def get_error_statistics(self, errors: List[ClassifiedError]) -> Dict[str, Any]:
        """
        Generate statistics from a list of classified errors.
        
        Args:
            errors: List of classified errors
            
        Returns:
            Dictionary with error statistics
        """
        if not errors:
            return {"total_errors": 0}
        
        total_errors = len(errors)
        
        # Count by category
        category_counts = {}
        for category in ErrorCategory:
            count = sum(1 for e in errors if e.analysis.category == category)
            if count > 0:
                category_counts[category.value] = count
        
        # Count by pattern
        pattern_counts = {}
        for pattern in ErrorPattern:
            count = sum(1 for e in errors if e.analysis.pattern == pattern)
            if count > 0:
                pattern_counts[pattern.value] = count
        
        # Count by severity
        severity_counts = {}
        for severity in ErrorSeverity:
            count = sum(1 for e in errors if e.analysis.severity == severity)
            if count > 0:
                severity_counts[severity.value] = count
        
        # Count recoverable vs non-recoverable
        recoverable_count = sum(1 for e in errors if e.analysis.is_recoverable)
        
        # Average confidence score
        avg_confidence = sum(e.analysis.confidence_score for e in errors) / total_errors
        
        # Most common stage
        stage_counts = {}
        for error in errors:
            stage = error.stage_name
            stage_counts[stage] = stage_counts.get(stage, 0) + 1
        most_common_stage = max(stage_counts.items(), key=lambda x: x[1])[0] if stage_counts else None
        
        return {
            "total_errors": total_errors,
            "category_breakdown": category_counts,
            "pattern_breakdown": pattern_counts,
            "severity_breakdown": severity_counts,
            "recoverable_errors": recoverable_count,
            "non_recoverable_errors": total_errors - recoverable_count,
            "average_confidence_score": round(avg_confidence, 3),
            "most_common_stage": most_common_stage,
            "stage_breakdown": stage_counts
        }