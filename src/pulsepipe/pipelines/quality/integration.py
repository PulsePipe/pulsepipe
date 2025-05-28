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

# src/pulsepipe/pipelines/quality/integration.py

"""
Integration module for data quality scoring engine.

Connects the quality scoring engine with the existing persistence layer
and provides high-level interfaces for quality assessment.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.persistence import QualityMetric
from .scoring_engine import DataQualityScorer, QualityScore

logger = LogFactory.get_logger(__name__)


class QualityAssessmentService:
    """
    High-level service for data quality assessment and persistence.
    
    Integrates the quality scoring engine with the tracking repository
    to provide comprehensive quality assessment capabilities.
    """
    
    def __init__(self, repository: Optional[Any] = None, config: Optional[Dict[str, Any]] = None):
        """
        Initialize quality assessment service.
        
        Args:
            repository: Tracking repository for persistence (legacy, deprecated)
            config: Configuration for quality scoring
        """
        self.repository = repository
        self.scorer = DataQualityScorer(config)
        self.config = config or {}
        
        # Quality assessment settings
        self.auto_persist = self.config.get('auto_persist', True)
        self.sampling_rate = self.config.get('sampling_rate', 1.0)  # 1.0 = assess all records
        self.batch_size = self.config.get('batch_size', 100)
        
        # Tracking for batch processing
        self.pending_scores: List[QualityScore] = []
        
        logger.info("Quality assessment service initialized")
    
    def assess_record(self, pipeline_run_id: str, data: Dict[str, Any], record_type: str,
                     record_id: Optional[str] = None,
                     context_data: Optional[List[Dict[str, Any]]] = None,
                     usage_context: Optional[Dict[str, List[str]]] = None,
                     persist: Optional[bool] = None) -> QualityScore:
        """
        Assess quality of a single record.
        
        Args:
            pipeline_run_id: Pipeline run identifier
            data: Record data to assess
            record_type: Type of the record
            record_id: Unique identifier for the record
            context_data: Other records for cross-record consistency
            usage_context: Field usage information by processing stage
            persist: Whether to persist results (overrides auto_persist)
            
        Returns:
            Quality score for the record
        """
        # Apply sampling if configured
        if self.sampling_rate < 1.0:
            import random
            if random.random() > self.sampling_rate:
                # Return placeholder score for non-sampled records
                return self._create_placeholder_score(record_id or "unknown", record_type)
        
        # Score the record
        quality_score = self.scorer.score_record(
            data=data,
            record_type=record_type,
            record_id=record_id,
            context_data=context_data,
            usage_context=usage_context
        )
        
        # Persist if requested
        should_persist = persist if persist is not None else self.auto_persist
        if should_persist:
            self._persist_quality_score(pipeline_run_id, quality_score)
        else:
            # Add to pending batch
            self.pending_scores.append(quality_score)
            if len(self.pending_scores) >= self.batch_size:
                self._persist_batch(pipeline_run_id)
        
        return quality_score
    
    def assess_batch(self, pipeline_run_id: str, records: List[Dict[str, Any]], 
                    record_type: str, persist: Optional[bool] = None) -> List[QualityScore]:
        """
        Assess quality of a batch of records.
        
        Args:
            pipeline_run_id: Pipeline run identifier
            records: List of records to assess
            record_type: Type of the records
            persist: Whether to persist results
            
        Returns:
            List of quality scores
        """
        logger.info(f"Assessing quality for {len(records)} {record_type} records")
        
        # Score the batch
        scores = self.scorer.score_batch(records, record_type)
        
        # Apply sampling if configured
        if self.sampling_rate < 1.0:
            sampled_scores = []
            import random
            for score in scores:
                if random.random() <= self.sampling_rate:
                    score.metadata['sampled'] = True
                    sampled_scores.append(score)
                else:
                    # Create placeholder for non-sampled
                    placeholder = self._create_placeholder_score(score.record_id, record_type)
                    placeholder.metadata['sampled'] = False
                    sampled_scores.append(placeholder)
            scores = sampled_scores
        
        # Persist if requested
        should_persist = persist if persist is not None else self.auto_persist
        if should_persist:
            self._persist_batch_scores(pipeline_run_id, scores)
        
        return scores
    
    def get_quality_summary(self, pipeline_run_id: Optional[str] = None,
                          record_type: Optional[str] = None,
                          start_date: Optional[datetime] = None,
                          end_date: Optional[datetime] = None) -> Dict[str, Any]:
        """
        Get quality summary from repository.
        
        Args:
            pipeline_run_id: Optional pipeline run filter
            record_type: Optional record type filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            
        Returns:
            Quality summary dictionary
        """
        return self.repository.get_quality_summary(pipeline_run_id)
    
    def get_quality_trends(self, record_type: Optional[str] = None,
                          days: int = 30) -> Dict[str, Any]:
        """
        Get quality trends over time.
        
        Args:
            record_type: Optional record type filter
            days: Number of days to analyze
            
        Returns:
            Quality trends analysis
        """
        # This would require additional repository methods for time-series analysis
        # For now, return basic summary
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        
        return {
            "period_days": days,
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
            "summary": self.get_quality_summary(start_date=start_date, end_date=end_date),
            "trend_analysis": "Not yet implemented"
        }
    
    def flush_pending(self, pipeline_run_id: str) -> None:
        """Flush any pending quality scores to repository."""
        if self.pending_scores:
            self._persist_batch_scores(pipeline_run_id, self.pending_scores)
            self.pending_scores.clear()
    
    def _persist_quality_score(self, pipeline_run_id: str, score: QualityScore) -> None:
        """Persist a single quality score to the repository."""
        try:
            # Convert QualityScore to QualityMetric
            metric = QualityMetric(
                id=None,
                pipeline_run_id=pipeline_run_id,
                record_id=score.record_id,
                record_type=score.record_type,
                completeness_score=score.completeness_score,
                consistency_score=score.consistency_score,
                validity_score=score.validity_score,
                accuracy_score=score.accuracy_score,
                overall_score=score.overall_score,
                missing_fields=score.missing_fields,
                invalid_fields=score.invalid_fields,
                outlier_fields=score.outlier_fields,
                quality_issues=[issue.description for issue in score.issues],
                metrics_details=score.to_dict(),
                sampled=score.metadata.get('sampled', False),
                timestamp=datetime.now()
            )
            
            self.repository.record_quality_metric(metric)
            logger.debug(f"Persisted quality metric for record {score.record_id}")
            
        except Exception as e:
            logger.error(f"Failed to persist quality metric for {score.record_id}: {e}")
    
    def _persist_batch_scores(self, pipeline_run_id: str, scores: List[QualityScore]) -> None:
        """Persist a batch of quality scores."""
        for score in scores:
            self._persist_quality_score(pipeline_run_id, score)
        
        logger.info(f"Persisted {len(scores)} quality metrics for pipeline {pipeline_run_id}")
    
    def _persist_batch(self, pipeline_run_id: str) -> None:
        """Persist pending scores as a batch."""
        if self.pending_scores:
            self._persist_batch_scores(pipeline_run_id, self.pending_scores)
            self.pending_scores.clear()
    
    def _create_placeholder_score(self, record_id: str, record_type: str) -> QualityScore:
        """Create a placeholder score for non-sampled records."""
        return QualityScore(
            record_id=record_id,
            record_type=record_type,
            completeness_score=0.0,
            consistency_score=0.0,
            validity_score=0.0,
            accuracy_score=0.0,
            outlier_score=0.0,
            data_usage_score=0.0,
            overall_score=0.0,
            issues=[],
            missing_fields=[],
            invalid_fields=[],
            outlier_fields=[],
            unused_fields=[],
            metadata={'placeholder': True, 'sampled': False}
        )


class QualityAssessmentPipeline:
    """
    Pipeline stage for integrating quality assessment into data processing.
    
    Can be inserted into the PulsePipe processing pipeline to automatically
    assess data quality during ingestion and processing.
    """
    
    def __init__(self, service: QualityAssessmentService):
        """
        Initialize quality assessment pipeline stage.
        
        Args:
            service: Quality assessment service instance
        """
        self.service = service
        self.processed_records = 0
        self.quality_stats = {
            'total_assessed': 0,
            'avg_quality_score': 0.0,
            'high_quality_records': 0,
            'low_quality_records': 0
        }
    
    async def process_record(self, pipeline_run_id: str, data: Dict[str, Any], 
                           record_type: str, record_id: Optional[str] = None,
                           usage_context: Optional[Dict[str, List[str]]] = None) -> Dict[str, Any]:
        """
        Process a single record through quality assessment.
        
        Args:
            pipeline_run_id: Pipeline run identifier
            data: Record data to process
            record_type: Type of the record
            record_id: Unique identifier for the record
            usage_context: Field usage tracking information
            
        Returns:
            Original data with quality metadata added
        """
        # Assess quality
        quality_score = self.service.assess_record(
            pipeline_run_id=pipeline_run_id,
            data=data,
            record_type=record_type,
            record_id=record_id,
            usage_context=usage_context
        )
        
        # Update statistics
        self._update_stats(quality_score)
        
        # Add quality metadata to the record
        quality_metadata = {
            'quality_score': quality_score.overall_score,
            'quality_grade': self._get_quality_grade(quality_score.overall_score),
            'has_issues': len(quality_score.issues) > 0,
            'issue_count': len(quality_score.issues),
            'high_severity_issues': len([i for i in quality_score.issues 
                                       if i.severity in ['high', 'critical']])
        }
        
        # Create enhanced record
        enhanced_data = data.copy()
        enhanced_data['_quality'] = quality_metadata
        
        return enhanced_data
    
    async def process_batch(self, pipeline_run_id: str, records: List[Dict[str, Any]], 
                          record_type: str) -> List[Dict[str, Any]]:
        """
        Process a batch of records through quality assessment.
        
        Args:
            pipeline_run_id: Pipeline run identifier
            records: List of records to process
            record_type: Type of the records
            
        Returns:
            List of enhanced records with quality metadata
        """
        # Assess batch quality
        scores = self.service.assess_batch(pipeline_run_id, records, record_type)
        
        # Update statistics
        for score in scores:
            self._update_stats(score)
        
        # Enhance records with quality metadata
        enhanced_records = []
        for record, score in zip(records, scores):
            quality_metadata = {
                'quality_score': score.overall_score,
                'quality_grade': self._get_quality_grade(score.overall_score),
                'has_issues': len(score.issues) > 0,
                'issue_count': len(score.issues),
                'critical_issues': [i.description for i in score.issues 
                                  if i.severity == 'critical']
            }
            
            enhanced_record = record.copy()
            enhanced_record['_quality'] = quality_metadata
            enhanced_records.append(enhanced_record)
        
        return enhanced_records
    
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get quality statistics for this pipeline run."""
        return {
            'processed_records': self.processed_records,
            'quality_stats': self.quality_stats.copy(),
            'quality_rate': (self.quality_stats['high_quality_records'] / 
                           max(1, self.processed_records)) * 100
        }
    
    def _update_stats(self, quality_score: QualityScore) -> None:
        """Update internal quality statistics."""
        if quality_score.metadata.get('placeholder', False):
            return  # Skip placeholder scores
        
        self.processed_records += 1
        self.quality_stats['total_assessed'] += 1
        
        # Update running average
        current_avg = self.quality_stats['avg_quality_score']
        total = self.quality_stats['total_assessed']
        new_avg = ((current_avg * (total - 1)) + quality_score.overall_score) / total
        self.quality_stats['avg_quality_score'] = new_avg
        
        # Count quality levels
        if quality_score.overall_score >= 0.8:
            self.quality_stats['high_quality_records'] += 1
        elif quality_score.overall_score < 0.5:
            self.quality_stats['low_quality_records'] += 1
    
    def _get_quality_grade(self, score: float) -> str:
        """Convert quality score to letter grade."""
        if score >= 0.9:
            return 'A'
        elif score >= 0.8:
            return 'B'
        elif score >= 0.7:
            return 'C'
        elif score >= 0.6:
            return 'D'
        else:
            return 'F'