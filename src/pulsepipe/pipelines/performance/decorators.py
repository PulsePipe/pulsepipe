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

# src/pulsepipe/pipelines/performance/decorators.py

"""
Performance tracking decorators for PulsePipe.

Provides easy-to-use decorators for automatic performance tracking.
"""

import functools
import inspect
import asyncio
from typing import Callable, Any, Optional, Dict, Union, List
from contextlib import contextmanager

from .tracker import PerformanceTracker, StepMetrics
from pulsepipe.utils.log_factory import LogFactory

logger = LogFactory.get_logger(__name__)


def track_performance(
    step_name: Optional[str] = None,
    track_args: bool = False,
    track_result: bool = False,
    count_records: Optional[str] = None,
    count_bytes: Optional[str] = None
):
    """
    Decorator to track performance of synchronous functions.
    
    Args:
        step_name: Name for the step (defaults to function name)
        track_args: Whether to include function arguments in metadata
        track_result: Whether to include function result in metadata
        count_records: Attribute name in result to count as records processed
        count_bytes: Attribute name in result to count as bytes processed
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Get tracker from context if available
            tracker = _get_tracker_from_args(args, kwargs)
            if not tracker:
                # No tracker available, just execute function
                return func(*args, **kwargs)
            
            # Determine step name
            name = step_name or func.__name__
            
            # Prepare metadata
            metadata = {'function': func.__name__}
            if track_args:
                metadata['args'] = _serialize_args(args, kwargs)
            
            # Start tracking
            step_metrics = tracker.start_step(name, metadata)
            
            try:
                # Execute function
                result = func(*args, **kwargs)
                
                # Process result for tracking
                records_processed, bytes_processed, success_count = _extract_counts(
                    result, count_records, count_bytes
                )
                
                if track_result and result is not None:
                    metadata['result_type'] = type(result).__name__
                    if hasattr(result, '__len__'):
                        metadata['result_size'] = len(result)
                
                # Finish tracking
                tracker.finish_step(
                    records_processed=records_processed,
                    bytes_processed=bytes_processed,
                    success_count=success_count,
                    failure_count=0,
                    metadata=metadata
                )
                
                return result
                
            except Exception as e:
                # Track failure
                metadata['error'] = str(e)
                metadata['error_type'] = type(e).__name__
                
                tracker.finish_step(
                    records_processed=0,
                    bytes_processed=0,
                    success_count=0,
                    failure_count=1,
                    metadata=metadata
                )
                
                raise
        
        return wrapper
    return decorator


def track_async_performance(
    step_name: Optional[str] = None,
    track_args: bool = False,
    track_result: bool = False,
    count_records: Optional[str] = None,
    count_bytes: Optional[str] = None
):
    """
    Decorator to track performance of asynchronous functions.
    
    Args:
        step_name: Name for the step (defaults to function name)
        track_args: Whether to include function arguments in metadata
        track_result: Whether to include function result in metadata
        count_records: Attribute name in result to count as records processed
        count_bytes: Attribute name in result to count as bytes processed
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get tracker from context if available
            tracker = _get_tracker_from_args(args, kwargs)
            if not tracker:
                # No tracker available, just execute function
                return await func(*args, **kwargs)
            
            # Determine step name
            name = step_name or func.__name__
            
            # Prepare metadata
            metadata = {'function': func.__name__, 'async': True}
            if track_args:
                metadata['args'] = _serialize_args(args, kwargs)
            
            # Start tracking
            step_metrics = tracker.start_step(name, metadata)
            
            try:
                # Execute function
                result = await func(*args, **kwargs)
                
                # Process result for tracking
                records_processed, bytes_processed, success_count = _extract_counts(
                    result, count_records, count_bytes
                )
                
                if track_result and result is not None:
                    metadata['result_type'] = type(result).__name__
                    if hasattr(result, '__len__'):
                        metadata['result_size'] = len(result)
                
                # Finish tracking
                tracker.finish_step(
                    records_processed=records_processed,
                    bytes_processed=bytes_processed,
                    success_count=success_count,
                    failure_count=0,
                    metadata=metadata
                )
                
                return result
                
            except Exception as e:
                # Track failure
                metadata['error'] = str(e)
                metadata['error_type'] = type(e).__name__
                
                tracker.finish_step(
                    records_processed=0,
                    bytes_processed=0,
                    success_count=0,
                    failure_count=1,
                    metadata=metadata
                )
                
                raise
        
        return wrapper
    return decorator


def track_stage_performance(stage_name: str):
    """
    Decorator specifically for pipeline stage methods.
    
    Args:
        stage_name: Name of the pipeline stage
    """
    def decorator(func: Callable) -> Callable:
        if asyncio.iscoroutinefunction(func):
            @functools.wraps(func)
            async def async_wrapper(self, context, *args, **kwargs):
                tracker = getattr(context, 'performance_tracker', None)
                if not tracker:
                    return await func(self, context, *args, **kwargs)
                
                metadata = {
                    'stage': stage_name,
                    'stage_class': self.__class__.__name__
                }
                
                step_metrics = tracker.start_step(stage_name, metadata)
                
                try:
                    result = await func(self, context, *args, **kwargs)
                    
                    # Extract metrics from result if it's a list/collection
                    records_processed = len(result) if isinstance(result, (list, tuple)) else 1
                    
                    tracker.finish_step(
                        records_processed=records_processed,
                        success_count=records_processed,
                        failure_count=0,
                        metadata=metadata
                    )
                    
                    return result
                    
                except Exception as e:
                    metadata['error'] = str(e)
                    metadata['error_type'] = type(e).__name__
                    
                    tracker.finish_step(
                        records_processed=0,
                        success_count=0,
                        failure_count=1,
                        metadata=metadata
                    )
                    
                    raise
            
            return async_wrapper
        else:
            @functools.wraps(func)
            def sync_wrapper(self, context, *args, **kwargs):
                tracker = getattr(context, 'performance_tracker', None)
                if not tracker:
                    return func(self, context, *args, **kwargs)
                
                metadata = {
                    'stage': stage_name,
                    'stage_class': self.__class__.__name__
                }
                
                step_metrics = tracker.start_step(stage_name, metadata)
                
                try:
                    result = func(self, context, *args, **kwargs)
                    
                    # Extract metrics from result if it's a list/collection
                    records_processed = len(result) if isinstance(result, (list, tuple)) else 1
                    
                    tracker.finish_step(
                        records_processed=records_processed,
                        success_count=records_processed,
                        failure_count=0,
                        metadata=metadata
                    )
                    
                    return result
                    
                except Exception as e:
                    metadata['error'] = str(e)
                    metadata['error_type'] = type(e).__name__
                    
                    tracker.finish_step(
                        records_processed=0,
                        success_count=0,
                        failure_count=1,
                        metadata=metadata
                    )
                    
                    raise
            
            return sync_wrapper
    return decorator


@contextmanager
def performance_context(tracker: PerformanceTracker, step_name: str, 
                       metadata: Optional[Dict[str, Any]] = None):
    """
    Context manager for manual performance tracking.
    
    Args:
        tracker: PerformanceTracker instance
        step_name: Name of the step to track
        metadata: Optional metadata to include
    """
    step_metrics = tracker.start_step(step_name, metadata)
    
    records_processed = 0
    bytes_processed = 0
    success_count = 0
    failure_count = 0
    
    class ContextData:
        def update_progress(self, records: int = 0, bytes_: int = 0, 
                          successes: int = 0, failures: int = 0):
            nonlocal records_processed, bytes_processed, success_count, failure_count
            records_processed += records
            bytes_processed += bytes_
            success_count += successes
            failure_count += failures
            
            # Also update the tracker's step progress
            tracker.update_step_progress(records, bytes_, successes, failures)
    
    context_data = ContextData()
    
    try:
        yield context_data
        
        # Successful completion
        tracker.finish_step(
            records_processed=records_processed,
            bytes_processed=bytes_processed,
            success_count=success_count,
            failure_count=failure_count,
            metadata=metadata
        )
        
    except Exception as e:
        # Failed completion
        if metadata is None:
            metadata = {}
        metadata['error'] = str(e)
        metadata['error_type'] = type(e).__name__
        
        tracker.finish_step(
            records_processed=records_processed,
            bytes_processed=bytes_processed,
            success_count=success_count,
            failure_count=failure_count + 1,
            metadata=metadata
        )
        
        raise


def _get_tracker_from_args(args: tuple, kwargs: dict) -> Optional[PerformanceTracker]:
    """Extract PerformanceTracker from function arguments."""
    # Check kwargs first
    if 'performance_tracker' in kwargs:
        return kwargs['performance_tracker']
    
    if 'tracker' in kwargs:
        return kwargs['tracker']
    
    if 'context' in kwargs:
        context = kwargs['context']
        return getattr(context, 'performance_tracker', None)
    
    # Check positional args
    for arg in args:
        if isinstance(arg, PerformanceTracker):
            return arg
        
        # Check if it's a context object with tracker
        if hasattr(arg, 'performance_tracker'):
            return arg.performance_tracker
    
    return None


def _serialize_args(args: tuple, kwargs: dict) -> Dict[str, Any]:
    """Serialize function arguments for metadata (safely)."""
    try:
        serialized = {}
        
        # Serialize positional args (limit to first few)
        if args:
            serialized['args'] = []
            for i, arg in enumerate(args[:3]):  # Limit to first 3 args
                if isinstance(arg, (str, int, float, bool, list, dict)):
                    serialized['args'].append(arg)
                else:
                    serialized['args'].append(f"<{type(arg).__name__}>")
        
        # Serialize keyword args (limit size)
        if kwargs:
            serialized['kwargs'] = {}
            for key, value in list(kwargs.items())[:5]:  # Limit to first 5 kwargs
                if isinstance(value, (str, int, float, bool)):
                    serialized['kwargs'][key] = value
                elif isinstance(value, (list, dict)) and len(str(value)) < 200:
                    serialized['kwargs'][key] = value
                else:
                    serialized['kwargs'][key] = f"<{type(value).__name__}>"
        
        return serialized
        
    except Exception:
        return {'serialization_error': 'Failed to serialize arguments'}


def _extract_counts(result: Any, count_records: Optional[str], 
                   count_bytes: Optional[str]) -> tuple[int, int, int]:
    """Extract record and byte counts from function result."""
    records_processed = 0
    bytes_processed = 0
    success_count = 0
    
    if result is None:
        return records_processed, bytes_processed, success_count
    
    # Try to count records
    if count_records:
        try:
            if hasattr(result, count_records):
                records_processed = getattr(result, count_records)
            elif isinstance(result, dict) and count_records in result:
                records_processed = result[count_records]
        except (AttributeError, KeyError, TypeError):
            pass
    elif isinstance(result, (list, tuple)):
        records_processed = len(result)
    elif hasattr(result, '__len__'):
        try:
            records_processed = len(result)
        except TypeError:
            records_processed = 1
    else:
        records_processed = 1 if result is not None else 0
    
    # Try to count bytes
    if count_bytes:
        try:
            if hasattr(result, count_bytes):
                bytes_processed = getattr(result, count_bytes)
            elif isinstance(result, dict) and count_bytes in result:
                bytes_processed = result[count_bytes]
        except (AttributeError, KeyError, TypeError):
            pass
    elif isinstance(result, (str, bytes)):
        bytes_processed = len(result)
    
    # Default success count to records processed if successful
    success_count = records_processed
    
    return records_processed, bytes_processed, success_count