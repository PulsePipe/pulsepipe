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

# src/pulsepipe/ingesters/ingestion_engine.py

import asyncio
from pulsepipe.utils.log_factory import LogFactory
from typing import Optional, Any, List, Union
from pulsepipe.models.clinical_content import PulseClinicalContent
from pulsepipe.models.operational_content import PulseOperationalContent
from pulsepipe.utils.errors import (
    IngestionEngineError, IngesterError, AdapterError, PulsePipeError
)

class IngestionEngine:
    """
    Core engine that coordinates data flow between adapters and ingesters.
    
    The IngestionEngine manages the flow of data from adapters (input sources)
    to ingesters (parsers) and coordinates the asynchronous processing of data.
    It handles error conditions and timeouts.
    """
    
    def __init__(self, adapter, ingester):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing IngestionEngine")
        self.adapter = adapter
        self.ingester = ingester
        self.queue = asyncio.Queue()
        self.results = []
        self.stop_flag = asyncio.Event()
        self.processing_errors = []

    async def process(self):
        """Worker that processes items from the queue"""
        try:
            while not (self.stop_flag.is_set() and self.queue.empty()):
                try:
                    # Get with timeout to check for stop_flag periodically
                    raw_data = await asyncio.wait_for(self.queue.get(), timeout=0.5)
                    
                    try:
                        result = self.ingester.parse(raw_data)
                        
                        # Handle case where ingester returns a list of results (batch processing)
                        if isinstance(result, list):
                            self.logger.info(f"Processed batch of {len(result)} items")
                            for item in result:
                                self.results.append(item)
                                # Print summary for each item
                                self.logger.info(f"🧪 Common Data Model Results (Item {len(self.results)}):")
                                self.logger.info(item.summary())
                        else:
                            self.results.append(result)
                            # Print results nicely
                            self.logger.info("🧪 Common Data Model Results:")
                            self.logger.info(result.summary())

                    except PulsePipeError as e:
                        # Handle our custom errors
                        self.logger.error(f"❌ Ingestion error: {e.message}")
                        self.processing_errors.append({
                            "message": e.message,
                            "type": type(e).__name__,
                            "details": e.details
                        })
                    except Exception as e:
                        # Handle other exceptions
                        self.logger.error(f"❌ Unexpected ingestion error: {e}", exc_info=True)
                        self.processing_errors.append({
                            "message": str(e),
                            "type": type(e).__name__
                        })
                    finally:
                        self.queue.task_done()
                        
                except asyncio.TimeoutError:
                    # This is just a timeout from the wait_for, not an error
                    continue
                    
        except asyncio.CancelledError:
            self.logger.debug("Process task was cancelled")
    
    def _get_current_results(self) -> Any:
        """
        Get the current results without waiting for the adapter to finish.
        For continuous mode operation.
        
        Returns:
            Current results list, single result, or None if no real data processed.
            Not returning an empty model helps prevent pipeline restart loops.
        """
        # In continuous mode, we need to return all accumulated results
        # and then clear the results list so it doesn't grow unbounded
        if len(self.results) == 1:
            # Return the single result
            result = self.results[0]
            # Clear results to avoid duplicate processing
            self.results = []
            return result
        elif len(self.results) > 1:
            # Return all results as a list
            results = self.results.copy()
            # Clear results to avoid duplicate processing
            self.results = []
            return results
        else:
            self.logger.debug("No data was processed yet in continuous mode, returning None")
            # Return None instead of empty model to prevent continuous processing loops
            return None
    
    async def run(self, timeout: Optional[float] = 30.0) -> Any:
        """
        Run the ingestion pipeline with adapter and ingester.
        
        Args:
            timeout: Seconds to wait for processing. None for no timeout.
                     Set to a reasonable value for one-time runs.
                     For continuous mode, set to None.
                     
        Returns:
            Processed content, list of processed content, or empty model if nothing processed.
            In continuous mode, returns current results without waiting for adapter to finish.
            
        Raises:
            IngestionEngineError: If there's an error in the ingestion process
            AdapterError: If there's an error in the adapter
            IngesterError: If there's an error in the ingester
        """
        adapter_task = None
        processor_task = None
        
        try:
            # Start the processor task
            processor_task = asyncio.create_task(self.process())
            
            # Start the adapter task
            adapter_task = asyncio.create_task(self.adapter.run(self.queue))
            
            # If continuous mode is disabled in FileWatcherAdapter, adapter_task might complete
            # Wait for the adapter task with a timeout
            try:
                # Handle differently based on mode
                if timeout is not None:
                    # One-time processing mode
                    await asyncio.wait_for(adapter_task, timeout=timeout)
                    self.logger.info("Adapter task completed normally")
                    # Signal processor to stop once queue is empty
                    self.stop_flag.set()
                    # Wait for processor to finish
                    await processor_task
                else:
                    # CONTINUOUS MODE:
                    # For continuous mode, we have to avoid getting stuck
                    # A better approach is to watch for a bit, and then return results
                    self.logger.info("Running in continuous mode - will watch for up to 5 seconds, then process results")
                    
                    # Watch for a short period to detect files
                    try:
                        # Wait with a short timeout to grab initial files
                        await asyncio.wait_for(adapter_task, timeout=5.0)
                        self.logger.info("Adapter task completed (all files processed)")
                    except asyncio.TimeoutError:
                        # Expected in continuous mode
                        self.logger.info("Completed initial file processing, returning results")
                        # Cancel the adapter task - we'll restart it next time
                        adapter_task.cancel()
                    
                    # Make sure processor has time to process everything
                    # Wait a short time for any in-flight processing
                    await asyncio.sleep(0.5)
                    
                    # Return what we've found so far
                    if self.results:
                        count = len(self.results) if isinstance(self.results, list) else 1
                        self.logger.info(f"Processed {count} items, returning for pipeline processing")
                        return self._get_current_results() 
                    else:
                        self.logger.info("No files found to process")
                        return None
            except asyncio.TimeoutError:
                # For one-time processing with timeout, stop after timeout
                self.logger.info(f"Stopping adapter after {timeout} seconds")
                # Signal processor to stop once queue is empty
                self.stop_flag.set()
                # Wait for processor to finish
                await processor_task
            
            # Check for processing errors
            if self.processing_errors:
                error_count = len(self.processing_errors)
                if not self.results:  # No successful results
                    raise IngestionEngineError(
                        f"Processing failed: {error_count} errors occurred with no successful results",
                        details={"errors": self.processing_errors[:10]}  # Include first 10 errors
                    )
                else:
                    # Log a warning but continue since some items were processed successfully
                    self.logger.warning(
                        f"Completed with {error_count} errors and {len(self.results)} successful results"
                    )
            
            # Return results
            if len(self.results) == 1:
                return self.results[0]
            elif len(self.results) > 1:
                return self.results
            else:
                self.logger.warning("No data was processed")
                # Return an empty model based on ingester type
                if hasattr(self.ingester, 'parse') and callable(self.ingester.parse):
                    try:
                        if 'X12' in self.ingester.__class__.__name__:
                            # Create with required fields to avoid validation errors
                            return PulseOperationalContent(
                                transaction_type="UNKNOWN",
                                interchange_control_number="NONE",
                                functional_group_control_number="NONE",
                                organization_id="NONE"
                            )
                        else:
                            # Clinical content doesn't have required fields
                            return PulseClinicalContent()
                    except Exception as e:
                        self.logger.error(f"Error creating empty model: {str(e)}")
                        return None
                return None
                
        except AdapterError:
            # Re-raise adapter errors
            raise
        except IngesterError:
            # Re-raise ingester errors
            raise
        except asyncio.CancelledError:
            raise IngestionEngineError(
                "Ingestion pipeline was cancelled",
                details={"processed_count": len(self.results)}
            )
        except Exception as e:
            # Wrap other exceptions
            raise IngestionEngineError(
                f"Unexpected error in ingestion engine: {str(e)}",
                cause=e
            ) from e
        finally:
            # Cleanup
            if adapter_task and not adapter_task.done():
                adapter_task.cancel()
                try:
                    await adapter_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.logger.error(f"Error cancelling adapter task: {str(e)}")
                    
            if processor_task and not processor_task.done():
                processor_task.cancel()
                try:
                    await processor_task
                except asyncio.CancelledError:
                    pass
                except Exception as e:
                    self.logger.error(f"Error cancelling processor task: {str(e)}")
