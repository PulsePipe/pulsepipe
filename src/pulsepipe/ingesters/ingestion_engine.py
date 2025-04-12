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

class IngestionEngine:
    def __init__(self, adapter, ingester):
        self.logger = LogFactory.get_logger(__name__)
        self.logger.info("📁 Initializing IngestionEngine")
        self.adapter = adapter
        self.ingester = ingester
        self.queue = asyncio.Queue()
        self.results = []
        self.stop_flag = asyncio.Event()

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

                    except Exception as e:
                        self.logger.error(f"❌ Ingestion error: {e}", exc_info=True)
                    finally:
                        self.queue.task_done()
                        
                except asyncio.TimeoutError:
                    # This is just a timeout from the wait_for, not an error
                    continue
                    
        except asyncio.CancelledError:
            self.logger.debug("Process task was cancelled")
    
    async def run(self, timeout: Optional[float] = 30.0) -> Any:
        """
        Run the ingestion pipeline with adapter and ingester.
        
        Args:
            timeout: Seconds to wait for processing. None for no timeout.
                     Set to a reasonable value for one-time runs.
                     
        Returns:
            Processed content, list of processed content, or empty model if nothing processed.
        """
        try:
            # Start the processor task
            processor = asyncio.create_task(self.process())
            
            # Start the adapter task
            adapter_task = asyncio.create_task(self.adapter.run(self.queue))
            
            # If continuous mode is disabled in FileWatcherAdapter, adapter_task might complete
            # Wait for the adapter task with a timeout
            try:
                await asyncio.wait_for(adapter_task, timeout=timeout)
                self.logger.info("Adapter task completed normally")
            except asyncio.TimeoutError:
                # For continuous watchers, this is expected - we'll stop after timeout
                self.logger.info(f"Stopping adapter after {timeout} seconds")
                adapter_task.cancel()
                
            # Signal processor to stop once queue is empty
            self.stop_flag.set()
            
            # Wait for processor to finish
            await processor
            
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
                            return PulseOperationalContent()
                        else:
                            return PulseClinicalContent()
                    except:
                        return None
                return None
                
        except Exception as e:
            self.logger.exception(f"Error in ingestion engine: {e}")
            raise
        finally:
            # Cleanup
            if not adapter_task.done():
                adapter_task.cancel()
            if not processor.done():
                processor.cancel()
