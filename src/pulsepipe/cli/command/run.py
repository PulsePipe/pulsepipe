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

# src/pulsepipe/cli/command/run.py

"""
Run command implementation for single pipeline execution.
"""

import os
import sys
import json
import asyncio
import click
from pathlib import Path
from typing import Dict, Any, Optional

import signal

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.errors import (
    PulsePipeError, ConfigurationError, MissingConfigurationError,
    AdapterError, IngesterError, IngestionEngineError, PipelineError,
    ChunkerError, FileSystemError, CLIError
)
from pulsepipe.cli.options import output_options
from pulsepipe.pipelines.runner import PipelineRunner

logger = LogFactory.get_logger(__name__)

# Improved signal handler for graceful shutdown

def run_async_with_shutdown(coro, runner=None):
    """Run an async coroutine with proper shutdown handling."""
    import asyncio
    import signal
    import os
    import sys
    
    # Create a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    # Track whether we're shutting down
    is_shutting_down = False
    
    # Create main task
    main_task = None
    
    def force_exit(signum, frame):
        print("\n⚠️ Force exiting due to multiple interrupt signals...")
        os._exit(1)  # Force exit the process
    
    async def shutdown_procedure():
        """Gracefully shut down all running pipelines"""
        if runner:
            print("\n🛑 Stopping running pipeline...")
            # For the single pipeline version, we don't need to stop multiple pipelines
            print("✅ Pipeline stopped")
    
    def signal_handler(signum, frame):
        nonlocal is_shutting_down
        if is_shutting_down:
            # If already shutting down and got another signal, force exit
            force_exit(signum, frame)
            return
            
        is_shutting_down = True
        print(f"\n🛑 Shutdown requested (signal {signum})...")
        
        # Schedule shutdown procedure
        if runner:
            shutdown_task = asyncio.run_coroutine_threadsafe(shutdown_procedure(), loop)
        
        # Cancel the main task if it exists
        if main_task and not main_task.done():
            loop.call_soon_threadsafe(main_task.cancel)
        
        # Schedule loop stop if it's running
        if loop.is_running():
            loop.call_soon_threadsafe(loop.stop)
    
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    try:
        # Define the main task
        main_task = loop.create_task(coro)
        
        # Run until complete
        result = loop.run_until_complete(main_task)
        return result
    except asyncio.CancelledError:
        print("✅ Operation cancelled gracefully")
        return {"success": False, "errors": ["Operation cancelled by user"]}
    except KeyboardInterrupt:
        print("✅ Operation interrupted by keyboard")
        return {"success": False, "errors": ["Operation interrupted by user"]}
    except Exception as e:
        print(f"❌ Error during execution: {e}")
        return {"success": False, "errors": [str(e)]}
    finally:
        try:
            # Cancel any remaining tasks
            tasks = asyncio.all_tasks(loop)
            for task in tasks:
                task.cancel()
            
            # Allow tasks to finalize
            if tasks:
                loop.run_until_complete(asyncio.gather(*tasks, return_exceptions=True))
            
            # Finally, close the loop
            loop.close()
        except Exception as e:
            print(f"Error during cleanup: {e}")
        
        # Reset signal handlers to default
        signal.signal(signal.SIGINT, signal.SIG_DFL)
        signal.signal(signal.SIGTERM, signal.SIG_DFL)


def display_error(error: PulsePipeError, verbose: bool = False):
    """Display error information in a structured, helpful format."""
    click.secho(f"❌ Error: {error.message}", fg='red', bold=True)
    
    # Show additional error context if available
    if error.details and verbose:
        click.echo("\nError details:")
        for key, value in error.details.items():
            click.echo(f"  {key}: {value}")
    
    # Show original cause if available and in verbose mode
    if error.cause and verbose:
        click.echo(f"\nCaused by: {type(error.cause).__name__}: {str(error.cause)}")
    
    # Provide hints based on error type
    if isinstance(error, ConfigurationError):
        click.echo("\nSuggestions:")
        click.echo("  • Check your configuration file for errors")
        click.echo("  • Run 'pulsepipe config validate' to validate your configuration")
    elif isinstance(error, AdapterError):
        click.echo("\nSuggestions:")
        click.echo("  • Verify the adapter configuration is correct")
        click.echo("  • Check that input sources are accessible")
    elif isinstance(error, IngesterError):
        click.echo("\nSuggestions:")
        click.echo("  • Verify that input data format matches the configured ingester")
        click.echo("  • Check for malformed or invalid input data")
    elif isinstance(error, ChunkerError):
        click.echo("\nSuggestions:")
        click.echo("  • Check the chunker configuration")
        click.echo("  • Verify that the data model is compatible with the chunker")


def find_profile_path(profile_name: str) -> Optional[str]:
    """Find the profile configuration file in the appropriate directories."""
    # Define possible locations in priority order
    possible_locations = [
        # 1. Check in ./config/ next to the binary
        os.path.join("config", f"{profile_name}.yaml"),
        # 2. Check in the current directory
        f"{profile_name}.yaml",
        # 3. Check in the location relative to the script directory (for development)
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "..", "config", f"{profile_name}.yaml"),
        # 4. As a fallback, try the src path
        os.path.join("src", "pulsepipe", "config", f"{profile_name}.yaml"),
    ]
    
    # Try each location
    for location in possible_locations:
        # Always normalize path separators for Windows
        if sys.platform == 'win32':
            normalized_location = location.replace('\\', '/')
        else:
            normalized_location = location
        
        # Check if the file exists
        if os.path.exists(normalized_location):
            return normalized_location
        
        # On Windows, also try with the original separators
        if sys.platform == 'win32' and os.path.exists(location):
            return location.replace('\\', '/')
    
    # Return None if not found
    return None


@click.command()
@click.option('--adapter', '-a', type=click.Path(exists=True, dir_okay=False), help="Adapter config YAML")
@click.option('--ingester', '-i', type=click.Path(exists=True, dir_okay=False), help="Ingester config YAML")
@click.option('--chunker', '-c', type=click.Path(exists=True, dir_okay=False), help="Chunker config YAML")
@click.option('--embedding', '-e', type=click.Path(exists=True, dir_okay=False), help="Embedding config YAML")
@click.option('--vectorstore', '-vs', type=click.Path(exists=True, dir_okay=False), help="Vector store config YAML")
@click.option('--profile', '-p', type=str, help="Profile name to use (e.g., new_profile)")
@click.option('--timeout', type=float, default=None, help="Timeout for pipeline execution in seconds")
@click.option('--continuous/--one-time', 'continuous_mode', default=None)
@click.option('--concurrent', '-cc', is_flag=True, help="Run pipeline stages concurrently")
@click.option('--watch', '-w', is_flag=True, help="Watch mode - keep running and process files as they arrive")
@click.option('--verbose', '-v', is_flag=True, help="Show detailed error information")
@output_options
@click.pass_context
def run(ctx, adapter, ingester, chunker, embedding, vectorstore, profile, timeout,
        continuous_mode, concurrent, watch, print_model, 
        summary, output, pretty, verbose):
    """Run a data processing pipeline.
    
    Process healthcare data through configurable adapter, ingester, chunker, embedding and vectorstore stages.
    """
    logger = LogFactory.get_logger("cli.run")
    
    # Create the pipeline runner
    runner = PipelineRunner()
    
    try:
        # Handle profile-based execution
        if profile:
            # Find the profile path
            profile_path = find_profile_path(profile)
            
            if not profile_path:
                raise MissingConfigurationError(
                    f"Profile not found: {profile}",
                    details={
                        "searched_locations": [
                            "./config/", 
                            "./", 
                            "src/pulsepipe/config/"
                        ]
                    }
                )
            
            try:
                profile_config = load_config(profile_path)
            except Exception as e:
                raise ConfigurationError(
                    f"Failed to load profile configuration: {profile}",
                    details={"profile_path": profile_path},
                    cause=e
                )
                
            # Check if we have the required configurations
            if not ("adapter" in profile_config and "ingester" in profile_config):
                raise ConfigurationError(
                    f"Profile {profile} is missing adapter or ingester configuration",
                    details={"profile_path": profile_path}
                )
                
            # Apply continuous mode override if specified
            if continuous_mode is not None and "adapter" in profile_config:
                if profile_config["adapter"].get("type") == "file_watcher":
                    profile_config["adapter"]["continuous"] = continuous_mode
                    
            click.echo(f"📋 Using profile: {profile} from {profile_path}")
            
            # Check for chunker, embedding, and vectorstore configurations
            has_chunker = "chunker" in profile_config
            has_embedding = "embedding" in profile_config
            has_vectorstore = "vectorstore" in profile_config
            
            # Warn about missing pipeline stages
            if not has_chunker:
                click.echo("⚠️ Warning: Profile does not include chunker configuration")
            if not has_embedding:
                click.echo("⚠️ Warning: Profile does not include embedding configuration")
            if not has_vectorstore:
                click.echo("⚠️ Warning: Profile does not include vectorstore configuration")
            
            # Run the pipeline with improved shutdown handling
            result = run_async_with_shutdown(
                runner.run_pipeline(
                    config=profile_config,
                    name=profile,
                    output_path=output,
                    summary=summary,
                    print_model=print_model,
                    pretty=pretty,
                    verbose=verbose,
                    concurrent=concurrent,
                    watch=watch,
                    timeout=timeout
                ),
                runner=runner
            )
            
            # Check for success
            if not result.get("success", False):
                logger.error(f"Pipeline execution failed: {result.get('errors')}")
                ctx.exit(1)
                
        # Handle explicit component config execution
        elif adapter and ingester:
            # Load adapter config
            try:
                adapter_config = load_config(adapter).get('adapter', {})
                if not adapter_config:
                    raise ConfigurationError(
                        f"File '{adapter}' does not contain adapter configuration",
                        details={"adapter_file": adapter}
                    )
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(
                    f"Failed to load adapter configuration: {adapter}",
                    details={"adapter_file": adapter},
                    cause=e
                )
                
            # Load ingester config
            try:
                ingester_config = load_config(ingester).get('ingester', {})
                if not ingester_config:
                    raise ConfigurationError(
                        f"File '{ingester}' does not contain ingester configuration",
                        details={"ingester_file": ingester}
                    )
            except Exception as e:
                if isinstance(e, ConfigurationError):
                    raise
                raise ConfigurationError(
                    f"Failed to load ingester configuration: {ingester}",
                    details={"ingester_file": ingester},
                    cause=e
                )
                
            # Apply continuous mode override if specified
            if continuous_mode is not None and adapter_config.get("type") == "file_watcher":
                adapter_config["continuous"] = continuous_mode
                
            # Combine configs into a single pipeline config
            combined_config = {
                "adapter": adapter_config,
                "ingester": ingester_config
            }
            
            # Add chunker config if provided
            if chunker:
                try:
                    chunker_config = load_config(chunker).get('chunker', {})
                    if not chunker_config:
                        click.echo(f"⚠️ Warning: File '{chunker}' does not contain chunker configuration")
                    else:
                        combined_config["chunker"] = chunker_config
                except Exception as e:
                    click.echo(f"⚠️ Warning: Failed to load chunker configuration: {str(e)}")
            
            # Add embedding config if provided
            if embedding:
                try:
                    embedding_config = load_config(embedding).get('embedding', {})
                    if not embedding_config:
                        click.echo(f"⚠️ Warning: File '{embedding}' does not contain embedding configuration")
                    else:
                        combined_config["embedding"] = embedding_config
                except Exception as e:
                    click.echo(f"⚠️ Warning: Failed to load embedding configuration: {str(e)}")
            
            # Add vectorstore config if provided
            if vectorstore:
                try:
                    vectorstore_config = load_config(vectorstore).get('vectorstore', {})
                    if not vectorstore_config:
                        click.echo(f"⚠️ Warning: File '{vectorstore}' does not contain vectorstore configuration")
                    else:
                        combined_config["vectorstore"] = vectorstore_config
                except Exception as e:
                    click.echo(f"⚠️ Warning: Failed to load vectorstore configuration: {str(e)}")
            
            click.echo(f"📋 Using adapter from {adapter} and ingester from {ingester}")
            if chunker:
                click.echo(f"📋 Using chunker from {chunker}")
            if embedding:
                click.echo(f"📋 Using embedding from {embedding}")
            if vectorstore:
                click.echo(f"📋 Using vectorstore from {vectorstore}")
            
            # Log warnings for missing stages
            if not chunker:
                click.echo("⚠️ Warning: No chunker configuration provided")
            if not embedding:
                click.echo("⚠️ Warning: No embedding configuration provided")
            if not vectorstore:
                click.echo("⚠️ Warning: No vectorstore configuration provided")
            
            # Run the pipeline with improved shutdown handling
            result = run_async_with_shutdown(
                runner.run_pipeline(
                    config=combined_config,
                    name="cli_direct",
                    output_path=output,
                    summary=summary,
                    print_model=print_model,
                    pretty=pretty,
                    verbose=verbose,
                    concurrent=concurrent,
                    watch=watch,
                    timeout=timeout
                ),
                runner=runner
            )
            
            # Check for success
            if not result.get("success", False):
                logger.error(f"Pipeline execution failed: {result.get('errors')}")
                ctx.exit(1)
                
        # No configuration provided
        else:
            raise CLIError(
                "You must specify either --profile, or both --adapter and --ingester",
                details={
                    "profile": profile,
                    "adapter": adapter,
                    "ingester": ingester
                }
            )
            
    except PulsePipeError as e:
        # Display errors in a user-friendly way
        display_error(e, verbose=verbose)
        ctx.exit(1)
    except Exception as e:
        # Wrap unexpected exceptions
        error = CLIError(
            f"Unexpected error in command execution: {str(e)}",
            cause=e
        )
        display_error(error, verbose=verbose)
        ctx.exit(1)