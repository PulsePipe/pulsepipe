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

# src/pulsepipe/cli/command/run.py

"""
Run command implementation using the new pipeline architecture.
"""

import os
import sys
import json
import asyncio
import click
from pathlib import Path
from typing import Dict, Any, Optional, List, Union

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.config_loader import load_config
from pulsepipe.utils.errors import (
    PulsePipeError, ConfigurationError, MissingConfigurationError,
    AdapterError, IngesterError, IngestionEngineError, PipelineError,
    ChunkerError, FileSystemError, CLIError
)
from pulsepipe.cli.options import output_options
from pulsepipe.pipelines.runner import PipelineRunner
import threading
import signal


def run_async_with_shutdown(coro):
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    stop_event = asyncio.Event()

    def handle_shutdown(signum, frame):
        print(f"\n🛑 Caught signal {signum}, setting stop_event")
        loop.call_soon_threadsafe(stop_event.set)

    # ✅ Portable signal registration — works on Windows and Linux
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    async def main():
        main_task = asyncio.create_task(coro)
        stop_task = asyncio.create_task(stop_event.wait())

        done, pending = await asyncio.wait(
            [main_task, stop_task],
            return_when=asyncio.FIRST_COMPLETED,
        )

        if stop_task in done:
            print("🛑 Shutdown requested, cancelling main task...")
            main_task.cancel()
            try:
                await main_task
            except asyncio.CancelledError:
                pass
            return {"success": False, "errors": ["Pipeline cancelled by user"]}

        return await main_task

    try:
        return loop.run_until_complete(main())
    finally:
        loop.close()


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
        if os.path.exists(location):
            return location
    
    # Return None if not found
    return None


@click.command()
@click.option('--adapter', '-a', type=click.Path(exists=True, dir_okay=False), help="Adapter config YAML")
@click.option('--ingester', '-i', type=click.Path(exists=True, dir_okay=False), help="Ingester config YAML")
@click.option('--profile', '-p', type=str, help="Profile name to use (e.g., new_profile)")
@click.option('--pipeline-config', '-pc', type=click.Path(exists=True, dir_okay=False), help="Pipeline config YAML")
@click.option('--pipeline', '-n', multiple=True, help="Specific pipeline names to run")
@click.option('--all', 'run_all', is_flag=True, help="Run all pipelines in config")
@click.option('--timeout', type=float, default=30.0, help="Timeout for file-based processing")
@click.option('--continuous/--one-time', 'continuous_mode', default=None)
@click.option('--verbose', '-v', is_flag=True, help="Show detailed error information")
@output_options
@click.pass_context
def run(ctx, adapter, ingester, profile, pipeline_config, pipeline, run_all, timeout,
        continuous_mode, print_model, summary, output, pretty, verbose):
    """Run a data processing pipeline.
    
    Process healthcare data through configurable adapters and ingesters.
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
            
            # Run the pipeline
            result = run_async_with_shutdown(runner.run_pipeline(
                config=profile_config,
                name=profile,
                output_path=output,
                summary=summary,
                print_model=print_model,
                pretty=pretty,
                verbose=verbose
            ))
            
            # Check for success
            if not result.get("success", False):
                logger.error(f"Pipeline execution failed: {result.get('errors')}")
                ctx.exit(1)
                
        # Handle pipeline config execution
        elif pipeline_config:
            click.echo(f"📋 Using pipeline config: {pipeline_config}")
            
            # Set up kwargs for pipeline runner
            kwargs = {
                "summary": summary,
                "print_model": print_model,
                "pretty": pretty,
                "verbose": verbose,
                "output_path": output
            }

            if continuous_mode is not None:
                kwargs["continuous_override"] = continuous_mode
                
            results = run_async_with_shutdown(runner.run_multiple_pipelines(
                config_path=pipeline_config,
                pipeline_names=list(pipeline) if pipeline else None,
                run_all=run_all,
                **kwargs
            ))
            
            # Check for success - we consider it successful if at least one pipeline succeeded
            success = any(r["result"].get("success", False) for r in results if r.get("result"))
            
            if not success:
                logger.error("All pipelines failed")
                ctx.exit(1)
                
            # Display summary of results
            if len(results) > 1:
                click.echo("\nPipeline Execution Summary:")
                for result in results:
                    name = result["name"]
                    success = result["result"].get("success", False)
                    status = "✅ Success" if success else "❌ Failed"
                    click.echo(f"{status}: {name}")
                    
        # Handle explicit adapter/ingester config execution
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
                "ingester": ingester_config,
                # Include chunker if it exists in the ingester config
                "chunker": ingester_config.get("chunker", {})
            }
            
            click.echo(f"📋 Using adapter from {adapter} and ingester from {ingester}")
            
            # Run the pipeline
            result = run_async_with_shutdown(runner.run_pipeline(
                config=combined_config,
                name="cli_direct",
                output_path=output,
                summary=summary,
                print_model=print_model,
                pretty=pretty,
                verbose=verbose
            ))
            
            # Check for success
            if not result.get("success", False):
                logger.error(f"Pipeline execution failed: {result.get('errors')}")
                ctx.exit(1)
                
        # No configuration provided
        else:
            raise CLIError(
                "You must specify either --profile, --pipeline-config, or both --adapter and --ingester",
                details={
                    "profile": profile,
                    "pipeline_config": pipeline_config,
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
