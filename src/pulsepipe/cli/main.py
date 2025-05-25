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
# 
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

# src/pulsepipe/cli/main.py

"""
PulsePipe CLI - Healthcare data pipeline tool
"""

import os
import sys
import warnings
import rich_click as click

# Suppress common warnings for cleaner CLI output
warnings.filterwarnings("ignore", category=FutureWarning, module="spacy")
warnings.filterwarnings("ignore", category=UserWarning, module="torch")
from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.config_loader import load_config
from pulsepipe.cli.banner import get_banner
from rich.pretty import pretty_repr
from rich.console import Console

console = Console()

click.rich_click.SHOW_ARGUMENTS = True
click.rich_click.SHOW_METAVARS_COLUMN = True
click.rich_click.STYLE_USAGE = "bold cyan"
click.rich_click.STYLE_COMMANDS = "bold white"
click.rich_click.STYLE_OPTIONS = "bold yellow"
click.rich_click.STYLE_HELPTEXT_FIRST_LINE = "green"
click.rich_click.STYLE_HELPTEXT = ""
click.rich_click.STYLE_OPTION_DEFAULT = "dim cyan"
click.rich_click.HELP_WIDTH = 100

class PipelineContext:
    """Context information for a pipeline run."""
    
    def __init__(self, pipeline_id=None, profile=None, user_id=None, org_id=None, is_dry_run=False):
        import uuid
        import time
        import socket
        import getpass
        
        # Primary identifiers
        self.pipeline_id = pipeline_id or str(uuid.uuid4())
        self.profile = profile
        
        # Enterprise fields
        self.user_id = user_id
        self.org_id = org_id
        
        # Execution metadata
        self.hostname = socket.gethostname()
        self.username = getpass.getuser()
        self.start_time = time.time()
        
        # Runtime flags
        self.is_dry_run = is_dry_run
    
    def as_dict(self):
        """Convert context to dictionary for logging."""
        return {k: v for k, v in self.__dict__.items() if v is not None}
    
    def get_log_prefix(self):
        """Get a prefix string for log messages."""
        parts = []
        if self.pipeline_id:
            parts.append(f"[{self.pipeline_id[:8]}]")
        if self.profile:
            parts.append(f"[{self.profile}]")
        if self.user_id and self.org_id:
            parts.append(f"[{self.user_id}@{self.org_id}]")
        return " ".join(parts)


# Import CLI options
from pulsepipe.cli.options import common_options, logging_options

# Import commands - do this after LogFactory is initialized
def import_commands():
    from pulsepipe.cli.command.run import run
    from pulsepipe.cli.command.config import config
    from pulsepipe.cli.command.model import model
    return run, config, model


@click.group(invoke_without_command=True)
@click.version_option(package_name="pulsepipe")
@common_options
@logging_options
@click.pass_context
def cli(ctx, config_path, profile, pipeline_id, log_level, json_logs, quiet):
    """PulsePipe: Healthcare data pipeline tool.
    

    Prepare healthcare data for AI through configurable adapters, ingesters, normalizers, chunkers, embedders, and vector database loaders.
    """
    ctx.ensure_object(dict)

    # Create pipeline context
    pipeline_context = PipelineContext(
        pipeline_id=pipeline_id,
        profile=profile
    )
    ctx.obj['context'] = pipeline_context

    # Handle config loading
    try:
        if profile:
            config_dir = os.path.dirname(config_path) if config_path else "config"
            profile_path = os.path.join(config_dir, f"{profile}.yaml")
            if not os.path.exists(profile_path):
                click.echo(f"❌ Profile not found: {profile_path}", err=True)
                sys.exit(1)
            config = load_config(profile_path)
            config_path = profile_path  # so we can print the actual path used
        elif config_path:
            config = load_config(config_path)
        else:
            config_path = "pulsepipe.yaml"
            config = load_config(config_path)
    except Exception as e:
        click.echo(f"❌ Failed to load configuration: {str(e)}", err=True)
        sys.exit(1)

    ctx.obj['config'] = config

    if config.get("logging", {}).get("show_banner", True):
        click.secho(get_banner(), fg='blue')

    # Show config and help if no subcommand is provided
    if ctx.invoked_subcommand is None:
        console.print(f"[bold cyan]📄 Loaded config from:[/bold cyan] {config_path}")
        console.print(pretty_repr(config))
        ctx.info_name = ""
        click.echo(cli.get_help(ctx))
        return

    # Setup logging
    log_config = config.get("logging", {})
    if log_level:
        log_config["level"] = log_level
    if json_logs:
        log_config["format"] = "json"

    LogFactory.init_from_config(
        log_config,
        context=pipeline_context.as_dict() if pipeline_context else None
    )


# Dynamically import commands after LogFactory has been initialized
run, config, model = import_commands()

# Register commands
cli.add_command(run)
cli.add_command(config)
cli.add_command(model)

if __name__ == "__main__":
    cli()