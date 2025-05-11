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

# src/pulsepipe/cli/options.py

"""
Common CLI options for PulsePipe commands
"""
import click
import functools
from typing import Callable


def common_options(func: Callable) -> Callable:
    """Common options for all PulsePipe commands."""
    @click.option(
        '--config', '-c',
        'config_path',
        type=click.Path(exists=True, dir_okay=False, resolve_path=True),
        help='Path to pulsepipe.yaml configuration file'
    )
    @click.option(
        '--profile', '-p',
        type=str,
        help='Config profile name (e.g., patient_fhir, lab_hl7)'
    )
    @click.option(
        '--pipeline-id',
        type=str,
        help='Unique identifier for this pipeline run'
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


def logging_options(func: Callable) -> Callable:
    """Logging-related options."""
    @click.option(
        '--log-level', '-l',
        type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], 
                         case_sensitive=False),
        help='Set the logging level'
    )
    @click.option(
        '--json-logs/--no-json-logs',
        default=False,
        help='Output logs in JSON format (for machine consumption)'
    )
    @click.option(
        '--quiet', '-q',
        is_flag=True,
        help='Suppress non-essential output'
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


def pipeline_options(func: Callable) -> Callable:
    """Options specific to pipeline execution."""
    @click.option(
        '--adapter', '-a',
        type=click.Path(exists=True, dir_okay=False, resolve_path=True),
        help='Path to adapter configuration file'
    )
    @click.option(
        '--ingester', '-i',
        type=click.Path(exists=True, dir_okay=False, resolve_path=True),
        help='Path to ingester configuration file'
    )
    @click.option(
        '--dry-run', '-d',
        is_flag=True,
        help='Validate configuration without running the pipeline'
    )
    @click.option(
        '--user-id',
        type=str,
        help='User identifier for logging/auditing (PulsePilot)'
    )
    @click.option(
        '--org-id',
        type=str,
        help='Organization identifier for multi-tenant usage (PulsePilot)'
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper


def output_options(func: Callable) -> Callable:
    """Options for controlling output format and detail."""
    @click.option(
        '--print-model', '-m',
        is_flag=True,
        help='Print the full normalized model after processing'
    )
    @click.option(
        '--summary', '-s',
        is_flag=True,
        help='Print summary of processed data'
    )
    @click.option(
        '--output', '-o',
        type=click.Path(dir_okay=False, resolve_path=True),
        help='Write output to specified file'
    )
    @click.option(
        '--pretty/--compact',
        default=True,
        help='Toggle between pretty-printed and compact output'
    )
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper