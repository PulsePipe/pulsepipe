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
# 
# PulsePipe - Open Source ❤️, Healthcare Tough 💪, Builders Only 🛠️
# ------------------------------------------------------------------------------

"""
Configuration management commands for PulsePipe CLI.
"""
import os
import sys
import yaml
import click
from typing import Dict, Any
from pathlib import Path

from pulsepipe.utils.log_factory import LogFactory
from pulsepipe.utils.config_loader import load_config


@click.group()
@click.pass_context
def config(ctx):
    """Manage PulsePipe configuration.
    
    View, validate, and manage configuration profiles.
    """
    pass


@config.command()
@click.option('--profile', '-p', type=str, help='Profile name to validate')
@click.option('--all', 'validate_all', is_flag=True, 
              help='Validate all profiles in config directory')
@click.pass_context
def validate(ctx, profile, validate_all):
    """Validate configuration files.
    
    Examples:
        pulsepipe config validate --profile patient_fhir
        pulsepipe config validate --all
    """
    logger = LogFactory.get_logger("config.validate")
    
    config_dir = Path("config")
    if not config_dir.exists():
        config_dir = Path("src/pulsepipe/config")
    
    if validate_all:
        profiles = [f for f in config_dir.glob("*.yaml") 
                   if f.is_file() and not f.name.startswith("_")]
        
        if not profiles:
            click.echo("No profiles found in config directory")
            return
            
        success_count = 0
        for profile_path in profiles:
            try:
                load_config(str(profile_path))
                click.echo(f"✅ {profile_path.name}: Valid")
                success_count += 1
            except Exception as e:
                click.echo(f"❌ {profile_path.name}: Invalid - {str(e)}")
        
        click.echo(f"\nValidated {len(profiles)} profiles. "
                  f"{success_count} valid, {len(profiles) - success_count} invalid.")
    
    elif profile:
        profile_path = config_dir / f"{profile}.yaml"
        
        if not profile_path.exists():
            click.echo(f"❌ Profile not found: {profile}", err=True)
            return
            
        try:
            config = load_config(str(profile_path))
            click.echo(f"✅ {profile}: Valid")
            
            # Show config components
            components = []
            if 'adapter' in config:
                components.append(f"Adapter: {config['adapter'].get('type', 'unknown')}")
            if 'ingester' in config:
                components.append(f"Ingester: {config['ingester'].get('type', 'unknown')}")
            if 'logging' in config:
                components.append(f"Logging: {config['logging'].get('level', 'INFO')}")
                
            if components:
                click.echo("\nComponents:")
                for comp in components:
                    click.echo(f"  • {comp}")
                    
        except Exception as e:
            click.echo(f"❌ {profile}: Invalid - {str(e)}", err=True)
    
    else:
        click.echo(ctx.get_help())


@config.command()
@click.option('--base', '-b', type=str, default="pulsepipe.yaml",
              help='Base config file (default: pulsepipe.yaml)')
@click.option('--adapter', '-a', type=str, required=True,
              help='Adapter config file')
@click.option('--ingester', '-i', type=str, required=True,
              help='Ingester config file')
@click.option('--name', '-n', type=str, required=True,
              help='Profile name to create')
@click.option('--description', '-d', type=str,
              help='Profile description')
@click.option('--force', '-f', is_flag=True,
              help='Overwrite existing profile')
def create_profile(base, adapter, ingester, name, description, force):
    """Create a unified profile from separate config files.
    
    Examples:
        pulsepipe config create-profile --adapter fhir.yaml --ingester json.yaml --name patient_fhir
    """
    logger = LogFactory.get_logger("config.create_profile")
    
    # Ensure config directory exists
    config_dir = Path("config")
    if not config_dir.exists():
        config_dir = Path("src/pulsepipe/config")
        if not config_dir.exists():
            config_dir.mkdir(parents=True)
    
    # Check if profile already exists
    profile_path = config_dir / f"{name}.yaml"
    if profile_path.exists() and not force:
        click.echo(f"❌ Profile already exists: {name}. Use --force to overwrite.", err=True)
        return
    
    try:
        # Load component configs
        base_config = load_config(base)
        adapter_config = load_config(adapter)
        ingester_config = load_config(ingester)
        
        # Create unified profile
        profile_config = {
            "profile": {
                "name": name,
                "description": description or f"{name} profile",
                "created_at": "auto-generated"
            },
            "adapter": adapter_config.get("adapter", {}),
            "ingester": ingester_config.get("ingester", {}),
            "logging": base_config.get("logging", {})
        }
        
        # Write profile
        with open(profile_path, 'w') as f:
            yaml.dump(profile_config, f, default_flow_style=False, sort_keys=False)
        
        click.echo(f"✅ Created profile: {name} at {profile_path}")
        
    except Exception as e:
        logger.error(f"Failed to create profile {name}: {str(e)}", exc_info=True)
        click.echo(f"❌ Error creating profile: {str(e)}", err=True)


@config.command()
@click.option('--config-dir', type=click.Path(exists=True, file_okay=False),
              help='Configuration directory')
@click.pass_context
def list(ctx, config_dir):
    """List available configuration profiles.
    
    Examples:
        pulsepipe config list
    """
    if not config_dir:
        config_dir = "config"
        if not os.path.exists(config_dir):
            config_dir = "src/pulsepipe/config"
    
    try:
        profiles = []
        for filename in os.listdir(config_dir):
            if filename.endswith(".yaml") and not filename.startswith("_"):
                # Only consider files that might be profiles
                if filename in ["adapter.yaml", "ingester.yaml", "pulsepipe.yaml"]:
                    continue
                    
                profile_path = os.path.join(config_dir, filename)
                try:
                    profile_data = load_config(profile_path)
                    
                    # Check if it's a valid profile with adapter and ingester
                    if "adapter" in profile_data and "ingester" in profile_data:
                        profile_name = filename.replace(".yaml", "")
                        description = (profile_data.get("profile", {}).get("description", "")
                                      or f"{profile_data['adapter'].get('type', 'unknown')} + "
                                      f"{profile_data['ingester'].get('type', 'unknown')}")
                        
                        profiles.append((profile_name, description))
                except Exception:
                    # Skip invalid profiles
                    pass
        
        if profiles:
            click.echo("Available profiles:")
            for name, desc in sorted(profiles):
                click.echo(f"  • {name}: {desc}")
        else:
            click.echo("No profiles found. Use 'pulsepipe config create-profile' to create one.")
            
    except Exception as e:
        click.echo(f"❌ Error listing profiles: {str(e)}", err=True)