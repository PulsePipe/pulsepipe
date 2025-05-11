#!/bin/bash
set -e

# Directory where this script resides
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SYNTH_DIR="$SCRIPT_DIR/synthea"

# Default population count (override with -p)
POPULATION=10

while [[ $# -gt 0 ]]; do
  case $1 in
    -p|--population)
      POPULATION="$2"
      shift; shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

# Ensure Synthea is cloned and built
if [ ! -d "$SYNTH_DIR" ]; then
  echo "Cloning Synthea..."
  git clone https://github.com/synthetichealth/synthea.git "$SYNTH_DIR"
fi

cd "$SYNTH_DIR"

echo "Building Synthea..."
./gradlew build check test

echo "Running Synthea for $POPULATION patients..."
./run_synthea -p "$POPULATION" --exporter.fhir.export=true

echo "Output available at: $SYNTH_DIR/output/fhir"
