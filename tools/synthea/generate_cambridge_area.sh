#!/bin/bash
set -e

# Total patients to generate
TOTAL=2000

# Towns with approximate weightings (out of 1000)
# Adjust weights based on relative population & proximity
declare -A TOWN_WEIGHTS=(
  [Cambridge]=250
  [Somerville]=180
  [Boston]=170
  [Arlington]=100
  [Belmont]=60
  [Watertown]=60
  [Medford]=60
  [Brookline]=40
  [Newton]=40
  [Chelsea]=30
  [Everett]=30
  [Malden]=30
  [Waltham]=20
  [Revere]=20
  [Winthrop]=10
  [Lexington]=20
  [Needham]=10
  [Dedham]=10
  [Quincy]=20
  [Melrose]=10
  [Winchester]=10
  [Woburn]=10
  [Allston]=10
  [Brighton]=10
  [East\ Boston]=10
)

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SYNTH_DIR="$SCRIPT_DIR/synthea"

# Clone/build Synthea if not present
if [ ! -d "$SYNTH_DIR" ]; then
  echo "Cloning Synthea..."
  git clone https://github.com/synthetichealth/synthea.git "$SYNTH_DIR"
fi

cd "$SYNTH_DIR"
echo "Building Synthea..."
./gradlew build check test

# Copy fixed config
cp "$SCRIPT_DIR/synthea.properties" "$SYNTH_DIR/src/main/resources/synthea.properties"

# Run Synthea for each town
echo "Generating $TOTAL synthetic patients..."
for town in "${!TOWN_WEIGHTS[@]}"; do
  weight=${TOWN_WEIGHTS[$town]}
  num_patients=$(( TOTAL * weight / 1000 ))
  if (( num_patients > 0 )); then
    echo "  -> $town: $num_patients patients"
    ./run_synthea -p "$num_patients" Massachusetts "$town"
  fi
done

echo "✅ Generation complete."
echo "FHIR output is in: $SYNTH_DIR/output/fhir/"
