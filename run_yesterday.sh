#!/bin/bash

# Script to run the weather data processor for yesterday's data
# This script processes daily records and handles the pending queue

# Get yesterday's date components
YESTERDAY=$(date -d "yesterday" '+%Y-%m-%d')
YEAR=$(date -d "yesterday" '+%Y')
MONTH=$(date -d "yesterday" '+%m')
DAY=$(date -d "yesterday" '+%d')

# Remove leading zeros from month and day to avoid octal interpretation issues
MONTH=$(echo $MONTH | sed 's/^0//')
DAY=$(echo $DAY | sed 's/^0//')

echo "Processing data for: $YESTERDAY (Year: $YEAR, Month: $MONTH, Day: $DAY)"

# Set the path to the main.py script
SCRIPT_DIR="$(dirname "$(readlink -f "$0")")"
MAIN_SCRIPT="$SCRIPT_DIR/main.py"

# Run the processor in daily mode for all stations and process pending records
python "$MAIN_SCRIPT" \
  --mode daily \
  --all \
  --process-pending \
  --year "$YEAR" \
  --month "$MONTH" \
  --day "$DAY"

exit_code=$?

if [ $exit_code -eq 0 ]; then
  echo "Successfully processed weather data for $YESTERDAY"
else
  echo "Error processing weather data for $YESTERDAY, exit code: $exit_code"
fi

exit $exit_code