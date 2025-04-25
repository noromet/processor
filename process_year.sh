#!/bin/bash

# Process all stations for all dates in a given year
# Usage: ./process_year.sh <year> [--dry-run] [--skip-daily] [--skip-monthly]

set -e

# Default values
YEAR=""
DRY_RUN=""
SKIP_DAILY=false
SKIP_MONTHLY=false
LOG_DIR="logs"

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --dry-run) DRY_RUN="--dry-run"; shift ;;
        --skip-daily) SKIP_DAILY=true; shift ;;
        --skip-monthly) SKIP_MONTHLY=true; shift ;;
        --help|-h) 
            echo "Usage: ./process_year.sh <year> [--dry-run] [--skip-daily] [--skip-monthly]"
            exit 0
            ;;
        *) 
            if [[ -z "$YEAR" ]]; then
                # Check if input is a valid year (4 digits)
                if [[ $1 =~ ^[0-9]{4}$ ]]; then
                    YEAR=$1
                else
                    echo "Error: Invalid year format. Please provide a 4-digit year."
                    exit 1
                fi
            else
                echo "Error: Unknown parameter: $1"
                echo "Usage: ./process_year.sh <year> [--dry-run] [--skip-daily] [--skip-monthly]"
                exit 1
            fi
            shift 
            ;;
    esac
done

# Check if year is provided
if [[ -z "$YEAR" ]]; then
    echo "Error: Year parameter is required"
    echo "Usage: ./process_year.sh <year> [--dry-run] [--skip-daily] [--skip-monthly]"
    exit 1
fi

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

echo "Processing all stations for year $YEAR"
if [[ -n "$DRY_RUN" ]]; then
    echo "Running in DRY RUN mode"
fi

# Get days in month
days_in_month() {
    cal $2 $1 | awk 'NF {DAYS = $NF}; END {print DAYS}'
}

# Process daily records for each day of the year
if [[ "$SKIP_DAILY" == false ]]; then
    echo "Starting daily processing..."
    
    for MONTH in {1..12}; do
        DAYS=$(days_in_month $YEAR $MONTH)
        
        for DAY in $(seq 1 $DAYS); do
            echo "Processing daily records for $YEAR-$(printf "%02d" $MONTH)-$(printf "%02d" $DAY)"
            python main.py \
                --all \
                --mode daily \
                --year $YEAR \
                --month $MONTH \
                --day $DAY \
                $DRY_RUN \
                >> "$LOG_DIR/daily_${YEAR}_$(printf "%02d" $MONTH)_$(printf "%02d" $DAY).log" 2>&1
            
            EXIT_CODE=$?
            if [[ $EXIT_CODE -ne 0 ]]; then
                echo "Error processing daily records for $YEAR-$(printf "%02d" $MONTH)-$(printf "%02d" $DAY). Exit code: $EXIT_CODE"
            else
                echo "Successfully processed daily records for $YEAR-$(printf "%02d" $MONTH)-$(printf "%02d" $DAY)"
            fi
        done
    done
    
    echo "Daily processing complete."
fi

# Process monthly records for each month of the year
if [[ "$SKIP_MONTHLY" == false ]]; then
    echo "Starting monthly processing..."
    
    for MONTH in {1..12}; do
        echo "Processing monthly records for $YEAR-$(printf "%02d" $MONTH)"
        python main.py \
            --all \
            --mode monthly \
            --year $YEAR \
            --month $MONTH \
            $DRY_RUN \
            >> "$LOG_DIR/monthly_${YEAR}_$(printf "%02d" $MONTH).log" 2>&1
        
        EXIT_CODE=$?
        if [[ $EXIT_CODE -ne 0 ]]; then
            echo "Error processing monthly records for $YEAR-$(printf "%02d" $MONTH). Exit code: $EXIT_CODE"
        else
            echo "Successfully processed monthly records for $YEAR-$(printf "%02d" $MONTH)"
        fi
    done
    
    echo "Monthly processing complete."
fi

echo "Processing for year $YEAR completed."
