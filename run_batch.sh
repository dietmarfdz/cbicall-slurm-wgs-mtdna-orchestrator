#!/usr/bin/env bash

# Do not use `set -e`: a failure launching one sample should not stop
# the remaining samples from being processed.
set -uo pipefail

###############################################################################
# Script: run_batch.sh
#
# Description:
#   Reads a text file containing sample IDs and launches the CBIcall
#   WGS -> mtDNA Slurm orchestrator once for each sample.
#
# Usage:
#   ./run_batch.sh <SAMPLE_LIST> <WORKDIR_BASE> [THREADS]
#
# Arguments:
#   SAMPLE_LIST    Text file containing one sample ID per line.
#   WORKDIR_BASE   Parent directory containing the sample directories.
#   THREADS        Optional number of threads passed to the orchestrator.
#                  Default: 4
#
# Example:
#   ./run_batch.sh examples/samples.example.txt /path/to/WGS 8
#
# Expected sample list format:
#
#   SAMPLE001
#   SAMPLE002
#   SAMPLE003
#
# Empty lines and lines beginning with "#" are ignored.
#
# Output:
#   A timestamped log file is created in the directory from which this
#   script is executed:
#
#       run_batch_YYYYMMDD_HHMMSS.log
#
#   The log records:
#     - start and completion time;
#     - sample IDs submitted successfully;
#     - samples that could not be submitted;
#     - orchestrator exit codes.
###############################################################################

VERSION="1.0"

###############################################################################
# Resolve repository paths
###############################################################################

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAUNCHER="${SCRIPT_DIR}/cbicall_wgs_mtdna_slurm.sh"

###############################################################################
# Logging
###############################################################################

TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
LOGFILE="run_batch_${TIMESTAMP}.log"

log()
{
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOGFILE"
}

###############################################################################
# Header
###############################################################################

log "============================================================"
log "run_batch.sh"
log "Version: $VERSION"
log "============================================================"

###############################################################################
# Validate arguments
###############################################################################

if [[ $# -lt 2 || $# -gt 3 ]]; then
    log "ERROR: incorrect number of arguments."
    log ""
    log "Usage:"
    log "    $0 <SAMPLE_LIST> <WORKDIR_BASE> [THREADS]"
    log ""
    log "Example:"
    log "    $0 examples/samples.example.txt /path/to/WGS 8"
    log ""
    log "No samples were submitted."
    exit 1
fi

SAMPLE_LIST="$1"
WORKDIR_BASE="$2"
THREADS="${3:-4}"

if ! [[ "$THREADS" =~ ^[1-9][0-9]*$ ]]; then
    log "ERROR: THREADS must be a positive integer."
    log "Received: $THREADS"
    exit 1
fi

###############################################################################
# Validate input files and paths
###############################################################################

if [[ ! -f "$SAMPLE_LIST" ]]; then
    log "ERROR: sample list not found:"
    log "    $SAMPLE_LIST"
    log ""
    log "No samples were submitted."
    exit 1
fi

if [[ ! -s "$SAMPLE_LIST" ]]; then
    log "ERROR: sample list is empty:"
    log "    $SAMPLE_LIST"
    log ""
    log "No samples were submitted."
    exit 1
fi

if [[ ! -f "$LAUNCHER" ]]; then
    log "ERROR: orchestrator not found:"
    log "    $LAUNCHER"
    log ""
    log "Expected cbicall_wgs_mtdna_slurm.sh in the same directory as run_batch.sh."
    log "No samples were submitted."
    exit 1
fi

if [[ ! -x "$LAUNCHER" ]]; then
    log "ERROR: orchestrator is not executable:"
    log "    $LAUNCHER"
    log ""
    log "Possible fix:"
    log "    chmod +x \"$LAUNCHER\""
    log ""
    log "No samples were submitted."
    exit 1
fi

if [[ ! -d "$WORKDIR_BASE" ]]; then
    log "ERROR: working directory does not exist:"
    log "    $WORKDIR_BASE"
    log ""
    log "No samples were submitted."
    exit 1
fi

###############################################################################
# Start
###############################################################################

log ""
log "Sample list    : $SAMPLE_LIST"
log "Working dir    : $WORKDIR_BASE"
log "Orchestrator   : $LAUNCHER"
log "Threads        : $THREADS"
log ""
log "Starting batch submission..."
log ""

TOTAL=0
OK=0
FAIL=0

###############################################################################
# Process sample list
###############################################################################

while IFS= read -r SAMPLE || [[ -n "$SAMPLE" ]]; do

    # Remove Windows carriage returns.
    SAMPLE="${SAMPLE//$'\r'/}"

    # Trim leading/trailing whitespace.
    SAMPLE="${SAMPLE#"${SAMPLE%%[![:space:]]*}"}"
    SAMPLE="${SAMPLE%"${SAMPLE##*[![:space:]]}"}"

    # Skip empty lines.
    [[ -z "$SAMPLE" ]] && continue

    # Skip comments.
    [[ "$SAMPLE" == \#* ]] && continue

    ((TOTAL++))

    log "------------------------------------------------------------"
    log "Sample: $SAMPLE"
    log "Running:"
    log "    $LAUNCHER \"$SAMPLE\" \"$WORKDIR_BASE\" \"$THREADS\""

    if "$LAUNCHER" "$SAMPLE" "$WORKDIR_BASE" "$THREADS" >> "$LOGFILE" 2>&1; then
        log "OK: $SAMPLE submitted successfully."
        ((OK++))
    else
        EXIT_CODE=$?
        log "ERROR: failed to submit $SAMPLE."
        log "Orchestrator exit code: $EXIT_CODE"
        log "Review the preceding log lines for details."
        ((FAIL++))
    fi

done < "$SAMPLE_LIST"

###############################################################################
# Summary
###############################################################################

log ""
log "============================================================"
log "SUMMARY"
log "============================================================"
log "Total processed : $TOTAL"
log "Submitted OK    : $OK"
log "Failed          : $FAIL"
log "============================================================"
log "Full log:"
log "    $LOGFILE"
log "============================================================"

###############################################################################
# Exit status
###############################################################################

if [[ $FAIL -gt 0 ]]; then
    exit 1
else
    exit 0
fi
