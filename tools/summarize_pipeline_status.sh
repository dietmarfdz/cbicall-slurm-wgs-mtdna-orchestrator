#!/usr/bin/env bash

set -euo pipefail

# ============================================================
# WGS → MIT → CLEANUP PIPELINE STATUS SUMMARY
# ============================================================
#
# v3:
#   Adapted to orchestrator v6. Explicitly distinguishes
#   FAIL_MIT_LOW_SIGNAL from other MIT failures and adds to
#   jobs_by_sample.tsv the mitochondrial BAM metrics/validation results
#   (BAM, BAI, quickcheck, mapped reads, mean depth and coverage >=1x),
#   the MIT failure reason and the WGS_BAM_CLEANUP_CANDIDATE flag.
#
# v2:
#   Adds generation of jobs_by_sample.tsv, linking each sample
#   to its WGS, CHECK_WGS, MIT, CHECK_MIT and CLEANUP jobs, including
#   current Slurm state and, when applicable, pending/failure reason.
#
# Summarizes the status of all samples launched through
# the CBIcall WGS → MIT → CLEANUP orchestrator.
#
# IMPORTANT:
#   Run from the directory containing the individual
#   sample directories.
#
# Only the MOST RECENT run recorded in each
# pipeline_status.log is analyzed.
#
# OUTPUT:
#   paths_pipeline_status_logs
#   pipeline_status_summary.log
#   jobs_by_sample.tsv
#
# ============================================================

PATHS_FILE="paths_pipeline_status_logs"
OUT="pipeline_status_summary.log"
JOBS_OUT="jobs_by_sample.tsv"

find . -type f -name "pipeline_status.log" | sort > "$PATHS_FILE"

TMP_RESULTS=$(mktemp)
TMP_JOBS=$(mktemp)

trap 'rm -f "$TMP_RESULTS" "$TMP_JOBS"' EXIT

TOTAL=0
OK_COUNT=0
PROCESSING_COUNT=0
FAIL_COUNT=0
LOW_SIGNAL_COUNT=0


# ============================================================
# FUNCTIONS
# ============================================================

# Returns the Slurm state of a job using sacct
slurm_state() {

    local JOB_ID="$1"

    if [[ -z "$JOB_ID" ]]; then
        echo ""
        return
    fi

    sacct -j "$JOB_ID" -X \
        --noheader \
        --format=State 2>/dev/null |
        awk 'NF {print $1; exit}'
}


# Returns success when a Slurm state represents a failure
is_failed_state() {

    case "$1" in
        FAILED|CANCELLED|TIMEOUT|OUT_OF_MEMORY|NODE_FAIL|PREEMPTED|BOOT_FAIL|DEADLINE)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}


# Returns:
#
# STATE|REASON
#
# Queries squeue first because it provides the reason
# for pending jobs.
#
# If the job is no longer in squeue, query sacct.
job_state_reason() {

    local JOB_ID="$1"

    if [[ -z "$JOB_ID" ]]; then
        echo "NA|NA"
        return
    fi

    local SQ

    SQ=$(
        squeue -h \
            -j "$JOB_ID" \
            -o "%T|%R" 2>/dev/null |
        head -1 || true
    )

    if [[ -n "$SQ" ]]; then
        echo "$SQ"
        return
    fi

    local STATE

    STATE=$(
        sacct -j "$JOB_ID" -X \
            --noheader \
            --format=State 2>/dev/null |
        awk 'NF {print $1; exit}'
    )

    [[ -z "$STATE" ]] && STATE="UNKNOWN"

    echo "${STATE}|-"
}


# ============================================================
# jobs_by_sample.tsv HEADER
# ============================================================

echo -e \
"SAMPLE\tWGS_JOB\tWGS_STATE\tWGS_REASON\tCHECK_WGS_JOB\tCHECK_WGS_STATE\tCHECK_WGS_REASON\tMIT_JOB\tMIT_STATE\tMIT_REASON\tCHECK_MIT_JOB\tCHECK_MIT_STATE\tCHECK_MIT_REASON\tCLEANUP_JOB\tCLEANUP_STATE\tCLEANUP_REASON\tMTDNA_BAM_CHECK\tMTDNA_BAI_CHECK\tMTDNA_BAI_USABLE\tMTDNA_BAM_QUICKCHECK\tMTDNA_MAPPED_READS\tMTDNA_MEAN_DEPTH\tMTDNA_COVERED_1X_PCT\tMIT_FAIL_REASON\tWGS_BAM_CLEANUP_CANDIDATE" \
> "$TMP_JOBS"


# ============================================================
# SAMPLE LOOP
# ============================================================

while read -r LOG; do

    [[ -z "$LOG" ]] && continue

    TOTAL=$((TOTAL + 1))

    SAMPLE=$(basename "$(dirname "$LOG")")


    # --------------------------------------------------------
    # Retrieve ONLY the most recent pipeline run
    #
    # Each run starts with:
    #
    # ===== date =====
    #
    # --------------------------------------------------------

    CURRENT_RUN=$(
        awk '
            /^===== / {
                block=""
            }
            {
                block = block $0 ORS
            }
            END {
                printf "%s", block
            }
        ' "$LOG"
    )


    # --------------------------------------------------------
    # Retrieve JOB IDs
    # --------------------------------------------------------

    WGS_JOB=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^WGS_JOB:/ {print $2; exit}'
    )

    CHECK_WGS_JOB=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^CHECK_WGS_JOB:/ {print $2; exit}'
    )

    MIT_JOB=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MIT_JOB:/ {print $2; exit}'
    )

    CHECK_MIT_JOB=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^CHECK_MIT_JOB:/ {print $2; exit}'
    )

    CLEANUP_JOB=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^CLEANUP_JOB:/ {print $2; exit}'
    )


    # --------------------------------------------------------
    # Retrieve state + reason for each job
    # --------------------------------------------------------

    IFS='|' read -r WGS_STATE WGS_REASON \
        <<< "$(job_state_reason "$WGS_JOB")"

    IFS='|' read -r CHECK_WGS_STATE CHECK_WGS_REASON \
        <<< "$(job_state_reason "$CHECK_WGS_JOB")"

    IFS='|' read -r MIT_STATE MIT_REASON \
        <<< "$(job_state_reason "$MIT_JOB")"

    IFS='|' read -r CHECK_MIT_STATE CHECK_MIT_REASON \
        <<< "$(job_state_reason "$CHECK_MIT_JOB")"

    IFS='|' read -r CLEANUP_STATE CLEANUP_REASON \
        <<< "$(job_state_reason "$CLEANUP_JOB")"


    # --------------------------------------------------------
    # Retrieve mtDNA / MIT-specific information (v6)
    # --------------------------------------------------------

    MTDNA_BAM_CHECK=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_BAM_CHECK:/ {split($2,a," "); value=a[1]} END {print value}'
    )

    MTDNA_BAI_CHECK=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_BAI_CHECK:/ {split($2,a," "); value=a[1]} END {print value}'
    )

    MTDNA_BAI_USABLE=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_BAI_USABLE:/ {value=$2} END {print value}'
    )

    MTDNA_BAM_QUICKCHECK=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_BAM_QUICKCHECK:/ {split($2,a," "); value=a[1]} END {print value}'
    )

    MTDNA_MAPPED_READS=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_MAPPED_READS:/ {value=$2} END {print value}'
    )

    MTDNA_MEAN_DEPTH=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_MEAN_DEPTH:/ {value=$2} END {print value}'
    )

    MTDNA_COVERED_1X_PCT=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_COVERED_1X_PCT:/ {value=$2} END {print value}'
    )

    MIT_FAIL_REASON=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MIT_FAIL_REASON:/ {value=$2} END {print value}'
    )

    WGS_BAM_CLEANUP_CANDIDATE=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^WGS_BAM_CLEANUP_CANDIDATE:/ {value=$2} END {print value}'
    )


    # --------------------------------------------------------
    # Store sample ↔ jobs mapping + mtDNA diagnostics
    # --------------------------------------------------------

    echo -e \
"${SAMPLE}\t${WGS_JOB:-NA}\t${WGS_STATE}\t${WGS_REASON}\t${CHECK_WGS_JOB:-NA}\t${CHECK_WGS_STATE}\t${CHECK_WGS_REASON}\t${MIT_JOB:-NA}\t${MIT_STATE}\t${MIT_REASON}\t${CHECK_MIT_JOB:-NA}\t${CHECK_MIT_STATE}\t${CHECK_MIT_REASON}\t${CLEANUP_JOB:-NA}\t${CLEANUP_STATE}\t${CLEANUP_REASON}\t${MTDNA_BAM_CHECK:-NA}\t${MTDNA_BAI_CHECK:-NA}\t${MTDNA_BAI_USABLE:-NA}\t${MTDNA_BAM_QUICKCHECK:-NA}\t${MTDNA_MAPPED_READS:-NA}\t${MTDNA_MEAN_DEPTH:-NA}\t${MTDNA_COVERED_1X_PCT:-NA}\t${MIT_FAIL_REASON:-NA}\t${WGS_BAM_CLEANUP_CANDIDATE:-NA}" \
    >> "$TMP_JOBS"


    # --------------------------------------------------------
    # Last recorded result for each stage
    # --------------------------------------------------------

    WGS_MARKER=$(
        echo "$CURRENT_RUN" |
        grep -E '^(WGS_OK|WGS_FAIL:)' |
        tail -1 || true
    )

    MIT_MARKER=$(
        echo "$CURRENT_RUN" |
        grep -E '^(MIT_OK|MIT_FAIL:)' |
        tail -1 || true
    )

    CLEANUP_MARKER=$(
        echo "$CURRENT_RUN" |
        grep -E '^(CLEANUP_OK|CLEANUP_ABORTED:|CLEANUP_FAIL:)' |
        tail -1 || true
    )


    # ========================================================
    # 1. FAILURES DETECTED BY VALIDATION CHECKS
    # ========================================================

    if [[ "$WGS_MARKER" == WGS_FAIL:* ]]; then

        echo -e \
"${SAMPLE}\tFAIL_WGS\t${WGS_MARKER}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    # MIT failure compatible with low mitochondrial signal (orchestrator v6)
    if [[ "$MIT_FAIL_REASON" == "LOW_MTDNA_SIGNAL" ]] || \
       [[ "$MIT_MARKER" == MIT_FAIL:\ LOW_MTDNA_SIGNAL* ]]; then

        DETAIL="mapped_reads=${MTDNA_MAPPED_READS:-NA}; mean_depth=${MTDNA_MEAN_DEPTH:-NA}; covered_1x_pct=${MTDNA_COVERED_1X_PCT:-NA}; mtDNA_BAM=${MTDNA_BAM_CHECK:-NA}; mtDNA_BAI=${MTDNA_BAI_CHECK:-NA}; quickcheck=${MTDNA_BAM_QUICKCHECK:-NA}; WGS_BAM_CLEANUP_CANDIDATE=${WGS_BAM_CLEANUP_CANDIDATE:-NA}"

        echo -e \
"${SAMPLE}\tFAIL_MIT_LOW_SIGNAL\t${DETAIL}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        LOW_SIGNAL_COUNT=$((LOW_SIGNAL_COUNT + 1))
        continue
    fi


    if [[ "$MIT_MARKER" == MIT_FAIL:* ]]; then

        echo -e \
"${SAMPLE}\tFAIL_MIT\t${MIT_MARKER}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if [[ "$CLEANUP_MARKER" == CLEANUP_ABORTED:* ]] || \
       [[ "$CLEANUP_MARKER" == CLEANUP_FAIL:* ]]; then

        echo -e \
"${SAMPLE}\tFAIL_CLEANUP\t${CLEANUP_MARKER}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    # ========================================================
    # 2. SLURM FAILURES
    # ========================================================

    if is_failed_state "$WGS_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_WGS\tSlurm=${WGS_STATE}; job=${WGS_JOB}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$CHECK_WGS_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_WGS_CHECK\tSlurm=${CHECK_WGS_STATE}; job=${CHECK_WGS_JOB}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$MIT_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_MIT\tSlurm=${MIT_STATE}; job=${MIT_JOB}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$CHECK_MIT_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_MIT_CHECK\tSlurm=${CHECK_MIT_STATE}; job=${CHECK_MIT_JOB}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$CLEANUP_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_CLEANUP\tSlurm=${CLEANUP_STATE}; job=${CLEANUP_JOB}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    # ========================================================
    # 3. COMPLETED SUCCESSFULLY
    # ========================================================

    if [[ "$WGS_MARKER" == "WGS_OK" ]] && \
       [[ "$MIT_MARKER" == "MIT_OK" ]] && \
       [[ "$CLEANUP_MARKER" == "CLEANUP_OK" ]]; then

        echo -e \
"${SAMPLE}\tOK\tWGS_OK;MIT_OK;CLEANUP_OK\t${LOG}" \
        >> "$TMP_RESULTS"

        OK_COUNT=$((OK_COUNT + 1))
        continue
    fi


    # ========================================================
    # 4. PROCESSING
    # ========================================================

    if [[ "$MIT_MARKER" == "MIT_OK" ]]; then

        DETAIL="waiting_or_running_CLEANUP"

    elif echo "$CURRENT_RUN" | grep -q "^MIT_REAL_START:"; then

        DETAIL="running_MIT"

    elif [[ "$WGS_MARKER" == "WGS_OK" ]]; then

        DETAIL="waiting_for_MIT"

    elif echo "$CURRENT_RUN" | grep -q "^WGS_REAL_START:"; then

        DETAIL="running_WGS"

    elif echo "$CURRENT_RUN" | grep -q "^PIPELINE_LAUNCHED_OK"; then

        DETAIL="queued_waiting_for_WGS"

    else

        LAST_LINE=$(
            echo "$CURRENT_RUN" |
            tail -n 1
        )

        DETAIL="last_line=${LAST_LINE}"
    fi


    echo -e \
"${SAMPLE}\tPROCESSING\t${DETAIL}\t${LOG}" \
    >> "$TMP_RESULTS"

    PROCESSING_COUNT=$((PROCESSING_COUNT + 1))


done < "$PATHS_FILE"


# ============================================================
# MAIN OUTPUT
# ============================================================

{
    echo "============================================================"
    echo "GLOBAL SUMMARY"
    echo "============================================================"
    echo "TOTAL_LAUNCHED : $TOTAL"
    echo "PROCESSING     : $PROCESSING_COUNT"
    echo "FAIL           : $FAIL_COUNT"
    echo "FAIL_MIT_LOW_SIGNAL : $LOW_SIGNAL_COUNT"
    echo "OK             : $OK_COUNT"
    echo
    echo "============================================================"
    echo "DETAIL BY SAMPLE"
    echo "============================================================"
    echo
    echo -e "SAMPLE\tSTATUS\tDETAIL\tPIPELINE_STATUS_LOG"

    cat "$TMP_RESULTS"

} > "$OUT"


# ============================================================
# JOB OUTPUT
# ============================================================

cp "$TMP_JOBS" "$JOBS_OUT"


# ============================================================
# FINAL MESSAGE
# ============================================================

echo
echo "Summary generated in:"
echo "  $OUT"
echo
echo "Sample ↔ jobs table generated in:"
echo "  $JOBS_OUT"
echo
echo "Pipeline status log list generated in:"
echo "  $PATHS_FILE"
echo
echo "Recommended viewing:"
echo
echo "  column -t -s \$'\\t' pipeline_status_summary.log | less -S"
echo
echo "  column -t -s \$'\\t' jobs_by_sample.tsv | less -S"
echo
