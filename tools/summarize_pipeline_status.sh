#!/bin/bash

set -euo pipefail

# ============================================================
# WGS -> MIT -> CLEANUP ANALYSIS STATUS
# ============================================================
#
# Reports the latest recorded execution per sample, combining Slurm job
# states with pipeline markers, mtDNA diagnostics, and version provenance.
# Active WGS jobs take precedence over stale downstream failure markers.
# Recognized MIT failure signatures are considered storage-resolved only
# when cleanup has completed and the exported mtDNA BAM is preserved.
# Temporary files use CBICALL_SUMMARY_TMPDIR, TMPDIR, or /tmp.
#
# Summarizes the status of all samples launched with the
# CBIcall WGS → MIT → CLEANUP orchestrator.
#
# IMPORTANT:
#   Run this script from the directory that contains the
#   individual sample directories.
#
# USAGE:
#   /path/to/repository/tools/summarize_pipeline_status.sh
#
# EXAMPLE:
#   cd /path/to/WORKDIR_BASE
#   /path/to/repository/tools/summarize_pipeline_status.sh
#
# Only the LAST execution recorded in each pipeline_status.log
# is analyzed.
#
# OUTPUT:
#   paths_pipeline_status_logs
#   pipeline_status_summary.log
#   jobs_by_sample.tsv
#
# ============================================================

SUMMARIZE_VERSION="7.4"
REFERENCE_LAUNCHER_VERSION="1.7"
REFERENCE_ORCHESTRATOR_VERSION="11"

PATHS_FILE="paths_pipeline_status_logs"
OUT="pipeline_status_summary.log"
JOBS_OUT="jobs_by_sample.tsv"

find . -type f -name "pipeline_status.log" | sort > "$PATHS_FILE"

# Portable temporary storage: override with CBICALL_SUMMARY_TMPDIR or TMPDIR.
TMP_DIR="${CBICALL_SUMMARY_TMPDIR:-${TMPDIR:-/tmp}}"
mkdir -p "$TMP_DIR"

TMP_RESULTS=$(mktemp "${TMP_DIR}/results.XXXXXXXXXX")
TMP_JOBS=$(mktemp "${TMP_DIR}/jobs.XXXXXXXXXX")

trap 'rm -f "$TMP_RESULTS" "$TMP_JOBS"' EXIT

TOTAL=0
OK_COUNT=0
PROCESSING_COUNT=0
FAIL_COUNT=0
LOW_SIGNAL_COUNT=0
MTOOLBOX_INTERNAL_ERROR_COUNT=0


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


# Determines whether a Slurm state corresponds to failure
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


# Determines whether a Slurm state represents a job that is still active.
#
# An active WGS job is checked before interpreting pipeline markers.
# This is important when duplicate/concurrent launches have interleaved stale
# MIT/CLEANUP lines into the latest pipeline_status.log block.
is_active_state() {

    case "$1" in
        PENDING|RUNNING|CONFIGURING|COMPLETING|SUSPENDED|REQUEUED|RESIZING|SIGNALING|STAGE_OUT)
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
# First checks squeue, because it allows retrieval of the reason
# for pending jobs.
#
# If the job is no longer in squeue, sacct is queried instead.
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
        awk 'NF {print $1; exit}' || true
    )

    [[ -z "$STATE" ]] && STATE="UNKNOWN"

    echo "${STATE}|-"
}


# ============================================================
# HEADER: jobs_by_sample.tsv
# ============================================================

echo -e \
"SAMPLE\tWGS_JOB\tWGS_STATE\tWGS_REASON\tCHECK_WGS_JOB\tCHECK_WGS_STATE\tCHECK_WGS_REASON\tMIT_JOB\tMIT_STATE\tMIT_REASON\tCHECK_MIT_JOB\tCHECK_MIT_STATE\tCHECK_MIT_REASON\tCLEANUP_JOB\tCLEANUP_STATE\tCLEANUP_REASON\tMTDNA_BAM_CHECK\tMTDNA_BAI_CHECK\tMTDNA_BAI_USABLE\tMTDNA_BAM_QUICKCHECK\tMTDNA_MAPPED_READS\tMTDNA_MEAN_DEPTH\tMTDNA_COVERED_1X_PCT\tMTDNA_LOW_SIGNAL_TRIGGER\tMIT_FAIL_REASON\tMIT_FAILURE_SIGNATURE\tWGS_BAM_CLEANUP_CANDIDATE\tWGS_BAMS_DELETED\tPIPELINE_FINAL_STATUS\tLAUNCHER_VERSION\tORCHESTRATOR_VERSION\tSUMMARIZE_VERSION\tREFERENCE_LAUNCHER_VERSION\tREFERENCE_ORCHESTRATOR_VERSION" \
> "$TMP_JOBS"


# ============================================================
# SAMPLE LOOP
# ============================================================

while read -r LOG; do

    [[ -z "$LOG" ]] && continue

    TOTAL=$((TOTAL + 1))

    SAMPLE=$(basename "$(dirname "$LOG")")


    # --------------------------------------------------------
    # Get ONLY the latest pipeline execution
    #
    # Each execution starts with:
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
    # Recover software-version provenance
    # --------------------------------------------------------

    LAUNCHER_VERSION=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^LAUNCHER_VERSION:/ {value=$2} END {print value}'
    )

    ORCHESTRATOR_VERSION=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^ORCHESTRATOR_VERSION:/ {value=$2} END {print value}'
    )

    [[ -z "$LAUNCHER_VERSION" ]] && LAUNCHER_VERSION="NA"
    [[ -z "$ORCHESTRATOR_VERSION" ]] && ORCHESTRATOR_VERSION="NA"


    # --------------------------------------------------------
    # Recover JOB IDs
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
    # Query state + reason for each job
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
    # Recover mtDNA / MIT-specific information (v6+)
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

    MTDNA_LOW_SIGNAL_TRIGGER=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MTDNA_LOW_SIGNAL_TRIGGER:/ {value=$2} END {print value}'
    )

    # For pre-v8 logs, infer the low-signal trigger from the stored metrics
    # for reporting purposes only. This does not rewrite pipeline history.
    if [[ -z "$MTDNA_LOW_SIGNAL_TRIGGER" ]] && \
       [[ "$MTDNA_MAPPED_READS" =~ ^[0-9]+$ ]] && \
       [[ "$MTDNA_COVERED_1X_PCT" =~ ^[0-9]+([.][0-9]+)?$ ]]; then

        LEGACY_LOW_READS=0
        LEGACY_LOW_COVERAGE=0

        if (( MTDNA_MAPPED_READS < 1000 )); then
            LEGACY_LOW_READS=1
        fi

        if awk -v value="$MTDNA_COVERED_1X_PCT" \
               'BEGIN { exit !(value < 50) }'; then
            LEGACY_LOW_COVERAGE=1
        fi

        if (( LEGACY_LOW_READS == 1 && LEGACY_LOW_COVERAGE == 1 )); then
            MTDNA_LOW_SIGNAL_TRIGGER="INFERRED_LOW_READS_AND_LOW_COVERAGE"
        elif (( LEGACY_LOW_READS == 1 )); then
            MTDNA_LOW_SIGNAL_TRIGGER="INFERRED_LOW_READS"
        elif (( LEGACY_LOW_COVERAGE == 1 )); then
            MTDNA_LOW_SIGNAL_TRIGGER="INFERRED_LOW_COVERAGE"
        else
            MTDNA_LOW_SIGNAL_TRIGGER="NONE"
        fi
    fi

    MIT_FAIL_REASON=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MIT_FAIL_REASON:/ {value=$2} END {print value}'
    )

    MIT_FAILURE_SIGNATURE=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^MIT_FAILURE_SIGNATURE:/ {value=$2} END {print value}'
    )

    WGS_BAM_CLEANUP_CANDIDATE=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^WGS_BAM_CLEANUP_CANDIDATE:/ {value=$2} END {print value}'
    )

    WGS_BAMS_DELETED=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^WGS_BAMS_DELETED:/ {value=$2} END {print value}'
    )

    PIPELINE_FINAL_STATUS=$(
        echo "$CURRENT_RUN" |
        awk -F': ' '/^PIPELINE_FINAL_STATUS:/ {value=$2} END {print value}'
    )


    # --------------------------------------------------------
    # Save sample ↔ jobs + mtDNA diagnostic information
    # --------------------------------------------------------

    echo -e \
"${SAMPLE}\t${WGS_JOB:-NA}\t${WGS_STATE}\t${WGS_REASON}\t${CHECK_WGS_JOB:-NA}\t${CHECK_WGS_STATE}\t${CHECK_WGS_REASON}\t${MIT_JOB:-NA}\t${MIT_STATE}\t${MIT_REASON}\t${CHECK_MIT_JOB:-NA}\t${CHECK_MIT_STATE}\t${CHECK_MIT_REASON}\t${CLEANUP_JOB:-NA}\t${CLEANUP_STATE}\t${CLEANUP_REASON}\t${MTDNA_BAM_CHECK:-NA}\t${MTDNA_BAI_CHECK:-NA}\t${MTDNA_BAI_USABLE:-NA}\t${MTDNA_BAM_QUICKCHECK:-NA}\t${MTDNA_MAPPED_READS:-NA}\t${MTDNA_MEAN_DEPTH:-NA}\t${MTDNA_COVERED_1X_PCT:-NA}\t${MTDNA_LOW_SIGNAL_TRIGGER:-NA}\t${MIT_FAIL_REASON:-NA}\t${MIT_FAILURE_SIGNATURE:-NA}\t${WGS_BAM_CLEANUP_CANDIDATE:-NA}\t${WGS_BAMS_DELETED:-NA}\t${PIPELINE_FINAL_STATUS:-NA}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${SUMMARIZE_VERSION}\t${REFERENCE_LAUNCHER_VERSION}\t${REFERENCE_ORCHESTRATOR_VERSION}" \
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
    # 0. ACTIVE CURRENT WGS JOB HAS PRIORITY
    # ========================================================
    #
    # Duplicate/concurrent launches can interleave output from older jobs into
    # the latest pipeline_status.log block. If the WGS_JOB recorded for the
    # latest run is currently active in Slurm, that live state is authoritative
    # and stale downstream MIT/CLEANUP markers must not classify the sample as
    # failed.

    if is_active_state "$WGS_STATE"; then

        if [[ "$WGS_STATE" == "PENDING" ]]; then
            DETAIL="queued_waiting_for_WGS; Slurm=${WGS_STATE}; job=${WGS_JOB:-NA}; reason=${WGS_REASON:-NA}"
        else
            DETAIL="running_WGS; Slurm=${WGS_STATE}; job=${WGS_JOB:-NA}; reason=${WGS_REASON:-NA}"
        fi

        echo -e \
"${SAMPLE}\tPROCESSING\t${DETAIL}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        PROCESSING_COUNT=$((PROCESSING_COUNT + 1))
        continue
    fi


    # Count internal MIT diagnoses only after excluding a currently active WGS
    # job, whose latest log block may contain stale downstream markers.
    if [[ "$MIT_FAIL_REASON" == "MTOOLBOX_INTERNAL_ERROR" ]]; then
        MTOOLBOX_INTERNAL_ERROR_COUNT=$((MTOOLBOX_INTERNAL_ERROR_COUNT + 1))
    fi

    # ========================================================
    # 1. FAILURES DETECTED BY THE CHECKS
    # ========================================================

    if [[ "$WGS_MARKER" == WGS_FAIL:* ]]; then

        echo -e \
"${SAMPLE}\tFAIL_WGS\t${WGS_MARKER}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    # Resolved LOW_MTDNA_SIGNAL case.
    # This covers both manual legacy normalization and automatic cleanup
    # performed by newer orchestrator versions.
    if [[ "$PIPELINE_FINAL_STATUS" == "OK_LOW_MTDNA_SIGNAL" ]] || \
       { [[ "$MIT_FAIL_REASON" == "LOW_MTDNA_SIGNAL" ]] && \
         [[ "$CLEANUP_MARKER" == "CLEANUP_OK" ]] && \
         [[ "$WGS_BAMS_DELETED" == "YES" ]]; }; then

        DETAIL="MIT_FAIL_REASON=LOW_MTDNA_SIGNAL; trigger=${MTDNA_LOW_SIGNAL_TRIGGER:-NA}; MIT_FAILURE_SIGNATURE=${MIT_FAILURE_SIGNATURE:-NA}; WGS_BAMS_DELETED=${WGS_BAMS_DELETED:-NA}; mapped_reads=${MTDNA_MAPPED_READS:-NA}; mean_depth=${MTDNA_MEAN_DEPTH:-NA}; covered_1x_pct=${MTDNA_COVERED_1X_PCT:-NA}"

        echo -e \
"${SAMPLE}\tOK\t${DETAIL}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        OK_COUNT=$((OK_COUNT + 1))
        continue
    fi


    # Recognized MToolBox/internal MIT software failure signatures.
    # The MIT job is expected to be FAILED; successful cleanup is the criterion
    # that makes the overall storage workflow resolved.
    INTERNAL_ERROR_RECOGNIZED=0
    if [[ "$MIT_FAIL_REASON" == "MTOOLBOX_INTERNAL_ERROR" ]]; then
        case "$MIT_FAILURE_SIGNATURE" in
            MTOOLBOX_MEDIAN_INDEX_ERROR|GATK_INDELREALIGNER_OUT_OF_ORDER)
                INTERNAL_ERROR_RECOGNIZED=1
                ;;
        esac
    fi

    if (( INTERNAL_ERROR_RECOGNIZED == 1 )) && \
       [[ "$CLEANUP_MARKER" == "CLEANUP_OK" ]] && \
       [[ "$WGS_BAMS_DELETED" == "YES" ]]; then

        DETAIL="MIT_FAIL_REASON=MTOOLBOX_INTERNAL_ERROR; MIT_FAILURE_SIGNATURE=${MIT_FAILURE_SIGNATURE}; WGS_BAMS_DELETED=YES"

        echo -e \
"${SAMPLE}\tOK\t${DETAIL}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        OK_COUNT=$((OK_COUNT + 1))
        continue
    fi


    # If a recognized internal MIT error has been detected but cleanup has not
    # completed yet, do not misclassify the expected FAILED MIT Slurm job as an
    # unresolved generic MIT failure.
    if (( INTERNAL_ERROR_RECOGNIZED == 1 )); then

        if [[ "$CLEANUP_MARKER" == CLEANUP_ABORTED:* ]] || \
           [[ "$CLEANUP_MARKER" == CLEANUP_FAIL:* ]] || \
           is_failed_state "$CLEANUP_STATE"; then

            DETAIL="MIT_FAIL_REASON=MTOOLBOX_INTERNAL_ERROR; MIT_FAILURE_SIGNATURE=${MIT_FAILURE_SIGNATURE}; cleanup=${CLEANUP_MARKER:-Slurm=$CLEANUP_STATE}"
            echo -e \
"${SAMPLE}\tFAIL_CLEANUP\t${DETAIL}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
            >> "$TMP_RESULTS"
            FAIL_COUNT=$((FAIL_COUNT + 1))
            continue
        fi

        DETAIL="MIT_FAIL_REASON=MTOOLBOX_INTERNAL_ERROR; MIT_FAILURE_SIGNATURE=${MIT_FAILURE_SIGNATURE}; waiting_or_running_CLEANUP"
        echo -e \
"${SAMPLE}\tPROCESSING\t${DETAIL}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"
        PROCESSING_COUNT=$((PROCESSING_COUNT + 1))
        continue
    fi


    # MIT failure compatible with low mitochondrial signal (orchestrator v6+).
    # If cleanup has not completed, keep it classified as a low-signal MIT failure.
    if [[ "$MIT_FAIL_REASON" == "LOW_MTDNA_SIGNAL" ]] || \
       [[ "$MIT_MARKER" == MIT_FAIL:\ LOW_MTDNA_SIGNAL* ]]; then

        DETAIL="trigger=${MTDNA_LOW_SIGNAL_TRIGGER:-NA}; MIT_FAILURE_SIGNATURE=${MIT_FAILURE_SIGNATURE:-NA}; mapped_reads=${MTDNA_MAPPED_READS:-NA}; mean_depth=${MTDNA_MEAN_DEPTH:-NA}; covered_1x_pct=${MTDNA_COVERED_1X_PCT:-NA}; mtDNA_BAM=${MTDNA_BAM_CHECK:-NA}; mtDNA_BAI=${MTDNA_BAI_CHECK:-NA}; quickcheck=${MTDNA_BAM_QUICKCHECK:-NA}; WGS_BAM_CLEANUP_CANDIDATE=${WGS_BAM_CLEANUP_CANDIDATE:-NA}"

        echo -e \
"${SAMPLE}\tFAIL_MIT_LOW_SIGNAL\t${DETAIL}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        LOW_SIGNAL_COUNT=$((LOW_SIGNAL_COUNT + 1))
        continue
    fi


    if [[ "$MIT_MARKER" == MIT_FAIL:* ]]; then

        echo -e \
"${SAMPLE}\tFAIL_MIT\t${MIT_MARKER}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if [[ "$CLEANUP_MARKER" == CLEANUP_ABORTED:* ]] || \
       [[ "$CLEANUP_MARKER" == CLEANUP_FAIL:* ]]; then

        echo -e \
"${SAMPLE}\tFAIL_CLEANUP\t${CLEANUP_MARKER}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    # ========================================================
    # 2. SLURM FAILURES
    # ========================================================

    if is_failed_state "$WGS_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_WGS\tSlurm=${WGS_STATE}; job=${WGS_JOB}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$CHECK_WGS_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_WGS_CHECK\tSlurm=${CHECK_WGS_STATE}; job=${CHECK_WGS_JOB}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$MIT_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_MIT\tSlurm=${MIT_STATE}; job=${MIT_JOB}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$CHECK_MIT_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_MIT_CHECK\tSlurm=${CHECK_MIT_STATE}; job=${CHECK_MIT_JOB}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    if is_failed_state "$CLEANUP_STATE"; then

        echo -e \
"${SAMPLE}\tFAIL_CLEANUP\tSlurm=${CLEANUP_STATE}; job=${CLEANUP_JOB}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        FAIL_COUNT=$((FAIL_COUNT + 1))
        continue
    fi


    # ========================================================
    # 3. ALL OK
    # ========================================================

    if [[ "$WGS_MARKER" == "WGS_OK" ]] && \
       [[ "$MIT_MARKER" == "MIT_OK" ]] && \
       [[ "$CLEANUP_MARKER" == "CLEANUP_OK" ]]; then

        echo -e \
"${SAMPLE}\tOK\tWGS_OK;MIT_OK;CLEANUP_OK\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
        >> "$TMP_RESULTS"

        OK_COUNT=$((OK_COUNT + 1))
        continue
    fi


    # ========================================================
    # 4. PROCESSING
    # ========================================================

    if [[ "$MIT_MARKER" == "MIT_OK" ]]; then

        DETAIL="waiting_or_running_CLEANUP"

    elif grep -q "^MIT_REAL_START:" <<< "$CURRENT_RUN"; then

        DETAIL="running_MIT"

    elif [[ "$WGS_MARKER" == "WGS_OK" ]]; then

        DETAIL="waiting_for_MIT"

    elif grep -q "^WGS_REAL_START:" <<< "$CURRENT_RUN"; then

        DETAIL="running_WGS"

    elif grep -q "^PIPELINE_LAUNCHED_OK" <<< "$CURRENT_RUN"; then

        DETAIL="queued_waiting_for_WGS"

    else

        LAST_LINE=$(
            echo "$CURRENT_RUN" |
            tail -n 1
        )

        DETAIL="last_line=${LAST_LINE}"
    fi


    echo -e \
"${SAMPLE}\tPROCESSING\t${DETAIL}\t${LAUNCHER_VERSION}\t${ORCHESTRATOR_VERSION}\t${LOG}" \
    >> "$TMP_RESULTS"

    PROCESSING_COUNT=$((PROCESSING_COUNT + 1))


done < "$PATHS_FILE"


# ============================================================
# MAIN OUTPUT
# ============================================================

{
    echo "============================================================"
    echo "REPORT METADATA"
    echo "============================================================"
    echo "SUMMARIZE_VERSION              : $SUMMARIZE_VERSION"
    echo "REFERENCE_LAUNCHER_VERSION     : $REFERENCE_LAUNCHER_VERSION"
    echo "REFERENCE_ORCHESTRATOR_VERSION : $REFERENCE_ORCHESTRATOR_VERSION"
    echo "REFERENCE_STACK                : launcher v${REFERENCE_LAUNCHER_VERSION} / orchestrator v${REFERENCE_ORCHESTRATOR_VERSION} / summarize v${SUMMARIZE_VERSION}"
    echo "GENERATED_AT                   : $(date)"
    echo "NOTE                           : NA launcher/orchestrator versions indicate legacy runs where version provenance was not recorded."
    echo
    echo "============================================================"
    echo "GLOBAL SUMMARY"
    echo "============================================================"
    echo "TOTAL_LAUNCHED      : $TOTAL"
    echo "PROCESSING          : $PROCESSING_COUNT"
    echo "FAIL                : $FAIL_COUNT"
    echo "FAIL_MIT_LOW_SIGNAL      : $LOW_SIGNAL_COUNT"
    echo "MTOOLBOX_INTERNAL_ERROR  : $MTOOLBOX_INTERNAL_ERROR_COUNT"
    echo "ALL_OK                   : $OK_COUNT"
    echo
    echo "============================================================"
    echo "PER-SAMPLE DETAILS"
    echo "============================================================"
    echo
    echo -e "SAMPLE\tSTATUS\tDETAIL\tLAUNCHER_VERSION\tORCHESTRATOR_VERSION\tPIPELINE_STATUS_LOG"

    cat "$TMP_RESULTS"

} > "$OUT"


# ============================================================
# JOBS OUTPUT
# ============================================================

cp "$TMP_JOBS" "$JOBS_OUT"


# ============================================================
# FINAL MESSAGE
# ============================================================

echo
echo "Summarize version: $SUMMARIZE_VERSION"
echo "Reference stack  : launcher v${REFERENCE_LAUNCHER_VERSION} / orchestrator v${REFERENCE_ORCHESTRATOR_VERSION}"
echo
echo "Summary written to:"
echo "  $OUT"
echo
echo "Sample ↔ jobs mapping written to:"
echo "  $JOBS_OUT"
echo
echo "Pipeline-status log list written to:"
echo "  $PATHS_FILE"
echo
echo "Recommended display for pipeline_status_summary.log:"
echo
echo "  {"
echo "      awk '/^SAMPLE\\tSTATUS\\tDETAIL\\t/ {exit} {print}' pipeline_status_summary.log"
echo "      awk 'f || /^SAMPLE\\tSTATUS\\tDETAIL\\t/ {f=1; print}' pipeline_status_summary.log \\"
echo "          | column -t -s \$'\\t' -o ' '"
echo "  } | less -S"
echo
echo "Recommended display for jobs_by_sample.tsv:"
echo
echo "  column -t -s \$'\\t' -o ' ' jobs_by_sample.tsv | less -S"
echo
