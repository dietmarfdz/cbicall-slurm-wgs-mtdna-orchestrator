#!/usr/bin/env bash
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
mkdir -p "$TMP/bin" "$TMP/inputs" "$TMP/tmp"
cat > "$TMP/bash_env" <<'EOF'
module() { :; }
EOF
export BASH_ENV="$TMP/bash_env"
cat > "$TMP/bin/sbatch" <<'EOF'
#!/usr/bin/env bash
count=$(cat "$MOCK_JOB_COUNTER")
count=$((count+1))
printf '%s' "$count" > "$MOCK_JOB_COUNTER"
printf '%s\n' "$count"
EOF
cat > "$TMP/bin/squeue" <<'EOF'
#!/usr/bin/env bash
job=""
while (( $# )); do
  if [[ "$1" == "-j" ]]; then job="$2"; shift 2; else shift; fi
done
if [[ -f "$MOCK_STATE_FILE" ]]; then
  awk -v id="$job" '$1 == id && $2 ~ /^(RUNNING|PENDING|CONFIGURING|COMPLETING|SUSPENDED|REQUEUED|RESIZING|SIGNALING|STAGE_OUT)$/ { print $2 "|" $3; exit }' "$MOCK_STATE_FILE"
fi
EOF
cat > "$TMP/bin/sacct" <<'EOF'
#!/usr/bin/env bash
job=""
while (( $# )); do
  if [[ "$1" == "-j" ]]; then job="$2"; shift 2; else shift; fi
done
if [[ -f "$MOCK_STATE_FILE" ]]; then
  state=$(awk -v id="$job" '$1 == id {print $2; exit}' "$MOCK_STATE_FILE")
fi
printf '%s\n' "${state:-COMPLETED}"
EOF
cat > "$TMP/bin/samtools" <<'EOF'
#!/usr/bin/env bash
cmd=$1; shift
case "$cmd" in
 quickcheck) [[ -s "$1" ]];;
 idxstats) [[ -f "${1}.bai" ]];;
 view) if [[ "$*" == *"-F 4"* ]]; then echo "${MOCK_MAPPED:-2000}"; else echo "${MOCK_TOTAL:-2500}"; fi;;
 depth) for ((i=1;i<=100;i++)); do if (( i <= ${MOCK_DEPTH_COVERED:-100} )); then d=5; else d=0; fi; printf 'chrM\t%s\t%s\n' "$i" "$d"; done;;
 *) echo "bad samtools: $cmd" >&2; exit 2;;
esac
EOF
chmod +x "$TMP/bin"/*
export PATH="$TMP/bin:$PATH" MOCK_JOB_COUNTER="$TMP/job_counter" MOCK_STATE_FILE="$TMP/states" MOCK_TOTAL=2500 MOCK_MAPPED=2000 MOCK_DEPTH_COVERED=100
printf 1000 > "$MOCK_JOB_COUNTER"
cp "$REPO/config/config.example.env" "$TMP/test.env"
sed -i 's/RUNTIME_PROFILE="XXX"/RUNTIME_PROFILE="test"/; s/WGS_PARTITION="XXX"/WGS_PARTITION="mock_long"/; s/SHORT_PARTITION="XXX"/SHORT_PARTITION="mock_short"/; s/MEM="MEMORY_LIMIT"/MEM="4G"/; s/WGS_TIME="WGS_WALLTIME"/WGS_TIME="01:00:00"/; s/MIT_TIME="MIT_WALLTIME"/MIT_TIME="01:00:00"/' "$TMP/test.env"
export CBICALL_ORCHESTRATOR_CONFIG="$TMP/test.env"
mkdir -p "$TMP/inputs/NORMAL" "$TMP/inputs/LOWCOV" "$TMP/inputs/LOWREADS" "$TMP/inputs/INTERNAL" "$TMP/inputs/GATK" "$TMP/inputs/UNRECOGNIZED" "$TMP/inputs/ACTIVE"
# direct invocation and generated-script syntax
for s in NORMAL LOWCOV LOWREADS INTERNAL GATK UNRECOGNIZED ACTIVE; do
  "$REPO/cbicall_wgs_mtdna_slurm.sh" "$s" "$TMP/inputs" 4 > /dev/null
  bash -n "$TMP/inputs/$s/check_mit.sh" "$TMP/inputs/$s/cleanup.sh" "$TMP/inputs/$s/check_wgs.sh" "$TMP/inputs/$s/job_${s}_wgs.slurm" "$TMP/inputs/$s/job_${s}_mit.slurm"
  grep -q '^ORCHESTRATOR_VERSION: 11$' "$TMP/inputs/$s/pipeline_status.log"
  grep -q '^LAUNCHER_VERSION: NOT_PROVIDED$' "$TMP/inputs/$s/pipeline_status.log"
  mkdir -p "$TMP/inputs/$s/${s}_cbicall_bash_gatk-4.6_wgs_test/01_bam" "$TMP/inputs/$s/${s}_cbicall_bash_gatk-4.6_wgs_test/exports/mtdna"
  mkdir -p "$TMP/inputs/$s/cbicall_bash_gatk-3.5_mit_single_rsrs_test/01_mtoolbox"
  printf 'test-bam' > "$TMP/inputs/$s/${s}_cbicall_bash_gatk-4.6_wgs_test/01_bam/data.bam"
  printf 'mt-bam' > "$TMP/inputs/$s/${s}_cbicall_bash_gatk-4.6_wgs_test/exports/mtdna/${s}_MIT.bam"
  touch "$TMP/inputs/$s/${s}_cbicall_bash_gatk-4.6_wgs_test/exports/mtdna/${s}_MIT.bam.bai"
done
# NORMAL success
MIT="$TMP/inputs/NORMAL/cbicall_bash_gatk-3.5_mit_single_rsrs_test"
touch "$MIT/01_mtoolbox/VCF_file.vcf"
echo 'All done!!!' > "$MIT/bash_gatk-3.5_mit_single_rsrs.log"
bash "$TMP/inputs/NORMAL/check_mit.sh"
grep -q 'CHECK_MIT_CLEANUP_ALLOWED: MIT_OK' "$TMP/inputs/NORMAL/pipeline_status.log"
bash "$TMP/inputs/NORMAL/cleanup.sh"
[[ ! -f "$TMP/inputs/NORMAL/NORMAL_cbicall_bash_gatk-4.6_wgs_test/01_bam/data.bam" ]]
[[ -f "$TMP/inputs/NORMAL/NORMAL_cbicall_bash_gatk-4.6_wgs_test/exports/mtdna/NORMAL_MIT.bam" ]]
# LOW COVERAGE only + known consensus signature: cleanup permitted
export MOCK_MAPPED=2000 MOCK_DEPTH_COVERED=10
MIT="$TMP/inputs/LOWCOV/cbicall_bash_gatk-3.5_mit_single_rsrs_test"
echo 'consensus_value referenced before assignment' > "$MIT/bash_gatk-3.5_mit_single_rsrs.log"
bash "$TMP/inputs/LOWCOV/check_mit.sh"
grep -q '^MTDNA_LOW_SIGNAL_TRIGGER: LOW_COVERAGE$' "$TMP/inputs/LOWCOV/pipeline_status.log"
grep -q '^CHECK_MIT_CLEANUP_ALLOWED: LOW_MTDNA_SIGNAL$' "$TMP/inputs/LOWCOV/pipeline_status.log"
bash "$TMP/inputs/LOWCOV/cleanup.sh"
[[ ! -f "$TMP/inputs/LOWCOV/LOWCOV_cbicall_bash_gatk-4.6_wgs_test/01_bam/data.bam" ]]
# LOW READS only + consensus error: cleanup permitted
export MOCK_MAPPED=50 MOCK_DEPTH_COVERED=100
MIT="$TMP/inputs/LOWREADS/cbicall_bash_gatk-3.5_mit_single_rsrs_test"
echo 'consensus_value referenced before assignment' > "$MIT/bash_gatk-3.5_mit_single_rsrs.log"
bash "$TMP/inputs/LOWREADS/check_mit.sh"
grep -q '^MTDNA_LOW_SIGNAL_TRIGGER: LOW_READS$' "$TMP/inputs/LOWREADS/pipeline_status.log"
bash "$TMP/inputs/LOWREADS/cleanup.sh"
[[ ! -f "$TMP/inputs/LOWREADS/LOWREADS_cbicall_bash_gatk-4.6_wgs_test/01_bam/data.bam" ]]
# Internal median failure cleanup allowed with high signal
export MOCK_MAPPED=2000 MOCK_DEPTH_COVERED=100
MIT="$TMP/inputs/INTERNAL/cbicall_bash_gatk-3.5_mit_single_rsrs_test"
printf 'qs1.append(median(qs2))\nIndexError: list index out of range\n' > "$MIT/bash_gatk-3.5_mit_single_rsrs.log"
bash "$TMP/inputs/INTERNAL/check_mit.sh"
grep -q '^MIT_FAILURE_SIGNATURE: MTOOLBOX_MEDIAN_INDEX_ERROR$' "$TMP/inputs/INTERNAL/pipeline_status.log"
bash "$TMP/inputs/INTERNAL/cleanup.sh"
# GATK out-of-order failure cleanup allowed
MIT="$TMP/inputs/GATK/cbicall_bash_gatk-3.5_mit_single_rsrs_test"
printf 'maximum allowable reads in memory is too small\nAlignments added out of order in SAMFileWriterImpl.addAlignment\n' > "$MIT/bash_gatk-3.5_mit_single_rsrs.log"
bash "$TMP/inputs/GATK/check_mit.sh"
grep -q '^MIT_FAILURE_SIGNATURE: GATK_INDELREALIGNER_OUT_OF_ORDER$' "$TMP/inputs/GATK/pipeline_status.log"
bash "$TMP/inputs/GATK/cleanup.sh"
# Unrecognized failure: CHECK_MIT fails, CLEANUP must not delete BAM
MIT="$TMP/inputs/UNRECOGNIZED/cbicall_bash_gatk-3.5_mit_single_rsrs_test"
echo 'other error' > "$MIT/bash_gatk-3.5_mit_single_rsrs.log"
if bash "$TMP/inputs/UNRECOGNIZED/check_mit.sh"; then echo 'ERROR: unrecognized MIT passed' >&2; exit 1; fi
if bash "$TMP/inputs/UNRECOGNIZED/cleanup.sh"; then echo 'ERROR: unrecognized cleanup passed' >&2; exit 1; fi
[[ -f "$TMP/inputs/UNRECOGNIZED/UNRECOGNIZED_cbicall_bash_gatk-4.6_wgs_test/01_bam/data.bam" ]]
# Summary active-WGS precedence despite stale MIT failure marker
active_id=$(awk -F': ' '/^WGS_JOB:/ {print $2; exit}' "$TMP/inputs/ACTIVE/pipeline_status.log")
printf '%s RUNNING None\n' "$active_id" > "$MOCK_STATE_FILE"
printf 'MIT_FAIL_REASON: MTOOLBOX_INTERNAL_ERROR\nMIT_FAILURE_SIGNATURE: MTOOLBOX_MEDIAN_INDEX_ERROR\nMIT_FAIL: stale marker\n' >> "$TMP/inputs/ACTIVE/pipeline_status.log"
(cd "$TMP/inputs" && CBICALL_SUMMARY_TMPDIR="$TMP/tmp" "$REPO/tools/summarize_pipeline_status.sh" >/dev/null)
grep -q $'ACTIVE\tPROCESSING\trunning_WGS' "$TMP/inputs/pipeline_status_summary.log"
grep -q '^MTOOLBOX_INTERNAL_ERROR  : 2$' "$TMP/inputs/pipeline_status_summary.log" # INTERNAL and GATK, not ACTIVE
# Version columns, metadata, status classification
head -n1 "$TMP/inputs/jobs_by_sample.tsv" | grep -q 'REFERENCE_ORCHESTRATOR_VERSION'
grep -q $'INTERNAL\tOK\tMIT_FAIL_REASON=MTOOLBOX_INTERNAL_ERROR' "$TMP/inputs/pipeline_status_summary.log"
grep -q $'LOWCOV\tOK\tMIT_FAIL_REASON=LOW_MTDNA_SIGNAL' "$TMP/inputs/pipeline_status_summary.log"
grep -q $'LOWREADS\tOK\tMIT_FAIL_REASON=LOW_MTDNA_SIGNAL' "$TMP/inputs/pipeline_status_summary.log"
grep -q $'UNRECOGNIZED\tFAIL_MIT\tMIT_FAIL:' "$TMP/inputs/pipeline_status_summary.log"
# Batch launcher passes version metadata
mkdir -p "$TMP/inputs/BATCH"
printf 'BATCH\n' > "$TMP/list.txt"
(cd "$TMP" && "$REPO/run_batch.sh" "$TMP/list.txt" "$TMP/inputs" 4 > /dev/null)
grep -q '^LAUNCHER_VERSION: 1.7$' "$TMP/inputs/BATCH/pipeline_status.log"
grep -q '^LAUNCHER_SCRIPT: run_batch.sh$' "$TMP/inputs/BATCH/pipeline_status.log"
# Validation rejects unsafe IDs and missing sample dirs
if "$REPO/cbicall_wgs_mtdna_slurm.sh" '../BAD' "$TMP/inputs" >/dev/null 2>&1; then echo 'ERROR: unsafe sample accepted' >&2; exit 1; fi
if "$REPO/cbicall_wgs_mtdna_slurm.sh" 'MISSING' "$TMP/inputs" >/dev/null 2>&1; then echo 'ERROR: missing sample accepted' >&2; exit 1; fi
mkdir -p "$TMP/bad\$(command)"/TEST
if "$REPO/cbicall_wgs_mtdna_slurm.sh" 'TEST' "$TMP/bad\$(command)" >/dev/null 2>&1; then echo 'ERROR: unsafe directory accepted' >&2; exit 1; fi
printf 'All mock integration checks passed (normal, low coverage, low reads, MToolBox median, GATK out-of-order, unrecognized, active WGS, version propagation).\n'
