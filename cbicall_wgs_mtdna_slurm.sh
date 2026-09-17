#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# CBIcall WGS -> mtDNA Slurm orchestrator
# ============================================================
#
# External orchestration wrapper around CBIcall.
# It submits a fail-safe Slurm dependency chain:
#
#   WGS -> CHECK_WGS -> MIT -> CHECK_MIT -> CLEANUP
#
# CHECK_MIT runs after the MIT job regardless of its exit status so that
# failures compatible with very low mtDNA signal can be characterized.
# CLEANUP is reached only when CHECK_MIT exits successfully. Before WGS
# BAMs are removed, the wrapper verifies the exported mtDNA BAM, its
# index, and BAM integrity with samtools quickcheck.
#
# Usage:
#   ./cbicall_wgs_mtdna_slurm.sh <SAMPLE_ID> <WORKDIR_BASE> [THREADS]
#
# Site-specific settings are read from:
#   config/config.env
# or from the path specified in:
#   CBICALL_ORCHESTRATOR_CONFIG
# ============================================================

if [[ $# -lt 2 || $# -gt 3 ]]; then
    echo "Usage: $0 <SAMPLE_ID> <WORKDIR_BASE> [THREADS]"
    exit 1
fi

SAMPLE="$1"
WORKDIR_BASE="$2"
THREADS="${3:-4}"

if ! [[ "$THREADS" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: THREADS must be a positive integer"
    exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONFIG_FILE="${CBICALL_ORCHESTRATOR_CONFIG:-${SCRIPT_DIR}/config/config.env}"

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "ERROR: configuration file not found: $CONFIG_FILE"
    echo "Copy config/config.example.env to config/config.env and edit it."
    exit 1
fi

# shellcheck disable=SC1090
source "$CONFIG_FILE"

required_vars=(
    CBICALL
    CBICALL_PYTHON_PREFIX
    PYTHON_MODULE
    PYTHON_SITE_PACKAGES_REL
    RUNTIME_PROFILE
    WGS_PARTITION
    SHORT_PARTITION
    MEM
    WGS_TIME
    MIT_TIME
    CBICALL_RESOURCE
    GENOME
    WGS_SOFTWARE_STACK
    MIT_SOFTWARE_STACK
    MIT_REFERENCE
)

for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: required configuration variable '$var' is empty"
        exit 1
    fi
done

EXCLUDE_NODES="${EXCLUDE_NODES:-}"
EXCLUDE_DIRECTIVE=""
if [[ -n "$EXCLUDE_NODES" ]]; then
    EXCLUDE_DIRECTIVE="#SBATCH --exclude=${EXCLUDE_NODES}"
fi

# Optional settings. These can be overridden in config/config.env.
SAMTOOLS_MODULE="${SAMTOOLS_MODULE:-SAMtools}"
MTDNA_LOW_MAPPED_READS_THRESHOLD="${MTDNA_LOW_MAPPED_READS_THRESHOLD:-1000}"
MTDNA_LOW_COVERED_1X_PCT_THRESHOLD="${MTDNA_LOW_COVERED_1X_PCT_THRESHOLD:-50}"

if ! [[ "$MTDNA_LOW_MAPPED_READS_THRESHOLD" =~ ^[0-9]+$ ]]; then
    echo "ERROR: MTDNA_LOW_MAPPED_READS_THRESHOLD must be a non-negative integer"
    exit 1
fi

if ! [[ "$MTDNA_LOW_COVERED_1X_PCT_THRESHOLD" =~ ^([0-9]+([.][0-9]+)?|[.][0-9]+)$ ]]; then
    echo "ERROR: MTDNA_LOW_COVERED_1X_PCT_THRESHOLD must be numeric"
    exit 1
fi

SAMPLE_DIR="${WORKDIR_BASE}/${SAMPLE}"
mkdir -p "$SAMPLE_DIR"

STATUS_LOG="${SAMPLE_DIR}/pipeline_status.log"

echo "===== $(date) =====" >> "$STATUS_LOG"
echo "START SAMPLE: $SAMPLE" >> "$STATUS_LOG"
echo "THREADS: $THREADS" >> "$STATUS_LOG"

# ============================================================
# 1. WGS YAML
# ============================================================

WGS_YAML="${SAMPLE_DIR}/${SAMPLE}_wgs_param.yaml"

cat > "$WGS_YAML" <<EOF_WGS_YAML
mode: single
pipeline: wgs
workflow_backend: bash
software_stack: ${WGS_SOFTWARE_STACK}
resource: "${CBICALL_RESOURCE}"
genome: ${GENOME}
input_dir: ${SAMPLE_DIR}
project_dir: ${SAMPLE}_cbicall
cleanup_bam: false
export_mtdna_bam: true
EOF_WGS_YAML

# ============================================================
# 2. WGS JOB
# ============================================================

WGS_JOB_SCRIPT="${SAMPLE_DIR}/job_${SAMPLE}_wgs.slurm"

cat > "$WGS_JOB_SCRIPT" <<EOF_WGS_JOB
#!/usr/bin/env bash
#SBATCH --job-name=wgs_${SAMPLE}
#SBATCH --partition=${WGS_PARTITION}
${EXCLUDE_DIRECTIVE}
#SBATCH -D ${SAMPLE_DIR}
#SBATCH -o ${SAMPLE_DIR}/slurm-wgs-%j.out
#SBATCH -e ${SAMPLE_DIR}/slurm-wgs-%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=${THREADS}
#SBATCH --mem=${MEM}
#SBATCH -t ${WGS_TIME}

set -euo pipefail

STATUS_LOG="${STATUS_LOG}"
START_TS=\$(date +%s)

echo "WGS_REAL_START: \$(date)" >> "\$STATUS_LOG"
trap 'END_TS=\$(date +%s); DURATION=\$((END_TS - START_TS)); echo "WGS_REAL_END: \$(date)" >> "\$STATUS_LOG"; echo "WGS_DURATION_SECONDS: \$DURATION" >> "\$STATUS_LOG"' EXIT

export LANG=C
export LC_ALL=C

CBICALL="${CBICALL}"
CBICALL_PYTHON_PREFIX="${CBICALL_PYTHON_PREFIX}"

module load ${PYTHON_MODULE}
export PYTHONPATH="\$CBICALL_PYTHON_PREFIX/${PYTHON_SITE_PACKAGES_REL}\${PYTHONPATH:+:\$PYTHONPATH}"

cd ${SAMPLE_DIR}

"\$CBICALL" run \
    -p "${WGS_YAML}" \
    --runtime-profile "${RUNTIME_PROFILE}" \
    -t ${THREADS} \
    --no-color

sleep 2

SACCT_INFO=\$(sacct -j \$SLURM_JOB_ID \
    --format=JobID,State,Elapsed,TotalCPU,AllocCPUS,MaxRSS,ExitCode \
    --noheader | head -1)

echo "WGS_SACCT: \$SACCT_INFO" >> "\$STATUS_LOG"
EOF_WGS_JOB

JOB_WGS=$(sbatch --parsable "$WGS_JOB_SCRIPT")
echo "WGS_JOB: $JOB_WGS" >> "$STATUS_LOG"

# ============================================================
# 3. CHECK WGS
# ============================================================

CHECK_WGS_SCRIPT="${SAMPLE_DIR}/check_wgs.sh"

cat > "$CHECK_WGS_SCRIPT" <<EOF_CHECK_WGS
#!/usr/bin/env bash
set -euo pipefail

STATUS_LOG="${STATUS_LOG}"

WGS_DIR=\$(find "${SAMPLE_DIR}" -maxdepth 1 \
    -type d \
    -name "${SAMPLE}_cbicall_bash_${WGS_SOFTWARE_STACK}_wgs_*" \
    -printf '%T@ %p\n' 2>/dev/null | \
    sort -nr | \
    head -1 | \
    cut -d' ' -f2- || true)

if [[ -z "\$WGS_DIR" || ! -d "\$WGS_DIR" ]]; then
    echo "WGS_FAIL: output directory not found" >> "\$STATUS_LOG"
    exit 1
fi

BAM=\$(find "\$WGS_DIR/01_bam" -maxdepth 1 \
    -type f \
    -name "*.bam" \
    -print -quit 2>/dev/null || true)

if [[ -z "\$BAM" || ! -f "\$BAM" ]]; then
    echo "WGS_FAIL: missing BAM" >> "\$STATUS_LOG"
    exit 1
fi

VCF=\$(find "\$WGS_DIR/02_varcall" -maxdepth 1 \
    -type f \
    -name "*.hc.QC.vcf.gz" \
    -print -quit 2>/dev/null || true)

if [[ -z "\$VCF" || ! -f "\$VCF" ]]; then
    echo "WGS_FAIL: missing QC VCF" >> "\$STATUS_LOG"
    exit 1
fi

MTDNA_BAM=\$(find "\$WGS_DIR/exports/mtdna" -maxdepth 1 \
    -type f \
    -name "*_MIT.bam" \
    -print -quit 2>/dev/null || true)

if [[ -z "\$MTDNA_BAM" || ! -f "\$MTDNA_BAM" ]]; then
    echo "WGS_FAIL: missing exported mtDNA BAM" >> "\$STATUS_LOG"
    exit 1
fi

MTDNA_BAI="\${MTDNA_BAM}.bai"
if [[ ! -f "\$MTDNA_BAI" ]]; then
    echo "WGS_FAIL: missing exported mtDNA BAM index" >> "\$STATUS_LOG"
    exit 1
fi

if ! grep -q "All done!" "\$WGS_DIR/bash_${WGS_SOFTWARE_STACK}_wgs_single_${GENOME}.log"; then
    echo "WGS_FAIL: incomplete log" >> "\$STATUS_LOG"
    exit 1
fi

echo "WGS_DIR: \$WGS_DIR" >> "\$STATUS_LOG"
echo "WGS_MTDNA_BAM: \$MTDNA_BAM" >> "\$STATUS_LOG"
echo "WGS_OK" >> "\$STATUS_LOG"
EOF_CHECK_WGS

chmod +x "$CHECK_WGS_SCRIPT"

CHECK_WGS_SBATCH=(sbatch --parsable --partition="$SHORT_PARTITION" --dependency="afterok:$JOB_WGS")
if [[ -n "$EXCLUDE_NODES" ]]; then
    CHECK_WGS_SBATCH+=(--exclude="$EXCLUDE_NODES")
fi
JOB_CHECK_WGS=$("${CHECK_WGS_SBATCH[@]}" "$CHECK_WGS_SCRIPT")
echo "CHECK_WGS_JOB: $JOB_CHECK_WGS" >> "$STATUS_LOG"

# ============================================================
# 4. MIT YAML
# ============================================================

MIT_YAML="${SAMPLE_DIR}/${SAMPLE}_mit_param.yaml"

cat > "$MIT_YAML" <<EOF_MIT_YAML
mode: single
pipeline: mit
workflow_backend: bash
input_dir: ${SAMPLE_DIR}
EOF_MIT_YAML

# ============================================================
# 5. MIT JOB
# ============================================================

MIT_JOB_SCRIPT="${SAMPLE_DIR}/job_${SAMPLE}_mit.slurm"

cat > "$MIT_JOB_SCRIPT" <<EOF_MIT_JOB
#!/usr/bin/env bash
#SBATCH --job-name=mit_${SAMPLE}
#SBATCH --partition=${SHORT_PARTITION}
${EXCLUDE_DIRECTIVE}
#SBATCH -D ${SAMPLE_DIR}
#SBATCH -o ${SAMPLE_DIR}/slurm-mit-%j.out
#SBATCH -e ${SAMPLE_DIR}/slurm-mit-%j.err
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=${THREADS}
#SBATCH --mem=${MEM}
#SBATCH -t ${MIT_TIME}

set -euo pipefail

STATUS_LOG="${STATUS_LOG}"
START_TS=\$(date +%s)
echo "MIT_REAL_START: \$(date)" >> "\$STATUS_LOG"
trap 'END_TS=\$(date +%s); DURATION=\$((END_TS - START_TS)); echo "MIT_REAL_END: \$(date)" >> "\$STATUS_LOG"; echo "MIT_DURATION_SECONDS: \$DURATION" >> "\$STATUS_LOG"' EXIT

export LANG=C
export LC_ALL=C

CBICALL="${CBICALL}"
CBICALL_PYTHON_PREFIX="${CBICALL_PYTHON_PREFIX}"

module load ${PYTHON_MODULE}
export PYTHONPATH="\$CBICALL_PYTHON_PREFIX/${PYTHON_SITE_PACKAGES_REL}\${PYTHONPATH:+:\$PYTHONPATH}"

cd ${SAMPLE_DIR}

"\$CBICALL" run \
    -p "${MIT_YAML}" \
    --runtime-profile "${RUNTIME_PROFILE}" \
    -t ${THREADS} \
    --no-color

sleep 2

SACCT_INFO=\$(sacct -j \$SLURM_JOB_ID \
    --format=JobID,State,Elapsed,TotalCPU,AllocCPUS,MaxRSS,ExitCode \
    --noheader | head -1)

echo "MIT_SACCT: \$SACCT_INFO" >> "\$STATUS_LOG"
EOF_MIT_JOB

JOB_MIT=$(sbatch --parsable --dependency="afterok:$JOB_CHECK_WGS" "$MIT_JOB_SCRIPT")
echo "MIT_JOB: $JOB_MIT" >> "$STATUS_LOG"

# ============================================================
# 6. CHECK MIT
# ============================================================

CHECK_MIT_SCRIPT="${SAMPLE_DIR}/check_mit.sh"

cat > "$CHECK_MIT_SCRIPT" <<EOF_CHECK_MIT
#!/usr/bin/env bash
set -euo pipefail

STATUS_LOG="${STATUS_LOG}"

module load "${SAMTOOLS_MODULE}"

LOW_MAPPED_READS_THRESHOLD=${MTDNA_LOW_MAPPED_READS_THRESHOLD}
LOW_COVERED_1X_PCT_THRESHOLD=${MTDNA_LOW_COVERED_1X_PCT_THRESHOLD}

# Locate WGS output and exported mtDNA BAM.
WGS_DIR=\$(find "${SAMPLE_DIR}" -maxdepth 1 \
    -type d \
    -name "${SAMPLE}_cbicall_bash_${WGS_SOFTWARE_STACK}_wgs_*" \
    -printf '%T@ %p\n' 2>/dev/null | \
    sort -nr | \
    head -1 | \
    cut -d' ' -f2- || true)

if [[ -z "\$WGS_DIR" || ! -d "\$WGS_DIR" ]]; then
    echo "MIT_FAIL: WGS output directory not found" >> "\$STATUS_LOG"
    exit 1
fi

MTDNA_BAM=\$(find "\$WGS_DIR/exports/mtdna" -maxdepth 1 \
    -type f \
    -name "*_MIT.bam" \
    -print -quit 2>/dev/null || true)

if [[ -z "\$MTDNA_BAM" || ! -f "\$MTDNA_BAM" ]]; then
    echo "MTDNA_BAM_CHECK: FAIL" >> "\$STATUS_LOG"
    echo "MIT_FAIL: exported mtDNA BAM not found" >> "\$STATUS_LOG"
    exit 1
fi

echo "MTDNA_BAM_CHECK: OK \$MTDNA_BAM" >> "\$STATUS_LOG"

MTDNA_BAI="\${MTDNA_BAM}.bai"
if [[ ! -f "\$MTDNA_BAI" ]]; then
    echo "MTDNA_BAI_CHECK: FAIL" >> "\$STATUS_LOG"
    echo "MIT_FAIL: exported mtDNA BAM index not found" >> "\$STATUS_LOG"
    exit 1
fi

echo "MTDNA_BAI_CHECK: OK \$MTDNA_BAI" >> "\$STATUS_LOG"

# Validate mtDNA BAM and collect coverage statistics.
if ! command -v samtools >/dev/null 2>&1; then
    echo "MTDNA_BAM_QUICKCHECK: NOT_RUN samtools_not_found" >> "\$STATUS_LOG"
    echo "MIT_FAIL: samtools not available for mtDNA BAM validation" >> "\$STATUS_LOG"
    exit 1
fi

if samtools quickcheck "\$MTDNA_BAM"; then
    echo "MTDNA_BAM_QUICKCHECK: OK" >> "\$STATUS_LOG"
else
    echo "MTDNA_BAM_QUICKCHECK: FAIL" >> "\$STATUS_LOG"
    echo "MIT_FAIL: exported mtDNA BAM failed samtools quickcheck" >> "\$STATUS_LOG"
    exit 1
fi

if samtools idxstats "\$MTDNA_BAM" >/dev/null 2>&1; then
    echo "MTDNA_BAI_USABLE: OK" >> "\$STATUS_LOG"
else
    echo "MTDNA_BAI_USABLE: FAIL" >> "\$STATUS_LOG"
    echo "MIT_FAIL: exported mtDNA BAM index is not usable" >> "\$STATUS_LOG"
    exit 1
fi

if ! MTDNA_TOTAL_READS=\$(samtools view -c "\$MTDNA_BAM"); then
    echo "MIT_FAIL: unable to count mtDNA BAM reads" >> "\$STATUS_LOG"
    exit 1
fi

if ! MTDNA_MAPPED_READS=\$(samtools view -c -F 4 "\$MTDNA_BAM"); then
    echo "MIT_FAIL: unable to count mapped mtDNA BAM reads" >> "\$STATUS_LOG"
    exit 1
fi

if ! DEPTH_STATS=\$(samtools depth -a -r chrM "\$MTDNA_BAM" 2>/dev/null | \
    awk '{
        sum += \$3
        if (\$3 > 0) c1++
        n++
    }
    END {
        if (n > 0)
            printf "%.4f %.4f", sum/n, 100*c1/n
        else
            printf "0.0000 0.0000"
    }'); then
    echo "MIT_FAIL: unable to calculate mtDNA coverage statistics" >> "\$STATUS_LOG"
    exit 1
fi

read -r MTDNA_MEAN_DEPTH MTDNA_COVERED_1X_PCT <<< "\$DEPTH_STATS"
if [[ -z "\${MTDNA_MEAN_DEPTH:-}" || -z "\${MTDNA_COVERED_1X_PCT:-}" ]]; then
    echo "MIT_FAIL: empty mtDNA coverage statistics" >> "\$STATUS_LOG"
    exit 1
fi

echo "MTDNA_TOTAL_READS: \$MTDNA_TOTAL_READS" >> "\$STATUS_LOG"
echo "MTDNA_MAPPED_READS: \$MTDNA_MAPPED_READS" >> "\$STATUS_LOG"
echo "MTDNA_MEAN_DEPTH: \$MTDNA_MEAN_DEPTH" >> "\$STATUS_LOG"
echo "MTDNA_COVERED_1X_PCT: \$MTDNA_COVERED_1X_PCT" >> "\$STATUS_LOG"

LOW_READS=0
LOW_COVERAGE=0

if (( MTDNA_MAPPED_READS < LOW_MAPPED_READS_THRESHOLD )); then
    LOW_READS=1
fi

if awk -v value="\$MTDNA_COVERED_1X_PCT" \
       -v threshold="\$LOW_COVERED_1X_PCT_THRESHOLD" \
       'BEGIN { exit !(value < threshold) }'; then
    LOW_COVERAGE=1
fi

# Locate and validate MIT output.
MIT_DIR=\$(find "${SAMPLE_DIR}" -maxdepth 1 \
    -type d \
    -name "cbicall_bash_${MIT_SOFTWARE_STACK}_mit_single_${MIT_REFERENCE}_*" \
    -printf '%T@ %p\n' 2>/dev/null | \
    sort -nr | \
    head -1 | \
    cut -d' ' -f2- || true)

if [[ -z "\$MIT_DIR" || ! -d "\$MIT_DIR" ]]; then
    if (( LOW_READS == 1 && LOW_COVERAGE == 1 )); then
        echo "MIT_FAIL_REASON: LOW_MTDNA_SIGNAL" >> "\$STATUS_LOG"
        echo "WGS_BAM_CLEANUP_CANDIDATE: YES" >> "\$STATUS_LOG"
        echo "MIT_FAIL: LOW_MTDNA_SIGNAL mapped_reads=\$MTDNA_MAPPED_READS mean_depth=\$MTDNA_MEAN_DEPTH covered_1x_pct=\$MTDNA_COVERED_1X_PCT; mtDNA_BAM=OK mtDNA_BAI=OK quickcheck=OK; WGS_BAM_CLEANUP_CANDIDATE=YES" >> "\$STATUS_LOG"
    else
        echo "MIT_FAIL: output directory not found" >> "\$STATUS_LOG"
    fi
    exit 1
fi

MIT_LOG="\$MIT_DIR/bash_${MIT_SOFTWARE_STACK}_mit_single_${MIT_REFERENCE}.log"
VCF="\$MIT_DIR/01_mtoolbox/VCF_file.vcf"

echo "MIT_DIR: \$MIT_DIR" >> "\$STATUS_LOG"

if [[ -f "\$VCF" ]] && [[ -f "\$MIT_LOG" ]] && grep -q "All done!!!" "\$MIT_LOG"; then
    echo "MIT_VCF: \$VCF" >> "\$STATUS_LOG"
    echo "MIT_OK" >> "\$STATUS_LOG"
    exit 0
fi

if (( LOW_READS == 1 && LOW_COVERAGE == 1 )); then
    echo "MIT_FAIL_REASON: LOW_MTDNA_SIGNAL" >> "\$STATUS_LOG"

    if [[ -f "\$MIT_LOG" ]] && \
       grep -q "consensus_value.*referenced before assignment" "\$MIT_LOG"; then
        echo "MIT_FAILURE_SIGNATURE: MTOOLBOX_CONSENSUS_VALUE_ERROR" >> "\$STATUS_LOG"
    fi

    echo "WGS_BAM_CLEANUP_CANDIDATE: YES" >> "\$STATUS_LOG"
    echo "MIT_FAIL: LOW_MTDNA_SIGNAL mapped_reads=\$MTDNA_MAPPED_READS mean_depth=\$MTDNA_MEAN_DEPTH covered_1x_pct=\$MTDNA_COVERED_1X_PCT; mtDNA_BAM=OK mtDNA_BAI=OK quickcheck=OK; WGS_BAM_CLEANUP_CANDIDATE=YES" >> "\$STATUS_LOG"
    exit 1
fi

if [[ ! -f "\$VCF" ]]; then
    echo "MIT_FAIL: missing VCF" >> "\$STATUS_LOG"
    exit 1
fi

if [[ ! -f "\$MIT_LOG" ]]; then
    echo "MIT_FAIL: MIT log not found" >> "\$STATUS_LOG"
    exit 1
fi

if ! grep -q "All done!!!" "\$MIT_LOG"; then
    echo "MIT_FAIL: incomplete log" >> "\$STATUS_LOG"
    exit 1
fi

echo "MIT_FAIL: unknown MIT validation error" >> "\$STATUS_LOG"
exit 1
EOF_CHECK_MIT

chmod +x "$CHECK_MIT_SCRIPT"

CHECK_MIT_SBATCH=(sbatch --parsable --partition="$SHORT_PARTITION" --dependency="afterany:$JOB_MIT")
if [[ -n "$EXCLUDE_NODES" ]]; then
    CHECK_MIT_SBATCH+=(--exclude="$EXCLUDE_NODES")
fi
JOB_CHECK_MIT=$("${CHECK_MIT_SBATCH[@]}" "$CHECK_MIT_SCRIPT")
echo "CHECK_MIT_JOB: $JOB_CHECK_MIT" >> "$STATUS_LOG"

# ============================================================
# 7. CLEANUP
# ============================================================

CLEAN_SCRIPT="${SAMPLE_DIR}/cleanup.sh"

cat > "$CLEAN_SCRIPT" <<EOF_CLEANUP
#!/usr/bin/env bash
set -euo pipefail

STATUS_LOG="${STATUS_LOG}"

module load "${SAMTOOLS_MODULE}"

WGS_DIR=\$(find "${SAMPLE_DIR}" -maxdepth 1 \
    -type d \
    -name "${SAMPLE}_cbicall_bash_${WGS_SOFTWARE_STACK}_wgs_*" \
    -printf '%T@ %p\n' 2>/dev/null | \
    sort -nr | \
    head -1 | \
    cut -d' ' -f2- || true)

if [[ -z "\$WGS_DIR" || ! -d "\$WGS_DIR" ]]; then
    echo "CLEANUP_ABORTED: WGS output directory not found" >> "\$STATUS_LOG"
    exit 1
fi

BAM_DIR="\$WGS_DIR/01_bam"
if [[ ! -d "\$BAM_DIR" ]]; then
    echo "CLEANUP_ABORTED: WGS BAM directory not found" >> "\$STATUS_LOG"
    exit 1
fi

MTDNA_BAM=\$(find "\$WGS_DIR/exports/mtdna" -maxdepth 1 \
    -type f \
    -name "*_MIT.bam" \
    -print -quit 2>/dev/null || true)

if [[ -z "\$MTDNA_BAM" || ! -f "\$MTDNA_BAM" ]]; then
    echo "CLEANUP_ABORTED: exported mtDNA BAM not found" >> "\$STATUS_LOG"
    exit 1
fi

MTDNA_BAI="\${MTDNA_BAM}.bai"
if [[ ! -f "\$MTDNA_BAI" ]]; then
    echo "CLEANUP_ABORTED: exported mtDNA BAM index not found" >> "\$STATUS_LOG"
    exit 1
fi

if ! command -v samtools >/dev/null 2>&1; then
    echo "CLEANUP_ABORTED: samtools not available for mtDNA BAM validation" >> "\$STATUS_LOG"
    exit 1
fi

if samtools quickcheck "\$MTDNA_BAM"; then
    echo "CLEANUP_MTDNA_QUICKCHECK: OK" >> "\$STATUS_LOG"
else
    echo "CLEANUP_ABORTED: exported mtDNA BAM failed samtools quickcheck" >> "\$STATUS_LOG"
    exit 1
fi

rm -f "\$BAM_DIR"/*.bam
rm -f "\$BAM_DIR"/*.bai

echo "CLEANUP_MTDNA_PRESERVED: \$MTDNA_BAM" >> "\$STATUS_LOG"
echo "CLEANUP_OK" >> "\$STATUS_LOG"
EOF_CLEANUP

chmod +x "$CLEAN_SCRIPT"

CLEANUP_SBATCH=(sbatch --parsable --partition="$SHORT_PARTITION" --dependency="afterok:$JOB_CHECK_MIT")
if [[ -n "$EXCLUDE_NODES" ]]; then
    CLEANUP_SBATCH+=(--exclude="$EXCLUDE_NODES")
fi
JOB_CLEANUP=$("${CLEANUP_SBATCH[@]}" "$CLEAN_SCRIPT")
echo "CLEANUP_JOB: $JOB_CLEANUP" >> "$STATUS_LOG"

echo "PIPELINE_LAUNCHED_OK" >> "$STATUS_LOG"
echo "Pipeline launched for $SAMPLE"
