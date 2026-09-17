# CBIcall WGS + mtDNA Slurm Orchestrator

A lightweight Slurm orchestration layer for running **CBIcall whole-genome sequencing (WGS) followed by mitochondrial DNA (mtDNA) analysis**, with explicit validation checkpoints, dependency-aware execution, failure diagnostics, status logging, batch monitoring, and conservative cleanup of large intermediate WGS BAM files.

This repository does **not** replace or reimplement CBIcall. It is an external Bash/Slurm wrapper designed to automate repeated CBIcall executions on HPC systems.

> **CBIcall** is a configuration-driven framework for reproducible variant calling in large sequencing cohorts.

Official CBIcall resources:

- Repository: https://github.com/CNAG-Biomedical-Informatics/cbicall
- Documentation: https://cnag-biomedical-informatics.github.io/cbicall/
- Publication: Rueda M, Fernandez-Orth D, Gut IG. *CBIcall: a configuration-driven framework for variant calling in large sequencing cohorts*. **Bioinformatics Advances** (2026). https://doi.org/10.1093/bioadv/vbag232

---

## Why this wrapper exists

Running WGS and mtDNA analysis over many samples on an HPC cluster involves more than repeatedly submitting the same command.

For each sample, the orchestrator:

1. generates the CBIcall YAML configuration for WGS;
2. submits the WGS analysis through Slurm;
3. validates the expected WGS outputs;
4. verifies that the mtDNA BAM exported by the WGS workflow exists and is indexed;
5. launches the CBIcall mitochondrial workflow only after WGS validation succeeds;
6. runs an mtDNA validation/diagnostic step even if the mitochondrial CBIcall job exits unsuccessfully;
7. characterizes failures compatible with very low mtDNA signal;
8. removes large WGS BAM intermediates only after mitochondrial analysis has been successfully validated;
9. records job IDs, timestamps, durations, validation results, and mtDNA diagnostics in a per-sample `pipeline_status.log`.

The main design principle is **fail safe**: cleanup must never be reached after an unsuccessful or unvalidated mitochondrial analysis.

---

## Workflow

```text
               CBIcall WGS
                    |
                    | afterok
                    v
               CHECK_WGS
                    |
                    | validates:
                    | - WGS output directory
                    | - WGS BAM
                    | - QC VCF
                    | - exported mtDNA BAM
                    | - mtDNA BAM index
                    | - successful WGS log marker
                    v
               CBIcall MIT
                    |
                    | afterany
                    v
               CHECK_MIT
                    |
                    | validates/records:
                    | - exported mtDNA BAM
                    | - BAM index
                    | - samtools quickcheck
                    | - usable BAM index
                    | - total/mapped reads
                    | - mean depth
                    | - % chrM covered >=1x
                    | - MIT output / VCF / completion log
                    | - low-mtDNA-signal failure class
                    v
           success only (afterok)
                    |
                    v
                 CLEANUP
                    |
                    | re-validates:
                    | - exported mtDNA BAM
                    | - BAM index
                    | - samtools quickcheck
                    v
        remove WGS 01_bam/*.bam and *.bai
        preserve exports/mtdna/*_MIT.bam(.bai)
```

The Slurm dependency chain is therefore:

```text
WGS
 └── afterok → CHECK_WGS
                 └── afterok → MIT
                                └── afterany → CHECK_MIT
                                                └── afterok → CLEANUP
```

The `afterany` dependency for `CHECK_MIT` is intentional. It allows the validator to inspect the exported mitochondrial BAM and characterize certain mitochondrial failures instead of leaving only a generic failed Slurm job.

`CLEANUP` still depends on `CHECK_MIT` with `afterok`, so a failed mitochondrial validation **does not delete the WGS BAMs**.

---

## Repository layout

```text
cbicall-slurm-wgs-mtdna-orchestrator/
├── README.md
├── LICENSE
├── CITATION.md
├── .gitignore
├── cbicall_wgs_mtdna_slurm.sh
├── run_batch.sh
├── config/
│   └── config.example.env
└── tools/
    └── summarize_pipeline_status.sh
```

### Main files

**`cbicall_wgs_mtdna_slurm.sh`**  
Main per-sample orchestrator. It creates the CBIcall YAML files, generates the Slurm jobs and validation scripts, and submits the full dependency chain.

**`run_batch.sh`**  
Batch launcher. It reads a text file containing one sample ID per line and submits one independent orchestrator chain per sample.

**`config/config.example.env`**  
Public example configuration. Copy it to `config/config.env` and adapt the values to the local HPC environment.

**`tools/summarize_pipeline_status.sh`**  
Batch monitoring and diagnostics utility. It scans the per-sample `pipeline_status.log` files, reports the current state of each pipeline, retrieves Slurm job states/reasons, and summarizes mtDNA validation metrics.

**`CITATION.md`**  
Citation information for this repository and the underlying CBIcall software.

**`LICENSE`**  
Repository license.

---

## Requirements

### Software

The orchestrator assumes the following are available:

- Linux / Bash;
- Slurm (`sbatch`, `squeue`, `sacct`);
- CBIcall;
- a CBIcall-compatible Python environment;
- SAMtools;
- the resources and software required by the selected CBIcall WGS and mitochondrial workflows.

CBIcall itself must be installed and configured independently.

### HPC environment

The scripts assume that:

- compute partitions are available for long WGS jobs and shorter validation/mtDNA jobs;
- CBIcall can be executed from compute nodes;
- sample directories are visible from the compute nodes;
- the required CBIcall reference/resource bundles are already installed;
- Slurm accounting is available through `sacct`.

Partition names, runtime profiles, memory, walltime, excluded nodes, module names, Python paths, and CBIcall installation paths are **site-specific**.

---

## Configuration

Create the private local configuration from the public example:

```bash
cp config/config.example.env config/config.env
```

Then edit:

```bash
config/config.env
```

The current example configuration contains:

```bash
# CBIcall executable and Python environment
CBICALL="/path/to/cbicall/bin/cbicall"
CBICALL_PYTHON_PREFIX="/path/to/cbicall/python/environment"

# Environment/module
PYTHON_MODULE="Python/X.Y.Z"
PYTHON_SITE_PACKAGES_REL="lib/pythonX.Y/site-packages"
SAMTOOLS_MODULE="SAMtools"

# Site-specific settings
RUNTIME_PROFILE="XXX"
WGS_PARTITION="XXX"
SHORT_PARTITION="XXX"
EXCLUDE_NODES=""

MEM="24G"
WGS_TIME="20-00:00:00"
MIT_TIME="10:00:00"

# CBIcall WGS settings
CBICALL_RESOURCE="cbicall-germline-resources-v1"
GENOME="hg38"
WGS_SOFTWARE_STACK="gatk-4.6"

# CBIcall MIT settings
MIT_SOFTWARE_STACK="gatk-3.5"
MIT_REFERENCE="rsrs"

# mtDNA low-signal thresholds
MTDNA_LOW_MAPPED_READS_THRESHOLD="1000"
MTDNA_LOW_COVERED_1X_PCT_THRESHOLD="50"
```

`config/config.env` must remain local and should **not** be committed. The repository `.gitignore` excludes it.

A different configuration file can be supplied through:

```bash
export CBICALL_ORCHESTRATOR_CONFIG=/path/to/another/config.env
```

The orchestrator uses the following defaults if the optional settings are not specified:

```text
SAMTOOLS_MODULE=SAMtools
MTDNA_LOW_MAPPED_READS_THRESHOLD=1000
MTDNA_LOW_COVERED_1X_PCT_THRESHOLD=50
```

---

## Input layout

Each sample is expected to have its own directory below a common working directory:

```text
WORKDIR_BASE/
├── SAMPLE001/
│   ├── SAMPLE001_R1.fastq.gz
│   └── SAMPLE001_R2.fastq.gz
├── SAMPLE002/
│   ├── SAMPLE002_R1.fastq.gz
│   └── SAMPLE002_R2.fastq.gz
└── SAMPLE003/
    ├── SAMPLE003_R1.fastq.gz
    └── SAMPLE003_R2.fastq.gz
```

The exact FASTQ naming requirements are ultimately determined by CBIcall.

---

## Running one sample

General syntax:

```bash
./cbicall_wgs_mtdna_slurm.sh <SAMPLE_ID> <WORKDIR_BASE> [THREADS]
```

Example:

```bash
./cbicall_wgs_mtdna_slurm.sh SAMPLE001 /path/to/WGS 4
```

Arguments:

```text
1. SAMPLE_ID      Sample directory / identifier
2. WORKDIR_BASE   Parent directory containing the sample directory
3. THREADS        Optional; default = 4
```

The orchestrator immediately submits the complete dependency chain. Slurm then controls when each job actually starts.

---

## Running multiple samples

Create a text file containing one sample ID per line:

```text
SAMPLE001
SAMPLE002
SAMPLE003
```

Empty lines and lines beginning with `#` are ignored by the batch launcher.

Run:

```bash
./run_batch.sh <SAMPLE_LIST> <WORKDIR_BASE> [THREADS]
```

For example:

```bash
./run_batch.sh samples.txt /path/to/WGS 8
```

`run_batch.sh` submits one independent orchestrator chain per sample and writes a timestamped batch log:

```text
run_batch_YYYYMMDD_HHMMSS.log
```

The log records successful submissions, failed submissions, and orchestrator exit codes.

Submitting multiple samples does not mean that all WGS jobs start simultaneously. Slurm schedules them according to resources, priorities, quotas, and cluster policy.

---

## WGS configuration generated by the orchestrator

For each sample, the orchestrator creates a CBIcall WGS YAML equivalent to:

```yaml
mode: single
pipeline: wgs
workflow_backend: bash
software_stack: gatk-4.6
resource: "cbicall-germline-resources-v1"
genome: hg38
input_dir: /path/to/sample
project_dir: SAMPLE_cbicall
cleanup_bam: false
export_mtdna_bam: true
```

Two parameters are especially important:

```yaml
cleanup_bam: false
export_mtdna_bam: true
```

- `cleanup_bam: false` prevents the WGS workflow from deleting BAM files before downstream validation and mtDNA analysis are complete.
- `export_mtdna_bam: true` instructs CBIcall to export the mitochondrial BAM under `exports/mtdna/`.

---

## WGS validation (`CHECK_WGS`)

After the WGS Slurm job completes successfully, `CHECK_WGS` locates the most recent matching WGS output directory.

It verifies:

```text
WGS output directory
01_bam/*.bam
02_varcall/*.hc.QC.vcf.gz
exports/mtdna/*_MIT.bam
exports/mtdna/*_MIT.bam.bai
successful WGS completion marker
```

A successful validation records:

```text
WGS_DIR: ...
WGS_MTDNA_BAM: ...
WGS_OK
```

If any required output is missing, `CHECK_WGS` exits with a non-zero status and the mitochondrial analysis is not started.

---

## Mitochondrial analysis

The mitochondrial YAML is equivalent to:

```yaml
mode: single
pipeline: mit
workflow_backend: bash
input_dir: /path/to/sample
```

The mitochondrial CBIcall job is submitted only after successful WGS validation.

---

## Mitochondrial validation and diagnostics (`CHECK_MIT`)

`CHECK_MIT` runs after the mitochondrial CBIcall job with an `afterany` dependency.

This is different from a normal strict `afterok` validation step: the checker also runs when the MIT job itself fails so that the failure can be characterized.

### mtDNA BAM validation

The checker locates the mtDNA BAM previously exported by WGS and verifies:

```text
mtDNA BAM exists
mtDNA BAM index exists
samtools quickcheck succeeds
samtools idxstats can use the index
```

It records:

```text
MTDNA_BAM_CHECK
MTDNA_BAI_CHECK
MTDNA_BAI_USABLE
MTDNA_BAM_QUICKCHECK
```

### mtDNA signal metrics

The checker calculates:

```text
MTDNA_TOTAL_READS
MTDNA_MAPPED_READS
MTDNA_MEAN_DEPTH
MTDNA_COVERED_1X_PCT
```

`MTDNA_COVERED_1X_PCT` is the percentage of `chrM` positions covered at least once.

### Low mtDNA signal classification

A failure is classified as:

```text
MIT_FAIL_REASON: LOW_MTDNA_SIGNAL
```

only when **both** conditions are met:

```text
MTDNA_MAPPED_READS < MTDNA_LOW_MAPPED_READS_THRESHOLD
AND
MTDNA_COVERED_1X_PCT < MTDNA_LOW_COVERED_1X_PCT_THRESHOLD
```

With the example configuration:

```text
mapped reads < 1000
AND
chrM coverage >=1x < 50%
```

This is an **operational failure classification used by the orchestrator**, not a biological or clinical QC threshold.

When the low-signal condition is met and the exported BAM, index, and BAM integrity checks are valid, the status log also records:

```text
WGS_BAM_CLEANUP_CANDIDATE: YES
```

This flag is informational only. The current orchestrator does **not** automatically delete the WGS BAM when MIT fails.

If the mitochondrial log contains the known MToolBox signature:

```text
consensus_value ... referenced before assignment
```

the checker additionally records:

```text
MIT_FAILURE_SIGNATURE: MTOOLBOX_CONSENSUS_VALUE_ERROR
```

### Successful mitochondrial validation

A normal successful run still requires the expected mitochondrial output, VCF, and completion log:

```text
MIT_DIR: ...
MIT_VCF: ...
MIT_OK
```

Only a successful `CHECK_MIT` allows the cleanup stage to run.

---

## Safe cleanup

WGS BAMs can occupy substantial disk space. Cleanup is therefore deliberately conservative.

`CLEANUP` runs only after:

```text
WGS completed
      +
CHECK_WGS passed
      +
MIT completed successfully
      +
CHECK_MIT passed
```

Immediately before deleting WGS BAMs, the cleanup script verifies again that:

```text
exported mtDNA BAM exists
exported mtDNA BAM index exists
samtools quickcheck succeeds
```

Only then are the WGS BAM/BAM-index files removed:

```text
WGS_OUTPUT/01_bam/*.bam
WGS_OUTPUT/01_bam/*.bai
```

The exported mitochondrial files are preserved.

Successful cleanup records:

```text
CLEANUP_MTDNA_QUICKCHECK: OK
CLEANUP_MTDNA_PRESERVED: ...
CLEANUP_OK
```

A validation failure should consume additional storage rather than risk deleting a BAM that may still be required.

---

## `pipeline_status.log`

Each sample receives a persistent status log.

A successful run contains entries conceptually similar to:

```text
===== DATE =====
START SAMPLE: SAMPLE001
THREADS: 4

WGS_JOB: <job-id>
CHECK_WGS_JOB: <job-id>
MIT_JOB: <job-id>
CHECK_MIT_JOB: <job-id>
CLEANUP_JOB: <job-id>

PIPELINE_LAUNCHED_OK

WGS_REAL_START: ...
WGS_REAL_END: ...
WGS_DURATION_SECONDS: ...
WGS_DIR: ...
WGS_MTDNA_BAM: ...
WGS_OK

MIT_REAL_START: ...
MIT_REAL_END: ...
MIT_DURATION_SECONDS: ...

MTDNA_BAM_CHECK: OK ...
MTDNA_BAI_CHECK: OK ...
MTDNA_BAI_USABLE: OK
MTDNA_BAM_QUICKCHECK: OK
MTDNA_TOTAL_READS: ...
MTDNA_MAPPED_READS: ...
MTDNA_MEAN_DEPTH: ...
MTDNA_COVERED_1X_PCT: ...

MIT_DIR: ...
MIT_VCF: ...
MIT_OK

CLEANUP_MTDNA_QUICKCHECK: OK
CLEANUP_MTDNA_PRESERVED: ...
CLEANUP_OK
```

If a sample has been launched more than once, additional run blocks are appended to the same file.

---

## Batch status summarizer

The repository includes:

```text
tools/summarize_pipeline_status.sh
```

This utility provides a cohort-level view of a batch launched with the orchestrator.

### What it does

The script:

- recursively finds `pipeline_status.log` files below the current directory;
- analyzes only the **most recent run** in each status log;
- retrieves the WGS, CHECK_WGS, MIT, CHECK_MIT, and CLEANUP job IDs;
- queries `squeue` for currently active/pending jobs and their reasons;
- falls back to `sacct` for jobs no longer present in the queue;
- identifies Slurm failures;
- detects failures already recorded by the orchestrator validation steps;
- distinguishes `FAIL_MIT_LOW_SIGNAL` from other mitochondrial failures;
- extracts mtDNA BAM validation results and signal metrics;
- reports the current stage for samples that are still processing.

### Running the summarizer

Run it from the directory containing the individual sample directories:

```bash
cd /path/to/WORKDIR_BASE
/path/to/cbicall-slurm-wgs-mtdna-orchestrator/tools/summarize_pipeline_status.sh
```

### Generated outputs

The summarizer creates:

```text
paths_pipeline_status_logs
pipeline_status_summary.log
jobs_by_sample.tsv
```

#### `pipeline_status_summary.log`

Provides global counts and one status row per sample.

Possible classifications include:

```text
OK
PROCESSING
FAIL_WGS
FAIL_WGS_CHECK
FAIL_MIT
FAIL_MIT_CHECK
FAIL_MIT_LOW_SIGNAL
FAIL_CLEANUP
```

For samples still running, the detail field may indicate:

```text
queued_waiting_for_WGS
running_WGS
waiting_for_MIT
running_MIT
waiting_or_running_CLEANUP
```

#### `jobs_by_sample.tsv`

Provides a tab-separated table containing:

- sample ID;
- job IDs for WGS, CHECK_WGS, MIT, CHECK_MIT, and CLEANUP;
- Slurm state and reason for each job;
- mtDNA BAM/BAI validation status;
- BAM quickcheck status;
- mapped mtDNA reads;
- mean mtDNA depth;
- percentage of `chrM` covered at least 1x;
- MIT failure reason;
- `WGS_BAM_CLEANUP_CANDIDATE`.

Recommended viewing:

```bash
column -t -s $'\t' pipeline_status_summary.log | less -S
```

```bash
column -t -s $'\t' jobs_by_sample.tsv | less -S
```

---

## Manual Slurm monitoring

Current jobs:

```bash
squeue -u "$USER"
```

Inspect an individual job:

```bash
sacct -j <JOB_ID> \
    --format=JobID,JobName,State,ExitCode,Elapsed,TotalCPU,AllocCPUS,MaxRSS,NodeList
```

The batch summarizer is intended to avoid repeatedly running these commands manually for every sample.

---

## Failure behaviour

### WGS job or WGS validation fails

The mitochondrial workflow is not started and no cleanup occurs.

### MIT job fails

`CHECK_MIT` still runs because it uses:

```text
afterany:<MIT_JOB>
```

The checker attempts to validate the exported mtDNA BAM and characterize the failure.

If the failure is compatible with very low mitochondrial signal, it records:

```text
MIT_FAIL_REASON: LOW_MTDNA_SIGNAL
WGS_BAM_CLEANUP_CANDIDATE: YES
```

`CHECK_MIT` exits unsuccessfully, so cleanup is not executed.

### MIT validation fails for another reason

The checker records the corresponding `MIT_FAIL` message and exits non-zero.

Cleanup is not executed.

### Cleanup validation fails

If the exported mtDNA BAM/index cannot be validated immediately before deletion, cleanup aborts and WGS BAMs are preserved.

---

## Important implementation notes

### Slurm dependencies

The dependency types are intentionally asymmetric:

```text
WGS       -> CHECK_WGS : afterok
CHECK_WGS -> MIT       : afterok
MIT       -> CHECK_MIT : afterany
CHECK_MIT -> CLEANUP   : afterok
```

This allows diagnostic inspection of a failed MIT job without weakening cleanup safety.

### CBIcall is executed directly inside the Slurm allocation

The generated jobs execute:

```bash
"$CBICALL" run ...
```

rather than starting an additional nested `srun` job step.

### Output-directory discovery

CBIcall output directories include run-specific suffixes. The validation scripts use `find`, modification time, and sorting to select the most recent directory matching the expected CBIcall naming pattern.

### Slurm accounting

The orchestrator records `sacct` information when available. Depending on accounting update timing, those fields can occasionally be empty even when a job completed correctly.

Pipeline validity is therefore determined by explicit output checks and CBIcall completion logs rather than by `sacct` alone.

---

## Data and privacy

This repository is intended to contain **code only**.

Do not commit:

- FASTQ files;
- BAM/CRAM files;
- VCF/gVCF files containing individual-level genomic data;
- sample metadata containing protected information;
- Slurm logs containing sensitive paths or identifiers;
- credentials, keys, or tokens;
- private HPC paths;
- private site-specific configuration.

The repository `.gitignore` excludes common genomic data types, logs, temporary files, credentials, and:

```text
config/config.env
```

Before every public push, inspect:

```bash
git status
git diff --cached
```

---

## Compatibility and maintenance

The orchestrator intentionally validates specific CBIcall output names. This makes failures explicit, but it also means that **CBIcall version changes can require updates to the wrapper**.

When upgrading CBIcall, review at least:

1. WGS output-directory naming;
2. WGS log filename and successful-completion marker;
3. QC VCF location/name;
4. `exports/mtdna/` naming;
5. mtDNA output-directory naming;
6. mtDNA log filename and completion marker;
7. `01_mtoolbox/VCF_file.vcf` location;
8. exported mtDNA BAM/index naming.

Always validate a known sample before submitting a large cohort.

---

## Relationship to CBIcall

This repository is an independent orchestration and monitoring layer built around CBIcall.

CBIcall performs the genomic analyses. This repository adds:

- Slurm dependency orchestration;
- per-stage output validation;
- WGS-to-mtDNA chaining;
- mtDNA failure diagnostics;
- centralized per-sample status logging;
- batch-level pipeline monitoring;
- conservative cleanup of large WGS BAM intermediates.

For authoritative information about CBIcall installation, workflows, configuration, and supported backends, refer to the upstream project:

https://github.com/CNAG-Biomedical-Informatics/cbicall

---

## Citation

Citation information for this repository is provided in [`CITATION.md`](CITATION.md).

If this wrapper is used together with CBIcall, please also cite the CBIcall publication:

> Rueda M, Fernandez-Orth D, Gut IG. **CBIcall: a configuration-driven framework for variant calling in large sequencing cohorts.** *Bioinformatics Advances*. 2026. https://doi.org/10.1093/bioadv/vbag232

---

## Author

**Dietmar Fernández**  
GitHub: [@dietmarfdz](https://github.com/dietmarfdz)

Repository:

https://github.com/dietmarfdz/cbicall-slurm-wgs-mtdna-orchestrator

---

## Acknowledgements

This repository builds on **CBIcall**, developed by CNAG Biomedical Informatics.

Development of this repository was assisted by **ChatGPT (OpenAI)** for code review, workflow design, debugging, and documentation. The workflow logic, adaptation to the target HPC environment, testing, and validation were performed by the repository author.

---

## License

See [`LICENSE`](LICENSE).

This wrapper is an independent utility. CBIcall remains a separate project maintained by its own authors and is subject to its own license and usage terms.

---

## Disclaimer

This repository is provided as a research/HPC automation utility.

Cluster configuration, CBIcall versions, output naming conventions, and computational policies differ between environments. Always validate the workflow with a small number of known samples before running a large cohort.

In particular, verify cleanup behaviour before enabling large-scale execution.
