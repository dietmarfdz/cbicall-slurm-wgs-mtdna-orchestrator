# CBIcall Slurm WGS + mtDNA Orchestrator

A lightweight Slurm orchestration layer for running **CBIcall WGS and mitochondrial DNA (mtDNA) analyses in sequence**, with explicit validation checkpoints, dependency-aware execution, status logging, and safe cleanup of large intermediate WGS BAM files.

This repository does **not** replace or modify CBIcall. It is an external Bash/Slurm wrapper designed to automate repeated CBIcall executions on HPC systems.

> **CBIcall** is a configuration-driven framework for reproducible variant calling in large sequencing cohorts. It supports WES, WGS and mitochondrial DNA workflows and can execute validated workflows through several backends, including Bash, Cromwell, Nextflow and Snakemake.

- CBIcall repository: https://github.com/CNAG-Biomedical-Informatics/cbicall
- CBIcall publication: https://doi.org/10.1093/bioadv/vbag232

---

## Why this wrapper exists

Running WGS and mtDNA analysis over many samples on an HPC cluster involves more than submitting the same command repeatedly.

For each sample, this wrapper:

1. creates the CBIcall YAML configuration for WGS;
2. submits the WGS job through Slurm;
3. verifies that the expected WGS outputs were actually produced;
4. verifies that the mtDNA BAM exported from the WGS workflow exists and is indexed;
5. launches the CBIcall mitochondrial pipeline only after the WGS validation succeeds;
6. validates the mitochondrial output;
7. removes large WGS BAM intermediates only after the mitochondrial analysis has been successfully validated;
8. records job IDs, execution times and validation status in a per-sample `pipeline_status.log`.

The main objective is therefore **safe batch automation**, rather than simply job submission.

---

## Workflow

```text
                         ┌─────────────────────┐
                         │    Input FASTQs     │
                         └──────────┬──────────┘
                                    │
                                    v
                         ┌─────────────────────┐
                         │     CBIcall WGS     │
                         │   GATK 4.6 / hg38   │
                         └──────────┬──────────┘
                                    │
                                    v
                         ┌─────────────────────┐
                         │      CHECK_WGS      │
                         ├─────────────────────┤
                         │ WGS directory       │
                         │ BAM                 │
                         │ QC VCF              │
                         │ exported mtDNA BAM  │
                         │ mtDNA BAM index     │
                         │ WGS completion log  │
                         └──────────┬──────────┘
                                    │
                              validation OK
                                    │
                                    v
                         ┌─────────────────────┐
                         │     CBIcall MIT     │
                         │ MToolBox / mtDNA    │
                         └──────────┬──────────┘
                                    │
                                    v
                         ┌─────────────────────┐
                         │      CHECK_MIT      │
                         ├─────────────────────┤
                         │ MIT directory       │
                         │ VCF_file.vcf        │
                         │ MIT completion log  │
                         └──────────┬──────────┘
                                    │
                              validation OK
                                    │
                                    v
                         ┌─────────────────────┐
                         │       CLEANUP       │
                         ├─────────────────────┤
                         │ re-check mtDNA BAM  │
                         │ re-check BAM index  │
                         │ delete WGS BAMs     │
                         │ preserve mtDNA BAM  │
                         └─────────────────────┘
```

Slurm dependencies are used so that a downstream step is submitted only with an `afterok` dependency on the preceding step.

In simplified form:

```text
WGS
 └── afterok → CHECK_WGS
                 └── afterok → MIT
                                └── afterok → CHECK_MIT
                                               └── afterok → CLEANUP
```

If a job or validation step fails, the downstream chain does not proceed.

---

## Repository structure

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
└── examples/
    └── samples.example.txt
```

### Main files

**`cbicall_wgs_mtdna_slurm.sh`**  
Main per-sample orchestrator. It creates the CBIcall YAML files, generates the Slurm jobs and check scripts, and submits the full dependency chain.

**`config/config.example.env`**  
Example cluster configuration. Copy this file to `config/config.env` and adapt it to the local HPC environment.

**`run_batch.sh`**  
Convenience launcher for processing multiple sample IDs from a text file.

**`examples/samples.example.txt`**  
Example input list containing one sample ID per line.

---

## Requirements

### Software

The wrapper assumes that the following are available:

- Bash
- Slurm (`sbatch`, `sacct`)
- CBIcall
- a CBIcall-compatible Python environment
- the software/resources required by the selected CBIcall workflows

CBIcall itself must be installed and configured independently.

See the official documentation:

https://github.com/CNAG-Biomedical-Informatics/cbicall

### HPC environment

The script was designed for a Slurm-based HPC system and assumes that:

- compute partitions are available for WGS and mtDNA jobs;
- CBIcall can be executed from compute nodes;
- the sample directories are visible from the nodes;
- the relevant CBIcall reference/resource bundles are already installed.

Partition names, memory, walltime, excluded nodes, Python modules and CBIcall paths are **site-specific** and must be configured locally.

---

## Configuration

Create a local configuration file:

```bash
cp config/config.example.env config/config.env
```

Then edit:

```bash
config/config.env
```

Typical parameters include:

```bash
CBICALL="/path/to/cbicall/bin/cbicall"
CBICALL_PYTHON_PREFIX="/path/to/cbicall/python/environment"

PYTHON_MODULE="Python/<version>"
CBICALL_RUNTIME_PROFILE="<runtime-profile>"

WGS_PARTITION="<wgs-partition>"
MIT_PARTITION="<mit-partition>"

WGS_TIME="20-00:00:00"
MIT_TIME="10:00:00"

MEM="24G"

EXCLUDE_NODES=""
```

Do **not** commit a private `config.env` containing institutional paths or infrastructure details.

The repository `.gitignore` is intended to exclude this file.

---

## Expected input layout

Each sample is expected to have its own directory below a common working directory.

For example:

```text
WORKDIR_BASE/
├── SAMPLE001/
│   ├── SAMPLE001_L001_R1_001.fastq.gz
│   └── SAMPLE001_L001_R2_001.fastq.gz
├── SAMPLE002/
│   ├── SAMPLE002_L001_R1_001.fastq.gz
│   └── SAMPLE002_L001_R2_001.fastq.gz
└── SAMPLE003/
    ├── SAMPLE003_L001_R1_001.fastq.gz
    └── SAMPLE003_L001_R2_001.fastq.gz
```

The exact FASTQ naming requirements are ultimately determined by CBIcall.

The wrapper does not recursively search arbitrary subdirectories for input FASTQs.

---

## WGS configuration generated by the wrapper

The orchestrator creates a per-sample WGS YAML equivalent to:

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

Two options are particularly important for the orchestration logic:

```yaml
cleanup_bam: false
export_mtdna_bam: true
```

`cleanup_bam: false` prevents the WGS pipeline from deleting BAM files before the downstream checks and mtDNA analysis are complete.

`export_mtdna_bam: true` instructs CBIcall to export the mitochondrial input BAM that will subsequently be consumed by the mitochondrial workflow.

---

## mtDNA configuration generated by the wrapper

The mitochondrial YAML is equivalent to:

```yaml
mode: single
pipeline: mit
workflow_backend: bash
input_dir: /path/to/sample
```

The mtDNA job is submitted only after `CHECK_WGS` finishes successfully.

---

## WGS validation

After CBIcall WGS completes, `CHECK_WGS` locates the most recent WGS output directory matching the expected CBIcall naming convention.

It verifies:

```text
WGS output directory
01_bam/*.bam
02_varcall/*.hc.QC.vcf.gz
exports/mtdna/*_MIT.bam
exports/mtdna/*_MIT.bam.bai
WGS completion message in the CBIcall log
```

The mtDNA export check is important because a WGS workflow can otherwise complete upstream processing without providing a usable mitochondrial input for the next stage.

If all checks succeed:

```text
WGS_MTDNA_BAM: /path/to/.../exports/mtdna/SAMPLE-DNA_MIT.bam
WGS_OK
```

is added to `pipeline_status.log`.

If any required output is missing, the check exits with a non-zero status and the MIT job does not run.

---

## Mitochondrial validation

After the mitochondrial CBIcall workflow completes, `CHECK_MIT` locates its output directory and verifies:

```text
cbicall_bash_gatk-3.5_mit_single_rsrs_<run-id>/
└── 01_mtoolbox/
    └── VCF_file.vcf
```

It also checks the CBIcall mitochondrial completion log.

Successful validation records:

```text
MIT_DIR: /path/to/cbicall_bash_gatk-3.5_mit_single_rsrs_<run-id>
MIT_VCF: /path/to/cbicall_bash_gatk-3.5_mit_single_rsrs_<run-id>/01_mtoolbox/VCF_file.vcf
MIT_OK
```

---

## Safe cleanup strategy

WGS BAMs can occupy substantial disk space.

The wrapper therefore performs cleanup only after:

```text
WGS completed
      +
CHECK_WGS passed
      +
MIT completed
      +
CHECK_MIT passed
```

Immediately before deleting anything, the cleanup script **again checks** that the exported mitochondrial BAM and its index exist.

Only then are the BAM/BAM-index files under the WGS `01_bam/` directory removed.

Conceptually:

```bash
rm -f "$BAM_DIR"/*.bam
rm -f "$BAM_DIR"/*.bai
```

The mitochondrial export is **not** removed:

```text
WGS_OUTPUT/
└── exports/
    └── mtdna/
        ├── SAMPLE-DNA_MIT.bam
        └── SAMPLE-DNA_MIT.bam.bai
```

Successful cleanup is recorded as:

```text
CLEANUP_MTDNA_PRESERVED: /path/to/.../SAMPLE-DNA_MIT.bam
CLEANUP_OK
```

This conservative design is intentional: a validation failure should consume extra storage rather than risk deleting a BAM that may still be needed.

---

## Running one sample

General syntax:

```bash
./cbicall_wgs_mtdna_slurm.sh <SAMPLE_ID> <WORKDIR_BASE> [THREADS]
```

For example:

```bash
./cbicall_wgs_mtdna_slurm.sh SAMPLE001 /path/to/WGS 4
```

If the number of threads is omitted, the configured/default value is used.

The script immediately submits the complete dependency chain to Slurm.

---

## Running multiple samples

Create a text file containing one sample ID per line:

```text
SAMPLE001
SAMPLE002
SAMPLE003
```

Then use the batch launcher:

```bash
./run_batch.sh samples.txt /path/to/WGS 4
```

Alternatively:

```bash
while IFS= read -r SAMPLE; do
    [[ -z "$SAMPLE" ]] && continue

    ./cbicall_wgs_mtdna_slurm.sh \
        "$SAMPLE" \
        /path/to/WGS \
        4

done < samples.txt
```

Each sample gets an independent Slurm dependency chain.

Submitting many samples does not imply that all WGS jobs execute simultaneously; Slurm controls actual scheduling according to cluster resources, priorities and configured limits.

---

## Monitoring

### Slurm queue

```bash
squeue -u "$USER"
```

### Accounting information

```bash
sacct -j <JOB_ID> \
    --format=JobID,JobName,State,ExitCode,Elapsed,TotalCPU,AllocCPUS,MaxRSS,NodeList
```

### Pipeline status

Each sample contains:

```text
pipeline_status.log
```

A successful run will contain entries similar to:

```text
START SAMPLE: SAMPLE001
THREADS: 4

WGS_JOB: 123456
CHECK_WGS_JOB: 123457
MIT_JOB: 123458
CHECK_MIT_JOB: 123459
CLEANUP_JOB: 123460

PIPELINE_LAUNCHED_OK

WGS_REAL_START: ...
WGS_REAL_END: ...
WGS_DURATION_SECONDS: ...

WGS_MTDNA_BAM: /path/to/.../SAMPLE001-DNA_MIT.bam
WGS_OK

MIT_REAL_START: ...
MIT_REAL_END: ...
MIT_DURATION_SECONDS: ...

MIT_DIR: /path/to/.../cbicall_bash_gatk-3.5_mit_single_rsrs_<run-id>
MIT_VCF: /path/to/.../01_mtoolbox/VCF_file.vcf
MIT_OK

CLEANUP_MTDNA_PRESERVED: /path/to/.../SAMPLE001-DNA_MIT.bam
CLEANUP_OK
```

The `WGS_SACCT` or `MIT_SACCT` fields may occasionally be empty if Slurm accounting information is not yet available when queried. Output validation is therefore deliberately based on the expected files and CBIcall completion logs rather than on `sacct` alone.

---

## Failure behaviour

The dependency chain is intentionally strict.

### WGS job fails

```text
WGS → FAILED
CHECK_WGS → DependencyNeverSatisfied
MIT → pending dependency
CHECK_MIT → pending dependency
CLEANUP → pending dependency
```

No cleanup occurs.

### WGS finishes but validation fails

For example:

```text
WGS_FAIL: missing exported mtDNA BAM
```

MIT is not executed.

### MIT finishes but validation fails

For example:

```text
MIT_FAIL: output directory not found
```

Cleanup is not executed and the WGS BAM files are preserved.

This is intentional.

---

## Troubleshooting

### `DependencyNeverSatisfied`

This normally means that a preceding job in the Slurm dependency chain did not exit successfully.

Inspect:

```bash
sacct -j <JOB_ID> --format=JobID,State,ExitCode,Elapsed,NodeList
```

and the corresponding Slurm `.out` and `.err` files.

### WGS reports no mitochondrial alignments

If CBIcall reports something similar to:

```text
Exported mtDNA BAM contains no alignments for contig 'chrM'
```

first verify the WGS BAM:

```bash
samtools idxstats sample.bam | awk '$1=="chrM"'
samtools view -c sample.bam chrM
```

If testing with subsampled FASTQs, also verify that the FASTQ records were not corrupted during subsampling.

A valid FASTQ record must remain:

```text
@read-header
SEQUENCE
+
QUALITY
```

When flattening FASTQ records with `paste`, remember that FASTQ headers may contain spaces. If AWK is used to reconstruct records, a tab-only field separator such as:

```bash
awk -F '\t'
```

may be necessary to avoid splitting headers into multiple fields.

### MIT completed but `CHECK_MIT` reports no directory

Check whether the CBIcall output naming convention has changed.

The current wrapper expects a directory matching:

```text
cbicall_bash_gatk-3.5_mit_single_rsrs_*
```

and a log named:

```text
bash_gatk-3.5_mit_single_rsrs.log
```

CBIcall naming conventions may change between releases, so these patterns should be reviewed after upgrading CBIcall.

---

## Important assumptions and version sensitivity

This wrapper depends on several CBIcall output conventions, including directory names, log names and expected output filenames.

It was developed and tested against the CBIcall workflow conventions available at the time of development.

Before using it with a newer CBIcall version, review at least:

```text
WGS output directory pattern
MIT output directory pattern
WGS log filename
MIT log filename
QC VCF filename
mtDNA exported BAM location
MIT VCF location
completion messages used by the checks
```

The wrapper should therefore be considered an **orchestration template that must be validated against the local CBIcall installation and HPC environment**.

---

## Data and security considerations

This repository is intended to contain **code only**.

Do not commit:

- FASTQ files;
- BAM/CRAM files;
- VCF files containing individual-level genomic data;
- sample metadata containing protected information;
- Slurm logs containing sensitive paths or identifiers;
- institutional credentials;
- private HPC paths;
- private environment configuration.

Review the repository before every public push:

```bash
git status
git diff --cached
```

For an additional check:

```bash
git grep -n "/private/path"
git grep -n "scratch"
```

Adapt these searches to the local infrastructure.

---

## Relationship to CBIcall

This repository is an independent orchestration wrapper built around CBIcall.

It is **not an official CBIcall component** and is not intended to duplicate the workflow implementation provided by CBIcall itself.

CBIcall performs the genomic analyses. This repository adds:

- Slurm dependency orchestration;
- per-stage output checks;
- WGS-to-mtDNA chaining;
- centralized per-sample status logging;
- defensive cleanup of large intermediate WGS BAMs;
- convenience support for batch execution.

For CBIcall installation, workflow implementation, supported configurations and authoritative documentation, refer to the upstream project:

https://github.com/CNAG-Biomedical-Informatics/cbicall

---

## Citation

If this wrapper is used together with CBIcall, please cite the CBIcall publication:

> Rueda M, Fernandez-Orth D, et al. **CBIcall: a configuration-driven framework for variant calling in large sequencing cohorts.** *Bioinformatics Advances*. 2026; vbag232. https://doi.org/10.1093/bioadv/vbag232

See also [`CITATION.md`](CITATION.md).

---

## Author

**Dietmar Fernández**  
GitHub: [@dietmarfdz](https://github.com/dietmarfdz)

Repository:

https://github.com/dietmarfdz/cbicall-slurm-wgs-mtdna-orchestrator

This wrapper was developed to support reproducible, failure-aware execution of repeated WGS and mitochondrial CBIcall analyses on Slurm-based HPC infrastructure.

---

## Acknowledgements

This repository builds on **CBIcall**, developed by CNAG Biomedical Informatics.

Development of this repository was assisted by **ChatGPT (OpenAI)** for code review, workflow design, debugging and documentation. The workflow logic, adaptation to the target HPC environment, testing and validation were performed by the repository author.

---

## License

See [`LICENSE`](LICENSE).

Because this wrapper invokes and is designed around CBIcall, users should also review the license and usage terms of the upstream CBIcall project.

---

## Disclaimer

This repository is provided as a research/HPC automation utility.

Cluster configuration, CBIcall versions, output naming conventions and computational policies differ between environments. Always validate the workflow with a small number of known samples before running a large cohort.

In particular, verify the cleanup behaviour before enabling large-scale execution.
