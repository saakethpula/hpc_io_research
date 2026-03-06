## IOR + mdtest Darshan label validation

This repo contains a small set of scripts and CSVs for validating a curated set of IOR workloads (25 cluster exemplar labels) plus complementary mdtest metadata workloads against Darshan traces, and for exporting those traces into model‑friendly CSVs.

The scripts are designed for an environment where:
- **Darshan** is installed and available as a shared library (`libdarshan.so`)
- **Darshan logs** are written into a directory tree you can read (e.g. `/darshan-logs`)
- **mpirun**, **ior**, and **mdtest** are available on `PATH`

The defaults in the scripts assume the environment used during development:
- `DARSHAN_LIB = "/opt/darshan-install/lib/libdarshan.so"`
- `LOG_BASE    = "/darshan-logs"`
- `CSV_PATH    = "/root/cluster_top25_exemplars_with_darshan.csv"`

You can override these either by editing the constants at the top of the scripts or by running inside a container that matches the original paths.

---

### Contents

- `data/cluster_top25_exemplars_with_darshan.csv` — 25 “exemplar” rows extracted from production Darshan logs; this defines the schema and target counters for each label.
- `validate_all_labels.py` — runs IOR workloads for all 25 labels, compares Darshan counters to the exemplar rows, prints a summary, and writes `data/ior_generated_traces.csv` (by default).
- `validate_all_labels_with_mdtest.py` — runs a set of mdtest metadata workloads plus the same 25 IOR labels, writing all traces into `data/mdtest_generated_traces.csv` (IOR + mdtest rows in a shared schema).
- `parse_darshan_results.py` — helper to parse arbitrary Darshan logs with PyDarshan and align them to the exemplar schema (used separately from the validation scripts).
- `merge_mdtest_into_ior.py` — merges selected mdtest counters into the IOR rows to produce a single CSV with both data‑path and metadata‑path information (`ior_plus_mdtest_25rows.csv`).
- `data/ior_generated_traces.csv` — Darshan counters for the 25 IOR labels only (output of `validate_all_labels.py`).
- `data/mdtest_generated_traces.csv` — combined IOR + mdtest trace rows (output of `validate_mdtest_labels.py`).
- `data/ior_only_25rows.csv` — 25‑row “clean” IOR dataset aligned to the exemplar schema (no mdtest augmentation).
- `data/ior_plus_mdtest_25rows.csv` — 25‑row dataset where selected IOR labels have mdtest metadata counters merged into them.

---

### Installation

From a clean Python environment:

```bash
pip install -r requirements.txt
```

You must also have:
- Darshan installed (PyDarshan Python package and `libdarshan.so`)
- `mpirun`, `ior`, and `mdtest` on `PATH`

---

### Example: validate all 25 IOR labels

This runs all 25 labels from the exemplar CSV, prints a per‑metric ratio table for each label, and writes a derived CSV of the generated traces under `data/`:

```bash
python3 validate_all_labels.py
```

To run only a subset of labels:

```bash
python3 validate_all_labels.py --labels 74,28,252
```

For a quick “what would you run?” pass that does not actually execute IOR, use:

```bash
python3 validate_all_labels.py --dry-run
```

---

### Example: IOR + mdtest joint validation

`validate_mdtest_labels.py` runs the same 25 IOR labels plus a collection of `meta_*` mdtest workloads that exercise metadata‑heavy patterns (opens, stats, fsyncs, etc.). Both kinds of labels are written into a shared output CSV.

Run all labels (IOR + mdtest) to populate `data/mdtest_generated_traces.csv`:

```bash
python3 validate_all_labels_with_mdtest.py
```

Run a selected subset (mixing numeric IOR labels and `meta_` labels):

```bash
python3 validate_all_labels_with_mdtest.py \
  --labels 74,28,meta_flat_1proc_creates,meta_unique_4proc_large_files
```

As with the IOR‑only script, you can use `--dry-run` to see the exact `mpirun` commands without launching them:

```bash
python3 validate_all_labels_with_mdtest.py --dry-run
```

On a non‑Darshan system this is a useful way to review or tweak the command lines without needing the full instrumentation stack.

---

### Example: merge mdtest counters into IOR rows

Once you have generated `data/mdtest_generated_traces.csv` with the joint validator, you can create a compact 25‑row CSV that augments each IOR label with a selected mdtest metadata pattern:

```bash
python3 merge_mdtest_into_ior.py
```

This:
- Reads `data/mdtest_generated_traces.csv`
- Uses `META_MAP` in `merge_mdtest_into_ior.py` to choose which `meta_*` label (if any) should be merged into each numeric IOR label
- Copies a small set of metadata counters (`POSIX_OPENS`, `POSIX_STATS`, `POSIX_FSYNCS`, `POSIX_BYTES_*`, `POSIX_*READS`/`WRITES`) from the mdtest row into the IOR row
- Writes the result to `data/ior_plus_mdtest_25rows.csv`

If you just want the original IOR traces, you can instead use `data/ior_only_25rows.csv` (no mdtest augmentation).

---

### Example: parsing arbitrary Darshan logs

`parse_darshan_results.py` lets you walk a directory tree of `.darshan` files, expand them with PyDarshan, and write a summarized CSV. You can optionally re‑order the columns to match the exemplar schema so that rows can be compared or joined easily.

Basic usage:

```bash
python3 parse_darshan_results.py /path/to/results \
  --schema-reference cluster_top25_exemplars_with_darshan.csv
```

This writes `darshan_posix_summary.csv` into the results directory and, when `--schema-reference` is given, reorders columns to match the exemplar CSV.

---

### Reproducing the labeled datasets

Given a Darshan‑enabled environment with IOR and mdtest:

1. **Generate IOR traces only**
   - `python3 validate_all_labels.py --csv cluster_top25_exemplars_with_darshan.csv --output-csv ior_generated_traces.csv`

2. **Generate IOR + mdtest traces**
   - `python3 validate_mdtest_labels.py --csv cluster_top25_exemplars_with_darshan.csv --output-csv mdtest_generated_traces.csv`

3. **Produce 25‑row training tables**
   - IOR only: copy or subset `ior_generated_traces.csv` into `ior_only_25rows.csv` (already included here)
   - IOR + mdtest: `python3 merge_mdtest_into_ior.py` → `ior_plus_mdtest_25rows.csv`

These runs and outputs are what the CSVs in this repo were derived from; they give you a reproducible path from raw Darshan traces to the model‑ready label tables.

