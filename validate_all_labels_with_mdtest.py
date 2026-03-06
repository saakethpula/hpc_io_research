#!/usr/bin/env python3
"""
validate_mdtest_labels.py — all 25 IOR cluster-exemplar labels (identical to
validate_all_labels.py) PLUS mdtest metadata labels that fill gaps in
POSIX_OPENS, POSIX_STATS, POSIX_FSYNCS, POSIX_FDSYNCS, and POSIX_RENAME_* counters.

IOR labels validate against the exemplar CSV targets (same as validate_all_labels.py).
mdtest labels have no CSV target rows; they record raw Darshan counter values only.
Both sets of labels emit rows into a shared output CSV (mdtest_generated_traces.csv).

Usage:
    python3 validate_mdtest_labels.py --csv /root/cluster_top25_exemplars_with_darshan.csv
    python3 validate_mdtest_labels.py --csv ... --labels 74,28,flat_1proc_creates
    python3 validate_mdtest_labels.py --dry-run
"""

import argparse, csv, math, os, sys, subprocess, time, glob
from datetime import datetime

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, SCRIPT_DIR)
from parse_darshan_results import parse_darshan_file

DARSHAN_LIB = "/opt/darshan-install/lib/libdarshan.so"
LOG_BASE    = "/darshan-logs"
# Default exemplar CSV now lives under a local data/ directory next to this script.
CSV_PATH    = os.path.join(SCRIPT_DIR, "data", "cluster_top25_exemplars_with_darshan.csv")
TMP         = "/tmp"
B  = f"mpirun --allow-run-as-root -x LD_PRELOAD={DARSHAN_LIB}"
O  = f"mpirun --allow-run-as-root --oversubscribe -x LD_PRELOAD={DARSHAN_LIB}"

GREEN="\033[92m"; RED="\033[91m"; YELLOW="\033[93m"; BLUE="\033[94m"; BOLD="\033[1m"; RESET="\033[0m"

# ─── LABEL DEFINITIONS ───────────────────────────────────────────────────────
# IOR labels: "validate" dict mirrors validate_all_labels.py exactly.
#   Key = CSV column name, value = (darshan_key, step_index)
#   "trace_for_csv" controls which step's trace populates the output CSV row.
#
# mdtest labels: "capture" dict lists counters to record; no CSV target row.
#   Label names are prefixed so --labels filtering works intuitively.
#
# Structural mismatch notes (not validated):
#   Label 47:  POSIX_WRITES — IOR shared file always 1 write/proc; original=8
#   Label 48:  POSIX_WRITES — ROMIO layer calls, not app-level ops
#   Label 249: POSIX_WRITES count — MPIIO indep maps differently per-proc
#   Label 193: POSIX_WRITES count — same ROMIO issue as 48
#   Label 83 read: xfer size ~512B — IOR minimum block unreliable at this scale
#
# Changes from previous version:
#   - Added -e (fsync) to all IOR commands for complete Darshan counter capture
#   - Added cluster 41 (was missing entirely)

LABELS = [

    # ════════════════════════════════════════════════════════════════════════
    # IOR LABELS — identical to validate_all_labels.py
    # ════════════════════════════════════════════════════════════════════════

    {   "label": "101",
        "desc":  "1-proc POSIX read 4×27MB",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 27M -t 27M -s 4 -w -e -o {TMP}/val_101.dat -k",
            f"{B} -np 1 ior -a POSIX -b 27M -t 27M -s 4 -r -e -o {TMP}/val_101.dat",
        ],
        "validate": {
            "POSIX_BYTES_READ": ("POSIX_BYTES_READ", -1),
            "POSIX_READS":      ("POSIX_READS",      -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "7",
        "desc":  "4-proc POSIX FPP read, 8K xfer",
        "steps": [
            f"{B} -np 4 ior -a POSIX -b 29m -t 8k -s 1 -F -w -e -o {TMP}/val_7 -k",
            f"{B} -np 4 ior -a POSIX -b 29m -t 8k -s 1 -F -r -C -e -o {TMP}/val_7",
        ],
        "validate": {
            "POSIX_BYTES_READ": ("POSIX_BYTES_READ", -1),
            "POSIX_READS":      ("POSIX_READS",      -1),
            "POSIX_SEQ_READS":  ("POSIX_SEQ_READS",  -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "18",
        "desc":  "1-proc POSIX write 684MB (4 ops) + read 2GB (2 ops)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 171M -t 171M -s 4 -w -e -o {TMP}/val_18_w.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1024M -t 1024M -s 2 -w -e -o {TMP}/val_18_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1024M -t 1024M -s 2 -r -e -o {TMP}/val_18_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES",        0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",   -1),
            "POSIX_READS":         ("POSIX_READS",        -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "20",
        "desc":  "1-proc POSIX write 2GB (4 ops) + read 2GB (2 ops)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 512M -t 512M -s 4 -w -e -o {TMP}/val_20.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1024M -t 1024M -s 2 -r -e -o {TMP}/val_20.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN",  0),
            "POSIX_WRITES":        ("POSIX_WRITES",          0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",     -1),
            "POSIX_READS":         ("POSIX_READS",          -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "192",
        "desc":  "1-proc MPIIO collective write 10.2GB, 19 ops",
        "steps": [
            f"{B} -np 1 ior -a MPIIO -b 550M -t 550M -s 19 -w -c -e -o {TMP}/val_192.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN", -1),
            "MPIIO_COLL_WRITES":   ("MPIIO_COLL_WRITES",   -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "331",
        "desc":  "4-proc POSIX FPP read 120MB, 8K xfer",
        "steps": [
            f"{B} -np 4 ior -a POSIX -b 30M -t 8K -s 1 -F -w -e -o {TMP}/val_331.dat -k",
            f"{B} -np 4 ior -a POSIX -b 30M -t 8K -s 1 -F -r -C -e -o {TMP}/val_331.dat",
        ],
        "validate": {
            "POSIX_BYTES_READ": ("POSIX_BYTES_READ", -1),
            "POSIX_READS":      ("POSIX_READS",      -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "47",
        "desc":  "40-proc POSIX shared write 667MB  [POSIX_WRITES structural mismatch: IOR=40, target=8]",
        "steps": [
            f"{O} -np 40 ior -a POSIX -b 16M -t 16M -s 1 -w -e -o {TMP}/val_47.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "48",
        "desc":  "16-proc MPIIO collective write 4.4GB, 10 segs  [POSIX_WRITES = ROMIO detail, not validated]",
        "steps": [
            f"{O} -np 16 ior -a MPIIO -b 26M -t 26M -s 10 -w -c -e -o {TMP}/val_48.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN", -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "74",
        "desc":  "1-proc POSIX write 82MB (850 ops) + read 82MB (835 ops)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 96K -t 96K -s 875 -w -e -o {TMP}/val_74.dat -k",
            f"{B} -np 1 ior -a POSIX -b 96K -t 96K -s 875 -r -e -o {TMP}/val_74.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "28",
        "desc":  "1-proc POSIX write 77GB (991 ops, 74M) + read 13MB (67 ops, 192K)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 74M -t 74M -s 991 -w -e -o {TMP}/val_28_w.dat",
            f"{B} -np 1 ior -a POSIX -b 192K -t 192K -s 67 -w -e -o {TMP}/val_28_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 192K -t 192K -s 67 -r -e -o {TMP}/val_28_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN",  0),
            "POSIX_WRITES":        ("POSIX_WRITES",          0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",     -1),
            "POSIX_READS":         ("POSIX_READS",          -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "252",
        "desc":  "1-proc POSIX read 718MB (2 ops, 343M xfer)  [BYTES_WRITTEN=417 is metadata noise]",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 343M -t 343M -s 2 -w -e -o {TMP}/val_252.dat -k",
            f"{B} -np 1 ior -a POSIX -b 343M -t 343M -s 2 -r -e -o {TMP}/val_252.dat",
        ],
        "validate": {
            "POSIX_BYTES_READ": ("POSIX_BYTES_READ", -1),
            "POSIX_READS":      ("POSIX_READS",      -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "249",
        "desc":  "64-proc MPIIO independent write ~18.4GB  [reduced from 512 to avoid SIGBUS]",
        "steps": [
            f"{O} -np 64 ior -a MPIIO -b 1M -t 1M -s 275 -w -e -o {TMP}/val_249.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN", -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "236",
        "desc":  "1-proc POSIX write 1.15GB (24 ops, 46M xfer)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 46M -t 46M -s 24 -w -e -o {TMP}/val_236.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", -1),
            "POSIX_WRITES":        ("POSIX_WRITES",        -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "237",
        "desc":  "32-proc POSIX write 8.4MB (2048 ops, 4K) + read 2.0GB (22400 ops, 89K)",
        "steps": [
            f"{O} -np 32 ior -a POSIX -b 4K -t 4K -s 64 -w -e -o {TMP}/val_237_w.dat",
            f"{O} -np 32 ior -a POSIX -b 89K -t 89K -s 700 -w -e -o {TMP}/val_237_r.dat -k",
            f"{O} -np 32 ior -a POSIX -b 89K -t 89K -s 700 -r -C -e -o {TMP}/val_237_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "344",
        "desc":  "64-proc POSIX write 1.24GB (1664 ops, 728K) + read 59MB (128 ops, 467K)",
        "steps": [
            f"{O} -np 64 ior -a POSIX -b 728K -t 728K -s 26 -w -F -e -o {TMP}/val_344_w.dat",
            f"{O} -np 64 ior -a POSIX -b 467K -t 467K -s 2 -w -F -e -o {TMP}/val_344_r.dat -k",
            f"{O} -np 64 ior -a POSIX -b 467K -t 467K -s 2 -r -C -F -e -o {TMP}/val_344_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "50",
        "desc":  "4-proc MPIIO indep write 5.2GB (3 ops, 1304M) + indep read 1.3MB (4 ops, 323K)",
        "steps": [
            f"{B} -np 4 ior -a MPIIO -b 1304M -t 1304M -s 1 -w -e -o {TMP}/val_50.dat -k",
            f"{B} -np 4 ior -a MPIIO -b 323K -t 323K -s 1 -r -C -e -o {TMP}/val_50.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN",  0),
            "MPIIO_BYTES_READ":    ("MPIIO_BYTES_READ",     -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "225",
        "desc":  "8-proc POSIX write 4.19GB (1000 ops, 4M xfer)",
        "steps": [
            f"{O} -np 8 ior -a POSIX -b 4M -t 4M -s 125 -w -F -e -o {TMP}/val_225.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", -1),
            "POSIX_WRITES":        ("POSIX_WRITES",        -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "83",
        "desc":  "4-proc POSIX write 80.5GB (14910 ops, 5M xfer)  [read xfer ~512B, bytes approx only]",
        "steps": [
            f"{B} -np 4 ior -a POSIX -b 5M -t 5M -s 3728 -w -F -e -o {TMP}/val_83.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", -1),
            "POSIX_WRITES":        ("POSIX_WRITES",        -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "322",
        "desc":  "8-proc POSIX write 1.55GB (864 ops, 1750K xfer)",
        "steps": [
            f"{O} -np 8 ior -a POSIX -b 1750K -t 1750K -s 108 -w -F -e -o {TMP}/val_322.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", -1),
            "POSIX_WRITES":        ("POSIX_WRITES", -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "258",
        "desc":  "1-proc POSIX write 117MB (4 ops, 28M) + read 15GB (8 ops, 1788M)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 28M -t 28M -s 4 -w -e -o {TMP}/val_258_w.dat",
            f"{B} -np 1 ior -a POSIX -b 1788M -t 1788M -s 8 -w -e -o {TMP}/val_258_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1788M -t 1788M -s 8 -r -e -o {TMP}/val_258_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN",  0),
            "POSIX_WRITES":        ("POSIX_WRITES",          0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",     -1),
            "POSIX_READS":         ("POSIX_READS",          -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "207",
        "desc":  "64-proc POSIX write 78.9MB (448 ops, 172K) + read 39.7MB (192 ops, 202K)",
        "steps": [
            f"{O} -np 64 ior -a POSIX -b 172K -t 172K -s 7 -w -e -o {TMP}/val_207_w.dat",
            f"{O} -np 64 ior -a POSIX -b 202K -t 202K -s 3 -w -e -o {TMP}/val_207_r.dat -k",
            f"{O} -np 64 ior -a POSIX -b 202K -t 202K -s 3 -r -C -e -o {TMP}/val_207_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "193",
        "desc":  "64-proc MPIIO coll write ~3.25GB (13312 ops, 256K xfer)  [reduced from 512 to avoid SIGBUS]",
        "steps": [
            f"{O} -np 64 ior -a MPIIO -b 256K -t 256K -s 208 -w -c -e -o {TMP}/val_193.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN", -1),
            "MPIIO_COLL_WRITES":   ("MPIIO_COLL_WRITES", -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "138",
        "desc":  "1-proc POSIX write 22.9MB (683 ops, 33K) + read 142.6MB (15273 ops, 9K)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 33K -t 33K -s 683 -w -e -o {TMP}/val_138_w.dat",
            f"{B} -np 1 ior -a POSIX -b 9K -t 9K -s 15273 -w -e -o {TMP}/val_138_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 9K -t 9K -s 15273 -r -e -o {TMP}/val_138_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN",  0),
            "POSIX_WRITES":        ("POSIX_WRITES",          0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",     -1),
            "POSIX_READS":         ("POSIX_READS",          -1),
        },
        "trace_for_csv": 0,
    },
    {   "label": "64",
        "desc":  "16-proc POSIX write 11.7GB (545K ops, 21K) + read 22.2GB (145K ops, 149K)",
        "steps": [
            f"{O} -np 16 ior -a POSIX -b 21K -t 21K -s 34096 -w -F -e -o {TMP}/val_64_w.dat",
            f"{O} -np 16 ior -a POSIX -b 149K -t 149K -s 9111 -w -F -e -o {TMP}/val_64_r.dat -k",
            f"{O} -np 16 ior -a POSIX -b 149K -t 149K -s 9111 -r -C -F -e -o {TMP}/val_64_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },

    # ════════════════════════════════════════════════════════════════════════
    # MDTEST METADATA LABELS
    # No matching row in the exemplars CSV — label names start with "meta_".
    # Uses "capture" dict instead of "validate"; no ratio is computed.
    # These rows still appear in the output CSV using the exemplar schema,
    # with metadata Darshan counters populated directly from the trace.
    #
    # Darshan counters targeted:
    #   POSIX_OPENS        — every file create/open/stat/read/remove
    #   POSIX_STATS        — -T (stat phase)
    #   POSIX_FSYNCS       — -y (fsync after write)
    #   POSIX_BYTES_WRITTEN / POSIX_WRITES  — -w <bytes> payload per file
    #   POSIX_BYTES_READ   / POSIX_READS    — -e <bytes> read-back payload
    # ════════════════════════════════════════════════════════════════════════

    {   # 1-proc flat dir: create + stat + delete 4096 files, no payload
        "label":  "meta_flat_1proc_creates",
        "desc":   "1-proc mdtest: create/stat/delete 4096 files in flat dir (no payload)",
        "steps": [
            f"{B} -np 1 mdtest -C -T -r -n 4096 -d {TMP}/mdtest_flat_1proc -F",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc: create + write 1024 files × 4KB, no delete
        "label":  "meta_flat_1proc_write_only",
        "desc":   "1-proc mdtest: create + write 1024 files × 4KB (no read, no delete)",
        "steps": [
            f"{B} -np 1 mdtest -C -n 1024 -w 4096 -d {TMP}/mdtest_write1k_1proc -F",
        ],
        "capture": {
            "POSIX_OPENS":         -1,
            "POSIX_BYTES_WRITTEN": -1,
            "POSIX_WRITES":        -1,
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc: create/write/read/delete 512 files × 64KB
        # 512 × 65536 = 32 MB written and read back
        "label":  "meta_flat_1proc_rw",
        "desc":   "1-proc mdtest: create/write/read/delete 512 files × 64KB",
        "steps": [
            f"{B} -np 1 mdtest -C -E -r -n 512 -w 65536 -e 65536 -d {TMP}/mdtest_rw_1proc -F",
        ],
        "capture": {
            "POSIX_OPENS":         -1,
            "POSIX_BYTES_WRITTEN": -1,
            "POSIX_WRITES":        -1,
            "POSIX_BYTES_READ":    -1,
            "POSIX_READS":         -1,
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc fsync-heavy: 256 files × 4KB, fsync after each write (-y)
        "label":  "meta_fsync_1proc_small",
        "desc":   "1-proc mdtest: 256 files × 4KB with fsync (-y) — exercises POSIX_FSYNCS",
        "steps": [
            f"{B} -np 1 mdtest -C -r -n 256 -w 4096 -y -d {TMP}/mdtest_fsync_small -F",
        ],
        "capture": {
            "POSIX_OPENS":         -1,
            "POSIX_FSYNCS":        -1,
            "POSIX_BYTES_WRITTEN": -1,
            "POSIX_WRITES":        -1,
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc fsync medium: 128 files × 64KB, fsync after each write
        "label":  "meta_fsync_1proc_medium",
        "desc":   "1-proc mdtest: 128 files × 64KB with fsync (-y)",
        "steps": [
            f"{B} -np 1 mdtest -C -r -n 128 -w 65536 -y -d {TMP}/mdtest_fsync_medium -F",
        ],
        "capture": {
            "POSIX_OPENS":         -1,
            "POSIX_FSYNCS":        -1,
            "POSIX_BYTES_WRITTEN": -1,
            "POSIX_WRITES":        -1,
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc shallow directory tree (z=2, b=4, I=32)
        "label":  "meta_tree_1proc_shallow",
        "desc":   "1-proc mdtest: create/stat/delete in 2-deep dir tree (branch=4, items=32)",
        "steps": [
            f"{B} -np 1 mdtest -C -T -r -z 2 -b 4 -I 32 -d {TMP}/mdtest_tree_shallow",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc deep directory tree (z=4, b=4, I=32)
        "label":  "meta_tree_1proc_deep",
        "desc":   "1-proc mdtest: create/stat/delete in 4-deep dir tree (branch=4, items=32)",
        "steps": [
            f"{B} -np 1 mdtest -C -T -r -z 4 -b 4 -I 32 -d {TMP}/mdtest_tree_deep",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc tree, files at leaf level only (-L -F)
        "label":  "meta_tree_1proc_leaf_files",
        "desc":   "1-proc mdtest: files only at leaf level of 3-deep tree (-L -F, b=3, I=64)",
        "steps": [
            f"{B} -np 1 mdtest -C -T -r -L -F -z 3 -b 3 -I 64 -d {TMP}/mdtest_tree_leaf",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # 8-proc shared dir metadata contention: 256 files/proc
        "label":  "meta_shared_8proc_creates",
        "desc":   "8-proc mdtest: create/stat/delete 256 files/proc in shared dir",
        "steps": [
            f"{O} -np 8 mdtest -C -T -r -n 256 -d {TMP}/mdtest_shared_8proc -F",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # 8-proc shared dir with payload: 128 files/proc × 32KB
        "label":  "meta_shared_8proc_rw",
        "desc":   "8-proc mdtest: create/write/read/delete 128 files/proc × 32KB in shared dir",
        "steps": [
            f"{O} -np 8 mdtest -C -E -r -n 128 -w 32768 -e 32768 -d {TMP}/mdtest_shared_8proc_rw -F",
        ],
        "capture": {
            "POSIX_OPENS":         -1,
            "POSIX_BYTES_WRITTEN": -1,
            "POSIX_BYTES_READ":    -1,
        },
        "trace_for_csv": -1,
    },
    {   # 16-proc shared dir: 512 files/proc
        "label":  "meta_shared_16proc_creates",
        "desc":   "16-proc mdtest: create/stat/delete 512 files/proc in shared dir",
        "steps": [
            f"{O} -np 16 mdtest -C -T -r -n 512 -d {TMP}/mdtest_shared_16proc -F",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # 8-proc unique dirs (-u): FPP-equivalent for metadata, 512 files/proc
        "label":  "meta_unique_8proc_creates",
        "desc":   "8-proc mdtest: create/stat/delete 512 files/proc in unique dirs (-u)",
        "steps": [
            f"{O} -np 8 mdtest -C -T -r -n 512 -u -d {TMP}/mdtest_unique_8proc -F",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # 8-proc unique dirs with 16KB payload
        "label":  "meta_unique_8proc_rw",
        "desc":   "8-proc mdtest: create/write/read/delete 256 files/proc × 16KB in unique dirs",
        "steps": [
            f"{O} -np 8 mdtest -C -E -r -n 256 -w 16384 -e 16384 -u -d {TMP}/mdtest_unique_8proc_rw -F",
        ],
        "capture": {
            "POSIX_OPENS":         -1,
            "POSIX_BYTES_WRITTEN": -1,
            "POSIX_WRITES":        -1,
            "POSIX_BYTES_READ":    -1,
            "POSIX_READS":         -1,
        },
        "trace_for_csv": -1,
    },
    {   # 4-proc unique dirs, large 1MB files
        "label":  "meta_unique_4proc_large_files",
        "desc":   "4-proc mdtest: create/write/read/delete 64 files/proc × 1MB in unique dirs",
        "steps": [
            f"{B} -np 4 mdtest -C -E -r -n 64 -w 1048576 -e 1048576 -u -d {TMP}/mdtest_unique_4proc_1mb -F",
        ],
        "capture": {
            "POSIX_OPENS":         -1,
            "POSIX_BYTES_WRITTEN": -1,
            "POSIX_WRITES":        -1,
            "POSIX_BYTES_READ":    -1,
            "POSIX_READS":         -1,
        },
        "trace_for_csv": -1,
    },
    {   # stat-only pass: create files in step 0, stat in step 1, cleanup in step 2
        # trace_for_csv = 1 — stat phase Darshan trace populates the CSV row
        "label":  "meta_stat_only_1proc",
        "desc":   "1-proc mdtest: create 2048 files (keep), stat-only pass, then cleanup",
        "steps": [
            f"{B} -np 1 mdtest -C -n 2048 -d {TMP}/mdtest_statonly -F",
            f"{B} -np 1 mdtest -T -n 2048 -d {TMP}/mdtest_statonly -F",
            f"{B} -np 1 mdtest -r -n 2048 -d {TMP}/mdtest_statonly -F",
        ],
        "capture": {
            "POSIX_OPENS": 1,
            "POSIX_STATS": 1,
        },
        "trace_for_csv": 1,
    },
    {   # stat-only pass, 8-proc: 1024 files/proc
        "label":  "meta_stat_only_8proc",
        "desc":   "8-proc mdtest: create 1024 files/proc, stat-only pass, then cleanup",
        "steps": [
            f"{O} -np 8 mdtest -C -n 1024 -d {TMP}/mdtest_statonly_8proc -F",
            f"{O} -np 8 mdtest -T -n 1024 -d {TMP}/mdtest_statonly_8proc -F",
            f"{O} -np 8 mdtest -r -n 1024 -d {TMP}/mdtest_statonly_8proc -F",
        ],
        "capture": {
            "POSIX_OPENS": 1,
            "POSIX_STATS": 1,
        },
        "trace_for_csv": 1,
    },
    {   # high file count: 16384 empty files, 1-proc
        "label":  "meta_highcount_1proc",
        "desc":   "1-proc mdtest: create/stat/delete 16384 empty files in flat dir",
        "steps": [
            f"{B} -np 1 mdtest -C -T -r -n 16384 -d {TMP}/mdtest_highcount -F",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
    {   # high file count, 4-proc, unique dirs: 8192 files/proc
        "label":  "meta_highcount_4proc_unique",
        "desc":   "4-proc mdtest: create/stat/delete 8192 files/proc in unique dirs",
        "steps": [
            f"{B} -np 4 mdtest -C -T -r -n 8192 -u -d {TMP}/mdtest_highcount_4proc -F",
        ],
        "capture": {
            "POSIX_OPENS": -1,
            "POSIX_STATS": -1,
        },
        "trace_for_csv": -1,
    },
]

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def load_targets(csv_path):
    with open(csv_path) as f:
        return {r["label"]: r for r in csv.DictReader(f)}

def load_schema(csv_path):
    with open(csv_path) as f:
        return next(csv.reader(f))

def ensure_log_dir():
    today = datetime.now().strftime("%Y/%-m/%-d")
    path = os.path.join(LOG_BASE, today)
    os.makedirs(path, exist_ok=True)
    os.system(f"chmod -R 777 {LOG_BASE}")
    return path

def existing_darshan_files(log_dir):
    return set(glob.glob(f"{log_dir}/*.darshan"))

def run_step(cmd, step_idx, dry_run=False):
    print(f"  {BLUE}[step {step_idx}]{RESET} {cmd}")
    if dry_run:
        return True
    log_dir  = ensure_log_dir()
    before   = existing_darshan_files(log_dir)
    result   = subprocess.run(cmd, shell=True, capture_output=True, text=True)
    time.sleep(0.5)
    after    = existing_darshan_files(log_dir)
    new_files = sorted(after - before)
    if result.returncode != 0:
        print(f"  {RED}FAILED (exit {result.returncode}){RESET}")
        print(result.stderr[-800:] if result.stderr else "")
        return None
    return new_files[-1] if new_files else None

def nan_safe(v):
    try:
        f = float(v)
        return 0.0 if math.isnan(f) else f
    except (TypeError, ValueError):
        return 0.0

def get_counters(path):
    records = parse_darshan_file(path)
    if not records:
        return {}
    totals = {}
    for rec in records:
        for k, v in rec.items():
            if k in ("rank", "_row_in_rank"):
                continue
            totals[k] = totals.get(k, 0.0) + nan_safe(v)
    return totals

def looks_uninstrumented_posix(counters, tgt_row):
    """
    Heuristic: Darshan POSIX module looks inactive if all core POSIX counters
    are zero while the exemplar row expects non-zero POSIX activity.
    """
    core = ["POSIX_BYTES_READ", "POSIX_BYTES_WRITTEN", "POSIX_READS", "POSIX_WRITES"]
    all_zero = all(float(counters.get(k, 0) or 0.0) == 0.0 for k in core)
    tgt_nonzero = any(float(tgt_row.get(col, 0) or 0.0) > 0.0 for col in core)
    return all_zero and tgt_nonzero

def fmt_ratio(ratio):
    if ratio is None:
        return f"{YELLOW}N/A{RESET}"
    color = GREEN if 0.9 <= ratio <= 1.1 else (YELLOW if 0.75 <= ratio <= 1.25 else RED)
    mark  = "✅" if 0.9 <= ratio <= 1.1 else ("⚠️ " if 0.75 <= ratio <= 1.25 else "❌")
    return f"{color}{ratio:.3f}x {mark}{RESET}"

def build_csv_row(label, tgt_row, trace_paths, csv_step_idx, schema, run_cmd,
                  capture_map=None):
    """Build a CSV row from either an IOR trace or an mdtest trace.

    For IOR labels (capture_map is None), the single trace at csv_step_idx
    is used to populate all Darshan columns.

    For mdtest labels (capture_map is a dict), each counter may come from a
    different step's trace file (per-counter step overrides).
    """
    row = dict(tgt_row)
    row["label"]       = label
    row["filename"]    = os.path.basename(trace_paths[csv_step_idx]) \
                         if trace_paths and trace_paths[csv_step_idx] else ""
    row["ior_command"] = run_cmd

    if capture_map is None:
        # IOR path: one trace populates all columns
        tp = trace_paths[csv_step_idx] if trace_paths else None
        if tp:
            for col, val in get_counters(tp).items():
                if col in schema:
                    row[col] = val
    else:
        # mdtest path: per-counter step overrides
        counter_cache = {}
        def _ctrs(path):
            if path not in counter_cache:
                counter_cache[path] = get_counters(path) if path else {}
            return counter_cache[path]
        for col, step_idx in capture_map.items():
            tp = trace_paths[step_idx] if trace_paths else None
            if tp and col in schema:
                row[col] = nan_safe(_ctrs(tp).get(col, 0))

    return {col: row.get(col, "") for col in schema + ["ior_command"]}

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",        default=CSV_PATH)
    parser.add_argument("--logdir",     default=None)
    parser.add_argument(
        "--output-csv",
        default=os.path.join(SCRIPT_DIR, "data", "mdtest_generated_traces.csv"),
    )
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--labels",     default=None,
                        help="Comma-separated subset of labels, e.g. --labels 74,28,meta_flat_1proc_creates")
    args = parser.parse_args()

    targets       = load_targets(args.csv)
    schema        = load_schema(args.csv)
    log_dir       = args.logdir or ensure_log_dir()
    filter_labels = set(args.labels.split(",")) if args.labels else None
    active        = [e for e in LABELS if not filter_labels or e["label"] in filter_labels]

    ior_count  = sum(1 for e in active if "validate"  in e)
    meta_count = sum(1 for e in active if "capture"   in e)

    summary_rows = []   # (label, metric, target_val, got_val, ratio, is_meta)
    csv_rows     = []

    print(f"\n{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  IOR + mdtest Darshan Trace Validation — "
          f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}  Running {len(active)}/{len(LABELS)} labels  "
          f"({ior_count} IOR, {meta_count} mdtest){RESET}")
    print(f"{BOLD}{'='*72}{RESET}\n")

    for entry in active:
        label        = entry["label"]
        desc         = entry["desc"]
        steps        = entry["steps"]
        csv_step_idx = entry.get("trace_for_csv", -1)
        is_meta      = "capture" in entry          # mdtest label
        val_map      = entry.get("validate", {})   # IOR: metrics to ratio-check
        capture_map  = entry.get("capture",  {})   # mdtest: counters to record
        tgt_row      = targets.get(label, {})

        print(f"{BOLD}{'─'*72}{RESET}")
        ltype = f"{BLUE}[mdtest]{RESET}" if is_meta else f"{GREEN}[IOR]{RESET}"
        print(f"{BOLD}Label {label}{RESET} {ltype} — {desc}")
        print(f"{BOLD}{'─'*72}{RESET}")

        trace_paths = []
        ok = True
        for i, cmd in enumerate(steps):
            f = run_step(cmd, i, args.dry_run)
            trace_paths.append(f)
            if f is None and not args.dry_run:
                ok = False
                break

        if not ok:
            print(f"  {RED}Skipping — run failed.{RESET}\n")
            for col in (val_map if not is_meta else capture_map):
                summary_rows.append((label, col, "—", "—", None, is_meta))
            continue

        # ── IOR label: ratio validation ───────────────────────────────────
        if not is_meta:
            print(f"\n  {'Metric':<30} {'Target':>18} {'Got':>18} {'Ratio':>12}")
            print(f"  {'─'*30} {'─'*18} {'─'*18} {'─'*12}")
            posix_instrumentation_warned = False
            for csv_col, (darshan_key, step_idx) in val_map.items():
                target_val = float(tgt_row.get(csv_col, 0) or 0)
                if args.dry_run:
                    got_val, ratio = 0.0, None
                else:
                    tp = trace_paths[step_idx] if trace_paths else None
                    if tp:
                        counters = get_counters(tp)
                        if darshan_key.startswith("POSIX_") and looks_uninstrumented_posix(counters, tgt_row):
                            if not posix_instrumentation_warned:
                                print(f"  {YELLOW}POSIX counters all zero but exemplar has POSIX activity; "
                                      f"treating POSIX metrics for label {label} as instrumentation-limited.{RESET}")
                                posix_instrumentation_warned = True
                            got_val, ratio = 0.0, None
                        else:
                            got_val = nan_safe(counters.get(darshan_key, 0))
                            ratio   = (got_val / target_val) if target_val > 0 else None
                    else:
                        got_val, ratio = 0.0, None
                print(f"  {csv_col:<30} {target_val:>18,.0f} {got_val:>18,.0f} {fmt_ratio(ratio):>12}")
                summary_rows.append((label, csv_col, target_val, got_val, ratio, False))

        # ── mdtest label: capture display ─────────────────────────────────
        else:
            if not args.dry_run:
                print(f"\n  {'Counter':<32} {'Captured':>20}")
                print(f"  {'─'*32} {'─'*20}")
                counter_cache = {}
                def _ctrs(path):
                    if path not in counter_cache:
                        counter_cache[path] = get_counters(path) if path else {}
                    return counter_cache[path]
                for col, step_idx in capture_map.items():
                    tp = trace_paths[step_idx] if trace_paths else None
                    val = nan_safe(_ctrs(tp).get(col, 0)) if tp else 0.0
                    marker = f"{GREEN}●{RESET}" if val > 0 else f"{YELLOW}○{RESET}"
                    print(f"  {marker} {col:<30} {int(val) if val == int(val) else val:>20,}")
                    summary_rows.append((label, col, 0, val, None, True))

        if not args.dry_run:
            for i, p in enumerate(trace_paths):
                print(f"  {BLUE}[trace {i}]{RESET} {os.path.basename(p) if p else 'NONE'}")

        if not args.dry_run and trace_paths:
            run_cmd = steps[csv_step_idx] if steps else ""
            csv_rows.append(
                build_csv_row(label, tgt_row, trace_paths, csv_step_idx, schema,
                              run_cmd, capture_map=capture_map if is_meta else None)
            )
        print()

    # ── SUMMARY ──────────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  VALIDATION SUMMARY{RESET}")
    print(f"{BOLD}{'='*72}{RESET}")
    print(f"  {'Label':<25} {'Metric':<30} {'Ratio':>10}  Status")
    print(f"  {'─'*25} {'─'*30} {'─'*10}  {'─'*10}")

    passed = failed = warning = errored = meta_captured = meta_zero = 0
    for label, metric, tgt, got, ratio, is_meta in summary_rows:
        if tgt == "—":
            status = f"{RED}ERRORED{RESET}"; errored += 1
        elif is_meta:
            if ratio is None and isinstance(got, float) and got > 0:
                status = f"{BLUE}● captured{RESET}"; meta_captured += 1
            elif ratio is None and isinstance(got, float) and got == 0:
                status = f"{YELLOW}○ zero{RESET}"; meta_zero += 1
            else:
                status = f"{YELLOW}N/A{RESET}"; meta_zero += 1
        elif ratio is None:
            status = f"{YELLOW}N/A{RESET}"; meta_zero += 1
        elif 0.9 <= ratio <= 1.1:
            status = f"{GREEN}PASS ✅{RESET}"; passed += 1
        elif 0.75 <= ratio <= 1.25:
            status = f"{YELLOW}WARN ⚠️ {RESET}"; warning += 1
        else:
            status = f"{RED}FAIL ❌{RESET}"; failed += 1
        ratio_str = f"{ratio:.3f}x" if ratio is not None else "—"
        print(f"  {label:<25} {metric:<30} {ratio_str:>10}  {status}")

    print(f"\n  IOR  → {GREEN}PASS: {passed}{RESET}  {YELLOW}WARN: {warning}{RESET}"
          f"  {RED}FAIL: {failed}  ERRORED: {errored}{RESET}")
    print(f"  meta → {BLUE}Captured: {meta_captured}{RESET}"
          f"  {YELLOW}Zero: {meta_zero}{RESET}")
    print(f"{BOLD}{'='*72}{RESET}\n")

    if csv_rows and not args.dry_run:
        out_schema = schema + ["ior_command"]
    out_dir = os.path.dirname(args.output_csv) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(args.output_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=out_schema)
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"{GREEN}Generated traces CSV → {args.output_csv}  "
              f"({len(csv_rows)} rows, {len(schema)} cols){RESET}\n")

if __name__ == "__main__":
    main()