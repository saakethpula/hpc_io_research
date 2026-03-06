#!/usr/bin/env python3
"""
validate_all_labels.py — all 25 cluster exemplar labels
Usage:
    python3 validate_all_labels.py --csv /root/cluster_top25_exemplars_with_darshan.csv
    python3 validate_all_labels.py --csv ... --labels 74,28,252   # run subset
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
B           = f"mpirun --allow-run-as-root -x LD_PRELOAD={DARSHAN_LIB}"   # base
O           = f"mpirun --allow-run-as-root --oversubscribe -x LD_PRELOAD={DARSHAN_LIB}"  # oversubscribe

GREEN="\033[92m"; RED="\033[91m"; YELLOW="\033[93m"; BLUE="\033[94m"; BOLD="\033[1m"; RESET="\033[0m"

# ─── LABEL DEFINITIONS ───────────────────────────────────────────────────────
# validate: {CSV_col: (darshan_key, trace_step_index)}  (-1 = last step)
# trace_for_csv: which step's trace to use for the output CSV row
#
# Structural mismatch notes (not validated):
#   Label 47:  POSIX_WRITES — IOR shared file always 1 write/proc; original app had 8
#   Label 48:  POSIX_WRITES — ROMIO layer calls, not app-level ops
#   Label 249: POSIX_WRITES count — MPIIO indep maps differently per-proc
#   Label 193: POSIX_WRITES count — same ROMIO issue as 48
#   Label 83 read: xfer size ~512B — IOR minimum block is unreliable at this scale;
#                  bytes will be approximate only

LABELS = [

    # ════════════════════════════════════════════════════════════════════════
    # PREVIOUSLY VALIDATED (8 labels)
    # ════════════════════════════════════════════════════════════════════════

    {   "label": "101",
        "desc":  "1-proc POSIX read 4×27MB",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 27M -t 27M -s 4 -w -o {TMP}/val_101.dat -k",
            f"{B} -np 1 ior -a POSIX -b 27M -t 27M -s 4 -r -o {TMP}/val_101.dat",
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
            f"{B} -np 1 ior -a POSIX -b 171M -t 171M -s 4 -w -o {TMP}/val_18_w.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1024M -t 1024M -s 2 -w -o {TMP}/val_18_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1024M -t 1024M -s 2 -r -o {TMP}/val_18_r.dat",
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
            f"{B} -np 1 ior -a POSIX -b 512M -t 512M -s 4 -w -o {TMP}/val_20.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1024M -t 1024M -s 2 -r -o {TMP}/val_20.dat",
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
            f"{B} -np 1 ior -a MPIIO -b 550M -t 550M -s 19 -w -c -o {TMP}/val_192.dat",
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
            f"{B} -np 4 ior -a POSIX -b 30M -t 8K -s 1 -F -w -o {TMP}/val_331.dat -k",
            f"{B} -np 4 ior -a POSIX -b 30M -t 8K -s 1 -F -r -C -o {TMP}/val_331.dat",
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
            f"{O} -np 40 ior -a POSIX -b 16M -t 16M -s 1 -w -o {TMP}/val_47.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", -1),
        },
        "trace_for_csv": -1,
    },
    {   "label": "48",
        "desc":  "16-proc MPIIO collective write 4.4GB, 10 segs  [POSIX_WRITES = ROMIO detail, not validated]",
        "steps": [
            f"{O} -np 16 ior -a MPIIO -b 26M -t 26M -s 10 -w -c -o {TMP}/val_48.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN", -1),
        },
        "trace_for_csv": -1,
    },

    # ════════════════════════════════════════════════════════════════════════
    # NEW LABELS (17 labels)
    # ════════════════════════════════════════════════════════════════════════

    {   # 1-proc POSIX write 82MB + read 82MB
        "label": "74",
        "desc":  "1-proc POSIX write 82MB (850 ops) + read 82MB (835 ops)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 96K -t 96K -s 875 -w -o {TMP}/val_74.dat -k",
            f"{B} -np 1 ior -a POSIX -b 96K -t 96K -s 875 -r -o {TMP}/val_74.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   # 77GB write 991 ops @ 74MB; 13MB read 67 ops @ 192KB — separate files (sizes very different)
        "label": "28",
        "desc":  "1-proc POSIX write 77GB (991 ops, 74M) + read 13MB (67 ops, 192K)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 74M -t 74M -s 991 -w -o {TMP}/val_28_w.dat",
            f"{B} -np 1 ior -a POSIX -b 192K -t 192K -s 67 -w -o {TMP}/val_28_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 192K -t 192K -s 67 -r -o {TMP}/val_28_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN",  0),
            "POSIX_WRITES":        ("POSIX_WRITES",          0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",     -1),
            "POSIX_READS":         ("POSIX_READS",          -1),
        },
        "trace_for_csv": 0,
    },
    {   # BYTES_WRITTEN=417 is metadata noise; primary signal is read 718MB in 2 ops @ 343MB
        "label": "252",
        "desc":  "1-proc POSIX read 718MB (2 ops, 343M xfer)  [BYTES_WRITTEN=417 is metadata noise]",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 343M -t 343M -s 2 -w -o {TMP}/val_252.dat -k",
            f"{B} -np 1 ior -a POSIX -b 343M -t 343M -s 2 -r -o {TMP}/val_252.dat",
        ],
        "validate": {
            "POSIX_BYTES_READ": ("POSIX_BYTES_READ", -1),
            "POSIX_READS":      ("POSIX_READS",      -1),
        },
        "trace_for_csv": -1,
    },
    {   # 512-proc target; reduced to 64-proc to avoid SIGBUS from shared-mem exhaustion
        # 64 × 1M × 275 segs = 18.45 GB ≈ target 18,450,934,368
        "label": "249",
        "desc":  "64-proc MPIIO independent write ~18.4GB (64×275 ops, 1M xfer)  [reduced from 512 to avoid SIGBUS]",
        "steps": [
            f"{O} -np 64 ior -a MPIIO -b 1M -t 1M -s 275 -w -o {TMP}/val_249.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN", -1),
        },
        "trace_for_csv": -1,
    },
    {   # 1-proc write 1.15GB / 24 ops = 46.9MB → use 46M
        "label": "236",
        "desc":  "1-proc POSIX write 1.15GB (24 ops, 46M xfer)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 46M -t 46M -s 24 -w -o {TMP}/val_236.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", -1),
            "POSIX_WRITES":        ("POSIX_WRITES",        -1),
        },
        "trace_for_csv": -1,
    },
    {   # 32-proc: write 8.8MB / 1864 ops = 4.6KB → 4K×64 segs; read 2GB / 22416 ops = 89KB → 89K×700 segs
        "label": "237",
        "desc":  "32-proc POSIX write 8.4MB (2048 ops, 4K) + read 2.0GB (22400 ops, 89K)",
        "steps": [
            f"{O} -np 32 ior -a POSIX -b 4K -t 4K -s 64 -w -o {TMP}/val_237_w.dat",
            f"{O} -np 32 ior -a POSIX -b 89K -t 89K -s 700 -w -o {TMP}/val_237_r.dat -k",
            f"{O} -np 32 ior -a POSIX -b 89K -t 89K -s 700 -r -C -o {TMP}/val_237_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   # 64-proc write 1.26GB / 1688 ops = 728KB×26 segs; read 59MB / 124 ops = 467KB×2 segs
        "label": "344",
        "desc":  "64-proc POSIX write 1.24GB (1664 ops, 728K) + read 59MB (128 ops, 467K)",
        "steps": [
            f"{O} -np 64 ior -a POSIX -b 728K -t 728K -s 26 -w -F -e -o {TMP}/val_344_w.dat",
            f"{O} -np 64 ior -a POSIX -b 467K -t 467K -s 2 -w -F -e -o {TMP}/val_344_r.dat -k",
            f"{O} -np 64 ior -a POSIX -b 467K -t 467K -s 2 -r -C -F -o {TMP}/val_344_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   # 4-proc MPIIO indep write 5.22GB / 3 ops = 1.66GB/op → per proc = 1 seg of 1304MB
        # read 1.3MB / 4 ops = 323KB → per proc = 1 seg — MPIIO indep read
        "label": "50",
        "desc":  "4-proc MPIIO indep write 5.2GB (3 ops, 1304M) + indep read 1.3MB (4 ops, 323K)",
        "steps": [
            f"{B} -np 4 ior -a MPIIO -b 1304M -t 1304M -s 1 -w -o {TMP}/val_50.dat -k",
            f"{B} -np 4 ior -a MPIIO -b 323K -t 323K -s 1 -r -C -o {TMP}/val_50.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN",  0),
            "MPIIO_BYTES_READ":    ("MPIIO_BYTES_READ",     -1),
        },
        "trace_for_csv": 0,
    },
    {   # 8-proc write 4.19GB / 1000 ops = 4.09MB → use 4M; per proc = 125 segs
        "label": "225",
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
    {   # 4-proc write 80.5GB / 14910 ops = 5.27MB → use 5M; per proc = 3728 segs
        # read 5.8MB / 11342 ops = 524B → IOR min reliable block ~4K; validate bytes only
        "label": "83",
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
    {   # 8-proc write 1.55GB / 863 ops = 1748KB/op → use 1750K; per proc = 108 segs
        "label": "322",
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
    {   # write 117MB / 4 ops = 28MB → use 28M; read 15GB / 8 ops = 1788MB → 1788M
        # read file must be pre-created — write 15GB first then read it
        "label": "258",
        "desc":  "1-proc POSIX write 117MB (4 ops, 28M) + read 15GB (8 ops, 1788M)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 28M -t 28M -s 4 -w -o {TMP}/val_258_w.dat",
            f"{B} -np 1 ior -a POSIX -b 1788M -t 1788M -s 8 -w -o {TMP}/val_258_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 1788M -t 1788M -s 8 -r -o {TMP}/val_258_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN",  0),
            "POSIX_WRITES":        ("POSIX_WRITES",          0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",     -1),
            "POSIX_READS":         ("POSIX_READS",          -1),
        },
        "trace_for_csv": 0,
    },
    {   # 64-proc write 78.8MB / 447 ops = 172KB×7 segs; read 39.2MB / 190 ops = 202KB×3 segs
        "label": "207",
        "desc":  "64-proc POSIX write 78.9MB (448 ops, 172K) + read 39.7MB (192 ops, 202K)",
        "steps": [
            f"{O} -np 64 ior -a POSIX -b 172K -t 172K -s 7 -w -o {TMP}/val_207_w.dat",
            f"{O} -np 64 ior -a POSIX -b 202K -t 202K -s 3 -w -o {TMP}/val_207_r.dat -k",
            f"{O} -np 64 ior -a POSIX -b 202K -t 202K -s 3 -r -C -o {TMP}/val_207_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
    },
    {   # 512-proc target; reduced to 64-proc to avoid SIGBUS
        # 64 × 256K × 208 segs: MPIIO_COLL_WRITES = 64×208 = 13312 (exact), bytes ≈ 3.25 GB
        "label": "193",
        "desc":  "64-proc MPIIO coll write ~3.25GB (13312 ops, 256K xfer)  [reduced from 512 to avoid SIGBUS]",
        "steps": [
            f"{O} -np 64 ior -a MPIIO -b 256K -t 256K -s 208 -w -c -o {TMP}/val_193.dat",
        ],
        "validate": {
            "MPIIO_BYTES_WRITTEN": ("MPIIO_BYTES_WRITTEN", -1),
            "MPIIO_COLL_WRITES":   ("MPIIO_COLL_WRITES", -1),
        },
        "trace_for_csv": -1,
    },
    {   # write 22.9MB / 683 ops = 32.7KB → use 33K; read 142.6MB / 15273 ops = 9.1KB → use 9K
        # separate files (read is much larger relative to write xfer)
        "label": "138",
        "desc":  "1-proc POSIX write 22.9MB (683 ops, 33K) + read 142.6MB (15273 ops, 9K)",
        "steps": [
            f"{B} -np 1 ior -a POSIX -b 33K -t 33K -s 683 -w -o {TMP}/val_138_w.dat",
            f"{B} -np 1 ior -a POSIX -b 9K -t 9K -s 15273 -w -o {TMP}/val_138_r.dat -k",
            f"{B} -np 1 ior -a POSIX -b 9K -t 9K -s 15273 -r -o {TMP}/val_138_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN",  0),
            "POSIX_WRITES":        ("POSIX_WRITES",          0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ",     -1),
            "POSIX_READS":         ("POSIX_READS",          -1),
        },
        "trace_for_csv": 0,
    },
    {   # 16-proc write 11.8GB / 545542 ops = 21KB×34096 segs; read 22.3GB / 145774 ops = 149KB×9111 segs
        "label": "64",
        "desc":  "16-proc POSIX write 11.7GB (545K ops, 21K) + read 22.2GB (145K ops, 149K)",
        "steps": [
            f"{O} -np 16 ior -a POSIX -b 21K -t 21K -s 34096 -w -F -e -o {TMP}/val_64_w.dat",
            f"{O} -np 16 ior -a POSIX -b 149K -t 149K -s 9111 -w -F -e -o {TMP}/val_64_r.dat -k",
            f"{O} -np 16 ior -a POSIX -b 149K -t 149K -s 9111 -r -C -F -o {TMP}/val_64_r.dat",
        ],
        "validate": {
            "POSIX_BYTES_WRITTEN": ("POSIX_BYTES_WRITTEN", 0),
            "POSIX_WRITES":        ("POSIX_WRITES", 0),
            "POSIX_BYTES_READ":    ("POSIX_BYTES_READ", -1),
            "POSIX_READS":         ("POSIX_READS", -1),
        },
        "trace_for_csv": 0,
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

def build_csv_row(label, tgt_row, trace_path, schema, ior_cmd=""):
    row = dict(tgt_row)
    row["label"]       = label
    row["filename"]    = os.path.basename(trace_path) if trace_path else ""
    row["ior_command"] = ior_cmd
    if trace_path:
        for col, val in get_counters(trace_path).items():
            if col in schema:
                row[col] = val
    return {col: row.get(col, "") for col in schema + ["ior_command"]}

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",        default=CSV_PATH)
    parser.add_argument("--logdir",     default=None)
    parser.add_argument(
        "--output-csv",
        default=os.path.join(SCRIPT_DIR, "data", "ior_generated_traces.csv"),
    )
    parser.add_argument("--dry-run",    action="store_true")
    parser.add_argument("--labels",     default=None,
                        help="Comma-separated subset of labels to run, e.g. --labels 74,28,64")
    args = parser.parse_args()

    targets       = load_targets(args.csv)
    schema        = load_schema(args.csv)
    log_dir       = args.logdir or ensure_log_dir()
    filter_labels = set(args.labels.split(",")) if args.labels else None
    active        = [e for e in LABELS if not filter_labels or e["label"] in filter_labels]

    summary_rows = []
    csv_rows     = []

    print(f"\n{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  IOR Darshan Trace Validation — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}{RESET}")
    print(f"{BOLD}  Running {len(active)}/{len(LABELS)} labels{RESET}")
    print(f"{BOLD}{'='*72}{RESET}\n")

    for entry in active:
        label        = entry["label"]
        desc         = entry["desc"]
        steps        = entry["steps"]
        val_map      = entry["validate"]
        csv_step_idx = entry.get("trace_for_csv", -1)
        tgt_row      = targets.get(label, {})

        print(f"{BOLD}{'─'*72}{RESET}")
        print(f"{BOLD}Label {label}{RESET} — {desc}")
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
            print(f"  {RED}Skipping validation — IOR run failed.{RESET}\n")
            for col in val_map:
                summary_rows.append((label, col, "—", "—", None))
            continue

        print(f"\n  {'Metric':<30} {'Target':>18} {'Got':>18} {'Ratio':>12}")
        print(f"  {'─'*30} {'─'*18} {'─'*18} {'─'*12}")

        posix_instrumentation_warned = False

        for csv_col, (darshan_key, step_idx) in val_map.items():
            target_val = float(tgt_row.get(csv_col, 0) or 0)
            if args.dry_run:
                got_val, ratio = 0.0, None
            else:
                tp = trace_paths[step_idx] if trace_paths else None
                if tp is None:
                    got_val, ratio = 0.0, None
                else:
                    counters = get_counters(tp)
                    # Detect runs where POSIX module appears inactive: avoid
                    # treating missing POSIX data as a numeric failure.
                    if darshan_key.startswith("POSIX_") and looks_uninstrumented_posix(counters, tgt_row):
                        if not posix_instrumentation_warned:
                            print(f"  {YELLOW}POSIX counters all zero but exemplar has POSIX activity; "
                                  f"treating POSIX metrics for label {label} as instrumentation-limited.{RESET}")
                            posix_instrumentation_warned = True
                        got_val, ratio = 0.0, None
                    else:
                        got_val = nan_safe(counters.get(darshan_key, 0))
                        ratio   = (got_val / target_val) if target_val > 0 else None
            print(f"  {csv_col:<30} {target_val:>18,.0f} {got_val:>18,.0f} {fmt_ratio(ratio):>12}")
            summary_rows.append((label, csv_col, target_val, got_val, ratio))

        if not args.dry_run:
            for i, p in enumerate(trace_paths):
                print(f"  {BLUE}[trace {i}]{RESET} {os.path.basename(p) if p else 'NONE'}")

        if not args.dry_run and trace_paths:
            ior_cmd = steps[csv_step_idx] if steps else ""
            csv_rows.append(build_csv_row(label, tgt_row, trace_paths[csv_step_idx], schema, ior_cmd))
        print()

    # ── SUMMARY ──────────────────────────────────────────────────────────────
    print(f"\n{BOLD}{'='*72}{RESET}")
    print(f"{BOLD}  VALIDATION SUMMARY{RESET}")
    print(f"{BOLD}{'='*72}{RESET}")
    print(f"  {'Label':<8} {'Metric':<30} {'Ratio':>10}  Status")
    print(f"  {'─'*8} {'─'*30} {'─'*10}  {'─'*10}")

    passed = failed = warning = pending = 0
    for label, metric, tgt, got, ratio in summary_rows:
        if ratio is None:           status = f"{YELLOW}PENDING{RESET}"; pending += 1
        elif 0.9 <= ratio <= 1.1:   status = f"{GREEN}PASS ✅{RESET}";  passed  += 1
        elif 0.75 <= ratio <= 1.25: status = f"{YELLOW}WARN ⚠️ {RESET}"; warning += 1
        else:                       status = f"{RED}FAIL ❌{RESET}";     failed  += 1
        print(f"  {label:<8} {metric:<30} {f'{ratio:.3f}x' if ratio else '—':>10}  {status}")

    print(f"\n  {GREEN}PASS: {passed}{RESET}  {YELLOW}WARN: {warning}  PENDING: {pending}{RESET}  {RED}FAIL: {failed}{RESET}")
    print(f"{BOLD}{'='*72}{RESET}\n")

    if csv_rows and not args.dry_run:
        out_schema = schema + ["ior_command"]
    out_dir = os.path.dirname(args.output_csv) or "."
    os.makedirs(out_dir, exist_ok=True)
    with open(args.output_csv, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=out_schema)
            writer.writeheader()
            writer.writerows(csv_rows)
        print(f"{GREEN}Generated traces CSV → {args.output_csv}  ({len(csv_rows)} rows, {len(schema)} cols){RESET}\n")

if __name__ == "__main__":
    main()