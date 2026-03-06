#!/usr/bin/env python3
"""
generate_feature_comparison_report.py

Produce a CSV that compares, for every cluster-exemplar label, the original
32 derived features against the features synthesised by IOR (+ mdtest).

Output format (wide):
    label | ior_command | original_<feat1> | synthesized_<feat1> | ... | original_<featN> | synthesized_<featN>

Creates two files:
  - {output_stem}_pct.csv: 32 derived percentage features only
  - {output_stem}_full.csv: all metrics (percentages, raw Darshan, POSIX/MPIIO/STDIO, etc.)

Usage:
    python3 generate_feature_comparison_report.py
    python3 generate_feature_comparison_report.py --original data/cluster_top25_exemplars_with_darshan.csv \
                                                  --synthesized data/ior_plus_mdtest_25rows.csv \
                                                  --output data/feature_comparison_report.csv
"""

import argparse
import os
import sys

import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Columns to exclude from comparison (identifiers, not metrics).
EXCLUDE_COLUMNS = frozenset({"label", "index", "filename", "ior_command"})

# The 32 derived percentage features used for clustering.
PCT_FEATURES = [
    "pct_file_not_aligned",
    "pct_mem_not_aligned",
    "pct_reads",
    "pct_writes",
    "pct_consec_reads",
    "pct_consec_writes",
    "pct_seq_reads",
    "pct_seq_writes",
    "pct_rw_switches",
    "pct_byte_reads",
    "pct_byte_writes",
    "pct_io_access",
    "pct_meta_open_access",
    "pct_meta_stat_access",
    "pct_meta_seek_access",
    "pct_meta_sync_access",
    "pct_read_0_100K",
    "pct_read_100K_10M",
    "pct_read_10M_1G_PLUS",
    "pct_write_0_100K",
    "pct_write_100K_10M",
    "pct_write_10M_1G_PLUS",
    "pct_shared_files",
    "pct_bytes_shared_files",
    "pct_unique_files",
    "pct_bytes_unique_files",
    "pct_read_only_files",
    "pct_bytes_read_only_files",
    "pct_read_write_files",
    "pct_bytes_read_write_files",
    "pct_write_only_files",
    "pct_bytes_write_only_files",
]


def get_compare_columns(orig_cols, synth_cols):
    """Return all metric columns present in both original and synthesized."""
    exclude = EXCLUDE_COLUMNS
    orig_set = set(orig_cols) - exclude
    synth_set = set(synth_cols) - exclude
    return sorted(orig_set.intersection(synth_set))


def main():
    parser = argparse.ArgumentParser(
        description="Compare original vs IOR-synthesised features for all cluster exemplars."
    )
    parser.add_argument(
        "--original",
        default=os.path.join(SCRIPT_DIR, "data", "cluster_top25_exemplars_with_darshan.csv"),
        help="CSV with the original cluster-exemplar features.",
    )
    parser.add_argument(
        "--synthesized",
        default=os.path.join(SCRIPT_DIR, "data", "ior_plus_mdtest_25rows.csv"),
        help="CSV with the IOR (+mdtest) synthesised features (must contain 'ior_command').",
    )
    parser.add_argument(
        "--output",
        default=os.path.join(SCRIPT_DIR, "data", "feature_comparison_report.csv"),
        help="Base path for output; writes <stem>_pct.csv and <stem>_full.csv.",
    )
    args = parser.parse_args()

    # ── Load data ────────────────────────────────────────────────────────
    orig = pd.read_csv(args.original)
    synth = pd.read_csv(args.synthesized)

    # Normalise to a common "label" column (some CSVs call it "index").
    for df, name in [(orig, "original"), (synth, "synthesized")]:
        if "label" not in df.columns:
            if "index" in df.columns:
                df.rename(columns={"index": "label"}, inplace=True)
            else:
                sys.exit(f"The {name} CSV has neither 'label' nor 'index' column.")

    orig["label"] = orig["label"].astype(str)
    synth["label"] = synth["label"].astype(str)

    orig = orig.set_index("label")
    synth = synth.set_index("label")

    ALL_FEATURES = get_compare_columns(orig.columns, synth.columns)
    if not ALL_FEATURES:
        sys.exit("No common metric columns found between original and synthesized CSVs.")

    # Percentages: only features that exist in both CSVs
    PCT_FEATURES_AVAIL = [f for f in PCT_FEATURES if f in ALL_FEATURES]

    # Only keep labels present in both
    common_labels = sorted(orig.index.intersection(synth.index), key=lambda x: int(x))
    if not common_labels:
        sys.exit("No labels found in common between original and synthesised CSVs.")

    print(f"Labels in original:    {len(orig)}")
    print(f"Labels in synthesised: {len(synth)}")
    print(f"Labels in common:      {len(common_labels)}")

    def build_dataframe(features):
        rows = []
        for label in common_labels:
            o = orig.loc[label]
            s = synth.loc[label]
            row = {"label": label, "ior_command": s.get("ior_command", "")}
            for feat in features:
                row[f"original_{feat}"] = o.get(feat, "")
                row[f"synthesized_{feat}"] = s.get(feat, "")
            rows.append(row)
        cols = ["label", "ior_command"]
        for feat in features:
            cols.extend([f"original_{feat}", f"synthesized_{feat}"])
        return pd.DataFrame(rows, columns=cols)

    df_pct = build_dataframe(PCT_FEATURES_AVAIL)
    df_full = build_dataframe(ALL_FEATURES)

    out_dir = os.path.dirname(args.output) or "."
    stem = os.path.splitext(os.path.basename(args.output))[0]
    path_pct = os.path.join(out_dir, f"{stem}_pct.csv")
    path_full = os.path.join(out_dir, f"{stem}_full.csv")

    os.makedirs(out_dir, exist_ok=True)
    df_pct.to_csv(path_pct, index=False)
    df_full.to_csv(path_full, index=False)

    print(f"\nComparison reports written:")
    print(f"  Percentages only:  {path_pct}  ({len(df_pct)} rows x {len(df_pct.columns)} cols, {len(PCT_FEATURES_AVAIL)} feature pairs)")
    print(f"  All metrics:       {path_full}  ({len(df_full)} rows x {len(df_full.columns)} cols, {len(ALL_FEATURES)} feature pairs)")


if __name__ == "__main__":
    main()
