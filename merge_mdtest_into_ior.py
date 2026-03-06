#!/usr/bin/env python3
"""
merge_mdtest_into_ior.py

Merge selected mdtest metadata counters into the IOR rows so that each
exemplar label has a single row with both data-path and metadata-path
information.

Inputs (expected in /root):
  - mdtest_generated_traces.csv  (output of validate_mdtest_labels.py)

Output:
  - ior_plus_mdtest_25rows.csv  (24 synthetic rows today: all numeric labels)
"""

import pandas as pd


META_COLS = [
    "POSIX_OPENS",
    "POSIX_STATS",
    "POSIX_FSYNCS",
    "POSIX_BYTES_WRITTEN",
    "POSIX_WRITES",
    "POSIX_BYTES_READ",
    "POSIX_READS",
]


META_MAP = {
    # 1-proc, mostly shared, open-dominated
    "101": "meta_flat_1proc_creates",
    "20":  "meta_flat_1proc_write_only",
    "18":  "meta_flat_1proc_rw",
    "192": "meta_highcount_1proc",

    # 1-proc, heavy fsync / metadata
    "74":  "meta_fsync_1proc_medium",
    "258": "meta_fsync_1proc_small",
    "138": "meta_fsync_1proc_medium",

    # 1-proc, other open‑dominated
    "28":  "meta_flat_1proc_write_only",
    "252": "meta_flat_1proc_creates",
    "236": "meta_flat_1proc_write_only",

    # 4‑proc, FPP / unique
    "331": "meta_unique_4proc_large_files",
    "7":   "meta_unique_8proc_rw",
    "83":  None,  # very little metadata; no extra mdtest mapped

    # 8‑proc, FPP / unique
    "225": "meta_stat_only_8proc",
    "322": "meta_unique_8proc_creates",
    "50":  "meta_shared_8proc_rw",
    "41":  "meta_highcount_4proc_unique",  # no IOR label 41 today; kept for completeness

    # 16‑proc, mostly shared / mixed
    "48":  "meta_shared_8proc_creates",
    "64":  "meta_unique_8proc_creates",

    # 32/40/64‑proc, shared / mixed
    "47":  "meta_shared_16proc_creates",
    "237": "meta_stat_only_8proc",
    "344": "meta_unique_8proc_creates",
    "207": "meta_shared_16proc_creates",

    # 512‑proc MPI‑IO structural‑mismatch cases
    "249": "meta_highcount_4proc_unique",
    "193": None,
}


def main() -> None:
    src = "mdtest_generated_traces.csv"
    dst = "ior_plus_mdtest_25rows.csv"

    df = pd.read_csv(src, dtype={"label": str})

    ior = df[~df["label"].str.startswith("meta_")].set_index("label")
    meta = df[df["label"].str.startswith("meta_")].set_index("label")

    rows = []
    for lbl, ior_row in ior.iterrows():
        out = ior_row.copy()

        meta_label = META_MAP.get(lbl)
        if meta_label:
            if meta_label not in meta.index:
                # Mapping refers to a metadata label that wasn't run;
                # leave this IOR row unchanged.
                pass
            else:
                mrow = meta.loc[meta_label]
                for col in META_COLS:
                    if col in meta.columns:
                        out[col] = mrow.get(col, out.get(col, 0))

        rows.append(out)

    merged = pd.DataFrame(rows).reset_index()  # label back as a column
    merged.to_csv(dst, index=False)
    print(f"Wrote {dst} with {len(merged)} rows.")


if __name__ == "__main__":
    main()

