#!/usr/bin/env python3
import os, csv, sys, argparse, re
try:
    from darshan.report import DarshanReport
except ImportError:
    sys.exit("PyDarshan module is required.")

MODULES_TO_PARSE = {"POSIX": "POSIX", "STDIO": "STDIO", "MPI-IO": "MPIIO"}

def parse_darshan_file(file_path):
    try:
        report = DarshanReport(file_path, read_all=False)
        module_frames = []
        for module_name, prefix in MODULES_TO_PARSE.items():
            if module_name not in report.modules:
                continue
            try:
                report.mod_read_all_records(module_name)
                module_records = report.records.get(module_name)
                if module_records is None or len(module_records) == 0:
                    continue
                module_data = module_records.to_df(attach=["rank"])
                if not isinstance(module_data, dict) or "counters" not in module_data or "fcounters" not in module_data:
                    continue
                counters_df = module_data["counters"].reset_index(drop=True)
                fcounters_df = module_data["fcounters"].reset_index(drop=True)
                module_df = counters_df.join(fcounters_df.drop(columns=["rank"], errors="ignore"), how="outer")
                module_df["_row_in_rank"] = module_df.groupby("rank").cumcount()
                rename_map = {col: f"{prefix}_{col}" for col in module_df.columns if col not in ["rank", "_row_in_rank"] and not col.startswith(f"{prefix}_")}
                module_df = module_df.rename(columns=rename_map)
                module_frames.append(module_df)
            except Exception as e:
                print(f"Warning: Skipping {module_name} for {file_path}: {e}")
        if not module_frames:
            return []
        combined_df = module_frames[0]
        for next_df in module_frames[1:]:
            combined_df = combined_df.merge(next_df, on=["rank", "_row_in_rank"], how="outer")
    except Exception as e:
        print(f"Error parsing {file_path}: {e}")
        return []
    return [{"rank": row["rank"], **{col: row[col] for col in combined_df.columns if col not in ["rank", "_row_in_rank"]}} for _, row in combined_df.iterrows()]

def extract_config_from_path(leaf_dir, results_dir):
    rel_path = os.path.relpath(leaf_dir, results_dir)
    parts = rel_path.split(os.sep)
    config = {}
    if parts:
        config["workload_name"] = parts[0]
    for i in range(1, len(parts) - 1, 2):
        config[parts[i]] = parts[i + 1]
    return config

def find_darshan_files(results_dir):
    darshan_files = []
    for root, dirs, files in os.walk(results_dir):
        for file in files:
            if file.endswith(".darshan"):
                darshan_files.append((root, os.path.join(root, file)))
    return darshan_files

def infer_jobid_from_filename(file_path):
    match = re.search(r"_id(\d+)-", os.path.basename(file_path))
    return int(match.group(1)) if match else ""

def align_csv_to_reference_schema(output_csv, reference_csv):
    if not reference_csv or not os.path.exists(reference_csv):
        return False
    with open(reference_csv, newline="") as f:
        ref_header = next(csv.reader(f), None)
    if not ref_header:
        return False
    with open(output_csv, newline="") as f:
        out_rows = list(csv.DictReader(f))
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=ref_header)
        writer.writeheader()
        writer.writerows([{col: row.get(col, "") for col in ref_header} for row in out_rows])
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("results_dir")
    parser.add_argument("--schema-reference", default="")
    args = parser.parse_args()
    results_dir = args.results_dir
    output_csv = os.path.join(results_dir, "darshan_posix_summary.csv")
    darshan_entries = find_darshan_files(results_dir)
    if not darshan_entries:
        print("No .darshan files found.")
        return
    rows, fieldnames = [], set()
    for leaf_dir, darshan_file in darshan_entries:
        config = extract_config_from_path(leaf_dir, results_dir)
        module_records = parse_darshan_file(darshan_file)
        if not module_records:
            continue
        distinct_ranks = {r.get("rank") for r in module_records if r.get("rank") == r.get("rank")}
        nprocs = len(distinct_ranks) if distinct_ranks else 1
        rel_filename = os.path.relpath(darshan_file, results_dir)
        jobid = infer_jobid_from_filename(darshan_file)
        for record in module_records:
            posix_total = (record.get("POSIX_BYTES_READ", 0) or 0) + (record.get("POSIX_BYTES_WRITTEN", 0) or 0)
            mpiio_total = (record.get("MPIIO_BYTES_READ", 0) or 0) + (record.get("MPIIO_BYTES_WRITTEN", 0) or 0)
            stdio_total = (record.get("STDIO_BYTES_READ", 0) or 0) + (record.get("STDIO_BYTES_WRITTEN", 0) or 0)
            combined = {**config, "filename": rel_filename, "jobid": jobid, "nprocs": nprocs, "total_bytes": posix_total + mpiio_total + stdio_total, **record}
            rows.append(combined)
            fieldnames.update(combined.keys())
    sorted_fieldnames = ["workload_name"]
    config_keys = sorted({k for row in rows for k in row if k not in ["workload_name","rank"] and not k.startswith(("POSIX","STDIO","MPIIO"))})
    sorted_fieldnames.extend(config_keys)
    sorted_fieldnames.append("rank")
    sorted_fieldnames.extend(sorted({k for row in rows for k in row if k.startswith(("POSIX","STDIO","MPIIO"))}))
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=sorted_fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    schema_ref = args.schema_reference or (os.path.join(os.getcwd(), "cluster_top25_exemplars_with_darshan.csv") if os.path.exists(os.path.join(os.getcwd(), "cluster_top25_exemplars_with_darshan.csv")) else "")
    if align_csv_to_reference_schema(output_csv, schema_ref):
        print(f"CSV saved to {output_csv} (aligned to schema)")
    else:
        print(f"CSV saved to {output_csv}")

if __name__ == "__main__":
    main()
