#!/usr/bin/env python3
"""Compute derived percentage features from raw Darshan counters.

These features are intended to represent synthesized behavior and should be
recomputed from raw counters, not copied from exemplar rows.
"""


def _f(row, key):
    try:
        v = row.get(key, 0)
        if v in ("", None):
            return 0.0
        return float(v)
    except Exception:
        return 0.0


def _safe_div(num, den):
    if den <= 0:
        return 0.0
    return float(num) / float(den)


def recompute_pct_features(row):
    reads = _f(row, "POSIX_READS")
    writes = _f(row, "POSIX_WRITES")
    accesses = reads + writes

    bytes_read = _f(row, "POSIX_BYTES_READ")
    bytes_written = _f(row, "POSIX_BYTES_WRITTEN")
    total_bytes = bytes_read + bytes_written

    row["pct_file_not_aligned"] = _safe_div(_f(row, "POSIX_FILE_NOT_ALIGNED"), accesses)
    row["pct_mem_not_aligned"] = _safe_div(_f(row, "POSIX_MEM_NOT_ALIGNED"), accesses)
    row["pct_reads"] = _safe_div(reads, accesses)
    row["pct_writes"] = _safe_div(writes, accesses)
    row["pct_consec_reads"] = _safe_div(_f(row, "POSIX_CONSEC_READS"), reads)
    row["pct_consec_writes"] = _safe_div(_f(row, "POSIX_CONSEC_WRITES"), writes)
    row["pct_seq_reads"] = _safe_div(_f(row, "POSIX_SEQ_READS"), reads)
    row["pct_seq_writes"] = _safe_div(_f(row, "POSIX_SEQ_WRITES"), writes)
    row["pct_rw_switches"] = _safe_div(_f(row, "POSIX_RW_SWITCHES"), accesses)
    row["pct_byte_reads"] = _safe_div(bytes_read, total_bytes)
    row["pct_byte_writes"] = _safe_div(bytes_written, total_bytes)

    meta_open = _f(row, "POSIX_OPENS")
    meta_stat = _f(row, "POSIX_STATS")
    meta_seek = _f(row, "POSIX_SEEKS")
    meta_sync = _f(row, "POSIX_FSYNCS") + _f(row, "POSIX_FDSYNCS")
    meta_total = meta_open + meta_stat + meta_seek + meta_sync

    row["pct_io_access"] = _safe_div(accesses, accesses + meta_total)
    row["pct_meta_open_access"] = _safe_div(meta_open, meta_total)
    row["pct_meta_stat_access"] = _safe_div(meta_stat, meta_total)
    row["pct_meta_seek_access"] = _safe_div(meta_seek, meta_total)
    row["pct_meta_sync_access"] = _safe_div(meta_sync, meta_total)

    read_small = _f(row, "POSIX_SIZE_READ_0_100") + _f(row, "POSIX_SIZE_READ_100_1K") + _f(row, "POSIX_SIZE_READ_1K_10K") + _f(row, "POSIX_SIZE_READ_10K_100K")
    read_mid = _f(row, "POSIX_SIZE_READ_100K_1M") + _f(row, "POSIX_SIZE_READ_1M_4M") + _f(row, "POSIX_SIZE_READ_4M_10M")
    read_large = _f(row, "POSIX_SIZE_READ_10M_100M") + _f(row, "POSIX_SIZE_READ_100M_1G") + _f(row, "POSIX_SIZE_READ_1G_PLUS")

    write_small = _f(row, "POSIX_SIZE_WRITE_0_100") + _f(row, "POSIX_SIZE_WRITE_100_1K") + _f(row, "POSIX_SIZE_WRITE_1K_10K") + _f(row, "POSIX_SIZE_WRITE_10K_100K")
    write_mid = _f(row, "POSIX_SIZE_WRITE_100K_1M") + _f(row, "POSIX_SIZE_WRITE_1M_4M") + _f(row, "POSIX_SIZE_WRITE_4M_10M")
    write_large = _f(row, "POSIX_SIZE_WRITE_10M_100M") + _f(row, "POSIX_SIZE_WRITE_100M_1G") + _f(row, "POSIX_SIZE_WRITE_1G_PLUS")

    row["pct_read_0_100K"] = _safe_div(read_small, reads)
    row["pct_read_100K_10M"] = _safe_div(read_mid, reads)
    row["pct_read_10M_1G_PLUS"] = _safe_div(read_large, reads)
    row["pct_write_0_100K"] = _safe_div(write_small, writes)
    row["pct_write_100K_10M"] = _safe_div(write_mid, writes)
    row["pct_write_10M_1G_PLUS"] = _safe_div(write_large, writes)

    total_files = _f(row, "POSIX_file_type_total_file_count")
    total_file_bytes = _f(row, "POSIX_file_type_total_total_bytes")

    row["pct_shared_files"] = _safe_div(_f(row, "POSIX_file_type_shared_file_count"), total_files)
    row["pct_bytes_shared_files"] = _safe_div(_f(row, "POSIX_file_type_shared_total_bytes"), total_file_bytes)
    row["pct_unique_files"] = _safe_div(_f(row, "POSIX_file_type_unique_file_count"), total_files)
    row["pct_bytes_unique_files"] = _safe_div(_f(row, "POSIX_file_type_unique_total_bytes"), total_file_bytes)
    row["pct_read_only_files"] = _safe_div(_f(row, "POSIX_file_type_read_only_file_count"), total_files)
    row["pct_bytes_read_only_files"] = _safe_div(_f(row, "POSIX_file_type_read_only_total_bytes"), total_file_bytes)
    row["pct_read_write_files"] = _safe_div(_f(row, "POSIX_file_type_read_write_file_count"), total_files)
    row["pct_bytes_read_write_files"] = _safe_div(_f(row, "POSIX_file_type_read_write_total_bytes"), total_file_bytes)
    row["pct_write_only_files"] = _safe_div(_f(row, "POSIX_file_type_write_only_file_count"), total_files)
    row["pct_bytes_write_only_files"] = _safe_div(_f(row, "POSIX_file_type_write_only_total_bytes"), total_file_bytes)

    return row
