#!/usr/bin/env python3
"""
generate_csv.py — Synthetic CSV generator based on a sample file.

Usage:
    python generate_csv.py <input_csv> <output_csv> <size_mb>

Example:
    python generate_csv.py sample.csv output.csv 10

The script reads the header and a few rows from the input CSV to infer
column types, value pools, and date formats, then streams plausible
random rows into the output file until the target size is reached.
"""

import csv
import os
import random
import re
import string
import sys
from datetime import datetime, timedelta

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def detect_date_format(values: list[str]) -> str | None:
    """
    Try to identify the strftime format used by timestamp strings in the sample.
    Returns the format string if detected, or None.
    """
    # Patterns tried in order — first match wins
    candidates = [
        "%Y-%m-%d %H:%M:%S.%f",   # 2025-03-19 01:24:18.946876
        "%Y-%m-%dT%H:%M:%S.%f",   # ISO-8601 with microseconds
        "%Y-%m-%dT%H:%M:%S",       # ISO-8601
        "%Y-%m-%d %H:%M:%S",       # simple datetime
        "%d/%m/%Y %H:%M:%S",
        "%m/%d/%Y %H:%M:%S",
        "%d-%m-%Y",
        "%Y-%m-%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
    ]
    for fmt in candidates:
        ok = 0
        for v in values:
            if not isinstance(v, str) or not v.strip():
                continue
            try:
                datetime.strptime(v.strip()[:len(fmt) + 4], fmt)
                ok += 1
            except ValueError:
                pass
        if ok >= max(1, len(values) // 2):
            return fmt
    return None


def random_date_str(base: datetime, fmt: str, sample_value: str) -> str:
    """
    Generate a random datetime string near `base`, formatted exactly like
    the original sample value (preserving sub-second digit count).
    """
    # Random jitter: up to ±30 minutes relative to the base timestamp
    delta_seconds = random.uniform(-1800, 1800)
    dt = base + timedelta(seconds=delta_seconds)

    # Detect how many sub-second digits the sample uses
    # e.g. "2025-03-19 01:24:18.9468760" → 7 digits after the dot
    subsecond_digits = 0
    dot_pos = sample_value.rfind(".")
    if dot_pos != -1:
        tail = re.split(r"[^\d]", sample_value[dot_pos + 1:])[0]
        subsecond_digits = len(tail)

    formatted = dt.strftime(fmt)

    if subsecond_digits and "%f" not in fmt:
        # fmt doesn't include microseconds but the sample has sub-seconds
        micro = f"{dt.microsecond:06d}"
        # pad or trim to match original digit count
        sub = micro[:subsecond_digits].ljust(subsecond_digits, "0")
        formatted = formatted + "." + sub
    elif subsecond_digits and "%f" in fmt:
        # strftime %f always gives 6 digits; adjust to match sample
        idx = formatted.rfind(".")
        if idx != -1:
            sub = formatted[idx + 1:][:subsecond_digits].ljust(subsecond_digits, "0")
            formatted = formatted[:idx + 1] + sub

    return formatted


def random_filename(extensions: list[str]) -> tuple[str, str]:
    """Return (name_with_ext, extension) picking from the observed extension pool."""
    prefixes = [
        "kernel", "user32", "ntdll", "advapi", "msvcrt", "comctl",
        "gdi32", "ole32", "shell", "rpc", "crypt", "winsock", "netapi",
        "sechost", "coml2", "imm32", "wldap", "combase", "cfgmgr",
        "profapi", "setupapi", "shlwapi", "urlmon", "wininet", "ws2",
        "clbcatq", "dnsapi", "iphlpapi", "mswsock", "rasapi32", "winmm",
        "wintrust", "dbghelp", "psapi", "powrprof", "uxtheme",
    ]
    ext = random.choice(extensions) if extensions else ""
    if ext and ext != "nan":
        name = random.choice(prefixes) + str(random.randint(1, 99)) + ext
        return name, ext
    else:
        # No extension: bare registry/system names
        bare = random.choice(["SOFTWARE", "SYSTEM", "SECURITY", "SAM",
                               "NTUSER.DAT", "DRIVERS", "DEFAULT"])
        return bare, ""


def random_entry_number(low: int, high: int) -> int:
    return random.randint(low, high)


def random_update_reasons(pool: list[str]) -> str:
    """Pick a single reason or pipe-joined combination from the observed pool."""
    if not pool:
        return "Close"
    # Sometimes combine two if the sample shows pipe-joined values
    has_pipe = any("|" in r for r in pool)
    bases = list({r.strip() for combo in pool for r in combo.split("|")})
    if has_pipe and random.random() < 0.25 and len(bases) >= 2:
        chosen = random.sample(bases, 2)
        return "|".join(chosen)
    return random.choice(bases)


# ---------------------------------------------------------------------------
# Column analysis
# ---------------------------------------------------------------------------

def analyse_column(col_name: str, values: list[str]) -> dict:
    """
    Return a descriptor dict for a column so we know how to generate values.
    Keys: type, pool, date_fmt, sample_date, int_range, offset_step, pattern
    """
    non_null = [v for v in values if v and v.lower() not in ("nan", "none", "")]

    info: dict = {"type": "str", "pool": non_null or [""]}

    if not non_null:
        info["type"] = "null"
        return info

    # --- Date detection ---
    fmt = detect_date_format(non_null[:10])
    if fmt:
        parsed = []
        for v in non_null:
            try:
                parsed.append(datetime.strptime(v.strip()[:30], fmt))
            except ValueError:
                pass
        if parsed:
            info["type"] = "date"
            info["date_fmt"] = fmt
            info["date_sample"] = non_null[0]       # keep for digit-count reference
            info["date_base"] = parsed[len(parsed) // 2]  # median as base
            return info

    # --- Integer detection ---
    try:
        ints = [int(v) for v in non_null]
        info["type"] = "int"
        info["int_range"] = (min(ints), max(ints))
        # Detect monotone offset (e.g. OffsetToData increments by 80/88)
        if len(ints) > 2:
            diffs = [ints[i + 1] - ints[i] for i in range(len(ints) - 1)]
            if all(d > 0 for d in diffs):
                info["int_step"] = int(round(sum(diffs) / len(diffs)))
        return info
    except (ValueError, TypeError):
        pass

    # --- Categorical (small pool) ---
    unique = list(set(non_null))
    if len(unique) <= max(5, len(non_null) // 3):
        info["type"] = "categorical"
        info["pool"] = unique
        return info

    # --- Looks like a path ---
    if any("\\" in v or "/" in v for v in non_null):
        info["type"] = "path"
        info["pool"] = non_null
        return info

    info["type"] = "str"
    info["pool"] = non_null
    return info


# ---------------------------------------------------------------------------
# Row generation
# ---------------------------------------------------------------------------

def build_row_generator(headers: list[str], sample_rows: list[dict]) -> callable:
    """
    Return a closure that, when called with a row index, produces a dict
    of column→value for one synthetic row.
    """
    # Build column descriptors from the sample
    col_info: dict[str, dict] = {}
    for col in headers:
        vals = [row.get(col, "") for row in sample_rows]
        col_info[col] = analyse_column(col, vals)

    # Pre-extract useful pools for special columns
    reason_pool = col_info.get("UpdateReasons", {}).get("pool", ["Close"])
    attr_pool   = col_info.get("FileAttributes", {}).get("pool", ["Archive"])
    ext_pool = list({row.get("Extension", ".dll") for row in sample_rows
                     if row.get("Extension", "").strip()})
    if not ext_pool:
        ext_pool = [".dll", ".sys", ".exe"]

    # Source file template: keep the prefix, vary only the hash-like part
    source_samples = [r.get("SourceFile", "") for r in sample_rows if r.get("SourceFile")]
    source_template = source_samples[0] if source_samples else ""

    # Running state for sequential columns
    state = {
        "offset": col_info.get("OffsetToData", {}).get("int_range", (0, 512))[0],
        "usn":    col_info.get("UpdateSequenceNumber", {}).get("int_range", (0, 512))[0],
        "offset_step": col_info.get("OffsetToData", {}).get("int_step", 88),
        "usn_step":    col_info.get("UpdateSequenceNumber", {}).get("int_step", 88),
    }

    # Date base: use median date from the sample, random drift per row
    date_info = col_info.get("UpdateTimestamp", {})
    base_date: datetime = date_info.get("date_base", datetime(2025, 3, 19, 1, 24, 18))
    date_fmt: str = date_info.get("date_fmt", "%Y-%m-%d %H:%M:%S")
    date_sample_str: str = date_info.get("date_sample", "2025-03-19 01:24:18.9468760")

    def generate_row(row_idx: int) -> dict:
        name, ext = random_filename(ext_pool)
        row: dict = {}
        for col in headers:
            info = col_info[col]
            t = info["type"]

            if col == "Name":
                row[col] = name

            elif col == "Extension":
                row[col] = ext if ext else ""

            elif col == "EntryNumber":
                lo, hi = info.get("int_range", (1000, 200000))
                row[col] = random_entry_number(lo, hi)

            elif col == "SequenceNumber":
                row[col] = 1

            elif col == "ParentEntryNumber":
                lo, hi = info.get("int_range", (1000, 50000))
                row[col] = random_entry_number(lo, hi)

            elif col == "ParentSequenceNumber":
                row[col] = 1

            elif col == "ParentPath":
                # Was all-NaN in the sample
                row[col] = ""

            elif col == "UpdateSequenceNumber":
                row[col] = state["usn"]
                state["usn"] += state["usn_step"]

            elif col == "UpdateTimestamp":
                row[col] = random_date_str(base_date, date_fmt, date_sample_str)

            elif col == "UpdateReasons":
                row[col] = random_update_reasons(reason_pool)

            elif col == "FileAttributes":
                row[col] = random.choice(attr_pool) if attr_pool else "Archive"

            elif col == "OffsetToData":
                row[col] = state["offset"]
                state["offset"] += state["offset_step"]

            elif col == "SourceFile":
                row[col] = source_template   # identical path in the sample; keep it

            else:
                # Generic fallback: pick from sample pool or random string
                if t == "null":
                    row[col] = ""
                elif t == "int":
                    lo, hi = info.get("int_range", (0, 9999))
                    row[col] = random.randint(lo, hi)
                elif t in ("categorical", "path", "str"):
                    row[col] = random.choice(info["pool"])
                else:
                    row[col] = ""

        return row

    return generate_row


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) != 4:
        print(__doc__)
        sys.exit(1)

    input_path  = sys.argv[1]
    output_path = sys.argv[2]
    try:
        target_mb = float(sys.argv[3])
    except ValueError:
        print(f"Error: size_mb must be a number, got {sys.argv[3]!r}")
        sys.exit(1)

    target_bytes = int(target_mb * 1024 * 1024)

    # Read the sample
    with open(input_path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        headers = reader.fieldnames
        if not headers:
            print("Error: could not read headers from input CSV.")
            sys.exit(1)
        sample_rows = list(reader)

    if not sample_rows:
        print("Error: input CSV has no data rows.")
        sys.exit(1)

    print(f"Sample: {len(sample_rows)} rows, {len(headers)} columns")
    print(f"Target: {target_mb} MB  ({target_bytes:,} bytes)")
    print(f"Output: {output_path}")

    generate_row = build_row_generator(headers, sample_rows)

    written = 0
    row_count = 0
    CHUNK = 1000   # flush every N rows

    with open(output_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=headers)
        writer.writeheader()
        # Measure header size
        fh.flush()
        written = fh.tell() if hasattr(fh, "tell") else 0

        while True:
            rows = [generate_row(row_count + i) for i in range(CHUNK)]
            writer.writerows(rows)
            row_count += CHUNK

            if row_count % 10000 == 0:
                fh.flush()
                written = os.path.getsize(output_path)
                pct = written / target_bytes * 100
                print(f"  {row_count:>8,} rows  |  {written / 1024 / 1024:.2f} MB  ({pct:.1f}%)")

            if os.path.getsize(output_path) >= target_bytes:
                break

    final_size = os.path.getsize(output_path)
    print(f"\nDone. {row_count:,} rows written.")
    print(f"Final size: {final_size / 1024 / 1024:.3f} MB  ({final_size:,} bytes)")


if __name__ == "__main__":
    main()