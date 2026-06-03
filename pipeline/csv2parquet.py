#!/usr/bin/env python3
"""
csv2parquet.py — Converts raw CSV data lake → partitioned Parquet
Runs as a daemon, polls RAW_PATH, writes to PARQUET_PATH.
Uses DuckDB for in-process columnar conversion (no Spark needed).
"""

import os
import time
import logging
import hashlib
import duckdb
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [csv2parquet] %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

RAW_PATH    = Path(os.environ.get("RAW_PATH",    "/data/raw"))
PARQUET_PATH= Path(os.environ.get("PARQUET_PATH","/data/parquet"))
POLL        = int(os.environ.get("POLL_INTERVAL","300"))
PROCESSED   = set()  # checksum cache to avoid reprocessing


def checksum(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def detect_file_type(path: Path) -> str:
    n = path.name
    if n == "master_energy_database.csv":    return "master"
    if n == "pipeline_summary.csv":          return "pipeline"
    if n.startswith("history_"):             return "granularity"
    return "unknown"


def infer_site_arch(path: Path) -> tuple[str, str]:
    """Infer site from path: /data/raw/<site>/..."""
    parts = path.parts
    try:
        raw_idx = parts.index("raw")
        site = parts[raw_idx + 1]
    except (ValueError, IndexError):
        site = "unknown"
    # Arch map — extend as needed
    arch_map = {
        "rennes": "x86_64", "lille": "x86_64",
        "nancy": "x86_64",  "lyon": "x86_64",
        "grenoble": "x86_64",
    }
    return site, arch_map.get(site, "x86_64")


def convert_csv(csv_path: Path, con: duckdb.DuckDBPyConnection):
    file_type = detect_file_type(csv_path)
    if file_type == "unknown":
        return

    site, arch = infer_site_arch(csv_path)
    cs = checksum(csv_path)
    key = f"{csv_path}:{cs}"
    if key in PROCESSED:
        return

    try:
        # Read CSV with DuckDB (handles malformed rows gracefully)
        rel = con.read_csv(str(csv_path), ignore_errors=True)
        df  = rel.df()
    except Exception as e:
        log.warning(f"skip {csv_path}: {e}")
        return

    if df.empty:
        return

    # Add metadata columns
    df["site"]          = site
    df["arch"]          = arch
    df["file_type"]     = file_type
    df["source_file"]   = str(csv_path)
    df["converted_at"]  = datetime.utcnow().isoformat()

    # Infer measurement_src
    if "measurement" in df.columns or "measurement_source" in df.columns:
        col = "measurement" if "measurement" in df.columns else "measurement_source"
        df["measurement_src"] = df[col].str.lower().map(
            lambda x: "ebpf" if x in ("ebpf","bpf") else ("process" if x in ("process","proc") else "unknown")
        )
    else:
        df["measurement_src"] = "unknown"

    # Parse date
    if "date" in df.columns:
        df["date"] = df["date"].astype(str).str.replace("_", " ", regex=False)
        df["date"] = df["date"].str[:19]  # trim to seconds
        try:
            import pandas as pd
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
            df["year"]  = df["date"].dt.year.astype("Int32")
            df["month"] = df["date"].dt.month.astype("Int32")
        except Exception:
            df["year"] = df["month"] = None

    # Extract job/component from granularity filename
    if file_type == "granularity":
        base = csv_path.stem  # history_docker-build_cpu
        rest = base[len("history_"):]
        idx  = rest.rfind("_")
        df["job_name"]  = rest[:idx] if idx >= 0 else rest
        df["component"] = rest[idx+1:] if idx >= 0 else "unknown"

    # Parquet partition: /data/parquet/<file_type>/site=<site>/year=<year>/month=<month>/
    out_dir = PARQUET_PATH / file_type / f"site={site}"
    if "year" in df.columns and df["year"].notna().any():
        yr = int(df["year"].dropna().iloc[0])
        mo = int(df["month"].dropna().iloc[0])
        out_dir = out_dir / f"year={yr}" / f"month={mo:02d}"

    out_dir.mkdir(parents=True, exist_ok=True)
    stem = csv_path.stem.replace(" ", "_")
    out_file = out_dir / f"{stem}_{site}.parquet"

    con.execute(f"COPY (SELECT * FROM df) TO '{out_file}' (FORMAT PARQUET, COMPRESSION ZSTD)")
    log.info(f"✅ {csv_path.name} → {out_file.relative_to(PARQUET_PATH)} ({len(df)} rows)")
    PROCESSED.add(key)


def run_cycle(con: duckdb.DuckDBPyConnection):
    csv_files = list(RAW_PATH.rglob("*.csv"))
    log.info(f"🔄 Found {len(csv_files)} CSV files")
    for f in csv_files:
        if f.suffix == ".lock":
            continue
        try:
            convert_csv(f, con)
        except Exception as e:
            log.error(f"❌ {f}: {e}")


def main():
    PARQUET_PATH.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()  # in-memory, no persistence needed
    log.info(f"Starting csv2parquet — raw={RAW_PATH} parquet={PARQUET_PATH} poll={POLL}s")
    while True:
        run_cycle(con)
        time.sleep(POLL)


if __name__ == "__main__":
    main()
