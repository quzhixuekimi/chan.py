#!/usr/bin/env python3
"""Standalone script to import CSV files from /root/tools/chan.py/data_cache/ into
PostgreSQL table `kline`.

Features:
- No dependency on the repo's internal modules (kline_store, db.py, etc.)
- Uses only standard library + psycopg2 (and pandas if available for easier CSV parsing).
- Handles daily (1d) and intraday (1h,2h,4h,15m,30m) CSV files.
- Performs UPSERT on the unique constraint (code, level, time).
- Logs progress and errors.

Prerequisites on the remote host:
    pip install psycopg2-binary   # required
    pip install pandas            # optional (makes CSV parsing robust)
Set the environment variable DATABASE_URL to a PostgreSQL connection string
    export DATABASE_URL="postgresql://user:pwd@host:5432/dbname"

Run:
    python scripts/import_kline_standalone.py
"""

import os
import sys
import logging
import csv
from pathlib import Path
from datetime import datetime
from typing import List, Tuple

# ---------------------------------------------------------------------------
# 1️⃣ Get DATABASE_URL from environment
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    sys.stderr.write("ERROR: DATABASE_URL environment variable not set\n")
    sys.exit(1)

# ---------------------------------------------------------------------------
# 2️⃣ Import required third‑party libraries
# ---------------------------------------------------------------------------
try:
    import psycopg2
    from psycopg2.extras import execute_values
except ImportError:
    sys.stderr.write(
        "ERROR: psycopg2 is required. Install with `pip install psycopg2-binary`\n"
    )
    sys.exit(1)

# pandas is optional – it makes CSV parsing easier and handles many edge cases.
try:
    import pandas as pd
    _HAS_PANDAS = True
except Exception:
    _HAS_PANDAS = False

# ---------------------------------------------------------------------------
# 3️⃣ Helpers: filename → (code, level)
# ---------------------------------------------------------------------------
def parse_filename(fp: Path) -> Tuple[str, str]:
    """Extract stock code and level from a CSV filename.

    Supported patterns (examples):
        AAPL_2026-06-04_1d.csv
        AAPL_2026-06-04_yf_15m_60d.csv
        TSLA_2026-06-04_yf_2h_730d.csv
    Returns: (code, level) where level is lower‑case.
    """
    stem = fp.stem
    parts = stem.split("_")
    code = parts[0].upper()
    if len(parts) >= 4 and parts[2] == "yf":
        level = parts[3].lower()
    else:
        level = parts[-1].lower()
    return code, level

# ---------------------------------------------------------------------------
# 4️⃣ CSV → list of rows ready for DB insertion
# ---------------------------------------------------------------------------
def read_csv_rows(csv_path: Path) -> List[Tuple[datetime, float, float, float, float, int]]:
    """Return rows as (time, open, high, low, close, volume).
    If pandas is available it is used; otherwise a pure‑csv implementation is used.
    """
    rows: List[Tuple[datetime, float, float, float, float, int]] = []
    if _HAS_PANDAS:
        df = pd.read_csv(csv_path)
        df.columns = [c.strip().lower() for c in df.columns]
        if "time" in df.columns:
            df = df.rename(columns={"time": "dt"})
        elif "date" in df.columns:
            df = df.rename(columns={"date": "dt"})
        required = {"dt", "open", "high", "low", "close"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"CSV 缺少列 {missing}: {csv_path}")
        if "volume" not in df.columns:
            df["volume"] = 0
        df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
        df = df.dropna(subset=["dt"]).sort_values("dt")
        for _, row in df.iterrows():
            rows.append(
                (
                    row["dt"].to_pydatetime(),
                    float(row["open"]),
                    float(row["high"]),
                    float(row["low"]),
                    float(row["close"]),
                    int(row["volume"]),
                )
            )
        return rows

    # ---------- fallback using csv module ----------
    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        # Normalize header names
        header_map = {name.strip().lower(): name for name in reader.fieldnames or []}
        # Resolve time column name (time/date/dt)
        time_key = next(
            (k for k in ("time", "date", "dt") if k in header_map), None
        )
        if not time_key:
            raise ValueError(f"CSV 无法定位时间列: {csv_path}")
        # Ensure required numeric columns exist
        for col in ("open", "high", "low", "close"):
            if col not in header_map:
                raise ValueError(f"CSV 缺少列 {col}: {csv_path}")
        volume_key = header_map.get("volume", None)
        for raw in reader:
            dt_raw = raw[header_map[time_key]]
            try:
                dt = datetime.fromisoformat(dt_raw)
            except Exception:
                dt = datetime.strptime(dt_raw, "%Y-%m-%d %H:%M:%S")
            open_ = float(raw[header_map["open"]])
            high = float(raw[header_map["high"]])
            low = float(raw[header_map["low"]])
            close = float(raw[header_map["close"]])
            vol = int(float(raw[header_map["volume"]])) if volume_key else 0
            rows.append((dt, open_, high, low, close, vol))
    rows.sort(key=lambda x: x[0])
    return rows

# ---------------------------------------------------------------------------
# 5️⃣ UPSERT SQL (PostgreSQL supports ON CONFLICT)
# ---------------------------------------------------------------------------
UPSERT_SQL = """
INSERT INTO kline (code, level, "time", open, high, low, close, volume)
VALUES %s
ON CONFLICT (code, level, "time") DO UPDATE
SET open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume;
"""

# ---------------------------------------------------------------------------
# 6️⃣ Import a single CSV file
# ---------------------------------------------------------------------------
def import_one_file(conn, code: str, level: str, csv_path: Path) -> None:
    raw_rows = read_csv_rows(csv_path)
    if not raw_rows:
        logging.info("%s 空文件, 跳过", csv_path.name)
        return
    # 为每条记录添加 code、level，用于 UPSERT
    rows = [(code, level, r[0], r[1], r[2], r[3], r[4], r[5]) for r in raw_rows]
    with conn.cursor() as cur:
        execute_values(cur, UPSERT_SQL, rows, page_size=500)
    conn.commit()
    logging.info("导入成功: code=%s level=%s file=%s rows=%d", code, level, csv_path.name, len(rows))

# ---------------------------------------------------------------------------
# 7️⃣ Main driver
# ---------------------------------------------------------------------------
def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Remote data directory – adjust if different
    data_dir = Path("/root/tools/chan.py/data_cache")
    if not data_dir.is_dir():
        logging.error("CSV 目录不存在: %s", data_dir)
        sys.exit(1)

    # Build a mapping of (code, level) -> list of CSV paths (normally one per combo)
    file_map: dict[Tuple[str, str], List[Path]] = {}
    for p in data_dir.glob("*.csv"):
        code, level = parse_filename(p)
        file_map.setdefault((code, level), []).append(p)

    # Open a single DB connection for the whole run
    conn = psycopg2.connect(DATABASE_URL)

    for (code, level), paths in file_map.items():
        # Sort paths by filename to keep chronological order if multiple files exist
        for csv_path in sorted(paths):
            try:
                import_one_file(conn, code, level, csv_path)
            except Exception as e:
                logging.exception(
                    "导入失败: code=%s level=%s file=%s error=%s",
                    code,
                    level,
                    csv_path.name,
                    e,
                )
    conn.close()
    logging.info("所有 CSV 已处理完毕")


if __name__ == "__main__":
    main()
