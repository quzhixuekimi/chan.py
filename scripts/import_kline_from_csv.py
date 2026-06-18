#!/usr/bin/env python3
"""Import CSV kline data from the `data_cache` directory into the PostgreSQL `kline` table.

The repository already provides `kline_store.backfill_from_csv` which safely upserts
CSV rows only when the corresponding (code, level) is empty in the database.
This script discovers all available stock codes and time‑level combinations in
`data_cache/` and invokes that helper.
"""

import logging
import sys
from pathlib import Path

# Ensure the project root is on the module search path.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from kline_store import backfill_from_csv


def _discover_codes_levels(cache_dir: Path) -> tuple[set[str], set[str]]:
    """Return unique stock codes and level strings (lower‑case) found in CSV files.

    Filenames follow two patterns:
    * ``{CODE}_*_1d.csv``                – daily data
    * ``{CODE}_*_yf_{LEVEL}_{DAYS}d.csv`` – intraday data where ``LEVEL`` is e.g.
      ``15m``, ``30m``, ``1h``, ``2h``, ``4h``.
    """
    codes: set[str] = set()
    levels: set[str] = set()
    for csv_path in cache_dir.glob("*.csv"):
        stem = csv_path.stem  # filename without extension
        parts = stem.split("_")
        if len(parts) < 3:
            continue
        code = parts[0].upper()
        # Intraday pattern includes a ``yf`` token.
        if parts[2] == "yf" and len(parts) >= 4:
            level = parts[3].lower()
        else:
            # Daily files end with ``1d``.
            level = parts[-1].lower()
        codes.add(code)
        levels.add(level)
    return codes, levels


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
    data_cache_dir = PROJECT_ROOT / "data_cache"
    if not data_cache_dir.is_dir():
        logging.error("data_cache directory not found: %s", data_cache_dir)
        sys.exit(1)

    codes, levels = _discover_codes_levels(data_cache_dir)
    if not codes:
        logging.info("No CSV files detected in %s", data_cache_dir)
        return

    logging.info("Discovered %d codes and %d levels", len(codes), len(levels))
    results = backfill_from_csv(list(codes), list(levels))
    for r in results:
        status = "OK" if r.success else "FAIL"
        msg = r.error or ""
        logging.info("%s %s %s %s", r.code, r.level, status, msg)


if __name__ == "__main__":
    main()
