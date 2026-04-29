#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Aggregate per-strategy per-symbol digest CSVs into a per-symbol aggregated CSV.

Usage:
  python scripts/aggregate_signals.py --date 2026-04-26 --out results/aggregated
  python scripts/aggregate_signals.py --dry-run

This script is non-invasive: it only reads existing results CSVs and writes a single
aggregated CSV per date into results/aggregated/.
"""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
import hashlib
import csv

BASE_DIR = Path(__file__).resolve().parent.parent

STRATEGIES = [
  (
    "v5_macdtd",
    BASE_DIR
    / "user_strategy_v5_macdtd"
    / "results"
    / "market_signal_digest_last_per_symbol_v5_macdtd.csv",
  ),
  (
    "v6_bspzs",
    BASE_DIR
    / "user_strategy_v6_bspzs"
    / "results"
    / "market_trading_signal_digest_last_per_symbol_v6_bspzs.csv",
  ),
  (
    "v7_bi",
    BASE_DIR
    / "user_strategy_v7_bi"
    / "results"
    / "market_signal_digest_last_per_symbol_v7_bi.csv",
  ),
  (
    "v8_byma",
    BASE_DIR
    / "user_strategy_v8_byma"
    / "results"
    / "market_signal_digest_last_per_symbol_v8_byma.csv",
  ),
  (
    "v9_mr",
    BASE_DIR
    / "user_strategy_v9_mr"
    / "results"
    / "market_signal_digest_last_per_symbol_v9_mr.csv",
  ),
]


def sha256_text(s: str) -> str:
  return hashlib.sha256(s.encode("utf-8")).hexdigest()


def get_summary_text(row):
  for key in ["summary_text", "summarytext", "summaryText"]:
    if key in row and row.get(key):
      return str(row[key]).strip()
  return ""


def read_strategy_csv(path):
  if not path.exists():
    print(f"Warning: file not found: {path}")
    return {}
  out = {}
  with path.open("r", encoding="utf-8-sig") as fh:
    reader = csv.DictReader(fh)
    for r in reader:
      sym = r.get("symbol", "").strip() if r.get("symbol") else None
      if not sym:
        continue
      out[sym] = r
  return out


def main():
  p = argparse.ArgumentParser()
  p.add_argument("--date", default=None)
  p.add_argument("--out", default=str(BASE_DIR / "results" / "aggregated"))
  p.add_argument("--dry-run", action="store_true")
  args = p.parse_args()

  out_dir = Path(args.out)
  out_dir.mkdir(parents=True, exist_ok=True)

  strategy_rows = {}
  for sid, path in STRATEGIES:
    strategy_rows[sid] = read_strategy_csv(Path(path))

  # collect symbols
  symbols = set()
  for rows in strategy_rows.values():
    symbols.update(rows.keys())

  today = args.date or datetime.utcnow().strftime("%Y/%m/%d")
  generated_at = datetime.utcnow().isoformat(timespec="seconds") + "Z"

  out_rows = []
  for s in sorted(symbols):
    per = {}
    parts = []
    for sid, _ in STRATEGIES:
      row = strategy_rows.get(sid, {}).get(s, {})
      summary = get_summary_text(row)
      per[sid] = summary
      parts.append(f"{sid} : {summary}")
    aggregated = " | ".join(parts)
    fingerprint = sha256_text(aggregated)
    out_rows.append(
      {
        "symbol": s,
        "reference_date": today,
        "generated_at_iso": generated_at,
        "aggregated_summary_text": aggregated,
        "per_strategy_json": json.dumps(per, ensure_ascii=False),
        "aggregated_fingerprint": fingerprint,
      }
    )

  out_file = (
    out_dir / f"market_aggregated_signal_digest_bystock_{today.replace('/', '-')}.csv"
  )
  if args.dry_run:
    print("Dry-run: would write", out_file)
    for r in out_rows[:20]:
      print(r)
    print("... total symbols:", len(out_rows))
    return

  with out_file.open("w", encoding="utf-8", newline="") as fh:
    fieldnames = [
      "symbol",
      "reference_date",
      "generated_at_iso",
      "aggregated_summary_text",
      "per_strategy_json",
      "aggregated_fingerprint",
    ]
    writer = csv.DictWriter(fh, fieldnames=fieldnames)
    writer.writeheader()
    for r in out_rows:
      writer.writerow(r)

  print("Wrote aggregated file:", out_file)


if __name__ == "__main__":
  main()
