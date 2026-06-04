#!/usr/bin/env python3
"""Resample 1‑hour K‑line CSV files to 2‑hour and 4‑hour intervals.

Scans a directory (default ./data_cache) for files matching the project's
1h naming convention, creates corresponding 2h and 4h CSVs using the same
OHLCV aggregation rules as the DB layer, and writes them with the proper
filenames so that `import_kline_standalone.py` can later ingest all files in
one pass.
"""

import argparse
import re
from pathlib import Path
import pandas as pd

def resample_df(df: pd.DataFrame, interval: str) -> pd.DataFrame:
    """Resample OHLCV DataFrame to ``interval`` (e.g. '2H' or '4H')."""
    df = df.copy()
    df['time'] = pd.to_datetime(df['time'])
    df = df.set_index('time')
    agg = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum',
    }
    resampled = df.resample(interval).apply(agg).dropna()
    return resampled.reset_index()

def generate_higher_intervals(data_dir: Path, allowed_codes: set) -> None:
    pattern = re.compile(r'(?P<code>[^_]+)_(?P<date>\d{4}-\d{2}-\d{2})_yf_1h_730d\.csv')
    for csv_path in data_dir.glob('*_1h_*.csv'):
        m = pattern.fullmatch(csv_path.name)
        if not m:
            continue
        code, date = m.group('code'), m.group('date')
        if code not in allowed_codes:
            continue
        print(f'Processing {csv_path.name} (code={code}, date={date})')
        try:
            df_1h = pd.read_csv(csv_path)
        except Exception as e:
            print(f'  Error reading {csv_path.name}: {e}')
            continue
        for hours, suffix in [(2, '2h'), (4, '4h')]:
            interval = f'{hours}h'
            try:
                df_res = resample_df(df_1h, interval)
            except Exception as e:
                print(f'  Resample to {suffix} failed: {e}')
                continue
            out_name = f'{code}_{date}_yf_{suffix}_730d.csv'
            out_path = data_dir / out_name
            try:
                df_res.to_csv(out_path, index=False)
                print(f'  Wrote {out_name} ({len(df_res)} rows)')
            except Exception as e:
                print(f'  Error writing {out_name}: {e}')

def main() -> None:
    parser = argparse.ArgumentParser(description='Generate 2h/4h CSVs from existing 1‑hour K‑line files.')
    parser.add_argument('--data-dir', type=Path, default=Path('./data_cache'), help='Directory containing 1‑hour CSV files')
    parser.add_argument('codes', nargs='+', help='One or more stock codes to process (e.g. AAPL MSFT)')
    args = parser.parse_args()
    if not args.data_dir.is_dir():
        raise SystemExit(f'Directory {args.data_dir} does not exist')
    # Convert list of codes to a set for fast membership tests
    code_set = set(args.codes)
    # Pass the set to the generation function
    generate_higher_intervals(args.data_dir, code_set)


if __name__ == '__main__':
    main()
