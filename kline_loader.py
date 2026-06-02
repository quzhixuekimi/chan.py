"""
kline_loader.py
===============

策略模块统一的数据加载入口，读 PostgreSQL kline 表。

对外暴露的函数：
  - load_kline_df(code, level, start=None, end=None) -> pd.DataFrame
  - ensure_kline_data(codes, levels) -> list[LevelUpdateResult]
  - backfill_from_csv(codes, levels) -> list[LevelUpdateResult]

策略回测前通常这样用：
  ensure_kline_data(['AAPL', 'TSLA'], ['1D', '1H', '2H', '4H', '30M', '15M'])
  df = load_kline_df('AAPL', '1D')
"""
from __future__ import annotations

import pandas as pd

import kline_store
from kline_store import backfill_from_csv, ensure_levels_updated, read_kline_df


def load_kline_df(
  code: str,
  level: str,
  start: str | None = None,
  end: str | None = None,
) -> pd.DataFrame:
  """从 kline 表读取 K 线数据。

  返回 DataFrame，列: time(open/high/low/close/volume)，time 列为 datetime。
  若 DB 为空且 data_cache/ 下有 CSV，会自动回退到 CSV 读取（单向，不会写 DB）。

  Args:
    code: 股票代码 e.g. 'AAPL'
    level: 周期字符串 e.g. '1D' / '1H' / '2H' / '4H' / '30M' / '15M'（大小写不敏感）
    start: 起始时间字符串，e.g. '2020-01-01'，None 表示从头读
    end: 结束时间字符串，None 表示读到最新

  Returns:
    pd.DataFrame，time 升序，列为 time/open/high/low/close/volume
  """
  level_lower = level.lower()

  df = read_kline_df(code, level_lower, begin_date=start, end_date=end)
  if df is not None and not df.empty:
    return df

  import logging
  from pathlib import Path

  logger = logging.getLogger("kline_loader")
  data_cache = Path(__file__).resolve().parent / "data_cache"

  import glob

  def glob_csv(pat: str):
    matches = sorted(glob.glob(str(data_cache / pat)))
    return matches[0] if matches else None

  code_u = code.upper()
  if level_lower == "1d":
    csv_path = glob_csv(f"{code_u}_*_1d.csv")
  elif level_lower in ("4h", "2h", "1h"):
    csv_path = glob_csv(f"{code_u}_*_yf_{level_lower}_730d.csv")
  elif level_lower in ("30m", "15m"):
    csv_path = glob_csv(f"{code_u}_*_yf_{level_lower}_60d.csv")
  else:
    csv_path = None

  if not csv_path:
    raise FileNotFoundError(
      f"kline({level_lower}) 为空，data_cache/ 下也找不到对应 CSV: code={code}"
    )

  logger.warning(
    "[LOAD-FALLBACK] kline table empty, reading from CSV: %s", csv_path
  )
  df = pd.read_csv(csv_path, encoding="utf-8-sig")
  df.columns = [str(c).strip().lower() for c in df.columns]

  if "time" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"time": "dt"})
  if "date" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"date": "dt"})

  required = {"dt", "open", "high", "low", "close"}
  missing = required - set(df.columns)
  if missing:
    raise ValueError(f"csv 缺少列: {missing}, path={csv_path}")

  if "volume" not in df.columns:
    df["volume"] = 0.0

  df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
  df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

  for c in ["open", "high", "low", "close", "volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

  df = df.rename(columns={"dt": "time"})
  if start:
    df = df[df["time"] >= pd.to_datetime(start)]
  if end:
    df = df[df["time"] < pd.to_datetime(end)]

  return df.reset_index(drop=True)


# 别名：策略里调用的是 ensure_kline_data，实际就是 ensure_levels_updated
def ensure_kline_data(codes, levels):
    return kline_store.ensure_levels_updated(codes, levels)