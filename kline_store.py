"""
kline_store.py
===============

Single source of truth for US-stock kline data, backed by PostgreSQL.

Responsibilities:
  1. Incremental daily update from yfinance (1D, 1H, 30M, 15M)
  2. Aggregate 2H / 4H from 1H (delete-then-rewrite last 7 days)
  3. Read DataFrame (used by CChan custom data sources)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Literal

import pandas as pd

import db
from db import engine
from kline_aggregation import (
  _apply_intraday_bar_end_time,
  aggregate_intraday,
  aggregate_intraday_24x7,
)

logger = logging.getLogger("kline_store")
logger.setLevel(logging.INFO)
kline_store_handler = logging.FileHandler("/tmp/chan_api_server.log", encoding="utf-8")
kline_store_handler.setFormatter(
  logging.Formatter("%(asctime)s %(levelname)s %(message)s")
)
logger.addHandler(kline_store_handler)


Level = Literal["1d", "1h", "2h", "4h", "30m", "15m"]

# 增量更新时相对当前时间的回溯天数
PULL_BACKFILL_DAYS: dict[str, int] = {
  "1d": 5,
  "1h": 5,
  "4h": 5,
  "30m": 3,
  "15m": 3,
}

# yfinance period / interval 映射
YFINANCE_INTERVAL: dict[str, str] = {
  "1d": "1d",
  "1h": "60m",
  "4h": "4h",
  "30m": "30m",
  "15m": "15m",
}

YFINANCE_PERIOD: dict[str, str] = {
  "1d": "20y",
  "1h": "730d",
  "4h": "730d",
  "30m": "60d",
  "15m": "60d",
}

# 2H/4H 重聚合窗口（覆盖最近 N 天，超出窗口的历史 2H/4H 不动）
REAGG_WINDOW_DAYS = 5


@dataclass
class LevelUpdateResult:
  code: str
  level: str
  fetched: int = 0  # 拉取的行数
  upserted: int = 0  # 实际写入的行数
  latest_time: datetime | None = None
  success: bool = True
  error: str | None = None


def _is_crypto(code: str) -> bool:
  return code.upper() in ("BTC-USD", "ETH-USD")


def _fetch_yfinance(code: str, level: Level, start: str | None) -> pd.DataFrame:
  """从 yfinance 拉取指定 level 的 K线。start=None 表示走 period 全量。"""
  import yfinance as yf

  ticker = yf.Ticker(code.upper())

  kwargs: dict = dict(
    interval=YFINANCE_INTERVAL[level],
    auto_adjust=False,
    actions=False,
  )
  if start:
    kwargs["start"] = start
  else:
    kwargs["period"] = YFINANCE_PERIOD[level]

  df = ticker.history(**kwargs)
  if df is None or df.empty:
    raise ValueError(f"yfinance returns empty data for {code} {level}")

  df = df.reset_index()
  time_col = "Date" if "Date" in df.columns else "Datetime"
  df = df.rename(
    columns={
      time_col: "time",
      "Open": "open",
      "High": "high",
      "Low": "low",
      "Close": "close",
      "Volume": "volume",
    }
  )
  df = df[["time", "open", "high", "low", "close", "volume"]].copy()
  df["time"] = pd.to_datetime(df["time"], errors="coerce")
  df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
  if getattr(df["time"].dt, "tz", None) is not None:
    df["time"] = df["time"].dt.tz_localize(None)
  return df


def _fetch_btc_eth_yfinance(code: str, level: Level, start: str | None) -> pd.DataFrame:
  """从 yfinance 拉取 BTC/ETH 指定 level 的 K线。BTC/ETH 支持 7x24，交易数据齐全。"""
  import yfinance as yf

  ticker = yf.Ticker(code.upper())

  kwargs: dict = dict(
    interval=YFINANCE_INTERVAL[level],
    auto_adjust=False,
    actions=False,
  )
  if start:
    kwargs["start"] = start
  else:
    kwargs["period"] = YFINANCE_PERIOD[level]

  df = ticker.history(**kwargs)
  if df is None or df.empty:
    raise ValueError(f"yfinance returns empty data for {code} {level}")

  df = df.reset_index()
  time_col = "Date" if "Date" in df.columns else "Datetime"
  df = df.rename(
    columns={
      time_col: "time",
      "Open": "open",
      "High": "high",
      "Low": "low",
      "Close": "close",
      "Volume": "volume",
    }
  )
  df = df[["time", "open", "high", "low", "close", "volume"]].copy()
  df["time"] = pd.to_datetime(df["time"], errors="coerce")
  df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
  if getattr(df["time"].dt, "tz", None) is not None:
    df["time"] = df["time"].dt.tz_localize(None)
  return df


def _align_intraday_end_time(df: pd.DataFrame, level: Level) -> pd.DataFrame:
  """把 yfinance 返回的 intraday K线时间对齐到 bar 结束时刻。"""
  bar_minutes = {"1h": 60, "30m": 30, "15m": 15}[level]
  out = df.copy()
  out["time"] = out["time"].apply(
    lambda x: _apply_intraday_bar_end_time(x, bar_minutes)
  )
  return out[["time", "open", "high", "low", "close", "volume"]]


def _df_to_rows(df: pd.DataFrame) -> list[dict]:
  return [
    {
      "time": pd.Timestamp(r["time"]).to_pydatetime(),
      "open": float(r["open"]),
      "high": float(r["high"]),
      "low": float(r["low"]),
      "close": float(r["close"]),
      "volume": int(r.get("volume") or 0),
    }
    for _, r in df.iterrows()
  ]


def _update_source_level(
  code: str, level: Literal["1d", "1h", "30m", "15m"]
) -> LevelUpdateResult:
  """拉取并 UPSERT 一个 source level (1D/1H/30M/15M)。

  - 首次（DB 中无数据）：全量拉取（period=20y/730d/60d）
  - 非首次：start = latest_time - backfill_days，捕获 yfinance 复权修正
  """
  logger.info("[UPDATE] code=%s level=%s start=_update_source_level", code, level)
  with engine.begin() as conn:
    latest = db.get_latest_kline_time(conn, code, level)

  if latest is None:
    start = None
    logger.info("[UPDATE] code=%s level=%s mode=full (no data in db)", code, level)
  else:
    backfill = PULL_BACKFILL_DAYS[level]
    start = (latest - timedelta(days=backfill)).strftime("%Y-%m-%d")
    logger.info(
      "[UPDATE] code=%s level=%s mode=incremental start=%s latest=%s",
      code,
      level,
      start,
      latest,
    )

  try:
    if _is_crypto(code):
      df = _fetch_btc_eth_yfinance(code, level, start=start)
    else:
      df = _fetch_yfinance(code, level, start=start)
    logger.info("[UPDATE] code=%s level=%s fetch_ok rows=%s", code, level, len(df))
  except Exception as e:
    logger.exception("[UPDATE] code=%s level=%s fetch_failed: %s", code, level, e)
    return LevelUpdateResult(code=code, level=level, success=False, error=str(e))

  if df.empty:
    logger.info("[UPDATE] code=%s level=%s empty dataframe, skipping", code, level)
    return LevelUpdateResult(code=code, level=level, latest_time=latest)

  if level in ("1h", "30m", "15m"):
    if not _is_crypto(code):
      df = _align_intraday_end_time(df, level)
  else:
    df = df[["time", "open", "high", "low", "close", "volume"]].copy()

  rows = _df_to_rows(df)
  with engine.begin() as conn:
    written = db.upsert_kline(conn, code, level, rows)
    new_latest = db.get_latest_kline_time(conn, code, level)

  logger.info(
    "[UPDATE] code=%s level=%s fetched=%s upserted=%s latest=%s",
    code,
    level,
    len(df),
    written,
    new_latest,
  )
  return LevelUpdateResult(
    code=code,
    level=level,
    fetched=len(df),
    upserted=written,
    latest_time=new_latest,
  )


def _reaggregate_from_1h(code: str, target: Literal["2h", "4h"]) -> LevelUpdateResult:
  """从 1H 重新聚合 2H 或 4H。

  策略：删除 (code, target) 在 [now - REAGG_WINDOW_DAYS, ∞) 的所有 bar，
  然后从 1H 完整重算后 UPSERT 回去。窗口外的历史不动。
  """
  logger.info("[REAGG] code=%s level=%s start - will delete and re-aggregate from 1h", code, target)
  cutoff = datetime.now() - timedelta(days=REAGG_WINDOW_DAYS)

  with engine.begin() as conn:
    db.delete_kline_range(conn, code, target, begin_date=cutoff)
    df_1h = db.read_kline(conn, code, "1h")

  logger.info("[REAGG] code=%s level=%s read 1h rows=%s", code, target, len(df_1h))

  if df_1h.empty:
    logger.warning("[REAGG] code=%s level=%s skipped - no 1h data in db", code, target)
    return LevelUpdateResult(code=code, level=target)

  hours = 2 if target == "2h" else 4
  agg_fn = aggregate_intraday_24x7 if _is_crypto(code) else aggregate_intraday
  df_agg = agg_fn(df_1h, hours)
  logger.info("[REAGG] code=%s level=%s aggregated to %s rows (hours=%s, fn=%s)", code, target, len(df_agg), hours, agg_fn.__name__)
  if df_agg.empty:
    return LevelUpdateResult(code=code, level=target)

  df_agg["time"] = pd.to_datetime(df_agg["time"])
  df_agg_new = df_agg[df_agg["time"] >= pd.Timestamp(cutoff)].copy()
  if df_agg_new.empty:
    return LevelUpdateResult(code=code, level=target)

  rows = _df_to_rows(df_agg_new)
  with engine.begin() as conn:
    written = db.upsert_kline(conn, code, target, rows)
    new_latest = db.get_latest_kline_time(conn, code, target)

  logger.info(
    "[REAGG] code=%s level=%s source_1h_rows=%s agg_rows=%s upserted=%s latest=%s",
    code,
    target,
    len(df_1h),
    len(df_agg_new),
    written,
    new_latest,
  )
  return LevelUpdateResult(
    code=code,
    level=target,
    fetched=len(df_1h),
    upserted=written,
    latest_time=new_latest,
  )


def ensure_levels_updated(
  codes: list[str], levels: list[str]
) -> list[LevelUpdateResult]:
  """确保 (code, level) 在 DB 中是最新数据。

  - source levels (1d/1h/30m/15m): 首次全量，之后增量（带回溯缓冲）
  - derived levels (2h/4h): 从 1H 重聚合最近 7 天

  Args:
    codes: 股票代码列表
    levels: 周期字符串列表（大小写不敏感，e.g. ["1D","1H"]）

  Returns:
    每个 (code, level) 一个 LevelUpdateResult；失败也返回，success=False
  """
  lvl_set = {l.lower() for l in levels}
  source_levels = [l for l in lvl_set if l in ("1d", "1h", "30m", "15m")]
  derived_levels = [l for l in lvl_set if l in ("2h", "4h")]

  for code in codes:
    is_crypto = _is_crypto(code)
    if is_crypto:
      if "4h" in derived_levels:
        derived_levels = [l for l in derived_levels if l != "4h"]
        if "4h" not in source_levels:
          source_levels = list(source_levels) + ["4h"]

  # 1H 必须在 2H/4H 之前更新
  needs_1h = ("1h" in source_levels) or bool(derived_levels)

  results: list[LevelUpdateResult] = []
  for code in codes:
    if needs_1h:
      try:
        results.append(_update_source_level(code, "1h"))
      except Exception as e:
        logger.exception("[UPDATE] code=%s level=1h failed: %s", code, e)
        results.append(LevelUpdateResult(code, "1h", success=False, error=str(e)))

    for level in source_levels:
      if level == "1h":
        continue
      try:
        results.append(_update_source_level(code, level))
      except Exception as e:
        logger.exception("[UPDATE] code=%s level=%s failed: %s", code, level, e)
        results.append(LevelUpdateResult(code, level, success=False, error=str(e)))

    for level in derived_levels:
      try:
        results.append(_reaggregate_from_1h(code, level))
      except Exception as e:
        logger.exception("[REAGG] code=%s level=%s failed: %s", code, level, e)
        results.append(LevelUpdateResult(code, level, success=False, error=str(e)))

  return results


def read_kline_df(
  code: str, level: str, begin_date=None, end_date=None
) -> pd.DataFrame:
  """供 CChan custom data source 使用的读取入口（按 time 升序）。"""
  with engine.connect() as conn:
    return db.read_kline(
      conn,
      code,
      level.lower(),
      begin_date=begin_date,
      end_date=end_date,
    )


# ---------------------------------------------------------------------------
# CSV backfill：把 data_cache/ 下的旧 CSV 一次性导入 DB，避免冷启动再拉 yfinance
# ---------------------------------------------------------------------------
DATA_CACHE_DIR = Path(__file__).resolve().parent / "data_cache"


def _glob_csv_for(code: str, level_lower: str) -> str | None:
  """在 data_cache/ 下找一份匹配的 CSV（按 glob 忽略日期部分）。"""
  import glob

  code_u = code.upper()
  if level_lower == "1d":
    pattern = str(DATA_CACHE_DIR / f"{code_u}_*_1d.csv")
  else:
    days_suffix = "60d" if level_lower in ("30m", "15m") else "730d"
    pattern = str(DATA_CACHE_DIR / f"{code_u}_*_yf_{level_lower}_{days_suffix}.csv")
  matches = sorted(glob.glob(pattern))
  return matches[0] if matches else None


def _read_csv_to_df(csv_path: str) -> pd.DataFrame:
  import pandas as pd

  df = pd.read_csv(csv_path)
  df.columns = [str(c).strip().lower() for c in df.columns]
  if "time" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"time": "dt"})
  if "date" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"date": "dt"})
  required = {"dt", "open", "high", "low", "close"}
  missing = required - set(df.columns)
  if missing:
    raise ValueError(f"csv missing columns {missing}, path={csv_path}")
  if "volume" not in df.columns:
    df["volume"] = 0.0
  df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
  df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)
  for c in ["open", "high", "low", "close", "volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")
  df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
  return df


def backfill_from_csv(codes: list[str], levels: list[str]) -> list[LevelUpdateResult]:
  """把 data_cache/ 下的旧 CSV 一次性导入到 kline 表。

  - 仅在该 (code, level) DB 中**完全为空**时写入（不会用旧 CSV 覆盖已有较新数据）
  - 找不到对应 CSV 的组合会跳过，留给 ensure_levels_updated 兜底
  - 2H/4H CSV 是预先聚合好的，直接 UPSERT 即可；之后 ensure_levels_updated
    会按 1H 重新聚合最近 7 天并覆盖

  Returns: 每个 (code, level) 一条 LevelUpdateResult（skipped / failed / imported）
  """
  lvl_set = {l.lower() for l in levels}
  results: list[LevelUpdateResult] = []

  for code in codes:
    for level in lvl_set:
      try:
        csv_path = _glob_csv_for(code, level)
        if not csv_path:
          results.append(
            LevelUpdateResult(
              code=code,
              level=level,
              success=True,
              error="csv not found, skipped",
            )
          )
          continue

        with engine.begin() as conn:
          existing = db.get_latest_kline_time(conn, code, level)
        if existing is not None:
          results.append(
            LevelUpdateResult(
              code=code,
              level=level,
              latest_time=existing,
              success=True,
              error="already in db, skipped",
            )
          )
          continue

        df = _read_csv_to_df(csv_path)
        df = df.rename(columns={"dt": "time"})
        rows = _df_to_rows(df)
        with engine.begin() as conn:
          written = db.upsert_kline(conn, code, level, rows)
          new_latest = db.get_latest_kline_time(conn, code, level)

        logger.info(
          "[BACKFILL] code=%s level=%s csv=%s rows=%s upserted=%s latest=%s",
          code,
          level,
          csv_path,
          len(df),
          written,
          new_latest,
        )
        results.append(
          LevelUpdateResult(
            code=code,
            level=level,
            fetched=len(df),
            upserted=written,
            latest_time=new_latest,
            success=True,
          )
        )
      except Exception as e:
        logger.exception("[BACKFILL] code=%s level=%s failed: %s", code, level, e)
        results.append(
          LevelUpdateResult(
            code=code,
            level=level,
            success=False,
            error=str(e),
          )
        )

  return results
