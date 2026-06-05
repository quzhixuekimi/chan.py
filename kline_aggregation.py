"""
kline_aggregation.py
====================

Intraday bar end-time alignment and 2H / 4H aggregation helpers.

Originally lived in chan_api_server.py (as `_apply_intraday_bar_end_time` and
`aggregate_intraday`). Moved out to break the import cycle:

    chan_api_server -> kline_store -> chan_api_server (cycle)
    chan_api_server -> kline_store -> kline_aggregation  (no cycle)
"""
from __future__ import annotations

import pandas as pd


def _apply_intraday_bar_end_time(ts: pd.Timestamp, bar_minutes: int) -> pd.Timestamp:
  """将 K 线时间对齐到 bar 结束时刻（美股市场本地时间）。"""
  dt = pd.Timestamp(ts)
  base = dt.normalize()

  hhmm = dt.strftime("%H:%M")

  if bar_minutes == 60:
    mapping = {
      "09:30": "09:30",
      "10:30": "10:30",
      "11:30": "11:30",
      "12:30": "12:30",
      "13:30": "13:30",
      "14:30": "14:30",
      "15:30": "15:30",
    }
  elif bar_minutes == 120:
    mapping = {
      "09:30": "09:30",
      "11:30": "11:30",
      "13:30": "13:30",
      "15:30": "15:30",
    }
  elif bar_minutes == 240:
    mapping = {
      "09:30": "09:30",
      "13:30": "13:30",
    }
  elif bar_minutes == 30:
    mapping = {
      "09:30": "09:30",
      "10:00": "10:00",
      "10:30": "10:30",
      "11:00": "11:00",
      "11:30": "11:30",
      "12:00": "12:00",
      "12:30": "12:30",
      "13:00": "13:00",
      "13:30": "13:30",
      "14:00": "14:00",
      "14:30": "14:30",
      "15:00": "15:00",
      "15:30": "15:30",
    }
  elif bar_minutes == 15:
    mapping = {
      "09:30": "09:30",
      "09:45": "09:45",
      "10:00": "10:00",
      "10:15": "10:15",
      "10:30": "10:30",
      "10:45": "10:45",
      "11:00": "11:00",
      "11:15": "11:15",
      "11:30": "11:30",
      "11:45": "11:45",
      "12:00": "12:00",
      "12:15": "12:15",
      "12:30": "12:30",
      "12:45": "12:45",
      "13:00": "13:00",
      "13:15": "13:15",
      "13:30": "13:30",
      "13:45": "13:45",
      "14:00": "14:00",
      "14:15": "14:15",
      "14:30": "14:30",
      "14:45": "14:45",
      "15:00": "15:00",
      "15:15": "15:15",
      "15:30": "15:30",
      "15:45": "15:45",
    }
  else:
    raise ValueError(f"unsupported bar_minutes: {bar_minutes}")

  if hhmm not in mapping:
    raise ValueError(
      f"unexpected intraday timestamp {hhmm} for {bar_minutes}M aggregation"
    )

  end_hhmm = mapping[hhmm]
  end_hour, end_minute = map(int, end_hhmm.split(":"))
  return base + pd.Timedelta(hours=end_hour, minutes=end_minute)


def aggregate_intraday(df_1h: pd.DataFrame, hours: int) -> pd.DataFrame:
  """从 1H K线聚合出 2H 或 4H。返回的 time 列为 bar 结束时刻。"""
  df = df_1h.copy()
  df["time"] = pd.to_datetime(df["time"], errors="coerce")
  df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

  df["trade_date"] = df["time"].dt.date
  df["hhmm"] = df["time"].dt.strftime("%H:%M")

  if hours == 1:
    allowed = ["09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:30"]
    df = df[df["hhmm"].isin(allowed)].copy()
    df["bar_end_time"] = df["time"].apply(
      lambda x: _apply_intraday_bar_end_time(x, hours * 60)
    )
    out = df[["bar_end_time", "open", "high", "low", "close", "volume"]].copy()
    out = out.rename(columns={"bar_end_time": "time"})
    return out.sort_values("time").reset_index(drop=True)

  if hours == 2:
    bucket_map = {
      "09:30": 0,
      "10:30": 0,
      "11:30": 1,
      "12:30": 1,
      "13:30": 2,
      "14:30": 2,
      "15:30": 3,
    }
  elif hours == 4:
    bucket_map = {
      "09:30": 0,
      "10:30": 0,
      "11:30": 0,
      "12:30": 0,
      "13:30": 1,
      "14:30": 1,
      "15:30": 1,
    }
  else:
    raise ValueError(f"unsupported aggregate hours: {hours}")

  df = df[df["hhmm"].isin(bucket_map.keys())].copy()
  df["bucket"] = df["hhmm"].map(bucket_map)

  agg = (
    df.groupby(["trade_date", "bucket"], sort=True)
    .agg(
      time=("time", "first"),
      open=("open", "first"),
      high=("high", "max"),
      low=("low", "min"),
      close=("close", "last"),
      volume=("volume", "sum"),
    )
    .reset_index(drop=True)
    .sort_values("time")
    .reset_index(drop=True)
  )

  agg["time"] = agg["time"].apply(lambda x: _apply_intraday_bar_end_time(x, hours * 60))
  return agg[["time", "open", "high", "low", "close", "volume"]]


def aggregate_intraday_24x7(df_1h: pd.DataFrame, hours: int) -> pd.DataFrame:
  """从 1H K线聚合出 2H 或 4H（24×7 交易品种专用，如 BTC/ETH）。

  按连续 hours 小时分组，time 取组内第一条时间。
  支持 hours=2 或 hours=4。
  """
  if hours not in (2, 4):
    raise ValueError(f"unsupported 24x7 aggregate hours: {hours}")

  df = df_1h.copy()
  df["time"] = pd.to_datetime(df["time"], errors="coerce")
  df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

  df["trade_date"] = df["time"].dt.date
  df["hour"] = df["time"].dt.hour
  df["bucket"] = df["hour"] // hours

  agg = (
    df.groupby(["trade_date", "bucket"], sort=True)
    .agg(
      time=("time", "first"),
      open=("open", "first"),
      high=("high", "max"),
      low=("low", "min"),
      close=("close", "last"),
      volume=("volume", "sum"),
    )
    .reset_index(drop=True)
    .sort_values("time")
    .reset_index(drop=True)
  )

  return agg[["time", "open", "high", "low", "close", "volume"]]
