from __future__ import annotations

from typing import Literal

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from datetime import date
import logging
from db import engine, get_cached, set_cached, delete_old_cache_for_code

import kline_store

router = APIRouter()

LevelType = Literal["1D", "1H", "2H", "4H", "30M", "15M"]


class IndicatorsRequest(BaseModel):
  code: str = Field(..., description="美股代码，例如 TSLA")
  level: LevelType = Field(..., description="级别：1D / 1H / 2H / 4H")


class IndicatorLinePoint(BaseModel):
  time: str
  value: float | None


class TD9Label(BaseModel):
  time: str
  price: float | None
  text: str
  position: Literal["above", "below"]
  color: str


class IndicatorsData(BaseModel):
  symbol: str
  level: str
  cache_file: str

  blue_upper: list[IndicatorLinePoint] = []
  blue_lower: list[IndicatorLinePoint] = []
  yellow_upper: list[IndicatorLinePoint] = []
  yellow_lower: list[IndicatorLinePoint] = []

  ma55: list[IndicatorLinePoint] = []
  ma60: list[IndicatorLinePoint] = []
  ma65: list[IndicatorLinePoint] = []
  ma120: list[IndicatorLinePoint] = []
  ma250: list[IndicatorLinePoint] = []

  td9_labels: list[TD9Label] = []


class IndicatorsResponse(BaseModel):
  code: int
  message: str
  data: IndicatorsData


def _load_db_df(code: str, level: LevelType) -> tuple[pd.DataFrame, str]:
  """从 kline 表读取 (code, level) 数据，返回的 df 含 dt 列（与原 csv reader 兼容）。"""
  df = kline_store.read_kline_df(code, level.lower())
  if df is None or df.empty:
    raise ValueError(f"kline({level}) 为空: code={code}")
  df = df.rename(columns={"time": "dt"})
  synthetic_path = f"kline://{code}/{level.lower()}"
  return df, synthetic_path


def _ema(series: pd.Series, span: int) -> pd.Series:
  return series.ewm(span=span, adjust=False).mean()


def _sma(series: pd.Series, window: int) -> pd.Series:
  return series.rolling(window=window, min_periods=window).mean()


def _to_line_points(df: pd.DataFrame, col: str) -> list[IndicatorLinePoint]:
  result: list[IndicatorLinePoint] = []
  for _, row in df.iterrows():
    value = row[col]
    result.append(
      IndicatorLinePoint(
        time=row["dt"].strftime("%Y-%m-%d %H:%M:%S"),
        value=None if pd.isna(value) else float(value),
      )
    )
  return result


def _calc_td9_labels(df: pd.DataFrame) -> list[TD9Label]:
  labels: list[TD9Label] = []

  td_up = 0
  td_dn = 0

  up_counts = [0] * len(df)
  dn_counts = [0] * len(df)

  closes = df["close"].tolist()
  highs = df["high"].tolist()
  lows = df["low"].tolist()
  times = df["dt"].tolist()

  for i in range(len(df)):
    if i < 4:
      td_up = 0
      td_dn = 0
    else:
      if closes[i] > closes[i - 4]:
        td_up += 1
        td_dn = 0
      elif closes[i] < closes[i - 4]:
        td_dn += 1
        td_up = 0
      else:
        td_up = 0
        td_dn = 0

    up_counts[i] = td_up
    dn_counts[i] = td_dn

  for i, count in enumerate(up_counts):
    if count == 9:
      for j in range(9):
        idx = i - (8 - j)
        if idx < 0:
          continue
        num = j + 1
        labels.append(
          TD9Label(
            time=times[idx].strftime("%Y-%m-%d %H:%M:%S"),
            price=float(highs[idx]),
            text=str(num),
            position="above",
            color="#00aa00" if num == 9 else "#FF00FF",
          )
        )

  for i, count in enumerate(dn_counts):
    if count == 9:
      for j in range(9):
        idx = i - (8 - j)
        if idx < 0:
          continue
        num = j + 1
        labels.append(
          TD9Label(
            time=times[idx].strftime("%Y-%m-%d %H:%M:%S"),
            price=float(lows[idx]),
            text=str(num),
            position="below",
            color="#FF00FF" if num == 9 else "#00aa00",
          )
        )

  if len(df) > 0:
    last_idx = len(df) - 1

    if 5 <= up_counts[last_idx] < 9:
      cnt = up_counts[last_idx]
      for j in range(cnt):
        idx = last_idx - (cnt - 1 - j)
        if idx < 0:
          continue
        num = j + 1
        labels.append(
          TD9Label(
            time=times[idx].strftime("%Y-%m-%d %H:%M:%S"),
            price=float(highs[idx]),
            text=str(num),
            position="above",
            color="#FF00FF",
          )
        )

    if 5 <= dn_counts[last_idx] < 9:
      cnt = dn_counts[last_idx]
      for j in range(cnt):
        idx = last_idx - (cnt - 1 - j)
        if idx < 0:
          continue
        num = j + 1
        labels.append(
          TD9Label(
            time=times[idx].strftime("%Y-%m-%d %H:%M:%S"),
            price=float(lows[idx]),
            text=str(num),
            position="below",
            color="#00aa00",
          )
        )

  return labels


def _build_indicators(code: str, level: LevelType) -> IndicatorsData:
  df, cache_path = _load_db_df(code, level)

  df = df.copy()

  from shared_indicators import compute_byma_indicators

  df = compute_byma_indicators(df)

  td9_labels = _calc_td9_labels(df)

  return IndicatorsData(
    symbol=code.upper(),
    level=level,
    cache_file=cache_path,
    blue_upper=_to_line_points(df, "blue_upper"),
    blue_lower=_to_line_points(df, "blue_lower"),
    yellow_upper=_to_line_points(df, "yellow_upper"),
    yellow_lower=_to_line_points(df, "yellow_lower"),
    ma55=_to_line_points(df, "ma55"),
    ma60=_to_line_points(df, "ma60"),
    ma65=_to_line_points(df, "ma65"),
    ma120=_to_line_points(df, "ma120"),
    ma250=_to_line_points(df, "ma250"),
    td9_labels=td9_labels,
  )


def _normalize_stock_code(code: str) -> str:
  code = code.strip().upper()
  # Support both ':' and '.' as exchange delimiters
  if ":" in code:
    return code.split(":", 1)[0]
  if "." in code:
    return code.split(".", 1)[0]
  return code


@router.get("/api/chan/indicators/health")
def indicators_health():
  return {"code": 0, "message": "ok"}


@router.post("/api/chan/indicators", response_model=IndicatorsResponse)
def get_indicators(req: IndicatorsRequest):
  logging.getLogger("indicators").info(
    "Received /api/chan/indicators request code=%s level=%s", req.code, req.level
  )
  try:
    code = _normalize_stock_code(req.code)
    level = req.level
    today = date.today()
    # ---- 1️⃣ DB cache lookup ----
    with engine.connect() as conn:
      cached = get_cached(conn, code, level, "indicators", today)
      if cached is not None:
        return IndicatorsResponse(**cached)
    # Delete old cache entries for this code before calculating new data
    with engine.begin() as conn:
      delete_old_cache_for_code(conn, code, today)
    # ---- 2️⃣ 确保 kline 表中有 (code, level) 的最新数据（增量 UPSERT） ----
    kline_store.ensure_levels_updated([code], [level])
    # ---- 3️⃣ 原有计算 ----
    data = _build_indicators(code, level)
    response = IndicatorsResponse(code=0, message="ok", data=data)
    # ---- 3️⃣ 写入缓存 ----
    with engine.begin() as conn:
      set_cached(conn, code, level, "indicators", today, response.dict())
    return response
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
