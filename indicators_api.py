from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Literal

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent
DATA_CACHE_DIR = BASE_DIR / "data_cache"

LevelType = Literal["1D", "1H", "2H", "4H"]


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


def _today_str() -> str:
  return datetime.now().strftime("%Y-%m-%d")


def _safe_filename_part(value: str | None) -> str:
  if not value:
    return "none"
  return str(value).replace("/", "-").replace(":", "_").replace(" ", "_")


def _build_daily_csv_cache_path(code: str) -> Path:
  return DATA_CACHE_DIR / f"{code.upper()}_{_today_str()}_1d.csv"


def _build_intraday_csv_cache_path(code: str, timeframe: str) -> Path:
  code_part = _safe_filename_part(code)
  tf_part = timeframe.lower()
  return DATA_CACHE_DIR / f"{code_part}_{_today_str()}_yf_{tf_part}_730d.csv"


def _resolve_cache_path(code: str, level: LevelType) -> Path:
  if level == "1D":
    return _build_daily_csv_cache_path(code)

  tf_map = {
    "1H": "1h",
    "2H": "2h",
    "4H": "4h",
  }
  return _build_intraday_csv_cache_path(code, tf_map[level])


def _load_cached_df(code: str, level: LevelType) -> tuple[pd.DataFrame, Path]:
  csv_path = _resolve_cache_path(code, level)

  if not csv_path.exists():
    raise FileNotFoundError(f"离线 csv 不存在: {csv_path}")

  df = pd.read_csv(csv_path)
  if df is None or df.empty:
    raise ValueError(f"离线 csv 为空: {csv_path}")

  df.columns = [str(c).strip().lower() for c in df.columns]

  if "time" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"time": "dt"})
  if "date" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"date": "dt"})

  required = {"dt", "open", "high", "low", "close"}
  missing = required - set(df.columns)
  if missing:
    raise ValueError(f"离线 csv 缺少列: {missing}, path={csv_path}")

  if "volume" not in df.columns:
    df["volume"] = 0.0

  df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
  df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

  for col in ["open", "high", "low", "close", "volume"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")

  df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

  if df.empty:
    raise ValueError(f"离线 csv 标准化后为空: {csv_path}")

  return df, csv_path


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
  df, csv_path = _load_cached_df(code, level)

  df = df.copy()

  from shared_indicators import compute_byma_indicators

  df = compute_byma_indicators(df)

  td9_labels = _calc_td9_labels(df)

  return IndicatorsData(
    symbol=code.upper(),
    level=level,
    cache_file=str(csv_path),
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


@router.get("/api/chan/indicators/health")
def indicators_health():
  return {"code": 0, "message": "ok"}


@router.post("/api/chan/indicators", response_model=IndicatorsResponse)
def get_indicators(req: IndicatorsRequest):
  try:
    data = _build_indicators(req.code.upper().strip(), req.level)
    return IndicatorsResponse(code=0, message="ok", data=data)
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
