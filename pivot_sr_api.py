from __future__ import annotations

from typing import Literal

import pandas as pd
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field
from datetime import date
import logging

from db import engine, get_cached, set_cached, delete_old_cache_for_code
import kline_store
from pivot_sr_indicator import compute_pivot_sr
from pivot_sr_config import DEFAULT_PIVOT_SR_CONFIG

router = APIRouter()

LevelType = Literal["1D", "1H", "2H", "4H", "30M", "15M"]


class PivotSrRequest(BaseModel):
  code: str = Field(..., description="美股代码，例如 TSLA")
  level: LevelType = Field(..., description="级别：1D / 1H / 2H / 4H / 30M / 15M")


class CvdPoint(BaseModel):
  time: str
  value: float | None      # 已归一化映射到box高度内的坐标，用于画折线
  raw_cvd: float | None    # 原始累计成交量delta（业务数值）


class PivotZone(BaseModel):
  left_time: str
  right_time: str
  top: float
  bottom: float
  vol_text: str
  cvd_points: list[CvdPoint] = []
  cvd_label: str
  is_broken: bool


class PivotSrData(BaseModel):
  symbol: str
  level: str
  cache_file: str

  resistance_zones: list[PivotZone] = []   # 最近 max_zones_per_side 个，按时间升序
  support_zones: list[PivotZone] = []      # 最近 max_zones_per_side 个，按时间升序


class PivotSrResponse(BaseModel):
  code: int
  message: str
  data: PivotSrData


def _load_db_df(code: str, level: LevelType) -> tuple[pd.DataFrame, str]:
  """从 kline 表读取 (code, level) 数据，返回的 df 含 dt 列（与 indicators_api.py 一致）。"""
  df = kline_store.read_kline_df(code, level.lower())
  if df is None or df.empty:
    raise ValueError(f"kline({level}) 为空: code={code}")
  df = df.rename(columns={"time": "dt"})
  synthetic_path = f"kline://{code}/{level.lower()}"
  return df, synthetic_path


def _build_pivot_sr(code: str, level: LevelType) -> PivotSrData:
  df, cache_path = _load_db_df(code, level)
  df = df.copy()

  result = compute_pivot_sr(df, DEFAULT_PIVOT_SR_CONFIG)

  return PivotSrData(
    symbol=code.upper(),
    level=level,
    cache_file=cache_path,
    resistance_zones=[PivotZone(**z) for z in result["resistance_zones"]],
    support_zones=[PivotZone(**z) for z in result["support_zones"]],
  )


def _normalize_stock_code(code: str) -> str:
  code = code.strip().upper()
  if ":" in code:
    return code.split(":", 1)[0]
  if "." in code:
    return code.split(".", 1)[0]
  return code


@router.get("/api/chan/pivot_sr/health")
def pivot_sr_health():
  return {"code": 0, "message": "ok"}


@router.post("/api/chan/pivot_sr", response_model=PivotSrResponse)
def get_pivot_sr(req: PivotSrRequest):
  logging.getLogger("pivot_sr").info(
    "Received /api/chan/pivot_sr request code=%s level=%s", req.code, req.level
  )
  try:
    code = _normalize_stock_code(req.code)
    level = req.level
    today = date.today()
    # ---- 1) DB cache lookup ----
    with engine.connect() as conn:
      cached = get_cached(conn, code, level, "pivot_sr", today)
      if cached is not None:
        return PivotSrResponse(**cached)
    # 注意：这里不调用 delete_old_cache_for_code —— 该函数在 indicators_api.py /
    # analyze 里会清掉该 code 当天全部旧缓存（跨 endpoint），indicators 接口已经在
    # 同一个 daily workflow 里做过一次了，pivot_sr 不需要重复清（否则会重复删同一批行、
    # 没有副作用但没必要）。如果 pivot_sr 是当天第一个被调用的 endpoint，缓存未命中时
    # 直接往下算、写自己的缓存行即可，不影响其它 endpoint 的缓存。
    # ---- 2) 确保 kline 表中有 (code, level) 的最新数据（增量 UPSERT） ----
    kline_store.ensure_levels_updated([code], [level])
    # ---- 3) 计算 ----
    data = _build_pivot_sr(code, level)
    response = PivotSrResponse(code=0, message="ok", data=data)
    # ---- 4) 写入缓存 ----
    with engine.begin() as conn:
      set_cached(conn, code, level, "pivot_sr", today, response.model_dump())
    return response
  except Exception as e:
    raise HTTPException(status_code=500, detail=str(e))
