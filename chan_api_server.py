from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Literal
import logging
from logging.handlers import TimedRotatingFileHandler
import sys

import pandas as pd
from fastapi import APIRouter, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from indicators_api import router as indicators_router
from backtest_api import router as backtest_router

router = APIRouter()

BASE_DIR = Path(__file__).resolve().parent
DATA_CACHE_DIR = BASE_DIR / "data_cache"
DATA_CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Logger setup: write logs to /tmp/chan_api_server.log and rotate every 7 days
LOG_FILE_PATH = Path("/tmp/chan_api_server.log")
logger = logging.getLogger("chan_api_server")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

file_handler = TimedRotatingFileHandler(
  filename=str(LOG_FILE_PATH), when="D", interval=7, backupCount=12, encoding="utf-8"
)
file_handler.setFormatter(formatter)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)

if not logger.handlers:
  logger.addHandler(file_handler)
  logger.addHandler(stream_handler)
logger.propagate = False

LevelType = Literal["1D", "1H", "2H", "4H"]

# =========================
# Pydantic models
# =========================


class ChanAnalyzeRequest(BaseModel):
  code: str = Field(..., description="美股代码，例如 TSLA")
  level: LevelType = Field(..., description="级别：1D / 1H / 2H / 4H")


class RawKLineItem(BaseModel):
  idx: int
  time: str
  open: float
  high: float
  low: float
  close: float
  volume: float | None = None


class BiItem(BaseModel):
  idx: int
  dir: str | None = None
  is_sure: bool
  seg_idx: int | None = None
  begin_klu_idx: int | None = None
  end_klu_idx: int | None = None
  begin_time: str | None = None
  end_time: str | None = None
  begin_price: float | None = None
  end_price: float | None = None


class ZSItem(BaseModel):
  idx: int
  begin_bi_idx: int | None = None
  end_bi_idx: int | None = None
  bi_in_idx: int | None = None
  bi_out_idx: int | None = None
  begin_time: str | None = None
  end_time: str | None = None
  low: float | None = None
  high: float | None = None
  peak_low: float | None = None
  peak_high: float | None = None
  bi_idx_list: list[int] = []


class BSPItem(BaseModel):
  idx: int
  bi_idx: int | None = None
  klu_idx: int | None = None
  time: str | None = None
  price: float | None = None
  is_buy: bool
  types: list[str] = []
  is_sure: bool | None = None


class MacdItem(BaseModel):
  time: str
  dif: float
  dea: float
  macd: float  # 柱状值 = 2*(DIF-DEA)，与框架 CMACD_item.macd 一致


class ChanAnalyzeSummary(BaseModel):
  raw_kline_count: int = 0
  bi_count: int = 0
  zs_count: int = 0
  bsp_count: int = 0


class ChanAnalyzeData(BaseModel):
  symbol: str
  market: str
  level: str
  cache_file: str
  summary: ChanAnalyzeSummary
  raw_kline_list: list[RawKLineItem] = []
  bi_list: list[BiItem] = []
  zs_list: list[ZSItem] = []
  bsp_list: list[BSPItem] = []
  macd_list: list[MacdItem] = []


class ChanAnalyzeResponse(BaseModel):
  code: int
  message: str
  data: ChanAnalyzeData


# =========================
# helpers
# =========================


def _today_str() -> str:
  return datetime.now().strftime("%Y-%m-%d")


def _safe_str(v: Any) -> str | None:
  return None if v is None else str(v)


def _safe_float(v: Any) -> float | None:
  if v is None:
    return None
  try:
    return float(v)
  except Exception:
    return None


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
  df = df.copy()

  rename_map = {}
  for col in df.columns:
    raw = str(col).strip()
    key = raw.lower()

    if raw in ["日期"] or key in ["time", "date", "datetime", "timestamp", "dt"]:
      rename_map[col] = "time"
    elif raw in ["开盘"] or key == "open":
      rename_map[col] = "open"
    elif raw in ["最高"] or key == "high":
      rename_map[col] = "high"
    elif raw in ["最低"] or key == "low":
      rename_map[col] = "low"
    elif raw in ["收盘"] or key == "close":
      rename_map[col] = "close"
    elif raw in ["成交量"] or key in ["volume", "vol"]:
      rename_map[col] = "volume"

  df = df.rename(columns=rename_map)

  required = ["time", "open", "high", "low", "close"]
  missing = [c for c in required if c not in df.columns]
  if missing:
    raise ValueError(
      f"missing columns after normalize: {missing}, raw columns={list(df.columns)}"
    )

  if "volume" not in df.columns:
    df["volume"] = 0.0

  df["time"] = pd.to_datetime(df["time"], errors="coerce")
  df = (
    df.dropna(subset=["time"])
    .sort_values("time")
    .drop_duplicates(subset=["time"])
    .reset_index(drop=True)
  )

  for c in ["open", "high", "low", "close", "volume"]:
    df[c] = pd.to_numeric(df[c], errors="coerce")

  df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)

  if df.empty:
    raise ValueError("normalized dataframe is empty")

  return df[["time", "open", "high", "low", "close", "volume"]]


def _save_csv(df: pd.DataFrame, path: Path) -> None:
  out = df.copy()
  out["time"] = pd.to_datetime(out["time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
  out.to_csv(path, index=False, encoding="utf-8-sig")


def _daily_cache_path(code: str) -> Path:
  return DATA_CACHE_DIR / f"{code.upper()}_{_today_str()}_1d.csv"


def _intraday_cache_path(code: str, level: Literal["1H", "2H", "4H"]) -> Path:
  level_map = {
    "1H": "1h",
    "2H": "2h",
    "4H": "4h",
  }
  return (
    DATA_CACHE_DIR / f"{code.upper()}_{_today_str()}_yf_{level_map[level]}_730d.csv"
  )


# =========================
# daily fetchers
# =========================


def fetch_daily_from_akshare_or_yfinance(code: str) -> pd.DataFrame:
  """
  1D 日线抓取：
  1. 优先 Akshare
  2. Akshare 失败自动回退 yfinance
  """
  # Akshare first
  # try:
  #  import akshare as ak

  #  df = ak.stock_us_hist(symbol=code.upper(), period="daily", adjust="")
  #  if df is not None and not df.empty:
  #    logger.info(f"1D source=akshare code={code}")
  #    return _normalize_columns(df)
  # except Exception as e:
  #  logger.exception(f"Akshare daily failed for {code}: {e}")

  # yfinance fallback
  try:
    import yfinance as yf

    ticker = yf.Ticker(code.upper())
    start_date = (datetime.now() - timedelta(days=365 * 20 + 30)).strftime("%Y-%m-%d")

    df = ticker.history(
      start=start_date,
      interval="1d",
      auto_adjust=False,
      actions=False,
    )

    if df is None or df.empty:
      raise ValueError(f"yfinance returns empty daily data for {code}")

    df = df.reset_index()

    if "Date" in df.columns:
      df = df.rename(columns={"Date": "time"})
    elif "Datetime" in df.columns:
      df = df.rename(columns={"Datetime": "time"})

    df = df.rename(
      columns={
        "Open": "open",
        "High": "high",
        "Low": "low",
        "Close": "close",
        "Volume": "volume",
      }
    )

    df = _normalize_columns(df)

    if getattr(df["time"].dt, "tz", None) is not None:
      df["time"] = df["time"].dt.tz_localize(None)

    logger.info(f"1D source=yfinance code={code}")
    return df

  except Exception as e:
    raise ValueError(f"daily fetch failed for {code}, akshare+yfinance all failed: {e}")


# =========================
# intraday fetchers
# =========================


def fetch_60m_from_yfinance(code: str) -> pd.DataFrame:
  import yfinance as yf

  ticker = yf.Ticker(code.upper())
  df = ticker.history(period="730d", interval="60m", auto_adjust=False, actions=False)

  if df is None or df.empty:
    raise ValueError(f"yfinance returns empty 60m data for {code}")

  df = df.reset_index()

  if "Datetime" in df.columns:
    df = df.rename(columns={"Datetime": "time"})
  elif "Date" in df.columns:
    df = df.rename(columns={"Date": "time"})

  df = df.rename(
    columns={
      "Open": "open",
      "High": "high",
      "Low": "low",
      "Close": "close",
      "Volume": "volume",
    }
  )

  df = _normalize_columns(df)

  if getattr(df["time"].dt, "tz", None) is not None:
    df["time"] = df["time"].dt.tz_localize(None)

  return df


def _apply_intraday_bar_end_time(ts: pd.Timestamp, hours: int) -> pd.Timestamp:
  dt = pd.Timestamp(ts)
  base = dt.normalize()

  hhmm = dt.strftime("%H:%M")

  if hours == 1:
    mapping = {
      "09:30": "10:30",
      "10:30": "11:30",
      "11:30": "12:30",
      "12:30": "13:30",
      "13:30": "14:30",
      "14:30": "15:30",
      "15:30": "16:00",
    }
  elif hours == 2:
    mapping = {
      "09:30": "11:30",
      "11:30": "13:30",
      "13:30": "15:30",
      "15:30": "16:00",
    }
  elif hours == 4:
    mapping = {
      "09:30": "13:30",
      "13:30": "16:00",
    }
  else:
    raise ValueError(f"unsupported hours: {hours}")

  if hhmm not in mapping:
    raise ValueError(f"unexpected intraday timestamp {hhmm} for {hours}H aggregation")

  end_hhmm = mapping[hhmm]
  end_hour, end_minute = map(int, end_hhmm.split(":"))
  return base + pd.Timedelta(hours=end_hour, minutes=end_minute)


def aggregate_intraday(df_1h: pd.DataFrame, hours: int) -> pd.DataFrame:
  df = df_1h.copy()
  df["time"] = pd.to_datetime(df["time"], errors="coerce")
  df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

  df["trade_date"] = df["time"].dt.date
  df["hhmm"] = df["time"].dt.strftime("%H:%M")

  if hours == 1:
    allowed = ["09:30", "10:30", "11:30", "12:30", "13:30", "14:30", "15:30"]
    df = df[df["hhmm"].isin(allowed)].copy()
    df["bar_end_time"] = df["time"].apply(lambda x: _apply_intraday_bar_end_time(x, 1))
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

  agg["time"] = agg["time"].apply(lambda x: _apply_intraday_bar_end_time(x, hours))
  return agg[["time", "open", "high", "low", "close", "volume"]]


# =========================
# cache builders
# =========================


def get_or_build_1d_csv(code: str) -> Path:
  cache_path = _daily_cache_path(code)

  if cache_path.exists():
    logger.info(f"hit 1D cache: {cache_path}")
    return cache_path

  df = fetch_daily_from_akshare_or_yfinance(code)

  if df is None or df.empty:
    raise ValueError(f"1D dataframe empty after fetch: {code}")

  _save_csv(df, cache_path)

  if not cache_path.exists():
    raise ValueError(f"1D csv save failed: {cache_path}")

  logger.info(f"build 1D cache: {cache_path}")
  return cache_path


def ensure_intraday_day_cache(code: str) -> dict[str, Path]:
  """
  小时级按天缓存：
  - 第一次访问任意 1H/2H/4H，抓一次 yfinance 60m/730d
  - 一次性生成今天的 1H / 2H / 4H 三份 csv
  - 同一天后续请求都直接命中本地缓存
  """
  path_1h = _intraday_cache_path(code, "1H")
  path_2h = _intraday_cache_path(code, "2H")
  path_4h = _intraday_cache_path(code, "4H")

  if path_1h.exists() and path_2h.exists() and path_4h.exists():
    logger.info(f"hit intraday cache all-ready: code={code}")
    return {
      "1H": path_1h,
      "2H": path_2h,
      "4H": path_4h,
    }

  df_1h = fetch_60m_from_yfinance(code)
  df_2h = aggregate_intraday(df_1h, 2)
  df_4h = aggregate_intraday(df_1h, 4)
  df_1h_shifted = aggregate_intraday(df_1h, 1)

  _save_csv(df_1h_shifted, path_1h)
  _save_csv(df_2h, path_2h)
  _save_csv(df_4h, path_4h)

  logger.info(f"build intraday cache: {path_1h}")
  logger.info(f"build intraday cache: {path_2h}")
  logger.info(f"build intraday cache: {path_4h}")

  return {
    "1H": path_1h,
    "2H": path_2h,
    "4H": path_4h,
  }


# =========================
# chan bridge
# =========================


def build_chan_from_csv(code: str, level: LevelType):
  from Chan import CChan
  from ChanConfig import CChanConfig
  from Common.CEnum import KL_TYPE

  trigger_step = True

  from shared_chan_config import DEFAULT_CHAN_CONFIG

  cfg = dict(DEFAULT_CHAN_CONFIG)
  cfg["trigger_step"] = trigger_step
  config = CChanConfig(cfg)

  if level == "1D":
    kl_type = KL_TYPE.K_DAY
    data_src = "custom:OfflineUsDailyCsvAPI.COfflineUsDailyCsvAPI"
  else:
    kl_type = KL_TYPE.K_60M
    data_src_map = {
      "1H": "COfflineYFinance1HCsvAPI",
      "2H": "COfflineYFinance2HCsvAPI",
      "4H": "COfflineYFinance4HCsvAPI",
    }
    data_src = f"custom:OfflineYFinanceIntradayCsvAPI.{data_src_map[level]}"

  chan = CChan(
    code=code.upper(),
    begin_time=None,
    end_time=None,
    data_src=data_src,
    lv_list=[kl_type],
    config=config,
  )

  logger.info("=" * 80)
  logger.info(f"[CHAN-CONFIG] code={code} level={level}")
  logger.info(f"[CHAN-CONFIG] kl_type={kl_type} data_src={data_src}")
  logger.info(f"[CHAN-CONFIG] bs_type=1,1p,2,2s,3a,3b")
  logger.info(f"[CHAN-CONFIG] bsp2_follow_1=True bsp3_follow_1=True")
  logger.info(f"[CHAN-CONFIG] strict_bsp3=False bsp1_only_multibi_zs=False")
  logger.info(f"[CHAN-CONFIG] bsp3_peak=False min_zs_cnt=0 trigger_step={trigger_step}")
  logger.info("=" * 80)
  return chan, kl_type, trigger_step


def extract_chan_data(code: str, level: LevelType, csv_path: Path) -> ChanAnalyzeData:
  chan, kl_type, trigger_step = build_chan_from_csv(code, level)

  logger.info("=" * 80)
  logger.info(f"[CHAN] code={code} level={level} csv_path={csv_path}")
  logger.info(f"[CHAN] trigger_step={trigger_step}")

  last_kl_list = None

  if trigger_step:
    step_cnt = 0
    logger.info("[STEP] start step_load()")

    for snapshot in chan.step_load():
      step_cnt += 1
      last_kl_list = snapshot[kl_type]

      bi_raw = getattr(getattr(last_kl_list, "bi_list", None), "bi_list", []) or []
      seg_raw = (
        getattr(getattr(last_kl_list, "seg_list", None), "lst", [])
        or getattr(getattr(last_kl_list, "seg_list", None), "seg_list", [])
        or []
      )
      zs_raw = getattr(getattr(last_kl_list, "zs_list", None), "zs_lst", []) or []

      bsp_obj = getattr(last_kl_list, "bs_point_lst", None)
      bsp_raw = []
      if bsp_obj is not None:
        bsp_raw = (
          getattr(bsp_obj, "lst", []) or getattr(bsp_obj, "bs_point_lst", []) or []
        )

      if step_cnt <= 10 or step_cnt % 100 == 0:
        logger.info(
          f"[STEP] n={step_cnt} "
          f"bi={len(bi_raw)} seg={len(seg_raw)} zs={len(zs_raw)} bsp={len(bsp_raw)}"
        )

    logger.info(f"[STEP] finished total_steps={step_cnt}")

    if last_kl_list is None:
      raise ValueError(
        "step_load() 没有产生任何 snapshot，请检查数据源或 trigger_step 模式"
      )

    kl_list = last_kl_list
  else:
    kl_list = chan.kl_datas[kl_type]

  logger.info(f"[CHAN] kl_list type={type(kl_list)}")
  logger.info(
    f"[CHAN] kl_list attrs sample={sorted([x for x in dir(kl_list) if not x.startswith('_')])[:80]}"
  )

  ck_list = getattr(kl_list, "lst", []) or []
  logger.info(f"[CHAN] combined_kline_count={len(ck_list)}")

  bi_raw = getattr(getattr(kl_list, "bi_list", None), "bi_list", []) or []
  seg_raw = (
    getattr(getattr(kl_list, "seg_list", None), "lst", [])
    or getattr(getattr(kl_list, "seg_list", None), "seg_list", [])
    or []
  )
  zs_raw = getattr(getattr(kl_list, "zs_list", None), "zs_lst", []) or []

  logger.info(f"[CHAN] bi_count_raw={len(bi_raw)}")
  logger.info(f"[CHAN] seg_count_raw={len(seg_raw)}")
  logger.info(f"[CHAN] zs_count_raw={len(zs_raw)}")

  if bi_raw:
    logger.info("[CHAN] first 5 bi:")
    for bi in bi_raw[:5]:
      begin_klu = bi.get_begin_klu() if hasattr(bi, "get_begin_klu") else None
      end_klu = bi.get_end_klu() if hasattr(bi, "get_end_klu") else None
      logger.info(
        f" bi idx={getattr(bi, 'idx', None)} "
        f"dir={getattr(bi, 'dir', None)} "
        f"is_sure={getattr(bi, 'is_sure', None)} "
        f"begin={getattr(begin_klu, 'time', None)} "
        f"end={getattr(end_klu, 'time', None)}"
      )

  if zs_raw:
    logger.info("[CHAN] first 3 zs:")
    for i, zs in enumerate(zs_raw[:3]):
      logger.info(
        f" zs idx={i} "
        f"begin_bi={getattr(getattr(zs, 'begin_bi', None), 'idx', None)} "
        f"end_bi={getattr(getattr(zs, 'end_bi', None), 'idx', None)} "
        f"low={getattr(zs, 'low', None)} "
        f"high={getattr(zs, 'high', None)} "
        f"bi_cnt={len(getattr(zs, 'bi_lst', []) or [])}"
      )

  raw_kline_list: list[RawKLineItem] = []
  bi_list: list[BiItem] = []
  zs_list: list[ZSItem] = []
  bsp_list: list[BSPItem] = []
  macd_list: list[MacdItem] = []

  for ck in ck_list:
    for klu in getattr(ck, "lst", []):
      # klu.time is a CTime object whose __str__ returns 'YYYY/MM/DD' for daily bars
      # For analyze JSON we want daily times to include a 00:00 suffix ("YYYY/MM/DD 00:00").
      raw_time = _safe_str(getattr(klu, "time", "")) or ""
      if raw_time and len(raw_time) == 10 and raw_time.count("/") == 2:
        # date-only string like '2010/06/29' -> append ' 00:00'
        klu_time = f"{raw_time} 00:00"
      else:
        klu_time = raw_time
      raw_kline_list.append(
        RawKLineItem(
          idx=int(getattr(klu, "idx", -1)),
          time=klu_time,
          open=float(getattr(klu, "open")),
          high=float(getattr(klu, "high")),
          low=float(getattr(klu, "low")),
          close=float(getattr(klu, "close")),
          volume=_safe_float(getattr(getattr(klu, "trade_info", None), "metric", {}).get("volume")),
        )
      )

      macd_obj = getattr(klu, "macd", None)
      if macd_obj is not None:
        dif = _safe_float(getattr(macd_obj, "DIF", None))
        dea = _safe_float(getattr(macd_obj, "DEA", None))
        macd_val = _safe_float(getattr(macd_obj, "macd", None))
        if dif is not None and dea is not None and macd_val is not None:
          macd_list.append(MacdItem(time=klu_time, dif=dif, dea=dea, macd=macd_val))

  for bi in bi_raw:
    begin_klu = bi.get_begin_klu() if hasattr(bi, "get_begin_klu") else None
    end_klu = bi.get_end_klu() if hasattr(bi, "get_end_klu") else None

    bi_list.append(
      BiItem(
        idx=int(getattr(bi, "idx", -1)),
        dir=_safe_str(getattr(bi, "dir", None)),
        is_sure=bool(getattr(bi, "is_sure", False)),
        seg_idx=getattr(bi, "seg_idx", None),
        begin_klu_idx=getattr(begin_klu, "idx", None),
        end_klu_idx=getattr(end_klu, "idx", None),
        begin_time=_safe_str(getattr(begin_klu, "time", None)),
        end_time=_safe_str(getattr(end_klu, "time", None)),
        begin_price=_safe_float(
          bi.get_begin_val() if hasattr(bi, "get_begin_val") else None
        ),
        end_price=_safe_float(bi.get_end_val() if hasattr(bi, "get_end_val") else None),
      )
    )

  for i, zs in enumerate(zs_raw):
    zs_bi_list = getattr(zs, "bi_lst", []) or []
    zs_list.append(
      ZSItem(
        idx=i,
        begin_bi_idx=getattr(getattr(zs, "begin_bi", None), "idx", None),
        end_bi_idx=getattr(getattr(zs, "end_bi", None), "idx", None),
        bi_in_idx=getattr(getattr(zs, "bi_in", None), "idx", None),
        bi_out_idx=getattr(getattr(zs, "bi_out", None), "idx", None),
        begin_time=_safe_str(getattr(getattr(zs, "begin", None), "time", None)),
        end_time=_safe_str(getattr(getattr(zs, "end", None), "time", None)),
        low=_safe_float(getattr(zs, "low", None)),
        high=_safe_float(getattr(zs, "high", None)),
        peak_low=_safe_float(getattr(zs, "peak_low", None)),
        peak_high=_safe_float(getattr(zs, "peak_high", None)),
        bi_idx_list=[int(getattr(x, "idx", -1)) for x in zs_bi_list],
      )
    )

  bs_point_lst_obj = getattr(kl_list, "bs_point_lst", None)
  logger.info(f"[BSP] bs_point_lst_obj exists: {bs_point_lst_obj is not None}")

  bsp_raw_list = []
  if bs_point_lst_obj is not None:
    logger.info(f"[BSP] bs_point_lst_obj type={type(bs_point_lst_obj)}")
    logger.info(
      f"[BSP] bs_point_lst_obj attrs={sorted([x for x in dir(bs_point_lst_obj) if not x.startswith('_')])}"
    )

    if hasattr(bs_point_lst_obj, "getSortedBspList"):
      try:
        bsp_raw_list = list(bs_point_lst_obj.getSortedBspList() or [])
      except Exception as e:
        logger.exception(f"getSortedBspList() failed: {e}")

    if not bsp_raw_list and hasattr(bs_point_lst_obj, "bsp_iter"):
      try:
        bsp_raw_list = list(bs_point_lst_obj.bsp_iter())
      except Exception as e:
        logger.exception(f"bsp_iter() failed: {e}")

    if not bsp_raw_list and hasattr(bs_point_lst_obj, "bsp_iter_v2"):
      try:
        bsp_raw_list = list(bs_point_lst_obj.bsp_iter_v2())
      except Exception as e:
        logger.exception(f"bsp_iter_v2() failed: {e}")

  logger.info(f"[BSP] Raw BSP count: {len(bsp_raw_list)}")

  for i, bsp in enumerate(bsp_raw_list[:5]):
    logger.info(
      f"[BSP] raw[{i}] "
      f"is_buy={getattr(bsp, 'is_buy', None)} "
      f"type={getattr(bsp, 'type', None)} "
      f"types={getattr(bsp, 'types', None)} "
      f"bi_idx={getattr(getattr(bsp, 'bi', None), 'idx', None)} "
      f"klu_idx={getattr(getattr(bsp, 'klu', None), 'idx', None)}"
    )

  for i, bsp in enumerate(bsp_raw_list):
    bi_obj = getattr(bsp, "bi", None)
    klu_obj = getattr(bsp, "klu", None) or getattr(bsp, "Klu", None)

    bsp_type_raw = getattr(bsp, "type", None) or getattr(bsp, "types", None) or []
    if not isinstance(bsp_type_raw, (list, tuple)):
      bsp_type_raw = [bsp_type_raw]

    types = [
      str(t).split(".")[-1] if "." in str(t) else str(t) for t in bsp_type_raw if t
    ]

    price = None
    if klu_obj:
      price = (
        _safe_float(getattr(klu_obj, "close", None))
        or _safe_float(getattr(klu_obj, "low", None))
        or _safe_float(getattr(klu_obj, "high", None))
      )

    bsp_list.append(
      BSPItem(
        idx=i,
        bi_idx=getattr(bi_obj, "idx", None) if bi_obj else None,
        klu_idx=getattr(klu_obj, "idx", None) if klu_obj else None,
        time=_safe_str(getattr(klu_obj, "time", None)) if klu_obj else None,
        price=price,
        is_buy=bool(getattr(bsp, "is_buy", False)),
        types=types,
        is_sure=getattr(bi_obj, "is_sure", None) if bi_obj else None,
      )
    )

  logger.info(
    f"[SUMMARY] code={code.upper()} level={level} "
    f"raw_kline_count={len(raw_kline_list)} "
    f"bi_count={len(bi_list)} "
    f"zs_count={len(zs_list)} "
    f"bsp_count={len(bsp_list)}"
  )

  return ChanAnalyzeData(
    symbol=code.upper(),
    market="US",
    level=level,
    cache_file=str(csv_path),
    summary=ChanAnalyzeSummary(
      raw_kline_count=len(raw_kline_list),
      bi_count=len(bi_list),
      zs_count=len(zs_list),
      bsp_count=len(bsp_list),
    ),
    raw_kline_list=raw_kline_list,
    bi_list=bi_list,
    zs_list=zs_list,
    bsp_list=bsp_list,
    macd_list=macd_list,
  )


# =========================
# routes
# =========================


@router.get("/health")
def health():
  return {"code": 0, "message": "ok"}


@router.post("/api/chan/analyze", response_model=ChanAnalyzeResponse)
def analyze_chan(req: ChanAnalyzeRequest):
  try:
    code = req.code.upper().strip()
    level = req.level

    if level == "1D":
      csv_path = get_or_build_1d_csv(code)
    else:
      cache_map = ensure_intraday_day_cache(code)
      csv_path = cache_map[level]

    data = extract_chan_data(code, level, csv_path)

    return ChanAnalyzeResponse(
      code=0,
      message="ok",
      data=data,
    )

  except Exception as e:
    import traceback

    traceback.print_exc()
    raise HTTPException(status_code=500, detail=str(e))


app = FastAPI(title="Chan API", version="0.5.0")

app.add_middleware(
  CORSMiddleware,
  allow_origins=["*"],
  allow_credentials=True,
  allow_methods=["*"],
  allow_headers=["*"],
)

app.include_router(router)
app.include_router(indicators_router)
app.include_router(backtest_router)
