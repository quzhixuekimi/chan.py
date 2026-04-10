from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from Common.CEnum import DATA_FIELD, AUTYPE
from Common.CTime import CTime
from DataAPI.CommonStockAPI import CCommonStockApi
from KLine.KLine_Unit import CKLine_Unit

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_CACHE_DIR = BASE_DIR / "data_cache"


def _today_str() -> str:
  return datetime.now().strftime("%Y-%m-%d")


def _safe_filename_part(value: str | None) -> str:
  if not value:
    return "none"
  return str(value).replace("/", "-").replace(":", "_").replace(" ", "_")


def _build_intraday_csv_cache_path(code: str, timeframe: str) -> Path:
  code_part = _safe_filename_part(code)
  tf_part = timeframe.lower()
  return DATA_CACHE_DIR / f"{code_part}_{_today_str()}_yf_{tf_part}_730d.csv"


def _load_intraday_df(code: str, timeframe: str) -> pd.DataFrame:
  csv_path = _build_intraday_csv_cache_path(code, timeframe)
  if not csv_path.exists():
    raise FileNotFoundError(f"离线 intraday csv 不存在: {csv_path}")

  df = pd.read_csv(csv_path)
  if df is None or df.empty:
    raise ValueError(f"离线 intraday csv 为空: {csv_path}")

  df.columns = [str(c).strip().lower() for c in df.columns]

  if "time" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"time": "dt"})

  required_cols = {"dt", "open", "high", "low", "close", "volume"}
  missing = required_cols - set(df.columns)
  if missing:
    raise ValueError(f"离线 intraday csv 缺少列: {missing}, path={csv_path}")

  df = df.copy()
  df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
  df = df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

  if df.empty:
    raise ValueError(f"离线 intraday csv 标准化后为空: {csv_path}")

  return df


class _BaseOfflineYFinanceIntradayCsvAPI(CCommonStockApi):
  TIMEFRAME = None

  def __init__(self, code, k_type, begin_date=None, end_date=None, autype=AUTYPE.QFQ):
    super().__init__(code, k_type, begin_date, end_date, autype)

  def SetBasciInfo(self):
    self.name = self.code
    self.is_stock = True

  @staticmethod
  def do_init():
    return

  @staticmethod
  def do_close():
    return

  def get_kl_data(self):
    if not self.TIMEFRAME:
      raise ValueError("TIMEFRAME 未配置")

    df = _load_intraday_df(self.code, self.TIMEFRAME)

    if self.begin_date:
      begin_dt = pd.to_datetime(self.begin_date)
      df = df[df["dt"] >= begin_dt]

    if self.end_date:
      end_dt = pd.to_datetime(self.end_date) + pd.Timedelta(days=1)
      df = df[df["dt"] < end_dt]

    df = df.sort_values("dt").reset_index(drop=True)
    if df.empty:
      raise ValueError(
        f"离线 intraday 数据过滤后为空: code={self.code}, timeframe={self.TIMEFRAME}, "
        f"begin={self.begin_date}, end={self.end_date}"
      )

    for _, row in df.iterrows():
      dt = row["dt"]
      item = {
        DATA_FIELD.FIELD_TIME: CTime(
          dt.year, dt.month, dt.day, dt.hour, dt.minute, auto=False
        ),
        DATA_FIELD.FIELD_OPEN: float(row["open"]),
        DATA_FIELD.FIELD_HIGH: float(row["high"]),
        DATA_FIELD.FIELD_LOW: float(row["low"]),
        DATA_FIELD.FIELD_CLOSE: float(row["close"]),
        DATA_FIELD.FIELD_VOLUME: float(row["volume"])
        if pd.notna(row["volume"])
        else 0.0,
      }
      yield CKLine_Unit(item)


class COfflineYFinance1HCsvAPI(_BaseOfflineYFinanceIntradayCsvAPI):
  TIMEFRAME = "1h"


class COfflineYFinance2HCsvAPI(_BaseOfflineYFinanceIntradayCsvAPI):
  TIMEFRAME = "2h"


class COfflineYFinance4HCsvAPI(_BaseOfflineYFinanceIntradayCsvAPI):
  TIMEFRAME = "4h"
