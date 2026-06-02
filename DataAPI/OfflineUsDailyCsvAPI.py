from __future__ import annotations

import pandas as pd

from Common.CEnum import DATA_FIELD, AUTYPE
from Common.CTime import CTime
from DataAPI.CommonStockAPI import CCommonStockApi
from KLine.KLine_Unit import CKLine_Unit

import kline_store


class COfflineUsDailyCsvAPI(CCommonStockApi):
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
    df = kline_store.read_kline_df(
      self.code, "1d",
      begin_date=self.begin_date, end_date=self.end_date,
    )

    if df.empty:
      raise ValueError(
        f"kline(1d) is empty in db: code={self.code}, "
        f"begin={self.begin_date}, end={self.end_date}"
      )

    for _, row in df.iterrows():
      dt = row["time"]
      item = {
        DATA_FIELD.FIELD_TIME: CTime(dt.year, dt.month, dt.day, 0, 0, auto=False),
        DATA_FIELD.FIELD_OPEN: float(row["open"]),
        DATA_FIELD.FIELD_HIGH: float(row["high"]),
        DATA_FIELD.FIELD_LOW: float(row["low"]),
        DATA_FIELD.FIELD_CLOSE: float(row["close"]),
        DATA_FIELD.FIELD_VOLUME: float(row["volume"])
        if pd.notna(row["volume"])
        else 0.0,
      }
      yield CKLine_Unit(item)
