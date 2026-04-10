# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
from typing import Tuple, List, Optional, Dict, Any

from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import KL_TYPE
from KLine.KLine_List import CKLine_List
from KLine.KLine_Unit import CKLine_Unit
from Bi.Bi import CBi


def load_chan_data(
  code: str,
  level: str,
  csv_path: Path,
  config: dict,
  trigger_step: bool = True,
  begin_time: Optional[str] = None,
  end_time: Optional[str] = None,
) -> Tuple[CChan, KL_TYPE, CKLine_List]:
  """
  从chan.py框架加载数据，返回CChan对象、KL_TYPE和CKLine_List
  """
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

  chan_config = CChanConfig(config)
  chan_config.trigger_step = trigger_step

  chan = CChan(
    code=code,
    begin_time=begin_time,
    end_time=end_time,
    data_src=data_src,
    lv_list=[kl_type],
    config=chan_config,
  )

  if trigger_step:
    kl_list = None
    for snapshot in chan.step_load():
      kl_list = snapshot[kl_type]
    if kl_list is None:
      raise ValueError("No data returned from step_load")
  else:
    kl_list = chan.kl_datas[kl_type]

  return chan, kl_type, kl_list


def extract_kline_data(kl_list: CKLine_List) -> pd.DataFrame:
  ck_list = getattr(kl_list, "lst", []) or []
  rows = []
  for ck in ck_list:
    for klu in ck.lst:
      rows.append(
        {
          "time": klu.time.to_str(),
          "open": klu.open,
          "high": klu.high,
          "low": klu.low,
          "close": klu.close,
          "volume": klu.trade_info.metric.get("volume", 0) if klu.trade_info else 0,
          "idx": klu.idx,
        }
      )
  return pd.DataFrame(rows)


def extract_bi_data(kl_list: CKLine_List) -> List[Dict[str, Any]]:
  """
  提取笔数据，用于v7回测
  """
  bi_list_raw = getattr(kl_list, "bi_list", None)
  if bi_list_raw is None:
    return []

  bis = []
  for bi in bi_list_raw:
    begin_klu = bi.get_begin_klu()
    end_klu = bi.get_end_klu()

    bis.append(
      {
        "bi_id": bi.idx,
        "direction": "up" if bi.is_up() else "down",
        "start_index": begin_klu.idx if begin_klu else None,
        "start_time": begin_klu.time.to_str() if begin_klu else None,
        "start_price": bi.get_begin_val(),
        "end_index": end_klu.idx if end_klu else None,
        "end_time": end_klu.time.to_str() if end_klu else None,
        "end_price": bi.get_end_val(),
        "bars": bi.get_klu_cnt(),
        "is_sure": bi.is_sure,
      }
    )
  return bis
