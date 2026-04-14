# -*- coding: utf-8 -*-
from pathlib import Path
from typing import Tuple, List, Optional, Dict, Any

import pandas as pd

from Chan import CChan
from ChanConfig import CChanConfig
from Common.CEnum import KL_TYPE
from KLine.KLine_List import CKLine_List


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
  从 chan.py 框架加载数据，返回 CChan 对象、KL_TYPE 和 CKLine_List
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
    if level not in data_src_map:
      raise ValueError(f"Unsupported intraday level: {level}")
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
    for klu in getattr(ck, "lst", []) or []:
      trade_info = getattr(klu, "trade_info", None)
      volume = 0
      if trade_info and getattr(trade_info, "metric", None):
        volume = trade_info.metric.get("volume", 0)

      rows.append(
        {
          "time": klu.time.to_str() if getattr(klu, "time", None) else None,
          "open": getattr(klu, "open", None),
          "high": getattr(klu, "high", None),
          "low": getattr(klu, "low", None),
          "close": getattr(klu, "close", None),
          "volume": volume,
          "idx": getattr(klu, "idx", None),
        }
      )

  return pd.DataFrame(rows)


def extract_bi_data(kl_list: CKLine_List) -> List[Dict[str, Any]]:
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


def extract_zs_data(kl_list: CKLine_List) -> List[Dict[str, Any]]:
  zs_list_obj = getattr(kl_list, "zs_list", None)
  if zs_list_obj is None:
    return []

  zs_raw = (
    getattr(zs_list_obj, "zs_lst", None) or getattr(zs_list_obj, "lst", None) or []
  )
  results = []

  for i, zs in enumerate(zs_raw):
    bi_lst = getattr(zs, "bi_lst", []) or []

    begin_bi = getattr(zs, "begin_bi", None)
    end_bi = getattr(zs, "end_bi", None)
    bi_in = getattr(zs, "bi_in", None)
    bi_out = getattr(zs, "bi_out", None)
    begin = getattr(zs, "begin", None)
    end = getattr(zs, "end", None)

    results.append(
      {
        "zs_id": i,
        "begin_bi_id": begin_bi.idx if begin_bi else None,
        "end_bi_id": end_bi.idx if end_bi else None,
        "bi_in_id": bi_in.idx if bi_in else None,
        "bi_out_id": bi_out.idx if bi_out else None,
        "begin_time": begin.time.to_str()
        if begin and getattr(begin, "time", None)
        else None,
        "end_time": end.time.to_str() if end and getattr(end, "time", None) else None,
        "low": getattr(zs, "low", None),
        "high": getattr(zs, "high", None),
        "peak_low": getattr(zs, "peak_low", None),
        "peak_high": getattr(zs, "peak_high", None),
        "bi_count": len(bi_lst),
        "bi_id_list": [x.idx for x in bi_lst],
      }
    )

  return results


def extract_bsp_data(kl_list: CKLine_List) -> List[Dict[str, Any]]:
  bsp_obj = getattr(kl_list, "bs_point_lst", None)
  if bsp_obj is None:
    return []

  bsp_raw = []
  if hasattr(bsp_obj, "getSortedBspList"):
    bsp_raw = list(bsp_obj.getSortedBspList() or [])
  elif hasattr(bsp_obj, "bsp_iter"):
    bsp_raw = list(bsp_obj.bsp_iter() or [])
  elif hasattr(bsp_obj, "bsp_iter_v2"):
    bsp_raw = list(bsp_obj.bsp_iter_v2() or [])
  else:
    bsp_raw = getattr(bsp_obj, "lst", []) or []

  results = []
  for i, bsp in enumerate(bsp_raw):
    bi_obj = getattr(bsp, "bi", None)
    klu_obj = getattr(bsp, "klu", None) or getattr(bsp, "Klu", None)

    raw_types = getattr(bsp, "type", None) or getattr(bsp, "types", None) or []
    if not isinstance(raw_types, (list, tuple)):
      raw_types = [raw_types]

    types = []
    for t in raw_types:
      s = str(t)
      if "." in s:
        s = s.split(".")[-1]
      types.append(s)

    price = None
    if klu_obj is not None:
      price = getattr(klu_obj, "close", None)
      if price is None:
        price = getattr(klu_obj, "low", None)
      if price is None:
        price = getattr(klu_obj, "high", None)

    results.append(
      {
        "bsp_id": i,
        "bi_id": bi_obj.idx if bi_obj else None,
        "klu_index": klu_obj.idx if klu_obj else None,
        "time": klu_obj.time.to_str()
        if klu_obj and getattr(klu_obj, "time", None)
        else None,
        "price": price,
        "is_buy": bool(getattr(bsp, "is_buy", False)),
        "types": types,
        "is_sure": bi_obj.is_sure if bi_obj else None,
      }
    )

  return results
