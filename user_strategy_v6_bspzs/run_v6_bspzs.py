from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from .config import StrategyConfig
from user_strategy_v6_bspzs.chan_loader import load_chan_data


def save_df(df: pd.DataFrame, path: Path) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  df.to_csv(path, index=False, encoding="utf-8-sig")


def get_all_symbols(data_dir: Path) -> List[str]:
  symbols = set()
  for file in data_dir.glob("*.csv"):
    parts = file.name.split("_")
    if parts:
      symbols.add(parts[0])
  return sorted(list(symbols))


def normalize_timeframe(tf: str) -> str:
  if tf is None:
    return ""
  s = str(tf).strip().lower()
  mapping = {
    "1day": "1d",
    "1d": "1d",
    "4h": "4h",
    "2h": "2h",
    "1h": "1h",
    "60m": "1h",
    "120m": "2h",
    "240m": "4h",
  }
  return mapping.get(s, s)


def format_timeframe_label(tf: str) -> str:
  tf = normalize_timeframe(tf)
  return tf.upper() if tf else ""


def normalize_time_value(v: Any) -> Optional[str]:
  if v is None:
    return None
  s = str(v).strip()
  if not s:
    return None

  ts = pd.to_datetime(s, errors="coerce")
  if pd.notna(ts):
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
      return ts.strftime("%Y-%m-%d")
    return ts.strftime("%Y-%m-%d %H:%M:%S")

  s = s.replace("/", "-")
  return s


def format_price(v: Any) -> str:
  if pd.isna(v):
    return ""
  try:
    return f"{float(v):.4f}".rstrip("0").rstrip(".")
  except Exception:
    return str(v)


def normalize_bsp_type_token(token: str) -> str:
  s = str(token).strip().upper()
  mapping = {
    "T1": "1",
    "T2": "2",
    "T3A": "3a",
    "T3B": "3b",
    "1": "1",
    "2": "2",
    "3A": "3a",
    "3B": "3b",
  }
  return mapping.get(s, str(token).strip())


def normalize_bsp_types(raw_types: Any) -> List[str]:
  if raw_types is None:
    return []
  if not isinstance(raw_types, (list, tuple, set)):
    raw_types = [raw_types]

  result: List[str] = []
  for item in raw_types:
    if item is None:
      continue
    s = str(item).strip()
    if not s:
      continue
    if "." in s:
      s = s.split(".")[-1]
    norm = normalize_bsp_type_token(s)
    if norm and norm not in result:
      result.append(norm)
  return result


def deduplicate_signal_events(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  out = df.copy()
  for col in [
    "symbol",
    "timeframe",
    "eventtype",
    "eventtime",
    "reason",
    "biid",
    "barindex",
    "tradeid",
    "eventseq",
  ]:
    if col not in out.columns:
      out[col] = None

  out = out.sort_values(
    by=["symbol", "timeframe", "eventtime", "barindex", "eventseq"],
    na_position="last",
  ).reset_index(drop=True)

  out = out.drop_duplicates(
    subset=[
      "symbol",
      "timeframe",
      "eventtype",
      "eventtime",
      "reason",
      "biid",
      "barindex",
      "tradeid",
    ],
    keep="first",
  ).reset_index(drop=True)

  if "eventseq" in out.columns:
    out["eventseq"] = range(1, len(out) + 1)
  return out


def ensure_event_columns(df: pd.DataFrame) -> pd.DataFrame:
  out = df.copy()

  for col in [
    "symbol",
    "timeframe",
    "eventtype",
    "eventtime",
    "eventdate",
    "price",
    "stopprice",
    "biid",
    "reason",
    "signaltext",
    "eventcount",
  ]:
    if col not in out.columns:
      out[col] = None

  if "latestprice" not in out.columns:
    out["latestprice"] = out["price"] if "price" in out.columns else None
  if "latesteventtype" not in out.columns:
    out["latesteventtype"] = out["eventtype"]
  if "latesteventtime" not in out.columns:
    out["latesteventtime"] = out["eventtime"]

  out["timeframe"] = out["timeframe"].map(normalize_timeframe)
  out["eventtimedt"] = pd.to_datetime(out["eventtime"], errors="coerce")
  out["eventdatedt"] = pd.to_datetime(out["eventdate"], errors="coerce")

  out["eventdatedt"] = out["eventdatedt"].where(
    out["eventdatedt"].notna(), out["eventtimedt"].dt.normalize()
  )

  out["eventdatestr"] = out["eventdatedt"].dt.strftime("%Y-%m-%d").fillna("")
  out["eventtimestr"] = out["eventtimedt"].dt.strftime("%Y-%m-%d %H:%M:%S").fillna("")

  return out


def build_readable_signal_events(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame(
      columns=[
        "symbol",
        "timeframe",
        "eventdate",
        "latesteventtype",
        "latesteventtime",
        "latestprice",
        "stopprice",
        "biid",
        "reason",
        "signaltext",
        "eventcount",
        "readabletext",
      ]
    )

  x = ensure_event_columns(df)
  x = x.sort_values(
    by=["symbol", "timeframe", "eventtimedt", "biid"], na_position="last"
  ).reset_index(drop=True)

  def event_label(eventtype: str) -> str:
    mapping = {
      "ZS_FORMED": "新中枢形成",
      "BSP1_BUY": "1类买点",
      "BSP1_SELL": "1类卖点",
      "BSP2_BUY": "2类买点",
      "BSP2_SELL": "2类卖点",
      "BSP3_BUY": "3类买点",
      "BSP3_SELL": "3类卖点",
    }
    return mapping.get(str(eventtype), str(eventtype))

  def to_text(row: pd.Series) -> str:
    symbol = row.get("symbol", "")
    timeframe = format_timeframe_label(row.get("timeframe", ""))
    eventtime = row.get("eventtime", "") or row.get("latesteventtime", "")
    eventtype = event_label(row.get("eventtype", row.get("latesteventtype", "")))
    price = format_price(row.get("price", row.get("latestprice", None)))
    signaltext = row.get("signaltext", "")
    reason = row.get("reason", "")
    extras = []
    if price:
      extras.append(f"price={price}")
    if signaltext:
      extras.append(str(signaltext))
    if reason:
      extras.append(f"reason={reason}")
    extra_text = " | ".join(extras)
    return f"{symbol} {timeframe} {eventtime} {eventtype}" + (
      f" | {extra_text}" if extra_text else ""
    )

  x["readabletext"] = x.apply(to_text, axis=1)

  return x[
    [
      "symbol",
      "timeframe",
      "eventdate",
      "latesteventtype",
      "latesteventtime",
      "latestprice",
      "stopprice",
      "biid",
      "reason",
      "signaltext",
      "eventcount",
      "readabletext",
    ]
  ].copy()


def build_last_events_per_symbol_timeframe(readable_df: pd.DataFrame) -> pd.DataFrame:
  if readable_df is None or readable_df.empty:
    return pd.DataFrame(
      columns=[
        "symbol",
        "timeframe",
        "eventdate",
        "latesteventtype",
        "latesteventtime",
        "latestprice",
        "stopprice",
        "biid",
        "reason",
        "signaltext",
        "eventcount",
        "readabletext",
      ]
    )

  x = readable_df.copy()
  x["eventdatedt"] = pd.to_datetime(x["eventdate"], errors="coerce")
  x["latesteventtimedt"] = pd.to_datetime(x["latesteventtime"], errors="coerce")
  x["timeframe"] = x["timeframe"].map(normalize_timeframe)

  x = x.sort_values(
    by=["symbol", "timeframe", "latesteventtimedt", "biid"], na_position="last"
  )

  latest = (
    x.groupby(["symbol", "timeframe"], as_index=False, group_keys=False).tail(1).copy()
  )

  latest = (
    latest[
      [
        "symbol",
        "timeframe",
        "eventdate",
        "latesteventtype",
        "latesteventtime",
        "latestprice",
        "stopprice",
        "biid",
        "reason",
        "signaltext",
        "eventcount",
        "readabletext",
      ]
    ]
    .sort_values(by=["symbol", "timeframe"])
    .reset_index(drop=True)
  )

  return latest


def event_type_rank(eventtype: str) -> int:
  rank = {
    "BSP3_SELL": 7,
    "BSP2_SELL": 6,
    "BSP1_SELL": 5,
    "BSP3_BUY": 4,
    "BSP2_BUY": 3,
    "BSP1_BUY": 2,
    "ZS_FORMED": 1,
  }
  return rank.get(str(eventtype), 0)


def build_last_digest_by_symbol(
  last_df: pd.DataFrame,
  freshdays: int = 2,
  reference_date: Optional[str] = None,
) -> pd.DataFrame:
  if last_df is None or last_df.empty:
    cols = [
      "symbol",
      "signaldate",
      "referencedate",
      "freshdays",
      "1deventtype",
      "1dsignaltext",
      "1deventtime",
      "1dlatestprice",
      "4heventtype",
      "4hsignaltext",
      "4heventtime",
      "4hlatestprice",
      "2heventtype",
      "2hsignaltext",
      "2heventtime",
      "2hlatestprice",
      "1heventtype",
      "1hsignaltext",
      "1heventtime",
      "1hlatestprice",
      "hassignal",
      "has_signal",
      "hastradingsignal",
      "summarytext",
      "summary_text",
    ]
    return pd.DataFrame(columns=cols)

  timeframe_order = ["1d", "4h", "2h", "1h"]
  trading_event_types = {
    "BSP1_BUY",
    "BSP1_SELL",
    "BSP2_BUY",
    "BSP2_SELL",
    "BSP3_BUY",
    "BSP3_SELL",
  }

  x = last_df.copy()
  x["timeframe"] = x["timeframe"].map(normalize_timeframe)
  x["eventdatedt"] = pd.to_datetime(x["eventdate"], errors="coerce")
  x["latesteventtimedt"] = pd.to_datetime(x["latesteventtime"], errors="coerce")

  if reference_date is not None and str(reference_date).strip():
    global_reference_dt = pd.to_datetime(reference_date, errors="coerce")
  else:
    global_reference_dt = x["eventdatedt"].max()

  rows = []
  for symbol, g in x.groupby("symbol", sort=True):
    referencedate = (
      global_reference_dt.strftime("%Y-%m-%d") if pd.notna(global_reference_dt) else ""
    )

    item: Dict[str, Any] = {
      "symbol": symbol,
      "signaldate": referencedate,
      "referencedate": referencedate,
      "freshdays": freshdays,
    }

    for tf in timeframe_order:
      sub = g[g["timeframe"] == tf].copy()
      if sub.empty:
        item[f"{tf}eventtype"] = ""
        item[f"{tf}signaltext"] = ""
        item[f"{tf}eventtime"] = ""
        item[f"{tf}latestprice"] = ""
        continue

      sub = sub.sort_values(
        by=["eventdatedt", "latesteventtimedt", "biid"],
        na_position="last",
      )
      r = sub.iloc[-1]

      eventdatedt = r.get("eventdatedt", pd.NaT)
      isfresh = False
      if pd.notna(global_reference_dt) and pd.notna(eventdatedt):
        agedays = int((global_reference_dt - eventdatedt).days)
        isfresh = agedays <= freshdays

      if isfresh:
        item[f"{tf}eventtype"] = r.get("latesteventtype", "")
        item[f"{tf}signaltext"] = r.get("signaltext", "")
        item[f"{tf}eventtime"] = r.get("latesteventtime", "")
        item[f"{tf}latestprice"] = format_price(r.get("latestprice", None))
      else:
        item[f"{tf}eventtype"] = ""
        item[f"{tf}signaltext"] = ""
        item[f"{tf}eventtime"] = ""
        item[f"{tf}latestprice"] = ""

    visible_event_types = [item.get(f"{tf}eventtype", "") for tf in timeframe_order]
    item["hassignal"] = any(bool(v) for v in visible_event_types)
    item["hastradingsignal"] = any(
      v in trading_event_types for v in visible_event_types
    )

    lines = []
    for tf in timeframe_order:
      et = item.get(f"{tf}eventtype", "")
      st = item.get(f"{tf}signaltext", "")
      tm = item.get(f"{tf}eventtime", "")
      pr = item.get(f"{tf}latestprice", "")
      label = tf.upper()
      if et:
        extras = []
        if st:
          extras.append(st)
        if tm:
          extras.append(f"time={tm}")
        if pr:
          extras.append(f"price={pr}")
        lines.append(f"{label}: {et}" + (f" ({', '.join(extras)})" if extras else ""))
      else:
        lines.append(f"{label}: -")

    item["summarytext"] = f"{symbol} | ref={referencedate} | " + " ; ".join(lines)
    rows.append(item)

  out = pd.DataFrame(rows)

  def symbol_rank(row: pd.Series) -> int:
    ranks = [event_type_rank(row.get(f"{tf}eventtype", "")) for tf in timeframe_order]
    return max(ranks) if ranks else 0

  out["_rank"] = out.apply(symbol_rank, axis=1)
  out = (
    out.sort_values(
      by=["hastradingsignal", "_rank", "symbol"],
      ascending=[False, False, True],
    )
    .drop(columns=["_rank"])
    .reset_index(drop=True)
  )
  out["has_signal"] = out["hassignal"]
  out["summary_text"] = out["summarytext"]

  return out


def filter_trading_digest(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()
  if "hastradingsignal" not in df.columns:
    return pd.DataFrame()
  return df[df["hastradingsignal"] == True].copy().reset_index(drop=True)


def build_v6_signal_digest(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  keep_event_types = {
    "ZS_FORMED",
    "BSP1_BUY",
    "BSP1_SELL",
    "BSP2_BUY",
    "BSP2_SELL",
    "BSP3_BUY",
    "BSP3_SELL",
  }

  x = df[df["eventtype"].isin(keep_event_types)].copy()
  if x.empty:
    return pd.DataFrame()

  for col in [
    "symbol",
    "timeframe",
    "eventtime",
    "eventseq",
    "price",
    "reason",
    "signaltext",
  ]:
    if col not in x.columns:
      x[col] = None

  x["eventdate"] = x["eventtime"].astype(str).str.slice(0, 10)
  x = x.sort_values(
    by=["symbol", "timeframe", "eventtime", "eventseq"], na_position="last"
  ).reset_index(drop=True)

  grouped_rows = []
  for (symbol, eventdate, timeframe), grp in x.groupby(
    ["symbol", "eventdate", "timeframe"], dropna=False
  ):
    latest = grp.iloc[-1]
    grouped_rows.append(
      {
        "symbol": symbol,
        "eventdate": eventdate,
        "timeframe": timeframe,
        "latesteventtype": latest.get("eventtype", ""),
        "latesteventtime": latest.get("eventtime", ""),
        "latestprice": latest.get("price", None),
        "stopprice": latest.get("stopprice", None),
        "biid": latest.get("biid", None),
        "reason": latest.get("reason", ""),
        "signaltext": latest.get("signaltext", ""),
        "eventcount": int(len(grp)),
      }
    )

  digest_df = pd.DataFrame(grouped_rows)
  if digest_df.empty:
    return digest_df

  digest_df = digest_df.sort_values(
    by=["eventdate", "symbol", "timeframe", "latesteventtime"],
    ascending=[True, True, True, True],
    na_position="last",
  ).reset_index(drop=True)
  return digest_df


def ensure_bsp_columns(df: pd.DataFrame) -> pd.DataFrame:
  out = df.copy()

  rename_map = {
    "biidx": "bi_idx",
    "kluidx": "klu_idx",
    "isbuy": "is_buy",
    "issure": "is_sure",
  }
  for old, new in rename_map.items():
    if old in out.columns and new not in out.columns:
      out = out.rename(columns={old: new})

  for col in [
    "idx",
    "bi_idx",
    "klu_idx",
    "time",
    "price",
    "is_buy",
    "types",
    "is_sure",
  ]:
    if col not in out.columns:
      out[col] = None

  def _norm_types(v: Any) -> str:
    if isinstance(v, str):
      vv = v.strip()
      if vv.startswith("[") and vv.endswith("]"):
        vv = vv[1:-1]
      items = [x.strip().strip("'").strip('"') for x in vv.split(",") if x.strip()]
    elif isinstance(v, (list, tuple, set)):
      items = [str(x).strip().strip("'").strip('"') for x in v]
    else:
      items = [v]
    return ",".join(normalize_bsp_types(items))

  out["types_raw"] = out["types"]
  out["types"] = out["types"].apply(_norm_types)
  out["time"] = out["time"].map(normalize_time_value)
  out["is_buy"] = out["is_buy"].fillna(False).astype(bool)

  return out


def ensure_zs_columns(df: pd.DataFrame) -> pd.DataFrame:
  out = df.copy()

  rename_map = {
    "beginbiidx": "begin_bi_idx",
    "endbiidx": "end_bi_idx",
    "biinidx": "bi_in_idx",
    "bioutidx": "bi_out_idx",
    "begintime": "begin_time",
    "endtime": "end_time",
    "peaklow": "peak_low",
    "peakhigh": "peak_high",
    "biidxlist": "bi_idx_list",
  }
  for old, new in rename_map.items():
    if old in out.columns and new not in out.columns:
      out = out.rename(columns={old: new})

  for col in [
    "idx",
    "begin_bi_idx",
    "end_bi_idx",
    "bi_in_idx",
    "bi_out_idx",
    "begin_time",
    "end_time",
    "low",
    "high",
    "peak_low",
    "peak_high",
    "bi_idx_list",
  ]:
    if col not in out.columns:
      out[col] = None

  out["begin_time"] = out["begin_time"].map(normalize_time_value)
  out["end_time"] = out["end_time"].map(normalize_time_value)
  return out


def extract_zs_data_from_chan_object(kllist: Any) -> pd.DataFrame:
  zs_raw = getattr(kllist, "zs_list", None) or getattr(kllist, "zs_lst", None) or []
  rows: List[Dict[str, Any]] = []

  for i, zs in enumerate(zs_raw):
    bi_list = getattr(zs, "bi_lst", None) or getattr(zs, "bilst", None) or []
    rows.append(
      {
        "idx": i,
        "begin_bi_idx": getattr(getattr(zs, "begin_bi", None), "idx", None),
        "end_bi_idx": getattr(getattr(zs, "end_bi", None), "idx", None),
        "bi_in_idx": getattr(getattr(zs, "bi_in", None), "idx", None),
        "bi_out_idx": getattr(getattr(zs, "bi_out", None), "idx", None),
        "begin_time": getattr(getattr(zs, "begin", None), "time", None),
        "end_time": getattr(getattr(zs, "end", None), "time", None),
        "low": getattr(zs, "low", None),
        "high": getattr(zs, "high", None),
        "peak_low": getattr(zs, "peak_low", None)
        if hasattr(zs, "peak_low")
        else getattr(zs, "peaklow", None),
        "peak_high": getattr(zs, "peak_high", None)
        if hasattr(zs, "peak_high")
        else getattr(zs, "peakhigh", None),
        "bi_idx_list": [getattr(x, "idx", None) for x in bi_list],
      }
    )

  return pd.DataFrame(rows)


def get_sorted_bsp_list(bsp_obj: Any) -> List[Any]:
  if bsp_obj is None:
    return []
  if hasattr(bsp_obj, "getSortedBspList"):
    try:
      return list(bsp_obj.getSortedBspList() or [])
    except Exception:
      pass
  if hasattr(bsp_obj, "bsp_iter"):
    try:
      return list(bsp_obj.bsp_iter() or [])
    except Exception:
      pass
  if hasattr(bsp_obj, "bsp_iter_v2"):
    try:
      return list(bsp_obj.bsp_iter_v2() or [])
    except Exception:
      pass
  if hasattr(bsp_obj, "lst"):
    try:
      return list(bsp_obj.lst or [])
    except Exception:
      pass
  return []


def extract_bsp_data_from_chan_object(kllist: Any) -> pd.DataFrame:
  bsp_obj = (
    getattr(kllist, "bs_point_lst", None)
    or getattr(kllist, "bspoint_lst", None)
    or getattr(kllist, "bspointlst", None)
  )
  bsp_raw_list = get_sorted_bsp_list(bsp_obj)

  rows: List[Dict[str, Any]] = []
  for i, bsp in enumerate(bsp_raw_list):
    biobj = getattr(bsp, "bi", None)
    kluobj = getattr(bsp, "klu", None) or getattr(bsp, "Klu", None)

    raw_types = getattr(bsp, "type", None) or getattr(bsp, "types", None) or []
    if not isinstance(raw_types, (list, tuple, set)):
      raw_types = [raw_types]

    clean_types = []
    for t in raw_types:
      if t is None:
        continue
      s = str(t).strip()
      if "." in s:
        s = s.split(".")[-1]
      clean_types.append(s)

    price = None
    if kluobj is not None:
      price = getattr(kluobj, "close", None)
      if price is None:
        price = getattr(kluobj, "low", None)
      if price is None:
        price = getattr(kluobj, "high", None)

    rows.append(
      {
        "idx": i,
        "bi_idx": getattr(biobj, "idx", None) if biobj is not None else None,
        "klu_idx": getattr(kluobj, "idx", None) if kluobj is not None else None,
        "time": getattr(kluobj, "time", None) if kluobj is not None else None,
        "price": price,
        "is_buy": bool(
          getattr(bsp, "is_buy", False)
          if hasattr(bsp, "is_buy")
          else getattr(bsp, "isbuy", False)
        ),
        "types": clean_types,
        "is_sure": getattr(biobj, "is_sure", None)
        if hasattr(biobj, "is_sure")
        else getattr(biobj, "issure", None)
        if biobj is not None
        else None,
      }
    )

  return pd.DataFrame(rows)


def build_v6_signal_events(
  symbol: str,
  timeframe: str,
  zs_df: pd.DataFrame,
  bsp_df: pd.DataFrame,
) -> pd.DataFrame:
  symbol = str(symbol).upper().strip()
  timeframe = normalize_timeframe(timeframe)

  rows: List[Dict[str, Any]] = []
  eventseq = 0

  zdf = (
    ensure_zs_columns(zs_df)
    if zs_df is not None and not zs_df.empty
    else pd.DataFrame()
  )

  bdf = (
    ensure_bsp_columns(bsp_df)
    if bsp_df is not None and not bsp_df.empty
    else pd.DataFrame()
  )

  if not zdf.empty:
    zdf = zdf.sort_values(by=["end_time", "idx"], na_position="last").reset_index(
      drop=True
    )

    for _, row in zdf.iterrows():
      eventtime = row.get("end_time")
      eventdate = ""
      ts = pd.to_datetime(eventtime, errors="coerce")
      if pd.notna(ts):
        eventdate = ts.strftime("%Y-%m-%d")

      eventseq += 1
      low = row.get("low", None)
      high = row.get("high", None)

      rows.append(
        {
          "eventseq": eventseq,
          "symbol": symbol,
          "timeframe": timeframe,
          "eventtype": "ZS_FORMED",
          "eventtime": eventtime,
          "eventdate": eventdate,
          "barindex": None,
          "price": None,
          "stopprice": None,
          "plannedexitidx": None,
          "plannedexittime": None,
          "triggerpriceref": "",
          "reason": "zsendconfirmed",
          "signaltext": f"新中枢形成 区间 {format_price(low)}-{format_price(high)}",
          "tradeid": None,
          "structuretype": "zs",
          "biid": row.get("end_bi_idx", None),
          "bidirection": None,
          "bistartindex": row.get("begin_bi_idx", None),
          "bistarttime": row.get("begin_time", None),
          "bistartprice": low,
          "biendindex": row.get("end_bi_idx", None),
          "biendtime": row.get("end_time", None),
          "biendprice": high,
          "bibars": None,
          "biissure": True,
          "entryidx": None,
          "entrytime": None,
          "entryprice": None,
          "exitidx": None,
          "exittimeactual": None,
          "exitpriceactual": None,
          "pnlabs": None,
          "pnlpct": None,
          "zs_idx": row.get("idx", None),
          "zs_low": low,
          "zs_high": high,
          "peak_low": row.get("peak_low", None),
          "peak_high": row.get("peak_high", None),
          "bsp_type": None,
          "bsp_type_raw": None,
          "source_row_idx": row.get("idx", None),
        }
      )

  if not bdf.empty:
    bdf = bdf.sort_values(
      by=["time", "klu_idx", "idx"], na_position="last"
    ).reset_index(drop=True)

    for _, row in bdf.iterrows():
      types_str = str(row.get("types", "") or "").strip()
      raw_types_str = row.get("types_raw", None)

      if isinstance(raw_types_str, str):
        vv = raw_types_str.strip()
        if vv.startswith("[") and vv.endswith("]"):
          vv = vv[1:-1]
        raw_types = [
          x.strip().strip("'").strip('"') for x in vv.split(",") if x.strip()
        ]
      elif isinstance(raw_types_str, (list, tuple, set)):
        raw_types = [str(x).strip() for x in raw_types_str if str(x).strip()]
      else:
        raw_types = []

      norm_types = [x.strip() for x in types_str.split(",") if x.strip()]
      raw_map = {}
      for raw_t in raw_types:
        norm_t = normalize_bsp_type_token(raw_t)
        raw_map.setdefault(norm_t, raw_t)

      for t in norm_types:
        is_buy = bool(row.get("is_buy", False))

        if t == "1":
          eventtype = "BSP1_BUY" if is_buy else "BSP1_SELL"
          signaltext = "1类买点" if is_buy else "1类卖点"
        elif t == "2":
          eventtype = "BSP2_BUY" if is_buy else "BSP2_SELL"
          signaltext = "2类买点" if is_buy else "2类卖点"
        elif t == "3a":
          eventtype = "BSP3_BUY" if is_buy else "BSP3_SELL"
          signaltext = "3类买点(3a)" if is_buy else "3类卖点(3a)"
        elif t == "3b":
          eventtype = "BSP3_BUY" if is_buy else "BSP3_SELL"
          signaltext = "3类买点(3b)" if is_buy else "3类卖点(3b)"
        else:
          continue

        eventtime = row.get("time", None)
        eventdate = ""
        ts = pd.to_datetime(eventtime, errors="coerce")
        if pd.notna(ts):
          eventdate = ts.strftime("%Y-%m-%d")

        eventseq += 1
        rows.append(
          {
            "eventseq": eventseq,
            "symbol": symbol,
            "timeframe": timeframe,
            "eventtype": eventtype,
            "eventtime": eventtime,
            "eventdate": eventdate,
            "barindex": row.get("klu_idx", None),
            "price": row.get("price", None),
            "stopprice": None,
            "plannedexitidx": None,
            "plannedexittime": None,
            "triggerpriceref": "bsp_price",
            "reason": f"chan_bsp_{t}_{'buy' if is_buy else 'sell'}",
            "signaltext": signaltext,
            "tradeid": None,
            "structuretype": "bsp",
            "biid": row.get("bi_idx", None),
            "bidirection": "buy" if is_buy else "sell",
            "bistartindex": None,
            "bistarttime": None,
            "bistartprice": None,
            "biendindex": row.get("bi_idx", None),
            "biendtime": eventtime,
            "biendprice": row.get("price", None),
            "bibars": None,
            "biissure": row.get("is_sure", None),
            "entryidx": None,
            "entrytime": None,
            "entryprice": None,
            "exitidx": None,
            "exittimeactual": None,
            "exitpriceactual": None,
            "pnlabs": None,
            "pnlpct": None,
            "zs_idx": None,
            "zs_low": None,
            "zs_high": None,
            "peak_low": None,
            "peak_high": None,
            "bsp_type": t,
            "bsp_type_raw": raw_map.get(t, None),
            "source_row_idx": row.get("idx", None),
          }
        )

  if not rows:
    return pd.DataFrame(
      columns=[
        "eventseq",
        "symbol",
        "timeframe",
        "eventtype",
        "eventtime",
        "eventdate",
        "barindex",
        "price",
        "stopprice",
        "plannedexitidx",
        "plannedexittime",
        "triggerpriceref",
        "reason",
        "signaltext",
        "tradeid",
        "structuretype",
        "biid",
        "bidirection",
        "bistartindex",
        "bistarttime",
        "bistartprice",
        "biendindex",
        "biendtime",
        "biendprice",
        "bibars",
        "biissure",
        "entryidx",
        "entrytime",
        "entryprice",
        "exitidx",
        "exittimeactual",
        "exitpriceactual",
        "pnlabs",
        "pnlpct",
        "zs_idx",
        "zs_low",
        "zs_high",
        "peak_low",
        "peak_high",
        "bsp_type",
        "bsp_type_raw",
        "source_row_idx",
      ]
    )

  out = pd.DataFrame(rows)
  out = out.sort_values(
    by=["symbol", "timeframe", "eventtime", "barindex", "eventseq"],
    na_position="last",
  ).reset_index(drop=True)
  out["eventseq"] = range(1, len(out) + 1)
  return out


# def load_chan_data(
#  code: str,
#  level: str,
#  csvpath: Path,
#  config: dict,
#  triggerstep: bool = True,
#  begin_time: Optional[str] = None,
#  end_time: Optional[str] = None,
# ):
#  from Chan import CChan
#  from ChanConfig import CChanConfig
#  from Common.CEnum import KL_TYPE
#
#  level = str(level).upper().strip()
#
#  if level == "1D":
#    kltype = KL_TYPE.K_DAY
#    data_src = "custom:OfflineUsDailyCsvAPI.COfflineUsDailyCsvAPI"
#  elif level == "4H":
#    kltype = KL_TYPE.K_60M
#    data_src = "custom:OfflineYFinanceIntradayCsvAPI.COfflineYFinance4HCsvAPI"
#  elif level == "2H":
#    kltype = KL_TYPE.K_60M
#    data_src = "custom:OfflineYFinanceIntradayCsvAPI.COfflineYFinance2HCsvAPI"
#  elif level == "1H":
#    kltype = KL_TYPE.K_60M
#    data_src = "custom:OfflineYFinanceIntradayCsvAPI.COfflineYFinance1HCsvAPI"
#  else:
#    raise ValueError(f"unsupported level: {level}")
#
#  chan_config = CChanConfig(
#    {
#      "bi_algo": config.get("bi_algo", "normal"),
#      "trigger_step": config.get("trigger_step", True),
#      "skip_step": config.get("skip_step", 0),
#      "divergence_rate": config.get("divergence_rate", float("inf")),
#      "bsp2_follow_1": config.get("bsp2_follow_1", True),
#      "bsp3_follow_1": config.get("bsp3_follow_1", True),
#      "strict_bsp3": config.get("strict_bsp3", False),
#      "bsp3_peak": config.get("bsp3_peak", False),
#      "bsp2s_follow_2": config.get("bsp2s_follow_2", False),
#      "max_bs2_rate": config.get("max_bs2_rate", 0.9999),
#      "macd_algo": config.get("macd_algo", "peak"),
#      "bs1_peak": config.get("bs1_peak", False),
#      "bs_type": config.get("bs_type", "1,2,3a,3b"),
#      "bsp1_only_multibi_zs": config.get("bsp1_only_multibi_zs", False),
#      "min_zs_cnt": config.get("min_zs_cnt", 0),
#    }
#  )
#
#  chan = CChan(
#    code=code.upper(),
#    begin_time=begin_time,
#    end_time=end_time,
#    data_src=data_src,
#    lv_list=[kltype],
#    config=chan_config,
#  )
#
#  def _extract_kllist_from_snapshot(snapshot: Any, kltype_obj: Any):
#    if snapshot is None:
#      return None
#
#    if isinstance(snapshot, dict):
#      if kltype_obj in snapshot:
#        return snapshot[kltype_obj]
#      if str(kltype_obj) in snapshot:
#        return snapshot[str(kltype_obj)]
#      vals = list(snapshot.values())
#      if len(vals) == 1:
#        return vals[0]
#
#    try:
#      return snapshot[kltype_obj]
#    except Exception:
#      pass
#
#    if hasattr(snapshot, "get"):
#      try:
#        val = snapshot.get(kltype_obj)
#        if val is not None:
#          return val
#      except Exception:
#        pass
#
#    if hasattr(snapshot, "kl_datas"):
#      try:
#        val = snapshot.kl_datas[kltype_obj]
#        if val is not None:
#          return val
#      except Exception:
#        pass
#
#    return None
#
#  last_kllist = None
#
#  if triggerstep:
#    for snapshot in chan.step_load():
#      extracted = _extract_kllist_from_snapshot(snapshot, kltype)
#      if extracted is not None:
#        last_kllist = extracted
#
#    if last_kllist is None:
#      try:
#        last_kllist = chan.kl_datas[kltype]
#      except Exception:
#        pass
#  else:
#    last_kllist = chan.kl_datas[kltype]
#
#  if last_kllist is None:
#    raise ValueError("chan step_load returned no usable snapshot")
#
#  return chan, kltype, last_kllist


def extract_kline_data(kllist: Any) -> pd.DataFrame:
  cklist = getattr(kllist, "lst", None) or []
  rows: List[Dict[str, Any]] = []

  for ck in cklist:
    klu_list = getattr(ck, "lst", None) or []
    for klu in klu_list:
      rows.append(
        {
          "idx": getattr(klu, "idx", None),
          "time": getattr(klu, "time", None),
          "open": getattr(klu, "open", None),
          "high": getattr(klu, "high", None),
          "low": getattr(klu, "low", None),
          "close": getattr(klu, "close", None),
          "volume": getattr(klu, "volume", None),
        }
      )

  df = pd.DataFrame(rows)
  if not df.empty and "time" in df.columns:
    df["time"] = df["time"].map(normalize_time_value)
  return df


def find_csv_matches(
  data_dir: Path, symbol: str, tf_level: str, tf_name: str
) -> List[Path]:
  symbol = str(symbol).upper().strip()
  tf_level = str(tf_level).upper().strip()
  tf_name = str(tf_name).lower().strip()

  patterns = []
  if tf_level == "1D":
    patterns = [
      f"{symbol}_*_1d.csv",
      f"{symbol}_1d.csv",
    ]
  else:
    patterns = [
      f"{symbol}_*_yf_{tf_name}_730d.csv",
      f"{symbol}_yf_{tf_name}_730d.csv",
      f"{symbol}_*_{tf_name}.csv",
    ]

  matches: List[Path] = []
  for pattern in patterns:
    matches.extend(list(data_dir.glob(pattern)))

  unique_matches = sorted(set(matches))
  return unique_matches


def main() -> None:
  repo_root = Path(__file__).resolve().parent.parent
  conf = StrategyConfig()

  if hasattr(conf, "resolved_datadir"):
    data_dir = conf.resolved_datadir(repo_root)
  elif hasattr(conf, "resolved_data_dir"):
    data_dir = conf.resolved_data_dir(repo_root)
  else:
    data_dir = repo_root / "data"

  out_dir = Path(__file__).resolve().parent / "results"
  out_dir.mkdir(parents=True, exist_ok=True)

  symbols = getattr(conf, "symbols", None) or get_all_symbols(data_dir)
  symbols = [str(s).upper().strip() for s in symbols]
  print(f"symbols={len(symbols)} -> {symbols}")

  class TF:
    def __init__(self, name: str, level: str):
      self.name = name
      self.level = level

  timeframes = [
    TF("1d", "1D"),
    TF("4h", "4H"),
    TF("2h", "2H"),
    TF("1h", "1H"),
  ]

  chan_config = conf.chan_config
  # chan_config = {
  #  "bi_algo": getattr(conf, "bi_algo", "normal"),
  #  "trigger_step": True,
  #  "skip_step": getattr(conf, "skip_step", 0),
  #  "divergence_rate": getattr(conf, "divergence_rate", float("inf")),
  #  "bsp2_follow_1": getattr(conf, "bsp2_follow_1", True),
  #  "bsp3_follow_1": getattr(conf, "bsp3_follow_1", True),
  #  "strict_bsp3": getattr(conf, "strict_bsp3", False),
  #  "bsp3_peak": getattr(conf, "bsp3_peak", False),
  #  "bsp2s_follow_2": getattr(conf, "bsp2s_follow_2", False),
  #  "max_bs2_rate": getattr(conf, "max_bs2_rate", 0.9999),
  #  "macd_algo": getattr(conf, "macd_algo", "peak"),
  #  "bs1_peak": getattr(conf, "bs1_peak", False),
  #  "bs_type": getattr(conf, "bs_type", "1,2,3a,3b"),
  #  "bsp1_only_multibi_zs": getattr(conf, "bsp1_only_multibi_zs", False),
  #  "min_zs_cnt": getattr(conf, "min_zs_cnt", 0),
  # }

  market_signal_events: List[pd.DataFrame] = []
  market_signal_digest: List[pd.DataFrame] = []

  for symbol in symbols:
    print("=" * 40, symbol, "=" * 40)

    symbol_signal_events: List[pd.DataFrame] = []
    symbol_signal_digest: List[pd.DataFrame] = []

    for tf in timeframes:
      try:
        matches = find_csv_matches(data_dir, symbol, tf.level, tf.name)
        if not matches:
          print(f"{tf.name}/{tf.level} -> no csv found")
          continue

        # csvpath = matches[-1]
        # print(f"{tf.name}/{tf.level} -> {csvpath.name}")

        _, _, kllist = load_chan_data(
          code=symbol,
          level=tf.level,
          # csv_path=csvpath,
          config=chan_config,
          trigger_step=True,
          begin_time=getattr(tf, "start_time", None),
        )

        kline_df = extract_kline_data(kllist)
        zs_df = extract_zs_data_from_chan_object(kllist)
        bsp_df = extract_bsp_data_from_chan_object(kllist)
        if not bsp_df.empty and "is_sure" in bsp_df.columns:
          bsp_df = bsp_df[bsp_df["is_sure"] == True].reset_index(drop=True)
        signal_events_df = build_v6_signal_events(symbol, tf.name, zs_df, bsp_df)
        signal_events_df = deduplicate_signal_events(signal_events_df)
        signal_digest_df = build_v6_signal_digest(signal_events_df)

        save_df(kline_df, out_dir / f"{symbol}_{tf.name}_ohlcv_v6_bspzs.csv")
        save_df(zs_df, out_dir / f"{symbol}_{tf.name}_zs_v6_bspzs.csv")
        save_df(bsp_df, out_dir / f"{symbol}_{tf.name}_bsp_v6_bspzs.csv")
        save_df(
          signal_events_df, out_dir / f"{symbol}_{tf.name}_signal_events_v6_bspzs.csv"
        )
        save_df(
          signal_digest_df, out_dir / f"{symbol}_{tf.name}_signal_digest_v6_bspzs.csv"
        )

        if not signal_events_df.empty:
          symbol_signal_events.append(signal_events_df)
          market_signal_events.append(signal_events_df)

        if not signal_digest_df.empty:
          symbol_signal_digest.append(signal_digest_df)
          market_signal_digest.append(signal_digest_df)

        print(
          f"ok - zs={len(zs_df)} bsp={len(bsp_df)} "
          f"events={len(signal_events_df)} digest={len(signal_digest_df)}"
        )

      except Exception as e:
        print(f"error - {symbol} {tf.name}: {e}")

    if symbol_signal_events:
      all_symbol_signal_events_df = pd.concat(symbol_signal_events, ignore_index=True)
      all_symbol_signal_events_df = deduplicate_signal_events(
        all_symbol_signal_events_df
      )

      save_df(
        all_symbol_signal_events_df,
        out_dir / f"{symbol}_all_signal_events_v6_bspzs.csv",
      )

      symbol_readable_df = build_readable_signal_events(all_symbol_signal_events_df)
      symbol_last_df = build_last_events_per_symbol_timeframe(symbol_readable_df)

      symbol_reference_date = None
      if not symbol_last_df.empty and "eventdate" in symbol_last_df.columns:
        tmp_ref = pd.to_datetime(symbol_last_df["eventdate"], errors="coerce").max()
        if pd.notna(tmp_ref):
          symbol_reference_date = tmp_ref.strftime("%Y-%m-%d")

      symbol_last_digest_df = build_last_digest_by_symbol(
        symbol_last_df,
        freshdays=2,
        reference_date=symbol_reference_date,
      )
      symbol_trading_digest_df = filter_trading_digest(symbol_last_digest_df)

      save_df(
        symbol_readable_df, out_dir / f"{symbol}_signal_events_readable_v6_bspzs.csv"
      )
      save_df(
        symbol_last_df,
        out_dir / f"{symbol}_signal_events_last_per_timeframe_v6_bspzs.csv",
      )
      save_df(
        symbol_last_digest_df,
        out_dir / f"{symbol}_signal_digest_last_per_symbol_v6_bspzs.csv",
      )
      save_df(
        symbol_trading_digest_df,
        out_dir / f"{symbol}_trading_signal_digest_last_per_symbol_v6_bspzs.csv",
      )

    if symbol_signal_digest:
      all_symbol_signal_digest_df = pd.concat(symbol_signal_digest, ignore_index=True)
      all_symbol_signal_digest_df = all_symbol_signal_digest_df.sort_values(
        by=["eventdate", "symbol", "timeframe", "latesteventtime"],
        na_position="last",
      ).reset_index(drop=True)
      save_df(
        all_symbol_signal_digest_df, out_dir / f"{symbol}_signal_digest_v6_bspzs.csv"
      )

  if market_signal_events:
    market_all_signal_events_df = pd.concat(market_signal_events, ignore_index=True)
    market_all_signal_events_df = deduplicate_signal_events(market_all_signal_events_df)
    save_df(
      market_all_signal_events_df, out_dir / "market_all_signal_events_v6_bspzs.csv"
    )

    market_readable_df = build_readable_signal_events(market_all_signal_events_df)
    market_last_df = build_last_events_per_symbol_timeframe(market_readable_df)

    global_reference_date = None
    if not market_last_df.empty and "eventdate" in market_last_df.columns:
      tmp_ref = pd.to_datetime(market_last_df["eventdate"], errors="coerce").max()
      if pd.notna(tmp_ref):
        global_reference_date = tmp_ref.strftime("%Y-%m-%d")

    market_last_digest_df = build_last_digest_by_symbol(
      market_last_df,
      freshdays=2,
      reference_date=global_reference_date,
    )
    market_trading_digest_df = filter_trading_digest(market_last_digest_df)

    save_df(market_readable_df, out_dir / "market_signal_events_readable_v6_bspzs.csv")
    save_df(
      market_last_df, out_dir / "market_signal_events_last_per_timeframe_v6_bspzs.csv"
    )
    save_df(
      market_last_digest_df,
      out_dir / "market_signal_digest_last_per_symbol_v6_bspzs.csv",
    )
    save_df(
      market_trading_digest_df,
      out_dir / "market_trading_signal_digest_last_per_symbol_v6_bspzs.csv",
    )

  if market_signal_digest:
    market_signal_digest_df = pd.concat(market_signal_digest, ignore_index=True)
    market_signal_digest_df = market_signal_digest_df.sort_values(
      by=["eventdate", "symbol", "timeframe", "latesteventtime"],
      na_position="last",
    ).reset_index(drop=True)
    save_df(market_signal_digest_df, out_dir / "market_signal_digest_v6_bspzs.csv")

  print("-" * 60)
  print("V6-BSPZS done")
  print(f"out_dir={out_dir}")


if __name__ == "__main__":
  main()
