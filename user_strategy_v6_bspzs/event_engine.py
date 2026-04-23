# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from typing import Any, Dict, List

import pandas as pd

TIMEFRAME_ORDER = ["1d", "4h", "2h", "1h"]
READABLE_EVENT_TYPES = {
  "ZS_FORMED",
  "BSP1_BUY",
  "BSP1_SELL",
  "BSP2_BUY",
  "BSP2_SELL",
  "BSP3_BUY",
  "BSP3_SELL",
}


def normalize_timeframe(tf: str | None) -> str:
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


def format_timeframe_label(tf: str | None) -> str:
  tf = normalize_timeframe(tf)
  return tf.upper() if tf else ""


def format_price(v: Any) -> str:
  if pd.isna(v):
    return ""
  try:
    return f"{float(v):.4f}".rstrip("0").rstrip(".")
  except Exception:
    return str(v)


def save_df(df: pd.DataFrame, path):
  path.parent.mkdir(parents=True, exist_ok=True)
  df.to_csv(path, index=False, encoding="utf-8-sig")


def deduplicate_events(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  out = df.copy()
  for col in [
    "symbol",
    "timeframe",
    "event_type",
    "event_time",
    "reason",
    "bi_id",
    "bar_index",
    "event_seq",
    "source_kind",
    "source_id",
  ]:
    if col not in out.columns:
      out[col] = None

  out = out.sort_values(
    by=["symbol", "timeframe", "event_time", "bar_index", "event_seq"],
    na_position="last",
  ).reset_index(drop=True)

  out = out.drop_duplicates(
    subset=[
      "symbol",
      "timeframe",
      "event_type",
      "event_time",
      "reason",
      "bi_id",
      "bar_index",
      "source_kind",
      "source_id",
    ],
    keep="first",
  ).reset_index(drop=True)

  return out


def map_bsp_event_type(types: list[str], is_buy: bool) -> str | None:
  norm_types = {str(x).strip().lower() for x in (types or []) if str(x).strip()}
  side = "BUY" if is_buy else "SELL"
  if "1" in norm_types:
    return f"BSP1_{side}"
  if "2" in norm_types:
    return f"BSP2_{side}"
  if "3a" in norm_types or "3b" in norm_types:
    return f"BSP3_{side}"
  return None


def build_events_from_zs_and_bsp(
  symbol: str,
  timeframe: str,
  zs_list: List[Dict[str, Any]],
  bsp_list: List[Dict[str, Any]],
) -> pd.DataFrame:
  rows: list[dict] = []
  seq = 0
  timeframe = normalize_timeframe(timeframe)

  for zs in zs_list or []:
    seq += 1
    event_time = zs.get("end_time") or zs.get("begin_time")
    rows.append(
      {
        "event_seq": seq,
        "symbol": symbol,
        "timeframe": timeframe,
        "event_type": "ZS_FORMED",
        "event_time": event_time,
        "event_date": str(event_time)[:10] if event_time else "",
        "bar_index": None,
        "price": None,
        "stop_price": None,
        "bi_id": zs.get("end_bi_id"),
        "reason": "new_zs_detected",
        "signal_text": f"new zs formed, bi_count={zs.get('bi_count', 0)}",
        "source_kind": "zs",
        "source_id": zs.get("zs_id"),
        "is_buy": None,
        "bsp_types": "",
        "zs_low": zs.get("low"),
        "zs_high": zs.get("high"),
        "zs_begin_bi_id": zs.get("begin_bi_id"),
        "zs_end_bi_id": zs.get("end_bi_id"),
        "zs_begin_time": zs.get("begin_time"),
        "zs_end_time": zs.get("end_time"),
      }
    )

  for bsp in bsp_list or []:
    event_type = map_bsp_event_type(
      bsp.get("types", []), bool(bsp.get("is_buy", False))
    )
    if not event_type:
      continue
    seq += 1
    event_time = bsp.get("time")
    rows.append(
      {
        "event_seq": seq,
        "symbol": symbol,
        "timeframe": timeframe,
        "event_type": event_type,
        "event_time": event_time,
        "event_date": str(event_time)[:10] if event_time else "",
        "bar_index": bsp.get("klu_index"),
        "price": bsp.get("price"),
        "stop_price": None,
        "bi_id": bsp.get("bi_id"),
        "reason": "chan_bsp_detected",
        "signal_text": ",".join(bsp.get("types", [])),
        "source_kind": "bsp",
        "source_id": bsp.get("bsp_id"),
        "is_buy": bsp.get("is_buy"),
        "bsp_types": ",".join(bsp.get("types", [])),
        "zs_low": None,
        "zs_high": None,
        "zs_begin_bi_id": None,
        "zs_end_bi_id": None,
        "zs_begin_time": None,
        "zs_end_time": None,
      }
    )

  if not rows:
    return pd.DataFrame()

  return deduplicate_events(pd.DataFrame(rows))


def build_signal_digest(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  x = df.copy()
  x = x[x["event_type"].isin(READABLE_EVENT_TYPES)].copy()
  if x.empty:
    return pd.DataFrame()

  for col in [
    "symbol",
    "timeframe",
    "event_time",
    "event_seq",
    "price",
    "bi_id",
    "reason",
    "signal_text",
  ]:
    if col not in x.columns:
      x[col] = None

  x["event_date"] = x["event_time"].astype(str).str.slice(0, 10)
  x = x.sort_values(
    by=["symbol", "timeframe", "event_time", "event_seq"],
    na_position="last",
  ).reset_index(drop=True)

  grouped_rows = []
  for (symbol, event_date, timeframe), grp in x.groupby(
    ["symbol", "event_date", "timeframe"], dropna=False
  ):
    latest = grp.iloc[-1]
    grouped_rows.append(
      {
        "symbol": symbol,
        "event_date": event_date,
        "timeframe": timeframe,
        "latest_event_type": latest.get("event_type", ""),
        "latest_event_time": latest.get("event_time", ""),
        "latest_price": latest.get("price", None),
        "bi_id": latest.get("bi_id", None),
        "reason": latest.get("reason", ""),
        "signal_text": latest.get("signal_text", ""),
        "event_count": int(len(grp)),
      }
    )

  out = pd.DataFrame(grouped_rows)
  if out.empty:
    return out

  return out.sort_values(
    by=["event_date", "symbol", "timeframe", "latest_event_time"],
    ascending=[True, True, True, True],
    na_position="last",
  ).reset_index(drop=True)


def ensure_event_columns(df: pd.DataFrame) -> pd.DataFrame:
  out = df.copy()
  for col in [
    "symbol",
    "timeframe",
    "signal_text",
    "reason",
    "bi_id",
    "event_count",
    "latest_price",
    "latest_event_type",
  ]:
    if col not in out.columns:
      out[col] = None

  if "event_count" in out.columns:
    out["event_count"] = out["event_count"].fillna(1)
  else:
    out["event_count"] = 1

  time_candidates = [
    "event_time",
    "latest_event_time",
    "timestamp",
    "datetime",
    "dt",
    "time",
  ]

  def pick_first_nonnull(row, cols):
    for c in cols:
      if c in row.index:
        v = row[c]
        if pd.notna(v) and str(v).strip() != "":
          return str(v)
    return ""

  out["event_time_raw"] = out.apply(
    lambda r: pick_first_nonnull(r, time_candidates), axis=1
  )
  out["timeframe"] = out["timeframe"].map(normalize_timeframe)
  out["event_time_dt"] = pd.to_datetime(out["event_time_raw"], errors="coerce")
  if "event_date" in out.columns:
    out["event_date_dt"] = pd.to_datetime(out["event_date"], errors="coerce")
  else:
    out["event_date_dt"] = pd.NaT

  out["event_date_dt"] = out["event_date_dt"].where(
    out["event_date_dt"].notna(),
    out["event_time_dt"].dt.normalize(),
  )

  out["event_date_str"] = out["event_date_dt"].dt.strftime("%Y-%m-%d").fillna("")
  out["event_time_str"] = out["event_time_dt"].dt.strftime("%Y-%m-%d %H:%M").fillna("")
  return out


def build_readable_signal_events(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame(
      columns=[
        "symbol",
        "timeframe",
        "event_date",
        "latest_event_type",
        "latest_event_time",
        "latest_price",
        "bi_id",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    )

  x = ensure_event_columns(df)
  x = x[x["latest_event_type"].isin(READABLE_EVENT_TYPES)].copy()
  if x.empty:
    return pd.DataFrame(
      columns=[
        "symbol",
        "timeframe",
        "event_date",
        "latest_event_type",
        "latest_event_time",
        "latest_price",
        "bi_id",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    )

  x = x.sort_values(
    by=["symbol", "timeframe", "event_time_dt", "bi_id"],
    ascending=[True, True, True, True],
  )

  def build_text(row) -> str:
    symbol = str(row.get("symbol", "")).strip()
    tf = format_timeframe_label(row.get("timeframe", ""))
    evt_time = str(row.get("event_time_str", "")).strip()
    evt_type = str(row.get("latest_event_type", "")).strip()
    signal_text = str(row.get("signal_text", "")).strip()
    price = format_price(row.get("latest_price", None))
    bi_id = row.get("bi_id", None)
    reason = str(row.get("reason", "")).strip()

    extra = []
    if signal_text:
      extra.append(signal_text)
    if price:
      extra.append(f"price={price}")
    if pd.notna(bi_id):
      extra.append(f"bi_id={int(float(bi_id))}")
    if reason:
      extra.append(f"reason={reason}")

    suffix = " | ".join(extra)
    return f"{symbol} [{tf}] {evt_time} {evt_type}" + (f" | {suffix}" if suffix else "")

  x["latest_event_time"] = x["event_time_str"]
  x["event_date"] = x["event_date_str"]
  x["readable_text"] = x.apply(build_text, axis=1)

  return x[
    [
      "symbol",
      "timeframe",
      "event_date",
      "latest_event_type",
      "latest_event_time",
      "latest_price",
      "bi_id",
      "reason",
      "signal_text",
      "event_count",
      "readable_text",
    ]
  ].copy()


def build_last_events_per_symbol_timeframe(readable_df: pd.DataFrame) -> pd.DataFrame:
  if readable_df is None or readable_df.empty:
    return pd.DataFrame(
      columns=[
        "symbol",
        "timeframe",
        "event_date",
        "latest_event_type",
        "latest_event_time",
        "latest_price",
        "bi_id",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    )

  x = readable_df.copy()
  x["event_date_dt"] = pd.to_datetime(x["event_date"], errors="coerce")
  x["latest_event_time_dt"] = pd.to_datetime(x["latest_event_time"], errors="coerce")
  x["timeframe"] = x["timeframe"].map(normalize_timeframe)

  x = x.sort_values(
    by=["symbol", "timeframe", "latest_event_time_dt", "bi_id"],
    ascending=[True, True, True, True],
  )

  latest = (
    x.groupby(["symbol", "timeframe"], as_index=False, group_keys=False).tail(1).copy()
  )
  return (
    latest[
      [
        "symbol",
        "timeframe",
        "event_date",
        "latest_event_type",
        "latest_event_time",
        "latest_price",
        "bi_id",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    ]
    .sort_values(by=["symbol", "timeframe"])
    .reset_index(drop=True)
  )


def event_type_rank(event_type: str) -> int:
  rank = {
    "BSP3_SELL": 7,
    "BSP3_BUY": 6,
    "BSP2_SELL": 5,
    "BSP2_BUY": 4,
    "BSP1_SELL": 3,
    "BSP1_BUY": 2,
    "ZS_FORMED": 1,
  }
  return rank.get(str(event_type), 0)


def build_last_digest_by_symbol(
  last_df: pd.DataFrame, fresh_days: int = 10
) -> pd.DataFrame:
  if last_df is None or last_df.empty:
    cols = [
      "symbol",
      "signal_date",
      "reference_date",
      "fresh_days",
      "1d_event_type",
      "1d_signal_text",
      "1d_event_time",
      "1d_latest_price",
      "4h_event_type",
      "4h_signal_text",
      "4h_event_time",
      "4h_latest_price",
      "2h_event_type",
      "2h_signal_text",
      "2h_event_time",
      "2h_latest_price",
      "1h_event_type",
      "1h_signal_text",
      "1h_event_time",
      "1h_latest_price",
      "has_signal",
      "summary_text",
      "summary_json",
    ]
    return pd.DataFrame(columns=cols)

  x = last_df.copy()
  x["timeframe"] = x["timeframe"].map(normalize_timeframe)
  x["event_date_dt"] = pd.to_datetime(x["event_date"], errors="coerce")
  x["latest_event_time_dt"] = pd.to_datetime(x["latest_event_time"], errors="coerce")

  rows = []
  for symbol, g in x.groupby("symbol", sort=True):
    item = {"symbol": symbol}
    reference_date_dt = g["event_date_dt"].max()
    reference_date = (
      reference_date_dt.strftime("%Y-%m-%d") if pd.notna(reference_date_dt) else ""
    )
    item["reference_date"] = reference_date
    item["signal_date"] = reference_date
    item["fresh_days"] = fresh_days

    tf_payload = {}
    for tf in TIMEFRAME_ORDER:
      sub = g[g["timeframe"] == tf].copy()
      if sub.empty:
        item[f"{tf}_event_type"] = ""
        item[f"{tf}_signal_text"] = ""
        item[f"{tf}_event_time"] = ""
        item[f"{tf}_latest_price"] = ""
        tf_payload[tf] = {
          "event_type": "",
          "signal_text": "",
          "event_time": "",
          "latest_price": "",
          "is_fresh": False,
          "age_days": None,
          "telegram_allowed": False,
        }
        continue

      sub = sub.sort_values(by=["event_date_dt", "latest_event_time_dt", "bi_id"])
      r = sub.iloc[-1]

      event_date_dt = r.get("event_date_dt")
      age_days = None
      is_fresh = False
      if pd.notna(reference_date_dt) and pd.notna(event_date_dt):
        age_days = int((reference_date_dt - event_date_dt).days)
        is_fresh = age_days <= fresh_days

      event_type = str(r.get("latest_event_type", "") or "")
      signal_text = str(r.get("signal_text", "") or "")
      event_time = str(r.get("latest_event_time", "") or "")
      latest_price = format_price(r.get("latest_price", None))
      telegram_allowed = bool(event_type) and is_fresh

      item[f"{tf}_event_type"] = event_type if is_fresh else ""
      item[f"{tf}_signal_text"] = signal_text if is_fresh else ""
      item[f"{tf}_event_time"] = event_time if is_fresh else ""
      item[f"{tf}_latest_price"] = latest_price if is_fresh else ""

      tf_payload[tf] = {
        "event_type": event_type,
        "signal_text": signal_text,
        "event_time": event_time,
        "latest_price": latest_price,
        "is_fresh": is_fresh,
        "age_days": age_days,
        "telegram_allowed": telegram_allowed,
      }

    item["has_signal"] = any(
      bool(item.get(f"{tf}_event_type", "")) for tf in TIMEFRAME_ORDER
    )

    tf_lines = []
    for tf in TIMEFRAME_ORDER:
      event_type = item.get(f"{tf}_event_type", "")
      signal_text = item.get(f"{tf}_signal_text", "")
      event_time = item.get(f"{tf}_event_time", "")
      latest_price = item.get(f"{tf}_latest_price", "")
      tf_label = tf.upper()

      if event_type:
        extra = []
        if signal_text:
          extra.append(signal_text)
        if event_time:
          extra.append(f"time={event_time}")
        if latest_price:
          extra.append(f"price={latest_price}")
        tf_lines.append(
          f"{tf_label}: {event_type}" + (f" | {' | '.join(extra)}" if extra else "")
        )
      else:
        tf_lines.append(f"{tf_label}: -")

    item["summary_text"] = (
      f"{symbol} | ref={reference_date} | fresh_days={fresh_days}\n"
      + "\n".join(tf_lines)
    )
    item["summary_json"] = json.dumps(tf_payload, ensure_ascii=False)

    rows.append(item)

  out = pd.DataFrame(rows)

  def symbol_rank(row) -> int:
    ranks = [event_type_rank(row.get(f"{tf}_event_type", "")) for tf in TIMEFRAME_ORDER]
    return max(ranks) if ranks else 0

  out["rank"] = out.apply(symbol_rank, axis=1)
  out = (
    out.sort_values(by=["rank", "symbol"], ascending=[False, True])
    .drop(columns=["rank"])
    .reset_index(drop=True)
  )
  return out
