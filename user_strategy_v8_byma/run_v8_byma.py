# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Tuple

import pandas as pd

from user_strategy_v8_byma.config import StrategyConfig
from user_strategy_v8_byma.backtest_engine import BymaBacktester

READABLE_EVENT_TYPES = {
  "LONG_ENTRY_READY",
  "LONG_WEAKEN_ALERT",
  "LONG_EXIT_TREND",
  "LONG_STOP_LOSS",
  "BULL_ENV_READY",
}

TELEGRAM_DIGEST_EVENT_TYPES = {
  "LONG_ENTRY_READY",
  "LONG_WEAKEN_ALERT",
  "LONG_EXIT_TREND",
  "LONG_STOP_LOSS",
}

TIMEFRAME_ORDER = ["1d", "4h", "2h", "1h"]

SYMBOL_TIMEFRAME_WHITELIST: Dict[str, set[str]] = {}

HIGHER_TF_FILTER_MAP: Dict[str, str] = {
  "1h": "4h",
  "2h": "1d",
  "4h": "1d",
}


def save_df(df: pd.DataFrame, path: Path):
  path.parent.mkdir(parents=True, exist_ok=True)
  df.to_csv(path, index=False, encoding="utf-8-sig")


def get_all_symbols(data_dir: Path) -> List[str]:
  symbols = set()
  for file in data_dir.glob("*.csv"):
    parts = file.name.split("_")
    if parts:
      symbols.add(parts[0])
  return sorted(list(symbols))


def get_timeframe_params(tf_name: str) -> Tuple[int, int]:
  tf = str(tf_name).strip().lower()
  if tf == "1d":
    return 1, 4
  if tf in {"4h", "2h"}:
    return 2, 8
  if tf == "1h":
    return 2, 10
  return 2, 8


def is_whitelisted(symbol: str, tf_name: str) -> bool:
  s = str(symbol).strip().upper()
  tf = str(tf_name).strip().lower()
  if not SYMBOL_TIMEFRAME_WHITELIST:
    return True
  allowed = SYMBOL_TIMEFRAME_WHITELIST.get(s)
  if allowed is None:
    return False
  return tf in {x.lower() for x in allowed}


def deduplicate_signal_events(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  result = df.copy()
  for col in [
    "symbol",
    "timeframe",
    "event_type",
    "event_time",
    "reason",
    "bar_index",
    "trade_id",
    "event_seq",
    "bull_regime_id",
    "entry_blocked_in_regime",
  ]:
    if col not in result.columns:
      result[col] = ""

  result = result.sort_values(
    by=["symbol", "timeframe", "event_time", "bar_index", "event_seq"],
    na_position="last",
  ).reset_index(drop=True)

  result = result.drop_duplicates(
    subset=[
      "symbol",
      "timeframe",
      "event_type",
      "event_time",
      "reason",
      "bar_index",
      "trade_id",
      "bull_regime_id",
    ],
    keep="first",
  ).reset_index(drop=True)

  return result


def build_signal_digest(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  keep = {
    "LONG_ENTRY_READY",
    "LONG_WEAKEN_ALERT",
    "LONG_EXIT_TREND",
    "LONG_STOP_LOSS",
  }
  xdf = df[df["event_type"].isin(keep)].copy()
  if xdf.empty:
    return pd.DataFrame()

  for col in [
    "symbol",
    "timeframe",
    "event_time",
    "event_seq",
    "price",
    "stop_price",
    "reason",
    "signal_text",
    "bull_regime_id",
    "entry_blocked_in_regime",
  ]:
    if col not in xdf.columns:
      xdf[col] = None

  xdf["event_date"] = xdf["event_time"].astype(str).str.slice(0, 10)
  xdf = xdf.sort_values(
    by=["symbol", "timeframe", "event_time", "event_seq"],
    na_position="last",
  ).reset_index(drop=True)

  grouped_rows = []
  for (symbol, event_date, timeframe), grp in xdf.groupby(
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
        "stop_price": latest.get("stop_price", None),
        "reason": latest.get("reason", ""),
        "signal_text": latest.get("signal_text", ""),
        "bull_regime_id": latest.get("bull_regime_id", None),
        "entry_blocked_in_regime": latest.get("entry_blocked_in_regime", False),
        "event_count": int(len(grp)),
      }
    )

  digest_df = pd.DataFrame(grouped_rows)
  if digest_df.empty:
    return digest_df

  digest_df = digest_df.sort_values(
    by=["event_date", "symbol", "timeframe", "latest_event_time"],
    ascending=[True, True, True, True],
    na_position="last",
  ).reset_index(drop=True)
  return digest_df


def _normalize_timeframe(tf: str) -> str:
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


def _format_timeframe_label(tf: str) -> str:
  tf = _normalize_timeframe(tf)
  return tf.upper() if tf else ""


def _format_price(v) -> str:
  if pd.isna(v):
    return ""
  try:
    return f"{float(v):.4f}".rstrip("0").rstrip(".")
  except Exception:
    return str(v)


def _ensure_event_columns(df: pd.DataFrame) -> pd.DataFrame:
  out = df.copy()
  if "symbol" not in out.columns:
    out["symbol"] = None
  if "timeframe" not in out.columns:
    out["timeframe"] = None
  if "signal_text" not in out.columns:
    out["signal_text"] = ""
  if "reason" not in out.columns:
    out["reason"] = ""
  if "stop_price" not in out.columns:
    out["stop_price"] = None
  if "event_count" not in out.columns:
    out["event_count"] = 1
  if "latest_price" not in out.columns:
    if "price" in out.columns:
      out["latest_price"] = out["price"]
    elif "close" in out.columns:
      out["latest_price"] = out["close"]
    else:
      out["latest_price"] = None
  if "latest_event_type" not in out.columns:
    out["latest_event_type"] = out["event_type"] if "event_type" in out.columns else ""
  if "bull_regime_id" not in out.columns:
    out["bull_regime_id"] = None
  if "entry_blocked_in_regime" not in out.columns:
    out["entry_blocked_in_regime"] = False

  time_candidates = [
    "event_time",
    "latest_event_time",
    "timestamp",
    "datetime",
    "dt",
    "time",
  ]

  def pick_first_non_null(row, cols):
    for c in cols:
      if c in row.index:
        v = row[c]
        if pd.notna(v) and str(v).strip() != "":
          return str(v)
    return ""

  out["event_time_raw"] = out.apply(
    lambda r: pick_first_non_null(r, time_candidates), axis=1
  )
  out["timeframe"] = out["timeframe"].map(_normalize_timeframe)
  out["event_time_dt"] = pd.to_datetime(out["event_time_raw"], errors="coerce")

  if "event_date" in out.columns:
    out["event_date_dt"] = pd.to_datetime(out["event_date"], errors="coerce")
  else:
    out["event_date_dt"] = pd.NaT

  out["event_date_dt"] = out["event_date_dt"].where(
    out["event_date_dt"].notna(),
    out["event_time_dt"].dt.normalize(),
  )
  out["event_date_str"] = out["event_date_dt"].dt.strftime("%Y/%m/%d").fillna("")
  out["event_time_str"] = out["event_time_dt"].dt.strftime("%Y/%m/%d %H:%M").fillna("")
  return out


def build_readable_signal_events(df: pd.DataFrame) -> pd.DataFrame:
  cols = [
    "symbol",
    "timeframe",
    "event_date",
    "latest_event_type",
    "latest_event_time",
    "latest_price",
    "stop_price",
    "reason",
    "signal_text",
    "bull_regime_id",
    "entry_blocked_in_regime",
    "event_count",
    "readable_text",
  ]
  if df is None or df.empty:
    return pd.DataFrame(columns=cols)

  x = _ensure_event_columns(df)
  x = x[x["latest_event_type"].isin(READABLE_EVENT_TYPES)].copy()
  if x.empty:
    return pd.DataFrame(columns=cols)

  x = x.sort_values(
    ["symbol", "timeframe", "event_time_dt"], ascending=[True, True, True]
  )
  x["latest_event_time"] = x["event_time_raw"].fillna("")
  x["event_date"] = x["event_date_str"].fillna("")

  def normalize_display_time(v: str) -> str:
    if not v:
      return ""
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
      return str(v)
    return ts.strftime("%Y/%m/%d %H:%M")

  x["latest_event_time"] = x["latest_event_time"].apply(normalize_display_time)
  x["readable_text"] = x.apply(
    lambda r: (
      f"{r['symbol']} | {_format_timeframe_label(r['timeframe'])} | {r['latest_event_time']} | {r['latest_event_type']} | {r['signal_text'] or ''}"
      f"{' | latest_price=' + _format_price(r['latest_price']) if _format_price(r['latest_price']) else ''}"
      f"{' | stop_price=' + _format_price(r['stop_price']) if _format_price(r['stop_price']) else ''}"
      f"{' | regime=' + str(int(float(r['bull_regime_id']))) if pd.notna(r['bull_regime_id']) and str(r['bull_regime_id']) != '' else ''}"
      f"{' | reason=' + str(r['reason']) if pd.notna(r['reason']) and str(r['reason']).strip() else ''}"
    ),
    axis=1,
  )
  return x[cols].copy()


def build_last_events_per_symbol_timeframe(readable_df: pd.DataFrame) -> pd.DataFrame:
  cols = [
    "symbol",
    "timeframe",
    "event_date",
    "latest_event_type",
    "latest_event_time",
    "latest_price",
    "stop_price",
    "reason",
    "signal_text",
    "bull_regime_id",
    "entry_blocked_in_regime",
    "event_count",
    "readable_text",
  ]
  if readable_df is None or readable_df.empty:
    return pd.DataFrame(columns=cols)

  x = readable_df.copy()
  x["event_date_dt"] = pd.to_datetime(x["event_date"], errors="coerce")
  x["latest_event_time_dt"] = pd.to_datetime(x["latest_event_time"], errors="coerce")
  x["timeframe"] = x["timeframe"].map(_normalize_timeframe)

  actionable_types = {
    "LONG_ENTRY_READY",
    "LONG_WEAKEN_ALERT",
    "LONG_EXIT_TREND",
    "LONG_STOP_LOSS",
  }

  x = x.sort_values(
    ["symbol", "timeframe", "event_date_dt", "latest_event_time_dt"],
    ascending=[True, True, True, True],
  ).reset_index(drop=True)

  picked_rows = []

  for (symbol, timeframe), grp in x.groupby(["symbol", "timeframe"], sort=True):
    grp = grp.sort_values(
      ["event_date_dt", "latest_event_time_dt"],
      ascending=[True, True],
    )

    actionable = grp[grp["latest_event_type"].isin(actionable_types)].copy()

    if not actionable.empty:
      picked_rows.append(actionable.iloc[-1].to_dict())
    else:
      picked_rows.append(grp.iloc[-1].to_dict())

  if not picked_rows:
    return pd.DataFrame(columns=cols)

  out = pd.DataFrame(picked_rows)
  return out[cols].sort_values(["symbol", "timeframe"]).reset_index(drop=True)


def build_symbol_reference_dates(readable_df: pd.DataFrame) -> pd.DataFrame:
  cols = ["symbol", "reference_date", "reference_date_dt"]
  if readable_df is None or readable_df.empty:
    return pd.DataFrame(columns=cols)

  x = readable_df.copy()
  x["event_date_dt"] = pd.to_datetime(x["event_date"], errors="coerce")
  ref = (
    x.groupby("symbol", as_index=False)["event_date_dt"]
    .max()
    .rename(columns={"event_date_dt": "reference_date_dt"})
  )
  ref["reference_date"] = ref["reference_date_dt"].dt.strftime("%Y/%m/%d").fillna("")
  return ref[cols]


def _event_type_rank(event_type: str) -> int:
  rank = {
    "LONG_STOP_LOSS": 4,
    "LONG_EXIT_TREND": 3,
    "LONG_WEAKEN_ALERT": 2,
    "LONG_ENTRY_READY": 1,
    "BULL_ENV_READY": 0,
  }
  return rank.get(str(event_type), 0)


def build_last_digest_by_symbol(
  last_df: pd.DataFrame,
  reference_dates_df: pd.DataFrame | None = None,
  fresh_days: int = 2,
) -> pd.DataFrame:
  cols = [
    "symbol",
    "signal_date",
    "reference_date",
    "fresh_days",
    "1d_event_type",
    "1d_signal_text",
    "1d_event_time",
    "1d_latest_price",
    "1d_stop_price",
    "1d_bull_regime_id",
    "4h_event_type",
    "4h_signal_text",
    "4h_event_time",
    "4h_latest_price",
    "4h_stop_price",
    "4h_bull_regime_id",
    "2h_event_type",
    "2h_signal_text",
    "2h_event_time",
    "2h_latest_price",
    "2h_stop_price",
    "2h_bull_regime_id",
    "1h_event_type",
    "1h_signal_text",
    "1h_event_time",
    "1h_latest_price",
    "1h_stop_price",
    "1h_bull_regime_id",
    "has_signal",
    "summary_text",
    "summary_json",
  ]
  if last_df is None or last_df.empty:
    return pd.DataFrame(columns=cols)

  x = last_df.copy()
  x["timeframe"] = x["timeframe"].map(_normalize_timeframe)
  x["event_date_dt"] = pd.to_datetime(x["event_date"], errors="coerce")
  x["latest_event_time_dt"] = pd.to_datetime(x["latest_event_time"], errors="coerce")

  ref_map: Dict[str, pd.Timestamp] = {}
  ref_str_map: Dict[str, str] = {}
  if reference_dates_df is not None and not reference_dates_df.empty:
    rdf = reference_dates_df.copy()
    rdf["reference_date_dt"] = pd.to_datetime(rdf["reference_date_dt"], errors="coerce")
    rdf["reference_date"] = rdf["reference_date"].fillna("")
    for _, rr in rdf.iterrows():
      sym = str(rr.get("symbol", "") or "")
      if sym:
        ref_map[sym] = rr.get("reference_date_dt")
        ref_str_map[sym] = rr.get("reference_date", "")

  rows = []
  for symbol, g in x.groupby("symbol", sort=True):
    item = {"symbol": symbol}

    reference_date_dt = ref_map.get(symbol)
    reference_date = ref_str_map.get(symbol, "")

    if pd.isna(reference_date_dt) or not reference_date:
      reference_date_dt = g["event_date_dt"].max()
      reference_date = (
        reference_date_dt.strftime("%Y/%m/%d") if pd.notna(reference_date_dt) else ""
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
        item[f"{tf}_stop_price"] = ""
        item[f"{tf}_bull_regime_id"] = ""
        tf_payload[tf] = {
          "event_type": "",
          "signal_text": "",
          "event_time": "",
          "latest_price": "",
          "stop_price": "",
          "bull_regime_id": "",
          "is_fresh": False,
          "age_days": None,
          "telegram_allowed": False,
        }
        continue

      sub = sub.sort_values(["event_date_dt", "latest_event_time_dt"])
      r = sub.iloc[-1]
      raw_event_type = str(r.get("latest_event_type", "") or "")
      event_date_dt = r.get("event_date_dt")
      age_days = None
      is_fresh = False
      if pd.notna(reference_date_dt) and pd.notna(event_date_dt):
        age_days = int((reference_date_dt - event_date_dt).days)
        is_fresh = age_days <= fresh_days

      telegram_allowed = raw_event_type in TELEGRAM_DIGEST_EVENT_TYPES

      if is_fresh and telegram_allowed:
        item[f"{tf}_event_type"] = raw_event_type
        item[f"{tf}_signal_text"] = r.get("signal_text", "")
        item[f"{tf}_event_time"] = r.get("latest_event_time", "")
        item[f"{tf}_latest_price"] = _format_price(r.get("latest_price"))
        item[f"{tf}_stop_price"] = _format_price(r.get("stop_price"))
        item[f"{tf}_bull_regime_id"] = (
          str(int(float(r.get("bull_regime_id"))))
          if pd.notna(r.get("bull_regime_id")) and str(r.get("bull_regime_id")) != ""
          else ""
        )
      else:
        item[f"{tf}_event_type"] = ""
        item[f"{tf}_signal_text"] = ""
        item[f"{tf}_event_time"] = ""
        item[f"{tf}_latest_price"] = ""
        item[f"{tf}_stop_price"] = ""
        item[f"{tf}_bull_regime_id"] = ""

      tf_payload[tf] = {
        "event_type": raw_event_type,
        "signal_text": r.get("signal_text", ""),
        "event_time": r.get("latest_event_time", ""),
        "latest_price": _format_price(r.get("latest_price")),
        "stop_price": _format_price(r.get("stop_price")),
        "bull_regime_id": (
          str(int(float(r.get("bull_regime_id"))))
          if pd.notna(r.get("bull_regime_id")) and str(r.get("bull_regime_id")) != ""
          else ""
        ),
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
      stop_price = item.get(f"{tf}_stop_price", "")
      bull_regime_id = item.get(f"{tf}_bull_regime_id", "")
      tf_label = tf.upper()

      if event_type:
        extra = []
        if signal_text:
          extra.append(signal_text)
        if event_time:
          extra.append(f"time={event_time}")
        if latest_price:
          extra.append(f"price={latest_price}")
        if stop_price:
          extra.append(f"stop={stop_price}")
        if bull_regime_id:
          extra.append(f"regime={bull_regime_id}")
        tf_lines.append(f"{tf_label}: {event_type} | " + " | ".join(extra))
      else:
        tf_lines.append(f"{tf_label}: 无信号")

    item["summary_text"] = (
      f"{symbol} | ref={item['reference_date']} | fresh_days={fresh_days} | "
      + " ; ".join(tf_lines)
    )
    item["summary_json"] = json.dumps(tf_payload, ensure_ascii=False)
    rows.append(item)

  out = pd.DataFrame(rows)
  out["_rank"] = out.apply(
    lambda row: max(
      [_event_type_rank(row.get(f"{tf}_event_type", "")) for tf in TIMEFRAME_ORDER]
      or [0]
    ),
    axis=1,
  )
  out = out.sort_values(["_rank", "symbol"], ascending=[False, True]).drop(
    columns=["_rank"]
  )
  return out


def load_price_data(
  csv_path: Path, start_time: str | None = None, end_time: str | None = None
) -> pd.DataFrame:
  df = pd.read_csv(csv_path)
  df.columns = [str(c).strip().lower() for c in df.columns]

  if "time" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"time": "dt"})
  if "date" in df.columns and "dt" not in df.columns:
    df = df.rename(columns={"date": "dt"})

  required = {"dt", "open", "high", "low", "close"}
  missing = required - set(df.columns)
  if missing:
    raise ValueError(f"csv missing columns: {missing}, path={csv_path}")

  df["dt"] = pd.to_datetime(df["dt"], errors="coerce")
  for col in ["open", "high", "low", "close"]:
    df[col] = pd.to_numeric(df[col], errors="coerce")
  if "volume" in df.columns:
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce")
  else:
    df["volume"] = 0.0

  df = (
    df.dropna(subset=["dt", "open", "high", "low", "close"])
    .sort_values("dt")
    .reset_index(drop=True)
  )

  if start_time:
    df = df[df["dt"] >= pd.to_datetime(start_time)].reset_index(drop=True)
  if end_time:
    df = df[df["dt"] <= pd.to_datetime(end_time)].reset_index(drop=True)

  return df


def compute_trend_filter_flags(df: pd.DataFrame) -> pd.DataFrame:
  x = df.copy().reset_index(drop=True)
  x["blue_upper"] = x["high"].ewm(span=24, adjust=False).mean()
  x["blue_lower"] = x["low"].ewm(span=23, adjust=False).mean()
  x["yellow_upper"] = x["high"].ewm(span=89, adjust=False).mean()
  x["yellow_lower"] = x["low"].ewm(span=90, adjust=False).mean()

  x["yellow_upper_prev"] = x["yellow_upper"].shift(1)
  x["yellow_lower_prev"] = x["yellow_lower"].shift(1)
  x["yellow_rising"] = (x["yellow_upper"] > x["yellow_upper_prev"]) & (
    x["yellow_lower"] > x["yellow_lower_prev"]
  )
  x["blue_over_yellow"] = (x["blue_upper"] >= x["yellow_upper"]) & (
    x["blue_lower"] >= x["yellow_lower"]
  )
  x["blue_below_yellow"] = (x["blue_upper"] <= x["yellow_upper"]) & (
    x["blue_lower"] <= x["yellow_lower"]
  )
  x["bull_env_ready_state"] = x["blue_over_yellow"] & x["yellow_rising"]
  x["exit_trend_state"] = x["blue_below_yellow"]
  x["higher_tf_trend_ok"] = x["bull_env_ready_state"] & (~x["exit_trend_state"])
  return x[["dt", "higher_tf_trend_ok"]].copy()


def build_external_trend_ok_series(
  cur_df: pd.DataFrame,
  higher_df: pd.DataFrame | None,
) -> pd.Series:
  if higher_df is None or higher_df.empty:
    return pd.Series([True] * len(cur_df))

  high_flags = compute_trend_filter_flags(higher_df).sort_values("dt").copy()
  merged = pd.merge_asof(
    cur_df[["dt"]].sort_values("dt"),
    high_flags,
    on="dt",
    direction="backward",
  )
  return merged["higher_tf_trend_ok"].fillna(False).astype(bool)


def main():
  repo_root = Path(__file__).resolve().parent.parent
  conf = StrategyConfig()

  data_dir = conf.resolved_data_dir(repo_root)
  out_dir = conf.resolved_output_dir(repo_root)
  out_dir.mkdir(parents=True, exist_ok=True)

  all_symbols = get_all_symbols(data_dir)
  print(f"发现 {len(all_symbols)} 个股票代码: {all_symbols}")

  final_summary_list = []
  market_signal_events = []
  market_signal_digest = []

  for symbol in all_symbols:
    print(f"\n{'=' * 40}\n正在处理股票: {symbol}\n{'=' * 40}")

    conf.symbol = symbol
    symbol_summary = []
    symbol_signal_events = []
    symbol_signal_digest = []

    price_cache: Dict[str, pd.DataFrame] = {}

    for tf in conf.timeframes:
      if not tf.enabled:
        continue

      tf_name = str(tf.name).lower()
      if not is_whitelisted(symbol, tf_name):
        print(f" [白名单跳过] {symbol} {tf.name}")
        continue

      if tf.level == "1D":
        matches = sorted(data_dir.glob(f"{symbol}_*_1d.csv"))
      else:
        matches = sorted(data_dir.glob(f"{symbol}_*_yf_{tf.name.lower()}_730d.csv"))

      if not matches:
        print(f" [!] 未找到 {symbol} 的 {tf.name} 数据文件，跳过")
        continue

      csv_path = matches[-1]
      print(f" 处理时间框架: {tf.name} ({tf.level}) -> {csv_path.name}")

      try:
        price_df = load_price_data(
          csv_path, start_time=tf.start_time, end_time=tf.end_time
        )
        price_cache[tf_name] = price_df.copy()

        if len(price_df) < conf.min_bars_required:
          print(
            f" - [跳过] 数据不足 {conf.min_bars_required} 根，当前仅 {len(price_df)} 根"
          )
          continue

        bull_confirm_bars, regime_cooldown_bars = get_timeframe_params(tf.name)

        higher_tf_name = HIGHER_TF_FILTER_MAP.get(tf_name)
        higher_price_df = None
        external_trend_ok = pd.Series([True] * len(price_df))
        higher_tf_filter_name = ""

        if higher_tf_name:
          if higher_tf_name in price_cache:
            higher_price_df = price_cache[higher_tf_name]
          else:
            if higher_tf_name == "1d":
              higher_matches = sorted(data_dir.glob(f"{symbol}_*_1d.csv"))
            else:
              higher_matches = sorted(
                data_dir.glob(f"{symbol}_*_yf_{higher_tf_name}_730d.csv")
              )
            if higher_matches:
              higher_price_df = load_price_data(higher_matches[-1])
              price_cache[higher_tf_name] = higher_price_df.copy()

          if higher_price_df is not None and not higher_price_df.empty:
            external_trend_ok = build_external_trend_ok_series(
              cur_df=price_df, higher_df=higher_price_df
            )
            higher_tf_filter_name = higher_tf_name

        bt = BymaBacktester(
          symbol=symbol,
          timeframe=tf.name,
          df=price_df,
          allow_reentry=conf.allow_reentry,
          close_open_positions_on_last_bar=conf.close_open_positions_on_last_bar,
          bull_confirm_bars=bull_confirm_bars,
          regime_cooldown_bars=regime_cooldown_bars,
          external_trend_ok_series=external_trend_ok,
        )

        summary = bt.run()
        summary["higher_tf_filter"] = higher_tf_filter_name
        summary["whitelist_enabled"] = True

        signal_events_df = deduplicate_signal_events(bt.signal_events_df())
        signal_digest_df = build_signal_digest(signal_events_df)

        indicators_df = bt.indicators_df().copy()
        indicators_df["bull_confirm_bars"] = bull_confirm_bars
        indicators_df["regime_cooldown_bars"] = regime_cooldown_bars
        indicators_df["higher_tf_filter"] = higher_tf_filter_name

        save_df(indicators_df, out_dir / f"{symbol}_{tf.name}_ohlcv_v8_byma.csv")
        save_df(bt.trades_df(), out_dir / f"{symbol}_{tf.name}_trades_v8_byma.csv")
        save_df(
          bt.trade_trace_df(), out_dir / f"{symbol}_{tf.name}_trade_trace_v8_byma.csv"
        )
        save_df(
          pd.DataFrame([summary]), out_dir / f"{symbol}_{tf.name}_summary_v8_byma.csv"
        )
        save_df(
          signal_events_df, out_dir / f"{symbol}_{tf.name}_signal_events_v8_byma.csv"
        )
        save_df(
          signal_digest_df, out_dir / f"{symbol}_{tf.name}_signal_digest_v8_byma.csv"
        )

        symbol_summary.append(summary)

        if not signal_events_df.empty:
          symbol_signal_events.append(signal_events_df)
          market_signal_events.append(signal_events_df)

        if not signal_digest_df.empty:
          symbol_signal_digest.append(signal_digest_df)
          market_signal_digest.append(signal_digest_df)

        print(
          f" - 完成，生成 {len(bt.trades)} 笔交易，"
          f"{len(signal_events_df)} 条信号事件，"
          f"{len(signal_digest_df)} 条信号摘要，"
          f"params=(bull_confirm_bars={bull_confirm_bars}, cooldown={regime_cooldown_bars}, higher_tf_filter={higher_tf_filter_name or 'NONE'})"
        )
      except Exception as e:
        print(f" - [错误] 处理 {tf.name} 时发生异常: {e}")

    if symbol_summary:
      all_symbol_summary_df = pd.DataFrame(symbol_summary)
      save_df(all_symbol_summary_df, out_dir / f"{symbol}_all_summary_v8_byma.csv")
      final_summary_list.extend(symbol_summary)

    if symbol_signal_events:
      all_symbol_signal_events_df = pd.concat(symbol_signal_events, ignore_index=True)
      all_symbol_signal_events_df = deduplicate_signal_events(
        all_symbol_signal_events_df
      )
      save_df(
        all_symbol_signal_events_df, out_dir / f"{symbol}_all_signal_events_v8_byma.csv"
      )

      symbol_readable_df = build_readable_signal_events(all_symbol_signal_events_df)
      symbol_last_df = build_last_events_per_symbol_timeframe(symbol_readable_df)
      symbol_ref_df = build_symbol_reference_dates(symbol_readable_df)
      symbol_last_digest_df = build_last_digest_by_symbol(
        symbol_last_df,
        reference_dates_df=symbol_ref_df,
        fresh_days=conf.fresh_days,
      )

      save_df(
        symbol_readable_df, out_dir / f"{symbol}_signal_events_readable_v8_byma.csv"
      )
      save_df(
        symbol_last_df,
        out_dir / f"{symbol}_signal_events_last_per_timeframe_v8_byma.csv",
      )
      save_df(
        symbol_last_digest_df,
        out_dir / f"{symbol}_signal_digest_last_per_symbol_v8_byma.csv",
      )

    if symbol_signal_digest:
      all_symbol_signal_digest_df = pd.concat(symbol_signal_digest, ignore_index=True)
      all_symbol_signal_digest_df = all_symbol_signal_digest_df.sort_values(
        by=["event_date", "symbol", "timeframe", "latest_event_time"],
        na_position="last",
      ).reset_index(drop=True)
      save_df(
        all_symbol_signal_digest_df, out_dir / f"{symbol}_signal_digest_v8_byma.csv"
      )
      print(f" [完成] {symbol} 的 last_per_timeframe / last_per_symbol 文件已输出")

  if final_summary_list:
    all_market_summary_df = pd.DataFrame(final_summary_list)
    save_df(all_market_summary_df, out_dir / "market_all_summary_v8_byma.csv")

  if market_signal_events:
    market_all_signal_events_df = pd.concat(market_signal_events, ignore_index=True)
    market_all_signal_events_df = deduplicate_signal_events(market_all_signal_events_df)
    save_df(
      market_all_signal_events_df, out_dir / "market_all_signal_events_v8_byma.csv"
    )

    market_readable_df = build_readable_signal_events(market_all_signal_events_df)
    market_last_df = build_last_events_per_symbol_timeframe(market_readable_df)
    market_ref_df = build_symbol_reference_dates(market_readable_df)
    market_last_digest_df = build_last_digest_by_symbol(
      market_last_df,
      reference_dates_df=market_ref_df,
      fresh_days=conf.fresh_days,
    )

    save_df(market_readable_df, out_dir / "market_signal_events_readable_v8_byma.csv")
    save_df(
      market_last_df, out_dir / "market_signal_events_last_per_timeframe_v8_byma.csv"
    )
    save_df(
      market_last_digest_df,
      out_dir / "market_signal_digest_last_per_symbol_v8_byma.csv",
    )

  if market_signal_digest:
    market_signal_digest_df = pd.concat(market_signal_digest, ignore_index=True)
    market_signal_digest_df = market_signal_digest_df.sort_values(
      by=["event_date", "symbol", "timeframe", "latest_event_time"],
      na_position="last",
    ).reset_index(drop=True)
    save_df(market_signal_digest_df, out_dir / "market_signal_digest_v8_byma.csv")

  print("\n" + "=" * 60)
  print("V8-BYMA 全市场回测完成")
  print(f"结果保存至: {out_dir}")
  print(f"新鲜度过滤 fresh_days = {conf.fresh_days}")
  print(f"白名单: {SYMBOL_TIMEFRAME_WHITELIST}")
  print(f"高周期过滤映射: {HIGHER_TF_FILTER_MAP}")


if __name__ == "__main__":
  main()
