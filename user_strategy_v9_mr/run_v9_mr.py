# -*- coding: utf-8 -*-
from __future__ import annotations

import json
import re
from pathlib import Path
from typing import List, Optional

import pandas as pd

from user_strategy_v9_mr.config import StrategyConfig
from user_strategy_v9_mr.backtest_engine import MRBacktester


READABLE_EVENT_TYPES = {
  "LONG_ENTRY_READY",
  "LONG_WEAKEN_ALERT",
  "LONG_EXIT_TREND",
  "LONG_STOP_LOSS",
}

TIMEFRAME_ORDER = ["1d"]


def save_df(df: pd.DataFrame, path: Path):
  path.parent.mkdir(parents=True, exist_ok=True)
  df.to_csv(path, index=False, encoding="utf-8-sig")


def get_all_symbols(data_dir: Path) -> List[str]:
  symbols = set()
  for file in data_dir.glob("*_1d.csv"):
    parts = file.name.split("_")
    if parts:
      symbols.add(parts[0])
  return sorted(list(symbols))


def pick_latest_1d_file(data_dir: Path, symbol: str) -> Optional[Path]:
  files = list(data_dir.glob(f"{symbol}_*_1d.csv"))
  if not files:
    return None

  def extract_date(p: Path):
    m = re.match(rf"^{re.escape(symbol)}_(\d{{4}}-\d{{2}}-\d{{2}})_1d\.csv$", p.name)
    if m:
      return pd.to_datetime(m.group(1), errors="coerce")
    return pd.NaT

  ranked = []
  for f in files:
    dt = extract_date(f)
    ranked.append((dt, f))

  ranked.sort(
    key=lambda x: (pd.notna(x[0]), x[0] if pd.notna(x[0]) else pd.Timestamp.min),
    reverse=True,
  )
  return ranked[0][1] if ranked else None


def load_ohlcv(
  csv_path: Path, start_time: Optional[str] = None, end_time: Optional[str] = None
) -> pd.DataFrame:
  df = pd.read_csv(csv_path)
  cols_lower = {c.lower(): c for c in df.columns}

  rename_map = {}
  for std in ["time", "open", "high", "low", "close", "volume"]:
    if std in cols_lower:
      rename_map[cols_lower[std]] = std

  df = df.rename(columns=rename_map)

  required = ["time", "open", "high", "low", "close", "volume"]
  for col in required:
    if col not in df.columns:
      raise ValueError(f"{csv_path.name} 缺少必要列: {col}")

  df["time"] = pd.to_datetime(df["time"], errors="coerce")
  df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)

  if start_time:
    df = df[df["time"] >= pd.to_datetime(start_time)]
  if end_time:
    df = df[df["time"] <= pd.to_datetime(end_time)]

  df = df.reset_index(drop=True)
  df["time"] = df["time"].dt.strftime("%Y-%m-%d %H:%M:%S")
  df["idx"] = df.index

  return df


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
    ],
    keep="first",
  ).reset_index(drop=True)

  return result


def build_signal_digest(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  keep_event_types = {
    "LONG_ENTRY_READY",
    "LONG_WEAKEN_ALERT",
    "LONG_EXIT_TREND",
    "LONG_STOP_LOSS",
  }
  xdf = df[df["event_type"].isin(keep_event_types)].copy()
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
  ]:
    if col not in xdf.columns:
      xdf[col] = None

  xdf["event_date"] = pd.to_datetime(xdf["event_time"], errors="coerce").dt.strftime(
    "%Y/%m/%d"
  )
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
    out["timeframe"] = "1d"
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
    if "event_type" in out.columns:
      out["latest_event_type"] = out["event_type"]
    else:
      out["latest_event_type"] = ""

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
  out["event_time_dt"] = pd.to_datetime(out["event_time_raw"], errors="coerce")

  if "event_date" in out.columns:
    out["event_date_dt"] = pd.to_datetime(out["event_date"], errors="coerce")
  else:
    out["event_date_dt"] = pd.NaT

  out["event_date_dt"] = out["event_date_dt"].where(
    out["event_date_dt"].notna(),
    out["event_time_dt"].dt.normalize(),
  )

  out["event_date_str"] = out["event_date_dt"].dt.strftime("%Y/%m/%d")
  out["event_time_str"] = out["event_time_dt"].dt.strftime("%Y/%m/%d %H:%M")
  out["event_date_str"] = out["event_date_str"].fillna("")
  out["event_time_str"] = out["event_time_str"].fillna("")

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
        "stop_price",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    )

  x = _ensure_event_columns(df)
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
        "stop_price",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    )

  x = x.sort_values(
    ["symbol", "timeframe", "event_time_dt"], ascending=[True, True, True]
  )
  x["latest_event_time"] = x["event_time_str"].fillna("")
  x["event_date"] = x["event_date_str"].fillna("")

  x["readable_text"] = x.apply(
    lambda r: (
      f"{r['symbol']} | 1D | {r['latest_event_time']} | "
      f"{r['latest_event_type']} | {r['signal_text'] or ''}"
      f"{' | latest_price=' + _format_price(r['latest_price']) if _format_price(r['latest_price']) else ''}"
      f"{' | stop_price=' + _format_price(r['stop_price']) if _format_price(r['stop_price']) else ''}"
      f"{' | reason=' + str(r['reason']) if pd.notna(r['reason']) and str(r['reason']).strip() else ''}"
    ),
    axis=1,
  )

  return x[
    [
      "symbol",
      "timeframe",
      "event_date",
      "latest_event_type",
      "latest_event_time",
      "latest_price",
      "stop_price",
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
        "stop_price",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    )

  x = readable_df.copy()
  x["event_date_dt"] = pd.to_datetime(x["event_date"], errors="coerce")
  x["latest_event_time_dt"] = pd.to_datetime(x["latest_event_time"], errors="coerce")

  x = x.sort_values(
    ["symbol", "timeframe", "latest_event_time_dt"],
    ascending=[True, True, True],
  )

  latest = (
    x.groupby(["symbol", "timeframe"], as_index=False, group_keys=False).tail(1).copy()
  )

  latest = latest[
    [
      "symbol",
      "timeframe",
      "event_date",
      "latest_event_type",
      "latest_event_time",
      "latest_price",
      "stop_price",
      "reason",
      "signal_text",
      "event_count",
      "readable_text",
    ]
  ].sort_values(["symbol", "timeframe"])

  return latest


def _event_type_rank(event_type: str) -> int:
  rank = {
    "LONG_STOP_LOSS": 4,
    "LONG_EXIT_TREND": 3,
    "LONG_ENTRY_READY": 2,
    "LONG_WEAKEN_ALERT": 1,
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
      "1d_stop_price",
      "has_signal",
      "summary_text",
      "summary_json",
    ]
    return pd.DataFrame(columns=cols)

  x = last_df.copy()
  x["event_date_dt"] = pd.to_datetime(x["event_date"], errors="coerce")
  x["latest_event_time_dt"] = pd.to_datetime(x["latest_event_time"], errors="coerce")

  rows = []
  for symbol, g in x.groupby("symbol", sort=True):
    item = {"symbol": symbol}
    reference_date_dt = g["event_date_dt"].max()
    reference_date = (
      reference_date_dt.strftime("%Y/%m/%d") if pd.notna(reference_date_dt) else ""
    )

    item["reference_date"] = reference_date
    item["signal_date"] = reference_date
    item["fresh_days"] = fresh_days

    tf_payload = {}

    sub = g[g["timeframe"] == "1d"].copy()
    if sub.empty:
      item["1d_event_type"] = ""
      item["1d_signal_text"] = ""
      item["1d_event_time"] = ""
      item["1d_latest_price"] = ""
      item["1d_stop_price"] = ""
      tf_payload["1d"] = {
        "event_type": "",
        "signal_text": "",
        "event_time": "",
        "latest_price": "",
        "stop_price": "",
        "is_fresh": False,
        "age_days": None,
      }
    else:
      sub = sub.sort_values(["event_date_dt", "latest_event_time_dt"])
      r = sub.iloc[-1]
      event_date_dt = r.get("event_date_dt")
      age_days = None
      is_fresh = False

      if pd.notna(reference_date_dt) and pd.notna(event_date_dt):
        age_days = int((reference_date_dt - event_date_dt).days)
        is_fresh = age_days <= fresh_days

      if is_fresh:
        item["1d_event_type"] = r.get("latest_event_type", "")
        item["1d_signal_text"] = r.get("signal_text", "")
        item["1d_event_time"] = r.get("latest_event_time", "")
        item["1d_latest_price"] = _format_price(r.get("latest_price"))
        item["1d_stop_price"] = _format_price(r.get("stop_price"))
      else:
        item["1d_event_type"] = ""
        item["1d_signal_text"] = ""
        item["1d_event_time"] = ""
        item["1d_latest_price"] = ""
        item["1d_stop_price"] = ""

      tf_payload["1d"] = {
        "event_type": r.get("latest_event_type", ""),
        "signal_text": r.get("signal_text", ""),
        "event_time": r.get("latest_event_time", ""),
        "latest_price": _format_price(r.get("latest_price")),
        "stop_price": _format_price(r.get("stop_price")),
        "is_fresh": is_fresh,
        "age_days": age_days,
      }

    item["has_signal"] = bool(item.get("1d_event_type", ""))

    if item["1d_event_type"]:
      extra = []
      if item["1d_signal_text"]:
        extra.append(item["1d_signal_text"])
      if item["1d_event_time"]:
        extra.append(f"time={item['1d_event_time']}")
      if item["1d_latest_price"]:
        extra.append(f"price={item['1d_latest_price']}")
      if item["1d_stop_price"]:
        extra.append(f"stop={item['1d_stop_price']}")
      line = "1D: " + item["1d_event_type"] + " | " + " | ".join(extra)
    else:
      line = "1D: 无信号"

    item["summary_text"] = (
      f"{symbol} | ref={item['reference_date']} | fresh_days={fresh_days} | {line}"
    )
    item["summary_json"] = json.dumps(tf_payload, ensure_ascii=False)
    rows.append(item)

  out = pd.DataFrame(rows)
  out["_rank"] = out["1d_event_type"].apply(_event_type_rank)
  out = out.sort_values(["_rank", "symbol"], ascending=[False, True]).drop(
    columns=["_rank"]
  )
  return out


def main():
  repo_root = Path(__file__).resolve().parent.parent
  conf = StrategyConfig()

  data_dir = conf.resolved_data_dir(repo_root)
  out_dir = conf.resolved_output_dir(repo_root)

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

    csv_path = pick_latest_1d_file(data_dir, symbol)
    if not csv_path:
      print(f" [!] 未找到 {symbol} 的 1d 数据文件，跳过")
      continue

    print(f" 处理时间框架: 1d (1D) -> {csv_path.name}")

    try:
      df = load_ohlcv(
        csv_path,
        start_time=conf.timeframes[0].start_time,
        end_time=conf.timeframes[0].end_time,
      )

      bt = MRBacktester(
        symbol=symbol,
        timeframe="1d",
        df=df,
        macd_fast=conf.macd_fast,
        macd_slow=conf.macd_slow,
        macd_signal=conf.macd_signal,
        rsi_period=conf.rsi_period,
        ma_period=conf.ma_period,
        vol_ma_period=conf.vol_ma_period,
        swing_lookback=conf.swing_lookback,
        volume_multiplier_entry=conf.volume_multiplier_entry,
        volume_multiplier_break=conf.volume_multiplier_break,
        fixed_stop_pct=conf.fixed_stop_pct,
        next_day_trigger=conf.next_day_trigger,
      )

      summary = bt.run()
      signal_events_df = deduplicate_signal_events(bt.signal_events_df())
      signal_digest_df = build_signal_digest(signal_events_df)

      save_df(bt.df, out_dir / f"{symbol}_1d_ohlcv_v9_mr.csv")
      save_df(bt.trades_df(), out_dir / f"{symbol}_1d_trades_v9_mr.csv")
      save_df(bt.trade_trace_df(), out_dir / f"{symbol}_1d_trade_trace_v9_mr.csv")
      save_df(pd.DataFrame([summary]), out_dir / f"{symbol}_1d_summary_v9_mr.csv")
      save_df(signal_events_df, out_dir / f"{symbol}_1d_signal_events_v9_mr.csv")
      save_df(signal_digest_df, out_dir / f"{symbol}_1d_signal_digest_v9_mr.csv")

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
        f"{len(signal_digest_df)} 条信号摘要"
      )
    except Exception as e:
      print(f" - [错误] 处理 1d 时发生异常: {e}")

    if symbol_summary:
      all_symbol_summary_df = pd.DataFrame(symbol_summary)
      save_df(all_symbol_summary_df, out_dir / f"{symbol}_all_summary_v9_mr.csv")
      final_summary_list.extend(symbol_summary)

    if symbol_signal_events:
      all_symbol_signal_events_df = pd.concat(symbol_signal_events, ignore_index=True)
      all_symbol_signal_events_df = deduplicate_signal_events(
        all_symbol_signal_events_df
      )

      save_df(
        all_symbol_signal_events_df, out_dir / f"{symbol}_all_signal_events_v9_mr.csv"
      )

      symbol_readable_df = build_readable_signal_events(all_symbol_signal_events_df)
      symbol_last_df = build_last_events_per_symbol_timeframe(symbol_readable_df)
      symbol_last_digest_df = build_last_digest_by_symbol(
        symbol_last_df, fresh_days=conf.fresh_days
      )

      save_df(
        symbol_readable_df, out_dir / f"{symbol}_signal_events_readable_v9_mr.csv"
      )
      save_df(
        symbol_last_df, out_dir / f"{symbol}_signal_events_last_per_timeframe_v9_mr.csv"
      )
      save_df(
        symbol_last_digest_df,
        out_dir / f"{symbol}_signal_digest_last_per_symbol_v9_mr.csv",
      )

    if symbol_signal_digest:
      all_symbol_signal_digest_df = pd.concat(symbol_signal_digest, ignore_index=True)
      all_symbol_signal_digest_df = all_symbol_signal_digest_df.sort_values(
        by=["event_date", "symbol", "timeframe", "latest_event_time"],
        na_position="last",
      ).reset_index(drop=True)
      save_df(
        all_symbol_signal_digest_df, out_dir / f"{symbol}_signal_digest_v9_mr.csv"
      )

    print(f" [完成] {symbol} 的 last_per_timeframe / last_per_symbol 文件已输出")

  if final_summary_list:
    all_market_summary_df = pd.DataFrame(final_summary_list)
    save_df(all_market_summary_df, out_dir / "market_all_summary_v9_mr.csv")

  if market_signal_events:
    market_all_signal_events_df = pd.concat(market_signal_events, ignore_index=True)
    market_all_signal_events_df = deduplicate_signal_events(market_all_signal_events_df)
    save_df(market_all_signal_events_df, out_dir / "market_all_signal_events_v9_mr.csv")

    market_readable_df = build_readable_signal_events(market_all_signal_events_df)
    market_last_df = build_last_events_per_symbol_timeframe(market_readable_df)
    market_last_digest_df = build_last_digest_by_symbol(
      market_last_df, fresh_days=conf.fresh_days
    )

    save_df(market_readable_df, out_dir / "market_signal_events_readable_v9_mr.csv")
    save_df(
      market_last_df, out_dir / "market_signal_events_last_per_timeframe_v9_mr.csv"
    )
    save_df(
      market_last_digest_df, out_dir / "market_signal_digest_last_per_symbol_v9_mr.csv"
    )

  if market_signal_digest:
    market_signal_digest_df = pd.concat(market_signal_digest, ignore_index=True)
    market_signal_digest_df = market_signal_digest_df.sort_values(
      by=["event_date", "symbol", "timeframe", "latest_event_time"],
      na_position="last",
    ).reset_index(drop=True)
    save_df(market_signal_digest_df, out_dir / "market_signal_digest_v9_mr.csv")

  print("\n" + "=" * 60)
  print("V9-MR 全市场回测完成")
  print(f"结果保存至: {out_dir}")
  print(f"新鲜度过滤 fresh_days = {conf.fresh_days}")


if __name__ == "__main__":
  main()
