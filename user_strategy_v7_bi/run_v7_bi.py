# -*- coding: utf-8 -*-
from __future__ import annotations

import json
from pathlib import Path
from typing import List

import pandas as pd

from user_strategy_v7_bi.config import StrategyConfig
from user_strategy_v7_bi.chan_loader import (
  load_chan_data,
  extract_kline_data,
  extract_bi_data,
)
from user_strategy_v7_bi.backtest_engine import BiBacktester


READABLE_EVENT_TYPES = {
  "BUY_SIGNAL",
  "SELL_SIGNAL",
  "STOP_LOSS_TRIGGERED",
}

TIMEFRAME_ORDER = ["1d", "4h", "2h", "1h"]
FRESH_DAYS = 10


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
    "bi_id",
    "bar_index",
    "trade_id",
    "event_seq",
  ]:
    if col not in result.columns:
      result[col] = ""

  result = result.sort_values(
    by=[
      "symbol",
      "timeframe",
      "event_time",
      "bar_index",
      "event_seq",
    ],
    na_position="last",
  ).reset_index(drop=True)

  result = result.drop_duplicates(
    subset=[
      "symbol",
      "timeframe",
      "event_type",
      "event_time",
      "reason",
      "bi_id",
      "bar_index",
      "trade_id",
    ],
    keep="first",
  ).reset_index(drop=True)

  return result


def build_signal_digest(df: pd.DataFrame) -> pd.DataFrame:
  if df is None or df.empty:
    return pd.DataFrame()

  keep_event_types = {"BUY_SIGNAL", "SELL_SIGNAL", "STOP_LOSS_TRIGGERED"}
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
    "bi_id",
    "reason",
    "signal_text",
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
        "bi_id": latest.get("bi_id", None),
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

  if "bi_id" not in out.columns:
    out["bi_id"] = None

  if "stop_price" not in out.columns:
    out["stop_price"] = None

  if "event_count" not in out.columns:
    out["event_count"] = 1

  if "latest_price" not in out.columns:
    if "price" in out.columns:
      out["latest_price"] = out["price"]
    elif "trigger_price" in out.columns:
      out["latest_price"] = out["trigger_price"]
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
    lambda r: pick_first_non_null(r, time_candidates),
    axis=1,
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
        "bi_id",
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
        "bi_id",
        "reason",
        "signal_text",
        "event_count",
        "readable_text",
      ]
    )

  x = x.sort_values(
    ["symbol", "timeframe", "event_time_dt", "bi_id"],
    ascending=[True, True, True, True],
  )

  x["latest_event_time"] = x["event_time_raw"].fillna("")
  x["event_date"] = x["event_date_str"].fillna("")

  def normalize_display_time(v: str) -> str:
    if not v:
      return ""
    ts = pd.to_datetime(v, errors="coerce")
    if pd.isna(ts):
      return str(v)
    if ts.hour == 0 and ts.minute == 0 and ts.second == 0:
      return ts.strftime("%Y/%m/%d %H:%M")
    return ts.strftime("%Y/%m/%d %H:%M")

  x["latest_event_time"] = x["latest_event_time"].apply(normalize_display_time)

  x["readable_text"] = x.apply(
    lambda r: (
      f"{r['symbol']} | {_format_timeframe_label(r['timeframe'])} | "
      f"{r['latest_event_time']} | "
      f"{r['latest_event_type']} | "
      f"{r['signal_text'] or ''}"
      f"{' | latest_price=' + _format_price(r['latest_price']) if _format_price(r['latest_price']) else ''}"
      f"{' | stop_price=' + _format_price(r['stop_price']) if _format_price(r['stop_price']) else ''}"
      f"{' | bi_id=' + str(int(float(r['bi_id']))) if pd.notna(r['bi_id']) and str(r['bi_id']) != '' else ''}"
      f"{' | reason=' + str(r['reason']) if pd.notna(r['reason']) and str(r['reason']).strip() else ''}"
    ),
    axis=1,
  )

  out = x[
    [
      "symbol",
      "timeframe",
      "event_date",
      "latest_event_type",
      "latest_event_time",
      "latest_price",
      "stop_price",
      "bi_id",
      "reason",
      "signal_text",
      "event_count",
      "readable_text",
    ]
  ].copy()

  return out


def build_last_events_per_symbol_timeframe(readable_df: pd.DataFrame) -> pd.DataFrame:
  """
  每个 symbol + timeframe 保留最后一条可读信号
  """
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
  x["timeframe"] = x["timeframe"].map(_normalize_timeframe)

  x = x.sort_values(
    ["symbol", "timeframe", "latest_event_time_dt", "bi_id"],
    ascending=[True, True, True, True],
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
      "bi_id",
      "reason",
      "signal_text",
      "event_count",
      "readable_text",
    ]
  ].sort_values(["symbol", "timeframe"])

  return latest


def _event_type_rank(event_type: str) -> int:
  rank = {
    "STOP_LOSS_TRIGGERED": 3,
    "SELL_SIGNAL": 2,
    "BUY_SIGNAL": 1,
  }
  return rank.get(str(event_type), 0)


def build_last_digest_by_symbol(
  last_df: pd.DataFrame, fresh_days: int = FRESH_DAYS
) -> pd.DataFrame:
  """
  对每个 symbol：
  - 每个 timeframe 取最后一条信号
  - 以该 symbol 所有 timeframe 最后信号中的最大日期作为 reference_date
  - 若某 timeframe 的最后信号距离 reference_date 超过 fresh_days，则按“无信号”处理
  """
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
      "4h_event_type",
      "4h_signal_text",
      "4h_event_time",
      "4h_latest_price",
      "4h_stop_price",
      "2h_event_type",
      "2h_signal_text",
      "2h_event_time",
      "2h_latest_price",
      "2h_stop_price",
      "1h_event_type",
      "1h_signal_text",
      "1h_event_time",
      "1h_latest_price",
      "1h_stop_price",
      "has_signal",
      "summary_text",
      "summary_json",
    ]
    return pd.DataFrame(columns=cols)

  x = last_df.copy()
  x["timeframe"] = x["timeframe"].map(_normalize_timeframe)
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

    for tf in TIMEFRAME_ORDER:
      sub = g[g["timeframe"] == tf].copy()
      if sub.empty:
        item[f"{tf}_event_type"] = ""
        item[f"{tf}_signal_text"] = ""
        item[f"{tf}_event_time"] = ""
        item[f"{tf}_latest_price"] = ""
        item[f"{tf}_stop_price"] = ""
        tf_payload[tf] = {
          "event_type": "",
          "signal_text": "",
          "event_time": "",
          "latest_price": "",
          "stop_price": "",
          "is_fresh": False,
          "age_days": None,
        }
        continue

      sub = sub.sort_values(["event_date_dt", "latest_event_time_dt", "bi_id"])
      r = sub.iloc[-1]

      event_date_dt = r.get("event_date_dt")
      age_days = None
      is_fresh = False

      if pd.notna(reference_date_dt) and pd.notna(event_date_dt):
        age_days = int((reference_date_dt - event_date_dt).days)
        is_fresh = age_days <= fresh_days

      if is_fresh:
        item[f"{tf}_event_type"] = r.get("latest_event_type", "")
        item[f"{tf}_signal_text"] = r.get("signal_text", "")
        item[f"{tf}_event_time"] = r.get("latest_event_time", "")
        item[f"{tf}_latest_price"] = _format_price(r.get("latest_price"))
        item[f"{tf}_stop_price"] = _format_price(r.get("stop_price"))
      else:
        item[f"{tf}_event_type"] = ""
        item[f"{tf}_signal_text"] = ""
        item[f"{tf}_event_time"] = ""
        item[f"{tf}_latest_price"] = ""
        item[f"{tf}_stop_price"] = ""

      tf_payload[tf] = {
        "event_type": r.get("latest_event_type", ""),
        "signal_text": r.get("signal_text", ""),
        "event_time": r.get("latest_event_time", ""),
        "latest_price": _format_price(r.get("latest_price")),
        "stop_price": _format_price(r.get("stop_price")),
        "is_fresh": is_fresh,
        "age_days": age_days,
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

  def _symbol_rank(row):
    ranks = [
      _event_type_rank(row.get(f"{tf}_event_type", "")) for tf in TIMEFRAME_ORDER
    ]
    return max(ranks) if ranks else 0

  out["_rank"] = out.apply(_symbol_rank, axis=1)
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

    for tf in conf.timeframes:
      if not tf.enabled:
        continue

      if tf.level == "1D":
        matches = list(data_dir.glob(f"{symbol}_*_1d.csv"))
      else:
        matches = list(data_dir.glob(f"{symbol}_*_yf_{tf.name.lower()}_730d.csv"))

      if not matches:
        print(f" [!] 未找到 {symbol} 的 {tf.name} 数据文件，跳过")
        continue

      csv_path = matches[0]
      print(f" 处理时间框架: {tf.name} ({tf.level}) -> {csv_path.name}")

      try:
        chan, kl_type, kl_list = load_chan_data(
          code=symbol,
          level=tf.level,
          csv_path=csv_path,
          config=conf.chan_config,
          trigger_step=conf.trigger_step,
          begin_time=tf.start_time,
          end_time=tf.end_time,
        )

        kline_df = extract_kline_data(kl_list)
        bi_list = extract_bi_data(kl_list)

        bt = BiBacktester(
          symbol=symbol,
          timeframe=tf.name,
          df=kline_df,
          bi_list=bi_list,
          entry_delay_bars=conf.entry_delay_bars,
          exit_delay_bars=conf.exit_delay_bars,
          use_structure_stop=conf.use_structure_stop,
        )

        summary = bt.run()
        signal_events_df = deduplicate_signal_events(bt.signal_events_df())
        signal_digest_df = build_signal_digest(signal_events_df)

        save_df(kline_df, out_dir / f"{symbol}_{tf.name}_ohlcv_v7_bi.csv")
        save_df(
          pd.DataFrame(bi_list),
          out_dir / f"{symbol}_{tf.name}_bi_snapshot_v7_bi.csv",
        )
        save_df(bt.trades_df(), out_dir / f"{symbol}_{tf.name}_trades_v7_bi.csv")
        save_df(
          bt.trade_trace_df(),
          out_dir / f"{symbol}_{tf.name}_trade_trace_v7_bi.csv",
        )
        save_df(
          pd.DataFrame([summary]),
          out_dir / f"{symbol}_{tf.name}_summary_v7_bi.csv",
        )

        save_df(
          signal_events_df,
          out_dir / f"{symbol}_{tf.name}_signal_events_v7_bi.csv",
        )
        save_df(
          signal_digest_df,
          out_dir / f"{symbol}_{tf.name}_signal_digest_v7_bi.csv",
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
          f"{len(signal_digest_df)} 条信号摘要"
        )

      except Exception as e:
        print(f" - [错误] 处理 {tf.name} 时发生异常: {e}")

    if symbol_summary:
      all_symbol_summary_df = pd.DataFrame(symbol_summary)
      save_df(all_symbol_summary_df, out_dir / f"{symbol}_all_summary_v7_bi.csv")
      final_summary_list.extend(symbol_summary)

    if symbol_signal_events:
      all_symbol_signal_events_df = pd.concat(symbol_signal_events, ignore_index=True)
      all_symbol_signal_events_df = deduplicate_signal_events(
        all_symbol_signal_events_df
      )

      save_df(
        all_symbol_signal_events_df,
        out_dir / f"{symbol}_all_signal_events_v7_bi.csv",
      )

      symbol_readable_df = build_readable_signal_events(all_symbol_signal_events_df)
      symbol_last_df = build_last_events_per_symbol_timeframe(symbol_readable_df)
      symbol_last_digest_df = build_last_digest_by_symbol(
        symbol_last_df, fresh_days=FRESH_DAYS
      )

      save_df(
        symbol_readable_df,
        out_dir / f"{symbol}_signal_events_readable_v7_bi.csv",
      )
      save_df(
        symbol_last_df,
        out_dir / f"{symbol}_signal_events_last_per_timeframe_v7_bi.csv",
      )
      save_df(
        symbol_last_digest_df,
        out_dir / f"{symbol}_signal_digest_last_per_symbol_v7_bi.csv",
      )

    if symbol_signal_digest:
      all_symbol_signal_digest_df = pd.concat(symbol_signal_digest, ignore_index=True)
      all_symbol_signal_digest_df = all_symbol_signal_digest_df.sort_values(
        by=["event_date", "symbol", "timeframe", "latest_event_time"],
        na_position="last",
      ).reset_index(drop=True)
      save_df(
        all_symbol_signal_digest_df,
        out_dir / f"{symbol}_signal_digest_v7_bi.csv",
      )

    print(f" [完成] {symbol} 的 last_per_timeframe / last_per_symbol 文件已输出")

  if final_summary_list:
    all_market_summary_df = pd.DataFrame(final_summary_list)
    save_df(all_market_summary_df, out_dir / "market_all_summary_v7_bi.csv")

  if market_signal_events:
    market_all_signal_events_df = pd.concat(market_signal_events, ignore_index=True)
    market_all_signal_events_df = deduplicate_signal_events(market_all_signal_events_df)
    save_df(
      market_all_signal_events_df,
      out_dir / "market_all_signal_events_v7_bi.csv",
    )

    market_readable_df = build_readable_signal_events(market_all_signal_events_df)
    market_last_df = build_last_events_per_symbol_timeframe(market_readable_df)
    market_last_digest_df = build_last_digest_by_symbol(
      market_last_df, fresh_days=FRESH_DAYS
    )

    save_df(
      market_readable_df,
      out_dir / "market_signal_events_readable_v7_bi.csv",
    )
    save_df(
      market_last_df,
      out_dir / "market_signal_events_last_per_timeframe_v7_bi.csv",
    )
    save_df(
      market_last_digest_df,
      out_dir / "market_signal_digest_last_per_symbol_v7_bi.csv",
    )

  if market_signal_digest:
    market_signal_digest_df = pd.concat(market_signal_digest, ignore_index=True)
    market_signal_digest_df = market_signal_digest_df.sort_values(
      by=["event_date", "symbol", "timeframe", "latest_event_time"],
      na_position="last",
    ).reset_index(drop=True)
    save_df(
      market_signal_digest_df,
      out_dir / "market_signal_digest_v7_bi.csv",
    )

  print("\n" + "=" * 60)
  print("V7-BI 全市场回测完成")
  print(f"结果保存至: {out_dir}")
  print(f"新鲜度过滤 fresh_days = {FRESH_DAYS}")


if __name__ == "__main__":
  main()
