import logging
import uuid
import json
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Literal, cast
from zoneinfo import ZoneInfo
import pandas as pd
import math

from trade_system.config import get_config

logger = logging.getLogger(__name__)


def _get_positions_dir() -> Path:
  return Path(__file__).resolve().parent.parent / "data" / "positions"


def _get_open_position_queue_id(symbol: str) -> str:
  """读取当前持仓文件，返回某symbol未平仓的买入持仓的queue_id。"""
  positions_dir = _get_positions_dir()
  if not positions_dir.exists():
    return ""
  # 读取最新的 positions 文件
  positions_files = sorted(positions_dir.glob("*-positions.json"), reverse=True)
  for pf in positions_files:
    try:
      data = json.loads(pf.read_text(encoding="utf-8"))
      for pos in data.get("positions", []):
        # 未平仓的持仓
        if pos.get("symbol") == symbol and not pos.get("sell_order_id"):
          return pos.get("queue_id", "")
    except Exception:
      continue
  return ""


def _get_queue_dir() -> Path:
  try:
    return get_config().queue_dir
  except Exception:
    return Path(__file__).resolve().parent / "queue"


BUY_PRIORITY = [
  "user_strategy_v7_bi",
  "user_strategy_v5_macdtd",
  "user_strategy_v8_byma",
]
SELL_PRIORITY = [
  "user_strategy_v5_macdtd",
  "user_strategy_v7_bi",
  "user_strategy_v8_byma",
]
PERIOD_PRIORITY = ["1H", "2H", "4H", "1D"]


class Signal(NamedTuple):
  id: str
  symbol: str
  action: str
  strategy: str
  period: str
  target_price: float | None
  stop_price: float | None
  status: Literal["queued", "manual_review", "filled", "cancelled", "failed"]
  generated_at: str
  related_queue_id: str = ""


def _build_signal(
  symbol: str,
  action: str,
  strategy: str,
  period: str,
  row,
  status: Literal[
    "queued", "manual_review", "filled", "cancelled", "failed"
  ] = "queued",
  related_queue_id: str = "",
) -> Signal:
  return Signal(
    id=str(uuid.uuid4()),
    symbol=symbol,
    action=action,
    strategy=strategy,
    period=period,
    target_price=row.get("target_price"),
    stop_price=row.get("stop_price"),
    status=status,
    generated_at=datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
    related_queue_id=related_queue_id,
  )


def _pick_signal(buy_df: pd.DataFrame, sell_df: pd.DataFrame) -> Signal | None:
  """按优先级从 buy_df / sell_df 中选一个信号。"""
  buy_list = [
    r for r in buy_df.to_dict("records") if str(r.get("action")).lower() == "buy"
  ]
  sell_list = [
    r for r in sell_df.to_dict("records") if str(r.get("action")).lower() == "sell"
  ]

  def _best_candidate(rows: list[dict], priority_strategies: list[str]) -> dict | None:
    # Return the single best row according to strategy priority then period priority
    if not rows:
      return None
    # scan by strategy then period
    for strat in priority_strategies:
      for period in PERIOD_PRIORITY:
        for r in rows:
          if r.get("strategy") == strat and r.get("period") == period:
            return r
    # fallback: prefer first row with any non-empty strategy/period, else first row
    for r in rows:
      if r.get("strategy") or r.get("period"):
        return r
    return rows[0]

  # If both buy and sell exist, resolve by comparing the best candidates' priorities
  if buy_list and sell_list:
    best_buy = _best_candidate(buy_list, BUY_PRIORITY)
    best_sell = _best_candidate(sell_list, SELL_PRIORITY)
    if best_buy is None or best_sell is None:
      # Defensive fallback: mark for manual review if candidates could not be selected
      candidate = best_buy or best_sell
      if candidate is None:
        return None
      return _build_signal(
        candidate.get("symbol", ""),
        "",
        "conflict",
        "N/A",
        candidate,
        status="manual_review",
      )

    # compute strategy rank (lower index = higher priority)
    def _rank_strategy(strategy_name: str, priority_list: list[str]) -> int:
      try:
        return priority_list.index(strategy_name)
      except ValueError:
        return len(priority_list)

    buy_rank = (
      _rank_strategy(best_buy.get("strategy", ""), BUY_PRIORITY)
      if best_buy
      else len(BUY_PRIORITY)
    )
    sell_rank = (
      _rank_strategy(best_sell.get("strategy", ""), SELL_PRIORITY)
      if best_sell
      else len(SELL_PRIORITY)
    )

    # If one side has a strictly higher strategy priority, pick it
    if buy_rank < sell_rank:
      assert best_buy is not None
      return _build_signal(
        best_buy["symbol"],
        "buy",
        best_buy.get("strategy", ""),
        best_buy.get("period", ""),
        best_buy,
      )
    if sell_rank < buy_rank:
      assert best_sell is not None
      return _build_signal(
        best_sell["symbol"],
        "sell",
        best_sell.get("strategy", ""),
        best_sell.get("period", ""),
        best_sell,
      )

    # If strategy ranks tie, use period priority
    def _period_index(period_label: str) -> int:
      try:
        return PERIOD_PRIORITY.index(period_label)
      except ValueError:
        return len(PERIOD_PRIORITY)

    buy_period_idx = (
      _period_index(best_buy.get("period", "")) if best_buy else len(PERIOD_PRIORITY)
    )
    sell_period_idx = (
      _period_index(best_sell.get("period", "")) if best_sell else len(PERIOD_PRIORITY)
    )

    if buy_period_idx < sell_period_idx:
      assert best_buy is not None
      return _build_signal(
        best_buy["symbol"],
        "buy",
        best_buy.get("strategy", ""),
        best_buy.get("period", ""),
        best_buy,
      )
    if sell_period_idx < buy_period_idx:
      assert best_sell is not None
      return _build_signal(
        best_sell["symbol"],
        "sell",
        best_sell.get("strategy", ""),
        best_sell.get("period", ""),
        best_sell,
      )

    # If still tied (unlikely), mark manual review and attach conflict info
    return _build_signal(
      best_buy.get("symbol", best_sell.get("symbol", "")),
      "",
      "conflict",
      "N/A",
      best_buy or best_sell,
      status="manual_review",
    )

  # Only buy candidates
  if buy_list:
    r = _best_candidate(buy_list, BUY_PRIORITY)
    if r is None:
      return None
    return _build_signal(
      r["symbol"], "buy", r.get("strategy", ""), r.get("period", ""), r
    )

  # Only sell candidates
  if sell_list:
    r = _best_candidate(sell_list, SELL_PRIORITY)
    if r is None:
      return None
    return _build_signal(
      r["symbol"], "sell", r.get("strategy", ""), r.get("period", ""), r
    )

  return None


def write_queue_from_digest(digest_csv: Path, output_path: Path | None = None) -> Path:
  """从单个 digest CSV 生成队列（保持向后兼容）。"""
  df = pd.read_csv(digest_csv)
  # Normalize schema: if 'action' is missing, attempt to infer from per-strategy event_type fields
  if "action" not in df.columns:
    try:
      strategy = infer_strategy_from_filename(digest_csv.name)
      df = _normalize_digest_df(df, strategy, source_file=digest_csv.name)
    except Exception:
      logger.exception("failed to normalize digest CSV %s", digest_csv)
  queue_data = {
    "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
    "signals": [],
  }
  for symbol, group in df.groupby("symbol"):
    buy_df = cast(pd.DataFrame, group.loc[group["action"] == "buy"])
    sell_df = cast(pd.DataFrame, group.loc[group["action"] == "sell"])
    signal = _pick_signal(buy_df, sell_df)
    if signal:
      queue_data["signals"].append(signal._asdict())

  if output_path is None:
    today = datetime.now().strftime("%Y%m%d")
    output_path = _get_queue_dir() / f"{today}-queue.json"

  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_path.write_text(json.dumps(queue_data, ensure_ascii=False, indent=2))
  return output_path


def write_queue_from_multiple_digests(
  digest_csvs: list[Path], output_path: Path | None = None
) -> Path:
  """从多个 digest CSV 合并生成队列，按策略+周期优先级去重选最优信号。"""
  all_rows = []
  for csv_file in digest_csvs:
    if not csv_file.exists():
      continue
    df = pd.read_csv(csv_file)
    # Normalize per-file if needed before merging
    if "action" not in df.columns:
      try:
        strategy = infer_strategy_from_filename(csv_file.name)
        df = _normalize_digest_df(df, strategy, source_file=csv_file.name)
      except Exception:
        logger.exception("failed to normalize digest %s", csv_file)
    df["_source_file"] = csv_file.name
    all_rows.append(df)

  # If no input files were valid, return an empty queue
  if not all_rows:
    queue_data = {
      "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
      "signals": [],
    }
    if output_path is None:
      today = datetime.now().strftime("%Y%m%d")
      output_path = _get_queue_dir() / f"{today}-queue.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(queue_data, ensure_ascii=False, indent=2))
    return output_path

  merged = pd.concat(all_rows, ignore_index=True)

  queue_data = {
    "generated_at": datetime.now(ZoneInfo("Asia/Shanghai")).isoformat(),
    "signals": [],
  }

  for symbol, group in merged.groupby("symbol"):
    if "action" not in group.columns:
      logger.warning(f"[QUEUE] symbol={symbol} group missing 'action' column, skipping")
      continue
    buy_df = cast(pd.DataFrame, group.loc[group["action"] == "buy"])
    sell_df = cast(pd.DataFrame, group.loc[group["action"] == "sell"])
    signal = _pick_signal(buy_df, sell_df)
    if signal:
      # 如果是卖出信号，检查是否有未平仓的持仓，建立关联
      if signal.action == "sell":
        related_qid = _get_open_position_queue_id(symbol)
        if related_qid:
          signal = Signal(
            id=signal.id,
            symbol=signal.symbol,
            action=signal.action,
            strategy=signal.strategy,
            period=signal.period,
            target_price=signal.target_price,
            stop_price=signal.stop_price,
            status=signal.status,
            generated_at=signal.generated_at,
            related_queue_id=related_qid,
          )
      queue_data["signals"].append(signal._asdict())

  if output_path is None:
    today = datetime.now().strftime("%Y%m%d")
    output_path = _get_queue_dir() / f"{today}-queue.json"

  output_path.parent.mkdir(parents=True, exist_ok=True)
  output_path.write_text(json.dumps(queue_data, ensure_ascii=False, indent=2))
  return output_path


def load_queue_today() -> dict:
  today = datetime.now().strftime("%Y%m%d")
  queue_path = _get_queue_dir() / f"{today}-queue.json"

  if not queue_path.exists():
    return {"generated_at": "", "signals": []}

  return json.loads(queue_path.read_text())


def infer_strategy_from_filename(name: str) -> str:
  """Infer strategy directory name from digest filename"""
  n = name.lower()
  if "v8_byma" in n or "byma" in n:
    return "user_strategy_v8_byma"
  if "v7_bi" in n or "v7" in n and "bi" in n:
    return "user_strategy_v7_bi"
  if "v5_macdtd" in n or "macdtd" in n or "v5" in n:
    return "user_strategy_v5_macdtd"
  # fallback: try generic
  return "unknown_strategy"


def _normalize_digest_df(
  df: pd.DataFrame, strategy: str, source_file: str | None = None
) -> pd.DataFrame:
  """Add/derive columns: 'strategy', 'action', 'period', 'target_price', 'stop_price'.

  Rules:
  - Check timeframe event_type columns in PERIOD_PRIORITY order (1H,2H,4H,1D -> maps to 1h_event_type etc.)
  - Use per-strategy event_type->action mapping
  - Populate 'strategy' column with detected strategy
  - Populate 'target_price' and 'stop_price' from corresponding '<period>_latest_price' and '<period>_stop_price'
  """
  df = df.copy()
  # Ensure strategy column
  if "strategy" not in df.columns:
    df["strategy"] = strategy

  # Load mapping (allow external overrides)
  event_map = load_signal_mappings(strategy)

  # timeframe order aligned with PERIOD_PRIORITY used by _pick_signal
  tf_pairs = [("1h", "1H"), ("2h", "2H"), ("4h", "4H"), ("1d", "1D")]

  actions = []
  periods = []
  target_prices = []
  stop_prices = []

  for _, row in df.iterrows():
    action = None
    period_label = ""
    tgt = None
    stp = None
    for tf_col, label in tf_pairs:
      ev_col = f"{tf_col}_event_type"
      if ev_col not in df.columns:
        continue
      ev = row.get(ev_col)
      # Ensure ev is a scalar (sometimes row.get can return Series/array); skip array-like
      if isinstance(ev, (pd.Series, pd.DataFrame, list, tuple)):
        continue
      if ev is None:
        continue
      # handle numeric NaN explicitly to avoid pandas.isna returning array-like
      if isinstance(ev, float) and math.isnan(ev):
        continue
      evs = str(ev).strip()
      if evs == "":
        continue
      mapped = event_map.get(evs)
      if mapped is not None:
        action = mapped
        period_label = label
        # pick price/stop from matching timeframe if available
        latest_col = f"{tf_col}_latest_price"
        stop_col = f"{tf_col}_stop_price"
        if latest_col in df.columns:
          tgt = row.get(latest_col)
        if stop_col in df.columns:
          stp = row.get(stop_col)
        break
    actions.append(action if action is not None else "")
    periods.append(period_label)
    target_prices.append(tgt)
    stop_prices.append(stp)

  df["action"] = actions
  df["period"] = periods
  df["target_price"] = target_prices
  df["stop_price"] = stop_prices

  # Normalize empty strings to proper dtypes
  return df


def load_signal_mappings(strategy: str) -> dict:
  """Load per-strategy event_type -> action mapping.

  Priority: configs/signal_mappings/{strategy}.json (if present) -> built-in defaults
  """
  # built-in defaults (fallback)
  v8_map = {
    "LONG_ENTRY_READY": "buy",
    "LONG_WEAKEN_ALERT": None,
    "LONG_EXIT_TREND": "sell",
    "LONG_STOP_LOSS": "sell",
    "BULL_ENV_READY": None,
  }
  v7_map = {
    "BUY_SIGNAL": "buy",
    "POSITION_OPEN": "buy",
    "STOP_LOSS_TRIGGERED": "sell",
    "STOP_LOSS_ARMED": None,
    "SELL_SIGNAL": "sell",
    "TRADE_CLOSED": None,
    "BI_SKIPPED": None,
  }
  v5_map = {
    "BUY_SIGNAL": "buy",
    "ADD_POSITION": "buy",
    "SELL_SIGNAL": "sell",
    "STOP_UPDATE": None,
    "TAKE_PROFIT": "sell",
  }

  builtin = {
    "user_strategy_v8_byma": v8_map,
    "user_strategy_v7_bi": v7_map,
    "user_strategy_v5_macdtd": v5_map,
  }

  cfg_dir = Path(__file__).resolve().parent.parent / "configs" / "signal_mappings"
  cfg_file = cfg_dir / f"{strategy}.json"
  if cfg_file.exists():
    try:
      txt = cfg_file.read_text(encoding="utf-8")
      data = json.loads(txt)
      # Expect data to be a mapping of event_type->action for timeframe keys optionally
      # Normalize so lookup can be done by raw event string
      flat_map = {}
      for k, v in data.items():
        if isinstance(v, dict):
          for ev, a in v.items():
            flat_map[ev] = a
        else:
          # unexpected shape; ignore
          continue
      # overlay onto builtin
      base = builtin.get(strategy, {}).copy()
      base.update(flat_map)
      return base
    except Exception:
      logger.exception("failed to load mapping file %s", cfg_file)
  # fallback
  return builtin.get(strategy, {})
