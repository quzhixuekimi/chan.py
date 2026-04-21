# -*- coding: utf-8 -*-
from typing import List, Dict, Any, Optional
import pandas as pd


class BiBacktester:
  def __init__(
    self,
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    bi_list: List[Dict],
    entry_delay_bars: int = 2,
    exit_delay_bars: int = 2,
    use_structure_stop: bool = True,
  ):
    self.symbol = symbol
    self.timeframe = timeframe
    self.df = df.reset_index(drop=True).copy()
    self.bi_list = bi_list
    self.entry_delay_bars = entry_delay_bars
    self.exit_delay_bars = exit_delay_bars
    self.use_structure_stop = use_structure_stop

    self.trades: List[Dict[str, Any]] = []
    self.trade_trace: List[Dict[str, Any]] = []
    self.signal_events: List[Dict[str, Any]] = []

    self._signal_event_seq = 0
    self._time_col = self._get_time_col()

  def _get_time_col(self) -> str:
    candidates = ["time", "timestamp", "datetime", "dt", "date"]
    for c in candidates:
      if c in self.df.columns:
        return c
    return ""

  def _get_row_time(self, idx: int) -> str:
    if idx is None:
      return ""
    if idx < 0 or idx >= len(self.df):
      return ""
    if not self._time_col:
      return ""
    v = self.df.loc[idx, self._time_col]
    if pd.isna(v):
      return ""
    return str(v)

  def _get_event_date(self, event_time: str | None) -> str:
    if not event_time:
      return ""
    ts = pd.to_datetime(event_time, errors="coerce")
    if pd.isna(ts):
      return ""
    return ts.strftime("%Y/%m/%d")

  def _safe_row(self, idx: int) -> Dict[str, Any]:
    if idx < 0 or idx >= len(self.df):
      return {}
    row = self.df.loc[idx]
    return {
      "time": self._get_row_time(idx),
      "open": float(row["open"]) if "open" in row and pd.notna(row["open"]) else None,
      "high": float(row["high"]) if "high" in row and pd.notna(row["high"]) else None,
      "low": float(row["low"]) if "low" in row and pd.notna(row["low"]) else None,
      "close": float(row["close"])
      if "close" in row and pd.notna(row["close"])
      else None,
      "idx": int(row["idx"]) if "idx" in row and pd.notna(row["idx"]) else idx,
    }

  def _find_next_confirmed_down_bi(
    self, current_bi_id: Any, current_end_index: int | None
  ) -> Optional[Dict[str, Any]]:
    for next_bi in self.bi_list:
      if not next_bi.get("is_sure", True):
        continue
      if next_bi.get("direction") != "down":
        continue

      next_bi_id = next_bi.get("bi_id")
      next_start_index = next_bi.get("start_index")
      next_end_index = next_bi.get("end_index")

      if current_bi_id is not None and next_bi_id is not None:
        if next_bi_id > current_bi_id:
          return next_bi

      if current_end_index is not None and next_start_index is not None:
        if next_start_index > current_end_index:
          return next_bi

      if current_end_index is not None and next_end_index is not None:
        if next_end_index > current_end_index:
          return next_bi

    return None

  def _record_signal_event(
    self,
    event_type: str,
    bi: Dict[str, Any],
    bar_index: int | None = None,
    event_time: str | None = None,
    price: float | None = None,
    stop_price: float | None = None,
    planned_exit_idx: int | None = None,
    planned_exit_time: str | None = None,
    trigger_price_ref: str | None = None,
    reason: str | None = None,
    signal_text: str | None = None,
    trade_id: int | None = None,
    extra: Dict[str, Any] | None = None,
  ):
    resolved_event_time = event_time or self._get_row_time(bar_index) or ""
    resolved_event_date = self._get_event_date(resolved_event_time)
    resolved_planned_exit_time = (
      planned_exit_time or self._get_row_time(planned_exit_idx) or ""
    )

    self._signal_event_seq += 1
    event = {
      "event_seq": self._signal_event_seq,
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "event_type": event_type,
      "event_time": resolved_event_time,
      "event_date": resolved_event_date,
      "bar_index": bar_index,
      "price": round(float(price), 6) if price is not None else None,
      "stop_price": round(float(stop_price), 6) if stop_price is not None else None,
      "planned_exit_idx": planned_exit_idx,
      "planned_exit_time": resolved_planned_exit_time,
      "trigger_price_ref": trigger_price_ref or "",
      "reason": reason or "",
      "signal_text": signal_text or "",
      "trade_id": trade_id,
      "structure_type": "bi",
      "bi_id": bi.get("bi_id"),
      "bi_direction": bi.get("direction"),
      "bi_start_index": bi.get("start_index"),
      "bi_start_time": bi.get("start_time"),
      "bi_start_price": bi.get("start_price"),
      "bi_end_index": bi.get("end_index"),
      "bi_end_time": bi.get("end_time"),
      "bi_end_price": bi.get("end_price"),
      "bi_bars": bi.get("bars"),
      "bi_is_sure": bi.get("is_sure"),
    }
    if extra:
      event.update(extra)
    self.signal_events.append(event)

  def run(self):
    trade_id = 0

    for bi in self.bi_list:
      if not bi.get("is_sure", True):
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=bi.get("end_index"),
          event_time=bi.get("end_time"),
          reason="bi_not_sure",
          signal_text="跳过：当前笔未确认（顶分型尚未完成），不发出卖出信号",
        )
        continue

      if bi["direction"] != "up":
        self.trade_trace.append(
          {
            "timeframe": self.timeframe,
            "bi_id": bi["bi_id"],
            "bi_direction": bi["direction"],
            "action": "SKIP",
            "reason": "bi_not_up",
          }
        )
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=bi.get("start_index"),
          event_time=bi.get("start_time"),
          reason="bi_not_up",
          signal_text="跳过：当前笔不是向上笔，不参与做多信号回测",
        )
        continue

      entry_idx = bi["start_index"] + self.entry_delay_bars

      if entry_idx >= len(self.df):
        self.trade_trace.append(
          {
            "timeframe": self.timeframe,
            "bi_id": bi["bi_id"],
            "action": "SKIP",
            "reason": "entry_idx_out_of_range",
          }
        )
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=entry_idx,
          event_time=self._get_row_time(entry_idx),
          reason="entry_idx_out_of_range",
          signal_text="跳过：买入触发位置超出K线范围",
        )
        continue

      next_down_bi = self._find_next_confirmed_down_bi(
        current_bi_id=bi.get("bi_id"),
        current_end_index=bi.get("end_index"),
      )

      if next_down_bi is None:
        self.trade_trace.append(
          {
            "timeframe": self.timeframe,
            "bi_id": bi["bi_id"],
            "action": "SKIP",
            "reason": "next_confirmed_down_bi_not_found",
          }
        )
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=bi.get("end_index"),
          event_time=bi.get("end_time"),
          reason="next_confirmed_down_bi_not_found",
          signal_text="跳过：后续尚未出现确认向下笔，暂不生成正常卖出信号",
        )
        continue

      exit_anchor_idx = next_down_bi["start_index"]
      exit_idx = exit_anchor_idx + self.exit_delay_bars

      if exit_idx >= len(self.df):
        self.trade_trace.append(
          {
            "timeframe": self.timeframe,
            "bi_id": bi["bi_id"],
            "action": "SKIP",
            "reason": "exit_idx_out_of_range",
          }
        )
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=exit_idx,
          event_time=self._get_row_time(exit_idx),
          reason="exit_idx_out_of_range",
          signal_text="跳过：确认向下笔出现后的卖出触发位置超出K线范围",
          extra={
            "exit_bi_id": next_down_bi.get("bi_id"),
            "exit_bi_direction": next_down_bi.get("direction"),
            "exit_bi_start_index": next_down_bi.get("start_index"),
            "exit_bi_start_time": next_down_bi.get("start_time"),
            "exit_bi_start_price": next_down_bi.get("start_price"),
            "exit_bi_end_index": next_down_bi.get("end_index"),
            "exit_bi_end_time": next_down_bi.get("end_time"),
            "exit_bi_end_price": next_down_bi.get("end_price"),
          },
        )
        continue

      if exit_idx <= entry_idx:
        self.trade_trace.append(
          {
            "timeframe": self.timeframe,
            "bi_id": bi["bi_id"],
            "action": "SKIP",
            "reason": "exit_before_entry",
          }
        )
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=entry_idx,
          event_time=self._get_row_time(entry_idx),
          reason="exit_before_entry",
          signal_text="跳过：确认向下笔出现后的卖出触发位置早于或等于买入位置",
          extra={
            "exit_bi_id": next_down_bi.get("bi_id"),
            "exit_bi_direction": next_down_bi.get("direction"),
            "exit_bi_start_index": next_down_bi.get("start_index"),
            "exit_bi_start_time": next_down_bi.get("start_time"),
            "exit_bi_start_price": next_down_bi.get("start_price"),
            "exit_bi_end_index": next_down_bi.get("end_index"),
            "exit_bi_end_time": next_down_bi.get("end_time"),
            "exit_bi_end_price": next_down_bi.get("end_price"),
          },
        )
        continue

      entry_row = self._safe_row(entry_idx)
      entry_time = entry_row.get("time", "")
      entry_price = entry_row.get("open")
      stop_price = float(bi["start_price"])

      planned_exit_row = self._safe_row(exit_idx)
      planned_exit_time = planned_exit_row.get("time", "")
      planned_exit_price = planned_exit_row.get("close")

      actual_exit_idx = exit_idx
      actual_exit_price = planned_exit_price
      exit_reason = "confirmed_down_bi_start_plus_delay_bars_close"

      self.trade_trace.append(
        {
          "timeframe": self.timeframe,
          "bi_id": bi["bi_id"],
          "action": "OPEN_PLAN",
          "entry_idx": entry_idx,
          "entry_time": entry_time,
          "entry_price": round(entry_price, 6) if entry_price is not None else None,
          "stop_price": round(stop_price, 6),
          "planned_exit_idx": exit_idx,
          "planned_exit_time": planned_exit_time,
          "exit_anchor_idx": exit_anchor_idx,
          "exit_bi_id": next_down_bi.get("bi_id"),
          "exit_bi_direction": next_down_bi.get("direction"),
          "exit_bi_start_index": next_down_bi.get("start_index"),
          "exit_bi_start_time": next_down_bi.get("start_time"),
          "exit_bi_end_index": next_down_bi.get("end_index"),
          "exit_bi_end_time": next_down_bi.get("end_time"),
        }
      )

      self._record_signal_event(
        event_type="BUY_SIGNAL",
        bi=bi,
        bar_index=entry_idx,
        event_time=entry_time,
        price=entry_price,
        stop_price=stop_price,
        planned_exit_idx=exit_idx,
        planned_exit_time=planned_exit_time,
        trigger_price_ref="open",
        reason="up_bi_entry_delay_bars",
        signal_text=f"买入信号：向上笔起点后延迟 {self.entry_delay_bars} 根K线，以开盘价作为买入触发",
        extra={
          "exit_anchor_idx": exit_anchor_idx,
          "exit_bi_id": next_down_bi.get("bi_id"),
          "exit_bi_direction": next_down_bi.get("direction"),
          "exit_bi_start_index": next_down_bi.get("start_index"),
          "exit_bi_start_time": next_down_bi.get("start_time"),
          "exit_bi_start_price": next_down_bi.get("start_price"),
          "exit_bi_end_index": next_down_bi.get("end_index"),
          "exit_bi_end_time": next_down_bi.get("end_time"),
          "exit_bi_end_price": next_down_bi.get("end_price"),
        },
      )

      self._record_signal_event(
        event_type="STOP_LOSS_ARMED",
        bi=bi,
        bar_index=entry_idx,
        event_time=entry_time,
        price=entry_price,
        stop_price=stop_price,
        planned_exit_idx=exit_idx,
        planned_exit_time=planned_exit_time,
        trigger_price_ref="bi_start_price",
        reason="bi_structure_stop_initialized",
        signal_text="止损位设定：以当前笔起点价格作为结构止损位",
        extra={
          "exit_anchor_idx": exit_anchor_idx,
          "exit_bi_id": next_down_bi.get("bi_id"),
          "exit_bi_direction": next_down_bi.get("direction"),
          "exit_bi_start_index": next_down_bi.get("start_index"),
          "exit_bi_start_time": next_down_bi.get("start_time"),
          "exit_bi_start_price": next_down_bi.get("start_price"),
          "exit_bi_end_index": next_down_bi.get("end_index"),
          "exit_bi_end_time": next_down_bi.get("end_time"),
          "exit_bi_end_price": next_down_bi.get("end_price"),
        },
      )

      stop_triggered = False

      if self.use_structure_stop:
        for j in range(entry_idx + 1, exit_idx + 1):
          row = self._safe_row(j)
          row_time = row.get("time", "")
          row_low = row.get("low")
          row_close = row.get("close")

          if row_low is not None and row_low < stop_price:
            actual_exit_idx = j
            actual_exit_price = row_close
            exit_reason = "bi_structure_stop_break"
            stop_triggered = True

            self._record_signal_event(
              event_type="STOP_LOSS_TRIGGERED",
              bi=bi,
              bar_index=j,
              event_time=row_time,
              price=row_close,
              stop_price=stop_price,
              planned_exit_idx=exit_idx,
              planned_exit_time=planned_exit_time,
              trigger_price_ref="low_break_stop_close_exit",
              reason="bi_structure_stop_break",
              signal_text="止损信号：后续K线最低价跌破结构止损位，按该K线收盘价退出",
              extra={
                "exit_anchor_idx": exit_anchor_idx,
                "exit_bi_id": next_down_bi.get("bi_id"),
                "exit_bi_direction": next_down_bi.get("direction"),
                "exit_bi_start_index": next_down_bi.get("start_index"),
                "exit_bi_start_time": next_down_bi.get("start_time"),
                "exit_bi_start_price": next_down_bi.get("start_price"),
                "exit_bi_end_index": next_down_bi.get("end_index"),
                "exit_bi_end_time": next_down_bi.get("end_time"),
                "exit_bi_end_price": next_down_bi.get("end_price"),
              },
            )
            break

      if not stop_triggered:
        self._record_signal_event(
          event_type="SELL_SIGNAL",
          bi=bi,
          bar_index=exit_idx,
          event_time=planned_exit_time,
          price=planned_exit_price,
          stop_price=stop_price,
          planned_exit_idx=exit_idx,
          planned_exit_time=planned_exit_time,
          trigger_price_ref="close",
          reason="confirmed_down_bi_start_plus_delay_bars_close",
          signal_text=f"卖出信号：后续出现确认向下笔后，再延迟 {self.exit_delay_bars} 根K线，以收盘价作为卖出触发",
          extra={
            "exit_anchor_idx": exit_anchor_idx,
            "exit_bi_id": next_down_bi.get("bi_id"),
            "exit_bi_direction": next_down_bi.get("direction"),
            "exit_bi_start_index": next_down_bi.get("start_index"),
            "exit_bi_start_time": next_down_bi.get("start_time"),
            "exit_bi_start_price": next_down_bi.get("start_price"),
            "exit_bi_end_index": next_down_bi.get("end_index"),
            "exit_bi_end_time": next_down_bi.get("end_time"),
            "exit_bi_end_price": next_down_bi.get("end_price"),
          },
        )

      exit_row = self._safe_row(actual_exit_idx)
      exit_time = exit_row.get("time", "")
      pnl_abs = actual_exit_price - entry_price
      pnl_pct = (actual_exit_price / entry_price - 1.0) * 100.0

      trade_id += 1
      trade = {
        "trade_id": trade_id,
        "symbol": self.symbol,
        "timeframe": self.timeframe,
        "structure_type": "bi",
        "direction": "LONG",
        "entry_idx": entry_idx,
        "entry_time": entry_time,
        "entry_price": round(entry_price, 6),
        "stop_price": round(stop_price, 6),
        "exit_idx": actual_exit_idx,
        "exit_time": exit_time,
        "exit_price": round(actual_exit_price, 6),
        "exit_reason": exit_reason,
        "pnl_abs": round(pnl_abs, 6),
        "pnl_pct": round(pnl_pct, 4),
      }
      self.trades.append(trade)

      self.trade_trace.append(
        {
          "timeframe": self.timeframe,
          "bi_id": bi["bi_id"],
          "action": "CLOSE",
          "trade_id": trade_id,
          "entry_idx": entry_idx,
          "entry_time": entry_time,
          "exit_idx": actual_exit_idx,
          "exit_time": exit_time,
          "exit_reason": exit_reason,
          "pnl_pct": round(pnl_pct, 4),
          "exit_anchor_idx": exit_anchor_idx,
          "exit_bi_id": next_down_bi.get("bi_id"),
          "exit_bi_direction": next_down_bi.get("direction"),
          "exit_bi_start_index": next_down_bi.get("start_index"),
          "exit_bi_start_time": next_down_bi.get("start_time"),
          "exit_bi_end_index": next_down_bi.get("end_index"),
          "exit_bi_end_time": next_down_bi.get("end_time"),
        }
      )

      self._record_signal_event(
        event_type="TRADE_CLOSED",
        bi=bi,
        bar_index=actual_exit_idx,
        event_time=exit_time,
        price=actual_exit_price,
        stop_price=stop_price,
        planned_exit_idx=exit_idx,
        planned_exit_time=planned_exit_time,
        trigger_price_ref="close",
        reason=exit_reason,
        signal_text="交易闭环完成：本次信号对应的回测交易已完成",
        trade_id=trade_id,
        extra={
          "entry_idx": entry_idx,
          "entry_time": entry_time,
          "entry_price": round(entry_price, 6),
          "exit_idx": actual_exit_idx,
          "exit_time_actual": exit_time,
          "exit_price_actual": round(actual_exit_price, 6),
          "pnl_abs": round(pnl_abs, 6),
          "pnl_pct": round(pnl_pct, 4),
          "exit_anchor_idx": exit_anchor_idx,
          "exit_bi_id": next_down_bi.get("bi_id"),
          "exit_bi_direction": next_down_bi.get("direction"),
          "exit_bi_start_index": next_down_bi.get("start_index"),
          "exit_bi_start_time": next_down_bi.get("start_time"),
          "exit_bi_start_price": next_down_bi.get("start_price"),
          "exit_bi_end_index": next_down_bi.get("end_index"),
          "exit_bi_end_time": next_down_bi.get("end_time"),
          "exit_bi_end_price": next_down_bi.get("end_price"),
        },
      )

    return self.summary()

  def trades_df(self) -> pd.DataFrame:
    return pd.DataFrame(self.trades) if self.trades else pd.DataFrame()

  def trade_trace_df(self) -> pd.DataFrame:
    return pd.DataFrame(self.trade_trace) if self.trade_trace else pd.DataFrame()

  def signal_events_df(self) -> pd.DataFrame:
    if not self.signal_events:
      return pd.DataFrame()

    df = pd.DataFrame(self.signal_events).copy()

    if "event_time" not in df.columns:
      df["event_time"] = ""
    if "event_date" not in df.columns:
      df["event_date"] = ""
    if "bar_index" not in df.columns:
      df["bar_index"] = None

    def fill_event_time(row):
      v = row.get("event_time", "")
      if pd.notna(v) and str(v).strip():
        return str(v)

      idx = row.get("bar_index", None)
      if pd.notna(idx):
        try:
          return self._get_row_time(int(idx))
        except Exception:
          return ""
      return ""

    df["event_time"] = df.apply(fill_event_time, axis=1)

    def fill_event_date(v):
      if pd.isna(v) or str(v).strip() == "":
        return ""
      ts = pd.to_datetime(v, errors="coerce")
      if pd.isna(ts):
        return ""
      return ts.strftime("%Y/%m/%d")

    df["event_date"] = df["event_time"].apply(fill_event_date)

    return df

  def summary(self) -> Dict[str, Any]:
    tdf = self.trades_df()
    if len(tdf) == 0:
      return {
        "symbol": self.symbol,
        "timeframe": self.timeframe,
        "total_trades": 0,
        "win_rate_pct": 0.0,
        "avg_pnl_pct": 0.0,
      }
    return {
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "total_trades": int(len(tdf)),
      "win_rate_pct": round((tdf["pnl_abs"] > 0).mean() * 100.0, 4),
      "avg_pnl_pct": round(float(tdf["pnl_pct"].mean()), 4),
    }
