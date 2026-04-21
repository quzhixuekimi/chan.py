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
    self.open_signals: List[Dict[str, Any]] = []

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
      "close": float(row["close"]) if "close" in row and pd.notna(row["close"]) else None,
      "idx": int(row["idx"]) if "idx" in row and pd.notna(row["idx"]) else idx,
    }

  def _get_bottom_anchor_index(self, bi: Dict[str, Any]) -> Optional[int]:
    candidates = [
      bi.get("bottom_fx_confirm_index"),
      bi.get("bottom_fractal_confirm_index"),
      bi.get("buy_anchor_index"),
      bi.get("entry_anchor_index"),
      bi.get("fx_confirm_index"),
      bi.get("fractal_confirm_index"),
      bi.get("confirm_index"),
      bi.get("bottom_fx_index"),
      bi.get("bottom_fractal_index"),
      bi.get("fx_index"),
      bi.get("fractal_index"),
      bi.get("start_index"),
    ]
    for v in candidates:
      if v is None or pd.isna(v):
        continue
      try:
        return int(v)
      except Exception:
        continue
    return None

  def _get_bottom_anchor_time(self, bi: Dict[str, Any], anchor_idx: int | None) -> str:
    candidates = [
      bi.get("bottom_fx_confirm_time"),
      bi.get("bottom_fractal_confirm_time"),
      bi.get("buy_anchor_time"),
      bi.get("entry_anchor_time"),
      bi.get("fx_confirm_time"),
      bi.get("fractal_confirm_time"),
      bi.get("confirm_time"),
      bi.get("bottom_fx_time"),
      bi.get("bottom_fractal_time"),
      bi.get("fx_time"),
      bi.get("fractal_time"),
      bi.get("start_time"),
    ]
    for v in candidates:
      if v is not None and str(v).strip():
        return str(v)
    return self._get_row_time(anchor_idx) if anchor_idx is not None else ""

  def _get_bottom_anchor_price(self, bi: Dict[str, Any]) -> Optional[float]:
    candidates = [
      bi.get("bottom_fx_price"),
      bi.get("bottom_fractal_price"),
      bi.get("buy_anchor_price"),
      bi.get("entry_anchor_price"),
      bi.get("fx_price"),
      bi.get("fractal_price"),
      bi.get("start_price"),
    ]
    for v in candidates:
      if v is None or pd.isna(v):
        continue
      try:
        return float(v)
      except Exception:
        continue
    return None

  def _find_next_confirmed_down_bi(self, current_bi_id: Any, current_end_index: int | None) -> Optional[Dict[str, Any]]:
    for next_bi in self.bi_list:
      if not next_bi.get("is_sure", True):
        continue
      if next_bi.get("direction") != "down":
        continue
      next_bi_id = next_bi.get("bi_id")
      next_start_index = next_bi.get("start_index")
      next_end_index = next_bi.get("end_index")
      if current_bi_id is not None and next_bi_id is not None and next_bi_id > current_bi_id:
        return next_bi
      if current_end_index is not None and next_start_index is not None and next_start_index > current_end_index:
        return next_bi
      if current_end_index is not None and next_end_index is not None and next_end_index > current_end_index:
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
    summary_text: str | None = None,
    trade_id: int | None = None,
    extra: Dict[str, Any] | None = None,
  ):
    resolved_event_time = event_time or self._get_row_time(bar_index) or ""
    resolved_event_date = self._get_event_date(resolved_event_time)
    resolved_planned_exit_time = planned_exit_time or self._get_row_time(planned_exit_idx) or ""
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
      "summary_text": summary_text or signal_text or "",
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

  def _record_open_signal(self, record: Dict[str, Any]):
    self.open_signals.append(record)

  def run(self):
    trade_id = 0
    for bi in self.bi_list:
      if bi.get("direction") != "up":
        self.trade_trace.append({
          "timeframe": self.timeframe,
          "bi_id": bi.get("bi_id"),
          "bi_direction": bi.get("direction"),
          "action": "SKIP",
          "reason": "bi_not_up",
        })
        continue

      bi_is_sure = bool(bi.get("is_sure", True))
      entry_anchor_idx = self._get_bottom_anchor_index(bi)
      if entry_anchor_idx is None:
        self.trade_trace.append({
          "timeframe": self.timeframe,
          "bi_id": bi.get("bi_id"),
          "action": "SKIP",
          "reason": "bottom_anchor_not_found",
        })
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=bi.get("start_index"),
          event_time=bi.get("start_time"),
          reason="bottom_anchor_not_found",
          signal_text="跳过：未找到底分型/底部结构锚点，无法按‘底分型 + 延迟K线’规则生成买入信号",
          summary_text="缺少底分型锚点，无法生成买入信号",
        )
        continue

      entry_anchor_time = self._get_bottom_anchor_time(bi, entry_anchor_idx)
      entry_anchor_price = self._get_bottom_anchor_price(bi)
      entry_idx = entry_anchor_idx + self.entry_delay_bars
      if entry_idx >= len(self.df):
        self.trade_trace.append({
          "timeframe": self.timeframe,
          "bi_id": bi.get("bi_id"),
          "action": "SKIP",
          "reason": "entry_idx_out_of_range",
        })
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=entry_idx,
          event_time=self._get_row_time(entry_idx),
          reason="entry_idx_out_of_range",
          signal_text="跳过：底分型确认后延迟买入的位置已超出K线范围",
          summary_text="底分型后的买入触发位置超出K线范围",
          extra={
            "entry_anchor_idx": entry_anchor_idx,
            "entry_anchor_time": entry_anchor_time,
            "entry_anchor_price": entry_anchor_price,
            "up_bi_is_sure": bi_is_sure,
          },
        )
        continue

      entry_row = self._safe_row(entry_idx)
      entry_time = entry_row.get("time", "")
      entry_price = entry_row.get("open")
      stop_price = float(bi["start_price"])

      buy_reason = "bottom_fractal_plus_delay_bars_open" if bi_is_sure else "bottom_fractal_plus_delay_bars_open_unsured_up_bi"
      buy_signal_text = (
        f"买入信号：以底分型/底部结构锚点为起点，延迟 {self.entry_delay_bars} 根K线后，以该K线开盘价作为买入触发"
        if bi_is_sure else
        f"买入预警信号：当前向上笔尚未确认完成，但底分型/底部结构形成后已走出 {self.entry_delay_bars} 根K线，先按该K线开盘价生成买入提示"
      )
      buy_summary_text = "买入信号成立：底分型后延迟K线触发开仓" if bi_is_sure else "买入预警成立：未确认向上笔也先发买入提示"

      self.trade_trace.append({
        "timeframe": self.timeframe,
        "bi_id": bi.get("bi_id"),
        "action": "ENTRY_SIGNAL",
        "entry_anchor_idx": entry_anchor_idx,
        "entry_anchor_time": entry_anchor_time,
        "entry_idx": entry_idx,
        "entry_time": entry_time,
        "entry_price": round(entry_price, 6) if entry_price is not None else None,
        "stop_price": round(stop_price, 6),
        "up_bi_is_sure": bi_is_sure,
      })

      self._record_signal_event(
        event_type="BUY_SIGNAL",
        bi=bi,
        bar_index=entry_idx,
        event_time=entry_time,
        price=entry_price,
        stop_price=stop_price,
        planned_exit_idx=None,
        planned_exit_time="",
        trigger_price_ref="open",
        reason=buy_reason,
        signal_text=buy_signal_text,
        summary_text=buy_summary_text,
        extra={
          "entry_anchor_idx": entry_anchor_idx,
          "entry_anchor_time": entry_anchor_time,
          "entry_anchor_price": entry_anchor_price,
          "position_status": "OPEN_PENDING_EXIT",
          "up_bi_is_sure": bi_is_sure,
        },
      )

      self._record_signal_event(
        event_type="STOP_LOSS_ARMED",
        bi=bi,
        bar_index=entry_idx,
        event_time=entry_time,
        price=entry_price,
        stop_price=stop_price,
        planned_exit_idx=None,
        planned_exit_time="",
        trigger_price_ref="bi_start_price",
        reason="bi_structure_stop_initialized",
        signal_text="止损位设定：以当前向上笔起点价格作为结构止损位",
        summary_text="结构止损已挂出",
        extra={
          "entry_anchor_idx": entry_anchor_idx,
          "entry_anchor_time": entry_anchor_time,
          "entry_anchor_price": entry_anchor_price,
          "position_status": "OPEN_PENDING_EXIT",
          "up_bi_is_sure": bi_is_sure,
        },
      )

      next_down_bi = self._find_next_confirmed_down_bi(bi.get("bi_id"), bi.get("end_index"))
      if next_down_bi is None:
        self.trade_trace.append({
          "timeframe": self.timeframe,
          "bi_id": bi.get("bi_id"),
          "action": "OPEN_UNCLOSED",
          "reason": "next_confirmed_down_bi_not_found",
          "entry_idx": entry_idx,
          "entry_time": entry_time,
          "entry_price": round(entry_price, 6) if entry_price is not None else None,
          "up_bi_is_sure": bi_is_sure,
        })
        self._record_signal_event(
          event_type="POSITION_OPEN",
          bi=bi,
          bar_index=entry_idx,
          event_time=entry_time,
          price=entry_price,
          stop_price=stop_price,
          reason="awaiting_next_confirmed_down_bi",
          signal_text="持仓已打开：买入信号已成立，但后续尚未出现确认向下笔，当前交易仍处于持仓等待中",
          summary_text="买入已记录，等待后续卖出或止损",
          extra={
            "entry_anchor_idx": entry_anchor_idx,
            "entry_anchor_time": entry_anchor_time,
            "entry_anchor_price": entry_anchor_price,
            "position_status": "OPEN_PENDING_EXIT",
            "up_bi_is_sure": bi_is_sure,
          },
        )
        self._record_open_signal({
          "symbol": self.symbol,
          "timeframe": self.timeframe,
          "structure_type": "bi",
          "direction": "LONG",
          "bi_id": bi.get("bi_id"),
          "entry_anchor_idx": entry_anchor_idx,
          "entry_anchor_time": entry_anchor_time,
          "entry_anchor_price": round(entry_anchor_price, 6) if entry_anchor_price is not None else None,
          "entry_idx": entry_idx,
          "entry_time": entry_time,
          "entry_price": round(entry_price, 6) if entry_price is not None else None,
          "stop_price": round(stop_price, 6),
          "position_status": "OPEN_PENDING_EXIT",
          "reason": "awaiting_next_confirmed_down_bi",
          "up_bi_is_sure": bi_is_sure,
        })
        continue

      exit_anchor_idx = next_down_bi["start_index"]
      exit_idx = exit_anchor_idx + self.exit_delay_bars
      if exit_idx >= len(self.df):
        self.trade_trace.append({
          "timeframe": self.timeframe,
          "bi_id": bi.get("bi_id"),
          "action": "OPEN_UNCLOSED",
          "reason": "exit_idx_out_of_range",
          "entry_idx": entry_idx,
          "entry_time": entry_time,
          "up_bi_is_sure": bi_is_sure,
        })
        self._record_signal_event(
          event_type="POSITION_OPEN",
          bi=bi,
          bar_index=entry_idx,
          event_time=entry_time,
          price=entry_price,
          stop_price=stop_price,
          planned_exit_idx=exit_idx,
          planned_exit_time=self._get_row_time(exit_idx),
          reason="exit_idx_out_of_range",
          signal_text="持仓已打开：买入信号已成立，但当前数据范围内尚未到达有效卖出触发位置",
          summary_text="买入已记录，卖出触发位置超出当前数据范围",
          extra={
            "entry_anchor_idx": entry_anchor_idx,
            "entry_anchor_time": entry_anchor_time,
            "entry_anchor_price": entry_anchor_price,
            "exit_anchor_idx": exit_anchor_idx,
            "exit_bi_id": next_down_bi.get("bi_id"),
            "position_status": "OPEN_PENDING_EXIT",
            "up_bi_is_sure": bi_is_sure,
          },
        )
        self._record_open_signal({
          "symbol": self.symbol,
          "timeframe": self.timeframe,
          "structure_type": "bi",
          "direction": "LONG",
          "bi_id": bi.get("bi_id"),
          "entry_anchor_idx": entry_anchor_idx,
          "entry_anchor_time": entry_anchor_time,
          "entry_anchor_price": round(entry_anchor_price, 6) if entry_anchor_price is not None else None,
          "entry_idx": entry_idx,
          "entry_time": entry_time,
          "entry_price": round(entry_price, 6) if entry_price is not None else None,
          "stop_price": round(stop_price, 6),
          "exit_anchor_idx": exit_anchor_idx,
          "exit_bi_id": next_down_bi.get("bi_id"),
          "position_status": "OPEN_PENDING_EXIT",
          "reason": "exit_idx_out_of_range",
          "up_bi_is_sure": bi_is_sure,
        })
        continue

      if exit_idx <= entry_idx:
        self.trade_trace.append({
          "timeframe": self.timeframe,
          "bi_id": bi.get("bi_id"),
          "action": "SKIP",
          "reason": "exit_before_entry",
          "up_bi_is_sure": bi_is_sure,
        })
        self._record_signal_event(
          event_type="BI_SKIPPED",
          bi=bi,
          bar_index=entry_idx,
          event_time=entry_time,
          price=entry_price,
          stop_price=stop_price,
          planned_exit_idx=exit_idx,
          planned_exit_time=self._get_row_time(exit_idx),
          reason="exit_before_entry",
          signal_text="跳过闭环交易：确认向下笔对应的卖出触发位置早于或等于买入位置，无法形成有效交易闭环",
          summary_text="买入已记录，但该笔无法形成有效闭环交易",
          extra={
            "entry_anchor_idx": entry_anchor_idx,
            "entry_anchor_time": entry_anchor_time,
            "entry_anchor_price": entry_anchor_price,
            "exit_anchor_idx": exit_anchor_idx,
            "exit_bi_id": next_down_bi.get("bi_id"),
            "position_status": "INVALID_EXIT_PATH",
            "up_bi_is_sure": bi_is_sure,
          },
        )
        continue

      planned_exit_row = self._safe_row(exit_idx)
      planned_exit_time = planned_exit_row.get("time", "")
      planned_exit_price = planned_exit_row.get("close")
      actual_exit_idx = exit_idx
      actual_exit_price = planned_exit_price
      exit_reason = "confirmed_down_bi_start_plus_delay_bars_close"
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
              signal_text="止损触发：后续K线最低价跌破结构止损位，按该K线收盘价退出",
              summary_text="结构止损触发，按收盘价离场",
              extra={
                "entry_anchor_idx": entry_anchor_idx,
                "entry_anchor_time": entry_anchor_time,
                "entry_anchor_price": entry_anchor_price,
                "exit_anchor_idx": exit_anchor_idx,
                "exit_bi_id": next_down_bi.get("bi_id"),
                "position_status": "CLOSED",
                "up_bi_is_sure": bi_is_sure,
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
          signal_text=f"卖出信号：后续出现确认向下笔后，再延迟 {self.exit_delay_bars} 根K线，以该K线收盘价作为卖出触发",
          summary_text="卖出信号成立：确认向下笔后延迟K线离场",
          extra={
            "entry_anchor_idx": entry_anchor_idx,
            "entry_anchor_time": entry_anchor_time,
            "entry_anchor_price": entry_anchor_price,
            "exit_anchor_idx": exit_anchor_idx,
            "exit_bi_id": next_down_bi.get("bi_id"),
            "position_status": "CLOSED",
            "up_bi_is_sure": bi_is_sure,
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
        "entry_anchor_idx": entry_anchor_idx,
        "entry_anchor_time": entry_anchor_time,
        "entry_anchor_price": round(entry_anchor_price, 6) if entry_anchor_price is not None else None,
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
        signal_text="交易闭环完成：本次买入与卖出/止损已完成结算",
        summary_text="交易已闭环完成",
        trade_id=trade_id,
        extra={
          "entry_anchor_idx": entry_anchor_idx,
          "entry_anchor_time": entry_anchor_time,
          "entry_anchor_price": entry_anchor_price,
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
          "position_status": "CLOSED",
          "up_bi_is_sure": bi_is_sure,
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
    if "summary_text" not in df.columns:
      df["summary_text"] = df.get("signal_text", "")
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
    df["summary_text"] = df["summary_text"].fillna("")
    return df

  def open_signals_df(self) -> pd.DataFrame:
    return pd.DataFrame(self.open_signals) if self.open_signals else pd.DataFrame()

  def summary(self) -> Dict[str, Any]:
    tdf = self.trades_df()
    odf = self.open_signals_df()
    if len(tdf) == 0:
      return {
        "symbol": self.symbol,
        "timeframe": self.timeframe,
        "total_trades": 0,
        "open_signals": int(len(odf)),
        "win_rate_pct": 0.0,
        "avg_pnl_pct": 0.0,
        "entry_rule": f"bottom_fractal_plus_{self.entry_delay_bars}_bars_allow_unsured_up_bi",
        "exit_rule": f"confirmed_down_bi_plus_{self.exit_delay_bars}_bars",
      }
    return {
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "total_trades": int(len(tdf)),
      "open_signals": int(len(odf)),
      "win_rate_pct": round((tdf["pnl_abs"] > 0).mean() * 100.0, 4),
      "avg_pnl_pct": round(float(tdf["pnl_pct"].mean()), 4),
      "entry_rule": f"bottom_fractal_plus_{self.entry_delay_bars}_bars_allow_unsured_up_bi",
      "exit_rule": f"confirmed_down_bi_plus_{self.exit_delay_bars}_bars",
    }
