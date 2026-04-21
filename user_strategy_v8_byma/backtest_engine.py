# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd


class BymaBacktester:
  def __init__(
    self,
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    allow_reentry: bool = True,
    close_open_positions_on_last_bar: bool = True,
    bull_confirm_bars: int = 2,
    regime_cooldown_bars: int = 8,
  ):
    self.symbol = symbol
    self.timeframe = timeframe
    self.df = df.reset_index(drop=True).copy()
    self.allow_reentry = allow_reentry
    self.close_open_positions_on_last_bar = close_open_positions_on_last_bar
    self.bull_confirm_bars = max(1, int(bull_confirm_bars))
    self.regime_cooldown_bars = max(0, int(regime_cooldown_bars))

    self.signal_events: List[Dict[str, Any]] = []
    self.trade_trace: List[Dict[str, Any]] = []
    self.trades: List[Dict[str, Any]] = []
    self.cycles: List[Dict[str, Any]] = []

    self._signal_event_seq = 0
    self._trade_id_seq = 0
    self._cycle_id_seq = 0
    self._prepare_dataframe()

  def _prepare_dataframe(self):
    cols = {str(c).strip().lower(): c for c in self.df.columns}
    rename_map = {}
    if "time" in cols and "dt" not in cols:
      rename_map[cols["time"]] = "dt"
    if "date" in cols and "dt" not in cols:
      rename_map[cols["date"]] = "dt"
    if rename_map:
      self.df = self.df.rename(columns=rename_map)

    self.df.columns = [str(c).strip().lower() for c in self.df.columns]
    if "dt" not in self.df.columns:
      raise ValueError("missing dt/date/time column")

    self.df["dt"] = pd.to_datetime(self.df["dt"], errors="coerce")
    self.df = self.df.dropna(subset=["dt"]).sort_values("dt").reset_index(drop=True)

    for col in ["open", "high", "low", "close"]:
      if col not in self.df.columns:
        raise ValueError(f"missing required column: {col}")
      self.df[col] = pd.to_numeric(self.df[col], errors="coerce")

    self.df = self.df.dropna(subset=["open", "high", "low", "close"]).reset_index(
      drop=True
    )
    self.df["idx"] = range(len(self.df))

    from shared_indicators import compute_byma_indicators

    self.df = compute_byma_indicators(self.df)

    self.df["yellow_upper_prev"] = self.df["yellow_upper"].shift(1)
    self.df["yellow_lower_prev"] = self.df["yellow_lower"].shift(1)
    self.df["yellow_rising"] = (
      self.df["yellow_upper"] > self.df["yellow_upper_prev"]
    ) & (self.df["yellow_lower"] > self.df["yellow_lower_prev"])

    self.df["blue_over_yellow"] = (self.df["blue_upper"] >= self.df["yellow_upper"]) & (
      self.df["blue_lower"] >= self.df["yellow_lower"]
    )
    self.df["blue_below_yellow"] = (self.df["blue_upper"] < self.df["yellow_upper"]) & (
      self.df["blue_lower"] < self.df["yellow_lower"]
    )

    self.df["in_blue_band"] = (self.df["close"] <= self.df["blue_upper"]) & (
      self.df["close"] >= self.df["blue_lower"]
    )
    self.df["above_blue_upper"] = self.df["close"] > self.df["blue_upper"]

    self.df["above_ma55_60_65"] = (
      (self.df["close"] > self.df["ma55"])
      & (self.df["close"] > self.df["ma60"])
      & (self.df["close"] > self.df["ma65"])
    )
    self.df["below_ma55_60_65"] = (
      (self.df["close"] < self.df["ma55"])
      & (self.df["close"] < self.df["ma60"])
      & (self.df["close"] < self.df["ma65"])
    )

    self.df["bull_env_ready_state"] = (
      self.df["blue_over_yellow"] & self.df["yellow_rising"]
    )

    bull_confirm = (
      self.df["bull_env_ready_state"]
      .rolling(self.bull_confirm_bars, min_periods=self.bull_confirm_bars)
      .sum()
      .eq(self.bull_confirm_bars)
    )
    self.df["bull_env_confirmed_state"] = bull_confirm.fillna(False)

    self.df["external_trend_ok"] = True

    self.df["close_prev_1"] = self.df["close"].shift(1)
    self.df["close_prev_2"] = self.df["close"].shift(2)
    self.df["blue_lower_prev_1"] = self.df["blue_lower"].shift(1)
    self.df["blue_lower_prev_2"] = self.df["blue_lower"].shift(2)

    self.df["initial_entry_ready_state"] = (
      self.df["bull_env_confirmed_state"]
      & (self.df["in_blue_band"] | self.df["above_blue_upper"])
      & (self.df["close"] > self.df["blue_lower"])
      & self.df["above_ma55_60_65"]
    )

    self.df["reentry_ready_state"] = (
      (self.df["close"] > self.df["blue_lower"])
      & (self.df["close_prev_1"] < self.df["blue_lower_prev_1"])
      & (self.df["close_prev_2"] < self.df["blue_lower_prev_2"])
    )

    self.df["entry_ready_state"] = (
      self.df["initial_entry_ready_state"] | self.df["reentry_ready_state"]
    )

    self.df["weaken_state"] = self.df["close"] < self.df["blue_lower"]
    self.df["stop_loss_state"] = self.df["weaken_state"] & self.df["below_ma55_60_65"]

    self.df["exit_trend_state"] = self.df["blue_below_yellow"] & (
      self.df["close"] < self.df["yellow_lower"]
    )

  def _row_time(self, idx: int) -> str:
    if idx < 0 or idx >= len(self.df):
      return ""
    v = self.df.loc[idx, "dt"]
    if pd.isna(v):
      return ""
    return pd.Timestamp(v).strftime("%Y-%m-%d %H:%M:%S")

  def _event_date(self, event_time: str) -> str:
    ts = pd.to_datetime(event_time, errors="coerce")
    if pd.isna(ts):
      return ""
    return ts.strftime("%Y/%m/%d")

  def _record_signal_event(
    self,
    event_type: str,
    idx: int,
    reason: str,
    signal_text: str,
    priority: int,
    trade_id: int | None = None,
    stop_price: float | None = None,
    extra: Dict[str, Any] | None = None,
  ):
    row = self.df.loc[idx]
    event_time = self._row_time(idx)
    self._signal_event_seq += 1

    event = {
      "event_seq": self._signal_event_seq,
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "event_type": event_type,
      "event_time": event_time,
      "event_date": self._event_date(event_time),
      "bar_index": int(idx),
      "price": round(float(row["close"]), 6),
      "latest_price": round(float(row["close"]), 6),
      "stop_price": round(float(stop_price), 6)
      if stop_price is not None and pd.notna(stop_price)
      else None,
      "planned_exit_idx": None,
      "planned_exit_time": "",
      "trigger_price_ref": "close",
      "reason": reason,
      "signal_text": signal_text,
      "trade_id": trade_id,
      "structure_type": "byma",
      "priority": int(priority),
      "bull_regime_id": None,
      "cycle_id": None,
      "cycle_trade_no": None,
      "cycle_status": "",
      "cycle_trade_count": None,
      "cycle_closed_trade_count": None,
      "cycle_total_pnl_pct": None,
      "entry_fired_in_regime": None,
      "entry_blocked_in_regime": False,
      "open": round(float(row["open"]), 6),
      "high": round(float(row["high"]), 6),
      "low": round(float(row["low"]), 6),
      "close": round(float(row["close"]), 6),
      "blue_upper": round(float(row["blue_upper"]), 6)
      if pd.notna(row["blue_upper"])
      else None,
      "blue_lower": round(float(row["blue_lower"]), 6)
      if pd.notna(row["blue_lower"])
      else None,
      "yellow_upper": round(float(row["yellow_upper"]), 6)
      if pd.notna(row["yellow_upper"])
      else None,
      "yellow_lower": round(float(row["yellow_lower"]), 6)
      if pd.notna(row["yellow_lower"])
      else None,
      "ma55": round(float(row["ma55"]), 6) if pd.notna(row["ma55"]) else None,
      "ma60": round(float(row["ma60"]), 6) if pd.notna(row["ma60"]) else None,
      "ma65": round(float(row["ma65"]), 6) if pd.notna(row["ma65"]) else None,
      "ma120": round(float(row["ma120"]), 6) if pd.notna(row["ma120"]) else None,
      "ma250": round(float(row["ma250"]), 6) if pd.notna(row["ma250"]) else None,
      "yellow_rising": bool(row["yellow_rising"]),
      "blue_over_yellow": bool(row["blue_over_yellow"]),
      "blue_below_yellow": bool(row["blue_below_yellow"]),
      "bull_env_ready_state": bool(row["bull_env_ready_state"]),
      "bull_env_confirmed_state": bool(row["bull_env_confirmed_state"]),
      "external_trend_ok": True,
      "initial_entry_ready_state": bool(row["initial_entry_ready_state"]),
      "reentry_ready_state": bool(row["reentry_ready_state"]),
      "entry_ready_state": bool(row["entry_ready_state"]),
      "weaken_state": bool(row["weaken_state"]),
      "exit_trend_state": bool(row["exit_trend_state"]),
      "stop_loss_state": bool(row["stop_loss_state"]),
    }
    if extra:
      event.update(extra)
    self.signal_events.append(event)

  def _record_trade_trace(
    self,
    action: str,
    idx: int,
    reason: str,
    trade_id: int | None = None,
    price: float | None = None,
    extra: Dict[str, Any] | None = None,
  ):
    item = {
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "action": action,
      "reason": reason,
      "trade_id": trade_id,
      "bar_index": int(idx),
      "time": self._row_time(idx),
      "price": round(float(price), 6)
      if price is not None and pd.notna(price)
      else None,
    }
    if extra:
      item.update(extra)
    self.trade_trace.append(item)

  def _new_cycle(self, idx: int) -> Dict[str, Any]:
    self._cycle_id_seq += 1
    return {
      "cycle_id": self._cycle_id_seq,
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "structure_type": "byma",
      "status": "ACTIVE",
      "start_idx": int(idx),
      "start_time": self._row_time(idx),
      "end_idx": None,
      "end_time": "",
      "end_reason": "",
      "entry_signal_count": 0,
      "trade_count": 0,
      "closed_trade_count": 0,
      "win_trade_count": 0,
      "loss_trade_count": 0,
      "sum_trade_pnl_pct": 0.0,
      "sum_trade_pnl_abs": 0.0,
      "avg_trade_pnl_pct": 0.0,
      "max_trade_win_pct": None,
      "max_trade_loss_pct": None,
      "open_trade_id": None,
      "open_trade_count": 0,
    }

  def _close_cycle(self, cycle: Dict[str, Any], idx: int, reason: str):
    cycle["status"] = "CLOSED"
    cycle["end_idx"] = int(idx)
    cycle["end_time"] = self._row_time(idx)
    cycle["end_reason"] = reason
    if cycle["closed_trade_count"] > 0:
      cycle["avg_trade_pnl_pct"] = round(
        float(cycle["sum_trade_pnl_pct"]) / float(cycle["closed_trade_count"]), 4
      )
    else:
      cycle["avg_trade_pnl_pct"] = 0.0

    cycle["sum_trade_pnl_pct"] = round(float(cycle["sum_trade_pnl_pct"]), 4)
    cycle["sum_trade_pnl_abs"] = round(float(cycle["sum_trade_pnl_abs"]), 6)
    if cycle["max_trade_win_pct"] is not None:
      cycle["max_trade_win_pct"] = round(float(cycle["max_trade_win_pct"]), 4)
    if cycle["max_trade_loss_pct"] is not None:
      cycle["max_trade_loss_pct"] = round(float(cycle["max_trade_loss_pct"]), 4)

    self.cycles.append(dict(cycle))

  def _attach_trade_result_to_cycle(
    self,
    cycle: Dict[str, Any] | None,
    trade: Dict[str, Any],
  ):
    if cycle is None:
      return
    pnl_pct = float(trade.get("pnl_pct") or 0.0)
    pnl_abs = float(trade.get("pnl_abs") or 0.0)
    cycle["closed_trade_count"] += 1
    cycle["open_trade_count"] = max(0, int(cycle.get("open_trade_count", 0)) - 1)
    cycle["open_trade_id"] = None
    cycle["sum_trade_pnl_pct"] += pnl_pct
    cycle["sum_trade_pnl_abs"] += pnl_abs
    if pnl_abs > 0:
      cycle["win_trade_count"] += 1
    elif pnl_abs < 0:
      cycle["loss_trade_count"] += 1

    prev_max_win = cycle.get("max_trade_win_pct")
    prev_max_loss = cycle.get("max_trade_loss_pct")
    cycle["max_trade_win_pct"] = (
      pnl_pct if prev_max_win is None else max(float(prev_max_win), pnl_pct)
    )
    cycle["max_trade_loss_pct"] = (
      pnl_pct if prev_max_loss is None else min(float(prev_max_loss), pnl_pct)
    )

  def run(self):
    in_position = False
    current_trade: Dict[str, Any] | None = None

    bull_regime_active = False
    bull_regime_id = 0
    cooldown_until_idx = -1

    weaken_alert_armed = False
    stop_loss_armed = True
    exit_trend_armed = False

    current_cycle: Dict[str, Any] | None = None

    for i in range(2, len(self.df)):
      cur = self.df.loc[i]

      cur_bull_confirmed = bool(cur["bull_env_confirmed_state"])
      cur_entry_ready = bool(cur["entry_ready_state"])
      cur_initial_entry_ready = bool(cur["initial_entry_ready_state"])
      cur_reentry_ready = bool(cur["reentry_ready_state"])
      cur_weaken = bool(cur["weaken_state"])
      cur_exit = bool(cur["exit_trend_state"])
      cur_stop = bool(cur["stop_loss_state"])

      can_open_new_regime = i > cooldown_until_idx

      if not cur_stop:
        stop_loss_armed = True

      if (not bull_regime_active) and cur_bull_confirmed and can_open_new_regime:
        bull_regime_active = True
        bull_regime_id += 1

        weaken_alert_armed = True
        exit_trend_armed = True

        current_cycle = self._new_cycle(i)

        self._record_signal_event(
          event_type="BULL_ENV_READY",
          idx=i,
          reason=f"bull_env_confirmed_{self.bull_confirm_bars}_bars",
          signal_text=f"多头环境：蓝梯子整体不弱于黄梯子，黄色梯子继续上行，且连续 {self.bull_confirm_bars} 根确认，进入完整做多周期观察区。",
          priority=5,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "cycle_id": current_cycle["cycle_id"] if current_cycle else None,
            "cycle_trade_no": 0,
            "cycle_status": current_cycle["status"] if current_cycle else "",
            "cycle_trade_count": current_cycle["trade_count"] if current_cycle else 0,
            "cycle_closed_trade_count": current_cycle["closed_trade_count"]
            if current_cycle
            else 0,
            "cycle_total_pnl_pct": current_cycle["sum_trade_pnl_pct"]
            if current_cycle
            else 0.0,
            "entry_fired_in_regime": False,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
            "bull_confirm_bars": self.bull_confirm_bars,
            "regime_cooldown_bars": self.regime_cooldown_bars,
          },
        )

      if bull_regime_active and cur_exit and exit_trend_armed:
        self._record_signal_event(
          event_type="LONG_EXIT_TREND",
          idx=i,
          reason="blue_fully_below_yellow_and_close_below_yellow_lower",
          signal_text="卖出信号：蓝色梯子完全死叉在黄色梯子下方，且收盘跌破黄色梯子下边缘，本轮完整做多周期结束。",
          priority=2,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "cycle_id": current_cycle["cycle_id"] if current_cycle else None,
            "cycle_trade_no": current_trade.get("cycle_trade_no")
            if current_trade
            else None,
            "cycle_status": current_cycle["status"] if current_cycle else "",
            "cycle_trade_count": current_cycle["trade_count"]
            if current_cycle
            else None,
            "cycle_closed_trade_count": current_cycle["closed_trade_count"]
            if current_cycle
            else None,
            "cycle_total_pnl_pct": round(float(current_cycle["sum_trade_pnl_pct"]), 4)
            if current_cycle
            else None,
            "entry_fired_in_regime": None,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
          },
        )
        exit_trend_armed = False
        bull_regime_active = False
        weaken_alert_armed = False
        cooldown_until_idx = i + self.regime_cooldown_bars

        if in_position and current_trade is not None:
          exit_price = float(cur["close"])
          current_trade["exit_idx"] = int(i)
          current_trade["exit_time"] = self._row_time(i)
          current_trade["exit_price"] = round(exit_price, 6)
          current_trade["exit_reason"] = "LONG_EXIT_TREND"
          current_trade["holding_bars"] = int(i - current_trade["entry_idx"])
          current_trade["pnl_abs"] = round(exit_price - current_trade["entry_price"], 6)
          current_trade["pnl_pct"] = round(
            (exit_price / current_trade["entry_price"] - 1.0) * 100.0, 4
          )

          self._attach_trade_result_to_cycle(current_cycle, current_trade)
          self.trades.append(current_trade)

          self._record_trade_trace(
            action="CLOSE",
            idx=i,
            reason="LONG_EXIT_TREND",
            trade_id=current_trade["trade_id"],
            price=exit_price,
            extra={
              "entry_idx": current_trade["entry_idx"],
              "entry_time": current_trade["entry_time"],
              "bull_regime_id": current_trade.get("entry_regime_id"),
              "cycle_id": current_trade.get("cycle_id"),
              "cycle_trade_no": current_trade.get("cycle_trade_no"),
            },
          )
          current_trade = None
          in_position = False

        if current_cycle is not None and current_cycle["status"] == "ACTIVE":
          self._close_cycle(current_cycle, i, "LONG_EXIT_TREND")
          current_cycle = None
        continue

      if cur_stop and stop_loss_armed and in_position:
        cycle_id = current_trade.get("cycle_id") if current_trade else None
        cycle_trade_no = current_trade.get("cycle_trade_no") if current_trade else None

        self._record_signal_event(
          event_type="LONG_STOP_LOSS",
          idx=i,
          reason="close_below_blue_and_ma55_60_65_first_armed",
          signal_text="止损信号：收盘价跌破蓝梯子下边缘，且失守 MA55/60/65，当前持仓止损，但若完整周期未结束，后续仍允许再次买入。",
          priority=1,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": current_trade.get("entry_regime_id")
            if current_trade
            else None,
            "cycle_id": cycle_id,
            "cycle_trade_no": cycle_trade_no,
            "cycle_status": current_cycle["status"] if current_cycle else "",
            "cycle_trade_count": current_cycle["trade_count"]
            if current_cycle
            else None,
            "cycle_closed_trade_count": current_cycle["closed_trade_count"]
            if current_cycle
            else None,
            "cycle_total_pnl_pct": round(float(current_cycle["sum_trade_pnl_pct"]), 4)
            if current_cycle
            else None,
            "entry_fired_in_regime": None,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
          },
        )
        stop_loss_armed = False

        if current_trade is not None:
          exit_price = float(cur["close"])
          current_trade["exit_idx"] = int(i)
          current_trade["exit_time"] = self._row_time(i)
          current_trade["exit_price"] = round(exit_price, 6)
          current_trade["exit_reason"] = "LONG_STOP_LOSS"
          current_trade["holding_bars"] = int(i - current_trade["entry_idx"])
          current_trade["pnl_abs"] = round(exit_price - current_trade["entry_price"], 6)
          current_trade["pnl_pct"] = round(
            (exit_price / current_trade["entry_price"] - 1.0) * 100.0, 4
          )

          self._attach_trade_result_to_cycle(current_cycle, current_trade)
          self.trades.append(current_trade)

          self._record_trade_trace(
            action="CLOSE",
            idx=i,
            reason="LONG_STOP_LOSS",
            trade_id=current_trade["trade_id"],
            price=exit_price,
            extra={
              "entry_idx": current_trade["entry_idx"],
              "entry_time": current_trade["entry_time"],
              "bull_regime_id": current_trade.get("entry_regime_id"),
              "cycle_id": current_trade.get("cycle_id"),
              "cycle_trade_no": current_trade.get("cycle_trade_no"),
            },
          )
          current_trade = None
          in_position = False
        continue

      if bull_regime_active and cur_weaken and weaken_alert_armed:
        self._record_signal_event(
          event_type="LONG_WEAKEN_ALERT",
          idx=i,
          reason="close_below_blue_lower_first_in_cycle",
          signal_text="预警信号：收盘价跌破蓝梯子下边缘，短线转弱；若后续重新站回蓝梯子内部，完整周期内仍允许再次买入。",
          priority=3,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "cycle_id": current_cycle["cycle_id"] if current_cycle else None,
            "cycle_trade_no": current_trade.get("cycle_trade_no")
            if current_trade
            else None,
            "cycle_status": current_cycle["status"] if current_cycle else "",
            "cycle_trade_count": current_cycle["trade_count"]
            if current_cycle
            else None,
            "cycle_closed_trade_count": current_cycle["closed_trade_count"]
            if current_cycle
            else None,
            "cycle_total_pnl_pct": round(float(current_cycle["sum_trade_pnl_pct"]), 4)
            if current_cycle
            else None,
            "entry_fired_in_regime": None,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
          },
        )
        weaken_alert_armed = False

      if bull_regime_active and (not cur_weaken):
        weaken_alert_armed = True

      allow_new_entry = bull_regime_active and cur_entry_ready and (not in_position)

      if allow_new_entry and self.allow_reentry:
        if current_cycle is None:
          current_cycle = self._new_cycle(i)

        current_cycle["entry_signal_count"] += 1
        current_cycle["trade_count"] += 1
        current_cycle["open_trade_count"] += 1

        cycle_trade_no = int(current_cycle["trade_count"])

        if cur_reentry_ready:
          entry_reason = "reenter_blue_band_after_2_closes_below_blue_lower"
          signal_text = "重新买入信号：前 2 根 K 线收盘都在蓝梯子下边缘下方，当前收盘重新站回蓝梯子内部，完整周期内允许再次买入。"
        else:
          entry_reason = "initial_entry_ready_in_active_bull_cycle"
          signal_text = f"买入信号：多头环境已连续{self.bull_confirm_bars}根确认，当前处于蓝梯子内部或上方，满足首次做多买点。"

        self._record_signal_event(
          event_type="LONG_ENTRY_READY",
          idx=i,
          reason=entry_reason,
          signal_text=signal_text,
          priority=4,
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "cycle_id": current_cycle["cycle_id"],
            "cycle_trade_no": cycle_trade_no,
            "cycle_status": current_cycle["status"],
            "cycle_trade_count": current_cycle["trade_count"],
            "cycle_closed_trade_count": current_cycle["closed_trade_count"],
            "cycle_total_pnl_pct": round(float(current_cycle["sum_trade_pnl_pct"]), 4),
            "entry_fired_in_regime": True,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
            "bull_confirm_bars": self.bull_confirm_bars,
            "regime_cooldown_bars": self.regime_cooldown_bars,
            "is_reentry": bool(cur_reentry_ready),
          },
        )

        self._trade_id_seq += 1
        entry_price = float(cur["close"])
        current_trade = {
          "trade_id": self._trade_id_seq,
          "symbol": self.symbol,
          "timeframe": self.timeframe,
          "structure_type": "byma",
          "direction": "LONG",
          "entry_idx": int(i),
          "entry_time": self._row_time(i),
          "entry_price": round(entry_price, 6),
          "entry_reason": "LONG_ENTRY_READY",
          "entry_regime_id": bull_regime_id,
          "cycle_id": current_cycle["cycle_id"],
          "cycle_trade_no": cycle_trade_no,
          "stop_price": round(float(cur["blue_lower"]), 6)
          if pd.notna(cur["blue_lower"])
          else None,
          "exit_idx": None,
          "exit_time": "",
          "exit_price": None,
          "exit_reason": "",
          "holding_bars": None,
          "pnl_abs": None,
          "pnl_pct": None,
        }
        current_cycle["open_trade_id"] = current_trade["trade_id"]
        in_position = True

        self._record_trade_trace(
          action="OPEN",
          idx=i,
          reason=entry_reason,
          trade_id=current_trade["trade_id"],
          price=entry_price,
          extra={
            "stop_price": current_trade["stop_price"],
            "bull_regime_id": bull_regime_id,
            "cycle_id": current_cycle["cycle_id"],
            "cycle_trade_no": cycle_trade_no,
            "is_reentry": bool(cur_reentry_ready),
          },
        )

    if (
      in_position
      and current_trade is not None
      and self.close_open_positions_on_last_bar
    ):
      last_idx = len(self.df) - 1
      last_close = float(self.df.loc[last_idx, "close"])
      current_trade["exit_idx"] = int(last_idx)
      current_trade["exit_time"] = self._row_time(last_idx)
      current_trade["exit_price"] = round(last_close, 6)
      current_trade["exit_reason"] = "FORCED_LAST_BAR"
      current_trade["holding_bars"] = int(last_idx - current_trade["entry_idx"])
      current_trade["pnl_abs"] = round(last_close - current_trade["entry_price"], 6)
      current_trade["pnl_pct"] = round(
        (last_close / current_trade["entry_price"] - 1.0) * 100.0, 4
      )

      self._attach_trade_result_to_cycle(current_cycle, current_trade)
      self.trades.append(current_trade)

      self._record_trade_trace(
        action="CLOSE",
        idx=last_idx,
        reason="FORCED_LAST_BAR",
        trade_id=current_trade["trade_id"],
        price=last_close,
        extra={
          "entry_idx": current_trade["entry_idx"],
          "entry_time": current_trade["entry_time"],
          "bull_regime_id": current_trade.get("entry_regime_id"),
          "cycle_id": current_trade.get("cycle_id"),
          "cycle_trade_no": current_trade.get("cycle_trade_no"),
        },
      )
      in_position = False
      current_trade = None

    if current_cycle is not None and current_cycle["status"] == "ACTIVE":
      last_idx = len(self.df) - 1
      self._close_cycle(current_cycle, last_idx, "DATA_END")

    return self.summary()

  def indicators_df(self) -> pd.DataFrame:
    return self.df.copy()

  def trades_df(self) -> pd.DataFrame:
    columns = [
      "trade_id",
      "symbol",
      "timeframe",
      "structure_type",
      "direction",
      "entry_idx",
      "entry_time",
      "entry_price",
      "entry_reason",
      "entry_regime_id",
      "cycle_id",
      "cycle_trade_no",
      "stop_price",
      "exit_idx",
      "exit_time",
      "exit_price",
      "exit_reason",
      "holding_bars",
      "pnl_abs",
      "pnl_pct",
    ]
    if not self.trades:
      return pd.DataFrame(columns=columns)
    tdf = pd.DataFrame(self.trades).copy()
    for c in columns:
      if c not in tdf.columns:
        tdf[c] = None
    return tdf[columns]

  def cycles_df(self) -> pd.DataFrame:
    columns = [
      "cycle_id",
      "symbol",
      "timeframe",
      "structure_type",
      "status",
      "start_idx",
      "start_time",
      "end_idx",
      "end_time",
      "end_reason",
      "entry_signal_count",
      "trade_count",
      "closed_trade_count",
      "win_trade_count",
      "loss_trade_count",
      "sum_trade_pnl_pct",
      "sum_trade_pnl_abs",
      "avg_trade_pnl_pct",
      "max_trade_win_pct",
      "max_trade_loss_pct",
      "open_trade_id",
      "open_trade_count",
    ]
    if not self.cycles:
      return pd.DataFrame(columns=columns)
    cdf = pd.DataFrame(self.cycles).copy()
    for c in columns:
      if c not in cdf.columns:
        cdf[c] = None
    return cdf[columns]

  def trade_trace_df(self) -> pd.DataFrame:
    if not self.trade_trace:
      return pd.DataFrame(
        columns=[
          "symbol",
          "timeframe",
          "action",
          "reason",
          "trade_id",
          "bar_index",
          "time",
          "price",
          "bull_regime_id",
          "cycle_id",
          "cycle_trade_no",
        ]
      )
    return pd.DataFrame(self.trade_trace)

  def signal_events_df(self) -> pd.DataFrame:
    if not self.signal_events:
      return pd.DataFrame()
    return pd.DataFrame(self.signal_events)

  def summary(self) -> Dict[str, Any]:
    tdf = self.trades_df()
    cdf = self.cycles_df()
    events_df = self.signal_events_df()

    if tdf.empty:
      total_cycles = int(len(cdf))
      completed_cycles = int((cdf["status"] == "CLOSED").sum()) if not cdf.empty else 0
      open_cycles = int((cdf["status"] != "CLOSED").sum()) if not cdf.empty else 0
      return {
        "symbol": self.symbol,
        "timeframe": self.timeframe,
        "total_bars": int(len(self.df)),
        "total_trades": 0,
        "win_rate_pct": 0.0,
        "avg_pnl_pct": 0.0,
        "total_pnl_pct": 0.0,
        "avg_holding_bars": 0.0,
        "max_win_pct": 0.0,
        "max_loss_pct": 0.0,
        "total_cycles": total_cycles,
        "completed_cycles": completed_cycles,
        "open_cycles": open_cycles,
        "cycle_win_rate_pct": 0.0,
        "avg_cycle_pnl_pct": 0.0,
        "total_cycle_pnl_pct": 0.0,
        "entry_signals": int((events_df["event_type"] == "LONG_ENTRY_READY").sum())
        if not events_df.empty
        else 0,
        "weaken_alerts": int((events_df["event_type"] == "LONG_WEAKEN_ALERT").sum())
        if not events_df.empty
        else 0,
        "trend_exits": int((events_df["event_type"] == "LONG_EXIT_TREND").sum())
        if not events_df.empty
        else 0,
        "stop_losses": int((events_df["event_type"] == "LONG_STOP_LOSS").sum())
        if not events_df.empty
        else 0,
      }

    closed = tdf.dropna(subset=["pnl_pct"]).copy()
    if closed.empty:
      closed = tdf.copy()

    closed_cycles = cdf.copy()
    if not closed_cycles.empty:
      closed_cycles = closed_cycles[closed_cycles["closed_trade_count"] > 0].copy()

    cycle_win_rate_pct = 0.0
    avg_cycle_pnl_pct = 0.0
    total_cycle_pnl_pct = 0.0

    if not closed_cycles.empty:
      cycle_win_rate_pct = round(
        (closed_cycles["sum_trade_pnl_abs"] > 0).mean() * 100.0, 4
      )
      avg_cycle_pnl_pct = round(float(closed_cycles["sum_trade_pnl_pct"].mean()), 4)
      total_cycle_pnl_pct = round(float(closed_cycles["sum_trade_pnl_pct"].sum()), 4)

    return {
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "total_bars": int(len(self.df)),
      "total_trades": int(len(closed)),
      "win_rate_pct": round((closed["pnl_abs"] > 0).mean() * 100.0, 4),
      "avg_pnl_pct": round(float(closed["pnl_pct"].mean()), 4),
      "total_pnl_pct": round(float(closed["pnl_pct"].sum()), 4),
      "avg_holding_bars": round(float(closed["holding_bars"].mean()), 4),
      "max_win_pct": round(float(closed["pnl_pct"].max()), 4),
      "max_loss_pct": round(float(closed["pnl_pct"].min()), 4),
      "total_cycles": int(len(cdf)),
      "completed_cycles": int((cdf["status"] == "CLOSED").sum())
      if not cdf.empty
      else 0,
      "open_cycles": int((cdf["status"] != "CLOSED").sum()) if not cdf.empty else 0,
      "cycle_win_rate_pct": cycle_win_rate_pct,
      "avg_cycle_pnl_pct": avg_cycle_pnl_pct,
      "total_cycle_pnl_pct": total_cycle_pnl_pct,
      "entry_signals": int((events_df["event_type"] == "LONG_ENTRY_READY").sum())
      if not events_df.empty
      else 0,
      "weaken_alerts": int((events_df["event_type"] == "LONG_WEAKEN_ALERT").sum())
      if not events_df.empty
      else 0,
      "trend_exits": int((events_df["event_type"] == "LONG_EXIT_TREND").sum())
      if not events_df.empty
      else 0,
      "stop_losses": int((events_df["event_type"] == "LONG_STOP_LOSS").sum())
      if not events_df.empty
      else 0,
    }
