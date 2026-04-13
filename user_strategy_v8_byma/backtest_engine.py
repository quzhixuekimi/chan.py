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
    external_trend_ok_series: pd.Series | None = None,
  ):
    self.symbol = symbol
    self.timeframe = timeframe
    self.df = df.reset_index(drop=True).copy()
    self.allow_reentry = allow_reentry
    self.close_open_positions_on_last_bar = close_open_positions_on_last_bar
    self.bull_confirm_bars = max(1, int(bull_confirm_bars))
    self.regime_cooldown_bars = max(0, int(regime_cooldown_bars))
    self.external_trend_ok_series = external_trend_ok_series

    self.signal_events: List[Dict[str, Any]] = []
    self.trade_trace: List[Dict[str, Any]] = []
    self.trades: List[Dict[str, Any]] = []

    self._signal_event_seq = 0
    self._trade_id_seq = 0
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

    self.df["blue_upper"] = self.df["high"].ewm(span=24, adjust=False).mean()
    self.df["blue_lower"] = self.df["low"].ewm(span=23, adjust=False).mean()
    self.df["yellow_upper"] = self.df["high"].ewm(span=89, adjust=False).mean()
    self.df["yellow_lower"] = self.df["low"].ewm(span=90, adjust=False).mean()

    self.df["ma55"] = self.df["close"].rolling(55, min_periods=55).mean()
    self.df["ma60"] = self.df["close"].rolling(60, min_periods=60).mean()
    self.df["ma65"] = self.df["close"].rolling(65, min_periods=65).mean()
    self.df["ma120"] = self.df["close"].rolling(120, min_periods=120).mean()
    self.df["ma250"] = self.df["close"].rolling(250, min_periods=250).mean()

    self.df["yellow_upper_prev"] = self.df["yellow_upper"].shift(1)
    self.df["yellow_lower_prev"] = self.df["yellow_lower"].shift(1)
    self.df["yellow_rising"] = (
      self.df["yellow_upper"] > self.df["yellow_upper_prev"]
    ) & (self.df["yellow_lower"] > self.df["yellow_lower_prev"])

    self.df["blue_over_yellow"] = (self.df["blue_upper"] >= self.df["yellow_upper"]) & (
      self.df["blue_lower"] >= self.df["yellow_lower"]
    )
    self.df["blue_below_yellow"] = (
      self.df["blue_upper"] <= self.df["yellow_upper"]
    ) & (self.df["blue_lower"] <= self.df["yellow_lower"])

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

    self.df["entry_ready_state_raw"] = (
      self.df["bull_env_ready_state"]
      & (self.df["in_blue_band"] | self.df["above_blue_upper"])
      & (self.df["close"] > self.df["blue_lower"])
      & self.df["above_ma55_60_65"]
    )

    if self.external_trend_ok_series is None:
      self.df["external_trend_ok"] = True
    else:
      ext = pd.Series(self.external_trend_ok_series).reset_index(drop=True)
      if len(ext) != len(self.df):
        raise ValueError("external_trend_ok_series length mismatch")
      self.df["external_trend_ok"] = ext.fillna(False).astype(bool)

    self.df["entry_ready_state"] = (
      self.df["entry_ready_state_raw"] & self.df["external_trend_ok"]
    )

    self.df["weaken_state"] = self.df["close"] < self.df["blue_lower"]
    self.df["exit_trend_state"] = self.df["blue_below_yellow"]
    self.df["stop_loss_state"] = self.df["weaken_state"] & self.df["below_ma55_60_65"]

    bull_confirm = (
      self.df["bull_env_ready_state"]
      .rolling(self.bull_confirm_bars, min_periods=self.bull_confirm_bars)
      .sum()
      .eq(self.bull_confirm_bars)
    )
    self.df["bull_env_confirmed_state"] = bull_confirm.fillna(False)

    self.df["close_prev"] = self.df["close"].shift(1)
    self.df["entry_cross_up_state"] = (
      (~self.df["entry_ready_state"].shift(1).fillna(False))
      & self.df["entry_ready_state"]
      & (self.df["close"] > self.df["close_prev"])
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
      "external_trend_ok": bool(row["external_trend_ok"]),
      "entry_ready_state_raw": bool(row["entry_ready_state_raw"]),
      "entry_ready_state": bool(row["entry_ready_state"]),
      "entry_cross_up_state": bool(row["entry_cross_up_state"]),
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

  def run(self):
    in_position = False
    current_trade: Dict[str, Any] | None = None

    bull_regime_active = False
    bull_regime_id = 0
    entry_fired_in_regime = False
    cooldown_until_idx = -1

    weaken_alert_armed = False
    stop_loss_armed = True
    exit_trend_armed = False

    for i in range(1, len(self.df)):
      prev = self.df.loc[i - 1]
      cur = self.df.loc[i]

      prev_bull_confirmed = bool(prev["bull_env_confirmed_state"])
      cur_bull_confirmed = bool(cur["bull_env_confirmed_state"])

      cur_entry_cross_up = bool(cur["entry_cross_up_state"])
      cur_weaken = bool(cur["weaken_state"])
      cur_exit = bool(cur["exit_trend_state"])
      cur_stop = bool(cur["stop_loss_state"])

      bull_cross_up = (not prev_bull_confirmed) and cur_bull_confirmed
      bull_cross_down = prev_bull_confirmed and (not cur_bull_confirmed)

      can_open_new_regime = i > cooldown_until_idx

      if not cur_stop:
        stop_loss_armed = True

      if bull_cross_up and (not bull_regime_active) and can_open_new_regime:
        bull_regime_active = True
        bull_regime_id += 1
        entry_fired_in_regime = False

        weaken_alert_armed = True
        exit_trend_armed = True

        self._record_signal_event(
          event_type="BULL_ENV_READY",
          idx=i,
          reason=f"bull_env_confirmed_{self.bull_confirm_bars}_bars",
          signal_text=f"多头环境：蓝梯子整体不弱于黄梯子，黄色梯子继续上行，且连续 {self.bull_confirm_bars} 根确认，进入做多观察区。",
          priority=5,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "entry_fired_in_regime": entry_fired_in_regime,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
            "bull_confirm_bars": self.bull_confirm_bars,
            "regime_cooldown_bars": self.regime_cooldown_bars,
          },
        )
        continue

      if bull_cross_down and bull_regime_active:
        bull_regime_active = False
        entry_fired_in_regime = False
        weaken_alert_armed = False
        exit_trend_armed = False
        cooldown_until_idx = i + self.regime_cooldown_bars

      if cur_stop and stop_loss_armed and in_position:
        self._record_signal_event(
          event_type="LONG_STOP_LOSS",
          idx=i,
          reason="close_below_blue_and_ma55_60_65_first_armed",
          signal_text="止损信号：收盘价跌破蓝梯子下边缘，且失守 MA55/60/65，短线结构明显恶化。",
          priority=1,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": current_trade.get("entry_regime_id")
            if current_trade
            else None,
            "entry_fired_in_regime": entry_fired_in_regime,
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
            },
          )
          current_trade = None
          in_position = False
        continue

      if bull_regime_active and cur_exit and exit_trend_armed:
        self._record_signal_event(
          event_type="LONG_EXIT_TREND",
          idx=i,
          reason="blue_below_yellow_first_in_regime",
          signal_text="卖出信号：蓝梯子整体回落至黄色梯子下方，多头趋势结构失效。",
          priority=2,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "entry_fired_in_regime": entry_fired_in_regime,
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
            },
          )
          current_trade = None
          in_position = False
        continue

      if bull_regime_active and cur_weaken and weaken_alert_armed:
        self._record_signal_event(
          event_type="LONG_WEAKEN_ALERT",
          idx=i,
          reason="close_below_blue_lower_first_in_regime",
          signal_text="预警信号：收盘价首次跌破蓝梯子下边缘，短线节奏转弱，原建仓逻辑进入观察状态。",
          priority=3,
          trade_id=(current_trade or {}).get("trade_id"),
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "entry_fired_in_regime": entry_fired_in_regime,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
          },
        )
        weaken_alert_armed = False

      entry_first_in_regime = (
        bull_regime_active and cur_entry_cross_up and (not entry_fired_in_regime)
      )

      if entry_first_in_regime:
        self._record_signal_event(
          event_type="LONG_ENTRY_READY",
          idx=i,
          reason="entry_cross_up_first_in_confirmed_bull_regime",
          signal_text=f"买入信号：多头环境已连续{self.bull_confirm_bars}根确认，且入场条件从不满足切换为满足，当前收盘强于前一根，并通过高周期方向过滤，作为本轮 bull regime 的首次买点。",
          priority=4,
          stop_price=cur["blue_lower"],
          extra={
            "bull_regime_id": bull_regime_id,
            "entry_fired_in_regime": True,
            "entry_blocked_in_regime": False,
            "cooldown_until_idx": cooldown_until_idx,
            "bull_confirm_bars": self.bull_confirm_bars,
            "regime_cooldown_bars": self.regime_cooldown_bars,
          },
        )

        entry_fired_in_regime = True

        if (not in_position) and self.allow_reentry:
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
          in_position = True

          self._record_trade_trace(
            action="OPEN",
            idx=i,
            reason="LONG_ENTRY_READY",
            trade_id=current_trade["trade_id"],
            price=entry_price,
            extra={
              "stop_price": current_trade["stop_price"],
              "bull_regime_id": bull_regime_id,
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
        },
      )

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
        ]
      )
    return pd.DataFrame(self.trade_trace)

  def signal_events_df(self) -> pd.DataFrame:
    if not self.signal_events:
      return pd.DataFrame()
    return pd.DataFrame(self.signal_events)

  def summary(self) -> Dict[str, Any]:
    tdf = self.trades_df()
    events_df = self.signal_events_df()

    if tdf.empty:
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
