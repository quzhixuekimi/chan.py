# -*- coding: utf-8 -*-
from __future__ import annotations

from typing import List, Dict, Any, Optional
import pandas as pd
import numpy as np


class MRBacktester:
  def __init__(
    self,
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,
    macd_fast: int = 12,
    macd_slow: int = 26,
    macd_signal: int = 9,
    rsi_period: int = 14,
    ma_period: int = 20,
    vol_ma_period: int = 20,
    swing_lookback: int = 10,
    volume_multiplier_entry: float = 1.2,
    volume_multiplier_break: float = 1.5,
    fixed_stop_pct: float = 0.07,
    stop_buffer_pct: float = 0.005,
    next_day_trigger: bool = True,
  ):
    self.symbol = symbol
    self.timeframe = timeframe
    self.df = df.reset_index(drop=True).copy()

    self.macd_fast = macd_fast
    self.macd_slow = macd_slow
    self.macd_signal = macd_signal
    self.rsi_period = rsi_period
    self.ma_period = ma_period
    self.vol_ma_period = vol_ma_period
    self.swing_lookback = swing_lookback
    self.volume_multiplier_entry = volume_multiplier_entry
    self.volume_multiplier_break = volume_multiplier_break
    self.fixed_stop_pct = fixed_stop_pct
    self.stop_buffer_pct = stop_buffer_pct
    self.next_day_trigger = next_day_trigger

    self.signal_events: List[Dict[str, Any]] = []
    self.trade_trace: List[Dict[str, Any]] = []
    self.trades: List[Dict[str, Any]] = []

    self._signal_event_seq = 0
    self._time_col = self._get_time_col()

    self._prepare_indicators()

  def _get_time_col(self) -> str:
    candidates = ["time", "timestamp", "datetime", "dt", "date"]
    for c in candidates:
      if c in self.df.columns:
        return c
    return ""

  def _get_row_time(self, idx: Optional[int]) -> str:
    if idx is None or idx < 0 or idx >= len(self.df) or not self._time_col:
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

  def _format_float(self, v):
    if v is None or pd.isna(v):
      return None
    return round(float(v), 6)

  def _prepare_indicators(self):
    df = self.df

    for col in ["open", "high", "low", "close", "volume"]:
      if col in df.columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    close = df["close"]
    volume = (
      df["volume"] if "volume" in df.columns else pd.Series(index=df.index, dtype=float)
    )

    ema_fast = close.ewm(span=self.macd_fast, adjust=False).mean()
    ema_slow = close.ewm(span=self.macd_slow, adjust=False).mean()
    df["macd_line"] = ema_fast - ema_slow
    df["signal_line"] = df["macd_line"].ewm(span=self.macd_signal, adjust=False).mean()
    df["macd_hist"] = df["macd_line"] - df["signal_line"]

    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = -delta.clip(upper=0.0)

    avg_gain = gain.ewm(
      alpha=1 / self.rsi_period, adjust=False, min_periods=self.rsi_period
    ).mean()
    avg_loss = loss.ewm(
      alpha=1 / self.rsi_period, adjust=False, min_periods=self.rsi_period
    ).mean()
    rs = avg_gain / avg_loss.replace(0, np.nan)
    df["rsi14"] = 100 - (100 / (1 + rs))
    df["rsi14"] = df["rsi14"].bfill()

    df["ma20"] = close.rolling(self.ma_period, min_periods=self.ma_period).mean()
    df["vol_ma20"] = volume.rolling(
      self.vol_ma_period, min_periods=self.vol_ma_period
    ).mean()

    df["macd_cross_up"] = (df["macd_line"] > df["signal_line"]) & (
      df["macd_line"].shift(1) <= df["signal_line"].shift(1)
    )
    df["macd_cross_down"] = (df["macd_line"] < df["signal_line"]) & (
      df["macd_line"].shift(1) >= df["signal_line"].shift(1)
    )

    df["hist_shrinking_2"] = (df["macd_hist"] < df["macd_hist"].shift(1)) & (
      df["macd_hist"].shift(1) < df["macd_hist"].shift(2)
    )

    df["rsi_rebound_from_oversold"] = (df["rsi14"].shift(1) <= 30) & (df["rsi14"] >= 40)

    df["rsi_fall_from_strong_zone"] = (
      (df["rsi14"].shift(1) >= 70)
      & (df["rsi14"] < df["rsi14"].shift(1))
      & (df["rsi14"] >= 50)
    )

    df["volume_ok_entry"] = df["volume"] > (
      df["vol_ma20"] * self.volume_multiplier_entry
    )
    df["volume_breakdown"] = df["volume"] > (
      df["vol_ma20"] * self.volume_multiplier_break
    )
    df["volume_normal_or_above"] = df["volume"] >= df["vol_ma20"]

    df["recent_10d_low_prev"] = (
      df["low"]
      .shift(1)
      .rolling(self.swing_lookback, min_periods=self.swing_lookback)
      .min()
    )

    self.df = df

  def _record_signal_event(
    self,
    event_type: str,
    bar_index: int,
    event_time: str | None = None,
    price: float | None = None,
    stop_price: float | None = None,
    trigger_date: str | None = None,
    reason: str | None = None,
    signal_text: str | None = None,
    extra: Dict[str, Any] | None = None,
  ):
    resolved_event_time = event_time or self._get_row_time(bar_index) or ""
    resolved_event_date = self._get_event_date(resolved_event_time)

    self._signal_event_seq += 1
    event = {
      "event_seq": self._signal_event_seq,
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "event_type": event_type,
      "event_time": resolved_event_time,
      "event_date": resolved_event_date,
      "bar_index": bar_index,
      "price": self._format_float(price),
      "stop_price": self._format_float(stop_price),
      "trigger_date": trigger_date or "",
      "reason": reason or "",
      "signal_text": signal_text or "",
      "structure_type": "mr",
    }
    if extra:
      event.update(extra)
    self.signal_events.append(event)

  def run(self):
    in_position = False
    entry_idx = None
    entry_price = None
    stop_price = None
    trade_id = 0
    weaken_alert_sent = False

    for i in range(len(self.df)):
      row = self.df.iloc[i]

      if i < max(self.macd_slow, self.rsi_period, self.ma_period, self.vol_ma_period):
        continue

      event_time = self._get_row_time(i)
      trigger_idx = i + 1 if self.next_day_trigger else i
      trigger_date = (
        self._get_row_time(trigger_idx) if trigger_idx < len(self.df) else ""
      )

      close = row.get("close")
      ma20 = row.get("ma20")
      rsi14 = row.get("rsi14")
      recent_10d_low_prev = row.get("recent_10d_low_prev")
      macd_hist = row.get("macd_hist")
      macd_line = row.get("macd_line")
      signal_line = row.get("signal_line")

      if pd.isna(close) or pd.isna(ma20) or pd.isna(rsi14):
        continue

      entry_cond = bool(
        row.get("macd_cross_up", False)
        and ((rsi14 > 50) or row.get("rsi_rebound_from_oversold", False))
        and (close > ma20)
        and row.get("volume_ok_entry", False)
      )

      weaken_cond = bool(
        in_position
        and (
          row.get("rsi_fall_from_strong_zone", False)
          or (
            row.get("hist_shrinking_2", False) and pd.notna(macd_hist) and macd_hist > 0
          )
          or (
            (close < ma20)
            and pd.notna(stop_price)
            and (close > stop_price * (1.0 - self.stop_buffer_pct))
            and (not row.get("volume_breakdown", False))
          )
        )
      )

      exit_cond = bool(
        in_position
        and row.get("macd_cross_down", False)
        and (rsi14 < 50)
        and (close < ma20)
        and row.get("volume_normal_or_above", False)
      )

      hard_stop_pct_price = None
      hard_stop_buffered_price = None
      swing_stop_buffered_price = None

      if in_position and entry_price is not None:
        hard_stop_pct_price = entry_price * (1.0 - self.fixed_stop_pct)
        hard_stop_buffered_price = hard_stop_pct_price * (1.0 - self.stop_buffer_pct)

      if pd.notna(recent_10d_low_prev):
        swing_stop_buffered_price = float(recent_10d_low_prev) * (
          1.0 - self.stop_buffer_pct
        )

      stop_cond = False
      stop_reason = ""
      stop_ref_price = None

      if in_position:
        cond_fixed = (
          hard_stop_buffered_price is not None and close <= hard_stop_buffered_price
        )

        cond_swing_break = (
          swing_stop_buffered_price is not None
          and close <= swing_stop_buffered_price
          and row.get("volume_breakdown", False)
        )

        if cond_fixed:
          stop_cond = True
          stop_reason = "fixed_stop_7pct_with_buffer"
          stop_ref_price = hard_stop_pct_price
        elif cond_swing_break:
          stop_cond = True
          stop_reason = "break_recent_10d_low_with_volume_and_buffer"
          stop_ref_price = recent_10d_low_prev

      if not in_position and entry_cond:
        if trigger_idx >= len(self.df):
          continue

        next_open = (
          self.df.iloc[trigger_idx]["open"] if "open" in self.df.columns else close
        )

        swing_stop = recent_10d_low_prev if pd.notna(recent_10d_low_prev) else None
        pct_stop = next_open * (1.0 - self.fixed_stop_pct)

        if swing_stop is not None and pd.notna(swing_stop):
          use_stop = min(pct_stop, float(swing_stop))
        else:
          use_stop = pct_stop

        trade_id += 1
        in_position = True
        entry_idx = trigger_idx
        entry_price = float(next_open)
        stop_price = float(use_stop)
        weaken_alert_sent = False

        self.trade_trace.append(
          {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "trade_id": trade_id,
            "action": "OPEN",
            "signal_bar_index": i,
            "entry_idx": entry_idx,
            "signal_time": event_time,
            "entry_time": self._get_row_time(entry_idx),
            "entry_price": self._format_float(entry_price),
            "stop_price": self._format_float(stop_price),
          }
        )

        self._record_signal_event(
          event_type="LONG_ENTRY_READY",
          bar_index=i,
          event_time=event_time,
          price=close,
          stop_price=stop_price,
          trigger_date=trigger_date,
          reason="macd_cross_up_rsi_volume_ma20",
          signal_text="多头准备信号：MACD金叉，RSI14 > 50 或超卖后回升，收盘站上MA20，且成交量高于20日均量1.2倍；次日触发",
          extra={
            "trade_id": trade_id,
            "entry_time": self._get_row_time(entry_idx),
            "entry_price": self._format_float(entry_price),
            "rsi14": self._format_float(rsi14),
            "macd_line": self._format_float(macd_line),
            "signal_line": self._format_float(signal_line),
            "macd_hist": self._format_float(macd_hist),
            "ma20": self._format_float(ma20),
            "volume": self._format_float(row.get("volume")),
            "vol_ma20": self._format_float(row.get("vol_ma20")),
          },
        )
        continue

      if in_position and stop_cond:
        exit_idx = trigger_idx if trigger_idx < len(self.df) else i
        exit_price = (
          self.df.iloc[exit_idx]["open"] if trigger_idx < len(self.df) else close
        )
        pnl_abs = float(exit_price) - float(entry_price)
        pnl_pct = (float(exit_price) / float(entry_price) - 1.0) * 100.0

        self.trades.append(
          {
            "trade_id": trade_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "direction": "LONG",
            "entry_idx": entry_idx,
            "entry_time": self._get_row_time(entry_idx),
            "entry_price": self._format_float(entry_price),
            "exit_idx": exit_idx,
            "exit_time": self._get_row_time(exit_idx),
            "exit_price": self._format_float(exit_price),
            "stop_price": self._format_float(stop_price),
            "exit_reason": stop_reason,
            "pnl_abs": self._format_float(pnl_abs),
            "pnl_pct": round(float(pnl_pct), 4),
          }
        )

        self.trade_trace.append(
          {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "trade_id": trade_id,
            "action": "STOP",
            "signal_bar_index": i,
            "exit_idx": exit_idx,
            "signal_time": event_time,
            "exit_time": self._get_row_time(exit_idx),
            "exit_price": self._format_float(exit_price),
            "reason": stop_reason,
          }
        )

        self._record_signal_event(
          event_type="LONG_STOP_LOSS",
          bar_index=i,
          event_time=event_time,
          price=close,
          stop_price=stop_ref_price or stop_price,
          trigger_date=trigger_date,
          reason=stop_reason,
          signal_text="风控退出：收盘有效跌破固定止损位，或有效跌破近10日低点且放量确认；次日触发",
          extra={
            "trade_id": trade_id,
            "entry_time": self._get_row_time(entry_idx),
            "entry_price": self._format_float(entry_price),
            "exit_time": self._get_row_time(exit_idx),
            "exit_price": self._format_float(exit_price),
            "pnl_abs": self._format_float(pnl_abs),
            "pnl_pct": round(float(pnl_pct), 4),
            "rsi14": self._format_float(rsi14),
            "macd_line": self._format_float(macd_line),
            "signal_line": self._format_float(signal_line),
            "macd_hist": self._format_float(macd_hist),
            "stop_buffer_pct": self._format_float(self.stop_buffer_pct),
            "hard_stop_buffered_price": self._format_float(hard_stop_buffered_price),
            "swing_stop_buffered_price": self._format_float(swing_stop_buffered_price),
          },
        )

        in_position = False
        entry_idx = None
        entry_price = None
        stop_price = None
        weaken_alert_sent = False
        continue

      if in_position and exit_cond:
        exit_idx = trigger_idx if trigger_idx < len(self.df) else i
        exit_price = (
          self.df.iloc[exit_idx]["open"] if trigger_idx < len(self.df) else close
        )
        pnl_abs = float(exit_price) - float(entry_price)
        pnl_pct = (float(exit_price) / float(entry_price) - 1.0) * 100.0

        self.trades.append(
          {
            "trade_id": trade_id,
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "direction": "LONG",
            "entry_idx": entry_idx,
            "entry_time": self._get_row_time(entry_idx),
            "entry_price": self._format_float(entry_price),
            "exit_idx": exit_idx,
            "exit_time": self._get_row_time(exit_idx),
            "exit_price": self._format_float(exit_price),
            "stop_price": self._format_float(stop_price),
            "exit_reason": "trend_exit",
            "pnl_abs": self._format_float(pnl_abs),
            "pnl_pct": round(float(pnl_pct), 4),
          }
        )

        self.trade_trace.append(
          {
            "symbol": self.symbol,
            "timeframe": self.timeframe,
            "trade_id": trade_id,
            "action": "EXIT",
            "signal_bar_index": i,
            "exit_idx": exit_idx,
            "signal_time": event_time,
            "exit_time": self._get_row_time(exit_idx),
            "exit_price": self._format_float(exit_price),
            "reason": "trend_exit",
          }
        )

        self._record_signal_event(
          event_type="LONG_EXIT_TREND",
          bar_index=i,
          event_time=event_time,
          price=close,
          stop_price=stop_price,
          trigger_date=trigger_date,
          reason="macd_cross_down_rsi_below_50_close_below_ma20",
          signal_text="趋势离场：MACD死叉，RSI14 < 50，收盘跌破MA20，且量能不弱；次日触发",
          extra={
            "trade_id": trade_id,
            "entry_time": self._get_row_time(entry_idx),
            "entry_price": self._format_float(entry_price),
            "exit_time": self._get_row_time(exit_idx),
            "exit_price": self._format_float(exit_price),
            "pnl_abs": self._format_float(pnl_abs),
            "pnl_pct": round(float(pnl_pct), 4),
            "rsi14": self._format_float(rsi14),
            "macd_line": self._format_float(macd_line),
            "signal_line": self._format_float(signal_line),
            "macd_hist": self._format_float(macd_hist),
          },
        )

        in_position = False
        entry_idx = None
        entry_price = None
        stop_price = None
        weaken_alert_sent = False
        continue

      if in_position and weaken_cond and (not weaken_alert_sent):
        self._record_signal_event(
          event_type="LONG_WEAKEN_ALERT",
          bar_index=i,
          event_time=event_time,
          price=close,
          stop_price=stop_price,
          trigger_date=trigger_date,
          reason="rsi_fall_or_hist_shrink_or_close_below_ma20_but_not_broken",
          signal_text="转弱预警：RSI从强区回落，或MACD柱连续缩短，或价格跌回MA20下方但未形成明确破坏；仅提示观察",
          extra={
            "trade_id": trade_id,
            "entry_time": self._get_row_time(entry_idx)
            if entry_idx is not None
            else "",
            "entry_price": self._format_float(entry_price),
            "rsi14": self._format_float(rsi14),
            "macd_line": self._format_float(macd_line),
            "signal_line": self._format_float(signal_line),
            "macd_hist": self._format_float(macd_hist),
            "ma20": self._format_float(ma20),
          },
        )
        weaken_alert_sent = True

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
