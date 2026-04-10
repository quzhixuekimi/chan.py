# -*- coding: utf-8 -*-
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
import pandas as pd


class BiBacktester:
  def __init__(
    self,
    symbol: str,
    timeframe: str,
    df: pd.DataFrame,  # K线数据DataFrame
    bi_list: List[Dict],  # 笔数据列表 (从chan_loader提取)
    entry_delay_bars=2,
    exit_delay_bars=2,
    use_structure_stop=True,
  ):
    self.symbol = symbol
    self.timeframe = timeframe
    self.df = df
    self.bi_list = bi_list
    self.entry_delay_bars = entry_delay_bars
    self.exit_delay_bars = exit_delay_bars
    self.use_structure_stop = use_structure_stop
    self.trades: List[Any] = []
    self.trade_trace: List[Dict] = []

  def run(self):
    trade_id = 0

    # 这里的逻辑是：基于“笔”的结构做回测
    # 对应 v5/v6 的线段逻辑，现在改为：
    # 当出现一笔向上的笔 (UP Bi) 时，在笔起点后延迟 N 根入场

    for bi in self.bi_list:
      # 只处理向上的笔
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
        continue

      entry_idx = bi["start_index"] + self.entry_delay_bars
      exit_idx = bi["end_index"] + self.exit_delay_bars

      if entry_idx >= len(self.df):
        self.trade_trace.append(
          {
            "timeframe": self.timeframe,
            "bi_id": bi["bi_id"],
            "action": "SKIP",
            "reason": "entry_idx_out_of_range",
          }
        )
        continue

      if exit_idx >= len(self.df):
        self.trade_trace.append(
          {
            "timeframe": self.timeframe,
            "bi_id": bi["bi_id"],
            "action": "SKIP",
            "reason": "exit_idx_out_of_range",
          }
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
        continue

      # 入场
      entry_row = self.df.loc[entry_idx]
      entry_price = float(entry_row["open"])
      stop_price = float(bi["start_price"])  # 止损设为笔起点价格

      actual_exit_idx = exit_idx
      actual_exit_price = float(self.df.loc[exit_idx, "close"])
      exit_reason = "up_bi_end_plus_delay_bars_close"

      self.trade_trace.append(
        {
          "timeframe": self.timeframe,
          "bi_id": bi["bi_id"],
          "action": "OPEN_PLAN",
          "entry_idx": entry_idx,
          "entry_price": round(entry_price, 6),
          "stop_price": round(stop_price, 6),
          "planned_exit_idx": exit_idx,
        }
      )

      # 结构止损
      if self.use_structure_stop:
        for j in range(entry_idx + 1, exit_idx + 1):
          row = self.df.loc[j]
          if float(row["low"]) < stop_price:
            actual_exit_idx = j
            actual_exit_price = float(self.df.loc[j, "close"])
            exit_reason = "bi_structure_stop_break"
            break

      # 计算盈亏
      exit_row = self.df.loc[actual_exit_idx]
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
        "entry_time": str(entry_row["time"]),
        "entry_price": round(entry_price, 6),
        "stop_price": round(stop_price, 6),
        "exit_idx": actual_exit_idx,
        "exit_time": str(exit_row["time"]),
        "exit_price": round(actual_exit_price, 6),
        "exit_reason": exit_reason,
        "pnl_abs": round(pnl_abs, 6),
        "pnl_pct": round(pnl_pct, 4),
      }
      self.trades.append(trade)
      self.trade_trace.append(
        {"action": "CLOSE", "trade_id": trade_id, "pnl_pct": round(pnl_pct, 4)}
      )

    return self.summary()

  def trades_df(self) -> pd.DataFrame:
    return pd.DataFrame(self.trades) if self.trades else pd.DataFrame()

  def trade_trace_df(self) -> pd.DataFrame:
    return pd.DataFrame(self.trade_trace) if self.trade_trace else pd.DataFrame()

  def summary(self) -> Dict:
    tdf = self.trades_df()
    if len(tdf) == 0:
      return {"total_trades": 0, "win_rate_pct": 0.0, "avg_pnl_pct": 0.0}
    return {
      "symbol": self.symbol,
      "timeframe": self.timeframe,
      "total_trades": int(len(tdf)),
      "win_rate_pct": round((tdf["pnl_abs"] > 0).mean() * 100.0, 4),
      "avg_pnl_pct": round(float(tdf["pnl_pct"].mean()), 4),
    }
