# V8 BYMA 策略 — CSV 输出 Schema

**策略模块**: `user_strategy_v8_byma/`
**运行入口**: `run_v8_byma.py`
**回测引擎**: `backtest_engine.py` → `BymaBacktester`

---

## 文件总览

| 文件名模式 | 说明 | 列数 |
|---|---|---|
| `{symbol}_{tf}_ohlcv_v8_byma.csv` | K 线 + 蓝黄通道 + MA | ~43 列 |
| `{symbol}_{tf}_trades_v8_byma.csv` | 成交记录 | 20 列 |
| `{symbol}_{tf}_cycles_v8_byma.csv` | 多头周期记录 | 23 列 |
| `{symbol}_{tf}_trade_trace_v8_byma.csv` | 逐笔操作轨迹 | ~11 列 |
| `{symbol}_{tf}_summary_v8_byma.csv` | 汇总统计 | 21 列 |
| `{symbol}_{tf}_signal_events_v8_byma.csv` | 事件流 | ~45 列 |
| `{symbol}_{tf}_signal_digest_v8_byma.csv` | 每日信号摘要 | 18 列 |

---

## 1. `{symbol}_{tf}_ohlcv_v8_byma.csv`

K 线数据经 `shared_indicators.compute_byma_indicators()` 处理后追加蓝黄通道和状态标记：

```
dt, open, high, low, close, volume, idx,
blue_upper, blue_lower, yellow_upper, yellow_lower,
yellow_upper_prev, yellow_lower_prev, yellow_rising,
blue_over_yellow, blue_below_yellow,
in_blue_band, above_blue_upper,
ma55, ma60, ma65, ma120, ma250,
above_ma55_60_65, below_ma55_60_65,
bull_env_ready_state, bull_env_confirmed_state,
external_trend_ok,
close_prev_1, close_prev_2, blue_lower_prev_1, blue_lower_prev_2,
initial_entry_ready_state, reentry_ready_state, entry_ready_state,
weaken_state, stop_loss_state, exit_trend_state
```

> 注意：`compute_byma_indicators` 还会注入额外列，CSV 包含该函数返回的所有列。

---

## 2. `{symbol}_{tf}_trades_v8_byma.csv`

每笔闭环交易一行（BymaBacktester 维护单持仓，允许多个 cycle）：

| 列名 | 说明 |
|---|---|
| `trade_id` | 交易序号 |
| `symbol` | 股票代码 |
| `timeframe` | 时间框架 |
| `structure_type` | 结构类型（byma） |
| `direction` | 方向（LONG） |
| `entry_idx` | 入场 K 线索引 |
| `entry_time` | 入场时间 |
| `entry_price` | 入场价格（当前收盘价） |
| `entry_reason` | 入场原因（LONG_ENTRY_READY） |
| `entry_regime_id` | 所属牛市 regime ID |
| `cycle_id` | 所属 cycle ID |
| `cycle_trade_no` | 该 cycle 内的第几笔交易 |
| `stop_price` | 止损价（blue_lower） |
| `exit_idx` | 出场 K 线索引 |
| `exit_time` | 出场时间 |
| `exit_price` | 出场价格 |
| `exit_reason` | 出场原因（LONG_EXIT_TREND / LONG_STOP_LOSS / FORCED_LAST_BAR） |
| `holding_bars` | 持仓根数 |
| `pnl_abs` | 绝对盈亏 |
| `pnl_pct` | 百分比盈亏 |

---

## 3. `{symbol}_{tf}_cycles_v8_byma.csv`

每个完整周期一行（牛市 regime 开启到关闭）：

```
cycle_id, symbol, timeframe, structure_type, status,
start_idx, start_time, end_idx, end_time, end_reason,
entry_signal_count, trade_count, closed_trade_count,
win_trade_count, loss_trade_count,
sum_trade_pnl_pct, sum_trade_pnl_abs, avg_trade_pnl_pct,
max_trade_win_pct, max_trade_loss_pct,
open_trade_id, open_trade_count
```

---

## 4. `{symbol}_{tf}_trade_trace_v8_byma.csv`

逐笔操作轨迹（OPEN / CLOSE），列名合集：

```
symbol, timeframe, action, reason, trade_id, bar_index, time, price,
bull_regime_id, cycle_id, cycle_trade_no
```

> 额外 key 可能被追加；CSV 列名为合集。

---

## 5. `{symbol}_{tf}_summary_v8_byma.csv`

单行汇总（无交易时字段名相同但值为 0）：

```
symbol, timeframe, total_bars, total_trades, win_rate_pct, avg_pnl_pct, total_pnl_pct,
avg_holding_bars, max_win_pct, max_loss_pct,
total_cycles, completed_cycles, open_cycles,
cycle_win_rate_pct, avg_cycle_pnl_pct, total_cycle_pnl_pct,
entry_signals, weaken_alerts, trend_exits, stop_losses
```

---

## 6. `{symbol}_{tf}_signal_events_v8_byma.csv`

事件流，核心列：

```
event_seq, symbol, timeframe, event_type, event_time, event_date, bar_index,
price, latest_price, stop_price,
planned_exit_idx, planned_exit_time, trigger_price_ref,
reason, signal_text, trade_id, structure_type, priority,
bull_regime_id, cycle_id, cycle_trade_no, cycle_status,
cycle_trade_count, cycle_closed_trade_count, cycle_total_pnl_pct,
entry_fired_in_regime, entry_blocked_in_regime, cooldown_until_idx,
bull_confirm_bars, regime_cooldown_bars,
open, high, low, close,
blue_upper, blue_lower, yellow_upper, yellow_lower,
ma55, ma60, ma65, ma120, ma250,
yellow_rising, blue_over_yellow, blue_below_yellow,
bull_env_ready_state, bull_env_confirmed_state,
initial_entry_ready_state, reentry_ready_state, entry_ready_state,
weaken_state, exit_trend_state, stop_loss_state
```

**事件类型**：`BULL_ENV_READY`、`LONG_ENTRY_READY`、`LONG_WEAKEN_ALERT`、`LONG_EXIT_TREND`、`LONG_STOP_LOSS`

---

## 7. `{symbol}_{tf}_signal_digest_v8_byma.csv`

每日信号摘要（每个 symbol + event_date + timeframe 一行）：

```
symbol, event_date, timeframe, latest_event_type, latest_event_time,
latest_price, stop_price, reason, signal_text,
bull_regime_id, cycle_id, cycle_trade_no, cycle_status,
cycle_trade_count, cycle_closed_trade_count, cycle_total_pnl_pct,
entry_blocked_in_regime, event_count
```

---

## 参数说明

| 参数 | 默认值 | 说明 |
|---|---|---|
| `allow_reentry` | True | 周期内是否允许重复入场 |
| `close_open_positions_on_last_bar` | True | 最后 K 线是否强制平仓 |
| `bull_confirm_bars` | TF 相关（见下） | 确认牛市环境的连续根数 |
| `regime_cooldown_bars` | TF 相关（见下） | Regime 关闭后冷却根数 |

**时间框架参数映射**（`get_timeframe_params`）：

| TF | bull_confirm_bars | regime_cooldown_bars |
|---|---|---|
| 1d | 1 | 4 |
| 4h / 2h | 2 | 8 |
| 1h | 2 | 10 |

---

## 生成逻辑简述

```
数据加载（load_price_data，标准化 dt 列）
  → BymaBacktester._prepare_dataframe()
    · shared_indicators.compute_byma_indicators() 计算蓝黄通道
    · 追加状态标记（bull_env_ready/confirmed, entry_ready, weaken, exit_trend, stop_loss）
  → run() 主循环
    · bull_env_confirmed + cooldown 通过 → 开 Regime / Cycle
    · Regime 内 entry_ready + 不持仓 → OPEN（LONG_ENTRY_READY）
    · stop_loss_state + stop_loss_armed → 止损（LONG_STOP_LOSS）
    · exit_trend_state + exit_trend_armed → 趋势离场（LONG_EXIT_TREND）
    · close < blue_lower → 预警（LONG_WEAKEN_ALERT）
  → 写入 ohlcv / trades / cycles / trade_trace / signal_events / signal_digest
```

---

*Generated from `docs/csv_schemas/v8_byma.json` — 2026-04-26*