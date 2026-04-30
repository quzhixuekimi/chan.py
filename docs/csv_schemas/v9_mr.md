# V9 MR 策略 — CSV 输出 Schema

**策略模块**: `user_strategy_v9_mr/`
**运行入口**: `run_v9_mr.py`
**回测引擎**: `backtest_engine.py` → `MRBacktester`

---

## 文件总览

| 文件名模式 | 说明 | 列数 |
|---|---|---|
| `{symbol}_1d_ohlcv_v9_mr.csv` | 原始 K 线 + 指标 | ~23 列 |
| `{symbol}_1d_trades_v9_mr.csv` | 成交记录 | 14 列 |
| `{symbol}_1d_trade_trace_v9_mr.csv` | 逐笔操作轨迹 | ~14 列 |
| `{symbol}_1d_summary_v9_mr.csv` | 汇总统计 | 5 列 |
| `{symbol}_1d_signal_events_v9_mr.csv` | 事件流 | ~13 列 + 动态列 |
| `{symbol}_1d_signal_digest_v9_mr.csv` | 每日信号摘要 | 10 列 |

---

## 1. `{symbol}_1d_ohlcv_v9_mr.csv`

原始 OHLCV 数据经 `MRBacktester._prepare_indicators()` 处理后追加指标列：

```
time, open, high, low, close, volume, idx,
macd_line, signal_line, macd_hist,
rsi14,
ma20,
vol_ma20,
macd_cross_up, macd_cross_down,
hist_shrinking_2,
rsi_rebound_from_oversold,
rsi_fall_from_strong_zone,
volume_ok_entry, volume_breakdown, volume_normal_or_above,
recent_10d_low_prev
```

> 注意：输入 CSV 中的其他列也会被原样保留。

---

## 2. `{symbol}_1d_trades_v9_mr.csv`

每笔闭环交易一行（不含未触发 stop 的记录）：

| 列名 | 说明 |
|---|---|
| `trade_id` | 交易序号 |
| `symbol` | 股票代码 |
| `timeframe` | 时间框架（固定为 1d） |
| `direction` | 方向（LONG） |
| `entry_idx` | 入场 K 线索引 |
| `entry_time` | 入场时间 |
| `entry_price` | 入场价格（次日开盘价） |
| `exit_idx` | 出场 K 线索引 |
| `exit_time` | 出场时间 |
| `exit_price` | 出场价格 |
| `stop_price` | 止损价（最近 10 日低点） |
| `exit_reason` | 出场原因（trend_exit） |
| `pnl_abs` | 绝对盈亏 |
| `pnl_pct` | 百分比盈亏 |

---

## 3. `{symbol}_1d_trade_trace_v9_mr.csv`

逐笔操作轨迹（OPEN / STOP / EXIT），列名为各 action 涉及字段的合集：

```
symbol, timeframe, trade_id, action, signal_bar_index, entry_idx, signal_time,
entry_time, entry_price, stop_price, exit_idx, exit_time, exit_price, reason
```

> 注意：OPEN 行只有前半部分字段有值，STOP/EXIT 行只有后半部分字段有值。

---

## 4. `{symbol}_1d_summary_v9_mr.csv`

单行汇总：

```
symbol, timeframe, total_trades, win_rate_pct, avg_pnl_pct
```

---

## 5. `{symbol}_1d_signal_events_v9_mr.csv`

事件流，核心列：

```
event_seq, symbol, timeframe, event_type, event_time, event_date, bar_index,
price, stop_price, trigger_date, reason, signal_text, structure_type
```

**动态列**（部分事件行会额外追加）：

```
trade_id, entry_time, entry_price,
rsi14, macd_line, signal_line, macd_hist,
ma20, volume, vol_ma20,
pnl_abs, pnl_pct,
stop_buffer_pct, hard_stop_buffered_price, swing_stop_buffered_price
```

**事件类型**：`LONG_ENTRY_READY`、`LONG_WEAKEN_ALERT`、`LONG_EXIT_TREND`、`LONG_STOP_LOSS`

---

## 6. `{symbol}_1d_signal_digest_v9_mr.csv`

每日信号摘要（每个 symbol + date + timeframe 一行）：

```
symbol, event_date, timeframe, latest_event_type, latest_event_time,
latest_price, stop_price, reason, signal_text, event_count
```

---

## 参数说明

| 参数 | 默认值（来源冲突注意） | 说明 |
|---|---|---|
| `macd_fast` | 12 | MACD 快线周期 |
| `macd_slow` | 26 | MACD 慢线周期 |
| `macd_signal` | 9 | MACD 信号线周期 |
| `rsi_period` | 14 | RSI 周期 |
| `ma_period` | 20 | MA 周期 |
| `vol_ma_period` | 20 | 成交量 MA 周期 |
| `swing_lookback` | 10 | 摆动低点回看根数 |
| `volume_multiplier_entry` | 1.2 | 入场量能倍数 |
| `volume_multiplier_break` | 1.5 | 放量突破倍数 |
| `fixed_stop_pct` | **0.07**（engine）/ **0.03**（config） | ⚠️ 两处默认值不一致 |
| `stop_buffer_pct` | 0.005 | 止损缓冲百分比 |
| `next_day_trigger` | True | 是否有触发延迟 |

---

## 生成逻辑简述

```
数据加载（pick_latest_1d_file）
  → load_ohlcv() 读取 CSV，标准化列名
  → MRBacktester._prepare_indicators() 计算指标和信号标记
  → run() 主循环扫描逐根 K 线
    · macd_cross_up + rsi + ma20 + volume_ok_entry → LONG_ENTRY_READY
    · macd_cross_down + rsi<50 + close<ma20 + volume_normal → LONG_EXIT_TREND
    · 止损条件（目前注释中，实际使用 recent_10d_low_prev）
    · 走弱预警 LONG_WEAKEN_ALERT
  → 写入 ohlcv / trades / trade_trace / signal_events / signal_digest
```

---

*Generated from `docs/csv_schemas/v9_mr.json` — 2026-04-26*