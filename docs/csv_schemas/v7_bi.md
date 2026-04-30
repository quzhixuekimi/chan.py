# V7 BI 策略 — CSV 输出 Schema

**策略模块**: `user_strategy_v7_bi/`
**运行入口**: `run_v7_bi.py`
**回测引擎**: `backtest_engine.py` → `BiBacktester`

---

## 文件总览

| 文件名模式 | 说明 | 列数 |
|---|---|---|
| `{symbol}_{tf}_ohlcv_v7_bi.csv` | 原始 K 线 | 7 列 |
| `{symbol}_{tf}_bi_snapshot_v7_bi.csv` | 笔（BI）快照 | ~10 列（不定） |
| `{symbol}_{tf}_trades_v7_bi.csv` | 成交记录 | 19 列 |
| `{symbol}_{tf}_trade_trace_v7_bi.csv` | 逐笔操作轨迹 | ~12 列 |
| `{symbol}_{tf}_signal_events_v7_bi.csv` | 事件流 | 27 列 |
| `{symbol}_{tf}_signal_digest_v7_bi.csv` | 每日信号摘要 | 11 列 |

---

## 1. `{symbol}_{tf}_ohlcv_v7_bi.csv`

`extract_kline_data(kl_list)` 从缠论 K 线列表展开：

```
idx, time, open, high, low, close, volume
```

---

## 2. `{symbol}_{tf}_bi_snapshot_v7_bi.csv`

`pd.DataFrame(bi_list)` 将笔列表直接导出：

```
bi_id, start_index, end_index, start_time, end_time,
start_price, end_price, direction, is_sure, bars
```

> 注意：CChan 输出的 bi_list 可能包含额外字段；CSV 为所有字段的合集。

---

## 3. `{symbol}_{tf}_trades_v7_bi.csv`

每笔闭环交易一行：

| 列名 | 说明 |
|---|---|
| `trade_id` | 交易序号 |
| `symbol` | 股票代码 |
| `timeframe` | 时间框架 |
| `structure_type` | 结构类型（bi） |
| `direction` | 方向（LONG） |
| `entry_anchor_idx` | 底分型锚点 K 线索引 |
| `entry_anchor_time` | 底分型锚点时间 |
| `entry_anchor_price` | 底分型锚点价格 |
| `entry_idx` | 入场 K 线索引 = anchor + delay |
| `entry_time` | 入场时间 |
| `entry_price` | 入场价格（入场 K 线开盘价） |
| `stop_price` | 止损价（bi.start_price） |
| `exit_idx` | 出场 K 线索引（下一向下笔起点 + delay） |
| `exit_time` | 出场时间 |
| `exit_price` | 出场价格 |
| `exit_reason` | 出场原因（bi_structure_stop_break / confirmed_down_bi...） |
| `pnl_abs` | 绝对盈亏 |
| `pnl_pct` | 百分比盈亏 |

---

## 4. `{symbol}_{tf}_trade_trace_v7_bi.csv`

逐笔操作轨迹，列名合集：

```
timeframe, bi_id, bi_direction, action, reason,
entry_anchor_idx, entry_anchor_time, entry_idx, entry_time, entry_price, stop_price, up_bi_is_sure
```

> 注意：不同 action（SKIP / ENTRY_SIGNAL / OPEN_UNCLOSED）行内容不同；CSV 为合集。

---

## 5. `{symbol}_{tf}_signal_events_v7_bi.csv`

事件流，核心列：

```
event_seq, symbol, timeframe, event_type, event_time, event_date, bar_index,
price, stop_price, planned_exit_idx, planned_exit_time, trigger_price_ref,
reason, signal_text, summary_text, trade_id,
structure_type,
bi_id, bi_direction, bi_start_index, bi_start_time, bi_start_price,
bi_end_index, bi_end_time, bi_end_price, bi_bars, bi_is_sure
```

**事件类型**：`BI_SKIPPED`、`BUY_SIGNAL`、`STOP_LOSS_ARMED`、`POSITION_OPEN`、`STOP_LOSS_TRIGGERED`、`SELL_SIGNAL`、`TRADE_CLOSED`

---

## 6. `{symbol}_{tf}_signal_digest_v7_bi.csv`

每日信号摘要：

```
symbol, event_date, timeframe, latest_event_type, latest_event_time,
latest_price, stop_price, bi_id, reason, signal_text, event_count
```

---

## 参数说明

| 参数 | 默认值 | 说明 |
|---|---|---|
| `entry_delay_bars` | 2 | 底分型确认后延迟入场根数 |
| `exit_delay_bars` | 2 | 向下笔确认后延迟出场根数 |
| `use_structure_stop` | True | 是否使用结构止损（扫描入场后到出场前最低价） |

---

## 生成逻辑简述

```
CChan 加载（load_chan_data）
  → extract_kline_data(kl_list) 展开 K 线
  → extract_bi_data(kl_list) 提取笔列表
  → BiBacktester.run() 遍历 bi_list
    · 跳过非向上笔
    · 找底分型锚点（多种 fallback）
    · anchor + delay → entry_idx，stop = bi.start_price
    · 找下一确认的向下笔 → exit_anchor
    · scan 低点 < stop_price → 结构止损提前离场
    · 否则正常离场（SELL_SIGNAL）
    · 无下一向下笔 → OPEN_PENDING_EXIT
  → 写入 ohlcv / bi_snapshot / trades / trade_trace / signal_events / signal_digest
```

---

*Generated from `docs/csv_schemas/v7_bi.json` — 2026-04-26*