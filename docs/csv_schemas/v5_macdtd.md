# V5 MACDTD 策略 — CSV 输出 Schema

**策略模块**: `user_strategy_v5_macdtd/`
**运行入口**: `run_v5_macdtd.py`
**回测引擎**: `backtest_engine.py` → `BacktestEngine`

---

## 文件总览

| 文件名模式 | 说明 | 列数 |
|---|---|---|
| `{symbol}_{tf}_ohlcv_v5_macdtd.csv` | 原始K线（策略处理后） | 7+ 列 |
| `{symbol}_{tf}_trades_v5_macdtd.csv` | 成交记录 | 18 列 |
| `{symbol}_{tf}_signal_events_v5_macdtd.csv` | 事件流 | ~19 列 |
| `{symbol}_{tf}_signal_digest_v5_macdtd.csv` | 每日信号摘要（单TF） | 10 列 |
| `{symbol}_signal_digest_last_per_symbol_v5_macdtd.csv` | 多TF汇总（symbol级） | ~27 列 |
| `market_all_summary_v5_macdtd.csv` | 市场汇总（symbol×TF） | 8 列 |
| `market_signal_digest_last_per_symbol_v5_macdtd.csv` | 市场信号摘要 | ~31 列 |

> 注意：V5 支持多时间框架(1d/4h/2h/1h)并行处理，这是与其他策略的主要区别。

---

## 1. `{symbol}_{tf}_ohlcv_v5_macdtd.csv`

K线数据经 `BacktestEngine.run()` 中 `strategy.prepare_indicators()` 处理后追加指标列：

```
time, open, high, low, close, volume, idx,
[可选列: macd_line, macd_signal, macd_hist, macd_fast, atr, rsi, td_setup_9]
```

> 注意：`prepare_indicators()` 调用 `indicators.py` 中的 `add_macd()`, `add_fast_macd()`, `add_atr()`, `add_rsi()`, `compute_td9()`。

---

## 2. `{symbol}_{tf}_trades_v5_macdtd.csv`

每笔闭环交易一行：

| 列名 | 说明 |
|---|---|
| `trade_id` | 交易序号 |
| `symbol` | 股票代码 |
| `timeframe` | 时间框架 |
| `structure_type` | 结构类型（macd_td9） |
| `direction` | 方向（LONG/SHORT） |
| `entry_anchor_idx` | 入场锚点K线索引 |
| `entry_anchor_time` | 入场锚点时间 |
| `entry_anchor_price` | 入场锚点价格 |
| `entry_idx` | 实际入场K线索引 |
| `entry_time` | 实际入场时间 |
| `entry_price` | 实际入场价格（含滑点） |
| `stop_price` | 止损价（基于 ATR × 1.5） |
| `exit_idx` | 出场K线索引 |
| `exit_time` | 出场时间 |
| `exit_price` | 出场价格 |
| `exit_reason` | 出场原因（signal_close/tp/...） |
| `pnl_abs` | 绝对盈亏（USD） |
| `pnl_pct` | 百分比盈亏（%） |

---

## 3. `{symbol}_{tf}_signal_events_v5_macdtd.csv`

事件流，核心列：

```
event_seq, symbol, timeframe, event_type, event_time, event_date, bar_index,
price, stop_price, planned_exit_idx, planned_exit_time, trigger_price_ref,
reason, signal_text, summary_text, trade_id,
structure_type, bi_id, bi_direction
```

**事件类型**：
- `BUY_SIGNAL` — MACD背驰/动量入场信号
- `ADD_POSITION` — 加仓（初始加仓/分时加仓）
- `TAKE_PROFIT` — 止盈（小级别TD9触发）
- `SELL_SIGNAL` — 平仓（止损/TD9反向）
- `STOP_UPDATE` — 移动止损更新

---

## 4. `{symbol}_{tf}_signal_digest_v5_macdtd.csv`

每日信号摘要（单时间框架）：

```
symbol, event_date, timeframe, latest_event_type, latest_event_time,
latest_price, stop_price, reason, signal_text, event_count
```

---

## 5. `{symbol}_signal_digest_last_per_symbol_v5_macdtd.csv`

多时间框架汇总（symbol级，最后事件）：

```
symbol, reference_date, signal_date, fresh_days,
1d_event_type, 1d_signal_text, 1d_event_time, 1d_latest_price, 1d_stop_price,
4h_event_type, 4h_signal_text, 4h_event_time, 4h_latest_price, 4h_stop_price,
2h_event_type, 2h_signal_text, 2h_event_time, 2h_latest_price, 2h_stop_price,
1h_event_type, 1h_signal_text, 1h_event_time, 1h_latest_price, 1h_stop_price,
has_signal, summary_text
```

---

## 6. `market_all_summary_v5_macdtd.csv`

市场汇总（symbol × timeframe 统计）：

```
symbol, timeframe, total_trades, open_signals, win_rate_pct,
avg_pnl_pct, entry_rule, exit_rule
```

---

## 7. `market_signal_digest_last_per_symbol_v5_macdtd.csv`

市场信号摘要（所有symbol最后事件，包含JSON格式）：

```
symbol, reference_date, signal_date, fresh_days,
1d_event_type, 1d_signal_text, ..., 1d_stop_price,
4h_..., 2h_..., 1h_...,
has_signal, summary_text, summary_json
```

---

## 参数说明

| 参数 | 默认值 | 说明 |
|---|---|---|
| `risk_per_trade` | 0.02 | 单笔风险比例（2%） |
| `min_divergence_strength` | 0.25 | 最小背驰强度阈值 |
| `enable_buy_filter` | True | 启用RSI买入过滤 |
| `buy_rsi_threshold` | 40 | 买入RSI阈值 |
| `initial_add_size` | 0.3 | 初始加仓比例（30%） |
| `trailing_stop_atr` | 2.0 | ATR移动止损倍数 |
| `trailing_stop_pct` | 0.05 | 百分比移动止损（5%） |
| `tp_ratios` | 1min:25%, 3min:20%, 5min:25% | 分时止盈比例 |
| `tp_thresholds` | [1%, 3%, 5%] | 分阶段止盈阈值 |

---

## 生成逻辑简述

```
数据加载（DataLoader）
  → BacktestEngine.run()
    · strategy.prepare_indicators() 计算指标
      - add_macd(fast=12, slow=26, signal=9)
      - add_fast_macd(fast=8, slow=17, signal=6)
      - add_atr(period=14)
      - add_rsi(period=14)
      - compute_td9(period=9)
    · 遍历逐根K线
      · compute_divergence_strength() 检测背驰
      · generate_signals() 生成信号
      · 处理开仓/加仓/平仓/止盈/止损
    · _record_event() 记录事件
  → trades_df() 构造成交记录
  → signal_events_df() 构造事件流
  → build_signal_digest() 每日摘要
  → build_symbol_digest() 多TF汇总
  → build_market_summary() 市场统计
  → build_market_digest() 市场信号摘要
  → 写入 ohlcv / trades / signal_events / signal_digest
```

---

## V5 与其他策略的关键区别

| 维度 | V5 | V6 | V7 | V8 | V9 |
|---|---|---|---|---|---|
| **性质** | MACD背驰+TD9指标交易 | 结构检测 | 笔级交易 | 周期/Regime交易 | 指标交易 |
| **入场** | MACD背驰/动量 + RSI过滤 | 无 | 底分型+延迟 | Regime+cycle | MACD+RSI+MA |
| **出场** | TD9反向/小级别止盈/移动止损 | 无 | 结构止损 | blue_lower | recent_10d_low |
| **加仓** | 15m TD9 + 分时止盈 | 无 | 无 | 周期内可重复 | 无 |
| **止损** | ATR-based + trailing stop | 无 | 笔起点 | blue_lower | 10日低点 |
| **数据源** | 外部CSV | CChan | CChan | 外部CSV | 外部CSV |
| **多TF** | 1d/4h/2h/1h并行 | 单TF | 单TF | 单TF | 单TF(1d) |

---

## 核心策略逻辑

### 入场条件
1. **背驰买入**：价格低点 vs MACD低点背驰 + strength >= 0.25 + macd_hist > 0 + RSI >= 40
2. **动量买入**（fallback）：macd_hist > 0 且持续上升 + RSI >= 40

### 出场条件
1. **止损**：价格触及 ATR-based stop_loss
2. **TD9反向**：td_setup_9 = -1 (多头) 或 = 1 (空头)
3. **分时止盈**：1min/3min/5min TD9反向触发部分平仓
4. **移动止损**：基于 ATR × 2.0 或 5% 移动

### 加仓逻辑
1. **初始加仓**：15m TD9 = 1 时加仓 30%
2. **分时加仓**：满足 should_scale_in 条件时

---

*Generated from `docs/csv_schemas/v5_macdtd.json` — 2026-04-29*