# V6 BSPZS 策略 — CSV 输出 Schema

**策略模块**: `user_strategy_v6_bspzs/`
**运行入口**: `run_v6_bspzs.py`

---

## 文件总览

| 文件名模式 | 说明 | 列数 |
|---|---|---|
| `{symbol}_{tf}_ohlcv_v6_bspzs.csv` | 原始 K 线 | 7 列 |
| `{symbol}_{tf}_zs_v6_bspzs.csv` | 中枢（ZS）快照 | 12 列 |
| `{symbol}_{tf}_bsp_v6_bspzs.csv` | 买卖点（BSP）快照 | 9 列 |
| `{symbol}_{tf}_signal_events_v6_bspzs.csv` | 事件流 | 45 列 |
| `{symbol}_{tf}_signal_digest_v6_bspzs.csv` | 每日信号摘要 | 11 列 |

> 注意：V6 不做交易生命周期管理，只做结构检测和事件输出。

---

## 1. `{symbol}_{tf}_ohlcv_v6_bspzs.csv`

`extract_kline_data(kllist)` 从缠论 K 线列表展开：

```
idx, time, open, high, low, close, volume
```

---

## 2. `{symbol}_{tf}_zs_v6_bspzs.csv`

`extract_zs_data_from_chan_object(kllist)` 提取中枢列表并规范化列名：

```
idx, begin_bi_idx, end_bi_idx, bi_in_idx, bi_out_idx,
begin_time, end_time, low, high, peak_low, peak_high, bi_idx_list
```

---

## 3. `{symbol}_{tf}_bsp_v6_bspzs.csv`

`extract_bsp_data_from_chan_object(kllist)` 提取买卖点并规范化 types：

```
idx, bi_idx, klu_idx, time, price, is_buy, types, is_sure, types_raw
```

> `types` 列已规范化：逗号分隔的标准化 tokens，如 `"1,2,3a"`。

---

## 4. `{symbol}_{tf}_signal_events_v6_bspzs.csv`

`build_v6_signal_events()` 构造统一事件流，所有事件（ZS 和 BSP）共用同一套列：

```
eventseq, symbol, timeframe, eventtype, eventtime, eventdate, barindex,
price, stopprice, plannedexitidx, plannedexittime, triggerpriceref,
reason, signaltext, tradeid,
structuretype, biid, bidirection,
bistartindex, bistarttime, bistartprice,
biendindex, biendtime, biendprice, bibars, biissure,
entryidx, entrytime, entryprice,
exitidx, exittimeactual, exitpriceactual, pnlabs, pnlpct,
zs_idx, zs_low, zs_high, peak_low, peak_high,
bsp_type, bsp_type_raw, source_row_idx
```

**事件类型**：
- `ZS_FORMED` — 新中枢形成（zs 相关字段有值，bsp 相关字段为 None）
- `BSP1_BUY`、`BSP1_SELL` — 1 类买卖点
- `BSP2_BUY`、`BSP2_SELL` — 2 类买卖点
- `BSP3_BUY`、`BSP3_SELL` — 3 类买卖点（3a / 3b 合并输出）

> 注意：BSP 行中大部分 ZS 字段为 None；ZS 行中大部分 BSP 字段为 None；CSV 列名为所有字段的合集。

---

## 5. `{symbol}_{tf}_signal_digest_v6_bspzs.csv`

每日信号摘要：

```
symbol, eventdate, timeframe, latesteventtype, latesteventtime,
latestprice, stopprice, biid, reason, signaltext, eventcount
```

---

## 参数说明

| 参数 | 默认值 | 说明 |
|---|---|---|
| `symbols` | config 中配置（默认 TSLA ⚠️） | 要处理的股票代码列表 |
| `trigger_step` | True | 是否使用 step_load 加载 CChan |
| `freshdays` | 2（symbol 级）/ 10（market 级） | 信号新鲜度过滤天数 |

> 注意：`build_last_digest_by_symbol` 中 symbol 级别 freshdays=2，market 级别 freshdays=10。

---

## 生成逻辑简述

```
CChan 加载（load_chan_data，trigger_step=True）
  → extract_kline_data(kllist) 展开 K 线
  → extract_zs_data_from_chan_object(kllist) 提取中枢
  → extract_bsp_data_from_chan_object(kllist) 提取买卖点
    · BSP 默认过滤 is_sure == True
  → build_v6_signal_events() 构造事件流
    · ZS 行：ZS_FORMED（中枢确认时生成）
    · BSP 行：按 normalized types 输出 BUY/SELL 事件
  → deduplicate_signal_events() 去重
  → build_v6_signal_digest() 每日摘要
  → 写入 ohlcv / zs / bsp / signal_events / signal_digest
```

---

## V6 与其他策略的关键区别

| 维度 | V6 | V7 | V8 | V9 |
|---|---|---|---|---|
| **性质** | 结构检测 | 笔级交易 | 周期/Regime 交易 | 指标交易 |
| **入场/出场** | 无 | 底分型 + 延迟 | Regime + cycle | MACD + RSI + MA |
| **止损** | 无 | 结构止损（笔起点） | blue_lower | recent_10d_low_prev |
| **输出** | ZS/BSP 事件 | 笔事件 + 交易 | Regime/cycle + 交易 | 指标事件 + 交易 |
| **数据源** | CChan | CChan（笔） | 外部 CSV | 外部 CSV |

---

*Generated from `docs/csv_schemas/v6_bspzs.json` — 2026-04-26*