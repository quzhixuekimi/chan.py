# 📊 四个回测策略的交易信号分析总结（中文）

## 概览

| 策略 | v6_bspzs | v7_bi | v8_byma | v9_mr |
|------|----------|-------|---------|-------|
| **类型** | 信号 | 交易 | 交易 | 交易 |
| **核心理论** | 缠论1、2、3类买卖点 | 底分形 + 向下笔 | BYMA均线梯子 | 多重共振 |
| **开仓** | ❌ 无 | ✅ 有 | ✅ 有 | ✅ 有 |
| **平仓** | ❌ 无 | ✅ 有 | ✅ 有 | ✅ 有 |
| **止损** | ❌ 无 | ✅ 有 | ✅ 有 | ⚠️ 未启用 |

---

## 1️⃣ V6_BSPZS - 缠论信号策略（纯信号，无交易）

### 📋 文件位置
- `/user_strategy_v6_bspzs/event_engine.py` (603 行)

### 🎯 核心功能
**v6 只提供缠论信号，不执行实际交易**

### 信号类型（无开仓、平仓、止损）
1. **ZS_FORMED** - 中枢形成信号
   - 触发条件：新的中枢结构被形成
   - 用途：观察市场结构

2. **BSP1_BUY / BSP1_SELL** - 1类买卖点
   - 缠论定义：背驰度最强的买卖点
   - 位置：通常在中枢边缘
   - 特性：理论最正确，但延迟最大

3. **BSP2_BUY / BSP2_SELL** - 2类买卖点
   - 缠论定义：中枢内部的回撤买卖点
   - 位置：中枢内部回撤至50%
   - 特性：出现频率高，但回撤风险大

4. **BSP3_BUY / BSP3_SELL** - 3类买卖点
   - 缠论定义：线段级别买卖点
   - 位置：新线段产生后立即出现
   - 特性：出现最早，但确认度最低

### 信号过滤规则
```python
# 新鲜度过滤 (run_v6_bspzs.py)
fresh_days = 2  # 只显示 2 天内的信号
age_days = (reference_date - event_date).days
is_fresh = age_days <= fresh_days
if is_fresh:
    显示信号
else:
    隐藏信号
```

### 📊 输出特性
- **无交易记录** - v6 不生成 `*_trades_*.csv` 文件
- **只有信号汇总** - 生成 `*_signal_digest_*.csv`
- **用途** - 作为参考指标，支持其他策略

---

## 2️⃣ V7_BI - 底分形确认交易策略（胜率最高）

### 📋 文件位置
- `/user_strategy_v7_bi/backtest_engine.py` (679 行)

### 🎯 核心交易逻辑

#### **开仓条件（BUY_SIGNAL）**
```
✅ 触发条件 (第 213-316 行):

1. 遍历所有向上笔 (up_bi)，过滤条件：
   - bi.direction == "up"
   - bi.is_sure == True（可选，也接受未确认笔）

2. 识别底分型锚点：
   - 优先级：bottom_fx_confirm_index 
            → bottom_fractal_confirm_index 
            → buy_anchor_index 
            → ... (多个备选字段)

3. 计算开仓位置：
   entry_idx = entry_anchor_idx + entry_delay_bars (默认 = 2)
   
   📝 说明：底分型确认后，延迟 2 根 K 线，在第 2 根 K 线的开盘价开仓

4. 开仓价格：
   entry_price = df.loc[entry_idx, "open"]  # 延迟 K 线的开盘价

5. 止损位：
   stop_price = float(bi["start_price"])  # 当前向上笔的起点价格（底部）
```

#### **平仓条件（SELL_SIGNAL）**
```
✅ 触发条件 (第 340-543 行):

1. 等待出现确认的向下笔：
   - 需要找到 bi_id > current_bi_id 的向下笔
   - 向下笔必须 is_sure == True
   - 向下笔 start_index > current_bi_end_index

2. 如果找到向下笔：
   exit_anchor_idx = next_down_bi["start_index"]
   exit_idx = exit_anchor_idx + exit_delay_bars (默认 = 2)
   
   📝 说明：向下笔确认后，再延迟 2 根 K 线，在该 K 线收盘价平仓

3. 平仓价格：
   - 正常平仓：planned_exit_price = df.loc[exit_idx, "close"]
   - 止损平仓：actual_exit_price = df.loc[stop_idx, "close"]

4. 如果没找到向下笔：
   - 交易保持开仓状态，等待更多数据
```

#### **止损条件（STOP_LOSS_TRIGGERED）**
```
✅ 触发条件 (第 484-518 行):

1. 结构止损：use_structure_stop == True（默认启用）

2. 检查范围：从 entry_idx+1 到 exit_idx 的所有 K 线

3. 触发条件：
   if row.low < stop_price:
       # 任何 K 线的最低价跌破止损位
       actual_exit_idx = j
       actual_exit_price = row.close  # 该 K 线收盘价
       exit_reason = "bi_structure_stop_break"

📝 说明：只要最低价跌破笔起点，就按该 K 线收盘价止损出场
         这是结构化止损（不会被突破后立即反弹触发虚假止损）
```

### 📊 关键特性
- **胜率最高** - 90.38%（见统计）
- **交易最多** - 2,743 笔
- **延迟小** - 仅 2 根 K 线延迟
- **止损可靠** - 基于缠论底部结构

---

## 3️⃣ V8_BYMA - 均线梯子循环交易策略（胜率最低）

### 📋 文件位置
- `/user_strategy_v8_byma/backtest_engine.py` (892 行)

### 🎯 核心交易逻辑

#### **开仓条件（LONG_ENTRY_READY）**
```
✅ 触发条件 (第 581-650 行):

前提条件：
  bull_regime_active = True  # 多头环境已激活
  cur_entry_ready = True     # 当前进入条件满足
  not in_position            # 当前未持仓

entry_ready_state 的定义 (第 130-132 行):
  = initial_entry_ready_state OR reentry_ready_state

初次进入 (initial_entry_ready_state, 第 117-122 行):
  ✅ bull_env_confirmed_state      # 多头环境连续确认 N 根 K 线
  AND (in_blue_band OR above_blue_upper)  # 价格在蓝梯子内或上方
  AND (close > blue_lower)         # 价格高于蓝梯子下边
  AND above_ma55_60_65             # 价格高于 MA55/60/65 均线

重新进入 (reentry_ready_state, 第 124-128 行):
  ✅ close > blue_lower            # 当前收盘回到蓝梯子上方
  AND close_prev_1 < blue_lower_prev_1  # 前 1 根收盘在蓝梯子下方
  AND close_prev_2 < blue_lower_prev_2  # 前 2 根收盘在蓝梯子下方

📝 说明：
  - 多头环境检测需连续 N 根 K 线确认 (bull_confirm_bars = 2)
  - 允许在同一多头周期内多次买卖 (allow_reentry = True)
  - 支持反复进出，充分利用振荡

开仓价格 (第 625 行):
  entry_price = float(cur["close"])  # 当前 K 线收盘价

止损位 (第 639 行):
  stop_price = float(cur["blue_lower"])  # 蓝梯子下边缘
```

#### **平仓条件（LONG_EXIT_TREND）**
```
✅ 触发条件 (第 406-476 行):

检测条件：
  bull_regime_active = True
  cur_exit = True
  exit_trend_armed = True

exit_trend_state 的定义 (第 137-139 行):
  = blue_below_yellow AND (close < yellow_lower)

📝 说明：
  1. 蓝梯子完全死叉黄梯子（下穿）
  2. 收盘跌破黄梯子下边缘
  3. 这是多头周期的结束信号

平仓价格 (第 442 行):
  exit_price = float(cur["close"])  # 当前 K 线收盘价

平仓后处理 (第 439 行):
  cooldown_until_idx = i + regime_cooldown_bars
  # 冷却期内不允许立即开启新周期 (regime_cooldown_bars = 8)
```

#### **止损条件（LONG_STOP_LOSS）**
```
✅ 触发条件 (第 478-544 行):

检测条件：
  cur_stop = True
  stop_loss_armed = True
  in_position = True

stop_loss_state 的定义 (第 134-135 行):
  = weaken_state AND below_ma55_60_65
  = (close < blue_lower) AND (close < ma55 AND close < ma60 AND close < ma65)

📝 说明：
  1. 价格跌破蓝梯子下边缘（第一道防线）
  2. 同时跌破 MA55/60/65（第二道防线，确认下降趋势）
  3. 需两个条件同时满足（避免虚假突破）

止损价格 (第 514 行):
  exit_price = float(cur["close"])  # 当前 K 线收盘价

重要特性 (第 366-367, 511 行):
  - stop_loss_armed 用触发后变为 False
  - 下一根 K 线如果 not cur_stop，则重新 arm
  - 这样避免连续多次触发
  - 多头周期内即使止损，后续仍可再次买入 (reentry)
```

#### **预警信号（LONG_WEAKEN_ALERT）**
```
⚠️ 预警触发 (第 546-576 行):

检测条件：
  bull_regime_active = True
  cur_weaken = True
  weaken_alert_armed = True

weaken_state 的定义 (第 134 行):
  = close < blue_lower  # 价格跌破蓝梯子下边

📝 说明：
  - 这只是短线转弱的预警
  - 不立即平仓，而是提示观察
  - 如果后续重新回到蓝梯子内部，仍可继续持仓并再次买卖
  - 这是 v8 高频交易的关键：充分利用梯子内的振荡
```

### 📊 关键特性
- **胜率最低** - 31.63%（见统计）
- **交易次数** - 1,932 笔
- **单笔收益最大** - 17.59%
- **周期管理** - 多头环境内支持多次进出
- **冷却期** - 离场后需要冷却 8 根 K 线才能开启新周期

---

## 4️⃣ V9_MR - 多重共振策略（最高效）

### 📋 文件位置
- `/user_strategy_v9_mr/backtest_engine.py` (563 行)

### 🎯 核心交易逻辑

#### **开仓条件（LONG_ENTRY_READY）**
```
✅ 触发条件 (第 221-226 行):

entry_cond = (
  row.macd_cross_up                    # MACD 金叉（快线上穿信号线）
  AND ((rsi14 > 50)                    # RSI 高于 50
       OR row.rsi_rebound_from_oversold)  # 或从超卖 ≤30 反弹至 ≥40
  AND (close > ma20)                   # 收盘价高于 20 日均线
  AND row.volume_ok_entry              # 成交量 > 20 日均量 × 1.2 倍
)

📝 说明：多重条件共振确认买入信号
  1. MACD 金叉 - 动能由负转正
  2. RSI 确认 - 在强区或超卖反弹
  3. 价格确认 - 站上短期均线
  4. 量能确认 - 放量认可

开仓执行 (第 289-313 行):
  trigger_idx = i + 1 if next_day_trigger else i
  next_open = df.iloc[trigger_idx]["open"]  # 次日开盘价（延迟触发）
  
  📝 说明：信号出现在第 i 根 K 线，下一根 K 线 (i+1) 的开盘价执行开仓

止损位计算 (第 297-312 行):
  swing_stop = recent_10d_low_prev  # 最近 10 日低点（前一根 K 线的值）
  use_stop = swing_stop if swing_stop is not None else None
  
  # 注意：本来还有一个固定止损 (fixed_stop_pct = 0.07)
  # 但在第 301-306 行被注释掉了，所以当前只用摆动止损
```

#### **平仓条件（LONG_EXIT_TREND）**
```
✅ 触发条件 (第 244-250 行):

exit_cond = (
  in_position
  AND row.macd_cross_down             # MACD 死叉（快线下穿信号线）
  AND (rsi14 < 50)                    # RSI 跌破 50
  AND (close < ma20)                  # 收盘价跌破 20 日均线
  AND row.volume_normal_or_above       # 成交量 ≥ 20 日均量
)

📝 说明：反向多重条件确认离场
  1. MACD 死叉 - 动能由正转负
  2. RSI 回落 - 跌破中线
  3. 价格下破 - 跌破短期均线
  4. 量能确认 - 量能不弱

平仓执行 (第 430-455 行):
  exit_idx = trigger_idx if trigger_idx < len(self.df) else i
  exit_price = df.iloc[exit_idx]["open"]  # 次日开盘价执行离场
  pnl_pct = (exit_price / entry_price - 1.0) * 100.0
```

#### **止损条件（当前未启用）**
```
⚠️ 止损代码 (第 265-287 行):

代码被注释掉（第 269-287 行）：

# if in_position:
#   hard_stop_pct_price = entry_price * (1.0 - fixed_stop_pct)  # 7% 固定止损
#   
#   cond_fixed = (hard_stop_buffered_price is not None 
#                 and close <= hard_stop_buffered_price)
#   cond_swing_break = (swing_stop_buffered_price is not None
#                       and close <= swing_stop_buffered_price
#                       and volume_breakdown)
#   
#   if cond_fixed:
#       stop_cond = True  # 止损触发
#   elif cond_swing_break:
#       stop_cond = True  # 或摆动止损触发

📝 说明：
  - 固定止损（7%）已被禁用
  - 摆动止损（10 日低点 + 放量）也被禁用
  - 当前依赖 exit_cond 作为唯一离场条件
  - 如果没有 MACD 死叉，会一直持仓直到数据结束
```

#### **预警信号（LONG_WEAKEN_ALERT）**
```
⚠️ 预警触发 (第 503-526 行):

weaken_cond = (
  in_position
  AND (
    row.rsi_fall_from_strong_zone     # RSI 从 ≥70 回落至 ≥50
    OR (hist_shrinking_2 AND macd_hist > 0)  # MACD 柱连续缩短但仍为正
    OR (close < ma20                   # 价格跌破 MA20
        AND close > stop_price * (1 - stop_buffer_pct)  # 但未跌破止损位
        AND not volume_breakdown)      # 且没有放量确认
  )
)

📝 说明：只是转弱预警，不执行平仓
  - 提示投资者关注风险
  - 等待正式的 exit_cond 才会离场
```

### 📊 关键特性
- **交易最少** - 仅 180 笔（选择性最强）
- **胜率适中** - 52.22%
- **单笔收益最大** - 41.94%
- **延迟触发** - 信号次日执行（避免假突破）
- **止损未启用** - 完全依赖趋势反转平仓

---

## 对比总结表

| 特性 | v6 | v7_bi | v8_byma | v9_mr |
|------|-----|--------|---------|-------|
| **信号类型** | 中枢/1/2/3 类 | 底分形 | 多头环境 | 多重共振 |
| **开仓触发** | ❌ | 底分形 + 2K | 多头环境初期 | MACD 金叉 + RSI |
| **平仓触发** | ❌ | 向下笔 + 2K | 死叉黄梯子 | MACD 死叉 + RSI |
| **止损方式** | ❌ | 笔起点 | 蓝梯子 + 均线 | 未启用 |
| **交易频率** | ❌ | 高 (2743) | 中 (1932) | 低 (180) |
| **延迟** | N/A | 2K线 | 0 | 1 天 |
| **胜率** | N/A | 90.38% | 31.63% | 52.22% |
| **单笔盈利** | N/A | 12.37% | 17.59% | 41.94% |
| **盈亏比** | N/A | 5.78 | 5.53 | 3.83 |

---

## 🎯 使用建议

### V6（仅信号参考）
- 用于学习缠论结构
- 作为其他策略的参考指标
- 不能单独用于实盘交易

### V7_BI（推荐 - 最稳定）
- 适合风险厌恶型投资者
- 胜率极高（90%+）
- 推荐作为主策略
- 适合中期持仓

### V8_BYMA（需谨慎 - 高风险）
- 低胜率（31%）但单笔高收益
- 依赖少数大赚弥补多数小亏
- 需严格风险管理
- 适合有经验的交易者

### V9_MR（推荐 - 最高效）
- 交易最少但单笔收益最大
- 适合追求高质量信号
- 样本量较小，需验证
- 可作为补充策略

