"""pivot_sr_indicator.py

High Volume Pivot Support & Resistance Zones 指标的纯 pandas/numpy 实现。

对应 requirements/tradingview_aio_latest.txt 第二部分 Pine 脚本
(High Volume Pivot Support & Resistance Zones [BigBeluga])。

设计说明（重要，写代码前跟用户讨论确认过的几个关键简化点）：

1. 这是离线批量计算，不是实时逐bar刷新。我们只需要算出"以最后一根bar为准"的
   静态快照：每个历史 box 的最终颜色状态/CVD折线/CVD数值，而不需要模拟 Pine 在
   实时行情上"逐帧重画"的动画过程。

2. 一个 box（阻力或支撑区域）的最终视觉状态，只取决于它被"冻结"那一刻
   （被下一个同侧pivot取代之前的最后一根bar，或者如果这个box还是当前最新的、
   活跃的box，就是数据集的最后一根bar）的收盘价相对box上/下沿的位置：
     - 阻力box: 冻结时 close > top  → 视为"已突破/翻转"（is_broken=True）
     - 支撑box: 冻结时 close < bottom → 视为"已跌破/翻转"（is_broken=True）
   Pine 原脚本里的 resBroken/supBroken 这两个flag只是用来给"形态标记事件"
   （突破/回踩/翻转回踩）去重用的，我们这版不做形态标记，所以不需要维护这两个flag。

3. CVD 折线的"归一化"逻辑：Pine 每根bar都会用"box生命周期内目前为止全部delta点
   的min/max"重新缩放整条折线。我们只关心冻结那一刻的最终结果，所以直接对
   box 生命周期内完整的 delta 序列，一次性用其 min/max 做归一化即可。

4. ATR(200) 使用 Wilder's RMA 平滑（Pine ta.atr 内部就是 ta.rma），实现用
   pandas ewm(alpha=1/atr_len, adjust=False) 近似（初始几十根bar跟 Pine 的
   sma-seed 方式会有细微差异，随着bar数增多迅速收敛，不影响长期数据的准确性）。

5. Pivot high/low 检测用 rolling(window=2*len+1, center=True) 做整窗口最大/最小值
   比较，允许并列（Pine 的精确 tie-break 规则未公开文档化，这里做标准定义近似）。
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field

import numpy as np
import pandas as pd

from pivot_sr_config import DEFAULT_PIVOT_SR_CONFIG


# ---------------------------------------------------------------------------
# 小工具函数
# ---------------------------------------------------------------------------
def _format_volume(vol: float) -> str:
  """等价于 Pine 的 formatVolume()。"""
  abs_vol = abs(vol)
  sign = "-" if vol < 0 else ""
  if abs_vol >= 1_000_000:
    return f"{sign}{abs_vol / 1_000_000:.1f}M"
  if abs_vol >= 1_000:
    return f"{sign}{abs_vol / 1_000:.1f}K"
  return f"{sign}{abs_vol:.0f}"


def _wilder_atr(df: pd.DataFrame, length: int) -> pd.Series:
  """近似 Pine ta.atr(length)（内部为 ta.rma(true_range, length)）。"""
  high = df["high"].astype(float)
  low = df["low"].astype(float)
  close = df["close"].astype(float)
  prev_close = close.shift(1)
  tr = pd.concat(
    [
      high - low,
      (high - prev_close).abs(),
      (low - prev_close).abs(),
    ],
    axis=1,
  ).max(axis=1)
  # 首根bar没有prev_close，tr退化为 high-low，属于正常边界情况。
  return tr.ewm(alpha=1.0 / length, adjust=False, min_periods=length).mean()


def _pivot_series(price: pd.Series, left: int, right: int, mode: str) -> pd.Series:
  """等价于 ta.pivothigh / ta.pivotlow。

  mode="high" -> 返回 pivot high 的值（非pivot位置为 NaN）
  mode="low"  -> 返回 pivot low 的值（非pivot位置为 NaN）
  """
  window = left + right + 1
  if mode == "high":
    roll_extreme = price.rolling(window=window, center=True, min_periods=window).max()
  else:
    roll_extreme = price.rolling(window=window, center=True, min_periods=window).min()
  is_pivot = (price == roll_extreme) & roll_extreme.notna()
  return price.where(is_pivot)


# ---------------------------------------------------------------------------
# 内部数据结构：一个正在构建/已冻结的 box
# ---------------------------------------------------------------------------
@dataclass
class _BoxState:
  left_idx: int
  conf_idx: int  # 该box被创建（pivot被确认）的那根bar的index
  top: float
  bottom: float
  vol_text: str
  cvd_idx: list[int] = field(default_factory=list)
  cvd_val: list[float] = field(default_factory=list)  # 累计 volDelta 序列（原始值，未归一化）


def _finalize_zone(box: _BoxState, freeze_idx: int, right_idx: int, times: pd.Series,
                    closes: np.ndarray, kind: str) -> dict:
  """把一个 box 的最终状态（冻结那一刻）转成输出dict。"""
  top, bottom = box.top, box.bottom
  cvd_val = box.cvd_val
  min_d = min(cvd_val)
  max_d = max(cvd_val)
  range_d = max_d - min_d
  range_box = top - bottom

  cvd_points = []
  for idx, raw in zip(box.cvd_idx, cvd_val):
    if range_d == 0:
      y = bottom + range_box / 2.0
    else:
      y = bottom + (raw - min_d) / range_d * range_box
    cvd_points.append(
      {
        "time": _fmt_time(times.iloc[idx]),
        "value": float(y),
        "raw_cvd": float(raw),
      }
    )

  raw_final = cvd_val[-1] if cvd_val else 0.0
  freeze_close = float(closes[freeze_idx])

  if kind == "resistance":
    is_broken = freeze_close > top
  else:
    is_broken = freeze_close < bottom

  return {
    "left_time": _fmt_time(times.iloc[box.left_idx]),
    "right_time": _fmt_time(times.iloc[right_idx]),
    "top": float(top),
    "bottom": float(bottom),
    "vol_text": box.vol_text,
    "cvd_points": cvd_points,
    "cvd_label": "CVD: " + _format_volume(raw_final),
    "is_broken": bool(is_broken),
  }


def _fmt_time(ts) -> str:
  return pd.Timestamp(ts).strftime("%Y-%m-%d %H:%M:%S")


def _run_side(
  n: int,
  pivot_idx_list: list[int],
  pivot_len: int,
  atr: pd.Series,
  open_: np.ndarray,
  close: np.ndarray,
  volume: np.ndarray,
  vol_delta: np.ndarray,
  times: pd.Series,
  kind: str,
) -> list[dict]:
  """跑一侧（阻力 或 支撑）完整的 box 生命周期模拟，返回全部历史 zone（未截断数量）。"""
  zones: list[dict] = []
  current: _BoxState | None = None

  for pivot_idx in pivot_idx_list:
    conf_idx = pivot_idx + pivot_len
    if conf_idx >= n:
      continue  # 数据不够长，这个pivot还没到确认bar，忽略

    if current is not None:
      # 新pivot出现之前，先把上一个box从"自己确认那根bar"到"被取代前最后一根bar"
      # 这段区间的逐bar CVD累计补上（等价 Pine else 分支每根bar都会执行一次）。
      # 这一步之前漏掉过（bug：只对数据集里最后一个box做了这个模拟，历史box没做，
      # 导致CVD折线在box创建后不久就断掉，跟box实际延伸的right_time对不上）。
      cum = current.cvd_val[-1] if current.cvd_val else 0.0
      for k in range(current.conf_idx + 1, conf_idx):
        cum += float(vol_delta[k])
        current.cvd_idx.append(k)
        current.cvd_val.append(cum)

      # 新pivot出现，冻结上一个box。
      # 注意：Pine 里 box.set_right(lastBox, bar_index) 是在新pivot确认bar(conf_idx)上
      # 执行的，但box的内容（颜色/CVD）实际停留在上一根bar(freeze_idx = conf_idx - 1)。
      # 为了保证输出的 right_time 跟 cvd_points 最后一个点的时间严格对齐（避免前端画出来
      # box右边界和CVD折线末端错位一根bar），这里统一用 freeze_idx 同时作为 box 的
      # right_time 和内容冻结点，而不是照抄 Pine 字面上的 conf_idx。
      freeze_idx = max(current.conf_idx, conf_idx - 1)
      zones.append(_finalize_zone(current, freeze_idx, freeze_idx, times, close, kind))

    if kind == "resistance":
      pivot_body_top = max(open_[pivot_idx], close[pivot_idx])
      atr_val = atr.iloc[pivot_idx]
      atr_val = 0.0 if (atr_val is None or (isinstance(atr_val, float) and math.isnan(atr_val))) else float(atr_val)
      top = pivot_body_top + atr_val
      bottom = pivot_body_top
      vol_text = "Vol: " + _format_volume(-volume[pivot_idx])
    else:
      pivot_body_bottom = min(open_[pivot_idx], close[pivot_idx])
      atr_val = atr.iloc[pivot_idx]
      atr_val = 0.0 if (atr_val is None or (isinstance(atr_val, float) and math.isnan(atr_val))) else float(atr_val)
      top = pivot_body_bottom
      bottom = pivot_body_bottom - atr_val
      vol_text = "Vol: " + _format_volume(volume[pivot_idx])

    box = _BoxState(
      left_idx=pivot_idx, conf_idx=conf_idx, top=top, bottom=bottom, vol_text=vol_text
    )
    # 回填 pivot bar -> confirm bar 这段（pivot_len+1 根bar）的累计delta，
    # 等价 Pine: for i = resLen to 0 by 1
    cum = 0.0
    for k in range(pivot_idx, conf_idx + 1):
      cum += float(vol_delta[k])
      box.cvd_idx.append(k)
      box.cvd_val.append(cum)

    current = box

  if current is not None:
    # 数据集结束时，最新的box仍然"活跃"：继续模拟到最后一根bar（等价 Pine 的 else 分支
    # 每根bar都会执行，直到被下一个pivot取代——但数据到这里就结束了，所以它是当前最新状态）。
    cum = current.cvd_val[-1] if current.cvd_val else 0.0
    for k in range(current.conf_idx + 1, n):
      cum += float(vol_delta[k])
      current.cvd_idx.append(k)
      current.cvd_val.append(cum)
    freeze_idx = n - 1
    zones.append(_finalize_zone(current, freeze_idx, n - 1, times, close, kind))

  return zones


# ---------------------------------------------------------------------------
# 对外主入口
# ---------------------------------------------------------------------------
def compute_pivot_sr(df: pd.DataFrame, config: dict | None = None) -> dict:
  """计算 Pivot S/R 区域。

  Args:
    df: 必须含 dt/open/high/low/close/volume 列，按时间升序排列（跟
        indicators_api.py 里 _load_db_df() 之后的 df 格式完全一致）。
    config: 可选，覆盖 DEFAULT_PIVOT_SR_CONFIG 里的部分字段。

  Returns:
    {"resistance_zones": [...], "support_zones": [...]}
    每个 zone 是一个 dict，字段跟 pivot_sr_api.py 里的 PivotZone 一一对应，
    截断为最近 max_zones_per_side 个（按 left_time 升序排列）。
  """
  cfg = {**DEFAULT_PIVOT_SR_CONFIG, **(config or {})}
  res_len = int(cfg["res_len"])
  sup_len = int(cfg["sup_len"])
  vol_avg_len = int(cfg["vol_avg_len"])
  vol_mult = float(cfg["vol_mult"])
  atr_len = int(cfg["atr_len"])
  max_zones = int(cfg["max_zones_per_side"])

  df = df.reset_index(drop=True)
  n = len(df)
  if n == 0:
    return {"resistance_zones": [], "support_zones": []}

  high = df["high"].astype(float)
  low = df["low"].astype(float)
  open_ = df["open"].astype(float).to_numpy()
  close = df["close"].astype(float).to_numpy()
  volume = df["volume"].astype(float).to_numpy()
  times = df["dt"]

  atr = _wilder_atr(df, atr_len)
  avg_vol = df["volume"].astype(float).rolling(window=vol_avg_len, min_periods=vol_avg_len).mean()
  is_high_volume = (df["volume"].astype(float) > (avg_vol * vol_mult)).fillna(False).to_numpy()

  vol_delta = np.where(close >= open_, volume, -volume)

  piv_high = _pivot_series(high, res_len, res_len, "high")
  piv_low = _pivot_series(low, sup_len, sup_len, "low")

  res_pivot_idx = [
    i for i in range(n) if not pd.isna(piv_high.iloc[i]) and is_high_volume[i]
  ]
  sup_pivot_idx = [
    i for i in range(n) if not pd.isna(piv_low.iloc[i]) and is_high_volume[i]
  ]

  all_res_zones = _run_side(
    n, res_pivot_idx, res_len, atr, open_, close, volume, vol_delta, times, "resistance"
  )
  all_sup_zones = _run_side(
    n, sup_pivot_idx, sup_len, atr, open_, close, volume, vol_delta, times, "support"
  )

  return {
    "resistance_zones": all_res_zones[-max_zones:] if max_zones > 0 else all_res_zones,
    "support_zones": all_sup_zones[-max_zones:] if max_zones > 0 else all_sup_zones,
  }
