# shared_indicators.py
from __future__ import annotations
import pandas as pd


def compute_byma_indicators(df: pd.DataFrame) -> pd.DataFrame:
  """
  统一计算蓝黄梯子 + MA55/60/65/120/250。
  输入 df 必须含有 high / low / close 列。
  返回添加了指标列的新 DataFrame（不修改原始 df）。
  """
  out = df.copy()
  out["blue_upper"] = out["high"].ewm(span=24, adjust=False).mean()
  out["blue_lower"] = out["low"].ewm(span=23, adjust=False).mean()
  out["yellow_upper"] = out["high"].ewm(span=89, adjust=False).mean()
  out["yellow_lower"] = out["low"].ewm(span=90, adjust=False).mean()
  out["ma55"] = out["close"].rolling(55, min_periods=55).mean()
  out["ma60"] = out["close"].rolling(60, min_periods=60).mean()
  out["ma65"] = out["close"].rolling(65, min_periods=65).mean()
  out["ma120"] = out["close"].rolling(120, min_periods=120).mean()
  out["ma250"] = out["close"].rolling(250, min_periods=250).mean()
  return out
