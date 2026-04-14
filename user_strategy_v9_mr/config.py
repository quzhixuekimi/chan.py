# -*- coding: utf-8 -*-
from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class TimeframeConfig:
  name: str
  level: str
  enabled: bool = True
  start_time: Optional[str] = None
  end_time: Optional[str] = None


@dataclass
class StrategyConfig:
  symbol: str = ""
  data_dir: str = "data_cache"
  output_dir: str = "user_strategy_v9_mr/results"

  timeframes: List[TimeframeConfig] = field(
    default_factory=lambda: [
      TimeframeConfig(
        name="1d",
        level="1D",
        enabled=True,
        start_time="2000-01-01 00:00:00",
      ),
    ]
  )

  macd_fast: int = 12
  macd_slow: int = 26
  macd_signal: int = 9
  rsi_period: int = 14
  ma_period: int = 20
  vol_ma_period: int = 20
  swing_lookback: int = 10

  volume_multiplier_entry: float = 1.2
  volume_multiplier_break: float = 1.5
  fixed_stop_pct: float = 0.07

  fresh_days: int = 2
  next_day_trigger: bool = True

  def resolved_data_dir(self, repo_root: Path) -> Path:
    return repo_root / self.data_dir

  def resolved_output_dir(self, repo_root: Path) -> Path:
    return repo_root / self.output_dir
