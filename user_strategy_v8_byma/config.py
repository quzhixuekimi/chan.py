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
  output_dir: str = "user_strategy_v8_byma/results"

  timeframes: List[TimeframeConfig] = field(
    default_factory=lambda: [
      TimeframeConfig(
        name="1d", level="1D", enabled=True, start_time="2023-01-01 00:00:00"
      ),
      TimeframeConfig(name="4h", level="4H", enabled=True),
      TimeframeConfig(name="2h", level="2H", enabled=True),
      TimeframeConfig(name="1h", level="1H", enabled=True),
    ]
  )

  fresh_days: int = 2
  allow_reentry: bool = True
  close_open_positions_on_last_bar: bool = True
  min_bars_required: int = 120

  def resolved_data_dir(self, repo_root: Path) -> Path:
    return repo_root / self.data_dir

  def resolved_output_dir(self, repo_root: Path) -> Path:
    return repo_root / self.output_dir
