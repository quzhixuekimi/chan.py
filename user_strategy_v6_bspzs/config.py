from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional


@dataclass
class TimeframeConfig:
  name: str
  level: str
  enabled: bool = True
  starttime: Optional[str] = None
  endtime: Optional[str] = None


@dataclass
class StrategyConfig:
  symbol: str = "TSLA"
  datadir: str = "data_cache"
  outputdir: str = "user_strategy_v6_bspzs/results"

  timeframes: List[TimeframeConfig] = field(
    default_factory=lambda: [
      TimeframeConfig(
        name="1d", level="1D", enabled=True, starttime="2023-01-01 00:00:00"
      ),
      TimeframeConfig(name="4h", level="4H", enabled=True),
      TimeframeConfig(name="2h", level="2H", enabled=True),
      TimeframeConfig(name="1h", level="1H", enabled=True),
    ]
  )

  triggerstep: bool = True

  chanconfig: dict = field(
    default_factory=lambda: {
      "bi_algo": "normal",
      "trigger_step": True,
      "skip_step": 0,
      "divergence_rate": float("inf"),
      "bsp2_follow_1": True,
      "bsp3_follow_1": True,
      "strict_bsp3": False,
      "bsp3_peak": False,
      "bsp2s_follow_2": False,
      "max_bs2_rate": 0.9999,
      "macd_algo": "peak",
      "bs1_peak": False,
      "bs_type": "1,2,3a,3b",
      "bsp1_only_multibi_zs": False,
      "min_zs_cnt": 0,
    }
  )

  # snake_case 版本
  def resolved_data_dir(self, repo_root: Path) -> Path:
    return repo_root / self.datadir

  def resolved_output_dir(self, repo_root: Path) -> Path:
    return repo_root / self.outputdir

  # 兼容旧命名
  def resolved_datadir(self, repo_root: Path) -> Path:
    return self.resolved_data_dir(repo_root)

  def resolved_outputdir(self, repo_root: Path) -> Path:
    return self.resolved_output_dir(repo_root)
