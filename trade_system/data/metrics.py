import json
from datetime import datetime
from pathlib import Path
from typing import NamedTuple

from trade_system.engine.position_tracker import PositionTracker


class Metrics(NamedTuple):
  date: str
  total_trades: int
  closed_trades: int
  winning_trades: int
  win_rate: float
  total_pnl: float
  by_symbol: dict
  by_strategy: dict


class MetricsCalculator:
  def __init__(
    self, trades_dir: Path, metrics_dir: Path, positions_dir: Path | None = None
  ):
    self.trades_dir = trades_dir
    self.metrics_dir = metrics_dir
    self.positions_dir = positions_dir
    self.metrics_dir.mkdir(parents=True, exist_ok=True)

  def calculate_today(self) -> Metrics:
    today = datetime.now().strftime("%Y%m%d")
    trades_path = self.trades_dir / f"{today}-trades.json"
    positions_path = None
    if self.positions_dir:
      positions_path = self.positions_dir / f"{today}-positions.json"

    # Load trades data
    trade_list = []
    if trades_path.exists():
      trades = json.loads(trades_path.read_text())
      trade_list = trades.get("trades", [])

    # Load positions data (more accurate for P&L and win rate)
    closed_positions = []
    if positions_path and positions_path.exists():
      positions_data = json.loads(positions_path.read_text())
      tracker = PositionTracker.__new__(PositionTracker)
      tracker.positions = {}
      for item in positions_data.get("positions", []):
        from trade_system.engine.position_tracker import Position

        pos = Position.from_dict(item)
        tracker.positions[pos.buy_order_id] = pos
      closed_positions = tracker.get_closed_positions()

    total = len(trade_list)
    closed = (
      len(closed_positions)
      if closed_positions
      else sum(1 for t in trade_list if t.get("status") == "filled")
    )
    winning = (
      sum(1 for p in closed_positions if p.pnl is not None and p.pnl > 0)
      if closed_positions
      else 0
    )
    total_pnl = (
      sum(p.pnl for p in closed_positions if p.pnl is not None)
      if closed_positions
      else 0.0
    )

    by_symbol = {}
    by_strategy = {}
    if closed_positions:
      for p in closed_positions:
        sym = p.symbol
        strat = p.strategy
        if sym not in by_symbol:
          by_symbol[sym] = {"trades": 0, "pnl": 0.0}
        by_symbol[sym]["trades"] += 1
        by_symbol[sym]["pnl"] += p.pnl or 0.0
        if strat not in by_strategy:
          by_strategy[strat] = {"trades": 0, "pnl": 0.0}
        by_strategy[strat]["trades"] += 1
    else:
      for t in trade_list:
        if t.get("status") == "filled":
          sym = t["symbol"]
          strat = t.get("reason", "unknown")
          if sym not in by_symbol:
            by_symbol[sym] = {"trades": 0, "pnl": 0.0}
          by_symbol[sym]["trades"] += 1
          if strat not in by_strategy:
            by_strategy[strat] = {"trades": 0, "pnl": 0.0}
          by_strategy[strat]["trades"] += 1

    win_rate = winning / closed if closed > 0 else 0.0
    metrics = Metrics(
      date=today,
      total_trades=total,
      closed_trades=closed,
      winning_trades=winning,
      win_rate=round(win_rate, 4),
      total_pnl=round(total_pnl, 2),
      by_symbol=by_symbol,
      by_strategy=by_strategy,
    )
    output_path = self.metrics_dir / f"{today}-metrics.json"
    output_path.write_text(json.dumps(metrics._asdict(), ensure_ascii=False, indent=2))
    return metrics
