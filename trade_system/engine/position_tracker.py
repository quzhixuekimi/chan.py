"""Position tracker - links buy/sell orders, calculates P&L, tracks win rate."""

from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import Optional
import json


@dataclass
class Position:
  """Represents a complete or open position (buy + optional sell)."""

  symbol: str
  strategy: str
  period: str
  buy_order_id: str
  buy_price: float
  buy_time: str
  quantity: int
  queue_id: str = ""  # original signal queue id

  # Sell fields
  sell_order_id: Optional[str] = None
  sell_price: Optional[float] = None
  sell_time: Optional[str] = None
  sell_reason: Optional[str] = None  # "target", "stop", "manual"

  @property
  def is_closed(self) -> bool:
    return self.sell_order_id is not None

  @property
  def pnl(self) -> Optional[float]:
    if not self.is_closed or self.sell_price is None:
      return None
    return round((self.sell_price - self.buy_price) * self.quantity, 2)

  @property
  def pnl_pct(self) -> Optional[float]:
    if not self.is_closed or self.sell_price is None or self.buy_price == 0:
      return None
    return round((self.sell_price - self.buy_price) / self.buy_price * 100, 2)

  @property
  def holding_seconds(self) -> Optional[float]:
    if not self.is_closed or self.sell_time is None:
      return None
    try:
      buy_dt = datetime.fromisoformat(self.buy_time)
      sell_dt = datetime.fromisoformat(self.sell_time)
      return (sell_dt - buy_dt).total_seconds()
    except (ValueError, TypeError):
      return None

  def to_dict(self) -> dict:
    d = asdict(self)
    return d

  @classmethod
  def from_dict(cls, data: dict) -> "Position":
    return cls(
      symbol=data["symbol"],
      strategy=data["strategy"],
      period=data["period"],
      buy_order_id=data["buy_order_id"],
      buy_price=data["buy_price"],
      buy_time=data["buy_time"],
      quantity=data["quantity"],
      queue_id=data.get("queue_id", ""),
      sell_order_id=data.get("sell_order_id"),
      sell_price=data.get("sell_price"),
      sell_time=data.get("sell_time"),
      sell_reason=data.get("sell_reason"),
    )


class PositionTracker:
  """Tracks all positions, persists to disk, links buy/sell orders."""

  def __init__(self, positions_dir: Path):
    self.positions_dir = Path(positions_dir)
    self.positions_dir.mkdir(parents=True, exist_ok=True)
    self.positions: dict[str, Position] = {}  # buy_order_id -> Position
    self._load()

  def _positions_file(self) -> Path:
    today = datetime.now().strftime("%Y%m%d")
    return self.positions_dir / f"{today}-positions.json"

  def _load(self):
    """Load existing positions from today's file if exists."""
    pfile = self._positions_file()
    if pfile.exists():
      try:
        data = json.loads(pfile.read_text(encoding="utf-8"))
        for item in data.get("positions", []):
          pos = Position.from_dict(item)
          self.positions[pos.buy_order_id] = pos
      except Exception:
        pass

  def _save(self):
    """Persist positions to today's file."""
    pfile = self._positions_file()
    data = {
      "updated_at": datetime.now().isoformat(),
      "positions": [p.to_dict() for p in self.positions.values()],
    }
    pfile.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

  def on_buy_filled(
    self,
    order_request,
    order_result,
    queue_id: str = "",
  ) -> Position:
    """Record a new position after buy order is filled."""
    pos = Position(
      symbol=order_request.symbol,
      strategy=getattr(order_request, "strategy", "unknown"),
      period=getattr(order_request, "period", "unknown"),
      buy_order_id=order_result.order_id,
      buy_price=order_result.filled_price,
      buy_time=datetime.now().isoformat(),
      quantity=order_result.filled_qty,
      queue_id=queue_id,
    )
    self.positions[pos.buy_order_id] = pos
    self._save()
    return pos

  def on_sell_filled(
    self,
    order_request,
    order_result,
    reason: str = "unknown",
  ) -> Optional[Position]:
    """Match sell order to existing open position and close it."""
    symbol = order_request.symbol
    # Find open position for this symbol
    target_pos = None
    for pos in self.positions.values():
      if pos.symbol == symbol and not pos.is_closed:
        target_pos = pos
        break

    if target_pos is None:
      return None

    target_pos.sell_order_id = order_result.order_id
    target_pos.sell_price = order_result.filled_price
    target_pos.sell_time = datetime.now().isoformat()
    target_pos.sell_reason = reason
    self._save()
    return target_pos

  def get_open_positions(self, symbol: Optional[str] = None) -> list[Position]:
    """Get all open (not yet sold) positions, optionally filtered by symbol."""
    result = [p for p in self.positions.values() if not p.is_closed]
    if symbol:
      result = [p for p in result if p.symbol == symbol]
    return result

  def get_closed_positions(self, symbol: Optional[str] = None) -> list[Position]:
    """Get all closed positions."""
    result = [p for p in self.positions.values() if p.is_closed]
    if symbol:
      result = [p for p in result if p.symbol == symbol]
    return result

  def calculate_win_rate(self, symbol: Optional[str] = None) -> float:
    """Calculate win rate from closed positions."""
    closed = self.get_closed_positions(symbol)
    if not closed:
      return 0.0
    winning = sum(1 for p in closed if p.pnl is not None and p.pnl > 0)
    return round(winning / len(closed), 4)

  def calculate_total_pnl(self, symbol: Optional[str] = None) -> float:
    """Calculate total P&L from closed positions."""
    closed = self.get_closed_positions(symbol)
    return round(sum(p.pnl for p in closed if p.pnl is not None), 2)

  def calculate_avg_holding_seconds(
    self, symbol: Optional[str] = None
  ) -> Optional[float]:
    """Calculate average holding time in seconds."""
    closed = self.get_closed_positions(symbol)
    holding_times = [p.holding_seconds for p in closed if p.holding_seconds is not None]
    if not holding_times:
      return None
    return round(sum(holding_times) / len(holding_times), 1)
