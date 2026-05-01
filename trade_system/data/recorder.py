import json
from datetime import datetime
from pathlib import Path
from typing import NamedTuple, Literal
import uuid


class TradeRecord(NamedTuple):
  id: str
  queue_id: str
  symbol: str
  action: str
  requested_price: float | None
  filled_price: float | None
  qty: int
  status: Literal["queued", "filled", "partial", "failed", "cancelled"]
  order_id: str | None
  filled_at: str | None
  pnl: float | None
  reason: str


class TradeRecorder:
  def __init__(self, output_dir: Path):
    self.output_dir = output_dir
    self.output_dir.mkdir(parents=True, exist_ok=True)

  def record(self, signal: dict, result: dict) -> TradeRecord:
    record = TradeRecord(
      id=str(uuid.uuid4()),
      queue_id=signal.get("id", ""),
      symbol=signal["symbol"],
      action=signal["action"],
      requested_price=signal.get("target_price"),
      filled_price=result.get("filled_price"),
      qty=result.get("filled_qty", 1),
      status="filled" if result.get("success") else "failed",
      order_id=result.get("order_id"),
      filled_at=datetime.now().isoformat() if result.get("success") else None,
      pnl=None,
      reason=f"{signal.get('strategy')}_{signal.get('period')}",
    )
    today = datetime.now().strftime("%Y%m%d")
    output_path = self.output_dir / f"{today}-trades.json"
    if output_path.exists():
      trades = json.loads(output_path.read_text())
    else:
      trades = {"trades": []}
    trades["trades"].append(record._asdict())
    output_path.write_text(json.dumps(trades, ensure_ascii=False, indent=2))
    return record
