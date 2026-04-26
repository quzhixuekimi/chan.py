from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any


@dataclass
class OrderRequest:
  request_id: str
  symbol: str
  side: str  # 'BUY'|'SELL'
  qty: int
  order_type: str = "market"
  price: Optional[float] = None
  time: Optional[str] = None
  meta: Dict[str, Any] = field(default_factory=dict)


@dataclass
class Fill:
  qty: int
  price: float
  ts: str


@dataclass
class OrderResponse:
  order_id: str
  request_id: str
  status: str  # 'filled'|'open'|'rejected'
  filled_qty: int = 0
  avg_price: Optional[float] = None
  fills: List[Fill] = field(default_factory=list)
  raw_response: Optional[Dict[str, Any]] = None
