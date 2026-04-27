"""CSV / digest -> OrderRequest mapper utilities

Provides functions to map a CSV row (dict) produced by strategy digest/run_* scripts
into OrderRequest dataclass instances consumed by the trade_system executor.
"""

from typing import Dict, List, Optional
import csv, hashlib
from . import __name__
from ..adapters.order_types import OrderRequest


def _gen_request_id_from_row(row: Dict[str, str]) -> str:
  """Generate a stable idempotency key from important row fields."""
  key_fields = [
    row.get("symbol", ""),
    row.get("event_time", row.get("time", row.get("timestamp", ""))),
    row.get("latest_event_type", row.get("event_type", "")),
    row.get("side", row.get("signal", "")),
  ]
  raw = "|".join([str(x) for x in key_fields])
  return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def map_row_to_order_request(row: Dict[str, str]) -> OrderRequest:
  """Map a CSV/dict row to OrderRequest.

  Expected common keys: symbol, side, qty, price, order_type, request_id, event_time
  Falls back to generate request_id if not provided.
  """
  symbol = row.get("symbol") or row.get("code") or row.get("stock_code")
  side = (row.get("side") or row.get("signal") or "").upper()
  qty_raw = row.get("qty") or row.get("quota") or row.get("size") or "0"
  try:
    qty = int(float(qty_raw))
  except Exception:
    qty = 0
  price_raw = row.get("price") or row.get("latest_price") or row.get("open_price") or ""
  try:
    price = float(price_raw) if price_raw != "" else None
  except Exception:
    price = None

  order_type = row.get("order_type") or row.get("type") or "market"
  request_id = (
    row.get("request_id") or row.get("requestId") or row.get("id") or row.get("uid")
  )
  if not request_id:
    request_id = _gen_request_id_from_row(row)

  time = row.get("event_time") or row.get("time") or row.get("timestamp")

  return OrderRequest(
    request_id=request_id,
    symbol=symbol,
    side=side,
    qty=qty,
    price=price,
    order_type=order_type,
    time=time,
    meta=row,
  )


def csv_to_order_requests(path: str) -> List[OrderRequest]:
  res: List[OrderRequest] = []
  with open(path, "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    for r in reader:
      res.append(map_row_to_order_request(r))
  return res
