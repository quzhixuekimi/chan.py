"""FutuAdapter skeleton supporting TrdEnv.SIMULATE (mockable)

This is a lightweight skeleton that provides the same interface as ExchangeAdapter
and can be swapped into the Executor for integration testing. It does not call
the real futu API; instead it exposes hooks for injecting a mock client.
"""

from .exchange_interface import ExchangeAdapter
from .order_types import OrderRequest, OrderResponse, Fill
import uuid, threading, time
from typing import Dict, Any, Optional


class FutuAdapter(ExchangeAdapter):
  def __init__(self, client=None, config: Dict[str, Any] = None):
    # client: optional futu client object (real or mock)
    self.client = client
    self.config = config or {}
    self.orders = {}
    self._lock = threading.Lock()
    # simulate_open: if True, place_order will create an open order instead of immediate fill
    self.simulate_open = bool(self.config.get("simulate_open", False))
    # auto_fill_delay seconds - if >0, reconcile_pending will simulate passage of time
    self.auto_fill_delay = float(self.config.get("auto_fill_delay", 0.0))

  def get_balance(self) -> float:
    # delegate to client if available
    if self.client and hasattr(self.client, "get_balance"):
      return self.client.get_balance()
    return float(self.config.get("initial_cash", 100000.0))

  def get_positions(self):
    # basic positions mapping, delegate to client if available
    if self.client and hasattr(self.client, "get_positions"):
      return self.client.get_positions()
    return {}

  def place_order(self, order_request: OrderRequest) -> OrderResponse:
    # for SIMULATE mode we can simulate immediate fills or delegate to client
    order_id = str(uuid.uuid4())
    if self.client and hasattr(self.client, "place_order") and not self.simulate_open:
      raw = self.client.place_order(order_request)
      # expect raw to be a dict with keys: success, fill_qty, avg_price
      if raw.get("success"):
        fills = [
          Fill(
            qty=raw.get("fill_qty", order_request.qty),
            price=raw.get("avg_price", 0.0),
            ts=raw.get("ts", ""),
          )
        ]
        return OrderResponse(
          order_id=order_id,
          request_id=order_request.request_id,
          status="filled",
          filled_qty=fills[0].qty,
          avg_price=fills[0].price,
          fills=fills,
          raw_response=raw,
        )
      else:
        return OrderResponse(
          order_id=order_id,
          request_id=order_request.request_id,
          status="rejected",
          raw_response=raw,
        )

    # simulate open order behavior if requested
    if self.simulate_open:
      with self._lock:
        self.orders[order_id] = {
          "request": order_request,
          "status": "open",
          "created_ts": time.time(),
        }
      return OrderResponse(
        order_id=order_id, request_id=order_request.request_id, status="open"
      )

    # fallback simple simulate immediate fill
    price = order_request.price or float(self.config.get("fallback_price", 0.0))
    fills = [
      Fill(
        qty=order_request.qty,
        price=price,
        ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
      )
    ]
    # register as filled in orders mapping
    with self._lock:
      self.orders[order_id] = {
        "request": order_request,
        "status": "filled",
        "fills": fills,
      }
    return OrderResponse(
      order_id=order_id,
      request_id=order_request.request_id,
      status="filled",
      filled_qty=order_request.qty,
      avg_price=price,
      fills=fills,
    )

  def cancel_order(self, order_id: str) -> Dict[str, Any]:
    with self._lock:
      if order_id in self.orders and self.orders[order_id]["status"] == "open":
        self.orders[order_id]["status"] = "cancelled"
        return {"success": True}
    return {"success": False}

  def get_order_status(self, order_id: str) -> Dict[str, Any]:
    if order_id in self.orders:
      entry = self.orders[order_id]
      return entry
    return {"status": "unknown"}

  def reconcile_pending(
    self, fill_if_older_than: Optional[float] = None
  ) -> Dict[str, OrderResponse]:
    """Attempt to reconcile open orders. If fill_if_older_than is provided (seconds),
    open orders older than that will be filled. If set to None and auto_fill_delay==0,
    all open orders are filled immediately.

    Returns map order_id -> OrderResponse for filled orders.
    """
    filled = {}
    now = time.time()
    with self._lock:
      for oid, entry in list(self.orders.items()):
        if entry.get("status") != "open":
          continue
        created = entry.get("created_ts", 0)
        should_fill = False
        if fill_if_older_than is not None:
          should_fill = (now - created) >= float(fill_if_older_than)
        elif self.auto_fill_delay > 0.0:
          should_fill = (now - created) >= float(self.auto_fill_delay)
        else:
          should_fill = True

        if should_fill:
          req = entry["request"]
          price = req.price or float(self.config.get("fallback_price", 0.0))
          fills = [
            Fill(
              qty=req.qty,
              price=price,
              ts=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            )
          ]
          entry["status"] = "filled"
          entry["fills"] = fills
          filled_resp = OrderResponse(
            order_id=oid,
            request_id=req.request_id,
            status="filled",
            filled_qty=req.qty,
            avg_price=price,
            fills=fills,
          )
          filled[oid] = filled_resp
    return filled
