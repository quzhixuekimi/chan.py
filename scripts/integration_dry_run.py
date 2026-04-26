#!/usr/bin/env python3
"""Integration dry-run script for bystock aggregation + notify

This script avoids importing real `requests` by injecting a minimal stub into
sys.modules before importing telegram_notify_api. It then exercises the
send-digest path for aggregate_mode='bystock', verifying dedup behavior by
sending the same file twice and then sending after a content change.

Run from repository root:
  python3 scripts/integration_dry_run.py
"""

from __future__ import annotations

import sys
from types import SimpleNamespace
from datetime import datetime
from pathlib import Path
import csv
import hashlib
import json


def make_stub_requests():
  """Create a minimal requests stub with get/post used by telegram_notify_api."""

  class Resp:
    def __init__(self, ok=True, payload=None):
      self.ok = ok
      self._payload = payload or {}

    def json(self):
      return self._payload

    @property
    def text(self):
      return str(self._payload)

  def get(url, timeout=20):
    return Resp(ok=True, payload={"ok": True, "result": {}})

  def post(url, json=None, timeout=20):
    # emulate Telegram sendMessage response
    payload = {
      "ok": True,
      "result": {
        "message_id": 123,
        "text": json.get("text") if isinstance(json, dict) else "",
        "chat": {"id": json.get("chat_id") if isinstance(json, dict) else None},
        "date": int(datetime.utcnow().timestamp()),
      },
    }
    return Resp(ok=True, payload=payload)

  stub = SimpleNamespace(get=get, post=post)
  return stub


def sha256_text(s: str) -> str:
  import hashlib

  return hashlib.sha256(s.encode("utf-8")).hexdigest()


def write_agg_csv(path: Path, symbol: str, aggregated_summary_text: str):
  per_strategy = {
    "v6_bspzs": "",
    "v7_bi": "",
    "v8_byma": "",
    "v9_mr": "",
  }
  # build per_strategy_json as minimal mapping (we keep it simple)
  # aggregated_summary_text should already contain prefixed strategy parts
  fingerprint = sha256_text(aggregated_summary_text)
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("w", encoding="utf-8", newline="") as fh:
    writer = csv.DictWriter(
      fh,
      fieldnames=[
        "symbol",
        "reference_date",
        "generated_at_iso",
        "aggregated_summary_text",
        "per_strategy_json",
        "aggregated_fingerprint",
      ],
    )
    writer.writeheader()
    writer.writerow(
      {
        "symbol": symbol,
        "reference_date": datetime.utcnow().strftime("%Y/%m/%d"),
        "generated_at_iso": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "aggregated_summary_text": aggregated_summary_text,
        "per_strategy_json": json.dumps(per_strategy, ensure_ascii=False),
        "aggregated_fingerprint": fingerprint,
      }
    )


def main():
  # inject stub requests before importing telegram_notify_api
  sys.modules["requests"] = make_stub_requests()

  # create minimal pandas stub to avoid heavy dependency during dry-run
  class PdStub:
    @staticmethod
    def read_csv(path):
      # very small CSV parser: return object with to_dict(orient='records')
      import csv

      rows = []
      with open(path, encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for r in reader:
          rows.append(r)

      class DF:
        def __init__(self, rows, cols):
          self._rows = rows
          self.columns = cols
          self.empty = len(rows) == 0

        def to_dict(self, orient="records"):
          return list(self._rows)

      cols = rows[0].keys() if rows else []
      return DF(rows, cols)

  sys.modules["pandas"] = PdStub()

  # minimal fastapi + pydantic stubs so telegram_notify_api can import
  class FastAPIStub:
    def __init__(self, *args, **kwargs):
      pass

    def get(self, path):
      def deco(fn):
        return fn

      return deco

    def post(self, path):
      def deco(fn):
        return fn

      return deco

  class HTTPExceptionStub(Exception):
    def __init__(self, status_code=None, detail=None):
      super().__init__(f"HTTPException {status_code}: {detail}")
      self.status_code = status_code
      self.detail = detail

  # pydantic BaseModel stub that accepts kwargs and sets attributes
  class BaseModelStub:
    def __init__(self, **kwargs):
      for k, v in kwargs.items():
        setattr(self, k, v)

  def FieldStub(*args, **kwargs):
    return None

  sys.modules["fastapi"] = SimpleNamespace(
    FastAPI=FastAPIStub, HTTPException=HTTPExceptionStub
  )
  sys.modules["pydantic"] = SimpleNamespace(BaseModel=BaseModelStub, Field=FieldStub)

  # ensure repository root is on sys.path so imports find top-level modules
  repo_root = Path(__file__).resolve().parent.parent
  if str(repo_root) not in sys.path:
    sys.path.insert(0, str(repo_root))

  # import after stubbing
  import telegram_notify_api as tna
  from telegram_notify_api import TelegramDigestRequest

  agg_dir = Path(__file__).resolve().parent.parent / "results" / "aggregated"
  test_file = agg_dir / "test_market_aggregated_signal_digest_bystock.csv"

  # 1) write initial file
  sym = "AAPL"
  initial_text = "v6_bspzs : buy_1 | v7_bi :  | v8_byma :  | v9_mr : "
  write_agg_csv(test_file, sym, initial_text)

  # ensure env vars so telegram_notify_api won't error when calling telegram_api_url
  import os

  os.environ.setdefault("TELEGRAM_BOT_TOKEN", "stub-token")
  os.environ.setdefault("TELEGRAM_CHAT_ID", "test-chat")

  req = TelegramDigestRequest(
    strategy_id="bystock",
    digest_file=str(test_file),
    chat_id="test-chat",
    only_has_signal=False,
    dry_run=False,
    deduplicate=True,
    aggregate_mode="bystock",
  )

  print("-- First send (should send 1 message)")
  res1 = tna.send_digest(req)
  print(json.dumps(res1, indent=2, ensure_ascii=False))

  print("-- Second send (should be deduplicated: 0 messages)")
  res2 = tna.send_digest(req)
  print(json.dumps(res2, indent=2, ensure_ascii=False))

  # 3) modify file (change summary) to simulate update
  modified_text = "v6_bspzs : buy_1 modified | v7_bi :  | v8_byma :  | v9_mr : "
  write_agg_csv(test_file, sym, modified_text)

  print("-- Third send after change (should send 1 message)")
  res3 = tna.send_digest(req)
  print(json.dumps(res3, indent=2, ensure_ascii=False))

  # show sent state file for 'bystock'
  state_path = tna.get_sent_state_path("bystock")
  print("sent_state_path:", state_path)
  if state_path.exists():
    print(state_path.read_text(encoding="utf-8"))


if __name__ == "__main__":
  main()
