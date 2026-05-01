from pathlib import Path
from pydantic import BaseModel
from typing import Literal
import os


class TradeConfig(BaseModel):
  futu_host: str = "127.0.0.1"
  futu_port: int = 22222
  trd_env: Literal["SIMULATE", "REAL"] = "SIMULATE"
  order_qty: int = 1
  order_price_type: Literal["market", "limit"] = "market"
  morning_trigger_hour: int = 9
  morning_trigger_minute: int = 30
  evening_trigger_hour: int = 21
  evening_trigger_minute: int = 30
  queue_dir: Path = Path("trade_system/queue")
  trades_dir: Path = Path("trade_system/data/trades")
  metrics_dir: Path = Path("trade_system/data/metrics")
  telegram_enabled: bool = True
  telegram_base_url: str = "http://127.0.0.1:8010"

  def __init__(self, **data):
    super().__init__(**data)
    self.queue_dir.mkdir(parents=True, exist_ok=True)
    self.trades_dir.mkdir(parents=True, exist_ok=True)
    self.metrics_dir.mkdir(parents=True, exist_ok=True)

  @property
  def telegram_chat_id(self) -> str:
    return os.getenv("TELEGRAM_CHAT_ID", "")

  @property
  def telegram_bot_token(self) -> str:
    return os.getenv("TELEGRAM_BOT_TOKEN", "")


_config: TradeConfig | None = None


def get_config() -> TradeConfig:
  global _config
  if _config is None:
    _config = TradeConfig()
  return _config


def init_config(**kwargs) -> TradeConfig:
  global _config
  _config = TradeConfig(**kwargs)
  return _config
