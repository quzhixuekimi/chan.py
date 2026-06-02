from __future__ import annotations

import os
from datetime import date
from sqlalchemy import (
  MetaData,
  Table,
  Column,
  BigInteger,
  Text,
  Date,
  JSON,
  TIMESTAMP,
  Numeric,
  UniqueConstraint,
  create_engine,
  select,
  insert,
  text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.dialects.postgresql import JSONB
import logging
from zoneinfo import ZoneInfo
from datetime import datetime
from datetime import date as _date

# ---------------------------------------------------------------------------
# 读取数据库 URL（系统环境变量）
# ---------------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
  raise RuntimeError("环境变量 DATABASE_URL 未定义，请在 systemd 环境文件中配置")

# ---------------------------------------------------------------------------
# 创建全局 engine（同步）
# ---------------------------------------------------------------------------
engine: Engine = create_engine(
  DATABASE_URL,
  pool_pre_ping=True,
  pool_size=5,
  max_overflow=10,
  future=True,
  connect_args={"connect_timeout": 5},
)

metadata = MetaData()

# ---------------------------------------------------------------------------
# 新增表：positions 与 trades（持仓与交易记录）
# ---------------------------------------------------------------------------
positions = Table(
  "positions",
  metadata,
  Column("id", BigInteger, primary_key=True, autoincrement=True),
  Column("code", Text, nullable=False),
  Column("position", BigInteger, default=0, nullable=False),
  Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()")),
  Column(
    "updated_at",
    TIMESTAMP(timezone=True),
    server_default=text("now()"),
    onupdate=text("now()"),
  ),
  UniqueConstraint("code", name="unique_code_position"),
)

trades = Table(
  "trades",
  metadata,
  Column("id", BigInteger, primary_key=True, autoincrement=True),
  Column("code", Text, nullable=False),
  Column("action", Text, nullable=False),  # buy or sell
  Column("strategy", Text, nullable=False),
  Column("order_id", Text, nullable=False),
  Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()")),
  Column(
    "updated_at",
    TIMESTAMP(timezone=True),
    server_default=text("now()"),
    onupdate=text("now()"),
  ),
  UniqueConstraint("code", "action", "strategy", "order_id", name="unique_code_trades"),
)

# ---------------------------------------------------------------------------
# 表定义（保持与数据库中 CREATE TABLE 语句一致）
# ---------------------------------------------------------------------------
api_response_cache = Table(
  "api_response_cache",
  metadata,
  Column("id", BigInteger, primary_key=True, autoincrement=True),
  Column("code", Text, nullable=False),
  Column("level", Text, nullable=False),
  Column("endpoint", Text, nullable=False),
  Column("today", Date, nullable=False),
  Column("content", JSONB, nullable=False),
  Column("created_at", TIMESTAMP(timezone=True), server_default="now()"),
  UniqueConstraint("code", "level", "endpoint", "today", name="unique_cache"),
)


# ---------------------------------------------------------------------------
# queue 表：每天一条生成的交易队列快照
# ---------------------------------------------------------------------------
queue = Table(
  "queue",
  metadata,
  Column("id", BigInteger, primary_key=True, autoincrement=True),
  Column("generated_at", TIMESTAMP(timezone=True), server_default=text("now()")),
  Column("generated_date", Date, nullable=False),
  Column("signals", JSONB, nullable=False),
  Column("created_at", TIMESTAMP(timezone=True), server_default=text("now()")),
  UniqueConstraint("generated_date", name="unique_generated_date"),
)


# ---------------------------------------------------------------------------
# Queue helpers
# ---------------------------------------------------------------------------
def get_queue_for_date(conn, generated_date: _date):
  """返回指定日期的最新一条 queue 记录（signals, generated_at），不存在返回 None"""
  stmt = (
    select(queue.c.signals, queue.c.generated_at)
    .where(queue.c.generated_date == generated_date)
    .order_by(queue.c.id.desc())
    .limit(1)
  )
  row = conn.execute(stmt).first()
  return row


def upsert_queue_for_date(conn, generated_date: _date, signals_json):
  """使用 PostgreSQL 的 INSERT ... ON CONFLICT 按 generated_date upsert signals 字段"""
  from sqlalchemy.dialects.postgresql import insert as pg_insert

  stmt = (
    pg_insert(queue)
    .values(generated_date=generated_date, signals=signals_json)
    .on_conflict_do_update(
      index_elements=["generated_date"],
      set_={"signals": signals_json, "generated_at": text("now()")},
    )
  )
  conn.execute(stmt)


def load_queue_today_from_db():
  """按 Asia/Shanghai 时区的日期读取今天的 queue（返回 dict 和原来 load_queue_today 的 shape 一致）"""
  today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
  with engine.connect() as conn:
    row = get_queue_for_date(conn, today)
    if not row:
      return {"generated_at": "", "signals": []}
    signals, generated_at = row[0], row[1]
    # signals 是 JSON 类型 -> Python 对象
    return {"generated_at": generated_at.isoformat(), "signals": signals}


def write_queue_back_to_db(queue_data: dict):
  """把内存中的 queue dict 写回当天的 queue 表（按 Asia/Shanghai 的日期 upsert）"""
  today = datetime.now(ZoneInfo("Asia/Shanghai")).date()
  signals = queue_data.get("signals", [])
  # 使用事务执行 upsert
  with engine.begin() as conn:
    upsert_queue_for_date(conn, today, signals)


# ---------------------------------------------------------------------------
# 简单的缓存读写函数（同步）
# ---------------------------------------------------------------------------
def get_cached(conn, code: str, level: str, endpoint: str, today: date):
  """根据四个键返回 JSON 内容，如果不存在返回 None
  若数据库不可达或查询异常，则返回 None 并记录日志，业务会回退到正常计算路径"""
  try:
    stmt = select(api_response_cache.c.content).where(
      api_response_cache.c.code == code,
      api_response_cache.c.level == level,
      api_response_cache.c.endpoint == endpoint,
      api_response_cache.c.today == today,
    )
    result = conn.execute(stmt).first()
    return result[0] if result else None
  except Exception as e:
    logging.getLogger("chan_api_server").error("Cache read error: %s", e)
    return None


def delete_old_cache_for_code(conn, code: str, today: date):
  """删除指定 code 中早于 today 的缓存记录（每天首次调用 API 时执行一次）"""
  stmt = api_response_cache.delete().where(
    api_response_cache.c.code == code,
    api_response_cache.c.today < today,
  )


def set_cached(conn, code: str, level: str, endpoint: str, today: date, content_json):
  """使用 ON CONFLICT 更新或插入缓存记录"""
  from datetime import timedelta
  from sqlalchemy.dialects.postgresql import insert as pg_insert

  stmt = (
    pg_insert(api_response_cache)
    .values(
      code=code,
      level=level,
      endpoint=endpoint,
      today=today,
      content=content_json,
    )
    .on_conflict_do_update(
      index_elements=["code", "level", "endpoint", "today"],
      set_={"content": content_json, "created_at": text("now()")},
    )
  )
  logging.getLogger("chan_api_server").info(
    "INSERT cache: code=%s level=%s endpoint=%s today=%s", code, level, endpoint, today
  )
  try:
    conn.execute(stmt)
  except Exception as e:
    logging.getLogger("chan_api_server").error("Cache write error: %s", e)


# ---------------------------------------------------------------------------
# Positions & Trades helpers
# ---------------------------------------------------------------------------


def get_position(conn, code: str):
  """返回给定 code 的持仓记录（如果不存在返回 None）"""
  stmt = select(positions.c.position).where(positions.c.code == code)
  result = conn.execute(stmt).first()
  return result[0] if result else None


def upsert_position(conn, code: str, delta: int):
  """根据 delta（正数增仓，负数减仓）更新或插入持仓记录。
  若记录不存在且 delta 为正，则插入新记录；若 delta 为负且记录不存在则不操作。
  返回更新后的持仓数量（>=0）。"""
  # 使用 PostgreSQL 的 INSERT ... ON CONFLICT
  from sqlalchemy.dialects.postgresql import insert as pg_insert

  if delta >= 0:
    stmt = (
      pg_insert(positions)
      .values(code=code, position=delta)
      .on_conflict_do_update(
        index_elements=["code"],
        set_={"position": positions.c.position + delta, "updated_at": text("now()")},
      )
    )
    conn.execute(stmt)
  else:
    # 对已有记录减仓
    current = get_position(conn, code)
    if current is None:
      return 0
    new_val = max(0, current + delta)
    stmt = (
      positions.update()
      .where(positions.c.code == code)
      .values(position=new_val, updated_at=text("now()"))
    )
    conn.execute(stmt)
  # 返回最新值
  return get_position(conn, code)


def insert_trade(conn, code: str, action: str, strategy: str, order_id: str):
  """在 trades 表中插入一条记录。若同一 (code, action, strategy, order_id) 已存在则忽略（避免唯一约束冲突）。"""
  from sqlalchemy.dialects.postgresql import insert as pg_insert

  stmt = (
    pg_insert(trades)
    .values(
      code=code,
      action=action,
      strategy=strategy,
      order_id=order_id,
    )
    .on_conflict_do_nothing(index_elements=["code", "action", "strategy", "order_id"])
  )
  conn.execute(stmt)


# ---------------------------------------------------------------------------
# kline 表: K线数据（所有股票、所有周期共用一张表）
# 与 migrations/001_kline_table.sql 保持一致
# ---------------------------------------------------------------------------
kline = Table(
  "kline",
  metadata,
  Column("id", BigInteger, primary_key=True, autoincrement=True),
  Column("code", Text, nullable=False),
  Column("level", Text, nullable=False),
  Column("time", TIMESTAMP, nullable=False),
  Column("open", Numeric(18, 6), nullable=False),
  Column("high", Numeric(18, 6), nullable=False),
  Column("low", Numeric(18, 6), nullable=False),
  Column("close", Numeric(18, 6), nullable=False),
  Column("volume", BigInteger, nullable=False, default=0),
  Column("updated_at", TIMESTAMP(timezone=True), server_default=text("now()")),
  UniqueConstraint("code", "level", "time", name="unique_kline_natural"),
)


# ---------------------------------------------------------------------------
# kline helpers
# ---------------------------------------------------------------------------
def get_latest_kline_time(conn, code: str, level: str):
  """返回 (code, level) 在 kline 表中最新一条 bar 的 time，未找到返回 None。"""
  stmt = (
    select(kline.c.time)
    .where(kline.c.code == code, kline.c.level == level)
    .order_by(kline.c.time.desc())
    .limit(1)
  )
  row = conn.execute(stmt).first()
  return row[0] if row else None


def upsert_kline(conn, code: str, level: str, rows: list[dict]) -> int:
  """批量 UPSERT K线数据，按 (code, level, time) 自然键去重。
  rows 中每条 dict 需包含: time (datetime), open, high, low, close, volume。
  返回受影响的行数（INSERT+UPDATE 合计）。"""
  if not rows:
    return 0

  from sqlalchemy.dialects.postgresql import insert as pg_insert

  values = [
    {
      "code": code,
      "level": level,
      "time": r["time"],
      "open": r["open"],
      "high": r["high"],
      "low": r["low"],
      "close": r["close"],
      "volume": int(r.get("volume") or 0),
    }
    for r in rows
  ]

  stmt = pg_insert(kline).values(values)
  stmt = stmt.on_conflict_do_update(
    index_elements=["code", "level", "time"],
    set_={
      "open": stmt.excluded.open,
      "high": stmt.excluded.high,
      "low": stmt.excluded.low,
      "close": stmt.excluded.close,
      "volume": stmt.excluded.volume,
      "updated_at": text("now()"),
    },
  )
  result = conn.execute(stmt)
  return result.rowcount or 0


def read_kline(conn, code: str, level: str, begin_date=None, end_date=None):
  """读取 (code, level) 的 K线数据，按 time 升序，可选过滤 begin_date / end_date。
  返回 pandas DataFrame，列: time, open, high, low, close, volume。"""
  import pandas as pd
  from datetime import datetime as _dt, timedelta as _td

  def _to_dt(v):
    if v is None:
      return None
    if isinstance(v, _dt):
      return v
    return pd.to_datetime(v).to_pydatetime()

  begin_dt = _to_dt(begin_date)
  end_dt = _to_dt(end_date)

  stmt = select(
    kline.c.time,
    kline.c.open,
    kline.c.high,
    kline.c.low,
    kline.c.close,
    kline.c.volume,
  ).where(kline.c.code == code, kline.c.level == level)
  if begin_dt is not None:
    stmt = stmt.where(kline.c.time >= begin_dt)
  if end_dt is not None:
    stmt = stmt.where(kline.c.time < end_dt + _td(days=1))
  stmt = stmt.order_by(kline.c.time.asc())

  rows = conn.execute(stmt).fetchall()
  if not rows:
    return pd.DataFrame(columns=["time", "open", "high", "low", "close", "volume"])

  df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume"])
  df["time"] = pd.to_datetime(df["time"])
  for c in ["open", "high", "low", "close"]:
    df[c] = pd.to_numeric(df[c])
  df["volume"] = df["volume"].astype("int64")
  return df


def delete_kline_range(
  conn, code: str, level: str, begin_date=None, end_date=None
) -> int:
  """删除 (code, level) 在 [begin_date, end_date) 范围内的 K线。
  用于 2H/4H 重新聚合时先清空目标区间。返回删除行数。"""
  import pandas as pd
  from datetime import datetime as _dt, timedelta as _td

  def _to_dt(v):
    if v is None:
      return None
    if isinstance(v, _dt):
      return v
    return pd.to_datetime(v).to_pydatetime()

  begin_dt = _to_dt(begin_date)
  end_dt = _to_dt(end_date)

  stmt = kline.delete().where(kline.c.code == code, kline.c.level == level)
  if begin_dt is not None:
    stmt = stmt.where(kline.c.time >= begin_dt)
  if end_dt is not None:
    stmt = stmt.where(kline.c.time < end_dt + _td(days=1))
  result = conn.execute(stmt)
  return result.rowcount or 0
