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
  UniqueConstraint,
  create_engine,
  select,
  insert,
  text,
)
from sqlalchemy.engine import Engine
import logging

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
  Column("content", JSON, nullable=False),
  Column("created_at", TIMESTAMP(timezone=True), server_default="now()"),
  UniqueConstraint("code", "level", "endpoint", "today", name="unique_cache"),
)


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


import logging


def delete_old_cache_for_code(conn, code: str, today: date):
  """删除指定 code 中早于 today 的缓存记录（每天首次调用 API 时执行一次）"""
  stmt = api_response_cache.delete().where(
    api_response_cache.c.code == code,
    api_response_cache.c.today < today,
  )
  conn.execute(stmt)


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
