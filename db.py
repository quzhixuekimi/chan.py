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
        logging.getLogger('chan_api_server').error('Cache read error: %s', e)
        return None


import logging

def set_cached(conn, code: str, level: str, endpoint: str, today: date, content_json):
    """使用 ON CONFLICT 更新或插入缓存记录，并自动清理过期数据（3天前）"""
    from datetime import timedelta
    from sqlalchemy.dialects.postgresql import insert as pg_insert

    cutoff = today - timedelta(days=3)
    delete_stmt = api_response_cache.delete().where(
        api_response_cache.c.code == code,
        api_response_cache.c.level == level,
        api_response_cache.c.endpoint == endpoint,
        api_response_cache.c.today < cutoff,
    )
    conn.execute(delete_stmt)

    stmt = pg_insert(api_response_cache).values(
        code=code,
        level=level,
        endpoint=endpoint,
        today=today,
        content=content_json,
    ).on_conflict_do_update(
        index_elements=["code", "level", "endpoint", "today"],
        set_={"content": content_json, "created_at": text('now()')},
    )
    logging.getLogger('chan_api_server').info('INSERT cache: code=%s level=%s endpoint=%s today=%s', code, level, endpoint, today)
    try:
        conn.execute(stmt)
    except Exception as e:
        logging.getLogger('chan_api_server').error('Cache write error: %s', e)

def delete_old_cache(conn, keep_date: date):
    """删除早于 keep_date 的缓存（可在每日调度后调用）"""
    stmt = api_response_cache.delete().where(api_response_cache.c.today < keep_date)
    conn.execute(stmt)
