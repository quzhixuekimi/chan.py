-- ============================================================================
-- Migration 001: create kline table
-- Schema B: single table for all US-stock kline data across all timeframes
--
-- Run with:
--   psql "$DATABASE_URL" -f migrations/001_kline_table.sql
-- ============================================================================

CREATE TABLE IF NOT EXISTS kline (
    id           BIGSERIAL    PRIMARY KEY,
    code         VARCHAR(16)  NOT NULL,
    level        VARCHAR(8)   NOT NULL,            -- '1d' | '1h' | '2h' | '4h' | '30m' | '15m'
    "time"       TIMESTAMP    NOT NULL,            -- bar end time (TZ-naive, US market local)
    open         NUMERIC(18,6) NOT NULL,
    high         NUMERIC(18,6) NOT NULL,
    low          NUMERIC(18,6) NOT NULL,
    close        NUMERIC(18,6) NOT NULL,
    volume       BIGINT       NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT   unique_kline_natural UNIQUE (code, level, "time")
);

-- Fast lookup: latest bar for (code, level)
CREATE INDEX IF NOT EXISTS idx_kline_code_level_time_desc
    ON kline (code, level, "time" DESC);

-- Fast lookup by date for scanning / cleanup
CREATE INDEX IF NOT EXISTS idx_kline_time ON kline ("time" DESC);

COMMENT ON TABLE  kline                IS 'US stock kline bars across all timeframes (single table, UPSERT on (code, level, time))';
COMMENT ON COLUMN kline.code           IS 'Stock ticker symbol, e.g. AAPL';
COMMENT ON COLUMN kline.level          IS 'Timeframe: 1d, 1h, 2h, 4h, 30m, 15m (lowercase)';
COMMENT ON COLUMN kline."time"         IS 'Bar end time, US market local (naive timestamp)';
COMMENT ON COLUMN kline.updated_at     IS 'Last UPSERT time (audit)';
