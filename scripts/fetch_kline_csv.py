#!/usr/bin/env python3
"""Fetch K‑line data from yfinance and write CSV files.

与生产环境 chan_api_server.py 保持一致，使用 ticker.history() 而非 yf.download()，
避免 timezone 问题。每个 interval 独立 try/except，失败只跳过该 interval。
"""

import argparse
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import List

try:
    import pandas as pd
except ImportError as e:
    raise ImportError("pandas is required. Install via: pip install pandas") from e
import yfinance as yf


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """与 chan_api_server.py 完全一致"""
    rename_map = {}
    for col in df.columns:
        raw = str(col).strip()
        key = raw.lower()
        if raw in ["日期"] or key in ["time", "date", "datetime", "timestamp", "dt"]:
            rename_map[col] = "time"
        elif raw in ["开盘"] or key == "open":
            rename_map[col] = "open"
        elif raw in ["最高"] or key == "high":
            rename_map[col] = "high"
        elif raw in ["最低"] or key == "low":
            rename_map[col] = "low"
        elif raw in ["收盘"] or key == "close":
            rename_map[col] = "close"
        elif raw in ["成交量"] or key in ["volume", "vol"]:
            rename_map[col] = "volume"
    df = df.rename(columns=rename_map)
    required = ["time", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(
            f"missing columns after normalize: {missing}, raw columns={list(df.columns)}"
        )
    if "volume" not in df.columns:
        df["volume"] = 0.0
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = (
        df.dropna(subset=["time"])
        .sort_values("time")
        .drop_duplicates(subset=["time"])
        .reset_index(drop=True)
    )
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    if df.empty:
        raise ValueError("normalized dataframe is empty")
    # 与 production 代码一致：如果有时区则移除，不做转换
    if getattr(df["time"].dt, "tz", None) is not None:
        df["time"] = df["time"].dt.tz_localize(None)
    return df[["time", "open", "high", "low", "close", "volume"]]


def _save_csv(df: pd.DataFrame, path: Path) -> None:
    out = df.copy()
    out["time"] = pd.to_datetime(out["time"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    out.to_csv(path, index=False, encoding="utf-8-sig")


def _today_str() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def fetch_daily_from_yfinance(code: str) -> pd.DataFrame:
    ticker = yf.Ticker(code.upper())
    start_date = (datetime.now() - timedelta(days=365 * 20 + 30)).strftime("%Y-%m-%d")
    df = ticker.history(start=start_date, interval="1d", auto_adjust=False, actions=False)
    if df is None or df.empty:
        raise ValueError(f"empty daily data for {code}")
    df = df.reset_index()
    if "Date" in df.columns:
        df = df.rename(columns={"Date": "time"})
    elif "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "time"})
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
    return _normalize_columns(df)


def fetch_60m_from_yfinance(code: str) -> pd.DataFrame:
    ticker = yf.Ticker(code.upper())
    df = ticker.history(period="730d", interval="60m", auto_adjust=False, actions=False)
    if df is None or df.empty:
        raise ValueError(f"empty 60m data for {code}")
    df = df.reset_index()
    if "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "time"})
    elif "Date" in df.columns:
        df = df.rename(columns={"Date": "time"})
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
    return _normalize_columns(df)


def fetch_30m_from_yfinance(code: str) -> pd.DataFrame:
    ticker = yf.Ticker(code.upper())
    df = ticker.history(period="60d", interval="30m", auto_adjust=False, actions=False)
    if df is None or df.empty:
        raise ValueError(f"empty 30m data for {code}")
    df = df.reset_index()
    if "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "time"})
    elif "Date" in df.columns:
        df = df.rename(columns={"Date": "time"})
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
    return _normalize_columns(df)


def fetch_15m_from_yfinance(code: str) -> pd.DataFrame:
    ticker = yf.Ticker(code.upper())
    df = ticker.history(period="60d", interval="15m", auto_adjust=False, actions=False)
    if df is None or df.empty:
        raise ValueError(f"empty 15m data for {code}")
    df = df.reset_index()
    if "Datetime" in df.columns:
        df = df.rename(columns={"Datetime": "time"})
    elif "Date" in df.columns:
        df = df.rename(columns={"Date": "time"})
    df = df.rename(columns={"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"})
    return _normalize_columns(df)


def download_and_save(symbol: str, output_dir: Path, today_str: str) -> None:
    logger = logging.getLogger("fetch_kline_csv")

    # 1D
    try:
        logger.info("Fetching 1D for %s", symbol)
        df = fetch_daily_from_yfinance(symbol)
        path = output_dir / f"{symbol.upper()}_{today_str}_1d.csv"
        _save_csv(df, path)
        logger.info("Saved 1D %s", path.name)
    except Exception as e:
        logger.warning("1D for %s failed: %s", symbol, e)

    # 1H
    df_1h = None
    try:
        logger.info("Fetching 1H for %s", symbol)
        df_1h = fetch_60m_from_yfinance(symbol)
        path = output_dir / f"{symbol.upper()}_{today_str}_yf_1h_730d.csv"
        _save_csv(df_1h, path)
        logger.info("Saved 1H %s", path.name)
    except Exception as e:
        logger.warning("1H for %s failed: %s", symbol, e)

    # 30M
    try:
        logger.info("Fetching 30M for %s", symbol)
        df = fetch_30m_from_yfinance(symbol)
        path = output_dir / f"{symbol.upper()}_{today_str}_yf_30m_60d.csv"
        _save_csv(df, path)
        logger.info("Saved 30M %s", path.name)
    except Exception as e:
        logger.warning("30M for %s failed: %s", symbol, e)

    # 15M
    try:
        logger.info("Fetching 15M for %s", symbol)
        df = fetch_15m_from_yfinance(symbol)
        path = output_dir / f"{symbol.upper()}_{today_str}_yf_15m_60d.csv"
        _save_csv(df, path)
        logger.info("Saved 15M %s", path.name)
    except Exception as e:
        logger.warning("15M for %s failed: %s", symbol, e)


def parse_symbols(arg: str) -> List[str]:
    return [s.strip() for s in arg.split(",") if s.strip()]


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    parser = argparse.ArgumentParser(description="Fetch yfinance K‑line CSVs")
    parser.add_argument("--symbols", required=True, type=parse_symbols, help="逗号分隔的股票代码，如 AAPL,TSLA")
    parser.add_argument("--output-dir", default="data_cache", help="CSV 输出目录（默认 ./data_cache）")
    args = parser.parse_args()
    out_dir = Path(args.output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    today_str = _today_str()
    for sym in args.symbols:
        try:
            download_and_save(sym, out_dir, today_str)
        except Exception as e:
            logging.exception("Failed to fetch data for %s: %s", sym, e)


if __name__ == "__main__":
    main()
