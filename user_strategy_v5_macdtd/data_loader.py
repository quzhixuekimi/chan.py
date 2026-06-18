"""从 data_cache 读取 CSV 数据并返回 pandas DataFrame；或从 DB kline 表读取。"""
from typing import Optional
import os
import pandas as pd
from .utils import ensure_datetime_index

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
import kline_loader


class DataLoader:
    def __init__(self, cache_path: str, source: str = "db"):
        """
        Args:
            cache_path: data_cache 目录路径（CSV 模式时使用）
            source: 'db' 从 PostgreSQL kline 表读取（默认）；'csv' 从 data_cache/*.csv 读取
        """
        self.cache_path = cache_path
        self.source = source

    def _csv_path(self, symbol: str, timeframe: str) -> str:
        from datetime import datetime
        today = datetime.now().strftime("%Y-%m-%d")
        symbol = symbol.upper()
        if timeframe == "1d":
            fname = f"{symbol}_{today}_1d.csv"
        elif timeframe in ("4h", "2h", "1h"):
            fname = f"{symbol}_{today}_yf_{timeframe}_730d.csv"
        elif timeframe in ("30m", "15m"):
            fname = f"{symbol}_{today}_yf_{timeframe}_60d.csv"
        else:
            raise FileNotFoundError(f"unknown timeframe: {timeframe}")
        path = os.path.join(self.cache_path, fname)
        if not os.path.exists(path):
            raise FileNotFoundError(f"找不到 {symbol} {timeframe} 对应的 CSV 文件于 {self.cache_path}")
        return path

    def load(self, symbol: str, timeframe: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        if self.source == "db":
            df = kline_loader.load_kline_df(symbol, timeframe, start=start, end=end)
            df = df.rename(columns={"time": "dt"}).set_index("dt")
            df = ensure_datetime_index(df)
            return df

        # CSV 原有逻辑
        path = self._csv_path(symbol, timeframe)
        df = pd.read_csv(path, encoding='utf-8-sig')
        df.columns = [c.strip() for c in df.columns]
        col_map = {c: c.lower() for c in df.columns}
        df = df.rename(columns=col_map)

        time_col = None
        for candidate in ['timestamp', 'time', 'datetime', 'date']:
            if candidate in df.columns:
                time_col = candidate
                break

        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col not in df.columns:
                raise ValueError(f"CSV 缺少必要列: {col}")
        if time_col is None:
            raise ValueError('CSV 缺少时间列（期望 timestamp/time/datetime/date 之一）')

        df[time_col] = pd.to_datetime(df[time_col])
        df = df.set_index(time_col)
        df = ensure_datetime_index(df)
        if start:
            df = df[df.index >= pd.to_datetime(start)]
        if end:
            df = df[df.index <= pd.to_datetime(end)]
        return df

    def load_multi(self, symbol: str, timeframes: list, start: Optional[str] = None, end: Optional[str] = None) -> dict:
        if self.source == "db":
            dfs = {}
            for tf in timeframes:
                try:
                    df = kline_loader.load_kline_df(symbol, tf, start=start, end=end)
                    df = df.rename(columns={"time": "dt"}).set_index("dt")
                    dfs[tf] = ensure_datetime_index(df)
                except FileNotFoundError:
                    dfs[tf] = None
            valid = [df for df in dfs.values() if df is not None and len(df) > 0]
            if not valid:
                return dfs
            starts = [df.index[0] for df in valid]
            ends = [df.index[-1] for df in valid]
            common_start = max(starts)
            common_end = min(ends)
            for k, df in list(dfs.items()):
                if df is None:
                    continue
                dfs[k] = df.loc[common_start:common_end].copy()
                dfs[k] = ensure_datetime_index(dfs[k])
            return dfs

        # CSV 原有逻辑
        dfs = {}
        for tf in timeframes:
            try:
                dfs[tf] = self.load(symbol, tf, start, end)
            except FileNotFoundError:
                dfs[tf] = None
        valid = [df for df in dfs.values() if df is not None and len(df) > 0]
        if not valid:
            return dfs
        starts = [df.index[0] for df in valid]
        ends = [df.index[-1] for df in valid]
        common_start = max(starts)
        common_end = min(ends)
        for k, df in list(dfs.items()):
            if df is None:
                continue
            dfs[k] = df.loc[common_start:common_end].copy()
            dfs[k] = ensure_datetime_index(dfs[k])
        return dfs
