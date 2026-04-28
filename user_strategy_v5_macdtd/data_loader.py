"""从 data_cache 读取 CSV 数据并返回 pandas DataFrame"""
from typing import Optional
import os
import pandas as pd
from .utils import ensure_datetime_index


class DataLoader:
    def __init__(self, cache_path: str):
        self.cache_path = cache_path

    def _csv_path(self, symbol: str, timeframe: str) -> str:
        # 假定 data_cache 中文件命名包含 symbol 和 timeframe 的信息
        # 尝试寻找匹配文件
        files = [f for f in os.listdir(self.cache_path) if f.endswith('.csv')]
        # 优先精确匹配 symbol + timeframe 子串
        for f in files:
            if symbol in f and timeframe in f:
                return os.path.join(self.cache_path, f)
        # 其次尝试 symbol 任意 + timeframe
        for f in files:
            if timeframe in f:
                return os.path.join(self.cache_path, f)
        # 兜底：symbol 任意匹配
        for f in files:
            if symbol in f:
                return os.path.join(self.cache_path, f)
        raise FileNotFoundError(f"找不到 {symbol} {timeframe} 对应的 CSV 文件于 {self.cache_path}")

    def load(self, symbol: str, timeframe: str, start: Optional[str] = None, end: Optional[str] = None) -> pd.DataFrame:
        path = self._csv_path(symbol, timeframe)
        # 使用 utf-8-sig 自动去除 BOM（有些 CSV 带 BOM，导致列名为 '\ufefftime'）
        df = pd.read_csv(path, encoding='utf-8-sig')
        # 统一列名为小写，便于兼容不同数据源
        df.columns = [c.strip() for c in df.columns]
        col_map = {c: c.lower() for c in df.columns}
        df = df.rename(columns=col_map)

        # 规范列名: 允许多种时间列名（timestamp, time, datetime, date）
        time_col = None
        for candidate in ['timestamp', 'time', 'datetime', 'date']:
            if candidate in df.columns:
                time_col = candidate
                break

        # 必要的行情列
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col not in df.columns:
                raise ValueError(f"CSV 缺少必要列: {col}")
        if time_col is None:
            # 额外尝试处理像 '\ufefftime' 之类的情况（已用 utf-8-sig 处理，理论上不再需要）
            raise ValueError('CSV 缺少时间列（期望 timestamp/time/datetime/date 之一）')

        # 确保时间列为 datetime 并设为索引
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.set_index(time_col)
        # 统一索引为 DatetimeIndex
        df = ensure_datetime_index(df)
        if start:
            df = df[df.index >= pd.to_datetime(start)]
        if end:
            df = df[df.index <= pd.to_datetime(end)]
        return df

    def load_multi(self, symbol: str, timeframes: list, start: Optional[str] = None, end: Optional[str] = None) -> dict:
        """加载多个 timeframe 的数据，返回 dict[timeframe]=DataFrame，且对齐时间范围"""
        dfs = {}
        for tf in timeframes:
            try:
                dfs[tf] = self.load(symbol, tf, start, end)
            except FileNotFoundError:
                dfs[tf] = None
        # 对齐到共同时间区间
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
            # 确保每个子 DataFrame 都有 DatetimeIndex
            dfs[k] = ensure_datetime_index(dfs[k])
        return dfs
