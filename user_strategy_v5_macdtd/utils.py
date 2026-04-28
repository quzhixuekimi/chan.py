"""通用工具函数"""
from typing import List
import pandas as pd


def ensure_datetime_index(df: pd.DataFrame, time_col: str = 'timestamp') -> pd.DataFrame:
    if time_col in df.columns:
        df[time_col] = pd.to_datetime(df[time_col])
        df = df.set_index(time_col)
    else:
        if not isinstance(df.index, pd.DatetimeIndex):
            raise ValueError('DataFrame must have a datetime index or timestamp column')
    return df


def align_timeframes(dfs: List[pd.DataFrame]) -> List[pd.DataFrame]:
    """按共有时间范围对齐多个 DataFrame"""
    if not dfs:
        return dfs
    starts = [df.index[0] for df in dfs]
    ends = [df.index[-1] for df in dfs]
    start = max(starts)
    end = min(ends)
    aligned = [df.loc[start:end].copy() for df in dfs]
    return aligned
