"""关键指标实现：MACD、ATR、RSI、TD9、背驰强度等
注意：优先使用 talib（如果可用），否则使用 pandas 实现近似版本
"""
from typing import Tuple
import numpy as np
import pandas as pd

import importlib
try:
    talib = importlib.import_module('talib')
    _HAS_TALIB = True
except Exception:
    talib = None
    _HAS_TALIB = False


def ema(series, span: int):
    return series.ewm(span=span, adjust=False).mean()


def add_macd(df: pd.DataFrame, fast: int = 12, slow: int = 26, signal: int = 9, prefix: str = 'macd') -> pd.DataFrame:
    close = df['close']
    if _HAS_TALIB:
        talib_local = talib
        assert talib_local is not None
        macd, macd_signal, macd_hist = talib_local.MACD(close.values, fastperiod=fast, slowperiod=slow, signalperiod=signal)
        df[f'{prefix}_line'] = macd
        df[f'{prefix}_signal'] = macd_signal
        df[f'{prefix}_hist'] = macd_hist
    else:
        ema_fast = ema(close, fast)
        ema_slow = ema(close, slow)
        macd_line = ema_fast - ema_slow
        macd_signal = ema(macd_line, signal)
        macd_hist = macd_line - macd_signal
        df[f'{prefix}_line'] = macd_line
        df[f'{prefix}_signal'] = macd_signal
        df[f'{prefix}_hist'] = macd_hist
    return df


def add_fast_macd(df: pd.DataFrame, fast: int = 8, slow: int = 17, signal: int = 6, prefix: str = 'macd_fast') -> pd.DataFrame:
    close = df['close']
    if _HAS_TALIB:
        talib_local = talib
        assert talib_local is not None
        macd_fast, _, _ = talib_local.MACD(close.values, fastperiod=fast, slowperiod=slow, signalperiod=signal)
        df[f'{prefix}'] = macd_fast
    else:
        ema_fast = ema(close, fast)
        ema_slow = ema(close, slow)
        macd_fast = ema_fast - ema_slow
        df[f'{prefix}'] = macd_fast
    return df


def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    if _HAS_TALIB:
        talib_local = talib
        assert talib_local is not None
        atr = talib_local.ATR(df['high'].values, df['low'].values, df['close'].values, timeperiod=period)
        df['atr'] = atr
        return df

    high = df['high']
    low = df['low']
    close = df['close']
    prev_close = close.shift(1)
    tr = pd.concat([high - low, (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    df['atr'] = atr
    return df


def add_rsi(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    if _HAS_TALIB:
        talib_local = talib
        assert talib_local is not None
        rsi = talib_local.RSI(df['close'].values, timeperiod=period)
        df['rsi'] = rsi
        return df

    delta = df['close'].diff()
    up = delta.clip(lower=0)
    down = -1 * delta.clip(upper=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / (ma_down + 1e-9)
    rsi = 100 - (100 / (1 + rs))
    df['rsi'] = rsi
    return df


def td_setup(df: pd.DataFrame, period: int = 9) -> int:
    """实现与原 v5 中 td_setup 相同的判定：
    若最近 period 根 K 线的 close 分别 <= 前 4 根对应 close 则为买入(1)
    若最近 period 根 K 线的 close 分别 >= 前 4 根对应 close 则为卖出(-1)
    否则 0
    """
    closes = df['close'].values
    if len(closes) < period + 4:
        return 0
    # compare last i closes to i+4
    buy = all(closes[-i] <= closes[-i - 4] for i in range(1, period + 1))
    sell = all(closes[-i] >= closes[-i - 4] for i in range(1, period + 1))
    if buy:
        return 1
    if sell:
        return -1
    return 0


def compute_td9(df: pd.DataFrame, period: int = 9) -> pd.DataFrame:
    # compute td_setup for every index using rolling windows
    vals = []
    for i in range(len(df)):
        window = df.iloc[max(0, i - (period + 4) + 1):i + 1]
        vals.append(td_setup(window, period))
    df[f'td_setup_{period}'] = pd.Series(vals, index=df.index)
    return df


def find_local_extremes(data, window: int = 3):
    arr = np.asarray(data)
    peaks = []
    troughs = []
    length = len(arr)
    for i in range(window, length - window):
        is_peak = all(arr[i] > arr[i - j] and arr[i] > arr[i + j] for j in range(1, window + 1))
        if is_peak:
            peaks.append((i, arr[i]))
        is_trough = all(arr[i] < arr[i - j] and arr[i] < arr[i + j] for j in range(1, window + 1))
        if is_trough:
            troughs.append((i, arr[i]))
    return peaks, troughs


def detect_bullish_divergence(price_troughs, macd_troughs, macd_fast_troughs=None):
    if len(price_troughs) < 2 or len(macd_troughs) < 2:
        return False, 0.0, None
    price1, price2 = price_troughs[-2], price_troughs[-1]
    macd1, macd2 = macd_troughs[-2], macd_troughs[-1]
    if price2[1] < price1[1] and macd2[1] > macd1[1]:
        strength = abs((price2[1] - price1[1]) / price1[1]) * 50 + \
                   ((macd2[1] - macd1[1]) / (abs(macd1[1]) + 1e-9)) * 50
        strength = min(1.0, strength)
        info = {'price_low': price2[1], 'price_prev_low': price1[1], 'index': price2[0], 'strength': strength}
        return True, strength, info
    if macd_fast_troughs and len(macd_fast_troughs) >= 2:
        macd_fast1, macd_fast2 = macd_fast_troughs[-2], macd_fast_troughs[-1]
        price1, price2 = price_troughs[-2], price_troughs[-1]
        if price2[1] < price1[1] and macd_fast2[1] > macd_fast1[1]:
            strength = abs((price2[1] - price1[1]) / price1[1]) * 30 + \
                       ((macd_fast2[1] - macd_fast1[1]) / (abs(macd_fast1[1]) + 1e-9)) * 30
            strength = min(0.8, strength)
            info = {'price_low': price2[1], 'price_prev_low': price1[1], 'index': price2[0], 'strength': strength, 'type': 'fast_macd'}
            return True, strength, info
    return False, 0.0, None


def detect_bearish_divergence(price_peaks, macd_peaks, macd_fast_peaks=None):
    if len(price_peaks) < 2 or len(macd_peaks) < 2:
        return False, 0.0, None
    price1, price2 = price_peaks[-2], price_peaks[-1]
    macd1, macd2 = macd_peaks[-2], macd_peaks[-1]
    if price2[1] > price1[1] and macd2[1] < macd1[1]:
        strength = abs((price2[1] - price1[1]) / price1[1]) * 50 + \
                   abs((macd2[1] - macd1[1]) / (abs(macd1[1]) + 1e-9)) * 50
        strength = min(1.0, strength)
        info = {'price_high': price2[1], 'price_prev_high': price1[1], 'index': price2[0], 'strength': strength}
        return True, strength, info
    if macd_fast_peaks and len(macd_fast_peaks) >= 2:
        macd_fast1, macd_fast2 = macd_fast_peaks[-2], macd_fast_peaks[-1]
        price1, price2 = price_peaks[-2], price_peaks[-1]
        if price2[1] > price1[1] and macd_fast2[1] < macd_fast1[1]:
            strength = abs((price2[1] - price1[1]) / price1[1]) * 30 + \
                       abs((macd_fast2[1] - macd_fast1[1]) / (abs(macd_fast1[1]) + 1e-9)) * 30
            strength = min(0.8, strength)
            info = {'price_high': price2[1], 'price_prev_high': price1[1], 'index': price2[0], 'strength': strength, 'type': 'fast_macd'}
            return True, strength, info
    return False, 0.0, None


def compute_divergence_strength(df: pd.DataFrame, window: int = 200) -> Tuple[bool, float, dict | None]:
    """在给定窗口内计算最近背驰（基于局部极值）并返回 (bullish, strength, info) 或 (bearish,...)
    返回示例： (True, 0.45, info)
    """
    close = df['close'].values
    if 'macd_hist' not in df.columns:
        return False, 0.0, None
    macd = df['macd_hist'].values
    macd_fast = df[f'macd_fast'] if f'macd_fast' in df.columns else None
    # 使用后 window 点
    data_slice = slice(max(0, len(close) - window), len(close))
    price_slice = close[data_slice]
    macd_slice = macd[data_slice]
    macd_fast_slice = macd_fast.values[data_slice] if macd_fast is not None else None
    price_peaks, price_troughs = find_local_extremes(price_slice, window=3)
    macd_peaks, macd_troughs = find_local_extremes(macd_slice, window=3)
    macd_fast_peaks, macd_fast_troughs = (find_local_extremes(macd_fast_slice, window=3) if macd_fast_slice is not None else ([], []))
    bullish, strength, info = detect_bullish_divergence(price_troughs, macd_troughs, macd_fast_troughs)
    if bullish:
        return True, strength, info
    bearish, strength, info = detect_bearish_divergence(price_peaks, macd_peaks, macd_fast_peaks)
    if bearish:
        return False, strength, info
    return False, 0.0, None
