"""指标验证脚本（不在此会话中执行）

用法示例（在本地运行）:
  python -m user_strategy_v5_macdtd.verify_indicators --cache-path ./data_cache --symbol ETHUSDT --timeframe 15m --rows 200

脚本会:
 - 从 data_cache 中选择匹配的 CSV
 - 用本包的 indicators 计算 macd, macd_fast, atr, rsi
 - 如果本机安装了 talib, 使用 talib 计算参考值
 - 输出每个指标的最大绝对误差与均方根误差
"""
import argparse
import numpy as np
import pandas as pd
import importlib
from .data_loader import DataLoader
from .indicators import add_macd, add_fast_macd, add_atr, add_rsi


def compare_series(a: pd.Series, b: pd.Series):
    # 对齐索引
    a, b = a.align(b, join='inner')
    diff = (a - b).dropna()
    if diff.empty:
        return {'max_abs': 0.0, 'rmse': 0.0, 'count': 0}
    return {'max_abs': float(diff.abs().max()), 'rmse': float(np.sqrt((diff**2).mean())), 'count': len(diff)}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--cache-path', required=True)
    parser.add_argument('--symbol', default='ETHUSDT')
    parser.add_argument('--timeframe', default='15m')
    parser.add_argument('--rows', type=int, default=500)
    args = parser.parse_args()

    loader = DataLoader(args.cache_path)
    df = loader.load(args.symbol, args.timeframe)
    if args.rows and len(df) > args.rows:
        df = df.tail(args.rows).copy()

    # 计算本实现指标
    df_local = df.copy()
    df_local = add_macd(df_local)
    df_local = add_fast_macd(df_local)
    df_local = add_atr(df_local)
    df_local = add_rsi(df_local)

    # 计算参考指标（如果 talib 可用）
    ref = None
    try:
        talib = importlib.import_module('talib')
    except Exception:
        talib = None

    if talib is not None:
        ref = df.copy()
        close = ref['close'].values
        ref_macd, ref_signal, ref_hist = talib.MACD(close, fastperiod=12, slowperiod=26, signalperiod=9)
        ref_macd_fast, _, _ = talib.MACD(close, fastperiod=8, slowperiod=17, signalperiod=6)
        ref_atr = talib.ATR(ref['high'].values, ref['low'].values, ref['close'].values, timeperiod=14)
        ref_rsi = talib.RSI(ref['close'].values, timeperiod=14)
        ref['macd_hist'] = pd.Series(ref_hist, index=ref.index)
        ref['macd_line'] = pd.Series(ref_macd, index=ref.index)
        ref['macd_signal'] = pd.Series(ref_signal, index=ref.index)
        ref['macd_fast'] = pd.Series(ref_macd_fast, index=ref.index)
        ref['atr'] = pd.Series(ref_atr, index=ref.index)
        ref['rsi'] = pd.Series(ref_rsi, index=ref.index)
    else:
        # talib 不可用：使用内部纯 pandas 实现作为参考（这会与本包的实现本质上相同），
        # 但仍能提供对 NaN/类型/对齐的验证与数值检查
        ref = df.copy()
        # macd (12,26,9)
        close = ref['close']
        ema_fast = close.ewm(span=12, adjust=False).mean()
        ema_slow = close.ewm(span=26, adjust=False).mean()
        macd_line = ema_fast - ema_slow
        macd_signal = macd_line.ewm(span=9, adjust=False).mean()
        macd_hist = macd_line - macd_signal
        ref['macd_line'] = macd_line
        ref['macd_signal'] = macd_signal
        ref['macd_hist'] = macd_hist
        # fast macd (8,17,6)
        ema_f = close.ewm(span=8, adjust=False).mean()
        ema_s = close.ewm(span=17, adjust=False).mean()
        ref['macd_fast'] = ema_f - ema_s
        # atr (14)
        prev_close = ref['close'].shift(1)
        tr = pd.concat([ref['high'] - ref['low'], (ref['high'] - prev_close).abs(), (ref['low'] - prev_close).abs()], axis=1).max(axis=1)
        ref['atr'] = tr.ewm(alpha=1/14, adjust=False).mean()
        # rsi (14)
        delta = ref['close'].diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ma_up = up.ewm(alpha=1/14, adjust=False).mean()
        ma_down = down.ewm(alpha=1/14, adjust=False).mean()
        rs = ma_up / (ma_down + 1e-9)
        ref['rsi'] = 100 - (100 / (1 + rs))

    checks = [
        ('macd_hist', df_local['macd_hist'], ref['macd_hist']),
        ('macd_line', df_local['macd_line'], ref['macd_line']),
        ('macd_signal', df_local['macd_signal'], ref['macd_signal']),
        ('macd_fast', df_local.get('macd_fast', pd.Series(dtype=float)), ref['macd_fast']),
        ('atr', df_local['atr'], ref['atr']),
        ('rsi', df_local['rsi'], ref['rsi']),
    ]

    for name, a, b in checks:
        res = compare_series(pd.Series(a), pd.Series(b))
        print(f"{name}: count={res['count']}, max_abs={res['max_abs']:.6f}, rmse={res['rmse']:.6f}")


if __name__ == '__main__':
    main()
