import pytest
import pandas as pd
from user_strategy_v5_macdtd.strategy import MACDTDStrategy, Position
from user_strategy_v5_macdtd.backtest_engine import BacktestEngine


def make_kline_sequence(prices):
    # create a simple dataframe with open/high/low/close and atr placeholder
    rows = []
    for p in prices:
        rows.append({'open': p, 'high': p, 'low': p, 'close': p, 'atr': 1.0})
    return pd.DataFrame(rows)


def test_open_and_close_simple():
    # simple up move triggers open_long (via divergence stub) - we simulate by monkeypatch
    df = make_kline_sequence([100, 102, 104, 103, 105, 107])
    strat = MACDTDStrategy()
    # force a synthetic signal by patching compute_divergence_strength via strategy
    # We'll just call generate_signals and manually set open_long True
    be = BacktestEngine(df)
    # inject a fake strategy that opens on first bar
    def fake_gen(df_, idx):
        s = {'open_long': False, 'open_short': False, 'close': False, 'stop_loss': None, 'info': None}
        if idx == 1:
            s['open_long'] = True
            s['stop_loss'] = 90
        if idx == 4:
            s['close'] = True
        return s

    be.strategy.generate_signals = fake_gen
    be.run()
    res = be.results()
    assert res['final_cash'] is not None
    assert isinstance(res['trades'], list)
    # ensure at least one open and one close recorded
    actions = [t['action'] for t in res['trades']]
    assert 'open_long' in actions or 'open_short' in actions
    assert 'close' in actions or any(a.startswith('tp_close') for a in actions)
