from user_strategy_v5_macdtd.backtest_engine import BacktestEngine
from user_strategy_v5_macdtd.strategy import MACDTDStrategy, Position
from user_strategy_v5_macdtd import DataLoader
import pandas as pd


def test_tp_close_flow():
    # load small synthetic dataset
    prices = [100, 101, 102, 103, 104, 105]
    df = pd.DataFrame([{'open':p,'high':p,'low':p,'close':p,'atr':1.0} for p in prices])
    be = BacktestEngine(df)

    # fake generator: open at idx 1, trigger tp at idx 3
    def fake_gen(df_, idx):
        s = {'open_long': False, 'open_short': False, 'close': False, 'stop_loss': None, 'info': None}
        if idx == 1:
            s['open_long'] = True
            s['stop_loss'] = 98
        if idx == 3:
            s['tp_close'] = {'pct': 0.5, 'source': 'test'}
        return s

    be.strategy.generate_signals = fake_gen
    be.run()
    actions = [t['action'] for t in be.trades]
    assert 'open_long' in actions, 'expected open_long'
    assert any(a == 'tp_close' for a in actions), f'expected tp_close, got {actions}'


def test_trailing_stop_update():
    strat = MACDTDStrategy()
    pos = Position('long', 100.0, 10.0, stop_loss=95.0)
    # simulate price moved up
    updated = strat.update_trailing_stop(pos, current_price=110.0, atr=2.0)
    assert updated is True
    assert pos.stop_loss > 95.0


if __name__ == '__main__':
    print('running test_tp_close_flow')
    test_tp_close_flow()
    print('tp_close_flow passed')
    print('running test_trailing_stop_update')
    test_trailing_stop_update()
    print('trailing_stop_update passed')
