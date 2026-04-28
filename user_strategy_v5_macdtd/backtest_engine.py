"""简化回测引擎：执行生成信号、成交模型（手续费/滑点）、统计与报表"""
from typing import List, Dict, Optional
import pandas as pd
import csv
import math
from .strategy import MACDTDStrategy, Position


class BacktestEngine:
    def __init__(self, df: pd.DataFrame, df_1m: pd.DataFrame = None, df_3m: pd.DataFrame = None, df_5m: pd.DataFrame = None, strategy: Optional[MACDTDStrategy] = None, fee=0.0005, slippage=0.0002, initial_balance: float = 100000.0, symbol: str = "UNKNOWN", timeframe: str = "1d"):
        self._time_index = df.index.copy()
        self.df = df.copy().reset_index(drop=True)
        self.df_1m = df_1m.copy().reset_index(drop=True) if df_1m is not None else None
        self.df_3m = df_3m.copy().reset_index(drop=True) if df_3m is not None else None
        self.df_5m = df_5m.copy().reset_index(drop=True) if df_5m is not None else None
        self.strategy = strategy or MACDTDStrategy()
        self.fee = fee
        self.slippage = slippage
        self.cash = float(initial_balance)
        self.initial_balance = float(initial_balance)
        self.position: Optional[Position] = None
        self.trades: List[Dict] = []
        self.symbol = symbol
        self.timeframe = timeframe
        self.events: List[Dict] = []

    def run(self):
        df = self.strategy.prepare_indicators(self.df.copy())
        df1 = self.strategy.prepare_indicators(self.df_1m.copy()) if self.df_1m is not None else None
        df3 = self.strategy.prepare_indicators(self.df_3m.copy()) if self.df_3m is not None else None
        df5 = self.strategy.prepare_indicators(self.df_5m.copy()) if self.df_5m is not None else None
        balance = self.cash
        equity_curve = []
        for i in range(len(df)):
            signals = self.strategy.generate_signals(df, i)
            price = float(df.iloc[i]['close'])
            # attach small timeframe info into signals if available
            if df1 is not None and idx1 is not None:
                signals.setdefault('info', {})['td_1m'] = df1.iloc[idx1].get('td_setup_9') if 'td_setup_9' in df1.columns else None
            if df3 is not None and idx3 is not None:
                signals.setdefault('info', {})['td_3m'] = df3.iloc[idx3].get('td_setup_9') if 'td_setup_9' in df3.columns else None
            if df5 is not None and idx5 is not None:
                signals.setdefault('info', {})['td_5m'] = df5.iloc[idx5].get('td_setup_9') if 'td_setup_9' in df5.columns else None
            time_i = df.iloc[i].get('time') or df.iloc[i].get('time_beijing') if 'time_beijing' in df.columns else None
            # find nearest indices in small timeframes
            idx1 = idx3 = idx5 = None
            if df1 is not None and time_i is not None:
                idx1 = (df1['time'] - time_i).abs().idxmin()
            if df3 is not None and time_i is not None:
                idx3 = (df3['time'] - time_i).abs().idxmin()
            if df5 is not None and time_i is not None:
                idx5 = (df5['time'] - time_i).abs().idxmin()
            # open long
            if signals.get('open_long') and self.position is None:
                stop = signals.get('stop_loss')
                size = self.strategy.calculate_position_size(price, stop, balance, signals.get('info', {}).get('strength', 0.5))
                if size > 0:
                    exec_price = price * (1 + self.slippage)
                    cost = exec_price * size * (1 + self.fee)
                    balance -= cost
                    self.position = Position('long', exec_price, size, stop_loss=stop)
                    # notify strategy
                    try:
                        self.strategy.on_fill({'action': 'open', 'side': 'long', 'price': exec_price, 'size': size, 'stop_loss': stop, 'index': i})
                    except Exception:
                        pass
                    self.trades.append({'action': 'open_long', 'index': i, 'price': exec_price, 'size': size, 'cash': balance})
                    self._record_event('BUY_SIGNAL', i, exec_price, stop, 'macd_td9_buy')
            # scale-in
            if signals.get('open_long') and self.position is not None and self.strategy.should_scale_in(df, i):
                # attempt scale in
                stop = signals.get('stop_loss')
                size = self.strategy.calculate_position_size(price, stop, balance, signals.get('info', {}).get('strength', 0.5))
                if size > 0:
                    exec_price = price * (1 + self.slippage)
                    cost = exec_price * size * (1 + self.fee)
                    balance -= cost
                    self.position.add_entry(exec_price, size, stop_loss=stop)
                    try:
                        self.strategy.on_fill({'action': 'add', 'side': 'long', 'price': exec_price, 'size': size, 'stop_loss': stop, 'index': i})
                    except Exception:
                        pass
                    self.trades.append({'action': 'add', 'index': i, 'price': exec_price, 'size': size, 'cash': balance})
                    self._record_event('ADD_POSITION', i, exec_price, stop, 'scale_in')
            # close (if any)
            if signals.get('close') and self.position is not None:
                exec_price = price * (1 - self.slippage)
                closed_size = self.position.total_size()
                pnl = self.position.close_all(exec_price)
                fee_amount = exec_price * closed_size * self.fee
                balance += exec_price * closed_size - fee_amount
                self.trades.append({'action': 'close', 'index': i, 'price': exec_price, 'size': closed_size, 'pnl': pnl, 'cash': balance})
                # notify strategy
                try:
                    self.strategy.on_fill({'action': 'close', 'price': exec_price, 'size': closed_size, 'pnl': pnl, 'index': i})
                except Exception:
                    pass
                # clear position
                self.position = None
                self._record_event('SELL_SIGNAL', i, exec_price, 0.0, 'macd_td9_sell')
                
            # partial close by TP triggers (support from signals dict)
            # support partial closes triggered by small-timeframe TD9 using available dfs
            if self.position is not None:
                # update trailing stop if atr present
                atr = df.iloc[i].get('atr') if 'atr' in df.columns else None
                if atr is not None:
                    updated = self.strategy.update_trailing_stop(self.position, price, float(atr))
                    if updated:
                        self.trades.append({'action': 'update_stop', 'index': i, 'price': price, 'stop': self.position.stop_loss, 'cash': balance})
                        self._record_event('STOP_UPDATE', i, price, self.position.stop_loss, 'trailing_stop')

                # initial add based on 15m td if available
                td15 = df.iloc[i].get('td_setup_9') if 'td_setup_9' in df.columns else None
                if td15 is not None and not getattr(self.position, 'initial_added', False):
                    if self.position.side == 'long' and td15 == 1:
                        add_size = self.position.initial_size * float(self.strategy.config.get('initial_add_size', 0.3))
                        exec_price = price * (1 + self.slippage)
                        cost = exec_price * add_size * (1 + self.fee)
                        if cost <= balance:
                            balance -= cost
                            self.position.add_entry(exec_price, add_size)
                            self.position.initial_added = True
                            self.trades.append({'action': 'initial_add', 'index': i, 'price': exec_price, 'size': add_size, 'cash': balance})
                            self._record_event('ADD_POSITION', i, exec_price, 0.0, 'initial_add_long')
                    if self.position.side == 'short' and td15 == -1:
                        add_size = self.position.initial_size * float(self.strategy.config.get('initial_add_size', 0.3))
                        exec_price = price * (1 - self.slippage)
                        cost = exec_price * add_size * (1 + self.fee)
                        if cost <= balance:
                            balance -= cost
                            self.position.add_entry(exec_price, add_size)
                            self.position.initial_added = True
                            self.trades.append({'action': 'initial_add', 'index': i, 'price': exec_price, 'size': add_size, 'cash': balance})

                # small timeframe TP triggers
                tp_map = [('1min', df1, idx1), ('3min', df3, idx3), ('5min', df5, idx5)]
                for key, dsmall, idx_small in tp_map:
                    if dsmall is None or idx_small is None:
                        continue
                    tdv = dsmall.iloc[idx_small].get('td_setup_9') if 'td_setup_9' in dsmall.columns else None
                    if tdv is None:
                        continue
                    if self.position.side == 'long' and tdv == -1 and key not in getattr(self.position, 'triggered_tps', set()):
                        pct = float(self.strategy.config.get('tp_ratios', {}).get(key, 0))
                        if pct > 0 and self.position.total_size() > 0:
                            close_amount = self.position.total_size() * pct
                            exec_price = price * (1 - self.slippage)
                            pnl = self.position.remove_size(close_amount, exec_price)
                            fee_amount = exec_price * close_amount * self.fee
                            balance += exec_price * close_amount - fee_amount
                            self.position.triggered_tps.add(key)
                            self.trades.append({'action': 'tp_close', 'index': i, 'price': exec_price, 'size': close_amount, 'pnl': pnl, 'cash': balance, 'pct': pct, 'source': key})
                            self._record_event('TAKE_PROFIT', i, exec_price, 0.0, f'tp_{key}')
                            try:
                                self.strategy.on_fill({'action': 'tp_close', 'price': exec_price, 'size': close_amount, 'pnl': pnl, 'source': key, 'index': i})
                            except Exception:
                                pass
                    if self.position.side == 'short' and tdv == 1 and key not in getattr(self.position, 'triggered_tps', set()):
                        pct = float(self.strategy.config.get('tp_ratios', {}).get(key, 0))
                        if pct > 0 and self.position.total_size() > 0:
                            close_amount = self.position.total_size() * pct
                            exec_price = price * (1 + self.slippage)
                            pnl = self.position.remove_size(close_amount, exec_price)
                            fee_amount = exec_price * close_amount * self.fee
                            balance += exec_price * close_amount - fee_amount
                            self.position.triggered_tps.add(key)
                            self.trades.append({'action': 'tp_close', 'index': i, 'price': exec_price, 'size': close_amount, 'pnl': pnl, 'cash': balance, 'pct': pct, 'source': key})
                            self._record_event('TAKE_PROFIT', i, exec_price, 0.0, f'tp_{key}_short')

            equity_curve.append(balance)

        self.cash = balance
        self.equity_curve = equity_curve
        # post-run: ensure numeric types
        self.cash = float(self.cash)

    def _record_event(self, event_type: str, idx: int, price: float, stop_price: float, reason: str) -> None:
        time_val = None
        if idx < len(self.df):
            time_val = self.df.iloc[idx].get('time') or self.df.iloc[idx].get('time_beijing')
        event = {
            'symbol': self.symbol,
            'event_date': time_val,
            'timeframe': self.timeframe,
            'latest_event_type': event_type,
            'latest_event_time': idx,
            'latest_price': price,
            'stop_price': stop_price,
            'reason': reason,
            'signal_text': f"{event_type} @ {price:.4f}",
        }
        self.events.append(event)

    def _get_row_time(self, idx: int) -> str:
        if idx < len(self._time_index):
            t = self._time_index[idx]
            if t is not None:
                dt = pd.Timestamp(t)
                if dt.hour == 0 and dt.minute == 0:
                    return dt.strftime('%Y/%m/%d')
                return dt.strftime('%Y/%m/%d %H:%M')
        return ""

    def trades_df(self) -> pd.DataFrame:
        if not self.trades:
            return pd.DataFrame()
        
        # Build v7-compatible trades from trade log
        trade_rows = []
        trade_id = 0
        open_trade = None
        
        for t in self.trades:
            action = t.get('action', '')
            idx = t.get('index')
            price = t.get('price')
            pnl = t.get('pnl', 0)
            
            if action == 'open_long':
                trade_id += 1
                open_trade = {
                    'trade_id': trade_id,
                    'symbol': self.symbol,
                    'timeframe': self.timeframe,
                    'structure_type': 'macd_td9',
                    'direction': 'LONG',
                    'entry_anchor_idx': idx,
                    'entry_anchor_time': self._get_row_time(idx),
                    'entry_anchor_price': price,
                    'entry_idx': idx,
                    'entry_time': self._get_row_time(idx),
                    'entry_price': price,
                    'stop_price': t.get('stop'),
                }
            elif action == 'close' and open_trade:
                open_trade['exit_idx'] = idx
                open_trade['exit_time'] = self._get_row_time(idx)
                open_trade['exit_price'] = price
                open_trade['exit_reason'] = 'signal_close'
                open_trade['pnl_abs'] = pnl
                open_trade['pnl_pct'] = (pnl / (open_trade['entry_price'] * open_trade.get('size', 0) or 1)) * 100 if open_trade.get('size') else 0
                trade_rows.append(open_trade.copy())
                open_trade = None
            elif action == 'tp_close' and open_trade:
                open_trade['exit_idx'] = idx
                open_trade['exit_time'] = self._get_row_time(idx)
                open_trade['exit_price'] = price
                open_trade['exit_reason'] = t.get('source', 'tp')
                open_trade['pnl_abs'] = pnl
                open_trade['pnl_pct'] = (pnl / (open_trade['entry_price'] * open_trade.get('size', 0) or 1)) * 100 if open_trade.get('size') else 0
                trade_rows.append(open_trade.copy())
                open_trade = None
        
        df = pd.DataFrame(trade_rows)
        
        # Ensure all v7 columns exist
        v7_cols = ['trade_id', 'symbol', 'timeframe', 'structure_type', 'direction',
                   'entry_anchor_idx', 'entry_anchor_time', 'entry_anchor_price',
                   'entry_idx', 'entry_time', 'entry_price', 'stop_price',
                   'exit_idx', 'exit_time', 'exit_price', 'exit_reason',
                   'pnl_abs', 'pnl_pct']
        for col in v7_cols:
            if col not in df.columns:
                df[col] = ''
        return df[v7_cols]

    def signal_events_df(self) -> pd.DataFrame:
        if not self.events:
            return pd.DataFrame()
        
        df = pd.DataFrame(self.events).copy()
        
        # Rename FIRST (before v7_cols loop) to avoid duplicate columns
        rename = {'latest_event_type': 'event_type'}
        df = df.rename(columns=rename)

        # Add missing columns for v7 compatibility
        df['event_seq'] = range(1, len(df) + 1)
        df['bar_index'] = df.get('latest_event_time', '')
        df['price'] = df.get('latest_price', '')
        df['stop_price'] = df.get('stop_price', '')
        df['trigger_price_ref'] = ''
        df['summary_text'] = df.get('signal_text', '')
        df['trade_id'] = ''
        df['bi_id'] = ''
        df['bi_direction'] = ''

        # Format event_date
        def fmt_date(v):
            if pd.isna(v) or v is None:
                return ''
            try:
                return str(v)[:10]
            except:
                return ''

        df['event_date'] = df['event_date'].apply(fmt_date)
        df['event_time'] = df['latest_event_time'].apply(lambda x: self._get_row_time(x) if isinstance(x, int) else str(x))

        v7_cols = ['event_seq', 'symbol', 'timeframe', 'event_type', 'event_time', 'event_date',
                  'bar_index', 'price', 'stop_price', 'planned_exit_idx', 'planned_exit_time',
                  'trigger_price_ref', 'reason', 'signal_text', 'summary_text', 'trade_id',
                  'structure_type', 'bi_id', 'bi_direction']

        for col in v7_cols:
            if col not in df.columns:
                df[col] = ''

        return df

    def results(self) -> Dict:
        pnl = self.cash - 100000.0
        return {'final_cash': self.cash, 'pnl': pnl, 'trades': self.trades, 'equity_curve': getattr(self, 'equity_curve', [])}

    def export_trades_csv(self, path: str) -> None:
        """Export executed trades to CSV path."""
        if not self.trades:
            return
        keys = sorted({k for t in self.trades for k in t.keys()})
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys)
            writer.writeheader()
            for t in self.trades:
                writer.writerow({k: t.get(k, '') for k in keys})

    def export_events_csv(self, path: str) -> None:
        """Export signal digest events to CSV."""
        if not self.events:
            return
        keys = ['symbol', 'event_date', 'timeframe', 'latest_event_type', 'latest_event_time', 'latest_price', 'stop_price', 'reason', 'signal_text']
        with open(path, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=keys, extrasaction='ignore')
            writer.writeheader()
            for e in self.events:
                writer.writerow(e)

    def compute_metrics(self) -> Dict:
        """Compute simple backtest metrics from equity_curve and trades."""
        eq = getattr(self, 'equity_curve', None)
        if not eq:
            return {}
        returns = []
        for i in range(1, len(eq)):
            prev = eq[i-1]
            if prev == 0:
                returns.append(0.0)
            else:
                returns.append((eq[i] - prev) / prev)

        # max drawdown
        peak = -math.inf
        max_dd = 0.0
        for v in eq:
            if v > peak:
                peak = v
            dd = (peak - v) / peak if peak and peak > 0 else 0.0
            if dd > max_dd:
                max_dd = dd

        wins = 0
        losses = 0
        total_pl = 0.0
        for t in self.trades:
            if 'pnl' in t:
                total_pl += float(t['pnl'])
                if float(t['pnl']) > 0:
                    wins += 1
                elif float(t['pnl']) < 0:
                    losses += 1

        total_trades = wins + losses
        win_rate = (wins / total_trades) if total_trades > 0 else None

        return {
            'final_cash': self.cash,
            'pnl': self.cash - float(self.initial_balance),
            'total_trades': total_trades,
            'wins': wins,
            'losses': losses,
            'win_rate': win_rate,
            'max_drawdown': max_dd,
        }
