"""策略逻辑：复刻 v5 的开仓/加仓/减仓/移动止损 等行为（骨架实现）"""
from typing import Dict, Optional, List
import pandas as pd
from .config import get_default_config
from .indicators import add_macd, add_fast_macd, add_atr, add_rsi, compute_td9, compute_divergence_strength


class Position:
    def __init__(self, side: str, entry_price: float, size: float, stop_loss: Optional[float] = None):
        self.side = side
        # entries: list of dicts {price, size, stop_loss}
        self.entries = [{'price': float(entry_price), 'size': float(size), 'stop_loss': stop_loss}]
        self.stop_loss = stop_loss
        self.initial_size = float(size)
        self.open_index = None
        self.triggered_tps = set()
        self.highest_price = float(entry_price)
        self.lowest_price = float(entry_price)
        self.triggered_tps = set()
        self.initial_added = False

    def __repr__(self):
        return f"Position({self.side}, entries={self.entries}, stop={self.stop_loss})"

    def add_entry(self, price: float, size: float, stop_loss: Optional[float] = None):
        self.entries.append({'price': float(price), 'size': float(size), 'stop_loss': stop_loss})
        if stop_loss is not None:
            self.stop_loss = stop_loss
        # update peaks
        if float(price) > self.highest_price:
            self.highest_price = float(price)
        if float(price) < self.lowest_price:
            self.lowest_price = float(price)

    def total_size(self) -> float:
        return sum(e['size'] for e in self.entries)

    def avg_price(self) -> float:
        total = sum(e['price'] * e['size'] for e in self.entries)
        sz = self.total_size()
        return total / sz if sz else 0.0

    def remove_size(self, amount: float, exec_price: float) -> float:
        """Remove amount from entries (FIFO) and return realized pnl for that removal at exec_price."""
        remaining = amount
        realized_pnl = 0.0
        new_entries = []
        for e in self.entries:
            if remaining <= 0:
                new_entries.append(e)
                continue
            e_size = float(e['size'])
            if e_size <= remaining + 1e-12:
                # remove whole entry
                pnl = (exec_price - e['price']) * e_size if self.side == 'long' else (e['price'] - exec_price) * e_size
                realized_pnl += pnl
                remaining -= e_size
            else:
                # partially remove
                keep_size = e_size - remaining
                pnl = (exec_price - e['price']) * remaining if self.side == 'long' else (e['price'] - exec_price) * remaining
                realized_pnl += pnl
                new_entries.append({'price': e['price'], 'size': keep_size, 'stop_loss': e.get('stop_loss')})
                remaining = 0
        self.entries = new_entries
        # update stop_loss maybe left as is
        # update highest/lowest based on remaining entries
        if self.entries:
            prices = [e['price'] for e in self.entries]
            self.highest_price = max(prices)
            self.lowest_price = min(prices)
        else:
            self.highest_price = 0.0
            self.lowest_price = 0.0
        return float(realized_pnl)

    def close_all(self, exec_price: float) -> float:
        """Close entire position at exec_price; return pnl."""
        total = 0.0
        for e in self.entries:
            e_size = float(e['size'])
            pnl = (exec_price - e['price']) * e_size if self.side == 'long' else (e['price'] - exec_price) * e_size
            total += pnl
        self.entries = []
        return float(total)


class MACDTDStrategy:
    def __init__(self, config: Optional[Dict] = None):
        self.config = config or get_default_config()
        self.position: Optional[Position] = None
        # TP sequence taken from config.tp_ratios values order
        self.tp_sequence = list(self.config.get('tp_ratios', {}).values())

    def prepare_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        df = add_macd(df)
        df = add_fast_macd(df)
        df = add_atr(df)
        df = add_rsi(df)
        df = compute_td9(df)
        return df

    def generate_signals(self, df: pd.DataFrame, index: int) -> Dict:
        """返回信号字典：{'open_long': bool, 'open_short': bool, 'close': bool, 'stop_loss': float}|"""
        row = df.iloc[index]
        signals = {'open_long': False, 'open_short': False, 'close': False, 'stop_loss': None, 'info': None}
        # 计算背驰：返回 (is_bullish, strength, info) 或 (False, strength, info) 表示看跌/看涨
        # apply min_bars_before_action protection
        if index < int(self.config.get('min_bars_before_action', 1)):
            return signals

        if 'macd_hist' in df.columns:
            bullish, strength, info = compute_divergence_strength(df.iloc[:index+1])
            # basic buy filters
            rsi_ok = True
            if 'rsi' in row and not pd.isna(row['rsi']) and self.config.get('enable_buy_filter', True):
                rsi_ok = row['rsi'] >= float(self.config.get('buy_rsi_threshold', 40))
            # open long: bullish divergence, macd_hist>0, strength阈值, rsi filter
            if bullish and row['macd_hist'] > 0 and strength >= self.config.get('min_divergence_strength', 0.25) and rsi_ok:
                signals['open_long'] = True
                signals['stop_loss'] = float(row['low'] - row['atr'] * 1.5) if 'atr' in row and not pd.isna(row['atr']) else None
                signals['info'] = {'strength': strength, 'divergence_info': info}
            # fallback momentum: macd_hist positive and increasing
            if not signals['open_long']:
                if index > 0 and pd.notna(row.get('macd_hist')) and pd.notna(df.iloc[index-1].get('macd_hist')):
                    if row['macd_hist'] > 0 and row['macd_hist'] > df.iloc[index-1]['macd_hist'] and rsi_ok:
                        signals['open_long'] = True
                        signals['stop_loss'] = float(row['low'] - row.get('atr', 1.0) * 1.2) if 'atr' in row and not pd.isna(row['atr']) else None
                        signals['info'] = {'strength': max(0.1, float(strength or 0)), 'divergence_info': info, 'fallback': 'macd_momentum'}
            # open short: bearish divergence indicated by bullish==False
            if (not bullish) and row['macd_hist'] < 0 and strength >= self.config.get('min_divergence_strength', 0.25) and rsi_ok:
                signals['open_short'] = True
                signals['stop_loss'] = float(row['high'] + row['atr'] * 1.5) if 'atr' in row and not pd.isna(row['atr']) else None
                signals['info'] = {'strength': strength, 'divergence_info': info}
            # fallback for short
            if not signals['open_short']:
                if index > 0 and pd.notna(row.get('macd_hist')) and pd.notna(df.iloc[index-1].get('macd_hist')):
                    if row['macd_hist'] < 0 and row['macd_hist'] < df.iloc[index-1]['macd_hist'] and rsi_ok:
                        signals['open_short'] = True
                        signals['stop_loss'] = float(row['high'] + row.get('atr', 1.0) * 1.2) if 'atr' in row and not pd.isna(row['atr']) else None
                        signals['info'] = {'strength': max(0.1, float(strength or 0)), 'divergence_info': info, 'fallback': 'macd_momentum'}
        # 平仓条件（示例）
        # 平仓条件（示例：触达止损或TD反向信号）
        if self.position:
            price = row['close']
            # 止损触发
            if self.position.side == 'long' and self.position.stop_loss is not None and price <= self.position.stop_loss:
                signals['close'] = True
            if self.position.side == 'short' and self.position.stop_loss is not None and price >= self.position.stop_loss:
                signals['close'] = True
            # TD9 反向清仓（如果 compute_td9 已经计算且存在 td_setup_9 列）
            td_col = 'td_setup_9'
            if td_col in df.columns:
                td_val = df.iloc[index].get(td_col, 0)
                if self.position.side == 'long' and td_val == -1:
                    # partial or full close depending on tp thresholds - prefer full close as conservative default
                    signals['close'] = True
                if self.position.side == 'short' and td_val == 1:
                    signals['close'] = True
            # check for TP partial close triggers (1min/3min/5min)
            # Prefer small-timeframe signals passed in signals['info'] (populated by BacktestEngine) for accuracy
            info = signals.get('info') or {}
            for key in ['1min', '3min', '5min']:
                tdv = None
                if info and f'td_{key}' in info:
                    tdv = info.get(f'td_{key}')
                # fallback to column-based detection
                colname = f'td_setup_9_{key}'
                if tdv is None and colname in df.columns:
                    tdv = df.iloc[index].get(colname, None)
                if tdv is None:
                    continue
                pct = float(self.config.get('tp_ratios', {}).get(key, 0))
                if pct <= 0:
                    continue
                if self.position and key not in getattr(self.position, 'triggered_tps', set()):
                    if self.position.side == 'long' and tdv == -1:
                        signals['tp_close'] = {'pct': pct, 'source': key}
                    if self.position.side == 'short' and tdv == 1:
                        signals['tp_close'] = {'pct': pct, 'source': key}
            # Profit-based TP sequence (checks against avg_price)
            avg = None
            if self.position:
                avg = self.position.avg_price()
            if self.position and avg and avg > 0:
                if self.position.side == 'long':
                    profit = (row['close'] - avg) / avg
                else:
                    profit = (avg - row['close']) / avg
                tp_thresholds = self.config.get('tp_thresholds', [])
                tp_ratios = list(self.config.get('tp_ratios', {}).values())
                for k, (thr, pct) in enumerate(zip(tp_thresholds, tp_ratios)):
                    key = f'profit_tp_{k}'
                    if profit >= float(thr) and key not in getattr(self.position, 'triggered_tps', set()):
                        signals['tp_close'] = {'pct': float(pct), 'source': 'profit_threshold', 'key': key}
                        break
        return signals

    def calculate_position_size(self, price: float, stop_loss: float, balance: float, strength: float) -> float:
        """计算仓位，参考 v5 的 risk-based sizing
        risk_amount = balance * risk_per_trade
        size = position value / price
        """
        risk_per_trade = self.config.get('risk_per_trade', 0.02)
        risk_amount = balance * risk_per_trade
        stop_distance = abs(price - stop_loss)
        if stop_distance <= 0:
            return 0.0
        strength_multiplier = 0.5 + float(strength)
        # position value in USD
        position_value = risk_amount * strength_multiplier / (stop_distance / price)
        max_pct = 0.2
        max_size = (balance * max_pct) / price
        size = min(position_value / price, max_size)
        return round(float(size), 6)

    def should_scale_in(self, df: pd.DataFrame, index: int) -> bool:
        """Placeholder for initial add/scale-in logic based on TD9 or other signals"""
        # default: allow scaling in when compute_td9 on 15m shows 1 (buy) for long
        if self.position is None:
            return False
        # simple rule: if last td_setup_9 on recent row equals 1 and side long
        td_col = 'td_setup_9'
        if td_col in df.columns:
            td_val = df.iloc[index].get(td_col, 0)
            if self.position.side == 'long' and td_val == 1:
                return True
            if self.position.side == 'short' and td_val == -1:
                return True
        return False

    def get_tp_sequence(self) -> List[float]:
        return self.tp_sequence

    def on_fill(self, fill: Dict):
        # 更新 position 信息（简化）
        action = fill.get('action')
        if action == 'open':
            self.position = Position(fill['side'], float(fill['price']), float(fill['size']), stop_loss=fill.get('stop_loss'))
            # record index if provided
            if 'index' in fill:
                self.position.open_index = fill.get('index')
        elif action == 'add':
            if self.position is not None:
                self.position.add_entry(float(fill['price']), float(fill['size']), stop_loss=fill.get('stop_loss'))
                self.position.initial_added = True
        elif action == 'tp_close':
            # mark triggered tps and adjust internal record
            src = fill.get('source') or fill.get('key')
            if self.position is not None and src:
                self.position.triggered_tps.add(src)
        elif action == 'close':
            self.position = None

    def update_trailing_stop(self, position: Position, current_price: float, atr: float) -> bool:
        """Update trailing stop on Position object. Return True if updated."""
        updated = False
        if position.side == 'long':
            # update highest
            if current_price > position.highest_price:
                position.highest_price = float(current_price)
            trailing_atr_stop = position.highest_price - atr * float(self.config.get('trailing_stop_atr', 2.0))
            trailing_pct_stop = position.highest_price * (1 - float(self.config.get('trailing_stop_pct', 0.05)))
            new_stop = max(trailing_atr_stop, trailing_pct_stop)
            if position.stop_loss is None or new_stop > position.stop_loss:
                position.stop_loss = float(new_stop)
                updated = True
        else:
            if current_price < position.lowest_price:
                position.lowest_price = float(current_price)
            trailing_atr_stop = position.lowest_price + atr * float(self.config.get('trailing_stop_atr', 2.0))
            trailing_pct_stop = position.lowest_price * (1 + float(self.config.get('trailing_stop_pct', 0.05)))
            new_stop = min(trailing_atr_stop, trailing_pct_stop)
            if position.stop_loss is None or new_stop < position.stop_loss:
                position.stop_loss = float(new_stop)
                updated = True
        return updated
