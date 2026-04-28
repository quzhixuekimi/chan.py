"""Run module for v5 MACD+TD9 backtest - simple CLI entrypoint
Usage: python -m user_strategy_v5_macdtd.run_v5_macdtd
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd

from . import DataLoader
from .backtest_engine import BacktestEngine


TIMEFRAMES = ["1d", "4h", "2h", "1h"]
TIMEFRAME_LEVELS = {"1d": "1D", "4h": "4H", "2h": "2H", "1h": "1H"}


def save_df(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False, encoding="utf-8-sig")


def get_all_symbols(data_dir: Path) -> List[str]:
    symbols = set()
    for file in data_dir.glob("*.csv"):
        name = file.name
        parts = name.split("_")
        if parts:
            for i in range(len(parts)):
                if len(parts[i]) == 10 and parts[i].count("-") == 2:
                    symbols.add(parts[0])
                    break
            else:
                symbols.add(parts[0])
    return sorted(list(symbols))


def find_csv_for_timeframe(data_dir: Path, symbol: str, tf: str) -> Path:
    symbol = symbol.upper()
    # For 1d, look for symbol_*_1d.csv
    if tf == "1d":
        matches = list(data_dir.glob(f"{symbol}_*_1d.csv"))
        if matches:
            return matches[0]
    else:
        # For intraday, look for symbol_*_yf_*.csv
        matches = list(data_dir.glob(f"{symbol}_*_yf_{tf}_730d.csv"))
        if matches:
            return matches[0]
    return None


def format_time(v) -> str:
    if v is None or pd.isna(v):
        return ""
    try:
        ts = pd.to_datetime(v)
        if ts.hour == 0 and ts.minute == 0:
            return ts.strftime("%Y/%m/%d")
        return ts.strftime("%Y/%m/%d %H:%M")
    except:
        return str(v)


def build_signal_digest(events_df: pd.DataFrame, symbol: str, tf: str) -> pd.DataFrame:
    if events_df.empty:
        return pd.DataFrame()
    
    rows = []
    # Group by event_date and get latest event
    for event_date, grp in events_df.groupby('event_date', dropna=False):
        if pd.isna(event_date) or event_date == '':
            continue
        latest = grp.iloc[-1]
        rows.append({
            'symbol': symbol,
            'event_date': str(event_date)[:10],
            'timeframe': tf,
            'latest_event_type': latest.get('event_type', ''),
            'latest_event_time': latest.get('event_time', ''),
            'latest_price': latest.get('price', ''),
            'stop_price': latest.get('stop_price', ''),
            'reason': latest.get('reason', ''),
            'signal_text': latest.get('signal_text', ''),
            'event_count': len(grp),
        })
    return pd.DataFrame(rows)


def build_market_summary(all_trades: List[pd.DataFrame]) -> pd.DataFrame:
    if not all_trades:
        return pd.DataFrame()
    
    rows = []
    for tf in TIMEFRAMES:
        tf_trades = [t[t['timeframe'] == tf] for t in all_trades if not t.empty and 'timeframe' in t.columns]
        if not tf_trades:
            continue
        combined = pd.concat(tf_trades, ignore_index=True)
        if combined.empty:
            continue
        
        total = len(combined)
        wins = len(combined[combined['pnl_abs'].astype(float) > 0]) if 'pnl_abs' in combined.columns else 0
        losses = len(combined[combined['pnl_abs'].astype(float) < 0]) if 'pnl_abs' in combined.columns else 0
        
        pnl_pcts = combined['pnl_pct'].astype(float) if 'pnl_pct' in combined.columns else []
        avg_pnl = pnl_pcts.mean() if len(pnl_pcts) > 0 else 0
        
        win_rate = (wins / total * 100) if total > 0 else 0
        
        rows.append({
            'symbol': 'ALL',
            'timeframe': tf,
            'total_trades': total,
            'wins': wins,
            'losses': losses,
            'win_rate_pct': round(win_rate, 2),
            'avg_pnl_pct': round(avg_pnl, 2),
            'entry_rule': 'macd_divergence_td9',
            'exit_rule': 'td9_reverse_or_stop',
        })
    
    return pd.DataFrame(rows)


def build_market_all_summary(events_df: pd.DataFrame, trades_df: pd.DataFrame) -> pd.DataFrame:
    """Build per-symbol × timeframe market summary similar to v7 format.

    events_df: concatenated signal events across symbols/timeframes (must contain symbol,timeframe,event_type)
    trades_df: concatenated trades across symbols/timeframes (may be empty)
    """
    if events_df is None or events_df.empty:
        # If no events, fall back to empty DataFrame
        return pd.DataFrame()

    trades_combined = pd.DataFrame()
    if trades_df is not None and not trades_df.empty:
        trades_combined = trades_df.copy()

    rows = []
    symbols = sorted(events_df['symbol'].unique())
    for symbol in symbols:
        for tf in TIMEFRAMES:
            tf_ev = events_df[(events_df['symbol'] == symbol) & (events_df['timeframe'] == tf)]
            # count open signals as non-empty event_type
            if not tf_ev.empty and 'event_type' in tf_ev.columns:
                open_signals = int(tf_ev['event_type'].astype(str).str.strip().replace('nan', '').replace('None', '').apply(lambda x: 1 if x else 0).sum())
            else:
                open_signals = 0

            # trades
            total_trades = 0
            wins = 0
            avg_pnl = 0
            if not trades_combined.empty:
                tf_trades = trades_combined[(trades_combined['symbol'] == symbol) & (trades_combined['timeframe'] == tf)]
                total_trades = len(tf_trades)
                if total_trades > 0:
                    if 'pnl_abs' in tf_trades.columns:
                        wins = int((tf_trades['pnl_abs'].astype(float) > 0).sum())
                    else:
                        wins = 0
                    if 'pnl_pct' in tf_trades.columns:
                        try:
                            avg_pnl = float(tf_trades['pnl_pct'].astype(float).mean())
                        except Exception:
                            avg_pnl = 0
                    else:
                        avg_pnl = 0

            win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

            rows.append({
                'symbol': symbol,
                'timeframe': tf,
                'total_trades': total_trades,
                'open_signals': open_signals,
                'win_rate_pct': round(win_rate, 4),
                'avg_pnl_pct': round(avg_pnl, 4),
                'entry_rule': 'macd_divergence_td9',
                'exit_rule': 'td9_reverse_or_stop',
            })

    return pd.DataFrame(rows)


import json

def _safe_get_str(val) -> str:
    """Safely convert any value to string"""
    if val is None:
        return ''
    if isinstance(val, pd.Series):
        val = val.iloc[-1] if len(val) > 0 else ''
    if val is None:
        return ''
    try:
        if pd.isna(val):
            return ''
    except:
        pass
    return str(val).strip()


def build_symbol_digest(all_events: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    """Build digest for ONE symbol across all timeframes"""
    if not all_events:
        return pd.DataFrame()
    
    symbols = set()
    for ev in all_events.values():
        if not ev.empty and 'symbol' in ev.columns:
            symbols.update(ev['symbol'].unique())
    
    rows = []
    for symbol in sorted(symbols):
        item = {
            'symbol': symbol,
            'reference_date': '2026/04/28',
            'signal_date': '2026/04/28',
            'fresh_days': 10,
        }
        
        for tf in TIMEFRAMES:
            tf_key = f"{tf}_event_type"
            tf_key_text = f"{tf}_signal_text"
            tf_key_time = f"{tf}_event_time"
            tf_key_price = f"{tf}_latest_price"
            tf_key_stop = f"{tf}_stop_price"
            
            ev = all_events.get(f"{symbol}_{tf}")
            if ev is None or ev.empty:
                item[tf_key] = ''
                item[tf_key_text] = ''
                item[tf_key_time] = ''
                item[tf_key_price] = ''
                item[tf_key_stop] = ''
            else:
                latest = ev.iloc[-1] if len(ev) > 0 else None
                if latest is not None:
                    item[tf_key] = latest.get('event_type', '')
                    item[tf_key_text] = latest.get('signal_text', '')
                    item[tf_key_time] = latest.get('event_time', '')
                    item[tf_key_price] = latest.get('price', '')
                    item[tf_key_stop] = latest.get('stop_price', '')
                else:
                    item[tf_key] = ''
                    item[tf_key_text] = ''
                    item[tf_key_time] = ''
                    item[tf_key_price] = ''
                    item[tf_key_stop] = ''
        
        has_signal = False
        for tf in TIMEFRAMES:
            et = item.get(f"{tf}_event_type")
            if _safe_get_str(et):
                has_signal = True
                break
        item['has_signal'] = has_signal
        
        lines = []
        for tf in TIMEFRAMES:
            et = item.get(f"{tf}_event_type", "")
            st = item.get(f"{tf}_signal_text", "")
            tm = item.get(f"{tf}_event_time", "")
            pr = item.get(f"{tf}_latest_price", "")
            label = tf.upper()
            if et is not None and str(et) != 'nan' and str(et).strip():
                extras = []
                if st:
                    extras.append(st[:30])
                if tm:
                    extras.append(f"time={tm}")
                if pr:
                    extras.append(f"price={pr}")
                lines.append(f"{label}: {et}" + (f" ({', '.join(extras)})" if extras else ""))
            else: 
                lines.append(f"{label}: 无信号")
        
        item['summary_text'] = f"{symbol} | ref=2026/04/28 | fresh_days=10 | " + " ; ".join(lines)
        rows.append(item)
    
    return pd.DataFrame(rows)


def build_market_digest(market_events: List[pd.DataFrame]) -> pd.DataFrame:
    """Build market digest across all symbols and timeframes"""
    if not market_events:
        return pd.DataFrame()
    
    combined = pd.concat(market_events, ignore_index=True)
    if combined.empty:
        return pd.DataFrame()
    
    symbols = sorted(combined['symbol'].unique())
    rows = []
    
    for symbol in symbols:
        item = {
            'symbol': symbol,
            'reference_date': '2026/04/28',
            'signal_date': '2026/04/28',
            'fresh_days': 10,
        }
        
        sym_ev = combined[combined['symbol'] == symbol]
        
        for tf in TIMEFRAMES:
            tf_key = f"{tf}_event_type"
            tf_key_text = f"{tf}_signal_text"
            tf_key_time = f"{tf}_event_time"
            tf_key_price = f"{tf}_latest_price"
            tf_key_stop = f"{tf}_stop_price"
            
            tf_ev = sym_ev[sym_ev['timeframe'] == tf]
            if tf_ev.empty:
                item[tf_key] = ''
                item[tf_key_text] = ''
                item[tf_key_time] = ''
                item[tf_key_price] = ''
                item[tf_key_stop] = ''
            else:
                latest = tf_ev.iloc[-1]
                item[tf_key] = _safe_get_str(latest.get('event_type', ''))
                item[tf_key_text] = _safe_get_str(latest.get('signal_text', ''))
                item[tf_key_time] = _safe_get_str(latest.get('event_time', ''))
                item[tf_key_price] = _safe_get_str(latest.get('price', ''))
                item[tf_key_stop] = _safe_get_str(latest.get('stop_price', ''))
        
        has_signal = False
        for tf in TIMEFRAMES:
            et = _safe_get_str(item.get(f"{tf}_event_type"))
            if et:
                has_signal = True
                break
        item['has_signal'] = has_signal
        
        lines = []
        for tf in TIMEFRAMES:
            et = _safe_get_str(item.get(f"{tf}_event_type"))
            st = _safe_get_str(item.get(f"{tf}_signal_text"))
            tm = _safe_get_str(item.get(f"{tf}_event_time"))
            pr = _safe_get_str(item.get(f"{tf}_latest_price"))
            label = tf.upper()
            if et:
                extras = []
                if st:
                    extras.append(st[:30])
                if tm:
                    extras.append(f"time={tm}")
                if pr:
                    extras.append(f"price={pr}")
                lines.append(f"{label}: {et}" + (f" ({', '.join(extras)})" if extras else ""))
            else:
                lines.append(f"{label}: 无信号")
        
        item['summary_text'] = f"{symbol} | ref=2026/04/28 | fresh_days=10 | " + " ; ".join(lines)
        
        tf_payload = {}
        for tf in TIMEFRAMES:
            tf_key = f"{tf}_event_type"
            et = _safe_get_str(item.get(tf_key))
            if et:
                tf_payload[tf] = {
                    "event_type": et,
                    "signal_text": _safe_get_str(item.get(f"{tf}_signal_text")),
                    "event_time": _safe_get_str(item.get(f"{tf}_event_time")),
                    "latest_price": _safe_get_str(item.get(f"{tf}_latest_price")),
                    "stop_price": _safe_get_str(item.get(f"{tf}_stop_price")),
                    "is_fresh": True,
                    "age_days": 0,
                }
            else:
                tf_payload[tf] = {
                    "event_type": "",
                    "signal_text": "",
                    "event_time": "",
                    "latest_price": "",
                    "stop_price": "",
                    "is_fresh": False,
                    "age_days": 0,
                }
        item["summary_json"] = json.dumps(tf_payload, ensure_ascii=False)
        
        rows.append(item)
    
    return pd.DataFrame(rows)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--symbol', default=None)
    parser.add_argument('--limit', type=int, default=500)
    args = parser.parse_args()

    repo_root = Path(__file__).resolve().parent.parent
    data_dir = repo_root / "data_cache"
    out_dir = repo_root / "user_strategy_v5_macdtd" / "results"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Get all symbols if not specified
    symbols = [args.symbol.upper()] if args.symbol else get_all_symbols(data_dir)
    symbols = [str(s).upper().strip() for s in symbols if s]
    print(f"Processing {len(symbols)} symbols: {symbols}")

    # Store all results for market summary
    all_trades: List[pd.DataFrame] = []
    all_events: Dict[str, pd.DataFrame] = {}  # key: f"{symbol}_{tf}"
    market_signal_events = []
    
    for symbol in symbols:
        print("=" * 40, symbol, "=" * 40)
        
        symbol_events = []
        
        for tf in TIMEFRAMES:
            csv_path = find_csv_for_timeframe(data_dir, symbol, tf)
            csv_path = find_csv_for_timeframe(data_dir, symbol, tf)
            if not csv_path:
                print(f"  [{tf}] no data file found")
                continue
            
            print(f"  [{tf}] {csv_path.name}")
            
            try:
                dl = DataLoader('data_cache')
                df = dl.load(symbol, tf)
                df = df.iloc[-args.limit:]
                if df.empty:
                    print(f"  [{tf}] no data loaded")
                    continue
                
                be = BacktestEngine(df, symbol=symbol, timeframe=tf)
                be.run()
                
                # Trades
                trades_df = be.trades_df()
                if not trades_df.empty:
                    trades_df['symbol'] = symbol
                    trades_df['timeframe'] = tf
                    all_trades.append(trades_df.copy())
                    save_df(trades_df, out_dir / f"{symbol}_{tf}_trades_v5_macdtd.csv")
                
                # Signal events
                events_df = be.signal_events_df()
                if not events_df.empty:
                    events_df['symbol'] = symbol
                    events_df['timeframe'] = tf
                    # Format times properly
                    if 'event_time' in events_df.columns:
                        events_df['event_time'] = events_df['event_time'].apply(format_time)
                    if 'event_date' in events_df.columns:
                        events_df['event_date'] = events_df['event_date'].apply(lambda x: str(x)[:10] if x else '')
                    symbol_events.append(events_df.copy())
                    market_signal_events.append(events_df.copy())
                    all_events[f"{symbol}_{tf}"] = events_df.copy()
                    save_df(events_df, out_dir / f"{symbol}_{tf}_signal_events_v5_macdtd.csv")
                
                # Signal digest per timeframe
                digest_df = build_signal_digest(events_df, symbol, tf)
                if not digest_df.empty:
                    save_df(digest_df, out_dir / f"{symbol}_{tf}_signal_digest_v5_macdtd.csv")
                
                # OHLCV
                save_df(df, out_dir / f"{symbol}_{tf}_ohlcv_v5_macdtd.csv")
                
                print(f"  [{tf}] trades={len(trades_df)} events={len(events_df)}")
                
            except Exception as e:
                print(f"  [{tf}] error: {e}")
        
        # Symbol-level summary (all timeframes combined)
        if symbol_events:
            symbol_all = pd.concat(symbol_events, ignore_index=True)
            symbol_events_by_tf = {}
            for tf in TIMEFRAMES:
                tf_ev = symbol_all[symbol_all['timeframe'] == tf] if not symbol_all.empty else pd.DataFrame()
                symbol_events_by_tf[f"{symbol}_{tf}"] = tf_ev
            symbol_digest = build_symbol_digest(symbol_events_by_tf)
            if not symbol_digest.empty:
                save_df(symbol_digest, out_dir / f"{symbol}_signal_digest_last_per_symbol_v5_macdtd.csv")

    # Market-wide summary (legacy aggregated ALL)
    market_summary = build_market_summary(all_trades) if all_trades else pd.DataFrame()
    if not market_summary.empty:
        save_df(market_summary, out_dir / "market_all_summary_v5_macdtd.csv")
        print(f"\nMarket summary: {len(market_summary)} rows")
    
    # Market digest (last event per timeframe per symbol)
    print(f"DEBUG all_events len={len(all_events)}, market_signal_events len={len(market_signal_events)}")
    if market_signal_events:
        market_digest = build_market_digest(market_signal_events)
        print(f"DEBUG market_digest empty={market_digest.empty}")
        if not market_digest.empty:
            save_df(market_digest, out_dir / "market_signal_digest_last_per_symbol_v5_macdtd.csv")
            print(f"Market digest: {len(market_digest)} rows")
    # Build per-symbol×timeframe market_all_summary similar to v7
    try:
        print(f"DEBUG building market_all_summary from events ({len(market_signal_events)}) and trades ({len(all_trades)})")
        events_concat = pd.concat(market_signal_events, ignore_index=True) if market_signal_events else pd.DataFrame()
        trades_concat = pd.concat(all_trades, ignore_index=True) if all_trades else pd.DataFrame()
        # Fallbacks: try loading saved files from results directory if no in-memory events/trades
        if events_concat.empty:
            # 1) market_signal_digest_last_per_symbol
            fallback_path = out_dir / "market_signal_digest_last_per_symbol_v5_macdtd.csv"
            if fallback_path.exists():
                try:
                    events_concat = pd.read_csv(fallback_path)
                    print(f"Loaded fallback events from {fallback_path}")
                except Exception as e:
                    print(f"Failed to read fallback events file: {e}")
            # 2) individual per-symbol signal_events files
            if events_concat.empty:
                import glob
                ev_files = list(out_dir.glob("*_signal_events_v5_macdtd.csv"))
                if ev_files:
                    try:
                        parts = [pd.read_csv(p) for p in ev_files]
                        events_concat = pd.concat(parts, ignore_index=True)
                        print(f"Loaded {len(ev_files)} per-symbol event files")
                    except Exception as e:
                        print(f"Failed to concat per-symbol event files: {e}")

        if trades_concat.empty:
            # try loading trades files from results
            tr_files = list(out_dir.glob("*_trades_v5_macdtd.csv"))
            if tr_files:
                try:
                    parts = [pd.read_csv(p) for p in tr_files]
                    trades_concat = pd.concat(parts, ignore_index=True)
                    print(f"Loaded {len(tr_files)} per-symbol trade files")
                except Exception as e:
                    print(f"Failed to concat per-symbol trade files: {e}")

        market_all = build_market_all_summary(events_concat, trades_concat)
        if not market_all.empty:
            save_df(market_all, out_dir / "market_all_summary_v5_macdtd.csv")
            print(f"Market all summary: {len(market_all)} rows")
    except Exception as e:
        print(f"Failed to build market_all_summary: {e}")
    
    print(f"\nDone! Results in {out_dir}")


if __name__ == '__main__':
    main()
