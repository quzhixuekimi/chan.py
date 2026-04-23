import pandas as pd
from pathlib import Path
from typing import Dict, Any
import re


def check_v8(results_dir: Path) -> Dict[str, Any]:
    """Checks for v8 BYMA strategy consistency.

    Conservative checks:
    - trades file exists
    - each trade has entry_reason and exit_reason fields
    - stop exits include a stop_price
    - cycle_id present and consistent across trade_trace if available
    """
    report = {'path': str(results_dir), 'checks': []}
    trades = list(results_dir.glob('*_trades_*.csv'))
    traces = list(results_dir.glob('*_trade_trace_*.csv'))

    if not trades:
        report['checks'].append({'ok': False, 'msg': 'no trades file found'})
        return report

    failures = []

    trace_map = {}
    for tf in traces:
        match = re.match(r'(.+_trades_v8_byma)\.csv', tf.name.replace('_trade_trace_', '_trades_'))
        if match:
            prefix = match.group(1)
            trace_map[prefix] = tf

    for tfile in trades:
        df = pd.read_csv(tfile)
        match = re.match(r'(.+)_trades_v8_byma\.csv', tfile.name)
        if not match:
            continue
        prefix = match.group(1)
        trace_file = trace_map.get(prefix)
        if not trace_file:
            continue

        trace = pd.read_csv(trace_file)
        if 'entry_reason' not in df.columns or 'exit_reason' not in df.columns:
            failures.append({
                'file': tfile.name,
                'reason': 'missing_reason_columns',
                'columns': list(df.columns)
            })

        for _, r in df.iterrows():
            er = str(r.get('exit_reason', '')).lower()
            if 'stop' in er and pd.isna(r.get('stop_price')):
                failures.append({
                    'file': tfile.name,
                    'trade': int(r.get('trade_id', -1)),
                    'reason': 'stop_exit_but_no_stop_price'
                })

        if 'cycle_id' in df.columns and 'cycle_id' in trace.columns:
            trades_cycle_ids = set(df['cycle_id'].dropna().unique())
            trace_cycle_ids = set(trace['cycle_id'].dropna().unique())
            missing = trades_cycle_ids - trace_cycle_ids

            if missing:
                missing_ids = list(missing)
                referencing = df[df['cycle_id'].isin(missing_ids)][
                    ['trade_id', 'cycle_id']
                ].to_dict(orient='records')
                failures.append({
                    'file': tfile.name,
                    'reason': 'cycle_id_missing_in_trace',
                    'missing_cycle_ids': missing_ids,
                    'referencing_trades': referencing
                })

    total_trades = sum(len(pd.read_csv(t)) for t in trades)
    report['checks'].append({
        'ok': len(failures) == 0,
        'failures': failures,
        'n_trades': total_trades
    })
    return report
