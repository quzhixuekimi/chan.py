import pandas as pd
from pathlib import Path
from typing import Dict, Any


def check_v9(results_dir: Path) -> Dict[str, Any]:
    """Checks for v9 MR strategy consistency.

    Conservative checks:
    - trades exist and contain expected stop/exit reason naming
    - stop exits have a stop_price
    - entry with next_day_trigger should have entry_anchor_idx present
    """
    report = {'path': str(results_dir), 'checks': []}
    trades = list(results_dir.glob('*_trades_*.csv'))
    if not trades:
        report['checks'].append({'ok': False, 'msg': 'no trades file found'})
        return report

    df = pd.read_csv(trades[0])
    failures = []

    for _, r in df.iterrows():
        er = str(r.get('exit_reason', ''))
        if 'stop' in er.lower() and pd.isna(r.get('stop_price')):
            failures.append({'trade': int(r.get('trade_id', -1)), 'reason': 'stop_exit_but_no_stop_price'})

        # next_day_trigger: expect an anchor idx
        if 'entry_anchor_idx' not in r or pd.isna(r.get('entry_anchor_idx')):
            # only flag if entry_reason suggests next-day entry
            if 'entry' in str(r.get('entry_reason', '')).lower():
                failures.append({'trade': int(r.get('trade_id', -1)), 'reason': 'entry_without_anchor'})

    report['checks'].append({'ok': len(failures) == 0, 'failures': failures, 'n_trades': len(df)})
    return report
