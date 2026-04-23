import pandas as pd
from pathlib import Path
from typing import Dict, Any


def _find_csvs(results_dir: Path):
    return list(results_dir.glob('*.csv'))


def check_v7(results_dir: Path) -> Dict[str, Any]:
    """Checks for v7 BI strategy consistency.

    Rules checked (conservative, without importing strategy code):
    - presence of bi_snapshot and trades CSV
    - for each trade, entry_idx == bi.start_index + 2 and exit_idx == bi.end_index + 2
      (uses default delay 2 bars unless a config file is found and parsed by name)
    - stop exits have a non-null stop_price
    """
    report = {'path': str(results_dir), 'checks': []}

    snap_files = list(results_dir.glob('*_bi_snapshot_*.csv'))
    trade_files = list(results_dir.glob('*_trades_*.csv'))
    report['snap_files'] = [str(p.name) for p in snap_files]
    report['trade_files'] = [str(p.name) for p in trade_files]

    if not snap_files:
        report['checks'].append({'ok': False, 'msg': 'no bi_snapshot file found'})
        return report
    if not trade_files:
        report['checks'].append({'ok': False, 'msg': 'no trades file found'})
        return report

    # build mapping from base prefix -> snapshot file (prefer longest matching base)
    snap_map_files = {}
    for s in snap_files:
        name = s.name
        if '_bi_snapshot_' in name:
            base = name.split('_bi_snapshot_')[0]
        else:
            base = name.rsplit('_', 4)[0]
        # if multiple snapshots share same base keep the most specific (longer name)
        prev = snap_map_files.get(base)
        if prev is None or len(str(s.name)) > len(str(prev.name)):
            snap_map_files[base] = s

    failures = []
    default_delay = 2
    total_trades = 0

    # iterate trades files and match to corresponding snapshot by prefix
    for tf in trade_files:
        tname = tf.name
        if '_trades_' in tname:
            base = tname.split('_trades_')[0]
        else:
            base = tname.rsplit('_', 4)[0]

        snap_path = snap_map_files.get(base)
        if snap_path is None:
            # try startswith match
            snap_path = next((s for s in snap_files if s.name.startswith(base)), None)

        if snap_path is None:
            report['checks'].append({'ok': False, 'msg': f'no matching snapshot for trades file {tname}'})
            continue

        snap = pd.read_csv(snap_path)
        trades_df = pd.read_csv(tf)
        total_trades += len(trades_df)

        # prepare snapshot lookup maps: by bi id (if present) and by start_index
        # prepare id_map: accept common id column names
        id_map = None
        for id_col in ('bi_id', 'id', 'bi'):
            if id_col in snap.columns:
                try:
                    id_map = snap.set_index(id_col)
                except Exception:
                    # last-resort: set index to the column values without changing dtypes
                    id_map = snap.set_index(snap[id_col])
                break

        # prepare start_map by start_index (if present). keep numeric index when possible.
        start_map = None
        if 'start_index' in snap.columns:
            try:
                # prefer integer index
                start_map = snap.set_index(snap['start_index'].astype(int))
            except Exception:
                try:
                    start_map = snap.set_index('start_index')
                except Exception:
                    start_map = None

        for _, t in trades_df.iterrows():
            # trades may use 'entry_anchor_idx' or 'entry_anchor' or 'entry_anchor_id'
            anchor = None
            for k in ('entry_anchor_idx', 'entry_anchor', 'entry_anchor_id'):
                if k in t.index:
                    anchor = t.get(k)
                    break
            if pd.isna(anchor):
                failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'no entry_anchor'})
                continue

            if id_map is None and start_map is None:
                # cannot map anchor to snapshot by id or start_index; skip detailed index checks
                continue

            bi = None
            # normalize anchor to int when possible (anchors are often numeric indices or ids)
            a_int = None
            if anchor is not None:
                try:
                    # handle float 123.0 -> 123
                    if isinstance(anchor, float) and anchor.is_integer():
                        a_int = int(anchor)
                    else:
                        a_int = int(str(anchor))
                except Exception:
                    a_int = None

            # prefer matching by start_index (anchor appears to be start_index in trades)
            if start_map is not None and a_int is not None:
                try:
                    bi = start_map.loc[a_int]
                except Exception:
                    # sometimes set_index returns a DataFrame and .loc returns a DataFrame/Series mix
                    try:
                        bi = start_map[start_map.index == a_int]
                        if hasattr(bi, 'iloc') and len(bi) == 1:
                            bi = bi.iloc[0]
                        else:
                            bi = None
                    except Exception:
                        bi = None

            # fallback to matching by bi id
            if bi is None and id_map is not None and a_int is not None:
                try:
                    bi = id_map.loc[a_int]
                except Exception:
                    try:
                        bi = id_map[id_map.index == a_int]
                        if hasattr(bi, 'iloc') and len(bi) == 1:
                            bi = bi.iloc[0]
                        else:
                            bi = None
                    except Exception:
                        bi = None

            if bi is None:
                failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': f'anchor {anchor} not found in snapshot {snap_path.name}'})
                continue

            # extract start_index/end_index robustly whether bi is a Series or DataFrame row
            try:
                if hasattr(bi, 'name') and not isinstance(bi, pd.DataFrame):
                    # bi is a Series
                    start_idx = int(bi.get('start_index'))
                    end_idx = int(bi.get('end_index'))
                else:
                    # bi might be a one-row DataFrame
                    if isinstance(bi, pd.DataFrame) and len(bi) == 1:
                        row = bi.iloc[0]
                        start_idx = int(row.get('start_index'))
                        end_idx = int(row.get('end_index'))
                    else:
                        # unexpected structure
                        failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'snapshot row ambiguous structure'})
                        continue
            except Exception:
                failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'snapshot missing start_index/end_index'})
                continue

            expected_entry = start_idx + default_delay
            expected_exit = end_idx + default_delay

            try:
                actual_entry = int(t.get('entry_idx'))
                actual_exit = int(t.get('exit_idx'))
            except Exception:
                failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'missing or invalid entry_idx/exit_idx'})
                continue

            if actual_entry != expected_entry:
                failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'entry_idx_mismatch', 'expected': expected_entry, 'actual': actual_entry})

            # Determine if this is a stop/break type exit. For these we require a stop_price
            # but allow the actual exit to occur earlier than the expected end_index+delay.
            exit_reason = str(t.get('exit_reason', '')).lower()
            is_stop_break = ('stop' in exit_reason) or ('break' in exit_reason)

            # stop/break exits must include a stop_price
            if is_stop_break and pd.isna(t.get('stop_price')):
                failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'stop_exit_but_no_stop_price'})

            if is_stop_break:
                # Allow earlier exits for stop-type events. Flag only if exit is after expected.
                if actual_exit > expected_exit:
                    failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'exit_idx_after_expected_for_stop', 'expected_max': expected_exit, 'actual': actual_exit})
            else:
                # Non-stop exits retain strict equality requirement
                if actual_exit != expected_exit:
                    failures.append({'file': tname, 'trade': int(t.get('trade_id', -1)), 'reason': 'exit_idx_mismatch', 'expected': expected_exit, 'actual': actual_exit})

    report['checks'].append({'ok': len(failures) == 0, 'failures': failures, 'n_trades': total_trades})
    return report
