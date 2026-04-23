import pandas as pd
from pathlib import Path


def _find_snapshot_for_base(snap_files, base):
    # prefer exact prefix match, fallback to startswith
    by_exact = [s for s in snap_files if s.name.startswith(base) and '_bi_snapshot_' in s.name]
    if by_exact:
        # pick the most specific (longest name)
        return sorted(by_exact, key=lambda p: len(p.name), reverse=True)[0]
    by_start = next((s for s in snap_files if s.name.startswith(base)), None)
    return by_start


def generate_reports(results_dir: Path, trade_files):
    results_dir = Path(results_dir)
    snap_files = list(results_dir.glob('*_bi_snapshot_*.csv'))

    out_dir = results_dir.parent.joinpath('consistency_reports')
    out_dir.mkdir(exist_ok=True)

    for tf_name in trade_files:
        tf_path = results_dir.joinpath(tf_name)
        if not tf_path.exists():
            print(f'skipping missing trade file: {tf_name}')
            continue

        # determine base prefix
        if '_trades_' in tf_name:
            base = tf_name.split('_trades_')[0]
        else:
            base = tf_name.rsplit('_', 4)[0]

        snap_path = _find_snapshot_for_base(snap_files, base)
        if snap_path is None:
            print(f'no snapshot found for {tf_name}, skipping')
            continue

        snap = pd.read_csv(snap_path)
        trades = pd.read_csv(tf_path)

        # build start_map and id_map
        start_map = None
        if 'start_index' in snap.columns:
            try:
                start_map = snap.set_index(snap['start_index'].astype(int))
            except Exception:
                try:
                    start_map = snap.set_index('start_index')
                except Exception:
                    start_map = None

        id_map = None
        for id_col in ('bi_id', 'id', 'bi'):
            if id_col in snap.columns:
                try:
                    id_map = snap.set_index(id_col)
                except Exception:
                    id_map = snap.set_index(snap[id_col])
                break

        rows = []
        default_delay = 2

        for _, t in trades.iterrows():
            anchor = None
            for k in ('entry_anchor_idx', 'entry_anchor', 'entry_anchor_id'):
                if k in t.index:
                    anchor = t.get(k)
                    break

            a_int = None
            if pd.notna(anchor):
                try:
                    if isinstance(anchor, float) and anchor.is_integer():
                        a_int = int(anchor)
                    else:
                        a_int = int(str(anchor))
                except Exception:
                    a_int = None

            bi = None
            if start_map is not None and a_int is not None:
                try:
                    bi = start_map.loc[a_int]
                except Exception:
                    try:
                        sub = start_map[start_map.index == a_int]
                        if hasattr(sub, 'iloc') and len(sub) == 1:
                            bi = sub.iloc[0]
                    except Exception:
                        bi = None

            if bi is None and id_map is not None and a_int is not None:
                try:
                    bi = id_map.loc[a_int]
                except Exception:
                    try:
                        sub = id_map[id_map.index == a_int]
                        if hasattr(sub, 'iloc') and len(sub) == 1:
                            bi = sub.iloc[0]
                    except Exception:
                        bi = None

            if bi is None:
                rows.append({
                    'trade_id': int(t.get('trade_id', -1)),
                    'entry_anchor': anchor,
                    'snapshot': snap_path.name,
                    'start_index': None,
                    'end_index': None,
                    'expected_entry': None,
                    'expected_exit': None,
                    'actual_entry': t.get('entry_idx'),
                    'actual_exit': t.get('exit_idx'),
                    'exit_reason': t.get('exit_reason'),
                    'stop_price': t.get('stop_price'),
                    'mismatch': 'anchor_not_found_in_snapshot'
                })
                continue

            try:
                if hasattr(bi, 'name') and not isinstance(bi, pd.DataFrame):
                    start_idx = int(bi.get('start_index'))
                    end_idx = int(bi.get('end_index'))
                else:
                    if isinstance(bi, pd.DataFrame) and len(bi) == 1:
                        row = bi.iloc[0]
                        start_idx = int(row.get('start_index'))
                        end_idx = int(row.get('end_index'))
                    else:
                        raise ValueError('ambiguous')
            except Exception:
                rows.append({
                    'trade_id': int(t.get('trade_id', -1)),
                    'entry_anchor': anchor,
                    'snapshot': snap_path.name,
                    'start_index': None,
                    'end_index': None,
                    'expected_entry': None,
                    'expected_exit': None,
                    'actual_entry': t.get('entry_idx'),
                    'actual_exit': t.get('exit_idx'),
                    'exit_reason': t.get('exit_reason'),
                    'stop_price': t.get('stop_price'),
                    'mismatch': 'snapshot_row_ambiguous'
                })
                continue

            expected_entry = start_idx + default_delay
            expected_exit = end_idx + default_delay

            actual_entry = t.get('entry_idx')
            actual_exit = t.get('exit_idx')

            exit_reason = str(t.get('exit_reason', '')).lower()
            is_stop_break = ('stop' in exit_reason) or ('break' in exit_reason)

            mismatch = None
            if actual_entry is None or actual_exit is None:
                mismatch = 'missing_entry_or_exit'
            else:
                try:
                    a_e = int(actual_entry)
                    a_x = int(actual_exit)
                except Exception:
                    a_e = None
                    a_x = None

                if a_e is None or a_x is None:
                    mismatch = 'invalid_entry_or_exit'
                else:
                    if a_e != expected_entry:
                        mismatch = f'entry_mismatch_expected_{expected_entry}_actual_{a_e}'
                    if is_stop_break:
                        if pd.isna(t.get('stop_price')):
                            mismatch = 'stop_exit_but_no_stop_price'
                        elif a_x > expected_exit:
                            mismatch = f'exit_after_expected_for_stop_expected_max_{expected_exit}_actual_{a_x}'
                    else:
                        if a_x != expected_exit:
                            mismatch = f'exit_mismatch_expected_{expected_exit}_actual_{a_x}'

            rows.append({
                'trade_id': int(t.get('trade_id', -1)),
                'entry_anchor': anchor,
                'snapshot': snap_path.name,
                'start_index': start_idx,
                'end_index': end_idx,
                'expected_entry': expected_entry,
                'expected_exit': expected_exit,
                'actual_entry': actual_entry,
                'actual_exit': actual_exit,
                'exit_reason': t.get('exit_reason'),
                'stop_price': t.get('stop_price'),
                'mismatch': mismatch
            })

        out_df = pd.DataFrame(rows)
        out_csv = out_dir.joinpath(f"{tf_name}.detailed.csv")
        out_json = out_dir.joinpath(f"{tf_name}.detailed.json")
        out_df.to_csv(out_csv, index=False)
        out_df.to_json(out_json, orient='records', indent=2)
        print(f'Wrote {out_csv} ({len(out_df)} rows)')


if __name__ == '__main__':
    # top failing files identified earlier
    top_files = [
        'PLTR_1h_trades_v7_bi.csv',
        'NVDA_1h_trades_v7_bi.csv',
        'SOXL_1h_trades_v7_bi.csv',
        'HOOD_1h_trades_v7_bi.csv',
        'SPY_1h_trades_v7_bi.csv',
    ]
    # results dir for v7
    results_dir = Path(__file__).parent.joinpath('..', 'user_strategy_v7_bi', 'results').resolve()
    generate_reports(results_dir, top_files)
