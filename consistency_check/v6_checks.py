import pandas as pd
from pathlib import Path
from typing import Dict, Any


def check_v6(results_dir: Path) -> Dict[str, Any]:
    """Checks for v6 bspzs outputs.

    Conservative checks:
    - presence of bsp and zs CSVs
    - signal digest/events exist
    - bsp rows have expected columns like 'type' or 'bsp_type'
    """
    report = {'path': str(results_dir), 'checks': []}
    bsps = list(results_dir.glob('*_bsp_*.csv'))
    zss = list(results_dir.glob('*_zs_*.csv'))
    signals = list(results_dir.glob('*signal*'))

    failures = []
    if not bsps:
        failures.append({'reason': 'no_bsp_files'})
    if not zss:
        failures.append({'reason': 'no_zs_files'})
    if not signals:
        failures.append({'reason': 'no_signal_files'})

    # spot-check bsp file columns
    if bsps:
        df = pd.read_csv(bsps[0])
        cols = set(df.columns.str.lower())
        # accept 'type', 'bsp_type', or 'types'
        if not ({'type', 'bsp_type', 'types'} & cols):
            failures.append({'reason': 'bsp_missing_type_column', 'columns': list(cols)})

    report['checks'].append({'ok': len(failures) == 0, 'failures': failures})
    return report
