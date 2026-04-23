#!/usr/bin/env python3
"""Top-level runner for consistency checks across strategies.

This script locates strategy result directories and runs per-strategy checks.
It is read-only and will not modify your repo.
"""
import json
from pathlib import Path
import numpy as np
from consistency_check.v7_checks import check_v7
from consistency_check.v8_checks import check_v8
from consistency_check.v9_checks import check_v9
from consistency_check.v6_checks import check_v6

ROOT = Path(__file__).resolve().parent.parent


def find_strategy_dir(prefix):
    # e.g., prefix = 'v7_bi' -> directory user_strategy_v7_bi
    candidate = ROOT / f"user_strategy_{prefix}"
    if candidate.exists() and candidate.is_dir():
        results = candidate / 'results'
        if results.exists() and results.is_dir():
            return candidate
    # fallback: try any directory that starts with user_strategy_{prefix}
    for d in ROOT.iterdir():
        if d.is_dir() and d.name.startswith(f'user_strategy_{prefix}'):
            r = d / 'results'
            if r.exists() and r.is_dir():
                return d
    return None


def main():
    reports = {}

    # v6
    d6 = find_strategy_dir('v6_bspzs')
    if d6:
        reports['v6'] = check_v6(d6 / 'results')
    else:
        reports['v6'] = {'error': 'not found'}

    d7 = find_strategy_dir('v7_bi')
    if d7:
        v7_report = check_v7(d7 / 'results')
        reports['v7'] = v7_report
        # if v7 had failures, generate detailed per-trade reports and a summary
        try:
            checks = v7_report.get('checks', [])
            has_failures = any((not c.get('ok', True)) for c in checks)
        except Exception:
            has_failures = False

        if has_failures:
            try:
                # generate reports for the top failing files if generator exists
                gen = Path(__file__).parent.joinpath('generate_v7_detailed_reports.py')
                if gen.exists():
                    # call as script
                    import runpy
                    runpy.run_path(str(gen), run_name='__main__')
                # summarize
                summ = Path(__file__).parent.joinpath('summarize_v7_consistency_reports.py')
                if summ.exists():
                    runpy.run_path(str(summ), run_name='__main__')
            except Exception as e:
                reports['v7']['report_generation_error'] = str(e)
    else:
        reports['v7'] = {'error': 'not found'}

    d8 = find_strategy_dir('v8_byma')
    if d8:
        reports['v8'] = check_v8(d8 / 'results')
    else:
        reports['v8'] = {'error': 'not found'}

    d9 = find_strategy_dir('v9_mr')
    if d9:
        reports['v9'] = check_v9(d9 / 'results')
    else:
        reports['v9'] = {'error': 'not found'}

    out = ROOT / 'consistency_check' / 'last_report.json'

    def _default(o):
        # convert numpy types to native Python types for JSON
        if isinstance(o, (np.integer,)):
            return int(o)
        if isinstance(o, (np.floating,)):
            return float(o)
        if isinstance(o, (np.ndarray,)):
            return o.tolist()
        return str(o)

    text = json.dumps(reports, indent=2, default=_default)
    out.write_text(text)
    print(text)


if __name__ == '__main__':
    main()
