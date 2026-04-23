import json
from pathlib import Path
import pandas as pd


def summarize_reports(reports_dir: Path):
    reports_dir = Path(reports_dir)
    if not reports_dir.exists():
        raise FileNotFoundError(f"reports dir not found: {reports_dir}")

    csvs = list(reports_dir.glob('*.detailed.csv'))
    if not csvs:
        return {'error': 'no detailed csv reports found', 'reports_dir': str(reports_dir)}

    mismatch_counts = {}
    total_rows = 0
    by_file = {}

    for p in csvs:
        df = pd.read_csv(p)
        total_rows += len(df)
        counts = df['mismatch'].fillna('').value_counts().to_dict()
        by_file[p.name] = counts
        for k, v in counts.items():
            mismatch_counts[k] = mismatch_counts.get(k, 0) + int(v)

    summary = {
        'reports_dir': str(reports_dir),
        'n_reports': len(csvs),
        'total_rows': int(total_rows),
        'mismatch_counts': mismatch_counts,
        'by_file': by_file,
    }

    out = reports_dir.joinpath('summary.json')
    out.write_text(json.dumps(summary, indent=2))
    print(json.dumps(summary, indent=2))
    return summary


if __name__ == '__main__':
    # default path used by generator
    reports_dir = Path(__file__).parent.joinpath('..', 'user_strategy_v7_bi', 'consistency_reports').resolve()
    summarize_reports(reports_dir)
