Consistency Check Scripts
=========================

Purpose
-------
These scripts perform read-only consistency checks between the CSV outputs in each strategy's
results/ directory and the high-level rules we inferred from the strategy code (v6, v7, v8, v9).

Important
---------
- The scripts only read files and produce a report. They do NOT modify your repository or run
  the backtests.
- I will not run these scripts. To run them, follow the instructions below.

Requirements
------------
- Python 3.8+
- pandas

Install
-------
python3 -m venv .venv
source .venv/bin/activate
pip install -r consistency_check/requirements.txt

How to run
----------
From the repository root run:

python3 consistency_check/check_all.py

This will search each user_strategy_v*_*/results/ subdir for CSVs and run per-strategy checks.

Output
------
- The script prints a compact JSON-like report to stdout and writes a detailed report to
  consistency_check/last_report.json.

Notes on checks
--------------
- The checks are conservative and designed not to depend on importing your strategy Python
  modules. For v7 we implement arithmetic index checks (entry_idx == bi.start_index + entry_delay_bars,
  exit_idx == bi.end_index + exit_delay_bars) using the default delays (2 bars) unless explicitly
  supplied in a config file that can be parsed as simple constants.
- For v6/v8/v9 we perform structural and semantic checks (presence of expected columns, valid
  enumerations for reason/type, index ordering, stop_price presence for stop exits, etc.).

If you want stricter checks that recompute indicators and re-evaluate the exact entry/exit
conditions, choose to run the scripts locally or grant permission for me to run them here.
