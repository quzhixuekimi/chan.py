#!/usr/bin/env python3
"""pivot_sr_standalone.py

Pivot S/R 指标的独立命令行测试工具。不经过 FastAPI/HTTP，方便在生产服务器上
快速验证算法结果，或者反复调参时肉眼核对 box 位置是否合理。

用法：

  # 模式一：完全脱离数据库，用本地CSV测算法本身（CSV需含 time,open,high,low,close,volume 列）
  python3 pivot_sr_standalone.py --csv /path/to/TSLA_15m.csv

  # 模式二：直接读生产DB（需要 DATABASE_URL 环境变量已配置，跟部署FastAPI服务用的是同一个），
  #         绕开FastAPI/HTTP这一层，比 curl 调接口更快、日志更直接
  python3 pivot_sr_standalone.py --code TSLA --level 15M

  # 顺便触发一次kline增量更新（跟接口首次调用时的行为一致）
  python3 pivot_sr_standalone.py --code TSLA --level 15M --update

  # 把完整JSON结果写到文件，方便进一步用脚本/前端核对
  python3 pivot_sr_standalone.py --code TSLA --level 15M --out result.json

  # 临时覆盖参数（比如想试试 res_len=20 而不是默认的40）
  python3 pivot_sr_standalone.py --code TSLA --level 15M --config '{"res_len": 20, "sup_len": 20}'
"""

from __future__ import annotations

import argparse
import json
import sys

import pandas as pd


def _print_zone(z: dict, kind: str) -> None:
  print(
    f"[{kind}] {z['left_time']} ~ {z['right_time']}  "
    f"top={z['top']:.4f}  bottom={z['bottom']:.4f}"
  )
  print(f"       {z['vol_text']}   {z['cvd_label']}   is_broken={z['is_broken']}")


def main() -> None:
  parser = argparse.ArgumentParser(description="Pivot S/R 指标独立测试工具")
  parser.add_argument("--code", default=None, help="股票代码，例如 TSLA（--csv模式下不需要）")
  parser.add_argument(
    "--level", default=None, help="级别：1D/1H/2H/4H/30M/15M（--csv模式下不需要）"
  )
  parser.add_argument(
    "--update",
    action="store_true",
    help="调用前先触发一次kline增量更新（跟接口首次调用行为一致，仅DB模式有效）",
  )
  parser.add_argument(
    "--csv",
    default=None,
    help="可选：直接用本地CSV文件代替数据库（需含 time,open,high,low,close,volume 列），"
    "完全脱离DB测试算法",
  )
  parser.add_argument("--out", default=None, help="可选：把完整JSON结果写到这个文件")
  parser.add_argument(
    "--config",
    default=None,
    help='可选：JSON字符串，覆盖部分参数，例如 \'{"res_len": 20}\'',
  )
  args = parser.parse_args()

  override_config = json.loads(args.config) if args.config else None

  from pivot_sr_indicator import compute_pivot_sr

  if args.csv:
    # ---- 模式一：完全脱离数据库/生产环境，纯算法测试 ----
    df = pd.read_csv(args.csv)
    df = df.rename(columns={"time": "dt"})
    df["dt"] = pd.to_datetime(df["dt"])
    for col in ["open", "high", "low", "close", "volume"]:
      if col not in df.columns:
        print(f"[错误] CSV缺少必需列: {col}", file=sys.stderr)
        sys.exit(1)
    result = compute_pivot_sr(df, override_config)
  else:
    # ---- 模式二：直接读生产DB，绕开FastAPI/HTTP层 ----
    if not args.code or not args.level:
      print("[错误] DB模式下 --code 和 --level 都是必填", file=sys.stderr)
      sys.exit(1)

    import kline_store

    code = args.code.strip().upper()
    level = args.level.strip().upper()

    if args.update:
      print(f"[更新] 触发 kline 增量更新 code={code} level={level} ...", file=sys.stderr)
      results = kline_store.ensure_levels_updated([code], [level])
      for r in results:
        print(f"  -> {r}", file=sys.stderr)

    df = kline_store.read_kline_df(code, level.lower())
    if df is None or df.empty:
      print(
        f"[错误] kline({level}) 为空: code={code}，"
        f"请先加 --update，或确认该股票/级别在数据库里已有数据",
        file=sys.stderr,
      )
      sys.exit(1)
    df = df.rename(columns={"time": "dt"})
    result = compute_pivot_sr(df, override_config)

  # ---- 输出 ----
  n_res = len(result["resistance_zones"])
  n_sup = len(result["support_zones"])
  print(f"\n共计算出 {n_res} 个阻力区域, {n_sup} 个支撑区域\n")

  for z in result["resistance_zones"]:
    _print_zone(z, "阻力")
  for z in result["support_zones"]:
    _print_zone(z, "支撑")

  if args.out:
    with open(args.out, "w", encoding="utf-8") as f:
      json.dump(result, f, ensure_ascii=False, indent=2)
    print(f"\n完整结果(含cvd_points明细)已写入: {args.out}")


if __name__ == "__main__":
  main()
