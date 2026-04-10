# -*- coding: utf-8 -*-
from pathlib import Path
import pandas as pd
from typing import List, Dict, Any

from user_strategy_v7_bi.config import StrategyConfig
from user_strategy_v7_bi.chan_loader import (
  load_chan_data,
  extract_kline_data,
  extract_bi_data,
)
from user_strategy_v7_bi.backtest_engine import BiBacktester


def save_df(df: pd.DataFrame, path: Path):
  path.parent.mkdir(parents=True, exist_ok=True)
  df.to_csv(path, index=False, encoding="utf-8-sig")


def get_all_symbols(data_dir: Path) -> List[str]:
  """
  扫描 data_cache 文件夹，提取所有不重复的股票代码
  """
  symbols = set()
  for file in data_dir.glob("*.csv"):
    parts = file.name.split("_")
    if parts:
      symbols.add(parts[0])
  return sorted(list(symbols))


def main():
  repo_root = Path(__file__).resolve().parent.parent
  conf = StrategyConfig()

  data_dir = conf.resolved_data_dir(repo_root)
  out_dir = conf.resolved_output_dir(repo_root)

  # 动态获取所有股票代码
  all_symbols = get_all_symbols(data_dir)
  print(f"发现 {len(all_symbols)} 个股票代码: {all_symbols}")

  final_summary_list = []

  for symbol in all_symbols:
    print(f"\n{'=' * 40}\n正在处理股票: {symbol}\n{'=' * 40}")

    # 为当前股票配置 config
    conf.symbol = symbol
    symbol_summary = []

    for tf in conf.timeframes:
      if not tf.enabled:
        continue

      # 动态匹配文件名
      # 1D: SYMBOL_DATE_1d.csv
      # Others: SYMBOL_DATE_yf_1h_730d.csv 等
      if tf.level == "1D":
        matches = list(data_dir.glob(f"{symbol}_*_1d.csv"))
      else:
        matches = list(data_dir.glob(f"{symbol}_*_yf_{tf.name.lower()}_730d.csv"))

      if not matches:
        print(f"  [!] 未找到 {symbol} 的 {tf.name} 数据文件，跳过")
        continue

      csv_path = matches[0]

      print(f"  处理时间框架: {tf.name} ({tf.level}) -> {csv_path.name}")

      try:
        # 加载框架数据
        chan, kl_type, kl_list = load_chan_data(
          code=symbol,
          level=tf.level,
          csv_path=csv_path,
          config=conf.chan_config,
          trigger_step=conf.trigger_step,
          begin_time=tf.start_time,
          end_time=tf.end_time,
        )

        kline_df = extract_kline_data(kl_list)
        bi_list = extract_bi_data(kl_list)

        # 运行基于笔的回测
        bt = BiBacktester(
          symbol=symbol,
          timeframe=tf.name,
          df=kline_df,
          bi_list=bi_list,
          entry_delay_bars=conf.entry_delay_bars,
          exit_delay_bars=conf.exit_delay_bars,
          use_structure_stop=conf.use_structure_stop,
        )
        summary = bt.run()

        # 保存结果
        save_df(kline_df, out_dir / f"{symbol}_{tf.name}_ohlcv_v7_bi.csv")
        save_df(
          pd.DataFrame(bi_list), out_dir / f"{symbol}_{tf.name}_bi_snapshot_v7_bi.csv"
        )
        save_df(bt.trades_df(), out_dir / f"{symbol}_{tf.name}_trades_v7_bi.csv")
        save_df(
          bt.trade_trace_df(), out_dir / f"{symbol}_{tf.name}_trade_trace_v7_bi.csv"
        )
        save_df(
          pd.DataFrame([summary]), out_dir / f"{symbol}_{tf.name}_summary_v7_bi.csv"
        )

        symbol_summary.append(summary)
        print(f"    - 完成，生成 {len(bt.trades)} 笔交易")
      except Exception as e:
        print(f"    - [错误] 处理 {tf.name} 时发生异常: {e}")

    # 保存该股票所有级别的汇总
    if symbol_summary:
      all_symbol_summary_df = pd.DataFrame(symbol_summary)
      save_df(all_symbol_summary_df, out_dir / f"{symbol}_all_summary_v7_bi.csv")
      final_summary_list.extend(symbol_summary)

  # 保存全市场汇总
  if final_summary_list:
    all_market_summary_df = pd.DataFrame(final_summary_list)
    save_df(all_market_summary_df, out_dir / f"market_all_summary_v7_bi.csv")
    print("\n" + "=" * 60)
    print("V7-BI 全市场回测完成")
    print(f"结果保存至: {out_dir}")
  else:
    print("未生成任何回测结果。")


if __name__ == "__main__":
  main()
