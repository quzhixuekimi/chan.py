# shared_chan_config.py
DEFAULT_CHAN_CONFIG: dict = {
  "bi_algo": "normal",
  "trigger_step": True,
  "skip_step": 0,
  "divergence_rate": float("inf"),
  "bsp2_follow_1": True,
  "bsp3_follow_1": True,
  "strict_bsp3": False,
  "bsp3_peak": False,
  "bsp2s_follow_2": False,
  "max_bs2_rate": 0.9999,
  "macd_algo": "peak",
  "bs1_peak": False,
  "bs_type": "1,2,3a,3b",
  "bsp1_only_multibi_zs": False,
  "min_zs_cnt": 0,
  "zs_algo": "over_seg",
}
