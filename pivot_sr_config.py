# pivot_sr_config.py
#
# High Volume Pivot Support & Resistance Zones 指标的全局配置。
# 数值来源：requirements/tradingview_aio_latest.txt 第二部分 Pine 脚本的 input 默认值。
# 后续如需调参，直接改这个文件即可，不需要改 pivot_sr_indicator.py / pivot_sr_api.py。

DEFAULT_PIVOT_SR_CONFIG: dict = {
  # ---- Pivot 检测 (对应 Pine: resLen / supLen) ----
  # pivot 确认所需的左右bar数量。数值越大，pivot越"结构性"，但也需要更多bar才能确认。
  "res_len": 40,
  "sup_len": 40,
  # ---- 放量校验 (对应 Pine: volAvgLen / volMult) ----
  # pivot bar 本身的成交量必须超过 SMA(volume, vol_avg_len) * vol_mult 才算有效pivot。
  "vol_avg_len": 20,
  "vol_mult": 1.2,
  # ---- ATR box padding (对应 Pine: atrLen，原脚本中是硬编码 100，不是 input) ----
  # box 的厚度 = ATR(atr_len)，加在 pivot bar 实体价格之上/下方。
  "atr_len": 100,
  # ---- 历史区域数量限制 (Pine 原版没有此参数，是我们自己为了控制前端渲染数量新增的) ----
  # 阻力/支撑各自最多保留最近 N 个区域(box)。
  "max_zones_per_side": 3,
}
