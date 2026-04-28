"""配置项：保持与 v5 原代码兼容的默认值"""
from typing import Dict

DEFAULT_CONFIG: Dict = {
    "symbol": "ETHUSDT",
    "timeframe": "15m",
    "initial_balance": None,
    "risk_per_trade": 0.02,
    "min_divergence_strength": 0.25,
    "enable_buy_filter": True,
    "buy_rsi_threshold": 40,
    "buy_volume_ratio": 0.8,
    "enable_30min_clear": True,
    "min_bars_before_action": 1,
    "initial_add_size": 0.3,
    "trailing_stop_atr": 2.0,
    "trailing_stop_pct": 0.05,
    "min_profit_for_trailing": 0.01,
    "tp_ratios": {
        '1min': 0.25,
        '3min': 0.20,
        '5min': 0.25,
    },
    # 阶段性止盈触发阈值（按比例），与 tp_ratios 顺序对应
    "tp_thresholds": [0.01, 0.03, 0.05],
    "klines_limit": 500,
    "check_interval": 2,
    "max_position_usdt": None,
    "min_position_usdt": None,
    "max_leverage": 2.0,
}


def get_default_config() -> Dict:
    return DEFAULT_CONFIG.copy()
