trade_system
=============

占位的 paper-trading / execution 骨架设计文档

目的
-----
为后续对接富途（Futu）或其它经纪 API 做准备，先设计并明确 trade_system 的职责、数据流与集成点。当前仅撰写设计文档与流程梳理，暂不实现任何业务代码。

设计原则
-----
- 最小侵入：不修改现有回测/信号代码，直接消费 run_*.py 产生的 CSV（signal_digest / signal_events / trades 等）作为信号来源
- 可渐进部署：先实现完全本地的 PaperAdapter（虚拟账户），后续可替换为 FutuAdapter 并切换到真实/模拟经纪环境
- 明确边界：trade_system 只负责“信号→订单→（虚拟）成交→记录/通知”链路，策略信号仍由现有 user_strategy_* 生产
- 可配置化：交易时段、执行策略（立即执行/入队等待开盘）、symbol 映射等均为配置项

数据输入（现有集成点）
-----
- 优先信号源：各策略目录输出的 digest CSV（示例路径见 telegram_notify_api.py 的 STRATEGY_DIGEST_REGISTRY），例如：
  - user_strategy_v7_bi/results/market_signal_digest_last_per_symbol_v7_bi.csv
  - user_strategy_v8_byma/results/...
- 也可直接读取 per-symbol 的 *_signal_digest_vX.csv 或 market_all_signal_events_xxx.csv

信号对象（字段参考）
-----
- 常见字段（run_v7_bi 已标准化）:
  - symbol (e.g. "HK.00700", "US.AAPL")
  - timeframe ("1d", "4h", ...)
  - latest_event_type / event_type ("BUY_SIGNAL","SELL_SIGNAL","STOP_LOSS_TRIGGERED")
  - latest_price / stop_price
  - event_time / event_date
  - bi_id (若适用)
  - signal_text / summary_text / summary_json
  - has_signal (布尔)

接入/消费流程（高层）
-----
1. 回测（现有）在 daily_workflow_scheduler 或手动触发后生成 signal_digest CSV。
2. trade_system 的“摄取器（Ingest）”读取指定 digest 文件，按行转为内部 OrderRequest（包含 symbol、side、price、reason、event_time、uid）
3. 根据配置决定：
   - a) 即时尝试执行（若市场/时段与 adapter 支持）
   - b) 将订单写入本地持久队列（trade_system/queue/*.json），等待调度器按预设的执行窗口处理（如夜盘、次日开盘）
4. 执行器（Executor）通过当前 adapter（PaperAdapter 或 FutuAdapter）提交订单：
   - PaperAdapter：立即模拟成交并返回 fill；
   - FutuAdapter：调用 futu-api.place_order(trd_env=TrdEnv.SIMULATE) 或真实下单（需解锁）
5. 成交后写入本地 trades/logs（格式尽量兼容回测输出，便于对比），并可调用现有 telegram_notify_api 提交 POSITION_OPEN / TRADE_CLOSED 的通知（HTTP POST /api/notify/telegram/send-digest 或 send-message）

队列与调度
-----
- 队列持久化：trade_system/queue/ 下按策略或市场分文件存储待执行订单（JSON），便于审计与重试
- 调度器：可由 daily_workflow_scheduler 在回测完成后触发一次摄取（短循环），也可配置为常驻 Scheduler 按时间窗口（例如 21:30）扫描队列并执行
- 去重：摄取器应支持基于 digest 的 dedup_key（见 telegram_notify_api.build_row_dedup_key）避免重复下单

时区与时点规则（关键）
-----
- 以交易所本地时间为准计算“是否可下单”与“当日/昨日”的界定（例如美股用 America/New_York）
- 对于日线（1d）信号：采用 run_v* 在早 9:30 读取的“前一完整交易日”数据；不要将当日未完成 bar 误当成信号来源

价格与滑点
-----
- 初版：使用 digest 中的 latest_price 作为下单价格（虚拟成交即时按该价成交）
- 后续可选：在执行前向 FutuAdapter 请求实时快照以获取可用盘口价，再决定是否下单或按盘口计算部分成交/滑点

符号映射
-----
- 保持现有 symbol 格式（例如 HK.00700 / US.AAPL）作为默认；若 FutuAdapter 需要特殊前缀或转换，trade_system 提供可配置的 symbol_map 映射表（由用户维护）

通知与审计
-----
- 成交后写入 trades CSV，并记录到 trade_system/logs/ 包含原 digest 行，成交回报，时间戳
- 可复用 telegram_notify_api 的 send-digest/send-message 接口发送交易通知（POSITION_OPEN / TRADE_CLOSED）

错误处理与重试
-----
- 摄取失败或 adapter 调用异常应写入 trade_system/errors/ 并支持人工/自动重试

安全与权限
-----
- 若后续接入真实 Futu 环境，需妥善管理解锁密码和 API 权限；初期 PaperAdapter 无需权限管理

操作步骤（短期 roadmap，非代码实现）
-----
1. 明确各策略 digest 的路径（见 telegram_notify_api.STRATEGY_DIGEST_REGISTRY）
2. 定义 OrderRequest 的字段映射（基于 run_v*_digest 中的列）
3. 确定执行策略（立即执行 vs 入队等待）并写入配置示例
4. 设计队列文件格式与调度时序（例如每晚 21:30 执行一次队列）
5. 设计日志/审计格式，保证可与回测 trades 做比对

下一步（由你决定）
-----
- 我可以把上面的流程整理成 trade_system/PROCESS.md（更详细的步骤、示例 CSV 字段、配置样例、调度时间建议），或者
- 也可以把这些文档补入到 trade_system/README.md 的末尾（我已经做了首版），或
- 在你确认文档后，开始实现“摄取器 + 本地队列 + PaperAdapter 的最小可用实现”。

注：本文件为设计与流程梳理，暂不包含任何实现代码。
