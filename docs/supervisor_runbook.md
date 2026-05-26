# Supervisor Runbook

这份 runbook 用于日常启动、排障和切换 `paper / live`。

推荐先在仓库根执行：

```bash
python -m pip install -e ".[dev]"
```

安装后优先使用 `ibkr-quant-preflight`、`ibkr-quant-auto-order-readiness` 和 `ibkr-quant-supervisor`。原有 `python -m src...` 路径仍然兼容。

## 1. 先跑 preflight

Paper scope 示例：

```bash
ibkr-quant-preflight \
  --config config/supervisor.yaml \
  --runtime_root runtime_data/paper_investment_only_duq152001 \
  --out_dir reports_preflight
```

Live scope 示例：

```bash
ibkr-quant-preflight \
  --config config/supervisor_live.yaml \
  --runtime_root runtime_data/live_investment_only_<account_id> \
  --out_dir reports_preflight_live
```

输出：

- `reports_preflight/supervisor_preflight_summary.json`
- `reports_preflight/supervisor_preflight_report.md`

重点关注：

- `dashboard_db`
- `ibkr_config`
- `runtime_root`
- `ibkr_port:*`

## 2. 常用启动方式

启动自动提交前，先跑只读 readiness：

```bash
ibkr-quant-auto-order-readiness \
  --config config/supervisor.yaml \
  --out_dir reports_supervisor
```

输出：

- `reports_supervisor/auto_order_readiness.json`
- `reports_supervisor/auto_order_readiness.md`

只有 `blocked_count == 0` 时，才允许启动会提交 paper 订单的 supervisor。`WARNING` 表示可以继续 paper 运行但仍有需要跟踪的建议项；`BLOCKED` 表示 supervisor 会在执行前跳过自动提交。

Readiness 的 hard block 会尽量按 portfolio/market 收窄：

- 组合级 preflight fail 只挡对应组合。
- `config/runtime_root/dashboard_db/ibkr_port` 这类全局 fail 仍挡所有相关组合。
- gateway budget 按 market 判断。
- strategy suggestion/follow-up 按 portfolio 判断，并去重 carried-forward 建议。

Paper：

```bash
ibkr-quant-supervisor --config config/supervisor.yaml
```

Live：

```bash
ibkr-quant-supervisor --config config/supervisor_live.yaml
```

Live 自动提交默认不被 `config/supervisor.yaml` 的 readiness policy 允许；切换 live 需要单独完成证据、审批、回滚与效果追踪，不应只通过启动命令完成。

只跑当前时刻应触发的一轮：

```bash
ibkr-quant-supervisor --config config/supervisor.yaml --once
```

## 3. Dashboard 控制模式

- `AUTO`
  - 允许自动执行和 guard 提交
- `REVIEW_ONLY`
  - 继续生成执行/guard 计划，但不自动提交
- `PAUSED`
  - 暂停执行和 guard，只保留报告与复盘链路

当顶部出现“建议切换执行模式”时，优先处理对应组合。

Dashboard 顶部的 `运维总览` 会同时显示：

- preflight 的 `PASS / WARN / FAIL`
- 本地 IBKR 端口监听告警
- 报告是否过旧
- 组合健康度是否降级
- 当前执行模式是否和建议模式不一致

如果 preflight 出现关键 warning/fail，trade 页面顶部还会直接出现 `Preflight 关键提示`，用于快速判断“现在是否不适合自动执行”。

Supervisor 启动的 IBKR Gateway 子任务会自动注入任务级 `IBKR_CLIENT_ID_OFFSET`，使 report、broker snapshot、opportunity、execution、guard 和 short-safety 同一市场不再抢占同一个 `client_id`。默认 `ibkr_client_id_retry_span: 1`，也就是同一任务只使用确定性 clientId；默认 `ibkr_connect_max_rounds: 3`，连接失败会退出本轮任务而不是无限挂起。如果 Gateway 显示 `clientId already in use`，优先停掉旧 supervisor / Python 进程或等待 Gateway 清理 orphan session，不再自动递增制造更多 client 标签。只有在明确接受更多 Gateway client session 的情况下，才临时把 `ibkr_client_id_retry_span` 调大。

行情层也会对重复失败做冷却：同一 symbol/request 的历史行情请求若刚刚返回空结果或错误，`MarketDataService` 会在 `hist_empty_cooldown_sec` / `hist_error_cooldown_sec` 内跳过重复 Gateway 请求，优先使用 stale cache 或交给 yfinance fallback。这个机制用于降低 IBKR 请求量，不会放宽交易 gate。

Dashboard 生成也有大 artifact 保护：如果 `reports_investment_weekly/weekly_review_summary.json` 或独立 weekly evidence JSON 已经膨胀到数百 MB，`generate_dashboard` 不再整文件反复 parse，而是优先读取独立 CSV/小 JSON，并用 JSON 头部元数据完成 artifact health。这样 dashboard 超时通常不应该通过调大 `dashboard_timeout_sec` 解决；如果再次超时，应先检查新的巨型 artifact 是否绕过了这个 fallback。

Weekly review 输出已经改成轻量 summary：`weekly_review_summary.json` 只保留 evidence row_count 与 artifact 引用，不再内嵌完整 decision/unified evidence 明细。需要深挖时直接读 `weekly_decision_evidence.csv`、`weekly_unified_evidence.csv` 或 `weekly_unified_evidence.json`。

SQLite 运行库现在使用统一锁等待策略：`Storage` 会设置 `busy_timeout`、尽量启用 WAL，并对短暂 `database is locked/busy` 做有限重试；直接读库的 dashboard、weekly、reconcile、export 工具也走 `connect_sqlite()`。如果仍看到 `database is locked`，优先检查是否有长时间运行的外部 SQLite 客户端、手工事务或卡住的 Python 进程，而不是直接重启 Gateway。

小账户 paper 没有订单时，先看：

- `investment_no_order_diagnostics.json`
- `investment_no_order_diagnostics.csv`
- `investment_owner_progression_assessment.json`

这三个文件会说明当前是资金参数挡住、target weight 为 0、market rule 挡住、edge gate 挡住、liquidity 挡住，还是只是 planned 但尚未 submit。1000 AUD 级别账户应优先使用 small account profile 的 whole-share ETF/cash/limit-order paper 路径，候选 ETF 需要 `last_close <= max_order_value` 且 edge/cost 通过；live 自动提交仍保持关闭。

如果 control service 已开启，还可以直接点击：

- `立即跑一轮`
- `立即跑 Preflight`
- `立即跑 Weekly Review`
- `刷新 Dashboard`

## 4. 周度执行质量的口径

dashboard 的“本周执行质量”现在优先读取：

- `reports_investment_weekly/weekly_execution_summary.csv`

这是 supervisor 会自动刷新的周报来源。  
`reports_investment_execution/investment_execution_weekly_summary.csv` 仍保留，但更适合手动深挖执行细节，不再是 dashboard 的首选来源。

## 5. 排障顺序

1. 先看 dashboard 顶部是否有风险告警和模式建议
2. 再看 `reports_preflight/*`
3. 再看 `reports_investment_weekly/weekly_review.md`
4. 若是执行问题，再看 `reports_investment_execution/*`
