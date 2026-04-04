# Supervisor Runbook

这份 runbook 用于日常启动、排障和切换 `paper / live`。

推荐先在仓库根执行：

```bash
python -m pip install -e ".[dev]"
```

安装后优先使用 `ibkr-quant-preflight` 和 `ibkr-quant-supervisor`。原有 `python -m src...` 路径仍然兼容。

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

Paper：

```bash
ibkr-quant-supervisor --config config/supervisor.yaml
```

Live：

```bash
ibkr-quant-supervisor --config config/supervisor_live.yaml
```

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
