# 2026-05-14 Owner P0-P6 Asset Growth Path

本归档把“1000 AUD 小资金如何从 paper 试运行走向可验证增值”拆成可执行工程步骤。目标不是承诺收益，也不是通过杠杆、CFD、期权或频繁交易赌生存资金；目标是让系统先证明 post-cost edge、执行质量和风控闭环，再考虑极小 live 试运行。

## Owner Principle

- 资金小，第一优先级是避免单次错误导致账户永久受损。
- 只允许 cash / long-only / 高流动性 / fractional-capable 的 paper 路径先跑通。
- live submit 默认继续关闭；micro-live 必须有 paper evidence、审批、回滚和效果追踪。
- 每次只能调一个 primary 参数，不能为了“快速增值”绕过 readiness、edge gate 或 execution evidence。

## P0: No-Order Diagnostics

目标：系统必须明确回答“为什么没有订单”。

实现：

- 新增 `investment_no_order_diagnostics.json/csv`。
- 新增 funnel：candidate -> plan -> target weights -> raw orders -> blocked orders -> executable orders -> submitted orders。
- 新增 capital checks：cash after buffer、target equity、max order value、fractional enabled。
- 新增 blocking reason 聚合：market rule、edge、liquidity、quality、opportunity、manual/shadow review、risk alert。

验收：

- 没有订单时必须有 `primary_no_order_reason` 和 `primary_action`。
- dashboard overview 能看到 no-order reason 和 owner progression status。

## P1: Small-Account Capital Profile

目标：1000 AUD 账户不再被默认中大账户参数永久挡住。

实现：

- `account_profiles.small` 改为 whole-share ETF/cash 小账户路径。
- 小账户默认：`cash_buffer_floor=100`、`min_trade_value=25`、`max_order_value_pct=0.10`、`max_orders_per_run=1`、`account_allocation_pct=0.25`、`allow_fractional_qty=false`、`order_type=LMT`。
- `AccountProfileExecutionOverrides` 支持 cash buffer、fractional、manual review、order type 等字段。
- `InvestmentExecutionConfig.account_equity_cap=1000.0` 用于把 IBKR paper 账户的 100 万级别体验金压到 owner 真实风险资金口径；summary 同时保留 raw broker equity 和 cap 后 equity。

验收：

- 1000 AUD 账户的 max order value >= min trade value。
- US paper 优先生成 last_close 不高于单笔上限的整股 ETF order；HK/CN/XETRA 仍受 market rule / research-only / data quality gate 约束。

## P2: Paper Order Activation

目标：先让 paper 真实产生 planned/submitted orders。

实现：

- owner progression assessment 直接标记 `PAPER_BLOCKED` / `PAPER_PLANNED` / `PAPER_SUBMITTED`。
- 若有 planned 但未 submit，下一步明确为 `run_paper_submit_after_readiness_passes`。
- 若无 planned，下一步回到 P0 top blocker。

验收：

- paper 至少能在 US fractional-capable 组合上产生 planned order。
- submit 仍必须通过 readiness gate。

## P3: Post-Cost Edge Gate

目标：不为了下单而放松 edge。所有 planned paper order 必须带 expected edge、cost 和 threshold。

实现：

- diagnostics 计算 allowed edge margin。
- owner progression 在 P3 标记 post-cost evidence 状态。

验收：

- order_count > 0 时，后续 weekly review 能把 expected edge -> realized edge 串起来。
- 没有 fill/outcome 前，P3 只允许 `INSUFFICIENT_SAMPLE`，不允许自动放宽 gate。

## P4: Micro-Live Acceptance

目标：live 之前必须证明 paper 链路有效。

当前状态：

- P4 永远保持 `BLOCKED`，直到连续多周 paper submitted/fill/post-cost evidence 通过。

最低接受规则：

- 多周 paper 有真实 submitted/fill evidence。
- post-cost realized edge 不显著为负。
- blocked-vs-allowed 不显示被挡单持续优于允许单。
- slippage/fill delay/fee ratio 没有异常。

## P5: Live Safety Controls

目标：live submit 默认锁住。

当前状态：

- owner progression 中 P5 为 `PASS`，含义是 live submit 仍默认关闭。
- P4 未通过前，不能打开 live automation。

后续要求：

- micro-live 只能 cash、long-only、极小 notional。
- 任何 unexpected order / slippage spike / DB lock / IBKR session 异常都应切回 `REVIEW_ONLY`。

## P6: Investment State Assessment

目标：每次 execution run 输出 owner-facing 当前状态。

实现：

- 新增 `investment_owner_progression_assessment.json/csv`。
- dashboard overview 增加 `owner_progression_status`。
- execution summary 增加 `primary_no_order_reason`、`no_order_primary_action`、`owner_progression_status`。

验收：

- 当前状态能被归类为 `PAPER_BLOCKED`、`PAPER_PLANNED` 或 `PAPER_SUBMITTED`。
- owner 下一步不再依赖人工猜测。

## Current Assessment

当前最可能的 no-order root cause 是历史默认参数不适合 1000 AUD：

- 旧 small profile `min_trade_value=1000`。
- US execution config `cash_buffer_floor=1000`。
- `max_order_value_pct=0.08` 时 1000 AUD 账户单笔 cap 只有约 80 AUD。
- IBKR paper 账户权益不等于 owner 真实风险资金，必须使用 `account_equity_cap` 避免 paper 计划过度放大。

本次改动后，系统会把这些条件直接暴露成 no-order diagnostics，而不是静默产生 0 单。
