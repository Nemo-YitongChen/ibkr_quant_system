# 项目目标、当前进度与未来规划

本文聚焦当前仓库 `ibkr_quant_system`，不包含同级目录下的 `booking-site`。

## 1. 项目定位

`ibkr_quant_system` 不是一个单点策略脚本，而是在朝“基于 IBKR 的中长期投资研究与执行操作系统”演进。  
它当前的核心目标是：

- 为 `US / HK / ASX / XETRA / CN` 多市场提供统一的投资研究流程
- 把“候选生成 -> 打分排序 -> 组合计划 -> paper 账本 -> broker 执行 -> 风控守护 -> 周度复盘”串成一条闭环
- 用统一的 `audit.db`、报告目录和 supervisor 调度，把研究、执行和复盘沉淀成可追踪的运行系统
- 在 `paper` 与 `live` 两种运行模式之间复用同一套流程，逐步从研究工具走向稳定的半自动投资运营系统

从当前实现看，项目主线已经明确切到“中长期投资”而不是“短线交易”。这一点可以从 `README.md`、`config/supervisor.yaml`、`config/supervisor_live.yaml` 以及大量 `investment_*` 工具链中得到验证。

## 2. 当前架构概览

系统大致分成 6 层：

### 2.1 数据与券商接入

- `src/ibkr/*`
  - IBKR 连接、合约、账户、订单、fills、市场数据
- `src/enrichment/*`
  - `yfinance / FMP / FRED / Finnhub` 等研究增强源
- `src/data/*`
  - 数据适配层与统一模型

### 2.2 研究与打分

- `src/analysis/investment.py`
  - 投资候选打分、动作分层、计划生成
- `src/analysis/investment_backtest.py`
  - 报告生成时附带轻量回测指标
- `src/analysis/investment_shadow_ml.py`
  - shadow ML 辅助打分
- `src/analysis/report.py`
  - 报告、CSV、JSON、Markdown 输出

### 2.3 组合与执行

- `src/tools/run_investment_paper.py`
  - 本地 paper 账本与调仓模拟
- `src/app/investment_engine.py`
  - broker 执行计划与提交
- `src/app/investment_guard.py`
  - 持仓防守与 guard 规则
- `src/app/investment_opportunity.py`
  - 盘中机会扫描

### 2.4 调度与运营

- `src/app/supervisor.py`
  - 多市场、多时间窗口自动调度
- `src/tools/preflight_supervisor.py`
  - 上线前轻量检查
- `src/app/dashboard_control.py`
  - dashboard 控制模式与运维开关

### 2.5 审计与复盘

- `src/common/storage.py`
  - SQLite 审计、订单、fills、risk、investment 运行记录
- `src/tools/review_investment_weekly.py`
  - 周度复盘、执行质量、风险反馈、阈值建议
- `src/tools/reconcile_investment_broker.py`
  - broker 对账
- `src/tools/sync_investment_paper_from_broker.py`
  - broker 快照回写本地账本

### 2.6 配置与市场扩展

- `config/`
  - 市场、风控、执行、paper、guard、opportunity、watchlist、holiday、regime 等配置
- 当前配置已覆盖：
  - `HK`
  - `US`
  - `ASX`
  - `XETRA`
  - `CN`

其中 `CN` 当前仍是 `research-only`，这在 `config/investment_cn.yaml` 中已有明确说明。

## 3. 当前进度判断

### 3.1 结论

基于代码规模、测试规模、运行产物和调度配置，我的判断是：

- 该项目已经明显超过“原型验证期”
- 当前更接近“单人主导、可实际运行的 Alpha 系统”
- 其中 `HK / US` 的链路成熟度最高，已具备研究、paper、执行、guard、weekly review 的完整闭环
- `ASX / XETRA` 已接入统一框架，但真实运营深度看起来弱于 `HK / US`
- `CN` 仍处在研究层验证阶段

这是一个“已经能跑、也确实跑过”的系统，但还没有完全进入多人协作、强工程治理、稳定生产化的阶段。

### 3.2 已完成内容

#### 目标层

- 已完成从“短线交易系统”向“中长期投资操作链路”的方向收敛
- 已形成统一主路径：
  - 投资报告
  - paper 账本
  - broker 执行
  - 持仓 guard
  - 机会扫描
  - 周复盘
  - baseline regression
  - broker 对账与同步

#### 工程层

- `src/` 下共有约 `98` 个 Python 源文件，约 `41180` 行代码
- `config/` 下共有约 `93` 个配置文件，说明市场与运行模式已经做了较深拆分
- 当前仅 `requirements.txt` 暴露了最小依赖，核心运行依赖轻量，但工程元数据仍偏简化
- 审计与事件存储已经统一进 SQLite，说明系统开始重视“可追踪性”而不是只输出一次性结果

#### 测试层

- `tests/` 下共有 `21` 个测试文件，约 `12838` 行
- 本次本地执行 `pytest -q` 时，已看到 `155 passed`
- 退出阶段因为人工中断出现 `KeyboardInterrupt`，同时有 `.pytest_cache` 写入权限 warning
- 这说明“测试主体是可通过的”，但当前运行环境在挂载卷上对 pytest cache 的写入不够友好

#### 运行层

- 仓库中存在大量已生成的运行产物目录，且时间分布不是一次性的
- `reports_investment_hk/*`、`reports_investment_us/*`、`reports_investment_xetra/*`、`reports_investment_asx/*`、`reports_investment_cn/*` 都已有实际输出
- `reports_preflight/` 中有较新的 preflight 与 `ibkr_history_probe` 结果
- `runtime_data/paper_investment_only_duq152001/audit.db` 说明 scoped runtime 路径已经实际使用

#### 运维层

- `config/supervisor.yaml` 与 `config/supervisor_live.yaml` 已把市场本地时区、定时任务、paper/live 区分、dashboard、weekly review、labeling 等统一进 supervisor
- dashboard control 已支持：
  - `AUTO`
  - `REVIEW_ONLY`
  - `PAUSED`
- 系统已经具备“运维视角”的设计意识，而不只是“研究脚本集合”

### 3.3 当前薄弱点

#### 文档不足

- 仓库此前只有两份核心文档：
  - `docs/runnable_code_summary.md`
  - `docs/supervisor_runbook.md`
- 对外仍缺少一份“项目目标/边界/阶段/路线图”文档

#### 模块复杂度偏高

- `src/app/supervisor.py` 约 `3123` 行
- `src/tools/review_investment_weekly.py` 约 `5252` 行
- `src/tools/generate_investment_report.py` 约 `2155` 行
- `src/common/storage.py` 约 `1378` 行

这代表系统功能已经较完整，但复杂度开始集中到少数超大文件，后续维护成本会上升。

#### 配置扩张明显

- 多市场、多模式是优势，但也带来了较重的配置复制
- `config/` 中已有大量 `investment_* / guard_* / execution_* / regime_* / ibkr_*` 文件
- 如果继续按当前方式扩展市场或策略，维护成本会持续增加

#### 协作工程化较弱

- GitHub 仓库当前为公开仓库：`Nemo-YitongChen/ibkr_quant_system`
- 本地 `main` 分支当前相对 `origin/main` 领先 `2` 个提交
- 仓库没有最近 PR 记录
- 提交历史目前较短，说明项目还主要是个人快速迭代模式，而不是标准化协作模式

## 4. 项目当前阶段总结

如果用一句话概括当前状态：

这是一个已经具备多市场研究、paper/live 执行和复盘闭环的个人量化投资操作系统雏形，功能上已经很完整，但工程治理、可维护性和生产化规范还需要补课。

更具体一点：

- 功能成熟度：中高
- 自动化程度：中高
- 可运维性：中
- 可协作性：中低
- 生产级稳健性：中低到中

## 5. 未来规划建议

这里的规划按“先稳住系统，再提升可维护性，再考虑扩展能力”的顺序来排。

### 5.1 近 30 天：补工程基础

优先级最高的不是再加新策略，而是把现有系统的工程边界先稳住。

当前已完成：

- 项目级文档体系
  - 架构图
  - 运行路径说明
  - market/config 对照表
  - paper/live 切换说明
- `.env.example` 和更明确的环境变量入口
- `pyproject.toml`、console scripts、README 安装运行闭环
- GitHub 基础 CI
  - 自动跑测试
- `src.main` 收口为 CLI 入口，核心装配下沉到独立 bootstrap 模块

### Dashboard freshness / health helper 对齐（进行中）

- 已为 dashboard helper 补充 freshness / market-state / health-overview / market-data-health 的测试覆盖。
- 正在把 helper 从旧的静态字符串映射升级为：
  - freshness 支持 `market + report_date + latest_generated_at + as_of_date`
  - market state 支持 `None -> 市场状态: 暂无数据`
  - health overview 按 `degraded > warning > ready` 聚合，并合并摘要
  - market data health overview 在空输入时给出 `warning + 明确兜底摘要`
- 当前已把这些 helper 接到 dashboard card / JSON payload / simple mode / advanced mode 的统一字段上，避免“测试语义”和“页面文案”继续脱节。
- 下一步会继续把这层 freshness / health 统一透到更多 dashboard control / ops 视图，并清理旧的同义字段。

下一步建议：

- 为 lint / static check 增加统一命令
- 继续把“研究产物目录”和“源代码目录”的关系在文档中说明清楚
- 继续压缩 `main` 之外的大文件复杂度

预期目标：

- 新人或未来的自己能在较短时间内理解项目
- 提交后能自动知道有没有把核心功能跑坏

### 5.2 30-60 天：降复杂度

建议聚焦技术债治理。

建议事项：

- 拆分 `src/app/supervisor.py`
  - 调度
  - 配置解析
  - 控制模式
  - 报告任务
  - 执行任务
- 拆分 `src/tools/review_investment_weekly.py`
  - 数据采集
  - KPI 汇总
  - 风险反馈
  - threshold 建议
  - Markdown 渲染
- 为 `Storage` 引入更清晰的 schema 分层或 migration 机制
- 收敛配置
  - 用市场默认值 + override，替代重复 YAML
- 把“研究-only / paper / live”抽象成更清晰的 capability matrix

预期目标：

- 降低修改一处牵动全局的风险
- 提升后续加市场、加策略、加规则的速度

### 5.3 60-90 天：强化生产化能力

当工程边界更稳后，再加强真正影响实盘质量的能力。

建议事项：

- 加强 observability
  - 执行失败告警
  - 关键任务耗时
  - 每市场最新报告新鲜度
  - paper/live 状态看板
- 增加更真实的集成测试
  - mock IBKR
  - runtime_data 回放
  - 周复盘样本回归
- 引入更明确的风险闸门
  - 账户状态异常
  - 数据源缺失
  - preflight fail 时自动降级到 `REVIEW_ONLY`
- 做好 live 运行留痕
  - 谁在什么模式下提交
  - 为什么提交
  - 风险建议与最终动作是否一致

预期目标：

- 让系统从“能运行”升级成“可长期稳定运营”

## 6. 建议的产品路线

如果项目最终目标是“个人长期可用的投资运营系统”，我建议路线是：

1. 先把 `HK / US` 做成最稳定的双市场主战场
2. 再把 `ASX / XETRA` 视为框架复用验证市场
3. `CN` 继续保持 research-only，等数据和执行条件成熟再决定是否进入 paper/live
4. 新功能优先级排序建议：
   - 稳定性
   - 可观测性
   - 配置收敛
   - 策略增强

核心原因是：这个项目现在最稀缺的不是“再多一个信号”，而是“让已有闭环长期可靠地工作”。

## 7. 本次分析结论

截至 `2026-03-27`，该项目最值得肯定的地方有三点：

- 已经形成了清晰的产品主线，而不是功能碎片堆叠
- 研究、执行、风控、复盘已经基本连成闭环
- 测试与运行产物都说明它不是停留在概念阶段

当前最值得优先投入的地方也有三点：

- 文档化
- 降复杂度
- 生产化治理

如果按这个顺序推进，`ibkr_quant_system` 有机会从“个人量化工程项目”进一步演进成“可长期维护的个人投资操作平台”。
