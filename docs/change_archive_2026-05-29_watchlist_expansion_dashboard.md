# 2026-05-29 Watchlist Expansion Dashboard

## 背景

股票池扩展已经能从本地 `investment_candidates.csv` 生成 auto-expanded watchlist，但 dashboard 没有消费该产物。结果是 operator 无法在 dashboard 里直接判断：

- 当前账户 profile 下扩展池是否新鲜；
- 哪些市场有候选但没有通过小账户整股/成本/质量门；
- 主要 reject reason 是价格、成本、流动性、edge 还是 whole-share tradability。

## 改动

- Dashboard 新增 `watchlist_expansion_summary` 顶层 payload，读取 `watchlist_expansion_summary.json`、summary CSV 和 candidate CSV。
- Dashboard v2 新增 advanced block `watchlist_expansion`，展示：
  - market count / candidate rows / selected count；
  - zero-selected market count；
  - account profile / account equity；
  - artifact age / max age；
  - selected symbols；
  - top reject reason 与 reason summary。
- 新增 tests 覆盖 watchlist expansion payload 加载、reject reason 聚合和 dashboard block 状态。

## 当前本地诊断

重新生成 dashboard 后，当前 small account profile 下：

- candidate rows: 65
- selected: 2
- selected symbols: `SPTM,SCHB`
- zero-selected markets: 3
- top reject reason: `expected_cost_above_max`

这说明下一步扩大 ASX/HK/XETRA 候选面时，不应直接放宽 risk gate，而应优先改善/校准成本、whole-share tradability、可交易 ETF-first universe 与小账户价格上限。

## 验证

- `PYTHONDONTWRITEBYTECODE=1 python -m py_compile src/tools/generate_dashboard.py src/tools/dashboard_blocks.py tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py`
- `PYTHONDONTWRITEBYTECODE=1 pytest -q -p no:cacheprovider tests/test_generate_dashboard_helpers.py tests/test_dashboard_blocks.py tests/test_watchlist_expansion.py tests/test_account_profile.py`
- `PYTHONDONTWRITEBYTECODE=1 python -m src.tools.generate_dashboard --config config/supervisor.yaml --out_dir runtime_data/paper_investment_only_duq152001/reports_supervisor`
