# GitHub Issue Backlog Drafts

This file contains issue-ready drafts for the current IBKR project backlog. The repository connector available here cannot create GitHub issues directly, so these drafts are committed into the repo for review and manual creation.

---

## P0-01 Fix `Storage` indentation and restore audit methods

**Labels:** `P0`, `bug`, `core`

### Summary
Fix `src/common/storage.py` indentation so audit table creation and helper methods belong to `Storage` again.

### Problem
The current file has `signals_audit` / `md_quality` table creation logic and helper functions placed outside the `Storage` class or outside `_init_db()`. This can break module import and also makes `Storage` instances miss expected methods.

### Scope
- Move `signals_audit` table creation into `_init_db()`
- Move `md_quality` table creation into `_init_db()`
- Move `insert_signal_audit()` into `Storage`
- Move `upsert_md_quality()` into `Storage`

### Tasks
- [ ] Fix indentation in `src/common/storage.py`
- [ ] Verify `Storage("audit.db")` initializes without errors
- [ ] Verify `insert_signal_audit()` works
- [ ] Verify `upsert_md_quality()` works
- [ ] Add a small test for these methods

### Acceptance Criteria
- `from src.common.storage import Storage` succeeds
- `Storage(...).insert_signal_audit(...)` succeeds
- `Storage(...).upsert_md_quality(...)` succeeds
- Audit tables are created automatically on initialization

---

## P0-02 Fix `TradingEngine` method scope so `run_forever()` exists

**Labels:** `P0`, `bug`, `engine`

### Summary
Fix `src/app/engine.py` so `_update_quality()` and `run_forever()` are methods of `TradingEngine`.

### Problem
The current indentation makes `_update_quality()` appear outside the class, and `run_forever()` is no longer attached to `TradingEngine`. `src/main.py` still calls `engine.run_forever()`, so startup fails.

### Scope
- Re-indent `_update_quality()`
- Re-indent `run_forever()`
- Confirm `TradingEngine` exposes the expected public API

### Tasks
- [ ] Fix method indentation in `src/app/engine.py`
- [ ] Verify `TradingEngine(...).run_forever` exists
- [ ] Verify `_update_quality()` can access `self._quality`
- [ ] Add a smoke test for engine instantiation

### Acceptance Criteria
- `hasattr(engine, "run_forever")` is `True`
- Main entrypoint no longer fails on missing method lookup
- Engine loop can start successfully

---

## P0-03 Add smoke tests for import and bootstrap

**Labels:** `P0`, `test`

### Summary
Add minimal smoke tests that validate imports and basic bootstrap paths.

### Problem
Current CI only compiles source files. It does not catch import-time failures or missing methods on core objects.

### Tasks
- [ ] Add `tests/test_imports.py`
- [ ] Add `tests/test_storage_smoke.py`
- [ ] Add `tests/test_engine_smoke.py`
- [ ] Mock `IB`, `OrderService`, `AccountService`, and runner dependencies

### Acceptance Criteria
- `import src.main` passes
- `Storage(...)` can be instantiated in tests
- `TradingEngine(...)` can be instantiated in tests
- Tests run in CI

---

## P0-04 Upgrade CI from compile-only to real validation

**Labels:** `P0`, `ci`

### Summary
Upgrade CI to catch real breakages, not just syntax/bytecode compilation.

### Tasks
- [ ] Update `.github/workflows/ci.yml`
- [ ] Run `pytest`
- [ ] Add linting step (`ruff` preferred)
- [ ] Add dependency health check (`pip check`)

### Acceptance Criteria
- CI fails on broken imports
- CI fails on missing methods detected by smoke tests
- CI output clearly shows which step failed

---

## P0-05 Remove repo artifacts and harden `.gitignore`

**Labels:** `P0`, `cleanup`

### Summary
Clean repository artifacts such as local virtualenv files, SQLite DB files, caches, and compiled Python outputs.

### Tasks
- [ ] Add missing ignore rules
- [ ] Remove tracked local artifacts
- [ ] Verify a clean checkout has no local runtime files committed
- [ ] Document ignored artifacts if needed

### Acceptance Criteria
- No local environment or DB artifacts remain tracked
- `.gitignore` prevents reintroduction of these files

---

## P0-06 Make root README the real entrypoint

**Labels:** `P0`, `docs`

### Summary
Replace the one-line root `README.md` with a real project entrypoint.

### Tasks
- [ ] Expand `README.md`
- [ ] Link `PROJECT_OVERVIEW.md`
- [ ] Link `docs/roadmap.md`
- [ ] Clarify paper-trading-only status
- [ ] Add risk disclaimer

### Acceptance Criteria
- A new collaborator can understand the repo from `README.md`
- README explains how to install and start the project
- README clearly states current project maturity level

---

## P1-01 Verify end-to-end paper trading loop

**Labels:** `P1`, `integration`, `trading`

### Summary
Validate the minimum paper-trading workflow from market data ingestion to order placement and audit.

### Tasks
- [ ] Configure paper account credentials
- [ ] Start IB Gateway or TWS in paper mode
- [ ] Run engine on 1-2 symbols
- [ ] Confirm signal generation
- [ ] Confirm bracket order placement
- [ ] Confirm fill / commission handling
- [ ] Confirm risk events and audit persistence

### Acceptance Criteria
- At least one full paper trade is recorded end-to-end
- Audit DB contains matching signal/order/fill/risk records
- Failures are documented if the loop does not complete

---

## P1-02 Persist order creation audit in `OrderService`

**Labels:** `P1`, `execution`, `audit`

### Summary
Persist order creation records when bracket orders are submitted.

### Tasks
- [ ] Insert parent order row after order creation
- [ ] Insert take-profit order row
- [ ] Insert stop-loss order row
- [ ] Capture symbol/action/qty/order_id/parent_id/status
- [ ] Verify order status updates modify these rows correctly

### Acceptance Criteria
- Every bracket submission produces corresponding DB rows
- Child orders reference the correct parent order ID
- Status changes are visible in storage

---

## P1-03 Add integration tests for `UniverseService`

**Labels:** `P1`, `test`, `universe`

### Summary
Add tests for `UniverseService.build()` and scanner caching behavior.

### Tasks
- [ ] Mock `ib.positions()`
- [ ] Mock `ib.openTrades()`
- [ ] Mock `ib.trades()`
- [ ] Mock scanner response
- [ ] Test `always_on`
- [ ] Test `short_candidates`
- [ ] Test scanner cache refresh timing

### Acceptance Criteria
- `build()` returns deterministic results for controlled inputs
- Duplicate symbols are removed while preserving order
- Scanner calls are reduced by cache settings

---

## P1-04 Add deterministic tests for strategy fusion

**Labels:** `P1`, `test`, `strategy`

### Summary
Add pure unit tests for the signal-generation stack.

### Tasks
- [ ] Add fixed input arrays for mean reversion cases
- [ ] Add fixed input arrays for breakout cases
- [ ] Add trend/volatility cases for regime filter
- [ ] Add fusion tests for short-blocked and low-mid-scale scenarios
- [ ] Add `TradeSignal` expectation tests

### Acceptance Criteria
- Strategy behavior is reproducible under unit tests
- Threshold behavior is explicitly covered
- Short-blocked cases are covered

---

## P1-05 Document real configuration contract

**Labels:** `P1`, `docs`, `config`

### Summary
Document all required configuration values and their meaning.

### Tasks
- [ ] Document `host`, `port`, `client_id`, `account_id`
- [ ] Document paper/live default ports
- [ ] Document daily loss limits and consecutive loss settings
- [ ] Add sample config snippets
- [ ] Clarify timezone expectations

### Acceptance Criteria
- A user can configure the project without reading source code
- Paper mode setup is explicitly documented
- Risk config semantics are clear

---

## P1-06 Add operational runbook for paper mode

**Labels:** `P1`, `docs`, `ops`

### Summary
Create a practical runbook for starting and validating the system in paper mode.

### Tasks
- [ ] Document TWS/IB Gateway startup requirements
- [ ] Document API enablement steps
- [ ] Document paper account connection checks
- [ ] Document engine launch command
- [ ] Document common errors and fixes

### Acceptance Criteria
- A new user can start paper mode by following the runbook
- The runbook includes a short validation checklist
- Common misconfigurations are called out

---

## P2-01 Consolidate duplicate market-data paths

**Labels:** `P2`, `refactor`, `market-data`

### Summary
Unify overlapping realtime market-data aggregation logic.

### Tasks
- [ ] Compare `src/ibkr/realtime_agg.py` and `src/ibkr/market_data.py`
- [ ] Choose one primary path
- [ ] Standardize bar data structure
- [ ] Remove dead or redundant code
- [ ] Update engine usage if needed

### Acceptance Criteria
- Only one canonical realtime aggregation path remains
- Documentation reflects the chosen path
- Engine and strategy code use the same bar contract

---

## P2-02 Introduce backtest-compatible strategy interface

**Labels:** `P2`, `strategy`, `backtest`

### Summary
Refactor strategy interfaces so the same signal logic can be used in both live trading and backtesting.

### Tasks
- [ ] Define a standard bar model
- [ ] Define a standard signal model
- [ ] Extract IB-independent strategy logic where needed
- [ ] Add a simple historical runner
- [ ] Verify one existing strategy works in both modes

### Acceptance Criteria
- At least one strategy can run in both live-style and backtest-style execution
- Signal logic is no longer tightly coupled to IB-specific objects

---

## P2-03 Add reconnect and degraded-mode handling

**Labels:** `P2`, `ops`, `resilience`

### Summary
Improve runtime resilience around disconnects and partial service failures.

### Tasks
- [ ] Detect broker disconnects in runtime loop
- [ ] Attempt reconnect safely
- [ ] Recreate realtime subscriptions after reconnect
- [ ] Allow engine to continue with `always_on` if scanner fails
- [ ] Add logs and counters for reconnect attempts

### Acceptance Criteria
- Temporary disconnects do not permanently kill the engine
- Scanner failures do not stop core tracking loop
- Recovery events are visible in logs

---

## P2-04 Add structured reporting around audit DB

**Labels:** `P2`, `reporting`, `audit`

### Summary
Build simple reporting on top of the audit database for daily review.

### Tasks
- [ ] Add a daily summary script
- [ ] Aggregate signal/order/fill/risk counts
- [ ] Include `md_quality` summary
- [ ] Export to CSV or Markdown
- [ ] Keep output simple and readable

### Acceptance Criteria
- A daily run produces a usable summary artifact
- Summary helps explain what the engine actually did that day

---

## P2-05 Add packaging and launcher cleanup

**Labels:** `P2`, `packaging`, `cleanup`

### Summary
Clean up imports and define one standard way to launch the project.

### Tasks
- [ ] Review absolute vs relative imports
- [ ] Add `pyproject.toml`
- [ ] Choose standard launcher (`python -m ...` or console script)
- [ ] Update docs to match the chosen launcher

### Acceptance Criteria
- The project has one documented startup convention
- Imports are consistent and work in local + CI environments

---

## P2-06 Add live-trading safety gates before any live mode

**Labels:** `P2`, `safety`, `live-trading`

### Summary
Add explicit safeguards so the system cannot accidentally run live trading.

### Tasks
- [ ] Add explicit `paper` / `live` mode handling
- [ ] Require a second live-mode enable flag
- [ ] Add stricter live defaults
- [ ] Add emergency kill switch path
- [ ] Document live-mode risk clearly

### Acceptance Criteria
- Live mode cannot be enabled accidentally
- Paper mode remains the default
- Safety controls are documented and tested
