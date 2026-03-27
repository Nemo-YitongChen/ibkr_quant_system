from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List

import yaml
from ib_insync import IB  # type: ignore

from ..analysis.report import write_csv, write_json
from ..common.logger import get_logger
from ..common.markets import add_market_args, market_config_path, resolve_market_code
from ..common.storage import Storage
from ..enrichment.yfinance_history import fetch_daily_bars
from ..ibkr.contracts import make_stock_contract
from ..ibkr.market_data import MarketDataService
from ..offhours.ib_setup import register_contracts, set_delayed_frozen

log = get_logger("tools.label_investment_snapshots")
BASE_DIR = Path(__file__).resolve().parents[2]
# 这些 reason 会直接进入周报和 dashboard，帮助解释“为什么当前没有形成可用的 outcome 回标样本”。
SKIP_REASON_LABELS: Dict[str, str] = {
    "MISSING_SNAPSHOT_TS": "缺少快照时间",
    "NO_HISTORY_BARS": "历史数据为空",
    "SNAPSHOT_NOT_IN_HISTORY": "历史数据起点晚于快照",
    "INSUFFICIENT_FORWARD_BARS": "前向样本不足",
    "INVALID_CLOSE_VALUES": "价格数据无效",
    "EMPTY_FORWARD_PATH": "前向收益路径为空",
    "INVALID_OUTCOME_TS": "结果时间无效",
}


class _LabelingIbkrHistoryLoader:
    def __init__(self, market: str) -> None:
        self.market = str(market or "").upper().strip()
        self._connect_attempted = False
        self._available = False
        self._ib: IB | None = None
        self._md: MarketDataService | None = None

    def _connect(self) -> bool:
        if self._connect_attempted:
            return bool(self._available and self._ib is not None and self._md is not None)
        self._connect_attempted = True
        if not self.market:
            return False
        cfg_path = market_config_path(BASE_DIR, self.market)
        if not cfg_path.exists():
            return False
        try:
            raw_cfg = yaml.safe_load(cfg_path.read_text(encoding="utf-8")) or {}
        except Exception as e:
            log.warning("labeling ibkr cfg load failed market=%s: %s %s", self.market, type(e).__name__, e)
            return False
        host = str(raw_cfg.get("host") or "127.0.0.1").strip() or "127.0.0.1"
        port = int(raw_cfg.get("port", 0) or 0)
        base_client_id = int(raw_cfg.get("client_id", 1) or 1)
        if port <= 0:
            return False
        ib = IB()
        try:
            # labeling 是闭市后的离线任务，不适合像主执行链路那样无限重试。
            # 这里失败就立刻回退，避免整个回标流程被单个 IBKR 连接卡死。
            ib.connect(host, port, clientId=base_client_id + 700, timeout=6)
            ib.reqCurrentTime()
            ib.RequestTimeout = 8
            set_delayed_frozen(ib)
            self._ib = ib
            self._md = MarketDataService(ib)
            self._available = True
            return True
        except Exception as e:
            log.info("labeling ibkr history unavailable market=%s host=%s port=%s: %s", self.market, host, port, e)
            try:
                ib.disconnect()
            except Exception:
                pass
            return False

    def get_daily_bars(self, symbol: str, days: int) -> tuple[List[Any], str]:
        if not self._connect():
            return [], ""
        ib = self._ib
        md = self._md
        if ib is None or md is None:
            return [], ""
        try:
            # 每次只注册当前 symbol，避免离线 labeling 为了回标一次性注册过大 universe。
            register_contracts(ib, md, [str(symbol)])
        except Exception:
            try:
                md.register(str(symbol).upper(), make_stock_contract(str(symbol)))
            except Exception:
                return [], ""
        try:
            bars = md.get_daily_bars(str(symbol).upper(), days=max(90, int(days)))
            return list(bars or []), "ibkr"
        except Exception as e:
            log.info("labeling ibkr daily history fallback symbol=%s market=%s: %s", symbol, self.market, e)
            return [], ""

    def close(self) -> None:
        if self._ib is not None:
            try:
                self._ib.disconnect()
            except Exception:
                pass
        self._ib = None
        self._md = None
        self._available = False


def _load_daily_bars_for_labeling(
    symbol: str,
    market: str,
    days: int,
    ibkr_loaders: Dict[str, _LabelingIbkrHistoryLoader],
) -> tuple[List[Any], str]:
    market_code = str(market or "").upper().strip()
    loader = ibkr_loaders.get(market_code)
    if loader is None and market_code:
        loader = _LabelingIbkrHistoryLoader(market_code)
        ibkr_loaders[market_code] = loader
    if loader is not None:
        bars, source = loader.get_daily_bars(symbol, days=max(90, int(days)))
        if bars:
            return bars, source
    bars = fetch_daily_bars(symbol, days=max(90, int(days)), allow_stale_cache=True)
    if bars:
        return bars, "yfinance_cache"
    return [], ""


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Backfill forward outcomes for investment candidate snapshots.")
    add_market_args(ap)
    ap.add_argument("--db", default="audit.db")
    ap.add_argument("--portfolio_id", default="", help="Optional report portfolio id filter, e.g. US:watchlist.")
    ap.add_argument("--stage", default="final", help="Snapshot stage filter. Use broad/deep/final or empty for all.")
    ap.add_argument("--horizons", default="5,20,60", help="Comma-separated forward horizons in trading days.")
    ap.add_argument("--limit", type=int, default=400)
    ap.add_argument("--out_dir", default="reports_investment_labels")
    return ap.parse_args()


def _resolve_project_path(path_str: str) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    for candidate in (BASE_DIR / path, Path.cwd() / path):
        if candidate.exists():
            return candidate.resolve()
    return (BASE_DIR / path).resolve()


def _parse_horizons(raw_value: str) -> List[int]:
    out: List[int] = []
    for chunk in str(raw_value or "").split(","):
        text = str(chunk).strip()
        if not text:
            continue
        try:
            value = int(text)
        except Exception:
            continue
        if value > 0:
            out.append(value)
    return sorted(set(out)) or [5, 20, 60]


def _parse_ts(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        ts = datetime.fromisoformat(str(value))
    except Exception:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _bar_time(bar: Any) -> datetime | None:
    ts = getattr(bar, "time", None)
    if ts is None:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _first_bar_index_on_or_after(bars: List[Any], target_dt: datetime) -> int | None:
    target_date = target_dt.astimezone(timezone.utc).date()
    for idx, bar in enumerate(list(bars or [])):
        bar_ts = _bar_time(bar)
        if bar_ts is None:
            continue
        if bar_ts.date() >= target_date:
            return idx
    return None


def _signed_return(close_value: float, start_close: float, direction: str) -> float:
    raw_return = (float(close_value) / float(start_close) - 1.0) if float(start_close) > 0 else 0.0
    return -raw_return if str(direction or "LONG").upper() == "SHORT" else raw_return


def _classify_outcome(future_return: float, max_drawdown: float) -> str:
    if future_return >= 0.15:
        return "OUTPERFORM"
    if future_return >= 0.03:
        return "POSITIVE"
    if future_return <= -0.12 or max_drawdown <= -0.15:
        return "BROKEN"
    if future_return <= -0.03:
        return "NEGATIVE"
    return "FLAT"


def _skip_reason_label(reason_code: str) -> str:
    return str(SKIP_REASON_LABELS.get(str(reason_code or "").strip().upper(), "未知原因") or "未知原因")


def _advance_weekdays(ts: datetime, trading_days: int) -> datetime:
    current = ts
    remaining = max(0, int(trading_days))
    while remaining > 0:
        current = current + timedelta(days=1)
        if current.weekday() < 5:
            remaining -= 1
    return current


def _build_skip_metadata(snapshot: Dict[str, Any], bars: List[Any], horizon_days: int, reason: str) -> Dict[str, Any]:
    if str(reason or "").upper().strip() != "INSUFFICIENT_FORWARD_BARS":
        return {}
    snapshot_ts = _parse_ts(snapshot.get("ts") or snapshot.get("snapshot_ts"))
    if snapshot_ts is None or not bars:
        return {}
    start_idx = _first_bar_index_on_or_after(bars, snapshot_ts)
    if start_idx is None:
        return {}
    latest_history_ts = _bar_time(bars[-1])
    remaining_forward_bars = max(1, start_idx + max(1, int(horizon_days)) - (len(bars) - 1))
    estimated_ready_ts = _advance_weekdays(latest_history_ts or snapshot_ts, remaining_forward_bars)
    return {
        "remaining_forward_bars": int(remaining_forward_bars),
        "latest_history_ts": latest_history_ts.isoformat() if latest_history_ts is not None else "",
        "estimated_ready_ts": estimated_ready_ts.isoformat(),
    }


def _build_snapshot_outcome_result(
    snapshot: Dict[str, Any],
    bars: List[Any],
    horizon_days: int,
) -> tuple[Dict[str, Any] | None, str]:
    # 这里返回 (outcome, reason)，而不是简单的 None。
    # 这样第三阶段看到“没有校准样本”时，能继续追到具体是历史为空、样本不足，还是快照时间不在历史覆盖内。
    snapshot_ts = _parse_ts(snapshot.get("ts") or snapshot.get("snapshot_ts"))
    if snapshot_ts is None:
        return None, "MISSING_SNAPSHOT_TS"
    if not bars:
        return None, "NO_HISTORY_BARS"
    start_idx = _first_bar_index_on_or_after(bars, snapshot_ts)
    horizon = max(1, int(horizon_days))
    if start_idx is None:
        return None, "SNAPSHOT_NOT_IN_HISTORY"
    end_idx = start_idx + horizon
    if end_idx >= len(bars) or end_idx <= start_idx:
        return None, "INSUFFICIENT_FORWARD_BARS"

    direction = str(snapshot.get("direction") or "LONG").upper()
    start_bar = bars[start_idx]
    end_bar = bars[end_idx]
    start_close = float(getattr(start_bar, "close", 0.0) or 0.0)
    end_close = float(getattr(end_bar, "close", 0.0) or 0.0)
    if start_close <= 0.0 or end_close <= 0.0:
        return None, "INVALID_CLOSE_VALUES"

    path_returns = [
        _signed_return(float(getattr(bar, "close", 0.0) or 0.0), start_close, direction)
        for bar in bars[start_idx + 1 : end_idx + 1]
        if float(getattr(bar, "close", 0.0) or 0.0) > 0.0
    ]
    if not path_returns:
        return None, "EMPTY_FORWARD_PATH"

    future_return = float(path_returns[-1])
    max_drawdown = float(min(path_returns))
    max_runup = float(max(path_returns))
    outcome_ts = _bar_time(end_bar)
    if outcome_ts is None:
        return None, "INVALID_OUTCOME_TS"

    return {
        "snapshot_id": str(snapshot.get("snapshot_id") or "").strip(),
        "market": str(snapshot.get("market") or "").upper(),
        "portfolio_id": str(snapshot.get("portfolio_id") or ""),
        "symbol": str(snapshot.get("symbol") or "").upper(),
        "horizon_days": int(horizon_days),
        "snapshot_ts": snapshot_ts.isoformat(),
        "outcome_ts": outcome_ts.isoformat(),
        "direction": direction,
        "start_close": float(start_close),
        "end_close": float(end_close),
        "future_return": float(future_return),
        "max_drawdown": float(max_drawdown),
        "max_runup": float(max_runup),
        "outcome_label": _classify_outcome(future_return, max_drawdown),
        "details": {
            "stage": str(snapshot.get("stage") or ""),
            "action": str(snapshot.get("action") or ""),
            "score": float(snapshot.get("score", 0.0) or 0.0),
            "model_recommendation_score": float(snapshot.get("model_recommendation_score", snapshot.get("score", 0.0)) or 0.0),
            "execution_score": float(snapshot.get("execution_score", 0.0) or 0.0),
        },
    }, ""


def build_snapshot_outcome(snapshot: Dict[str, Any], bars: List[Any], horizon_days: int) -> Dict[str, Any] | None:
    outcome, _ = _build_snapshot_outcome_result(snapshot, bars, horizon_days)
    return outcome


def _build_skip_summary_rows(skip_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[tuple[str, str, str, str], Dict[str, Any]] = {}
    for row in list(skip_rows or []):
        key = (
            str(row.get("market") or "").upper(),
            str(row.get("portfolio_id") or ""),
            str(int(row.get("horizon_days") or 0)),
            str(row.get("skip_reason") or "").upper(),
        )
        item = grouped.setdefault(
            key,
            {
                "market": key[0],
                "portfolio_id": key[1],
                "horizon_days": int(key[2] or 0),
                "skip_reason": key[3],
                "skip_reason_label": _skip_reason_label(key[3]),
                "skip_count": 0,
                "symbol_count": 0,
                "sample_symbols": [],
                "oldest_snapshot_ts": "",
                "latest_snapshot_ts": "",
                "min_remaining_forward_bars": 0,
                "max_remaining_forward_bars": 0,
                "estimated_ready_start_ts": "",
                "estimated_ready_end_ts": "",
            },
        )
        item["skip_count"] = int(item.get("skip_count", 0) or 0) + 1
        symbols = set(str(item.get("_symbols_json") or "").split(",")) if item.get("_symbols_json") else set()
        symbol = str(row.get("symbol") or "").upper().strip()
        if symbol:
            symbols.add(symbol)
        item["_symbols_json"] = ",".join(sorted(x for x in symbols if x))
        item["symbol_count"] = int(len([x for x in symbols if x]))
        ts = str(row.get("snapshot_ts") or "")
        if ts:
            oldest = str(item.get("oldest_snapshot_ts") or "")
            latest = str(item.get("latest_snapshot_ts") or "")
            if not oldest or ts < oldest:
                item["oldest_snapshot_ts"] = ts
            if not latest or ts > latest:
                item["latest_snapshot_ts"] = ts
        remaining_forward_bars = int(row.get("remaining_forward_bars") or 0)
        if remaining_forward_bars > 0:
            current_min = int(item.get("min_remaining_forward_bars") or 0)
            current_max = int(item.get("max_remaining_forward_bars") or 0)
            item["min_remaining_forward_bars"] = remaining_forward_bars if current_min <= 0 else min(current_min, remaining_forward_bars)
            item["max_remaining_forward_bars"] = max(current_max, remaining_forward_bars)
        estimated_ready_ts = str(row.get("estimated_ready_ts") or "")
        if estimated_ready_ts:
            ready_start = str(item.get("estimated_ready_start_ts") or "")
            ready_end = str(item.get("estimated_ready_end_ts") or "")
            if not ready_start or estimated_ready_ts < ready_start:
                item["estimated_ready_start_ts"] = estimated_ready_ts
            if not ready_end or estimated_ready_ts > ready_end:
                item["estimated_ready_end_ts"] = estimated_ready_ts
        current_samples = list(item.get("sample_symbols") or [])
        if symbol and symbol not in current_samples and len(current_samples) < 6:
            current_samples.append(symbol)
            item["sample_symbols"] = current_samples
    rows: List[Dict[str, Any]] = []
    for item in grouped.values():
        out = dict(item)
        out.pop("_symbols_json", None)
        out["sample_symbols"] = ",".join(list(out.get("sample_symbols") or []))
        rows.append(out)
    rows.sort(
        key=lambda row: (
            -int(row.get("skip_count", 0) or 0),
            str(row.get("market", "") or ""),
            str(row.get("portfolio_id", "") or ""),
            str(row.get("skip_reason", "") or ""),
        )
    )
    return rows


def main() -> None:
    args = parse_args()
    market = resolve_market_code(getattr(args, "market", "")) or ""
    stage = str(args.stage or "").strip().lower()
    horizons = _parse_horizons(args.horizons)
    storage = Storage(str(_resolve_project_path(args.db)))

    pending_by_horizon: Dict[int, List[Dict[str, Any]]] = {}
    symbol_days: Dict[str, int] = {}
    now_utc = datetime.now(timezone.utc)
    for horizon in horizons:
        rows = storage.get_pending_investment_candidate_snapshots(
            market=market,
            portfolio_id=str(args.portfolio_id or ""),
            stage=stage,
            horizon_days=horizon,
            limit=int(args.limit),
        )
        pending_by_horizon[horizon] = rows
        for row in rows:
            symbol = str(row.get("symbol") or "").upper().strip()
            snapshot_ts = _parse_ts(row.get("ts"))
            if not symbol or snapshot_ts is None:
                continue
            age_days = max(30, (now_utc.date() - snapshot_ts.date()).days + int(horizon) + 30)
            symbol_days[symbol] = max(symbol_days.get(symbol, 0), age_days)

    bars_cache: Dict[str, List[Any]] = {}
    ibkr_loaders: Dict[str, _LabelingIbkrHistoryLoader] = {}
    try:
        for symbol, days in sorted(symbol_days.items()):
            market_code = ""
            for horizon_rows in pending_by_horizon.values():
                matched = next((row for row in horizon_rows if str(row.get("symbol") or "").upper().strip() == symbol), None)
                if matched is not None:
                    market_code = str(matched.get("market") or "").upper().strip()
                    break
            try:
                bars, _source = _load_daily_bars_for_labeling(symbol, market_code, max(90, int(days)), ibkr_loaders)
                bars_cache[symbol] = list(bars or [])
            except Exception as e:
                log.warning("snapshot labeling history failed symbol=%s: %s %s", symbol, type(e).__name__, e)
                bars_cache[symbol] = []
    finally:
        for loader in ibkr_loaders.values():
            loader.close()

    written_rows: List[Dict[str, Any]] = []
    skip_rows: List[Dict[str, Any]] = []
    skipped = 0
    for horizon, rows in pending_by_horizon.items():
        for row in rows:
            symbol = str(row.get("symbol") or "").upper().strip()
            outcome, skip_reason = _build_snapshot_outcome_result(row, bars_cache.get(symbol, []), horizon)
            if outcome is None:
                skipped += 1
                snapshot_ts = _parse_ts(row.get("ts") or row.get("snapshot_ts"))
                skip_rows.append(
                    {
                        "market": str(row.get("market") or "").upper(),
                        "portfolio_id": str(row.get("portfolio_id") or ""),
                        "symbol": symbol,
                        "snapshot_id": str(row.get("snapshot_id") or ""),
                        "snapshot_ts": snapshot_ts.isoformat() if snapshot_ts is not None else "",
                        "stage": str(row.get("stage") or ""),
                        "horizon_days": int(horizon),
                        "skip_reason": str(skip_reason or "UNKNOWN"),
                        "skip_reason_label": _skip_reason_label(skip_reason),
                        **_build_skip_metadata(row, bars_cache.get(symbol, []), horizon, skip_reason),
                    }
                )
                continue
            storage.upsert_investment_candidate_outcome(outcome)
            written_rows.append({k: v for k, v in outcome.items() if k != "details"})

    out_dir = _resolve_project_path(args.out_dir)
    out_dir = out_dir / (market.lower() if market else "all")
    out_dir.mkdir(parents=True, exist_ok=True)
    write_csv(str(out_dir / "investment_candidate_outcomes.csv"), written_rows)
    write_csv(str(out_dir / "investment_candidate_outcome_skip_details.csv"), skip_rows)
    skip_summary_rows = _build_skip_summary_rows(skip_rows)
    write_csv(str(out_dir / "investment_candidate_outcome_skip_summary.csv"), skip_summary_rows)
    label_counts = Counter(str(row.get("outcome_label") or "") for row in written_rows)
    horizon_counts = Counter(int(row.get("horizon_days") or 0) for row in written_rows)
    skip_reason_counts = Counter(str(row.get("skip_reason") or "").upper() for row in skip_rows)
    summary = {
        "generated_utc": datetime.now(timezone.utc).isoformat(),
        "market": market,
        "portfolio_id": str(args.portfolio_id or ""),
        "stage": stage,
        "horizons": horizons,
        "labeled_rows": int(len(written_rows)),
        "skipped_rows": int(skipped),
        "label_counts": dict(label_counts),
        "horizon_counts": {str(k): int(v) for k, v in horizon_counts.items()},
        "skip_reason_counts": {str(k): int(v) for k, v in skip_reason_counts.items()},
        "skip_reason_labels": {str(k): _skip_reason_label(k) for k in skip_reason_counts.keys()},
        "skip_summary_rows": skip_summary_rows,
    }
    write_json(str(out_dir / "investment_candidate_outcomes_summary.json"), summary)
    print(
        f"market={market or 'ALL'} stage={stage or 'ALL'} labeled_rows={len(written_rows)} "
        f"skipped_rows={skipped} horizons={','.join(str(x) for x in horizons)}"
    )
    print(f"outcomes_csv={out_dir / 'investment_candidate_outcomes.csv'}")
    print(f"summary_json={out_dir / 'investment_candidate_outcomes_summary.json'}")


if __name__ == "__main__":
    main()
