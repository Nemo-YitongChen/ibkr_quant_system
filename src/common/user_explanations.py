from __future__ import annotations

from typing import Any, Dict, Tuple


def _text(value: Any) -> str:
    return str(value or "").strip()


def opportunity_user_explanation(row: Dict[str, Any]) -> Tuple[str, str]:
    status = _text(row.get("entry_status")).upper()
    detail = _text(row.get("entry_reason"))
    mapping = {
        "ENTRY_NOW": ("可开始分批", "当前价格回到计划进场带，可以开始小步分批。"),
        "ADD_ON_PULLBACK": ("回撤后可加仓", "已有持仓前提下，当前回撤位置允许温和加仓。"),
        "NEAR_ENTRY": ("接近进场区", "价格接近理想进场带，先继续观察确认。"),
        "WAIT_PULLBACK": ("先等回撤", "当前价格偏离理想进场带，先不追价。"),
        "WAIT_TREND": ("趋势未站稳", "价格仍低于中期趋势线，先等趋势重新站稳。"),
        "WAIT_EVENT": ("先避开事件窗口", "事件窗口临近，先等不确定性释放。"),
        "WAIT_MARKET_RULE": ("当前市场仅研究", "当前市场在本项目中仍以研究为主，不直接转成交易动作。"),
        "WAIT_ACCOUNT_RULE": ("账户规则限制", "当前账户先优先 ETF 或高流动性基础标的。"),
        "WAIT_DEFENSIVE_REGIME": ("防守阶段先观察", "当前市场偏防守，新开仓先降级为观察。"),
    }
    label, fallback = mapping.get(status, ("继续观察", "当前还不满足直接进场条件，先观察。"))
    return label, detail or fallback


def execution_user_explanation(row: Dict[str, Any]) -> Tuple[str, str]:
    status = _text(row.get("status")).upper()
    manual_review_status = _text(row.get("manual_review_status")).upper()
    shadow_review_status = _text(row.get("shadow_review_status")).upper()
    market_structure_status = _text(row.get("market_structure_review_status")).upper()
    risk_alert_status = _text(row.get("risk_alert_status")).upper()
    hotspot_status = _text(row.get("hotspot_penalty_status")).upper()
    quality_status = _text(row.get("quality_status")).upper()

    if hotspot_status == "DEFERRED" or status == "DEFERRED_EXECUTION_HOTSPOT":
        return "执行热点延后", _text(row.get("hotspot_penalty_reason")) or "当前时段同名标的执行压力偏高，先延后。"
    if risk_alert_status == "DEFERRED" or status == "DEFERRED_RISK_ALERT":
        return "组合风险偏高", _text(row.get("risk_alert_reason")) or "组合风险抬升，先延后新增交易。"
    if market_structure_status == "REVIEW_REQUIRED":
        return "账户规则限制", "当前账户规模先优先 ETF 或基础标的，单股扩仓先人工确认。"
    if shadow_review_status == "REVIEW_REQUIRED":
        return "模型保护期复核", "模型仍在保护期，先人工确认，不直接自动下单。"
    if manual_review_status == "REVIEW_REQUIRED":
        reason = _text(row.get("manual_review_reason"))
        if "exceeds auto-submit threshold" in reason:
            return "大额订单待人工确认", "单笔订单超出自动提交阈值，先人工确认。"
        return "需要人工确认", reason or "这笔订单当前不适合直接自动提交。"
    if quality_status == "LOW_QUALITY" or status == "BLOCKED_QUALITY":
        return "信号质量不足", "当前信号质量或执行准备度不足，先不自动下单。"
    if status == "BLOCKED_MARKET_RULE":
        market_rule_status = _text(row.get("market_rule_status")).upper()
        market_rule_reason = _text(row.get("market_rule_reason"))
        if market_rule_status == "BLOCKED_RESEARCH_ONLY":
            return "当前市场仅研究", market_rule_reason or "当前市场仍处于研究阶段，不直接进入自动执行。"
        if market_rule_status == "BLOCKED_BOARD_LOT":
            return "整手规则限制", market_rule_reason or "当前下单数量不符合整手规则，先不自动下单。"
        if market_rule_status == "BLOCKED_SHORT_ENTRY":
            return "市场规则限制", market_rule_reason or "当前市场规则不支持这类自动卖空或回转交易。"
        return "市场规则限制", market_rule_reason or "当前订单触发市场结构或交易规则限制，先不自动下单。"
    if status == "BLOCKED_EDGE":
        return (
            "边际收益不够覆盖成本",
            _text(row.get("edge_gate_reason")) or "当前预期边际收益还不足以覆盖执行成本和安全缓冲，先不下单。",
        )
    if status == "BLOCKED_OPPORTUNITY":
        opp_row = {
            "entry_status": _text(row.get("opportunity_status")),
            "entry_reason": _text(row.get("opportunity_reason")),
        }
        return opportunity_user_explanation(opp_row)
    if status == "SUBMITTED":
        return "已发单等待成交", "订单已经发往 IB Gateway，等待成交回报。"
    if status == "PLANNED":
        return "计划已生成", "本轮计划已经生成，等待是否提交。"
    return "执行中", _text(row.get("manual_review_reason") or row.get("opportunity_reason") or row.get("reason")) or "当前按计划执行。"


def guard_user_explanation(row: Dict[str, Any]) -> Tuple[str, str]:
    reason = _text(row.get("reason")).lower()
    adaptive_note = _text(row.get("adaptive_strategy_note"))
    if reason.startswith("guard_take_profit"):
        return "先锁定利润", adaptive_note or "当前回撤已触发止盈规则，先锁定部分利润。"
    if "stop" in reason:
        return "先控制回撤", adaptive_note or "当前价格触发保护性退出规则，先控制仓位风险。"
    return "保护性调整", adaptive_note or _text(row.get("reason")) or "当前按保护性规则处理仓位。"


def annotate_opportunity_user_explanation(row: Dict[str, Any]) -> Dict[str, Any]:
    label, detail = opportunity_user_explanation(row)
    row["user_reason_label"] = label
    row["user_reason"] = detail
    return row


def annotate_execution_user_explanation(row: Dict[str, Any]) -> Dict[str, Any]:
    label, detail = execution_user_explanation(row)
    row["user_reason_label"] = label
    row["user_reason"] = detail
    return row


def annotate_guard_user_explanation(row: Dict[str, Any]) -> Dict[str, Any]:
    label, detail = guard_user_explanation(row)
    row["user_reason_label"] = label
    row["user_reason"] = detail
    return row
