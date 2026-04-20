from __future__ import annotations

from decimal import Decimal, InvalidOperation
from typing import Any


DIP_SWING_MIN_PROJECTED_NET_PCT = Decimal("0.25")
DIP_SWING_NEAR_THRESHOLD_MAX_DEFICIT_PCT = Decimal("0.02")
DIP_SWING_NEAR_THRESHOLD_MIN_EXECUTION_QUALITY = Decimal("58")
DIP_SWING_NEAR_THRESHOLD_MIN_QUATERNION_QUALITY = Decimal("0.68")
DIP_SWING_NEAR_THRESHOLD_MIN_QUATERNION_STABILITY = Decimal("0.58")
DIP_SWING_RELU_GATE_MIN_SCORE = Decimal("62")
DIP_SWING_RELU_GATE_STRONG_SCORE = Decimal("78")
DIP_SWING_RELU_MAX_RELIEF_PCT = Decimal("0.025")
DIP_SWING_RELU_MAX_PENALTY_PCT = Decimal("0.030")
DIP_SWING_RELU_SOFT_MAX_DEFICIT_PCT = Decimal("0.030")
DIP_SWING_RELU_MIN_LIQUIDITY_SCORE = Decimal("3")
DIP_SWING_RELU_SLIP_WARN_PCT = Decimal("0.05")
DIP_SWING_MIN_EDGE_COST_RATIO = Decimal("1.9")
DIP_SWING_MIN_RANGE_COST_RATIO = Decimal("3.0")
DIP_SWING_MIN_ATR_COST_RATIO = Decimal("1.4")
DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT = Decimal("24")


def safe_decimal(value: Any, default: str = "0") -> Decimal:
    if value is None:
        return Decimal(default)
    if isinstance(value, Decimal):
        return value
    try:
        text = str(value).strip()
        if not text:
            return Decimal(default)
        return Decimal(text)
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def decimal_to_str(value: Any) -> str:
    return format(safe_decimal(value), "f")


def relu_decimal(value: Decimal | str | int | float | None) -> Decimal:
    return max(safe_decimal(value, "0"), Decimal("0"))


def relu_ratio(value: Decimal | str | int | float | None, width: Decimal | str | int | float) -> Decimal:
    span = safe_decimal(width, "1")
    if span <= 0:
        return Decimal("0")
    return relu_decimal(value) / span


def build_dip_swing_relu_gate(
    *,
    predicted_net_pct: Decimal,
    base_required_predicted_net_pct: Decimal,
    execution_quality_score: Decimal,
    quaternion_quality: Decimal,
    quaternion_stability: Decimal,
    liquidity_score: Decimal,
    edge_cost_ratio: Decimal,
    range_cost_ratio: Decimal,
    atr_cost_ratio: Decimal,
    recent_taker_fill_pct: Decimal,
    recent_abs_slip_pct: Decimal,
    liquidity_ready: bool,
    edge_cost_ready: bool,
    range_cost_ready: bool,
    atr_cost_ready: bool,
    symbol_performance_blocked: bool = False,
    symbol_taker_blocked: bool = False,
    symbol_pressure_blocked: bool = False,
) -> dict[str, Any]:
    predicted = max(safe_decimal(predicted_net_pct, "0"), Decimal("0"))
    base_required = max(safe_decimal(base_required_predicted_net_pct, "0"), Decimal("0"))
    execution_quality = safe_decimal(execution_quality_score, "0")
    quaternion_q = safe_decimal(quaternion_quality, "0")
    quaternion_s = safe_decimal(quaternion_stability, "0")
    liquidity = safe_decimal(liquidity_score, "0")
    edge_ratio = safe_decimal(edge_cost_ratio, "0")
    range_ratio = safe_decimal(range_cost_ratio, "0")
    atr_ratio = safe_decimal(atr_cost_ratio, "0")
    taker_fill = safe_decimal(recent_taker_fill_pct, "0")
    slip_pct = safe_decimal(recent_abs_slip_pct, "0")

    relief_pct = min(
        DIP_SWING_RELU_MAX_RELIEF_PCT,
        relu_ratio(execution_quality - Decimal("64"), Decimal("12")) * Decimal("0.004")
        + relu_ratio(quaternion_q - Decimal("0.72"), Decimal("0.04")) * Decimal("0.004")
        + relu_ratio(quaternion_s - Decimal("0.62"), Decimal("0.04")) * Decimal("0.003")
        + relu_ratio(edge_ratio - DIP_SWING_MIN_EDGE_COST_RATIO, Decimal("0.35")) * Decimal("0.003")
        + relu_ratio(range_ratio - DIP_SWING_MIN_RANGE_COST_RATIO, Decimal("0.60")) * Decimal("0.003")
        + relu_ratio(atr_ratio - DIP_SWING_MIN_ATR_COST_RATIO, Decimal("0.30")) * Decimal("0.003")
        + relu_ratio(liquidity - DIP_SWING_RELU_MIN_LIQUIDITY_SCORE, Decimal("1.50")) * Decimal("0.002")
    )
    penalty_pct = min(
        DIP_SWING_RELU_MAX_PENALTY_PCT,
        relu_ratio(Decimal("58") - execution_quality, Decimal("10")) * Decimal("0.005")
        + relu_ratio(taker_fill - DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT, Decimal("8")) * Decimal("0.006")
        + relu_ratio(slip_pct - DIP_SWING_RELU_SLIP_WARN_PCT, Decimal("0.02")) * Decimal("0.005")
    )
    required_predicted_net_pct = max(
        DIP_SWING_MIN_PROJECTED_NET_PCT,
        base_required + penalty_pct - relief_pct,
    )
    predicted_net_deficit_pct = max(required_predicted_net_pct - predicted, Decimal("0"))

    gate_score = (
        Decimal("50")
        + relu_ratio(predicted - base_required, Decimal("0.01")) * Decimal("10")
        + relu_ratio(execution_quality - DIP_SWING_NEAR_THRESHOLD_MIN_EXECUTION_QUALITY, Decimal("8")) * Decimal("12")
        + relu_ratio(quaternion_q - DIP_SWING_NEAR_THRESHOLD_MIN_QUATERNION_QUALITY, Decimal("0.05")) * Decimal("10")
        + relu_ratio(quaternion_s - DIP_SWING_NEAR_THRESHOLD_MIN_QUATERNION_STABILITY, Decimal("0.05")) * Decimal("8")
        + relu_ratio(edge_ratio - DIP_SWING_MIN_EDGE_COST_RATIO, Decimal("0.25")) * Decimal("8")
        + relu_ratio(range_ratio - DIP_SWING_MIN_RANGE_COST_RATIO, Decimal("0.40")) * Decimal("6")
        + relu_ratio(atr_ratio - DIP_SWING_MIN_ATR_COST_RATIO, Decimal("0.25")) * Decimal("5")
        + relu_ratio(liquidity - DIP_SWING_RELU_MIN_LIQUIDITY_SCORE, Decimal("1.25")) * Decimal("4")
        - relu_ratio(predicted_net_deficit_pct, Decimal("0.005")) * Decimal("8")
        - relu_ratio(taker_fill - DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT, Decimal("6")) * Decimal("8")
        - relu_ratio(slip_pct - DIP_SWING_RELU_SLIP_WARN_PCT, Decimal("0.02")) * Decimal("7")
    )
    if symbol_performance_blocked:
        gate_score -= Decimal("30")
    if symbol_taker_blocked:
        gate_score -= Decimal("22")
    if symbol_pressure_blocked:
        gate_score -= Decimal("18")
    gate_score = max(Decimal("0"), gate_score)

    hard_blocked = symbol_performance_blocked or symbol_taker_blocked or symbol_pressure_blocked
    soft_window_pct = min(
        DIP_SWING_RELU_SOFT_MAX_DEFICIT_PCT,
        DIP_SWING_NEAR_THRESHOLD_MAX_DEFICIT_PCT + relief_pct,
    )
    near_threshold_ready = (
        not hard_blocked
        and predicted_net_deficit_pct > 0
        and predicted_net_deficit_pct <= soft_window_pct
        and gate_score >= DIP_SWING_RELU_GATE_MIN_SCORE
        and liquidity_ready
        and edge_cost_ready
        and range_cost_ready
        and atr_cost_ready
    )
    allow_entry_ready = not hard_blocked and predicted >= required_predicted_net_pct

    if gate_score >= DIP_SWING_RELU_GATE_STRONG_SCORE:
        gate_label = "强放行"
    elif gate_score >= DIP_SWING_RELU_GATE_MIN_SCORE:
        gate_label = "接近放行"
    elif predicted_net_deficit_pct > 0:
        gate_label = "仍需等待"
    else:
        gate_label = "观察中"

    return {
        "requiredPredictedNetPct": required_predicted_net_pct,
        "predictedNetDeficitPct": predicted_net_deficit_pct,
        "nearThresholdReady": near_threshold_ready,
        "allowEntryReady": allow_entry_ready,
        "gateScore": gate_score,
        "gateLabel": gate_label,
        "thresholdReliefPct": relief_pct,
        "thresholdPenaltyPct": penalty_pct,
        "softWindowPct": soft_window_pct,
        "hardBlocked": hard_blocked,
    }


def build_execution_overlay_snapshot(
    analysis: dict[str, Any],
    *,
    base_score: Decimal,
) -> dict[str, Any]:
    predicted_net_pct = safe_decimal(analysis.get("predictedNetPct"), "0")
    required_predicted_net_pct = safe_decimal(analysis.get("requiredPredictedNetPct"), "0")
    threshold_delta_pct = predicted_net_pct - required_predicted_net_pct
    relu_gate_score = safe_decimal(analysis.get("reluGateScore"), "0")
    relu_gate_label = str(analysis.get("reluGateLabel") or "")
    decision_label = str(analysis.get("decisionLabel") or "")
    selected_symbol = str(analysis.get("selectedWatchlistSymbol") or "")
    hard_blockers = list(analysis.get("hardBlockers") or [])
    soft_blockers = list(analysis.get("softBlockers") or [])
    block_state_label = str(analysis.get("blockStateLabel") or "")
    near_threshold_ready = bool(analysis.get("nearThresholdReady"))
    allow_new_entries = bool(analysis.get("allowNewEntries"))
    candidate_count = int(analysis.get("candidateCount") or 0)
    market_candidate_count = int(analysis.get("marketCandidateCount") or 0)
    execution_quality_score = safe_decimal(analysis.get("executionQualityScore"), "0")

    execution_bonus = relu_gate_score * Decimal("0.006")
    execution_bonus += relu_ratio(threshold_delta_pct, Decimal("0.01")) * Decimal("0.60")
    execution_bonus += relu_ratio(execution_quality_score - Decimal("60"), Decimal("10")) * Decimal("0.40")
    execution_bonus += Decimal("1.60") if allow_new_entries else Decimal("0")
    execution_bonus += Decimal("0.70") if near_threshold_ready else Decimal("0")
    execution_bonus += Decimal("0.30") if candidate_count > 0 else Decimal("0")
    execution_bonus += Decimal("0.20") if market_candidate_count > 0 else Decimal("0")
    execution_bonus -= Decimal("1.20") * Decimal(min(len(hard_blockers), 2))
    execution_bonus -= Decimal("0.35") * Decimal(min(len(soft_blockers), 3))
    combined_score = base_score + execution_bonus

    return {
        "evaluated": True,
        "decisionLabel": decision_label,
        "selectedSymbol": selected_symbol,
        "predictedNetPct": decimal_to_str(predicted_net_pct),
        "requiredPredictedNetPct": decimal_to_str(required_predicted_net_pct),
        "thresholdDeltaPct": decimal_to_str(threshold_delta_pct),
        "reluGateScore": decimal_to_str(relu_gate_score),
        "reluGateLabel": relu_gate_label,
        "nearThresholdReady": near_threshold_ready,
        "allowNewEntries": allow_new_entries,
        "executionQualityScore": decimal_to_str(execution_quality_score),
        "candidateCount": candidate_count,
        "marketCandidateCount": market_candidate_count,
        "blockStateLabel": block_state_label,
        "hardBlockers": hard_blockers,
        "softBlockers": soft_blockers,
        "executionBonus": decimal_to_str(execution_bonus),
        "combinedScore": decimal_to_str(combined_score),
    }


__all__ = [
    "DIP_SWING_MIN_ATR_COST_RATIO",
    "DIP_SWING_MIN_EDGE_COST_RATIO",
    "DIP_SWING_MIN_PROJECTED_NET_PCT",
    "DIP_SWING_MIN_RANGE_COST_RATIO",
    "DIP_SWING_NEAR_THRESHOLD_MAX_DEFICIT_PCT",
    "DIP_SWING_NEAR_THRESHOLD_MIN_EXECUTION_QUALITY",
    "DIP_SWING_NEAR_THRESHOLD_MIN_QUATERNION_QUALITY",
    "DIP_SWING_NEAR_THRESHOLD_MIN_QUATERNION_STABILITY",
    "DIP_SWING_RELU_GATE_MIN_SCORE",
    "DIP_SWING_RELU_GATE_STRONG_SCORE",
    "DIP_SWING_RELU_MAX_PENALTY_PCT",
    "DIP_SWING_RELU_MAX_RELIEF_PCT",
    "DIP_SWING_RELU_MIN_LIQUIDITY_SCORE",
    "DIP_SWING_RELU_SLIP_WARN_PCT",
    "DIP_SWING_RELU_SOFT_MAX_DEFICIT_PCT",
    "DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT",
    "build_dip_swing_relu_gate",
    "build_execution_overlay_snapshot",
]
