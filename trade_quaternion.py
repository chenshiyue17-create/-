from __future__ import annotations

import math
from decimal import Decimal, InvalidOperation
from typing import Any


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        if isinstance(value, Decimal):
            return float(value)
        return float(value or 0.0)
    except (TypeError, ValueError, InvalidOperation):
        return default


def _to_decimal(value: float) -> Decimal:
    return Decimal(str(round(value, 6)))


def _clamp(value: float, lower: float, upper: float) -> float:
    return max(lower, min(upper, value))


def _smooth(value: float, scale: float) -> float:
    safe_scale = scale if abs(scale) > 1e-9 else 1.0
    return math.tanh(value / safe_scale)


def _normalize_quaternion(w: float, x: float, y: float, z: float) -> tuple[tuple[float, float, float, float], float]:
    magnitude = math.sqrt(w * w + x * x + y * y + z * z)
    if magnitude <= 1e-9:
        return (0.0, 0.0, 0.0, 0.0), 0.0
    return (w / magnitude, x / magnitude, y / magnitude, z / magnitude), magnitude


def _dot(left: tuple[float, float, float, float], right: tuple[float, float, float, float]) -> float:
    return sum(a * b for a, b in zip(left, right))


def _signed_component_label(value: float, positive_label: str, negative_label: str) -> str:
    return positive_label if value >= 0 else negative_label


RESEARCH_TARGET = _normalize_quaternion(0.62, 0.48, 0.56, 0.32)[0]
EXECUTION_TARGET = _normalize_quaternion(0.66, 0.50, 0.52, 0.22)[0]


def build_research_quaternion(summary: dict[str, Any]) -> dict[str, Any]:
    return_pct = _to_float(summary.get("returnPct"))
    drawdown_pct = abs(_to_float(summary.get("maxDrawdownPct")))
    win_rate_pct = _to_float(summary.get("winRatePct"))
    trade_count = max(_to_float(summary.get("tradeCount"), 0.0), 0.0)

    edge_component = _smooth(return_pct, 6.0)
    consistency_component = _smooth(win_rate_pct - 52.0, 18.0)
    stability_component = _smooth(3.5 - drawdown_pct, 2.5)
    efficiency_component = _smooth((return_pct / max(trade_count, 1.0)) * 14.0, 1.4)

    normalized, magnitude = _normalize_quaternion(
        edge_component,
        consistency_component,
        stability_component,
        efficiency_component,
    )
    quality = _clamp((_dot(normalized, RESEARCH_TARGET) + 1.0) / 2.0, 0.0, 1.0)
    stability = _clamp(
        0.45
        + (stability_component * 0.25)
        + (consistency_component * 0.18)
        + (efficiency_component * 0.12),
        0.0,
        1.0,
    )
    drift_penalty = _clamp(
        max(0.0, -stability_component)
        + max(0.0, -efficiency_component) * 0.6
        + max(0.0, -edge_component) * 0.4,
        0.0,
        2.5,
    )
    score_adjustment = (
        (quality - 0.5) * 3.2
        + max(0.0, stability_component) * 1.1
        - drift_penalty * 1.15
    )
    dominant = max(
        (
            (abs(edge_component), _signed_component_label(edge_component, "收益牵引", "收益衰减")),
            (abs(consistency_component), _signed_component_label(consistency_component, "胜率稳定", "胜率失真")),
            (abs(stability_component), _signed_component_label(stability_component, "回撤可控", "回撤拖累")),
            (abs(efficiency_component), _signed_component_label(efficiency_component, "交易效率高", "交易效率低")),
        ),
        key=lambda item: item[0],
    )[1]

    return {
        "w": _to_decimal(normalized[0]),
        "x": _to_decimal(normalized[1]),
        "y": _to_decimal(normalized[2]),
        "z": _to_decimal(normalized[3]),
        "magnitude": _to_decimal(magnitude),
        "quality": _to_decimal(quality),
        "stability": _to_decimal(stability),
        "driftPenalty": _to_decimal(drift_penalty),
        "scoreAdjustment": _to_decimal(score_adjustment),
        "dominantLabel": dominant,
        "biasLabel": "顺势放大" if normalized[0] >= 0 and normalized[2] >= 0 else "需要再校准",
    }


def build_execution_quaternion(
    *,
    predicted_net_pct: Any,
    required_net_pct: Any,
    net_edge_pct: Any,
    volatility_pct: Any,
    execution_quality_score: Any,
    taker_fill_pct: Any,
    abs_slip_pct: Any,
    funding_rate_pct: Any,
    basis_pct: Any,
    liq_buffer_pct: Any,
    recent_net_pnl: Any,
) -> dict[str, Any]:
    predicted_net = _to_float(predicted_net_pct)
    required_net = max(_to_float(required_net_pct), 0.01)
    net_edge = _to_float(net_edge_pct)
    volatility = _to_float(volatility_pct)
    execution_quality = _to_float(execution_quality_score)
    taker_fill = _to_float(taker_fill_pct)
    abs_slip = _to_float(abs_slip_pct)
    funding = abs(_to_float(funding_rate_pct))
    basis = abs(_to_float(basis_pct))
    liq_buffer = _to_float(liq_buffer_pct)
    recent_pnl = _to_float(recent_net_pnl)

    edge_component = _smooth(min(predicted_net, net_edge) - required_net, 0.22)
    volatility_component = _smooth(volatility - required_net * 1.6, 0.55)
    execution_drag = (taker_fill / 55.0) + (abs_slip / 0.08) + max(0.0, 0.4 - execution_quality / 6.0)
    execution_component = _smooth((execution_quality / 4.2) - execution_drag, 0.9)
    pressure_balance = (
        (liq_buffer - 18.0) / 18.0
        - (funding / 0.05)
        - (basis / 0.25)
        + _clamp(recent_pnl / 40.0, -1.2, 0.8)
    )
    pressure_component = _smooth(pressure_balance, 1.0)

    normalized, magnitude = _normalize_quaternion(
        edge_component,
        volatility_component,
        execution_component,
        pressure_component,
    )
    quality = _clamp((_dot(normalized, EXECUTION_TARGET) + 1.0) / 2.0, 0.0, 1.0)
    stability = _clamp(
        0.42
        + (volatility_component * 0.18)
        + (execution_component * 0.25)
        + (pressure_component * 0.15),
        0.0,
        1.0,
    )
    drift_penalty = _clamp(
        max(0.0, -execution_component)
        + max(0.0, -pressure_component) * 0.8
        + max(0.0, -edge_component) * 0.55,
        0.0,
        2.5,
    )
    required_edge_boost_pct = max(
        0.0,
        required_net * max(0.06, drift_penalty * 0.20)
        + max(0.0, 0.62 - quality) * 0.18,
    )
    maker_bias = quality < 0.70 or execution_component < 0.12 or pressure_component < 0.0
    allow_aggressive_entry = (
        quality >= 0.78
        and stability >= 0.58
        and execution_component > 0.10
        and pressure_component > -0.08
    )
    dominant = max(
        (
            (abs(edge_component), _signed_component_label(edge_component, "净优势扩张", "净优势不足")),
            (abs(volatility_component), _signed_component_label(volatility_component, "波动充足", "波动不够")),
            (abs(execution_component), _signed_component_label(execution_component, "执行顺滑", "执行拖累")),
            (abs(pressure_component), _signed_component_label(pressure_component, "拥挤缓和", "拥挤升温")),
        ),
        key=lambda item: item[0],
    )[1]

    return {
        "w": _to_decimal(normalized[0]),
        "x": _to_decimal(normalized[1]),
        "y": _to_decimal(normalized[2]),
        "z": _to_decimal(normalized[3]),
        "magnitude": _to_decimal(magnitude),
        "quality": _to_decimal(quality),
        "stability": _to_decimal(stability),
        "driftPenalty": _to_decimal(drift_penalty),
        "requiredEdgeBoostPct": _to_decimal(required_edge_boost_pct),
        "makerBias": maker_bias,
        "allowAggressiveEntry": allow_aggressive_entry,
        "dominantLabel": dominant,
        "biasLabel": "顺势扩张" if normalized[0] >= 0 and normalized[2] >= 0 else "缩量校准",
    }
