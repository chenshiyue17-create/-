from __future__ import annotations

import copy
from decimal import Decimal, InvalidOperation
from typing import Any, Callable

from strategy_execution_gate import build_execution_overlay_snapshot


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


def review_research_candidates_with_execution_gate(
    entries: list[dict[str, Any]],
    client: Any,
    *,
    default_config_factory: Callable[[], dict[str, Any]],
    enforce_strategy: Callable[[dict[str, Any]], dict[str, Any]],
    analysis_builder: Callable[[dict[str, Any], Any], dict[str, Any]],
    limit: int = 3,
) -> tuple[list[dict[str, Any]], dict[str, Any] | None]:
    if not entries:
        return entries, None

    reviewed: list[dict[str, Any]] = []
    evaluated: list[dict[str, Any]] = []
    review_limit = max(1, min(limit, len(entries)))

    for index, entry in enumerate(entries):
        enriched = copy.deepcopy(entry)
        base_score = safe_decimal(entry.get("score"), "0")
        if index < review_limit:
            full_config = default_config_factory()
            full_config.update(copy.deepcopy(entry.get("fullConfig") or entry.get("config") or {}))
            full_config = enforce_strategy(full_config)
            try:
                analysis = analysis_builder(full_config, client)
                overlay = build_execution_overlay_snapshot(analysis, base_score=base_score)
            except Exception as exc:
                overlay = {
                    "evaluated": False,
                    "error": str(exc),
                    "combinedScore": str(base_score),
                }
            enriched["executionOverlay"] = overlay
            enriched["combinedScore"] = str(overlay.get("combinedScore") or entry.get("score") or "0")
            if overlay.get("evaluated"):
                evaluated.append(enriched)
        reviewed.append(enriched)

    if not evaluated:
        return reviewed, None

    selected = max(
        evaluated,
        key=lambda item: safe_decimal(
            ((item.get("executionOverlay") or {}).get("combinedScore") or item.get("combinedScore") or item.get("score")),
            "0",
        ),
    )
    return reviewed, selected


__all__ = ["review_research_candidates_with_execution_gate"]
