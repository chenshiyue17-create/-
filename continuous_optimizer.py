#!/usr/bin/env python3
from __future__ import annotations

import itertools
import json
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
STATUS_PATH = DATA_DIR / "optimizer-status.json"
BEST_PATH = DATA_DIR / "optimizer-best-strategy.json"
LOG_PATH = DATA_DIR / "optimizer-runner.log"
DEFAULT_BASE_URL = os.environ.get("OKX_LOCAL_APP_BASE", "http://127.0.0.1:8765")
DEADLINE_HOUR = int(os.environ.get("OKX_OPTIMIZER_DEADLINE_HOUR", "12"))
TARGET_RETURN_PCT = float(os.environ.get("OKX_OPTIMIZER_TARGET_RETURN", "96"))
MIN_SLEEP_SECONDS = int(os.environ.get("OKX_OPTIMIZER_MIN_SLEEP", "20"))


RUNNING = True


def now_str() -> str:
    return datetime.now().isoformat(timespec="seconds")


def deadline_ts(now: datetime | None = None) -> float:
    now = now or datetime.now()
    target = now.replace(hour=DEADLINE_HOUR, minute=0, second=0, microsecond=0)
    if now.timestamp() >= target.timestamp():
        from datetime import timedelta
        target = target + timedelta(days=1)
    return target.timestamp()


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def append_log(message: str) -> None:
    line = f"[{now_str()}] {message}\n"
    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(line)


def on_stop(_signum, _frame) -> None:
    global RUNNING
    RUNNING = False
    append_log("收到停止信号，准备安全退出。")


signal.signal(signal.SIGTERM, on_stop)
signal.signal(signal.SIGINT, on_stop)


@dataclass
class Champion:
    objective: float = -10**9
    return_pct: float = -10**9
    score: float = -10**9
    payload: dict[str, Any] | None = None


def load_existing_champion() -> Champion:
    if not BEST_PATH.exists():
        return Champion()
    try:
        payload = json.loads(BEST_PATH.read_text(encoding="utf-8"))
        objective = float(payload.get("objective", -10**9))
        return_pct = float(payload.get("returnPct", -10**9))
        score = float(payload.get("score", -10**9))
        return Champion(objective=objective, return_pct=return_pct, score=score, payload=payload)
    except Exception:
        return Champion()


def score_candidate(summary: dict[str, Any]) -> tuple[float, float, float]:
    return_pct = float(summary.get("returnPct") or 0.0)
    drawdown = abs(float(summary.get("maxDrawdownPct") or 0.0))
    win_rate = float(summary.get("winRatePct") or 0.0)
    score = float(summary.get("score") or 0.0)
    objective = return_pct * 1.25 + win_rate * 0.08 - drawdown * 1.6 + score * 0.2
    return objective, return_pct, score


def request_json(session: requests.Session, method: str, url: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    response = session.request(method, url, json=payload, timeout=120)
    response.raise_for_status()
    data = response.json()
    if data.get("ok") is False:
        raise RuntimeError(data.get("error") or "接口返回失败")
    return data


def load_config(session: requests.Session, base: str) -> dict[str, Any]:
    data = request_json(session, "GET", f"{base}/api/automation/config")
    return data.get("config") or {}


def optimization_payloads(base_config: dict[str, Any]) -> list[dict[str, Any]]:
    bars = ["1m", "5m", "15m", "1H"]
    presets = ["dual_engine", "btc_lotto"]
    depths = ["quick", "standard"]
    history_limits = [180, 240, 360]
    loop_options = [2, 3, 4]
    race_sizes = [10, 12]
    toggle_sets = [
        {"spotEnabled": True, "swapEnabled": True, "swapStrategyMode": "long_only"},
        {"spotEnabled": True, "swapEnabled": True, "swapStrategyMode": "trend_follow"},
        {"spotEnabled": True, "swapEnabled": False, "swapStrategyMode": "long_only"},
        {"spotEnabled": False, "swapEnabled": True, "swapStrategyMode": "long_only"},
    ]

    payloads: list[dict[str, Any]] = []
    for preset, bar, depth, history_limit, loops, race_size, toggles in itertools.product(
        presets, bars, depths, history_limits, loop_options, race_sizes, toggle_sets
    ):
        payload = dict(base_config)
        payload.update(
            {
                "strategyPreset": preset,
                "bar": bar,
                "historyLimit": history_limit,
                "optimizationDepth": depth,
                "evolutionLoops": loops,
                "raceSize": race_size,
                "includeAltBars": True,
                "enableHybrid": True,
                "enableFineTune": True,
            }
        )
        payload.update(toggles)
        if preset == "btc_lotto":
            payload.update(
                {
                    "spotInstId": "BTC-USDT",
                    "swapInstId": "BTC-USDT-SWAP",
                    "pollSeconds": 8,
                    "cooldownSeconds": 30,
                    "stopLossPct": "0.7",
                    "takeProfitPct": "1.4",
                    "maxDailyLossPct": "2.0",
                    "spotQuoteBudget": "30",
                    "spotMaxExposure": "120",
                    "swapLeverage": "5",
                }
            )
        payloads.append(payload)
    return payloads


def persist_champion(session: requests.Session, base: str, champion_payload: dict[str, Any]) -> None:
    best_config = champion_payload["research"].get("bestConfig") or {}
    if best_config:
        request_json(session, "POST", f"{base}/api/automation/config", best_config)
    try:
        export_data = request_json(session, "POST", f"{base}/api/automation/research/export", {"index": 0})
    except Exception as exc:
        export_data = {"ok": False, "error": str(exc)}
    champion_payload["export"] = export_data.get("export") if isinstance(export_data, dict) else None
    write_json(BEST_PATH, champion_payload)


def main() -> int:
    session = requests.Session()
    base = DEFAULT_BASE_URL.rstrip("/")
    base_config = load_config(session, base)
    payloads = optimization_payloads(base_config)
    champion = load_existing_champion()
    started_at = time.time()
    deadline = deadline_ts()
    iteration = 0
    append_log(f"持续优化任务启动，长期运行，当前统计窗口截止到 {datetime.fromtimestamp(deadline).isoformat(timespec='seconds')}，候选 {len(payloads)} 组。")

    while RUNNING:
        if time.time() >= deadline:
            deadline = deadline_ts()
            append_log(f"进入新的统计窗口，新的截止时间为 {datetime.fromtimestamp(deadline).isoformat(timespec='seconds')}。")
        payload = payloads[iteration % len(payloads)]
        iteration += 1
        status: dict[str, Any] = {
            "running": True,
            "startedAt": datetime.fromtimestamp(started_at).isoformat(timespec="seconds"),
            "deadline": datetime.fromtimestamp(deadline).isoformat(timespec="seconds"),
            "iteration": iteration,
            "payloadHint": {
                "strategyPreset": payload.get("strategyPreset"),
                "bar": payload.get("bar"),
                "historyLimit": payload.get("historyLimit"),
                "optimizationDepth": payload.get("optimizationDepth"),
                "raceSize": payload.get("raceSize"),
                "evolutionLoops": payload.get("evolutionLoops"),
                "spotEnabled": payload.get("spotEnabled"),
                "swapEnabled": payload.get("swapEnabled"),
                "swapStrategyMode": payload.get("swapStrategyMode"),
            },
            "targetReturnPct": TARGET_RETURN_PCT,
        }
        try:
            optimize = request_json(session, "POST", f"{base}/api/automation/research/optimize", payload)
            research = optimize.get("research") or {}
            analysis = request_json(session, "POST", f"{base}/api/automation/analyze", {})
            top = (research.get("leaderboard") or [{}])[0]
            summary = research.get("summary") or {}
            objective, return_pct, score = score_candidate(summary)
            status.update(
                {
                    "lastRunAt": now_str(),
                    "lastStrategyName": top.get("name") or "",
                    "lastReturnPct": return_pct,
                    "lastScore": score,
                    "lastDrawdownPct": float(summary.get("maxDrawdownPct") or 0.0),
                    "lastDecision": (analysis.get("analysis") or {}).get("decisionLabel", ""),
                    "progressTowardTargetPct": round((return_pct / TARGET_RETURN_PCT) * 100, 2) if TARGET_RETURN_PCT > 0 else 0,
                }
            )

            if objective > champion.objective:
                champion = Champion(objective=objective, return_pct=return_pct, score=score, payload={
                    "capturedAt": now_str(),
                    "objective": objective,
                    "returnPct": return_pct,
                    "score": score,
                    "payloadHint": status["payloadHint"],
                    "research": research,
                    "analysis": analysis.get("analysis") or {},
                })
                persist_champion(session, base, champion.payload)
                append_log(
                    f"刷新冠军: {top.get('name')} | return={return_pct:.2f}% | score={score:.2f} | decision={(analysis.get('analysis') or {}).get('decisionLabel', '-')}"
                )
                status["championUpdated"] = True
            else:
                status["championUpdated"] = False

            if champion.payload:
                status["best"] = {
                    "name": ((champion.payload.get("research") or {}).get("leaderboard") or [{}])[0].get("name", ""),
                    "returnPct": champion.return_pct,
                    "score": champion.score,
                    "capturedAt": champion.payload.get("capturedAt"),
                }

        except Exception as exc:
            status.update({"lastRunAt": now_str(), "error": str(exc)})
            append_log(f"本轮失败: {exc}")

        write_json(STATUS_PATH, status)
        remaining = max(int(deadline - time.time()), 0)
        sleep_for = min(MIN_SLEEP_SECONDS, remaining)
        time.sleep(max(sleep_for, 1))

    final_status = {
        "running": False,
        "finishedAt": now_str(),
        "deadline": datetime.fromtimestamp(deadline).isoformat(timespec="seconds"),
        "targetReturnPct": TARGET_RETURN_PCT,
        "best": None,
    }
    if champion.payload:
        final_status["best"] = {
            "name": ((champion.payload.get("research") or {}).get("leaderboard") or [{}])[0].get("name", ""),
            "returnPct": champion.return_pct,
            "score": champion.score,
            "capturedAt": champion.payload.get("capturedAt"),
        }
    write_json(STATUS_PATH, final_status)
    append_log("持续优化任务结束。")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
