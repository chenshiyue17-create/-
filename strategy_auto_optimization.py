from __future__ import annotations

import copy
import json
import secrets
import threading
from dataclasses import dataclass
from typing import Any, Callable


@dataclass
class StrategyAutoOptimizationDependencies:
    automation_state: Any
    automation_config: Any
    config: Any
    mirofish_autopilot_config: Any
    automation_engine: Any
    now_local_iso: Callable[[], str]
    normalize_mirofish_autopilot_config: Callable[[dict[str, Any]], dict[str, Any]]
    is_remote_execution_enabled: Callable[[dict[str, Any]], bool]
    remote_gateway_request: Callable[..., Any]
    research_optimize: Callable[[dict[str, Any], dict[str, Any], Any], dict[str, Any]]
    runtime_research_options: Callable[[dict[str, Any]], dict[str, Any]]
    build_public_client: Callable[[dict[str, Any]], Any]
    merge_optimized_automation_config: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]
    validate_automation_config: Callable[[dict[str, Any]], tuple[bool, str, dict[str, Any]]]
    build_automation_config_diff: Callable[[dict[str, Any], dict[str, Any]], Any]
    normalize_remote_automation_config_payload: Callable[[dict[str, Any], dict[str, Any]], dict[str, Any]]


def normalize_focus_symbol_token(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    if not value:
        return ""
    if "-" in value:
        value = value.split("-", 1)[0]
    return "".join(ch for ch in value if ch.isalnum())


def build_focus_seed_automation_config(config: dict[str, Any], focus_symbol: str) -> dict[str, Any]:
    focused = copy.deepcopy(config)
    normalized_symbol = normalize_focus_symbol_token(focus_symbol)
    if not normalized_symbol:
        return focused
    overrides = copy.deepcopy(focused.get("watchlistOverrides") or {})
    symbol_override = copy.deepcopy(overrides.get(normalized_symbol) or {})
    focused["watchlistSymbols"] = normalized_symbol
    focused["watchlistOverrides"] = {normalized_symbol: symbol_override} if symbol_override else {}
    focused["spotInstId"] = f"{normalized_symbol}-USDT"
    focused["swapInstId"] = f"{normalized_symbol}-USDT-SWAP"
    focused["focusSymbol"] = normalized_symbol
    return focused


class StrategyAutoOptimizationManager:
    def __init__(self, deps: StrategyAutoOptimizationDependencies) -> None:
        self.deps = deps
        self.lock = threading.RLock()
        self.state = self._blank_state()

    def _blank_state(self) -> dict[str, Any]:
        return {
            "running": False,
            "phase": "idle",
            "message": "等待自动推演完成后再执行策略优化。",
            "startedAt": "",
            "updatedAt": "",
            "completedAt": "",
            "error": "",
            "optimizationId": "",
            "sourceSimulationId": "",
            "focusSymbol": "",
            "ordersCount": 0,
            "applied": False,
            "appliedAt": "",
            "bestConfig": {},
            "researchSummary": {},
            "researchPipeline": {},
            "executionOverlay": {},
            "notes": [],
            "metrics": {},
        }

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self.state)

    def _sync_automation_state(self) -> None:
        try:
            state_snapshot = self.snapshot()
            self.deps.automation_state.update(lambda current: current.update({"strategyOptimization": state_snapshot}))
        except Exception:
            return

    def _update(self, **patch: Any) -> dict[str, Any]:
        with self.lock:
            next_state = copy.deepcopy(self.state)
            next_state.update({k: v for k, v in patch.items() if v is not None})
            next_state["updatedAt"] = self.deps.now_local_iso()
            self.state = next_state
        self._sync_automation_state()
        return self.snapshot()

    def _set_running(
        self,
        *,
        simulation_id: str,
        focus_symbol: str,
        orders_count: int,
    ) -> dict[str, Any]:
        with self.lock:
            if self.state.get("running"):
                raise RuntimeError("已有策略优化任务在运行。")
            self.state = self._blank_state()
            self.state.update(
                {
                    "running": True,
                    "phase": "preparing",
                    "message": "正在根据最新推演结果和订单快照生成优化方案。",
                    "startedAt": self.deps.now_local_iso(),
                    "updatedAt": self.deps.now_local_iso(),
                    "optimizationId": f"opt_{secrets.token_hex(5)}",
                    "sourceSimulationId": simulation_id,
                    "focusSymbol": focus_symbol,
                    "ordersCount": orders_count,
                }
            )
        self._sync_automation_state()
        return self.snapshot()

    def reset_for_simulation(self, *, simulation_id: str, focus_symbol: str, orders_count: int) -> dict[str, Any]:
        with self.lock:
            self.state = self._blank_state()
            self.state.update(
                {
                    "phase": "pending-simulation",
                    "message": "推演已启动，等待模拟完成后自动优化策略。",
                    "sourceSimulationId": simulation_id,
                    "focusSymbol": focus_symbol,
                    "ordersCount": orders_count,
                    "updatedAt": self.deps.now_local_iso(),
                }
            )
        self._sync_automation_state()
        return self.snapshot()

    def _attempt_resume_automation_after_optimization(self) -> dict[str, Any]:
        config = self.deps.config.current()
        result = {
            "attempted": False,
            "started": False,
            "alreadyRunning": False,
            "remote": self.deps.is_remote_execution_enabled(config),
            "stateSource": "",
            "detail": "",
        }
        autopilot_config = self.deps.normalize_mirofish_autopilot_config(self.deps.mirofish_autopilot_config.current())
        if not autopilot_config.get("autoResumeAutomation", True):
            result["detail"] = "自动恢复量化运行已关闭。"
            return result
        if result["remote"]:
            result["attempted"] = True
            try:
                state_response = self.deps.remote_gateway_request(
                    config,
                    "GET",
                    "/api/automation/state",
                    timeout=6.0,
                )
                state_payload = state_response.json() if state_response.content else {}
                state = state_payload.get("state") or {}
                result["stateSource"] = str(state_payload.get("remoteStateSource") or state.get("stateSource") or "")
                if bool(state.get("running")):
                    result["started"] = True
                    result["alreadyRunning"] = True
                    result["detail"] = "远端量化引擎已在运行，无需重复启动。"
                    return result
                start_response = self.deps.remote_gateway_request(
                    config,
                    "POST",
                    "/api/automation/start",
                    body=b"{}",
                    content_type="application/json; charset=utf-8",
                    timeout=10.0,
                )
                start_payload = start_response.json() if start_response.content else {}
                if not start_response.ok or start_payload.get("ok") is False:
                    raise RuntimeError(str(start_payload.get("error") or f"远端返回 {start_response.status_code}"))
                result["started"] = True
                result["detail"] = "已自动恢复远端量化运行。"
                return result
            except Exception as exc:
                result["detail"] = f"自动恢复远端量化运行失败: {exc}"
                return result
        local_runtime_config = self.deps.config.current()
        has_local_credentials = all(
            bool(str(local_runtime_config.get(key) or "").strip())
            for key in ("apiKey", "secretKey", "passphrase")
        )
        if not has_local_credentials:
            result["detail"] = "本地未保存 API 凭据，已跳过自动恢复量化运行。"
            return result
        result["attempted"] = True
        if self.deps.automation_engine.snapshot().get("running"):
            result["started"] = True
            result["alreadyRunning"] = True
            result["detail"] = "本地量化引擎已在运行，无需重复启动。"
            return result
        try:
            self.deps.automation_engine.start(autostart=False)
        except Exception as exc:
            result["detail"] = f"自动恢复本地量化运行失败: {exc}"
            return result
        result["started"] = True
        result["detail"] = "已自动恢复本地量化运行。"
        return result

    def run(
        self,
        *,
        mode: str,
        simulation_id: str,
        focus_symbol: str,
        orders_count: int,
        simulation_result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        self._set_running(
            simulation_id=simulation_id,
            focus_symbol=focus_symbol,
            orders_count=orders_count,
        )
        try:
            current_config = self.deps.automation_config.current()
            optimization_seed_config = build_focus_seed_automation_config(current_config, focus_symbol)
            runtime_config = self.deps.config.current()
            self._update(phase="optimizing", message="正在回测并搜索更优参数。")
            research = self.deps.research_optimize(
                optimization_seed_config,
                self.deps.runtime_research_options(optimization_seed_config),
                self.deps.build_public_client(runtime_config),
            )
            best_config = self.deps.merge_optimized_automation_config(
                optimization_seed_config,
                copy.deepcopy(research.get("bestConfig") or optimization_seed_config),
            )
            ok, message, normalized = self.deps.validate_automation_config(best_config)
            if not ok:
                raise RuntimeError(f"优化结果校验失败: {message}")
            current_normalized = self.deps.validate_automation_config(current_config)[2]
            changed = json.dumps(normalized, sort_keys=True, ensure_ascii=False) != json.dumps(
                current_normalized,
                sort_keys=True,
                ensure_ascii=False,
            )
            config_diff = self.deps.build_automation_config_diff(current_normalized, normalized)
            if self.deps.is_remote_execution_enabled(runtime_config):
                remote_response = self.deps.remote_gateway_request(
                    runtime_config,
                    "POST",
                    "/api/automation/config",
                    body=json.dumps(normalized, ensure_ascii=False).encode("utf-8"),
                    content_type="application/json; charset=utf-8",
                    timeout=20.0,
                )
                remote_payload = remote_response.json() if remote_response.content else {}
                if not remote_response.ok or remote_payload.get("ok") is False:
                    remote_error = remote_payload.get("error") or f"远端返回 {remote_response.status_code}"
                    raise RuntimeError(f"远端策略配置回写失败: {remote_error}")
                mirrored = self.deps.normalize_remote_automation_config_payload(remote_payload, normalized)
                self.deps.automation_config.replace(mirrored)
            else:
                self.deps.automation_config.replace(normalized)
            applied_at = self.deps.now_local_iso()
            resume_result: dict[str, Any] = {}
            autopilot_config = self.deps.normalize_mirofish_autopilot_config(self.deps.mirofish_autopilot_config.current())
            if autopilot_config.get("autoResumeAutomation", True):
                self._update(phase="resuming-automation", message="最佳参数已写回，正在恢复量化运行。")
                resume_result = self._attempt_resume_automation_after_optimization()
            applied_strategy = {
                "stage": "strategy-optimization",
                "title": "自动推演已回写策略",
                "detail": (
                    f"来源模拟 {simulation_id} · 焦点 {focus_symbol or '未指定'} · "
                    f"EMA {normalized.get('fastEma')}/{normalized.get('slowEma')} · "
                    f"{normalized.get('swapStrategyMode')} · 杠杆 {normalized.get('swapLeverage')}x"
                ),
                "appliedAt": applied_at,
            }
            final_state = self._update(
                running=False,
                phase="completed",
                message=(
                    "策略优化完成，最佳配置已自动回写并恢复量化运行。"
                    if resume_result.get("started")
                    else (
                        f"策略优化完成，最佳配置已自动回写；{resume_result.get('detail')}"
                        if resume_result.get("detail")
                        else "策略优化完成，最佳配置已自动回写。"
                    )
                ),
                completedAt=applied_at,
                applied=changed,
                appliedAt=applied_at,
                baseConfig=current_normalized,
                bestConfig=normalized,
                configDiff=config_diff,
                researchSummary=copy.deepcopy(research.get("summary") or {}),
                researchPipeline=copy.deepcopy(research.get("pipeline") or {}),
                executionOverlay=copy.deepcopy(((research.get("pipeline") or {}).get("executionOverlay") or {})),
                notes=list(research.get("notes") or []),
                metrics={
                    "sampleCount": int(research.get("sampleCount") or 0),
                    "focusSymbol": focus_symbol,
                    "ordersCount": orders_count,
                    "simulationResult": simulation_result or {},
                    "resumeResult": resume_result,
                },
            )
            self.deps.automation_state.update(
                lambda current: current.update(
                    {
                        "research": research,
                        "lastAppliedStrategy": applied_strategy,
                        "strategyOptimization": final_state,
                    }
                )
            )
            return final_state
        except Exception as exc:
            return self._update(
                running=False,
                phase="failed",
                message=f"策略优化失败: {exc}",
                completedAt=self.deps.now_local_iso(),
                error=str(exc),
            )


__all__ = [
    "StrategyAutoOptimizationDependencies",
    "StrategyAutoOptimizationManager",
]
