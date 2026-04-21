from __future__ import annotations

import copy
import csv
import json
import secrets
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

import requests


@dataclass
class MiroFishAutoSimulationDependencies:
    config: Any
    mirofish_runtime: Any
    strategy_auto_optimization: Any
    now_local_iso: Callable[[], str]
    build_runtime_snapshot: Callable[..., Any]
    runtime_snapshot_type: type
    mirofish_backend_port: int
    mirofish_backend_dir: Path
    mirofish_runtime_dir: Path
    mirofish_app_base: str


class MiroFishAutoSimulationManager:
    def __init__(self, deps: MiroFishAutoSimulationDependencies) -> None:
        self.deps = deps
        self.lock = threading.RLock()
        self.state: dict[str, Any] = self._blank_state()

    def _blank_state(self) -> dict[str, Any]:
        return {
            "running": False,
            "taskId": "",
            "mode": "",
            "modeLabel": "",
            "phase": "idle",
            "progress": 0,
            "message": "可根据当前订单或策略一键发起自动推演。",
            "startedAt": "",
            "updatedAt": "",
            "completedAt": "",
            "error": "",
            "projectId": "",
            "graphId": "",
            "graphTaskId": "",
            "prepareTaskId": "",
            "simulationId": "",
            "simulationPath": "",
            "simulationUrl": "",
            "seedPath": "",
            "projectName": "",
            "ordersCount": 0,
            "focusSymbol": "",
            "result": {},
        }

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self.state)

    def _update(self, **patch: Any) -> dict[str, Any]:
        with self.lock:
            next_state = copy.deepcopy(self.state)
            next_state.update({k: v for k, v in patch.items() if v is not None})
            next_state["updatedAt"] = self.deps.now_local_iso()
            self.state = next_state
            return copy.deepcopy(self.state)

    def _set_running(self, mode: str) -> dict[str, Any]:
        mode_label = "订单推演" if mode == "orders" else "策略推演"
        task_id = f"mirofish-auto-{secrets.token_hex(4)}"
        with self.lock:
            if self.state.get("running"):
                raise RuntimeError("已有自动推演任务在运行，请等待当前任务完成。")
            self.state = self._blank_state()
            self.state.update(
                {
                    "running": True,
                    "taskId": task_id,
                    "mode": mode,
                    "modeLabel": mode_label,
                    "phase": "queued",
                    "progress": 1,
                    "message": f"{mode_label}任务已排队，正在准备运行态和推演材料。",
                    "startedAt": self.deps.now_local_iso(),
                    "updatedAt": self.deps.now_local_iso(),
                }
            )
            state = copy.deepcopy(self.state)
        try:
            self.deps.strategy_auto_optimization.reset_for_simulation(
                simulation_id="",
                focus_symbol="",
                orders_count=0,
            )
        except Exception:
            pass
        return state

    def _backend_url(self, path: str) -> str:
        suffix = path if path.startswith("/") else f"/{path}"
        return f"http://127.0.0.1:{self.deps.mirofish_backend_port}{suffix}"

    def _backend_request(
        self,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None = None,
        data: dict[str, Any] | None = None,
        files: dict[str, Any] | None = None,
        timeout: float = 120.0,
    ) -> dict[str, Any]:
        response = requests.request(
            method.upper(),
            self._backend_url(path),
            json=json_body,
            data=data,
            files=files,
            timeout=timeout,
        )
        try:
            payload = response.json()
        except Exception as exc:
            raise RuntimeError(f"MiroFish 接口返回无效响应: {exc}") from exc
        if not response.ok or payload.get("success") is False:
            raise RuntimeError(str(payload.get("error") or payload.get("message") or f"{method} {path} 失败"))
        return payload

    def _format_order_time(self, order: dict[str, Any]) -> str:
        for field_name in ("fillTime", "cTime", "uTime"):
            raw = str(order.get(field_name) or "").strip()
            if not raw:
                continue
            try:
                ts = int(raw)
                if ts > 10_000_000_000:
                    ts = ts / 1000.0
                return datetime.fromtimestamp(ts, tz=timezone.utc).astimezone().strftime("%m-%d %H:%M:%S")
            except Exception:
                continue
        return ""

    def _decimal_text(self, value: Any, digits: int = 2) -> str:
        try:
            quant = Decimal(1).scaleb(-digits)
            return str(Decimal(str(value)).quantize(quant))
        except Exception:
            return str(value or "0")

    def _top_symbol_rows(self, orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows: dict[str, dict[str, Any]] = {}
        for order in orders:
            symbol = str(order.get("instId") or "").strip()
            if not symbol:
                continue
            bucket = rows.setdefault(
                symbol,
                {"instId": symbol, "count": 0, "notional": Decimal("0"), "fees": Decimal("0"), "pnl": Decimal("0")},
            )
            bucket["count"] += 1
            for field_name, key in (("fillNotionalUsd", "notional"), ("fillFee", "fees"), ("fillPnl", "pnl")):
                try:
                    bucket[key] += Decimal(str(order.get(field_name) or "0"))
                except Exception:
                    continue
        return sorted(rows.values(), key=lambda item: (abs(item["pnl"]), item["count"]), reverse=True)[:6]

    def _build_seed_material(self, mode: str) -> dict[str, Any]:
        snapshot = self.deps.build_runtime_snapshot(self.deps.config.current(), include_account=True)
        automation_state = snapshot.automation_state or {}
        orders_payload = snapshot.orders_payload(limit=24)
        orders = list(orders_payload.get("orders") or [])
        analysis = automation_state.get("analysis") or {}
        summary = orders_payload.get("journal") or {}
        account_summary = (snapshot.account_payload or {}).get("summary") or {}
        focus_symbol = str(
            analysis.get("selectedWatchlistSymbol")
            or automation_state.get("focusSymbol")
            or ((automation_state.get("markets") or {}).get("swap") or {}).get("instId")
            or "BTC-USDT-SWAP"
        )
        predicted_net = self._decimal_text(analysis.get("predictedNetPct") or "0", 2)
        exec_cost = self._decimal_text(summary.get("executionCostFloorPct") or analysis.get("executionCostFloorPct") or "0", 4)
        total_fees = self._decimal_text(summary.get("totalFees") or "0", 2)
        net_pnl = self._decimal_text(summary.get("netPnl") or "0", 2)
        realized_pnl = self._decimal_text(summary.get("realizedPnl") or "0", 2)
        equity = self._decimal_text(account_summary.get("totalEq") or automation_state.get("currentEq") or "0", 2)
        recent_symbols = [row["instId"] for row in self._top_symbol_rows(orders)]
        symbol_rows = self._top_symbol_rows(orders)
        order_lines: list[str] = []
        for order in orders[:12]:
            order_lines.append(
                "| {time} | {inst} | {side} | {fill_px} | {fill_sz} | {pnl} | {fee} |".format(
                    time=self._format_order_time(order) or "-",
                    inst=order.get("instId") or "-",
                    side=order.get("side") or "-",
                    fill_px=order.get("fillPx") or order.get("avgPx") or "-",
                    fill_sz=order.get("fillSz") or order.get("sz") or "-",
                    pnl=self._decimal_text(order.get("fillPnl") or order.get("pnl") or "0", 2),
                    fee=self._decimal_text(order.get("fillFee") or order.get("fee") or "0", 2),
                )
            )
        symbol_lines: list[str] = []
        for row in symbol_rows:
            symbol_lines.append(
                f"- {row['instId']}: {row['count']} 笔, 订单金额约 {self._decimal_text(row['notional'], 2)}U, 已实现 {self._decimal_text(row['pnl'], 2)}U, 手续费 {self._decimal_text(row['fees'], 2)}U"
            )

        role_lines = [
            "- TrendCaptain（趋势交易员）: 追逐延续信号，偏好顺势加仓。",
            "- MeanReverter（反转交易员）: 专门捕捉过热后的回归和止盈压力。",
            "- DepthKeeper（做市商）: 关注盘口深度、滑点和 taker 冲击。",
            "- RiskSentinel（风控官）: 控制回撤、杠杆与资金占用约束。",
            "- FundingWatcher（资金费观察者）: 追踪资金费率和持仓拥挤度。",
            "- NewsPulse（情绪观察者）: 根据新闻、社群情绪与价格波动调整预期。",
        ]

        mode_title = "根据最近订单复盘并反推更优执行" if mode == "orders" else "根据当前策略和运行态推演未来演化"
        simulation_requirement = (
            f"请围绕 {focus_symbol} 搭建一组小规模市场参与者仿真，"
            f"重点推演 {mode_title}。要求保留手续费、滑点、maker/taker、方向切换、仓位与风控约束。"
            "请让不同角色在 6 到 8 轮互动内给出各自决策、彼此影响、以及最终对策略表现的反馈。"
            "输出应强调哪些行为导致亏损、哪些执行切换可能改善结果，以及是否应继续当前策略。"
        )

        title = f"OKX 自动推演 · {'订单视角' if mode == 'orders' else '策略视角'} · {focus_symbol}"
        markdown = "\n".join(
            [
                f"# {title}",
                "",
                f"- 生成时间: {self.deps.now_local_iso()}",
                f"- 模式: {'订单推演' if mode == 'orders' else '策略推演'}",
                f"- 当前策略: {analysis.get('selectedStrategyName') or automation_state.get('modeText') or '利润循环'}",
                f"- 当前决策: {analysis.get('decisionLabel') or automation_state.get('statusText') or '运行中'}",
                f"- 当前方向: {analysis.get('plannedSideLabel') or '未明确'}",
                f"- 焦点标的: {focus_symbol}",
                "",
                "## 账户与执行摘要",
                f"- 总权益: {equity} USDT",
                f"- 近场净结果: {net_pnl} USDT",
                f"- 已实现: {realized_pnl} USDT",
                f"- 手续费: {total_fees} USDT",
                f"- 预期净优势: {predicted_net}%",
                f"- 真实执行成本地板: {exec_cost}%",
                f"- 最近订单数: {len(orders)}",
                "",
                "## 当前策略与运行态",
                f"- 模式文本: {automation_state.get('modeText') or '-'}",
                f"- 摘要: {analysis.get('summary') or '-'}",
                f"- 详细策略: {analysis.get('selectedStrategyDetail') or '-'}",
                f"- 最后动作: {((automation_state.get('markets') or {}).get('swap') or {}).get('lastAction') or '-'}",
                f"- 最后说明: {((automation_state.get('markets') or {}).get('swap') or {}).get('lastMessage') or '-'}",
                "",
                "## 主要订单分布",
                *(symbol_lines or ["- 暂无有效订单分布。"]),
                "",
                "## 最近订单样本",
                "| 时间 | 标的 | 方向 | 成交价 | 数量 | 盈亏 | 手续费 |",
                "| --- | --- | --- | --- | --- | --- | --- |",
                *(order_lines or ["| - | - | - | - | - | - | - |"]),
                "",
                "## 仿真核心角色",
                *role_lines,
                "",
                "## 仿真目标",
                simulation_requirement,
            ]
        )
        return {
            "snapshot": snapshot,
            "orders": orders,
            "focusSymbol": focus_symbol,
            "projectName": f"{'订单' if mode == 'orders' else '策略'}推演-{focus_symbol}-{datetime.now().strftime('%m%d-%H%M')}",
            "simulationRequirement": simulation_requirement,
            "markdown": markdown,
            "recentSymbols": recent_symbols,
        }

    def _wait_for_graph_task(self, task_id: str, deadline: float) -> dict[str, Any]:
        while time.time() < deadline:
            payload = self._backend_request("GET", f"/api/graph/task/{task_id}", timeout=20.0)
            data = payload.get("data") or {}
            status = str(data.get("status") or "")
            progress = int(data.get("progress") or 0)
            message = str(data.get("message") or "正在构建图谱")
            self._update(phase="building-graph", progress=min(55, 20 + progress // 2), message=message)
            if status == "completed":
                return data
            if status == "failed":
                raise RuntimeError(str(data.get("error") or message or "图谱构建失败"))
            time.sleep(2.0)
        raise RuntimeError("等待图谱构建超时")

    def _wait_for_prepare(self, simulation_id: str, task_id: str | None, deadline: float) -> dict[str, Any]:
        state_path = self._simulation_dir(simulation_id) / "state.json"
        while time.time() < deadline:
            payload = self._backend_request(
                "POST",
                "/api/simulation/prepare/status",
                json_body={k: v for k, v in {"simulation_id": simulation_id, "task_id": task_id}.items() if v},
                timeout=20.0,
            )
            data = payload.get("data") or {}
            status = str(data.get("status") or "")
            progress = int((data.get("task") or {}).get("progress") or 0)
            task_message = str((data.get("task") or {}).get("message") or data.get("message") or "正在准备模拟")
            self._update(phase="preparing-simulation", progress=min(85, 55 + progress // 3), message=task_message)
            if status == "ready" or data.get("already_prepared"):
                return data
            if state_path.exists():
                try:
                    state_payload = json.loads(state_path.read_text(encoding="utf-8"))
                except Exception:
                    state_payload = {}
                state_status = str(state_payload.get("status") or "")
                if state_status == "failed":
                    raise RuntimeError(str(state_payload.get("error") or task_message or "模拟准备失败"))
            if status == "completed" and not self._simulation_artifacts_ready(simulation_id):
                raise RuntimeError(task_message or "准备任务已结束，但没有生成可运行配置")
            if status == "failed":
                task = data.get("task") or {}
                raise RuntimeError(str(task.get("error") or task_message or "模拟准备失败"))
            time.sleep(2.5)
        raise RuntimeError("等待模拟准备超时")

    def _simulation_dir(self, simulation_id: str) -> Path:
        return self.deps.mirofish_backend_dir / "uploads" / "simulations" / simulation_id

    def _project_meta_path(self, project_id: str) -> Path:
        return self.deps.mirofish_backend_dir / "uploads" / "projects" / project_id / "project.json"

    def _local_graph_path(self, graph_id: str) -> Path:
        return self.deps.mirofish_backend_dir / "uploads" / "local_graphs" / f"{graph_id}.json"

    def _use_fast_local_graph(self) -> bool:
        try:
            status = self.deps.mirofish_runtime.snapshot()
        except Exception:
            status = {}
        return str(status.get("graphBackend") or "").strip().lower() == "local"

    def _build_fast_local_graph_payload(
        self,
        *,
        graph_id: str,
        graph_name: str,
        ontology: dict[str, Any],
        seed: dict[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        created_at = self.deps.now_local_iso()
        episode_uuid = secrets.token_hex(16)
        focus_symbol = str(seed.get("focusSymbol") or "BTC")
        snapshot_obj = seed.get("snapshot")
        if isinstance(snapshot_obj, self.deps.runtime_snapshot_type):
            snapshot = snapshot_obj.automation_state or {}
            snapshot_account = (snapshot_obj.account_payload or {}).get("summary") or {}
        elif isinstance(snapshot_obj, dict):
            snapshot = snapshot_obj
            snapshot_account = ((snapshot_obj.get("account") or {}).get("summary") or {})
        else:
            snapshot = {}
            snapshot_account = {}
        summary = snapshot.get("summary") or {}
        analysis = snapshot.get("analysis") or {}
        current_decision = str((analysis.get("decisionLabel") or summary.get("decisionLabel") or "观察")).strip()
        current_direction = str((analysis.get("decisionSideLabel") or summary.get("decisionSideLabel") or "中性")).strip()
        total_eq = summary.get("totalEq") or snapshot_account.get("totalEq")
        expected_net_advantage = analysis.get("predictedNetPct")
        execution_cost_floor = analysis.get("executionCostFloorPct")
        last_action = str((snapshot.get("lastAppliedStrategy") or {}).get("title") or "").strip()
        last_note = str((analysis.get("lastMessage") or summary.get("lastMessage") or "")).strip()
        roles: list[tuple[str, str, str, dict[str, Any], str]] = [
            (
                "OKX",
                "Organization",
                f"当前{mode == 'orders' and '订单' or '策略'}推演依附的交易与执行平台。",
                {"context": "OKX 自动推演", "report_focus": f"{focus_symbol} {'订单' if mode == 'orders' else '策略'}视角"},
                "platform",
            ),
            (
                f"{focus_symbol} 利润循环",
                "StrategyEngine",
                "当前策略；空仓直开，持仓同向继续循环，单笔净赚 1U+ 就平。",
                {
                    "mode": "订单推演" if mode == "orders" else "策略推演",
                    "current_decision": current_decision,
                    "current_direction": current_direction,
                    "focus_symbol": focus_symbol,
                    "profit_take_threshold": "1U+",
                    "expected_net_advantage": f"{expected_net_advantage}%",
                    "real_execution_cost_floor": f"{execution_cost_floor}%",
                    "last_action": last_action,
                    "last_note": last_note,
                },
                "strategy",
            ),
            (
                f"{focus_symbol} 执行桌",
                "ExecutionDesk",
                "负责当前币种下单、撤单与成交跟踪的执行席位。",
                {
                    "execution_style": current_decision,
                    "focus_symbol": focus_symbol,
                    "recent_orders": len(seed.get("orders") or []),
                },
                "desk",
            ),
            (
                "风险观察员",
                "RiskSentinel",
                "关注回撤、杠杆、滑点与资金占用约束。",
                {
                    "risk_mode": "自动风控",
                    "account_equity": total_eq,
                    "limit_note": str((summary.get("riskReason") or analysis.get("riskReason") or "观察手续费和回撤")).strip(),
                },
                "risk",
            ),
            (
                f"{focus_symbol} 做市商",
                "MarketMaker",
                "对执行桌提供被动流动性并影响成交质量。",
                {"quote_style": "maker-first", "symbol_scope": focus_symbol},
                "maker",
            ),
            (
                "趋势推动者",
                "TrendTrader",
                "放大顺势信号、推动趋势延续。",
                {"preferred_side": current_direction, "trigger_note": current_decision},
                "trend",
            ),
            (
                "反转承接者",
                "ContrarianTrader",
                "在价格过热时提供反向流动性和止盈承接。",
                {"reversion_anchor": "短线扩张后的回归", "tolerance": "中等"},
                "contra",
            ),
            (
                "资金费观察者",
                "FundingWatcher",
                "观察资金费、基差与拥挤度变化。",
                {"funding_bias": "跟踪资金费和基差", "crowding_signal": "关注多空拥挤"},
                "funding",
            ),
        ]
        nodes: list[dict[str, Any]] = []
        node_ids: dict[str, str] = {}
        for name, node_type, node_summary, attributes, key in roles:
            uuid_value = secrets.token_hex(16)
            node_ids[key] = uuid_value
            nodes.append(
                {
                    "uuid": uuid_value,
                    "name": name,
                    "labels": ["Entity", node_type],
                    "summary": node_summary,
                    "attributes": attributes,
                    "created_at": created_at,
                }
            )
        edges = [
            {
                "uuid": secrets.token_hex(16),
                "name": "IMPLEMENTS_STRATEGY_FOR",
                "fact": f"{focus_symbol} 执行桌执行当前利润循环策略。",
                "fact_type": "IMPLEMENTS_STRATEGY_FOR",
                "source_node_uuid": node_ids["desk"],
                "target_node_uuid": node_ids["strategy"],
                "attributes": {"context": "自动推演快路径"},
                "created_at": created_at,
                "valid_at": None,
                "invalid_at": None,
                "expired_at": None,
                "episodes": [episode_uuid],
            },
            {
                "uuid": secrets.token_hex(16),
                "name": "SUPERVISES_RISK_FOR",
                "fact": "风险观察员持续监督执行席位的回撤和仓位约束。",
                "fact_type": "SUPERVISES_RISK_FOR",
                "source_node_uuid": node_ids["risk"],
                "target_node_uuid": node_ids["desk"],
                "attributes": {"context": "自动推演快路径"},
                "created_at": created_at,
                "valid_at": None,
                "invalid_at": None,
                "expired_at": None,
                "episodes": [episode_uuid],
            },
            {
                "uuid": secrets.token_hex(16),
                "name": "QUOTES_LIQUIDITY_TO",
                "fact": f"{focus_symbol} 做市商为执行桌提供盘口流动性。",
                "fact_type": "QUOTES_LIQUIDITY_TO",
                "source_node_uuid": node_ids["maker"],
                "target_node_uuid": node_ids["desk"],
                "attributes": {"context": "自动推演快路径"},
                "created_at": created_at,
                "valid_at": None,
                "invalid_at": None,
                "expired_at": None,
                "episodes": [episode_uuid],
            },
            {
                "uuid": secrets.token_hex(16),
                "name": "COMPETES_WITH",
                "fact": "趋势推动者与反转承接者围绕短线方向竞争。",
                "fact_type": "COMPETES_WITH",
                "source_node_uuid": node_ids["trend"],
                "target_node_uuid": node_ids["contra"],
                "attributes": {"context": "自动推演快路径"},
                "created_at": created_at,
                "valid_at": None,
                "invalid_at": None,
                "expired_at": None,
                "episodes": [episode_uuid],
            },
        ]
        return {
            "graph_id": graph_id,
            "name": graph_name,
            "description": f"{focus_symbol} {'订单' if mode == 'orders' else '策略'}自动推演本地图谱",
            "created_at": created_at,
            "ontology": ontology or {},
            "nodes": nodes,
            "edges": edges,
            "episodes": [
                {
                    "uuid": episode_uuid,
                    "chunk_count": 1,
                    "processed": True,
                    "created_at": created_at,
                }
            ],
        }

    def _create_fast_local_graph(
        self,
        *,
        project_id: str,
        graph_name: str,
        ontology: dict[str, Any],
        seed: dict[str, Any],
        mode: str,
    ) -> dict[str, str]:
        graph_id = f"mirofish_local_{secrets.token_hex(8)}"
        graph_payload = self._build_fast_local_graph_payload(
            graph_id=graph_id,
            graph_name=graph_name,
            ontology=ontology,
            seed=seed,
            mode=mode,
        )
        graph_path = self._local_graph_path(graph_id)
        graph_path.parent.mkdir(parents=True, exist_ok=True)
        graph_path.write_text(json.dumps(graph_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        task_id = f"local-fast-{secrets.token_hex(6)}"
        project_meta_path = self._project_meta_path(project_id)
        if project_meta_path.exists():
            try:
                project_payload = json.loads(project_meta_path.read_text(encoding="utf-8"))
            except Exception:
                project_payload = {}
            project_payload.update(
                {
                    "graph_id": graph_id,
                    "graph_build_task_id": task_id,
                    "status": "graph_completed",
                    "updated_at": self.deps.now_local_iso(),
                    "error": None,
                }
            )
            project_meta_path.write_text(json.dumps(project_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return {"graph_id": graph_id, "task_id": task_id}

    def _simulation_artifacts_ready(self, simulation_id: str) -> bool:
        sim_dir = self._simulation_dir(simulation_id)
        required = [
            sim_dir / "state.json",
            sim_dir / "simulation_config.json",
            sim_dir / "reddit_profiles.json",
            sim_dir / "twitter_profiles.csv",
        ]
        return all(path.exists() for path in required)

    def _build_local_simulation_config(
        self,
        *,
        simulation_id: str,
        project_id: str,
        graph_id: str,
        seed: dict[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        focus_symbol = str(seed.get("focusSymbol") or "BTC-USDT-SWAP")
        recent_symbols = [str(item or "").strip() for item in (seed.get("recentSymbols") or []) if str(item or "").strip()]
        hot_topics = list(dict.fromkeys(([focus_symbol] + recent_symbols)[:6]))
        roles = [
            ("TrendCaptain", "趋势交易员，偏好追随延续信号。"),
            ("MeanReverter", "反转交易员，专门捕捉过热回归。"),
            ("DepthKeeper", "做市观察者，关注盘口深度和滑点。"),
            ("RiskSentinel", "风控官，控制杠杆、回撤和仓位。"),
            ("FundingWatcher", "资金费观察者，评估拥挤交易。"),
            ("NewsPulse", "情绪观察者，结合舆情与波动反应。"),
        ]
        agent_configs: list[dict[str, Any]] = []
        for index, (name, description) in enumerate(roles, start=1):
            agent_configs.append(
                {
                    "agent_id": index,
                    "agent_name": name,
                    "bio": description,
                    "focus_symbols": hot_topics[:3] or [focus_symbol],
                    "platform_preference": "twitter" if index % 2 else "reddit",
                    "activity_level": round(0.55 + index * 0.05, 2),
                    "influence_weight": round(1.0 + index * 0.18, 2),
                }
            )
        total_rounds = 6 if mode == "orders" else 8
        total_hours = 72 if mode == "orders" else 96
        minutes_per_round = max(30, int(total_hours * 60 / max(total_rounds, 1)))
        return {
            "simulation_id": simulation_id,
            "project_id": project_id,
            "graph_id": graph_id,
            "simulation_requirement": str(seed.get("simulationRequirement") or "").strip(),
            "generated_at": self.deps.now_local_iso(),
            "llm_model": "codex-local",
            "time_config": {
                "total_simulation_hours": total_hours,
                "minutes_per_round": minutes_per_round,
            },
            "agent_configs": agent_configs,
            "event_config": {
                "initial_posts": [
                    {
                        "platform": "twitter",
                        "content": f"{focus_symbol} 近期订单表现与执行成本值得重点复盘。",
                        "poster_agent_id": 1,
                    },
                    {
                        "platform": "reddit",
                        "content": f"围绕 {focus_symbol} 的策略推演已经启动，重点观察手续费、滑点与方向切换。",
                        "poster_agent_id": 2,
                    },
                ],
                "hot_topics": hot_topics,
            },
            "twitter_config": {"enabled": True},
            "reddit_config": {"enabled": True},
        }

    def _write_local_profiles(self, sim_dir: Path, agent_configs: list[dict[str, Any]]) -> None:
        reddit_profiles = []
        for agent in agent_configs:
            reddit_profiles.append(
                {
                    "agent_id": agent["agent_id"],
                    "username": f"{agent['agent_name'].lower()}_{agent['agent_id']}",
                    "profile": agent.get("bio") or "",
                    "persona": agent.get("bio") or "",
                    "focus_symbols": agent.get("focus_symbols") or [],
                }
            )
        (sim_dir / "reddit_profiles.json").write_text(
            json.dumps(reddit_profiles, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        with (sim_dir / "twitter_profiles.csv").open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=["agent_id", "username", "display_name", "bio", "focus_symbols"],
            )
            writer.writeheader()
            for agent in agent_configs:
                writer.writerow(
                    {
                        "agent_id": agent["agent_id"],
                        "username": f"{agent['agent_name'].lower()}_{agent['agent_id']}",
                        "display_name": agent["agent_name"],
                        "bio": agent.get("bio") or "",
                        "focus_symbols": ",".join(agent.get("focus_symbols") or []),
                    }
                )

    def _ensure_local_simulation_artifacts(
        self,
        *,
        simulation_id: str,
        project_id: str,
        graph_id: str,
        seed: dict[str, Any],
        mode: str,
    ) -> dict[str, Any]:
        sim_dir = self._simulation_dir(simulation_id)
        sim_dir.mkdir(parents=True, exist_ok=True)
        config = self._build_local_simulation_config(
            simulation_id=simulation_id,
            project_id=project_id,
            graph_id=graph_id,
            seed=seed,
            mode=mode,
        )
        (sim_dir / "simulation_config.json").write_text(
            json.dumps(config, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        self._write_local_profiles(sim_dir, list(config.get("agent_configs") or []))
        state_path = sim_dir / "state.json"
        state_payload: dict[str, Any] = {}
        if state_path.exists():
            try:
                state_payload = json.loads(state_path.read_text(encoding="utf-8"))
            except Exception:
                state_payload = {}
        state_payload.update(
            {
                "simulation_id": simulation_id,
                "project_id": project_id,
                "graph_id": graph_id,
                "enable_twitter": True,
                "enable_reddit": True,
                "status": "ready",
                "entities_count": len(config.get("agent_configs") or []),
                "profiles_count": len(config.get("agent_configs") or []),
                "entity_types": ["SyntheticTrader"],
                "config_generated": True,
                "config_reasoning": "本地图谱没有可用实体，已回退到基于订单与策略快照的最小可运行仿真配置。",
                "updated_at": self.deps.now_local_iso(),
                "error": "",
            }
        )
        state_path.write_text(json.dumps(state_payload, ensure_ascii=False, indent=2), encoding="utf-8")
        return config

    def _wait_for_run_completion(self, simulation_id: str, deadline: float) -> dict[str, Any]:
        last_status = "idle"
        while time.time() < deadline:
            payload = self._backend_request(
                "GET",
                f"/api/simulation/{simulation_id}/run-status",
                timeout=20.0,
            )
            data = payload.get("data") or {}
            runner_status = str(data.get("runner_status") or "idle")
            progress_percent = int(data.get("progress_percent") or 0)
            current_round = int(data.get("current_round") or 0)
            total_rounds = int(data.get("total_rounds") or 0)
            total_actions = int(data.get("total_actions_count") or 0)
            status_message = (
                f"模拟运行中：第 {current_round}/{max(total_rounds, 1)} 轮，动作 {total_actions}，进度 {progress_percent}%"
                if runner_status == "running"
                else (
                    f"模拟已完成：共 {max(current_round, total_rounds)} 轮，动作 {total_actions}"
                    if runner_status == "completed"
                    else f"正在等待模拟启动（状态 {runner_status}）"
                )
            )
            self._update(
                phase="running-simulation" if runner_status != "completed" else "simulation-completed",
                progress=min(96, max(91, 90 + progress_percent // 10)),
                message=status_message,
                result={
                    **(self.snapshot().get("result") or {}),
                    "runStatus": data,
                },
            )
            if runner_status == "completed":
                return data
            if runner_status in {"failed", "stopped"}:
                raise RuntimeError(str(data.get("error") or f"模拟运行失败: {runner_status}"))
            last_status = runner_status
            time.sleep(3.0)
        raise RuntimeError(f"等待模拟运行完成超时，最后状态 {last_status}")

    def _run(self, task_id: str, mode: str) -> None:
        try:
            self._update(phase="starting-runtime", progress=5, message="正在确认 MiroFish 运行环境。")
            self.deps.mirofish_runtime.ensure_started()
            seed = self._build_seed_material(mode)
            seed_dir = self.deps.mirofish_runtime_dir / "auto-sim-seeds"
            seed_dir.mkdir(parents=True, exist_ok=True)
            seed_path = seed_dir / f"{task_id}.md"
            seed_path.write_text(seed["markdown"], encoding="utf-8")
            self._update(
                phase="generating-project",
                progress=12,
                message="已生成推演材料，正在创建 MiroFish 项目。",
                seedPath=str(seed_path),
                focusSymbol=seed["focusSymbol"],
                projectName=seed["projectName"],
                ordersCount=len(seed["orders"]),
            )

            with seed_path.open("rb") as handle:
                payload = self._backend_request(
                    "POST",
                    "/api/graph/ontology/generate",
                    data={
                        "simulation_requirement": seed["simulationRequirement"],
                        "project_name": seed["projectName"],
                        "additional_context": "该材料来自 OKX Local App 的实时订单和策略快照，请重点服务于自动交易复盘与未来路径推演。",
                    },
                    files={"files": (seed_path.name, handle, "text/markdown")},
                    timeout=360.0,
                )
            project_data = payload.get("data") or {}
            project_id = str(project_data.get("project_id") or "")
            if not project_id:
                raise RuntimeError("MiroFish 未返回 project_id")
            graph_id = ""
            graph_task_id = ""
            if self._use_fast_local_graph():
                fast_graph = self._create_fast_local_graph(
                    project_id=project_id,
                    graph_name=seed["projectName"],
                    ontology={
                        "entity_types": list((project_data.get("ontology") or {}).get("entity_types") or []),
                        "edge_types": list((project_data.get("ontology") or {}).get("edge_types") or []),
                    },
                    seed=seed,
                    mode=mode,
                )
                graph_id = str(fast_graph.get("graph_id") or "")
                graph_task_id = str(fast_graph.get("task_id") or "")
                self._update(
                    phase="building-graph",
                    progress=55,
                    message="已生成本地图谱快照，正在创建模拟。",
                    projectId=project_id,
                    graphId=graph_id,
                    graphTaskId=graph_task_id,
                )
            else:
                self._update(
                    phase="building-graph",
                    progress=20,
                    message="项目已创建，正在构建图谱。",
                    projectId=project_id,
                )

                build_payload = self._backend_request(
                    "POST",
                    "/api/graph/build",
                    json_body={"project_id": project_id, "graph_name": seed["projectName"], "force": True},
                    timeout=60.0,
                )
                build_data = build_payload.get("data") or {}
                graph_task_id = str(build_data.get("task_id") or "")
                if not graph_task_id:
                    raise RuntimeError("图谱构建任务未返回 task_id")
                self._update(graphTaskId=graph_task_id)
                graph_task = self._wait_for_graph_task(graph_task_id, time.time() + 10 * 60)
                graph_result = graph_task.get("result") or {}
                graph_id = str(graph_result.get("graph_id") or "")
                if not graph_id:
                    raise RuntimeError("图谱构建完成后未返回 graph_id")
            self._update(phase="creating-simulation", progress=58, message="图谱已就绪，正在创建模拟。", graphId=graph_id)

            simulation_payload = self._backend_request(
                "POST",
                "/api/simulation/create",
                json_body={"project_id": project_id, "graph_id": graph_id, "enable_twitter": True, "enable_reddit": True},
                timeout=60.0,
            )
            simulation_data = simulation_payload.get("data") or {}
            simulation_id = str(simulation_data.get("simulation_id") or "")
            if not simulation_id:
                raise RuntimeError("MiroFish 未返回 simulation_id")
            simulation_path = f"{self.deps.mirofish_app_base.rstrip('/')}/simulation/{simulation_id}/start"
            self._update(
                phase="preparing-simulation",
                progress=64,
                message="模拟已创建，正在准备参与者与场景。",
                simulationId=simulation_id,
                simulationPath=simulation_path,
                simulationUrl=simulation_path,
            )
            self.deps.strategy_auto_optimization.reset_for_simulation(
                simulation_id=simulation_id,
                focus_symbol=seed["focusSymbol"],
                orders_count=len(seed["orders"]),
            )

            prepare_error = ""
            prepare_task_id = ""
            try:
                prepare_payload = self._backend_request(
                    "POST",
                    "/api/simulation/prepare",
                    json_body={
                        "simulation_id": simulation_id,
                        "use_llm_for_profiles": True,
                        "parallel_profile_count": 3,
                        "force_regenerate": True,
                    },
                    timeout=90.0,
                )
                prepare_data = prepare_payload.get("data") or {}
                prepare_task_id = str(prepare_data.get("task_id") or "")
                self._update(prepareTaskId=prepare_task_id)
                self._wait_for_prepare(simulation_id, prepare_task_id or None, time.time() + 15 * 60)
            except Exception as exc:
                prepare_error = str(exc)
            if not self._simulation_artifacts_ready(simulation_id):
                self._ensure_local_simulation_artifacts(
                    simulation_id=simulation_id,
                    project_id=project_id,
                    graph_id=graph_id,
                    seed=seed,
                    mode=mode,
                )
                fallback_message = "图谱未生成可用实体，已自动回退到基于订单与策略快照的本地仿真配置。"
                if prepare_error:
                    fallback_message = f"{fallback_message} 原准备阶段提示：{prepare_error}"
                self._update(
                    phase="preparing-simulation",
                    progress=85,
                    message=fallback_message,
                    prepareTaskId=prepare_task_id,
                )

            self._update(phase="starting-simulation", progress=90, message="准备完成，正在启动推演。")
            start_payload = self._backend_request(
                "POST",
                "/api/simulation/start",
                json_body={
                    "simulation_id": simulation_id,
                    "platform": "parallel",
                    "max_rounds": 6 if mode == "orders" else 8,
                    "force": True,
                },
                timeout=60.0,
            )
            start_data = start_payload.get("data") or {}
            self._update(
                phase="running-simulation",
                progress=91,
                message="推演已启动，正在等待模拟完成。",
                result={"start": start_data},
            )
            run_result = self._wait_for_run_completion(simulation_id, time.time() + 20 * 60)
            optimization_result = self.deps.strategy_auto_optimization.run(
                mode=mode,
                simulation_id=simulation_id,
                focus_symbol=seed["focusSymbol"],
                orders_count=len(seed["orders"]),
                simulation_result=run_result,
            )
            self._update(
                running=False,
                phase="completed",
                progress=100,
                message="自动推演与策略优化已完成，最新配置已自动回写。",
                completedAt=self.deps.now_local_iso(),
                result={
                    "start": start_data,
                    "runStatus": run_result,
                    "strategyOptimization": optimization_result,
                },
            )
        except Exception as exc:
            self._update(
                running=False,
                phase="failed",
                progress=100,
                message=f"自动推演失败: {exc}",
                completedAt=self.deps.now_local_iso(),
                error=str(exc),
            )

    def start(self, mode: str) -> dict[str, Any]:
        if mode not in {"orders", "strategy"}:
            raise RuntimeError("自动推演模式不支持")
        state = self._set_running(mode)
        thread = threading.Thread(target=self._run, args=(state["taskId"], mode), name=f"mirofish-auto-{mode}", daemon=True)
        thread.start()
        return state


__all__ = [
    "MiroFishAutoSimulationDependencies",
    "MiroFishAutoSimulationManager",
]
