"""
业务服务模块

改为懒加载，避免在 codex/local 模式启动时把所有外部 SDK 一次性拖进来。
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORTS: dict[str, str] = {
    "OntologyGenerator": ".ontology_generator",
    "GraphBuilderService": ".graph_builder",
    "TextProcessor": ".text_processor",
    "ZepEntityReader": ".zep_entity_reader",
    "EntityNode": ".zep_entity_reader",
    "FilteredEntities": ".zep_entity_reader",
    "OasisProfileGenerator": ".oasis_profile_generator",
    "OasisAgentProfile": ".oasis_profile_generator",
    "SimulationManager": ".simulation_manager",
    "SimulationState": ".simulation_manager",
    "SimulationStatus": ".simulation_manager",
    "SimulationConfigGenerator": ".simulation_config_generator",
    "SimulationParameters": ".simulation_config_generator",
    "AgentActivityConfig": ".simulation_config_generator",
    "TimeSimulationConfig": ".simulation_config_generator",
    "EventConfig": ".simulation_config_generator",
    "PlatformConfig": ".simulation_config_generator",
    "SimulationRunner": ".simulation_runner",
    "SimulationRunState": ".simulation_runner",
    "RunnerStatus": ".simulation_runner",
    "AgentAction": ".simulation_runner",
    "RoundSummary": ".simulation_runner",
    "ZepGraphMemoryUpdater": ".zep_graph_memory_updater",
    "ZepGraphMemoryManager": ".zep_graph_memory_updater",
    "AgentActivity": ".zep_graph_memory_updater",
    "SimulationIPCClient": ".simulation_ipc",
    "SimulationIPCServer": ".simulation_ipc",
    "IPCCommand": ".simulation_ipc",
    "IPCResponse": ".simulation_ipc",
    "CommandType": ".simulation_ipc",
    "CommandStatus": ".simulation_ipc",
}

__all__ = list(_EXPORTS.keys())


def __getattr__(name: str) -> Any:
    module_path = _EXPORTS.get(name)
    if not module_path:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module = import_module(module_path, __name__)
    value = getattr(module, name)
    globals()[name] = value
    return value
