"""
本地图谱存储
使用项目内 JSON 持久化图谱数据，替代外部 Zep 图服务。
"""

from __future__ import annotations

import json
import os
import re
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from ..config import Config
from ..utils.llm_client import LLMClient
from ..utils.logger import get_logger

logger = get_logger("mirofish.local_graph")


class LocalGraphStore:
    _lock = threading.RLock()
    _instance: "LocalGraphStore | None" = None

    def __init__(self) -> None:
        self.root = Path(Config.UPLOAD_FOLDER) / "local_graphs"
        self.root.mkdir(parents=True, exist_ok=True)
        self.llm = LLMClient()

    @classmethod
    def instance(cls) -> "LocalGraphStore":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def _graph_path(self, graph_id: str) -> Path:
        return self.root / f"{graph_id}.json"

    def create_graph(self, name: str, description: str = "") -> str:
        graph_id = f"mirofish_local_{uuid.uuid4().hex[:16]}"
        payload = {
            "graph_id": graph_id,
            "name": name,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "ontology": {},
            "nodes": [],
            "edges": [],
            "episodes": [],
        }
        self._save_graph(payload)
        return graph_id

    def delete_graph(self, graph_id: str) -> None:
        path = self._graph_path(graph_id)
        if path.exists():
            path.unlink()

    def load_graph(self, graph_id: str) -> Dict[str, Any]:
        path = self._graph_path(graph_id)
        if not path.exists():
            raise FileNotFoundError(f"图谱不存在: {graph_id}")
        return json.loads(path.read_text(encoding="utf-8"))

    def _save_graph(self, payload: Dict[str, Any]) -> None:
        path = self._graph_path(payload["graph_id"])
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def set_ontology(self, graph_id: str, ontology: Dict[str, Any]) -> None:
        with self._lock:
            graph = self.load_graph(graph_id)
            graph["ontology"] = ontology or {}
            self._save_graph(graph)

    def add_text_batch(self, graph_id: str, chunks: List[str]) -> List[str]:
        if not chunks:
            return []
        with self._lock:
            graph = self.load_graph(graph_id)
            extraction = self._extract_batch(graph.get("ontology") or {}, chunks)
            episode_uuid = uuid.uuid4().hex
            self._apply_extraction(graph, extraction, episode_uuid)
            graph.setdefault("episodes", []).append(
                {
                    "uuid": episode_uuid,
                    "chunk_count": len(chunks),
                    "processed": True,
                    "created_at": datetime.now().isoformat(),
                }
            )
            self._save_graph(graph)
            return [episode_uuid]

    def get_graph_info(self, graph_id: str) -> Dict[str, Any]:
        graph = self.load_graph(graph_id)
        entity_types = sorted(
            {
                label
                for node in graph.get("nodes", [])
                for label in node.get("labels", [])
                if label not in {"Entity", "Node"}
            }
        )
        return {
            "graph_id": graph_id,
            "node_count": len(graph.get("nodes", [])),
            "edge_count": len(graph.get("edges", [])),
            "entity_types": entity_types,
        }

    def get_graph_data(self, graph_id: str) -> Dict[str, Any]:
        graph = self.load_graph(graph_id)
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])
        node_map = {node["uuid"]: node for node in nodes}
        edges_data = []
        for edge in edges:
            edges_data.append(
                {
                    **edge,
                    "source_node_name": node_map.get(edge["source_node_uuid"], {}).get("name", ""),
                    "target_node_name": node_map.get(edge["target_node_uuid"], {}).get("name", ""),
                }
            )
        return {
            "graph_id": graph_id,
            "nodes": nodes,
            "edges": edges_data,
            "node_count": len(nodes),
            "edge_count": len(edges_data),
        }

    def get_all_nodes(self, graph_id: str) -> List[Dict[str, Any]]:
        return list(self.load_graph(graph_id).get("nodes", []))

    def get_all_edges(self, graph_id: str) -> List[Dict[str, Any]]:
        return list(self.load_graph(graph_id).get("edges", []))

    def get_node(self, graph_id: str, entity_uuid: str) -> Optional[Dict[str, Any]]:
        for node in self.get_all_nodes(graph_id):
            if node.get("uuid") == entity_uuid:
                return node
        return None

    def get_node_edges(self, graph_id: str, entity_uuid: str) -> List[Dict[str, Any]]:
        return [
            edge
            for edge in self.get_all_edges(graph_id)
            if edge.get("source_node_uuid") == entity_uuid or edge.get("target_node_uuid") == entity_uuid
        ]

    def _extract_batch(self, ontology: Dict[str, Any], chunks: List[str]) -> Dict[str, Any]:
        entity_types = [item.get("name", "") for item in ontology.get("entity_types", []) if item.get("name")]
        edge_types = [item.get("name", "") for item in ontology.get("edge_types", []) if item.get("name")]
        examples = {
            item.get("name", ""): item.get("examples", [])
            for item in ontology.get("entity_types", [])
            if item.get("name")
        }
        prompt = (
            "你是知识图谱抽取器。根据给定本体，从文本中抽取实体和关系。\n"
            "只返回一个 JSON 对象，不要额外解释。\n"
            "实体类型必须只使用允许列表中的名称；关系类型也必须只使用允许列表。\n"
            "如果文本里没有明确证据，就不要臆造。\n\n"
            f"允许的实体类型: {json.dumps(entity_types, ensure_ascii=False)}\n"
            f"允许的关系类型: {json.dumps(edge_types, ensure_ascii=False)}\n"
            f"实体示例: {json.dumps(examples, ensure_ascii=False)}\n\n"
            "返回格式:\n"
            "{\n"
            '  "entities": [{"name":"", "type":"", "summary":"", "attributes":{}}],\n'
            '  "relations": [{"name":"", "fact":"", "source":"", "target":"", "source_type":"", "target_type":"", "attributes":{}}]\n'
            "}\n\n"
            "待分析文本:\n"
            + "\n\n---\n\n".join(chunks[:3])
        )
        try:
            result = self.llm.chat_json(
                messages=[
                    {"role": "system", "content": "你只输出合法 JSON。"},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                max_tokens=4000,
            )
            if not isinstance(result, dict):
                return {"entities": [], "relations": []}
            return {
                "entities": result.get("entities", []) or [],
                "relations": result.get("relations", []) or [],
            }
        except Exception as exc:
            logger.warning(f"本地图谱抽取失败，返回空批次: {exc}")
            return {"entities": [], "relations": []}

    def _normalize_name(self, text: str) -> str:
        return re.sub(r"\s+", " ", str(text or "").strip())

    def _find_node(self, graph: Dict[str, Any], name: str, entity_type: str) -> Optional[Dict[str, Any]]:
        norm_name = self._normalize_name(name).lower()
        for node in graph.get("nodes", []):
            labels = node.get("labels", [])
            if entity_type in labels and self._normalize_name(node.get("name", "")).lower() == norm_name:
                return node
        return None

    def _apply_extraction(self, graph: Dict[str, Any], extraction: Dict[str, Any], episode_uuid: str) -> None:
        graph.setdefault("nodes", [])
        graph.setdefault("edges", [])
        entity_index: Dict[tuple[str, str], str] = {}

        for entity in extraction.get("entities", []):
            name = self._normalize_name(entity.get("name"))
            entity_type = self._normalize_name(entity.get("type"))
            if not name or not entity_type:
                continue
            existing = self._find_node(graph, name, entity_type)
            if existing:
                key = (name.lower(), entity_type)
                entity_index[key] = existing["uuid"]
                if entity.get("summary") and not existing.get("summary"):
                    existing["summary"] = entity.get("summary")
                existing.setdefault("attributes", {}).update(entity.get("attributes") or {})
                continue
            node_uuid = uuid.uuid4().hex
            node = {
                "uuid": node_uuid,
                "name": name,
                "labels": ["Entity", entity_type],
                "summary": self._normalize_name(entity.get("summary")) or f"{entity_type}: {name}",
                "attributes": entity.get("attributes") or {},
                "created_at": datetime.now().isoformat(),
            }
            graph["nodes"].append(node)
            entity_index[(name.lower(), entity_type)] = node_uuid

        for relation in extraction.get("relations", []):
            source_name = self._normalize_name(relation.get("source"))
            target_name = self._normalize_name(relation.get("target"))
            source_type = self._normalize_name(relation.get("source_type"))
            target_type = self._normalize_name(relation.get("target_type"))
            rel_name = self._normalize_name(relation.get("name"))
            if not source_name or not target_name or not rel_name:
                continue
            source_uuid = entity_index.get((source_name.lower(), source_type))
            target_uuid = entity_index.get((target_name.lower(), target_type))
            if not source_uuid or not target_uuid:
                continue
            fact = self._normalize_name(relation.get("fact")) or f"{source_name} {rel_name} {target_name}"
            edge_key = (rel_name, source_uuid, target_uuid, fact)
            if any(
                edge.get("name") == edge_key[0]
                and edge.get("source_node_uuid") == edge_key[1]
                and edge.get("target_node_uuid") == edge_key[2]
                and edge.get("fact") == edge_key[3]
                for edge in graph["edges"]
            ):
                continue
            graph["edges"].append(
                {
                    "uuid": uuid.uuid4().hex,
                    "name": rel_name,
                    "fact": fact,
                    "fact_type": rel_name,
                    "source_node_uuid": source_uuid,
                    "target_node_uuid": target_uuid,
                    "attributes": relation.get("attributes") or {},
                    "created_at": datetime.now().isoformat(),
                    "valid_at": None,
                    "invalid_at": None,
                    "expired_at": None,
                    "episodes": [episode_uuid],
                }
            )
