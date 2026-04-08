#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import base64
import concurrent.futures
import copy
import csv
import glob
import hashlib
import hmac
import json
import ipaddress
import os
import re
import secrets
import shutil
import socket
import subprocess
import sys
import threading
import time
import urllib.parse
import urllib.request
import ssl
import errno
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

import aiohttp
import requests
from requests.adapters import HTTPAdapter


APP_DIR = Path(__file__).resolve().parent
STATIC_DIR = APP_DIR / "static"
DATA_DIR = Path(os.environ.get("OKX_LOCAL_APP_DATA_DIR", APP_DIR / "data"))
CONFIG_PATH = DATA_DIR / "local-config.json"
AUTOMATION_CONFIG_PATH = DATA_DIR / "automation-config.json"
AUTOMATION_STATE_PATH = DATA_DIR / "automation-state.json"
LOCAL_ORDER_STATE_PATH = DATA_DIR / "local-order-state.json"
MINER_CONFIG_PATH = DATA_DIR / "miner-config.json"
MINER_STATE_PATH = DATA_DIR / "miner-state.json"
MAC_LOTTO_STATUS_PATH = DATA_DIR / "mac-lotto-status.json"
MAC_LOTTO_LOG_PATH = DATA_DIR / "mac-lotto.log"
HOST = os.environ.get("OKX_LOCAL_APP_HOST", "127.0.0.1")
PORT = int(os.environ.get("OKX_LOCAL_APP_PORT", "8765"))
MAX_LOG_ENTRIES = 120
SECURE_FILE_MAGIC = "okx-local-app-secure-v1"
KEYCHAIN_SERVICE = "com.cc.okxlocalapp.local-file-key"
KEYCHAIN_ACCOUNT = "default"
FALLBACK_SECRET_FILE = DATA_DIR / ".local-file-key"
VENDOR_ROOT = APP_DIR.parent / "btc-lotto-miner-app" / "vendor"
HASHRATE_BENCHMARK_CACHE: dict[str, Any] = {"ts": 0.0, "hashrate": 0.0, "duration": 0.0}
MINER_OPTIONS_CACHE: dict[str, Any] = {"ts": 0.0, "items": []}
POOL_DIAG_CACHE: dict[str, Any] = {"ts": 0.0, "host": "", "port": 0, "result": {}}
FOCUS_DATA_CACHE: dict[str, dict[str, Any]] = {
    "account": {"ts": 0.0, "value": None, "error": ""},
    "orders": {"ts": 0.0, "value": None, "error": ""},
}
FOCUS_CACHE_LOCK = threading.RLock()
FOCUS_WARMER_THREAD: threading.Thread | None = None
FOCUS_WARMER_LOCK = threading.RLock()
SESSION_POOL: dict[str, requests.Session] = {}
SESSION_POOL_LOCK = threading.RLock()
OKX_DNS_CACHE: dict[str, dict[str, Any]] = {}
OKX_DNS_CACHE_LOCK = threading.RLock()
OKX_ROUTE_CACHE: dict[str, dict[str, Any]] = {}
OKX_ROUTE_CACHE_LOCK = threading.RLock()
ORIGINAL_GETADDRINFO = socket.getaddrinfo


def _extract_a_records_from_doh(payload: dict[str, Any]) -> list[str]:
    answers = payload.get("Answer") or []
    ips: list[str] = []
    for item in answers:
        if int(item.get("type") or 0) != 1:
            continue
        value = str(item.get("data") or "").strip()
        if not value:
            continue
        try:
            ipaddress.ip_address(value)
        except ValueError:
            continue
        ips.append(value)
    return ips


def resolve_okx_host_via_doh(host: str) -> list[str]:
    now = time.time()
    with OKX_DNS_CACHE_LOCK:
        cached = OKX_DNS_CACHE.get(host)
        if cached and now - float(cached.get("ts") or 0) < 180:
            return list(cached.get("ips") or [])

    endpoints = (
        "https://dns.google/resolve",
        "https://1.1.1.1/dns-query",
    )
    last_error: Exception | None = None
    for base_url in endpoints:
        try:
            response = requests.get(
                base_url,
                params={"name": host, "type": "A"},
                headers={"accept": "application/dns-json", "user-agent": "OKXLocalApp/1.0"},
                timeout=6,
            )
            response.raise_for_status()
            ips = _extract_a_records_from_doh(response.json())
            if ips:
                with OKX_DNS_CACHE_LOCK:
                    OKX_DNS_CACHE[host] = {"ts": now, "ips": ips}
                return ips
        except Exception as exc:
            last_error = exc
            continue

    if last_error:
        raise last_error
    return []


def okx_aware_getaddrinfo(host: Any, port: Any, family: int = 0, type: int = 0, proto: int = 0, flags: int = 0):
    if isinstance(host, str):
        normalized = host.rstrip(".").lower()
        if normalized.endswith("okx.com"):
            try:
                ips = resolve_okx_host_via_doh(normalized)
            except Exception:
                ips = []
            if ips:
                resolved: list[tuple[Any, ...]] = []
                for ip in ips:
                    try:
                        resolved.extend(ORIGINAL_GETADDRINFO(ip, port, family, type, proto, flags))
                    except OSError:
                        continue
                if resolved:
                    return resolved
    return ORIGINAL_GETADDRINFO(host, port, family, type, proto, flags)


socket.getaddrinfo = okx_aware_getaddrinfo


def default_cpu_worker_count() -> int:
    return max(1, min(os.cpu_count() or 1, 8))


def load_cached_focus_section(
    key: str,
    ttl_seconds: float,
    loader: Any,
) -> tuple[Any, str | None, bool]:
    now = time.time()
    with FOCUS_CACHE_LOCK:
        cache_entry = FOCUS_DATA_CACHE.setdefault(key, {"ts": 0.0, "value": None, "error": ""})
        cached_value = copy.deepcopy(cache_entry.get("value"))
        cached_error = str(cache_entry.get("error") or "")
        cached_ts = float(cache_entry.get("ts") or 0.0)

    if cached_value is not None and now - cached_ts < ttl_seconds:
        return cached_value, (cached_error or None), True

    try:
        loaded_value = loader()
    except Exception as exc:
        if cached_value is not None:
            error_text = str(exc)
            with FOCUS_CACHE_LOCK:
                cache_entry["error"] = error_text
            return cached_value, error_text, True
        raise

    with FOCUS_CACHE_LOCK:
        cache_entry["ts"] = now
        cache_entry["value"] = copy.deepcopy(loaded_value)
        cache_entry["error"] = ""
    return loaded_value, None, False


def reset_focus_cache(*keys: str) -> None:
    with FOCUS_CACHE_LOCK:
        target_keys = keys or tuple(FOCUS_DATA_CACHE.keys())
        for key in target_keys:
            FOCUS_DATA_CACHE[key] = {"ts": 0.0, "value": None, "error": ""}


def config_session_key(config: dict[str, Any]) -> str:
    return "|".join(
        [
            str(config.get("baseUrl") or "https://www.okx.com").strip(),
            "1" if bool(config.get("simulated")) else "0",
            str(config.get("apiKey") or ""),
            str(config.get("passphrase") or ""),
        ]
    )


def shared_http_session(config: dict[str, Any]) -> requests.Session:
    key = config_session_key(config)
    with SESSION_POOL_LOCK:
        session = SESSION_POOL.get(key)
        if session is not None:
            return session
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=16, pool_maxsize=32, max_retries=0, pool_block=False)
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        session.headers.update(
            {
                "Connection": "keep-alive",
                "Accept": "application/json",
                "User-Agent": "OKXLocalApp/1.0",
            }
        )
        SESSION_POOL[key] = session
        return session


def unique_strings(values: list[str]) -> list[str]:
    seen = set()
    result: list[str] = []
    for value in values:
        normalized = str(value or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(normalized)
    return result


def build_okx_route_cache_key(config: dict[str, Any]) -> str:
    return "|".join(
        [
            str(config.get("baseUrl") or "https://www.okx.com").strip(),
            "1" if bool(config.get("simulated")) else "0",
            str(config.get("executionMode") or "local").strip(),
            str(config.get("remoteGatewayUrl") or "").strip(),
        ]
    )


def is_remote_execution_enabled(config: dict[str, Any]) -> bool:
    return (
        str(config.get("executionMode") or "local").strip() == "remote"
        and bool(str(config.get("remoteGatewayUrl") or "").strip())
    )


def remote_gateway_url(config: dict[str, Any]) -> str:
    return str(config.get("remoteGatewayUrl") or "").strip().rstrip("/")


def remote_gateway_headers(config: dict[str, Any], content_type: str = "") -> dict[str, str]:
    headers = {
        "Accept": "application/json",
        "User-Agent": "OKXLocalApp/1.0",
        "X-OKX-Desk-Forwarded": "1",
    }
    token = str(config.get("remoteGatewayToken") or "").strip()
    if token:
        headers["X-OKX-Desk-Gateway-Token"] = token
    if content_type:
        headers["Content-Type"] = content_type
    return headers


def remote_gateway_session(config: dict[str, Any]) -> requests.Session:
    return shared_http_session(
        {
            "baseUrl": remote_gateway_url(config) or "https://gateway.invalid",
            "simulated": False,
            "apiKey": str(config.get("remoteGatewayToken") or ""),
            "passphrase": "gateway",
        }
    )


def remote_gateway_request(
    config: dict[str, Any],
    method: str,
    path_with_query: str,
    *,
    body: bytes | None = None,
    content_type: str = "",
    timeout: float = 20.0,
) -> requests.Response:
    base = remote_gateway_url(config)
    if not base:
        raise OkxApiError("未配置远端执行节点 URL")
    url = f"{base}{path_with_query if path_with_query.startswith('/') else '/' + path_with_query}"
    headers = remote_gateway_headers(config, content_type=content_type)
    # Gateway calls are control-plane operations. Prefer a fresh connection so a stale
    # keep-alive socket to the remote node or nginx does not pin the UI on env switches.
    headers["Connection"] = "close"
    response = requests.request(
        method=method.upper(),
        url=url,
        data=body,
        headers=headers,
        timeout=timeout,
    )
    return response


def is_loopback_client(address: str) -> bool:
    return address in ("127.0.0.1", "::1", "localhost")


def configured_gateway_access_token() -> str:
    return str(os.environ.get("OKX_DESK_GATEWAY_TOKEN") or "").strip()


def enforce_gateway_auth(handler: BaseHTTPRequestHandler, path: str) -> bool:
    if not path.startswith("/api/"):
        return True
    access_token = configured_gateway_access_token()
    if not access_token:
        return True
    client_ip = str((handler.client_address or ("", 0))[0] or "")
    if is_loopback_client(client_ip) and handler.headers.get("X-OKX-Desk-Forwarded") != "1":
        return True
    incoming = str(handler.headers.get("X-OKX-Desk-Gateway-Token") or "")
    if incoming and hmac.compare_digest(incoming, access_token):
        return True
    error_response(handler, "远端执行节点鉴权失败", status=403)
    return False


REMOTE_PROXY_EXACT_PATHS = {
    "/api/focus-snapshot",
    "/api/account/overview",
    "/api/orders/recent",
    "/api/config/test",
    "/api/order/place",
}

REMOTE_PROXY_PREFIXES = (
    "/api/automation/",
    "/api/market/",
)

REMOTE_CONFIG_KEYS = (
    "envPreset",
    "apiKey",
    "secretKey",
    "passphrase",
    "baseUrl",
    "simulated",
)
REMOTE_CONFIG_FETCH_TIMEOUT = 8.0
REMOTE_NODE_HEALTH_TIMEOUT = 6.0


def build_proxy_runtime_config(current: dict[str, Any], payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    merged = deep_merge(default_config(), current)
    if "executionMode" in payload:
        merged["executionMode"] = payload.get("executionMode")
    remote_url = str(payload.get("remoteGatewayUrl") or "").strip()
    if remote_url:
        merged["remoteGatewayUrl"] = remote_url
    remote_token = str(payload.get("remoteGatewayToken") or "").strip()
    if remote_token:
        merged["remoteGatewayToken"] = remote_token
    return merged


def build_local_runtime_config(current: dict[str, Any], payload: dict[str, Any]) -> dict[str, Any]:
    merged = deep_merge(default_config(), current)
    for key in ("envPreset", "baseUrl", "simulated", "executionMode", "remoteGatewayUrl"):
        if key in payload:
            merged[key] = payload.get(key)
    for key in ("apiKey", "secretKey", "passphrase"):
        if key in payload:
            merged[key] = payload.get(key)
    remote_token = str(payload.get("remoteGatewayToken") or "").strip()
    if remote_token:
        merged["remoteGatewayToken"] = remote_token
    return merged


def build_remote_trading_config(current: dict[str, Any], payload: dict[str, Any], *, persist: bool) -> dict[str, Any]:
    selection = deep_merge(default_config(), current)
    for key in ("envPreset", "baseUrl", "simulated"):
        if key in payload:
            selection[key] = payload.get(key)
    profile_key = config_profile_key(selection)
    profiles = deep_merge(default_trading_profiles(), current.get("profiles") or {})
    profile_defaults = default_trading_profiles().get(profile_key, {})
    stored_profile = deep_merge(profile_defaults, profiles.get(profile_key) or {})
    merged = {
        "envPreset": payload.get("envPreset", stored_profile.get("envPreset", current.get("envPreset", "okx_main_demo"))),
        "apiKey": payload.get("apiKey") or stored_profile.get("apiKey", ""),
        "secretKey": payload.get("secretKey") or stored_profile.get("secretKey", ""),
        "passphrase": payload.get("passphrase") or stored_profile.get("passphrase", ""),
        "baseUrl": payload.get("baseUrl", stored_profile.get("baseUrl", current.get("baseUrl", "https://www.okx.com"))),
        "simulated": bool(payload.get("simulated", stored_profile.get("simulated", current.get("simulated", True)))),
        "executionMode": "local",
        "remoteGatewayUrl": "",
        "remoteGatewayToken": "",
        "persist": persist,
    }
    return merged


def merge_remote_redacted_config(
    local_redacted: dict[str, Any], remote_payload: dict[str, Any] | None
) -> dict[str, Any]:
    merged = deep_merge(default_config(), local_redacted)
    remote_config = (remote_payload or {}).get("config") or {}
    for key in ("envPreset", "baseUrl", "simulated"):
        if key in remote_config:
            merged[key] = remote_config.get(key)
    for key in ("apiKey", "secretKey", "passphrase"):
        merged[key] = ""
    for mask_key in ("apiKeyMask", "secretKeyMask", "passphraseMask"):
        if remote_config.get(mask_key):
            merged[mask_key] = remote_config.get(mask_key)
        else:
            merged.pop(mask_key, None)
    merged["executionMode"] = local_redacted.get("executionMode", "remote")
    merged["remoteGatewayUrl"] = local_redacted.get("remoteGatewayUrl", "")
    merged["remoteGatewayToken"] = ""
    if local_redacted.get("remoteGatewayTokenMask"):
        merged["remoteGatewayTokenMask"] = local_redacted.get("remoteGatewayTokenMask")
    merged.pop("profiles", None)
    return merged


def should_proxy_to_remote(config: dict[str, Any], path: str) -> bool:
    if not is_remote_execution_enabled(config):
        return False
    if path in REMOTE_PROXY_EXACT_PATHS:
        return True
    return any(path.startswith(prefix) for prefix in REMOTE_PROXY_PREFIXES)


def remote_node_health(config: dict[str, Any], *, timeout: float = REMOTE_NODE_HEALTH_TIMEOUT) -> dict[str, Any]:
    last_error: Exception | None = None
    for attempt, attempt_timeout in enumerate((timeout, max(timeout, 10.0)), start=1):
        try:
            response = remote_gateway_request(config, "GET", "/api/health", timeout=attempt_timeout)
            payload = response.json()
            route = payload.get("okxRoute") or {}
            if not route:
                route = {
                    "healthy": response.ok,
                    "summary": "远端执行节点已连接" if response.ok else "远端执行节点未就绪",
                    "detail": "",
                }
            route["executionMode"] = "remote"
            route["remoteGatewayUrl"] = remote_gateway_url(config)
            return route
        except Exception as exc:
            last_error = exc
            if attempt == 1:
                time.sleep(0.15)
                continue
    detail = str(last_error) if last_error else "未知错误"
    return {
        "healthy": False,
        "status": "remote_unreachable",
        "summary": "远端执行节点未连通",
        "detail": detail,
        "technicalDetail": detail,
        "executionMode": "remote",
        "remoteGatewayUrl": remote_gateway_url(config),
    }


def safe_host_ips(host: str) -> list[str]:
    ips: list[str] = []
    try:
        infos = ORIGINAL_GETADDRINFO(host, 443, type=socket.SOCK_STREAM)
        for info in infos:
            ip = str(info[4][0] or "")
            if ip and ip not in ips:
                ips.append(ip)
    except Exception:
        pass
    return ips


def probe_rest_route(base_url: str, config: dict[str, Any]) -> dict[str, Any]:
    parsed = urllib.parse.urlparse(base_url)
    host = parsed.hostname or ""
    url = urllib.parse.urljoin(base_url.rstrip("/") + "/", "api/v5/public/time")
    result: dict[str, Any] = {
        "baseUrl": base_url,
        "host": host,
        "systemIps": safe_host_ips(host),
        "dohIps": [],
        "ok": False,
        "status": 0,
        "elapsedMs": 0.0,
        "error": "",
    }
    try:
        result["dohIps"] = resolve_okx_host_via_doh(host)
    except Exception as exc:
        result["dohError"] = str(exc)
    started_at = time.perf_counter()
    try:
        response = shared_http_session(config).get(url, timeout=(2.5, 4.0))
        result["elapsedMs"] = round((time.perf_counter() - started_at) * 1000, 2)
        result["status"] = response.status_code
        payload = response.json()
        if response.ok and str(payload.get("code", "0")) in {"0", ""}:
            result["ok"] = True
        else:
            result["error"] = str(payload)[:240]
    except Exception as exc:
        result["elapsedMs"] = round((time.perf_counter() - started_at) * 1000, 2)
        result["error"] = str(exc)
    return result


def probe_private_ws_route(config: dict[str, Any]) -> dict[str, Any]:
    ws_url = derive_private_ws_url(config)
    parsed = urllib.parse.urlparse(ws_url)
    host = parsed.hostname or ""
    port = int(parsed.port or 443)
    result: dict[str, Any] = {
        "url": ws_url,
        "host": host,
        "port": port,
        "systemIps": safe_host_ips(host),
        "dohIps": [],
        "ok": False,
        "elapsedMs": 0.0,
        "error": "",
    }
    try:
        result["dohIps"] = resolve_okx_host_via_doh(host)
    except Exception as exc:
        result["dohError"] = str(exc)
    started_at = time.perf_counter()
    sock = None
    tls_sock = None
    try:
        sock = socket.create_connection((host, port), timeout=3.0)
        context = ssl.create_default_context()
        tls_sock = context.wrap_socket(sock, server_hostname=host)
        tls_sock.do_handshake()
        result["ok"] = True
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        result["elapsedMs"] = round((time.perf_counter() - started_at) * 1000, 2)
        if tls_sock is not None:
            try:
                tls_sock.close()
            except Exception:
                pass
        elif sock is not None:
            try:
                sock.close()
            except Exception:
                pass
    return result


def summarize_okx_route(best_rest: dict[str, Any], ws_route: dict[str, Any], *, simulated: bool) -> tuple[str, str]:
    rest_error = str(best_rest.get("error") or "")
    ws_error = str(ws_route.get("error") or "")
    rest_ips = [str(ip) for ip in (best_rest.get("systemIps") or [])]

    if best_rest.get("ok") and (ws_route.get("ok") or simulated):
        if simulated:
            return "模拟盘链路正常", f"REST {best_rest.get('elapsedMs')}ms；模拟盘模式"
        return "实盘链路正常", f"REST {best_rest.get('elapsedMs')}ms；私有 WS {ws_route.get('elapsedMs')}ms"

    if any(ip.startswith("169.254.") for ip in rest_ips):
        if simulated:
            return "模拟盘仅本地纸面可用", "OKX 域名解析异常，当前网络把目标站点指到了无效地址"
        return "实盘不可用", "OKX 域名解析异常，当前网络把目标站点指到了无效地址"

    combined = f"{rest_error} {ws_error}".lower()
    if "unexpected_eof" in combined or "ssleoferror" in combined or "ssl_error_syscall" in combined:
        if simulated:
            return "模拟盘仅本地纸面可用", "当前网络无法完成 OKX TLS 握手，已切到本地 paper 模式"
        return "实盘不可用", "当前网络无法完成 OKX TLS 握手"
    if "timed out" in combined or "connecttimeout" in combined:
        if simulated:
            return "模拟盘仅本地纸面可用", "连接 OKX 超时，已切到本地 paper 模式"
        return "实盘不可用", "连接 OKX 超时"
    if "host is down" in combined or "no route to host" in combined:
        if simulated:
            return "模拟盘仅本地纸面可用", "当前网络到 OKX 主机不可达，已切到本地 paper 模式"
        return "实盘不可用", "当前网络到 OKX 主机不可达"

    if simulated:
        return "模拟盘仅本地纸面可用", "OKX 模拟盘链路当前不可用，已切到本地 paper 模式"
    return "实盘不可用", "OKX 交易链路当前不可用"


def evaluate_okx_route_health(config: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    cache_key = build_okx_route_cache_key(config)
    now = time.time()
    with OKX_ROUTE_CACHE_LOCK:
        cached = OKX_ROUTE_CACHE.get(cache_key)
        if not force and cached and now - float(cached.get("ts") or 0) < 25:
            return copy.deepcopy(cached.get("value") or {})

    base_url = str(config.get("baseUrl") or "https://www.okx.com").strip()
    simulated = bool(config.get("simulated"))
    candidate_urls = unique_strings([base_url])
    rest_routes = [probe_rest_route(candidate, config) for candidate in candidate_urls]
    best_rest = next((item for item in rest_routes if item.get("ok")), rest_routes[0] if rest_routes else {})
    ws_route = probe_private_ws_route(config)
    healthy = bool(best_rest.get("ok")) and bool(ws_route.get("ok") or simulated)
    status = "ok" if healthy else "blocked"
    if not best_rest.get("ok"):
        status = "rest_blocked"
    elif not ws_route.get("ok") and not simulated:
        status = "private_ws_blocked"

    summary, short_detail = summarize_okx_route(best_rest, ws_route, simulated=simulated)
    detail_parts: list[str] = [short_detail]
    technical_parts: list[str] = []
    if best_rest.get("ok"):
        technical_parts.append(f"REST {best_rest.get('elapsedMs')}ms")
    else:
        technical_parts.append(f"REST 不可用: {best_rest.get('error') or '未通过'}")
    if simulated:
        technical_parts.append("模拟盘允许无私有 WS 继续")
    elif ws_route.get("ok"):
        technical_parts.append(f"私有 WS {ws_route.get('elapsedMs')}ms")
    else:
        technical_parts.append(f"私有 WS 不可用: {ws_route.get('error') or '未通过'}")

    payload = {
        "ts": now,
        "value": {
            "status": status,
            "healthy": healthy,
            "simulated": simulated,
            "baseUrl": base_url,
            "rest": best_rest,
            "restCandidates": rest_routes,
            "privateWs": ws_route,
            "summary": summary,
            "detail": "；".join(detail_parts),
            "technicalDetail": "；".join(technical_parts),
        },
    }
    with OKX_ROUTE_CACHE_LOCK:
        OKX_ROUTE_CACHE[cache_key] = payload
    return copy.deepcopy(payload["value"])


def ensure_live_route_ready(config: dict[str, Any], *, force: bool = False) -> dict[str, Any]:
    if bool(config.get("simulated")):
        return evaluate_okx_route_health(config, force=force)
    route = evaluate_okx_route_health(config, force=force)
    if route.get("healthy"):
        return route
    raise OkxApiError(f"{route.get('summary') or '实盘不可用'}：{route.get('detail') or route.get('status') or '未知原因'}")


def warm_focus_cache_once() -> None:
    config = CONFIG.current()
    valid, message = validate_config(config)
    if not valid:
        return
    refresh_route = bool(config.get("apiKey")) and bool(config.get("passphrase"))

    def fetch_account() -> dict[str, Any]:
        client = OkxClient(config)
        balances = parse_balance_snapshot(client.get_account_balance())
        return {
            "summary": balances["summary"],
            "balanceCount": len(balances["details"]),
        }

    def fetch_orders() -> dict[str, Any]:
        client = OkxClient(config)
        return {"orders": client.get_recent_orders("").get("data", [])}

    try:
        load_cached_focus_section("account", 0.0, fetch_account)
    except Exception:
        pass
    try:
        load_cached_focus_section("orders", 0.0, fetch_orders)
    except Exception:
        pass
    if refresh_route:
        try:
            evaluate_okx_route_health(config, force=True)
        except Exception:
            pass


def ensure_focus_warmer() -> None:
    global FOCUS_WARMER_THREAD
    with FOCUS_WARMER_LOCK:
        if FOCUS_WARMER_THREAD and FOCUS_WARMER_THREAD.is_alive():
            return

        def _loop() -> None:
            while True:
                try:
                    warm_focus_cache_once()
                except Exception:
                    pass
                time.sleep(10)

        FOCUS_WARMER_THREAD = threading.Thread(
            target=_loop,
            name="focus-cache-warmer",
            daemon=True,
        )
        FOCUS_WARMER_THREAD.start()


def utc_timestamp() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def now_local_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def read_json_request(handler: BaseHTTPRequestHandler) -> dict[str, Any]:
    content_length = int(handler.headers.get("Content-Length", "0"))
    raw = handler.rfile.read(content_length) if content_length else b"{}"
    if not raw:
        return {}
    return json.loads(raw.decode("utf-8"))


def ensure_private_permissions(path: Path, *, is_dir: bool = False) -> None:
    mode = 0o700 if is_dir else 0o600
    try:
        os.chmod(path, mode)
    except OSError:
        return


def keychain_secret() -> str:
    if sys.platform != "darwin" or not shutil.which("security"):
        return file_secret()

    found = subprocess.run(
        [
            "security",
            "find-generic-password",
            "-a",
            KEYCHAIN_ACCOUNT,
            "-s",
            KEYCHAIN_SERVICE,
            "-w",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if found.returncode == 0 and found.stdout.strip():
        return found.stdout.strip()

    generated = secrets.token_urlsafe(48)
    created = subprocess.run(
        [
            "security",
            "add-generic-password",
            "-a",
            KEYCHAIN_ACCOUNT,
            "-s",
            KEYCHAIN_SERVICE,
            "-w",
            generated,
            "-U",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if created.returncode != 0:
        message = created.stderr.strip() or "无法写入 macOS Keychain"
        raise RuntimeError(message)
    return generated


def file_secret() -> str:
    secret = str(os.environ.get("OKX_LOCAL_APP_FILE_SECRET") or "").strip()
    if secret:
        return secret

    FALLBACK_SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
    ensure_private_permissions(FALLBACK_SECRET_FILE.parent, is_dir=True)
    if FALLBACK_SECRET_FILE.exists():
        ensure_private_permissions(FALLBACK_SECRET_FILE)
        value = FALLBACK_SECRET_FILE.read_text(encoding="utf-8").strip()
        if value:
            return value

    generated = secrets.token_urlsafe(48)
    FALLBACK_SECRET_FILE.write_text(generated, encoding="utf-8")
    ensure_private_permissions(FALLBACK_SECRET_FILE)
    return generated


def encrypt_payload(raw: bytes) -> dict[str, Any]:
    secret = keychain_secret()
    enc_secret = hashlib.sha256(f"{secret}:enc".encode("utf-8")).hexdigest()
    mac_secret = hashlib.sha256(f"{secret}:mac".encode("utf-8")).digest()
    proc = subprocess.run(
        [
            "openssl",
            "enc",
            "-aes-256-cbc",
            "-pbkdf2",
            "-salt",
            "-pass",
            f"pass:{enc_secret}",
        ],
        input=raw,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", errors="ignore").strip() or "本地加密失败")
    cipher = proc.stdout
    return {
        "magic": SECURE_FILE_MAGIC,
        "version": 1,
        "cipher": "aes-256-cbc",
        "kdf": "pbkdf2",
        "digest": hmac.new(mac_secret, cipher, hashlib.sha256).hexdigest(),
        "payload": base64.b64encode(cipher).decode("utf-8"),
    }


def decrypt_payload(payload: dict[str, Any]) -> bytes:
    if payload.get("magic") != SECURE_FILE_MAGIC:
        raise RuntimeError("文件不是受保护格式")
    secret = keychain_secret()
    enc_secret = hashlib.sha256(f"{secret}:enc".encode("utf-8")).hexdigest()
    mac_secret = hashlib.sha256(f"{secret}:mac".encode("utf-8")).digest()
    cipher = base64.b64decode(payload["payload"])
    digest = hmac.new(mac_secret, cipher, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(digest, payload.get("digest", "")):
        raise RuntimeError("本地文件完整性校验失败")
    proc = subprocess.run(
        [
            "openssl",
            "enc",
            "-d",
            "-aes-256-cbc",
            "-pbkdf2",
            "-pass",
            f"pass:{enc_secret}",
        ],
        input=cipher,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr.decode("utf-8", errors="ignore").strip() or "本地解密失败")
    return proc.stdout


def secure_dump_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_private_permissions(path.parent, is_dir=True)
    envelope = encrypt_payload(
        json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    )
    path.write_text(json.dumps(envelope, ensure_ascii=False, indent=2), encoding="utf-8")
    ensure_private_permissions(path)


def secure_load_json(path: Path, default_factory) -> tuple[dict[str, Any], bool]:
    if not path.exists():
        return default_factory(), False
    ensure_private_permissions(path)
    raw = path.read_text(encoding="utf-8")
    decoded = json.loads(raw)
    if isinstance(decoded, dict) and decoded.get("magic") == SECURE_FILE_MAGIC:
        plain = decrypt_payload(decoded).decode("utf-8")
        return json.loads(plain), False
    if isinstance(decoded, dict):
        secure_dump_json(path, decoded)
        return decoded, True
    raise RuntimeError(f"无法读取本地安全文件: {path}")


def dump_worker_status(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_private_permissions(path.parent, is_dir=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    ensure_private_permissions(path)


def load_worker_status(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    ensure_private_permissions(path)
    raw = path.read_text(encoding="utf-8")
    decoded = json.loads(raw)
    if isinstance(decoded, dict) and decoded.get("magic") == SECURE_FILE_MAGIC:
        plain = decrypt_payload(decoded).decode("utf-8")
        merged = json.loads(plain)
        if isinstance(merged, dict):
            for key, value in decoded.items():
                if key not in {"magic", "version", "cipher", "kdf", "digest", "payload"}:
                    merged[key] = value
            return merged
        return {}
    if isinstance(decoded, dict):
        return decoded
    return {}


def safe_decimal(value: Any, default: str = "0") -> Decimal:
    if isinstance(value, Decimal):
        return value
    try:
        if value in (None, "", "None"):
            raise InvalidOperation
        return Decimal(str(value))
    except (InvalidOperation, ValueError, TypeError):
        return Decimal(default)


def decimal_to_str(value: Decimal | str | int | float) -> str:
    number = safe_decimal(value)
    rendered = format(number.normalize(), "f")
    if rendered in ("", "-0"):
        return "0"
    if "." in rendered:
        rendered = rendered.rstrip("0").rstrip(".")
    return rendered or "0"


def round_down(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_DOWN)
    return units * step


def parse_iso(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def deep_merge(defaults: Any, current: Any) -> Any:
    if isinstance(defaults, dict) and isinstance(current, dict):
        merged = copy.deepcopy(defaults)
        for key, value in current.items():
            if key in merged:
                merged[key] = deep_merge(merged[key], value)
            else:
                merged[key] = copy.deepcopy(value)
        return merged
    return copy.deepcopy(current)


def default_automation_config() -> dict[str, Any]:
    return {
        "strategyPreset": "dual_engine",
        "spotInstId": "BTC-USDT",
        "swapInstId": "BTC-USDT-SWAP",
        "bar": "5m",
        "fastEma": 9,
        "slowEma": 21,
        "pollSeconds": 20,
        "cooldownSeconds": 180,
        "maxOrdersPerDay": 20,
        "spotEnabled": True,
        "spotQuoteBudget": "100",
        "spotMaxExposure": "300",
        "swapEnabled": True,
        "swapContracts": "1",
        "swapTdMode": "cross",
        "swapLeverage": "5",
        "swapStrategyMode": "trend_follow",
        "stopLossPct": "1.2",
        "takeProfitPct": "2.4",
        "maxDailyLossPct": "3.0",
        "autostart": False,
        "allowLiveTrading": False,
        "allowLiveAutostart": False,
        "enforceNetMode": True,
    }


def default_market_state() -> dict[str, Any]:
    return {
        "enabled": False,
        "instId": "",
        "signal": "hold",
        "trend": "flat",
        "lastPrice": "",
        "positionSide": "flat",
        "positionSize": "0",
        "positionNotional": "0",
        "entryPrice": "",
        "lastAction": "",
        "lastActionAt": "",
        "lastOrderId": "",
        "lastMessage": "",
        "lastTradeAt": "",
        "prepared": False,
    }


def default_automation_state() -> dict[str, Any]:
    return {
        "running": False,
        "statusText": "未启动",
        "modeText": "等待配置",
        "lastCycleAt": "",
        "lastCycleDurationMs": 0,
        "lastActionAt": "",
        "lastError": "",
        "sessionStartedAt": "",
        "sessionStartEq": "",
        "currentEq": "",
        "maxObservedEq": "",
        "dailyDrawdownPct": "0",
        "orderCountToday": 0,
        "today": "",
        "consecutiveErrors": 0,
        "equityCurve": [],
        "logs": [],
        "analysis": {
            "statusText": "等待联网分析",
            "decision": "pending",
            "decisionLabel": "待分析",
            "summary": "",
            "selectedStrategyName": "",
            "selectedStrategyDetail": "",
            "selectedReturnPct": "",
            "selectedDrawdownPct": "",
            "selectedScore": "",
            "allowNewEntries": False,
            "optimizerRefreshed": False,
            "lastAnalyzedAt": "",
            "marketRegime": "",
            "spotTrend": "",
            "swapTrend": "",
            "volatilityPct": "",
            "spreadPct": "",
            "basisPct": "",
            "fundingRatePct": "",
            "openInterest": "",
            "warnings": [],
            "blockers": [],
        },
        "markets": {
            "spot": default_market_state(),
            "swap": default_market_state(),
        },
        "research": {
            "running": False,
            "statusText": "未运行",
            "mode": "",
            "lastRunAt": "",
            "historyLimit": 240,
            "sampleCount": 0,
            "summary": {},
            "bestConfig": {},
            "leaderboard": [],
            "generationSummaries": [],
            "pipeline": {},
            "markets": {},
            "notes": [],
            "equityCurve": [],
        },
    }


def default_miner_config() -> dict[str, Any]:
    return {
        "mode": "mac_lotto",
        "wallet": "",
        "workerName": "desk",
        "poolHost": "solo.ckpool.org",
        "poolPort": 3333,
        "poolPassword": "x",
        "poolApiBase": "",
        "bitaxeHosts": "",
        "serialPort": "",
        "boardType": "Mac 本机 CPU 集群",
        "refreshSeconds": 20,
        "cpuRandomNonce": False,
        "cpuWorkers": default_cpu_worker_count(),
        "autoStartMacLotto": True,
    }


def default_local_order_state() -> dict[str, Any]:
    return {
        "orders": [],
    }


def default_config() -> dict[str, Any]:
    return {
        "envPreset": "okx_main_demo",
        "apiKey": "",
        "secretKey": "",
        "passphrase": "",
        "baseUrl": "https://www.okx.com",
        "simulated": True,
        "executionMode": "local",
        "remoteGatewayUrl": "",
        "remoteGatewayToken": "",
        "profiles": default_trading_profiles(),
    }


def default_trading_profiles() -> dict[str, dict[str, Any]]:
    return {
        "okx_main_demo": {
            "envPreset": "okx_main_demo",
            "baseUrl": "https://www.okx.com",
            "simulated": True,
            "apiKey": "",
            "secretKey": "",
            "passphrase": "",
        },
        "okx_main_live": {
            "envPreset": "okx_main_live",
            "baseUrl": "https://www.okx.com",
            "simulated": False,
            "apiKey": "",
            "secretKey": "",
            "passphrase": "",
        },
        "okx_us_live": {
            "envPreset": "okx_us_live",
            "baseUrl": "https://us.okx.com",
            "simulated": False,
            "apiKey": "",
            "secretKey": "",
            "passphrase": "",
        },
        "custom_demo": {
            "envPreset": "custom",
            "baseUrl": "https://www.okx.com",
            "simulated": True,
            "apiKey": "",
            "secretKey": "",
            "passphrase": "",
        },
        "custom_live": {
            "envPreset": "custom",
            "baseUrl": "https://www.okx.com",
            "simulated": False,
            "apiKey": "",
            "secretKey": "",
            "passphrase": "",
        },
    }


def config_profile_key(config: dict[str, Any]) -> str:
    env_preset = str(config.get("envPreset") or "").strip() or "okx_main_demo"
    simulated = bool(config.get("simulated"))
    if env_preset == "okx_main_demo":
        return "okx_main_demo"
    if env_preset == "okx_main_live":
        return "okx_main_live"
    if env_preset == "okx_us_live":
        return "okx_us_live"
    if env_preset == "custom":
        return "custom_demo" if simulated else "custom_live"
    return "custom_demo" if simulated else "custom_live"


def default_miner_state() -> dict[str, Any]:
    return {
        "lastRefreshAt": "",
        "network": {},
        "sources": [],
        "options": [],
        "serialPorts": [],
        "devices": [],
        "pool": {},
        "progress": {},
        "macLotto": {
            "running": False,
            "pid": 0,
            "status": "idle",
            "hashrate": "",
            "lastHashrateAt": "",
            "wallet": "",
            "poolHost": "",
            "poolPort": 0,
            "logTail": [],
        },
        "logs": [],
    }


class JsonStore:
    def __init__(self, path: Path, default_factory) -> None:
        self.path = path
        self.default_factory = default_factory
        self.lock = threading.RLock()
        self.data = default_factory()
        self.load()

    def load(self) -> None:
        with self.lock:
            loaded, _ = secure_load_json(self.path, self.default_factory)
            self.data = deep_merge(self.default_factory(), loaded)

    def current(self) -> dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self.data)

    def replace(self, payload: dict[str, Any]) -> None:
        with self.lock:
            self.data = deep_merge(self.default_factory(), payload)
            secure_dump_json(self.path, self.data)

    def update(self, mutator) -> dict[str, Any]:
        with self.lock:
            data = copy.deepcopy(self.data)
            mutator(data)
            self.data = deep_merge(self.default_factory(), data)
            secure_dump_json(self.path, self.data)
            return copy.deepcopy(self.data)


class ConfigStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.RLock()
        self.runtime_config: dict[str, Any] = default_config()
        self.load()

    def _normalize_state(self, payload: dict[str, Any]) -> dict[str, Any]:
        state = deep_merge(default_config(), payload)
        profiles = deep_merge(default_trading_profiles(), state.get("profiles") or {})
        legacy_has_secret = any(str(state.get(key) or "").strip() for key in ("apiKey", "secretKey", "passphrase"))
        active_key = config_profile_key(state)
        active_profile = deep_merge(default_trading_profiles().get(active_key, {}), profiles.get(active_key) or {})
        if legacy_has_secret and not any(
            str(active_profile.get(key) or "").strip() for key in ("apiKey", "secretKey", "passphrase")
        ):
            for key in ("apiKey", "secretKey", "passphrase"):
                active_profile[key] = str(state.get(key) or "").strip()
            active_profile["baseUrl"] = str(state.get("baseUrl") or active_profile.get("baseUrl") or "").strip() or active_profile.get("baseUrl", "")
            active_profile["simulated"] = bool(state.get("simulated"))
            active_profile["envPreset"] = str(state.get("envPreset") or active_profile.get("envPreset") or "custom")
            profiles[active_key] = active_profile
        state["profiles"] = profiles
        return state

    def current_for_selection(
        self, selection: dict[str, Any] | None = None, state_override: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        state = self._normalize_state(state_override if state_override is not None else self.runtime_config)
        target = deep_merge(deep_merge(default_config(), state), selection or {})
        profile_key = config_profile_key(target)
        profiles = deep_merge(default_trading_profiles(), state.get("profiles") or {})
        profile = deep_merge(default_trading_profiles().get(profile_key, {}), profiles.get(profile_key) or {})

        merged = deep_merge(default_config(), state)
        merged["envPreset"] = str(target.get("envPreset") or profile.get("envPreset") or merged.get("envPreset") or "custom")
        merged["baseUrl"] = str(target.get("baseUrl") or profile.get("baseUrl") or merged.get("baseUrl") or "https://www.okx.com")
        merged["simulated"] = bool(target.get("simulated", profile.get("simulated", merged.get("simulated"))))
        for key in ("apiKey", "secretKey", "passphrase"):
            merged[key] = str(profile.get(key) or "")
        merged["profiles"] = profiles
        return merged

    def load(self) -> None:
        with self.lock:
            loaded, _ = secure_load_json(self.path, dict)
            self.runtime_config = self._normalize_state(loaded)

    def snapshot(self) -> dict[str, Any]:
        with self.lock:
            return copy.deepcopy(self._normalize_state(self.runtime_config))

    def save(self, config: dict[str, Any], persist: bool) -> None:
        with self.lock:
            state = self._normalize_state(self.runtime_config)
            target = deep_merge(deep_merge(default_config(), state), config)
            profile_key = config_profile_key(target)
            profiles = deep_merge(default_trading_profiles(), state.get("profiles") or {})
            profile = deep_merge(default_trading_profiles().get(profile_key, {}), profiles.get(profile_key) or {})

            profile["envPreset"] = str(target.get("envPreset") or profile.get("envPreset") or "custom")
            profile["baseUrl"] = str(target.get("baseUrl") or profile.get("baseUrl") or "https://www.okx.com")
            profile["simulated"] = bool(target.get("simulated", profile.get("simulated")))
            for key in ("apiKey", "secretKey", "passphrase"):
                if key in config:
                    incoming = str(config.get(key) or "").strip()
                    if incoming:
                        profile[key] = incoming
                    elif not str(profile.get(key) or "").strip():
                        profile[key] = ""
            profiles[profile_key] = profile

            for key in ("envPreset", "baseUrl", "simulated", "executionMode", "remoteGatewayUrl"):
                if key in config:
                    state[key] = target[key]
            if "remoteGatewayToken" in config:
                incoming_token = str(config.get("remoteGatewayToken") or "").strip()
                if incoming_token:
                    state["remoteGatewayToken"] = incoming_token
                elif not str(state.get("remoteGatewayToken") or "").strip():
                    state["remoteGatewayToken"] = ""

            state["profiles"] = profiles
            active = self.current_for_selection(state_override=state)
            for key in ("apiKey", "secretKey", "passphrase"):
                state[key] = active[key]
            self.runtime_config = self._normalize_state(state)
            if persist:
                secure_dump_json(self.path, self.runtime_config)

    def current(self) -> dict[str, Any]:
        with self.lock:
            return self.current_for_selection()

    def merged_with_existing_secrets(self, config: dict[str, Any]) -> dict[str, Any]:
        merged = deep_merge(default_config(), config)
        existing = self.current_for_selection(merged)
        for key in ("apiKey", "secretKey", "passphrase", "remoteGatewayToken"):
            if not merged.get(key) and existing.get(key):
                merged[key] = existing[key]
        return merged

    def redacted(self) -> dict[str, Any]:
        config = self.current()
        masks: dict[str, str] = {}
        for key in ("apiKey", "secretKey", "passphrase", "remoteGatewayToken"):
            value = config.get(key, "")
            if value:
                masks[f"{key}Mask"] = value[:4] + "..." + value[-2:]
                config[key] = ""
        config.pop("profiles", None)
        config.update(masks)
        return config


class OkxApiError(RuntimeError):
    pass


def persist_local_orders(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not orders:
        return get_local_recent_orders(limit=80)

    def mutate(state: dict[str, Any]) -> None:
        existing = list(state.get("orders") or [])
        merged = list(orders) + existing
        deduped: list[dict[str, Any]] = []
        seen = set()
        for item in merged:
            key = item.get("ordId") or item.get("clOrdId")
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(copy.deepcopy(item))
            if len(deduped) >= 80:
                break
        deduped.sort(
            key=lambda row: int(row.get("uTime") or row.get("cTime") or 0),
            reverse=True,
        )
        state["orders"] = deduped

    current = LOCAL_ORDER_STORE.update(mutate)
    return list(current.get("orders") or [])


def get_stored_local_orders(inst_type: str = "", limit: int = 20) -> list[dict[str, Any]]:
    state = LOCAL_ORDER_STORE.current()
    items = list(state.get("orders") or [])
    if inst_type:
        expected = inst_type.upper()
        items = [item for item in items if str(item.get("instType") or "").upper() == expected]
    items.sort(
        key=lambda row: int(row.get("uTime") or row.get("cTime") or 0),
        reverse=True,
    )
    return items[:limit]


def derive_orders_from_automation_state(state: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    snapshot = state or AUTOMATION_STATE.current()
    items: list[dict[str, Any]] = []
    for market_name in ("spot", "swap"):
        market = (snapshot.get("markets") or {}).get(market_name) or {}
        order_id = str(market.get("lastOrderId") or "").strip()
        if not order_id:
            continue
        inst_id = str(market.get("instId") or "")
        inst_type = "SWAP" if market_name == "swap" or inst_id.endswith("-SWAP") else "SPOT"
        action_text = str(market.get("lastAction") or market.get("lastMessage") or "")
        position_side = str(market.get("positionSide") or "flat")
        if "卖" in action_text or "空" in action_text or position_side == "short":
            side = "sell"
        else:
            side = "buy"
        stamp = parse_iso(str(market.get("lastTradeAt") or market.get("lastActionAt") or ""))
        millis = str(int(stamp.timestamp() * 1000)) if stamp else str(int(time.time() * 1000))
        size = str(market.get("positionSize") or "0")
        avg_px = str(market.get("entryPrice") or market.get("lastPrice") or "")
        items.append(
            {
                "ordId": order_id,
                "clOrdId": "",
                "instId": inst_id,
                "instType": inst_type,
                "tdMode": "cross" if inst_type == "SWAP" else "cash",
                "side": side,
                "ordType": "market",
                "state": "filled",
                "sz": size,
                "fillSz": size,
                "accFillSz": size,
                "avgPx": avg_px,
                "fillPx": avg_px,
                "px": "",
                "fee": "0",
                "reduceOnly": False,
                "posSide": "net" if inst_type == "SWAP" else "",
                "uTime": millis,
                "cTime": millis,
                "tag": "paper-state-recovered",
            }
        )
    items.sort(
        key=lambda row: int(row.get("uTime") or row.get("cTime") or 0),
        reverse=True,
    )
    return items[:20]


def get_local_recent_orders(inst_type: str = "", limit: int = 20) -> list[dict[str, Any]]:
    stored = get_stored_local_orders(inst_type, limit=limit)
    if stored:
        return stored
    derived = derive_orders_from_automation_state()
    if inst_type:
        expected = inst_type.upper()
        derived = [item for item in derived if str(item.get("instType") or "").upper() == expected]
    return derived[:limit]


def merge_order_feeds(*feeds: list[dict[str, Any]], limit: int = 20) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen = set()
    for feed in feeds:
        for item in feed:
            key = item.get("ordId") or item.get("clOrdId")
            if not key or key in seen:
                continue
            seen.add(key)
            merged.append(item)
    merged.sort(
        key=lambda row: int(row.get("uTime") or row.get("cTime") or 0),
        reverse=True,
    )
    return merged[:limit]


def paper_state_has_activity(state: dict[str, Any] | None = None) -> bool:
    if get_stored_local_orders(limit=1):
        return True
    snapshot = state or AUTOMATION_STATE.current()
    for market in (snapshot.get("markets") or {}).values():
        if safe_decimal(market.get("positionSize"), "0") > 0:
            return True
        if str(market.get("lastOrderId") or "").strip():
            return True
    return False


def record_manual_order_activity(order: dict[str, Any]) -> None:
    order_id = str(order.get("ordId") or order.get("clOrdId") or "")
    if not order_id:
        return
    inst_id = str(order.get("instId") or "")
    inst_type = str(order.get("instType") or ("SWAP" if inst_id.endswith("-SWAP") else "SPOT")).upper()
    market = "swap" if inst_type == "SWAP" else "spot"
    side = "买入" if str(order.get("side") or "").lower() == "buy" else "卖出"
    stamp = now_local_iso()

    def mutate(state: dict[str, Any]) -> None:
        state["orderCountToday"] = int(state.get("orderCountToday", 0)) + 1
        state["lastActionAt"] = stamp
        target = state["markets"].setdefault(market, default_market_state())
        target["lastActionAt"] = stamp
        target["lastTradeAt"] = stamp
        target["lastOrderId"] = order_id
        target["lastAction"] = f"手动{side}"
        target["lastMessage"] = f"手动{side} · {inst_id}"

    AUTOMATION_STATE.update(mutate)


def reconcile_automation_state_from_markets() -> None:
    snapshot = AUTOMATION_STATE.current()
    derived = derive_orders_from_automation_state(snapshot)
    latest_stamp = ""
    for market in (snapshot.get("markets") or {}).values():
        stamp = str(market.get("lastActionAt") or market.get("lastTradeAt") or "")
        if stamp and stamp > latest_stamp:
            latest_stamp = stamp

    if not derived and not latest_stamp:
        return

    if derived and not get_stored_local_orders(limit=1):
        persist_local_orders(derived)

    def mutate(state: dict[str, Any]) -> None:
        if latest_stamp and not state.get("lastActionAt"):
            state["lastActionAt"] = latest_stamp
        if derived and int(state.get("orderCountToday", 0)) <= 0:
            today = datetime.now().date().isoformat()
            if not state.get("today"):
                state["today"] = today
            if state.get("today") == today:
                state["orderCountToday"] = len(derived)

    AUTOMATION_STATE.update(mutate)


class OkxClient:
    def __init__(self, config: dict[str, Any]) -> None:
        self.api_key = config["apiKey"]
        self.secret_key = config["secretKey"]
        self.passphrase = config["passphrase"]
        self.base_url = config.get("baseUrl") or "https://www.okx.com"
        self.simulated = bool(config.get("simulated"))
        self.session = shared_http_session(config)
        self.timeout = 12

    def _sign_headers(self, method: str, path: str, body: str = "") -> dict[str, str]:
        timestamp = utc_timestamp()
        prehash = f"{timestamp}{method.upper()}{path}{body}"
        digest = hmac.new(
            self.secret_key.encode("utf-8"),
            prehash.encode("utf-8"),
            hashlib.sha256,
        ).digest()
        signature = base64.b64encode(digest).decode("utf-8")
        headers = {
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase,
            "Content-Type": "application/json",
        }
        if self.simulated:
            headers["x-simulated-trading"] = "1"
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: dict[str, Any] | None = None,
        private: bool = True,
    ) -> dict[str, Any]:
        started_at = time.perf_counter()
        query = ""
        if params:
            filtered = {k: v for k, v in params.items() if v not in (None, "", [])}
            if filtered:
                query = "?" + urllib.parse.urlencode(filtered)
        body = ""
        if payload is not None and method.upper() != "GET":
            body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False)
        headers = self._sign_headers(method, path + query, body) if private else {}
        retryable_public = (not private and method.upper() == "GET")
        attempts = 3 if retryable_public else 1
        last_error: Exception | None = None

        for attempt in range(attempts):
            try:
                response = self.session.request(
                    method=method.upper(),
                    url=self.base_url + path + query,
                    data=body if body else None,
                    headers=headers,
                    timeout=self.timeout + (5 if retryable_public else 0),
                )
            except requests.RequestException as exc:
                last_error = OkxApiError(f"网络请求失败: {exc}")
                if attempt < attempts - 1:
                    time.sleep(0.35 * (attempt + 1))
                    continue
                raise last_error

            try:
                data = response.json()
            except ValueError as exc:
                last_error = OkxApiError(f"响应不是合法 JSON: {response.text[:200]}")
                if retryable_public and attempt < attempts - 1:
                    time.sleep(0.35 * (attempt + 1))
                    continue
                raise last_error from exc

            if response.status_code >= 400:
                message = data.get("msg") or data.get("error") or response.text
                last_error = OkxApiError(f"HTTP {response.status_code}: {message}")
                if retryable_public and response.status_code >= 500 and attempt < attempts - 1:
                    time.sleep(0.35 * (attempt + 1))
                    continue
                raise last_error

            code = data.get("code")
            if code not in (None, 0, "0"):
                last_error = OkxApiError(f"{code}: {data.get('msg') or '接口返回错误'}")
                if retryable_public and attempt < attempts - 1:
                    time.sleep(0.35 * (attempt + 1))
                    continue
                raise last_error

            if isinstance(data, dict):
                data["_clientElapsedMs"] = round((time.perf_counter() - started_at) * 1000, 2)
            return data

        raise last_error or OkxApiError("请求失败")

    @staticmethod
    def _extract_data_or_raise(response: dict[str, Any]) -> dict[str, Any]:
        data = response.get("data") or [{}]
        first = data[0] if data else {}
        s_code = first.get("sCode")
        if s_code not in (None, "", 0, "0"):
            raise OkxApiError(f"{s_code}: {first.get('sMsg') or '接口执行失败'}")
        return first

    def _paper_enabled(self) -> bool:
        return self.simulated

    def _paper_state_authoritative(self) -> bool:
        return self._paper_enabled() and paper_state_has_activity(self._paper_state())

    @staticmethod
    def _binance_symbol(inst_id: str) -> str:
        return inst_id.replace("-SWAP", "").replace("-", "").upper()

    @staticmethod
    def _binance_interval(bar: str) -> str:
        mapping = {
            "1m": "1m",
            "3m": "3m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "1H": "1h",
            "4H": "4h",
            "1D": "1d",
        }
        return mapping.get(bar, "5m")

    def _binance_get_json(self, url: str, params: dict[str, Any] | None = None) -> Any:
        response = requests.get(
            url,
            params=params,
            headers={"accept": "application/json", "user-agent": "OKXLocalApp/1.0"},
            timeout=10,
        )
        response.raise_for_status()
        return response.json()

    def _paper_state(self) -> dict[str, Any]:
        return AUTOMATION_STATE.current()

    def _paper_total_eq(self) -> Decimal:
        state = self._paper_state()
        total_eq = safe_decimal(state.get("currentEq"), "0")
        if total_eq > 0:
            return total_eq
        total_eq = safe_decimal(state.get("sessionStartEq"), "0")
        if total_eq > 0:
            return total_eq
        return Decimal("10000")

    def _paper_account_balance(self) -> dict[str, Any]:
        state = self._paper_state()
        automation = AUTOMATION_CONFIG.current()
        total_eq = self._paper_total_eq()
        spot_market = (state.get("markets") or {}).get("spot") or {}
        spot_inst = str(automation.get("spotInstId") or "BTC-USDT")
        base_ccy = spot_inst.split("-")[0]
        base_size = safe_decimal(spot_market.get("positionSize"), "0")
        last_price = safe_decimal(spot_market.get("lastPrice"), "0")
        if last_price <= 0:
            try:
                ticker = self._fallback_ticker(spot_inst)
                last_price = safe_decimal((ticker.get("data") or [{}])[0].get("last"), "0")
            except Exception:
                last_price = Decimal("0")
        base_notional = base_size * last_price
        usdt_avail = max(Decimal("0"), total_eq - base_notional)
        return {
            "code": "0",
            "data": [
                {
                    "totalEq": decimal_to_str(total_eq),
                    "details": [
                        {
                            "ccy": "USDT",
                            "availBal": decimal_to_str(usdt_avail),
                            "cashBal": decimal_to_str(usdt_avail),
                            "eqUsd": decimal_to_str(usdt_avail),
                            "eq": decimal_to_str(usdt_avail),
                        },
                        {
                            "ccy": base_ccy,
                            "availBal": decimal_to_str(base_size),
                            "cashBal": decimal_to_str(base_size),
                            "eqUsd": decimal_to_str(base_notional),
                            "eq": decimal_to_str(base_size),
                        },
                    ],
                }
            ],
        }

    def _paper_positions(self, inst_id: str | None = None) -> dict[str, Any]:
        state = self._paper_state()
        swap_market = (state.get("markets") or {}).get("swap") or {}
        position_side = str(swap_market.get("positionSide") or "flat")
        position_size = safe_decimal(swap_market.get("positionSize"), "0")
        if position_side == "flat" or position_size <= 0:
            return {"code": "0", "data": []}
        signed_pos = position_size if position_side == "long" else -position_size
        payload = {
            "instId": inst_id or swap_market.get("instId") or AUTOMATION_CONFIG.current().get("swapInstId") or "",
            "pos": decimal_to_str(signed_pos),
            "avgPx": str(swap_market.get("entryPrice") or ""),
            "posSide": "net",
            "upl": "0",
        }
        return {"code": "0", "data": [payload]}

    def _paper_recent_orders(self) -> dict[str, Any]:
        return {"code": "0", "data": get_local_recent_orders(limit=20)}

    def _paper_place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        inst_id = str(payload.get("instId") or "")
        is_swap = inst_id.endswith("-SWAP")
        side = str(payload.get("side") or "buy")
        now_ms = str(int(time.time() * 1000))
        cl_ord_id = str(payload.get("clOrdId") or f"paper-{secrets.token_hex(6)}")
        ord_id = f"paper-{secrets.token_hex(8)}"

        ticker = self._fallback_ticker(inst_id)
        fill_px = safe_decimal((ticker.get("data") or [{}])[0].get("last"), "0")
        if fill_px <= 0:
            raise OkxApiError("本地模拟盘未拿到可用价格，无法生成纸面订单")

        filled_size = safe_decimal(payload.get("sz"), "0")
        state = self._paper_state()
        markets = state.get("markets") or {}
        automation = AUTOMATION_CONFIG.current()

        if is_swap:
            market_key = "swap"
            current = copy.deepcopy(markets.get("swap") or default_market_state())
            current_side = str(current.get("positionSide") or "flat")
            current_size = safe_decimal(current.get("positionSize"), "0")
            reduce_only = bool(payload.get("reduceOnly"))
            if side == "buy":
                if current_side == "short":
                    next_size = max(Decimal("0"), current_size - filled_size)
                    next_side = "flat" if next_size <= 0 else "short"
                else:
                    next_size = current_size if reduce_only else current_size + filled_size
                    next_side = "flat" if next_size <= 0 else "long"
            else:
                if current_side == "long":
                    next_size = max(Decimal("0"), current_size - filled_size)
                    next_side = "flat" if next_size <= 0 else "long"
                else:
                    next_size = current_size if reduce_only else current_size + filled_size
                    next_side = "flat" if next_size <= 0 else "short"

            def mutate(current_state: dict[str, Any]) -> None:
                market = current_state["markets"].setdefault(market_key, default_market_state())
                market.update(
                    {
                        "enabled": True,
                        "instId": inst_id,
                        "positionSide": next_side,
                        "positionSize": decimal_to_str(next_size),
                        "positionNotional": decimal_to_str(next_size * fill_px),
                        "entryPrice": "" if next_side == "flat" else decimal_to_str(fill_px),
                        "lastPrice": decimal_to_str(fill_px),
                        "lastTradeAt": now_local_iso(),
                        "lastActionAt": now_local_iso(),
                        "lastOrderId": ord_id,
                    }
                )

            AUTOMATION_STATE.update(mutate)
            inst_type = "SWAP"
            pos_side = "net"
        else:
            market_key = "spot"
            current = copy.deepcopy(markets.get("spot") or default_market_state())
            current_size = safe_decimal(current.get("positionSize"), "0")
            if side == "buy":
                quote_budget = filled_size
                base_fill = (quote_budget / fill_px) if fill_px > 0 else Decimal("0")
                next_size = current_size + base_fill
                actual_fill_size = base_fill
            else:
                actual_fill_size = min(current_size, filled_size)
                next_size = max(Decimal("0"), current_size - actual_fill_size)

            def mutate(current_state: dict[str, Any]) -> None:
                market = current_state["markets"].setdefault(market_key, default_market_state())
                market.update(
                    {
                        "enabled": True,
                        "instId": inst_id,
                        "positionSide": "flat" if next_size <= 0 else "long",
                        "positionSize": decimal_to_str(next_size),
                        "positionNotional": decimal_to_str(next_size * fill_px),
                        "entryPrice": "" if next_size <= 0 else decimal_to_str(fill_px),
                        "lastPrice": decimal_to_str(fill_px),
                        "lastTradeAt": now_local_iso(),
                        "lastActionAt": now_local_iso(),
                        "lastOrderId": ord_id,
                    }
                )

            AUTOMATION_STATE.update(mutate)
            inst_type = "SPOT"
            pos_side = ""
            filled_size = actual_fill_size

        order = {
            "ordId": ord_id,
            "clOrdId": cl_ord_id,
            "instId": inst_id,
            "instType": inst_type,
            "tdMode": str(payload.get("tdMode") or ("cross" if is_swap else "cash")),
            "side": side,
            "ordType": str(payload.get("ordType") or "market"),
            "state": "filled",
            "sz": decimal_to_str(safe_decimal(payload.get("sz"), "0")),
            "fillSz": decimal_to_str(filled_size),
            "accFillSz": decimal_to_str(filled_size),
            "avgPx": decimal_to_str(fill_px),
            "fillPx": decimal_to_str(fill_px),
            "px": str(payload.get("px") or ""),
            "fee": "0",
            "reduceOnly": bool(payload.get("reduceOnly")),
            "posSide": pos_side,
            "uTime": now_ms,
            "cTime": now_ms,
            "tag": "paper-sim",
        }
        PRIVATE_ORDER_STREAM._ingest_orders([order])
        return {"code": "0", "data": [order], "_paperSim": True}

    def _fallback_public_instruments(self, inst_type: str, inst_id: str | None = None) -> dict[str, Any]:
        inst = inst_id or ""
        base = inst.split("-")[0] if inst else "BTC"
        if inst_type.upper() == "SWAP":
            ct_val = "0.01" if base == "BTC" else "0.1" if base == "ETH" else "1"
            row = {"instId": inst, "tickSz": "0.1", "lotSz": "1", "minSz": "1", "ctVal": ct_val}
        else:
            row = {"instId": inst, "tickSz": "0.01", "lotSz": "0.00001", "minSz": "0.00001"}
        return {"code": "0", "data": [row]}

    def _fallback_ticker(self, inst_id: str) -> dict[str, Any]:
        symbol = self._binance_symbol(inst_id)
        if inst_id.endswith("-SWAP"):
            data = self._binance_get_json("https://fapi.binance.com/fapi/v1/ticker/price", {"symbol": symbol})
        else:
            data = self._binance_get_json("https://api.binance.com/api/v3/ticker/price", {"symbol": symbol})
        return {"code": "0", "data": [{"instId": inst_id, "last": str(data.get("price") or "0")}]}

    def _fallback_history_candles(self, inst_id: str, bar: str, limit: int) -> dict[str, Any]:
        symbol = self._binance_symbol(inst_id)
        interval = self._binance_interval(bar)
        if inst_id.endswith("-SWAP"):
            rows = self._binance_get_json("https://fapi.binance.com/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        else:
            rows = self._binance_get_json("https://api.binance.com/api/v3/klines", {"symbol": symbol, "interval": interval, "limit": limit})
        candles = [
            [
                str(row[0]),
                row[1],
                row[2],
                row[3],
                row[4],
                row[5],
                row[7],
                row[7],
                "1",
            ]
            for row in rows
        ]
        return {"code": "0", "data": candles}

    def _fallback_mark_price(self, inst_id: str) -> dict[str, Any]:
        symbol = self._binance_symbol(inst_id)
        data = self._binance_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": symbol})
        return {"code": "0", "data": [{"instId": inst_id, "markPx": str(data.get("markPrice") or data.get("indexPrice") or "0")}]}

    def _fallback_funding_rate(self, inst_id: str) -> dict[str, Any]:
        symbol = self._binance_symbol(inst_id)
        data = self._binance_get_json("https://fapi.binance.com/fapi/v1/premiumIndex", {"symbol": symbol})
        return {"code": "0", "data": [{"instId": inst_id, "fundingRate": str(data.get("lastFundingRate") or "0")}]}

    def _fallback_open_interest(self, inst_id: str) -> dict[str, Any]:
        symbol = self._binance_symbol(inst_id)
        data = self._binance_get_json("https://fapi.binance.com/fapi/v1/openInterest", {"symbol": symbol})
        return {"code": "0", "data": [{"instId": inst_id, "oi": str(data.get("openInterest") or "0")}]}

    def get_account_balance(self, ccy: str | None = None) -> dict[str, Any]:
        params = {"ccy": ccy} if ccy else None
        if self._paper_state_authoritative():
            return self._paper_account_balance()
        try:
            return self._request("GET", "/api/v5/account/balance", params=params)
        except Exception:
            if self._paper_enabled():
                return self._paper_account_balance()
            raise

    def get_funding_balances(self, ccy: str | None = None) -> dict[str, Any]:
        params = {"ccy": ccy} if ccy else None
        if self._paper_state_authoritative():
            return {"code": "0", "data": [], "_paperSim": True}
        try:
            return self._request("GET", "/api/v5/asset/balances", params=params)
        except Exception:
            if self._paper_enabled():
                return {"code": "0", "data": [], "_paperSim": True}
            raise

    def get_asset_valuation(self, ccy: str = "USDT") -> dict[str, Any]:
        params = {"ccy": ccy} if ccy else None
        if self._paper_state_authoritative():
            return {"code": "0", "data": [{"ccy": ccy or "USDT", "totalBal": "0", "ts": str(int(time.time() * 1000))}], "_paperSim": True}
        try:
            return self._request("GET", "/api/v5/asset/asset-valuation", params=params)
        except Exception:
            if self._paper_enabled():
                return {"code": "0", "data": [{"ccy": ccy or "USDT", "totalBal": "0", "ts": str(int(time.time() * 1000))}], "_paperSim": True}
            raise

    def get_positions(self, inst_id: str | None = None) -> dict[str, Any]:
        params = {"instId": inst_id} if inst_id else None
        if self._paper_state_authoritative():
            return self._paper_positions(inst_id)
        try:
            return self._request("GET", "/api/v5/account/positions", params=params)
        except Exception:
            if self._paper_enabled():
                return self._paper_positions(inst_id)
            raise

    def get_recent_orders(self, inst_type: str = "", limit: int = 20) -> dict[str, Any]:
        params = {"limit": limit}
        if inst_type:
            params["instType"] = inst_type
        if self._paper_state_authoritative():
            return {
                "code": "0",
                "data": get_local_recent_orders(inst_type, limit=limit),
                "_paperSim": True,
            }
        try:
            return self._request("GET", "/api/v5/trade/orders-history", params=params)
        except Exception:
            if self._paper_enabled():
                return self._paper_recent_orders()
            raise

    def get_ticker(self, inst_id: str) -> dict[str, Any]:
        try:
            return self._request(
                "GET",
                "/api/v5/market/ticker",
                params={"instId": inst_id},
                private=False,
            )
        except Exception:
            if self._paper_enabled():
                return self._fallback_ticker(inst_id)
            raise

    def get_history_candles(self, inst_id: str, bar: str, limit: int) -> dict[str, Any]:
        try:
            return self._request(
                "GET",
                "/api/v5/market/history-candles",
                params={"instId": inst_id, "bar": bar, "limit": limit},
                private=False,
            )
        except Exception:
            if self._paper_enabled():
                return self._fallback_history_candles(inst_id, bar, limit)
            raise

    def get_public_instruments(self, inst_type: str, inst_id: str | None = None) -> dict[str, Any]:
        params = {"instType": inst_type}
        if inst_id:
            params["instId"] = inst_id
        try:
            return self._request(
                "GET",
                "/api/v5/public/instruments",
                params=params,
                private=False,
            )
        except Exception:
            if self._paper_enabled():
                return self._fallback_public_instruments(inst_type, inst_id)
            raise

    def get_mark_price(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        try:
            return self._request(
                "GET",
                "/api/v5/public/mark-price",
                params={"instType": inst_type, "instId": inst_id},
                private=False,
            )
        except Exception:
            if self._paper_enabled():
                return self._fallback_mark_price(inst_id)
            raise

    def get_open_interest(self, inst_type: str, inst_id: str) -> dict[str, Any]:
        try:
            return self._request(
                "GET",
                "/api/v5/public/open-interest",
                params={"instType": inst_type, "instId": inst_id},
                private=False,
            )
        except Exception:
            if self._paper_enabled():
                return self._fallback_open_interest(inst_id)
            raise

    def get_funding_rate(self, inst_id: str) -> dict[str, Any]:
        try:
            return self._request(
                "GET",
                "/api/v5/public/funding-rate",
                params={"instId": inst_id},
                private=False,
            )
        except Exception:
            if self._paper_enabled():
                return self._fallback_funding_rate(inst_id)
            raise

    def get_account_config(self) -> dict[str, Any]:
        try:
            return self._request("GET", "/api/v5/account/config")
        except Exception:
            if self._paper_enabled():
                return {"code": "0", "data": [{"posMode": "net_mode"}]}
            raise

    def set_position_mode(self, pos_mode: str) -> dict[str, Any]:
        try:
            result = self._request(
                "POST",
                "/api/v5/account/set-position-mode",
                payload={"posMode": pos_mode},
            )
            self._extract_data_or_raise(result)
            return result
        except Exception:
            if self._paper_enabled():
                return {"code": "0", "data": [{"posMode": pos_mode, "sCode": "0"}], "_paperSim": True}
            raise

    def set_leverage(self, inst_id: str, lever: str, mgn_mode: str) -> dict[str, Any]:
        try:
            result = self._request(
                "POST",
                "/api/v5/account/set-leverage",
                payload={
                    "instId": inst_id,
                    "lever": lever,
                    "mgnMode": mgn_mode,
                },
            )
            self._extract_data_or_raise(result)
            return result
        except Exception:
            if self._paper_enabled():
                return {
                    "code": "0",
                    "data": [{"instId": inst_id, "lever": lever, "mgnMode": mgn_mode, "sCode": "0"}],
                    "_paperSim": True,
                }
            raise

    def place_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            result = self._request("POST", "/api/v5/trade/order", payload=payload)
            self._extract_data_or_raise(result)
            return result
        except Exception:
            if self._paper_enabled():
                return self._paper_place_order(payload)
            raise

    def get_order(self, inst_id: str, ord_id: str | None = None, cl_ord_id: str | None = None) -> dict[str, Any]:
        params = {"instId": inst_id}
        if ord_id:
            params["ordId"] = ord_id
        if cl_ord_id:
            params["clOrdId"] = cl_ord_id
        return self._request("GET", "/api/v5/trade/order", params=params)


def derive_private_ws_url(config: dict[str, Any]) -> str:
    base_url = str(config.get("baseUrl") or "https://www.okx.com").lower()
    if bool(config.get("simulated")):
        return "wss://wspap.okx.com:8443/ws/v5/private"
    if "us.okx.com" in base_url:
        return "wss://wsus.okx.com:8443/ws/v5/private"
    return "wss://ws.okx.com:8443/ws/v5/private"


def okx_ws_login_args(config: dict[str, Any]) -> dict[str, str]:
    timestamp = str(int(time.time()))
    prehash = f"{timestamp}GET/users/self/verify"
    digest = hmac.new(
        str(config["secretKey"]).encode("utf-8"),
        prehash.encode("utf-8"),
        hashlib.sha256,
    ).digest()
    signature = base64.b64encode(digest).decode("utf-8")
    return {
        "apiKey": str(config["apiKey"]),
        "passphrase": str(config["passphrase"]),
        "timestamp": timestamp,
        "sign": signature,
    }


class OkxPrivateOrderStream:
    def __init__(self) -> None:
        self.thread: threading.Thread | None = None
        self.thread_lock = threading.RLock()
        self.state_lock = threading.RLock()
        self.connected = False
        self.last_error = ""
        self.last_event_at = ""
        self.orders: list[dict[str, Any]] = []
        self.config_signature = ""

    def ensure_running(self) -> None:
        with self.thread_lock:
            if self.thread and self.thread.is_alive():
                return
            self.thread = threading.Thread(
                target=self._thread_main,
                name="okx-private-order-stream",
                daemon=True,
            )
            self.thread.start()

    def mark_dirty(self) -> None:
        with self.state_lock:
            self.config_signature = ""

    def snapshot(self) -> dict[str, Any]:
        with self.state_lock:
            return {
                "connected": self.connected,
                "lastError": self.last_error,
                "lastEventAt": self.last_event_at,
                "orderCount": len(self.orders),
            }

    def get_recent_orders(self, inst_type: str = "", limit: int = 20) -> list[dict[str, Any]]:
        with self.state_lock:
            items = list(self.orders)
        if inst_type:
            items = [item for item in items if str(item.get("instType") or "").upper() == inst_type.upper()]
        return items[:limit]

    def _thread_main(self) -> None:
        asyncio.run(self._run_forever())

    async def _run_forever(self) -> None:
        backoff = 1.5
        while True:
            config = CONFIG.current()
            valid, _ = validate_config(config)
            if not valid:
                await asyncio.sleep(3)
                continue
            current_signature = config_session_key(config)
            try:
                await self._run_socket(config, current_signature)
                backoff = 1.5
            except Exception as exc:
                with self.state_lock:
                    self.connected = False
                    self.last_error = str(exc)
                await asyncio.sleep(min(backoff, 12))
                backoff = min(backoff * 1.8, 12)

    async def _run_socket(self, config: dict[str, Any], signature: str) -> None:
        url = derive_private_ws_url(config)
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=45)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.ws_connect(url, heartbeat=20) as ws:
                await ws.send_json({"op": "login", "args": [okx_ws_login_args(config)]})
                login_ok = False
                async for msg in ws:
                    if msg.type != aiohttp.WSMsgType.TEXT:
                        continue
                    payload = json.loads(msg.data)
                    event = payload.get("event")
                    if event == "login":
                        if str(payload.get("code", "0")) not in {"0", ""}:
                            raise RuntimeError(payload.get("msg") or "私有 WS 登录失败")
                        await ws.send_json(
                            {
                                "op": "subscribe",
                                "args": [
                                    {"channel": "orders", "instType": "SPOT"},
                                    {"channel": "orders", "instType": "SWAP"},
                                ],
                            }
                        )
                        login_ok = True
                        with self.state_lock:
                            self.connected = True
                            self.last_error = ""
                            self.config_signature = signature
                        continue
                    if event == "error":
                        raise RuntimeError(payload.get("msg") or "私有 WS 订阅失败")
                    if not login_ok:
                        continue
                    if payload.get("arg", {}).get("channel") == "orders":
                        self._ingest_orders(payload.get("data") or [])
                    if self.config_signature and self.config_signature != config_session_key(CONFIG.current()):
                        raise RuntimeError("配置已变更，重新连接订单快通道")

    def _ingest_orders(self, orders: list[dict[str, Any]]) -> None:
        if not orders:
            return
        with self.state_lock:
            merged = list(orders) + self.orders
            deduped: list[dict[str, Any]] = []
            seen = set()
            for item in merged:
                key = item.get("ordId") or item.get("clOrdId")
                if not key or key in seen:
                    continue
                seen.add(key)
                deduped.append(item)
                if len(deduped) >= 40:
                    break
            self.orders = deduped
            self.last_event_at = now_local_iso()
            self.connected = True
            self.last_error = ""
        persist_local_orders(deduped[:40])


def validate_config(config: dict[str, Any]) -> tuple[bool, str]:
    execution_mode = str(config.get("executionMode") or "local").strip()
    if execution_mode not in ("local", "remote"):
        return False, "执行节点模式仅支持 local 或 remote"
    if execution_mode == "remote":
        gateway_url = remote_gateway_url(config)
        if not gateway_url:
            return False, "远端执行模式缺少远端执行节点 URL"
        if not gateway_url.startswith(("http://", "https://")):
            return False, "远端执行节点 URL 必须以 http:// 或 https:// 开头"
        return True, ""
    required = ["apiKey", "secretKey", "passphrase"]
    missing = [key for key in required if not config.get(key)]
    if missing:
        return False, f"缺少字段: {', '.join(missing)}"
    return True, ""


def explain_auth_error(message: str, config: dict[str, Any]) -> str:
    simulated = bool(config.get("simulated"))
    env_label = "模拟盘" if simulated else "实盘"
    hints: list[str] = []
    if "50101" in message:
        hints.append(f"当前是{env_label}模式，API Key 很可能创建在另一个环境。模拟盘必须在“模拟交易”里单独创建 API Key。")
    if "50105" in message:
        hints.append("Passphrase 错了。Passphrase 是创建 API Key 时你自己填写的口令，忘了只能重建 API Key。")
    if "50110" in message or "IP" in message:
        hints.append("API Key 开了 IP 白名单，但这台机器当前出口 IP 不在白名单里。")
    if "50111" in message:
        hints.append("API Key 无效，可能输错、删掉了，或者不是当前账户/环境生成的。")
    if "50113" in message:
        hints.append("签名失败，通常是 Secret Key 不对。")
    if not hints and "401" in message:
        hints.append(
            f"这是鉴权失败。你现在保存的是{env_label}模式；如果你填的是实盘 Key，请切到实盘。如果你要跑模拟盘，请到 OKX 的“模拟交易 -> 个人中心”重新创建一组 Demo API。"
        )
    return f"{message}；排查建议：{' '.join(hints)}" if hints else message


def validate_automation_config(config: dict[str, Any]) -> tuple[bool, str, dict[str, Any]]:
    normalized = deep_merge(default_automation_config(), config)
    if normalized.get("strategyPreset") not in ("dual_engine", "btc_lotto"):
        return False, "策略模式不支持", normalized
    fast = int(normalized.get("fastEma", 0))
    slow = int(normalized.get("slowEma", 0))
    poll = int(normalized.get("pollSeconds", 0))
    cooldown = int(normalized.get("cooldownSeconds", 0))
    max_orders = int(normalized.get("maxOrdersPerDay", 0))
    if fast < 2 or slow <= fast:
        return False, "EMA 参数不合法，必须满足 slow > fast >= 2", normalized
    if poll < 5 or poll > 300:
        return False, "轮询秒数需在 5 到 300 之间", normalized
    if cooldown < 0 or cooldown > 3600:
        return False, "冷却秒数需在 0 到 3600 之间", normalized
    if max_orders < 1 or max_orders > 500:
        return False, "每日最大订单数需在 1 到 500 之间", normalized
    if normalized.get("swapStrategyMode") not in ("long_only", "short_only", "trend_follow"):
        return False, "永续策略模式不支持", normalized
    if normalized.get("swapTdMode") not in ("cross", "isolated"):
        return False, "永续保证金模式仅支持 cross 或 isolated", normalized
    for field in (
        "spotQuoteBudget",
        "spotMaxExposure",
        "swapContracts",
        "swapLeverage",
        "stopLossPct",
        "takeProfitPct",
        "maxDailyLossPct",
    ):
        if safe_decimal(normalized.get(field), "0") < 0:
            return False, f"{field} 不能小于 0", normalized
    return True, "", normalized


def strategy_label(preset: str) -> str:
    return {
        "dual_engine": "标准双引擎",
        "btc_lotto": "BTC 乐透机",
    }.get(preset, "标准双引擎")


def strategy_symbol_label(config: dict[str, Any]) -> str:
    for key in ("spotInstId", "swapInstId"):
        inst_id = str(config.get(key, "") or "").strip()
        if inst_id:
            return inst_id.split("-")[0]
    return "OKX"


def strategy_scope_label(config: dict[str, Any]) -> str:
    scopes: list[str] = []
    if config.get("spotEnabled"):
        scopes.append("现货")
    if config.get("swapEnabled"):
        scopes.append("永续")
    return "+".join(scopes) if scopes else "观察"


def strategy_mode_label(config: dict[str, Any]) -> str:
    if not config.get("swapEnabled"):
        return "现货执行"
    mode = str(config.get("swapStrategyMode", "trend_follow"))
    if mode == "trend_follow":
        return "顺势双向"
    if mode == "short_only":
        return "只做空"
    return "只做多"


def strategy_mode_badge(config: dict[str, Any]) -> str:
    if not config.get("swapEnabled"):
        return "现货"
    mode = str(config.get("swapStrategyMode", "trend_follow"))
    if mode == "trend_follow":
        return "双向"
    if mode == "short_only":
        return "只空"
    return "只多"


def strategy_short_name(config: dict[str, Any], rank: int | None = None) -> str:
    prefix = f"S{rank:02d} " if rank else ""
    symbol = strategy_symbol_label(config)
    preset = "乐透" if str(config.get("strategyPreset", "")) == "btc_lotto" else "双引擎"
    leverage = f"{int(safe_decimal(config.get('swapLeverage'), '1'))}x" if config.get("swapEnabled") else ""
    tail = " ".join(part for part in (strategy_mode_badge(config), leverage) if part)
    return f"{prefix}{symbol} {preset} {config.get('bar', '5m')} EMA{config['fastEma']}/{config['slowEma']}{(' ' + tail) if tail else ''}"


def strategy_detail_line(config: dict[str, Any], origin_label: str = "") -> str:
    parts = [strategy_scope_label(config)]
    if config.get("swapEnabled"):
        parts.append(f"{strategy_mode_label(config)} · {int(safe_decimal(config.get('swapLeverage'), '1'))}x")
    else:
        parts.append(strategy_mode_label(config))
    parts.append(f"SL {config['stopLossPct']}% / TP {config['takeProfitPct']}%")
    if origin_label:
        parts.append(origin_label)
    return " · ".join(part for part in parts if part)


def export_slug(value: str) -> str:
    slug = re.sub(r"[^a-zA-Z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "strategy"


def compact_metric(value: Any, scale: str = "0.01") -> str:
    return decimal_to_str(safe_decimal(value, "0").quantize(Decimal(scale)))


def build_cl_ord_id(prefix: str) -> str:
    stamp = int(time.time() * 1000)
    return f"cc{prefix}{stamp}"[-32:]


def get_closed_candles(client: OkxClient, inst_id: str, bar: str, limit: int) -> list[dict[str, Any]]:
    response = client.get_history_candles(inst_id, bar, limit)
    candles: list[dict[str, Any]] = []
    for row in reversed(response.get("data", [])):
        if len(row) < 9 or str(row[8]) != "1":
            continue
        candles.append(
            {
                "ts": row[0],
                "open": safe_decimal(row[1]),
                "high": safe_decimal(row[2]),
                "low": safe_decimal(row[3]),
                "close": safe_decimal(row[4]),
                "confirm": row[8],
            }
        )
    return candles


def ema(values: list[Decimal], period: int) -> list[Decimal]:
    if not values:
        return []
    multiplier = Decimal("2") / Decimal(period + 1)
    current = values[0]
    output = [current]
    for value in values[1:]:
        current = (value - current) * multiplier + current
        output.append(current)
    return output


def build_signal(candles: list[dict[str, Any]], fast: int, slow: int) -> dict[str, Any]:
    closes = [row["close"] for row in candles]
    if len(closes) < slow + 2:
        raise OkxApiError("K 线样本不足，无法计算 EMA")
    fast_values = ema(closes, fast)
    slow_values = ema(closes, slow)
    prev_fast = fast_values[-2]
    prev_slow = slow_values[-2]
    curr_fast = fast_values[-1]
    curr_slow = slow_values[-1]
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        signal = "bull_cross"
    elif prev_fast >= prev_slow and curr_fast < curr_slow:
        signal = "bear_cross"
    else:
        signal = "hold"
    if curr_fast > curr_slow:
        trend = "up"
    elif curr_fast < curr_slow:
        trend = "down"
    else:
        trend = "flat"
    return {
        "signal": signal,
        "trend": trend,
        "fastValue": decimal_to_str(curr_fast),
        "slowValue": decimal_to_str(curr_slow),
        "lastClose": decimal_to_str(closes[-1]),
    }


def normalize_research_options(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    config_payload = copy.deepcopy(payload)
    history_limit = int(config_payload.pop("historyLimit", 240) or 240)
    optimization_depth = str(config_payload.pop("optimizationDepth", "quick") or "quick")
    include_alt_bars = bool(config_payload.pop("includeAltBars", True))
    race_size = int(config_payload.pop("raceSize", 10) or 10)
    evolution_loops = int(config_payload.pop("evolutionLoops", 4) or 4)
    enable_hybrid = bool(config_payload.pop("enableHybrid", True))
    enable_fine_tune = bool(config_payload.pop("enableFineTune", True))
    population_size = int(config_payload.pop("populationSize", max(race_size * 4, 24)) or max(race_size * 4, 24))
    return config_payload, {
        "historyLimit": max(120, min(history_limit, 720)),
        "optimizationDepth": optimization_depth if optimization_depth in {"quick", "standard"} else "quick",
        "includeAltBars": include_alt_bars,
        "raceSize": max(6, min(race_size, 16)),
        "populationSize": max(12, min(population_size, 96)),
        "evolutionLoops": max(2, min(evolution_loops, 6)),
        "enableHybrid": enable_hybrid,
        "enableFineTune": enable_fine_tune,
    }


def crossover_signal(
    fast_values: list[Decimal],
    slow_values: list[Decimal],
    index: int,
) -> tuple[str, str]:
    if index <= 0:
        return "hold", "flat"
    prev_fast = fast_values[index - 1]
    prev_slow = slow_values[index - 1]
    curr_fast = fast_values[index]
    curr_slow = slow_values[index]
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        signal = "bull_cross"
    elif prev_fast >= prev_slow and curr_fast < curr_slow:
        signal = "bear_cross"
    else:
        signal = "hold"
    if curr_fast > curr_slow:
        trend = "up"
    elif curr_fast < curr_slow:
        trend = "down"
    else:
        trend = "flat"
    return signal, trend


def compute_drawdown_pct(curve: list[tuple[int, Decimal]]) -> Decimal:
    if not curve:
        return Decimal("0")
    peak = curve[0][1]
    worst = Decimal("0")
    for _, eq in curve:
        if eq > peak:
            peak = eq
        if peak > 0:
            drawdown = ((eq / peak) - Decimal("1")) * Decimal("100")
            if drawdown < worst:
                worst = drawdown
    return worst


def sample_curve_points(curve: list[tuple[int, Decimal]], max_points: int = 80) -> list[dict[str, Any]]:
    if not curve:
        return []
    step = max(1, len(curve) // max_points)
    sampled = [curve[index] for index in range(0, len(curve), step)]
    if sampled[-1] != curve[-1]:
        sampled.append(curve[-1])
    return [{"ts": ts, "eq": decimal_to_str(eq)} for ts, eq in sampled]


def backtest_summary(
    starting_eq: Decimal,
    ending_eq: Decimal,
    trade_count: int,
    win_count: int,
    curve: list[tuple[int, Decimal]],
) -> dict[str, Any]:
    return_pct = Decimal("0")
    if starting_eq > 0:
        return_pct = ((ending_eq / starting_eq) - Decimal("1")) * Decimal("100")
    win_rate = (Decimal(win_count) / Decimal(trade_count) * Decimal("100")) if trade_count else Decimal("0")
    return {
        "startingEq": decimal_to_str(starting_eq),
        "endingEq": decimal_to_str(ending_eq),
        "returnPct": decimal_to_str(return_pct),
        "maxDrawdownPct": decimal_to_str(compute_drawdown_pct(curve)),
        "tradeCount": trade_count,
        "winCount": win_count,
        "winRatePct": decimal_to_str(win_rate),
    }


def simulate_spot_market(candles: list[dict[str, Any]], config: dict[str, Any]) -> dict[str, Any]:
    if not candles:
        raise OkxApiError("现货样本为空，无法回测")
    fast = int(config["fastEma"])
    slow = int(config["slowEma"])
    quote_budget = safe_decimal(config.get("spotQuoteBudget"), "0")
    max_exposure = safe_decimal(config.get("spotMaxExposure"), "0")
    stop_loss = safe_decimal(config.get("stopLossPct"), "0") / Decimal("100")
    take_profit = safe_decimal(config.get("takeProfitPct"), "0") / Decimal("100")
    cooldown_ms = max(0, int(config.get("cooldownSeconds", 0))) * 1000
    fee_rate = Decimal("0.001")

    starting_eq = max(max_exposure, quote_budget * Decimal("3"), Decimal("1000"))
    cash = starting_eq
    base_size = Decimal("0")
    entry_price = Decimal("0")
    next_trade_ts = 0
    trade_count = 0
    win_count = 0
    curve: list[tuple[int, Decimal]] = []

    closes = [row["close"] for row in candles]
    fast_values = ema(closes, fast)
    slow_values = ema(closes, slow)

    for index, candle in enumerate(candles):
        price = candle["close"]
        ts = int(candle["ts"])
        signal, _ = crossover_signal(fast_values, slow_values, index)
        can_trade = ts >= next_trade_ts

        if base_size > 0 and entry_price > 0 and can_trade:
            if stop_loss > 0 and price <= entry_price * (Decimal("1") - stop_loss):
                gross = base_size * price
                exit_fee = gross * fee_rate
                cash += gross - exit_fee
                trade_count += 1
                if price > entry_price:
                    win_count += 1
                base_size = Decimal("0")
                entry_price = Decimal("0")
                next_trade_ts = ts + cooldown_ms
                curve.append((ts, cash))
                continue
            if take_profit > 0 and price >= entry_price * (Decimal("1") + take_profit):
                gross = base_size * price
                exit_fee = gross * fee_rate
                cash += gross - exit_fee
                trade_count += 1
                if price > entry_price:
                    win_count += 1
                base_size = Decimal("0")
                entry_price = Decimal("0")
                next_trade_ts = ts + cooldown_ms
                curve.append((ts, cash))
                continue

        if signal == "bull_cross" and base_size <= 0 and can_trade:
            spend_target = quote_budget
            if max_exposure > 0:
                spend_target = min(spend_target, max_exposure)
            spend = min(spend_target, cash / (Decimal("1") + fee_rate))
            if spend > 0 and price > 0:
                entry_fee = spend * fee_rate
                base_size = spend / price
                cash -= spend + entry_fee
                entry_price = price
                next_trade_ts = ts + cooldown_ms
        elif signal == "bear_cross" and base_size > 0 and can_trade:
            gross = base_size * price
            exit_fee = gross * fee_rate
            cash += gross - exit_fee
            trade_count += 1
            if price > entry_price:
                win_count += 1
            base_size = Decimal("0")
            entry_price = Decimal("0")
            next_trade_ts = ts + cooldown_ms

        curve.append((ts, cash + base_size * price))

    ending_eq = cash + base_size * candles[-1]["close"]
    result = backtest_summary(starting_eq, ending_eq, trade_count, win_count, curve)
    result.update(
        {
            "instId": config["spotInstId"],
            "enabled": True,
            "lastPrice": decimal_to_str(candles[-1]["close"]),
            "positionSide": "long" if base_size > 0 else "flat",
            "equityCurve": sample_curve_points(curve),
            "_curveRaw": curve,
        }
    )
    return result


def simulate_swap_market(
    candles: list[dict[str, Any]],
    config: dict[str, Any],
    meta: dict[str, Any],
) -> dict[str, Any]:
    if not candles:
        raise OkxApiError("永续样本为空，无法回测")
    fast = int(config["fastEma"])
    slow = int(config["slowEma"])
    contracts = safe_decimal(config.get("swapContracts"), "0")
    leverage = max(safe_decimal(config.get("swapLeverage"), "1"), Decimal("1"))
    stop_loss = safe_decimal(config.get("stopLossPct"), "0") / Decimal("100")
    take_profit = safe_decimal(config.get("takeProfitPct"), "0") / Decimal("100")
    cooldown_ms = max(0, int(config.get("cooldownSeconds", 0))) * 1000
    ct_val = safe_decimal(meta.get("ctVal"), "1")
    fee_rate = Decimal("0.0005")
    mode = str(config.get("swapStrategyMode", "trend_follow"))

    seed_price = candles[0]["close"]
    contract_notional = contracts * ct_val * seed_price
    starting_eq = max((contract_notional / leverage) * Decimal("12"), Decimal("1000"))
    cash = starting_eq
    position_side = 0
    position_size = Decimal("0")
    entry_price = Decimal("0")
    next_trade_ts = 0
    trade_count = 0
    win_count = 0
    curve: list[tuple[int, Decimal]] = []

    closes = [row["close"] for row in candles]
    fast_values = ema(closes, fast)
    slow_values = ema(closes, slow)

    def close_position(price: Decimal, ts: int) -> None:
        nonlocal cash, position_side, position_size, entry_price, trade_count, win_count, next_trade_ts
        if position_side == 0 or position_size <= 0:
            return
        realized = Decimal(position_side) * (price - entry_price) * position_size * ct_val
        exit_fee = position_size * ct_val * price * fee_rate
        cash += realized - exit_fee
        trade_count += 1
        if realized > 0:
            win_count += 1
        position_side = 0
        position_size = Decimal("0")
        entry_price = Decimal("0")
        next_trade_ts = ts + cooldown_ms

    def open_position(side: int, price: Decimal, ts: int) -> None:
        nonlocal cash, position_side, position_size, entry_price, next_trade_ts
        if contracts <= 0:
            return
        entry_fee = contracts * ct_val * price * fee_rate
        cash -= entry_fee
        position_side = side
        position_size = contracts
        entry_price = price
        next_trade_ts = ts + cooldown_ms

    for index, candle in enumerate(candles):
        price = candle["close"]
        ts = int(candle["ts"])
        signal, _ = crossover_signal(fast_values, slow_values, index)
        can_trade = ts >= next_trade_ts

        if position_side != 0 and entry_price > 0 and can_trade:
            if position_side > 0:
                if stop_loss > 0 and price <= entry_price * (Decimal("1") - stop_loss):
                    close_position(price, ts)
                    curve.append((ts, cash))
                    continue
                if take_profit > 0 and price >= entry_price * (Decimal("1") + take_profit):
                    close_position(price, ts)
                    curve.append((ts, cash))
                    continue
            else:
                if stop_loss > 0 and price >= entry_price * (Decimal("1") + stop_loss):
                    close_position(price, ts)
                    curve.append((ts, cash))
                    continue
                if take_profit > 0 and price <= entry_price * (Decimal("1") - take_profit):
                    close_position(price, ts)
                    curve.append((ts, cash))
                    continue

        if signal == "bull_cross" and can_trade:
            if mode == "long_only":
                if position_side < 0:
                    close_position(price, ts)
                elif position_side == 0:
                    open_position(1, price, ts)
            elif mode == "short_only":
                if position_side < 0:
                    close_position(price, ts)
                elif position_side > 0:
                    close_position(price, ts)
            else:
                if position_side < 0:
                    close_position(price, ts)
                    open_position(1, price, ts)
                elif position_side == 0:
                    open_position(1, price, ts)
        elif signal == "bear_cross" and can_trade:
            if mode == "long_only":
                if position_side > 0:
                    close_position(price, ts)
            elif mode == "short_only":
                if position_side > 0:
                    close_position(price, ts)
                    open_position(-1, price, ts)
                elif position_side == 0:
                    open_position(-1, price, ts)
            else:
                if position_side > 0:
                    close_position(price, ts)
                    open_position(-1, price, ts)
                elif position_side == 0:
                    open_position(-1, price, ts)

        unrealized = Decimal("0")
        if position_side != 0 and position_size > 0:
            unrealized = Decimal(position_side) * (price - entry_price) * position_size * ct_val
        curve.append((ts, cash + unrealized))

    ending_eq = curve[-1][1] if curve else cash
    result = backtest_summary(starting_eq, ending_eq, trade_count, win_count, curve)
    result.update(
        {
            "instId": config["swapInstId"],
            "enabled": True,
            "lastPrice": decimal_to_str(candles[-1]["close"]),
            "positionSide": "long" if position_side > 0 else "short" if position_side < 0 else "flat",
            "equityCurve": sample_curve_points(curve),
            "_curveRaw": curve,
        }
    )
    return result


def merge_equity_curves(curves: list[list[tuple[int, Decimal]]]) -> list[tuple[int, Decimal]]:
    valid_curves = [curve for curve in curves if curve]
    if not valid_curves:
        return []
    max_len = max(len(curve) for curve in valid_curves)
    merged: list[tuple[int, Decimal]] = []
    for index in range(max_len):
        points = [curve[min(index, len(curve) - 1)] for curve in valid_curves]
        ts = max(point[0] for point in points)
        eq = sum(point[1] for point in points)
        merged.append((ts, eq))
    return merged


def score_research_summary(summary: dict[str, Any]) -> Decimal:
    return_pct = safe_decimal(summary.get("returnPct"), "0")
    drawdown = safe_decimal(summary.get("maxDrawdownPct"), "0")
    win_rate = safe_decimal(summary.get("winRatePct"), "0")
    return return_pct + drawdown * Decimal("0.35") + win_rate * Decimal("0.05")


def run_backtest_bundle(
    config: dict[str, Any],
    client: OkxClient,
    history_limit: int,
    candle_cache: dict[tuple[str, str], list[dict[str, Any]]],
    meta_cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    market_results: dict[str, Any] = {}
    notes = [
        "使用历史收盘价做本地 close 级回测，不含逐笔撮合、滑点和资金费。",
        "研究结果适合先筛参数，再回到模拟盘观察实盘风格的执行行为。",
    ]
    curves: list[list[tuple[int, Decimal]]] = []
    sample_count = 0

    if config.get("spotEnabled"):
        spot_key = (config["spotInstId"], config["bar"])
        spot_candles = candle_cache.get(spot_key)
        if spot_candles is None:
            spot_candles = get_closed_candles(client, config["spotInstId"], config["bar"], history_limit)
            candle_cache[spot_key] = spot_candles
        spot_result = simulate_spot_market(spot_candles, config)
        curves.append(spot_result.pop("_curveRaw", []))
        market_results["spot"] = spot_result
        sample_count = max(sample_count, len(spot_candles))

    if config.get("swapEnabled"):
        swap_key = (config["swapInstId"], config["bar"])
        swap_candles = candle_cache.get(swap_key)
        if swap_candles is None:
            swap_candles = get_closed_candles(client, config["swapInstId"], config["bar"], history_limit)
            candle_cache[swap_key] = swap_candles
        swap_meta_key = ("SWAP", config["swapInstId"])
        swap_meta = meta_cache.get(swap_meta_key)
        if swap_meta is None:
            swap_meta = get_instrument_meta(client, "SWAP", config["swapInstId"])
            meta_cache[swap_meta_key] = swap_meta
        swap_result = simulate_swap_market(swap_candles, config, swap_meta)
        curves.append(swap_result.pop("_curveRaw", []))
        market_results["swap"] = swap_result
        sample_count = max(sample_count, len(swap_candles))

    if not market_results:
        raise OkxApiError("至少启用现货或永续中的一个策略，才能运行研究")

    combined_curve = merge_equity_curves(curves)
    starting_eq = sum(safe_decimal(result["startingEq"], "0") for result in market_results.values())
    ending_eq = sum(safe_decimal(result["endingEq"], "0") for result in market_results.values())
    trade_count = sum(int(result.get("tradeCount", 0)) for result in market_results.values())
    win_count = sum(int(result.get("winCount", 0)) for result in market_results.values())
    summary = backtest_summary(starting_eq, ending_eq, trade_count, win_count, combined_curve)
    summary["score"] = decimal_to_str(score_research_summary(summary))

    return {
        "summary": summary,
        "markets": market_results,
        "notes": notes,
        "sampleCount": sample_count,
        "historyLimit": history_limit,
        "equityCurve": sample_curve_points(combined_curve),
        "_score": score_research_summary(summary),
    }


def clamp_int(value: Any, lower: int, upper: int) -> int:
    return max(lower, min(int(value), upper))


def clamp_decimal_value(
    value: Any,
    lower: Decimal,
    upper: Decimal,
    step: Decimal = Decimal("0.1"),
) -> Decimal:
    number = safe_decimal(value, str(lower))
    if number < lower:
        number = lower
    if number > upper:
        number = upper
    return number.quantize(step)


def candidate_signature(config: dict[str, Any]) -> str:
    keys = (
        "bar",
        "fastEma",
        "slowEma",
        "stopLossPct",
        "takeProfitPct",
        "spotEnabled",
        "swapEnabled",
        "swapStrategyMode",
        "swapLeverage",
    )
    return "|".join(str(config.get(key, "")) for key in keys)


def candidate_view_config(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "bar": config["bar"],
        "fastEma": int(config["fastEma"]),
        "slowEma": int(config["slowEma"]),
        "swapStrategyMode": str(config.get("swapStrategyMode", "trend_follow")),
        "swapLeverage": str(config.get("swapLeverage", "1")),
        "spotEnabled": bool(config.get("spotEnabled")),
        "swapEnabled": bool(config.get("swapEnabled")),
        "stopLossPct": str(config["stopLossPct"]),
        "takeProfitPct": str(config["takeProfitPct"]),
        "pollSeconds": int(config.get("pollSeconds", 20)),
        "cooldownSeconds": int(config.get("cooldownSeconds", 180)),
    }


def candidate_label(config: dict[str, Any], origin: str = "", generation: int = 0) -> str:
    prefix = []
    if generation:
        prefix.append(f"G{generation}")
    if origin:
        prefix.append(candidate_origin_label(origin))
    head = " · ".join(prefix)
    mode = strategy_mode_badge(config)
    leverage = f"{int(safe_decimal(config.get('swapLeverage'), '1'))}x" if config.get("swapEnabled") else "现货"
    body = f"{config['bar']} · EMA {config['fastEma']}/{config['slowEma']} · {mode} {leverage} · SL {config['stopLossPct']} / TP {config['takeProfitPct']}"
    return f"{head} · {body}" if head else body


def candidate_origin_label(origin: str) -> str:
    origin_map = {
        "seed": "种子",
        "hybrid": "杂交",
        "fine_tune": "微调",
        "evolve": "进化",
        "explore": "补位",
    }
    return origin_map.get(origin, origin)


def create_candidate_envelope(
    config: dict[str, Any],
    generation: int,
    origin: str,
) -> dict[str, Any]:
    return {
        "generation": generation,
        "origin": origin,
        "config": deep_merge({}, config),
    }


def unique_candidate_pool(candidates: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        signature = candidate_signature(candidate["config"])
        if signature in seen:
            continue
        seen.add(signature)
        unique.append(candidate)
    return unique


def evaluate_candidate_entry(
    candidate: dict[str, Any],
    client: OkxClient,
    history_limit: int,
    candle_cache: dict[tuple[str, str], list[dict[str, Any]]],
    meta_cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Any]:
    report = run_backtest_bundle(candidate["config"], client, history_limit, candle_cache, meta_cache)
    origin_label = candidate_origin_label(candidate["origin"])
    return {
        "candidate": candidate,
        "report": report,
        "entry": {
            "name": strategy_short_name(candidate["config"]),
            "detail": strategy_detail_line(candidate["config"], origin_label),
            "label": candidate_label(candidate["config"], candidate["origin"], candidate["generation"]),
            "origin": candidate["origin"],
            "originLabel": origin_label,
            "generation": candidate["generation"],
            "scopeLabel": strategy_scope_label(candidate["config"]),
            "presetLabel": strategy_label(str(candidate["config"].get("strategyPreset", "dual_engine"))),
            "score": decimal_to_str(report["_score"]),
            "returnPct": report["summary"]["returnPct"],
            "maxDrawdownPct": report["summary"]["maxDrawdownPct"],
            "winRatePct": report["summary"]["winRatePct"],
            "tradeCount": report["summary"]["tradeCount"],
            "config": candidate_view_config(candidate["config"]),
            "fullConfig": deep_merge({}, candidate["config"]),
        },
        "signature": candidate_signature(candidate["config"]),
        "_score": report["_score"],
    }


def pick_seed_candidates(
    config: dict[str, Any],
    options: dict[str, Any],
    previous_research: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    population_size = int(options.get("populationSize") or options["raceSize"])
    candidate_bank = optimization_candidates(config, options["optimizationDepth"], options["includeAltBars"])
    pool = [create_candidate_envelope(config, 1, "seed")]
    previous_population = previous_research.get("population") or [] if previous_research else []
    for item in previous_population[: max(4, min(population_size // 3, 16))]:
        carried = item.get("fullConfig") or item.get("config") or {}
        if carried:
            pool.append(create_candidate_envelope(copied := deep_merge({}, carried), 1, "carry_over"))
    if candidate_bank:
        step = max(1, len(candidate_bank) // max(population_size - 1, 1))
        index = 0
        while len(pool) < population_size and index < len(candidate_bank):
            pool.append(create_candidate_envelope(candidate_bank[index], 1, "seed"))
            index += step
        for candidate in candidate_bank:
            if len(pool) >= population_size:
                break
            pool.append(create_candidate_envelope(candidate, 1, "seed"))
    return unique_candidate_pool(pool)[:population_size]


def mutate_strategy_config(
    config: dict[str, Any],
    bars: list[str],
    seed: int,
    *,
    fine_tune: bool,
) -> dict[str, Any]:
    fast_deltas = (-1, 1, -2, 2, -3, 3) if fine_tune else (-2, 2, -4, 4, -6, 6)
    slow_deltas = (-3, 3, -5, 5, -8, 8, -13, 13) if fine_tune else (-5, 5, -8, 8, -13, 13, -21, 21)
    stop_deltas = (Decimal("-0.1"), Decimal("0.1"), Decimal("-0.2"), Decimal("0.2")) if fine_tune else (Decimal("-0.2"), Decimal("0.2"), Decimal("-0.4"), Decimal("0.4"))
    take_deltas = (Decimal("-0.2"), Decimal("0.2"), Decimal("-0.4"), Decimal("0.4")) if fine_tune else (Decimal("-0.4"), Decimal("0.4"), Decimal("-0.8"), Decimal("0.8"))
    current_bar = str(config.get("bar", bars[0] if bars else "5m"))
    if current_bar not in bars:
        bars = [current_bar, *bars]
    bar_index = bars.index(current_bar) if current_bar in bars else 0
    next_bar = bars[(bar_index + seed) % len(bars)]
    fast = clamp_int(int(config["fastEma"]) + fast_deltas[seed % len(fast_deltas)], 2, 48)
    slow = clamp_int(int(config["slowEma"]) + slow_deltas[(seed // 2) % len(slow_deltas)], fast + 2, 120)
    stop_loss = clamp_decimal_value(
        safe_decimal(config["stopLossPct"], "1.2") + stop_deltas[(seed // 3) % len(stop_deltas)],
        Decimal("0.3"),
        Decimal("6.0"),
    )
    take_profit = clamp_decimal_value(
        safe_decimal(config["takeProfitPct"], "2.4") + take_deltas[(seed // 4) % len(take_deltas)],
        Decimal("0.6"),
        Decimal("12.0"),
    )
    leverage_deltas = (-1, 1, 0, 2) if fine_tune else (-2, -1, 1, 2, 3)
    leverage = clamp_int(
        int(safe_decimal(config.get("swapLeverage"), "3")) + leverage_deltas[(seed // 2) % len(leverage_deltas)],
        1,
        10,
    )
    mode_cycle = ["trend_follow", "long_only", "short_only"]
    current_mode = str(config.get("swapStrategyMode", "trend_follow"))
    try:
        mode_index = mode_cycle.index(current_mode)
    except ValueError:
        mode_index = 0
    next_mode = mode_cycle[(mode_index + seed + (0 if fine_tune else 1)) % len(mode_cycle)]
    return deep_merge(
        config,
        {
            "bar": next_bar if len(bars) > 1 else current_bar,
            "fastEma": fast,
            "slowEma": slow,
            "swapStrategyMode": next_mode if config.get("swapEnabled") else current_mode,
            "swapLeverage": str(leverage),
            "stopLossPct": decimal_to_str(stop_loss),
            "takeProfitPct": decimal_to_str(take_profit),
        },
    )


def hybridize_strategy_config(
    primary: dict[str, Any],
    secondary: dict[str, Any],
    bars: list[str],
    seed: int,
) -> dict[str, Any]:
    fast = clamp_int(round((int(primary["fastEma"]) + int(secondary["fastEma"])) / 2), 2, 48)
    slow = clamp_int(
        round((int(primary["slowEma"]) + int(secondary["slowEma"])) / 2) + (1 if seed % 2 else 0),
        fast + 2,
        120,
    )
    stop_loss = clamp_decimal_value(
        (safe_decimal(primary["stopLossPct"], "1.2") + safe_decimal(secondary["stopLossPct"], "1.2")) / Decimal("2"),
        Decimal("0.3"),
        Decimal("6.0"),
    )
    take_profit = clamp_decimal_value(
        (safe_decimal(primary["takeProfitPct"], "2.4") + safe_decimal(secondary["takeProfitPct"], "2.4")) / Decimal("2"),
        Decimal("0.6"),
        Decimal("12.0"),
    )
    leverage = clamp_int(
        round(
            (
                int(safe_decimal(primary.get("swapLeverage"), "3"))
                + int(safe_decimal(secondary.get("swapLeverage"), "3"))
            )
            / 2
        ),
        1,
        10,
    )
    primary_mode = str(primary.get("swapStrategyMode", "trend_follow"))
    secondary_mode = str(secondary.get("swapStrategyMode", "trend_follow"))
    mode_options = [primary_mode, secondary_mode, "trend_follow", "short_only", "long_only"]
    chosen_mode = mode_options[seed % len(mode_options)]
    bar_options = [str(primary.get("bar", "5m")), str(secondary.get("bar", "5m"))] + bars
    chosen_bar = bar_options[seed % len(bar_options)]
    return deep_merge(
        primary,
        {
            "bar": chosen_bar,
            "fastEma": fast,
            "slowEma": slow,
            "swapStrategyMode": chosen_mode,
            "swapLeverage": str(leverage),
            "stopLossPct": decimal_to_str(stop_loss),
            "takeProfitPct": decimal_to_str(take_profit),
        },
    )


def build_next_generation_pool(
    sorted_results: list[dict[str, Any]],
    candidate_bank: list[dict[str, Any]],
    seen_signatures: set[str],
    generation: int,
    options: dict[str, Any],
) -> list[dict[str, Any]]:
    population_size = int(options.get("populationSize") or options["raceSize"])
    survivor_count = max(4, min(12, max(2, population_size // 4), len(sorted_results)))
    survivors = sorted_results[:survivor_count]
    bars = []
    for candidate in candidate_bank[:12]:
        bar = str(candidate.get("bar", "5m"))
        if bar not in bars:
            bars.append(bar)
    if not bars:
        bars = ["5m"]

    next_pool: list[dict[str, Any]] = []

    def add_candidate(config: dict[str, Any], origin: str) -> None:
        signature = candidate_signature(config)
        if signature in seen_signatures:
            return
        seen_signatures.add(signature)
        next_pool.append(create_candidate_envelope(config, generation, origin))

    if options.get("enableHybrid", True) and len(survivors) >= 2:
        for left in range(len(survivors)):
            for right in range(left + 1, len(survivors)):
                child = hybridize_strategy_config(
                    survivors[left]["candidate"]["config"],
                    survivors[right]["candidate"]["config"],
                    bars,
                    generation + left + right,
                )
                add_candidate(child, "hybrid")
                if len(next_pool) >= population_size:
                    return next_pool[:population_size]

    if options.get("enableFineTune", True):
        for seed, result in enumerate(survivors):
            for offset in range(2):
                candidate = mutate_strategy_config(
                    result["candidate"]["config"],
                    bars,
                    generation + seed + offset + 1,
                    fine_tune=True,
                )
                add_candidate(candidate, "fine_tune")
                if len(next_pool) >= population_size:
                    return next_pool[:population_size]

    for seed, result in enumerate(survivors):
        candidate = mutate_strategy_config(
            result["candidate"]["config"],
            bars,
            generation + seed + 3,
            fine_tune=False,
        )
        add_candidate(candidate, "evolve")
        if len(next_pool) >= population_size:
            return next_pool[:population_size]

    for index, candidate in enumerate(candidate_bank):
        add_candidate(candidate, "explore")
        if len(next_pool) >= population_size:
            return next_pool[:population_size]

    champion = survivors[0]["candidate"]["config"]
    seed = generation * 7
    while len(next_pool) < population_size:
        candidate = mutate_strategy_config(champion, bars, seed, fine_tune=False)
        seed += 1
        add_candidate(candidate, "evolve")

    return next_pool[:population_size]


def optimization_candidates(config: dict[str, Any], depth: str, include_alt_bars: bool) -> list[dict[str, Any]]:
    base_bar = str(config.get("bar", "5m") or "5m")
    bars = [base_bar]
    if include_alt_bars:
        for candidate in ("5m", "15m", "1H"):
            if candidate not in bars:
                bars.append(candidate)

    fast_seed = int(config.get("fastEma", 9) or 9)
    slow_seed = int(config.get("slowEma", 21) or 21)
    stop_seed = safe_decimal(config.get("stopLossPct"), "1.2")
    take_seed = safe_decimal(config.get("takeProfitPct"), "2.4")

    if depth == "standard":
        fast_values = sorted({max(2, fast_seed - 4), max(2, fast_seed - 2), fast_seed, fast_seed + 2, fast_seed + 4, fast_seed + 6})
        slow_values = sorted({max(5, slow_seed - 13), max(6, slow_seed - 8), slow_seed, slow_seed + 5, slow_seed + 13, slow_seed + 21})
        stop_offsets = (Decimal("-0.6"), Decimal("-0.3"), Decimal("0"), Decimal("0.4"))
        take_offsets = (Decimal("-1.0"), Decimal("-0.4"), Decimal("0"), Decimal("0.8"))
    else:
        fast_values = sorted({max(2, fast_seed - 2), fast_seed, fast_seed + 2, fast_seed + 4})
        slow_values = sorted({max(5, slow_seed - 8), slow_seed, slow_seed + 5, slow_seed + 13})
        stop_offsets = (Decimal("-0.3"), Decimal("0"), Decimal("0.4"))
        take_offsets = (Decimal("-0.6"), Decimal("0"), Decimal("0.8"))

    stop_values = sorted(
        {
            max(Decimal("0.3"), (stop_seed + offset).quantize(Decimal("0.1")))
            for offset in stop_offsets
        }
    )
    take_values = sorted(
        {
            max(Decimal("0.6"), (take_seed + offset).quantize(Decimal("0.1")))
            for offset in take_offsets
        }
    )

    leverage_seed = int(safe_decimal(config.get("swapLeverage"), "3"))
    if depth == "standard":
        leverage_values = sorted({max(1, leverage_seed - 2), max(1, leverage_seed - 1), leverage_seed, min(10, leverage_seed + 1), min(10, leverage_seed + 3)})
    else:
        leverage_values = sorted({max(1, leverage_seed - 1), leverage_seed, min(10, leverage_seed + 2)})
    mode_values = [str(config.get("swapStrategyMode", "trend_follow"))]
    for candidate_mode in ("trend_follow", "long_only", "short_only"):
        if candidate_mode not in mode_values:
            mode_values.append(candidate_mode)

    candidates: list[dict[str, Any]] = []
    for bar in bars:
        for fast in fast_values:
            for slow in slow_values:
                if slow <= fast + 1:
                    continue
                for stop_loss in stop_values:
                    for take_profit in take_values:
                        for leverage in leverage_values if config.get("swapEnabled") else [int(safe_decimal(config.get("swapLeverage"), "1"))]:
                            for mode in mode_values if config.get("swapEnabled") else [str(config.get("swapStrategyMode", "trend_follow"))]:
                                patch = {
                                    "bar": bar,
                                    "fastEma": fast,
                                    "slowEma": slow,
                                    "swapLeverage": str(leverage),
                                    "swapStrategyMode": mode,
                                    "stopLossPct": decimal_to_str(stop_loss),
                                    "takeProfitPct": decimal_to_str(take_profit),
                                }
                                candidates.append(deep_merge(config, patch))
    return candidates


def build_research_state(
    mode: str,
    config: dict[str, Any],
    options: dict[str, Any],
    report: dict[str, Any],
    leaderboard: list[dict[str, Any]] | None = None,
    population: list[dict[str, Any]] | None = None,
    generation_summaries: list[dict[str, Any]] | None = None,
    pipeline: dict[str, Any] | None = None,
) -> dict[str, Any]:
    leaderboard = leaderboard or []
    population = population or []
    generation_summaries = generation_summaries or []
    pipeline = pipeline or {}
    best_config = deep_merge({}, leaderboard[0].get("fullConfig") or config) if leaderboard else deep_merge({}, config)
    return {
        "running": False,
        "statusText": "自动优化已完成" if mode == "optimize" else "回测完成",
        "mode": mode,
        "lastRunAt": now_local_iso(),
        "historyLimit": options["historyLimit"],
        "sampleCount": report.get("sampleCount", 0),
        "summary": report.get("summary", {}),
        "baseConfig": deep_merge({}, config),
        "bestConfig": best_config,
        "leaderboard": leaderboard,
        "population": population,
        "generationSummaries": generation_summaries,
        "pipeline": pipeline,
        "markets": report.get("markets", {}),
        "notes": report.get("notes", []),
        "equityCurve": report.get("equityCurve", []),
    }


def research_backtest(config: dict[str, Any], options: dict[str, Any], client: OkxClient) -> dict[str, Any]:
    candle_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    meta_cache: dict[tuple[str, str], dict[str, Any]] = {}
    adjusted_options, capital_context, capital_notes = resolve_research_capital_limits(
        config,
        options,
        client,
        candle_cache,
        meta_cache,
    )
    report = run_backtest_bundle(config, client, adjusted_options["historyLimit"], candle_cache, meta_cache)
    report["notes"] = report.get("notes", []) + capital_notes
    return build_research_state(
        "backtest",
        config,
        adjusted_options,
        report,
        pipeline={"capital": capital_context},
    )


def research_optimize(config: dict[str, Any], options: dict[str, Any], client: OkxClient) -> dict[str, Any]:
    candle_cache: dict[tuple[str, str], list[dict[str, Any]]] = {}
    meta_cache: dict[tuple[str, str], dict[str, Any]] = {}
    adjusted_options, capital_context, capital_notes = resolve_research_capital_limits(
        config,
        options,
        client,
        candle_cache,
        meta_cache,
    )
    candidate_bank = optimization_candidates(config, adjusted_options["optimizationDepth"], adjusted_options["includeAltBars"])
    previous_research = AUTOMATION_STATE.current().get("research") or {}
    current_pool = pick_seed_candidates(config, adjusted_options, previous_research)
    all_ranked: dict[str, dict[str, Any]] = {}
    generation_summaries: list[dict[str, Any]] = []
    best_report: dict[str, Any] | None = None
    seen_signatures = {candidate_signature(item["config"]) for item in current_pool}

    total_evaluated = 0
    for generation in range(1, int(adjusted_options["evolutionLoops"]) + 1):
        if not current_pool:
            break
        generation_results = [
            evaluate_candidate_entry(candidate, client, adjusted_options["historyLimit"], candle_cache, meta_cache)
            for candidate in current_pool[: int(adjusted_options.get("populationSize") or adjusted_options["raceSize"])]
        ]
        total_evaluated += len(generation_results)
        generation_results.sort(key=lambda item: item["_score"], reverse=True)
        for result in generation_results:
            existing = all_ranked.get(result["signature"])
            if existing is None or result["_score"] > existing["_score"]:
                all_ranked[result["signature"]] = result
            if best_report is None or result["_score"] > best_report["_score"]:
                best_report = result["report"]

        winner = generation_results[0]
        avg_score = sum((item["_score"] for item in generation_results), Decimal("0")) / Decimal(len(generation_results))
        generation_summaries.append(
            {
                "generation": generation,
                "winnerLabel": winner["entry"]["label"],
                "winnerScore": winner["entry"]["score"],
                "winnerReturnPct": winner["entry"]["returnPct"],
                "winnerDrawdownPct": winner["entry"]["maxDrawdownPct"],
                "avgScore": decimal_to_str(avg_score),
                "candidateCount": len(generation_results),
            }
        )
        if generation >= int(adjusted_options["evolutionLoops"]):
            break
        current_pool = build_next_generation_pool(
            generation_results,
            candidate_bank,
            seen_signatures,
            generation + 1,
            adjusted_options,
        )

    ranked_entries = sorted(
        (result["entry"] for result in all_ranked.values()),
        key=lambda item: safe_decimal(item["score"], "0"),
        reverse=True,
    )
    leaderboard = ranked_entries[: int(adjusted_options["raceSize"])]
    population = ranked_entries[: int(adjusted_options.get("populationSize") or adjusted_options["raceSize"])]
    if best_report is None:
        raise OkxApiError("没有可用的优化结果")
    best_report["notes"] = best_report.get("notes", []) + capital_notes + [
        f"已完成 {adjusted_options['evolutionLoops']} 轮持续赛马进化，榜单展示前 {adjusted_options['raceSize']} 条，后台种群保留 {adjusted_options.get('populationSize') or adjusted_options['raceSize']} 条。",
        f"总共评估 {total_evaluated} 条候选；杂交 {'开启' if adjusted_options['enableHybrid'] else '关闭'}，微调 {'开启' if adjusted_options['enableFineTune'] else '关闭'}。",
        "现在会保留上一轮高分种群做 carry-over，不再每次只拿 10 条从头开始。",
    ]
    return build_research_state(
        "optimize",
        config,
        adjusted_options,
        best_report,
        leaderboard,
        population,
        generation_summaries,
        {
            "raceSize": adjusted_options["raceSize"],
            "populationSize": adjusted_options.get("populationSize") or adjusted_options["raceSize"],
            "evolutionLoops": adjusted_options["evolutionLoops"],
            "evaluatedCount": total_evaluated,
            "enableHybrid": adjusted_options["enableHybrid"],
            "enableFineTune": adjusted_options["enableFineTune"],
            "capital": capital_context,
        },
    )


def recommended_import_config(config: dict[str, Any]) -> dict[str, Any]:
    exported = deep_merge({}, config)
    exported["autostart"] = False
    exported["allowLiveAutostart"] = False
    return exported


def strategy_export_record(rank: int, entry: dict[str, Any], base_config: dict[str, Any]) -> dict[str, Any]:
    full_config = deep_merge(base_config, entry.get("fullConfig") or entry.get("config") or {})
    exported_config = recommended_import_config(full_config)
    name = strategy_short_name(full_config, rank=rank)
    file_stub = f"s{rank:02d}-{export_slug(strategy_short_name(full_config))}"
    return {
        "rank": rank,
        "id": f"S{rank:02d}",
        "name": name,
        "detail": strategy_detail_line(full_config, entry.get("originLabel", "")),
        "presetLabel": strategy_label(str(full_config.get("strategyPreset", "dual_engine"))),
        "scopeLabel": strategy_scope_label(full_config),
        "originLabel": entry.get("originLabel", ""),
        "generation": int(entry.get("generation") or 0),
        "performance": {
            "returnPct": compact_metric(entry.get("returnPct", "0")),
            "maxDrawdownPct": compact_metric(entry.get("maxDrawdownPct", "0")),
            "winRatePct": compact_metric(entry.get("winRatePct", "0")),
            "tradeCount": int(entry.get("tradeCount") or 0),
            "score": compact_metric(entry.get("score", "0")),
        },
        "instIds": {
            "spot": full_config.get("spotInstId", ""),
            "swap": full_config.get("swapInstId", ""),
        },
        "configPatch": deep_merge({}, entry.get("config") or {}),
        "automationConfig": exported_config,
        "fileStub": file_stub,
    }


def export_research_pack(
    research: dict[str, Any],
    current_config: dict[str, Any],
    *,
    index: int | None = None,
) -> dict[str, Any]:
    leaderboard = research.get("leaderboard") or []
    if not leaderboard:
        raise OkxApiError("还没有可导出的策略，请先运行自动优化")

    base_config = deep_merge({}, research.get("baseConfig") or current_config or default_automation_config())
    exported_rows: list[dict[str, Any]] = []
    if index is None:
        for rank, entry in enumerate(leaderboard, start=1):
            exported_rows.append(strategy_export_record(rank, entry, base_config))
    else:
        if index < 0 or index >= len(leaderboard):
            raise OkxApiError("策略索引超出范围")
        exported_rows.append(strategy_export_record(index + 1, leaderboard[index], base_config))

    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    mode = "single" if index is not None else "pack"
    pack_dir = Path.home() / "Downloads" / f"okx-strategy-pack-{timestamp}"
    pack_dir.mkdir(parents=True, exist_ok=True)

    csv_path = pack_dir / "leaderboard.csv"
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "rank",
                "name",
                "detail",
                "swapStrategyMode",
                "swapLeverage",
                "returnPct",
                "maxDrawdownPct",
                "winRatePct",
                "tradeCount",
                "score",
                "spotInstId",
                "swapInstId",
                "bar",
                "fastEma",
                "slowEma",
                "stopLossPct",
                "takeProfitPct",
                "pollSeconds",
                "cooldownSeconds",
            ]
        )
        for row in exported_rows:
            config = row["automationConfig"]
            perf = row["performance"]
            writer.writerow(
                [
                    row["rank"],
                    row["name"],
                    row["detail"],
                    config.get("swapStrategyMode", ""),
                    config.get("swapLeverage", ""),
                    perf["returnPct"],
                    perf["maxDrawdownPct"],
                    perf["winRatePct"],
                    perf["tradeCount"],
                    perf["score"],
                    row["instIds"]["spot"],
                    row["instIds"]["swap"],
                    config.get("bar", ""),
                    config.get("fastEma", ""),
                    config.get("slowEma", ""),
                    config.get("stopLossPct", ""),
                    config.get("takeProfitPct", ""),
                    config.get("pollSeconds", ""),
                    config.get("cooldownSeconds", ""),
                ]
            )

    strategy_files: list[dict[str, Any]] = []
    for row in exported_rows:
        item_path = pack_dir / f"{row['fileStub']}.json"
        item_path.write_text(
            json.dumps(
                {
                    "exportedAt": now_local_iso(),
                    "exchange": "OKX",
                    "readyFor": "OKX Local App",
                    **row,
                },
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )
        strategy_files.append(
            {
                "rank": row["rank"],
                "name": row["name"],
                "returnPct": row["performance"]["returnPct"],
                "path": str(item_path),
            }
        )

    manifest_path = pack_dir / "strategies.json"
    manifest = {
        "exportedAt": now_local_iso(),
        "mode": mode,
        "exchange": "OKX",
        "readyFor": "OKX Local App",
        "strategyCount": len(exported_rows),
        "summary": research.get("summary") or {},
        "strategies": exported_rows,
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")

    readme_path = pack_dir / "README.txt"
    readme_path.write_text(
        "\n".join(
            [
                "OKX Strategy Pack",
                "",
                "1. strategies.json 是完整策略包清单。",
                "2. leaderboard.csv 适合直接查看名字、收益、回撤、胜率。",
                "3. 每个 sXX-*.json 都包含一条可直接回填到当前 OKX Local App 的 automationConfig。",
                "4. 导出时已自动关闭 autostart / allowLiveAutostart，避免导入后直接自启。",
            ]
        ),
        encoding="utf-8",
    )

    return {
        "mode": mode,
        "folder": str(pack_dir),
        "manifestPath": str(manifest_path),
        "csvPath": str(csv_path),
        "readmePath": str(readme_path),
        "strategyCount": len(exported_rows),
        "strategies": strategy_files,
    }


def parse_balance_snapshot(raw: dict[str, Any]) -> dict[str, Any]:
    entries = raw.get("data") or []
    top = entries[0] if entries else {}
    details = top.get("details") or []
    details.sort(
        key=lambda row: safe_decimal(
            row.get("eqUsd") or row.get("disEq") or row.get("cashBal") or "0"
        ),
        reverse=True,
    )
    return {
        "summary": {
            "totalEq": top.get("totalEq", "0"),
            "uTime": top.get("uTime", ""),
            "isoEq": top.get("isoEq", "0"),
            "adjEq": top.get("adjEq", "0"),
            "imr": top.get("imr", "0"),
            "mmr": top.get("mmr", "0"),
        },
        "details": details,
    }


def parse_funding_balance_snapshot(
    raw: dict[str, Any],
    valuation_raw: dict[str, Any] | None = None,
    ccy: str = "USDT",
) -> dict[str, Any]:
    details = list(raw.get("data") or [])
    details.sort(
        key=lambda row: safe_decimal(
            row.get("usdEq") or row.get("eqUsd") or row.get("bal") or row.get("availBal") or "0"
        ),
        reverse=True,
    )
    valuation_entries = (valuation_raw or {}).get("data") or []
    valuation_top = valuation_entries[0] if valuation_entries else {}
    total_bal = safe_decimal(valuation_top.get("totalBal"), "0")
    if total_bal <= 0:
        total_bal = sum(
            safe_decimal(row.get("usdEq") or row.get("eqUsd") or row.get("bal") or row.get("availBal") or "0")
            for row in details
        )
    return {
        "summary": {
            "totalBal": decimal_to_str(total_bal),
            "ccy": valuation_top.get("ccy") or ccy,
            "ts": valuation_top.get("ts", ""),
        },
        "details": details,
    }


def build_account_snapshot(
    trading_snapshot: dict[str, Any],
    funding_snapshot: dict[str, Any] | None = None,
    positions: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    funding_snapshot = funding_snapshot or {"summary": {}, "details": []}
    positions = positions or []
    trading_summary = copy.deepcopy(trading_snapshot.get("summary") or {})
    trading_total = safe_decimal(trading_summary.get("totalEq"), "0")
    funding_summary = copy.deepcopy(funding_snapshot.get("summary") or {})
    funding_total = safe_decimal(funding_summary.get("totalBal"), "0")
    display_total = trading_total + funding_total
    if display_total <= 0:
        display_total = trading_total

    if funding_total > 0 and trading_total > 0:
        display_source = "资金账户 + 交易账户"
        display_breakdown = (
            f"资金账户 {format_decimal(funding_total, 2)} USDT · "
            f"交易账户 {format_decimal(trading_total, 2)} USDT"
        )
    elif funding_total > 0:
        display_source = "资金账户"
        display_breakdown = f"资金账户 {format_decimal(funding_total, 2)} USDT"
    elif trading_total > 0:
        display_source = "交易账户"
        display_breakdown = f"交易账户 {format_decimal(trading_total, 2)} USDT"
    else:
        display_source = "交易账户"
        display_breakdown = ""

    summary = {
        **trading_summary,
        "tradingTotalEq": decimal_to_str(trading_total),
        "fundingTotalEq": decimal_to_str(funding_total),
        "displayTotalEq": decimal_to_str(display_total),
        "displaySource": display_source,
        "displayBreakdown": display_breakdown,
    }
    summary["totalEq"] = summary["displayTotalEq"]

    trading_balances = list(trading_snapshot.get("details") or [])
    funding_balances = list(funding_snapshot.get("details") or [])
    return {
        "summary": summary,
        "balances": trading_balances,
        "tradingBalances": trading_balances,
        "fundingBalances": funding_balances,
        "fundingSummary": funding_summary,
        "positions": positions,
        "balanceCount": len(trading_balances) + len(funding_balances),
        "positionCount": len(positions),
    }


def fetch_account_snapshot(client: OkxClient, include_positions: bool = True) -> dict[str, Any]:
    trading_snapshot = parse_balance_snapshot(client.get_account_balance())
    funding_warning = ""
    funding_snapshot = {"summary": {"totalBal": "0", "ccy": "USDT", "ts": ""}, "details": []}
    try:
        funding_raw = client.get_funding_balances()
        valuation_raw = client.get_asset_valuation("USDT")
        funding_snapshot = parse_funding_balance_snapshot(funding_raw, valuation_raw, "USDT")
    except Exception as exc:
        funding_warning = str(exc)

    positions = client.get_positions().get("data", []) if include_positions else []
    payload = build_account_snapshot(trading_snapshot, funding_snapshot, positions)
    if funding_warning:
        payload["fundingWarning"] = funding_warning
    return payload


def find_balance_detail(snapshot: dict[str, Any], ccy: str) -> dict[str, Any]:
    for row in snapshot.get("details", []):
        if row.get("ccy") == ccy:
            return row
    return {}


def latest_public_price(
    client: OkxClient,
    inst_id: str,
    bar: str,
    candle_cache: dict[tuple[str, str], list[dict[str, Any]]],
    sample_limit: int = 8,
) -> Decimal:
    cache_key = (inst_id, bar)
    candles = candle_cache.get(cache_key)
    if candles is None:
        try:
            candles = get_closed_candles(client, inst_id, bar, sample_limit)
            candle_cache[cache_key] = candles
        except Exception:
            candles = []
    if candles:
        return safe_decimal(candles[-1].get("close"), "0")
    try:
        row = extract_first_row(client.get_ticker(inst_id))
        return safe_decimal(row.get("last"), "0")
    except Exception:
        return Decimal("0")


def estimate_strategy_capital_requirement(
    config: dict[str, Any],
    client: OkxClient,
    candle_cache: dict[tuple[str, str], list[dict[str, Any]]],
    meta_cache: dict[tuple[str, str], dict[str, Any]],
) -> dict[str, Decimal]:
    spot_need = Decimal("0")
    if config.get("spotEnabled"):
        quote_budget = safe_decimal(config.get("spotQuoteBudget"), "0")
        max_exposure = safe_decimal(config.get("spotMaxExposure"), "0")
        spot_need = max(quote_budget, max_exposure)

    swap_need = Decimal("0")
    if config.get("swapEnabled"):
        inst_id = str(config.get("swapInstId", "") or "").strip()
        if inst_id:
            swap_meta_key = ("SWAP", inst_id)
            swap_meta = meta_cache.get(swap_meta_key)
            if swap_meta is None:
                try:
                    swap_meta = get_instrument_meta(client, "SWAP", inst_id)
                    meta_cache[swap_meta_key] = swap_meta
                except Exception:
                    swap_meta = {}
            mark_price = latest_public_price(
                client,
                inst_id,
                str(config.get("bar", "5m") or "5m"),
                candle_cache,
            )
            contracts = safe_decimal(config.get("swapContracts"), "0")
            leverage = max(safe_decimal(config.get("swapLeverage"), "1"), Decimal("1"))
            ct_val = safe_decimal((swap_meta or {}).get("ctVal"), "1")
            notional = contracts * ct_val * mark_price
            swap_need = (notional / leverage) if leverage > 0 else notional
            if swap_need > 0:
                swap_need *= Decimal("1.08")

    per_horse = spot_need + swap_need
    if per_horse <= 0:
        per_horse = Decimal("100")
    return {
        "spotNeed": spot_need,
        "swapNeed": swap_need,
        "perHorseCapital": per_horse,
    }


def resolve_research_capital_limits(
    config: dict[str, Any],
    options: dict[str, Any],
    public_client: OkxClient,
    candle_cache: dict[tuple[str, str], list[dict[str, Any]]],
    meta_cache: dict[tuple[str, str], dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    requested_race = int(options["raceSize"])
    requested_population = int(options.get("populationSize") or requested_race)
    requirement = estimate_strategy_capital_requirement(config, public_client, candle_cache, meta_cache)
    per_horse = requirement["perHorseCapital"]

    available_capital = Decimal("0")
    capital_source = "config_estimate"
    capital_note = ""
    api_config = CONFIG.current()
    valid_api, _ = validate_config(api_config)
    if valid_api:
        try:
            private_client = OkxClient(api_config)
            balance_snapshot = parse_balance_snapshot(private_client.get_account_balance())
            available_capital = safe_decimal(balance_snapshot["summary"].get("totalEq"), "0")
            capital_source = "okx_live_balance"
        except Exception as exc:
            capital_note = f"未取到 OKX 实时余额，已退回配置预算估算：{exc}"

    if available_capital <= 0:
        spot_estimate = max(
            safe_decimal(config.get("spotMaxExposure"), "0"),
            safe_decimal(config.get("spotQuoteBudget"), "0"),
        )
        available_capital = spot_estimate + requirement["swapNeed"]
        if available_capital <= 0:
            available_capital = per_horse

    horse_slots = 1
    if per_horse > 0:
        horse_slots = max(1, int(available_capital / per_horse))

    adjusted_race = min(requested_race, horse_slots)
    adjusted_population = min(requested_population, max(adjusted_race, horse_slots * 4))
    adjusted_options = deep_merge(
        options,
        {
            "raceSize": max(1, adjusted_race),
            "populationSize": max(1, adjusted_population),
        },
    )
    capital_context = {
        "source": capital_source,
        "availableCapital": decimal_to_str(available_capital),
        "perHorseCapital": decimal_to_str(per_horse),
        "horseSlots": horse_slots,
        "requestedRaceSize": requested_race,
        "requestedPopulationSize": requested_population,
        "appliedRaceSize": adjusted_options["raceSize"],
        "appliedPopulationSize": adjusted_options["populationSize"],
    }

    notes: list[str] = [
        f"资金槽位约束：可用资金约 {decimal_to_str(available_capital)} USDT，单马预计占用约 {decimal_to_str(per_horse)} USDT，当前最多支撑 {horse_slots} 匹。"
    ]
    if adjusted_options["raceSize"] != requested_race or adjusted_options["populationSize"] != requested_population:
        notes.append(
            f"已按资金约束把榜单从 {requested_race} 收到 {adjusted_options['raceSize']}，后台种群从 {requested_population} 收到 {adjusted_options['populationSize']}。"
        )
    if capital_note:
        notes.append(capital_note)
    return adjusted_options, capital_context, notes


def get_instrument_meta(client: OkxClient, inst_type: str, inst_id: str) -> dict[str, Any]:
    result = client.get_public_instruments(inst_type, inst_id)
    data = result.get("data") or []
    if not data:
        raise OkxApiError(f"未找到交易对: {inst_id}")
    return data[0]


def build_public_client(config: dict[str, Any]) -> OkxClient:
    return OkxClient(
        {
            "apiKey": config.get("apiKey", "public") if config else "public",
            "secretKey": config.get("secretKey", "public") if config else "public",
            "passphrase": config.get("passphrase", "public") if config else "public",
            "baseUrl": (config.get("baseUrl") if config else "") or "https://www.okx.com",
            "simulated": bool(config.get("simulated")) if config else False,
        }
    )


def recent_range_pct(candles: list[dict[str, Any]], window: int = 24) -> Decimal:
    if not candles:
        return Decimal("0")
    sample = candles[-min(len(candles), window):]
    high = max((row["high"] for row in sample), default=Decimal("0"))
    low = min((row["low"] for row in sample), default=Decimal("0"))
    last_close = sample[-1]["close"]
    if last_close <= 0:
        return Decimal("0")
    return ((high - low) / last_close) * Decimal("100")


def runtime_research_options(automation: dict[str, Any]) -> dict[str, Any]:
    slow = max(int(automation.get("slowEma", 21) or 21), 8)
    return {
        "historyLimit": max(180, min(slow * 10, 360)),
        "optimizationDepth": "quick",
        "includeAltBars": True,
        "raceSize": 10,
        "populationSize": 40,
        "evolutionLoops": 2,
        "enableHybrid": True,
        "enableFineTune": True,
    }


def extract_first_row(response: dict[str, Any]) -> dict[str, Any]:
    rows = response.get("data") or []
    return rows[0] if rows else {}


def build_execution_analysis(
    automation: dict[str, Any],
    client: OkxClient,
) -> dict[str, Any]:
    research = research_optimize(automation, runtime_research_options(automation), client)
    best_entry = (research.get("leaderboard") or [{}])[0]
    best_config = deep_merge({}, research.get("bestConfig") or automation)
    bar = str(best_config.get("bar", automation.get("bar", "5m")))
    fast = int(best_config.get("fastEma", automation.get("fastEma", 9)))
    slow = int(best_config.get("slowEma", automation.get("slowEma", 21)))

    jobs: dict[str, Any] = {
        "spotTicker": lambda: extract_first_row(client.get_ticker(best_config["spotInstId"])),
        "swapTicker": lambda: extract_first_row(client.get_ticker(best_config["swapInstId"])),
        "markPrice": lambda: extract_first_row(client.get_mark_price("SWAP", best_config["swapInstId"])),
        "fundingRate": lambda: extract_first_row(client.get_funding_rate(best_config["swapInstId"])),
        "openInterest": lambda: extract_first_row(client.get_open_interest("SWAP", best_config["swapInstId"])),
        "spotCandles": lambda: get_closed_candles(
            client,
            best_config["spotInstId"],
            bar,
            max(slow + 30, 80),
        ),
        "swapCandles": lambda: get_closed_candles(
            client,
            best_config["swapInstId"],
            bar,
            max(slow + 30, 80),
        ),
    }

    fetched: dict[str, Any] = {}
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        future_map = {executor.submit(fn): key for key, fn in jobs.items()}
        for future in concurrent.futures.as_completed(future_map):
            key = future_map[future]
            try:
                fetched[key] = future.result()
            except Exception as exc:
                errors.append(f"{key}: {exc}")

    if errors:
        raise OkxApiError(f"联网分析失败: {'; '.join(errors)}")

    spot_candles = fetched["spotCandles"]
    swap_candles = fetched["swapCandles"]
    spot_signal = build_signal(spot_candles, fast, slow)
    swap_signal = build_signal(swap_candles, fast, slow)

    spot_last = safe_decimal(fetched["spotTicker"].get("last"), "0")
    swap_last = safe_decimal(fetched["swapTicker"].get("last"), "0")
    mark_price = safe_decimal(fetched["markPrice"].get("markPx"), "0")
    funding_rate_pct = safe_decimal(fetched["fundingRate"].get("fundingRate"), "0") * Decimal("100")
    spread_pct = (
        ((swap_last - spot_last) / spot_last) * Decimal("100")
        if spot_last > 0
        else Decimal("0")
    )
    basis_pct = (
        ((swap_last - mark_price) / mark_price) * Decimal("100")
        if mark_price > 0
        else Decimal("0")
    )
    volatility_pct = max(recent_range_pct(spot_candles), recent_range_pct(swap_candles))
    selected_return = safe_decimal(best_entry.get("returnPct"), "0")
    selected_drawdown = safe_decimal(best_entry.get("maxDrawdownPct"), "0")
    selected_score = safe_decimal(best_entry.get("score"), "0")
    selected_win_rate = safe_decimal(best_entry.get("winRatePct"), "0")
    open_interest = (
        fetched["openInterest"].get("oiUsd")
        or fetched["openInterest"].get("oi")
        or fetched["openInterest"].get("oiCcy")
        or "--"
    )

    blockers: list[str] = []
    warnings: list[str] = []
    if abs(spread_pct) >= Decimal("0.25"):
        blockers.append("现货和永续价差偏大")
    if abs(basis_pct) >= Decimal("0.18"):
        warnings.append("永续最新价偏离标记价")
    if volatility_pct <= Decimal("0.35"):
        warnings.append("最近波动偏低")
    if selected_return <= Decimal("0") or selected_score < Decimal("1.0"):
        warnings.append("最近样本优势不够强")
    if selected_drawdown <= Decimal("-1.20"):
        warnings.append("候选策略最近回撤偏深")
    if abs(funding_rate_pct) >= Decimal("0.08"):
        warnings.append("当前资金费偏热")
    if spot_signal["trend"] != swap_signal["trend"] and "flat" not in {spot_signal["trend"], swap_signal["trend"]}:
        warnings.append("现货与永续趋势不完全一致")
    if spot_signal["signal"] == "hold" and swap_signal["signal"] == "hold":
        warnings.append("当前没有新的交叉触发")

    allow_new_entries = not blockers and selected_return > Decimal("0") and selected_score >= Decimal("1.0")
    if volatility_pct <= Decimal("0.35") or (spot_signal["signal"] == "hold" and swap_signal["signal"] == "hold"):
        allow_new_entries = False
    best_mode = str(best_config.get("swapStrategyMode", "trend_follow"))
    if abs(funding_rate_pct) >= Decimal("0.08"):
        if best_mode == "long_only" and funding_rate_pct > 0:
            allow_new_entries = False
        if best_mode == "short_only" and funding_rate_pct < 0:
            allow_new_entries = False

    if blockers:
        decision = "skip"
        decision_label = "跳过新开仓"
    elif allow_new_entries:
        decision = "execute"
        decision_label = "允许执行"
    else:
        decision = "observe"
        decision_label = "观察为主"

    regime = "趋势共振" if spot_signal["trend"] == swap_signal["trend"] and spot_signal["trend"] != "flat" else "震荡筛选"
    summary_bits = [
        f"采用 {best_entry.get('name') or strategy_short_name(best_config)}",
        f"现货 {spot_signal['trend']}",
        f"永续 {swap_signal['trend']}",
        f"波动 {decimal_to_str(volatility_pct.quantize(Decimal('0.01')))}%",
        f"资金费 {decimal_to_str(funding_rate_pct.quantize(Decimal('0.001')))}%",
    ]
    if blockers:
        summary_bits.append(f"本轮先不新开仓：{blockers[0]}")
    elif allow_new_entries:
        summary_bits.append("允许按最优参数执行")
    else:
        summary_bits.append("本轮先观察，只保留减仓/止损思路")

    analysis = {
        "statusText": "已联网分析",
        "decision": decision,
        "decisionLabel": decision_label,
        "summary": " · ".join(summary_bits),
        "selectedStrategyName": best_entry.get("name") or strategy_short_name(best_config),
        "selectedStrategyDetail": best_entry.get("detail") or strategy_detail_line(best_config, best_entry.get("originLabel", "")),
        "selectedReturnPct": compact_metric(best_entry.get("returnPct"), "0.01"),
        "selectedDrawdownPct": compact_metric(best_entry.get("maxDrawdownPct"), "0.01"),
        "selectedWinRatePct": compact_metric(selected_win_rate, "0.01"),
        "selectedScore": compact_metric(best_entry.get("score"), "0.01"),
        "allowNewEntries": allow_new_entries,
        "optimizerRefreshed": True,
        "lastAnalyzedAt": now_local_iso(),
        "marketRegime": regime,
        "spotTrend": f"{spot_signal['trend']} / {spot_signal['signal']}",
        "swapTrend": f"{swap_signal['trend']} / {swap_signal['signal']}",
        "volatilityPct": compact_metric(volatility_pct, "0.01"),
        "spreadPct": compact_metric(spread_pct, "0.01"),
        "basisPct": compact_metric(basis_pct, "0.01"),
        "fundingRatePct": compact_metric(funding_rate_pct, "0.001"),
        "openInterest": str(open_interest),
        "warnings": warnings,
        "blockers": blockers,
        "selectedConfig": best_config,
        "research": research,
    }
    return analysis


def vendor_repo_summary(repo_id: str, name: str, path: Path, summary: str, route: str) -> dict[str, Any]:
    info = {
        "id": repo_id,
        "name": name,
        "route": route,
        "summary": summary,
        "path": str(path),
        "available": path.exists(),
        "commit": "",
        "updatedAt": "",
        "headline": "",
    }
    if not path.exists():
        return info
    try:
        commit = subprocess.run(
            ["git", "-C", str(path), "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            check=False,
        )
        if commit.returncode == 0:
            info["commit"] = commit.stdout.strip()
        headline = subprocess.run(
            ["git", "-C", str(path), "log", "-1", "--format=%cs|%s"],
            capture_output=True,
            text=True,
            check=False,
        )
        if headline.returncode == 0 and headline.stdout.strip():
            updated_at, _, subject = headline.stdout.strip().partition("|")
            info["updatedAt"] = updated_at
            info["headline"] = subject
    except Exception:
        return info
    return info


def miner_sources() -> list[dict[str, Any]]:
    return [
        vendor_repo_summary(
            "nerdminer_v2",
            "NerdMiner v2",
            VENDOR_ROOT / "NerdMiner_v2",
            "ESP32 Stratum solo miner，适合桌面乐透学习和低难度 share 池。",
            "ESP32 乐透机",
        ),
        vendor_repo_summary(
            "leafminer",
            "LeafMiner",
            VENDOR_ROOT / "leafminer",
            "轻量 ESP32 / ESP8266 比特币 solo miner，支持快速网页刷机。",
            "轻量乐透机",
        ),
        vendor_repo_summary(
            "esp_miner",
            "ESP-Miner / AxeOS",
            VENDOR_ROOT / "ESP-Miner",
            "Bitaxe 固件与 AxeOS API，适合真矿机监控、控制和 OTA。",
            "Bitaxe 真矿机",
        ),
    ]


def serial_ports() -> list[str]:
    ignored = (
        "Bluetooth",
        "debug-console",
        "wlan-debug",
    )
    ports = []
    for path in sorted(glob.glob("/dev/cu.*")):
        if any(token in path for token in ignored):
            continue
        ports.append(path)
    return ports


def parse_hosts(raw: str) -> list[str]:
    hosts = []
    for part in (raw or "").replace("\n", ",").split(","):
        host = part.strip()
        if not host:
            continue
        if not host.startswith(("http://", "https://")):
            host = f"http://{host}"
        hosts.append(host.rstrip("/"))
    return hosts


def normalize_wallet_worker(wallet: str, worker: str) -> str:
    wallet = (wallet or "").strip()
    worker = (worker or "").strip()
    if not wallet:
        return ""
    if not worker:
        return wallet
    if "_" in wallet:
        return wallet
    return f"{wallet}_{worker}"


def fetch_json(url: str, timeout: int = 3) -> Any:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.json()


def system_resolve_ipv4(host: str) -> list[str]:
    try:
        infos = socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
    except OSError:
        return []
    return sorted({item[4][0] for item in infos if item[0] == socket.AF_INET})


def doh_resolve_ipv4(host: str) -> list[str]:
    req = urllib.request.Request(
        f"https://cloudflare-dns.com/dns-query?name={urllib.parse.quote(host)}&type=A",
        headers={"accept": "application/dns-json"},
    )
    ctx = ssl._create_unverified_context()
    with urllib.request.urlopen(req, timeout=6, context=ctx) as response:
        payload = json.load(response)
    return [row.get("data") for row in payload.get("Answer", []) if row.get("type") == 1 and row.get("data")]


def reset_pool_diag_cache() -> None:
    POOL_DIAG_CACHE.update({"ts": 0.0, "host": "", "port": 0, "result": {}})


def ip_is_bogon(ip: str) -> bool:
    try:
        addr = ipaddress.ip_address(ip)
    except ValueError:
        return True
    reserved_benchmark = ipaddress.ip_network("198.18.0.0/15")
    return (
        addr.is_private
        or addr.is_loopback
        or addr.is_link_local
        or addr.is_multicast
        or addr.is_reserved
        or addr.is_unspecified
        or addr in reserved_benchmark
    )


def diagnose_pool_endpoint(host: str, port: int) -> dict[str, Any]:
    host = (host or "").strip()
    if not host or port <= 0:
        return {"status": "missing", "systemIps": [], "publicIps": [], "connectHost": "", "detail": "未配置矿池地址"}

    now = time.time()
    cached = POOL_DIAG_CACHE.get("result") or {}
    if (
        cached
        and POOL_DIAG_CACHE.get("host") == host
        and int(POOL_DIAG_CACHE.get("port") or 0) == int(port)
        and now - float(POOL_DIAG_CACHE.get("ts") or 0.0) < 90
    ):
        return copy.deepcopy(cached)

    system_ips = system_resolve_ipv4(host)
    try:
        public_ips = doh_resolve_ipv4(host)
    except Exception as exc:
        public_ips = []
        doh_error = str(exc)
    else:
        doh_error = ""

    connect_host = public_ips[0] if public_ips else host
    tcp_connected = False
    bytes_received = 0
    recv_error = ""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        sock.connect((connect_host, int(port)))
        tcp_connected = True
        sock.sendall(b'{"id":1,"method":"mining.subscribe","params":["OKXLocalApp"]}\n')
        try:
            data = sock.recv(256)
            bytes_received = len(data or b"")
        except Exception as exc:
            recv_error = str(exc)
        finally:
            try:
                sock.close()
            except Exception:
                pass
    except Exception as exc:
        recv_error = str(exc)

    if system_ips and public_ips and system_ips[0] != public_ips[0] and any(ip_is_bogon(ip) for ip in system_ips):
        status = "dns_hijack"
        detail = f"本机 DNS 把 {host} 解析到 {', '.join(system_ips)}，真实公网结果是 {', '.join(public_ips)}。"
    elif tcp_connected and bytes_received == 0:
        status = "stratum_silent"
        detail = f"{connect_host}:{port} 可连通，但订阅后没有收到任何 stratum 返回。"
    elif not tcp_connected:
        status = "connect_failed"
        detail = f"{host}:{port} 连接失败：{recv_error or '未知错误'}"
    else:
        status = "ok"
        detail = f"{connect_host}:{port} 已返回 {bytes_received} 字节。"

    result = {
        "status": status,
        "detail": detail,
        "systemIps": system_ips,
        "publicIps": public_ips,
        "connectHost": connect_host,
        "port": int(port),
        "tcpConnected": tcp_connected,
        "bytesReceived": bytes_received,
        "error": recv_error,
        "dohError": doh_error,
    }
    POOL_DIAG_CACHE.update({"ts": now, "host": host, "port": int(port), "result": copy.deepcopy(result)})
    return result


def fetch_text(url: str, timeout: int = 3) -> str:
    response = requests.get(url, timeout=timeout)
    response.raise_for_status()
    return response.text.strip()


def format_hashrate(value: Any) -> str:
    num = float(safe_decimal(value, "0"))
    if num <= 0:
        return "--"
    units = ["H/s", "KH/s", "MH/s", "GH/s", "TH/s", "PH/s", "EH/s", "ZH/s"]
    index = 0
    while num >= 1000 and index < len(units) - 1:
        num /= 1000.0
        index += 1
    decimals = 2 if num < 100 else 1
    return f"{num:.{decimals}f} {units[index]}"


def format_duration_brief(seconds: float | int) -> str:
    seconds = float(seconds or 0)
    if seconds <= 0:
        return "--"
    minute = 60
    hour = 3600
    day = 86400
    year = 365 * day
    if seconds < minute:
        return f"{seconds:.0f} 秒"
    if seconds < hour:
        return f"{seconds / minute:.1f} 分钟"
    if seconds < day:
        return f"{seconds / hour:.1f} 小时"
    if seconds < year:
        return f"{seconds / day:.1f} 天"
    return f"{seconds / year:.1f} 年"


def format_btc_amount(value: float | int) -> str:
    amount = float(value or 0)
    if amount <= 0:
        return "≈ 0 BTC"
    if amount >= 0.01:
        return f"{amount:.4f} BTC"
    if amount >= 0.000001:
        return f"{amount:.8f} BTC"
    return f"{amount:.3e} BTC"


def format_probability_pct(value: float | int) -> str:
    pct = float(value or 0)
    if pct <= 0:
        return "≈ 0%"
    if pct >= 1:
        return f"{pct:.2f}%"
    if pct >= 0.01:
        return f"{pct:.3f}%"
    return f"{pct:.3e}%"


def format_usd_estimate(value: float | int) -> str:
    amount = float(value or 0)
    if amount <= 0:
        return "≈ 0 USDT/天"
    if amount >= 0.01:
        return f"≈ {amount:.4f} USDT/天"
    if amount >= 0.000001:
        return f"≈ {amount:.8f} USDT/天"
    return f"≈ {amount:.3e} USDT/天"


def benchmark_cpu_hashrate(force: bool = False) -> dict[str, Any]:
    now = time.time()
    cached_age = now - float(HASHRATE_BENCHMARK_CACHE.get("ts") or 0)
    if not force and HASHRATE_BENCHMARK_CACHE.get("hashrate") and cached_age < 900:
        return dict(HASHRATE_BENCHMARK_CACHE)

    seed = b"okx-local-app-benchmark"
    count = 0
    batch = 2000
    start = time.perf_counter()
    elapsed = 0.0
    payload = seed
    while elapsed < 0.35:
        for _ in range(batch):
            payload = hashlib.sha256(hashlib.sha256(payload).digest()).digest()
        count += batch
        elapsed = time.perf_counter() - start
    hashrate = count / max(elapsed, 0.001)
    HASHRATE_BENCHMARK_CACHE.update(
        {"ts": now, "hashrate": hashrate, "duration": elapsed}
    )
    return dict(HASHRATE_BENCHMARK_CACHE)


def parse_pool_payload_stats(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    stats: dict[str, Any] = {}
    for key in ("accepted", "rejected", "hashrate", "shares", "bestshare", "bestShare"):
        if key in payload:
            stats[key] = payload.get(key)
    nested = payload.get("data")
    if isinstance(nested, dict):
        for key in ("accepted", "rejected", "hashrate", "shares", "bestshare", "bestShare"):
            if key in nested and key not in stats:
                stats[key] = nested.get(key)
    return stats


def build_miner_progress(
    config: dict[str, Any],
    network: dict[str, Any],
    pool: dict[str, Any],
    mac_lotto: dict[str, Any],
    pool_diag: dict[str, Any] | None = None,
) -> dict[str, Any]:
    benchmark = benchmark_cpu_hashrate()
    reported_hashrate = float(safe_decimal(mac_lotto.get("hashrate"), "0"))
    estimated_hashrate = float(
        safe_decimal(mac_lotto.get("estimatedHashrateHps"), benchmark.get("hashrate") or 0)
    )
    effective_hashrate = reported_hashrate if reported_hashrate > 0 else estimated_hashrate
    worker_count = int(mac_lotto.get("effectiveWorkerCount") or 1)
    hashrate_source = "矿机实测聚合" if reported_hashrate > 0 else f"本机基准估算 x {worker_count}"

    network_hashrate_payload = network.get("hashrate") or {}
    network_hashrate = float(safe_decimal(network_hashrate_payload.get("currentHashrate"), "0"))
    network_difficulty = float(
        safe_decimal(
            network_hashrate_payload.get("currentDifficulty")
            or (network.get("difficulty") or {}).get("currentDifficulty"),
            "0",
        )
    )
    best_difficulty = float(safe_decimal(mac_lotto.get("best_difficulty"), "0"))
    best_ratio_pct = (best_difficulty / network_difficulty * 100.0) if network_difficulty and best_difficulty else 0.0
    ratio_divisor = (network_difficulty / best_difficulty) if network_difficulty and best_difficulty else 0.0
    daily_btc = (
        (effective_hashrate / network_hashrate) * 144.0 * 3.125
        if effective_hashrate > 0 and network_hashrate > 0
        else 0.0
    )
    btc_price = float(safe_decimal((network.get("btcTicker") or {}).get("last"), "0"))
    daily_usd = daily_btc * btc_price if btc_price > 0 else 0.0
    expected_block_seconds = (
        (network_hashrate / effective_hashrate) * 600.0
        if effective_hashrate > 0 and network_hashrate > 0
        else 0.0
    )
    chance_per_day_pct = (
        min((86400.0 / expected_block_seconds) * 100.0, 100.0)
        if expected_block_seconds > 0
        else 0.0
    )

    started_at = parse_iso(str(mac_lotto.get("startedAt") or ""))
    uptime_seconds = max((datetime.now() - started_at).total_seconds(), 0.0) if started_at else 0.0
    waiting_count = sum(1 for line in mac_lotto.get("logTail", []) if "Waiting for stratum job" in line)
    new_hash_count = sum(1 for line in mac_lotto.get("logTail", []) if "New hash:" in line)
    block_found = sum(1 for line in mac_lotto.get("logTail", []) if "Block" in line and "solved" in line)
    pool_stats = parse_pool_payload_stats(pool.get("payload"))
    accepted = pool_stats.get("accepted")
    rejected = pool_stats.get("rejected")
    pool_diag = pool_diag or {}

    status = str(mac_lotto.get("status") or "idle")
    running = bool(mac_lotto.get("running"))
    if running and status == "mining":
        headline = "在线 / 挖矿中"
        detail = "作业已下发，持续刷 nonce。"
    elif running and status in {"subscribed", "new-job"}:
        headline = "在线 / 新作业"
        detail = "job 已刷新，继续刷 nonce。"
    elif running and status == "waiting-for-job":
        if pool_diag.get("status") == "dns_hijack":
            headline = "未在挖 / DNS 异常"
            detail = str(pool_diag.get("detail") or "本机 DNS 解析异常，当前没连到真实矿池。")
        elif pool_diag.get("status") == "stratum_silent":
            headline = "未在挖 / 矿池无回包"
            detail = str(pool_diag.get("detail") or "矿池端口可连，但没有下发 stratum job。")
        elif pool_diag.get("status") == "connect_failed":
            headline = "未在挖 / 连接失败"
            detail = str(pool_diag.get("detail") or "当前矿池连接失败。")
        else:
            headline = "在线 / 待作业"
            detail = "Stratum 已连通，等待 job 下发。"
    elif status == "pool-offline":
        headline = "离线 / 矿池异常"
        detail = f"矿池连接异常：{mac_lotto.get('pool_error') or pool.get('statusText') or '连接失败'}"
    elif running:
        headline = "在线 / 状态波动"
        detail = "进程已运行，等待状态稳定。"
    else:
        headline = "已停止"
        detail = "当前没有持续提交算力。"
    if worker_count > 1:
        detail = f"{detail} {worker_count} workers。"

    progress_text = (
        f"最佳结果约摸到网络难度的 {format_probability_pct(best_ratio_pct)}"
        if best_ratio_pct > 0
        else "还没有形成可见的难度进展"
    )
    progress_sub = (
        f"约等于网络难度的 1 / {ratio_divisor:,.0f}"
        if ratio_divisor >= 1
        else "彩票矿机一般很久都不会接近整块难度"
    )

    return {
        "headline": headline,
        "detail": detail,
        "hashrateHps": effective_hashrate,
        "hashrateText": format_hashrate(effective_hashrate),
        "hashrateSource": hashrate_source,
        "benchmarkHashrateText": format_hashrate(estimated_hashrate),
        "workerCount": worker_count,
        "networkHashrateHps": network_hashrate,
        "networkHashrateText": format_hashrate(network_hashrate),
        "networkDifficulty": network_difficulty,
        "networkDifficultyText": f"{network_difficulty:,.0f}" if network_difficulty > 0 else "--",
        "bestDifficulty": best_difficulty,
        "bestDifficultyText": f"{best_difficulty:,.2f}" if best_difficulty > 0 else "--",
        "progressPct": best_ratio_pct,
        "progressText": progress_text,
        "progressSubtext": progress_sub,
        "dailyBtc": daily_btc,
        "dailyBtcText": format_btc_amount(daily_btc),
        "dailyUsd": daily_usd,
        "dailyUsdText": format_usd_estimate(daily_usd),
        "expectedBlockSeconds": expected_block_seconds,
        "expectedBlockText": format_duration_brief(expected_block_seconds),
        "chancePerDayPct": chance_per_day_pct,
        "chancePerDayText": format_probability_pct(chance_per_day_pct),
        "uptimeSeconds": uptime_seconds,
        "uptimeText": format_duration_brief(uptime_seconds),
        "startedAt": mac_lotto.get("startedAt") or "",
        "waitingCycles": waiting_count,
        "hashEvents": new_hash_count,
        "blocksFound": block_found,
        "acceptedShares": accepted if accepted is not None else "-",
        "rejectedShares": rejected if rejected is not None else "-",
        "poolReachable": "失败" not in str(pool.get("statusText") or ""),
        "rewardModel": "这是彩票矿机：多数时候没有稳定 share 收益，主要看极低概率中整块。",
        "poolDiagnosis": pool_diag,
    }


def btc_network_snapshot(public_client: OkxClient | None = None) -> dict[str, Any]:
    network: dict[str, Any] = {}
    jobs: list[tuple[str, Any]] = [
        ("tipHeight", lambda: fetch_text("https://mempool.space/api/blocks/tip/height", timeout=2)),
        ("fees", lambda: fetch_json("https://mempool.space/api/v1/fees/recommended", timeout=2)),
        ("difficulty", lambda: fetch_json("https://mempool.space/api/v1/difficulty-adjustment", timeout=2)),
        ("hashrate", lambda: fetch_json("https://mempool.space/api/v1/mining/hashrate/3d", timeout=2)),
    ]
    if public_client:
        jobs.append(
            ("btcTicker", lambda: (public_client.get_ticker("BTC-USDT").get("data") or [{}])[0])
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(jobs)) as executor:
        future_map = {
            executor.submit(fn): key for key, fn in jobs
        }
        for future in concurrent.futures.as_completed(future_map):
            key = future_map[future]
            try:
                network[key] = future.result()
            except Exception as exc:
                network[f"{key}Error"] = str(exc)
    return network


def pool_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    worker_wallet = normalize_wallet_worker(config.get("wallet", ""), config.get("workerName", ""))
    if not worker_wallet:
        return {"worker": "", "statusText": "填入 BTC 地址后可读取 pool 侧统计"}
    api_base = (config.get("poolApiBase") or "").strip()
    pool_host = (config.get("poolHost") or "").strip().lower()
    if not api_base:
        return {"worker": worker_wallet, "statusText": "未配置 pool API 地址"}
    if pool_host in {"127.0.0.1", "localhost"}:
        return {
            "worker": worker_wallet,
            "statusText": "本地测试模式，跳过外部 pool 统计",
        }
    if not api_base.endswith("/"):
        api_base += "/"
    try:
        payload = fetch_json(f"{api_base}{urllib.parse.quote(worker_wallet, safe='')}", timeout=3)
        return {
            "worker": worker_wallet,
            "statusText": "已拿到 pool 侧统计",
            "payload": payload,
        }
    except Exception as exc:
        return {
            "worker": worker_wallet,
            "statusText": f"读取 pool 统计失败: {exc}",
        }


def bitaxe_device_snapshot(host: str) -> dict[str, Any]:
    device = {
        "host": host,
        "reachable": False,
        "info": {},
        "asic": {},
        "dashboard": {},
        "statusText": "未连接",
    }
    try:
        info = fetch_json(f"{host}/api/system/info", timeout=4)
        asic = fetch_json(f"{host}/api/system/asic", timeout=4)
        dashboard = fetch_json(f"{host}/api/system/statistics/dashboard", timeout=4)
        device.update(
            {
                "reachable": True,
                "info": info,
                "asic": asic,
                "dashboard": dashboard,
                "statusText": "AxeOS 在线",
            }
        )
    except Exception as exc:
        device["statusText"] = f"探测失败: {exc}"
    return device


def remote_miner_options() -> list[dict[str, Any]]:
    cached_age = time.time() - float(MINER_OPTIONS_CACHE.get("ts") or 0.0)
    if MINER_OPTIONS_CACHE.get("items") and cached_age < 1800:
        return copy.deepcopy(MINER_OPTIONS_CACHE["items"])

    seeds = [
        {
            "id": "cpuminer-opt",
            "repo": "JayDDee/cpuminer-opt",
            "label": "CPU 多线程矿工",
            "note": "适合本机 CPU 拉满，多线程比当前单脚本更接近真正 CPU 挖矿形态。",
        },
        {
            "id": "cpuminer",
            "repo": "pooler/cpuminer",
            "label": "轻量 CPU 方案",
            "note": "更轻、更老牌，适合做最小化本地 CPU 直连实验。",
        },
        {
            "id": "bfgminer",
            "repo": "luke-jr/bfgminer",
            "label": "多设备控制台",
            "note": "更适合同时挂多池、多设备和远程接口，不只盯单台本机。",
        },
        {
            "id": "farm-proxy",
            "repo": "braiins/farm-proxy",
            "label": "矿场聚合代理",
            "note": "适合把更多矿机统一挂到一层代理后面，再集中监控和切池。",
        },
    ]

    def fetch(seed: dict[str, Any]) -> dict[str, Any]:
        fallback = {
            "id": seed["id"],
            "name": seed["repo"],
            "label": seed["label"],
            "note": seed["note"],
            "url": f"https://github.com/{seed['repo']}",
            "stars": None,
            "updatedAt": "",
            "description": "",
        }
        try:
            payload = fetch_json(f"https://api.github.com/repos/{seed['repo']}", timeout=4)
        except Exception:
            return fallback
        return {
            **fallback,
            "name": payload.get("full_name") or seed["repo"],
            "url": payload.get("html_url") or fallback["url"],
            "stars": payload.get("stargazers_count"),
            "updatedAt": str(payload.get("pushed_at") or "")[:10],
            "description": payload.get("description") or "",
        }

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(seeds)) as executor:
        items = list(executor.map(fetch, seeds))
    MINER_OPTIONS_CACHE.update({"ts": time.time(), "items": items})
    return copy.deepcopy(items)


def miner_overview(config: dict[str, Any], public_client: OkxClient | None = None) -> dict[str, Any]:
    hosts = parse_hosts(config.get("bitaxeHosts", ""))
    devices = [bitaxe_device_snapshot(host) for host in hosts[:6]]
    network = btc_network_snapshot(public_client)
    pool = pool_snapshot(config)
    mac_lotto = MAC_LOTTO.snapshot(config)
    pool_diag = diagnose_pool_endpoint(
        str(config.get("poolHost", "")).strip(),
        int(config.get("poolPort", 0) or 0),
    )
    return {
        "config": config,
        "sources": miner_sources(),
        "options": remote_miner_options(),
        "serialPorts": serial_ports(),
        "network": network,
        "pool": pool,
        "progress": build_miner_progress(config, network, pool, mac_lotto, pool_diag),
        "poolDiagnosis": pool_diag,
        "devices": devices,
        "macLotto": mac_lotto,
        "lastRefreshAt": now_local_iso(),
    }


def miner_focus_overview(config: dict[str, Any]) -> dict[str, Any]:
    cached = MINER_STATE.current()
    mac_lotto = MAC_LOTTO.snapshot(config)
    network = cached.get("network") if isinstance(cached.get("network"), dict) else {}
    progress = cached.get("progress") if isinstance(cached.get("progress"), dict) else {}
    pool = cached.get("pool") if isinstance(cached.get("pool"), dict) else {}
    pool_diag = cached.get("poolDiagnosis") if isinstance(cached.get("poolDiagnosis"), dict) else {}
    options = cached.get("options") if isinstance(cached.get("options"), list) else []
    serial_ports_cached = cached.get("serialPorts") if isinstance(cached.get("serialPorts"), list) else []
    if not progress:
        progress = build_miner_progress(config, network, pool, mac_lotto, pool_diag)
    return {
        "config": config,
        "options": options,
        "serialPorts": serial_ports_cached,
        "network": network,
        "pool": pool,
        "progress": progress,
        "poolDiagnosis": pool_diag,
        "devices": [],
        "macLotto": mac_lotto,
        "lastRefreshAt": cached.get("lastRefreshAt") or now_local_iso(),
    }


def update_miner_state(overview: dict[str, Any]) -> dict[str, Any]:
    def mutate(state: dict[str, Any]) -> None:
        state.update(
            {
                "lastRefreshAt": overview.get("lastRefreshAt", ""),
                "network": overview.get("network", {}),
                "sources": overview.get("sources", []),
                "options": overview.get("options", []),
                "serialPorts": overview.get("serialPorts", []),
                "devices": overview.get("devices", []),
                "pool": overview.get("pool", {}),
                "poolDiagnosis": overview.get("poolDiagnosis", {}),
                "progress": overview.get("progress", {}),
                "macLotto": overview.get("macLotto", {}),
            }
        )
        logs = state.setdefault("logs", [])
        logs.append(
            {
                "ts": overview.get("lastRefreshAt", now_local_iso()),
                "level": "info",
                "message": f"矿机概览已刷新，并发矿工 {overview.get('macLotto', {}).get('effectiveWorkerCount', 0)} 个，可扩展方案 {len(overview.get('options', []))} 条。",
            }
        )
        state["logs"] = logs[-40:]

    return MINER_STATE.update(mutate)


def append_miner_log(level: str, message: str) -> None:
    stamp = now_local_iso()

    def mutate(state: dict[str, Any]) -> None:
        logs = state.setdefault("logs", [])
        logs.append({"ts": stamp, "level": level, "message": message})
        state["logs"] = logs[-40:]

    MINER_STATE.update(mutate)


def post_bitaxe_action(host: str, action: str) -> dict[str, Any]:
    host = host.strip().rstrip("/")
    if not host:
        raise RuntimeError("缺少 Bitaxe 地址")
    if not host.startswith(("http://", "https://")):
        host = f"http://{host}"
    response = requests.post(f"{host}/api/system/{action}", timeout=6)
    response.raise_for_status()
    text = response.text.strip()
    try:
        payload = response.json()
    except ValueError:
        payload = {"raw": text or "ok"}
    return {"host": host, "action": action, "payload": payload}


def tail_lines(path: Path, count: int = 20) -> list[str]:
    if not path.exists():
        return []
    try:
        text = path.read_text(encoding="utf-8", errors="ignore")
    except OSError:
        return []
    lines = [line for line in text.splitlines() if line.strip()]
    return lines[-count:]


class MacLottoManager:
    def __init__(self) -> None:
        self.processes: dict[str, dict[str, Any]] = {}
        self.lock = threading.RLock()
        self.supervisor_thread: threading.Thread | None = None
        self.supervisor_lock = threading.RLock()
        self.next_restart_not_before = 0.0
        self.last_signature: tuple[Any, ...] | None = None

    def _script_dir(self) -> Path:
        return APP_DIR / "vendor" / "solominer"

    def _script_path(self) -> Path:
        return self._script_dir() / "solo_miner.py"

    def _workers_dir(self) -> Path:
        return DATA_DIR / "mac-lotto-workers"

    def _kill_stray_workers(self) -> None:
        script_marker = str(self._script_path())
        try:
            subprocess.run(
                ["pkill", "-f", script_marker],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                check=False,
            )
        except Exception:
            pass

    def _cleanup_if_dead(self) -> None:
        dead_ids = []
        for worker_id, meta in self.processes.items():
            process = meta.get("process")
            if process and process.poll() is None:
                continue
            handle = meta.get("log_handle")
            if handle:
                try:
                    handle.close()
                except Exception:
                    pass
            dead_ids.append(worker_id)
        for worker_id in dead_ids:
            self.processes.pop(worker_id, None)

    def _should_autostart(self, config: dict[str, Any]) -> bool:
        return bool(config.get("autoStartMacLotto", True)) and bool(str(config.get("wallet", "")).strip())

    def _worker_limit(self) -> int:
        return max(1, min((os.cpu_count() or 1) * 2, 32))

    def _guard_mode(self, config: dict[str, Any]) -> dict[str, Any]:
        pool_host = str(config.get("poolHost", "")).strip()
        pool_port = int(config.get("poolPort", 0) or 0)
        if not pool_host or pool_port <= 0:
            return {"active": False, "reason": "", "diagnosis": {}}
        diagnosis = diagnose_pool_endpoint(pool_host, pool_port)
        active = diagnosis.get("status") in {"dns_hijack", "stratum_silent", "connect_failed"}
        return {
            "active": active,
            "reason": str(diagnosis.get("detail") or ""),
            "diagnosis": diagnosis,
        }

    def _normalized_worker_count(self, config: dict[str, Any]) -> tuple[int, int]:
        try:
            requested = int(config.get("cpuWorkers") or default_cpu_worker_count())
        except (TypeError, ValueError):
            requested = default_cpu_worker_count()
        requested = max(1, requested)
        effective = min(requested, self._worker_limit())
        guard = self._guard_mode(config)
        if guard["active"]:
            effective = 1
        return requested, max(1, effective)

    def _worker_specs(self, config: dict[str, Any]) -> tuple[int, int, list[dict[str, Any]]]:
        requested, effective = self._normalized_worker_count(config)
        workers_dir = self._workers_dir()
        specs = []
        for index in range(effective):
            worker_id = f"w{index + 1:02d}"
            specs.append(
                {
                    "id": worker_id,
                    "index": index + 1,
                    "status_path": workers_dir / f"{worker_id}.status.json",
                    "log_path": workers_dir / f"{worker_id}.log",
                }
            )
        return requested, effective, specs

    def _signature(self, config: dict[str, Any]) -> tuple[Any, ...]:
        requested, effective = self._normalized_worker_count(config)
        return (
            str(config.get("wallet", "")).strip(),
            str(config.get("poolHost", "")).strip(),
            int(config.get("poolPort", 0) or 0),
            str(config.get("poolPassword", "") or "x"),
            bool(config.get("cpuRandomNonce")),
            requested,
            effective,
        )

    def _all_running(self, expected_count: int) -> bool:
        if len(self.processes) != expected_count:
            return False
        return all(meta.get("process") and meta["process"].poll() is None for meta in self.processes.values())

    def _write_aggregate_status(self, payload: dict[str, Any]) -> None:
        safe_payload = {
            key: value
            for key, value in payload.items()
            if key not in {"logTail", "workers"}
        }
        try:
            secure_dump_json(MAC_LOTTO_STATUS_PATH, safe_payload)
        except Exception:
            pass

    def ensure_supervisor(self) -> None:
        with self.supervisor_lock:
            if self.supervisor_thread and self.supervisor_thread.is_alive():
                return
            self.supervisor_thread = threading.Thread(
                target=self._supervisor_loop,
                name="mac-lotto-supervisor",
                daemon=True,
            )
            self.supervisor_thread.start()

    def _supervisor_loop(self) -> None:
        while True:
            time.sleep(15)
            config = MINER_CONFIG.current()
            if not self._should_autostart(config):
                self.next_restart_not_before = 0.0
                continue
            requested, effective, _ = self._worker_specs(config)
            self._cleanup_if_dead()
            if self._all_running(effective):
                self.next_restart_not_before = 0.0
                continue
            if time.time() < self.next_restart_not_before:
                continue
            try:
                self.start(config)
                self.next_restart_not_before = 0.0
                append_miner_log("info", f"Mac 本机乐透机已自动恢复，并发 {effective} 个矿工。")
            except Exception as exc:
                self.next_restart_not_before = time.time() + 60
                append_miner_log("error", f"Mac 本机乐透机自动恢复失败: {exc}")

    def start(self, config: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            reset_pool_diag_cache()
            self._cleanup_if_dead()
            requested, effective, specs = self._worker_specs(config)
            signature = self._signature(config)
            if self._all_running(effective) and self.last_signature == signature:
                return self.snapshot(config)
            if self.processes:
                self.stop(config)
            else:
                self._kill_stray_workers()
            wallet = str(config.get("wallet", "")).strip()
            if not wallet:
                raise RuntimeError("先填 BTC 地址，再启动 Mac 乐透机")
            script_path = self._script_path()
            if not script_path.exists():
                raise RuntimeError("本机 CPU 乐透脚本不存在")

            MAC_LOTTO_STATUS_PATH.parent.mkdir(parents=True, exist_ok=True)
            ensure_private_permissions(MAC_LOTTO_STATUS_PATH.parent, is_dir=True)
            workers_dir = self._workers_dir()
            workers_dir.mkdir(parents=True, exist_ok=True)
            ensure_private_permissions(workers_dir, is_dir=True)
            for leftover in workers_dir.glob("w*.status.json"):
                leftover.unlink(missing_ok=True)
            for leftover in workers_dir.glob("w*.log"):
                leftover.unlink(missing_ok=True)

            start_stamp = now_local_iso()
            pool_host = str(config.get("poolHost", "solo.ckpool.org")).strip() or "solo.ckpool.org"
            pool_port = int(config.get("poolPort", 3333) or 3333)
            pool_password = str(config.get("poolPassword", "x") or "x")
            force_random_nonce = effective > 1 or bool(config.get("cpuRandomNonce"))

            for spec in specs:
                dump_worker_status(
                    spec["status_path"],
                    {
                        "running": False,
                        "status": "starting",
                        "startedAt": start_stamp,
                        "address": wallet,
                        "pool_host": pool_host,
                        "pool_port": pool_port,
                        "workerId": spec["id"],
                    },
                )
                env = dict(os.environ)
                env.update(
                    {
                        "SOLOMINER_ADDRESS": wallet,
                        "SOLOMINER_POOL_HOST": pool_host,
                        "SOLOMINER_POOL_PORT": str(pool_port),
                        "SOLOMINER_POOL_PASSWORD": pool_password,
                        "SOLOMINER_STATUS_PATH": str(spec["status_path"]),
                        "SOLOMINER_LOG_PATH": str(spec["log_path"]),
                        "SOLOMINER_RANDOM": "1" if force_random_nonce else "0",
                    }
                )
                log_handle = open(spec["log_path"], "w", encoding="utf-8")
                args = [sys.executable, str(script_path)]
                if not force_random_nonce:
                    args.append("1")

                process = subprocess.Popen(
                    args,
                    cwd=str(self._script_dir()),
                    env=env,
                    stdout=log_handle,
                    stderr=log_handle,
                    text=True,
                )
                self.processes[spec["id"]] = {
                    "process": process,
                    "log_handle": log_handle,
                    "status_path": spec["status_path"],
                    "log_path": spec["log_path"],
                    "startedAt": start_stamp,
                }

            self.last_signature = signature
            self.next_restart_not_before = 0.0
            return self.snapshot(config)

    def stop(self, config: dict[str, Any] | None = None) -> dict[str, Any]:
        with self.lock:
            self._cleanup_if_dead()
            for worker_id, meta in list(self.processes.items()):
                process = meta.get("process")
                if process and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=5)
                    except subprocess.TimeoutExpired:
                        process.kill()
                handle = meta.get("log_handle")
                if handle:
                    try:
                        handle.close()
                    except Exception:
                        pass
            self._kill_stray_workers()
            self.processes = {}
            self.last_signature = None
            secure_dump_json(
                MAC_LOTTO_STATUS_PATH,
                {
                    "running": False,
                    "status": "stopped",
                    "last_stop_at": now_local_iso(),
                    "effectiveWorkerCount": 0,
                },
            )
            return self.snapshot(config or MINER_CONFIG.current())

    def snapshot(self, config: dict[str, Any]) -> dict[str, Any]:
        with self.lock:
            self._cleanup_if_dead()
            guard = self._guard_mode(config)
            requested, effective, specs = self._worker_specs(config)
            workers = []
            aggregate_logs: list[str] = []
            total_reported_hashrate = 0.0
            best_difficulty = 0.0
            earliest_started_at = ""
            statuses: list[str] = []
            running = False

            for spec in specs:
                meta = self.processes.get(spec["id"], {})
                process = meta.get("process")
                worker_payload: dict[str, Any] = {}
                if spec["status_path"].exists():
                    try:
                        loaded = load_worker_status(spec["status_path"])
                        worker_payload.update(loaded)
                    except Exception:
                        pass
                worker_running = bool(process and process.poll() is None)
                worker_logs = [
                    line
                    for line in tail_lines(spec["log_path"], 8)
                    if not line.strip().lower().endswith((" false", " true"))
                    and line.strip().lower() not in {"false", "true"}
                ]
                aggregate_logs.extend([f"[{spec['id']}] {line}" for line in worker_logs])
                reported = float(safe_decimal(worker_payload.get("hashrate"), "0"))
                total_reported_hashrate += reported
                best_difficulty = max(best_difficulty, float(safe_decimal(worker_payload.get("best_difficulty"), "0")))
                started_at = str(worker_payload.get("startedAt") or meta.get("startedAt") or "")
                if started_at and (not earliest_started_at or started_at < earliest_started_at):
                    earliest_started_at = started_at
                status = str(worker_payload.get("status") or ("running" if worker_running else "idle"))
                statuses.append(status)
                running = running or worker_running
                workers.append(
                    {
                        "id": spec["id"],
                        "running": worker_running,
                        "pid": process.pid if worker_running and process else 0,
                        "status": status,
                        "hashrate": worker_payload.get("hashrate", ""),
                        "hashrateText": f"{worker_payload.get('hashrate')} hash/s" if worker_payload.get("hashrate") not in (None, "") else "--",
                        "bestDifficulty": worker_payload.get("best_difficulty", ""),
                        "startedAt": started_at,
                        "logTail": worker_logs,
                    }
                )

            status_priority = [
                "block-found",
                "mining",
                "new-job",
                "subscribed",
                "waiting-for-job",
                "pool-offline",
                "starting",
                "booting",
                "stopped",
                "idle",
            ]
            status = next((item for item in status_priority if item in statuses), "idle")
            if running and status in {"starting", "booting"}:
                status = "waiting-for-job"

            estimated_hashrate = float(benchmark_cpu_hashrate().get("hashrate") or 0) * effective
            payload = {
                "running": running,
                "pid": next((worker["pid"] for worker in workers if worker["pid"]), 0),
                "wallet": str(config.get("wallet", "")).strip(),
                "poolHost": str(config.get("poolHost", "")).strip(),
                "poolPort": int(config.get("poolPort", 0) or 0),
                "mode": "mac_lotto",
                "status": status,
                "startedAt": earliest_started_at,
                "requestedWorkerCount": requested,
                "effectiveWorkerCount": effective,
                "guardMode": guard["active"],
                "guardReason": guard["reason"],
                "guardDiagnosis": guard["diagnosis"],
                "workerLimit": self._worker_limit(),
                "workers": workers,
                "hashrate": round(total_reported_hashrate, 2) if total_reported_hashrate > 0 else "",
                "hashrateText": format_hashrate(total_reported_hashrate if total_reported_hashrate > 0 else estimated_hashrate),
                "estimatedHashrateHps": estimated_hashrate,
                "best_difficulty": best_difficulty,
                "logTail": aggregate_logs[-24:],
            }
            started_at = parse_iso(str(payload.get("startedAt") or ""))
            if started_at:
                payload["uptimeSeconds"] = max((datetime.now() - started_at).total_seconds(), 0.0)
                payload["uptimeText"] = format_duration_brief(payload["uptimeSeconds"])
            else:
                payload["uptimeSeconds"] = 0.0
                payload["uptimeText"] = "--"
            self._write_aggregate_status(payload)
            return payload


class AutomationEngine:
    def __init__(
        self,
        config_store: ConfigStore,
        automation_store: JsonStore,
        state_store: JsonStore,
    ) -> None:
        self.config_store = config_store
        self.automation_store = automation_store
        self.state_store = state_store
        self.thread: threading.Thread | None = None
        self.stop_event = threading.Event()
        self.lock = threading.RLock()

    def snapshot(self) -> dict[str, Any]:
        return self.state_store.current()

    def _update_state(self, mutator) -> dict[str, Any]:
        return self.state_store.update(mutator)

    def _log(self, level: str, message: str) -> None:
        stamp = now_local_iso()

        def mutate(state: dict[str, Any]) -> None:
            logs = state.setdefault("logs", [])
            logs.append({"ts": stamp, "level": level, "message": message})
            state["logs"] = logs[-MAX_LOG_ENTRIES:]
            if level == "error":
                state["lastError"] = message

        self._update_state(mutate)

    def _set_market(self, market: str, patch: dict[str, Any]) -> None:
        def mutate(state: dict[str, Any]) -> None:
            target = state["markets"].setdefault(market, default_market_state())
            target.update(patch)

        self._update_state(mutate)

    def _set_analysis(self, analysis: dict[str, Any], research: dict[str, Any]) -> None:
        def mutate(state: dict[str, Any]) -> None:
            state["analysis"] = deep_merge(state.get("analysis", {}), analysis)
            state["research"] = research

        self._update_state(mutate)

    def _touch_session(self, total_eq: Decimal) -> dict[str, Any]:
        today = datetime.now().date().isoformat()
        stamp = now_local_iso()

        def mutate(state: dict[str, Any]) -> None:
            if state.get("today") != today:
                state["today"] = today
                state["orderCountToday"] = 0
                state["sessionStartedAt"] = stamp
                state["sessionStartEq"] = decimal_to_str(total_eq)
                state["maxObservedEq"] = decimal_to_str(total_eq)
                state["equityCurve"] = []
            if not state.get("sessionStartedAt"):
                state["sessionStartedAt"] = stamp
            if not state.get("sessionStartEq"):
                state["sessionStartEq"] = decimal_to_str(total_eq)
            current_max = safe_decimal(state.get("maxObservedEq"), "0")
            if total_eq > current_max:
                state["maxObservedEq"] = decimal_to_str(total_eq)
            state["currentEq"] = decimal_to_str(total_eq)
            start_eq = safe_decimal(state.get("sessionStartEq"), "0")
            drawdown = Decimal("0")
            if start_eq > 0:
                drawdown = ((total_eq - start_eq) / start_eq) * Decimal("100")
            state["dailyDrawdownPct"] = decimal_to_str(drawdown)
            curve = state.setdefault("equityCurve", [])
            curve.append({"ts": stamp, "eq": decimal_to_str(total_eq)})
            state["equityCurve"] = curve[-120:]

        return self._update_state(mutate)

    def _increment_order_count(self, market: str, order_id: str, message: str) -> None:
        stamp = now_local_iso()

        def mutate(state: dict[str, Any]) -> None:
            state["orderCountToday"] = int(state.get("orderCountToday", 0)) + 1
            state["lastActionAt"] = stamp
            target = state["markets"].setdefault(market, default_market_state())
            target["lastActionAt"] = stamp
            target["lastTradeAt"] = stamp
            target["lastOrderId"] = order_id
            target["lastAction"] = message
            target["lastMessage"] = message

        self._update_state(mutate)

    def _cooldown_ready(self, market: str, cooldown_seconds: int) -> tuple[bool, str]:
        state = self.snapshot()["markets"].get(market, {})
        last_trade = parse_iso(state.get("lastTradeAt", ""))
        if not last_trade or cooldown_seconds <= 0:
            return True, ""
        delta = (datetime.now() - last_trade).total_seconds()
        if delta >= cooldown_seconds:
            return True, ""
        return False, f"冷却中，还需 {int(cooldown_seconds - delta)} 秒"

    def _guard_live_mode(self, automation: dict[str, Any], api_config: dict[str, Any], *, autostart: bool) -> None:
        if api_config.get("simulated"):
            return
        if autostart and not automation.get("allowLiveAutostart"):
            raise OkxApiError("当前是实盘，未开启“允许实盘自动启动”")
        if not automation.get("allowLiveTrading"):
            raise OkxApiError("当前是实盘，未开启“允许实盘自动交易”")

    def _prepare_swap(self, client: OkxClient, automation: dict[str, Any]) -> None:
        if not automation.get("swapEnabled"):
            return
        if automation.get("enforceNetMode"):
            account_config = client.get_account_config()
            account_row = (account_config.get("data") or [{}])[0]
            pos_mode = account_row.get("posMode", "")
            if pos_mode != "net_mode":
                client.set_position_mode("net_mode")
                self._log("info", "已尝试切换持仓模式到 net_mode")
        client.set_leverage(
            automation["swapInstId"],
            str(automation["swapLeverage"]),
            automation["swapTdMode"],
        )
        self._set_market("swap", {"prepared": True})

    def start(self, *, autostart: bool = False) -> None:
        with self.lock:
            if self.thread and self.thread.is_alive():
                return
            api_config = self.config_store.current()
            valid, message = validate_config(api_config)
            if not valid:
                raise OkxApiError(message)
            ensure_live_route_ready(api_config, force=True)
            ok, error, automation = validate_automation_config(self.automation_store.current())
            if not ok:
                raise OkxApiError(error)
            self._guard_live_mode(automation, api_config, autostart=autostart)
            client = OkxClient(api_config)
            client.get_account_balance()
            self._prepare_swap(client, automation)
            self.stop_event = threading.Event()
            self._update_state(
                lambda state: state.update(
                    {
                        "running": True,
                        "statusText": "运行中",
                        "modeText": (
                            f"{strategy_label(automation.get('strategyPreset', 'dual_engine'))} · "
                            + ("模拟盘自动交易" if api_config.get("simulated") else "实盘自动交易")
                        ),
                        "lastError": "",
                        "consecutiveErrors": 0,
                    }
                )
            )
            self.thread = threading.Thread(target=self._run_loop, daemon=True)
            self.thread.start()
            self._log(
                "info",
                "自动量化已启动，策略会按轮询周期检查信号并执行风控。",
            )

    def stop(self, reason: str = "已手动停止") -> None:
        with self.lock:
            self.stop_event.set()
            thread = self.thread
            self.thread = None
        if thread and thread.is_alive() and thread is not threading.current_thread():
            thread.join(timeout=1.5)
        self._update_state(
            lambda state: state.update(
                {
                    "running": False,
                    "statusText": reason,
                }
            )
        )
        self._log("info", reason)

    def run_once(self) -> None:
        api_config = self.config_store.current()
        valid, message = validate_config(api_config)
        if not valid:
            raise OkxApiError(message)
        ensure_live_route_ready(api_config, force=True)
        ok, error, automation = validate_automation_config(self.automation_store.current())
        if not ok:
            raise OkxApiError(error)
        self._guard_live_mode(automation, api_config, autostart=False)
        client = OkxClient(api_config)
        self._prepare_swap(client, automation)
        self._run_cycle(client, automation)

    def _run_loop(self) -> None:
        while not self.stop_event.is_set():
            api_config = self.config_store.current()
            ok, error, automation = validate_automation_config(self.automation_store.current())
            if not ok:
                self._log("error", error)
                self.stop("自动量化已停止：参数不合法")
                return
            try:
                ensure_live_route_ready(api_config, force=False)
                client = OkxClient(api_config)
                self._run_cycle(client, automation)
                self._update_state(lambda state: state.update({"consecutiveErrors": 0}))
            except Exception as exc:
                message = str(exc)
                state = self._update_state(
                    lambda current: current.update(
                        {"consecutiveErrors": int(current.get("consecutiveErrors", 0)) + 1}
                    )
                )
                self._log("error", message)
                if int(state.get("consecutiveErrors", 0)) >= 5:
                    self.stop("自动量化已停止：连续错误过多")
                    return
            wait_seconds = max(5, int(automation.get("pollSeconds", 20)))
            if self.stop_event.wait(wait_seconds):
                return

    def _run_cycle(self, client: OkxClient, automation: dict[str, Any]) -> None:
        cycle_started = time.perf_counter()
        analysis_bundle = build_execution_analysis(automation, client)
        analysis = {
            key: value
            for key, value in analysis_bundle.items()
            if key not in {"selectedConfig", "research"}
        }
        research = analysis_bundle["research"]
        effective_automation = deep_merge(automation, analysis_bundle.get("selectedConfig") or {})
        allow_new_entries = bool(analysis_bundle.get("allowNewEntries"))
        self._set_analysis(analysis, research)
        self._log(
            "info",
            f"联网分析完成：{analysis.get('selectedStrategyName', '未选策略')} · {analysis.get('decisionLabel', '待分析')}",
        )
        balance_snapshot = parse_balance_snapshot(client.get_account_balance())
        total_eq = safe_decimal(balance_snapshot["summary"].get("totalEq"), "0")
        state = self._touch_session(total_eq)
        max_daily_loss = safe_decimal(effective_automation.get("maxDailyLossPct"), "0")
        if max_daily_loss > 0 and safe_decimal(state.get("dailyDrawdownPct"), "0") <= -max_daily_loss:
            self.stop("自动量化已停止：超过日内最大回撤")
            return
        if int(state.get("orderCountToday", 0)) >= int(effective_automation.get("maxOrdersPerDay", 20)):
            self.stop("自动量化已停止：达到今日最大下单次数")
            return

        if effective_automation.get("spotEnabled"):
            self._run_spot_cycle(client, effective_automation, balance_snapshot, allow_new_entries, analysis.get("decisionLabel", "观察为主"))
        else:
            self._set_market(
                "spot",
                {
                    "enabled": False,
                    "instId": effective_automation.get("spotInstId", ""),
                    "lastMessage": "现货策略未启用",
                },
            )

        if effective_automation.get("swapEnabled"):
            self._run_swap_cycle(client, effective_automation, allow_new_entries, analysis.get("decisionLabel", "观察为主"))
        else:
            self._set_market(
                "swap",
                {
                    "enabled": False,
                    "instId": effective_automation.get("swapInstId", ""),
                    "lastMessage": "永续策略未启用",
                },
            )

        self._update_state(
            lambda current: current.update(
                {
                    "lastCycleAt": now_local_iso(),
                    "lastCycleDurationMs": int((time.perf_counter() - cycle_started) * 1000),
                    "statusText": "运行中" if allow_new_entries else analysis.get("decisionLabel", "观察中"),
                    "lastError": "",
                    "modeText": (
                        f"{analysis.get('selectedStrategyName', strategy_label(effective_automation.get('strategyPreset', 'dual_engine')))} · "
                        + analysis.get("decisionLabel", "待分析")
                    ),
                }
            )
        )

    def _run_spot_cycle(
        self,
        client: OkxClient,
        automation: dict[str, Any],
        balance_snapshot: dict[str, Any],
        allow_new_entries: bool,
        analysis_label: str,
    ) -> None:
        inst_id = automation["spotInstId"]
        meta = get_instrument_meta(client, "SPOT", inst_id)
        candles = get_closed_candles(
            client,
            inst_id,
            automation["bar"],
            max(int(automation["slowEma"]) + 30, 80),
        )
        signal = build_signal(candles, int(automation["fastEma"]), int(automation["slowEma"]))
        base_ccy = inst_id.split("-")[0]
        balance_row = find_balance_detail(balance_snapshot, base_ccy)
        base_size = safe_decimal(
            balance_row.get("availBal") or balance_row.get("cashBal") or balance_row.get("eq"),
            "0",
        )
        min_size = safe_decimal(meta.get("minSz") or meta.get("lotSz"), "0.00000001")
        lot_size = safe_decimal(meta.get("lotSz") or meta.get("minSz"), "0.00000001")
        last_price = safe_decimal(signal["lastClose"])
        notional = base_size * last_price
        active = base_size >= min_size and notional >= Decimal("5")
        market_state = self.snapshot()["markets"].get("spot", {})
        entry_price = safe_decimal(market_state.get("entryPrice"), "0")

        patch = {
            "enabled": True,
            "instId": inst_id,
            "signal": signal["signal"],
            "trend": signal["trend"],
            "lastPrice": signal["lastClose"],
            "positionSide": "long" if active else "flat",
            "positionSize": decimal_to_str(base_size),
            "positionNotional": decimal_to_str(notional),
            "lastMessage": f"现货 {signal['trend']} · 信号 {signal['signal']}",
        }
        if active and entry_price <= 0:
            patch["entryPrice"] = signal["lastClose"]
        self._set_market("spot", patch)

        cooldown_ready, reason = self._cooldown_ready("spot", int(automation["cooldownSeconds"]))
        stop_loss = safe_decimal(automation.get("stopLossPct"), "0")
        take_profit = safe_decimal(automation.get("takeProfitPct"), "0")
        if active and entry_price > 0:
            if stop_loss > 0 and last_price <= entry_price * (Decimal("1") - stop_loss / Decimal("100")):
                if cooldown_ready:
                    sell_size = round_down(base_size, lot_size)
                    if sell_size >= min_size:
                        self._place_spot_order(client, inst_id, "sell", sell_size, "止损离场")
                else:
                    self._set_market("spot", {"lastMessage": f"现货止损触发，但{reason}"})
                return
            if take_profit > 0 and last_price >= entry_price * (Decimal("1") + take_profit / Decimal("100")):
                if cooldown_ready:
                    sell_size = round_down(base_size, lot_size)
                    if sell_size >= min_size:
                        self._place_spot_order(client, inst_id, "sell", sell_size, "止盈离场")
                else:
                    self._set_market("spot", {"lastMessage": f"现货止盈触发，但{reason}"})
                return

        if signal["signal"] == "bull_cross" and not active:
            if not allow_new_entries:
                self._set_market("spot", {"lastMessage": f"现货金叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开仓"})
                return
            if not cooldown_ready:
                self._set_market("spot", {"lastMessage": f"现货买入信号出现，但{reason}"})
                return
            quote_budget = safe_decimal(automation.get("spotQuoteBudget"), "0")
            max_exposure = safe_decimal(automation.get("spotMaxExposure"), "0")
            if quote_budget <= 0:
                self._set_market("spot", {"lastMessage": "现货预算为 0，已跳过买入"})
                return
            if max_exposure > 0 and notional >= max_exposure:
                self._set_market("spot", {"lastMessage": "现货仓位已达到上限，跳过加仓"})
                return
            self._place_spot_order(client, inst_id, "buy", quote_budget, "金叉开仓")
            self._set_market("spot", {"entryPrice": signal["lastClose"]})
            return

        if signal["signal"] == "bear_cross" and active:
            if not cooldown_ready:
                self._set_market("spot", {"lastMessage": f"现货卖出信号出现，但{reason}"})
                return
            sell_size = round_down(base_size, lot_size)
            if sell_size >= min_size:
                self._place_spot_order(client, inst_id, "sell", sell_size, "死叉离场")

    def _run_swap_cycle(
        self,
        client: OkxClient,
        automation: dict[str, Any],
        allow_new_entries: bool,
        analysis_label: str,
    ) -> None:
        inst_id = automation["swapInstId"]
        meta = get_instrument_meta(client, "SWAP", inst_id)
        candles = get_closed_candles(
            client,
            inst_id,
            automation["bar"],
            max(int(automation["slowEma"]) + 30, 80),
        )
        signal = build_signal(candles, int(automation["fastEma"]), int(automation["slowEma"]))
        positions = client.get_positions(inst_id).get("data", [])
        open_position = next(
            (row for row in positions if safe_decimal(row.get("pos"), "0") != 0),
            {},
        )
        pos_value = safe_decimal(open_position.get("pos"), "0")
        abs_pos = abs(pos_value)
        entry_price = safe_decimal(open_position.get("avgPx"), "0")
        last_price = safe_decimal(signal["lastClose"])
        tick_size = safe_decimal(meta.get("tickSz"), "0.1")
        lot_size = safe_decimal(meta.get("lotSz"), "1")

        position_side = "flat"
        if pos_value > 0:
            position_side = "long"
        elif pos_value < 0:
            position_side = "short"

        self._set_market(
            "swap",
            {
                "enabled": True,
                "instId": inst_id,
                "signal": signal["signal"],
                "trend": signal["trend"],
                "lastPrice": signal["lastClose"],
                "positionSide": position_side,
                "positionSize": decimal_to_str(abs_pos),
                "positionNotional": decimal_to_str(abs_pos * last_price),
                "entryPrice": decimal_to_str(entry_price) if entry_price > 0 else "",
                "lastMessage": f"永续 {signal['trend']} · 信号 {signal['signal']}",
            },
        )

        cooldown_ready, reason = self._cooldown_ready("swap", int(automation["cooldownSeconds"]))
        stop_loss = safe_decimal(automation.get("stopLossPct"), "0")
        take_profit = safe_decimal(automation.get("takeProfitPct"), "0")
        if position_side == "long" and entry_price > 0:
            if stop_loss > 0 and last_price <= entry_price * (Decimal("1") - stop_loss / Decimal("100")):
                if cooldown_ready:
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        round_down(abs_pos, lot_size),
                        automation["swapTdMode"],
                        "永续多单止损",
                        reduce_only=True,
                    )
                else:
                    self._set_market("swap", {"lastMessage": f"永续多单止损触发，但{reason}"})
                return
            if take_profit > 0 and last_price >= entry_price * (Decimal("1") + take_profit / Decimal("100")):
                if cooldown_ready:
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        round_down(abs_pos, lot_size),
                        automation["swapTdMode"],
                        "永续多单止盈",
                        reduce_only=True,
                    )
                else:
                    self._set_market("swap", {"lastMessage": f"永续多单止盈触发，但{reason}"})
                return

        if position_side == "short" and entry_price > 0:
            if stop_loss > 0 and last_price >= entry_price * (Decimal("1") + stop_loss / Decimal("100")):
                if cooldown_ready:
                    self._place_swap_order(
                        client,
                        inst_id,
                        "buy",
                        round_down(abs_pos, lot_size),
                        automation["swapTdMode"],
                        "永续空单止损",
                        reduce_only=True,
                    )
                else:
                    self._set_market("swap", {"lastMessage": f"永续空单止损触发，但{reason}"})
                return
            if take_profit > 0 and last_price <= entry_price * (Decimal("1") - take_profit / Decimal("100")):
                if cooldown_ready:
                    self._place_swap_order(
                        client,
                        inst_id,
                        "buy",
                        round_down(abs_pos, lot_size),
                        automation["swapTdMode"],
                        "永续空单止盈",
                        reduce_only=True,
                    )
                else:
                    self._set_market("swap", {"lastMessage": f"永续空单止盈触发，但{reason}"})
                return

        trade_contracts = round_down(safe_decimal(automation.get("swapContracts"), "0"), lot_size)
        if trade_contracts <= 0:
            self._set_market("swap", {"lastMessage": "永续张数为 0，已停止发单"})
            return

        mode = automation["swapStrategyMode"]
        if signal["signal"] == "bull_cross":
            if not cooldown_ready:
                self._set_market("swap", {"lastMessage": f"永续金叉出现，但{reason}"})
                return
            if mode == "long_only":
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market("swap", {"lastMessage": f"永续金叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "buy",
                        trade_contracts,
                        automation["swapTdMode"],
                        "永续金叉开多",
                    )
                elif position_side == "short":
                    close_size = round_down(abs_pos, lot_size)
                    if close_size > 0:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "buy",
                            close_size,
                            automation["swapTdMode"],
                            "永续空单回补",
                            reduce_only=True,
                        )
            elif mode == "short_only":
                if position_side == "short":
                    close_size = round_down(abs_pos, lot_size)
                    if close_size > 0:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "buy",
                            close_size,
                            automation["swapTdMode"],
                            "永续空单回补",
                            reduce_only=True,
                        )
                elif position_side == "long":
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        round_down(abs_pos, lot_size),
                        automation["swapTdMode"],
                        "永续异常多单平仓",
                        reduce_only=True,
                    )
            else:
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market("swap", {"lastMessage": f"永续金叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "buy",
                        trade_contracts,
                        automation["swapTdMode"],
                        "永续金叉开多",
                    )
                elif position_side == "short":
                    close_size = round_down(abs_pos, lot_size)
                    if close_size > 0:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "buy",
                            close_size,
                            automation["swapTdMode"],
                            "永续空单回补",
                            reduce_only=True,
                        )
                    if allow_new_entries:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "buy",
                            round_down(trade_contracts, lot_size),
                            automation["swapTdMode"],
                            "永续金叉翻多",
                        )
            return

        if signal["signal"] == "bear_cross":
            if not cooldown_ready:
                self._set_market("swap", {"lastMessage": f"永续死叉出现，但{reason}"})
                return
            if mode == "long_only":
                if position_side == "long":
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        round_down(abs_pos, lot_size),
                        automation["swapTdMode"],
                        "永续死叉平多",
                        reduce_only=True,
                    )
            elif mode == "short_only":
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market("swap", {"lastMessage": f"永续死叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开空仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        trade_contracts,
                        automation["swapTdMode"],
                        "永续死叉开空",
                    )
                elif position_side == "long":
                    close_size = round_down(abs_pos, lot_size)
                    if close_size > 0:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "sell",
                            close_size,
                            automation["swapTdMode"],
                            "永续异常多单回吐",
                            reduce_only=True,
                        )
                    if allow_new_entries:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "sell",
                            round_down(trade_contracts, lot_size),
                            automation["swapTdMode"],
                            "永续死叉翻空",
                        )
            else:
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market("swap", {"lastMessage": f"永续死叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开空仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        trade_contracts,
                        automation["swapTdMode"],
                        "永续死叉开空",
                    )
                elif position_side == "long":
                    close_size = round_down(abs_pos, lot_size)
                    if close_size > 0:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "sell",
                            close_size,
                            automation["swapTdMode"],
                            "永续多单回吐",
                            reduce_only=True,
                        )
                    if allow_new_entries:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "sell",
                            round_down(trade_contracts, lot_size),
                            automation["swapTdMode"],
                            "永续死叉翻空",
                        )

    def _place_spot_order(self, client: OkxClient, inst_id: str, side: str, size: Decimal, reason: str) -> None:
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": side,
            "ordType": "market",
            "clOrdId": build_cl_ord_id("s"),
        }
        if side == "buy":
            payload["sz"] = decimal_to_str(size)
            payload["tgtCcy"] = "quote_ccy"
        else:
            payload["sz"] = decimal_to_str(size)
        result = client.place_order(payload)
        order = (result.get("data") or [{}])[0]
        order_id = order.get("ordId", "")
        self._increment_order_count("spot", order_id, reason)
        self._log("info", f"{reason} · 现货 {inst_id} 已发单")

    def _place_swap_order(
        self,
        client: OkxClient,
        inst_id: str,
        side: str,
        size: Decimal,
        td_mode: str,
        reason: str,
        *,
        reduce_only: bool = False,
    ) -> None:
        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "market",
            "sz": decimal_to_str(size),
            "clOrdId": build_cl_ord_id("w"),
        }
        if reduce_only:
            payload["reduceOnly"] = True
        result = client.place_order(payload)
        order = (result.get("data") or [{}])[0]
        order_id = order.get("ordId", "")
        self._increment_order_count("swap", order_id, reason)
        self._log("info", f"{reason} · 永续 {inst_id} 已发单")


CONFIG = ConfigStore(CONFIG_PATH)
AUTOMATION_CONFIG = JsonStore(AUTOMATION_CONFIG_PATH, default_automation_config)
AUTOMATION_STATE = JsonStore(AUTOMATION_STATE_PATH, default_automation_state)
LOCAL_ORDER_STORE = JsonStore(LOCAL_ORDER_STATE_PATH, default_local_order_state)
reconcile_automation_state_from_markets()
MINER_CONFIG = JsonStore(MINER_CONFIG_PATH, default_miner_config)
MINER_STATE = JsonStore(MINER_STATE_PATH, default_miner_state)
MAC_LOTTO = MacLottoManager()
AUTOMATION_ENGINE = AutomationEngine(CONFIG, AUTOMATION_CONFIG, AUTOMATION_STATE)
PRIVATE_ORDER_STREAM = OkxPrivateOrderStream()


def is_disconnect_error(exc: BaseException | None) -> bool:
    if exc is None:
        return False
    if isinstance(exc, (BrokenPipeError, ConnectionResetError, ConnectionAbortedError)):
        return True
    if isinstance(exc, OSError):
        return exc.errno in {errno.EPIPE, errno.ECONNRESET, errno.ECONNABORTED}
    return False


def write_http_response(
    handler: BaseHTTPRequestHandler,
    *,
    status: int,
    content_type: str,
    raw: bytes,
) -> None:
    try:
        handler.send_response(status)
        handler.send_header("Content-Type", content_type)
        handler.send_header("Content-Length", str(len(raw)))
        handler.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
        handler.send_header("Pragma", "no-cache")
        handler.send_header("Expires", "0")
        handler.end_headers()
        handler.wfile.write(raw)
    except BaseException as exc:
        if is_disconnect_error(exc):
            handler.close_connection = True
            return
        raise


def json_response(
    handler: BaseHTTPRequestHandler, payload: dict[str, Any], status: int = 200
) -> None:
    raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    write_http_response(
        handler,
        status=status,
        content_type="application/json; charset=utf-8",
        raw=raw,
    )


def error_response(
    handler: BaseHTTPRequestHandler, message: str, status: int = 400
) -> None:
    json_response(handler, {"ok": False, "error": message}, status=status)


def read_raw_request(handler: BaseHTTPRequestHandler) -> bytes:
    try:
        length = int(handler.headers.get("Content-Length") or 0)
    except (TypeError, ValueError):
        length = 0
    if length <= 0:
        return b""
    return handler.rfile.read(length)


def relay_requests_response(handler: BaseHTTPRequestHandler, response: requests.Response) -> None:
    content_type = response.headers.get("Content-Type") or "application/json; charset=utf-8"
    write_http_response(
        handler,
        status=response.status_code,
        content_type=content_type,
        raw=response.content,
    )


class AppHandler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_HEAD(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        relative_path = "index.html" if path in ("", "/") else path.lstrip("/")
        candidate = (STATIC_DIR / relative_path).resolve()
        if not str(candidate).startswith(str(STATIC_DIR.resolve())) or not candidate.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        content_type = "text/plain; charset=utf-8"
        if candidate.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif candidate.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif candidate.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"

        try:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(candidate.stat().st_size))
            self.send_header("Cache-Control", "no-store, no-cache, must-revalidate, max-age=0")
            self.send_header("Pragma", "no-cache")
            self.send_header("Expires", "0")
            self.end_headers()
        except BaseException as exc:
            if is_disconnect_error(exc):
                self.close_connection = True
                return
            raise

    def do_GET(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        if not enforce_gateway_auth(self, path):
            return
        config = CONFIG.current()

        if path != "/api/automation/config" and should_proxy_to_remote(config, path):
            try:
                if path in ("/api/health", "/api/focus-snapshot"):
                    response = remote_gateway_request(config, "GET", self.path)
                    payload = response.json()
                    if path == "/api/focus-snapshot":
                        payload["minerOverview"] = miner_focus_overview(MINER_CONFIG.current())
                    payload["executionMode"] = "remote"
                    payload["remoteGatewayUrl"] = remote_gateway_url(config)
                    json_response(self, payload, status=response.status_code)
                else:
                    response = remote_gateway_request(config, "GET", self.path)
                    relay_requests_response(self, response)
            except Exception as exc:
                error_response(self, f"远端执行节点请求失败: {exc}", status=502)
            return

        if path == "/api/health":
            route_health = {}
            if is_remote_execution_enabled(config):
                route_health = remote_node_health(config)
            elif config.get("apiKey") and config.get("passphrase"):
                try:
                    route_health = evaluate_okx_route_health(config, force=False)
                except Exception as exc:
                    route_health = {"healthy": False, "status": "probe_failed", "detail": str(exc)}
            json_response(
                self,
                {
                    "ok": True,
                    "service": "okx-local-app",
                    "privateOrderStream": PRIVATE_ORDER_STREAM.snapshot(),
                    "okxRoute": route_health,
                },
            )
            return

        if path == "/api/ping":
            json_response(
                self,
                {
                    "ok": True,
                    "service": "okx-local-app",
                    "executionMode": config.get("executionMode") or "local",
                },
            )
            return

        if path == "/api/local-config":
            json_response(self, {"ok": True, "config": CONFIG.redacted()})
            return

        if path == "/api/config":
            local_config = CONFIG.current()
            local_redacted = CONFIG.redacted()
            if is_remote_execution_enabled(local_config):
                try:
                    response = remote_gateway_request(
                        local_config,
                        "GET",
                        self.path,
                        timeout=REMOTE_CONFIG_FETCH_TIMEOUT,
                    )
                    remote_payload = response.json()
                    merged = merge_remote_redacted_config(local_redacted, remote_payload)
                    json_response(
                        self,
                        {
                            "ok": bool(remote_payload.get("ok", True)),
                            "config": merged,
                            "remoteConfigLoaded": True,
                        },
                        status=response.status_code,
                    )
                except Exception as exc:
                    json_response(
                        self,
                        {
                            "ok": True,
                            "config": local_redacted,
                            "remoteConfigLoaded": False,
                            "remoteConfigError": str(exc),
                        },
                    )
                return
            json_response(self, {"ok": True, "config": local_redacted})
            return

        if path == "/api/automation/config":
            if is_remote_execution_enabled(config):
                try:
                    response = remote_gateway_request(
                        config,
                        "GET",
                        self.path,
                        timeout=REMOTE_CONFIG_FETCH_TIMEOUT,
                    )
                    relay_requests_response(self, response)
                except Exception as exc:
                    json_response(
                        self,
                        {
                            "ok": True,
                            "config": AUTOMATION_CONFIG.current(),
                            "remoteConfigLoaded": False,
                            "remoteConfigError": str(exc),
                        },
                    )
                return
            json_response(self, {"ok": True, "config": AUTOMATION_CONFIG.current()})
            return

        if path == "/api/automation/state":
            json_response(self, {"ok": True, "state": AUTOMATION_ENGINE.snapshot()})
            return

        if path == "/api/focus-snapshot":
            payload: dict[str, Any] = {
                "ok": True,
                "automationState": AUTOMATION_ENGINE.snapshot(),
                "timestamp": int(time.time()),
            }

            valid, message = validate_config(config)
            miner_config = MINER_CONFIG.current()

            def load_account() -> dict[str, Any]:
                if not valid:
                    raise RuntimeError(message)
                def fetch() -> dict[str, Any]:
                    ensure_live_route_ready(config, force=False)
                    client = OkxClient(config)
                    snapshot = fetch_account_snapshot(client, include_positions=False)
                    return {
                        "summary": snapshot["summary"],
                        "fundingSummary": snapshot.get("fundingSummary", {}),
                        "balanceCount": snapshot.get("balanceCount", 0),
                        "positionCount": snapshot.get("positionCount", 0),
                    }

                data, error_text, cached = load_cached_focus_section("account", 12.0, fetch)
                if error_text and cached:
                    payload["accountWarning"] = f"账户快照沿用缓存: {error_text}"
                return data

            def load_miner() -> dict[str, Any]:
                return miner_focus_overview(miner_config)

            jobs = {
                "account": load_account,
                "minerOverview": load_miner,
            }
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                future_map = {
                    executor.submit(loader): key for key, loader in jobs.items()
                }
                for future in concurrent.futures.as_completed(future_map):
                    key = future_map[future]
                    try:
                        payload[key] = future.result()
                    except Exception as exc:
                        payload[f"{key}Error"] = str(exc)

            json_response(self, payload)
            return

        if path == "/api/miner/config":
            json_response(self, {"ok": True, "config": MINER_CONFIG.current()})
            return

        if path == "/api/miner/overview":
            config = MINER_CONFIG.current()
            public_client = build_public_client(CONFIG.current())
            try:
                overview = miner_overview(config, public_client)
                update_miner_state(overview)
                json_response(self, {"ok": True, "overview": overview})
            except Exception as exc:
                error_response(self, f"获取矿机概览失败: {exc}", status=502)
            return

        if path == "/api/miner/mac-lotto/state":
            json_response(self, {"ok": True, "state": MAC_LOTTO.snapshot(MINER_CONFIG.current())})
            return

        if path == "/api/account/overview":
            config = CONFIG.current()
            valid, message = validate_config(config)
            if not valid:
                error_response(self, message, status=400)
                return
            try:
                route = ensure_live_route_ready(config, force=False)
                client = OkxClient(config)
                snapshot = fetch_account_snapshot(client, include_positions=True)
                json_response(
                    self,
                    {
                        "ok": True,
                        "summary": snapshot["summary"],
                        "balances": snapshot["balances"],
                        "tradingBalances": snapshot.get("tradingBalances", []),
                        "fundingBalances": snapshot.get("fundingBalances", []),
                        "fundingSummary": snapshot.get("fundingSummary", {}),
                        "positions": snapshot["positions"],
                        "balanceCount": snapshot.get("balanceCount", 0),
                        "positionCount": snapshot.get("positionCount", 0),
                        "fundingWarning": snapshot.get("fundingWarning", ""),
                        "route": route,
                    },
                )
            except Exception as exc:
                error_response(self, f"获取账户数据失败: {exc}", status=502)
            return

        if path == "/api/orders/recent":
            config = CONFIG.current()
            valid, message = validate_config(config)
            if not valid:
                error_response(self, message, status=400)
                return
            inst_type = (query.get("instType") or [""])[0]
            cached_orders = get_local_recent_orders(inst_type, limit=20)
            stream_orders = PRIVATE_ORDER_STREAM.get_recent_orders(inst_type, limit=20)
            merged_live_orders = merge_order_feeds(stream_orders, cached_orders, limit=20)
            if merged_live_orders:
                json_response(
                    self,
                    {
                        "ok": True,
                        "orders": merged_live_orders,
                        "source": "private_ws" if stream_orders else "local_cache",
                        "stream": PRIVATE_ORDER_STREAM.snapshot(),
                    },
                )
                return
            try:
                ensure_live_route_ready(config, force=False)
                client = OkxClient(config)
                if inst_type:
                    result = client.get_recent_orders(inst_type)
                    json_response(self, {"ok": True, "orders": result.get("data", []), "source": "rest"})
                    return

                merged_orders: list[dict[str, Any]] = []
                errors: list[str] = []
                for fallback_type in ("SPOT", "SWAP"):
                    try:
                        fallback_result = client.get_recent_orders(fallback_type)
                        merged_orders.extend(fallback_result.get("data", []))
                    except Exception as fallback_exc:
                        errors.append(f"{fallback_type}: {fallback_exc}")

                if not merged_orders and errors:
                    raise OkxApiError("; ".join(errors))

                merged_orders.sort(
                    key=lambda item: int(item.get("uTime") or item.get("cTime") or 0),
                    reverse=True,
                )
                json_response(
                    self,
                    {
                        "ok": True,
                        "orders": merged_orders[:20],
                        "source": "rest_multi",
                    },
                )
            except Exception as exc:
                error_response(self, f"获取订单失败: {exc}", status=502)
            return

        if path == "/api/market/ticker":
            inst_id = (query.get("instId") or [""])[0]
            if not inst_id:
                error_response(self, "缺少 instId", status=400)
                return
            config = CONFIG.current()
            client = build_public_client(config)
            try:
                result = client.get_ticker(inst_id)
                json_response(self, {"ok": True, "ticker": result.get("data", [])})
            except Exception as exc:
                error_response(self, f"获取行情失败: {exc}", status=502)
            return

        if path == "/api/market/candles":
            inst_id = (query.get("instId") or [""])[0]
            bar = (query.get("bar") or ["5m"])[0]
            limit = int((query.get("limit") or ["120"])[0])
            if not inst_id:
                error_response(self, "缺少 instId", status=400)
                return
            config = CONFIG.current()
            client = build_public_client(config)
            try:
                result = client.get_history_candles(inst_id, bar, limit)
                json_response(self, {"ok": True, "candles": result.get("data", [])})
            except Exception as exc:
                error_response(self, f"获取K线失败: {exc}", status=502)
            return

        if path == "/":
            self._serve_file("index.html")
            return

        self._serve_file(path.lstrip("/"))

    def do_POST(self) -> None:
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if not enforce_gateway_auth(self, path):
            return
        config = CONFIG.current()
        config_state = CONFIG.snapshot()

        if path != "/api/config" and should_proxy_to_remote(config, path):
            try:
                raw = read_raw_request(self)
                content_type = str(self.headers.get("Content-Type") or "application/json; charset=utf-8")
                if path == "/api/config/test":
                    if raw:
                        payload = json.loads(raw.decode("utf-8"))
                    else:
                        payload = {}
                    proxy_config = build_proxy_runtime_config(config, payload)
                    remote_payload = build_remote_trading_config(config_state, payload, persist=False)
                    raw = json.dumps(remote_payload, ensure_ascii=False).encode("utf-8")
                    content_type = "application/json; charset=utf-8"
                else:
                    proxy_config = config
                response = remote_gateway_request(
                    proxy_config,
                    "POST",
                    self.path,
                    body=raw,
                    content_type=content_type,
                )
                relay_requests_response(self, response)
            except Exception as exc:
                error_response(self, f"远端执行节点请求失败: {exc}", status=502)
            return

        if path == "/api/config":
            payload = read_json_request(self)
            persist = bool(payload.pop("persist", False))
            target_mode = str(payload.get("executionMode") or config.get("executionMode") or "local").strip()
            if target_mode == "remote":
                local_runtime = build_local_runtime_config(config, payload)
                proxy_config = build_proxy_runtime_config(config, payload)
                valid, message = validate_config(local_runtime)
                if not valid:
                    error_response(self, message, status=400)
                    return
                CONFIG.save(local_runtime, persist=persist)
                remote_payload = build_remote_trading_config(config_state, payload, persist=persist)
                try:
                    response = remote_gateway_request(
                        proxy_config,
                        "POST",
                        self.path,
                        body=json.dumps(remote_payload, ensure_ascii=False).encode("utf-8"),
                        content_type="application/json; charset=utf-8",
                    )
                    remote_data = response.json()
                    merged = merge_remote_redacted_config(CONFIG.redacted(), remote_data)
                    reset_focus_cache("account", "orders")
                    PRIVATE_ORDER_STREAM.mark_dirty()
                    json_response(
                        self,
                        {
                            "ok": bool(remote_data.get("ok", response.ok)),
                            "config": merged,
                            "persisted": persist,
                            "remoteConfigSaved": True,
                        },
                        status=response.status_code,
                    )
                except Exception as exc:
                    json_response(
                        self,
                        {
                            "ok": False,
                            "error": f"远端执行节点保存失败: {exc}",
                            "config": CONFIG.redacted(),
                            "persisted": persist,
                            "remoteConfigSaved": False,
                        },
                        status=502,
                    )
                return

            payload = CONFIG.merged_with_existing_secrets(payload)
            valid, message = validate_config(payload)
            if not valid:
                error_response(self, message, status=400)
                return
            CONFIG.save(payload, persist=persist)
            reset_focus_cache("account", "orders")
            PRIVATE_ORDER_STREAM.mark_dirty()
            json_response(
                self,
                {"ok": True, "config": CONFIG.redacted(), "persisted": persist},
            )
            return

        if path == "/api/config/test":
            payload = read_json_request(self)
            config = CONFIG.merged_with_existing_secrets(payload or CONFIG.current())
            valid, message = validate_config(config)
            if not valid:
                error_response(self, message, status=400)
                return
            try:
                route = evaluate_okx_route_health(config, force=True)
                if not bool(config.get("simulated")) and not route.get("healthy"):
                    raise OkxApiError(f"测试失败: {route.get('summary') or '实盘不可用'}；{route.get('detail')}")
                client = OkxClient(config)
                result = parse_balance_snapshot(client.get_account_balance())
                json_response(
                    self,
                    {
                        "ok": True,
                        "message": (
                            "连接成功"
                            if route.get("healthy")
                            else f"{route.get('summary') or '模拟盘仅本地纸面可用'}；{route.get('detail')}"
                        ),
                        "sample": result["details"][:1],
                        "route": route,
                    },
                )
            except Exception as exc:
                error_response(
                    self,
                    explain_auth_error(f"测试失败: {exc}", config),
                    status=502,
                )
            return

        if path == "/api/automation/config":
            payload = read_json_request(self)
            ok, message, normalized = validate_automation_config(payload)
            if not ok:
                error_response(self, message, status=400)
                return
            AUTOMATION_CONFIG.replace(normalized)
            json_response(self, {"ok": True, "config": normalized})
            return

        if path == "/api/automation/analyze":
            try:
                payload = read_json_request(self) or {}
                automation_payload = payload.get("automation") or AUTOMATION_CONFIG.current()
                public_config = deep_merge(CONFIG.current(), payload.get("publicConfig") or {})
                ok, message, normalized = validate_automation_config(automation_payload)
                if not ok:
                    error_response(self, message, status=400)
                    return
                analysis_bundle = build_execution_analysis(normalized, build_public_client(public_config))
                analysis = {
                    key: value
                    for key, value in analysis_bundle.items()
                    if key not in {"selectedConfig", "research"}
                }
                AUTOMATION_STATE.update(
                    lambda current: current.update(
                        {
                            "analysis": analysis,
                            "research": analysis_bundle["research"],
                        }
                    )
                )
                json_response(self, {"ok": True, "analysis": analysis, "research": analysis_bundle["research"]})
            except Exception as exc:
                error_response(self, f"联网预检失败: {exc}", status=502)
            return

        if path == "/api/automation/research/backtest":
            payload = read_json_request(self)
            config_payload, options = normalize_research_options(payload)
            ok, message, normalized = validate_automation_config(config_payload)
            if not ok:
                error_response(self, message, status=400)
                return
            try:
                research = research_backtest(normalized, options, build_public_client(CONFIG.current()))
                AUTOMATION_STATE.update(lambda current: current.update({"research": research}))
                json_response(self, {"ok": True, "research": research})
            except Exception as exc:
                error_response(self, f"回测失败: {exc}", status=502)
            return

        if path == "/api/automation/research/optimize":
            payload = read_json_request(self)
            config_payload, options = normalize_research_options(payload)
            ok, message, normalized = validate_automation_config(config_payload)
            if not ok:
                error_response(self, message, status=400)
                return
            try:
                research = research_optimize(normalized, options, build_public_client(CONFIG.current()))
                AUTOMATION_STATE.update(lambda current: current.update({"research": research}))
                json_response(self, {"ok": True, "research": research})
            except Exception as exc:
                error_response(self, f"自动优化失败: {exc}", status=502)
            return

        if path == "/api/automation/research/export":
            payload = read_json_request(self)
            export_index = payload.get("index")
            try:
                index = int(export_index) if export_index not in (None, "") else None
            except (TypeError, ValueError):
                error_response(self, "导出索引格式不正确", status=400)
                return
            try:
                export_info = export_research_pack(
                    AUTOMATION_ENGINE.snapshot().get("research") or {},
                    AUTOMATION_CONFIG.current(),
                    index=index,
                )
                json_response(self, {"ok": True, "export": export_info})
            except Exception as exc:
                error_response(self, f"导出策略失败: {exc}", status=502)
            return

        if path == "/api/miner/config":
            payload = read_json_request(self)
            normalized = deep_merge(default_miner_config(), payload)
            MINER_CONFIG.replace(normalized)
            if MAC_LOTTO._should_autostart(normalized):
                try:
                    MAC_LOTTO.start(normalized)
                except Exception as exc:
                    append_miner_log("error", f"保存矿机配置后自动启动失败: {exc}")
            json_response(self, {"ok": True, "config": normalized})
            return

        if path == "/api/miner/bitaxe-action":
            payload = read_json_request(self)
            host = str(payload.get("host", "")).strip()
            action = str(payload.get("action", "")).strip()
            if action not in ("identify", "restart", "pause", "resume"):
                error_response(self, "Bitaxe 动作仅支持 identify / restart / pause / resume", status=400)
                return
            try:
                result = post_bitaxe_action(host, action)
                json_response(self, {"ok": True, "result": result})
            except Exception as exc:
                error_response(self, f"Bitaxe 动作失败: {exc}", status=502)
            return

        if path == "/api/miner/mac-lotto/start":
            try:
                config = MINER_CONFIG.update(lambda current: current.update({"autoStartMacLotto": True}))
                state = MAC_LOTTO.start(config)
                json_response(self, {"ok": True, "state": state})
            except Exception as exc:
                error_response(self, f"启动 Mac 乐透机失败: {exc}", status=400)
            return

        if path == "/api/miner/mac-lotto/stop":
            config = MINER_CONFIG.update(lambda current: current.update({"autoStartMacLotto": False}))
            state = MAC_LOTTO.stop(config)
            json_response(self, {"ok": True, "state": state})
            return

        if path == "/api/automation/start":
            try:
                AUTOMATION_ENGINE.start(autostart=False)
                json_response(self, {"ok": True, "state": AUTOMATION_ENGINE.snapshot()})
            except Exception as exc:
                error_response(self, f"启动失败: {exc}", status=400)
            return

        if path == "/api/automation/stop":
            AUTOMATION_ENGINE.stop("已手动停止")
            json_response(self, {"ok": True, "state": AUTOMATION_ENGINE.snapshot()})
            return

        if path == "/api/automation/run-once":
            try:
                AUTOMATION_ENGINE.run_once()
                json_response(self, {"ok": True, "state": AUTOMATION_ENGINE.snapshot()})
            except Exception as exc:
                error_response(self, f"执行失败: {exc}", status=400)
            return

        if path == "/api/order/place":
            payload = read_json_request(self)
            valid, message = validate_config(config)
            if not valid:
                error_response(self, message, status=400)
                return

            required = ["instId", "tdMode", "side", "ordType", "sz"]
            missing = [key for key in required if not payload.get(key)]
            if missing:
                error_response(self, f"订单缺少字段: {', '.join(missing)}", status=400)
                return

            if payload.get("ordType") == "limit" and not payload.get("px"):
                error_response(self, "限价单必须填写价格 px", status=400)
                return

            order = {
                key: value for key, value in payload.items() if value not in (None, "", False)
            }
            try:
                route = ensure_live_route_ready(config, force=False)
                client = OkxClient(config)
                started_at = time.perf_counter()
                result = client.place_order(order)
                placed_orders = list(result.get("data") or [])
                PRIVATE_ORDER_STREAM._ingest_orders(placed_orders)
                if placed_orders:
                    record_manual_order_activity(placed_orders[0])
                elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
                json_response(self, {"ok": True, "result": result, "elapsedMs": elapsed_ms, "route": route})
            except Exception as exc:
                error_response(self, f"下单失败: {exc}", status=502)
            return

        error_response(self, "未找到接口", status=404)

    def _serve_file(self, relative_path: str) -> None:
        if relative_path in ("", "/"):
            relative_path = "index.html"
        candidate = (STATIC_DIR / relative_path).resolve()
        if not str(candidate).startswith(str(STATIC_DIR.resolve())) or not candidate.exists():
            self.send_error(HTTPStatus.NOT_FOUND, "Not found")
            return

        content_type = "text/plain; charset=utf-8"
        if candidate.suffix == ".html":
            content_type = "text/html; charset=utf-8"
        elif candidate.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif candidate.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"

        raw = candidate.read_bytes()
        write_http_response(
            self,
            status=200,
            content_type=content_type,
            raw=raw,
        )


class QuietThreadingHTTPServer(ThreadingHTTPServer):
    def handle_error(self, request, client_address) -> None:  # type: ignore[override]
        exc_type, exc, _ = sys.exc_info()
        if is_disconnect_error(exc):
            return
        super().handle_error(request, client_address)


def maybe_autostart() -> None:
    automation = AUTOMATION_CONFIG.current()
    if not automation.get("autostart"):
        return
    config = CONFIG.current()
    valid, _ = validate_config(config)
    if not valid:
        return
    try:
        AUTOMATION_ENGINE.start(autostart=True)
    except Exception as exc:
        AUTOMATION_ENGINE._log("error", f"自动启动失败: {exc}")


def maybe_autostart_miner() -> None:
    config = MINER_CONFIG.current()
    MAC_LOTTO.ensure_supervisor()
    if not MAC_LOTTO._should_autostart(config):
        return
    try:
        MAC_LOTTO.start(config)
        append_miner_log("info", "Mac 本机乐透机已随桌面服务自动启动。")
    except Exception as exc:
        append_miner_log("error", f"Mac 本机乐透机自动启动失败: {exc}")


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ensure_private_permissions(DATA_DIR, is_dir=True)
    maybe_autostart()
    maybe_autostart_miner()
    ensure_focus_warmer()
    PRIVATE_ORDER_STREAM.ensure_running()
    try:
        httpd = QuietThreadingHTTPServer((HOST, PORT), AppHandler)
    except OSError as exc:
        if exc.errno == 48:
            raise RuntimeError(
                f"Local port {PORT} is already in use. Refusing to auto-shift to a different port."
            ) from exc
        raise
    print(f"OKX Local App running at http://{HOST}:{PORT}", flush=True)
    httpd.serve_forever()


if __name__ == "__main__":
    main()
