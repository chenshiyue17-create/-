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
from decimal import Decimal, InvalidOperation, ROUND_DOWN, ROUND_UP
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
SPOT_SELL_PROTECTION_PCT = Decimal("2.0")
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
BASIS_ARB_SCAN_SYMBOL_LIMIT = 24
BASIS_ARB_PREFERRED_SYMBOLS = (
    "BTC",
    "ETH",
    "SOL",
    "DOGE",
    "XRP",
    "ADA",
    "SUI",
    "TRX",
    "LINK",
    "AVAX",
    "LTC",
    "BCH",
    "TON",
    "ARB",
    "PEPE",
    "APT",
    "DOT",
    "NEAR",
    "ETC",
    "FIL",
    "ATOM",
    "WLD",
    "OP",
    "INJ",
)
BASIS_ARB_SCAN_LOCK = threading.RLock()
BASIS_ARB_MARKET_UNIVERSE_CACHE: dict[str, Any] = {"ts": 0.0, "symbols": []}
BASIS_ARB_MARKET_SCAN_CACHE: dict[str, Any] = {"ts": 0.0, "key": "", "rows": []}
DIP_SWING_SCAN_SYMBOL_LIMIT = 64
DIP_SWING_EXECUTION_TARGET_LIMIT = 64
DIP_SWING_WATCHLIST_LIMIT = 64
DIP_SWING_SCAN_WORKER_LIMIT = 24
DIP_SWING_SCAN_LOCK = threading.RLock()
DIP_SWING_MARKET_UNIVERSE_CACHE: dict[str, Any] = {"ts": 0.0, "symbols": []}
DIP_SWING_MARKET_SCAN_CACHE: dict[str, Any] = {"ts": 0.0, "key": "", "rows": []}
OKX_FEE_RATE_CACHE_LOCK = threading.RLock()
OKX_FEE_RATE_CACHE: dict[str, Any] = {}
REMOTE_AUTOMATION_CONFIG_LOCK = threading.RLock()
REMOTE_AUTOMATION_CONFIG_CACHE: dict[str, Any] = {"ts": 0.0, "url": "", "config": {}}
REMOTE_AUTOMATION_STATE_LOCK = threading.RLock()
REMOTE_AUTOMATION_STATE_CACHE: dict[str, Any] = {"ts": 0.0, "url": "", "state": {}}
ONLY_STRATEGY_PRESET = "dip_swing"
ONLY_STRATEGY_LABEL = "利润循环"
ONLY_STRATEGY_FALLBACK_DECISION = "持续开仓"
DIP_SWING_AGGRESSIVE_SCALP_MODE = True
DIP_SWING_NET_TARGET_USDT = Decimal("1")
DIP_SWING_MIN_NET_HOLD_USDT = Decimal("-999999")
DIP_SWING_ENTRY_ORDER_MAX_AGE_SECONDS = 4
DIP_SWING_EXIT_ORDER_MAX_AGE_SECONDS = 4
DIP_SWING_EXIT_RETRY_SECONDS = 2
DIP_SWING_MAX_PENDING_ENTRY_ORDERS_PER_SYMBOL = 64
DIP_SWING_NON_BLOCKING_ORDER_TYPES = {"market", "ioc", "fok", "optimal_limit_ioc"}
OKX_BATCH_ORDER_LIMIT = 20
DIP_SWING_DIRECTION_LOOKBACK_BARS = 6
DIP_SWING_MIN_PULLBACK_PCT = Decimal("0.45")
DIP_SWING_MAX_PULLBACK_PCT = Decimal("1.60")
DIP_SWING_MIN_REBOUND_PCT = Decimal("0.25")
DIP_SWING_MAX_REBOUND_PCT = Decimal("1.20")
DIP_SWING_MIN_REBOUND_LOOKBACK_BARS = 6
DIP_SWING_MAX_REBOUND_LOOKBACK_BARS = 12
DIP_SWING_MIN_EMA_SPREAD_PCT = Decimal("0.03")
DIP_SWING_MIN_FAST_SLOPE_PCT = Decimal("0.02")
DIP_SWING_MAX_CHASE_PCT = Decimal("0.18")
DIP_SWING_MIN_ENTRY_SCORE = 5
DIP_SWING_MIN_EXIT_SCORE = 4
DIP_SWING_HARD_EXIT_SCORE = 5
DIP_SWING_EST_ROUNDTRIP_COST_PCT = Decimal("0.09")
DIP_SWING_MIN_NET_EDGE_PCT = Decimal("0.08")
DIP_SWING_MIN_EDGE_COST_RATIO = Decimal("1.9")
DIP_SWING_MIN_RANGE_COST_RATIO = Decimal("3.0")
DIP_SWING_MIN_ATR_COST_RATIO = Decimal("1.4")
DIP_SWING_MIN_AVG_QUOTE_VOLUME_USD = Decimal("3000000")
DIP_SWING_MIN_OPEN_INTEREST_USD = Decimal("6000000")
DIP_SWING_MIN_PROTECTIVE_EXIT_PCT = Decimal("0.28")
DIP_SWING_ORDER_PRESSURE_WINDOW_MINUTES = 45
DIP_SWING_MAX_OPEN_CLOSE_GAP = 64
DIP_SWING_MAX_CONSECUTIVE_OPEN_STREAK = 24
DIP_SWING_MAX_OPEN_ONLY_ORDERS_PER_SYMBOL = 3
DIP_SWING_MAX_UNDERWATER_OPEN_GAP = 2
DIP_SWING_SYMBOL_PERF_MIN_CLOSE_ORDERS = 2
DIP_SWING_SYMBOL_NEGATIVE_NET_WARN_USDT = Decimal("5")
DIP_SWING_SYMBOL_NEGATIVE_NET_BLOCK_USDT = Decimal("15")
DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT = Decimal("24")
DIP_SWING_SYMBOL_PERFORMANCE_PENALTY_DIVISOR = Decimal("4")
DIP_SWING_HARD_BREAK_LOSS_PCT = Decimal("0.45")
DIP_SWING_POST_ONLY_BUFFER_PCT = Decimal("0")
DIP_SWING_EXIT_POST_ONLY_BUFFER_PCT = Decimal("0")
DIP_SWING_POST_ONLY_ENTRY_EXTRA_TICKS = Decimal("0")
DIP_SWING_POST_ONLY_EXIT_EXTRA_TICKS = Decimal("0")
DIP_SWING_MARKET_FALLBACK_OPEN_GAP = 12
DIP_SWING_MARKET_FALLBACK_OPEN_STREAK = 6
DIP_SWING_FORCE_MARKET_ENTRY_POLL_SECONDS = 1
DIP_SWING_FORCE_MARKET_ENTRY_TARGET_COUNT = 8
DIP_SWING_EXIT_PROTECTION_PCT = Decimal("0.08")
DIP_SWING_TARGET_MIN_MARGIN_RATIO = Decimal("0.012")
DIP_SWING_TARGET_MAX_MARGIN_RATIO = Decimal("0.050")
DIP_SWING_AVAILABLE_MARGIN_UTILIZATION = Decimal("0.92")
DIP_SWING_STALLED_POSITION_MAX_HOLD_MINUTES = 20
DIP_SWING_STALLED_POSITION_MIN_AVAILABLE_MARGIN_USDT = Decimal("25")
DIP_SWING_MIN_LIQ_BUFFER_PCT = Decimal("18")
DIP_SWING_MAX_LEVERAGE = Decimal("10")
OKX_DEFAULT_SWAP_MAKER_FEE_PCT = Decimal("0.02")
OKX_DEFAULT_SWAP_TAKER_FEE_PCT = Decimal("0.05")
OKX_DEFAULT_PASSIVE_EXIT_WEIGHT = Decimal("0.70")
PAPER_SPOT_FEE_RATE = Decimal("0.001")
PAPER_SWAP_FEE_RATE = Decimal("0.0005")


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


def should_run_local_okx_background_tasks(config: dict[str, Any]) -> bool:
    if is_remote_execution_enabled(config):
        return False
    return bool(
        str(config.get("apiKey") or "").strip()
        and str(config.get("secretKey") or "").strip()
        and str(config.get("passphrase") or "").strip()
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
    client_ip = str((handler.client_address or ("", 0))[0] or "")
    is_direct_loopback = is_loopback_client(client_ip) and handler.headers.get("X-OKX-Desk-Forwarded") != "1"
    if is_direct_loopback:
        return True
    access_token = configured_gateway_access_token()
    if not access_token:
        error_response(handler, "远端执行节点未配置鉴权令牌", status=503)
        return False
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
REMOTE_AUTOMATION_STATE_TIMEOUT = 2.5


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
    # Remote execution stores trading secrets on the remote node instead of the local control plane.
    merged["apiKey"] = ""
    merged["secretKey"] = ""
    merged["passphrase"] = ""
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
    if not should_run_local_okx_background_tasks(config):
        return
    refresh_route = True

    def fetch_account() -> dict[str, Any]:
        client = OkxClient(config)
        snapshot = fetch_account_snapshot(client, include_positions=False)
        return {
            "summary": snapshot["summary"],
            "fundingSummary": snapshot.get("fundingSummary", {}),
            "balanceCount": snapshot.get("balanceCount", 0),
            "positionCount": snapshot.get("positionCount", 0),
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


def format_decimal(value: Decimal | str | int | float, places: int = 2) -> str:
    number = safe_decimal(value)
    if places <= 0:
        quantized = number.quantize(Decimal("1"))
    else:
        quantized = number.quantize(Decimal("1").scaleb(-places))
    rendered = format(quantized, "f")
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


def round_up(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    units = (value / step).to_integral_value(rounding=ROUND_UP)
    return units * step


def flag_true(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on"}
    return bool(value)


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
        "strategyPreset": ONLY_STRATEGY_PRESET,
        "spotInstId": "BTC-USDT",
        "swapInstId": "BTC-USDT-SWAP",
        "watchlistSymbols": "BTC",
        "watchlistOverrides": {},
        "bar": "1m",
        "fastEma": 6,
        "slowEma": 24,
        "pollSeconds": 5,
        "cooldownSeconds": 0,
        "maxOrdersPerDay": 0,
        "spotEnabled": False,
        "spotQuoteBudget": "0",
        "spotMaxExposure": "0",
        "swapEnabled": True,
        "swapContracts": "0",
        "swapTdMode": "isolated",
        "swapLeverage": "10",
        "swapStrategyMode": "trend_follow",
        "stopLossPct": "0",
        "takeProfitPct": "0",
        "maxDailyLossPct": "0",
        "targetBalanceMultiple": "100",
        "arbEntrySpreadPct": "0.18",
        "arbExitSpreadPct": "0.05",
        "arbMinFundingRatePct": "0.005",
        "arbMaxHoldMinutes": 180,
        "arbRequireFundingAlignment": True,
        "autostart": False,
        "allowLiveManualOrders": False,
        "allowLiveTrading": False,
        "allowLiveAutostart": False,
        "enforceNetMode": True,
    }


def enforce_only_dip_swing_strategy(config: dict[str, Any]) -> dict[str, Any]:
    enforced = copy.deepcopy(config)
    enforced["strategyPreset"] = ONLY_STRATEGY_PRESET
    enforced["spotEnabled"] = False
    enforced["spotQuoteBudget"] = "0"
    enforced["spotMaxExposure"] = "0"
    enforced["swapEnabled"] = True
    enforced["swapTdMode"] = "isolated"
    enforced["swapStrategyMode"] = "trend_follow"
    enforced["maxOrdersPerDay"] = 0
    enforced["cooldownSeconds"] = 0
    enforced["pollSeconds"] = min(max(int(enforced.get("pollSeconds", 5) or 5), 1), 3)
    return enforced


def sanitize_only_dip_swing_override(override: dict[str, Any]) -> dict[str, Any]:
    sanitized = copy.deepcopy(override)
    for field in (
        "spotEnabled",
        "spotQuoteBudget",
        "spotMaxExposure",
        "arbEntrySpreadPct",
        "arbExitSpreadPct",
        "arbMinFundingRatePct",
        "arbMaxHoldMinutes",
        "arbRequireFundingAlignment",
    ):
        sanitized.pop(field, None)
    sanitized["swapEnabled"] = True
    sanitized["swapTdMode"] = "isolated"
    sanitized["swapStrategyMode"] = "long_only"
    return sanitized


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
        "floatingPnl": "0",
        "floatingPnlPct": "0",
        "pnlSource": "",
        "riskBudget": "0",
        "riskCap": "0",
        "riskMode": "",
        "riskLabel": "",
        "lastAction": "",
        "lastActionAt": "",
        "lastOrderId": "",
        "lastMessage": "",
        "lastTradeAt": "",
        "liquidationPrice": "",
        "liquidationBufferPct": "",
        "contractValue": "",
        "arbStage": "",
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
        "targetBalanceEq": "",
        "targetBalanceProgressPct": "0",
        "targetBalanceRemainingEq": "",
        "targetBalanceReached": False,
        "dailyDrawdownPct": "0",
        "orderCountToday": 0,
        "today": "",
        "consecutiveErrors": 0,
        "equityCurve": [],
        "lastAppliedStrategy": {
            "stage": "idle",
            "title": "等待应用策略",
            "detail": "",
            "appliedAt": "",
        },
        "lastPipeline": {
            "signal": "idle",
            "portfolio": "idle",
            "risk": "idle",
            "execution": "idle",
            "targetCount": 0,
            "allowNewEntries": False,
            "equity": "0",
            "completedAt": "",
            "summary": "",
        },
        "lastRiskReport": {
            "status": "idle",
            "stopReason": "",
            "drawdownPct": "0",
            "maxDailyLossPct": "0",
            "orderCountToday": 0,
            "maxOrdersPerDay": 0,
            "activeMarkets": 0,
            "watchedSymbols": 0,
            "checks": [],
            "updatedAt": "",
        },
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
        "watchlist": [],
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


LEGACY_NON_SWING_MARKERS = (
    "高频套利",
    "价差回归套利",
    "当前负基差",
    "不做这侧套利",
    "watchlist 可做",
    "市场候选",
    "反向",
    "套利入场",
    "套利窗口",
    "资金费",
)


def has_legacy_non_swing_text(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    return any(marker in text for marker in LEGACY_NON_SWING_MARKERS)


def sanitize_only_dip_swing_message(message: Any, *, fallback: str = "等待本轮方向确认") -> str:
    text = str(message or "").strip()
    if not text:
        return ""
    if has_legacy_non_swing_text(text) or "联网决策层" in text:
        return fallback
    return text


def normalize_watchlist_symbols(raw: Any, config: dict[str, Any] | None = None) -> list[str]:
    tokens = re.split(r"[\s,;/|]+", str(raw or "").strip())
    normalized: list[str] = []
    for token in tokens:
        value = token.strip().upper()
        if not value:
            continue
        if "-" in value:
            value = value.split("-")[0]
        value = re.sub(r"[^A-Z0-9]", "", value)
        if not value or value in normalized:
            continue
        normalized.append(value)

    if normalized:
        return normalized[:8]

    fallback_config = config or {}
    for key in ("spotInstId", "swapInstId"):
        inst_id = str(fallback_config.get(key, "") or "").strip().upper()
        if inst_id:
            symbol = inst_id.split("-")[0]
            if symbol and symbol not in normalized:
                normalized.append(symbol)

    return (normalized or ["BTC"])[:8]


WATCHLIST_OVERRIDE_DECIMAL_FIELDS = {
    "spotQuoteBudget",
    "spotMaxExposure",
    "swapContracts",
    "swapLeverage",
    "stopLossPct",
    "takeProfitPct",
    "maxDailyLossPct",
    "arbEntrySpreadPct",
    "arbExitSpreadPct",
    "arbMinFundingRatePct",
}
WATCHLIST_OVERRIDE_INT_FIELDS = {
    "fastEma",
    "slowEma",
    "pollSeconds",
    "cooldownSeconds",
    "maxOrdersPerDay",
    "arbMaxHoldMinutes",
}
WATCHLIST_OVERRIDE_BOOL_FIELDS = {"spotEnabled", "swapEnabled", "arbRequireFundingAlignment"}
WATCHLIST_OVERRIDE_ENUM_FIELDS = {
    "bar": {"1m", "5m", "15m", "1H"},
    "swapTdMode": {"cross", "isolated"},
    "swapStrategyMode": {"long_only", "short_only", "trend_follow"},
}
WATCHLIST_OVERRIDE_ALLOWED_FIELDS = (
    WATCHLIST_OVERRIDE_DECIMAL_FIELDS
    | WATCHLIST_OVERRIDE_INT_FIELDS
    | WATCHLIST_OVERRIDE_BOOL_FIELDS
    | set(WATCHLIST_OVERRIDE_ENUM_FIELDS.keys())
)


def normalize_symbol_token(raw: Any) -> str:
    value = str(raw or "").strip().upper()
    if "-" in value:
        value = value.split("-")[0]
    return re.sub(r"[^A-Z0-9]", "", value)


def parse_watchlist_overrides(raw: Any) -> tuple[dict[str, dict[str, Any]], str]:
    if raw in (None, "", {}, []):
        return {}, ""
    parsed = raw
    if isinstance(raw, str):
        text = raw.strip()
        if not text:
            return {}, ""
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError as exc:
            return {}, f"按币覆盖配置不是合法 JSON: {exc.msg}"
    if not isinstance(parsed, dict):
        return {}, "按币覆盖配置必须是对象，例如 {\"BTC\": {\"fastEma\": 12}}"

    normalized: dict[str, dict[str, Any]] = {}
    for symbol_key, override in parsed.items():
        symbol = normalize_symbol_token(symbol_key)
        if not symbol:
            continue
        if not isinstance(override, dict):
            return {}, f"{symbol} 的按币覆盖必须是对象"
        normalized_override: dict[str, Any] = {}
        for field, value in override.items():
            if field not in WATCHLIST_OVERRIDE_ALLOWED_FIELDS:
                return {}, f"{symbol} 的按币覆盖字段不支持: {field}"
            if field in WATCHLIST_OVERRIDE_DECIMAL_FIELDS:
                normalized_override[field] = decimal_to_str(safe_decimal(value, "0"))
            elif field in WATCHLIST_OVERRIDE_INT_FIELDS:
                try:
                    normalized_override[field] = int(value)
                except (TypeError, ValueError):
                    return {}, f"{symbol} 的按币覆盖字段 {field} 必须是整数"
            elif field in WATCHLIST_OVERRIDE_BOOL_FIELDS:
                normalized_override[field] = bool(value)
            else:
                normalized_value = str(value or "").strip()
                allowed = WATCHLIST_OVERRIDE_ENUM_FIELDS.get(field)
                if allowed and normalized_value not in allowed:
                    return {}, f"{symbol} 的按币覆盖字段 {field} 不支持值 {normalized_value}"
                normalized_override[field] = normalized_value
        if normalized_override:
            normalized[symbol] = normalized_override
    return normalized, ""


def validate_single_automation_target(config: dict[str, Any]) -> str:
    if str(config.get("bar") or "") not in {"1m", "5m", "15m", "1H"}:
        return "K 线周期仅支持 1m、5m、15m、1H"
    fast = int(config.get("fastEma", 0))
    slow = int(config.get("slowEma", 0))
    poll = int(config.get("pollSeconds", 0))
    cooldown = int(config.get("cooldownSeconds", 0))
    max_orders = int(config.get("maxOrdersPerDay", 0))
    if fast < 2 or slow <= fast:
        return "EMA 参数不合法，必须满足 slow > fast >= 2"
    if poll < 1 or poll > 300:
        return "轮询秒数需在 1 到 300 之间"
    if cooldown < 0 or cooldown > 3600:
        return "冷却秒数需在 0 到 3600 之间"
    if max_orders < 0 or max_orders > 500:
        return "每日最大订单数需在 0 到 500 之间；0 代表不限制"
    if config.get("swapStrategyMode") not in ("long_only", "short_only", "trend_follow"):
        return "永续策略模式不支持"
    if config.get("swapTdMode") not in ("cross", "isolated"):
        return "永续保证金模式仅支持 cross 或 isolated"
    for field in (
        "spotQuoteBudget",
        "spotMaxExposure",
        "swapContracts",
        "swapLeverage",
        "stopLossPct",
        "takeProfitPct",
        "maxDailyLossPct",
        "targetBalanceMultiple",
        "arbEntrySpreadPct",
        "arbExitSpreadPct",
        "arbMinFundingRatePct",
    ):
        if safe_decimal(config.get(field), "0") < 0:
            return f"{field} 不能小于 0"
    arb_max_hold = int(config.get("arbMaxHoldMinutes", 0) or 0)
    if arb_max_hold < 0 or arb_max_hold > 7 * 24 * 60:
        return "套利最长持有分钟需在 0 到 10080 之间"
    if str(config.get("strategyPreset") or "") == "basis_arb":
        if not config.get("spotEnabled") or not config.get("swapEnabled"):
            return "高频套利必须同时启用现货和永续"
        if safe_decimal(config.get("spotQuoteBudget"), "0") <= 0:
            return "高频套利的现货预算必须大于 0"
        if safe_decimal(config.get("swapContracts"), "0") <= 0:
            return "高频套利的永续张数必须大于 0"
        entry_spread = safe_decimal(config.get("arbEntrySpreadPct"), "0")
        exit_spread = safe_decimal(config.get("arbExitSpreadPct"), "0")
        if entry_spread <= 0:
            return "高频套利的入场价差必须大于 0"
        if exit_spread < 0:
            return "高频套利的回补价差不能小于 0"
        if exit_spread >= entry_spread:
            return "高频套利的回补价差必须小于入场价差"
    if str(config.get("strategyPreset") or "") == "dip_swing":
        if not config.get("swapEnabled"):
            return f"{ONLY_STRATEGY_LABEL}必须启用永续"
        if str(config.get("swapStrategyMode") or "") != "trend_follow":
            return f"{ONLY_STRATEGY_LABEL}只支持 trend_follow"
        if str(config.get("swapTdMode") or "") != "isolated":
            return f"{ONLY_STRATEGY_LABEL}必须使用 isolated 逐仓"
        if safe_decimal(config.get("swapLeverage"), "1") > DIP_SWING_MAX_LEVERAGE:
            return f"{ONLY_STRATEGY_LABEL}杠杆不能高于 {decimal_to_str(DIP_SWING_MAX_LEVERAGE)}x"
    target_multiple = safe_decimal(config.get("targetBalanceMultiple"), "1")
    if target_multiple < Decimal("1") or target_multiple > Decimal("100"):
        return "目标余额倍数需在 1 到 100 之间"
    return ""


def allocate_watchlist_numeric_field(
    symbols: list[str],
    overrides: dict[str, dict[str, Any]],
    field: str,
    total: Decimal,
) -> dict[str, Decimal]:
    explicit: dict[str, Decimal] = {}
    for symbol in symbols:
        if field in (overrides.get(symbol) or {}):
            explicit[symbol] = safe_decimal(overrides[symbol].get(field), "0")
    remaining_symbols = [symbol for symbol in symbols if symbol not in explicit]
    remaining_total = total - sum(explicit.values(), Decimal("0"))
    if remaining_total < 0:
        remaining_total = Decimal("0")
    per_symbol = remaining_total / Decimal(len(remaining_symbols)) if remaining_symbols else Decimal("0")
    allocations: dict[str, Decimal] = {}
    for symbol in symbols:
        allocations[symbol] = explicit.get(symbol, per_symbol)
    return allocations


def build_execution_targets(config: dict[str, Any]) -> list[dict[str, Any]]:
    symbols = normalize_watchlist_symbols(config.get("watchlistSymbols"), config)
    target_count = max(1, len(symbols))
    overrides = copy.deepcopy(config.get("watchlistOverrides") or {})
    spot_budget = safe_decimal(config.get("spotQuoteBudget"), "0")
    spot_exposure = safe_decimal(config.get("spotMaxExposure"), "0")
    spot_budget_allocations = allocate_watchlist_numeric_field(symbols, overrides, "spotQuoteBudget", spot_budget)
    spot_exposure_allocations = allocate_watchlist_numeric_field(symbols, overrides, "spotMaxExposure", spot_exposure)
    targets: list[dict[str, Any]] = []

    for index, symbol in enumerate(symbols):
        pair_config = deep_merge(config, {})
        pair_config["watchlistSymbols"] = ",".join(symbols)
        pair_config["watchlistOverrides"] = copy.deepcopy(overrides)
        pair_config["watchlistSymbol"] = symbol
        pair_config["watchlistIndex"] = index
        pair_config["watchlistCount"] = target_count
        pair_config["spotInstId"] = f"{symbol}-USDT"
        pair_config["swapInstId"] = f"{symbol}-USDT-SWAP"
        if pair_config.get("spotEnabled"):
            pair_config["spotQuoteBudget"] = decimal_to_str(spot_budget_allocations.get(symbol, Decimal("0")))
            pair_config["spotMaxExposure"] = decimal_to_str(spot_exposure_allocations.get(symbol, Decimal("0")))
        if pair_config.get("swapEnabled") and str(pair_config.get("strategyPreset") or "") == "dip_swing":
            pair_config["swapContracts"] = "0"
        symbol_override = copy.deepcopy(overrides.get(symbol) or {})
        if symbol_override:
            pair_config = deep_merge(pair_config, symbol_override)
            if str(pair_config.get("strategyPreset") or "") == "dip_swing":
                pair_config["swapContracts"] = "0"
        pair_config["watchlistOverride"] = symbol_override
        targets.append(pair_config)

    return targets


def resolve_selected_execution_target(config: dict[str, Any]) -> dict[str, Any]:
    spot_inst_id = str(config.get("spotInstId") or "")
    swap_inst_id = str(config.get("swapInstId") or "")
    targets = build_execution_targets(config)
    for target in targets:
        if (
            str(target.get("spotInstId") or "") == spot_inst_id
            and str(target.get("swapInstId") or "") == swap_inst_id
        ):
            return target
    return deep_merge(config, {})


def resolve_target_balance_multiple(config: dict[str, Any]) -> Decimal:
    raw = safe_decimal(config.get("targetBalanceMultiple"), "1")
    if raw < Decimal("1"):
        return Decimal("1")
    if raw > Decimal("100"):
        return Decimal("100")
    return raw


def build_target_balance_snapshot(
    start_eq: Decimal,
    current_eq: Decimal,
    target_multiple: Decimal,
) -> dict[str, Any]:
    if start_eq <= 0 or current_eq <= 0 or target_multiple <= Decimal("1"):
        return {
            "targetEq": Decimal("0"),
            "remainingEq": Decimal("0"),
            "progressPct": Decimal("0"),
            "currentMultiple": Decimal("1"),
            "reached": False,
        }
    target_eq = start_eq * target_multiple
    remaining_eq = max(target_eq - current_eq, Decimal("0"))
    progress_pct = (current_eq / target_eq) * Decimal("100") if target_eq > 0 else Decimal("0")
    current_multiple = current_eq / start_eq if start_eq > 0 else Decimal("1")
    return {
        "targetEq": target_eq,
        "remainingEq": remaining_eq,
        "progressPct": progress_pct,
        "currentMultiple": current_multiple,
        "reached": current_eq >= target_eq,
    }


def target_execution_phase_label(phase: str) -> str:
    normalized = str(phase or "").strip().lower()
    mapping = {
        "idle": "待机",
        "fixed": "固定",
        "protect": "守仓",
        "compound": "复利",
        "advance": "进攻",
        "attack": "满攻",
    }
    return mapping.get(normalized, normalized or "待机")


def should_keep_running_in_test_mode(automation: dict[str, Any]) -> bool:
    return not bool(automation.get("allowLiveTrading"))


def build_target_execution_ability_snapshot(
    automation: dict[str, Any],
    *,
    state: dict[str, Any] | None = None,
    journal: dict[str, Any] | None = None,
    target_snapshot: dict[str, Any] | None = None,
    limit: int = 120,
) -> dict[str, Any]:
    session_state = state or AUTOMATION_STATE.current()
    journal_snapshot = journal if journal is not None else get_execution_journal_snapshot(
        limit=limit,
        live_only=prefer_live_execution_state(CONFIG.current()),
    )
    summary = journal_snapshot.get("summary") or {}
    target_balance = target_snapshot or build_target_balance_snapshot(
        safe_decimal(session_state.get("sessionStartEq"), "0"),
        safe_decimal(session_state.get("currentEq"), "0"),
        resolve_target_balance_multiple(automation),
    )

    start_eq = safe_decimal(session_state.get("sessionStartEq"), "0")
    current_eq = safe_decimal(session_state.get("currentEq"), "0")
    max_eq = safe_decimal(session_state.get("maxObservedEq"), "0")
    close_orders = int(summary.get("closeOrders") or 0)
    winning_close_orders = int(summary.get("winningCloseOrders") or 0)
    losing_close_orders = int(summary.get("losingCloseOrders") or 0)
    breakeven_close_orders = int(summary.get("breakevenCloseOrders") or 0)
    close_win_rate_pct = safe_decimal(summary.get("closeWinRatePct"), "0")
    realized_pnl = safe_decimal(summary.get("realizedPnl"), "0")
    total_fees = safe_decimal(summary.get("totalFees"), "0")
    net_pnl = safe_decimal(summary.get("netPnl"), decimal_to_str(realized_pnl + total_fees))
    avg_abs_slip_mark_pct = safe_decimal(summary.get("avgAbsSlipMarkPct"), "0")
    taker_fill_pct = safe_decimal(summary.get("takerFillPct"), "0")
    execution_cost_floor_pct = safe_decimal(summary.get("executionCostFloorPct"), "0")
    progress_pct = safe_decimal(target_balance.get("progressPct"), "0")
    current_multiple = safe_decimal(target_balance.get("currentMultiple"), "1")

    session_return_pct = Decimal("0")
    if start_eq > 0:
        session_return_pct = ((current_eq - start_eq) / start_eq) * Decimal("100")
    setback_pct = Decimal("0")
    if max_eq > 0 and current_eq < max_eq:
        setback_pct = ((max_eq - current_eq) / max_eq) * Decimal("100")

    score = Decimal(winning_close_orders - losing_close_orders)
    phase = "fixed"
    if close_orders <= 0:
        phase = "fixed"
    elif score >= Decimal("6"):
        phase = "attack"
    elif score >= Decimal("3"):
        phase = "advance"
    elif score >= Decimal("1"):
        phase = "compound"
    else:
        phase = "protect"
    if current_eq <= 0:
        phase = "protect"
    if progress_pct >= Decimal("100"):
        phase = "attack"

    return {
        "phase": phase,
        "phaseLabel": target_execution_phase_label(phase),
        "score": score,
        "closeOrders": close_orders,
        "winningCloseOrders": winning_close_orders,
        "losingCloseOrders": losing_close_orders,
        "breakevenCloseOrders": breakeven_close_orders,
        "closeWinRatePct": close_win_rate_pct,
        "realizedPnl": realized_pnl,
        "totalFees": total_fees,
        "netPnl": net_pnl,
        "avgAbsSlipMarkPct": avg_abs_slip_mark_pct,
        "takerFillPct": taker_fill_pct,
        "executionCostFloorPct": execution_cost_floor_pct,
        "sessionReturnPct": session_return_pct,
        "setbackPct": setback_pct,
        "progressPct": progress_pct,
        "currentMultiple": current_multiple,
        "scalingAllowed": True,
    }


def strategy_mode_text(mode: str) -> str:
    normalized = str(mode or "").strip()
    if normalized == "short_only":
        return "只做空"
    if normalized == "trend_follow":
        return "顺势双向"
    return "只做多"


def build_market_risk_label(target: dict[str, Any], market_kind: str) -> str:
    if str(target.get("strategyPreset") or "") == "basis_arb":
        entry = safe_decimal(target.get("arbEntrySpreadPct"), "0")
        exit_spread = safe_decimal(target.get("arbExitSpreadPct"), "0")
        min_funding = safe_decimal(target.get("arbMinFundingRatePct"), "0")
        return (
            f"入场 ≥ {format_decimal(entry, 3)}% · "
            f"回补 ≤ {format_decimal(exit_spread, 3)}% · "
            f"资金费 ≥ {format_decimal(min_funding, 3)}%"
        )
    if str(target.get("strategyPreset") or "") == "dip_swing":
        return (
            f"每单净赚 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U+ 就平 · "
            f"缓冲 ≥ {format_decimal(DIP_SWING_MIN_LIQ_BUFFER_PCT, 0)}% · "
            f"动态仓位 · {format_decimal(safe_decimal(target.get('swapLeverage'), '10'), 0)}x 逐仓"
        )
    if market_kind == "spot":
        budget = safe_decimal(target.get("spotQuoteBudget"), "0")
        cap = safe_decimal(target.get("spotMaxExposure"), "0")
        return f"单次 {format_decimal(budget, 2)}U · 上限 {format_decimal(cap, 2)}U"
    leverage = safe_decimal(target.get("swapLeverage"), "0")
    td_mode = "逐仓" if str(target.get("swapTdMode") or "").strip() == "isolated" else "全仓"
    return f"动态仓位 · {format_decimal(leverage, 0)}x · {td_mode}"


def apply_target_market_allocation(
    target: dict[str, Any],
    market_kind: str,
    market_state: dict[str, Any],
) -> dict[str, Any]:
    patched = copy.deepcopy(market_state or default_market_state())
    if market_kind == "spot":
        patched.update(
            {
                "riskBudget": decimal_to_str(safe_decimal(target.get("spotQuoteBudget"), "0")),
                "riskCap": decimal_to_str(safe_decimal(target.get("spotMaxExposure"), "0")),
                "riskMode": "basis_arb" if str(target.get("strategyPreset") or "") == "basis_arb" else "cash",
                "riskLabel": build_market_risk_label(target, "spot"),
            }
        )
        return patched

    patched.update(
        {
            "riskBudget": "" if str(target.get("strategyPreset") or "") == "dip_swing" else decimal_to_str(safe_decimal(target.get("swapContracts"), "0")),
            "riskCap": decimal_to_str(safe_decimal(target.get("swapLeverage"), "0")),
            "riskMode": "basis_arb" if str(target.get("strategyPreset") or "") == "basis_arb" else str(target.get("swapTdMode") or "cross"),
            "riskLabel": build_market_risk_label(target, "swap"),
        }
    )
    return patched


def basis_arb_stage_text(stage: str) -> str:
    normalized = str(stage or "").strip().lower()
    if normalized == "reverse_basis":
        return "当前负基差，不做这侧套利"
    if normalized == "window_open":
        return "套利窗口打开"
    if normalized == "entry_wait":
        return "套利窗口已出现，等待冷却"
    if normalized == "funding_blocked":
        return "资金费未对齐"
    if normalized == "blocked_budget":
        return "套利预算不足"
    if normalized == "blocked_hedge":
        return "永续对冲腿未配置"
    if normalized == "hedged":
        return "双腿已对冲，等待回补"
    if normalized == "exit_wait":
        return "满足回补条件，等待冷却"
    if normalized == "exiting":
        return "正在回补套利双腿"
    if normalized == "rollback":
        return "对冲失败，正在回滚"
    if normalized == "broken_pair":
        return "套利双腿不完整"
    return "等待套利窗口"


def build_watchlist_entry(
    symbol: str,
    target: dict[str, Any],
    spot_market: dict[str, Any],
    swap_market: dict[str, Any],
) -> dict[str, Any]:
    spot_market = apply_target_market_allocation(target, "spot", spot_market)
    swap_market = apply_target_market_allocation(target, "swap", swap_market)
    symbol_override = copy.deepcopy(target.get("watchlistOverride") or {})
    override_keys = sorted(symbol_override.keys())
    spot_notional = safe_decimal(spot_market.get("positionNotional"), "0")
    swap_notional = safe_decimal(swap_market.get("positionNotional"), "0")
    spot_pnl = safe_decimal(spot_market.get("floatingPnl"), "0")
    swap_pnl = safe_decimal(swap_market.get("floatingPnl"), "0")
    exposure_total = spot_notional + swap_notional
    pnl_total = spot_pnl + swap_pnl
    pnl_pct = Decimal("0")
    if exposure_total > 0:
        pnl_pct = (pnl_total / exposure_total) * Decimal("100")

    active_legs = []
    if str(spot_market.get("positionSide") or "flat") != "flat":
        active_legs.append("现货")
    if str(swap_market.get("positionSide") or "flat") != "flat":
        active_legs.append("永续")

    is_basis_arb = str(target.get("strategyPreset") or "") == "basis_arb"
    arb_stage = str(spot_market.get("arbStage") or swap_market.get("arbStage") or "").strip().lower()
    if is_basis_arb:
        status = basis_arb_stage_text(arb_stage)
    elif active_legs:
        status = f"{' + '.join(active_legs)} 持仓中"
    elif (
        spot_market.get("signal") in {"arb_entry", "arb_hedged", "arb_active"}
        or swap_market.get("signal") in {"arb_entry", "arb_hedged", "arb_active"}
    ):
        status = "套利窗口打开"
    elif spot_market.get("signal") in {"bull_cross", "bear_cross"} or swap_market.get("signal") in {"bull_cross", "bear_cross"}:
        status = "出现执行信号"
    else:
        status = "观察中"

    detail_bits = []
    if override_keys:
        detail_bits.append(
            f"独立覆盖 {len(override_keys)} 项 · {target.get('bar', '5m')} EMA {target.get('fastEma', '-')}/{target.get('slowEma', '-')}"
        )
    if str(spot_market.get("lastMessage") or "").strip():
        detail_bits.append(f"现货 {spot_market.get('lastMessage')}")
    if str(swap_market.get("lastMessage") or "").strip():
        detail_bits.append(f"永续 {swap_market.get('lastMessage')}")

    return {
        "symbol": symbol,
        "spot": copy.deepcopy(spot_market),
        "swap": copy.deepcopy(swap_market),
        "allocation": {
            "strategyPreset": str(target.get("strategyPreset") or "dual_engine"),
            "spotBudget": decimal_to_str(safe_decimal(target.get("spotQuoteBudget"), "0")),
            "spotMaxExposure": decimal_to_str(safe_decimal(target.get("spotMaxExposure"), "0")),
            "swapContracts": "" if str(target.get("strategyPreset") or "") == "dip_swing" else decimal_to_str(safe_decimal(target.get("swapContracts"), "0")),
            "swapLeverage": decimal_to_str(safe_decimal(target.get("swapLeverage"), "0")),
            "swapTdMode": str(target.get("swapTdMode") or "cross"),
            "swapStrategyMode": str(target.get("swapStrategyMode") or "trend_follow"),
            "overrideKeys": override_keys,
        },
        "overrideActive": bool(override_keys),
        "summary": {
            "status": status,
            "detail": " · ".join(detail_bits[:2]),
            "activeLegs": active_legs,
            "exposureTotal": decimal_to_str(exposure_total),
            "floatingPnl": decimal_to_str(pnl_total),
            "floatingPnlPct": decimal_to_str(pnl_pct),
            "arbStage": arb_stage,
            "arbStageText": basis_arb_stage_text(arb_stage) if is_basis_arb else "",
            "riskLabel": (
                f"现货 {build_market_risk_label(target, 'spot')} · "
                f"永续 {build_market_risk_label(target, 'swap')} · "
                f"{strategy_mode_text(str(target.get('swapStrategyMode') or 'trend_follow'))}"
            ),
        },
    }


def reconcile_runtime_state_with_automation(state: dict[str, Any], automation: dict[str, Any]) -> dict[str, Any]:
    hydrated = copy.deepcopy(state)
    if not hydrated.get("running") and not hydrated.get("watchlist"):
        return hydrated

    markets = hydrated.setdefault("markets", {})
    targets = build_execution_targets(automation)
    if not targets:
        return hydrated

    watchlist_entries: list[dict[str, Any]] = []
    for target in targets:
        spot_key = f"spot:{target['spotInstId']}"
        swap_key = f"swap:{target['swapInstId']}"
        spot_market = apply_target_market_allocation(
            target,
            "spot",
            markets.get(spot_key) or default_market_state(),
        )
        swap_market = apply_target_market_allocation(
            target,
            "swap",
            markets.get(swap_key) or default_market_state(),
        )
        markets[spot_key] = spot_market
        markets[swap_key] = swap_market
        watchlist_entries.append(
            build_watchlist_entry(
                str(target.get("watchlistSymbol") or ""),
                target,
                spot_market,
                swap_market,
            )
        )

    hydrated["watchlist"] = watchlist_entries
    if watchlist_entries:
        markets["spot"] = copy.deepcopy(watchlist_entries[0].get("spot") or default_market_state())
        markets["swap"] = copy.deepcopy(watchlist_entries[0].get("swap") or default_market_state())
    return hydrated


def strip_paper_runtime_artifacts(state: dict[str, Any], config: dict[str, Any] | None = None) -> dict[str, Any]:
    if not prefer_live_execution_state(config):
        return copy.deepcopy(state)
    sanitized = copy.deepcopy(state)
    markets = sanitized.get("markets") or {}
    for key, market in list(markets.items()):
        if not isinstance(market, dict):
            continue
        if not str(market.get("lastOrderId") or "").strip().startswith("paper-"):
            continue
        preserved = copy.deepcopy(market)
        cleared = default_market_state()
        for keep_key in (
            "enabled",
            "instId",
            "signal",
            "trend",
            "lastPrice",
            "riskBudget",
            "riskCap",
            "riskMode",
            "riskLabel",
            "contractValue",
            "prepared",
        ):
            cleared[keep_key] = preserved.get(keep_key, cleared.get(keep_key))
        markets[key] = cleared
    sanitized["markets"] = markets
    return sanitized


def sanitize_only_dip_swing_analysis(analysis: dict[str, Any], automation: dict[str, Any]) -> dict[str, Any]:
    effective = enforce_only_dip_swing_strategy(deep_merge(default_automation_config(), automation or {}))
    sanitized = copy.deepcopy(analysis or {})
    symbols = normalize_watchlist_symbols(effective.get("watchlistSymbols"), effective)
    primary_symbol = symbols[0] if symbols else "BTC"
    stale = (
        has_legacy_non_swing_text(sanitized.get("selectedStrategyName"))
        or has_legacy_non_swing_text(sanitized.get("selectedStrategyDetail"))
        or has_legacy_non_swing_text(sanitized.get("decisionLabel"))
        or has_legacy_non_swing_text(sanitized.get("summary"))
        or has_legacy_non_swing_text(sanitized.get("marketRegime"))
    )

    sanitized["selectedStrategyName"] = f"{primary_symbol} {ONLY_STRATEGY_LABEL}"
    sanitized["selectedStrategyDetail"] = strategy_detail_line(effective)
    if stale:
        allow_new_entries = bool(sanitized.get("allowNewEntries"))
        sanitized["decision"] = "execute" if allow_new_entries else "observe"
        sanitized["decisionLabel"] = "持续开仓" if allow_new_entries else "等待本轮方向确认"
        sanitized["summary"] = "当前只保留利润循环策略，旧策略分析结果已隐藏。"
        sanitized["marketRegime"] = "24h 利润循环"
        sanitized["warnings"] = []
        sanitized["blockers"] = []
    detail_text = " ".join(
        str(item or "")
        for item in (
            sanitized.get("selectedStrategyName"),
            sanitized.get("selectedStrategyDetail"),
            sanitized.get("summary"),
            sanitized.get("marketRegime"),
            sanitized.get("decisionLabel"),
        )
    )
    aggressive_scalp_mode = any(
        marker in detail_text
        for marker in (
            "利润循环",
            "24h 利润循环",
            "每单净利目标 1U",
            "每单净赚 1U",
            "空仓直开",
            "无脑直开",
            "持续开仓",
            "当前为超短直开模式",
        )
    )
    if aggressive_scalp_mode:
        sanitized["executionAbilityPhase"] = "attack"
        sanitized["executionAbilityPhaseLabel"] = "直开"
        sanitized["fundingRatePct"] = ""
        sanitized["basisPct"] = ""
        sanitized["liquidationPrice"] = ""
        sanitized["allowNewEntries"] = True
        sanitized["decision"] = "execute"
        sanitized["decisionLabel"] = "直接开单"
        sanitized["summary"] = "空仓直开，持仓同向继续循环，单笔净赚 1U+ 就平。"
        sanitized["blockers"] = []
        sanitized["warnings"] = [
            item for item in list(sanitized.get("warnings") or [])
            if not any(
                marker in str(item or "")
                for marker in (
                    "安全缓冲",
                    "收缩风险",
                    "跳过新开仓",
                    "阻断:",
                    "临时禁开新仓",
                    "结构裁判快照",
                    "等待本轮方向确认",
                    "taker 占比偏高",
                )
            )
        ]
    return sanitized


def sanitize_only_dip_swing_runtime_state(state: dict[str, Any], automation: dict[str, Any]) -> dict[str, Any]:
    effective = enforce_only_dip_swing_strategy(deep_merge(default_automation_config(), automation or {}))
    sanitized = reconcile_runtime_state_with_automation(copy.deepcopy(state or {}), effective)
    sanitized = strip_paper_runtime_artifacts(sanitized, CONFIG.current())
    sanitized["analysis"] = sanitize_only_dip_swing_analysis(sanitized.get("analysis") or {}, effective)
    if not sanitized["analysis"].get("lastAnalyzedAt") and sanitized.get("lastCycleAt"):
        sanitized["analysis"]["lastAnalyzedAt"] = sanitized.get("lastCycleAt")
    if sanitized["analysis"].get("executionAbilityPhaseLabel") == "直开":
        sanitized["analysis"]["lastAnalyzedAt"] = sanitized.get("lastCycleAt") or sanitized["analysis"].get("lastAnalyzedAt") or ""
    raw_last_error = str(sanitized.get("lastError") or "").strip()
    if "exit_score" in raw_last_error:
        sanitized["lastError"] = ""
        if not sanitized.get("running"):
            sanitized["statusText"] = "等待重新启动利润循环"

    if has_legacy_non_swing_text(sanitized.get("modeText")):
        sanitized["modeText"] = ONLY_STRATEGY_LABEL
    if has_legacy_non_swing_text(sanitized.get("statusText")):
        sanitized["statusText"] = (
            "自动量化已启动，策略会按轮询周期维持利润循环并执行风控。"
            if sanitized.get("running")
            else "自动量化已停止"
        )

    last_pipeline = copy.deepcopy(sanitized.get("lastPipeline") or {})
    if has_legacy_non_swing_text(last_pipeline.get("summary")):
        last_pipeline["summary"] = (
            f"{sanitized['analysis'].get('selectedStrategyName', ONLY_STRATEGY_LABEL)} · "
            f"{sanitized['analysis'].get('decisionLabel', '等待本轮方向确认')}"
        )
    sanitized["lastPipeline"] = last_pipeline

    research = copy.deepcopy(sanitized.get("research") or {})
    if str(research.get("mode") or "").strip() == "basis_arb":
        research["mode"] = "dip_swing"
        research["statusText"] = f"{ONLY_STRATEGY_LABEL}模式"
        best_config = deep_merge(default_automation_config(), research.get("bestConfig") or {})
        research["bestConfig"] = enforce_only_dip_swing_strategy(best_config)
    sanitized["research"] = research

    markets = sanitized.get("markets") or {}
    for key, market in list(markets.items()):
        if not isinstance(market, dict):
            continue
        patched = copy.deepcopy(market)
        patched["lastMessage"] = sanitize_only_dip_swing_message(patched.get("lastMessage"))
        if str(patched.get("trend") or "").strip() == "basis_arb":
            patched["trend"] = "dip_swing"
        if str(patched.get("riskMode") or "").strip() == "basis_arb":
            patched["riskMode"] = str(patched.get("tdMode") or effective.get("swapTdMode") or "isolated")
        patched["arbStage"] = ""
        patched["arbBias"] = ""
        patched["arbSpotSize"] = ""
        patched["arbSwapSize"] = ""
        patched["arbOpenedAt"] = ""
        patched["arbEntrySpreadPct"] = ""
        patched["arbEntrySpotPx"] = ""
        patched["arbEntrySwapPx"] = ""
        markets[key] = patched
    sanitized["markets"] = markets

    cleaned_watchlist: list[dict[str, Any]] = []
    for entry in sanitized.get("watchlist") or []:
        patched_entry = copy.deepcopy(entry)
        allocation = copy.deepcopy(patched_entry.get("allocation") or {})
        allocation["strategyPreset"] = ONLY_STRATEGY_PRESET
        patched_entry["allocation"] = allocation
        for leg_name in ("spot", "swap"):
            leg = copy.deepcopy(patched_entry.get(leg_name) or {})
            leg["lastMessage"] = sanitize_only_dip_swing_message(leg.get("lastMessage"))
            if str(leg.get("trend") or "").strip() == "basis_arb":
                leg["trend"] = "dip_swing"
            if str(leg.get("riskMode") or "").strip() == "basis_arb":
                leg["riskMode"] = str(leg.get("tdMode") or effective.get("swapTdMode") or "isolated")
            leg["arbStage"] = ""
            patched_entry[leg_name] = leg
        summary = copy.deepcopy(patched_entry.get("summary") or {})
        summary["arbStage"] = ""
        summary["arbStageText"] = ""
        summary["status"] = "循环持仓中" if summary.get("activeLegs") else "等待开仓"
        summary["detail"] = sanitize_only_dip_swing_message(summary.get("detail"), fallback="等待本轮方向确认")
        patched_entry["summary"] = summary
        cleaned_watchlist.append(patched_entry)
    sanitized["watchlist"] = cleaned_watchlist
    return sanitized


def normalize_remote_automation_config_payload(
    payload: dict[str, Any] | None,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    candidate = copy.deepcopy((payload or {}).get("config") or {})
    ok, _, normalized = validate_automation_config(candidate)
    if ok:
        return enforce_only_dip_swing_strategy(deep_merge(default_automation_config(), normalized))
    if fallback:
        ok, _, normalized_fallback = validate_automation_config(copy.deepcopy(fallback))
        if ok:
            return enforce_only_dip_swing_strategy(deep_merge(default_automation_config(), normalized_fallback))
    return default_automation_config()


def load_remote_automation_config_for_proxy(config: dict[str, Any]) -> dict[str, Any]:
    cache_key = remote_gateway_url(config)
    now = time.time()
    with REMOTE_AUTOMATION_CONFIG_LOCK:
        cached_key = str(REMOTE_AUTOMATION_CONFIG_CACHE.get("url") or "")
        cached_ts = float(REMOTE_AUTOMATION_CONFIG_CACHE.get("ts") or 0.0)
        cached_config = REMOTE_AUTOMATION_CONFIG_CACHE.get("config") or {}
        if cached_key == cache_key and cached_config and now - cached_ts < 10:
            return copy.deepcopy(cached_config)

    response = remote_gateway_request(
        config,
        "GET",
        "/api/automation/config",
        timeout=REMOTE_CONFIG_FETCH_TIMEOUT,
    )
    payload = response.json() if response.content else {}
    normalized = normalize_remote_automation_config_payload(payload, AUTOMATION_CONFIG.current())
    with REMOTE_AUTOMATION_CONFIG_LOCK:
        REMOTE_AUTOMATION_CONFIG_CACHE["ts"] = now
        REMOTE_AUTOMATION_CONFIG_CACHE["url"] = cache_key
        REMOTE_AUTOMATION_CONFIG_CACHE["config"] = copy.deepcopy(normalized)
    return normalized


def remote_automation_state_cache_key(config: dict[str, Any] | None) -> str:
    current = config or {}
    return str(remote_gateway_url(current) or "").rstrip("/")


def load_cached_remote_automation_state(config: dict[str, Any]) -> tuple[dict[str, Any], float]:
    cache_key = remote_automation_state_cache_key(config)
    if not cache_key:
        return {}, 0.0
    with REMOTE_AUTOMATION_STATE_LOCK:
        cached_key = str(REMOTE_AUTOMATION_STATE_CACHE.get("url") or "")
        cached_ts = float(REMOTE_AUTOMATION_STATE_CACHE.get("ts") or 0.0)
        cached_state = REMOTE_AUTOMATION_STATE_CACHE.get("state") or {}
        if cached_key != cache_key or not isinstance(cached_state, dict) or not cached_state:
            return {}, 0.0
        return copy.deepcopy(cached_state), cached_ts


def store_cached_remote_automation_state(config: dict[str, Any], state: dict[str, Any]) -> None:
    cache_key = remote_automation_state_cache_key(config)
    if not cache_key or not isinstance(state, dict) or not state:
        return
    with REMOTE_AUTOMATION_STATE_LOCK:
        REMOTE_AUTOMATION_STATE_CACHE["ts"] = time.time()
        REMOTE_AUTOMATION_STATE_CACHE["url"] = cache_key
        REMOTE_AUTOMATION_STATE_CACHE["state"] = copy.deepcopy(state)


def stamp_runtime_state_sync(
    state: dict[str, Any],
    *,
    source: str,
    loaded: bool,
    error: str = "",
    fetched_at: str | None = None,
    age_seconds: float = 0.0,
    stale: bool = False,
) -> dict[str, Any]:
    payload = copy.deepcopy(state or {})
    payload["stateRevision"] = int(time.time() * 1000)
    payload["stateFetchedAt"] = fetched_at or now_local_iso()
    payload["stateSource"] = source
    payload["remoteStateLoaded"] = loaded
    payload["remoteStateError"] = error
    payload["remoteStateAgeSeconds"] = max(0.0, float(age_seconds or 0.0))
    payload["remoteStateStale"] = bool(stale)
    return payload


def enrich_remote_dip_swing_runtime_state(
    state: dict[str, Any],
    automation: dict[str, Any],
    public_config: dict[str, Any],
) -> dict[str, Any]:
    effective = enforce_only_dip_swing_strategy(deep_merge(default_automation_config(), automation or {}))
    sanitized = sanitize_only_dip_swing_runtime_state(state, effective)
    analysis = copy.deepcopy(sanitized.get("analysis") or {})
    candidate_count = int(analysis.get("candidateCount") or 0)
    market_candidate_count = int(analysis.get("marketCandidateCount") or 0)
    market_scan_count = int(analysis.get("marketScanCount") or 0)
    active_symbols = int(
        ((sanitized.get("lastPipeline") or {}).get("executionSummary") or {}).get("activeSymbols") or 0
    )
    selected_from_market = bool(analysis.get("selectedFromMarketScan"))

    if not analysis.get("warnings"):
        analysis["warnings"] = []
    sanitized["analysis"] = analysis

    sanitized["statusText"] = (
        "运行中" if sanitized.get("running") and bool(analysis.get("allowNewEntries")) else
        (analysis.get("decisionLabel") or "观察中") if sanitized.get("running") else
        "自动量化已停止"
    )
    sanitized["modeText"] = (
        f"{analysis.get('selectedStrategyName', f'BTC {ONLY_STRATEGY_LABEL}')}"
        f" · {analysis.get('decisionLabel', '待分析')}"
        f" · 市场候选 {market_candidate_count}/{market_scan_count}"
        + (" · 轮动接管" if selected_from_market else "")
    )

    last_pipeline = copy.deepcopy(sanitized.get("lastPipeline") or {})
    last_pipeline["summary"] = (
        f"信号 {analysis.get('decisionLabel', '待分析')} · "
        f"watchlist 候选 {candidate_count} · "
        f"市场候选 {market_candidate_count} / 扫描 {market_scan_count} · "
        f"{active_symbols} 币持仓"
    )
    sanitized["lastPipeline"] = last_pipeline

    last_applied = copy.deepcopy(sanitized.get("lastAppliedStrategy") or {})
    last_applied["title"] = analysis.get("selectedStrategyName") or f"BTC {ONLY_STRATEGY_LABEL}"
    last_applied["detail"] = strategy_detail_line(effective)
    last_applied["stage"] = "running" if bool(analysis.get("allowNewEntries")) else "synced"
    last_applied["appliedAt"] = sanitized.get("lastCycleAt") or last_applied.get("appliedAt") or now_local_iso()
    sanitized["lastAppliedStrategy"] = last_applied
    return sanitized


def summarize_basis_arb_watchlist(entries: list[dict[str, Any]]) -> dict[str, int]:
    counts = {
        "watching": 0,
        "windowOpen": 0,
        "hedged": 0,
        "exitQueue": 0,
        "rollback": 0,
        "brokenPair": 0,
        "blocked": 0,
        "reverseBasis": 0,
    }
    for entry in entries:
        summary = entry.get("summary") or {}
        stage = str(summary.get("arbStage") or "").strip().lower()
        if stage in {"window_open", "entry_wait"}:
            counts["windowOpen"] += 1
        elif stage == "reverse_basis":
            counts["reverseBasis"] += 1
        elif stage == "hedged":
            counts["hedged"] += 1
        elif stage in {"exit_wait", "exiting"}:
            counts["exitQueue"] += 1
        elif stage == "rollback":
            counts["rollback"] += 1
        elif stage == "broken_pair":
            counts["brokenPair"] += 1
        elif stage.startswith("blocked") or stage == "funding_blocked":
            counts["blocked"] += 1
        else:
            counts["watching"] += 1
    return counts


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
        "lastReconciledAt": "",
        "lastSource": "",
        "summary": {
            "totalOrders": 0,
            "filledOrders": 0,
            "workingOrders": 0,
            "canceledOrders": 0,
            "rejectedOrders": 0,
            "filledRatioPct": "0",
            "realizedPnl": "0",
            "totalFees": "0",
            "lastCancelReason": "",
        },
        "symbols": [],
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


def config_has_any_private_credentials(config: dict[str, Any] | None) -> bool:
    current = config or {}
    if any(str(current.get(key) or "").strip() for key in ("apiKey", "secretKey", "passphrase")):
        return True
    profiles = current.get("profiles") or {}
    for profile in profiles.values():
        if any(str((profile or {}).get(key) or "").strip() for key in ("apiKey", "secretKey", "passphrase")):
            return True
    return False


def is_paper_execution_order(order: dict[str, Any] | None) -> bool:
    row = order or {}
    order_id = str(row.get("ordId") or row.get("clOrdId") or "").strip()
    tag = str(row.get("tag") or "").strip().lower()
    return order_id.startswith("paper-") or tag == "paper-sim"


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


def trading_environment_changed(previous: dict[str, Any], current: dict[str, Any]) -> bool:
    keys = ("envPreset", "baseUrl", "simulated")
    for key in keys:
        if previous.get(key) != current.get(key):
            return True
    return False


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
        self._mtime_ns = 0
        self.load()

    def _path_mtime_ns(self) -> int:
        try:
            return int(self.path.stat().st_mtime_ns)
        except OSError:
            return 0

    def _remember_current_mtime(self) -> None:
        self._mtime_ns = self._path_mtime_ns()

    def load(self) -> None:
        with self.lock:
            loaded, _ = secure_load_json(self.path, self.default_factory)
            self.data = deep_merge(self.default_factory(), loaded)
            self._remember_current_mtime()

    def maybe_reload(self) -> None:
        with self.lock:
            current_mtime = self._path_mtime_ns()
            if current_mtime and current_mtime > self._mtime_ns:
                loaded, _ = secure_load_json(self.path, self.default_factory)
                self.data = deep_merge(self.default_factory(), loaded)
                self._mtime_ns = current_mtime

    def current(self) -> dict[str, Any]:
        self.maybe_reload()
        with self.lock:
            return copy.deepcopy(self.data)

    def replace(self, payload: dict[str, Any]) -> None:
        with self.lock:
            self.data = deep_merge(self.default_factory(), payload)
            secure_dump_json(self.path, self.data)
            self._remember_current_mtime()

    def update(self, mutator) -> dict[str, Any]:
        with self.lock:
            data = copy.deepcopy(self.data)
            mutator(data)
            self.data = deep_merge(self.default_factory(), data)
            secure_dump_json(self.path, self.data)
            self._remember_current_mtime()
            return copy.deepcopy(self.data)


class ConfigStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.lock = threading.RLock()
        self.runtime_config: dict[str, Any] = default_config()
        self._mtime_ns = 0
        self.load()

    def _path_mtime_ns(self) -> int:
        try:
            return int(self.path.stat().st_mtime_ns)
        except OSError:
            return 0

    def _remember_current_mtime(self) -> None:
        self._mtime_ns = self._path_mtime_ns()

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
            self._remember_current_mtime()

    def maybe_reload(self) -> None:
        with self.lock:
            current_mtime = self._path_mtime_ns()
            if current_mtime and current_mtime > self._mtime_ns:
                loaded, _ = secure_load_json(self.path, dict)
                self.runtime_config = self._normalize_state(loaded)
                self._mtime_ns = current_mtime

    def snapshot(self) -> dict[str, Any]:
        self.maybe_reload()
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
                self._remember_current_mtime()

    def current(self) -> dict[str, Any]:
        self.maybe_reload()
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


def reset_automation_live_permissions(*, reason: str = "") -> dict[str, Any]:
    def mutate(config: dict[str, Any]) -> None:
        config["autostart"] = False
        config["allowLiveManualOrders"] = False
        config["allowLiveTrading"] = False
        config["allowLiveAutostart"] = False

    updated = AUTOMATION_CONFIG.update(mutate)
    if AUTOMATION_ENGINE.snapshot().get("running"):
        AUTOMATION_ENGINE.stop(reason or "交易环境已切换，已自动停止策略并锁回实盘权限")
    return updated


def ensure_automation_permissions_match_environment(api_config: dict[str, Any]) -> dict[str, Any]:
    current = AUTOMATION_CONFIG.current()
    if not bool(api_config.get("simulated")):
        return current
    if not any(
        bool(current.get(key))
        for key in ("allowLiveManualOrders", "allowLiveTrading", "allowLiveAutostart")
    ):
        return current

    def mutate(config: dict[str, Any]) -> None:
        config["allowLiveManualOrders"] = False
        config["allowLiveTrading"] = False
        config["allowLiveAutostart"] = False

    return AUTOMATION_CONFIG.update(mutate)


class OkxApiError(RuntimeError):
    pass


def normalize_execution_order(order: dict[str, Any]) -> dict[str, Any]:
    row = copy.deepcopy(order or {})
    for key in (
        "ordId",
        "clOrdId",
        "instId",
        "instType",
        "tdMode",
        "side",
        "ordType",
        "state",
        "avgPx",
        "fillPx",
        "fillPnl",
        "realizedPnl",
        "fillFee",
        "fillNotionalUsd",
        "notionalUsd",
        "fillMarkPx",
        "fillIdxPx",
        "execType",
        "accFillSz",
        "fillSz",
        "sz",
        "lever",
        "contractValue",
        "ctVal",
        "px",
        "fee",
        "pnl",
        "subType",
        "tradeSubType",
        "tradeCount",
        "cancelSource",
        "cancelSourceReason",
        "uTime",
        "cTime",
        "fillTime",
        "tag",
    ):
        if key in row and row.get(key) is not None:
            row[key] = str(row.get(key))
    if "reduceOnly" in row:
        value = row.get("reduceOnly")
        if isinstance(value, str):
            row["reduceOnly"] = value.lower() == "true"
        else:
            row["reduceOnly"] = bool(value)
    return row


def order_decimal_metric(order: dict[str, Any], *keys: str) -> Decimal:
    first_value: Decimal | None = None
    for key in keys:
        raw = order.get(key)
        if raw in (None, ""):
            continue
        value = safe_decimal(raw, "0")
        if first_value is None:
            first_value = value
        if value != 0:
            return value
    return first_value if first_value is not None else Decimal("0")


def order_realized_pnl(order: dict[str, Any]) -> Decimal:
    return order_decimal_metric(order, "realizedPnl", "fillPnl", "pnl")


def order_total_fee(order: dict[str, Any]) -> Decimal:
    return order_decimal_metric(order, "fillFee", "fee")


def is_benign_ioc_cancel_reason(reason: Any) -> bool:
    text = str(reason or "").strip().lower()
    if not text:
        return False
    return (
        "immediate or cancel" in text
        and "was canceled" in text
        and "wasn" in text
        and "filled completely" in text
    )


def summarize_execution_cancel_reason(reason: Any) -> str:
    text = str(reason or "").strip()
    if not text:
        return ""
    lower = text.lower()
    if is_benign_ioc_cancel_reason(text):
        return "IOC 保护单未完全成交，剩余部分已自动撤销。"
    if (
        "estimated fill price exceeded the price limit" in lower
        or "slipped beyond the best bid or ask price by at least 5%" in lower
    ):
        return "滑点 / 价格保护触发，交易所取消了这笔单。"
    return text


def summarize_remote_runtime_error(error: Any) -> str:
    text = str(error or "").strip()
    lower = text.lower()
    if not text:
        return "远端执行节点暂时未回。"
    if "timed out" in lower or "timeout" in lower or "read timed out" in lower:
        return "远端执行节点暂时未回，已切到本地缓存视图。"
    if "connection refused" in lower or "failed to establish a new connection" in lower:
        return "远端执行节点未启动或端口未响应，已切到本地缓存视图。"
    if "max retries exceeded" in lower or "connectionpool" in lower:
        return "远端执行节点连接失败，已切到本地缓存视图。"
    if "403" in lower or "鉴权" in lower or "forbidden" in lower:
        return "远端执行节点鉴权失败，已切到本地缓存视图。"
    if "404" in lower or "not found" in lower:
        return "远端执行节点接口暂时不可用，已切到本地缓存视图。"
    return "远端执行节点暂时未回，已切到本地缓存视图。"


def order_execution_type(order: dict[str, Any]) -> str:
    return str(order.get("execType") or "").strip().upper()


def order_contract_value(order: dict[str, Any]) -> Decimal:
    inst_id = str(order.get("instId") or "")
    raw = order_decimal_metric(order, "contractValue", "ctVal")
    if raw > 0:
        return raw
    if str(order.get("instType") or ("SWAP" if inst_id.endswith("-SWAP") else "")).upper() == "SWAP":
        return default_swap_contract_value(inst_id)
    return Decimal("1")


def order_fill_notional_usd(order: dict[str, Any]) -> Decimal:
    direct = abs(order_decimal_metric(order, "fillNotionalUsd", "notionalUsd"))
    if direct > 0:
        return direct
    fill_px = order_decimal_metric(order, "fillPx", "avgPx", "px")
    fill_size = abs(order_decimal_metric(order, "accFillSz", "fillSz", "sz"))
    if fill_px <= 0 or fill_size <= 0:
        return Decimal("0")
    inst_type = str(order.get("instType") or ("SWAP" if str(order.get("instId") or "").endswith("-SWAP") else "")).upper()
    if inst_type == "SWAP":
        return abs(fill_px * fill_size * order_contract_value(order))
    return abs(fill_px * fill_size)


def order_slippage_cost_pct(order: dict[str, Any], *, benchmark: str = "mark") -> Decimal:
    fill_px = order_decimal_metric(order, "fillPx", "avgPx", "px")
    if fill_px <= 0:
        return Decimal("0")
    if benchmark == "index":
        reference_px = order_decimal_metric(order, "fillIdxPx")
    else:
        reference_px = order_decimal_metric(order, "fillMarkPx")
    if reference_px <= 0:
        return Decimal("0")
    side = str(order.get("side") or "").strip().lower()
    if side == "buy":
        return ((fill_px - reference_px) / reference_px) * Decimal("100")
    if side == "sell":
        return ((reference_px - fill_px) / reference_px) * Decimal("100")
    return Decimal("0")


def build_execution_cost_aggregate(orders: list[dict[str, Any]]) -> dict[str, Decimal | int]:
    maker_orders = 0
    taker_orders = 0
    filled_notional_usd = Decimal("0")
    weighted_abs_slip_mark = Decimal("0")
    weighted_abs_slip_index = Decimal("0")
    total_fees = Decimal("0")

    for order in orders or []:
        if classify_execution_order_state(order) != "filled":
            continue
        exec_type = order_execution_type(order)
        if exec_type == "M":
            maker_orders += 1
        elif exec_type == "T":
            taker_orders += 1
        fee_value = order_total_fee(order)
        total_fees += fee_value
        notional_usd = order_fill_notional_usd(order)
        if notional_usd <= 0:
            continue
        filled_notional_usd += notional_usd
        weighted_abs_slip_mark += abs(order_slippage_cost_pct(order, benchmark="mark")) * notional_usd
        weighted_abs_slip_index += abs(order_slippage_cost_pct(order, benchmark="index")) * notional_usd

    avg_abs_slip_mark_pct = (
        weighted_abs_slip_mark / filled_notional_usd if filled_notional_usd > 0 else Decimal("0")
    )
    avg_abs_slip_index_pct = (
        weighted_abs_slip_index / filled_notional_usd if filled_notional_usd > 0 else Decimal("0")
    )
    recent_fee_pct_on_notional = (
        (abs(total_fees) / filled_notional_usd) * Decimal("100") if filled_notional_usd > 0 else Decimal("0")
    )
    roundtrip_fee_floor_pct = recent_fee_pct_on_notional * Decimal("2")
    execution_cost_floor_pct = roundtrip_fee_floor_pct + max(avg_abs_slip_mark_pct, avg_abs_slip_index_pct)
    filled_count = maker_orders + taker_orders
    maker_fill_pct = (Decimal(maker_orders) / Decimal(filled_count) * Decimal("100")) if filled_count > 0 else Decimal("0")
    taker_fill_pct = (Decimal(taker_orders) / Decimal(filled_count) * Decimal("100")) if filled_count > 0 else Decimal("0")
    return {
        "makerOrders": maker_orders,
        "takerOrders": taker_orders,
        "makerFillPct": maker_fill_pct,
        "takerFillPct": taker_fill_pct,
        "filledNotionalUsd": filled_notional_usd,
        "avgAbsSlipMarkPct": avg_abs_slip_mark_pct,
        "avgAbsSlipIndexPct": avg_abs_slip_index_pct,
        "feePctOnNotional": recent_fee_pct_on_notional,
        "roundtripFeeFloorPct": roundtrip_fee_floor_pct,
        "executionCostFloorPct": execution_cost_floor_pct,
    }


def is_close_like_execution_order(order: dict[str, Any]) -> bool:
    inst_type = str(order.get("instType") or "").upper()
    side = str(order.get("side") or "").lower()
    if bool(order.get("reduceOnly")):
        return True
    trade_sub_type = str(order.get("tradeSubType") or order.get("subType") or "").strip()
    if inst_type == "SPOT":
        return side == "sell"
    if trade_sub_type in {"5", "6", "100", "101", "125", "126", "208", "209", "274", "275", "328", "329"}:
        return True
    reason = str(order.get("strategyReason") or order.get("lastMessage") or "")
    return any(token in reason for token in ("平", "止盈", "止损", "退场", "回补", "卖出"))


def build_execution_journal_insight(orders: list[dict[str, Any]], summary: dict[str, Any] | None = None) -> str:
    normalized = [normalize_execution_order(item) for item in orders or []]
    filled = [item for item in normalized if classify_execution_order_state(item) == "filled"]
    if not filled:
        return ""
    close_count = sum(1 for item in filled if is_close_like_execution_order(item))
    open_count = len(filled) - close_count
    realized_value = safe_decimal((summary or {}).get("realizedPnl"), "0")
    fee_value = safe_decimal((summary or {}).get("totalFees"), "0")
    net_value = safe_decimal((summary or {}).get("netPnl"), decimal_to_str(realized_value + fee_value))
    if close_count <= 0 and open_count > 0:
        message = f"当前这批单全是开仓，还没看到平仓回报，所以已实现收益还是 0。"
        if fee_value != 0:
            message += f" 当前已累计手续费 {format_decimal(fee_value, 4)} USDT。"
        return message
    if close_count > 0 and realized_value == 0:
        message = f"这批单里已经有 {close_count} 笔平仓，但还没拿到明确的已实现收益字段。"
        if fee_value != 0:
            message += f" 当前已累计手续费 {format_decimal(fee_value, 4)} USDT。"
        return message
    if close_count > 0 and realized_value > 0 and net_value < 0:
        message = (
            f"当前已实现盈利 {format_decimal(realized_value, 4)} USDT，"
            f"但手续费 {format_decimal(fee_value, 4)} USDT 已把净结果压到 {format_decimal(net_value, 4)} USDT。"
        )
        slip_mark_pct = safe_decimal((summary or {}).get("avgAbsSlipMarkPct"), "0")
        taker_fill_pct = safe_decimal((summary or {}).get("takerFillPct"), "0")
        if slip_mark_pct > 0 or taker_fill_pct > 0:
            message += (
                f" 近期加权滑点约 {format_decimal(slip_mark_pct, 4)}% / "
                f"taker 占比 {format_decimal(taker_fill_pct, 1)}%。"
            )
        return message
    if net_value < 0:
        slip_mark_pct = safe_decimal((summary or {}).get("avgAbsSlipMarkPct"), "0")
        taker_fill_pct = safe_decimal((summary or {}).get("takerFillPct"), "0")
        message = (
            f"当前净结果 {format_decimal(net_value, 4)} USDT。"
            f" 已实现 {format_decimal(realized_value, 4)} USDT / 手续费 {format_decimal(fee_value, 4)} USDT。"
        )
        if slip_mark_pct > 0 or taker_fill_pct > 0:
            message += (
                f" 近期加权滑点约 {format_decimal(slip_mark_pct, 4)}% / "
                f"taker 占比 {format_decimal(taker_fill_pct, 1)}%。"
            )
        return message
    if realized_value != 0:
        direction = "盈利" if realized_value > 0 else "亏损"
        return f"当前已实现{direction} {format_decimal(realized_value, 4)} USDT。"
    return ""


def execution_journal_summary_payload(journal: dict[str, Any] | None) -> dict[str, Any]:
    if not isinstance(journal, dict):
        return {}
    summary = journal.get("summary")
    if isinstance(summary, dict):
        return summary
    return journal


def execution_journal_has_paper_orders(journal: dict[str, Any] | None) -> bool:
    if not isinstance(journal, dict):
        return False
    if "paper" in str(journal.get("lastSource") or "").lower():
        return True
    for raw in journal.get("orders") or []:
        order = raw if isinstance(raw, dict) else {}
        ord_id = str(order.get("ordId") or "")
        tag = str(order.get("tag") or "")
        if ord_id.startswith("paper-") or "paper" in tag.lower():
            return True
    return False


def build_equity_display(
    account_summary: dict[str, Any] | None = None,
    automation_state: dict[str, Any] | None = None,
    execution_journal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    summary = copy.deepcopy(account_summary or {})
    session_state = automation_state or {}
    journal = execution_journal or {}
    journal_summary = execution_journal_summary_payload(journal)

    account_total_eq = safe_decimal(summary.get("displayTotalEq") or summary.get("totalEq"), "0")
    current_eq = safe_decimal(session_state.get("currentEq"), "0")
    session_start_eq = safe_decimal(session_state.get("sessionStartEq"), "0")
    uses_session_equity = current_eq > 0
    uses_paper_equity = execution_journal_has_paper_orders(journal)

    display_total_eq = current_eq if uses_session_equity else account_total_eq
    start_eq = session_start_eq if session_start_eq > 0 else (current_eq if uses_session_equity else Decimal("0"))
    has_session_pnl = uses_session_equity and start_eq > 0
    pnl_amount = (display_total_eq - start_eq) if has_session_pnl else Decimal("0")
    pnl_pct = ((pnl_amount / start_eq) * Decimal("100")) if has_session_pnl and start_eq > 0 else Decimal("0")

    realized_pnl = safe_decimal(journal_summary.get("realizedPnl"), "0")
    total_fees = safe_decimal(journal_summary.get("totalFees"), "0")
    net_pnl = safe_decimal(journal_summary.get("netPnl"), decimal_to_str(realized_pnl + total_fees))
    total_orders = int(journal_summary.get("totalOrders") or 0)
    net_result_ready = (
        total_orders > 0
        or realized_pnl != 0
        or total_fees != 0
        or net_pnl != 0
    )

    if uses_session_equity:
        source_mode = "paper" if uses_paper_equity else "session"
        source_label = "纸面权益" if uses_paper_equity else "会话权益"
        balance_breakdown = f"{source_label} {format_decimal(display_total_eq, 2)} USDT · 余额与订单统一口径"
    else:
        source_mode = "account"
        source_label = str(summary.get("displaySource") or "账户权益")
        balance_breakdown = str(summary.get("displayBreakdown") or "")

    return {
        "sourceMode": source_mode,
        "sourceLabel": source_label,
        "usesSessionEquity": uses_session_equity,
        "usesPaperEquity": uses_paper_equity,
        "displayTotalEq": decimal_to_str(display_total_eq),
        "sessionStartEq": decimal_to_str(start_eq),
        "pnlAmount": decimal_to_str(pnl_amount),
        "pnlPct": decimal_to_str(pnl_pct),
        "hasSessionPnl": has_session_pnl,
        "netResult": decimal_to_str(net_pnl),
        "realizedPnl": decimal_to_str(realized_pnl),
        "totalFees": decimal_to_str(total_fees),
        "netResultReady": net_result_ready,
        "balanceBreakdown": balance_breakdown,
        "totalOrders": total_orders,
    }


def order_event_timestamp_ms(order: dict[str, Any]) -> int:
    for key in ("uTime", "fillTime", "cTime"):
        raw = order.get(key)
        try:
            value = int(str(raw or "0").strip() or "0")
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    return 0


def build_execution_symbol_pressure_snapshot(
    inst_id: str,
    *,
    journal: dict[str, Any] | None = None,
    limit: int = 120,
    window_minutes: int = DIP_SWING_ORDER_PRESSURE_WINDOW_MINUTES,
) -> dict[str, Any]:
    journal_snapshot = journal if journal is not None else get_execution_journal_snapshot(
        limit=limit,
        live_only=prefer_live_execution_state(CONFIG.current()),
    )
    orders = [normalize_execution_order(item) for item in (journal_snapshot.get("orders") or [])]
    if not inst_id:
        return {
            "instId": "",
            "windowMinutes": int(window_minutes),
            "filledOrders": 0,
            "openOrders": 0,
            "closeOrders": 0,
            "openCloseGap": 0,
            "consecutiveOpenStreak": 0,
            "recentRealizedPnl": Decimal("0"),
            "recentTotalFees": Decimal("0"),
            "recentNetPnl": Decimal("0"),
        }
    cutoff_ms = 0
    if window_minutes > 0:
        cutoff_ms = int((time.time() - (window_minutes * 60)) * 1000)
    filtered: list[dict[str, Any]] = []
    broader: list[dict[str, Any]] = []
    for item in orders:
        if str(item.get("instId") or "") != str(inst_id):
            continue
        if classify_execution_order_state(item) != "filled":
            continue
        broader.append(item)
        if cutoff_ms > 0 and order_event_timestamp_ms(item) < cutoff_ms:
            continue
        filtered.append(item)
    if len(filtered) < 8 and broader:
        filtered = list(broader)
    filtered.sort(key=order_event_timestamp_ms, reverse=True)
    open_orders = 0
    close_orders = 0
    recent_realized = Decimal("0")
    recent_fees = Decimal("0")
    for item in filtered:
        close_like = is_close_like_execution_order(item)
        if close_like:
            close_orders += 1
        else:
            open_orders += 1
        recent_realized += order_realized_pnl(item)
        recent_fees += order_total_fee(item)
    consecutive_open_streak = 0
    for item in filtered:
        close_like = is_close_like_execution_order(item)
        if close_like:
            break
        consecutive_open_streak += 1
    cost_snapshot = build_execution_cost_aggregate(filtered)
    return {
        "instId": str(inst_id),
        "windowMinutes": int(window_minutes),
        "filledOrders": len(filtered),
        "openOrders": open_orders,
        "closeOrders": close_orders,
        "openCloseGap": max(0, open_orders - close_orders),
        "consecutiveOpenStreak": consecutive_open_streak,
        "recentRealizedPnl": recent_realized,
        "recentTotalFees": recent_fees,
        "recentNetPnl": recent_realized + recent_fees,
        "makerOrders": int(cost_snapshot.get("makerOrders") or 0),
        "takerOrders": int(cost_snapshot.get("takerOrders") or 0),
        "makerFillPct": safe_decimal(cost_snapshot.get("makerFillPct"), "0"),
        "takerFillPct": safe_decimal(cost_snapshot.get("takerFillPct"), "0"),
        "filledNotionalUsd": safe_decimal(cost_snapshot.get("filledNotionalUsd"), "0"),
        "avgAbsSlipMarkPct": safe_decimal(cost_snapshot.get("avgAbsSlipMarkPct"), "0"),
        "avgAbsSlipIndexPct": safe_decimal(cost_snapshot.get("avgAbsSlipIndexPct"), "0"),
        "feePctOnNotional": safe_decimal(cost_snapshot.get("feePctOnNotional"), "0"),
        "roundtripFeeFloorPct": safe_decimal(cost_snapshot.get("roundtripFeeFloorPct"), "0"),
        "executionCostFloorPct": safe_decimal(cost_snapshot.get("executionCostFloorPct"), "0"),
    }


def dip_swing_symbol_cycle_block_reason(
    symbol_pressure: dict[str, Any] | None,
    *,
    net_target_usdt: Decimal = DIP_SWING_NET_TARGET_USDT,
) -> str:
    pressure = symbol_pressure or {}
    open_orders = int(pressure.get("openOrders") or 0)
    close_orders = int(pressure.get("closeOrders") or 0)
    open_close_gap = int(pressure.get("openCloseGap") or 0)
    recent_net_pnl = safe_decimal(pressure.get("recentNetPnl"), "0")
    if close_orders == 0 and open_orders >= DIP_SWING_MAX_OPEN_ONLY_ORDERS_PER_SYMBOL:
        return (
            f"同币最近已连续开仓 {open_orders} 笔，"
            "还没有形成有效平仓闭环，先停止继续叠加"
        )
    if close_orders > 0 and recent_net_pnl < net_target_usdt and open_close_gap >= DIP_SWING_MAX_UNDERWATER_OPEN_GAP:
        return (
            f"同币最近净结果仅 {format_decimal(recent_net_pnl, 2)}U，"
            f"开平差还有 {open_close_gap}，先别继续放量"
        )
    return ""


def backfill_paper_execution_metrics(orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not orders:
        return []
    chronological = [copy.deepcopy(item) for item in orders]
    chronological.sort(key=lambda row: int(row.get("uTime") or row.get("cTime") or 0))
    states: dict[str, dict[str, Decimal | str]] = {}
    enriched_by_key: dict[str, dict[str, Any]] = {}

    for raw in chronological:
        row = normalize_execution_order(raw)
        key = str(row.get("ordId") or row.get("clOrdId") or "")
        if not key:
            continue
        if classify_execution_order_state(row) != "filled" or str(row.get("tag") or "") != "paper-sim":
            enriched_by_key[key] = row
            continue

        inst_id = str(row.get("instId") or "")
        inst_type = str(row.get("instType") or ("SWAP" if inst_id.endswith("-SWAP") else "SPOT")).upper()
        side = str(row.get("side") or "").lower()
        fill_px = safe_decimal(row.get("fillPx") or row.get("avgPx"), "0")
        fill_size = safe_decimal(row.get("accFillSz") or row.get("fillSz") or row.get("sz"), "0")
        reduce_only = bool(row.get("reduceOnly"))
        if fill_px <= 0 or fill_size <= 0 or side not in {"buy", "sell"}:
            enriched_by_key[key] = row
            continue

        state = states.setdefault(
            inst_id,
            {
                "positionSide": "flat",
                "positionSize": Decimal("0"),
                "entryPrice": Decimal("0"),
            },
        )
        position_side = str(state.get("positionSide") or "flat")
        position_size = safe_decimal(state.get("positionSize"), "0")
        entry_price = safe_decimal(state.get("entryPrice"), "0")
        realized = Decimal("0")
        fee = order_total_fee(row)
        if inst_type == "SWAP":
            ct_val = safe_decimal(row.get("contractValue"), decimal_to_str(default_swap_contract_value(inst_id)))
            if fee == 0:
                fee = -(fill_size * ct_val * fill_px * PAPER_SWAP_FEE_RATE)
            if side == "buy":
                row["tradeSubType"] = "6" if position_side == "short" and position_size > 0 else "3"
                if position_side == "short" and position_size > 0:
                    close_size = min(position_size, fill_size)
                    realized = (entry_price - fill_px) * close_size * ct_val
                if position_side == "short":
                    closed = min(position_size, fill_size)
                    remaining = max(Decimal("0"), position_size - closed)
                    if reduce_only or fill_size <= position_size:
                        state["positionSize"] = remaining
                        state["positionSide"] = "flat" if remaining <= 0 else "short"
                        state["entryPrice"] = Decimal("0") if remaining <= 0 else entry_price
                    else:
                        opened = fill_size - position_size
                        state["positionSize"] = opened
                        state["positionSide"] = "long"
                        state["entryPrice"] = fill_px if opened > 0 else Decimal("0")
                else:
                    next_size = position_size if reduce_only else position_size + fill_size
                    state["positionSize"] = next_size
                    state["positionSide"] = "flat" if next_size <= 0 else "long"
                    if reduce_only or next_size <= 0:
                        state["entryPrice"] = Decimal("0")
                    elif position_side == "long" and position_size > 0 and entry_price > 0:
                        state["entryPrice"] = ((entry_price * position_size) + (fill_px * fill_size)) / next_size
                    else:
                        state["entryPrice"] = fill_px
            else:
                row["tradeSubType"] = "5" if position_side == "long" and position_size > 0 else "4"
                if position_side == "long" and position_size > 0:
                    close_size = min(position_size, fill_size)
                    realized = (fill_px - entry_price) * close_size * ct_val
                if position_side == "long":
                    closed = min(position_size, fill_size)
                    remaining = max(Decimal("0"), position_size - closed)
                    if reduce_only or fill_size <= position_size:
                        state["positionSize"] = remaining
                        state["positionSide"] = "flat" if remaining <= 0 else "long"
                        state["entryPrice"] = Decimal("0") if remaining <= 0 else entry_price
                    else:
                        opened = fill_size - position_size
                        state["positionSize"] = opened
                        state["positionSide"] = "short"
                        state["entryPrice"] = fill_px if opened > 0 else Decimal("0")
                else:
                    next_size = position_size if reduce_only else position_size + fill_size
                    state["positionSize"] = next_size
                    state["positionSide"] = "flat" if next_size <= 0 else "short"
                    if reduce_only or next_size <= 0:
                        state["entryPrice"] = Decimal("0")
                    elif position_side == "short" and position_size > 0 and entry_price > 0:
                        state["entryPrice"] = ((entry_price * position_size) + (fill_px * fill_size)) / next_size
                    else:
                        state["entryPrice"] = fill_px
        else:
            if fee == 0:
                fee = -(fill_size * fill_px * PAPER_SPOT_FEE_RATE)
            row["tradeSubType"] = "1" if side == "buy" else "2"
            if side == "buy":
                next_size = position_size + fill_size
                if next_size > 0:
                    if position_size > 0 and entry_price > 0:
                        state["entryPrice"] = ((entry_price * position_size) + (fill_px * fill_size)) / next_size
                    else:
                        state["entryPrice"] = fill_px
                state["positionSize"] = next_size
                state["positionSide"] = "flat" if next_size <= 0 else "long"
            else:
                close_size = min(position_size, fill_size)
                if position_size > 0 and entry_price > 0:
                    realized = (fill_px - entry_price) * close_size
                remaining = max(Decimal("0"), position_size - close_size)
                state["positionSize"] = remaining
                state["positionSide"] = "flat" if remaining <= 0 else "long"
                state["entryPrice"] = Decimal("0") if remaining <= 0 else entry_price

        if "fee" not in row or safe_decimal(row.get("fee"), "0") == 0:
            row["fee"] = decimal_to_str(fee)
        row["fillFee"] = decimal_to_str(fee)
        row["pnl"] = decimal_to_str(realized)
        row["fillPnl"] = decimal_to_str(realized)
        row["realizedPnl"] = decimal_to_str(realized)
        enriched_by_key[key] = row

    enriched: list[dict[str, Any]] = []
    for raw in orders:
        key = str(raw.get("ordId") or raw.get("clOrdId") or "")
        enriched.append(copy.deepcopy(enriched_by_key.get(key, normalize_execution_order(raw))))
    return enriched


def classify_execution_order_state(order: dict[str, Any]) -> str:
    state = str(order.get("state") or "").strip().lower()
    if state == "filled":
        return "filled"
    if state in {"live", "partially_filled", "partially-filled", "pending"}:
        return "working"
    if state in {"canceled", "cancelled", "mmp_canceled"}:
        return "canceled"
    if state in {"rejected", "failed", "order_failed"}:
        return "rejected"
    return "other"


def is_non_blocking_execution_order(order: dict[str, Any]) -> bool:
    ord_type = str(order.get("ordType") or "").strip().lower()
    return ord_type in DIP_SWING_NON_BLOCKING_ORDER_TYPES


def execution_tag_family(order: dict[str, Any]) -> str:
    tag = str(order.get("tag") or order.get("strategyTag") or "").strip().lower()
    reason = str(order.get("strategyReason") or order.get("lastMessage") or "").strip()
    is_arb_order = tag.startswith("arb_") or ("套利" in reason)
    if not is_arb_order:
        return ""
    action = str(order.get("strategyAction") or "").strip().lower()
    if action in {"entry", "hedge", "exit", "cover", "rollback"}:
        if action == "cover":
            return "exit"
        return action
    if "entry" in tag:
        return "entry"
    if "hedge" in tag:
        return "hedge"
    if "cover" in tag or "exit" in tag:
        return "exit"
    if "rollback" in tag or tag.endswith("_rb"):
        return "rollback"
    return "arb"


def build_execution_journal_summary(orders: list[dict[str, Any]]) -> dict[str, Any]:
    normalized_orders = backfill_paper_execution_metrics([normalize_execution_order(item) for item in orders])
    total_orders = len(normalized_orders)
    filled_orders = 0
    working_orders = 0
    canceled_orders = 0
    rejected_orders = 0
    open_orders = 0
    close_orders = 0
    winning_close_orders = 0
    losing_close_orders = 0
    breakeven_close_orders = 0
    realized_pnl = Decimal("0")
    total_fees = Decimal("0")
    last_cancel_reason = ""
    arb_order_count = 0
    arb_entry_orders = 0
    arb_hedge_orders = 0
    arb_exit_orders = 0
    arb_rollback_orders = 0
    arb_realized_pnl = Decimal("0")
    arb_total_fees = Decimal("0")
    symbol_rows: dict[str, dict[str, Any]] = {}

    for order in normalized_orders:
        kind = classify_execution_order_state(order)
        tag_family = execution_tag_family(order)
        realized_value = order_realized_pnl(order)
        fee_value = order_total_fee(order)
        close_like = is_close_like_execution_order(order)
        if kind == "filled":
            filled_orders += 1
            if close_like:
                close_orders += 1
                net_value = realized_value + fee_value
                if net_value > 0:
                    winning_close_orders += 1
                elif net_value < 0:
                    losing_close_orders += 1
                else:
                    breakeven_close_orders += 1
            else:
                open_orders += 1
        elif kind == "working":
            working_orders += 1
        elif kind == "canceled":
            canceled_orders += 1
            if not last_cancel_reason:
                last_cancel_reason = summarize_execution_cancel_reason(
                    order.get("cancelSourceReason") or order.get("outcome") or ""
                )
        elif kind == "rejected":
            rejected_orders += 1
            if not last_cancel_reason:
                last_cancel_reason = summarize_execution_cancel_reason(
                    order.get("cancelSourceReason") or order.get("outcome") or ""
                )

        realized_pnl += realized_value
        total_fees += fee_value
        if tag_family:
            arb_order_count += 1
            arb_realized_pnl += realized_value
            arb_total_fees += fee_value
            if tag_family == "entry":
                arb_entry_orders += 1
            elif tag_family == "hedge":
                arb_hedge_orders += 1
            elif tag_family == "exit":
                arb_exit_orders += 1
            elif tag_family == "rollback":
                arb_rollback_orders += 1

        symbol = str(order.get("instId") or "UNKNOWN").strip() or "UNKNOWN"
        target = symbol_rows.setdefault(
            symbol,
            {
                "symbol": symbol,
                "orderCount": 0,
                "filledOrders": 0,
                "workingOrders": 0,
                "canceledOrders": 0,
                "rejectedOrders": 0,
                "realizedPnl": "0",
                "totalFees": "0",
                "lastState": "",
                "lastTime": "0",
                "arbOrderCount": 0,
                "arbEntryOrders": 0,
                "arbHedgeOrders": 0,
                "arbExitOrders": 0,
                "arbRollbackOrders": 0,
                "arbRealizedPnl": "0",
                "arbTotalFees": "0",
            },
        )
        target["orderCount"] = int(target["orderCount"]) + 1
        if kind == "filled":
            target["filledOrders"] = int(target["filledOrders"]) + 1
        elif kind == "working":
            target["workingOrders"] = int(target["workingOrders"]) + 1
        elif kind == "canceled":
            target["canceledOrders"] = int(target["canceledOrders"]) + 1
        elif kind == "rejected":
            target["rejectedOrders"] = int(target["rejectedOrders"]) + 1
        target["realizedPnl"] = decimal_to_str(safe_decimal(target.get("realizedPnl"), "0") + realized_value)
        target["totalFees"] = decimal_to_str(safe_decimal(target.get("totalFees"), "0") + fee_value)
        if tag_family:
            target["arbOrderCount"] = int(target.get("arbOrderCount") or 0) + 1
            target["arbRealizedPnl"] = decimal_to_str(safe_decimal(target.get("arbRealizedPnl"), "0") + realized_value)
            target["arbTotalFees"] = decimal_to_str(safe_decimal(target.get("arbTotalFees"), "0") + fee_value)
            if tag_family == "entry":
                target["arbEntryOrders"] = int(target.get("arbEntryOrders") or 0) + 1
            elif tag_family == "hedge":
                target["arbHedgeOrders"] = int(target.get("arbHedgeOrders") or 0) + 1
            elif tag_family == "rollback":
                target["arbRollbackOrders"] = int(target.get("arbRollbackOrders") or 0) + 1
            elif tag_family in {"exit", "cover"}:
                target["arbExitOrders"] = int(target.get("arbExitOrders") or 0) + 1
        last_time = str(order.get("uTime") or order.get("cTime") or "0")
        if int(last_time or 0) >= int(target.get("lastTime") or 0):
            target["lastState"] = str(order.get("state") or "")
            target["lastTime"] = last_time

    filled_ratio = Decimal("0")
    terminal_orders = filled_orders + canceled_orders + rejected_orders
    if terminal_orders > 0:
        filled_ratio = (Decimal(filled_orders) / Decimal(terminal_orders)) * Decimal("100")
    close_win_rate = Decimal("0")
    if close_orders > 0:
        close_win_rate = (Decimal(winning_close_orders) / Decimal(close_orders)) * Decimal("100")
    net_pnl = realized_pnl + total_fees
    cost_aggregate = build_execution_cost_aggregate(normalized_orders)
    orders_by_symbol: dict[str, list[dict[str, Any]]] = {}
    for order in normalized_orders:
        symbol = str(order.get("instId") or "UNKNOWN").strip() or "UNKNOWN"
        orders_by_symbol.setdefault(symbol, []).append(order)

    symbols = list(symbol_rows.values())
    for row in symbols:
        symbol_cost_aggregate = build_execution_cost_aggregate(orders_by_symbol.get(str(row.get("symbol") or "UNKNOWN"), []))
        row["makerOrders"] = int(symbol_cost_aggregate.get("makerOrders") or 0)
        row["takerOrders"] = int(symbol_cost_aggregate.get("takerOrders") or 0)
        row["makerFillPct"] = decimal_to_str(safe_decimal(symbol_cost_aggregate.get("makerFillPct"), "0"))
        row["takerFillPct"] = decimal_to_str(safe_decimal(symbol_cost_aggregate.get("takerFillPct"), "0"))
        row["avgAbsSlipMarkPct"] = decimal_to_str(safe_decimal(symbol_cost_aggregate.get("avgAbsSlipMarkPct"), "0"))
        row["avgAbsSlipIndexPct"] = decimal_to_str(safe_decimal(symbol_cost_aggregate.get("avgAbsSlipIndexPct"), "0"))
        row["feePctOnNotional"] = decimal_to_str(safe_decimal(symbol_cost_aggregate.get("feePctOnNotional"), "0"))
        row["executionCostFloorPct"] = decimal_to_str(safe_decimal(symbol_cost_aggregate.get("executionCostFloorPct"), "0"))
    symbols.sort(key=lambda row: int(row.get("lastTime") or 0), reverse=True)
    insight = build_execution_journal_insight(normalized_orders, {
        "realizedPnl": decimal_to_str(realized_pnl),
        "totalFees": decimal_to_str(total_fees),
        "avgAbsSlipMarkPct": decimal_to_str(safe_decimal(cost_aggregate.get("avgAbsSlipMarkPct"), "0")),
        "takerFillPct": decimal_to_str(safe_decimal(cost_aggregate.get("takerFillPct"), "0")),
    })
    return {
        "totalOrders": total_orders,
        "filledOrders": filled_orders,
        "workingOrders": working_orders,
        "canceledOrders": canceled_orders,
        "rejectedOrders": rejected_orders,
        "openOrders": open_orders,
        "closeOrders": close_orders,
        "winningCloseOrders": winning_close_orders,
        "losingCloseOrders": losing_close_orders,
        "breakevenCloseOrders": breakeven_close_orders,
        "closeWinRatePct": decimal_to_str(close_win_rate),
        "filledRatioPct": decimal_to_str(filled_ratio),
        "realizedPnl": decimal_to_str(realized_pnl),
        "totalFees": decimal_to_str(total_fees),
        "netPnl": decimal_to_str(net_pnl),
        "makerOrders": int(cost_aggregate.get("makerOrders") or 0),
        "takerOrders": int(cost_aggregate.get("takerOrders") or 0),
        "makerFillPct": decimal_to_str(safe_decimal(cost_aggregate.get("makerFillPct"), "0")),
        "takerFillPct": decimal_to_str(safe_decimal(cost_aggregate.get("takerFillPct"), "0")),
        "filledNotionalUsd": decimal_to_str(safe_decimal(cost_aggregate.get("filledNotionalUsd"), "0")),
        "avgAbsSlipMarkPct": decimal_to_str(safe_decimal(cost_aggregate.get("avgAbsSlipMarkPct"), "0")),
        "avgAbsSlipIndexPct": decimal_to_str(safe_decimal(cost_aggregate.get("avgAbsSlipIndexPct"), "0")),
        "feePctOnNotional": decimal_to_str(safe_decimal(cost_aggregate.get("feePctOnNotional"), "0")),
        "roundtripFeeFloorPct": decimal_to_str(safe_decimal(cost_aggregate.get("roundtripFeeFloorPct"), "0")),
        "executionCostFloorPct": decimal_to_str(safe_decimal(cost_aggregate.get("executionCostFloorPct"), "0")),
        "lastCancelReason": last_cancel_reason,
        "arbOrderCount": arb_order_count,
        "arbEntryOrders": arb_entry_orders,
        "arbHedgeOrders": arb_hedge_orders,
        "arbExitOrders": arb_exit_orders,
        "arbRollbackOrders": arb_rollback_orders,
        "arbRealizedPnl": decimal_to_str(arb_realized_pnl),
        "arbTotalFees": decimal_to_str(arb_total_fees),
        "arbNetPnl": decimal_to_str(arb_realized_pnl + arb_total_fees),
        "insight": insight,
        "symbols": symbols,
    }


def persist_local_orders(
    orders: list[dict[str, Any]],
    *,
    source: str = "",
    live_only: bool = False,
) -> list[dict[str, Any]]:
    if not orders:
        return get_local_recent_orders(limit=80)

    def mutate(state: dict[str, Any]) -> None:
        existing = list(state.get("orders") or [])
        merged = [normalize_execution_order(item) for item in list(orders) + existing]
        if live_only:
            merged = [item for item in merged if not is_paper_execution_order(item)]
        deduped: list[dict[str, Any]] = []
        seen = set()
        for item in merged:
            key = item.get("ordId") or item.get("clOrdId")
            if not key or key in seen:
                continue
            seen.add(key)
            deduped.append(copy.deepcopy(item))
            if len(deduped) >= MAX_RECENT_ORDER_LIMIT:
                break
        deduped.sort(
            key=lambda row: int(row.get("uTime") or row.get("cTime") or 0),
            reverse=True,
        )
        state["orders"] = deduped
        state["lastReconciledAt"] = now_local_iso()
        if source:
            state["lastSource"] = source
        summary = build_execution_journal_summary(deduped)
        state["summary"] = {
            key: value for key, value in summary.items() if key != "symbols"
        }
        state["symbols"] = summary.get("symbols") or []

    current = LOCAL_ORDER_STORE.update(mutate)
    return list(current.get("orders") or [])


def get_stored_local_orders(inst_type: str = "", limit: int = 20, *, live_only: bool = False) -> list[dict[str, Any]]:
    state = LOCAL_ORDER_STORE.current()
    items = list(state.get("orders") or [])
    if live_only:
        items = [item for item in items if not is_paper_execution_order(item)]
    if inst_type:
        expected = inst_type.upper()
        items = [item for item in items if str(item.get("instType") or "").upper() == expected]
    items.sort(
        key=lambda row: int(row.get("uTime") or row.get("cTime") or 0),
        reverse=True,
    )
    return items[:limit]


def get_execution_journal_orders(
    inst_type: str = "",
    limit: int = 80,
    *,
    live_only: bool = False,
) -> tuple[list[dict[str, Any]], str, str]:
    state = LOCAL_ORDER_STORE.current()
    stored_orders = get_stored_local_orders(inst_type, limit=MAX_RECENT_ORDER_LIMIT, live_only=live_only)
    stream_orders = PRIVATE_ORDER_STREAM.get_recent_orders(inst_type, limit=MAX_RECENT_ORDER_LIMIT)
    if live_only:
        stream_orders = [item for item in stream_orders if not is_paper_execution_order(item)]
    fallback_orders = derive_orders_from_automation_state()
    if inst_type:
        expected = inst_type.upper()
        fallback_orders = [item for item in fallback_orders if str(item.get("instType") or "").upper() == expected]
    if live_only:
        fallback_orders = [item for item in fallback_orders if not is_paper_execution_order(item)]
    merged_orders = merge_order_feeds(stream_orders, stored_orders, fallback_orders, limit=MAX_RECENT_ORDER_LIMIT)
    merged_orders = backfill_paper_execution_metrics(merged_orders)
    source_parts: list[str] = []
    if stream_orders:
        source_parts.append("private_ws")
    stored_source = str(state.get("lastSource") or "").strip()
    if stored_source:
        source_parts.append(stored_source)
    if fallback_orders and not source_parts:
        source_parts.append("paper_state_recovered")
    source_label = "+".join(dict.fromkeys(source_parts)) if source_parts else ""
    last_reconciled = str(state.get("lastReconciledAt") or "")
    stream_last_event = str((PRIVATE_ORDER_STREAM.snapshot() or {}).get("lastEventAt") or "")
    if stream_last_event and stream_last_event > last_reconciled:
        last_reconciled = stream_last_event
    return merged_orders[:limit], source_label, last_reconciled


def get_execution_journal_snapshot(
    inst_type: str = "",
    limit: int = 20,
    *,
    live_only: bool = False,
) -> dict[str, Any]:
    orders, source_label, last_reconciled = get_execution_journal_orders(inst_type, limit=limit, live_only=live_only)
    summary = build_execution_journal_summary(orders)
    return {
        "orders": orders,
        "summary": {key: value for key, value in summary.items() if key != "symbols"},
        "symbols": summary.get("symbols") or [],
        "lastReconciledAt": last_reconciled,
        "lastSource": source_label,
    }


def prefer_live_execution_state(config: dict[str, Any] | None = None) -> bool:
    current = config or CONFIG.current()
    if is_remote_execution_enabled(current):
        return True
    if config_has_any_private_credentials(current):
        return True
    return any(
        str(current.get(mask_key) or "").strip()
        for mask_key in ("apiKeyMask", "secretKeyMask", "passphraseMask")
    )


def aggregate_recent_fills_by_order(fills: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[str, dict[str, Any]] = {}
    for raw in fills or []:
        row = normalize_execution_order(raw)
        order_id = str(row.get("ordId") or "").strip()
        if not order_id:
            continue
        target = grouped.setdefault(
            order_id,
            {
                "fillPnl": Decimal("0"),
                "fillFee": Decimal("0"),
                "fillSz": Decimal("0"),
                "tradeCount": 0,
                "subType": "",
                "fillTime": "0",
            },
        )
        target["fillPnl"] += safe_decimal(row.get("fillPnl"), "0")
        target["fillFee"] += safe_decimal(row.get("fee"), "0")
        target["fillSz"] += safe_decimal(row.get("fillSz"), "0")
        target["tradeCount"] = int(target.get("tradeCount") or 0) + 1
        fill_time = str(row.get("fillTime") or row.get("uTime") or row.get("cTime") or "0")
        if int(fill_time or 0) >= int(target.get("fillTime") or 0):
            target["fillTime"] = fill_time
            target["subType"] = str(row.get("subType") or "")
    return grouped


def enrich_execution_orders_with_fills(
    orders: list[dict[str, Any]],
    fills: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    if not orders:
        return []
    grouped = aggregate_recent_fills_by_order(fills)
    if not grouped:
        return [normalize_execution_order(item) for item in orders]
    enriched: list[dict[str, Any]] = []
    for raw in orders:
        row = normalize_execution_order(raw)
        order_id = str(row.get("ordId") or "").strip()
        fill_meta = grouped.get(order_id)
        if not fill_meta:
            enriched.append(row)
            continue
        pnl_value = decimal_to_str(fill_meta["fillPnl"])
        fee_value = decimal_to_str(fill_meta["fillFee"])
        row["fillPnl"] = pnl_value
        row["fillFee"] = fee_value
        row["tradeCount"] = str(fill_meta["tradeCount"])
        row["tradeSubType"] = str(fill_meta.get("subType") or "")
        if str(row.get("fillTime") or "") in {"", "0"} and str(fill_meta.get("fillTime") or "") not in {"", "0"}:
            row["fillTime"] = str(fill_meta["fillTime"])
        if order_realized_pnl(row) == 0 and fill_meta["fillPnl"] != 0:
            row["realizedPnl"] = pnl_value
        if order_total_fee(row) == 0 and fill_meta["fillFee"] != 0:
            row["fee"] = fee_value
        enriched.append(row)
    return enriched


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


def default_swap_contract_value(inst_id: str) -> Decimal:
    base = str(inst_id or "").split("-")[0].upper()
    if base == "BTC":
        return Decimal("0.01")
    if base == "ETH":
        return Decimal("0.1")
    return Decimal("1")


def paper_market_contract_value(market: dict[str, Any] | None) -> Decimal:
    snapshot = market or {}
    inst_id = str(snapshot.get("instId") or "")
    return safe_decimal(snapshot.get("contractValue"), decimal_to_str(default_swap_contract_value(inst_id)))


def paper_swap_unrealized_pnl(market: dict[str, Any] | None) -> Decimal:
    snapshot = market or {}
    position_side = str(snapshot.get("positionSide") or "flat")
    position_size = safe_decimal(snapshot.get("positionSize"), "0")
    entry_price = safe_decimal(snapshot.get("entryPrice"), "0")
    last_price = safe_decimal(snapshot.get("lastPrice"), "0")
    ct_val = paper_market_contract_value(snapshot)
    if position_side == "flat" or position_size <= 0 or entry_price <= 0 or last_price <= 0 or ct_val <= 0:
        return Decimal("0")
    direction = Decimal("1") if position_side == "long" else Decimal("-1")
    return direction * (last_price - entry_price) * position_size * ct_val


def paper_swap_market_key(inst_id: str) -> str:
    return f"swap:{str(inst_id or '').strip()}"


def iter_paper_swap_markets(markets: dict[str, Any] | None) -> list[dict[str, Any]]:
    source = markets or {}
    specific_rows = [
        copy.deepcopy(value)
        for key, value in source.items()
        if str(key).startswith("swap:") and isinstance(value, dict)
    ]
    if specific_rows:
        return specific_rows
    generic = source.get("swap")
    return [copy.deepcopy(generic)] if isinstance(generic, dict) and generic else []


DEFAULT_RECENT_ORDER_LIMIT = 80
MAX_RECENT_ORDER_LIMIT = 200


def parse_recent_order_limit(query: dict[str, list[str]] | None, default: int = DEFAULT_RECENT_ORDER_LIMIT) -> int:
    raw = ""
    if query:
        raw = str((query.get("limit") or [default])[0] or "")
    try:
        parsed = int(raw or default)
    except Exception:
        parsed = default
    return max(20, min(parsed, MAX_RECENT_ORDER_LIMIT))


def get_local_recent_orders(inst_type: str = "", limit: int = DEFAULT_RECENT_ORDER_LIMIT) -> list[dict[str, Any]]:
    stored = get_stored_local_orders(inst_type, limit=limit)
    if stored:
        return stored
    derived = derive_orders_from_automation_state()
    if inst_type:
        expected = inst_type.upper()
        derived = [item for item in derived if str(item.get("instType") or "").upper() == expected]
    return derived[:limit]


def merge_order_feeds(*feeds: list[dict[str, Any]], limit: int = DEFAULT_RECENT_ORDER_LIMIT) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    seen = set()
    for feed in feeds:
        for item in feed:
            item = normalize_execution_order(item)
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
        persist_local_orders(derived, source="paper_state_recovered")

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

    def _has_private_credentials(self) -> bool:
        return bool(
            str(self.api_key or "").strip()
            and str(self.secret_key or "").strip()
            and str(self.passphrase or "").strip()
        )

    def _paper_state_authoritative(self) -> bool:
        return (
            self._paper_enabled()
            and not config_has_any_private_credentials(CONFIG.current())
            and paper_state_has_activity(self._paper_state())
        )

    def _paper_read_fallback_allowed(self) -> bool:
        return self._paper_enabled() and not config_has_any_private_credentials(CONFIG.current())

    def _paper_trading_fallback_allowed(self) -> bool:
        if str(CONFIG.current().get("executionMode") or "local").strip() == "remote":
            return False
        return self._paper_enabled() and not config_has_any_private_credentials(CONFIG.current())

    def _public_market_fallback_allowed(self) -> bool:
        if str(CONFIG.current().get("executionMode") or "local").strip() == "remote":
            return False
        return self._paper_read_fallback_allowed()

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
        spot_market = (state.get("markets") or {}).get("spot") or {}
        swap_markets = iter_paper_swap_markets((state.get("markets") or {}))
        total_eq = self._paper_total_eq() + sum(
            (paper_swap_unrealized_pnl(item) for item in swap_markets),
            Decimal("0"),
        )
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
        markets = (state.get("markets") or {})
        candidate_markets: list[dict[str, Any]]
        if inst_id:
            specific = markets.get(paper_swap_market_key(inst_id))
            if isinstance(specific, dict) and specific:
                candidate_markets = [specific]
            else:
                generic = markets.get("swap") or {}
                candidate_markets = [generic] if str(generic.get("instId") or "") == str(inst_id) else []
        else:
            candidate_markets = iter_paper_swap_markets(markets)
        rows: list[dict[str, Any]] = []
        for swap_market in candidate_markets:
            position_side = str(swap_market.get("positionSide") or "flat")
            position_size = safe_decimal(swap_market.get("positionSize"), "0")
            if position_side == "flat" or position_size <= 0:
                continue
            signed_pos = position_size if position_side == "long" else -position_size
            rows.append(
                {
                    "instId": str(swap_market.get("instId") or inst_id or AUTOMATION_CONFIG.current().get("swapInstId") or ""),
                    "pos": decimal_to_str(signed_pos),
                    "avgPx": str(swap_market.get("entryPrice") or ""),
                    "posSide": "net",
                    "upl": decimal_to_str(paper_swap_unrealized_pnl(swap_market)),
                }
            )
        return {"code": "0", "data": rows}

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
        current_eq_before = self._paper_total_eq()
        realized_pnl = Decimal("0")
        order_fee = Decimal("0")
        trade_sub_type = "1" if side == "buy" else "2"
        reduce_only = bool(payload.get("reduceOnly"))

        if is_swap:
            market_key = "swap"
            inst_market_key = paper_swap_market_key(inst_id)
            current = copy.deepcopy(markets.get(inst_market_key) or default_market_state())
            if not current.get("instId"):
                generic_current = markets.get("swap") or {}
                if str(generic_current.get("instId") or "") == inst_id:
                    current = copy.deepcopy(generic_current)
            current_side = str(current.get("positionSide") or "flat")
            current_size = safe_decimal(current.get("positionSize"), "0")
            current_entry = safe_decimal(current.get("entryPrice"), "0")
            reverse_entry_blocked = (
                not reduce_only
                and current_size > 0
                and (
                    (current_side == "long" and side == "sell")
                    or (current_side == "short" and side == "buy")
                )
            )
            if reverse_entry_blocked:
                blocked_order = {
                    "ordId": ord_id,
                    "clOrdId": cl_ord_id,
                    "instId": inst_id,
                    "instType": "SWAP",
                    "tdMode": str(payload.get("tdMode") or "cross"),
                    "side": side,
                    "ordType": str(payload.get("ordType") or "market"),
                    "state": "canceled",
                    "sz": decimal_to_str(safe_decimal(payload.get("sz"), "0")),
                    "fillSz": "0",
                    "accFillSz": "0",
                    "avgPx": "",
                    "fillPx": "",
                    "px": str(payload.get("px") or ""),
                    "fee": "0",
                    "fillFee": "0",
                    "pnl": "0",
                    "fillPnl": "0",
                    "realizedPnl": "0",
                    "reduceOnly": reduce_only,
                    "posSide": "net",
                    "tradeSubType": "",
                    "uTime": now_ms,
                    "cTime": now_ms,
                    "tag": "paper-sim",
                    "cancelSourceReason": "paper-sim reverse entry blocked before prior leg reached net +1U",
                    "sMsg": "paper-sim reverse entry blocked before prior leg reached net +1U",
                }
                PRIVATE_ORDER_STREAM._ingest_orders([blocked_order])
                return {"code": "0", "data": [blocked_order], "_paperSim": True}
            meta = (self.get_public_instruments("SWAP", inst_id).get("data") or [{}])[0]
            ct_val = safe_decimal(meta.get("ctVal"), decimal_to_str(default_swap_contract_value(inst_id)))
            order_fee = -(filled_size * ct_val * fill_px * PAPER_SWAP_FEE_RATE)
            next_entry = current_entry
            if side == "buy":
                if current_side == "short":
                    close_size = min(current_size, filled_size)
                    realized_pnl = (current_entry - fill_px) * close_size * ct_val
                    remaining_size = max(Decimal("0"), current_size - close_size)
                    if reduce_only or filled_size <= current_size:
                        next_size = remaining_size
                        next_side = "flat" if next_size <= 0 else "short"
                        next_entry = current_entry if next_size > 0 else Decimal("0")
                        trade_sub_type = "6"
                    else:
                        open_size = filled_size - current_size
                        next_size = open_size
                        next_side = "long"
                        next_entry = fill_px
                        trade_sub_type = "6"
                else:
                    next_size = current_size if reduce_only else current_size + filled_size
                    next_side = "flat" if next_size <= 0 else "long"
                    if reduce_only:
                        next_entry = Decimal("0")
                    elif current_side == "long" and current_size > 0 and current_entry > 0 and next_size > 0:
                        next_entry = ((current_entry * current_size) + (fill_px * filled_size)) / next_size
                    else:
                        next_entry = fill_px if next_size > 0 else Decimal("0")
                    trade_sub_type = "3"
            else:
                if current_side == "long":
                    close_size = min(current_size, filled_size)
                    realized_pnl = (fill_px - current_entry) * close_size * ct_val
                    remaining_size = max(Decimal("0"), current_size - close_size)
                    if reduce_only or filled_size <= current_size:
                        next_size = remaining_size
                        next_side = "flat" if next_size <= 0 else "long"
                        next_entry = current_entry if next_size > 0 else Decimal("0")
                        trade_sub_type = "5"
                    else:
                        open_size = filled_size - current_size
                        next_size = open_size
                        next_side = "short"
                        next_entry = fill_px
                        trade_sub_type = "5"
                else:
                    next_size = current_size if reduce_only else current_size + filled_size
                    next_side = "flat" if next_size <= 0 else "short"
                    if reduce_only:
                        next_entry = Decimal("0")
                    elif current_side == "short" and current_size > 0 and current_entry > 0 and next_size > 0:
                        next_entry = ((current_entry * current_size) + (fill_px * filled_size)) / next_size
                    else:
                        next_entry = fill_px if next_size > 0 else Decimal("0")
                    trade_sub_type = "4"

            current_eq_after = current_eq_before + realized_pnl + order_fee

            def mutate(current_state: dict[str, Any]) -> None:
                market = current_state["markets"].setdefault(market_key, default_market_state())
                patch = {
                    "enabled": True,
                    "instId": inst_id,
                    "positionSide": next_side,
                    "positionSize": decimal_to_str(next_size),
                    "positionNotional": decimal_to_str(next_size * fill_px * ct_val),
                    "entryPrice": "" if next_side == "flat" else decimal_to_str(next_entry),
                    "lastPrice": decimal_to_str(fill_px),
                    "floatingPnl": "0",
                    "floatingPnlPct": "0",
                    "lastTradeAt": now_local_iso(),
                    "lastActionAt": now_local_iso(),
                    "lastOrderId": ord_id,
                    "contractValue": decimal_to_str(ct_val),
                }
                market.update(patch)
                current_state["markets"].setdefault(inst_market_key, default_market_state()).update(copy.deepcopy(patch))
                if not current_state.get("sessionStartEq"):
                    current_state["sessionStartEq"] = decimal_to_str(current_eq_before)
                current_state["currentEq"] = decimal_to_str(current_eq_after)

            AUTOMATION_STATE.update(mutate)
            inst_type = "SWAP"
            pos_side = "net"
        else:
            market_key = "spot"
            current = copy.deepcopy(markets.get("spot") or default_market_state())
            current_size = safe_decimal(current.get("positionSize"), "0")
            current_entry = safe_decimal(current.get("entryPrice"), "0")
            if side == "buy":
                quote_budget = filled_size
                base_fill = (quote_budget / fill_px) if fill_px > 0 else Decimal("0")
                next_size = current_size + base_fill
                actual_fill_size = base_fill
                next_entry = (
                    ((current_entry * current_size) + (fill_px * base_fill)) / next_size
                    if current_size > 0 and current_entry > 0 and next_size > 0
                    else fill_px if next_size > 0 else Decimal("0")
                )
                order_fee = -(quote_budget * PAPER_SPOT_FEE_RATE)
                trade_sub_type = "1"
            else:
                actual_fill_size = min(current_size, filled_size)
                next_size = max(Decimal("0"), current_size - actual_fill_size)
                next_entry = current_entry if next_size > 0 else Decimal("0")
                realized_pnl = (fill_px - current_entry) * actual_fill_size if current_entry > 0 else Decimal("0")
                order_fee = -(actual_fill_size * fill_px * PAPER_SPOT_FEE_RATE)
                trade_sub_type = "2"

            current_eq_after = current_eq_before + realized_pnl + order_fee

            def mutate(current_state: dict[str, Any]) -> None:
                market = current_state["markets"].setdefault(market_key, default_market_state())
                market.update(
                    {
                        "enabled": True,
                        "instId": inst_id,
                        "positionSide": "flat" if next_size <= 0 else "long",
                        "positionSize": decimal_to_str(next_size),
                        "positionNotional": decimal_to_str(next_size * fill_px),
                        "entryPrice": "" if next_size <= 0 else decimal_to_str(next_entry),
                        "lastPrice": decimal_to_str(fill_px),
                        "floatingPnl": "0",
                        "floatingPnlPct": "0",
                        "lastTradeAt": now_local_iso(),
                        "lastActionAt": now_local_iso(),
                        "lastOrderId": ord_id,
                    }
                )
                if not current_state.get("sessionStartEq"):
                    current_state["sessionStartEq"] = decimal_to_str(current_eq_before)
                current_state["currentEq"] = decimal_to_str(current_eq_after)

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
            "fee": decimal_to_str(order_fee),
            "fillFee": decimal_to_str(order_fee),
            "pnl": decimal_to_str(realized_pnl),
            "fillPnl": decimal_to_str(realized_pnl),
            "realizedPnl": decimal_to_str(realized_pnl),
            "reduceOnly": reduce_only,
            "posSide": pos_side,
            "tradeSubType": trade_sub_type,
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
            if self._paper_read_fallback_allowed():
                return self._paper_account_balance()
            raise

    def get_funding_balances(self, ccy: str | None = None) -> dict[str, Any]:
        params = {"ccy": ccy} if ccy else None
        if self._paper_state_authoritative():
            return {"code": "0", "data": [], "_paperSim": True}
        try:
            return self._request("GET", "/api/v5/asset/balances", params=params)
        except Exception:
            if self._paper_read_fallback_allowed():
                return {"code": "0", "data": [], "_paperSim": True}
            raise

    def get_asset_valuation(self, ccy: str = "USDT") -> dict[str, Any]:
        params = {"ccy": ccy} if ccy else None
        if self._paper_state_authoritative():
            return {"code": "0", "data": [{"ccy": ccy or "USDT", "totalBal": "0", "ts": str(int(time.time() * 1000))}], "_paperSim": True}
        try:
            return self._request("GET", "/api/v5/asset/asset-valuation", params=params)
        except Exception:
            if self._paper_read_fallback_allowed():
                return {"code": "0", "data": [{"ccy": ccy or "USDT", "totalBal": "0", "ts": str(int(time.time() * 1000))}], "_paperSim": True}
            raise

    def get_trade_fee(
        self,
        inst_type: str,
        *,
        inst_family: str | None = None,
        inst_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {"instType": inst_type}
        if inst_family:
            params["instFamily"] = inst_family
        elif inst_id:
            params["instId"] = inst_id
        return self._request("GET", "/api/v5/account/trade-fee", params=params)

    def get_positions(self, inst_id: str | None = None) -> dict[str, Any]:
        params = {"instId": inst_id} if inst_id else None
        if self._paper_state_authoritative():
            return self._paper_positions(inst_id)
        try:
            return self._request("GET", "/api/v5/account/positions", params=params)
        except Exception:
            if self._paper_read_fallback_allowed():
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
            if self._paper_read_fallback_allowed():
                return self._paper_recent_orders()
            raise

    def get_recent_fills(self, inst_type: str, limit: int = 100) -> dict[str, Any]:
        params = {"instType": inst_type, "limit": limit}
        if self._paper_state_authoritative():
            return {"code": "0", "data": [], "_paperSim": True}
        try:
            return self._request("GET", "/api/v5/trade/fills-history", params=params)
        except Exception:
            try:
                return self._request("GET", "/api/v5/trade/fills", params=params)
            except Exception:
                if self._paper_read_fallback_allowed():
                    return {"code": "0", "data": [], "_paperSim": True}
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
            if self._public_market_fallback_allowed():
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
            if self._public_market_fallback_allowed():
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
            if self._public_market_fallback_allowed():
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
            if self._public_market_fallback_allowed():
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
            if self._public_market_fallback_allowed():
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
            if self._public_market_fallback_allowed():
                return self._fallback_funding_rate(inst_id)
            raise

    def get_account_config(self) -> dict[str, Any]:
        try:
            return self._request("GET", "/api/v5/account/config")
        except Exception:
            if self._paper_read_fallback_allowed():
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
            if self._paper_trading_fallback_allowed():
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
            if self._paper_trading_fallback_allowed():
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
            if self._paper_trading_fallback_allowed():
                return self._paper_place_order(payload)
            raise

    def place_orders(self, payloads: list[dict[str, Any]]) -> dict[str, Any]:
        cleaned = [payload for payload in (payloads or []) if isinstance(payload, dict) and payload]
        if not cleaned:
            return {"code": "0", "data": []}
        try:
            result = self._request("POST", "/api/v5/trade/batch-orders", payload=cleaned)
            self._extract_data_or_raise(result)
            return result
        except Exception:
            if self._paper_trading_fallback_allowed():
                rows: list[dict[str, Any]] = []
                for payload in cleaned:
                    result = self._paper_place_order(payload)
                    rows.extend(result.get("data") or [])
                return {"code": "0", "data": rows, "_paperSim": True}
            raise

    def get_order(self, inst_id: str, ord_id: str | None = None, cl_ord_id: str | None = None) -> dict[str, Any]:
        params = {"instId": inst_id}
        if ord_id:
            params["ordId"] = ord_id
        if cl_ord_id:
            params["clOrdId"] = cl_ord_id
        return self._request("GET", "/api/v5/trade/order", params=params)

    def cancel_order(self, inst_id: str, ord_id: str | None = None, cl_ord_id: str | None = None) -> dict[str, Any]:
        payload = {"instId": inst_id}
        if ord_id:
            payload["ordId"] = ord_id
        if cl_ord_id:
            payload["clOrdId"] = cl_ord_id
        try:
            result = self._request("POST", "/api/v5/trade/cancel-order", payload=payload)
            self._extract_data_or_raise(result)
            return result
        except Exception:
            if self._paper_trading_fallback_allowed():
                return {
                    "code": "0",
                    "data": [{"instId": inst_id, "ordId": ord_id or "", "clOrdId": cl_ord_id or "", "sCode": "0"}],
                    "_paperSim": True,
                }
            raise

    def cancel_orders(self, payloads: list[dict[str, Any]]) -> dict[str, Any]:
        cleaned = [payload for payload in (payloads or []) if isinstance(payload, dict) and payload.get("instId")]
        if not cleaned:
            return {"code": "0", "data": []}
        try:
            result = self._request("POST", "/api/v5/trade/cancel-batch-orders", payload=cleaned)
            self._extract_data_or_raise(result)
            return result
        except Exception:
            if self._paper_trading_fallback_allowed():
                rows: list[dict[str, Any]] = []
                for payload in cleaned:
                    rows.append(
                        {
                            "instId": str(payload.get("instId") or ""),
                            "ordId": str(payload.get("ordId") or ""),
                            "clOrdId": str(payload.get("clOrdId") or ""),
                            "sCode": "0",
                        }
                    )
                return {"code": "0", "data": rows, "_paperSim": True}
            raise


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
            if not should_run_local_okx_background_tasks(config):
                with self.state_lock:
                    self.connected = False
                    self.last_error = ""
                await asyncio.sleep(3)
                continue
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
            merged = [normalize_execution_order(item) for item in list(orders) + self.orders]
            deduped: list[dict[str, Any]] = []
            seen = set()
            for item in merged:
                key = item.get("ordId") or item.get("clOrdId")
                if not key or key in seen:
                    continue
                seen.add(key)
                deduped.append(item)
                if len(deduped) >= MAX_RECENT_ORDER_LIMIT:
                    break
            self.orders = deduped
            self.last_event_at = now_local_iso()
            self.connected = True
            self.last_error = ""
        persist_local_orders(deduped[:MAX_RECENT_ORDER_LIMIT], source="private_ws")


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
    normalized = enforce_only_dip_swing_strategy(normalized)
    watchlist_symbols = normalize_watchlist_symbols(normalized.get("watchlistSymbols"), normalized)
    normalized["watchlistSymbols"] = ",".join(watchlist_symbols)
    watchlist_overrides, overrides_error = parse_watchlist_overrides(normalized.get("watchlistOverrides"))
    if overrides_error:
        return False, overrides_error, normalized
    normalized["watchlistOverrides"] = {
        symbol: sanitize_only_dip_swing_override(override)
        for symbol, override in watchlist_overrides.items()
        if symbol in watchlist_symbols
    }
    primary_symbol = watchlist_symbols[0]
    normalized["spotInstId"] = f"{primary_symbol}-USDT"
    normalized["swapInstId"] = f"{primary_symbol}-USDT-SWAP"
    if len(watchlist_symbols) > DIP_SWING_WATCHLIST_LIMIT:
        return False, f"多币 watchlist 最多支持 {DIP_SWING_WATCHLIST_LIMIT} 个标的", normalized
    base_error = validate_single_automation_target(normalized)
    if base_error:
        return False, base_error, normalized
    for symbol, override in normalized["watchlistOverrides"].items():
        target_config = deep_merge(normalized, override)
        override_error = validate_single_automation_target(target_config)
        if override_error:
            return False, f"{symbol} 覆盖配置无效：{override_error}", normalized
    return True, "", normalized


def strategy_label(preset: str) -> str:
    return ONLY_STRATEGY_LABEL


def strategy_symbol_label(config: dict[str, Any]) -> str:
    symbols = normalize_watchlist_symbols(config.get("watchlistSymbols"), config)
    if len(symbols) > 1:
        return f"{symbols[0]} +{len(symbols) - 1}"
    for key in ("spotInstId", "swapInstId"):
        inst_id = str(config.get(key, "") or "").strip()
        if inst_id:
            return inst_id.split("-")[0]
    return "OKX"


def strategy_scope_label(config: dict[str, Any]) -> str:
    if str(config.get("strategyPreset") or "") == "dip_swing":
        return "逐仓永续利润循环"
    scopes: list[str] = []
    if config.get("spotEnabled"):
        scopes.append("现货")
    if config.get("swapEnabled"):
        scopes.append("永续")
    return "+".join(scopes) if scopes else "观察"


def strategy_mode_label(config: dict[str, Any]) -> str:
    if str(config.get("strategyPreset") or "") == "dip_swing":
        return "空仓即开 / 净利 1U 平仓"
    if not config.get("swapEnabled"):
        return "现货执行"
    mode = str(config.get("swapStrategyMode", "trend_follow"))
    if mode == "trend_follow":
        return "顺势双向"
    if mode == "short_only":
        return "只做空"
    return "只做多"


def strategy_mode_badge(config: dict[str, Any]) -> str:
    if str(config.get("strategyPreset") or "") == "dip_swing":
        return ONLY_STRATEGY_LABEL
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
    preset = ONLY_STRATEGY_LABEL
    leverage = f"{int(safe_decimal(config.get('swapLeverage'), '1'))}x" if config.get("swapEnabled") else ""
    tail = " ".join(part for part in (strategy_mode_badge(config), leverage) if part)
    return f"{prefix}{symbol} {preset} {config.get('bar', '5m')} EMA{config['fastEma']}/{config['slowEma']}{(' ' + tail) if tail else ''}"


def strategy_detail_line(config: dict[str, Any], origin_label: str = "") -> str:
    if str(config.get("strategyPreset") or "") == "basis_arb":
        selected_target = resolve_selected_execution_target(config)
        watchlist_count = len(normalize_watchlist_symbols(config.get("watchlistSymbols"), config))
        parts = [
            "现货买入 + 永续对冲",
            f"入场价差 ≥ {config.get('arbEntrySpreadPct', '0.18')}%",
            f"回补价差 ≤ {config.get('arbExitSpreadPct', '0.05')}%",
            f"最低资金费 {config.get('arbMinFundingRatePct', '0.005')}%",
            f"最长持有 {config.get('arbMaxHoldMinutes', 180)} 分钟",
        ]
        if watchlist_count > 1:
            parts.append(
                f"{watchlist_count} 币并行 · 当前币约 {decimal_to_str(safe_decimal(selected_target.get('spotQuoteBudget'), '0'))}U / "
                f"{decimal_to_str(safe_decimal(selected_target.get('swapContracts'), '0'))} 张"
            )
        if origin_label:
            parts.append(origin_label)
        return " · ".join(part for part in parts if part)
    if str(config.get("strategyPreset") or "") == "dip_swing":
        target_multiple = resolve_target_balance_multiple(config)
        parts = [
            "市场扫描 + 方向轮动 + 因子裁判",
            "空仓即开 · 净赚 1U+ 就平 · 24 小时循环",
            f"顺势双向 · {config.get('swapTdMode', 'isolated')} {config.get('swapLeverage', '2')}x",
            "开仓 maker-first / 平仓 IOC",
            f"优势/成本 ≥ {format_decimal(DIP_SWING_MIN_EDGE_COST_RATIO, 1)}x · 波动/成本 ≥ {format_decimal(DIP_SWING_MIN_RANGE_COST_RATIO, 1)}x · ATR/成本 ≥ {format_decimal(DIP_SWING_MIN_ATR_COST_RATIO, 1)}x",
            f"强平缓冲 ≥ {format_decimal(DIP_SWING_MIN_LIQ_BUFFER_PCT, 0)}% · 动态仓位",
        ]
        if target_multiple > Decimal("1"):
            parts.append(f"目标余额 {format_decimal(target_multiple, 0)}x")
        if origin_label:
            parts.append(origin_label)
        return " · ".join(part for part in parts if part)
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
                "volume": safe_decimal(row[5]),
                "quoteVolume": safe_decimal(row[7] if len(row) > 7 else row[5]),
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


def build_pullback_signal(
    candles: list[dict[str, Any]],
    fast: int,
    slow: int,
    *,
    pullback_threshold_pct: Decimal | None = None,
    rebound_threshold_pct: Decimal | None = None,
) -> dict[str, Any]:
    closes = [row["close"] for row in candles]
    if len(closes) < slow + 2:
        raise OkxApiError("K 线样本不足，无法计算利润循环信号")
    adaptive_thresholds = build_dip_swing_adaptive_thresholds(candles, fast, slow)
    pullback_threshold_pct = safe_decimal(
        pullback_threshold_pct,
        decimal_to_str(adaptive_thresholds.get("pullbackThresholdPct")),
    )
    rebound_threshold_pct = safe_decimal(
        rebound_threshold_pct,
        decimal_to_str(adaptive_thresholds.get("reboundThresholdPct")),
    )
    fast_values = ema(closes, fast)
    slow_values = ema(closes, slow)
    prev_fast = fast_values[-2]
    prev_slow = slow_values[-2]
    curr_fast = fast_values[-1]
    curr_slow = slow_values[-1]
    last_close = closes[-1]
    prev_close = closes[-2]
    sample = candles[-max(slow, 12):]
    recent_high = max((row["high"] for row in sample), default=last_close)
    pullback_window = candles[-min(len(candles), max(fast * 2, 6)) :]
    pullback_low = min((row["low"] for row in pullback_window), default=last_close)
    rebound_window = candles[-min(len(candles), int(adaptive_thresholds["reboundLookbackBars"])) :]
    rebound_low = min((row["low"] for row in rebound_window), default=last_close)
    pullback_pct = pct_gap(recent_high, last_close)
    rebound_pct = pct_gap(last_close, rebound_low)
    ema_spread_pct = pct_gap(curr_fast, curr_slow)
    fast_slope_pct = pct_gap(curr_fast, prev_fast)
    slow_slope_pct = pct_gap(curr_slow, prev_slow)
    price_vs_fast_pct = pct_gap(last_close, curr_fast)
    price_vs_slow_pct = pct_gap(last_close, curr_slow)
    if curr_fast > curr_slow:
        trend = "up"
    elif curr_fast < curr_slow:
        trend = "down"
    else:
        trend = "flat"
    bull_cross = prev_fast <= prev_slow and curr_fast > curr_slow
    close_above_fast = last_close >= curr_fast
    close_above_slow = last_close >= curr_slow
    pullback_touched_fast = pullback_low <= max(curr_fast, prev_fast)
    rebound_ready = last_close >= prev_close and rebound_pct >= rebound_threshold_pct and close_above_fast
    not_overextended = price_vs_fast_pct <= DIP_SWING_MAX_CHASE_PCT
    ema_spread_ready = ema_spread_pct >= DIP_SWING_MIN_EMA_SPREAD_PCT
    fast_slope_ready = fast_slope_pct >= DIP_SWING_MIN_FAST_SLOPE_PCT
    slow_slope_ready = slow_slope_pct >= Decimal("0")
    trend_strength_ready = trend == "up" and ema_spread_ready and fast_slope_ready and slow_slope_ready
    pullback_context = pullback_pct >= pullback_threshold_pct and pullback_touched_fast
    bull_cross_context = bull_cross and close_above_fast and close_above_slow
    entry_checks = {
        "trend_up": trend == "up",
        "ema_spread": ema_spread_ready,
        "fast_slope": fast_slope_ready,
        "slow_slope": slow_slope_ready,
        "pullback_context": pullback_context or bull_cross_context,
        "rebound_ready": rebound_ready or bull_cross_context,
        "not_overextended": not_overextended,
        "close_above_slow": close_above_slow,
    }
    entry_score = sum(1 for ready in entry_checks.values() if ready)
    exit_checks = {
        "trend_down": trend == "down",
        "close_below_fast": last_close < curr_fast,
        "close_below_slow": last_close < curr_slow,
        "fast_slope_negative": fast_slope_pct <= (Decimal("0") - DIP_SWING_MIN_FAST_SLOPE_PCT),
        "slow_slope_negative": slow_slope_pct < Decimal("0"),
    }
    exit_score = sum(1 for ready in exit_checks.values() if ready)
    weak_trend_ready = exit_score >= DIP_SWING_MIN_EXIT_SCORE
    if trend_strength_ready and bull_cross_context and not_overextended and entry_score >= DIP_SWING_MIN_ENTRY_SCORE:
        signal = "bull_cross_buy"
    elif trend_strength_ready and pullback_context and rebound_ready and not_overextended and entry_score >= DIP_SWING_MIN_ENTRY_SCORE:
        signal = "pullback_buy"
    elif weak_trend_ready:
        signal = "trend_break"
    else:
        signal = "hold"
    return {
        "signal": signal,
        "trend": trend,
        "fastValue": decimal_to_str(curr_fast),
        "slowValue": decimal_to_str(curr_slow),
        "lastClose": decimal_to_str(last_close),
        "recentHigh": decimal_to_str(recent_high),
        "recentLow": decimal_to_str(rebound_low),
        "pullbackPct": decimal_to_str(pullback_pct),
        "reboundPct": decimal_to_str(rebound_pct),
        "pullbackThresholdPct": decimal_to_str(pullback_threshold_pct),
        "reboundThresholdPct": decimal_to_str(rebound_threshold_pct),
        "reboundLookbackBars": int(adaptive_thresholds["reboundLookbackBars"]),
        "microRangePct": decimal_to_str(safe_decimal(adaptive_thresholds.get("microRangePct"), "0")),
        "volatilityPct": decimal_to_str(safe_decimal(adaptive_thresholds.get("volatilityPct"), "0")),
        "atrPct": decimal_to_str(safe_decimal(adaptive_thresholds.get("atrPct"), "0")),
        "estimatedCostPct": decimal_to_str(safe_decimal(adaptive_thresholds.get("estimatedCostPct"), "0")),
        "bullCross": bull_cross,
        "pullbackTouchedFast": pullback_touched_fast,
        "closeAboveFast": close_above_fast,
        "closeAboveSlow": close_above_slow,
        "trendStrengthReady": trend_strength_ready,
        "pullbackContext": pullback_context,
        "reboundReady": rebound_ready,
        "notOverextended": not_overextended,
        "emaSpreadPct": decimal_to_str(ema_spread_pct),
        "fastSlopePct": decimal_to_str(fast_slope_pct),
        "slowSlopePct": decimal_to_str(slow_slope_pct),
        "priceVsFastPct": decimal_to_str(price_vs_fast_pct),
        "priceVsSlowPct": decimal_to_str(price_vs_slow_pct),
        "entryScore": entry_score,
        "exitScore": exit_score,
        "weakTrendReady": weak_trend_ready,
    }


def profit_loop_trade_side(signal: dict[str, Any], candles: list[dict[str, Any]]) -> str:
    trend = str(signal.get("trend") or "flat").strip().lower()
    fast_slope_pct = safe_decimal(signal.get("fastSlopePct"), "0")
    slow_slope_pct = safe_decimal(signal.get("slowSlopePct"), "0")
    last_close = safe_decimal(signal.get("lastClose"), "0")
    prev_close = candles[-2]["close"] if len(candles) >= 2 else last_close
    if trend == "up" and fast_slope_pct >= 0 and slow_slope_pct >= 0:
        return "buy"
    if trend == "down" and fast_slope_pct <= 0 and slow_slope_pct <= 0:
        return "sell"
    return "buy" if last_close >= prev_close else "sell"


def profit_loop_trade_side_label(side: str) -> str:
    return "做多" if str(side or "").strip().lower() == "buy" else "做空"


def estimate_profit_loop_position_net_pnl(
    *,
    position_side: str,
    position_size: Decimal,
    entry_price: Decimal,
    last_price: Decimal,
    contract_value: Decimal,
    floating_pnl: Decimal,
    maker_fee_pct: Decimal,
    taker_fee_pct: Decimal,
) -> dict[str, Decimal]:
    if str(position_side or "").strip().lower() not in {"long", "short"} or position_size <= 0:
        return {
            "entryNotional": Decimal("0"),
            "exitNotional": Decimal("0"),
            "entryFeeEstimate": Decimal("0"),
            "exitFeeEstimate": Decimal("0"),
            "netClosePnl": Decimal("0"),
        }
    entry_notional = max(position_size * entry_price * contract_value, Decimal("0"))
    exit_notional = max(position_size * last_price * contract_value, Decimal("0"))
    entry_fee_estimate = entry_notional * maker_fee_pct / Decimal("100")
    exit_fee_estimate = exit_notional * taker_fee_pct / Decimal("100")
    net_close_pnl = floating_pnl - entry_fee_estimate - exit_fee_estimate
    return {
        "entryNotional": entry_notional,
        "exitNotional": exit_notional,
        "entryFeeEstimate": entry_fee_estimate,
        "exitFeeEstimate": exit_fee_estimate,
        "netClosePnl": net_close_pnl,
    }


def estimate_profit_loop_entry_net_pnl(
    *,
    planned_contracts: Decimal,
    last_price: Decimal,
    contract_value: Decimal,
    predicted_net_pct: Decimal,
) -> dict[str, Decimal]:
    if planned_contracts <= 0 or last_price <= 0 or contract_value <= 0:
        return {
            "entryNotional": Decimal("0"),
            "projectedNetPnl": Decimal("0"),
        }
    entry_notional = planned_contracts * last_price * contract_value
    projected_net_pnl = entry_notional * (predicted_net_pct / Decimal("100"))
    return {
        "entryNotional": entry_notional,
        "projectedNetPnl": projected_net_pnl,
    }


def liquidation_buffer_pct(last_price: Decimal, liq_price: Decimal, position_side: str) -> Decimal:
    if liq_price <= 0 or last_price <= 0:
        return Decimal("0")
    side = str(position_side or "").strip().lower()
    if side == "long":
        return ((last_price - liq_price) / last_price) * Decimal("100")
    if side == "short":
        return ((liq_price - last_price) / last_price) * Decimal("100")
    return Decimal("0")


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
    valuation_details = copy.deepcopy(valuation_top.get("details") or {})
    total_bal = safe_decimal(valuation_top.get("totalBal"), "0")
    source = "valuation" if total_bal > 0 else "funding_balances"
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
            "source": source,
            "valuationDetails": valuation_details,
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
    valuation_total = safe_decimal(funding_summary.get("totalBal"), "0")
    valuation_source = str(funding_summary.get("source") or "")
    valuation_details = copy.deepcopy(funding_summary.get("valuationDetails") or {})
    funding_total = (
        safe_decimal(valuation_details.get("funding"), "0")
        + safe_decimal(valuation_details.get("classic"), "0")
        + safe_decimal(valuation_details.get("earn"), "0")
    )

    if valuation_source == "valuation" and valuation_total > 0:
        display_total = valuation_total
        display_source = "总资产估值"
        breakdown_parts: list[str] = []
        for key, label in (
            ("trading", "交易账户"),
            ("funding", "资金账户"),
            ("classic", "经典账户"),
            ("earn", "赚币"),
        ):
            amount = safe_decimal(valuation_details.get(key), "0")
            if amount > 0:
                breakdown_parts.append(f"{label} {compact_metric(amount)} USDT")
        display_breakdown = " · ".join(breakdown_parts)
    elif funding_total > 0 and trading_total > 0:
        display_total = trading_total + funding_total
        display_source = "资金账户 + 交易账户"
        display_breakdown = (
            f"资金账户 {compact_metric(funding_total)} USDT · "
            f"交易账户 {compact_metric(trading_total)} USDT"
        )
    elif funding_total > 0:
        display_total = funding_total
        display_source = "资金账户"
        display_breakdown = f"资金账户 {compact_metric(funding_total)} USDT"
    elif trading_total > 0:
        display_total = trading_total
        display_source = "交易账户"
        display_breakdown = f"交易账户 {compact_metric(trading_total)} USDT"
    else:
        display_total = trading_total
        display_source = "交易账户"
        display_breakdown = ""

    summary = {
        **trading_summary,
        "tradingTotalEq": decimal_to_str(trading_total),
        "fundingTotalEq": decimal_to_str(funding_total),
        "valuationTotalEq": decimal_to_str(valuation_total),
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


def resolve_swap_available_margin(snapshot: dict[str, Any], margin_ccy: str = "USDT") -> Decimal:
    row = find_balance_detail(snapshot, margin_ccy)
    if row:
        for key in ("availEq", "availBal", "cashBal", "eq", "eqUsd"):
            amount = safe_decimal(row.get(key), "0")
            if amount > 0:
                return amount
    summary = snapshot.get("summary") or {}
    for key in ("displayTotalEq", "totalEq", "adjEq", "isoEq"):
        amount = safe_decimal(summary.get(key), "0")
        if amount > 0:
            return amount
    return Decimal("0")


def clamp_swap_contracts_to_available_margin(
    planned_contracts: Decimal,
    *,
    available_margin: Decimal,
    lot_size: Decimal,
    last_price: Decimal,
    contract_value: Decimal,
    leverage: Decimal,
) -> dict[str, Any]:
    if planned_contracts <= 0 or lot_size <= 0:
        return {
            "contracts": Decimal("0"),
            "contractMargin": Decimal("0"),
            "availableBudget": Decimal("0"),
            "clamped": False,
        }
    contract_margin = (last_price * contract_value) / leverage if leverage > 0 and last_price > 0 and contract_value > 0 else Decimal("0")
    if contract_margin <= 0:
        return {
            "contracts": Decimal("0"),
            "contractMargin": Decimal("0"),
            "availableBudget": Decimal("0"),
            "clamped": True,
        }
    usable_margin = max(Decimal("0"), available_margin * DIP_SWING_AVAILABLE_MARGIN_UTILIZATION)
    if usable_margin <= 0:
        return {
            "contracts": Decimal("0"),
            "contractMargin": contract_margin,
            "availableBudget": Decimal("0"),
            "clamped": True,
        }
    affordable_contracts = round_down(usable_margin / contract_margin, lot_size)
    if affordable_contracts < lot_size:
        affordable_contracts = Decimal("0")
    contracts = min(planned_contracts, affordable_contracts)
    return {
        "contracts": contracts,
        "contractMargin": contract_margin,
        "availableBudget": usable_margin,
        "clamped": contracts < planned_contracts,
    }


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


def clamp_decimal(value: Decimal, lower: Decimal, upper: Decimal) -> Decimal:
    return max(lower, min(upper, value))


def dip_swing_rebound_lookback_bars(fast: int) -> int:
    return max(
        DIP_SWING_MIN_REBOUND_LOOKBACK_BARS,
        min(DIP_SWING_MAX_REBOUND_LOOKBACK_BARS, int(round(max(6, int(fast or 6)) * 0.67))),
    )


def build_dip_swing_adaptive_thresholds(
    candles: list[dict[str, Any]],
    fast: int,
    slow: int,
) -> dict[str, Decimal | int]:
    volatility_pct = recent_range_pct(candles)
    atr_pct = average_true_range_pct(candles)
    rebound_lookback_bars = dip_swing_rebound_lookback_bars(fast)
    micro_range_pct = recent_range_pct(candles, window=rebound_lookback_bars)
    cost_snapshot = estimate_dip_swing_cost_snapshot(volatility_pct)
    estimated_cost_pct = safe_decimal(cost_snapshot.get("estimatedCostPct"), decimal_to_str(DIP_SWING_EST_ROUNDTRIP_COST_PCT))
    pullback_threshold_pct = clamp_decimal(
        max(
            estimated_cost_pct * Decimal("2.8"),
            atr_pct * Decimal("1.35"),
            micro_range_pct * Decimal("0.80"),
        ),
        DIP_SWING_MIN_PULLBACK_PCT,
        DIP_SWING_MAX_PULLBACK_PCT,
    )
    rebound_threshold_pct = clamp_decimal(
        max(
            estimated_cost_pct * Decimal("2.0"),
            atr_pct * Decimal("0.95"),
            micro_range_pct * Decimal("0.72"),
        ),
        DIP_SWING_MIN_REBOUND_PCT,
        DIP_SWING_MAX_REBOUND_PCT,
    )
    return {
        "volatilityPct": volatility_pct,
        "atrPct": atr_pct,
        "estimatedCostPct": estimated_cost_pct,
        "reboundLookbackBars": rebound_lookback_bars,
        "microRangePct": micro_range_pct,
        "pullbackThresholdPct": pullback_threshold_pct,
        "reboundThresholdPct": rebound_threshold_pct,
    }


def average_true_range_pct(candles: list[dict[str, Any]], window: int = 14) -> Decimal:
    if len(candles) < 2:
        return Decimal("0")
    sample = candles[-min(len(candles), max(window + 1, 3)) :]
    true_ranges: list[Decimal] = []
    prev_close = safe_decimal(sample[0].get("close"), "0")
    for row in sample[1:]:
        high = safe_decimal(row.get("high"), "0")
        low = safe_decimal(row.get("low"), "0")
        if prev_close <= 0:
            prev_close = safe_decimal(row.get("close"), "0")
            continue
        true_range = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close),
        )
        true_ranges.append((true_range / prev_close) * Decimal("100"))
        prev_close = safe_decimal(row.get("close"), "0")
    if not true_ranges:
        return Decimal("0")
    return sum(true_ranges, Decimal("0")) / Decimal(len(true_ranges))


def average_quote_volume_usd(candles: list[dict[str, Any]], window: int = 12) -> Decimal:
    if not candles:
        return Decimal("0")
    sample = candles[-min(len(candles), window) :]
    quote_volumes = [safe_decimal(row.get("quoteVolume"), "0") for row in sample]
    if any(volume > 0 for volume in quote_volumes):
        positive = [volume for volume in quote_volumes if volume > 0]
        if positive:
            return sum(positive, Decimal("0")) / Decimal(len(positive))
    notionals = [safe_decimal(row.get("close"), "0") * safe_decimal(row.get("volume"), "0") for row in sample]
    positive_notionals = [notional for notional in notionals if notional > 0]
    if not positive_notionals:
        return Decimal("0")
    return sum(positive_notionals, Decimal("0")) / Decimal(len(positive_notionals))


def normalize_fee_rate_pct(raw: Any, fallback_pct: Decimal) -> Decimal:
    value = safe_decimal(raw, "0")
    if value <= 0:
        return fallback_pct
    if value < Decimal("1"):
        return value * Decimal("100")
    return value


def estimate_dip_swing_cost_snapshot(
    volatility_pct: Decimal,
    *,
    funding_rate_pct: Decimal = Decimal("0"),
    maker_fee_pct: Decimal = OKX_DEFAULT_SWAP_MAKER_FEE_PCT,
    taker_fee_pct: Decimal = OKX_DEFAULT_SWAP_TAKER_FEE_PCT,
    passive_exit_weight: Decimal = OKX_DEFAULT_PASSIVE_EXIT_WEIGHT,
) -> dict[str, Decimal]:
    maker_fee_pct = max(Decimal("0"), maker_fee_pct)
    taker_fee_pct = max(maker_fee_pct, taker_fee_pct)
    passive_exit_weight = clamp_decimal(passive_exit_weight, Decimal("0"), Decimal("1"))
    blended_exit_fee_pct = (
        maker_fee_pct * passive_exit_weight
        + taker_fee_pct * (Decimal("1") - passive_exit_weight)
    )
    slippage_pct = min(Decimal("0.08"), max(Decimal("0"), volatility_pct) * Decimal("0.025"))
    funding_drag_pct = max(Decimal("0"), funding_rate_pct) * Decimal("0.35")
    roundtrip_fee_pct = maker_fee_pct + blended_exit_fee_pct
    estimated_cost_pct = roundtrip_fee_pct + slippage_pct + funding_drag_pct
    protective_exit_floor_pct = max(
        DIP_SWING_MIN_PROTECTIVE_EXIT_PCT,
        roundtrip_fee_pct + (slippage_pct * Decimal("0.75")),
    )
    return {
        "makerFeePct": maker_fee_pct,
        "takerFeePct": taker_fee_pct,
        "blendedExitFeePct": blended_exit_fee_pct,
        "roundtripFeePct": roundtrip_fee_pct,
        "slippagePct": slippage_pct,
        "fundingDragPct": funding_drag_pct,
        "estimatedCostPct": estimated_cost_pct,
        "protectiveExitFloorPct": protective_exit_floor_pct,
    }


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


def ticker_bid_price(row: dict[str, Any]) -> Decimal:
    return safe_decimal(row.get("bidPx") or row.get("bidPrice") or row.get("last"), "0")


def ticker_ask_price(row: dict[str, Any]) -> Decimal:
    return safe_decimal(row.get("askPx") or row.get("askPrice") or row.get("last"), "0")


def pct_gap(numerator: Decimal, denominator: Decimal) -> Decimal:
    if denominator <= 0:
        return Decimal("0")
    return ((numerator - denominator) / denominator) * Decimal("100")


def normalized_open_interest_usd(
    inst_id: str,
    open_interest_row: dict[str, Any],
    *,
    last_price: Decimal,
) -> Decimal:
    open_interest_usd = safe_decimal(open_interest_row.get("oiUsd"), "0")
    if open_interest_usd > 0:
        return open_interest_usd
    contract_value = default_swap_contract_value(inst_id)
    raw_oi = safe_decimal(open_interest_row.get("oi"), "0")
    if raw_oi > 0 and contract_value > 0 and last_price > 0:
        return raw_oi * contract_value * last_price
    oi_ccy = safe_decimal(open_interest_row.get("oiCcy"), "0")
    if oi_ccy > 0 and last_price > 0:
        return oi_ccy * last_price
    return Decimal("0")


def swap_inst_family(inst_id: str, meta: dict[str, Any] | None = None) -> str:
    if isinstance(meta, dict):
        family = str(meta.get("instFamily") or "").strip()
        if family:
            return family
    inst = str(inst_id or "").strip()
    return inst[:-5] if inst.endswith("-SWAP") else inst


def cache_okx_swap_fee_rates(
    client: "OkxClient",
    inst_id: str,
    *,
    meta: dict[str, Any] | None = None,
    ttl_seconds: float = 600.0,
) -> dict[str, Decimal]:
    family = swap_inst_family(inst_id, meta)
    cache_key = f"{client.base_url}|{1 if client.simulated else 0}|{str(client.api_key or '')}|{family}"
    now = time.time()
    with OKX_FEE_RATE_CACHE_LOCK:
        cached = OKX_FEE_RATE_CACHE.get(cache_key) or {}
        if cached and now - float(cached.get("ts") or 0.0) < ttl_seconds:
            return {
                "makerFeePct": safe_decimal(cached.get("makerFeePct"), decimal_to_str(OKX_DEFAULT_SWAP_MAKER_FEE_PCT)),
                "takerFeePct": safe_decimal(cached.get("takerFeePct"), decimal_to_str(OKX_DEFAULT_SWAP_TAKER_FEE_PCT)),
            }

    maker_fee_pct = OKX_DEFAULT_SWAP_MAKER_FEE_PCT
    taker_fee_pct = OKX_DEFAULT_SWAP_TAKER_FEE_PCT
    try:
        response = client.get_trade_fee("SWAP", inst_family=family)
        row = extract_first_row(response)
        maker_fee_pct = normalize_fee_rate_pct(row.get("maker"), maker_fee_pct)
        taker_fee_pct = normalize_fee_rate_pct(row.get("taker"), taker_fee_pct)
    except Exception:
        pass

    payload = {
        "ts": now,
        "makerFeePct": decimal_to_str(maker_fee_pct),
        "takerFeePct": decimal_to_str(taker_fee_pct),
    }
    with OKX_FEE_RATE_CACHE_LOCK:
        OKX_FEE_RATE_CACHE[cache_key] = payload
    return {"makerFeePct": maker_fee_pct, "takerFeePct": taker_fee_pct}


def build_dip_swing_factor_bundle(
    signal: dict[str, Any],
    *,
    entry_score: int,
    edge_cost_ready: bool,
    range_cost_ready: bool,
    atr_cost_ready: bool,
    liquidity_ready: bool,
) -> dict[str, Any]:
    entry_factors = {
        "trendUp": str(signal.get("trend") or "") == "up",
        "trendStrength": bool(signal.get("trendStrengthReady")),
        "pullbackContext": bool(signal.get("pullbackContext")) or bool(signal.get("bullCross")),
        "reboundReady": bool(signal.get("reboundReady")) or bool(signal.get("bullCross")),
        "notOverextended": bool(signal.get("notOverextended")),
        "edgeCost": edge_cost_ready,
        "rangeCost": range_cost_ready,
        "atrCost": atr_cost_ready,
        "liquidity": liquidity_ready,
        "entryScore": entry_score >= DIP_SWING_MIN_ENTRY_SCORE,
    }
    vetoes: list[str] = []
    if not entry_factors["trendUp"]:
        vetoes.append("方向还没真正扩散出来")
    elif not entry_factors["trendStrength"]:
        vetoes.append("趋势扩散和斜率不够")
    if not entry_factors["pullbackContext"]:
        vetoes.append("没有新的启动结构")
    if not entry_factors["reboundReady"]:
        vetoes.append("反弹确认不够")
    if not entry_factors["notOverextended"]:
        vetoes.append("当前位置属于追价")
    if not entry_factors["edgeCost"]:
        vetoes.append("结构优势/成本比不够")
    if not entry_factors["rangeCost"]:
        vetoes.append("波动区间太窄")
    if not entry_factors["atrCost"]:
        vetoes.append("真实波幅不够覆盖成本")
    if not entry_factors["liquidity"]:
        vetoes.append("流动性不够厚")
    if not entry_factors["entryScore"]:
        vetoes.append("入场评分不够")
    ready = all(entry_factors.values())
    factor_score = sum(1 for value in entry_factors.values() if value)
    return {
        "entryFactors": entry_factors,
        "entryVetoes": vetoes,
        "entryReady": ready,
        "entryFactorScore": factor_score,
    }


def build_basis_arb_scan_target(config: dict[str, Any], symbol: str) -> dict[str, Any]:
    target = deep_merge({}, config)
    target["watchlistSymbol"] = symbol
    target["spotInstId"] = f"{symbol}-USDT"
    target["swapInstId"] = f"{symbol}-USDT-SWAP"
    return target


def build_dip_swing_scan_target(config: dict[str, Any], symbol: str) -> dict[str, Any]:
    normalized_symbol = normalize_symbol_token(symbol)
    target = deep_merge({}, config)
    symbol_override = sanitize_only_dip_swing_override(
        copy.deepcopy((config.get("watchlistOverrides") or {}).get(normalized_symbol) or {})
    )
    target["watchlistSymbols"] = normalized_symbol
    target["watchlistSymbol"] = normalized_symbol
    target["watchlistIndex"] = 0
    target["watchlistCount"] = 1
    target["watchlistOverrides"] = {normalized_symbol: copy.deepcopy(symbol_override)} if symbol_override else {}
    target["watchlistOverride"] = copy.deepcopy(symbol_override)
    target["spotInstId"] = f"{normalized_symbol}-USDT"
    target["swapInstId"] = f"{normalized_symbol}-USDT-SWAP"
    target = deep_merge(target, symbol_override)
    return target


def evaluate_dip_swing_target_snapshot(
    target: dict[str, Any],
    swap_ticker: dict[str, Any],
    mark_price_row: dict[str, Any],
    funding_row: dict[str, Any],
    open_interest_row: dict[str, Any],
    candles: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    *,
    execution_journal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    signal = build_pullback_signal(candles, int(target["fastEma"]), int(target["slowEma"]))
    last_price = safe_decimal(signal.get("lastClose"), "0")
    mark_price = safe_decimal(mark_price_row.get("markPx"), "0")
    funding_rate_pct = safe_decimal(funding_row.get("fundingRate"), "0") * Decimal("100")
    basis_pct = pct_gap(last_price, mark_price)
    volatility_pct = safe_decimal(signal.get("volatilityPct"), decimal_to_str(recent_range_pct(candles)))
    atr_pct = safe_decimal(signal.get("atrPct"), decimal_to_str(average_true_range_pct(candles)))
    avg_quote_volume_usd = average_quote_volume_usd(candles)
    pullback_pct = safe_decimal(signal.get("pullbackPct"), "0")
    rebound_pct = safe_decimal(signal.get("reboundPct"), "0")
    pullback_threshold_pct = safe_decimal(signal.get("pullbackThresholdPct"), decimal_to_str(DIP_SWING_MIN_PULLBACK_PCT))
    rebound_threshold_pct = safe_decimal(signal.get("reboundThresholdPct"), decimal_to_str(DIP_SWING_MIN_REBOUND_PCT))
    rebound_lookback_bars = int(signal.get("reboundLookbackBars") or dip_swing_rebound_lookback_bars(int(target.get("fastEma", 12) or 12)))
    ema_spread_pct = safe_decimal(signal.get("emaSpreadPct"), "0")
    fast_slope_pct = safe_decimal(signal.get("fastSlopePct"), "0")
    slow_slope_pct = safe_decimal(signal.get("slowSlopePct"), "0")
    price_vs_fast_pct = safe_decimal(signal.get("priceVsFastPct"), "0")
    entry_score = int(signal.get("entryScore") or 0)
    exit_score = int(signal.get("exitScore") or 0)
    estimated_cost_pct = safe_decimal(signal.get("estimatedCostPct"), decimal_to_str(DIP_SWING_EST_ROUNDTRIP_COST_PCT + min(Decimal("0.12"), volatility_pct * Decimal("0.04"))))
    symbol_pressure = build_execution_symbol_pressure_snapshot(
        str(target.get("swapInstId") or ""),
        journal=execution_journal,
        limit=160,
    )
    recent_avg_abs_slip_mark_pct = safe_decimal(symbol_pressure.get("avgAbsSlipMarkPct"), "0")
    recent_avg_abs_slip_index_pct = safe_decimal(symbol_pressure.get("avgAbsSlipIndexPct"), "0")
    recent_taker_fill_pct = safe_decimal(symbol_pressure.get("takerFillPct"), "0")
    recent_execution_cost_floor_pct = safe_decimal(symbol_pressure.get("executionCostFloorPct"), "0")
    recent_net_pnl = safe_decimal(symbol_pressure.get("recentNetPnl"), "0")
    recent_close_orders = int(symbol_pressure.get("closeOrders") or 0)
    recent_open_close_gap = int(symbol_pressure.get("openCloseGap") or 0)
    recent_consecutive_open_streak = int(symbol_pressure.get("consecutiveOpenStreak") or 0)
    symbol_cycle_block_reason = dip_swing_symbol_cycle_block_reason(symbol_pressure)
    symbol_cycle_blocked = bool(symbol_cycle_block_reason)
    symbol_performance_blocked = (
        recent_close_orders >= DIP_SWING_SYMBOL_PERF_MIN_CLOSE_ORDERS
        and recent_net_pnl <= -DIP_SWING_SYMBOL_NEGATIVE_NET_BLOCK_USDT
    )
    symbol_taker_blocked = (
        recent_close_orders >= DIP_SWING_SYMBOL_PERF_MIN_CLOSE_ORDERS
        and recent_net_pnl <= Decimal("0")
        and recent_taker_fill_pct >= DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT
    )
    symbol_pressure_blocked = (
        recent_open_close_gap > DIP_SWING_MAX_OPEN_CLOSE_GAP
        or recent_consecutive_open_streak > DIP_SWING_MAX_CONSECUTIVE_OPEN_STREAK
    )
    if DIP_SWING_AGGRESSIVE_SCALP_MODE:
        symbol_cycle_block_reason = ""
        symbol_cycle_blocked = False
        symbol_performance_blocked = False
        symbol_taker_blocked = False
        symbol_pressure_blocked = False
    funding_drag_pct = max(Decimal("0"), funding_rate_pct) * Decimal("0.35")
    estimated_cost_pct = max(estimated_cost_pct, recent_execution_cost_floor_pct + funding_drag_pct)
    setup_edge_pct = max(ema_spread_pct, Decimal("0")) + max(fast_slope_pct, Decimal("0")) + max(rebound_pct, Decimal("0"))
    net_edge_pct = setup_edge_pct - estimated_cost_pct
    edge_cost_ratio = (setup_edge_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
    range_cost_ratio = (volatility_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
    atr_cost_ratio = (atr_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
    edge_cost_ready = edge_cost_ratio >= DIP_SWING_MIN_EDGE_COST_RATIO
    range_cost_ready = range_cost_ratio >= DIP_SWING_MIN_RANGE_COST_RATIO
    atr_cost_ready = atr_cost_ratio >= DIP_SWING_MIN_ATR_COST_RATIO
    open_interest_usd = normalized_open_interest_usd(
        str(target.get("swapInstId") or ""),
        open_interest_row,
        last_price=last_price,
    )
    liquidity_ready = (
        avg_quote_volume_usd >= DIP_SWING_MIN_AVG_QUOTE_VOLUME_USD
        or open_interest_usd >= DIP_SWING_MIN_OPEN_INTEREST_USD
    )
    factor_bundle = build_dip_swing_factor_bundle(
        signal,
        entry_score=entry_score,
        edge_cost_ready=edge_cost_ready,
        range_cost_ready=range_cost_ready,
        atr_cost_ready=atr_cost_ready,
        liquidity_ready=liquidity_ready,
    )
    factor_bundle = copy.deepcopy(factor_bundle)
    entry_factors = dict(factor_bundle.get("entryFactors") or {})
    entry_vetoes = list(factor_bundle.get("entryVetoes") or [])
    execution_health_ready = not (symbol_performance_blocked or symbol_taker_blocked or symbol_pressure_blocked)
    entry_factors["executionHealth"] = execution_health_ready
    entry_factors["closeLoop"] = not symbol_cycle_blocked
    if symbol_performance_blocked:
        entry_vetoes.append(
            f"近场净结果 {format_decimal(recent_net_pnl, 2)}U，最近 {recent_close_orders} 笔平仓表现偏差"
        )
    elif recent_close_orders >= DIP_SWING_SYMBOL_PERF_MIN_CLOSE_ORDERS and recent_net_pnl <= -DIP_SWING_SYMBOL_NEGATIVE_NET_WARN_USDT:
        entry_vetoes.append(
            f"近场净结果 {format_decimal(recent_net_pnl, 2)}U，先别继续放大这个标的"
        )
    if symbol_taker_blocked:
        entry_vetoes.append(
            f"近期 taker 占比 {format_decimal(recent_taker_fill_pct, 1)}%，执行质量偏差"
        )
    if symbol_pressure_blocked:
        entry_vetoes.append(
            f"开平差 {recent_open_close_gap} / 连开 {recent_consecutive_open_streak}，交易结构失衡"
        )
    if symbol_cycle_blocked:
        entry_vetoes.append(symbol_cycle_block_reason)
    factor_bundle["entryFactors"] = entry_factors
    factor_bundle["entryVetoes"] = entry_vetoes
    factor_bundle["entryReady"] = all(bool(value) for value in entry_factors.values())
    factor_bundle["entryFactorScore"] = sum(1 for value in entry_factors.values() if value)
    liquidity_score = min(avg_quote_volume_usd / Decimal("1000000"), Decimal("12")) + min(
        open_interest_usd / Decimal("10000000"),
        Decimal("12"),
    )
    symbol_performance_penalty = Decimal("0")
    if recent_net_pnl < 0:
        symbol_performance_penalty += abs(recent_net_pnl) / DIP_SWING_SYMBOL_PERFORMANCE_PENALTY_DIVISOR
    if recent_taker_fill_pct > DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT:
        symbol_performance_penalty += (recent_taker_fill_pct - DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT) * Decimal("0.4")
    if recent_open_close_gap > DIP_SWING_MAX_OPEN_CLOSE_GAP:
        symbol_performance_penalty += Decimal(recent_open_close_gap - DIP_SWING_MAX_OPEN_CLOSE_GAP) * Decimal("2")
    if recent_consecutive_open_streak > DIP_SWING_MAX_CONSECUTIVE_OPEN_STREAK:
        symbol_performance_penalty += Decimal(recent_consecutive_open_streak - DIP_SWING_MAX_CONSECUTIVE_OPEN_STREAK) * Decimal("1.5")
    execution_quality_score = (
        max(net_edge_pct, Decimal("0")) * Decimal("6")
        + edge_cost_ratio * Decimal("1.8")
        + range_cost_ratio * Decimal("1.2")
        + atr_cost_ratio * Decimal("1.1")
        + liquidity_score
        + Decimal(factor_bundle.get("entryFactorScore") or 0) * Decimal("0.5")
    ) - symbol_performance_penalty
    open_position = next((row for row in positions if safe_decimal(row.get("pos"), "0") != 0), {})
    pos_value = safe_decimal(open_position.get("pos"), "0")
    position_size = abs(pos_value)
    position_side = "flat"
    if pos_value > 0:
        position_side = "long"
    elif pos_value < 0:
        position_side = "short"
    entry_price = safe_decimal(open_position.get("avgPx"), "0")
    floating_pnl = safe_decimal(open_position.get("upl"), "0")
    liq_price = safe_decimal(open_position.get("liqPx"), "0")
    liq_buffer = liquidation_buffer_pct(last_price, liq_price, position_side)
    open_interest = (
        open_interest_row.get("oiUsd")
        or open_interest_row.get("oi")
        or open_interest_row.get("oiCcy")
        or "--"
    )
    candidate = (
        bool(factor_bundle.get("entryReady"))
        and net_edge_pct >= DIP_SWING_MIN_NET_EDGE_PCT
        and position_side == "flat"
        and not symbol_performance_blocked
        and not symbol_taker_blocked
        and not symbol_pressure_blocked
        and not symbol_cycle_blocked
    )
    return {
        "symbol": str(target.get("watchlistSymbol") or strategy_symbol_label(target)),
        "target": target,
        "swapTicker": swap_ticker,
        "signal": signal,
        "candles": candles,
        "lastPrice": last_price,
        "markPrice": mark_price,
        "fundingRatePct": funding_rate_pct,
        "basisPct": basis_pct,
        "volatilityPct": volatility_pct,
        "atrPct": atr_pct,
        "avgQuoteVolumeUsd": avg_quote_volume_usd,
        "pullbackPct": pullback_pct,
        "reboundPct": rebound_pct,
        "pullbackThresholdPct": pullback_threshold_pct,
        "reboundThresholdPct": rebound_threshold_pct,
        "reboundLookbackBars": rebound_lookback_bars,
        "emaSpreadPct": ema_spread_pct,
        "fastSlopePct": fast_slope_pct,
        "slowSlopePct": slow_slope_pct,
        "priceVsFastPct": price_vs_fast_pct,
        "entryScore": entry_score,
        "exitScore": exit_score,
        "estimatedCostPct": estimated_cost_pct,
        "setupEdgePct": setup_edge_pct,
        "netEdgePct": net_edge_pct,
        "edgeCostRatio": edge_cost_ratio,
        "rangeCostRatio": range_cost_ratio,
        "atrCostRatio": atr_cost_ratio,
        "edgeCostReady": edge_cost_ready,
        "rangeCostReady": range_cost_ready,
        "atrCostReady": atr_cost_ready,
        "liquidityReady": liquidity_ready,
        "openInterestUsd": open_interest_usd,
        "liquidityScore": liquidity_score,
        "executionQualityScore": execution_quality_score,
        "factorBundle": factor_bundle,
        "symbolPressure": symbol_pressure,
        "recentAvgAbsSlipMarkPct": recent_avg_abs_slip_mark_pct,
        "recentAvgAbsSlipIndexPct": recent_avg_abs_slip_index_pct,
        "recentTakerFillPct": recent_taker_fill_pct,
        "executionCostFloorPct": recent_execution_cost_floor_pct,
        "recentNetPnl": recent_net_pnl,
        "recentCloseOrders": recent_close_orders,
        "symbolCycleBlocked": symbol_cycle_blocked,
        "symbolCycleBlockReason": symbol_cycle_block_reason,
        "symbolPerformanceBlocked": symbol_performance_blocked,
        "symbolTakerBlocked": symbol_taker_blocked,
        "symbolPressureBlocked": symbol_pressure_blocked,
        "symbolPerformancePenalty": symbol_performance_penalty,
        "positionSide": position_side,
        "positionSize": position_size,
        "entryPrice": entry_price,
        "floatingPnl": floating_pnl,
        "liqPrice": liq_price,
        "liqBufferPct": liq_buffer,
        "openInterest": open_interest,
        "candidate": candidate,
    }


def fetch_dip_swing_target_snapshot(
    client: OkxClient,
    target: dict[str, Any],
    execution_journal: dict[str, Any] | None = None,
) -> dict[str, Any]:
    inst_id = str(target.get("swapInstId") or "")
    jobs: dict[str, Any] = {
        "swapTicker": lambda: extract_first_row(client.get_ticker(inst_id)),
        "markPrice": lambda: extract_first_row(client.get_mark_price("SWAP", inst_id)),
        "fundingRate": lambda: extract_first_row(client.get_funding_rate(inst_id)),
        "openInterest": lambda: extract_first_row(client.get_open_interest("SWAP", inst_id)),
        "swapCandles": lambda: get_closed_candles(
            client,
            inst_id,
            target["bar"],
            max(int(target.get("slowEma", 21)) + 30, 80),
        ),
        "positions": lambda: client.get_positions(inst_id).get("data", []) if getattr(client, "api_key", "") else [],
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
        raise OkxApiError(f"{inst_id} 分析失败: {'; '.join(errors)}")
    return evaluate_dip_swing_target_snapshot(
        target,
        fetched["swapTicker"],
        fetched["markPrice"],
        fetched["fundingRate"],
        fetched["openInterest"],
        fetched["swapCandles"],
        fetched["positions"],
        execution_journal=execution_journal,
    )


def build_profit_loop_snapshot_metrics(snapshot: dict[str, Any]) -> dict[str, Any]:
    signal = snapshot.get("signal") or {}
    candles = snapshot.get("candles") or []
    side = profit_loop_trade_side(signal, candles) if candles else "buy"
    ema_spread_pct = abs(safe_decimal(snapshot.get("emaSpreadPct"), "0"))
    fast_slope_pct = abs(safe_decimal(snapshot.get("fastSlopePct"), "0"))
    slow_slope_pct = abs(safe_decimal(snapshot.get("slowSlopePct"), "0"))
    atr_pct = safe_decimal(snapshot.get("atrPct"), "0")
    volatility_pct = safe_decimal(snapshot.get("volatilityPct"), "0")
    estimated_cost_pct = safe_decimal(snapshot.get("estimatedCostPct"), "0")
    liquidity_score = safe_decimal(snapshot.get("liquidityScore"), "0")
    execution_quality_score = safe_decimal(snapshot.get("executionQualityScore"), "0")
    symbol_performance_penalty = safe_decimal(snapshot.get("symbolPerformancePenalty"), "0")
    basis_penalty = abs(safe_decimal(snapshot.get("basisPct"), "0")) * Decimal("0.5")
    predicted_move_pct = max(
        atr_pct * Decimal("0.55"),
        ema_spread_pct + fast_slope_pct + (slow_slope_pct / Decimal("2")),
        volatility_pct * Decimal("0.35"),
    )
    predicted_net_pct = predicted_move_pct - estimated_cost_pct
    loop_quality_score = (
        execution_quality_score
        + (predicted_net_pct * Decimal("16"))
        + liquidity_score
        - basis_penalty
        - symbol_performance_penalty
    )
    return {
        "plannedSide": side,
        "plannedSideLabel": profit_loop_trade_side_label(side),
        "predictedMovePct": predicted_move_pct,
        "predictedNetPct": predicted_net_pct,
        "loopQualityScore": loop_quality_score,
    }


def list_dip_swing_market_symbols(
    client: OkxClient,
    config: dict[str, Any],
    *,
    limit: int = DIP_SWING_SCAN_SYMBOL_LIMIT,
) -> list[str]:
    watchlist_symbols = normalize_watchlist_symbols(config.get("watchlistSymbols"), config)
    now = time.time()
    with DIP_SWING_SCAN_LOCK:
        cached_symbols = list(DIP_SWING_MARKET_UNIVERSE_CACHE.get("symbols") or [])
        cached_ts = float(DIP_SWING_MARKET_UNIVERSE_CACHE.get("ts") or 0.0)
    shared_symbols = cached_symbols
    if not shared_symbols or now - cached_ts >= 900:
        swap_rows = (client.get_public_instruments("SWAP").get("data") or [])
        symbols: list[str] = []
        seen: set[str] = set()
        for row in swap_rows:
            inst_id = str(row.get("instId") or "")
            if not inst_id.endswith("-USDT-SWAP"):
                continue
            state = str(row.get("state") or "live").strip().lower()
            settle_ccy = str(row.get("settleCcy") or "").strip().upper()
            if state not in {"", "live"} or settle_ccy not in {"", "USDT"}:
                continue
            symbol = normalize_symbol_token(inst_id)
            if symbol and symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)
        shared_symbols = symbols
        with DIP_SWING_SCAN_LOCK:
            DIP_SWING_MARKET_UNIVERSE_CACHE["ts"] = now
            DIP_SWING_MARKET_UNIVERSE_CACHE["symbols"] = list(shared_symbols)

    ordered: list[str] = []
    for symbol in watchlist_symbols:
        if symbol in shared_symbols and symbol not in ordered:
            ordered.append(symbol)
    for symbol in BASIS_ARB_PREFERRED_SYMBOLS:
        if symbol in shared_symbols and symbol not in ordered:
            ordered.append(symbol)
    for symbol in shared_symbols:
        if symbol not in ordered:
            ordered.append(symbol)
        if len(ordered) >= max(limit, len(watchlist_symbols)):
            break
    return ordered[: max(limit, len(watchlist_symbols))]


def scan_dip_swing_market_snapshots(
    client: OkxClient,
    config: dict[str, Any],
    symbols: list[str],
    *,
    execution_journal: dict[str, Any] | None = None,
) -> tuple[list[dict[str, Any]], list[str]]:
    normalized_symbols = [normalize_symbol_token(symbol) for symbol in symbols if normalize_symbol_token(symbol)]
    if not normalized_symbols:
        return [], []
    cache_key = json.dumps(
        {
            "symbols": normalized_symbols,
            "bar": str(config.get("bar") or "15m"),
            "fast": int(config.get("fastEma", 12) or 12),
            "slow": int(config.get("slowEma", 48) or 48),
            "tp": decimal_to_str(safe_decimal(config.get("takeProfitPct"), "8")),
            "sl": decimal_to_str(safe_decimal(config.get("stopLossPct"), "1.2")),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    now = time.time()
    with DIP_SWING_SCAN_LOCK:
        cached_key = str(DIP_SWING_MARKET_SCAN_CACHE.get("key") or "")
        cached_ts = float(DIP_SWING_MARKET_SCAN_CACHE.get("ts") or 0.0)
        if cached_key == cache_key and now - cached_ts < 20:
            return copy.deepcopy(DIP_SWING_MARKET_SCAN_CACHE.get("rows") or []), []

    targets = [build_dip_swing_scan_target(config, symbol) for symbol in normalized_symbols]
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(len(targets), DIP_SWING_SCAN_WORKER_LIMIT))
    ) as executor:
        future_map = {
            executor.submit(fetch_dip_swing_target_snapshot, client, target, execution_journal): target
            for target in targets
        }
        for future in concurrent.futures.as_completed(future_map):
            target = future_map[future]
            try:
                rows.append(future.result())
            except Exception as exc:
                errors.append(f"{target.get('watchlistSymbol') or target.get('swapInstId')}: {exc}")
    if errors and not rows:
        raise OkxApiError(f"{ONLY_STRATEGY_LABEL}扩展市场扫描失败: {'; '.join(errors)}")
    with DIP_SWING_SCAN_LOCK:
        DIP_SWING_MARKET_SCAN_CACHE["ts"] = now
        DIP_SWING_MARKET_SCAN_CACHE["key"] = cache_key
        DIP_SWING_MARKET_SCAN_CACHE["rows"] = copy.deepcopy(rows)
    return rows, errors


def evaluate_basis_arb_target_snapshot(
    target: dict[str, Any],
    spot_ticker: dict[str, Any],
    swap_ticker: dict[str, Any],
    funding_row: dict[str, Any],
) -> dict[str, Any]:
    spot_bid = ticker_bid_price(spot_ticker)
    spot_ask = ticker_ask_price(spot_ticker)
    swap_bid = ticker_bid_price(swap_ticker)
    swap_ask = ticker_ask_price(swap_ticker)
    spot_last = safe_decimal(spot_ticker.get("last"), "0")
    swap_last = safe_decimal(swap_ticker.get("last"), "0")
    funding_rate_pct = safe_decimal(funding_row.get("fundingRate"), "0") * Decimal("100")
    entry_threshold = safe_decimal(target.get("arbEntrySpreadPct"), "0")
    exit_threshold = safe_decimal(target.get("arbExitSpreadPct"), "0")
    min_funding = safe_decimal(target.get("arbMinFundingRatePct"), "0")
    require_funding_alignment = bool(target.get("arbRequireFundingAlignment"))
    funding_aligned = funding_rate_pct >= min_funding if require_funding_alignment else True
    entry_spread_pct = pct_gap(swap_bid, spot_ask)
    close_spread_pct = pct_gap(swap_ask, spot_bid)
    reverse_basis = entry_spread_pct <= 0
    funding_blocked = (
        entry_spread_pct > 0
        and entry_spread_pct >= entry_threshold
        and require_funding_alignment
        and not funding_aligned
    )
    return {
        "symbol": str(target.get("watchlistSymbol") or target.get("spotInstId") or "").split("-")[0],
        "target": target,
        "spotTicker": spot_ticker,
        "swapTicker": swap_ticker,
        "spotLast": spot_last,
        "swapLast": swap_last,
        "fundingRatePct": funding_rate_pct,
        "entrySpreadPct": entry_spread_pct,
        "closeSpreadPct": close_spread_pct,
        "entryThresholdPct": entry_threshold,
        "exitThresholdPct": exit_threshold,
        "fundingThresholdPct": min_funding,
        "requireFundingAlignment": require_funding_alignment,
        "fundingAligned": funding_aligned,
        "reverseBasis": reverse_basis,
        "fundingBlocked": funding_blocked,
        "candidate": (not reverse_basis) and entry_spread_pct >= entry_threshold and funding_aligned,
    }


def fetch_basis_arb_target_snapshot(client: OkxClient, target: dict[str, Any]) -> dict[str, Any]:
    spot_ticker = extract_first_row(client.get_ticker(target["spotInstId"]))
    swap_ticker = extract_first_row(client.get_ticker(target["swapInstId"]))
    funding_row = extract_first_row(client.get_funding_rate(target["swapInstId"]))
    return evaluate_basis_arb_target_snapshot(target, spot_ticker, swap_ticker, funding_row)


def list_basis_arb_market_symbols(
    client: OkxClient,
    config: dict[str, Any],
    *,
    limit: int = BASIS_ARB_SCAN_SYMBOL_LIMIT,
) -> list[str]:
    watchlist_symbols = normalize_watchlist_symbols(config.get("watchlistSymbols"), config)
    now = time.time()
    with BASIS_ARB_SCAN_LOCK:
        cached_symbols = list(BASIS_ARB_MARKET_UNIVERSE_CACHE.get("symbols") or [])
        cached_ts = float(BASIS_ARB_MARKET_UNIVERSE_CACHE.get("ts") or 0.0)
    shared_symbols = cached_symbols
    if not shared_symbols or now - cached_ts >= 900:
        spot_rows = (client.get_public_instruments("SPOT").get("data") or [])
        swap_rows = (client.get_public_instruments("SWAP").get("data") or [])

        def collect_spot_symbols(rows: list[dict[str, Any]]) -> set[str]:
            symbols: set[str] = set()
            for row in rows:
                inst_id = str(row.get("instId") or "")
                if not inst_id.endswith("-USDT"):
                    continue
                state = str(row.get("state") or "live").strip().lower()
                quote_ccy = str(row.get("quoteCcy") or "").strip().upper()
                if state not in {"", "live"} or quote_ccy not in {"", "USDT"}:
                    continue
                symbol = normalize_symbol_token(inst_id)
                if symbol:
                    symbols.add(symbol)
            return symbols

        def collect_swap_symbols(rows: list[dict[str, Any]]) -> set[str]:
            symbols: set[str] = set()
            for row in rows:
                inst_id = str(row.get("instId") or "")
                if not inst_id.endswith("-USDT-SWAP"):
                    continue
                state = str(row.get("state") or "live").strip().lower()
                settle_ccy = str(row.get("settleCcy") or "").strip().upper()
                if state not in {"", "live"} or settle_ccy not in {"", "USDT"}:
                    continue
                symbol = normalize_symbol_token(inst_id)
                if symbol:
                    symbols.add(symbol)
            return symbols

        shared_symbols = sorted(collect_spot_symbols(spot_rows) & collect_swap_symbols(swap_rows))
        with BASIS_ARB_SCAN_LOCK:
            BASIS_ARB_MARKET_UNIVERSE_CACHE["ts"] = now
            BASIS_ARB_MARKET_UNIVERSE_CACHE["symbols"] = list(shared_symbols)

    ordered: list[str] = []
    for symbol in watchlist_symbols:
        if symbol in shared_symbols and symbol not in ordered:
            ordered.append(symbol)
    for symbol in BASIS_ARB_PREFERRED_SYMBOLS:
        if symbol in shared_symbols and symbol not in ordered:
            ordered.append(symbol)
    for symbol in shared_symbols:
        if symbol not in ordered:
            ordered.append(symbol)
        if len(ordered) >= max(limit, len(watchlist_symbols)):
            break
    return ordered[: max(limit, len(watchlist_symbols))]


def scan_basis_arb_market_snapshots(
    client: OkxClient,
    config: dict[str, Any],
    symbols: list[str],
) -> tuple[list[dict[str, Any]], list[str]]:
    normalized_symbols = [normalize_symbol_token(symbol) for symbol in symbols if normalize_symbol_token(symbol)]
    if not normalized_symbols:
        return [], []
    cache_key = json.dumps(
        {
            "symbols": normalized_symbols,
            "entry": decimal_to_str(safe_decimal(config.get("arbEntrySpreadPct"), "0")),
            "exit": decimal_to_str(safe_decimal(config.get("arbExitSpreadPct"), "0")),
            "funding": decimal_to_str(safe_decimal(config.get("arbMinFundingRatePct"), "0")),
            "requireFundingAlignment": bool(config.get("arbRequireFundingAlignment")),
        },
        ensure_ascii=False,
        sort_keys=True,
    )
    now = time.time()
    with BASIS_ARB_SCAN_LOCK:
        cached_key = str(BASIS_ARB_MARKET_SCAN_CACHE.get("key") or "")
        cached_ts = float(BASIS_ARB_MARKET_SCAN_CACHE.get("ts") or 0.0)
        if cached_key == cache_key and now - cached_ts < 20:
            return copy.deepcopy(BASIS_ARB_MARKET_SCAN_CACHE.get("rows") or []), []

    targets = [build_basis_arb_scan_target(config, symbol) for symbol in normalized_symbols]
    rows: list[dict[str, Any]] = []
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(len(targets), DIP_SWING_SCAN_WORKER_LIMIT))
    ) as executor:
        future_map = {executor.submit(fetch_basis_arb_target_snapshot, client, target): target for target in targets}
        for future in concurrent.futures.as_completed(future_map):
            target = future_map[future]
            try:
                rows.append(future.result())
            except Exception as exc:
                errors.append(f"{target.get('watchlistSymbol') or target.get('spotInstId')}: {exc}")
    if errors and not rows:
        raise OkxApiError(f"扩展市场扫描失败: {'; '.join(errors)}")
    with BASIS_ARB_SCAN_LOCK:
        BASIS_ARB_MARKET_SCAN_CACHE["ts"] = now
        BASIS_ARB_MARKET_SCAN_CACHE["key"] = cache_key
        BASIS_ARB_MARKET_SCAN_CACHE["rows"] = copy.deepcopy(rows)
    return rows, errors


def build_basis_arb_analysis(
    automation: dict[str, Any],
    client: OkxClient,
) -> dict[str, Any]:
    targets = build_execution_targets(automation)
    watchlist_count = len(targets)

    target_snapshots: list[dict[str, Any]] = []
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(len(targets), DIP_SWING_SCAN_WORKER_LIMIT))
    ) as executor:
        future_map = {executor.submit(fetch_basis_arb_target_snapshot, client, target): target for target in targets}
        for future in concurrent.futures.as_completed(future_map):
            target = future_map[future]
            try:
                target_snapshots.append(future.result())
            except Exception as exc:
                errors.append(f"{target.get('watchlistSymbol') or target.get('spotInstId')}: {exc}")

    if errors:
        raise OkxApiError(f"联网分析失败: {'; '.join(errors)}")

    candidate_snapshots = [row for row in target_snapshots if row.get("candidate")]
    positive_snapshots = [row for row in target_snapshots if not row.get("reverseBasis")]
    reverse_basis_count = sum(1 for row in target_snapshots if row.get("reverseBasis"))
    funding_blocked_count = sum(1 for row in target_snapshots if row.get("fundingBlocked"))
    watchlist_symbols = [str(row.get("symbol") or "") for row in target_snapshots if row.get("symbol")]

    market_scan_symbols = list_basis_arb_market_symbols(client, automation)
    extra_scan_symbols = [symbol for symbol in market_scan_symbols if symbol not in set(watchlist_symbols)]
    extra_market_snapshots, market_scan_errors = scan_basis_arb_market_snapshots(client, automation, extra_scan_symbols)
    market_snapshots = target_snapshots + extra_market_snapshots
    market_candidate_snapshots = [row for row in market_snapshots if row.get("candidate")]
    market_positive_snapshots = [row for row in market_snapshots if not row.get("reverseBasis")]
    market_reverse_basis_count = sum(1 for row in market_snapshots if row.get("reverseBasis"))
    market_funding_blocked_count = sum(1 for row in market_snapshots if row.get("fundingBlocked"))
    market_candidate_count = len(market_candidate_snapshots)
    market_top_candidates = [
        {
            "symbol": str(row.get("symbol") or ""),
            "entrySpreadPct": compact_metric(safe_decimal(row.get("entrySpreadPct"), "0"), "0.001"),
            "fundingRatePct": compact_metric(safe_decimal(row.get("fundingRatePct"), "0"), "0.001"),
        }
        for row in sorted(
            market_candidate_snapshots,
            key=lambda row: (
                safe_decimal(row.get("entrySpreadPct"), "0"),
                safe_decimal(row.get("fundingRatePct"), "0"),
            ),
            reverse=True,
        )[:3]
    ]
    outside_watchlist_candidates = [
        item for item in market_top_candidates if str(item.get("symbol") or "") not in set(watchlist_symbols)
    ]

    if candidate_snapshots:
        selected_snapshot = max(
            candidate_snapshots,
            key=lambda row: (safe_decimal(row.get("entrySpreadPct"), "0"), safe_decimal(row.get("fundingRatePct"), "0")),
        )
    elif positive_snapshots:
        selected_snapshot = max(
            positive_snapshots,
            key=lambda row: (safe_decimal(row.get("entrySpreadPct"), "0"), safe_decimal(row.get("fundingRatePct"), "0")),
        )
    elif target_snapshots:
        selected_snapshot = max(
            target_snapshots,
            key=lambda row: safe_decimal(row.get("entrySpreadPct"), "-999"),
        )
    else:
        selected_snapshot = {
            "symbol": strategy_symbol_label(automation),
            "target": resolve_selected_execution_target(automation),
            "spotTicker": {},
            "swapTicker": {},
            "spotLast": Decimal("0"),
            "swapLast": Decimal("0"),
            "fundingRatePct": Decimal("0"),
            "entrySpreadPct": Decimal("0"),
            "closeSpreadPct": Decimal("0"),
            "entryThresholdPct": safe_decimal(automation.get("arbEntrySpreadPct"), "0"),
            "exitThresholdPct": safe_decimal(automation.get("arbExitSpreadPct"), "0"),
            "fundingThresholdPct": safe_decimal(automation.get("arbMinFundingRatePct"), "0"),
            "requireFundingAlignment": bool(automation.get("arbRequireFundingAlignment")),
            "fundingAligned": True,
            "reverseBasis": False,
            "fundingBlocked": False,
            "candidate": False,
        }

    selected_target = copy.deepcopy(selected_snapshot.get("target") or resolve_selected_execution_target(automation))
    selected_symbol = str(selected_snapshot.get("symbol") or strategy_symbol_label(automation))
    selected_config = deep_merge(
        deep_merge({}, automation),
        {
            "spotInstId": selected_target.get("spotInstId"),
            "swapInstId": selected_target.get("swapInstId"),
        },
    )
    allocated_spot_budget = safe_decimal(selected_target.get("spotQuoteBudget"), "0")

    detail_jobs: dict[str, Any] = {
        "markPrice": lambda: extract_first_row(client.get_mark_price("SWAP", selected_target["swapInstId"])),
        "openInterest": lambda: extract_first_row(client.get_open_interest("SWAP", selected_target["swapInstId"])),
        "spotCandles": lambda: get_closed_candles(
            client,
            selected_target["spotInstId"],
            selected_target["bar"],
            max(int(selected_target.get("slowEma", 21)) + 20, 60),
        ),
        "swapCandles": lambda: get_closed_candles(
            client,
            selected_target["swapInstId"],
            selected_target["bar"],
            max(int(selected_target.get("slowEma", 21)) + 20, 60),
        ),
    }

    fetched: dict[str, Any] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(detail_jobs)) as executor:
        future_map = {executor.submit(fn): key for key, fn in detail_jobs.items()}
        for future in concurrent.futures.as_completed(future_map):
            key = future_map[future]
            try:
                fetched[key] = future.result()
            except Exception as exc:
                errors.append(f"{key}: {exc}")

    if errors:
        raise OkxApiError(f"联网分析失败: {'; '.join(errors)}")

    spot_ticker = selected_snapshot.get("spotTicker") or {}
    swap_ticker = selected_snapshot.get("swapTicker") or {}
    mark_price = safe_decimal(fetched["markPrice"].get("markPx"), "0")
    funding_rate_pct = safe_decimal(selected_snapshot.get("fundingRatePct"), "0")
    spot_bid = ticker_bid_price(spot_ticker)
    spot_ask = ticker_ask_price(spot_ticker)
    swap_bid = ticker_bid_price(swap_ticker)
    swap_ask = ticker_ask_price(swap_ticker)
    spot_last = safe_decimal(selected_snapshot.get("spotLast"), "0")
    swap_last = safe_decimal(selected_snapshot.get("swapLast"), "0")
    entry_spread_pct = safe_decimal(selected_snapshot.get("entrySpreadPct"), "0")
    close_spread_pct = safe_decimal(selected_snapshot.get("closeSpreadPct"), "0")
    candidate_count = len(candidate_snapshots)
    mid_spread_pct = pct_gap(swap_last, spot_last)
    basis_pct = pct_gap(swap_last, mark_price)
    volatility_pct = max(recent_range_pct(fetched["spotCandles"]), recent_range_pct(fetched["swapCandles"]))
    entry_threshold = safe_decimal(selected_snapshot.get("entryThresholdPct"), "0")
    exit_threshold = safe_decimal(selected_snapshot.get("exitThresholdPct"), "0")
    min_funding = safe_decimal(selected_snapshot.get("fundingThresholdPct"), "0")
    require_funding_alignment = bool(selected_snapshot.get("requireFundingAlignment"))
    funding_aligned = bool(selected_snapshot.get("fundingAligned"))

    aggressive_scalp_mode = DIP_SWING_AGGRESSIVE_SCALP_MODE
    aggressive_scalp_mode = DIP_SWING_AGGRESSIVE_SCALP_MODE
    blockers: list[str] = []
    warnings: list[str] = []
    reverse_basis = bool(selected_snapshot.get("reverseBasis"))
    if reverse_basis:
        warnings.append(f"当前有 {reverse_basis_count} 币是负基差，这版只做现货多 / 永续空，不做反向套利")
    if require_funding_alignment and not funding_aligned:
        blockers.append("当前资金费不足以支撑现货多 / 永续空套利")
    if volatility_pct >= Decimal("4.5"):
        warnings.append("最近波动偏大，套利腿可能被趋势拉扯")
    if basis_pct <= Decimal("-0.10"):
        warnings.append("永续最新价低于标记价，套利窗口不稳定")
    if funding_blocked_count:
        warnings.append(f"当前有 {funding_blocked_count} 币资金费未对齐")
    if watchlist_count > 1:
        warnings.append(
            f"当前 {watchlist_count} 币并行，当前优先币 {selected_symbol} 分到约 {decimal_to_str(allocated_spot_budget)}U 现货预算，永续按动态仓位执行"
        )
    if market_candidate_count == 0:
        warnings.append(f"扩展市场已扫 {len(market_snapshots)} 币，当前也没有正基差候选")
    elif outside_watchlist_candidates:
        warnings.append(
            "watchlist 暂时没窗口，但扩展市场有候选: "
            + " / ".join(
                f"{item['symbol']} {item['entrySpreadPct']}% · 资金费 {item['fundingRatePct']}%"
                for item in outside_watchlist_candidates
            )
        )
    elif market_candidate_count > candidate_count:
        warnings.append(f"扩展市场共发现 {market_candidate_count} 币可做，watchlist 当前命中 {candidate_count} 币")
    if market_scan_errors:
        skipped_symbols = [str(item).split(":", 1)[0] for item in market_scan_errors[:3]]
        warnings.append(
            f"扩展扫描跳过 {len(market_scan_errors)} 币"
            + (f": {', '.join(skipped_symbols)}" if skipped_symbols else "")
        )

    allow_new_entries = candidate_count > 0 and not blockers
    if allow_new_entries:
        decision = "execute"
        decision_label = f"可做 {candidate_count} 币"
    elif market_candidate_count > 0:
        decision = "observe"
        decision_label = f"watchlist 无窗口，市场有 {market_candidate_count} 币"
    elif blockers:
        decision = "skip"
        decision_label = "暂停套利"
    elif funding_blocked_count > 0 and candidate_count == 0 and reverse_basis_count < watchlist_count:
        decision = "observe"
        decision_label = "资金费未对齐"
    elif reverse_basis:
        decision = "observe"
        decision_label = "当前负基差，不做这侧套利"
    else:
        decision = "observe"
        decision_label = "等待价差扩大"

    symbol = selected_symbol if watchlist_count <= 1 else f"{selected_symbol} +{watchlist_count - 1}"
    summary_bits = [
        f"{symbol} 高频套利",
        f"入场 {decimal_to_str(entry_spread_pct.quantize(Decimal('0.001')))}%",
        f"回补 {decimal_to_str(close_spread_pct.quantize(Decimal('0.001')))}%",
        f"资金费 {decimal_to_str(funding_rate_pct.quantize(Decimal('0.001')))}%",
    ]
    if allow_new_entries:
        summary_bits.append(
            f"watchlist 可做 {candidate_count} / 市场候选 {market_candidate_count} / 反向 {reverse_basis_count}"
        )
    elif market_candidate_count > 0:
        summary_bits.append(
            "watchlist 暂时没窗口，但扩展市场有候选: "
            + " / ".join(item["symbol"] for item in outside_watchlist_candidates or market_top_candidates)
        )
    elif blockers:
        summary_bits.append(blockers[0])
    elif funding_blocked_count > 0 and candidate_count == 0 and reverse_basis_count < watchlist_count:
        summary_bits.append("正基差已出现，但资金费还不够整齐")
    elif reverse_basis:
        summary_bits.append("当前是负基差，这版先不做反向套利")
    else:
        summary_bits.append("继续等待正基差扩大")

    return {
        "statusText": "已联网分析",
        "decision": decision,
        "decisionLabel": decision_label,
        "summary": " · ".join(summary_bits),
        "selectedStrategyName": f"{symbol} 高频套利",
        "selectedStrategyDetail": strategy_detail_line(selected_config),
        "selectedReturnPct": "",
        "selectedDrawdownPct": "",
        "selectedWinRatePct": "",
        "selectedScore": "",
        "allowNewEntries": allow_new_entries,
        "optimizerRefreshed": False,
        "lastAnalyzedAt": now_local_iso(),
        "marketRegime": "价差回归套利",
        "spotTrend": f"买入参考 {decimal_to_str(spot_ask)}",
        "swapTrend": f"对冲参考 {decimal_to_str(swap_bid)}",
        "volatilityPct": compact_metric(volatility_pct, "0.01"),
        "spreadPct": compact_metric(mid_spread_pct, "0.001"),
        "basisPct": compact_metric(basis_pct, "0.001"),
        "fundingRatePct": compact_metric(funding_rate_pct, "0.001"),
        "entrySpreadPct": compact_metric(entry_spread_pct, "0.001"),
        "closeSpreadPct": compact_metric(close_spread_pct, "0.001"),
        "entrySpreadThresholdPct": compact_metric(entry_threshold, "0.001"),
        "exitSpreadThresholdPct": compact_metric(exit_threshold, "0.001"),
        "fundingThresholdPct": compact_metric(min_funding, "0.001"),
        "fundingAligned": funding_aligned,
        "watchlistCount": watchlist_count,
        "candidateCount": candidate_count,
        "reverseBasisCount": reverse_basis_count,
        "fundingBlockedCount": funding_blocked_count,
        "marketScanCount": len(market_snapshots),
        "marketCandidateCount": market_candidate_count,
        "marketReverseBasisCount": market_reverse_basis_count,
        "marketFundingBlockedCount": market_funding_blocked_count,
        "marketTopCandidates": market_top_candidates,
        "selectedWatchlistSymbol": selected_symbol,
        "allocatedSpotBudget": decimal_to_str(allocated_spot_budget),
        "allocatedSwapContracts": "",
        "openInterest": str(
            fetched["openInterest"].get("oiUsd")
            or fetched["openInterest"].get("oi")
            or fetched["openInterest"].get("oiCcy")
            or "--"
        ),
        "warnings": warnings,
        "blockers": blockers,
        "selectedConfig": selected_config,
        "research": {
            "running": False,
            "statusText": "套利分析模式",
            "mode": "basis_arb",
            "lastRunAt": now_local_iso(),
            "historyLimit": max(int(selected_target.get("slowEma", 21)) + 20, 60),
            "sampleCount": len(fetched["spotCandles"]),
            "summary": {
                "entrySpreadPct": compact_metric(entry_spread_pct, "0.001"),
                "closeSpreadPct": compact_metric(close_spread_pct, "0.001"),
                "fundingRatePct": compact_metric(funding_rate_pct, "0.001"),
            },
            "bestConfig": deep_merge({}, selected_config),
            "leaderboard": [],
            "generationSummaries": [],
            "pipeline": {
                "mode": "basis_arb",
                "status": decision,
            },
            "markets": {},
            "notes": [],
            "equityCurve": [],
        },
    }


def build_dip_swing_analysis(
    automation: dict[str, Any],
    client: OkxClient,
) -> dict[str, Any]:
    watchlist_symbols = normalize_watchlist_symbols(automation.get("watchlistSymbols"), automation)
    watchlist_targets = [build_dip_swing_scan_target(automation, symbol) for symbol in watchlist_symbols]
    execution_journal = get_execution_journal_snapshot(
        limit=160,
        live_only=prefer_live_execution_state(CONFIG.current()),
    )
    target_snapshots: list[dict[str, Any]] = []
    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=max(1, min(len(watchlist_targets), DIP_SWING_SCAN_WORKER_LIMIT))
    ) as executor:
        future_map = {
            executor.submit(fetch_dip_swing_target_snapshot, client, target, execution_journal): target
            for target in watchlist_targets
        }
        for future in concurrent.futures.as_completed(future_map):
            target = future_map[future]
            try:
                target_snapshots.append(future.result())
            except Exception as exc:
                errors.append(f"{target.get('watchlistSymbol') or target.get('swapInstId')}: {exc}")
    if errors:
        raise OkxApiError(f"联网分析失败: {'; '.join(errors)}")
    for row in target_snapshots:
        row["profitLoop"] = build_profit_loop_snapshot_metrics(row)
    holding_snapshots = [row for row in target_snapshots if str(row.get("positionSide") or "") in {"long", "short"}]
    market_scan_symbols = list_dip_swing_market_symbols(client, automation)
    extra_scan_symbols = [symbol for symbol in market_scan_symbols if symbol not in set(watchlist_symbols)]
    extra_market_snapshots, market_scan_errors = scan_dip_swing_market_snapshots(
        client,
        automation,
        extra_scan_symbols,
        execution_journal=execution_journal,
    )
    for row in extra_market_snapshots:
        row["profitLoop"] = build_profit_loop_snapshot_metrics(row)
    market_snapshots = target_snapshots + extra_market_snapshots
    watchlist_entry_ready_snapshots = [row for row in target_snapshots if bool(row.get("candidate"))]
    market_entry_ready_snapshots = [row for row in market_snapshots if bool(row.get("candidate"))]
    market_positive_snapshots = [
        row
        for row in market_snapshots
        if safe_decimal((row.get("profitLoop") or {}).get("predictedNetPct"), "0") > Decimal("0")
    ]

    ranking_key = lambda row: (
        safe_decimal((row.get("profitLoop") or {}).get("loopQualityScore"), "-999"),
        safe_decimal((row.get("profitLoop") or {}).get("predictedNetPct"), "-999"),
        safe_decimal((row.get("profitLoop") or {}).get("predictedMovePct"), "-999"),
        safe_decimal(row.get("avgQuoteVolumeUsd"), "0"),
        safe_decimal(row.get("openInterestUsd"), "0"),
        safe_decimal(row.get("executionQualityScore"), "-999"),
    )
    if holding_snapshots:
        selected_snapshot = max(
            holding_snapshots,
            key=lambda row: (
                safe_decimal(row.get("liqBufferPct"), "0"),
                safe_decimal((row.get("profitLoop") or {}).get("loopQualityScore"), "-999"),
            ),
        )
    elif market_entry_ready_snapshots:
        selected_snapshot = max(market_entry_ready_snapshots, key=ranking_key)
    elif market_positive_snapshots:
        selected_snapshot = max(market_positive_snapshots, key=ranking_key)
    elif target_snapshots:
        selected_snapshot = max(target_snapshots, key=ranking_key)
    else:
        selected_target = resolve_selected_execution_target(automation)
        selected_snapshot = {
            "symbol": strategy_symbol_label(selected_target),
            "target": selected_target,
            "signal": {"trend": "flat", "signal": "hold"},
            "lastPrice": Decimal("0"),
            "markPrice": Decimal("0"),
            "fundingRatePct": Decimal("0"),
            "basisPct": Decimal("0"),
            "volatilityPct": Decimal("0"),
            "pullbackPct": Decimal("0"),
            "reboundPct": Decimal("0"),
            "emaSpreadPct": Decimal("0"),
            "fastSlopePct": Decimal("0"),
            "slowSlopePct": Decimal("0"),
            "priceVsFastPct": Decimal("0"),
            "entryScore": 0,
            "exitScore": 0,
            "estimatedCostPct": DIP_SWING_EST_ROUNDTRIP_COST_PCT,
            "setupEdgePct": Decimal("0"),
            "netEdgePct": Decimal("0"),
            "positionSide": "flat",
            "positionSize": Decimal("0"),
            "entryPrice": Decimal("0"),
            "floatingPnl": Decimal("0"),
            "liqPrice": Decimal("0"),
            "liqBufferPct": Decimal("0"),
            "openInterest": "--",
            "candidate": False,
            "profitLoop": {
                "plannedSide": "buy",
                "plannedSideLabel": profit_loop_trade_side_label("buy"),
                "predictedMovePct": Decimal("0"),
                "predictedNetPct": Decimal("0"),
                "loopQualityScore": Decimal("0"),
            },
        }

    selected_target = copy.deepcopy(selected_snapshot.get("target") or resolve_selected_execution_target(automation))
    selected_symbol = str(selected_snapshot.get("symbol") or strategy_symbol_label(selected_target))
    selected_from_market = selected_symbol not in set(watchlist_symbols)
    selected_signal = selected_snapshot.get("signal") or {}
    last_price = safe_decimal(selected_snapshot.get("lastPrice"), "0")
    mark_price = safe_decimal(selected_snapshot.get("markPrice"), "0")
    funding_rate_pct = safe_decimal(selected_snapshot.get("fundingRatePct"), "0")
    basis_pct = safe_decimal(selected_snapshot.get("basisPct"), "0")
    volatility_pct = safe_decimal(selected_snapshot.get("volatilityPct"), "0")
    pullback_pct = safe_decimal(selected_snapshot.get("pullbackPct"), "0")
    rebound_pct = safe_decimal(selected_snapshot.get("reboundPct"), "0")
    pullback_threshold_pct = safe_decimal(selected_snapshot.get("pullbackThresholdPct"), decimal_to_str(DIP_SWING_MIN_PULLBACK_PCT))
    rebound_threshold_pct = safe_decimal(selected_snapshot.get("reboundThresholdPct"), decimal_to_str(DIP_SWING_MIN_REBOUND_PCT))
    rebound_lookback_bars = int(selected_snapshot.get("reboundLookbackBars") or dip_swing_rebound_lookback_bars(int(selected_target.get("fastEma", 12) or 12)))
    ema_spread_pct = safe_decimal(selected_snapshot.get("emaSpreadPct"), "0")
    fast_slope_pct = safe_decimal(selected_snapshot.get("fastSlopePct"), "0")
    slow_slope_pct = safe_decimal(selected_snapshot.get("slowSlopePct"), "0")
    price_vs_fast_pct = safe_decimal(selected_snapshot.get("priceVsFastPct"), "0")
    entry_score = int(selected_snapshot.get("entryScore") or 0)
    exit_score = int(selected_snapshot.get("exitScore") or 0)
    estimated_cost_pct = safe_decimal(selected_snapshot.get("estimatedCostPct"), "0")
    net_edge_pct = safe_decimal(selected_snapshot.get("netEdgePct"), "0")
    setup_edge_pct = safe_decimal(selected_snapshot.get("setupEdgePct"), "0")
    edge_cost_ratio = safe_decimal(selected_snapshot.get("edgeCostRatio"), "0")
    range_cost_ratio = safe_decimal(selected_snapshot.get("rangeCostRatio"), "0")
    atr_pct = safe_decimal(selected_snapshot.get("atrPct"), "0")
    atr_cost_ratio = safe_decimal(selected_snapshot.get("atrCostRatio"), "0")
    avg_quote_volume_usd = safe_decimal(selected_snapshot.get("avgQuoteVolumeUsd"), "0")
    open_interest_usd = safe_decimal(selected_snapshot.get("openInterestUsd"), "0")
    execution_quality_score = safe_decimal(selected_snapshot.get("executionQualityScore"), "0")
    recent_avg_abs_slip_mark_pct = safe_decimal(selected_snapshot.get("recentAvgAbsSlipMarkPct"), "0")
    recent_avg_abs_slip_index_pct = safe_decimal(selected_snapshot.get("recentAvgAbsSlipIndexPct"), "0")
    recent_taker_fill_pct = safe_decimal(selected_snapshot.get("recentTakerFillPct"), "0")
    execution_cost_floor_pct = safe_decimal(selected_snapshot.get("executionCostFloorPct"), "0")
    recent_symbol_net_pnl = safe_decimal(selected_snapshot.get("recentNetPnl"), "0")
    recent_symbol_close_orders = int(selected_snapshot.get("recentCloseOrders") or 0)
    symbol_performance_blocked = bool(selected_snapshot.get("symbolPerformanceBlocked"))
    symbol_taker_blocked = bool(selected_snapshot.get("symbolTakerBlocked"))
    symbol_pressure_blocked = bool(selected_snapshot.get("symbolPressureBlocked"))
    edge_cost_ready = bool(selected_snapshot.get("edgeCostReady"))
    range_cost_ready = bool(selected_snapshot.get("rangeCostReady"))
    atr_cost_ready = bool(selected_snapshot.get("atrCostReady"))
    liquidity_ready = bool(selected_snapshot.get("liquidityReady"))
    factor_bundle = copy.deepcopy(selected_snapshot.get("factorBundle") or {})
    entry_vetoes = list(factor_bundle.get("entryVetoes") or [])
    position_side = str(selected_snapshot.get("positionSide") or "flat")
    position_size = safe_decimal(selected_snapshot.get("positionSize"), "0")
    entry_price = safe_decimal(selected_snapshot.get("entryPrice"), "0")
    floating_pnl = safe_decimal(selected_snapshot.get("floatingPnl"), "0")
    liq_price = safe_decimal(selected_snapshot.get("liqPrice"), "0")
    liq_buffer = safe_decimal(selected_snapshot.get("liqBufferPct"), "0")
    open_interest = selected_snapshot.get("openInterest") or "--"
    leverage = safe_decimal(selected_target.get("swapLeverage"), "1")
    target_multiple = resolve_target_balance_multiple(selected_target)
    aggressive_scalp_mode = DIP_SWING_AGGRESSIVE_SCALP_MODE
    target_balance = build_target_balance_snapshot(
        safe_decimal(AUTOMATION_STATE.current().get("sessionStartEq"), "0"),
        safe_decimal(AUTOMATION_STATE.current().get("currentEq"), "0"),
        target_multiple,
    )
    ability_snapshot = build_target_execution_ability_snapshot(
        selected_target,
        state=AUTOMATION_STATE.current(),
        target_snapshot=target_balance,
    )
    loop_metrics = copy.deepcopy(selected_snapshot.get("profitLoop") or {})
    planned_side = str(loop_metrics.get("plannedSide") or "buy")
    planned_side_label = str(loop_metrics.get("plannedSideLabel") or profit_loop_trade_side_label(planned_side))
    holding_same_direction = (
        (position_side == "long" and planned_side == "buy")
        or (position_side == "short" and planned_side == "sell")
    )
    if aggressive_scalp_mode:
        symbol_cycle_blocked = False
        symbol_cycle_block_reason = ""
        symbol_performance_blocked = False
        symbol_taker_blocked = False
        symbol_pressure_blocked = False
        entry_vetoes = []
    predicted_move_pct = safe_decimal(loop_metrics.get("predictedMovePct"), "0")
    predicted_net_pct = safe_decimal(loop_metrics.get("predictedNetPct"), "0")
    loop_quality_score = safe_decimal(loop_metrics.get("loopQualityScore"), "0")
    candidate_count = len(watchlist_entry_ready_snapshots)
    market_candidate_count = len(market_entry_ready_snapshots)
    market_positive_count = len(market_positive_snapshots)
    top_candidates = [
        {
            "symbol": str(row.get("symbol") or ""),
            "plannedSideLabel": str((row.get("profitLoop") or {}).get("plannedSideLabel") or profit_loop_trade_side_label("buy")),
            "predictedNetPct": compact_metric(safe_decimal((row.get("profitLoop") or {}).get("predictedNetPct"), "0"), "0.01"),
            "qualityScore": compact_metric(safe_decimal((row.get("profitLoop") or {}).get("loopQualityScore"), "0"), "0.1"),
            "atrPct": compact_metric(safe_decimal(row.get("atrPct"), "0"), "0.01"),
        }
        for row in sorted(
            market_entry_ready_snapshots or market_positive_snapshots,
            key=ranking_key,
            reverse=True,
        )[:3]
    ]

    execution_candidate_rows = sorted(
        market_positive_snapshots or market_snapshots,
        key=ranking_key,
        reverse=True,
    )
    execution_symbols: list[str] = []
    seen_execution_symbols: set[str] = set()
    for row in execution_candidate_rows:
        symbol = str(row.get("symbol") or "").strip()
        if not symbol or symbol in seen_execution_symbols:
            continue
        seen_execution_symbols.add(symbol)
        execution_symbols.append(symbol)
        if len(execution_symbols) >= DIP_SWING_EXECUTION_TARGET_LIMIT:
            break
    if not execution_symbols:
        execution_symbols = list(watchlist_symbols) or [selected_symbol]

    selected_config = deep_merge({}, automation)
    selected_config["watchlistSymbols"] = ",".join(execution_symbols)
    selected_config["watchlistOverrides"] = copy.deepcopy(automation.get("watchlistOverrides") or {})
    selected_config["spotInstId"] = f"{execution_symbols[0]}-USDT"
    selected_config["swapInstId"] = f"{execution_symbols[0]}-USDT-SWAP"
    live_fee_rates = cache_okx_swap_fee_rates(client, str(selected_target.get("swapInstId") or ""))
    maker_fee_pct = safe_decimal(live_fee_rates.get("makerFeePct"), decimal_to_str(OKX_DEFAULT_SWAP_MAKER_FEE_PCT))
    taker_fee_pct = safe_decimal(live_fee_rates.get("takerFeePct"), decimal_to_str(OKX_DEFAULT_SWAP_TAKER_FEE_PCT))
    live_cost_snapshot = estimate_dip_swing_cost_snapshot(
        volatility_pct,
        funding_rate_pct=max(Decimal("0"), funding_rate_pct),
        maker_fee_pct=maker_fee_pct,
        taker_fee_pct=taker_fee_pct,
    )
    estimated_cost_pct = safe_decimal(live_cost_snapshot.get("estimatedCostPct"), decimal_to_str(estimated_cost_pct))
    estimated_cost_pct = max(
        estimated_cost_pct,
        execution_cost_floor_pct + safe_decimal(live_cost_snapshot.get("fundingDragPct"), "0"),
    )
    net_edge_pct = setup_edge_pct - estimated_cost_pct
    edge_cost_ratio = (setup_edge_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
    range_cost_ratio = (volatility_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
    atr_cost_ratio = (atr_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
    edge_cost_ready = edge_cost_ratio >= DIP_SWING_MIN_EDGE_COST_RATIO
    range_cost_ready = range_cost_ratio >= DIP_SWING_MIN_RANGE_COST_RATIO
    atr_cost_ready = atr_cost_ratio >= DIP_SWING_MIN_ATR_COST_RATIO
    swap_meta = get_instrument_meta(client, "SWAP", str(selected_target.get("swapInstId") or ""))
    contract_value = safe_decimal(swap_meta.get("ctVal"), decimal_to_str(default_swap_contract_value(str(selected_target.get("swapInstId") or ""))))
    net_close_snapshot = estimate_profit_loop_position_net_pnl(
        position_side=position_side,
        position_size=position_size,
        entry_price=entry_price,
        last_price=last_price,
        contract_value=contract_value,
        floating_pnl=floating_pnl,
        maker_fee_pct=maker_fee_pct,
        taker_fee_pct=taker_fee_pct,
    )
    net_close_pnl = safe_decimal(net_close_snapshot.get("netClosePnl"), "0")
    planned_contracts_for_analysis = Decimal("0")
    projected_entry_notional = Decimal("0")
    projected_entry_net_pnl = Decimal("0")
    if position_side == "flat":
        contract_margin = (last_price * contract_value) / leverage if leverage > 0 and last_price > 0 and contract_value > 0 else Decimal("0")
        current_eq = safe_decimal(AUTOMATION_STATE.current().get("currentEq"), "0")
        if aggressive_scalp_mode and current_eq > 0 and contract_margin > 0:
            planned_contracts_for_analysis = round_down((current_eq * DIP_SWING_TARGET_MAX_MARGIN_RATIO) / contract_margin, safe_decimal(swap_meta.get("lotSz"), "1"))
        entry_projection = estimate_profit_loop_entry_net_pnl(
            planned_contracts=planned_contracts_for_analysis,
            last_price=last_price,
            contract_value=contract_value,
            predicted_net_pct=predicted_net_pct,
        )
        projected_entry_notional = safe_decimal(entry_projection.get("entryNotional"), "0")
        projected_entry_net_pnl = safe_decimal(entry_projection.get("projectedNetPnl"), "0")

    blockers: list[str] = []
    warnings: list[str] = []
    if str(selected_target.get("swapTdMode") or "") != "isolated":
        blockers.append(f"{ONLY_STRATEGY_LABEL}要求逐仓，避免把整账户拖进强平")
    if leverage > DIP_SWING_MAX_LEVERAGE:
        blockers.append(f"当前杠杆 {decimal_to_str(leverage)}x 过高，已限制到 ≤ {decimal_to_str(DIP_SWING_MAX_LEVERAGE)}x")
    if position_side in {"long", "short"} and liq_buffer > 0 and liq_buffer <= DIP_SWING_MIN_LIQ_BUFFER_PCT:
        blockers.append(f"当前强平缓冲只剩 {compact_metric(liq_buffer, '0.1')}%，不满足安全缓冲")
    if funding_rate_pct >= Decimal("0.03"):
        warnings.append("当前多头资金费偏热，抬高持仓成本")
    if basis_pct >= Decimal("0.20"):
        warnings.append("永续高于标记价较多，追高风险升高")
    if position_side in {"long", "short"}:
        warnings.append(
            f"当前持有 {selected_symbol}{'多单' if position_side == 'long' else '空单'}"
            f" · 当前这单净结果估算 {format_decimal(net_close_pnl, 2)}U / 每单目标 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U"
        )
        if aggressive_scalp_mode and not holding_same_direction:
            warnings.append(f"当前方向已切到 {planned_side_label}，会先平掉这笔旧仓再继续直开")
    elif selected_from_market:
        warnings.append(f"watchlist 外出现更优循环目标，当前切到 {selected_symbol}")
    if len(execution_symbols) > 1:
        warnings.append(f"执行层当前按 {len(execution_symbols)} 币并行：{', '.join(execution_symbols)}")
    if target_multiple > Decimal("1") and not aggressive_scalp_mode:
        if int(ability_snapshot.get("closeOrders") or 0) <= 0:
            warnings.append(
                f"{format_decimal(target_multiple, 0)}x 是项目目标，不会直接把一个数字乘到仓位上；当前还没拿到足够的平仓样本，先按方向循环并累计积分"
            )
        else:
            warnings.append(
                f"{format_decimal(target_multiple, 0)}x 是项目目标，不会直接把一个数字乘到仓位上；当前按赢亏积分执行"
                f" · 近场净收益 {format_decimal(safe_decimal(ability_snapshot.get('netPnl'), '0'), 2)}U"
                f" · 积分 {format_decimal(safe_decimal(ability_snapshot.get('score'), '0'), 0)}"
            )
    ability_score = safe_decimal(ability_snapshot.get("score"), "0")
    ability_wins = int(ability_snapshot.get("winningCloseOrders") or 0)
    ability_losses = int(ability_snapshot.get("losingCloseOrders") or 0)
    if aggressive_scalp_mode:
        warnings.append(
            f"当前只记录赢亏积分，不再用能力阶段限制开仓 · 当前积分 {format_decimal(ability_score, 0)}"
            f" · 胜 {ability_wins} / 负 {ability_losses}"
        )
    else:
        warnings.append(
            f"当前积分机制：赢 +1 / 亏 -1 · 当前积分 {format_decimal(ability_score, 0)}"
            f" · 胜 {ability_wins} / 负 {ability_losses}"
        )
    if aggressive_scalp_mode:
        warnings.append("当前为超短直开模式：分析只负责方向，执行层直接开单，单笔净赚 1U 就平")
    warnings.append(
        f"当前费率采用 OKX 实际/回退费率：maker {compact_metric(maker_fee_pct, '0.001')}% / taker {compact_metric(taker_fee_pct, '0.001')}%"
    )
    if recent_avg_abs_slip_mark_pct > 0 or recent_taker_fill_pct > 0:
        warnings.append(
            f"近期真实执行拖累：加权滑点 {compact_metric(recent_avg_abs_slip_mark_pct, '0.001')}%"
            f" / index 偏离 {compact_metric(recent_avg_abs_slip_index_pct, '0.001')}%"
            f" / taker {compact_metric(recent_taker_fill_pct, '0.1')}%"
            f" / 成本地板 {compact_metric(execution_cost_floor_pct, '0.01')}%"
        )
    if recent_symbol_close_orders >= DIP_SWING_SYMBOL_PERF_MIN_CLOSE_ORDERS and recent_symbol_net_pnl <= -DIP_SWING_SYMBOL_NEGATIVE_NET_WARN_USDT:
        warnings.append(
            f"{selected_symbol} 近场净结果 {format_decimal(recent_symbol_net_pnl, 2)}U / 平仓 {recent_symbol_close_orders} 笔，先降权处理"
        )
    if symbol_performance_blocked and not aggressive_scalp_mode:
        warnings.append(f"{selected_symbol} 最近净亏偏大，已临时禁开新仓")
    if symbol_taker_blocked and not aggressive_scalp_mode:
        warnings.append(f"{selected_symbol} 近期 taker 占比偏高，已临时禁开新仓")
    if symbol_pressure_blocked and not aggressive_scalp_mode:
        warnings.append(f"{selected_symbol} 开平结构失衡，先等平仓闭环")
    if entry_vetoes and not aggressive_scalp_mode:
        warnings.append("结构裁判快照: " + " / ".join(entry_vetoes[:4]))
    warnings.append(
        f"当前方向 {planned_side_label} · 预期波动 {compact_metric(predicted_move_pct, '0.01')}% / 预期净优势 {compact_metric(predicted_net_pct, '0.01')}%"
    )
    if not liquidity_ready:
        warnings.append(
            f"近端成交额 {format_decimal(avg_quote_volume_usd, 0)}U / 持仓量 {format_decimal(open_interest_usd, 0)}U，流动性一般"
        )
    if market_positive_count == 0:
        warnings.append(f"扩展市场已扫 {len(market_snapshots)} 币，当前没有明显正净优势候选，但会继续按方向循环")
    elif top_candidates:
        warnings.append(
            "当前候选: " + " / ".join(
                f"{item['symbol']} {item['plannedSideLabel']} · 质量 {item['qualityScore']} · 预期净优势 {item['predictedNetPct']}% · ATR {item['atrPct']}%"
                for item in top_candidates
            )
        )
    if market_scan_errors:
        warnings.append(f"扩展扫描跳过 {len(market_scan_errors)} 币")
    if position_side == "flat":
        warnings.append(
            f"预计这单净结果 {format_decimal(projected_entry_net_pnl, 2)}U / 目标 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U"
        )

    ignored_blockers = list(blockers)
    if aggressive_scalp_mode and blockers:
        warnings.append("超短直开模式已忽略风险闸门：" + " / ".join(ignored_blockers[:3]))
        blockers = []

    if position_side == "flat" and projected_entry_net_pnl < DIP_SWING_NET_TARGET_USDT:
        blockers.append(
            f"预计这单净赚只有 {format_decimal(projected_entry_net_pnl, 2)}U，达不到每单 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U"
        )

    allow_new_entries = True if aggressive_scalp_mode else (not blockers)
    if aggressive_scalp_mode and position_side == "flat":
        allow_new_entries = projected_entry_net_pnl >= DIP_SWING_NET_TARGET_USDT
    if blockers:
        decision = "skip"
        decision_label = "这单预计净利不够"
    elif position_side in {"long", "short"}:
        decision = "manage"
        if net_close_pnl >= DIP_SWING_NET_TARGET_USDT:
            decision_label = "这单已达净利，准备平仓"
        elif aggressive_scalp_mode and not holding_same_direction:
            decision_label = "方向反转，先平旧仓"
        elif allow_new_entries and holding_same_direction:
            decision_label = "持仓中继续循环"
        else:
            decision_label = "持仓中观察平仓"
    elif allow_new_entries:
        decision = "execute"
        decision_label = "直接开单"
    else:
        decision = "observe"
        decision_label = ONLY_STRATEGY_FALLBACK_DECISION

    symbol = selected_symbol
    summary_bits = [
        f"{symbol} {ONLY_STRATEGY_LABEL}",
        f"方向 {planned_side_label}",
        f"预期波动 {compact_metric(predicted_move_pct, '0.01')}%",
        f"预期净优势 {compact_metric(predicted_net_pct, '0.01')}%",
        f"maker {compact_metric(maker_fee_pct, '0.001')}% / taker {compact_metric(taker_fee_pct, '0.001')}%",
        f"每单净利目标 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U",
    ]
    if target_multiple > Decimal("1") and not aggressive_scalp_mode:
        summary_bits.append(f"目标余额 {format_decimal(target_multiple, 0)}x")
        summary_bits.append(f"积分 {format_decimal(ability_score, 0)}")
        if int(ability_snapshot.get("closeOrders") or 0) > 0:
            summary_bits.append(f"近场净收益 {format_decimal(safe_decimal(ability_snapshot.get('netPnl'), '0'), 2)}U")
            summary_bits.append(f"胜 {ability_wins} / 负 {ability_losses}")
        else:
            summary_bits.append("先积累赢亏积分，再放大仓位")
    elif aggressive_scalp_mode:
        summary_bits.append(f"积分 {format_decimal(ability_score, 0)}")
    if selected_from_market:
        summary_bits.append("市场轮动目标")
    if len(execution_symbols) > 1:
        summary_bits.append(f"执行 {len(execution_symbols)} 币并行")
    if position_side in {"long", "short"} and liq_buffer > 0 and not aggressive_scalp_mode:
        summary_bits.append(f"强平缓冲 {compact_metric(liq_buffer, '0.1')}%")
    if blockers:
        summary_bits.append(blockers[0])
    elif position_side in {"long", "short"}:
        summary_bits.append(f"当前这单净结果估算 {format_decimal(net_close_pnl, 2)}U，达到 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U 就平")
        if aggressive_scalp_mode and not holding_same_direction:
            summary_bits.append("方向已反转，先平旧仓再恢复直开")
        if allow_new_entries and holding_same_direction:
            summary_bits.append("同方向持仓不中断，继续循环开仓")
    elif allow_new_entries:
        summary_bits.append(f"空仓直开，预计这单净赚 {format_decimal(projected_entry_net_pnl, 2)}U，达到 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U 就平")
    else:
        summary_bits.append(f"预计这单净赚 {format_decimal(projected_entry_net_pnl, 2)}U，不够每单 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U")

    return {
        "statusText": "已联网分析",
        "decision": decision,
        "decisionLabel": decision_label,
        "summary": " · ".join(summary_bits),
        "selectedStrategyName": f"{symbol} {ONLY_STRATEGY_LABEL}",
        "selectedStrategyDetail": (
            ("方向驱动 + 无脑直开" if aggressive_scalp_mode else "方向驱动 + 超短直开")
            + 
            f" · 空仓即开 {planned_side_label}"
            f" · 每单净赚 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U+ 就平"
            f" · 开仓直开 / 平仓 IOC · {selected_config.get('swapTdMode', 'isolated')} {selected_config.get('swapLeverage', '2')}x"
        ),
        "selectedReturnPct": "",
        "selectedDrawdownPct": "",
        "selectedWinRatePct": "",
        "selectedScore": "",
        "allowNewEntries": allow_new_entries,
        "optimizerRefreshed": False,
        "lastAnalyzedAt": now_local_iso(),
        "marketRegime": "24h 利润循环",
        "spotTrend": "",
        "swapTrend": f"{selected_signal.get('trend', 'flat')} / {planned_side_label}",
        "volatilityPct": compact_metric(volatility_pct, "0.01"),
        "spreadPct": "",
        "basisPct": compact_metric(basis_pct, "0.01"),
        "fundingRatePct": compact_metric(funding_rate_pct, "0.001"),
        "makerFeePct": compact_metric(maker_fee_pct, "0.001"),
        "takerFeePct": compact_metric(taker_fee_pct, "0.001"),
        "openInterest": str(open_interest),
        "warnings": warnings,
        "blockers": blockers,
        "selectedConfig": deep_merge({}, selected_config),
        "selectedWatchlistSymbol": selected_symbol,
        "executionWatchlistSymbols": execution_symbols,
        "executionTargetCount": len(execution_symbols),
        "selectedFromMarketScan": selected_from_market,
        "watchlistCount": len(watchlist_symbols),
        "candidateCount": candidate_count,
        "marketScanCount": len(market_snapshots),
        "marketCandidateCount": market_candidate_count,
        "marketPositiveCount": market_positive_count,
        "marketTopCandidates": top_candidates,
        "plannedSide": planned_side,
        "plannedSideLabel": planned_side_label,
        "predictedMovePct": compact_metric(predicted_move_pct, "0.01"),
        "predictedNetPct": compact_metric(predicted_net_pct, "0.01"),
        "projectedEntryNetPnl": compact_metric(projected_entry_net_pnl, "0.01"),
        "projectedEntryNotional": compact_metric(projected_entry_notional, "0.01"),
        "profitTargetUsdt": format_decimal(DIP_SWING_NET_TARGET_USDT, 0),
        "loopQualityScore": compact_metric(loop_quality_score, "0.1"),
        "pullbackPct": compact_metric(pullback_pct, "0.1"),
        "reboundPct": compact_metric(rebound_pct, "0.1"),
        "pullbackThresholdPct": compact_metric(pullback_threshold_pct, "0.1"),
        "reboundThresholdPct": compact_metric(rebound_threshold_pct, "0.1"),
        "reboundLookbackBars": rebound_lookback_bars,
        "emaSpreadPct": compact_metric(ema_spread_pct, "0.01"),
        "fastSlopePct": compact_metric(fast_slope_pct, "0.01"),
        "slowSlopePct": compact_metric(slow_slope_pct, "0.01"),
        "entryScore": entry_score,
        "exitScore": exit_score,
        "estimatedCostPct": compact_metric(estimated_cost_pct, "0.01"),
        "setupEdgePct": compact_metric(setup_edge_pct, "0.01"),
        "netEdgePct": compact_metric(net_edge_pct, "0.01"),
        "edgeCostRatio": compact_metric(edge_cost_ratio, "0.01"),
        "rangeCostRatio": compact_metric(range_cost_ratio, "0.01"),
        "atrPct": compact_metric(atr_pct, "0.01"),
        "atrCostRatio": compact_metric(atr_cost_ratio, "0.01"),
        "avgQuoteVolumeUsd": compact_metric(avg_quote_volume_usd, "1"),
        "openInterestUsd": compact_metric(open_interest_usd, "1"),
        "executionQualityScore": compact_metric(execution_quality_score, "0.1"),
        "recentAvgAbsSlipMarkPct": compact_metric(recent_avg_abs_slip_mark_pct, "0.001"),
        "recentAvgAbsSlipIndexPct": compact_metric(recent_avg_abs_slip_index_pct, "0.001"),
        "recentTakerFillPct": compact_metric(recent_taker_fill_pct, "0.1"),
        "executionCostFloorPct": compact_metric(execution_cost_floor_pct, "0.01"),
        "recentSymbolNetPnl": compact_metric(recent_symbol_net_pnl, "0.01"),
        "recentSymbolCloseOrders": recent_symbol_close_orders,
        "symbolPerformanceBlocked": symbol_performance_blocked,
        "symbolTakerBlocked": symbol_taker_blocked,
        "symbolPressureBlocked": symbol_pressure_blocked,
        "liquidationPrice": decimal_to_str(liq_price) if liq_price > 0 else "",
        "liquidationBufferPct": compact_metric(liq_buffer, "0.1") if liq_buffer > 0 else "",
        "targetBalanceProgressPct": compact_metric(target_balance.get("progressPct"), "0.1"),
        "targetCurrentMultiple": compact_metric(target_balance.get("currentMultiple"), "0.01"),
        "executionAbilityPhase": ("attack" if aggressive_scalp_mode else str(ability_snapshot.get("phase") or "protect")),
        "executionAbilityPhaseLabel": ("直开" if aggressive_scalp_mode else str(ability_snapshot.get("phaseLabel") or "守仓")),
        "executionAbilityScore": compact_metric(ability_snapshot.get("score"), "0.1"),
        "executionAbilityNetPnl": compact_metric(ability_snapshot.get("netPnl"), "0.01"),
        "executionAbilityCloseOrders": int(ability_snapshot.get("closeOrders") or 0),
        "executionAbilityWinRatePct": compact_metric(ability_snapshot.get("closeWinRatePct"), "0.1"),
        "estimatedNetClosePnl": compact_metric(net_close_pnl, "0.01"),
        "research": {
            "running": False,
            "statusText": f"{ONLY_STRATEGY_LABEL}模式",
            "mode": "profit_loop",
            "lastRunAt": now_local_iso(),
            "historyLimit": len(selected_snapshot.get("candles") or []),
            "sampleCount": len(selected_snapshot.get("candles") or []),
            "summary": {
                "plannedSide": planned_side_label,
                "predictedMovePct": compact_metric(predicted_move_pct, "0.01"),
                "predictedNetPct": compact_metric(predicted_net_pct, "0.01"),
                "fundingRatePct": compact_metric(funding_rate_pct, "0.001"),
            },
            "bestConfig": deep_merge({}, selected_config),
            "leaderboard": [],
            "generationSummaries": [],
            "pipeline": {
                "mode": "profit_loop",
                "status": decision,
                "candidateCount": candidate_count,
                "marketCandidateCount": market_candidate_count,
            },
            "markets": {},
            "notes": [],
            "equityCurve": [],
        },
    }


def build_execution_analysis(
    automation: dict[str, Any],
    client: OkxClient,
) -> dict[str, Any]:
    if str(automation.get("strategyPreset") or "") == "basis_arb":
        return build_basis_arb_analysis(automation, client)
    if str(automation.get("strategyPreset") or "") == "dip_swing":
        return build_dip_swing_analysis(automation, client)
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

    dns_hijacked = system_ips and public_ips and system_ips[0] != public_ips[0] and any(ip_is_bogon(ip) for ip in system_ips)

    if not tcp_connected:
        status = "connect_failed"
        detail = f"{host}:{port} 连接失败：{recv_error or '未知错误'}"
    elif tcp_connected and bytes_received == 0:
        status = "stratum_silent"
        if dns_hijacked:
            detail = (
                f"本机 DNS 把 {host} 解析到 {', '.join(system_ips)}，真实公网结果是 {', '.join(public_ips)}，"
                f"但直连 {connect_host}:{port} 后 subscribe 仍然没有任何回包。"
            )
        else:
            detail = f"{connect_host}:{port} 可连通，但订阅后没有收到任何 stratum 返回。"
    elif dns_hijacked:
        status = "dns_hijack"
        detail = f"本机 DNS 把 {host} 解析到 {', '.join(system_ips)}，真实公网结果是 {', '.join(public_ips)}。"
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
    elif status == "guard-blocked":
        if pool_diag.get("status") == "stratum_silent":
            headline = "未在挖 / 矿池无回包"
        elif pool_diag.get("status") == "connect_failed":
            headline = "未在挖 / 连接失败"
        elif pool_diag.get("status") == "dns_hijack":
            headline = "未在挖 / DNS 异常"
        else:
            headline = "未在挖 / 启动已拦截"
        detail = str(pool_diag.get("detail") or mac_lotto.get("guardReason") or "当前矿池条件不满足启动要求。")
    elif running and status == "waiting-for-job":
        if pool_diag.get("bypassActive"):
            headline = "在线 / DNS 已绕过"
            detail = f"本机 DNS 异常，但矿工已改走 {pool_diag.get('workerPoolHost') or pool_diag.get('connectHost') or '公网矿池'} 直连。"
        elif pool_diag.get("status") == "dns_hijack":
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
    effective_pool_diag = mac_lotto.get("guardDiagnosis") if isinstance(mac_lotto.get("guardDiagnosis"), dict) else pool_diag
    return {
        "config": config,
        "sources": miner_sources(),
        "options": remote_miner_options(),
        "serialPorts": serial_ports(),
        "network": network,
        "pool": pool,
        "progress": build_miner_progress(config, network, pool, mac_lotto, effective_pool_diag),
        "poolDiagnosis": effective_pool_diag,
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
    effective_pool_diag = mac_lotto.get("guardDiagnosis") if isinstance(mac_lotto.get("guardDiagnosis"), dict) else pool_diag
    if not progress:
        progress = build_miner_progress(config, network, pool, mac_lotto, effective_pool_diag)
    return {
        "config": config,
        "options": options,
        "serialPorts": serial_ports_cached,
        "network": network,
        "pool": pool,
        "progress": progress,
        "poolDiagnosis": effective_pool_diag,
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
        if not bool(config.get("autoStartMacLotto", True)) or not bool(str(config.get("wallet", "")).strip()):
            return False
        guard = self._guard_mode(config)
        return not bool(guard.get("active"))

    def _worker_limit(self) -> int:
        return max(1, min((os.cpu_count() or 1) * 2, 32))

    def _worker_connect_host(self, diagnosis: dict[str, Any]) -> tuple[str, bool]:
        status = str(diagnosis.get("status") or "")
        connect_host = str(diagnosis.get("connectHost") or "").strip()
        bypass_ready = (
            status == "dns_hijack"
            and bool(connect_host)
            and int(diagnosis.get("bytesReceived") or 0) > 0
            and connect_host != str(diagnosis.get("host") or "")
            and not ip_is_bogon(connect_host)
        )
        return (connect_host if bypass_ready else "", bypass_ready)

    def _guard_mode(self, config: dict[str, Any]) -> dict[str, Any]:
        pool_host = str(config.get("poolHost", "")).strip()
        pool_port = int(config.get("poolPort", 0) or 0)
        if not pool_host or pool_port <= 0:
            return {"active": False, "reason": "", "diagnosis": {}, "workerPoolHost": "", "bypassActive": False}
        diagnosis = diagnose_pool_endpoint(pool_host, pool_port)
        diagnosis["host"] = pool_host
        worker_pool_host, bypass_active = self._worker_connect_host(diagnosis)
        if bypass_active:
            diagnosis["bypassActive"] = True
            diagnosis["workerPoolHost"] = worker_pool_host
            reason = f"检测到本机 DNS 劫持，矿工将直连 {worker_pool_host}:{pool_port}。"
            active = False
        else:
            active = diagnosis.get("status") in {"dns_hijack", "stratum_silent", "connect_failed"}
            reason = str(diagnosis.get("detail") or "")
        return {
            "active": active,
            "reason": reason,
            "diagnosis": diagnosis,
            "workerPoolHost": worker_pool_host or pool_host,
            "bypassActive": bypass_active,
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
        guard = self._guard_mode(config)
        return (
            str(config.get("wallet", "")).strip(),
            str(config.get("poolHost", "")).strip(),
            str(guard.get("workerPoolHost") or config.get("poolHost", "")).strip(),
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
                if self.processes:
                    self.stop(config)
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
            guard = self._guard_mode(config)
            worker_pool_host = str(guard.get("workerPoolHost") or pool_host).strip() or pool_host
            if guard["active"] and not guard["bypassActive"]:
                self._kill_stray_workers()
                self.processes = {}
                self.last_signature = signature
                secure_dump_json(
                    MAC_LOTTO_STATUS_PATH,
                    {
                        "running": False,
                        "status": "guard-blocked",
                        "last_stop_at": now_local_iso(),
                        "effectiveWorkerCount": 0,
                        "pool_host": pool_host,
                        "pool_port": pool_port,
                        "guardMode": True,
                        "guardReason": str(guard.get("reason") or ""),
                        "guardDiagnosis": guard.get("diagnosis") or {},
                    },
                )
                return self.snapshot(config)

            for spec in specs:
                dump_worker_status(
                    spec["status_path"],
                    {
                        "running": False,
                        "status": "starting",
                        "startedAt": start_stamp,
                        "address": wallet,
                        "pool_host": worker_pool_host,
                        "pool_port": pool_port,
                        "workerId": spec["id"],
                        "configured_pool_host": pool_host,
                    },
                )
                env = dict(os.environ)
                env.update(
                    {
                        "SOLOMINER_ADDRESS": wallet,
                        "SOLOMINER_POOL_HOST": worker_pool_host,
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
                "guard-blocked",
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
            elif not running and guard["active"]:
                status = "guard-blocked"

            estimated_hashrate = float(benchmark_cpu_hashrate().get("hashrate") or 0) * effective
            payload = {
                "running": running,
                "pid": next((worker["pid"] for worker in workers if worker["pid"]), 0),
                "wallet": str(config.get("wallet", "")).strip(),
                "poolHost": str(config.get("poolHost", "")).strip(),
                "workerPoolHost": str(guard.get("workerPoolHost") or config.get("poolHost", "")).strip(),
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
        self._prepared_swap_signature: tuple[Any, ...] | None = None
        self._queued_swap_orders: list[dict[str, Any]] = []
        self._queued_swap_orders_lock = threading.RLock()

    def snapshot(self) -> dict[str, Any]:
        snapshot = self.state_store.current()
        ok, _, automation = validate_automation_config(self.automation_store.current())
        if not ok:
            return snapshot
        return reconcile_runtime_state_with_automation(snapshot, automation)

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

    def _set_watchlist(self, entries: list[dict[str, Any]]) -> None:
        def mutate(state: dict[str, Any]) -> None:
            state["watchlist"] = entries
            if entries:
                state["markets"]["spot"] = copy.deepcopy(entries[0].get("spot") or default_market_state())
                state["markets"]["swap"] = copy.deepcopy(entries[0].get("swap") or default_market_state())
            else:
                state["markets"]["spot"] = default_market_state()
                state["markets"]["swap"] = default_market_state()

        self._update_state(mutate)

    def _set_analysis(self, analysis: dict[str, Any], research: dict[str, Any]) -> None:
        def mutate(state: dict[str, Any]) -> None:
            state["analysis"] = deep_merge(state.get("analysis", {}), analysis)
            state["research"] = research

        self._update_state(mutate)

    def _queue_swap_order(self, item: dict[str, Any]) -> None:
        with self._queued_swap_orders_lock:
            self._queued_swap_orders.append(item)

    def _drain_queued_swap_orders(self) -> list[dict[str, Any]]:
        with self._queued_swap_orders_lock:
            queued = list(self._queued_swap_orders)
            self._queued_swap_orders.clear()
        return queued

    def _flush_queued_swap_orders(self, client: OkxClient) -> int:
        queued = self._drain_queued_swap_orders()
        if not queued:
            return 0
        submitted = 0
        for start in range(0, len(queued), OKX_BATCH_ORDER_LIMIT):
            chunk = queued[start:start + OKX_BATCH_ORDER_LIMIT]
            payloads = [dict(item.get("payload") or {}) for item in chunk if item.get("payload")]
            if not payloads:
                continue
            try:
                result = client.place_orders(payloads)
                rows = result.get("data") or []
                row_map = {
                    (
                        str((row or {}).get("instId") or ""),
                        str((row or {}).get("clOrdId") or ""),
                    ): (row or {})
                    for row in rows
                }
                ack_rows: list[dict[str, Any]] = []
                for item in chunk:
                    payload = dict(item.get("payload") or {})
                    key = (str(payload.get("instId") or ""), str(payload.get("clOrdId") or ""))
                    row = row_map.get(key, {})
                    order = deep_merge(payload, row)
                    strategy_tag = str(item.get("strategyTag") or "")
                    strategy_action = str(item.get("strategyAction") or "")
                    strategy_leg = str(item.get("strategyLeg") or "")
                    if strategy_tag:
                        order["tag"] = str(order.get("tag") or strategy_tag)
                        order["strategyTag"] = strategy_tag
                    if strategy_action:
                        order["strategyAction"] = strategy_action
                    if strategy_leg:
                        order["strategyLeg"] = strategy_leg
                    order["strategyReason"] = item.get("reason") or ""
                    ack_rows.append(order)
                    order_id = order.get("ordId", "")
                    self._increment_order_count(str(item.get("marketKey") or "swap"), order_id, str(item.get("reason") or ""))
                    self._log(
                        "info",
                        f"{item.get('reason') or '批量下单'} · 永续 {payload.get('instId') or ''} 已发单 · {item.get('executionMode') or '批量'}",
                    )
                if ack_rows:
                    PRIVATE_ORDER_STREAM._ingest_orders(ack_rows)
                    submitted += len(ack_rows)
            except Exception as exc:
                self._log("warn", f"批量永续下单失败，回退逐笔: {exc}")
                for item in chunk:
                    payload = dict(item.get("payload") or {})
                    if not payload:
                        continue
                    try:
                        result = client.place_order(payload)
                        order = deep_merge(payload, (result.get("data") or [{}])[0])
                        strategy_tag = str(item.get("strategyTag") or "")
                        strategy_action = str(item.get("strategyAction") or "")
                        strategy_leg = str(item.get("strategyLeg") or "")
                        if strategy_tag:
                            order["tag"] = str(order.get("tag") or strategy_tag)
                            order["strategyTag"] = strategy_tag
                        if strategy_action:
                            order["strategyAction"] = strategy_action
                        if strategy_leg:
                            order["strategyLeg"] = strategy_leg
                        order["strategyReason"] = item.get("reason") or ""
                        if order.get("ordId") or order.get("clOrdId"):
                            PRIVATE_ORDER_STREAM._ingest_orders([order])
                        order_id = order.get("ordId", "")
                        self._increment_order_count(str(item.get("marketKey") or "swap"), order_id, str(item.get("reason") or ""))
                        self._log(
                            "info",
                            f"{item.get('reason') or '逐笔回退'} · 永续 {payload.get('instId') or ''} 已发单 · {item.get('executionMode') or '逐笔回退'}",
                        )
                        submitted += 1
                    except Exception as single_exc:
                        self._log("warn", f"{item.get('reason') or '批量下单'} · 永续 {payload.get('instId') or ''} 发单失败: {single_exc}")
        return submitted

    def _touch_session(self, total_eq: Decimal, automation: dict[str, Any] | None = None) -> dict[str, Any]:
        today = datetime.now().date().isoformat()
        stamp = now_local_iso()
        target_multiple = resolve_target_balance_multiple(automation or {})

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
            target_snapshot = build_target_balance_snapshot(start_eq, total_eq, target_multiple)
            state["targetBalanceEq"] = decimal_to_str(target_snapshot["targetEq"])
            state["targetBalanceProgressPct"] = decimal_to_str(target_snapshot["progressPct"])
            state["targetBalanceRemainingEq"] = decimal_to_str(target_snapshot["remainingEq"])
            state["targetBalanceReached"] = bool(target_snapshot["reached"])
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

    def _target_balance_snapshot(self, automation: dict[str, Any]) -> dict[str, Any]:
        if not bool(self.config_store.current().get("simulated")):
            return build_target_balance_snapshot(Decimal("0"), Decimal("0"), Decimal("1"))
        state = self.state_store.current()
        start_eq = safe_decimal(state.get("sessionStartEq"), "0")
        current_eq = safe_decimal(state.get("currentEq"), "0")
        target_multiple = resolve_target_balance_multiple(automation)
        return build_target_balance_snapshot(start_eq, current_eq, target_multiple)

    def _target_execution_plan(
        self,
        automation: dict[str, Any],
        lot_size: Decimal,
        *,
        last_price: Decimal,
        contract_value: Decimal,
        leverage: Decimal,
        entry_score: int = 0,
    ) -> dict[str, Any]:
        aggressive_scalp_mode = DIP_SWING_AGGRESSIVE_SCALP_MODE
        minimum_contracts = round_down(lot_size, lot_size) if lot_size > 0 else Decimal("0")
        if minimum_contracts <= 0:
            return {
                "plannedContracts": Decimal("0"),
                "baseContracts": Decimal("0"),
                "marginBudget": Decimal("0"),
                "marginUsagePct": Decimal("0"),
                "phase": "idle",
                "phaseLabel": target_execution_phase_label("idle"),
                "progressPct": Decimal("0"),
                "scoreScale": Decimal("1"),
                "abilityScore": Decimal("0"),
                "abilityNetPnl": Decimal("0"),
                "abilityCloseOrders": 0,
                "abilityCloseWinRatePct": Decimal("0"),
                "abilityScalingAllowed": False,
            }
        target_snapshot = self._target_balance_snapshot(automation)
        target_multiple = resolve_target_balance_multiple(automation)
        progress_pct = safe_decimal(target_snapshot.get("progressPct"), "0")
        state = self.state_store.current()
        current_eq = safe_decimal(state.get("currentEq"), "0")
        ability_snapshot = build_target_execution_ability_snapshot(
            automation,
            state=state,
            target_snapshot=target_snapshot,
        )
        if aggressive_scalp_mode and current_eq > 0 and leverage > 0 and last_price > 0 and contract_value > 0:
            contract_margin = (last_price * contract_value) / leverage
            planned_contracts = round_down((current_eq * DIP_SWING_TARGET_MAX_MARGIN_RATIO) / contract_margin, lot_size) if contract_margin > 0 else Decimal("0")
            if planned_contracts < minimum_contracts:
                planned_contracts = Decimal("0")
            margin_budget = contract_margin * planned_contracts if planned_contracts > 0 else Decimal("0")
            margin_usage_pct = ((margin_budget / current_eq) * Decimal("100")) if current_eq > 0 and margin_budget > 0 else Decimal("0")
            return {
                "plannedContracts": planned_contracts,
                "baseContracts": Decimal("0"),
                "marginBudget": margin_budget,
                "marginUsagePct": margin_usage_pct,
                "phase": "attack",
                "phaseLabel": "直开",
                "progressPct": progress_pct,
                "scoreScale": Decimal("1"),
                "abilityScore": safe_decimal(ability_snapshot.get("score"), "0"),
                "abilityNetPnl": safe_decimal(ability_snapshot.get("netPnl"), "0"),
                "abilityCloseOrders": int(ability_snapshot.get("closeOrders") or 0),
                "abilityCloseWinRatePct": safe_decimal(ability_snapshot.get("closeWinRatePct"), "0"),
                "abilityScalingAllowed": True,
            }
        if (
            target_multiple <= Decimal("1")
            or not bool(self.config_store.current().get("simulated"))
            or current_eq <= 0
            or leverage <= 0
            or last_price <= 0
            or contract_value <= 0
        ):
            contract_margin = (last_price * contract_value) / leverage if leverage > 0 and last_price > 0 and contract_value > 0 else Decimal("0")
            planned_contracts = Decimal("0")
            margin_budget = Decimal("0")
            margin_usage_pct = Decimal("0")
            if current_eq > 0 and contract_margin > 0:
                fixed_margin_ratio = DIP_SWING_TARGET_MIN_MARGIN_RATIO
                planned_contracts = round_down((current_eq * fixed_margin_ratio) / contract_margin, lot_size)
                if planned_contracts < minimum_contracts:
                    planned_contracts = Decimal("0")
                margin_budget = contract_margin * planned_contracts if planned_contracts > 0 else Decimal("0")
                margin_usage_pct = ((margin_budget / current_eq) * Decimal("100")) if current_eq > 0 and margin_budget > 0 else Decimal("0")
            return {
                "plannedContracts": planned_contracts,
                "baseContracts": Decimal("0"),
                "marginBudget": margin_budget,
                "marginUsagePct": margin_usage_pct,
                "phase": "fixed",
                "phaseLabel": target_execution_phase_label("fixed"),
                "progressPct": progress_pct,
                "scoreScale": Decimal("1"),
                "abilityScore": safe_decimal(ability_snapshot.get("score"), "0"),
                "abilityNetPnl": safe_decimal(ability_snapshot.get("netPnl"), "0"),
                "abilityCloseOrders": int(ability_snapshot.get("closeOrders") or 0),
                "abilityCloseWinRatePct": safe_decimal(ability_snapshot.get("closeWinRatePct"), "0"),
                "abilityScalingAllowed": False,
            }

        phase = str(ability_snapshot.get("phase") or "fixed")
        ability_score = safe_decimal(ability_snapshot.get("score"), "0")
        positive_score = max(Decimal("0"), ability_score)
        negative_score = abs(min(Decimal("0"), ability_score))
        score_ratio = min(Decimal("1"), positive_score / Decimal("10")) if positive_score > 0 else Decimal("0")
        margin_ratio = DIP_SWING_TARGET_MIN_MARGIN_RATIO + (
            (DIP_SWING_TARGET_MAX_MARGIN_RATIO - DIP_SWING_TARGET_MIN_MARGIN_RATIO) * score_ratio
        )
        if progress_pct >= Decimal("50"):
            margin_ratio *= Decimal("0.80")
        if progress_pct >= Decimal("80"):
            margin_ratio *= Decimal("0.60")
        setback_pct = safe_decimal(ability_snapshot.get("setbackPct"), "0")
        if setback_pct >= Decimal("1.0"):
            margin_ratio *= Decimal("0.65")
        elif setback_pct >= Decimal("0.5"):
            margin_ratio *= Decimal("0.80")
        if negative_score > 0:
            margin_ratio *= max(Decimal("0.55"), Decimal("1") - (negative_score * Decimal("0.08")))

        entry_score_scale = Decimal("1")
        if entry_score > 0:
            entry_score_scale += Decimal(entry_score) * Decimal("0.02")
        margin_ratio = max(
            DIP_SWING_TARGET_MIN_MARGIN_RATIO,
            min(DIP_SWING_TARGET_MAX_MARGIN_RATIO, margin_ratio * entry_score_scale),
        )
        contract_margin = (last_price * contract_value) / leverage
        if contract_margin <= 0:
            budget_contracts = Decimal("0")
        else:
            budget_contracts = round_down((current_eq * margin_ratio) / contract_margin, lot_size)
        planned_contracts = budget_contracts
        if planned_contracts < minimum_contracts:
            planned_contracts = Decimal("0")
        margin_budget = contract_margin * planned_contracts if contract_margin > 0 else Decimal("0")
        margin_usage_pct = ((margin_budget / current_eq) * Decimal("100")) if current_eq > 0 else Decimal("0")
        return {
            "plannedContracts": planned_contracts,
            "baseContracts": Decimal("0"),
            "marginBudget": margin_budget,
            "marginUsagePct": margin_usage_pct,
            "phase": phase,
            "phaseLabel": target_execution_phase_label(phase),
            "progressPct": progress_pct,
            "scoreScale": entry_score_scale,
            "abilityScore": ability_score,
            "abilityNetPnl": safe_decimal(ability_snapshot.get("netPnl"), "0"),
            "abilityCloseOrders": int(ability_snapshot.get("closeOrders") or 0),
            "abilityCloseWinRatePct": safe_decimal(ability_snapshot.get("closeWinRatePct"), "0"),
            "abilityScalingAllowed": bool(ability_snapshot.get("scalingAllowed")),
        }

    def _target_scaled_swap_contracts(
        self,
        automation: dict[str, Any],
        lot_size: Decimal,
        *,
        last_price: Decimal,
        contract_value: Decimal,
        leverage: Decimal,
        entry_score: int = 0,
    ) -> Decimal:
        plan = self._target_execution_plan(
            automation,
            lot_size,
            last_price=last_price,
            contract_value=contract_value,
            leverage=leverage,
            entry_score=entry_score,
        )
        return safe_decimal(plan.get("plannedContracts"), "0")

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

    @staticmethod
    def _guard_live_manual_order(api_config: dict[str, Any], automation: dict[str, Any]) -> None:
        if api_config.get("simulated"):
            return
        if not automation.get("allowLiveManualOrders"):
            raise OkxApiError("当前是实盘，未开启“允许手动实盘下单”")

    def _prepare_swap(self, client: OkxClient, automation: dict[str, Any]) -> None:
        if not automation.get("swapEnabled"):
            return
        targets = build_execution_targets(automation)
        if automation.get("enforceNetMode"):
            account_config = client.get_account_config()
            account_row = (account_config.get("data") or [{}])[0]
            pos_mode = account_row.get("posMode", "")
            if pos_mode != "net_mode":
                client.set_position_mode("net_mode")
                self._log("info", "已尝试切换持仓模式到 net_mode")
        for target in targets:
            if not target.get("swapEnabled"):
                continue
            client.set_leverage(
                target["swapInstId"],
                str(target["swapLeverage"]),
                target["swapTdMode"],
            )
            self._set_market(f"swap:{target['swapInstId']}", {"prepared": True, "instId": target["swapInstId"]})

    def _swap_prepare_signature(self, automation: dict[str, Any]) -> tuple[Any, ...]:
        targets = build_execution_targets(automation)
        enabled_targets = [
            (
                str(target.get("swapInstId") or ""),
                str(target.get("swapLeverage") or ""),
                str(target.get("swapTdMode") or ""),
            )
            for target in targets
            if target.get("swapEnabled")
        ]
        return (bool(automation.get("enforceNetMode")), tuple(enabled_targets))

    def _ensure_swap_prepared(self, client: OkxClient, automation: dict[str, Any]) -> None:
        signature = self._swap_prepare_signature(automation)
        if signature == self._prepared_swap_signature:
            return
        self._prepare_swap(client, automation)
        self._prepared_swap_signature = signature

    @staticmethod
    def _order_age_seconds(order: dict[str, Any]) -> float:
        stamp = str(order.get("uTime") or order.get("cTime") or "").strip()
        if not stamp:
            return 0.0
        try:
            order_ts = float(stamp) / 1000.0
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, time.time() - order_ts)

    def _working_swap_orders(
        self,
        inst_id: str,
        *,
        side: str | None = None,
        reduce_only: bool | None = None,
        limit: int = 120,
    ) -> list[dict[str, Any]]:
        orders = PRIVATE_ORDER_STREAM.get_recent_orders("SWAP", limit=limit)
        working: list[dict[str, Any]] = []
        for order in orders:
            if str(order.get("instId") or "") != inst_id:
                continue
            if classify_execution_order_state(order) != "working":
                continue
            if is_non_blocking_execution_order(order):
                continue
            if side and str(order.get("side") or "").lower() != side.lower():
                continue
            if reduce_only is not None and flag_true(order.get("reduceOnly")) != reduce_only:
                continue
            working.append(order)
        return working

    def _cancel_swap_orders(
        self,
        client: OkxClient,
        inst_id: str,
        orders: list[dict[str, Any]],
        reason: str,
        *,
        market_key: str = "swap",
    ) -> None:
        canceled = 0
        cancel_payloads: list[dict[str, Any]] = []
        order_lookup: dict[tuple[str, str], dict[str, Any]] = {}
        for order in orders:
            ord_id = str(order.get("ordId") or "").strip()
            cl_ord_id = str(order.get("clOrdId") or "").strip()
            if not ord_id and not cl_ord_id:
                continue
            payload = {"instId": inst_id}
            if ord_id:
                payload["ordId"] = ord_id
            if cl_ord_id:
                payload["clOrdId"] = cl_ord_id
            cancel_payloads.append(payload)
            order_lookup[(ord_id, cl_ord_id)] = order

        for start in range(0, len(cancel_payloads), OKX_BATCH_ORDER_LIMIT):
            chunk = cancel_payloads[start:start + OKX_BATCH_ORDER_LIMIT]
            if not chunk:
                continue
            try:
                result = client.cancel_orders(chunk)
                rows = result.get("data") or []
                if not rows:
                    rows = chunk
                ack_rows: list[dict[str, Any]] = []
                for row in rows:
                    ord_id = str(row.get("ordId") or "").strip()
                    cl_ord_id = str(row.get("clOrdId") or "").strip()
                    original = order_lookup.get((ord_id, cl_ord_id), {})
                    ack = deep_merge(original, row)
                    ack["state"] = "canceled"
                    ack_rows.append(ack)
                if ack_rows:
                    PRIVATE_ORDER_STREAM._ingest_orders(ack_rows)
                    canceled += len(ack_rows)
            except Exception as exc:
                self._log("warn", f"{reason} · 永续 {inst_id} 批量撤单失败，回退逐笔: {exc}")
                for payload in chunk:
                    ord_id = str(payload.get("ordId") or "").strip()
                    cl_ord_id = str(payload.get("clOrdId") or "").strip()
                    try:
                        result = client.cancel_order(inst_id, ord_id=ord_id or None, cl_ord_id=cl_ord_id or None)
                        original = order_lookup.get((ord_id, cl_ord_id), {})
                        ack = deep_merge(original, (result.get("data") or [{}])[0])
                        ack["state"] = "canceled"
                        PRIVATE_ORDER_STREAM._ingest_orders([ack])
                        canceled += 1
                    except Exception as single_exc:
                        self._log("warn", f"{reason} · 永续 {inst_id} 撤单失败: {single_exc}")
        if canceled > 0:
            self._set_market(market_key, {"lastMessage": f"{reason} · 已撤 {canceled} 笔挂单"})
            self._log("info", f"{reason} · 永续 {inst_id} 已撤 {canceled} 笔挂单")

    def _spot_reference_price(self, client: OkxClient, inst_id: str, side: str) -> Decimal:
        row = extract_first_row(client.get_ticker(inst_id))
        bid_px = safe_decimal(row.get("bidPx") or row.get("bidPrice"), "0")
        ask_px = safe_decimal(row.get("askPx") or row.get("askPrice"), "0")
        last_px = safe_decimal(row.get("last"), "0")
        if side == "sell":
            return bid_px if bid_px > 0 else (last_px if last_px > 0 else ask_px)
        return ask_px if ask_px > 0 else (last_px if last_px > 0 else bid_px)

    def _build_passive_swap_entry_order(
        self,
        client: OkxClient,
        inst_id: str,
        side: str,
        size: Decimal,
        td_mode: str,
    ) -> tuple[dict[str, Any], str]:
        meta = get_instrument_meta(client, "SWAP", inst_id)
        tick_size = max(safe_decimal(meta.get("tickSz"), "0.0001"), Decimal("0.0001"))
        lot_size = max(safe_decimal(meta.get("lotSz") or meta.get("minSz"), "0.0001"), Decimal("0.0001"))
        min_size = max(safe_decimal(meta.get("minSz") or meta.get("lotSz"), "0.0001"), Decimal("0.0001"))
        rounded_size = round_down(size, lot_size)
        if rounded_size <= 0 or rounded_size < min_size:
            raise OkxApiError(f"永续下单数量过小，无法在 {inst_id} 发起被动挂单")

        row = extract_first_row(client.get_ticker(inst_id))
        bid_px = safe_decimal(row.get("bidPx") or row.get("bidPrice"), "0")
        ask_px = safe_decimal(row.get("askPx") or row.get("askPrice"), "0")
        last_px = safe_decimal(row.get("last"), "0")
        buffer_ratio = DIP_SWING_POST_ONLY_BUFFER_PCT / Decimal("100")
        extra_ticks = max(Decimal("0"), DIP_SWING_POST_ONLY_ENTRY_EXTRA_TICKS)

        if side == "buy":
            anchor_px = (
                max(bid_px - (tick_size * extra_ticks), tick_size)
                if bid_px > 0
                else (last_px * (Decimal("1") - buffer_ratio) if last_px > 0 else ask_px)
            )
            passive_px = round_down(anchor_px, tick_size)
            if ask_px > 0 and passive_px >= ask_px:
                passive_px = round_down(max(ask_px - tick_size, tick_size), tick_size)
        else:
            anchor_px = (
                ask_px + (tick_size * extra_ticks)
                if ask_px > 0
                else (last_px * (Decimal("1") + buffer_ratio) if last_px > 0 else bid_px)
            )
            passive_px = round_up(anchor_px, tick_size)
            if bid_px > 0 and passive_px <= bid_px:
                passive_px = round_up(bid_px + tick_size, tick_size)

        if passive_px <= 0:
            raise OkxApiError(f"未拿到 {inst_id} 的有效盘口，无法构造被动挂单")

        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "post_only",
            "px": decimal_to_str(passive_px),
            "sz": decimal_to_str(rounded_size),
            "clOrdId": build_cl_ord_id("w"),
        }
        label = f"post-only 挂单 · {decimal_to_str(passive_px)}"
        return payload, label

    def _build_aggressive_swap_entry_order(
        self,
        client: OkxClient,
        inst_id: str,
        side: str,
        size: Decimal,
        td_mode: str,
    ) -> tuple[dict[str, Any], str]:
        meta = get_instrument_meta(client, "SWAP", inst_id)
        lot_size = max(safe_decimal(meta.get("lotSz") or meta.get("minSz"), "0.0001"), Decimal("0.0001"))
        min_size = max(safe_decimal(meta.get("minSz") or meta.get("lotSz"), "0.0001"), Decimal("0.0001"))
        rounded_size = round_down(size, lot_size)
        if rounded_size <= 0 or rounded_size < min_size:
            raise OkxApiError(f"永续下单数量过小，无法在 {inst_id} 发起最优限价 IOC")

        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "optimal_limit_ioc",
            "sz": decimal_to_str(rounded_size),
            "clOrdId": build_cl_ord_id("w"),
        }
        return payload, "最优限价 IOC"

    def _build_passive_swap_exit_order(
        self,
        client: OkxClient,
        inst_id: str,
        side: str,
        size: Decimal,
        td_mode: str,
    ) -> tuple[dict[str, Any], str]:
        meta = get_instrument_meta(client, "SWAP", inst_id)
        tick_size = max(safe_decimal(meta.get("tickSz"), "0.0001"), Decimal("0.0001"))
        lot_size = max(safe_decimal(meta.get("lotSz") or meta.get("minSz"), "0.0001"), Decimal("0.0001"))
        min_size = max(safe_decimal(meta.get("minSz") or meta.get("lotSz"), "0.0001"), Decimal("0.0001"))
        rounded_size = round_down(size, lot_size)
        if rounded_size <= 0 or rounded_size < min_size:
            raise OkxApiError(f"永续平仓数量过小，无法在 {inst_id} 发起被动挂单")

        row = extract_first_row(client.get_ticker(inst_id))
        bid_px = safe_decimal(row.get("bidPx") or row.get("bidPrice"), "0")
        ask_px = safe_decimal(row.get("askPx") or row.get("askPrice"), "0")
        last_px = safe_decimal(row.get("last"), "0")
        buffer_ratio = DIP_SWING_EXIT_POST_ONLY_BUFFER_PCT / Decimal("100")
        extra_ticks = max(Decimal("0"), DIP_SWING_POST_ONLY_EXIT_EXTRA_TICKS)

        if side == "sell":
            anchor_px = (
                ask_px + (tick_size * extra_ticks)
                if ask_px > 0
                else (last_px * (Decimal("1") + buffer_ratio) if last_px > 0 else bid_px)
            )
            passive_px = round_up(anchor_px, tick_size)
            if bid_px > 0 and passive_px <= bid_px:
                passive_px = round_up(bid_px + tick_size, tick_size)
        else:
            anchor_px = (
                max(bid_px - (tick_size * extra_ticks), tick_size)
                if bid_px > 0
                else (last_px * (Decimal("1") - buffer_ratio) if last_px > 0 else ask_px)
            )
            passive_px = round_down(anchor_px, tick_size)
            if ask_px > 0 and passive_px >= ask_px:
                passive_px = round_down(max(ask_px - tick_size, tick_size), tick_size)

        if passive_px <= 0:
            raise OkxApiError(f"未拿到 {inst_id} 的有效盘口，无法构造被动平仓挂单")

        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "post_only",
            "px": decimal_to_str(passive_px),
            "sz": decimal_to_str(rounded_size),
            "reduceOnly": True,
            "clOrdId": build_cl_ord_id("w"),
        }
        label = f"post-only 平仓 · {decimal_to_str(passive_px)}"
        return payload, label

    def _build_aggressive_swap_exit_order(
        self,
        client: OkxClient,
        inst_id: str,
        side: str,
        size: Decimal,
        td_mode: str,
    ) -> tuple[dict[str, Any], str]:
        meta = get_instrument_meta(client, "SWAP", inst_id)
        lot_size = max(safe_decimal(meta.get("lotSz") or meta.get("minSz"), "0.0001"), Decimal("0.0001"))
        min_size = max(safe_decimal(meta.get("minSz") or meta.get("lotSz"), "0.0001"), Decimal("0.0001"))
        rounded_size = round_down(size, lot_size)
        if rounded_size <= 0 or rounded_size < min_size:
            raise OkxApiError(f"永续平仓数量过小，无法在 {inst_id} 发起最优限价 IOC")

        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "optimal_limit_ioc",
            "sz": decimal_to_str(rounded_size),
            "reduceOnly": True,
            "clOrdId": build_cl_ord_id("w"),
        }
        return payload, "最优限价 IOC 平仓"

    def _build_protected_swap_exit_order(
        self,
        client: OkxClient,
        inst_id: str,
        side: str,
        size: Decimal,
        td_mode: str,
    ) -> tuple[dict[str, Any], str]:
        meta = get_instrument_meta(client, "SWAP", inst_id)
        tick_size = max(safe_decimal(meta.get("tickSz"), "0.0001"), Decimal("0.0001"))
        lot_size = max(safe_decimal(meta.get("lotSz") or meta.get("minSz"), "0.0001"), Decimal("0.0001"))
        min_size = max(safe_decimal(meta.get("minSz") or meta.get("lotSz"), "0.0001"), Decimal("0.0001"))
        rounded_size = round_down(size, lot_size)
        if rounded_size <= 0 or rounded_size < min_size:
            raise OkxApiError(f"永续平仓数量过小，无法在 {inst_id} 发起保护性退出单")

        row = extract_first_row(client.get_ticker(inst_id))
        bid_px = safe_decimal(row.get("bidPx") or row.get("bidPrice"), "0")
        ask_px = safe_decimal(row.get("askPx") or row.get("askPrice"), "0")
        last_px = safe_decimal(row.get("last"), "0")
        protect_ratio = DIP_SWING_EXIT_PROTECTION_PCT / Decimal("100")

        if side == "sell":
            reference_px = bid_px if bid_px > 0 else last_px
            protected_px = round_down(reference_px * (Decimal("1") - protect_ratio), tick_size)
        else:
            reference_px = ask_px if ask_px > 0 else last_px
            protected_px = round_up(reference_px * (Decimal("1") + protect_ratio), tick_size)
        if protected_px <= 0:
            raise OkxApiError(f"未拿到 {inst_id} 的有效保护价，无法发起保护性平仓")

        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": td_mode,
            "side": side,
            "ordType": "ioc",
            "px": decimal_to_str(protected_px),
            "sz": decimal_to_str(rounded_size),
            "reduceOnly": True,
            "clOrdId": build_cl_ord_id("w"),
        }
        label = f"保护价 IOC · {decimal_to_str(protected_px)}"
        return payload, label

    def _build_protected_spot_sell_order(
        self,
        client: OkxClient,
        inst_id: str,
        size: Decimal,
    ) -> tuple[dict[str, Any], str]:
        meta = get_instrument_meta(client, "SPOT", inst_id)
        tick_size = max(safe_decimal(meta.get("tickSz"), "0.00000001"), Decimal("0.00000001"))
        lot_size = max(safe_decimal(meta.get("lotSz") or meta.get("minSz"), "0.00000001"), Decimal("0.00000001"))
        min_size = max(safe_decimal(meta.get("minSz") or meta.get("lotSz"), "0.00000001"), Decimal("0.00000001"))
        rounded_size = round_down(size, lot_size)
        if rounded_size <= 0 or rounded_size < min_size:
            raise OkxApiError(f"现货卖出数量过小，无法在 {inst_id} 发单")

        reference_px = self._spot_reference_price(client, inst_id, "sell")
        if reference_px <= 0:
            raise OkxApiError(f"未拿到 {inst_id} 的可用卖一/现价，无法构造保护价卖单")

        protected_px = round_down(
            reference_px * (Decimal("1") - (SPOT_SELL_PROTECTION_PCT / Decimal("100"))),
            tick_size,
        )
        if protected_px <= 0:
            protected_px = round_down(reference_px, tick_size)
        if protected_px <= 0:
            raise OkxApiError(f"未拿到 {inst_id} 的有效保护价，无法发单")

        payload: dict[str, Any] = {
            "instId": inst_id,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "ioc",
            "px": decimal_to_str(protected_px),
            "sz": decimal_to_str(rounded_size),
            "clOrdId": build_cl_ord_id("s"),
        }
        label = f"保护价 IOC · {decimal_to_str(protected_px)}"
        return payload, label

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
            self._ensure_swap_prepared(client, automation)
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
        self._prepared_swap_signature = None
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
        self._ensure_swap_prepared(client, automation)
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
                self._guard_live_mode(automation, api_config, autostart=False)
            except Exception as exc:
                self._log("error", str(exc))
                self.stop(f"自动量化已停止：{exc}")
                return
            try:
                ensure_live_route_ready(api_config, force=False)
                client = OkxClient(api_config)
                self._ensure_swap_prepared(client, automation)
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
                    if should_keep_running_in_test_mode(automation):
                        self._update_state(
                            lambda current: current.update(
                                {
                                    "statusText": "测试阶段连续错误，但保持运行",
                                    "lastError": message,
                                    "modeText": f"{ONLY_STRATEGY_LABEL} · 测试阶段不中断",
                                }
                            )
                        )
                    else:
                        self.stop("自动量化已停止：连续错误过多")
                        return
            wait_seconds = max(1, int(automation.get("pollSeconds", 20)))
            if self.stop_event.wait(wait_seconds):
                return

    def _run_signal_stage(self, client: OkxClient, automation: dict[str, Any]) -> dict[str, Any]:
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
        return {
            "analysisBundle": analysis_bundle,
            "analysis": analysis,
            "research": research,
            "effectiveAutomation": effective_automation,
            "allowNewEntries": allow_new_entries,
        }

    def _run_portfolio_stage(self, client: OkxClient, effective_automation: dict[str, Any]) -> dict[str, Any]:
        account_snapshot = fetch_account_snapshot(client, include_positions=False)
        balance_snapshot = {
            "summary": account_snapshot.get("summary", {}),
            "details": account_snapshot.get("tradingBalances") or account_snapshot.get("balances") or [],
        }
        total_eq = safe_decimal(
            account_snapshot["summary"].get("displayTotalEq")
            or account_snapshot["summary"].get("totalEq"),
            "0",
        )
        session_state = self._touch_session(total_eq, effective_automation)
        targets = build_execution_targets(effective_automation)
        return {
            "accountSnapshot": account_snapshot,
            "balanceSnapshot": balance_snapshot,
            "totalEq": total_eq,
            "sessionState": session_state,
            "targets": targets,
        }

    def _run_risk_stage(self, effective_automation: dict[str, Any], session_state: dict[str, Any]) -> str:
        active_markets = 0
        watched_symbols = 0
        snapshot = self.snapshot()
        watchlist = snapshot.get("watchlist") or []
        watched_symbols = len(watchlist)
        for entry in watchlist:
            summary = entry.get("summary") or {}
            if summary.get("activeLegs"):
                active_markets += len(summary.get("activeLegs") or [])

        max_daily_loss = safe_decimal(effective_automation.get("maxDailyLossPct"), "0")
        drawdown_pct = safe_decimal(session_state.get("dailyDrawdownPct"), "0")
        max_orders_per_day = int(effective_automation.get("maxOrdersPerDay", 20))
        order_count_today = int(session_state.get("orderCountToday", 0))
        target_multiple = resolve_target_balance_multiple(effective_automation)
        target_eq = safe_decimal(session_state.get("targetBalanceEq"), "0")
        target_reached = bool(session_state.get("targetBalanceReached"))
        stop_reason = ""
        checks: list[dict[str, Any]] = []

        test_mode_keep_running = should_keep_running_in_test_mode(effective_automation)

        if target_multiple > Decimal("1") and target_eq > 0:
            checks.append(
                {
                    "name": "target_balance",
                    "passed": not target_reached,
                    "detail": (
                        f"目标 {format_decimal(target_multiple, 0)}x · 当前 {format_decimal(safe_decimal(session_state.get('currentEq'), '0'), 2)} "
                        f"/ 目标 {format_decimal(target_eq, 2)} USDT"
                    ),
                }
            )
            if target_reached and not stop_reason and not test_mode_keep_running:
                stop_reason = f"模拟盘目标已完成：余额达到 {format_decimal(target_multiple, 0)}x"

        if max_daily_loss > 0:
            passed = drawdown_pct > -max_daily_loss
            checks.append(
                {
                    "name": "max_daily_loss",
                    "passed": passed,
                    "detail": (
                        f"日内回撤 {format_decimal(drawdown_pct, 3)}% / 上限 {format_decimal(max_daily_loss, 3)}%"
                    ),
                }
            )
            if not passed and not stop_reason and not test_mode_keep_running:
                stop_reason = "自动量化已停止：超过日内最大回撤"

        passed_order_limit = max_orders_per_day <= 0 or order_count_today < max_orders_per_day
        checks.append(
            {
                "name": "max_orders_per_day",
                "passed": passed_order_limit,
                "detail": f"当日下单 {order_count_today} / 上限 {'不限制' if max_orders_per_day <= 0 else max_orders_per_day}",
            }
        )
        if not passed_order_limit and not stop_reason and not test_mode_keep_running:
            stop_reason = "自动量化已停止：达到今日最大下单次数"

        report = {
            "status": "blocked" if stop_reason else "ok",
            "stopReason": stop_reason,
            "drawdownPct": decimal_to_str(drawdown_pct),
            "maxDailyLossPct": decimal_to_str(max_daily_loss),
            "orderCountToday": order_count_today,
            "maxOrdersPerDay": max_orders_per_day,
            "activeMarkets": active_markets,
            "watchedSymbols": watched_symbols,
            "checks": checks,
            "updatedAt": now_local_iso(),
        }
        self._update_state(lambda state: state.update({"lastRiskReport": report}))
        return stop_reason

    def _run_execution_stage(
        self,
        client: OkxClient,
        effective_automation: dict[str, Any],
        balance_snapshot: dict[str, Any],
        allow_new_entries: bool,
        analysis_label: str,
        targets: list[dict[str, Any]],
    ) -> dict[str, Any]:
        watchlist_entries: list[dict[str, Any]] = []
        enabled_spot = sum(1 for target in targets if target.get("spotEnabled"))
        enabled_swap = sum(1 for target in targets if target.get("swapEnabled"))
        active_symbols = 0
        arb_runtime = {
            "watching": 0,
            "windowOpen": 0,
            "hedged": 0,
            "exitQueue": 0,
            "rollback": 0,
            "brokenPair": 0,
            "blocked": 0,
        }

        def run_target_cycle(target: dict[str, Any]) -> tuple[dict[str, Any], str, str]:
            spot_key = f"spot:{target['spotInstId']}"
            swap_key = f"swap:{target['swapInstId']}"
            try:
                if str(target.get("strategyPreset") or "") == "basis_arb":
                    self._run_basis_arb_cycle(
                        client,
                        target,
                        balance_snapshot,
                        analysis_label,
                        allow_new_entries,
                        spot_key=spot_key,
                        swap_key=swap_key,
                    )
                else:
                    if target.get("spotEnabled"):
                        self._run_spot_cycle(
                            client,
                            target,
                            balance_snapshot,
                            allow_new_entries,
                            analysis_label,
                            market_key=spot_key,
                        )
                    else:
                        self._set_market(
                            spot_key,
                            {
                                "enabled": False,
                                "instId": target.get("spotInstId", ""),
                                "lastMessage": "现货策略未启用",
                            },
                        )

                    if target.get("swapEnabled"):
                        self._run_swap_cycle(
                            client,
                            target,
                            balance_snapshot,
                            allow_new_entries,
                            analysis_label,
                            market_key=swap_key,
                        )
                    else:
                        self._set_market(
                            swap_key,
                            {
                                "enabled": False,
                                "instId": target.get("swapInstId", ""),
                                "lastMessage": "永续策略未启用",
                            },
                        )
            except Exception as exc:
                self._log("error", f"{target.get('watchlistSymbol') or target.get('swapInstId')}: 执行失败: {exc}")
                self._set_market(
                    swap_key,
                    {
                        "enabled": bool(target.get("swapEnabled")),
                        "instId": target.get("swapInstId", ""),
                        "lastMessage": f"执行失败: {exc}",
                    },
                )
                self._set_market(
                    spot_key,
                    {
                        "enabled": bool(target.get("spotEnabled")),
                        "instId": target.get("spotInstId", ""),
                        "lastMessage": f"执行失败: {exc}",
                    },
                )
            return target, spot_key, swap_key

        if len(targets) <= 1:
            executed_targets = [run_target_cycle(target) for target in targets]
        else:
            max_workers = max(1, min(len(targets), DIP_SWING_EXECUTION_TARGET_LIMIT))
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_map = {executor.submit(run_target_cycle, target): target for target in targets}
                executed_targets = [future.result() for future in concurrent.futures.as_completed(future_map)]

        queued_submitted = self._flush_queued_swap_orders(client)

        current_state = self.snapshot()
        for target, spot_key, swap_key in executed_targets:
            spot_market = copy.deepcopy(current_state.get("markets", {}).get(spot_key) or default_market_state())
            swap_market = copy.deepcopy(current_state.get("markets", {}).get(swap_key) or default_market_state())
            entry = build_watchlist_entry(
                str(target.get("watchlistSymbol") or ""),
                target,
                spot_market,
                swap_market,
            )
            if entry.get("summary", {}).get("activeLegs"):
                active_symbols += 1
            watchlist_entries.append(entry)

        if str(effective_automation.get("strategyPreset") or "") == "basis_arb":
            arb_runtime = summarize_basis_arb_watchlist(watchlist_entries)
        self._set_watchlist(watchlist_entries)
        return {
            "watchlistEntries": watchlist_entries,
            "executionSummary": {
                "targetCount": len(targets),
                "enabledSpot": enabled_spot,
                "enabledSwap": enabled_swap,
                "activeSymbols": active_symbols,
                "queuedSubmitted": queued_submitted,
                "arbRuntime": arb_runtime,
            },
        }

    def _clear_basis_arb_state(self, spot_key: str, swap_key: str) -> None:
        clear_patch = {
            "arbBias": "",
            "arbSpotSize": "0",
            "arbSwapSize": "0",
            "arbOpenedAt": "",
            "arbEntrySpreadPct": "",
            "arbEntrySpotPx": "",
            "arbEntrySwapPx": "",
            "arbStage": "",
        }
        self._set_market(spot_key, clear_patch)
        self._set_market(swap_key, clear_patch)

    def _run_basis_arb_cycle(
        self,
        client: OkxClient,
        automation: dict[str, Any],
        balance_snapshot: dict[str, Any],
        analysis_label: str,
        allow_new_entries: bool,
        *,
        spot_key: str,
        swap_key: str,
    ) -> None:
        spot_inst_id = automation["spotInstId"]
        swap_inst_id = automation["swapInstId"]
        spot_meta = get_instrument_meta(client, "SPOT", spot_inst_id)
        swap_meta = get_instrument_meta(client, "SWAP", swap_inst_id)
        spot_ticker = extract_first_row(client.get_ticker(spot_inst_id))
        swap_ticker = extract_first_row(client.get_ticker(swap_inst_id))
        funding_row = extract_first_row(client.get_funding_rate(swap_inst_id))
        spot_bid = ticker_bid_price(spot_ticker)
        spot_ask = ticker_ask_price(spot_ticker)
        swap_bid = ticker_bid_price(swap_ticker)
        swap_ask = ticker_ask_price(swap_ticker)
        spot_last = safe_decimal(spot_ticker.get("last"), "0")
        swap_last = safe_decimal(swap_ticker.get("last"), "0")
        funding_rate_pct = safe_decimal(funding_row.get("fundingRate"), "0") * Decimal("100")
        entry_spread_pct = pct_gap(swap_bid, spot_ask)
        close_spread_pct = pct_gap(swap_ask, spot_bid)
        entry_threshold = safe_decimal(automation.get("arbEntrySpreadPct"), "0")
        exit_threshold = safe_decimal(automation.get("arbExitSpreadPct"), "0")
        min_funding = safe_decimal(automation.get("arbMinFundingRatePct"), "0")
        max_hold_minutes = int(automation.get("arbMaxHoldMinutes", 180) or 180)
        require_funding_alignment = bool(automation.get("arbRequireFundingAlignment"))
        funding_aligned = funding_rate_pct >= min_funding if require_funding_alignment else True

        base_ccy = spot_inst_id.split("-")[0]
        balance_row = find_balance_detail(balance_snapshot, base_ccy)
        available_base = safe_decimal(
            balance_row.get("availBal") or balance_row.get("cashBal") or balance_row.get("eq"),
            "0",
        )
        spot_min_size = safe_decimal(spot_meta.get("minSz") or spot_meta.get("lotSz"), "0.00000001")
        spot_lot_size = safe_decimal(spot_meta.get("lotSz") or spot_meta.get("minSz"), "0.00000001")
        swap_lot_size = safe_decimal(swap_meta.get("lotSz"), "1")
        positions = client.get_positions(swap_inst_id).get("data", [])
        open_position = next((row for row in positions if safe_decimal(row.get("pos"), "0") != 0), {})
        pos_value = safe_decimal(open_position.get("pos"), "0")
        abs_pos = abs(pos_value)
        position_side = "flat"
        if pos_value > 0:
            position_side = "long"
        elif pos_value < 0:
            position_side = "short"

        current_state = self.snapshot()
        spot_state = current_state.get("markets", {}).get(spot_key) or {}
        swap_state = current_state.get("markets", {}).get(swap_key) or {}
        tracked_bias = str(spot_state.get("arbBias") or swap_state.get("arbBias") or "").strip()
        tracked_spot_size = safe_decimal(spot_state.get("arbSpotSize") or swap_state.get("arbSpotSize"), "0")
        tracked_swap_size = safe_decimal(swap_state.get("arbSwapSize") or spot_state.get("arbSwapSize"), "0")
        tracked_opened_at = str(spot_state.get("arbOpenedAt") or swap_state.get("arbOpenedAt") or "")
        tracked_spot_live = round_down(min(available_base, tracked_spot_size), spot_lot_size)
        tracked_swap_live = round_down(min(abs_pos, tracked_swap_size), swap_lot_size)
        pair_active = (
            tracked_bias == "long_spot_short_swap"
            and tracked_spot_live >= spot_min_size
            and position_side == "short"
            and tracked_swap_live > 0
        )

        hold_minutes = 0
        opened_at = parse_iso(tracked_opened_at)
        if opened_at:
            hold_minutes = max(int((datetime.now() - opened_at).total_seconds() // 60), 0)
        reverse_basis = entry_spread_pct <= 0
        arb_stage = "watching"
        if tracked_bias and not pair_active:
            arb_stage = "broken_pair"
        elif pair_active:
            arb_stage = "hedged"
        elif reverse_basis:
            arb_stage = "reverse_basis"
        elif entry_spread_pct >= entry_threshold and funding_aligned:
            arb_stage = "window_open"
        elif entry_spread_pct >= entry_threshold and not funding_aligned:
            arb_stage = "funding_blocked"

        shared_label = (
            f"套利入场 {decimal_to_str(entry_spread_pct.quantize(Decimal('0.001')))}% / "
            f"回补 {decimal_to_str(close_spread_pct.quantize(Decimal('0.001')))}% / "
            f"资金费 {decimal_to_str(funding_rate_pct.quantize(Decimal('0.001')))}%"
        )
        spot_entry_px = safe_decimal(spot_state.get("arbEntrySpotPx"), "0")
        swap_entry_px = safe_decimal(swap_state.get("arbEntrySwapPx"), "0")
        spot_pnl = (spot_last - spot_entry_px) * tracked_spot_live if tracked_spot_live > 0 and spot_entry_px > 0 else Decimal("0")
        spot_pnl_pct = pct_gap(spot_last, spot_entry_px) if tracked_spot_live > 0 and spot_entry_px > 0 else Decimal("0")
        self._set_market(
            spot_key,
            {
                "enabled": True,
                "instId": spot_inst_id,
                "signal": "arb_active" if pair_active else ("arb_entry" if entry_spread_pct >= entry_threshold else "hold"),
                "trend": "basis_arb",
                "lastPrice": decimal_to_str(spot_last),
                "positionSide": "long" if tracked_spot_live >= spot_min_size else "flat",
                "positionSize": decimal_to_str(tracked_spot_live),
                "positionNotional": decimal_to_str(tracked_spot_live * spot_last),
                "entryPrice": decimal_to_str(spot_entry_px) if spot_entry_px > 0 else "",
                "floatingPnl": decimal_to_str(spot_pnl),
                "floatingPnlPct": decimal_to_str(spot_pnl_pct),
                "pnlSource": "套利现货腿按最新价估算",
                "riskBudget": decimal_to_str(safe_decimal(automation.get("spotQuoteBudget"), "0")),
                "riskCap": decimal_to_str(safe_decimal(automation.get("spotMaxExposure"), "0")),
                "riskMode": "basis_arb",
                "riskLabel": build_market_risk_label(automation, "spot"),
                "lastMessage": shared_label,
                "arbBias": tracked_bias,
                "arbSpotSize": decimal_to_str(tracked_spot_size),
                "arbSwapSize": decimal_to_str(tracked_swap_size),
                "arbOpenedAt": tracked_opened_at,
                "arbEntrySpreadPct": str(spot_state.get("arbEntrySpreadPct") or ""),
                "arbEntrySpotPx": str(spot_state.get("arbEntrySpotPx") or ""),
                "arbEntrySwapPx": str(spot_state.get("arbEntrySwapPx") or ""),
                "arbStage": arb_stage,
            },
        )
        self._set_market(
            swap_key,
            {
                "enabled": True,
                "instId": swap_inst_id,
                "signal": "arb_hedged" if pair_active else ("arb_entry" if entry_spread_pct >= entry_threshold else "hold"),
                "trend": "basis_arb",
                "lastPrice": decimal_to_str(swap_last),
                "positionSide": position_side,
                "positionSize": decimal_to_str(tracked_swap_live if pair_active else abs_pos),
                "positionNotional": decimal_to_str((tracked_swap_live if pair_active else abs_pos) * swap_last),
                "entryPrice": decimal_to_str(swap_entry_px) if swap_entry_px > 0 else (decimal_to_str(safe_decimal(open_position.get("avgPx"), "0")) if open_position else ""),
                "floatingPnl": decimal_to_str(safe_decimal(open_position.get("upl"), "0")),
                "floatingPnlPct": decimal_to_str(Decimal("0")),
                "pnlSource": "套利永续腿未实现盈亏",
                "riskBudget": decimal_to_str(safe_decimal(automation.get("swapContracts"), "0")),
                "riskCap": decimal_to_str(safe_decimal(automation.get("swapLeverage"), "0")),
                "riskMode": "basis_arb",
                "riskLabel": build_market_risk_label(automation, "swap"),
                "lastMessage": shared_label,
                "arbBias": tracked_bias,
                "arbSpotSize": decimal_to_str(tracked_spot_size),
                "arbSwapSize": decimal_to_str(tracked_swap_size),
                "arbOpenedAt": tracked_opened_at,
                "arbEntrySpreadPct": str(swap_state.get("arbEntrySpreadPct") or ""),
                "arbEntrySpotPx": str(swap_state.get("arbEntrySpotPx") or ""),
                "arbEntrySwapPx": str(swap_state.get("arbEntrySwapPx") or ""),
                "arbStage": arb_stage,
            },
        )

        if tracked_bias and not pair_active:
            if tracked_spot_live <= 0 and tracked_swap_live <= 0 and position_side == "flat":
                self._clear_basis_arb_state(spot_key, swap_key)
            else:
                self._set_market(spot_key, {"arbStage": "broken_pair", "lastMessage": "检测到套利双腿不完整，暂不继续自动接管"})
                self._set_market(swap_key, {"arbStage": "broken_pair", "lastMessage": "检测到套利双腿不完整，暂不继续自动接管"})
            return

        cooldown_ready, cooldown_reason = self._cooldown_ready(spot_key, int(automation.get("cooldownSeconds", 0)))
        if pair_active:
            should_exit = close_spread_pct <= exit_threshold
            exit_reason = ""
            if should_exit:
                exit_reason = "价差回补，准备平掉套利双腿"
            elif require_funding_alignment and not funding_aligned:
                should_exit = True
                exit_reason = "资金费不再支持套利，准备退场"
            elif max_hold_minutes > 0 and hold_minutes >= max_hold_minutes:
                should_exit = True
                exit_reason = f"已持有 {hold_minutes} 分钟，达到最长持有时长"
            if not should_exit:
                self._set_market(spot_key, {"arbStage": "hedged", "lastMessage": f"套利持有中 · {shared_label} · 已持有 {hold_minutes} 分钟"})
                self._set_market(swap_key, {"arbStage": "hedged", "lastMessage": f"套利持有中 · {shared_label} · 已持有 {hold_minutes} 分钟"})
                return
            if not cooldown_ready:
                self._set_market(spot_key, {"arbStage": "exit_wait", "lastMessage": f"{exit_reason}，但{cooldown_reason}"})
                self._set_market(swap_key, {"arbStage": "exit_wait", "lastMessage": f"{exit_reason}，但{cooldown_reason}"})
                return
            close_swap_size = round_down(tracked_swap_live, swap_lot_size)
            close_spot_size = round_down(tracked_spot_live, spot_lot_size)
            if close_swap_size > 0:
                self._place_swap_order(
                    client,
                    swap_inst_id,
                    "buy",
                    close_swap_size,
                    automation["swapTdMode"],
                    "套利回补永续空腿",
                    reduce_only=True,
                    market_key=swap_key,
                    strategy_tag="arb_cover",
                    strategy_action="cover",
                    strategy_leg="swap",
                )
            if close_spot_size >= spot_min_size:
                self._place_spot_order(
                    client,
                    spot_inst_id,
                    "sell",
                    close_spot_size,
                    "套利卖出现货腿",
                    market_key=spot_key,
                    strategy_tag="arb_exit",
                    strategy_action="exit",
                    strategy_leg="spot",
                )
            self._clear_basis_arb_state(spot_key, swap_key)
            self._set_market(spot_key, {"arbStage": "exiting", "lastMessage": exit_reason})
            self._set_market(swap_key, {"arbStage": "exiting", "lastMessage": exit_reason})
            return

        if not cooldown_ready:
            self._set_market(spot_key, {"arbStage": "entry_wait", "lastMessage": f"套利窗口出现，但{cooldown_reason}"})
            self._set_market(swap_key, {"arbStage": "entry_wait", "lastMessage": f"套利窗口出现，但{cooldown_reason}"})
            return
        if entry_spread_pct <= 0:
            self._set_market(spot_key, {"arbStage": "reverse_basis", "lastMessage": f"{shared_label} · 当前是负基差，这版只做现货多 + 永续空"})
            self._set_market(swap_key, {"arbStage": "reverse_basis", "lastMessage": f"{shared_label} · 当前是负基差，这版只做现货多 + 永续空"})
            return
        if entry_spread_pct < entry_threshold:
            self._set_market(spot_key, {"arbStage": "watching", "lastMessage": f"{shared_label} · 正基差还没到入场阈值 {decimal_to_str(entry_threshold)}%"})
            self._set_market(swap_key, {"arbStage": "watching", "lastMessage": f"{shared_label} · 正基差还没到入场阈值 {decimal_to_str(entry_threshold)}%"})
            return
        if require_funding_alignment and not funding_aligned:
            self._set_market(spot_key, {"arbStage": "funding_blocked", "lastMessage": f"{shared_label} · 资金费低于阈值 {decimal_to_str(min_funding)}%"})
            self._set_market(swap_key, {"arbStage": "funding_blocked", "lastMessage": f"{shared_label} · 资金费低于阈值 {decimal_to_str(min_funding)}%"})
            return

        quote_budget = safe_decimal(automation.get("spotQuoteBudget"), "0")
        trade_contracts = round_down(safe_decimal(automation.get("swapContracts"), "0"), swap_lot_size)
        estimated_spot_size = round_down((quote_budget / spot_ask) if spot_ask > 0 else Decimal("0"), spot_lot_size)
        if quote_budget <= 0 or estimated_spot_size < spot_min_size:
            self._set_market(spot_key, {"arbStage": "blocked_budget", "lastMessage": "套利预算过小，现货腿无法开仓"})
            self._set_market(swap_key, {"arbStage": "blocked_budget", "lastMessage": "套利预算过小，现货腿无法开仓"})
            return
        if trade_contracts <= 0:
            hedge_message = (
                f"当前币只分到 {decimal_to_str(safe_decimal(automation.get('swapContracts'), '0'))} 张，"
                f"按步长 {decimal_to_str(swap_lot_size)} 取整后为 0，无法建立套利对冲"
            )
            self._set_market(spot_key, {"arbStage": "blocked_hedge", "lastMessage": hedge_message})
            self._set_market(swap_key, {"arbStage": "blocked_hedge", "lastMessage": hedge_message})
            return

        self._place_spot_order(
            client,
            spot_inst_id,
            "buy",
            quote_budget,
            "套利买入现货腿",
            market_key=spot_key,
            strategy_tag="arb_entry",
            strategy_action="entry",
            strategy_leg="spot",
        )
        try:
            self._place_swap_order(
                client,
                swap_inst_id,
                "sell",
                trade_contracts,
                automation["swapTdMode"],
                "套利卖出永续对冲腿",
                market_key=swap_key,
                strategy_tag="arb_hedge",
                strategy_action="hedge",
                strategy_leg="swap",
            )
        except Exception:
            try:
                self._place_spot_order(
                    client,
                    spot_inst_id,
                    "sell",
                    estimated_spot_size,
                    "套利对冲失败，回滚现货腿",
                    market_key=spot_key,
                    strategy_tag="arb_rb",
                    strategy_action="rollback",
                    strategy_leg="spot",
                )
                self._set_market(spot_key, {"arbStage": "rollback", "lastMessage": "套利对冲失败，已发起现货回滚"})
                self._set_market(swap_key, {"arbStage": "rollback", "lastMessage": "套利对冲失败，已发起现货回滚"})
            except Exception as rollback_exc:
                self._log("error", f"套利对冲腿失败且现货回滚也失败：{rollback_exc}")
            raise

        arb_patch = {
            "arbBias": "long_spot_short_swap",
            "arbSpotSize": decimal_to_str(estimated_spot_size),
            "arbSwapSize": decimal_to_str(trade_contracts),
            "arbOpenedAt": now_local_iso(),
            "arbEntrySpreadPct": decimal_to_str(entry_spread_pct),
            "arbEntrySpotPx": decimal_to_str(spot_ask),
            "arbEntrySwapPx": decimal_to_str(swap_bid),
            "arbStage": "hedged",
        }
        self._set_market(spot_key, {**arb_patch, "entryPrice": decimal_to_str(spot_ask), "lastMessage": "套利双腿已建立，等待价差回补"})
        self._set_market(swap_key, {**arb_patch, "entryPrice": decimal_to_str(swap_bid), "lastMessage": "套利双腿已建立，等待价差回补"})

    def _complete_cycle_stage(
        self,
        *,
        cycle_started: float,
        cycle_stamp: str,
        analysis: dict[str, Any],
        effective_automation: dict[str, Any],
        allow_new_entries: bool,
        watchlist_entries: list[dict[str, Any]],
        total_eq: Decimal,
        execution_summary: dict[str, Any],
    ) -> None:
        is_basis_arb = str(effective_automation.get("strategyPreset") or "") == "basis_arb"
        arb_runtime = execution_summary.get("arbRuntime") or {}
        hedged_pairs = int(arb_runtime.get("hedged") or 0)
        window_open = int(arb_runtime.get("windowOpen") or 0)
        exit_queue = int(arb_runtime.get("exitQueue") or 0)
        rollback_pairs = int(arb_runtime.get("rollback") or 0)
        blocked_pairs = int(arb_runtime.get("blocked") or 0)
        broken_pairs = int(arb_runtime.get("brokenPair") or 0)
        reverse_basis_pairs = int(arb_runtime.get("reverseBasis") or 0)
        if is_basis_arb:
            candidate_count = int(analysis.get("candidateCount") or 0)
            market_candidate_count = int(analysis.get("marketCandidateCount") or 0)
            pipeline_summary = (
                f"可做 {candidate_count} · "
                f"市场 {market_candidate_count} · "
                f"对冲 {hedged_pairs} · "
                f"回补 {exit_queue}"
            )
            if reverse_basis_pairs:
                pipeline_summary += f" · 反向 {reverse_basis_pairs}"
            if rollback_pairs:
                pipeline_summary += f" · 回滚 {rollback_pairs}"
            if blocked_pairs:
                pipeline_summary += f" · 阻塞 {blocked_pairs}"
            if broken_pairs:
                pipeline_summary += f" · 断腿 {broken_pairs}"
            mode_text = (
                f"{analysis.get('selectedStrategyName', strategy_label(effective_automation.get('strategyPreset', 'basis_arb')))}"
                f" · {len(watchlist_entries)} 币"
                f" · 可做 {candidate_count}"
                f" · 市场 {market_candidate_count}"
                f" · 对冲 {hedged_pairs}"
                f" · 回补 {exit_queue}"
            )
            if reverse_basis_pairs:
                mode_text += f" · 反向 {reverse_basis_pairs}"
            if rollback_pairs:
                mode_text += f" · 回滚 {rollback_pairs}"
            last_strategy_detail = (
                f"入场 {effective_automation.get('arbEntrySpreadPct', '0')}% · "
                f"回补 {effective_automation.get('arbExitSpreadPct', '0')}% · "
                f"资金费 {effective_automation.get('arbMinFundingRatePct', '0')}% · "
                f"{len(watchlist_entries) or 1} 币组合"
            )
        else:
            if str(effective_automation.get("strategyPreset") or "") == "dip_swing":
                candidate_count = int(analysis.get("candidateCount") or 0)
                market_scan_count = int(analysis.get("marketScanCount") or 0)
                market_candidate_count = int(analysis.get("marketCandidateCount") or 0)
                selected_from_market = bool(analysis.get("selectedFromMarketScan"))
                pipeline_summary = (
                    f"信号 {analysis.get('decisionLabel', '待分析')} · "
                    f"watchlist 候选 {candidate_count} · "
                    f"市场候选 {market_candidate_count} / 扫描 {market_scan_count} · "
                    f"{int(execution_summary.get('activeSymbols') or 0)} 币持仓"
                )
                mode_text = (
                    f"{analysis.get('selectedStrategyName', strategy_label(effective_automation.get('strategyPreset', 'dip_swing')))}"
                    f" · {analysis.get('decisionLabel', '待分析')}"
                    f" · 市场候选 {market_candidate_count}/{market_scan_count}"
                )
                if selected_from_market:
                    mode_text += " · 轮动接管"
                last_strategy_detail = (
                    f"{effective_automation.get('bar', '5m')} · EMA "
                    f"{effective_automation.get('fastEma', '-')}/{effective_automation.get('slowEma', '-')}"
                    f" · 净优势 {analysis.get('netEdgePct', '--')}%"
                    f" · 评分 {analysis.get('entryScore', '--')}/8"
                )
            else:
                pipeline_summary = (
                    f"信号 {analysis.get('decisionLabel', '待分析')} · "
                    f"{len(watchlist_entries)} 币观察 · "
                    f"{int(execution_summary.get('activeSymbols') or 0)} 币持仓"
                )
                mode_text = (
                    f"{analysis.get('selectedStrategyName', strategy_label(effective_automation.get('strategyPreset', 'dual_engine')))}"
                    + (
                        f" · {len(watchlist_entries)} 币并行"
                        if len(watchlist_entries) > 1
                        else ""
                    )
                    + " · "
                    + analysis.get("decisionLabel", "待分析")
                )
                last_strategy_detail = (
                    f"{effective_automation.get('bar', '5m')} · EMA "
                    f"{effective_automation.get('fastEma', '-')}/{effective_automation.get('slowEma', '-')}"
                    f" · {len(watchlist_entries) or 1} 币组合"
                )
        self._update_state(
            lambda current: current.update(
                {
                    "lastCycleAt": now_local_iso(),
                    "lastCycleDurationMs": int((time.perf_counter() - cycle_started) * 1000),
                    "statusText": "运行中" if allow_new_entries else analysis.get("decisionLabel", "观察中"),
                    "lastError": "",
                    "modeText": mode_text,
                    "lastAppliedStrategy": {
                        "stage": "running" if allow_new_entries else "synced",
                        "title": analysis.get("selectedStrategyName")
                        or strategy_label(effective_automation.get("strategyPreset", "dual_engine")),
                        "detail": last_strategy_detail,
                        "appliedAt": cycle_stamp,
                    },
                    "lastPipeline": {
                        "signal": "ok",
                        "portfolio": "ok",
                        "risk": "ok",
                        "execution": "ok",
                        "targetCount": len(watchlist_entries),
                        "allowNewEntries": allow_new_entries,
                        "equity": decimal_to_str(total_eq),
                        "completedAt": cycle_stamp,
                        "summary": pipeline_summary,
                        "executionSummary": execution_summary,
                    },
                }
            )
        )

    def _run_cycle(self, client: OkxClient, automation: dict[str, Any]) -> None:
        cycle_started = time.perf_counter()
        cycle_stamp = now_local_iso()
        signal_stage = self._run_signal_stage(client, automation)
        analysis = signal_stage["analysis"]
        effective_automation = signal_stage["effectiveAutomation"]
        allow_new_entries = bool(signal_stage["allowNewEntries"])

        portfolio_stage = self._run_portfolio_stage(client, effective_automation)
        risk_stop_reason = self._run_risk_stage(
            effective_automation,
            portfolio_stage["sessionState"],
        )
        if risk_stop_reason:
            self._update_state(
                lambda current: current.update(
                    {
                        "statusText": (
                            f"{risk_stop_reason} · 测试继续"
                            if should_keep_running_in_test_mode(effective_automation)
                            else risk_stop_reason
                        ),
                        "lastPipeline": {
                            "signal": "ok",
                            "portfolio": "ok",
                            "risk": "blocked",
                            "execution": "skipped",
                            "targetCount": len(portfolio_stage["targets"]),
                            "allowNewEntries": allow_new_entries,
                            "equity": decimal_to_str(portfolio_stage["totalEq"]),
                            "completedAt": cycle_stamp,
                            "summary": risk_stop_reason,
                        }
                    }
                )
            )
            if should_keep_running_in_test_mode(effective_automation):
                self._log("warning", f"{risk_stop_reason} · 测试阶段继续运行")
                return
            self.stop(risk_stop_reason)
            return

        execution_stage = self._run_execution_stage(
            client,
            effective_automation,
            portfolio_stage["balanceSnapshot"],
            allow_new_entries,
            analysis.get("decisionLabel", "观察为主"),
            portfolio_stage["targets"],
        )
        self._complete_cycle_stage(
            cycle_started=cycle_started,
            cycle_stamp=cycle_stamp,
            analysis=analysis,
            effective_automation=effective_automation,
            allow_new_entries=allow_new_entries,
            watchlist_entries=execution_stage["watchlistEntries"],
            total_eq=portfolio_stage["totalEq"],
            execution_summary=execution_stage["executionSummary"],
        )

    def _run_spot_cycle(
        self,
        client: OkxClient,
        automation: dict[str, Any],
        balance_snapshot: dict[str, Any],
        allow_new_entries: bool,
        analysis_label: str,
        *,
        market_key: str = "spot",
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
        market_state = self.snapshot()["markets"].get(market_key, {})
        entry_price = safe_decimal(market_state.get("entryPrice"), "0")
        floating_pnl = Decimal("0")
        floating_pnl_pct = Decimal("0")
        if active and entry_price > 0 and last_price > 0:
            floating_pnl = (last_price - entry_price) * base_size
            floating_pnl_pct = ((last_price - entry_price) / entry_price) * Decimal("100")

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
            "floatingPnl": decimal_to_str(floating_pnl),
            "floatingPnlPct": decimal_to_str(floating_pnl_pct),
            "pnlSource": "现货持仓按最新价估算",
            "riskBudget": decimal_to_str(safe_decimal(automation.get("spotQuoteBudget"), "0")),
            "riskCap": decimal_to_str(safe_decimal(automation.get("spotMaxExposure"), "0")),
            "riskMode": "cash",
            "riskLabel": build_market_risk_label(automation, "spot"),
        }
        if active and entry_price <= 0:
            patch["entryPrice"] = signal["lastClose"]
        self._set_market(market_key, patch)

        cooldown_ready, reason = self._cooldown_ready(market_key, int(automation["cooldownSeconds"]))
        stop_loss = safe_decimal(automation.get("stopLossPct"), "0")
        take_profit = safe_decimal(automation.get("takeProfitPct"), "0")
        if active and entry_price > 0:
            if stop_loss > 0 and last_price <= entry_price * (Decimal("1") - stop_loss / Decimal("100")):
                if cooldown_ready:
                    sell_size = round_down(base_size, lot_size)
                    if sell_size >= min_size:
                        self._place_spot_order(client, inst_id, "sell", sell_size, "止损离场", market_key=market_key)
                else:
                    self._set_market(market_key, {"lastMessage": f"现货止损触发，但{reason}"})
                return
            if take_profit > 0 and last_price >= entry_price * (Decimal("1") + take_profit / Decimal("100")):
                if cooldown_ready:
                    sell_size = round_down(base_size, lot_size)
                    if sell_size >= min_size:
                        self._place_spot_order(client, inst_id, "sell", sell_size, "止盈离场", market_key=market_key)
                else:
                    self._set_market(market_key, {"lastMessage": f"现货止盈触发，但{reason}"})
                return

        if signal["signal"] == "bull_cross" and not active:
            if not allow_new_entries:
                self._set_market(market_key, {"lastMessage": f"现货金叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开仓"})
                return
            if not cooldown_ready:
                self._set_market(market_key, {"lastMessage": f"现货买入信号出现，但{reason}"})
                return
            quote_budget = safe_decimal(automation.get("spotQuoteBudget"), "0")
            max_exposure = safe_decimal(automation.get("spotMaxExposure"), "0")
            if quote_budget <= 0:
                self._set_market(market_key, {"lastMessage": "现货预算为 0，已跳过买入"})
                return
            if max_exposure > 0 and notional >= max_exposure:
                self._set_market(market_key, {"lastMessage": "现货仓位已达到上限，跳过加仓"})
                return
            self._place_spot_order(client, inst_id, "buy", quote_budget, "金叉开仓", market_key=market_key)
            self._set_market(market_key, {"entryPrice": signal["lastClose"]})
            return

        if signal["signal"] == "bear_cross" and active:
            if not cooldown_ready:
                self._set_market(market_key, {"lastMessage": f"现货卖出信号出现，但{reason}"})
                return
            sell_size = round_down(base_size, lot_size)
            if sell_size >= min_size:
                self._place_spot_order(client, inst_id, "sell", sell_size, "死叉离场", market_key=market_key)

    def _run_dip_swing_swap_cycle(
        self,
        client: OkxClient,
        automation: dict[str, Any],
        balance_snapshot: dict[str, Any],
        allow_new_entries: bool,
        analysis_label: str,
        *,
        market_key: str = "swap",
    ) -> None:
        inst_id = automation["swapInstId"]
        meta = get_instrument_meta(client, "SWAP", inst_id)
        candles = get_closed_candles(
            client,
            inst_id,
            automation["bar"],
            max(int(automation["slowEma"]) + 30, 80),
        )
        signal = build_pullback_signal(candles, int(automation["fastEma"]), int(automation["slowEma"]))
        desired_side = profit_loop_trade_side(signal, candles)
        desired_side_label = profit_loop_trade_side_label(desired_side)
        loop_snapshot = build_profit_loop_snapshot_metrics(
            {
                "signal": signal,
                "candles": candles,
                "emaSpreadPct": signal.get("emaSpreadPct"),
                "fastSlopePct": signal.get("fastSlopePct"),
                "atrPct": signal.get("atrPct"),
                "volatilityPct": signal.get("volatilityPct"),
                "estimatedCostPct": signal.get("estimatedCostPct"),
                "liquidityScore": Decimal("0"),
                "executionQualityScore": Decimal("0"),
                "symbolPerformancePenalty": Decimal("0"),
                "basisPct": Decimal("0"),
            }
        )
        predicted_net_pct = safe_decimal(loop_snapshot.get("predictedNetPct"), "0")
        aggressive_scalp_mode = DIP_SWING_AGGRESSIVE_SCALP_MODE
        volatility_pct = safe_decimal(signal.get("volatilityPct"), decimal_to_str(recent_range_pct(candles)))
        avg_quote_volume_usd = average_quote_volume_usd(candles)
        positions = client.get_positions(inst_id).get("data", [])
        open_position = next((row for row in positions if safe_decimal(row.get("pos"), "0") != 0), {})
        pos_value = safe_decimal(open_position.get("pos"), "0")
        abs_pos = abs(pos_value)
        entry_price = safe_decimal(open_position.get("avgPx"), "0")
        last_price = safe_decimal(signal["lastClose"])
        liq_price = safe_decimal(open_position.get("liqPx"), "0")
        liq_buffer = liquidation_buffer_pct(last_price, liq_price, "long" if pos_value > 0 else "short" if pos_value < 0 else "flat")
        lot_size = safe_decimal(meta.get("lotSz"), "1")
        contract_value = safe_decimal(meta.get("ctVal"), decimal_to_str(default_swap_contract_value(inst_id)))
        leverage = safe_decimal(automation.get("swapLeverage"), "1")
        fee_rates = cache_okx_swap_fee_rates(client, inst_id, meta=meta)
        maker_fee_pct = safe_decimal(fee_rates.get("makerFeePct"), decimal_to_str(OKX_DEFAULT_SWAP_MAKER_FEE_PCT))
        taker_fee_pct = safe_decimal(fee_rates.get("takerFeePct"), decimal_to_str(OKX_DEFAULT_SWAP_TAKER_FEE_PCT))
        entry_score = int(signal.get("entryScore") or 0)
        exit_score = int(signal.get("exitScore") or 0)
        cost_snapshot = estimate_dip_swing_cost_snapshot(
            volatility_pct,
            funding_rate_pct=max(Decimal("0"), safe_decimal(extract_first_row(client.get_funding_rate(inst_id)).get("fundingRate"), "0") * Decimal("100")),
            maker_fee_pct=maker_fee_pct,
            taker_fee_pct=taker_fee_pct,
        )
        estimated_cost_pct = safe_decimal(cost_snapshot.get("estimatedCostPct"), decimal_to_str(DIP_SWING_EST_ROUNDTRIP_COST_PCT))
        atr_pct = safe_decimal(signal.get("atrPct"), decimal_to_str(average_true_range_pct(candles)))
        setup_edge_pct = (
            max(safe_decimal(signal.get("emaSpreadPct"), "0"), Decimal("0"))
            + max(safe_decimal(signal.get("fastSlopePct"), "0"), Decimal("0"))
            + max(safe_decimal(signal.get("reboundPct"), "0"), Decimal("0"))
        )
        net_edge_pct = setup_edge_pct - estimated_cost_pct
        edge_cost_ratio = (setup_edge_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
        range_cost_ratio = (volatility_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
        atr_cost_ratio = (atr_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
        edge_cost_ready = edge_cost_ratio >= DIP_SWING_MIN_EDGE_COST_RATIO
        range_cost_ready = range_cost_ratio >= DIP_SWING_MIN_RANGE_COST_RATIO
        atr_cost_ready = atr_cost_ratio >= DIP_SWING_MIN_ATR_COST_RATIO
        liquidity_ready = avg_quote_volume_usd >= DIP_SWING_MIN_AVG_QUOTE_VOLUME_USD
        fee_exit_floor_pct = max(
            safe_decimal(cost_snapshot.get("protectiveExitFloorPct"), decimal_to_str(DIP_SWING_MIN_PROTECTIVE_EXIT_PCT)),
            estimated_cost_pct + (DIP_SWING_MIN_NET_EDGE_PCT / Decimal("2")),
        )
        severe_trend_break = bool(signal.get("weakTrendReady")) and (
            exit_score >= DIP_SWING_HARD_EXIT_SCORE
            or (
                str(signal.get("trend") or "") == "down"
                and not bool(signal.get("closeAboveSlow"))
                and safe_decimal(signal.get("slowSlopePct"), "0") < Decimal("0")
            )
        )
        target_plan = self._target_execution_plan(
            automation,
            lot_size,
            last_price=last_price,
            contract_value=contract_value,
            leverage=leverage,
            entry_score=entry_score,
        )
        target_snapshot = self._target_balance_snapshot(automation)
        planned_trade_contracts = safe_decimal(target_plan.get("plannedContracts"), "0")
        margin_ccy = str(meta.get("settleCcy") or "USDT").strip() or "USDT"
        available_margin = resolve_swap_available_margin(balance_snapshot, margin_ccy)
        available_margin_plan = clamp_swap_contracts_to_available_margin(
            planned_trade_contracts,
            available_margin=available_margin,
            lot_size=lot_size,
            last_price=last_price,
            contract_value=contract_value,
            leverage=leverage,
        )
        trade_contracts = safe_decimal(available_margin_plan.get("contracts"), "0")
        effective_margin_budget = (
            safe_decimal(available_margin_plan.get("contractMargin"), "0") * trade_contracts
            if trade_contracts > 0
            else Decimal("0")
        )
        entry_projection = estimate_profit_loop_entry_net_pnl(
            planned_contracts=trade_contracts,
            last_price=last_price,
            contract_value=contract_value,
            predicted_net_pct=predicted_net_pct,
        )
        projected_entry_net_pnl = safe_decimal(entry_projection.get("projectedNetPnl"), "0")
        position_side = "flat"
        if pos_value > 0:
            position_side = "long"
        elif pos_value < 0:
            position_side = "short"
        tracked_market = copy.deepcopy((self.snapshot().get("markets") or {}).get(market_key) or {})
        tracked_trade_at = parse_iso(str(tracked_market.get("lastTradeAt") or tracked_market.get("lastActionAt") or ""))
        position_hold_minutes = (
            max(int((datetime.now() - tracked_trade_at).total_seconds() // 60), 0)
            if tracked_trade_at
            else 0
        )
        holding_same_direction = (
            (position_side == "long" and desired_side == "buy")
            or (position_side == "short" and desired_side == "sell")
        )
        configured_target_count = max(1, len(build_execution_targets(automation)))
        force_market_entry_by_mode = (
            aggressive_scalp_mode
            or int(automation.get("pollSeconds", 0) or 0) <= DIP_SWING_FORCE_MARKET_ENTRY_POLL_SECONDS
            or configured_target_count >= DIP_SWING_FORCE_MARKET_ENTRY_TARGET_COUNT
        )
        effective_cooldown_seconds = 0 if aggressive_scalp_mode else min(max(0, int(automation.get("cooldownSeconds", 0))), 2)
        pending_entry_orders = self._working_swap_orders(inst_id, side="buy", reduce_only=False)
        pending_entry_orders += self._working_swap_orders(inst_id, side="sell", reduce_only=False)
        pending_entry_capacity = max(1, DIP_SWING_MAX_PENDING_ENTRY_ORDERS_PER_SYMBOL)
        prefer_aggressive_entry = False
        pending_exit_orders = self._working_swap_orders(inst_id, side="sell", reduce_only=True)
        pending_exit_orders += self._working_swap_orders(inst_id, side="buy", reduce_only=True)
        stale_exit_orders = [
            order for order in pending_exit_orders
            if self._order_age_seconds(order) >= DIP_SWING_EXIT_ORDER_MAX_AGE_SECONDS
        ]
        symbol_pressure = build_execution_symbol_pressure_snapshot(inst_id, limit=160)
        recent_avg_abs_slip_mark_pct = safe_decimal(symbol_pressure.get("avgAbsSlipMarkPct"), "0")
        recent_avg_abs_slip_index_pct = safe_decimal(symbol_pressure.get("avgAbsSlipIndexPct"), "0")
        recent_taker_fill_pct = safe_decimal(symbol_pressure.get("takerFillPct"), "0")
        execution_cost_floor_pct = safe_decimal(symbol_pressure.get("executionCostFloorPct"), "0")
        recent_net_pnl = safe_decimal(symbol_pressure.get("recentNetPnl"), "0")
        recent_close_orders = int(symbol_pressure.get("closeOrders") or 0)
        symbol_cycle_block_reason = dip_swing_symbol_cycle_block_reason(symbol_pressure)
        symbol_cycle_blocked = bool(symbol_cycle_block_reason)
        symbol_performance_blocked = (
            recent_close_orders >= DIP_SWING_SYMBOL_PERF_MIN_CLOSE_ORDERS
            and recent_net_pnl <= -DIP_SWING_SYMBOL_NEGATIVE_NET_BLOCK_USDT
        )
        symbol_taker_blocked = (
            recent_close_orders >= DIP_SWING_SYMBOL_PERF_MIN_CLOSE_ORDERS
            and recent_net_pnl <= Decimal("0")
            and recent_taker_fill_pct >= DIP_SWING_SYMBOL_MAX_TAKER_FILL_PCT
        )
        if aggressive_scalp_mode:
            symbol_cycle_block_reason = ""
            symbol_cycle_blocked = False
            symbol_performance_blocked = False
            symbol_taker_blocked = False
        estimated_cost_pct = max(
            estimated_cost_pct,
            execution_cost_floor_pct + safe_decimal(cost_snapshot.get("fundingDragPct"), "0"),
        )
        net_edge_pct = setup_edge_pct - estimated_cost_pct
        edge_cost_ratio = (setup_edge_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
        range_cost_ratio = (volatility_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
        atr_cost_ratio = (atr_pct / estimated_cost_pct) if estimated_cost_pct > 0 else Decimal("0")
        edge_cost_ready = edge_cost_ratio >= DIP_SWING_MIN_EDGE_COST_RATIO
        range_cost_ready = range_cost_ratio >= DIP_SWING_MIN_RANGE_COST_RATIO
        atr_cost_ready = atr_cost_ratio >= DIP_SWING_MIN_ATR_COST_RATIO
        floating_pnl = safe_decimal(open_position.get("upl"), "0")
        floating_pnl_pct = Decimal("0")
        if entry_price > 0 and last_price > 0 and position_side == "long":
            floating_pnl_pct = ((last_price - entry_price) / entry_price) * Decimal("100")
        elif entry_price > 0 and last_price > 0 and position_side == "short":
            floating_pnl_pct = ((entry_price - last_price) / entry_price) * Decimal("100")
        net_close_snapshot = estimate_profit_loop_position_net_pnl(
            position_side=position_side,
            position_size=abs_pos,
            entry_price=entry_price,
            last_price=last_price,
            contract_value=contract_value,
            floating_pnl=floating_pnl,
            maker_fee_pct=maker_fee_pct,
            taker_fee_pct=taker_fee_pct,
        )
        net_close_pnl = safe_decimal(net_close_snapshot.get("netClosePnl"), "0")
        profit_target_reached = net_close_pnl >= DIP_SWING_NET_TARGET_USDT
        capital_recycle_needed = (
            aggressive_scalp_mode
            and position_side in {"long", "short"}
            and not profit_target_reached
            and position_hold_minutes >= DIP_SWING_STALLED_POSITION_MAX_HOLD_MINUTES
            and (
                available_margin < DIP_SWING_STALLED_POSITION_MIN_AVAILABLE_MARGIN_USDT
                or trade_contracts <= 0
                or projected_entry_net_pnl < DIP_SWING_NET_TARGET_USDT
            )
        )

        status_text = (
            f"方向 {desired_side_label} / "
            f"每单净利目标 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U / "
            f"当前这单净结果 {format_decimal(net_close_pnl, 2)}U / "
            f"预计这单净结果 {format_decimal(projected_entry_net_pnl, 2)}U / "
            f"预期净优势 {compact_metric(net_edge_pct, '0.01')}% / "
            f"ATR {compact_metric(atr_pct, '0.01')}% / "
            f"maker {compact_metric(maker_fee_pct, '0.001')}% / taker {compact_metric(taker_fee_pct, '0.001')}% / "
            f"滑点 {compact_metric(recent_avg_abs_slip_mark_pct, '0.001')}% / taker占比 {compact_metric(recent_taker_fill_pct, '0.1')}% / "
            f"近场净收益 {format_decimal(recent_net_pnl, 2)}U / "
            f"成交额 {format_decimal(avg_quote_volume_usd, 0)}U / "
            f"开平差 {symbol_pressure['openCloseGap']} / 连开 {symbol_pressure['consecutiveOpenStreak']}"
        )
        if symbol_cycle_blocked:
            status_text += f" / 闭环拦截 {symbol_cycle_block_reason}"
        if liq_buffer > 0 and not aggressive_scalp_mode:
            status_text += f" / 强平缓冲 {compact_metric(liq_buffer, '0.1')}%"
        if safe_decimal(target_snapshot.get("targetEq"), "0") > 0 and not aggressive_scalp_mode:
            ability_text = (
                f"近场净收益 {format_decimal(safe_decimal(target_plan.get('abilityNetPnl'), '0'), 2)}U"
                f" / 平仓胜率 {compact_metric(target_plan.get('abilityCloseWinRatePct'), '0.1')}%"
            )
            if int(target_plan.get("abilityCloseOrders") or 0) <= 0:
                ability_text = "平仓样本不足，先按基础能力执行"
            status_text += (
                f" / 目标 {compact_metric(target_snapshot.get('progressPct'), '0.1')}%"
                f" / 能力 {target_plan.get('phaseLabel', target_execution_phase_label(target_plan.get('phase', 'fixed')))}"
                f" / {ability_text}"
                f" / 保证金 {format_decimal(safe_decimal(target_plan.get('marginBudget'), '0'), 2)}U"
                f" / 动态仓位 {decimal_to_str(trade_contracts)} 张"
            )
        elif aggressive_scalp_mode:
            status_text += (
                f" / 保证金 {format_decimal(effective_margin_budget, 2)}U"
                f" / 动态仓位 {decimal_to_str(trade_contracts)} 张"
            )
        if bool(available_margin_plan.get("clamped")):
            status_text += (
                f" / 可用保证金 {format_decimal(available_margin, 2)} {margin_ccy}"
                f" / 已按账户余量裁剪到 {decimal_to_str(trade_contracts)} 张"
            )
        self._set_market(
            market_key,
            {
                "enabled": True,
                "instId": inst_id,
                "signal": f"profit_loop_{desired_side}",
                "trend": "profit_loop",
                "lastPrice": signal["lastClose"],
                "positionSide": position_side,
                "positionSize": decimal_to_str(abs_pos),
                "positionNotional": decimal_to_str(abs_pos * last_price * contract_value),
                "entryPrice": decimal_to_str(entry_price) if entry_price > 0 else "",
                "floatingPnl": decimal_to_str(floating_pnl),
                "floatingPnlPct": decimal_to_str(floating_pnl_pct),
                "pnlSource": "逐仓永续未实现盈亏",
                "riskBudget": "",
                "riskCap": decimal_to_str(safe_decimal(automation.get("swapLeverage"), "0")),
                "riskMode": str(automation.get("swapTdMode") or "isolated"),
                "riskLabel": build_market_risk_label(automation, "swap"),
                "liquidationPrice": decimal_to_str(liq_price) if liq_price > 0 else "",
                "liquidationBufferPct": decimal_to_str(liq_buffer) if liq_buffer > 0 else "",
                "lastMessage": f"{ONLY_STRATEGY_LABEL}监控 · {status_text}",
            },
        )

        if position_side in {"long", "short"}:
            stale_entry_orders = [
                order for order in pending_entry_orders
                if self._order_age_seconds(order) >= DIP_SWING_ENTRY_ORDER_MAX_AGE_SECONDS
            ]
            wrong_side_entry_orders = [
                order for order in pending_entry_orders
                if str(order.get("side") or "").lower() != desired_side
            ]
            if wrong_side_entry_orders or not holding_same_direction:
                self._cancel_swap_orders(client, inst_id, pending_entry_orders, "持仓方向变化，撤掉旧开仓单", market_key=market_key)
                pending_entry_orders = []
            elif stale_entry_orders:
                self._cancel_swap_orders(client, inst_id, stale_entry_orders, "同向挂单超时，准备重挂", market_key=market_key)
                pending_entry_orders = [order for order in pending_entry_orders if order not in stale_entry_orders]
                prefer_aggressive_entry = True
            elif pending_entry_orders and len(pending_entry_orders) >= pending_entry_capacity:
                if aggressive_scalp_mode:
                    oldest_order = max(pending_entry_orders, key=self._order_age_seconds)
                    self._cancel_swap_orders(client, inst_id, [oldest_order], "超短循环释放挂单槽位", market_key=market_key)
                    pending_entry_orders = [order for order in pending_entry_orders if order != oldest_order]
                else:
                    oldest_age = max(self._order_age_seconds(order) for order in pending_entry_orders)
                    self._set_market(
                        market_key,
                        {
                            "lastMessage": (
                                f"{ONLY_STRATEGY_LABEL}同向持仓继续挂单中 · 已挂 {len(pending_entry_orders)} / 上限 {pending_entry_capacity} 笔"
                                f" / {int(oldest_age)} 秒"
                                f" · {status_text}"
                            )
                        },
                    )
                    return
            if (not aggressive_scalp_mode) and liq_buffer > 0 and liq_buffer <= DIP_SWING_MIN_LIQ_BUFFER_PCT:
                if pending_exit_orders:
                    self._cancel_swap_orders(client, inst_id, pending_exit_orders, "强平缓冲不足，撤掉旧平仓单后紧急退出", market_key=market_key)
                    pending_exit_orders = []
                close_side = "sell" if position_side == "long" else "buy"
                self._place_swap_order(
                    client,
                    inst_id,
                    close_side,
                    round_down(abs_pos, lot_size),
                    automation["swapTdMode"],
                    "强平缓冲不足，主动退场",
                    reduce_only=True,
                    protected_exit=True,
                    market_key=market_key,
                    strategy_action="exit",
                    strategy_leg="swap",
                )
                return
            if aggressive_scalp_mode and not holding_same_direction:
                if pending_exit_orders:
                    if stale_exit_orders:
                        self._cancel_swap_orders(client, inst_id, stale_exit_orders, "方向反转，撤掉超时旧平仓单后重新退出", market_key=market_key)
                        pending_exit_orders = [order for order in pending_exit_orders if order not in stale_exit_orders]
                    if pending_exit_orders:
                        self._set_market(
                            market_key,
                            {
                                "lastMessage": (
                                    f"{ONLY_STRATEGY_LABEL}方向已反转，正在平掉旧仓释放方向"
                                    f" · {status_text}"
                                )
                            },
                        )
                        return
                exit_retry_ready, exit_retry_reason = self._cooldown_ready(market_key, DIP_SWING_EXIT_RETRY_SECONDS)
                if not exit_retry_ready:
                    self._set_market(
                        market_key,
                        {
                            "lastMessage": (
                                f"{ONLY_STRATEGY_LABEL}方向已反转，等待上一笔平仓回报"
                                f" · {exit_retry_reason} · {status_text}"
                            )
                        },
                    )
                    return
                close_side = "sell" if position_side == "long" else "buy"
                self._place_swap_order(
                    client,
                    inst_id,
                    close_side,
                    round_down(abs_pos, lot_size),
                    automation["swapTdMode"],
                    f"{ONLY_STRATEGY_LABEL}方向反转，先平旧仓再继续循环",
                    reduce_only=True,
                    prefer_fill=True,
                    batchable=True,
                    market_key=market_key,
                    strategy_action="exit",
                    strategy_leg="swap",
                )
                return
            if profit_target_reached:
                if pending_exit_orders:
                    if stale_exit_orders:
                        self._cancel_swap_orders(client, inst_id, stale_exit_orders, "净利平仓挂单超时，重新发起保护退出", market_key=market_key)
                        pending_exit_orders = [order for order in pending_exit_orders if order not in stale_exit_orders]
                    if pending_exit_orders:
                        self._set_market(
                            market_key,
                            {
                                "lastMessage": (
                                    f"{ONLY_STRATEGY_LABEL}这单已达到净利目标，已有 {len(pending_exit_orders)} 笔平仓单在执行 · {status_text}"
                                )
                            },
                        )
                        return
                exit_retry_ready, exit_retry_reason = self._cooldown_ready(market_key, DIP_SWING_EXIT_RETRY_SECONDS)
                if not exit_retry_ready:
                    self._set_market(
                        market_key,
                        {
                            "lastMessage": (
                                f"{ONLY_STRATEGY_LABEL}这单已达到净利目标，等待上一笔平仓回报 · {exit_retry_reason} · {status_text}"
                            )
                        },
                    )
                    return
                close_side = "sell" if position_side == "long" else "buy"
                self._place_swap_order(
                    client,
                    inst_id,
                    close_side,
                    round_down(abs_pos, lot_size),
                    automation["swapTdMode"],
                    f"{ONLY_STRATEGY_LABEL}这单净赚 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U+ 平仓",
                    reduce_only=True,
                    passive_exit=not aggressive_scalp_mode,
                    protected_exit=not aggressive_scalp_mode,
                    prefer_fill=aggressive_scalp_mode,
                    batchable=aggressive_scalp_mode,
                    market_key=market_key,
                    strategy_action="exit",
                    strategy_leg="swap",
                )
                return
            if capital_recycle_needed:
                if pending_exit_orders:
                    if stale_exit_orders:
                        self._cancel_swap_orders(client, inst_id, stale_exit_orders, "释放保证金平仓挂单超时，重新发起退出", market_key=market_key)
                        pending_exit_orders = [order for order in pending_exit_orders if order not in stale_exit_orders]
                    if pending_exit_orders:
                        self._set_market(
                            market_key,
                            {
                                "lastMessage": (
                                    f"{ONLY_STRATEGY_LABEL}仓位占用资金过久，正在释放保证金 · 持仓 {position_hold_minutes} 分钟"
                                    f" · {status_text}"
                                )
                            },
                        )
                        return
                exit_retry_ready, exit_retry_reason = self._cooldown_ready(market_key, DIP_SWING_EXIT_RETRY_SECONDS)
                if not exit_retry_ready:
                    self._set_market(
                        market_key,
                        {
                            "lastMessage": (
                                f"{ONLY_STRATEGY_LABEL}仓位占用资金过久，等待上一笔平仓回报后释放保证金"
                                f" · {exit_retry_reason} · {status_text}"
                            )
                        },
                    )
                    return
                close_side = "sell" if position_side == "long" else "buy"
                self._place_swap_order(
                    client,
                    inst_id,
                    close_side,
                    round_down(abs_pos, lot_size),
                    automation["swapTdMode"],
                    (
                        f"{ONLY_STRATEGY_LABEL}仓位占用资金过久，释放保证金重新循环"
                        f" · 持仓 {position_hold_minutes} 分钟"
                    ),
                    reduce_only=True,
                    prefer_fill=True,
                    batchable=aggressive_scalp_mode,
                    market_key=market_key,
                    strategy_action="exit",
                    strategy_leg="swap",
                )
                return
            if pending_exit_orders:
                self._cancel_swap_orders(client, inst_id, pending_exit_orders, "这单净利目标未到，撤掉旧平仓单继续持有", market_key=market_key)
            if allow_new_entries and holding_same_direction and trade_contracts > 0:
                if symbol_cycle_blocked:
                    self._set_market(
                        market_key,
                        {
                            "lastMessage": f"{ONLY_STRATEGY_LABEL}同向持仓暂停加仓 · {symbol_cycle_block_reason} · {status_text}",
                        },
                    )
                    return
                cooldown_ready, reason = self._cooldown_ready(market_key, effective_cooldown_seconds)
                if not cooldown_ready and not aggressive_scalp_mode:
                    self._set_market(
                        market_key,
                        {
                            "lastMessage": f"{ONLY_STRATEGY_LABEL}同方向持仓继续循环，但{reason} · {status_text}",
                        },
                    )
                    return
                force_market_entry = (
                    force_market_entry_by_mode
                    or prefer_aggressive_entry
                    or int(symbol_pressure.get("openCloseGap") or 0) >= DIP_SWING_MARKET_FALLBACK_OPEN_GAP
                    or int(symbol_pressure.get("consecutiveOpenStreak") or 0) >= DIP_SWING_MARKET_FALLBACK_OPEN_STREAK
                )
                entry_mode_label = (
                    "市价直开"
                    if force_market_entry
                    else ("最优限价IOC" if aggressive_scalp_mode else "maker-first")
                )
                self._place_swap_order(
                    client,
                    inst_id,
                    desired_side,
                    trade_contracts,
                    automation["swapTdMode"],
                    (
                        f"{ONLY_STRATEGY_LABEL}同向加仓循环 · {desired_side_label}"
                        f" · 动态仓位 {decimal_to_str(trade_contracts)} 张"
                        f" · {entry_mode_label}"
                        f" · 并发挂单 {len(pending_entry_orders) + 1}/{pending_entry_capacity}"
                    ),
                    passive_entry=not force_market_entry,
                    prefer_fill=aggressive_scalp_mode and not force_market_entry,
                    batchable=aggressive_scalp_mode,
                    market_key=market_key,
                    strategy_action="entry",
                    strategy_leg="swap",
                )
                return
            self._set_market(
                market_key,
                {
                    "lastMessage": f"{ONLY_STRATEGY_LABEL}持仓中 · {status_text}",
                },
            )
            return

        if trade_contracts <= 0:
            self._set_market(
                market_key,
                {
                    "lastMessage": (
                        f"当前可用保证金仅 {format_decimal(available_margin, 2)} {margin_ccy}"
                        "，不足以触发最小下单单位，继续观察"
                    )
                },
            )
            return
        if projected_entry_net_pnl < DIP_SWING_NET_TARGET_USDT:
            self._set_market(
                market_key,
                {
                    "lastMessage": (
                        f"{ONLY_STRATEGY_LABEL}预计这单净赚只有 {format_decimal(projected_entry_net_pnl, 2)}U"
                        f"，达不到每单 {format_decimal(DIP_SWING_NET_TARGET_USDT, 0)}U，不开仓"
                    )
                },
            )
            return
        if symbol_cycle_blocked:
            self._set_market(
                market_key,
                {
                    "lastMessage": f"{ONLY_STRATEGY_LABEL}暂不开仓 · {symbol_cycle_block_reason} · {status_text}"
                },
            )
            return
        entry_signal_ready = True
        if pending_entry_orders:
            stale_entry_orders = [
                order for order in pending_entry_orders
                if self._order_age_seconds(order) >= DIP_SWING_ENTRY_ORDER_MAX_AGE_SECONDS
            ]
            wrong_side_orders = [
                order for order in pending_entry_orders
                if str(order.get("side") or "").lower() != desired_side
            ]
            if position_side != "flat" or wrong_side_orders:
                self._cancel_swap_orders(client, inst_id, pending_entry_orders, "方向变化，撤掉旧开仓单", market_key=market_key)
                pending_entry_orders = []
            elif not entry_signal_ready:
                self._cancel_swap_orders(client, inst_id, pending_entry_orders, "买点失效，撤掉被动挂单", market_key=market_key)
                pending_entry_orders = []
            elif stale_entry_orders:
                self._cancel_swap_orders(client, inst_id, stale_entry_orders, "被动挂单超时，准备重挂", market_key=market_key)
                pending_entry_orders = [order for order in pending_entry_orders if order not in stale_entry_orders]
                prefer_aggressive_entry = True
            elif len(pending_entry_orders) >= pending_entry_capacity:
                if aggressive_scalp_mode:
                    oldest_order = max(pending_entry_orders, key=self._order_age_seconds)
                    self._cancel_swap_orders(client, inst_id, [oldest_order], "超短循环释放挂单槽位", market_key=market_key)
                    pending_entry_orders = [order for order in pending_entry_orders if order != oldest_order]
                else:
                    oldest_age = max(self._order_age_seconds(order) for order in pending_entry_orders)
                    self._set_market(
                        market_key,
                        {
                            "lastMessage": (
                                f"maker-first 挂单等待成交 · 已挂 {len(pending_entry_orders)} / 上限 {pending_entry_capacity} 笔"
                                f" / {int(oldest_age)} 秒"
                                f" · {status_text}"
                            )
                        },
                    )
                    return
        if not allow_new_entries and not aggressive_scalp_mode:
            self._set_market(market_key, {"lastMessage": f"{ONLY_STRATEGY_LABEL}准备开仓，但当前联网决策层为“{analysis_label}”，本轮不新开仓"})
            return
        if not entry_signal_ready and not aggressive_scalp_mode:
            reasons: list[str] = []
            if not edge_cost_ready:
                reasons.append(f"结构优势/成本比不足 {compact_metric(edge_cost_ratio, '0.01')}x")
            if not range_cost_ready:
                reasons.append(f"波动/成本比不足 {compact_metric(range_cost_ratio, '0.01')}x")
            if not atr_cost_ready:
                reasons.append(f"ATR/成本比不足 {compact_metric(atr_cost_ratio, '0.01')}x")
            if not liquidity_ready:
                reasons.append("流动性不足")
            if int(symbol_pressure.get("openCloseGap") or 0) > DIP_SWING_MAX_OPEN_CLOSE_GAP:
                reasons.append(f"开平差过大 {symbol_pressure['openCloseGap']}")
            if int(symbol_pressure.get("consecutiveOpenStreak") or 0) > DIP_SWING_MAX_CONSECUTIVE_OPEN_STREAK:
                reasons.append(f"连续开仓过多 {symbol_pressure['consecutiveOpenStreak']}")
            if symbol_performance_blocked:
                reasons.append(f"近场净亏 {format_decimal(recent_net_pnl, 2)}U")
            if symbol_taker_blocked:
                reasons.append(f"taker 占比偏高 {compact_metric(recent_taker_fill_pct, '0.1')}%")
            if recent_avg_abs_slip_mark_pct > 0 or recent_taker_fill_pct > 0:
                reasons.append(
                    f"近期滑点 {compact_metric(recent_avg_abs_slip_mark_pct, '0.001')}% / taker {compact_metric(recent_taker_fill_pct, '0.1')}%"
                )
            self._set_market(
                market_key,
                {
                    "lastMessage": (
                        f"{ONLY_STRATEGY_LABEL}准备开仓，但近期真实执行拖累偏高"
                        + (f" · {' / '.join(reasons[:4])}" if reasons else "")
                    )
                },
            )
            return
        cooldown_ready, reason = self._cooldown_ready(market_key, effective_cooldown_seconds)
        if not cooldown_ready and not aggressive_scalp_mode:
            self._set_market(market_key, {"lastMessage": f"{ONLY_STRATEGY_LABEL}准备开仓，但{reason}"})
            return
        force_market_entry = (
            force_market_entry_by_mode
            or prefer_aggressive_entry
            or int(symbol_pressure.get("openCloseGap") or 0) >= DIP_SWING_MARKET_FALLBACK_OPEN_GAP
            or int(symbol_pressure.get("consecutiveOpenStreak") or 0) >= DIP_SWING_MARKET_FALLBACK_OPEN_STREAK
        )
        entry_mode_label = (
            "市价直开"
            if force_market_entry
            else ("最优限价IOC" if aggressive_scalp_mode else "maker-first")
        )
        self._place_swap_order(
            client,
            inst_id,
            desired_side,
            trade_contracts,
            automation["swapTdMode"],
            (
                f"{ONLY_STRATEGY_LABEL}开仓 · {desired_side_label}"
                f" · 动态仓位 {decimal_to_str(trade_contracts)} 张"
                f" · {entry_mode_label}"
                f" · 并发挂单 {len(pending_entry_orders) + 1}/{pending_entry_capacity}"
            ),
            passive_entry=not force_market_entry,
            prefer_fill=aggressive_scalp_mode and not force_market_entry,
            batchable=aggressive_scalp_mode,
            market_key=market_key,
            strategy_action="entry",
            strategy_leg="swap",
        )

    def _run_swap_cycle(
        self,
        client: OkxClient,
        automation: dict[str, Any],
        balance_snapshot: dict[str, Any],
        allow_new_entries: bool,
        analysis_label: str,
        *,
        market_key: str = "swap",
    ) -> None:
        if str(automation.get("strategyPreset") or "") == "dip_swing":
            self._run_dip_swing_swap_cycle(
                client,
                automation,
                balance_snapshot,
                allow_new_entries,
                analysis_label,
                market_key=market_key,
            )
            return
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
        floating_pnl = safe_decimal(open_position.get("upl"), "0")
        floating_pnl_pct = Decimal("0")
        if entry_price > 0 and last_price > 0:
            if position_side == "long":
                floating_pnl_pct = ((last_price - entry_price) / entry_price) * Decimal("100")
            elif position_side == "short":
                floating_pnl_pct = ((entry_price - last_price) / entry_price) * Decimal("100")

        self._set_market(
            market_key,
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
                "floatingPnl": decimal_to_str(floating_pnl),
                "floatingPnlPct": decimal_to_str(floating_pnl_pct),
                "pnlSource": "永续持仓未实现盈亏",
                "riskBudget": decimal_to_str(safe_decimal(automation.get("swapContracts"), "0")),
                "riskCap": decimal_to_str(safe_decimal(automation.get("swapLeverage"), "0")),
                "riskMode": str(automation.get("swapTdMode") or "cross"),
                "riskLabel": build_market_risk_label(automation, "swap"),
                "lastMessage": f"永续 {signal['trend']} · 信号 {signal['signal']}",
            },
        )

        cooldown_ready, reason = self._cooldown_ready(market_key, int(automation["cooldownSeconds"]))
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
                        market_key=market_key,
                    )
                else:
                    self._set_market(market_key, {"lastMessage": f"永续多单止损触发，但{reason}"})
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
                        market_key=market_key,
                    )
                else:
                    self._set_market(market_key, {"lastMessage": f"永续多单止盈触发，但{reason}"})
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
                        market_key=market_key,
                    )
                else:
                    self._set_market(market_key, {"lastMessage": f"永续空单止损触发，但{reason}"})
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
                        market_key=market_key,
                    )
                else:
                    self._set_market(market_key, {"lastMessage": f"永续空单止盈触发，但{reason}"})
                return

        trade_contracts = round_down(safe_decimal(automation.get("swapContracts"), "0"), lot_size)
        margin_ccy = str(meta.get("settleCcy") or "USDT").strip() or "USDT"
        available_margin = resolve_swap_available_margin(balance_snapshot, margin_ccy)
        available_margin_plan = clamp_swap_contracts_to_available_margin(
            trade_contracts,
            available_margin=available_margin,
            lot_size=lot_size,
            last_price=last_price,
            contract_value=safe_decimal(meta.get("ctVal"), decimal_to_str(default_swap_contract_value(inst_id))),
            leverage=safe_decimal(automation.get("swapLeverage"), "1"),
        )
        trade_contracts = safe_decimal(available_margin_plan.get("contracts"), "0")
        if trade_contracts <= 0:
            self._set_market(
                market_key,
                {
                    "lastMessage": (
                        f"可用保证金仅 {format_decimal(available_margin, 2)} {margin_ccy}"
                        "，永续张数被裁剪到 0，已停止发单"
                    )
                },
            )
            return

        mode = automation["swapStrategyMode"]
        if signal["signal"] == "bull_cross":
            if not cooldown_ready:
                self._set_market(market_key, {"lastMessage": f"永续金叉出现，但{reason}"})
                return
            if mode == "long_only":
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market(market_key, {"lastMessage": f"永续金叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "buy",
                        trade_contracts,
                            automation["swapTdMode"],
                            "永续金叉开多",
                            market_key=market_key,
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
                            market_key=market_key,
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
                            market_key=market_key,
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
                        market_key=market_key,
                    )
            else:
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market(market_key, {"lastMessage": f"永续金叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "buy",
                        trade_contracts,
                        automation["swapTdMode"],
                        "永续金叉开多",
                        market_key=market_key,
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
                            market_key=market_key,
                        )
                    if allow_new_entries:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "buy",
                            round_down(trade_contracts, lot_size),
                            automation["swapTdMode"],
                            "永续金叉翻多",
                            market_key=market_key,
                        )
            return

        if signal["signal"] == "bear_cross":
            if not cooldown_ready:
                self._set_market(market_key, {"lastMessage": f"永续死叉出现，但{reason}"})
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
                        market_key=market_key,
                    )
            elif mode == "short_only":
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market(market_key, {"lastMessage": f"永续死叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开空仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        trade_contracts,
                        automation["swapTdMode"],
                        "永续死叉开空",
                        market_key=market_key,
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
                            market_key=market_key,
                        )
                    if allow_new_entries:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "sell",
                            round_down(trade_contracts, lot_size),
                            automation["swapTdMode"],
                            "永续死叉翻空",
                            market_key=market_key,
                        )
            else:
                if position_side == "flat":
                    if not allow_new_entries:
                        self._set_market(market_key, {"lastMessage": f"永续死叉出现，但当前联网决策层为“{analysis_label}”，本轮不新开空仓"})
                        return
                    self._place_swap_order(
                        client,
                        inst_id,
                        "sell",
                        trade_contracts,
                        automation["swapTdMode"],
                        "永续死叉开空",
                        market_key=market_key,
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
                            market_key=market_key,
                        )
                    if allow_new_entries:
                        self._place_swap_order(
                            client,
                            inst_id,
                            "sell",
                            round_down(trade_contracts, lot_size),
                            automation["swapTdMode"],
                            "永续死叉翻空",
                            market_key=market_key,
                        )

    def _place_spot_order(
        self,
        client: OkxClient,
        inst_id: str,
        side: str,
        size: Decimal,
        reason: str,
        *,
        market_key: str = "spot",
        strategy_tag: str = "",
        strategy_action: str = "",
        strategy_leg: str = "",
    ) -> dict[str, Any]:
        execution_mode = "市价"
        if side == "buy":
            payload: dict[str, Any] = {
                "instId": inst_id,
                "tdMode": "cash",
                "side": side,
                "ordType": "market",
                "clOrdId": build_cl_ord_id("s"),
                "sz": decimal_to_str(size),
                "tgtCcy": "quote_ccy",
            }
        else:
            try:
                payload, execution_mode = self._build_protected_spot_sell_order(client, inst_id, size)
            except Exception as exc:
                payload = {
                    "instId": inst_id,
                    "tdMode": "cash",
                    "side": side,
                    "ordType": "market",
                    "clOrdId": build_cl_ord_id("s"),
                    "sz": decimal_to_str(size),
                }
                execution_mode = "市价回退"
                self._log("warn", f"{reason} · 现货 {inst_id} 未能构造保护价 IOC，回退市价: {exc}")
        if strategy_tag:
            payload["tag"] = strategy_tag
        result = client.place_order(payload)
        order = deep_merge(payload, (result.get("data") or [{}])[0])
        if strategy_tag:
            order["tag"] = str(order.get("tag") or strategy_tag)
            order["strategyTag"] = strategy_tag
        if strategy_action:
            order["strategyAction"] = strategy_action
        if strategy_leg:
            order["strategyLeg"] = strategy_leg
        order["strategyReason"] = reason
        if order.get("ordId") or order.get("clOrdId"):
            PRIVATE_ORDER_STREAM._ingest_orders([order])
        order_id = order.get("ordId", "")
        self._increment_order_count(market_key, order_id, reason)
        self._log("info", f"{reason} · 现货 {inst_id} 已发单 · {execution_mode}")
        return order

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
        passive_entry: bool = False,
        passive_exit: bool = False,
        protected_exit: bool = False,
        market_key: str = "swap",
        strategy_tag: str = "",
        strategy_action: str = "",
        strategy_leg: str = "",
        prefer_fill: bool = False,
        batchable: bool = False,
    ) -> dict[str, Any]:
        if strategy_action == "entry" and not reduce_only:
            try:
                positions = client.get_positions(inst_id).get("data", [])
            except Exception:
                positions = []
            open_position = next(
                (row for row in positions if safe_decimal(row.get("pos"), "0") != 0),
                {},
            )
            pos_value = safe_decimal(open_position.get("pos"), "0")
            reverse_entry_blocked = (
                (pos_value > 0 and side == "sell")
                or (pos_value < 0 and side == "buy")
            )
            if reverse_entry_blocked:
                position_side = "做多" if pos_value > 0 else "做空"
                self._set_market(
                    market_key,
                    {
                        "lastMessage": (
                            f"{ONLY_STRATEGY_LABEL}检测到同币反向持仓仍在 {position_side}"
                            "，未达到每单净赚 1U 前不允许反手开新仓"
                        )
                    },
                )
                self._log(
                    "info",
                    (
                        f"{reason} · 永续 {inst_id} 跳过反手开仓"
                        f" · 当前仍有{position_side}仓位 {decimal_to_str(abs(pos_value))}"
                    ),
                )
                return {}
        execution_mode = "市价"
        if prefer_fill and not reduce_only:
            try:
                payload, execution_mode = self._build_aggressive_swap_entry_order(client, inst_id, side, size, td_mode)
            except Exception as exc:
                payload = {
                    "instId": inst_id,
                    "tdMode": td_mode,
                    "side": side,
                    "ordType": "market",
                    "sz": decimal_to_str(size),
                    "clOrdId": build_cl_ord_id("w"),
                }
                execution_mode = "市价回退"
                self._log("warn", f"{reason} · 永续 {inst_id} 未能构造最优限价 IOC，回退市价: {exc}")
        elif passive_entry and not reduce_only:
            try:
                payload, execution_mode = self._build_passive_swap_entry_order(client, inst_id, side, size, td_mode)
            except Exception as exc:
                payload = {
                    "instId": inst_id,
                    "tdMode": td_mode,
                    "side": side,
                    "ordType": "market",
                    "sz": decimal_to_str(size),
                    "clOrdId": build_cl_ord_id("w"),
                }
                execution_mode = "市价回退"
                self._log("warn", f"{reason} · 永续 {inst_id} 未能构造被动挂单，回退市价: {exc}")
        elif prefer_fill and reduce_only:
            try:
                payload, execution_mode = self._build_aggressive_swap_exit_order(client, inst_id, side, size, td_mode)
            except Exception as exc:
                try:
                    payload, execution_mode = self._build_protected_swap_exit_order(client, inst_id, side, size, td_mode)
                    execution_mode = f"{execution_mode} 回退"
                    self._log("warn", f"{reason} · 永续 {inst_id} 未能构造最优限价 IOC 平仓，回退保护价 IOC: {exc}")
                except Exception as fallback_exc:
                    payload = {
                        "instId": inst_id,
                        "tdMode": td_mode,
                        "side": side,
                        "ordType": "market",
                        "sz": decimal_to_str(size),
                        "clOrdId": build_cl_ord_id("w"),
                    }
                    execution_mode = "市价回退"
                    self._log("warn", f"{reason} · 永续 {inst_id} 快速平仓失败，回退市价: {fallback_exc}")
        elif passive_exit and reduce_only:
            try:
                payload, execution_mode = self._build_passive_swap_exit_order(client, inst_id, side, size, td_mode)
            except Exception as exc:
                try:
                    payload, execution_mode = self._build_protected_swap_exit_order(client, inst_id, side, size, td_mode)
                    execution_mode = f"{execution_mode} 回退"
                    self._log("warn", f"{reason} · 永续 {inst_id} 未能构造被动平仓，回退保护价 IOC: {exc}")
                except Exception as fallback_exc:
                    payload = {
                        "instId": inst_id,
                        "tdMode": td_mode,
                        "side": side,
                        "ordType": "market",
                        "sz": decimal_to_str(size),
                        "clOrdId": build_cl_ord_id("w"),
                    }
                    execution_mode = "市价回退"
                    self._log("warn", f"{reason} · 永续 {inst_id} 被动平仓失败，回退市价: {fallback_exc}")
        elif protected_exit and reduce_only:
            try:
                payload, execution_mode = self._build_protected_swap_exit_order(client, inst_id, side, size, td_mode)
            except Exception as exc:
                payload = {
                    "instId": inst_id,
                    "tdMode": td_mode,
                    "side": side,
                    "ordType": "market",
                    "sz": decimal_to_str(size),
                    "clOrdId": build_cl_ord_id("w"),
                }
                execution_mode = "市价回退"
                self._log("warn", f"{reason} · 永续 {inst_id} 保护性退出失败，回退市价: {exc}")
        else:
            payload = {
                "instId": inst_id,
                "tdMode": td_mode,
                "side": side,
                "ordType": "market",
                "sz": decimal_to_str(size),
                "clOrdId": build_cl_ord_id("w"),
            }
        if reduce_only:
            payload["reduceOnly"] = True
        if strategy_tag:
            payload["tag"] = strategy_tag
        if batchable:
            self._queue_swap_order(
                {
                    "payload": deep_merge({}, payload),
                    "marketKey": market_key,
                    "reason": reason,
                    "executionMode": execution_mode,
                    "strategyTag": strategy_tag,
                    "strategyAction": strategy_action,
                    "strategyLeg": strategy_leg,
                }
            )
            self._log("info", f"{reason} · 永续 {inst_id} 已加入批量队列 · {execution_mode}")
            return {"queued": True, "instId": inst_id, "clOrdId": payload.get("clOrdId", ""), "ordType": payload.get("ordType", "")}
        result = client.place_order(payload)
        order = deep_merge(payload, (result.get("data") or [{}])[0])
        if strategy_tag:
            order["tag"] = str(order.get("tag") or strategy_tag)
            order["strategyTag"] = strategy_tag
        if strategy_action:
            order["strategyAction"] = strategy_action
        if strategy_leg:
            order["strategyLeg"] = strategy_leg
        order["strategyReason"] = reason
        if order.get("ordId") or order.get("clOrdId"):
            PRIVATE_ORDER_STREAM._ingest_orders([order])
        order_id = order.get("ordId", "")
        self._increment_order_count(market_key, order_id, reason)
        self._log("info", f"{reason} · 永续 {inst_id} 已发单 · {execution_mode}")
        return order


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
        if path.startswith("/api/miner/"):
            error_response(self, "矿机功能已移除", status=410)
            return
        config = CONFIG.current()
        live_only = prefer_live_execution_state(config)

        if path != "/api/automation/config" and should_proxy_to_remote(config, path):
            try:
                if path in ("/api/health", "/api/focus-snapshot"):
                    response = remote_gateway_request(config, "GET", self.path)
                    payload = response.json()
                    if path == "/api/focus-snapshot":
                        remote_automation = load_remote_automation_config_for_proxy(config)
                        if isinstance(payload.get("automationState"), dict):
                            payload["automationState"] = enrich_remote_dip_swing_runtime_state(
                                payload.get("automationState") or {},
                                remote_automation,
                                CONFIG.current(),
                            )
                        payload.pop("minerOverview", None)
                    payload["executionMode"] = "remote"
                    payload["remoteGatewayUrl"] = remote_gateway_url(config)
                    json_response(self, payload, status=response.status_code)
                elif path == "/api/automation/state":
                    try:
                        response = remote_gateway_request(
                            config,
                            "GET",
                            self.path,
                            timeout=REMOTE_AUTOMATION_STATE_TIMEOUT,
                        )
                        payload = response.json()
                        remote_automation = load_remote_automation_config_for_proxy(config)
                        if isinstance(payload.get("state"), dict):
                            payload["state"] = enrich_remote_dip_swing_runtime_state(
                                payload.get("state") or {},
                                remote_automation,
                                CONFIG.current(),
                            )
                            local_journal = get_execution_journal_snapshot(limit=80, live_only=live_only)
                            if (local_journal.get("orders") or local_journal.get("summary")):
                                payload["state"]["executionJournal"] = local_journal
                            payload["state"]["equityDisplay"] = build_equity_display(
                                None,
                                payload.get("state") or {},
                                payload["state"].get("executionJournal") or local_journal,
                            )
                            payload["state"] = stamp_runtime_state_sync(
                                payload.get("state") or {},
                                source="remote_live",
                                loaded=True,
                                fetched_at=now_local_iso(),
                                age_seconds=0.0,
                                stale=False,
                            )
                            store_cached_remote_automation_state(config, payload["state"])
                        payload["remoteStateLoaded"] = True
                        payload["remoteStateSource"] = "remote_live"
                        json_response(self, payload, status=response.status_code)
                    except Exception as exc:
                        local_journal = get_execution_journal_snapshot(limit=80, live_only=live_only)
                        fallback_error = summarize_remote_runtime_error(exc)
                        cached_state, cached_ts = load_cached_remote_automation_state(config)
                        if cached_state:
                            fallback_state = copy.deepcopy(cached_state)
                            if (local_journal.get("orders") or local_journal.get("summary")):
                                fallback_state["executionJournal"] = local_journal
                            fallback_state["equityDisplay"] = build_equity_display(
                                None,
                                fallback_state,
                                fallback_state.get("executionJournal") or local_journal,
                            )
                            warnings = list((fallback_state.get("analysis") or {}).get("warnings") or [])
                            warnings.append(fallback_error)
                            fallback_state.setdefault("analysis", {})["warnings"] = warnings[-6:]
                            fallback_state = stamp_runtime_state_sync(
                                fallback_state,
                                source="remote_cache",
                                loaded=False,
                                error=fallback_error,
                                fetched_at=fallback_state.get("stateFetchedAt") or now_local_iso(),
                                age_seconds=time.time() - cached_ts if cached_ts else 0.0,
                                stale=True,
                            )
                        else:
                            fallback_state = sanitize_only_dip_swing_runtime_state(
                                AUTOMATION_ENGINE.snapshot(),
                                AUTOMATION_CONFIG.current(),
                            )
                            if (local_journal.get("orders") or local_journal.get("summary")):
                                fallback_state["executionJournal"] = local_journal
                            fallback_state["equityDisplay"] = build_equity_display(
                                None,
                                fallback_state,
                                fallback_state.get("executionJournal") or local_journal,
                            )
                            warnings = list((fallback_state.get("analysis") or {}).get("warnings") or [])
                            warnings.append(fallback_error)
                            fallback_state.setdefault("analysis", {})["warnings"] = warnings[-6:]
                            fallback_state = stamp_runtime_state_sync(
                                fallback_state,
                                source="local_fallback",
                                loaded=False,
                                error=fallback_error,
                                fetched_at=now_local_iso(),
                                age_seconds=0.0,
                                stale=True,
                            )
                        json_response(
                            self,
                            {
                                "ok": True,
                                "state": fallback_state,
                                "remoteStateLoaded": False,
                                "remoteStateSource": fallback_state.get("stateSource") or "local_fallback",
                                "remoteStateError": fallback_error,
                            },
                        )
                elif path == "/api/orders/recent":
                    limit = parse_recent_order_limit(query)
                    response = remote_gateway_request(config, "GET", self.path)
                    payload = response.json()
                    orders = payload.get("orders") or []
                    if isinstance(orders, list):
                        if live_only:
                            orders = [item for item in orders if not is_paper_execution_order(item)]
                        inst_type = (query.get("instType") or [""])[0]
                        cached_orders = get_stored_local_orders(
                            inst_type,
                            limit=MAX_RECENT_ORDER_LIMIT,
                            live_only=live_only,
                        )
                        stream_orders = PRIVATE_ORDER_STREAM.get_recent_orders(inst_type, limit=MAX_RECENT_ORDER_LIMIT)
                        if live_only:
                            stream_orders = [item for item in stream_orders if not is_paper_execution_order(item)]
                        merged_orders = merge_order_feeds(orders, stream_orders, cached_orders, limit=MAX_RECENT_ORDER_LIMIT)
                        persist_local_orders(merged_orders, source="remote_proxy", live_only=live_only)
                        journal = get_execution_journal_snapshot(inst_type, limit=limit, live_only=live_only)
                        payload["orders"] = journal.get("orders") or backfill_paper_execution_metrics(merged_orders[:limit])
                        payload["journal"] = journal.get("summary") or {}
                        payload["symbols"] = journal.get("symbols") or payload.get("symbols") or []
                        payload["lastSource"] = journal.get("lastSource") or payload.get("lastSource") or payload.get("source") or "remote_proxy"
                        payload["lastReconciledAt"] = journal.get("lastReconciledAt") or payload.get("lastReconciledAt") or now_local_iso()
                    json_response(self, payload, status=response.status_code)
                elif path == "/api/account/overview":
                    response = remote_gateway_request(config, "GET", self.path)
                    payload = response.json() if response.content else {}
                    remote_runtime_state: dict[str, Any] = {}
                    try:
                        runtime_response = remote_gateway_request(
                            config,
                            "GET",
                            "/api/automation/state",
                            timeout=REMOTE_AUTOMATION_STATE_TIMEOUT,
                        )
                        runtime_payload = runtime_response.json() if runtime_response.content else {}
                        remote_automation = load_remote_automation_config_for_proxy(config)
                        if isinstance(runtime_payload.get("state"), dict):
                            remote_runtime_state = enrich_remote_dip_swing_runtime_state(
                                runtime_payload.get("state") or {},
                                remote_automation,
                                CONFIG.current(),
                            )
                    except Exception:
                        cached_runtime_state, _cached_ts = load_cached_remote_automation_state(config)
                        remote_runtime_state = cached_runtime_state or {}
                    payload["equityDisplay"] = build_equity_display(
                        payload.get("summary") or {},
                        remote_runtime_state,
                        get_execution_journal_snapshot(
                            limit=DEFAULT_RECENT_ORDER_LIMIT,
                            live_only=live_only,
                        ),
                    )
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
                    payload = response.json() if response.content else {}
                    normalized = normalize_remote_automation_config_payload(payload, AUTOMATION_CONFIG.current())
                    json_response(
                        self,
                        {
                            "ok": bool(payload.get("ok", response.ok)),
                            "config": normalized,
                            "remoteConfigLoaded": True,
                        },
                        status=response.status_code,
                    )
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
            json_response(
                self,
                {
                    "ok": True,
                    "config": ensure_automation_permissions_match_environment(CONFIG.current()),
                },
            )
            return

        if path == "/api/automation/state":
            state = sanitize_only_dip_swing_runtime_state(AUTOMATION_ENGINE.snapshot(), AUTOMATION_CONFIG.current())
            state["executionJournal"] = get_execution_journal_snapshot(
                limit=DEFAULT_RECENT_ORDER_LIMIT,
                live_only=live_only,
            )
            state["equityDisplay"] = build_equity_display(
                None,
                state,
                state.get("executionJournal") or {},
            )
            state = stamp_runtime_state_sync(
                state,
                source="local_live",
                loaded=True,
                fetched_at=now_local_iso(),
                age_seconds=0.0,
                stale=False,
            )
            json_response(self, {"ok": True, "state": state})
            return

        if path == "/api/focus-snapshot":
            payload: dict[str, Any] = {
                "ok": True,
                "automationState": sanitize_only_dip_swing_runtime_state(
                    AUTOMATION_ENGINE.snapshot(),
                    AUTOMATION_CONFIG.current(),
                ),
                "executionJournal": get_execution_journal_snapshot(
                    limit=DEFAULT_RECENT_ORDER_LIMIT,
                    live_only=live_only,
                ),
                "timestamp": int(time.time()),
            }

            valid, message = validate_config(config)
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

            jobs = {
                "account": load_account,
            }
            with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
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
                        "equityDisplay": build_equity_display(
                            snapshot.get("summary") or {},
                            AUTOMATION_STATE.current(),
                            get_execution_journal_snapshot(
                                limit=DEFAULT_RECENT_ORDER_LIMIT,
                                live_only=live_only,
                            ),
                        ),
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
            limit = parse_recent_order_limit(query)
            cached_journal = get_execution_journal_snapshot(inst_type, limit=limit, live_only=live_only)
            cached_orders = cached_journal.get("orders") or []
            stream_orders = PRIVATE_ORDER_STREAM.get_recent_orders(inst_type, limit=limit)
            if live_only:
                stream_orders = [item for item in stream_orders if not is_paper_execution_order(item)]
            merged_live_orders = merge_order_feeds(stream_orders, cached_orders, limit=limit)
            has_private_credentials = live_only
            if merged_live_orders and not has_private_credentials:
                json_response(
                    self,
                    {
                        "ok": True,
                        "orders": merged_live_orders,
                        "source": "private_ws" if stream_orders else "local_cache",
                        "journal": cached_journal.get("summary") or {},
                        "symbols": cached_journal.get("symbols") or [],
                        "lastReconciledAt": cached_journal.get("lastReconciledAt") or "",
                        "lastSource": cached_journal.get("lastSource") or "",
                        "stream": PRIVATE_ORDER_STREAM.snapshot(),
                    },
                )
                return
            try:
                ensure_live_route_ready(config, force=False)
                client = OkxClient(config)
                if inst_type:
                    result = client.get_recent_orders(inst_type)
                    fills_result = client.get_recent_fills(inst_type, limit=100)
                    merged_orders = merge_order_feeds(result.get("data", []), stream_orders, cached_orders, limit=limit)
                    merged_orders = enrich_execution_orders_with_fills(merged_orders, fills_result.get("data", []))
                    persist_local_orders(
                        merged_orders,
                        source="rest_multi" if (stream_orders or cached_orders) else "rest",
                        live_only=live_only,
                    )
                    journal = get_execution_journal_snapshot(inst_type, limit=limit, live_only=live_only)
                    json_response(
                        self,
                        {
                            "ok": True,
                            "orders": journal.get("orders") or merged_orders,
                            "source": "rest_multi" if (stream_orders or cached_orders) else "rest",
                            "journal": journal.get("summary") or {},
                            "symbols": journal.get("symbols") or [],
                            "lastReconciledAt": journal.get("lastReconciledAt") or "",
                            "lastSource": journal.get("lastSource") or "",
                            "stream": PRIVATE_ORDER_STREAM.snapshot(),
                        },
                    )
                    return

                merged_orders: list[dict[str, Any]] = []
                recent_fills: list[dict[str, Any]] = []
                errors: list[str] = []
                for fallback_type in ("SPOT", "SWAP"):
                    try:
                        fallback_result = client.get_recent_orders(fallback_type)
                        merged_orders.extend(fallback_result.get("data", []))
                    except Exception as fallback_exc:
                        errors.append(f"{fallback_type}: {fallback_exc}")
                    try:
                        fills_result = client.get_recent_fills(fallback_type, limit=100)
                        recent_fills.extend(fills_result.get("data", []))
                    except Exception as fill_exc:
                        errors.append(f"{fallback_type} fills: {fill_exc}")

                if not merged_orders and errors:
                    raise OkxApiError("; ".join(errors))

                merged_orders = merge_order_feeds(merged_orders, stream_orders, cached_orders, limit=limit)
                merged_orders = enrich_execution_orders_with_fills(merged_orders, recent_fills)
                persist_local_orders(merged_orders, source="rest_multi", live_only=live_only)
                journal = get_execution_journal_snapshot(inst_type, limit=limit, live_only=live_only)
                json_response(
                    self,
                    {
                        "ok": True,
                        "orders": journal.get("orders") or merged_orders[:20],
                        "source": "rest_multi",
                        "journal": journal.get("summary") or {},
                        "symbols": journal.get("symbols") or [],
                        "lastReconciledAt": journal.get("lastReconciledAt") or "",
                        "lastSource": journal.get("lastSource") or "",
                        "stream": PRIVATE_ORDER_STREAM.snapshot(),
                    },
                )
            except Exception as exc:
                if merged_live_orders:
                    json_response(
                        self,
                        {
                            "ok": True,
                            "orders": merged_live_orders,
                            "source": "private_ws" if stream_orders else "local_cache",
                            "journal": cached_journal.get("summary") or {},
                            "symbols": cached_journal.get("symbols") or [],
                            "lastReconciledAt": cached_journal.get("lastReconciledAt") or "",
                            "lastSource": cached_journal.get("lastSource") or "",
                            "stream": PRIVATE_ORDER_STREAM.snapshot(),
                        },
                    )
                    return
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
        if path.startswith("/api/miner/"):
            error_response(self, "矿机功能已移除", status=410)
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
            previous_effective = CONFIG.current()
            if target_mode == "remote":
                local_runtime = build_local_runtime_config(config, payload)
                proxy_config = build_proxy_runtime_config(config, payload)
                valid, message = validate_config(local_runtime)
                if not valid:
                    error_response(self, message, status=400)
                    return
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
                    if not response.ok or remote_data.get("ok") is False:
                        remote_error = remote_data.get("error") or f"远端执行节点返回 {response.status_code}"
                        raise OkxApiError(remote_error)
                    CONFIG.save(local_runtime, persist=persist)
                    env_changed = trading_environment_changed(previous_effective, local_runtime)
                    if env_changed:
                        reset_automation_live_permissions(reason="交易环境已切换，已自动停止策略并锁回实盘权限")
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
                            "automationReset": env_changed,
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
            env_changed = trading_environment_changed(previous_effective, CONFIG.current())
            if env_changed:
                reset_automation_live_permissions(reason="交易环境已切换，已自动停止策略并锁回实盘权限")
            reset_focus_cache("account", "orders")
            PRIVATE_ORDER_STREAM.mark_dirty()
            json_response(
                self,
                {"ok": True, "config": CONFIG.redacted(), "persisted": persist, "automationReset": env_changed},
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
            normalized = deep_merge(
                ensure_automation_permissions_match_environment(CONFIG.current()),
                normalized,
            )
            if bool(CONFIG.current().get("simulated")):
                normalized["allowLiveManualOrders"] = False
                normalized["allowLiveTrading"] = False
                normalized["allowLiveAutostart"] = False
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
                automation = AUTOMATION_CONFIG.current()
                self._guard_live_manual_order(config, automation)
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


def main() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    ensure_private_permissions(DATA_DIR, is_dir=True)
    maybe_autostart()
    startup_config = CONFIG.current()
    if should_run_local_okx_background_tasks(startup_config):
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
