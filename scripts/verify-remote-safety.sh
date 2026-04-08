#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${1:-http://127.0.0.1:18765}"
EXPECT_ENV="${2:-}"
TOKEN="${OKX_DESK_GATEWAY_TOKEN:-${OKX_REMOTE_GATEWAY_TOKEN:-}}"

if [[ -z "$TOKEN" ]]; then
  echo "缺少远端节点令牌。"
  echo "用法：OKX_DESK_GATEWAY_TOKEN=your-token $0 [base-url] [demo|live]"
  exit 1
fi

BASE_URL="${BASE_URL%/}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

fetch_json() {
  local path="$1"
  local target="$2"
  curl -fsS \
    -H "X-OKX-Desk-Gateway-Token: $TOKEN" \
    "$BASE_URL$path" >"$target"
}

echo "验证远端节点：$BASE_URL"
[[ -n "$EXPECT_ENV" ]] && echo "期望环境：$EXPECT_ENV"

fetch_json "/api/ping" "$TMP_DIR/ping.json"
fetch_json "/api/health" "$TMP_DIR/health.json"
fetch_json "/api/local-config" "$TMP_DIR/local-config.json"
fetch_json "/api/automation/config" "$TMP_DIR/automation-config.json"
fetch_json "/api/account/overview" "$TMP_DIR/account-overview.json"

python3 - "$EXPECT_ENV" "$TMP_DIR" "$BASE_URL" <<'PY'
import json
import sys
from decimal import Decimal
from pathlib import Path


def load(base: Path, name: str) -> dict:
    return json.loads((base / name).read_text(encoding="utf-8"))


def as_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return bool(value)


def fmt_decimal(raw) -> str:
    try:
        return f"{Decimal(str(raw)):.8f}".rstrip("0").rstrip(".")
    except Exception:
        return str(raw)


expect_env = sys.argv[1].strip().lower()
base = Path(sys.argv[2])
base_url = sys.argv[3]

ping = load(base, "ping.json")
health = load(base, "health.json")
local_config = (load(base, "local-config.json") or {}).get("config") or {}
automation = (load(base, "automation-config.json") or {}).get("config") or {}
account = load(base, "account-overview.json")
summary = account.get("summary") or {}
route = (health or {}).get("okxRoute") or {}
private_stream = (health or {}).get("privateOrderStream") or {}
funding_warning = str(account.get("fundingWarning") or "")

errors = []
warnings = []

if not ping.get("ok"):
    errors.append("ping 返回 ok=false")
if not health.get("ok"):
    errors.append("health 返回 ok=false")

env_preset = str(local_config.get("envPreset") or "")
simulated = as_bool(local_config.get("simulated"))
execution_mode = str(local_config.get("executionMode") or "")

if execution_mode != "local":
    warnings.append(f"当前节点 executionMode={execution_mode}，远端执行节点直连时通常应为 local")

if expect_env in {"demo", "sim", "simulated", "paper"}:
    if "demo" not in env_preset or not simulated:
        errors.append(f"当前环境不是模拟盘：envPreset={env_preset}, simulated={simulated}")
elif expect_env in {"live", "real", "prod"}:
    if "live" not in env_preset or simulated:
        errors.append(f"当前环境不是实盘：envPreset={env_preset}, simulated={simulated}")

if route and route.get("healthy") is False:
    errors.append(f"链路不健康：{route.get('summary') or route.get('detail') or 'unknown'}")

display_total = summary.get("displayTotalEq") or summary.get("totalEq") or "0"
display_source = summary.get("displaySource") or ""
display_breakdown = summary.get("displayBreakdown") or ""
funding_total = summary.get("fundingTotalEq") or "0"
valuation_total = summary.get("valuationTotalEq") or "0"

if not display_total:
    errors.append("账户摘要缺少 displayTotalEq/totalEq")

if simulated and Decimal(str(valuation_total or "0")) > 0 and display_source != "总资产估值":
    warnings.append(f"模拟盘估值口径未优先显示：displaySource={display_source}")

if simulated and funding_warning:
    errors.append(f"模拟盘资金/估值接口异常：{funding_warning}")

if simulated:
    if automation.get("allowLiveTrading"):
        errors.append("模拟盘下 allowLiveTrading 仍为开启")
    if automation.get("allowLiveManualOrders"):
        errors.append("模拟盘下 allowLiveManualOrders 仍为开启")
    if automation.get("allowLiveAutostart"):
        errors.append("模拟盘下 allowLiveAutostart 仍为开启")
else:
    if automation.get("allowLiveTrading"):
        warnings.append("实盘自动交易当前已解锁")
    if automation.get("allowLiveManualOrders"):
        warnings.append("实盘手动下单当前已解锁")
    if automation.get("allowLiveAutostart"):
        warnings.append("实盘自动启动当前已解锁")

print(f"节点: {base_url}")
print(f"环境: {env_preset} | simulated={simulated} | executionMode={execution_mode}")
print(f"链路: {route.get('summary') or '未提供'}")
print(f"细节: {route.get('detail') or '未提供'}")
print(f"私有 WS: connected={private_stream.get('connected')} orderCount={private_stream.get('orderCount')}")
print(f"显示总额: {fmt_decimal(display_total)}")
print(f"显示来源: {display_source or '未提供'}")
print(f"显示拆分: {display_breakdown or '未提供'}")
print(f"估值总额: {fmt_decimal(valuation_total)}")
print(f"资金口径: {fmt_decimal(funding_total)}")
print(f"资金告警: {funding_warning or '无'}")
print(
    "策略开关: "
    f"autostart={automation.get('autostart')} "
    f"allowLiveManualOrders={automation.get('allowLiveManualOrders')} "
    f"allowLiveTrading={automation.get('allowLiveTrading')} "
    f"allowLiveAutostart={automation.get('allowLiveAutostart')}"
)

if warnings:
    print("\n警告:")
    for item in warnings:
        print(f"- {item}")

if errors:
    print("\n错误:")
    for item in errors:
        print(f"- {item}")
    raise SystemExit(1)

print("\n验证通过")
PY
