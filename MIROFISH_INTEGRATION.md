# MiroFish 集成说明

MiroFish 现在不是“另开一个项目”，而是已经作为当前 OKX Local App 里的一个内嵌模块运行。

源码位置：

- `/Users/cc/Documents/New project/okx-local-app/vendor/MiroFish`

当前集成方式：

- 前端入口直接挂在当前桌面：`/mirofish/`
- 后端代理入口：`/mirofish-api/*`
- 桌面导航里有 `仿真推演`
- 默认不再依赖外部 `LLM_API_KEY / ZEP_API_KEY`
- 需要 LLM 的部分默认改用 `Codex CLI`
- 图谱读取默认走本地 `local graph`，不再强制依赖 Zep

## 默认运行模式

默认 `.env` 里会写入：

```env
MIROFISH_LLM_BACKEND=codex
MIROFISH_GRAPH_BACKEND=local
MIROFISH_CODEX_COMMAND=codex
MIROFISH_CODEX_TIMEOUT_SECONDS=240
```

这意味着：

- 不配置 `LLM_API_KEY` 也能跑
- 不配置 `ZEP_API_KEY` 也能跑
- 只有当你明确切回 OpenAI / Zep 后端时，才需要再补这些 key

## 一次性初始化

```bash
cd /Users/cc/Documents/New\ project/okx-local-app
./scripts/mirofish-setup.sh
```

这一步会：

- 自动选择可用的 Python 3.12 / 3.11 / 3.x
- 初始化 `vendor/MiroFish/.env`
- 把运行模式默认改成 `codex + local`
- 安装前端依赖
- 创建后端 `.venv`

## 在当前桌面里使用

### 方式 1：直接用 OKX Local App

启动当前桌面 App 后：

- 左侧导航点击 `仿真推演`
- 先点 `初始化`
- 再点 `启动`
- 下方 iframe 会直接内嵌当前 MiroFish 工作区

关键接口：

- 状态：`http://127.0.0.1:8765/api/mirofish/status`
- 页面：`http://127.0.0.1:8765/mirofish/`

### 方式 2：开发时单独跑上游 dev server

```bash
cd /Users/cc/Documents/New\ project/okx-local-app
./scripts/mirofish-dev.sh
```

默认地址：

- 前端：`http://127.0.0.1:3000`
- 后端：`http://127.0.0.1:5001`
- 当前桌面内嵌入口：`http://127.0.0.1:8765/mirofish/`

这个脚本现在也会自动补齐：

- `UV_PYTHON`
- `PYO3_USE_ABI3_FORWARD_COMPATIBILITY`
- `MIROFISH_LLM_BACKEND=codex`
- `MIROFISH_GRAPH_BACKEND=local`

## 当前接入点

- OKX Local App 后端集成：
  - `/Users/cc/Documents/New project/okx-local-app/server.py`
- 桌面壳运行时同步：
  - `/Users/cc/Documents/New project/okx-local-app/native-mac/main.swift`
- 初始化脚本：
  - `/Users/cc/Documents/New project/okx-local-app/scripts/mirofish-setup.sh`
- 开发脚本：
  - `/Users/cc/Documents/New project/okx-local-app/scripts/mirofish-dev.sh`

## 什么时候才需要 API Key

只有这两种情况才需要：

- 你明确把 `MIROFISH_LLM_BACKEND` 改成非 `codex`
- 你明确把 `MIROFISH_GRAPH_BACKEND` 改成非 `local`

否则：

- `LLM_API_KEY` 可为空
- `ZEP_API_KEY` 可为空

## 备注

- 这是源码 vendoring，不是 submodule。
- 上游许可证保持为 `AGPL-3.0`。
- 桌面壳会把 `server.py / static / scripts / vendor` 一起同步到 `runtime-app`，确保运行态和当前项目源码一致。
