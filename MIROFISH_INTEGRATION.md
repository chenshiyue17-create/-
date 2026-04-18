# MiroFish 集成说明

本项目已内置 [MiroFish](https://github.com/666ghj/MiroFish) 源码，目录如下：

- `/Users/cc/Documents/New project/okx-local-app/vendor/MiroFish`

当前集成方式是“项目内独立模块”：
- 不改动现有 OKX Local App 主链
- 不与现有 `server.py` 混跑
- 保留 MiroFish 自己的前后端启动方式
- 通过本项目脚本统一安装和启动

## 快速使用

### 1. 安装依赖

```bash
cd /Users/cc/Documents/New\ project/okx-local-app
./scripts/mirofish-setup.sh
```

这一步会：
- 进入 `vendor/MiroFish`
- 首次自动从 `.env.example` 复制出 `.env`
- 执行 `npm run setup:all`

### 2. 配置环境变量

编辑：

- `/Users/cc/Documents/New project/okx-local-app/vendor/MiroFish/.env`

至少补齐：
- `LLM_API_KEY`
- `LLM_BASE_URL`
- `LLM_MODEL_NAME`
- `ZEP_API_KEY`

### 3. 启动

```bash
cd /Users/cc/Documents/New\ project/okx-local-app
./scripts/mirofish-dev.sh
```

默认地址：
- 前端：`http://127.0.0.1:3000`
- 后端：`http://127.0.0.1:5001`

## 目录说明

- MiroFish upstream 源码：`vendor/MiroFish`
- 安装脚本：`scripts/mirofish-setup.sh`
- 启动脚本：`scripts/mirofish-dev.sh`

## 说明

- 这次集成是“源码 vendoring”，不是 Git submodule。
- 上游仓库自带许可证：`AGPL-3.0`
- 上游自带 `.gitignore` 已保留，依赖和日志目录不会默认进入本项目版本管理。
