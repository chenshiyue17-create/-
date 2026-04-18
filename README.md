# OKX Local App

本地现货+合约操作台，面向 OKX API v5。

功能范围：

- 本地保存或临时加载 API 配置
- 主站实盘 / 主站模拟盘 / US 实盘地址预设
- 自动量化策略面板，支持后台轮询、启动、停止、单轮执行
- 默认内置 EMA 趋势策略，支持现货与永续分别开关
- 内置 `BTC 乐透机` 预设：BTC 专用、更快节奏、更小仓位、更紧风控
- 日内最大回撤、止盈止损、冷却时间、每日最大订单数等风控
- 账户总览、余额、持仓、最近订单
- 主流币实时价格带 + 当前交易对实时 K 线
- WebSocket 实时推送，握手失败时自动退回本地轮询兜底
- 现货与合约下单

## 启动

```bash
cd /Users/cc/Documents/New\ project/okx-local-app
python3 server.py
```

默认地址：

`http://127.0.0.1:8765`

如果 `8765` 已被占用，服务会自动顺延到后续端口，例如 `8766`、`8767`。

## 说明

- 默认使用 OKX REST API v5
- 模拟盘通过请求头 `x-simulated-trading: 1` 控制
- 本地保存的配置、策略、状态文件会加密落盘，密钥绑定到当前 macOS 登录用户的 Keychain
- 自动量化配置与状态会分别保存到：
  - `data/automation-config.json`
  - `data/automation-state.json`
- 该文件已经加入 `.gitignore`
- 界面内置环境预设：
  - `OKX 主站模拟盘`
  - `OKX 主站实盘`
  - `OKX US 实盘`
  - `自定义`
- 非自定义环境会自动带出 REST / WebSocket 地址展示

## 自动量化默认逻辑

- 默认策略：`5m EMA 9 / 21`
- 现货：只做多，金叉买入，死叉或止盈止损卖出
- 永续：默认只做多，可切到顺势双向
- `BTC 乐透机`：默认切到 `BTC-USDT / BTC-USDT-SWAP`，使用更短周期和更小预算，适合先在模拟盘跑波动试验
- 启动时会尝试为永续设置杠杆，并可选切换到 `net_mode`
- 日内回撤、止盈止损、冷却秒数、每日最大订单数都会限制自动化发单

## 直接使用建议

1. 先把环境切到 `OKX 主站模拟盘`
2. 填入模拟盘 API Key / Secret / Passphrase 并保存
3. 在“自动量化”面板保留默认参数，先点“执行一轮”
4. 无报错后再点“启动自动化”
5. 如果要上实盘，必须显式勾选：
   - `允许实盘自动交易`
   - 如需开机自跑，再勾 `允许实盘自动启动`

## 集成模块：MiroFish

项目内已经集成了 MiroFish 源码，位置：

- `/Users/cc/Documents/New project/okx-local-app/vendor/MiroFish`

它现在不是单独跑的外部项目，而是已经内嵌到当前 OKX Local App 工作台里：

- 页面入口：`/mirofish/`
- 代理接口：`/mirofish-api/*`
- 桌面导航：`仿真推演`

默认模式已经切成：

- `MIROFISH_LLM_BACKEND=codex`
- `MIROFISH_GRAPH_BACKEND=local`

所以默认不需要再额外配置：

- `LLM_API_KEY`
- `ZEP_API_KEY`

一次性初始化：

```bash
cd /Users/cc/Documents/New\ project/okx-local-app
./scripts/mirofish-setup.sh
```

然后直接打开当前桌面，在导航里进入 `仿真推演`，点：

- `初始化`
- `启动`

如果你要调试上游 dev server，再执行：

```bash
./scripts/mirofish-dev.sh
```

开发模式默认地址：
- 前端：`http://127.0.0.1:3000`
- 后端：`http://127.0.0.1:5001`
- 当前桌面集成入口：`http://127.0.0.1:8765/mirofish/`

详细说明见：

- [MIROFISH_INTEGRATION.md](/Users/cc/Documents/New%20project/okx-local-app/MIROFISH_INTEGRATION.md)
