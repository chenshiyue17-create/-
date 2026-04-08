# 远端执行节点

这套桌面 App 现在支持两种执行模式：

- `本机直连`：本机直接连 OKX
- `远端执行节点`：本机只做控制台，真正的账户查询、订单、自动化和订单回报走远端节点

当本机网络无法稳定访问 OKX，但远端 VPS/云主机可以时，建议使用远端执行节点。

## 远端节点启动

在远端机器上准备同一份项目目录后，使用：

```bash
cd /path/to/okx-local-app
chmod +x scripts/start-remote-node.sh
OKX_DESK_GATEWAY_TOKEN=your-token \
OKX_LOCAL_APP_HOST=127.0.0.1 \
OKX_LOCAL_APP_PORT=18765 \
OKX_LOCAL_APP_DATA_DIR=/path/to/okx-remote-data \
./scripts/start-remote-node.sh
```

如果远端节点要直接让本机桌面端访问，而不是只监听本机回环，可以改成：

```bash
OKX_LOCAL_APP_HOST=0.0.0.0
```

更推荐的做法是：

- 远端节点仍监听 `127.0.0.1`
- 服务器上再用 Nginx / Caddy / SSH 隧道把 `18765` 暴露给本机
- `OKX_DESK_GATEWAY_TOKEN` 必须保留

然后在远端节点本机先完成一次交易配置保存，让远端节点自己持有：

- `API Key`
- `Secret Key`
- `Passphrase`
- `Base URL`
- `模拟 / 实盘`

## systemd 常驻

仓库里已经带了模板：

```bash
deploy/systemd/okx-remote-node.service
```

典型部署方式：

```bash
sudo mkdir -p /opt/okx-local-app
sudo rsync -a ./ /opt/okx-local-app/
sudo cp /opt/okx-local-app/deploy/systemd/okx-remote-node.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now okx-remote-node
sudo systemctl status okx-remote-node
```

使用前先把 service 文件里的：

- `WorkingDirectory`
- `User/Group`

改成服务器上的真实值。

更省事的方式是直接在服务器目录里运行：

```bash
chmod +x deploy/install-remote-node.sh
sudo ./deploy/install-remote-node.sh /opt/okx-local-app okx-remote-node
sudo nano /etc/okx-remote-node.env
sudo systemctl restart okx-remote-node
```

部署前先检查和现有项目会不会冲突：

```bash
chmod +x deploy/check-server-collisions.sh
./deploy/check-server-collisions.sh 18765 okx-remote-node
```

## 打包上传

本机可以先生成远端部署包：

```bash
chmod +x scripts/package-remote-node.sh
./scripts/package-remote-node.sh
```

然后把输出的 `.tar.gz` 上传到服务器解压，再执行 `deploy/install-remote-node.sh`。

## 本机桌面端配置

在本机桌面 App 的“连接配置”中：

- `执行节点` 选 `远端执行节点`
- `远端节点 URL` 填 `https://your-host` 或 `http://your-host:18765`
- `远端节点令牌` 填和远端 `OKX_DESK_GATEWAY_TOKEN` 相同的值

如果你把远端节点挂在现有 Nginx 的路径下，也可以直接填带路径的 URL，例如：

- `https://your-host/okx-node`

本机保存后：

- `/api/health`
- `/api/account/overview`
- `/api/orders/recent`
- `/api/order/place`
- `/api/automation/*`
- `/api/market/*`
- `/api/focus-snapshot`

都会自动走远端节点。

## 当前限制

- 矿机相关接口仍然保留在本机
- 远端节点需要自己有可用的 OKX 网络链路
- 如果远端节点没保存交易凭据，本机控制台不会替它保管密钥
- 默认推荐回环监听 `127.0.0.1:18765`，再由 Nginx 反代
- 独立站点模板在 `deploy/nginx/okx-remote-node.conf.example`
- 现有站点挂路径模板在 `deploy/nginx/okx-remote-node-path.conf.example`
