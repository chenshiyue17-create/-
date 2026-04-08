# ECS 47.87.68.74 快速落地

目标：

- 服务器跑实盘执行节点
- 本地 Mac 保留矿机和桌面控制台
- 尽量不影响服务器上其他项目

基于这台 `47.87.68.74` 当前情况：

- `80` 口上已经有别的项目
- `/okx-node` 现在没有挂到这套服务
- 最快可用方案是：直接对外放 `18765/tcp`

推荐默认值：

- 节点监听：`0.0.0.0:18765`
- 对外暴露：先走高位端口，后续再接 Nginx
- systemd 服务名：`okx-remote-node`
- 部署目录：`/opt/okx-local-app`

## 1. 服务器直接从 GitHub 拉代码

```bash
sudo mkdir -p /opt
cd /opt
sudo rm -rf /opt/okx-local-app
sudo git clone https://github.com/chenshiyue17-create/-.git /opt/okx-local-app
sudo chown -R ecs-assist-user:ecs-assist-user /opt/okx-local-app
cd /opt/okx-local-app
```

## 2. 一键检查冲突并安装

```bash
cd /opt/okx-local-app
chmod +x deploy/bootstrap-cloned-node.sh
./deploy/bootstrap-cloned-node.sh /opt/okx-local-app okx-remote-node 18765
```

## 3. 配环境

编辑：

```bash
sudo nano /etc/okx-remote-node.env
```

建议先填成：

```bash
OKX_LOCAL_APP_HOST=0.0.0.0
OKX_LOCAL_APP_PORT=18765
OKX_LOCAL_APP_DATA_DIR=/opt/okx-local-app/data-remote
OKX_DESK_GATEWAY_TOKEN=change-this-to-your-own-long-random-token
```

## 4. 启动并检查 systemd

```bash
sudo systemctl restart okx-remote-node
sudo systemctl status okx-remote-node --no-pager
curl -s http://127.0.0.1:18765/api/health
```

如果你之前手动写过 `/etc/systemd/system/okx-remote-node.service`，请确认里面没有把
`OKX_LOCAL_APP_HOST=127.0.0.1` 这类值写死，否则会覆盖 `/etc/okx-remote-node.env`。
最稳妥的做法是重新复制仓库里的最新 service 文件，再 `daemon-reload`：

```bash
sudo cp /opt/okx-local-app/deploy/systemd/okx-remote-node.service /etc/systemd/system/okx-remote-node.service
sudo systemctl daemon-reload
sudo systemctl restart okx-remote-node
```

别忘了同时在云控制台/安全组放开 `18765/tcp`。

外网验证：

```bash
curl -s http://47.87.68.74:18765/api/health
```

## 5A. 后续如果你想并到 Nginx，再挂路径共存

把下面模板合并到你现有站点配置里：

```bash
/opt/okx-local-app/deploy/nginx/okx-remote-node-path.conf.example
```

核心就是：

```nginx
location /okx-node/ {
    proxy_pass http://127.0.0.1:18765/;
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header Connection "";
    proxy_read_timeout 60s;
    proxy_send_timeout 60s;
}
```

改完后：

```bash
sudo nginx -t
sudo systemctl reload nginx
```

然后你本地桌面 App 的远端 URL 直接填：

```text
http://47.87.68.74/okx-node
```

或你的正式域名：

```text
https://your-domain/okx-node
```

## 5B. 当前最快方案：直接走高位端口

```text
http://47.87.68.74:18765
```

长期如果要和现有 Web 项目更深共存，再切去 Nginx。

## 6. 本地桌面 App 怎么填

在本地桌面 App 的“连接配置”里：

- `执行节点`：`远端执行节点`
- `远端节点 URL`：`http://47.87.68.74:18765` 或 `http://47.87.68.74/okx-node`
- `远端节点令牌`：和 `/etc/okx-remote-node.env` 里的 `OKX_DESK_GATEWAY_TOKEN` 一致

矿机配置不要动，仍然留在本地。

## 7. 远端节点保存真实交易凭据

远端节点第一次启动后，需要在服务器上的这套节点里保存真实：

- API Key
- Secret Key
- Passphrase
- Base URL
- 模拟 / 实盘

本地桌面控制台不会替远端保管这些交易密钥。
