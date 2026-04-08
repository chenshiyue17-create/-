# ECS 47.87.68.74 快速落地

目标：

- 服务器跑实盘执行节点
- 本地 Mac 保留矿机和桌面控制台
- 尽量不影响服务器上其他项目

推荐默认值：

- 节点监听：`127.0.0.1:18765`
- 对外暴露：通过现有 Nginx 挂路径 `/okx-node/`
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
OKX_LOCAL_APP_HOST=127.0.0.1
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

## 5A. 如果服务器已经有 Nginx，建议挂路径共存

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

## 5B. 如果你暂时没有 Nginx，也可以临时直开高位端口

先把安全组放开 `18765/tcp`，然后本地桌面 App 填：

```text
http://47.87.68.74:18765
```

但这只是临时方案，长期还是建议挂到 Nginx 后面。

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
