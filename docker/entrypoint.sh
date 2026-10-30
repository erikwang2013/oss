#!/bin/sh
# oss 容器入口：/secrets 是**宿主 bind 目录**（docker 自动建时属 root:root）。root 起步先把
# 目录交给运行用户并收紧到 0700，再 gosu 降权到 oss（服务进程不以 root 跑）。
# 密钥文件（OSS_SECRETS_FILE，如 /secrets/generated.env）由服务端读写与落盘：
# 首启生成（0600，仅容器内 oss 可读），重启/集群多节点复用同一份（flock 串行化，无并发
# 首启竞态）。宿主查看用 `docker compose exec ... cat` 或 `sudo cat`（README「Docker」）。
set -e
if [ "$(id -u)" = 0 ] && [ -d /secrets ]; then
    chown -R oss:oss /secrets
    chmod 700 /secrets
fi
exec gosu oss oss-server "$@"
