#!/bin/sh
# oss 容器入口：/secrets 是**宿主 bind 目录**（docker 自动建时属 root:root，宿主用户还要能
# 删文件做轮换）——root 起步 chmod 777 放开，再 gosu 降权到 oss 运行（服务进程不以 root 跑）。
# 密钥文件（OSS_SECRETS_FILE，如 /secrets/generated.env）由服务端读写与落盘：
# 首启生成（0644，宿主可直读），重启/集群多节点复用同一份（flock 串行化，无并发首启竞态）。
set -e
if [ -d /secrets ]; then
    chmod 777 /secrets 2>/dev/null || true
fi
exec gosu oss oss-server "$@"
