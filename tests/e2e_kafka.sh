#!/usr/bin/env bash
# M6 切片 1：Kafka 真 broker e2e（redpanda 单节点 docker）。
# 三阶段验证验收 ①②：① broker 在线投递实收+字段断言；② broker 停机积压（outbox pending、
# 永不 dead）→ 恢复后全部补投、不丢不重。
# 前置：docker 可用、镜像 redpandadata/redpanda:v24.2.7 已就位（首次可自动 pull）。
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=$(python3 - <<'PY'
import socket
s=socket.socket(); s.bind(("127.0.0.1",0)); print(s.getsockname()[1]); s.close()
PY
)
NAME="oss-m6rp-$(date +%s)"
WORK=$(mktemp -d)
DB="$WORK/kafka-e2e.db"
TOPIC="oss-events-e2e-$RANDOM"

cleanup() {
  docker rm -f "$NAME" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "== 起 redpanda（容器 $NAME，127.0.0.1:$PORT）=="
docker run -d --name "$NAME" \
  -p "127.0.0.1:$PORT:9092" \
  redpandadata/redpanda:v24.2.7 \
  redpanda start --overprovisioned --smp 1 --memory 512M --reserve-memory 0M \
    --node-id 0 --check=false \
    --kafka-addr PLAINTEXT://0.0.0.0:9092 \
    --advertise-kafka-addr "PLAINTEXT://127.0.0.1:$PORT" >/dev/null

for i in $(seq 1 60); do
  if python3 - "$PORT" <<'PY'
import socket,sys
s=socket.socket(); s.settimeout(0.5)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); print("up")
except OSError:
    sys.exit(1)
PY
  then break; fi
  sleep 1
done
echo "   broker 就绪"

export OSS_KAFKA_E2E_BROKERS="127.0.0.1:$PORT"
export OSS_KAFKA_E2E_TOPIC="$TOPIC"
export OSS_KAFKA_E2E_DB="$DB"

echo "== 阶段 1：投递实收 + 字段断言（验收 ①）=="
cargo test -p oss-server --test kafka_e2e phase1_broker_up_delivers -- --nocapture \
  | grep -E "phase1|test result"

echo "== 阶段 2：停 broker → 事件积压（pending、永不 dead）=="
docker stop "$NAME" >/dev/null
cargo test -p oss-server --test kafka_e2e phase2_broker_down_buffers -- --nocapture \
  | grep -E "phase2|test result"

echo "== 阶段 3：起 broker → 全部补投、不丢不重（验收 ②）=="
docker start "$NAME" >/dev/null
for i in $(seq 1 60); do
  if python3 - "$PORT" <<'PY'
import socket,sys
s=socket.socket(); s.settimeout(0.5)
try:
    s.connect(("127.0.0.1", int(sys.argv[1]))); print("up")
except OSError:
    sys.exit(1)
PY
  then break; fi
  sleep 1
done
cargo test -p oss-server --test kafka_e2e phase3_recover_replays_without_dupes -- --nocapture \
  | grep -E "phase3|test result"

echo "Kafka e2e 全部通过（① 实收字段 / ② 停机积压→追平不丢不重）"
