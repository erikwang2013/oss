#!/usr/bin/env bash
# M6 切片 4/5：桶复制端到端（两个真实 oss-server 实例）。
# 覆盖验收④⑤：同 ETag + x-amz-replication-status PENDING→COMPLETED；目标停机积压 → 恢复追平；
# CopyObject / DeleteMarkerReplication；REPLICA 摄入头。
#
# 前置：`cargo build -p oss-server`；aws-cli；curl；python3。
# 用法：OSS_BIN=target/debug/oss-server tests/e2e_replication.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
[ -x "$BIN" ] || { echo "缺少 oss-server 二进制（先 cargo build -p oss-server，或设 OSS_BIN）"; exit 2; }
command -v aws >/dev/null || { echo "需要 aws-cli"; exit 2; }

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()
PY
}

SRC_HTTP=$(free_port); DST_HTTP=$(free_port)
W=$(mktemp -d)
PIDS=()
cleanup() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
  rm -rf "$W"
}
trap cleanup EXIT

wait_http() {
  for _ in $(seq 1 60); do
    s=$(curl -s -o /dev/null -w "%{http_code}" --max-time 1 "http://127.0.0.1:$1/health/live" 2>/dev/null || true)
    [ "$s" = "200" ] && return 0
    sleep 1
  done
  return 1
}

mkdir -p "$W/dst/d0" "$W/src/d0" "$W/dst/staging" "$W/src/staging"
cat > "$W/dst/oss.toml" <<EOF
listen = "127.0.0.1:$DST_HTTP"
data_dir = "$W/dst/d0"
staging_dir = "$W/dst/staging"
database_url = "sqlite://$W/dst/oss.db?mode=rwc"
jwt_secret = "dev-only-change-me-0123456789abcdef"
max_put_bytes = 268435456
groups = []
EOF

start_dst() {
  (cd "$W/dst" && OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw exec "$BIN" serve >>"$W/dst/server.log" 2>&1) &
  DST_PID=$!
  PIDS+=("$DST_PID")
  wait_http "$DST_HTTP"
}

echo "== 0. 起目标实例（127.0.0.1:$DST_HTTP）=="
start_dst || { echo "FAIL: 目标未就绪"; tail -20 "$W/dst/server.log"; exit 1; }
DST="http://127.0.0.1:$DST_HTTP"

login() {
  curl -s -X POST "$1/api/v1/login" -H 'content-type: application/json' \
    -d '{"username":"erik","password":"pw"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])'
}
mint() {
  curl -s -X POST "$1/api/v1/access-keys" -H "authorization: Bearer $2" \
    -H 'content-type: application/json' -d '{}'
}

DT=$(login "$DST")
[ -n "$DT" ] || { echo "FAIL: 目标登录"; exit 1; }
DKEYS=$(mint "$DST" "$DT")
DST_AK=$(printf '%s' "$DKEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_key"])')
DST_SK=$(printf '%s' "$DKEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin)["secret_key"])')
[ -n "$DST_AK" ] || { echo "FAIL: 目标铸键: $DKEYS"; exit 1; }

cat > "$W/src/oss.toml" <<EOF
listen = "127.0.0.1:$SRC_HTTP"
data_dir = "$W/src/d0"
staging_dir = "$W/src/staging"
database_url = "sqlite://$W/src/oss.db?mode=rwc"
jwt_secret = "dev-only-change-me-0123456789abcdef"
max_put_bytes = 268435456
groups = []

[[remotes]]
id = "r1"
endpoint = "http://127.0.0.1:$DST_HTTP"
access_key = "$DST_AK"
secret_key = "$DST_SK"
region = "us-east-1"
EOF

echo "== 1. 起源实例（127.0.0.1:$SRC_HTTP，remote r1 → 目标）=="
(cd "$W/src" && OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw exec "$BIN" serve >>"$W/src/server.log" 2>&1) &
PIDS+=($!)
wait_http "$SRC_HTTP" || { echo "FAIL: 源未就绪"; tail -20 "$W/src/server.log"; exit 1; }
SRC="http://127.0.0.1:$SRC_HTTP"
ST=$(login "$SRC")
SKEYS=$(mint "$SRC" "$ST")
SRC_AK=$(printf '%s' "$SKEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_key"])')
SRC_SK=$(printf '%s' "$SKEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin)["secret_key"])')
[ -n "$SRC_AK" ] || { echo "FAIL: 源铸键: $SKEYS"; exit 1; }

src_aws() { AWS_ACCESS_KEY_ID="$SRC_AK" AWS_SECRET_ACCESS_KEY="$SRC_SK" AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "$SRC" "$@"; }
dst_aws() { AWS_ACCESS_KEY_ID="$DST_AK" AWS_SECRET_ACCESS_KEY="$DST_SK" AWS_DEFAULT_REGION=us-east-1 \
  aws --endpoint-url "$DST" "$@"; }

echo "== 2. 建桶（源版本控制 Enabled）=="
dst_aws s3 mb s3://dstbucket >/dev/null
src_aws s3 mb s3://srcbucket >/dev/null
# 复制两侧都需版本控制（目标侧 marker 语义依赖）
dst_aws s3api put-bucket-versioning --bucket dstbucket \
  --versioning-configuration Status=Enabled
src_aws s3api put-bucket-versioning --bucket srcbucket \
  --versioning-configuration Status=Enabled

echo "== 3. PutBucketReplication（Destination=r1/dstbucket，前缀 logs/，marker 开启）=="
cat > "$W/repl.json" <<'EOF'
{
  "Role": "arn:oss:iam::e2e:role/replication",
  "Rules": [
    {
      "ID": "e2e",
      "Status": "Enabled",
      "Filter": {"Prefix": "logs/"},
      "Destination": {"Bucket": "r1/dstbucket"},
      "DeleteMarkerReplication": {"Status": "Enabled"}
    }
  ]
}
EOF
src_aws s3api put-bucket-replication --bucket srcbucket --replication-configuration "file://$W/repl.json" \
  || { echo "FAIL: put-bucket-replication"; exit 1; }
GOT=$(src_aws s3api get-bucket-replication --bucket srcbucket --query 'ReplicationConfiguration.Rules[0].Destination.Bucket' --output text)
[ "$GOT" = "r1/dstbucket" ] || { echo "FAIL: 规则回读 $GOT"; exit 1; }

echo "== 4. PUT 复制（同 ETag + PENDING→COMPLETED + REPLICA 头）=="
head -c 1048576 /dev/urandom > "$W/a.bin"
src_aws s3 cp "$W/a.bin" s3://srcbucket/logs/a.bin >/dev/null
SRC_ETAG=$(src_aws s3api head-object --bucket srcbucket --key logs/a.bin --query ETag --output text)
for _ in $(seq 1 60); do
  ST_RS=$(src_aws s3api head-object --bucket srcbucket --key logs/a.bin --query ReplicationStatus --output text 2>/dev/null || true)
  [ "$ST_RS" = "COMPLETED" ] && break
  sleep 1
done
[ "$ST_RS" = "COMPLETED" ] || { echo "FAIL: 源状态 $ST_RS（期望 COMPLETED）"; exit 1; }
DST_ETAG=$(dst_aws s3api head-object --bucket dstbucket --key logs/a.bin --query ETag --output text)
[ "$DST_ETAG" = "$SRC_ETAG" ] || { echo "FAIL: ETag 不一致 src=$SRC_ETAG dst=$DST_ETAG"; exit 1; }
DST_RS=$(dst_aws s3api head-object --bucket dstbucket --key logs/a.bin --query ReplicationStatus --output text)
[ "$DST_RS" = "REPLICA" ] || { echo "FAIL: 目标状态 $DST_RS（期望 REPLICA）"; exit 1; }
dst_aws s3 cp s3://dstbucket/logs/a.bin "$W/a.back" >/dev/null
cmp "$W/a.bin" "$W/a.back" || { echo "FAIL: 内容不一致"; exit 1; }

echo "== 5. CopyObject 复制 =="
src_aws s3api copy-object --bucket srcbucket --key logs/copy.bin --copy-source srcbucket/logs/a.bin >/dev/null
for _ in $(seq 1 60); do
  if dst_aws s3api head-object --bucket dstbucket --key logs/copy.bin >/dev/null 2>&1; then break; fi
  sleep 1
done
dst_aws s3api head-object --bucket dstbucket --key logs/copy.bin >/dev/null || { echo "FAIL: copy 未复制"; exit 1; }

echo "== 6. DeleteMarkerReplication =="
src_aws s3api delete-object --bucket srcbucket --key logs/a.bin >/dev/null
for _ in $(seq 1 60); do
  DM=$(dst_aws s3api list-object-versions --bucket dstbucket --prefix logs/a.bin --query 'length(DeleteMarkers)' --output text 2>/dev/null || echo 0)
  [ "$DM" = "1" ] && break
  sleep 1
done
[ "$DM" = "1" ] || { echo "FAIL: 目标 DeleteMarkers=$DM"; exit 1; }

echo "== 7. 目标停机积压 → 恢复追平（验收⑤）=="
kill "$DST_PID" 2>/dev/null
sleep 1
src_aws s3 cp "$W/a.bin" s3://srcbucket/logs/backlog.bin >/dev/null
sleep 3
RS=$(src_aws s3api head-object --bucket srcbucket --key logs/backlog.bin --query ReplicationStatus --output text)
[ "$RS" = "PENDING" ] || { echo "FAIL: 停机期状态 $RS（期望 PENDING）"; exit 1; }
start_dst || { echo "FAIL: 目标重启"; exit 1; }
for _ in $(seq 1 90); do
  RS=$(src_aws s3api head-object --bucket srcbucket --key logs/backlog.bin --query ReplicationStatus --output text 2>/dev/null || true)
  [ "$RS" = "COMPLETED" ] && break
  sleep 1
done
[ "$RS" = "COMPLETED" ] || { echo "FAIL: 恢复后状态 $RS"; exit 1; }
dst_aws s3 cp s3://dstbucket/logs/backlog.bin "$W/backlog.back" >/dev/null
cmp "$W/a.bin" "$W/backlog.back" || { echo "FAIL: 积压追平内容不一致"; exit 1; }

echo "== 8. 边界：未开版本控制的桶拒绝 PutBucketReplication =="
dst_aws s3 mb s3://plainbucket >/dev/null 2>&1 || true
src_aws s3 mb s3://plainbucket >/dev/null 2>&1 || true
if src_aws s3api put-bucket-replication --bucket plainbucket --replication-configuration "file://$W/repl.json" >/dev/null 2>&1; then
  echo "FAIL: 未开版本控制竟接受复制配置"; exit 1
fi

echo ""
echo "复制 e2e 全部通过（PUT/Copy/marker/积压恢复；同 ETag + PENDING→COMPLETED + REPLICA）"
