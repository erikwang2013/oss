#!/usr/bin/env bash
# M9 S4：Stretch EC 2×6 端到端（两个真实 oss-server，2 数据站点 × 6 盘，EC 8+4）。
#
# 覆盖：
#   1. 两站点启动（产品二进制起 [stretch] mesh gRPC，不经测试 harness）
#   2. 600 KiB / 4 KiB 写入；两站点 GET 内容一致；meta 镜像 12 份且逐字节相同
#   3. 每站点恰 6 个分片文件、每盘 1 片（placement v2）；inline=0 时小对象也全部分片
#   4. 停 B：GET/PUT 经 A 均非 2xx（Q_s=9 > 单站点 6 槽，fail-closed）
#   5. 恢复 B：GET 重新成功
#   6. 配置负例（inline≠0 / 盘数≠6 / peer_drives≠6 / EC 非 8+4 / 缺 [raft] /
#      缺 [site] / [stretch]+[cluster] / placement≠v2 / https / self 与 raft 同端口）
#
# 与 S9 的关系：本脚本不依赖 witness C（S7）；控制面 Raft 三投票人隔离测试在 S9 跑。
# 前置：cargo build -p oss-server；aws-cli；curl；python3。
# 用法：OSS_BIN=target/debug/oss-server tests/e2e_stretch.sh
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

A_HTTP=$(free_port); B_HTTP=$(free_port)
A_MESH=$(free_port); B_MESH=$(free_port)
A="http://127.0.0.1:$A_HTTP"; B="http://127.0.0.1:$B_HTTP"
W=$(mktemp -d)
MESH_SECRET=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
RAFT_TOKEN_A=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
RAFT_TOKEN_B=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
TOK_A=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_B=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
A_ADMIN="erik"; A_PW="pw-admin-a"
B_ADMIN="erik"; B_PW="pw-admin-b"
A_PID=""; B_PID=""
FAILS=0
ck() { if ! "$@"; then echo "FAIL: $*"; FAILS=$((FAILS+1)); fi; }

cleanup() {
  [ -n "$A_PID" ] && kill -9 "$A_PID" 2>/dev/null
  [ -n "$B_PID" ] && kill -9 "$B_PID" 2>/dev/null
  rm -rf "$W"
}
trap cleanup EXIT

wait_http() {
  for _ in $(seq 1 90); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$1/health/live" 2>/dev/null || true)" = "200" ] && return 0
    sleep 1
  done
  return 1
}
wait_tcp() { # port
  for _ in $(seq 1 90); do
    (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null && return 0
    sleep 1
  done
  return 1
}

# mk_cfg <dir> <node> <peer> <http> <mesh> <peer_mesh> <raft_id> <bootstrap>
mk_cfg() {
  local dir="$W/$1" node="$2" peer="$3" http="$4" mesh="$5" peermesh="$6" raft_id="$7" boot="$8"
  local token="$TOK_A" raft_token="$RAFT_TOKEN_A" region="r1"
  [ "$node" = "B" ] && { token="$TOK_B"; raft_token="$RAFT_TOKEN_B"; region="r2"; }
  mkdir -p "$dir"
  local dirs=()
  for i in 0 1 2 3 4 5; do mkdir -p "$dir/d$i"; dirs+=("\"$dir/d$i\""); done
  local dirs_toml
  dirs_toml=$(IFS=,; echo "${dirs[*]}")
  mkdir -p "$dir/staging"
  cat > "$dir/oss.toml" <<EOF
listen = "127.0.0.1:$http"
data_dir = "$dir/d0"
data_dirs = [$dirs_toml]
staging_dir = "$dir/staging"
database_url = "sqlite://$dir/oss.db?mode=rwc"
jwt_secret = "dev-only-change-me-0123456789abcdef"
max_put_bytes = 268435456
groups = []

[site]
name = "$node"
region = "$region"
priority = 10
token = "$token"

[stretch]
node_id = "$node"
self_addr = "127.0.0.1:$mesh"
peer_node_id = "$peer"
peer_addr = "127.0.0.1:$peermesh"
peer_drives = 6
placement = "v2"
mesh_secret = "$MESH_SECRET"
mesh_timeout_ms = 5000

[ec]
enabled = true
data = 8
parity = 4
inline_threshold = 0

[raft]
node_id = $raft_id
role = "voter"
bootstrap = $boot
advertise_endpoint = "http://127.0.0.1:$http"
cluster_token = "$raft_token"
EOF
}

start_a() {
  (cd "$W/a" && OSS_ADMIN_USER="$A_ADMIN" OSS_ADMIN_PASSWORD="$A_PW" exec "$BIN" serve >>"$W/a/server.log" 2>&1) &
  A_PID=$!
  wait_http "$A_HTTP" || { echo "FAIL: A 未就绪"; tail -40 "$W/a/server.log"; exit 1; }
}
start_b() {
  (cd "$W/b" && OSS_ADMIN_USER="$B_ADMIN" OSS_ADMIN_PASSWORD="$B_PW" exec "$BIN" serve >>"$W/b/server.log" 2>&1) &
  B_PID=$!
  wait_http "$B_HTTP" || { echo "FAIL: B 未就绪"; tail -40 "$W/b/server.log"; exit 1; }
}
stop_a() { [ -n "$A_PID" ] && kill -9 "$A_PID" 2>/dev/null; wait "$A_PID" 2>/dev/null; A_PID=""; }
stop_b() { [ -n "$B_PID" ] && kill -9 "$B_PID" 2>/dev/null; wait "$B_PID" 2>/dev/null; B_PID=""; }

login() { # base user pw
  curl -s -X POST "$1/api/v1/login" -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"$3\"}" |
    python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("token",""))'
}
mint() { # base jwt
  curl -s -X POST "$1/api/v1/access-keys" -H "authorization: Bearer $2" \
    -H 'content-type: application/json' -d '{}'
}
json_get() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get(sys.argv[1],""))' "$1"; }

mk_cfg a A B "$A_HTTP" "$A_MESH" "$B_MESH" 1 true
mk_cfg b B A "$B_HTTP" "$B_MESH" "$A_MESH" 2 false
start_a
start_b

echo "== 1. 两站点启动 + mesh 端口独立监听 =="
# /health/cluster：两节点均在册且各持 6 槽；mesh gRPC 端口可连
HC=$(curl -s "$A/health/cluster")
ck python3 -c "import json,sys; d=json.loads(sys.argv[1]); assert len(d['nodes'])==2 and all(n['up'] for n in d['nodes']), d; assert sorted(n['slots'] for n in d['nodes'])==[6,6], d" "$HC"
ck wait_tcp "$A_MESH"
ck wait_tcp "$B_MESH"

AT=$(login "$A" "$A_ADMIN" "$A_PW"); BT=$(login "$B" "$B_ADMIN" "$B_PW")
ck [ -n "$AT" ]
ck [ -n "$BT" ]
# /api/v1/cluster/status：stretch 视图（12 槽、两站点各 6 盘）
CS=$(curl -s "$A/api/v1/cluster/status" -H "authorization: Bearer $AT")
ck python3 -c "import json,sys; d=json.loads(sys.argv[1]); assert d['mode']=='cluster' and d['slot_count']==12, d; assert [n['drive_count'] for n in d['nodes']]==[6,6], d" "$CS"
AKEYS=$(mint "$A" "$AT"); BKEYS=$(mint "$B" "$BT")
A_AK=$(printf '%s' "$AKEYS" | json_get access_key); A_SK=$(printf '%s' "$AKEYS" | json_get secret_key)
B_AK=$(printf '%s' "$BKEYS" | json_get access_key); B_SK=$(printf '%s' "$BKEYS" | json_get secret_key)
ck [ -n "$A_AK" ]
ck [ -n "$B_AK" ]

aws_a() { AWS_ACCESS_KEY_ID="$A_AK" AWS_SECRET_ACCESS_KEY="$A_SK" AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url "$A" "$@"; }
aws_b() { AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url "$B" "$@"; }
# 失败段带超时：单站点故障时不能无限等待
aws_a_t() { timeout "${AWS_TIMEOUT:-90}" env AWS_ACCESS_KEY_ID="$A_AK" AWS_SECRET_ACCESS_KEY="$A_SK" AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url "$A" "$@"; }
aws_b_t() { timeout "${AWS_TIMEOUT:-90}" env AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url "$B" "$@"; }
BUCKET="stretch-$(date +%s)"

echo "== 2. 建桶 + 600 KiB / 4 KiB 写入 =="
ck aws_a s3 mb "s3://$BUCKET"
head -c 614400 /dev/urandom > "$W/big.bin"
head -c 4096 /dev/urandom > "$W/small.bin"
ck aws_a s3 cp "$W/big.bin" "s3://$BUCKET/big.bin"
ck aws_a s3 cp "$W/small.bin" "s3://$BUCKET/small.bin"

echo "== 3. 两站点读一致 + HEAD =="
ck aws_a s3 cp "s3://$BUCKET/big.bin" "$W/big.a"
ck aws_b s3 cp "s3://$BUCKET/big.bin" "$W/big.b"
ck cmp "$W/big.bin" "$W/big.a"
ck cmp "$W/big.bin" "$W/big.b"
ck [ "$(sha256sum "$W/big.a" | cut -d' ' -f1)" = "$(sha256sum "$W/big.b" | cut -d' ' -f1)" ]
ck [ "$(aws_a s3api head-object --bucket "$BUCKET" --key big.bin --query ContentLength --output text)" = "614400" ]
ck [ "$(aws_b s3api head-object --bucket "$BUCKET" --key big.bin --query ContentLength --output text)" = "614400" ]
ck [ "$(aws_a s3api head-object --bucket "$BUCKET" --key big.bin --query ETag --output text)" = \
    "$(aws_b s3api head-object --bucket "$BUCKET" --key big.bin --query ETag --output text)" ]

echo "== 4. 分片/meta 落盘证明（每站点 6 片/块、每盘 1 片、meta 12 份同哈希）=="
parts_a=$(find "$W/a"/d[0-5] -type f -name 'part.*' 2>/dev/null | wc -l)
parts_b=$(find "$W/b"/d[0-5] -type f -name 'part.*' 2>/dev/null | wc -l)
# big.bin = 2 块 × 12 片；small.bin = 1 块 × 12 片 → 每站点 (2+1)×6 = 18
ck [ "$parts_a" = "18" ]
ck [ "$parts_b" = "18" ]
# 每块每站点恰 6 片（1 片/盘）
for p in 1 2; do
  n=$(find "$W/a"/d[0-5] -type f -name "part.$p.*" 2>/dev/null | wc -l)
  ck [ "$n" = "6" ]
done
# 每个 drive 上都有分片（12 片覆盖 12 盘，1 片/盘/块）
for d in 0 1 2 3 4 5; do
  ck [ "$(find "$W/a/d$d" -type f -name 'part.*' 2>/dev/null | wc -l)" -ge 1 ]
done
meta_a=$(find "$W/a"/d[0-5] -type f -path '*/big.bin/*' -name xl.meta 2>/dev/null | wc -l)
ck [ "$meta_a" = "12" ]
uniq_meta=$(find "$W/a"/d[0-5] -type f -path '*/big.bin/*' -name xl.meta -exec sha256sum {} + 2>/dev/null | cut -d' ' -f1 | sort -u | wc -l)
ck [ "$uniq_meta" = "1" ]

echo "== 5. 停 B：单站点 GET/PUT 均 fail-closed =="
stop_b
if aws_a_t s3 cp "s3://$BUCKET/big.bin" "$W/down.bin" >/dev/null 2>&1; then
  echo "FAIL: 停 B 后 GET 不应成功"; FAILS=$((FAILS+1))
fi
if aws_a_t s3 cp "$W/small.bin" "s3://$BUCKET/while-b-down.bin" >/dev/null 2>&1; then
  echo "FAIL: 停 B 后 PUT 不应成功"; FAILS=$((FAILS+1))
fi
# 单站点 6 片 < data=8：离线不可重建（对象目录里只有本站点分片）
ck [ "$(find "$W/a"/d[0-5] -type f -name 'part.1.*' 2>/dev/null | wc -l)" -lt 8 ]

echo "== 6. 恢复 B：GET 重新成功 =="
start_b
ok=""
for _ in $(seq 1 60); do
  if AWS_TIMEOUT=10 aws_a_t s3 cp "s3://$BUCKET/big.bin" "$W/back.bin" >/dev/null 2>&1; then ok=1; break; fi
  sleep 1
done
ck [ "$ok" = "1" ]
ck cmp "$W/big.bin" "$W/back.bin"

echo "== 7. 对称：停 A 后经 B 读非 2xx =="
stop_a
if aws_b_t s3 cp "s3://$BUCKET/big.bin" "$W/down2.bin" >/dev/null 2>&1; then
  echo "FAIL: 停 A 后经 B 读不应成功"; FAILS=$((FAILS+1))
fi
start_a

echo "== 8. 配置负例（每项必须非零退出，不启动）=="
neg() { # name python-transform
  local name="$1" code="$2" d="$W/neg" port
  port=$(free_port)
  rm -rf "$d"; mkdir -p "$d"
  python3 - "$W/a/oss.toml" "$d/oss.toml" "$code" "$port" <<'PY'
import re, sys
src, dst, code, port = sys.argv[1:5]
t = open(src).read()
exec(code)
# 负例若意外通过校验也不得撞正在运行的 A：换监听端口与库路径
t = re.sub(r'listen = "[^"]*"', 'listen = "127.0.0.1:%s"' % port, t, count=1)
t = re.sub(r'database_url = "[^"]*"', 'database_url = "sqlite://%s/oss.db?mode=rwc"' % dst.rsplit("/", 1)[0], t, count=1)
open(dst, "w").write(t)
PY
  ( cd "$d" && timeout 10 "$BIN" serve >"$d/$name.log" 2>&1 )
  local rc=$?
  if [ "$rc" -eq 0 ] || [ "$rc" -eq 124 ]; then
    echo "FAIL: 负例 $name 未被拒（rc=$rc）"; FAILS=$((FAILS+1)); tail -5 "$d/$name.log"
  fi
}
neg inline_nonzero  't = t.replace("inline_threshold = 0", "inline_threshold = 1024")'
neg five_drives     'import re; m = re.search(r"data_dirs = \[(.*?)\]", t); p = m.group(1).split(", "); t = t.replace(m.group(0), "data_dirs = [" + ", ".join(p[:-1]) + "]")'
neg peer_five       't = t.replace("peer_drives = 6", "peer_drives = 5")'
neg ec_8_3          't = t.replace("parity = 4", "parity = 3")'
neg placement_v1    't = t.replace("placement = \"v2\"", "placement = \"v1\"")'
neg https_peer      't = t.replace("peer_addr = \"127.0.0.1:", "peer_addr = \"https://127.0.0.1:")'
neg self_eq_raft    'import re; mesh = re.search(r"self_addr = \"127\.0\.0\.1:(\d+)\"", t).group(1); t = re.sub(r"advertise_endpoint = \"http://127\.0\.0\.1:\d+\"", "advertise_endpoint = \"http://127.0.0.1:" + mesh + "\"", t)'
neg no_raft         'import re; t = re.sub(r"\n\[raft\][\s\S]*", "", t)'
neg no_site         'import re; t = re.sub(r"\n\[site\][\s\S]*?(?=\n\[stretch\])", "", t)'
neg with_cluster    't += "\n[cluster]\nnode_id = \"A\"\nsecret = \"x\"\nnodes = []\n"'

echo "== 结果 =="
if [ "$FAILS" -ne 0 ]; then
  echo "FAILED ($FAILS)"; tail -40 "$W/a/server.log" "$W/b/server.log" 2>/dev/null
  exit 1
fi
echo "ALL PASS"
