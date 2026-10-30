#!/usr/bin/env bash
# M7 多站点异步复制端到端（两个真实 oss-server 实例，双向站点对）。
# 覆盖：快照/ping、建桶后立刻写、策略/用户传播、停对端积压重放、版本桶 marker、
# 双端并发同 key、非空桶删除不生效（R3b）、重启不回退 versioning（R8）、非版本桶不跨界。
#
# 前置：`cargo build -p oss-server`；aws-cli；curl；python3。
# 用法：OSS_BIN=target/debug/oss-server tests/e2e_sites.sh
set -uo pipefail
# 所有 curl 统一超时：e2e 卡住时失败而不是挂死。
curl() { command curl --max-time 30 "$@"; }
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
A="http://127.0.0.1:$A_HTTP"; B="http://127.0.0.1:$B_HTTP"
W=$(mktemp -d)
TOK_A=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_B=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
REGION_A="ap-east-1"; REGION_B="eu-west-1"
A_ADMIN="erik"; A_PW="pw-admin-a"
B_ADMIN="erik"; B_PW="pw-admin-b"
A_PID=""; B_PID=""
FAILS=0
ck() { if ! "$@"; then echo "FAIL: $*"; FAILS=$((FAILS+1)); fi; }

cleanup() {
  [ -n "$A_PID" ] && kill "$A_PID" 2>/dev/null
  [ -n "$B_PID" ] && kill "$B_PID" 2>/dev/null
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

mk_cfg() { # dir port region token
  mkdir -p "$W/$1/d0" "$W/$1/staging"
  cat > "$W/$1/oss.toml" <<EOF
listen = "127.0.0.1:$2"
data_dir = "$W/$1/d0"
staging_dir = "$W/$1/staging"
database_url = "sqlite://$W/$1/oss.db?mode=rwc"
jwt_secret = "dev-only-change-me-0123456789abcdef"
max_put_bytes = 268435456
groups = []

[site]
name = "$1"
region = "$3"
priority = 10
token = "$4"
EOF
}

start_a() {
  (cd "$W/a" && OSS_ADMIN_USER="$A_ADMIN" OSS_ADMIN_PASSWORD="$A_PW" exec "$BIN" serve >>"$W/a/server.log" 2>&1) &
  A_PID=$!
  wait_http "$A_HTTP" || { echo "FAIL: A 未就绪"; tail -30 "$W/a/server.log"; exit 1; }
}
start_b() {
  local admin_user="${B_ADMIN_ENV:-$B_ADMIN}" admin_pw="${B_PW_ENV:-$B_PW}"
  (cd "$W/b" && OSS_ADMIN_USER="$admin_user" OSS_ADMIN_PASSWORD="$admin_pw" exec "$BIN" serve >>"$W/b/server.log" 2>&1) &
  B_PID=$!
  wait_http "$B_HTTP" || { echo "FAIL: B 未就绪"; tail -30 "$W/b/server.log"; exit 1; }
}
stop_a() { [ -n "$A_PID" ] && kill "$A_PID"; wait "$A_PID" 2>/dev/null; A_PID=""; }
stop_b() { [ -n "$B_PID" ] && kill "$B_PID"; wait "$B_PID" 2>/dev/null; B_PID=""; }

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

mk_cfg a "$A_HTTP" "$REGION_A" "$TOK_A"
mk_cfg b "$B_HTTP" "$REGION_B" "$TOK_B"
start_a
start_b
AT=$(login "$A" "$A_ADMIN" "$A_PW"); BT=$(login "$B" "$B_ADMIN" "$B_PW")
[ -n "$AT" ] && [ -n "$BT" ] || { echo "FAIL: 登录"; exit 1; }
AKEYS=$(mint "$A" "$AT"); BKEYS=$(mint "$B" "$BT")
A_AK=$(printf '%s' "$AKEYS" | json_get access_key); A_SK=$(printf '%s' "$AKEYS" | json_get secret_key)
B_AK=$(printf '%s' "$BKEYS" | json_get access_key); B_SK=$(printf '%s' "$BKEYS" | json_get secret_key)
[ -n "$A_AK" ] && [ -n "$B_AK" ] || { echo "FAIL: 铸键"; exit 1; }

aws_a() { AWS_ACCESS_KEY_ID="$A_AK" AWS_SECRET_ACCESS_KEY="$A_SK" AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url "$A" "$@"; }
aws_b() { AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 aws --endpoint-url "$B" "$@"; }
STAMP="$(date +%s)"
PRE_A="pre-a-$STAMP"; PRE_B="pre-b-$STAMP"
LIVE_A="live-a-$STAMP"; LIVE_B="live-b-$STAMP"
VER_A="ver-a-$STAMP"; VER_B="ver-b-$STAMP"
FULL_A="full-a-$STAMP"; EMPTY_A="empty-a-$STAMP"; R8_A="r8-a-$STAMP"
NOVER_A="nover-a-$STAMP"

echo "== 1. 配对前资源 + 双向配对 + 快照/ping =="
aws_a s3 mb "s3://$PRE_A"
aws_b s3 mb "s3://$PRE_B"
curl -s -X POST "$A/api/v1/users" -H "authorization: Bearer $AT" -H 'content-type: application/json' \
  -d '{"username":"alice","password":"alice-pw-1"}' >/dev/null

A_ID=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get id)
A_SELF_TOKEN=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get site_token)
B_ID=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get id)
B_SELF_TOKEN=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get site_token)
[ "$A_SELF_TOKEN" = "$TOK_A" ] && [ "$B_SELF_TOKEN" = "$TOK_B" ] || { echo "FAIL: 站点 token 不符"; exit 1; }

pair() { # base jwt peer_id endpoint region ak sk peer_token
  curl -s -o /dev/null -w '%{http_code}' -X POST "$1/api/v1/sites" -H "authorization: Bearer $2" \
    -H 'content-type: application/json' \
    -d "{\"peer_id\":\"$3\",\"endpoint\":\"$4\",\"region\":\"$5\",\"priority\":10,\"s3_access_key\":\"$6\",\"s3_secret_key\":\"$7\",\"peer_site_token\":\"$8\"}"
}
ck [ "$(pair "$A" "$AT" b "$B" "$REGION_B" "$B_AK" "$B_SK" "$TOK_B")" = "200" ]
ck [ "$(pair "$B" "$BT" a "$A" "$REGION_A" "$A_AK" "$A_SK" "$TOK_A")" = "200" ]

poll_lag() { # base jwt peer
  for _ in $(seq 1 60); do
    LAG=$(curl -s "$1/api/v1/sites/$3/status" -H "authorization: Bearer $2" | json_get control_lag)
    [ "$LAG" = "0" ] && return 0
    sleep 1
  done
  return 1
}
ck poll_lag "$A" "$AT" b
ck poll_lag "$B" "$BT" a
ck [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$A/api/v1/sites/b/ping" -H "authorization: Bearer $AT")" = "200" ]

# 快照：对方的桶/用户可见（物理桶按控制面行对账创建）。
ck aws_b s3api head-bucket --bucket "$PRE_A"
ck aws_a s3api head-bucket --bucket "$PRE_B"
found_alice=""
for _ in $(seq 1 60); do
  if curl -s "$B/api/v1/users" -H "authorization: Bearer $BT" | grep -q '"alice"'; then found_alice=1; break; fi
  sleep 1
done
ck [ "$found_alice" = "1" ]

echo "== 2. 建桶后立刻写（不等待）→ 对端有版本/ETag/内容 =="
aws_a s3 mb "s3://$LIVE_A" >/dev/null
aws_a s3api put-bucket-versioning --bucket "$LIVE_A" --versioning-configuration Status=Enabled
head -c 65536 /dev/urandom > "$W/live.bin"
aws_a s3 cp "$W/live.bin" "s3://$LIVE_A/k.bin" >/dev/null
ok=""
for _ in $(seq 1 90); do
  VID=$(aws_b s3api head-object --bucket "$LIVE_A" --key k.bin --query VersionId --output text 2>/dev/null || true)
  [ -n "$VID" ] && [ "$VID" != "None" ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]
aws_b s3 cp "s3://$LIVE_A/k.bin" "$W/live.back" >/dev/null 2>&1
ck cmp "$W/live.bin" "$W/live.back"
ck [ "$(aws_b s3api get-bucket-versioning --bucket "$LIVE_A" --query Status --output text 2>/dev/null)" = "Enabled" ]

# 反向：B 建桶立刻写 → A 收到。
aws_b s3 mb "s3://$LIVE_B" >/dev/null
aws_b s3api put-bucket-versioning --bucket "$LIVE_B" --versioning-configuration Status=Enabled
aws_b s3 cp "$W/live.bin" "s3://$LIVE_B/k.bin" >/dev/null
ok=""
for _ in $(seq 1 90); do
  VID=$(aws_a s3api head-object --bucket "$LIVE_B" --key k.bin --query VersionId --output text 2>/dev/null || true)
  [ -n "$VID" ] && [ "$VID" != "None" ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]

echo "== 3. 策略 / 用户传播 / 同名管理员保护 =="
POL='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$PRE_A"'/*"}]}'
ck [ "$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$A/api/v1/buckets/$PRE_A/policy" \
  -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "$POL")" = "204" ]
ok=""
for _ in $(seq 1 60); do
  P=$(curl -s "$B/api/v1/buckets/$PRE_A/policy" -H "authorization: Bearer $BT")
  if printf '%s' "$P" | grep -q 's3:GetObject'; then ok=1; break; fi
  sleep 1
done
ck [ "$ok" = "1" ]

# 同名管理员保护：B 重启并引导出管理员 carol；A 的普通 carol upsert 在 B 侧应为 dead。
stop_b
B_ADMIN_ENV="carol" B_PW_ENV="carol-admin-pw" start_b
BT=$(login "$B" "carol" "carol-admin-pw")
ck [ -n "$BT" ]
curl -s -X POST "$A/api/v1/users" -H "authorization: Bearer $AT" -H 'content-type: application/json' \
  -d '{"username":"carol","password":"carol-user-pw"}' >/dev/null
sleep 4
ck [ -n "$(login "$B" "carol" "carol-admin-pw")" ]                 # B 管理员口令未被覆盖
ck [ -z "$(login "$B" "carol" "carol-user-pw" 2>/dev/null || true)" ] # 普通用户口令未生效

echo "== 4. 停对端积压 → 恢复追平 =="
aws_a s3api put-bucket-versioning --bucket "$LIVE_A" --versioning-configuration Status=Enabled
stop_b
for i in 1 2 3 4 5; do
  printf 'backlog-%s' "$i" > "$W/b$i"
  aws_a s3 cp "$W/b$i" "s3://$LIVE_A/backlog-$i" >/dev/null
done
sleep 3
start_b
BT=$(login "$B" "carol" "carol-admin-pw")
ok=""
for _ in $(seq 1 120); do
  n=$(aws_b s3api list-objects-v2 --bucket "$LIVE_A" --prefix backlog- --query 'length(Contents)' --output text 2>/dev/null || echo 0)
  [ "$n" = "5" ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]
ck [ "$(curl -s "$A/api/v1/sites/b/status" -H "authorization: Bearer $AT" | json_get object_pending)" = "0" ]

# 反向窗口：停 A 写 B → 起 A 追平。
stop_a
for i in 1 2 3; do
  printf 'rev-%s' "$i" > "$W/r$i"
  aws_b s3 cp "$W/r$i" "s3://$LIVE_B/rev-$i" >/dev/null
done
sleep 3
start_a
AT=$(login "$A" "$A_ADMIN" "$A_PW")
ok=""
for _ in $(seq 1 120); do
  n=$(aws_a s3api list-objects-v2 --bucket "$LIVE_B" --prefix rev- --query 'length(Contents)' --output text 2>/dev/null || echo 0)
  [ "$n" = "3" ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]

echo "== 5. 版本桶删除 marker + 双端并发同 key =="
aws_a s3 mb "s3://$VER_A" >/dev/null
aws_a s3api put-bucket-versioning --bucket "$VER_A" --versioning-configuration Status=Enabled
aws_a s3 cp "$W/live.bin" "s3://$VER_A/dm.bin" >/dev/null
aws_a s3api delete-object --bucket "$VER_A" --key dm.bin >/dev/null
ok=""
for _ in $(seq 1 90); do
  DM=$(aws_b s3api list-object-versions --bucket "$VER_A" --prefix dm.bin --query 'DeleteMarkers[0].Key' --output text 2>/dev/null || true)
  [ "$DM" = "dm.bin" ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]
# P0-2：双向配对下 marker 不得自激回环——6 秒内 marker 数必须稳定。
MA=$(aws_a s3api list-object-versions --bucket "$VER_A" --prefix dm.bin --query 'length(DeleteMarkers)' --output text)
MB=$(aws_b s3api list-object-versions --bucket "$VER_A" --prefix dm.bin --query 'length(DeleteMarkers)' --output text)
sleep 6
MA2=$(aws_a s3api list-object-versions --bucket "$VER_A" --prefix dm.bin --query 'length(DeleteMarkers)' --output text)
MB2=$(aws_b s3api list-object-versions --bucket "$VER_A" --prefix dm.bin --query 'length(DeleteMarkers)' --output text)
ck [ "$MA" = "$MA2" ]
ck [ "$MB" = "$MB2" ]

printf 'from-a' > "$W/ca"; printf 'from-b' > "$W/cb"
aws_a s3 cp "$W/ca" "s3://$LIVE_A/race" >/dev/null & p1=$!
aws_b s3 cp "$W/cb" "s3://$LIVE_A/race" >/dev/null & p2=$!
wait $p1 $p2
ok=""
for _ in $(seq 1 120); do
  na=$(aws_a s3api list-object-versions --bucket "$LIVE_A" --prefix race --query 'length(Versions)' --output text 2>/dev/null || echo 0)
  nb=$(aws_b s3api list-object-versions --bucket "$LIVE_A" --prefix race --query 'length(Versions)' --output text 2>/dev/null || echo 0)
  [ "$na" -ge 2 ] && [ "$nb" -ge 2 ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]

echo "== 6. 非空桶删除不生效（R3b）+ 空桶删除 + R8 重启不回退 =="
# R3b 真场景：B 的副本桶有本地对象（B→A 对象 worker 关闭，A 不知道），A 删空的源桶 →
# B apply 删除时 BucketNotEmpty → dead，保留行与数据。
aws_a s3 mb "s3://$FULL_A" >/dev/null
aws_a s3api put-bucket-versioning --bucket "$FULL_A" --versioning-configuration Status=Enabled
sleep 4
stop_b
OSS_REPL_INTERVAL_SECS=0 start_b
BT=$(login "$B" "carol" "carol-admin-pw")
aws_b s3 cp "$W/live.bin" "s3://$FULL_A/keep.bin" >/dev/null
sleep 1
aws_a s3 rb "s3://$FULL_A" >/dev/null
sleep 6
if aws_a s3api head-bucket --bucket "$FULL_A" >/dev/null 2>&1; then
  echo "FAIL: A 的空桶删除未生效"; FAILS=$((FAILS+1))
fi
ck aws_b s3api head-bucket --bucket "$FULL_A"
ck aws_b s3api head-object --bucket "$FULL_A" --key keep.bin >/dev/null
DEAD=$(curl -s "$B/api/v1/sites/a/status" -H "authorization: Bearer $BT" | json_get control_dead)
[ "${DEAD:-0}" -ge 1 ] || { echo "FAIL: B 未把非空桶删除记为 dead（control_dead=$DEAD）"; FAILS=$((FAILS+1)); }

aws_a s3 mb "s3://$EMPTY_A" >/dev/null
sleep 3
aws_a s3 rb "s3://$EMPTY_A" >/dev/null
ok=""
for _ in $(seq 1 90); do
  c=$(aws_b s3api head-bucket --bucket "$EMPTY_A" >/dev/null 2>&1; echo $?)
  [ "$c" != "0" ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]

# R8：B 开 versioning → A 重启（启动回填）→ versioning 不回退。
aws_b s3 mb "s3://$R8_A" >/dev/null
sleep 3
aws_b s3api put-bucket-versioning --bucket "$R8_A" --versioning-configuration Status=Enabled
ok=""
for _ in $(seq 1 60); do
  [ "$(aws_a s3api get-bucket-versioning --bucket "$R8_A" --query Status --output text 2>/dev/null)" = "Enabled" ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = "1" ]
stop_a
start_a
AT=$(login "$A" "$A_ADMIN" "$A_PW")
sleep 5
ck [ "$(aws_b s3api get-bucket-versioning --bucket "$R8_A" --query Status --output text)" = "Enabled" ]
ck [ "$(aws_a s3api get-bucket-versioning --bucket "$R8_A" --query Status --output text)" = "Enabled" ]

echo "== 7. 非版本桶写入/删除不跨界 =="
aws_a s3 mb "s3://$NOVER_A" >/dev/null
aws_a s3 cp "$W/live.bin" "s3://$NOVER_A/n.bin" >/dev/null
sleep 5
ck aws_b s3api head-bucket --bucket "$NOVER_A"
if aws_b s3api head-object --bucket "$NOVER_A" --key n.bin >/dev/null 2>&1; then
  echo "FAIL: 非版本桶对象跨界复制了"; FAILS=$((FAILS+1))
fi
aws_a s3 rm "s3://$NOVER_A/n.bin" >/dev/null
sleep 3
if aws_b s3api head-object --bucket "$NOVER_A" --key n.bin >/dev/null 2>&1; then
  echo "FAIL: 非版本桶删除跨界了"; FAILS=$((FAILS+1))
fi

echo "== 结果 =="
if [ "$FAILS" -ne 0 ]; then echo "FAIL: $FAILS 项"; exit 1; fi
echo "PASS: M7 多站点复制 e2e"
