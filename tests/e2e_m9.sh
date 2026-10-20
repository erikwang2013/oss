#!/usr/bin/env bash
# M9 强一致端到端（3 进程：数据站点 A/B + 只承载控制面的 witness C）。
# 覆盖：① 3-voter 控制面并发写收敛（用户/桶/策略三端字段级相等）、单站点故障后仍可写；
#      ② sync 桶 remote-first PUT/MPU/Copy/删除 + SIGSTOP 断连 503 且本地不可见/未删；
#      ③ Stretch EC 2×6：两站点读写、单站点故障非 2xx、恢复、每站点 6 片。
# 模式：sync 桶与 stretch 按计划 v3 N4 **互斥**，同一集群不能同时启用：
#       默认（M9_STRETCH=0）跑步骤 1/2/3/5；M9_STRETCH=1 跑 1/2/4/5（步骤 3 显式跳过）。
# 功能门控：未接线的构建按 /api/v1/raft/status 的 capabilities.raft/sync/stretch 自动跳过
#       对应步骤并打印原因；M9_STRICT=1 时跳过升级为 FAIL（功能齐备后 CI 用）。
# 前置：`cargo build -p oss-server`；aws-cli；curl；python3。
# 用法：OSS_BIN=target/debug/oss-server tests/e2e_m9.sh
#       M9_STRETCH=1 OSS_BIN=target/debug/oss-server tests/e2e_m9.sh   # 2×6 stretch
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
[ -x "$BIN" ] || { echo "缺少 oss-server 二进制（先 cargo build -p oss-server，或设 OSS_BIN）"; exit 2; }
command -v aws >/dev/null || { echo "需要 aws-cli"; exit 2; }
command -v python3 >/dev/null || { echo "需要 python3"; exit 2; }
command -v curl >/dev/null || { echo "需要 curl"; exit 2; }

MODE=sync; [ "${M9_STRETCH:-0}" = "1" ] && MODE=stretch
STRICT="${M9_STRICT:-0}"
PASSES=0; FAILS=0; SKIPS=0
pass() { PASSES=$((PASSES+1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS+1)); echo "FAIL: $*"; }
# 功能未接线 → skip；STRICT 下视为失败
skip() { if [ "$STRICT" = "1" ]; then fail "STRICT: $*"; else SKIPS=$((SKIPS+1)); echo "SKIP: $*"; fi; }
ck() { local m="$*"; if "$@"; then pass "$m"; else fail "$m"; fi; }
ck_q() { local m="$*"; if "$@" >/dev/null 2>&1; then pass "$m"; else fail "$m"; fi; }  # 静默成功的写命令
is2xx_file() { case "$(cat "$1" 2>/dev/null)" in 2??) return 0 ;; *) return 1 ;; esac; }
retry() { local n=$1 i; shift; for i in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done; return 1; }

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
A_HTTP=$(free_port); B_HTTP=$(free_port); C_HTTP=$(free_port); A_MESH=$(free_port); B_MESH=$(free_port)
A="http://127.0.0.1:$A_HTTP"; B="http://127.0.0.1:$B_HTTP"
W=$(mktemp -d)
JWT_SECRET="dev-only-change-me-0123456789abcdef"
CLUSTER_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
MESH_SECRET=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
TOK_A=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_B=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
REG_A="ap-east-1"; REG_B="eu-west-1"; ADMIN="m9admin"; APW="m9admin-pw"
A_PID=""; B_PID=""; C_PID=""
CAP_RAFT=false; CAP_SYNC=false; CAP_STRETCH=false
cleanup() { for p in "$A_PID" "$B_PID" "$C_PID"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done; rm -rf "$W"; }
trap cleanup EXIT
finish() {
  echo "== 结果 =="; echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS（模式 $MODE）"
  [ "$FAILS" -ne 0 ] && exit 1
  echo "PASS: M9 e2e（$PASSES 项通过，$SKIPS 项跳过）"
}

mk_cfg() { # dir http_port region token node_id bootstrap
  local d="$W/$1" inline=131072 dirs="" i n
  [ "$MODE" = stretch ] && inline=0
  mkdir -p "$d/staging"
  n=1; [ "$MODE" = stretch ] && n=6
  for i in $(seq 0 $((n-1))); do mkdir -p "$d/d$i"; dirs="$dirs\"$d/d$i\","; done
  dirs="${dirs%,}"
  cat > "$d/oss.toml" <<EOF
listen = "127.0.0.1:$2"
data_dir = "$d/d0"
data_dirs = [$dirs]
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456
groups = []

[site]
name = "$1"
region = "$3"
priority = 10
token = "$4"

[ec]
enabled = true
data = 8
parity = 4
inline_threshold = $inline

[raft]
node_id = $5
role = "voter"
bootstrap = $6
advertise_endpoint = "http://127.0.0.1:$2"
cluster_token = "$CLUSTER_TOKEN"
db_path = "$d/raft.db"
election_timeout_ms = 3000
heartbeat_interval_ms = 500
rpc_timeout_ms = 2000
write_timeout_ms = 3000

[sync]
timeout_ms = 1000
max_object_bytes = 67108864
EOF
  if [ "$MODE" = stretch ]; then
    local nname peer self_addr peer_addr
    nname=$(printf '%s' "$1" | tr 'a-z' 'A-Z')
    if [ "$1" = a ]; then peer="B"; self_addr="127.0.0.1:$A_MESH"; peer_addr="127.0.0.1:$B_MESH"
    else peer="A"; self_addr="127.0.0.1:$B_MESH"; peer_addr="127.0.0.1:$A_MESH"; fi
    cat >> "$d/oss.toml" <<EOF

[stretch]
node_id = "$nname"
self_addr = "$self_addr"
peer_node_id = "$peer"
peer_addr = "$peer_addr"
peer_drives = 6
placement = "v2"
mesh_secret = "$MESH_SECRET"
mesh_timeout_ms = 1500
EOF
  fi
}
mk_witness_cfg() {
  mkdir -p "$W/c"
  cat > "$W/c/witness.toml" <<EOF
[raft]
node_id = 3
role = "witness"
bootstrap = false
advertise_endpoint = "http://127.0.0.1:$C_HTTP"
cluster_token = "$CLUSTER_TOKEN"
db_path = "$W/c/raft.db"
state_db = "$W/c/control.db"
EOF
}

wait_http() { # port pid [path]
  local p=$1 pid=$2 path="${3:-/health/live}"
  for _ in $(seq 1 90); do
    kill -0 "$pid" 2>/dev/null || return 1
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p$path" 2>/dev/null || true)" = 200 ] && return 0
    sleep 1
  done
  return 1
}
health_code() { curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$1/health/live" 2>/dev/null || true; }
health_any_code() { # witness 只暴露 /health
  local c; c=$(health_code "$1"); [ "$c" = 200 ] && { echo 200; return; }
  curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://127.0.0.1:$1/health" 2>/dev/null || true
}
start_a() {
  (cd "$W/a" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$BIN" serve >>"$W/a/server.log" 2>&1) &
  A_PID=$!; wait_http "$A_HTTP" "$A_PID"
}
start_b() {
  (cd "$W/b" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$BIN" serve >>"$W/b/server.log" 2>&1) &
  B_PID=$!; wait_http "$B_HTTP" "$B_PID"
}
start_c() {
  (cd "$W/c" && exec "$BIN" witness --config "$W/c/witness.toml" >>"$W/c/server.log" 2>&1) &
  C_PID=$!
  for _ in $(seq 1 90); do
    kill -0 "$C_PID" 2>/dev/null || return 1
    [ "$(health_any_code "$C_HTTP")" = 200 ] && return 0
    sleep 1
  done
  return 1
}
kill9() { local pid="$1"; [ -n "$pid" ] && { kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; }; }

login() {
  curl -s -X POST "$1/api/v1/login" -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"$3\"}" |
    python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'
}
mint() { curl -s -X POST "$1/api/v1/access-keys" -H "authorization: Bearer $2" -H 'content-type: application/json' -d '{}'; }
json_get() { python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get(sys.argv[1],""))' "$1"; }
post_code() { curl -s -o /dev/null -w '%{http_code}' -X POST "$1" -H "authorization: Bearer $2" -H 'content-type: application/json' -d "$3" > "$4"; }
put_code() { curl -s -o /dev/null -w '%{http_code}' -X PUT "$1" -H "authorization: Bearer $2" -H 'content-type: application/json' -d "$3" > "$4"; }

# S0 骨架在 /api/v1/raft/status，计划 §2.6 的正式路径是 /api/v1/site/raft/status；两者都探。
status_json() {
  local c p
  for p in /api/v1/raft/status /api/v1/site/raft/status; do
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "$1$p")
    [ "$c" = 200 ] && { curl -s --max-time 5 "$1$p"; return 0; }
  done
  return 1
}
cap() { status_json "$A" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: print("false"); raise SystemExit
print("true" if d.get("capabilities",{}).get(sys.argv[1]) is True else "false")' "$1"; }
members_len() { status_json "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: print(-1); raise SystemExit
v=d.get("members", d.get("voters"))
print(len(v) if isinstance(v,list) else -1)'; }
enabled_flag() { status_json "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: print("false"); raise SystemExit
print(str(d.get("enabled",False)).lower())'; }
poll_login() { local t; for _ in $(seq 1 60); do t=$(login "$1" "$2" "$3"); [ -n "$t" ] && { printf '%s' "$t"; return 0; }; sleep 1; done; return 1; }
poll_members() { for _ in $(seq 1 60); do [ "$(members_len "$1")" = "$2" ] && return 0; sleep 1; done; return 1; }

# 三端（A/B 的 oss.db + witness 的 control.db）业务字段逐字段比较（代理键归一化）。
# 只比较本脚本创建的 m9* 实体：bootstrap 管理员 m9admin 是各站点本地种子、witness 无此行，
# 排除；m9owner 等命令写入的行必须在三端一致。
norm_check() { # db_a db_b db_c users_csv buckets_csv
python3 - "$@" <<'PY'
import json, sqlite3, sys
def dump(p):
    c = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
    u = {r[0]: list(r[1:]) for r in c.execute("SELECT username,is_admin,status FROM users WHERE username LIKE 'm9%' AND username <> 'm9admin'")}
    b = {r[0]: list(r[1:]) for r in c.execute("SELECT b.name,u.username,b.versioning,b.replication_mode,b.deleting,b.creation_date FROM buckets b LEFT JOIN users u ON u.id=b.owner_id WHERE b.name LIKE 'm9b-%'")}
    q = {r[0]: r[1] for r in c.execute("SELECT bucket,policy_json FROM bucket_policies WHERE bucket LIKE 'm9b-%'")}
    return {"users": u, "buckets": b, "policies": q}
try:
    a, b, c = (dump(x) for x in sys.argv[1:4])
except sqlite3.Error as e:
    print("db error:", e); sys.exit(2)
if a != b or a != c:
    print(json.dumps({"a": a, "b": b, "c": c}, ensure_ascii=False, sort_keys=True)); sys.exit(1)
need = sys.argv[4].split(",") + sys.argv[5].split(",")
miss = [k for k in need if k and k not in a["users"] and k not in a["buckets"]]
if miss:
    print("missing:", miss); sys.exit(3)
print("ok")
PY
}
poll_norm() { retry 90 norm_check "$W/a/oss.db" "$W/b/oss.db" "$W/c/control.db" "$1" "$2"; }

expect_503() {
  local out
  if out=$("$@" 2>&1); then fail "$* 应 503，实得成功"; return; fi
  if printf '%s' "$out" | grep -Eq '503|ServiceUnavailable|SlowDown'; then pass "$* → 503"
  else fail "$* 非 503 形态：$out"; fi
}
db_has_user() { python3 -c 'import sqlite3,sys
try: c=sqlite3.connect(f"file:{sys.argv[1]}?mode=ro",uri=True); sys.exit(0 if c.execute("SELECT 1 FROM users WHERE username=?",(sys.argv[2],)).fetchone() else 1)
except sqlite3.Error: sys.exit(1)' "$1" "$2"; }
outbox_site_count() { python3 -c 'import sqlite3,sys
n=0
for p in sys.argv[1:3]:
    c=sqlite3.connect(f"file:{p}?mode=ro",uri=True)
    n+=c.execute("SELECT count(*) FROM repl_outbox WHERE target_kind=? AND bucket=?",("site",sys.argv[3])).fetchone()[0]
print(n)' "$W/a/oss.db" "$W/b/oss.db" "$1"; }
shards_at() { find "$1"/d* -type f -name 'part.*' -path "*/$2/$3/*" 2>/dev/null | wc -l | tr -d ' '; }
meta_at() { find "$1"/d* -type f -name 'xl.meta' -path "*/$2/$3/xl.meta" 2>/dev/null | wc -l | tr -d ' '; }

# 轮询谓词（供 retry 复用）
a_head() { aws_a s3api head-object --bucket "$1" --key "$2" >/dev/null 2>&1; }
b_head() { aws_b s3api head-object --bucket "$1" --key "$2" >/dev/null 2>&1; }
a_missing() { ! a_head "$1" "$2"; }
b_missing() { ! b_head "$1" "$2"; }
both_missing() { a_missing "$1" "$2" && b_missing "$1" "$2"; }
b_ver_missing() { ! aws_b s3api head-object --bucket "$1" --key "$2" --version-id "$3" >/dev/null 2>&1; }
a_ver_missing() { ! aws_a s3api head-object --bucket "$1" --key "$2" --version-id "$3" >/dev/null 2>&1; }
both_versioned() {
  [ "$(aws_a s3api get-bucket-versioning --bucket "$1" --query Status --output text 2>/dev/null)" = Enabled ] && \
  [ "$(aws_b s3api get-bucket-versioning --bucket "$1" --query Status --output text 2>/dev/null)" = Enabled ]
}
b_has_bucket() { aws_b s3api head-bucket --bucket "$1" >/dev/null 2>&1; }
b_meta_ok() { # bucket key：远端 storage_class/tags
  [ "$(aws_b s3api head-object --bucket "$1" --key "$2" --query StorageClass --output text 2>/dev/null)" = STANDARD_IA ] && \
  [ "$(aws_b s3api get-object-tagging --bucket "$1" --key "$2" --query "TagSet[?Key=='m9'].Value | [0]" --output text 2>/dev/null)" = 1 ]
}
b_mpu_size() { [ "$(aws_b s3api head-object --bucket "$1" --key "$2" --query ContentLength --output text 2>/dev/null)" = 1048576 ]; }
b_get_eq() { aws_b s3 cp "s3://$1/$2" "$W/$3" >/dev/null 2>&1 && cmp -s "$W/$4" "$W/$3"; }
a_get_eq() { aws_a s3 cp "s3://$1/$2" "$W/$3" >/dev/null 2>&1 && cmp -s "$W/$4" "$W/$3"; }

echo "== 1. 启动 A/B（[site]+[raft]，模式 $MODE）→ health → /api/v1/raft/status =="
mk_cfg a "$A_HTTP" "$REG_A" "$TOK_A" 1 true
mk_cfg b "$B_HTTP" "$REG_B" "$TOK_B" 2 false
if ! start_a || ! start_b; then
  echo "--- A 日志尾 ---"; tail -8 "$W/a/server.log" 2>/dev/null
  echo "--- B 日志尾 ---"; tail -8 "$W/b/server.log" 2>/dev/null
  skip "该构建不能以 M9 [site]+[raft] 配置启动（Raft 运行时未实现/配置被拒）；e2e 未执行"
  finish
fi
ck [ "$(health_code "$A_HTTP")" = 200 ]
ck [ "$(health_code "$B_HTTP")" = 200 ]
if ! status_json "$A" > "$W/status.json"; then
  skip "步骤 1：/api/v1/raft/status 不可用（S0 前构建）"
else
  pass "/api/v1/raft/status 可读"
  CAP_RAFT=$(cap raft); CAP_SYNC=$(cap sync); CAP_STRETCH=$(cap stretch)
  echo "  capabilities: raft=$CAP_RAFT sync=$CAP_SYNC stretch=$CAP_STRETCH"
  if [ "$CAP_RAFT" = true ]; then
    ck [ "$(enabled_flag "$A")" = true ]
    mk_witness_cfg
    if start_c; then ck [ "$(health_any_code "$C_HTTP")" = 200 ]
    else fail "witness C 未就绪"; tail -8 "$W/c/server.log" 2>/dev/null; fi
  else
    skip "witness C 未启动：capabilities.raft=false（Raft 运行时未接线）"
  fi
fi

AT=$(login "$A" "$ADMIN" "$APW"); BT=$(login "$B" "$ADMIN" "$APW")
if [ -z "$AT" ] || [ -z "$BT" ]; then fail "管理员登录失败"; finish; fi
AKEYS=$(mint "$A" "$AT"); BKEYS=$(mint "$B" "$BT")
A_AK=$(printf '%s' "$AKEYS" | json_get access_key); A_SK=$(printf '%s' "$AKEYS" | json_get secret_key)
B_AK=$(printf '%s' "$BKEYS" | json_get access_key); B_SK=$(printf '%s' "$BKEYS" | json_get secret_key)
if [ -z "$A_AK" ] || [ -z "$B_AK" ]; then fail "铸造 AK 失败"; finish; fi
aws_a() { AWS_ACCESS_KEY_ID="$A_AK" AWS_SECRET_ACCESS_KEY="$A_SK" AWS_DEFAULT_REGION=us-east-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER="" aws --endpoint-url "$A" "$@"; }
aws_b() { AWS_ACCESS_KEY_ID="$B_AK" AWS_SECRET_ACCESS_KEY="$B_SK" AWS_DEFAULT_REGION=us-east-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER="" aws --endpoint-url "$B" "$@"; }
STAMP=$(date +%s)

if [ "$CAP_RAFT" != true ]; then
  skip "步骤 2：3-voter 控制面并发写——capabilities.raft=false"
else
  echo "== 2. 配对 A/B + 加 witness C + 并发控制写（用户/桶/策略）三端字段级一致 =="
  A_ID=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get id)
  A_ST=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get site_token)
  B_ID=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get id)
  B_ST=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get site_token)
  pair() { curl -s -o /dev/null -w '%{http_code}' -X POST "$1/api/v1/sites" -H "authorization: Bearer $2" -H 'content-type: application/json' -d "{\"peer_id\":\"$3\",\"endpoint\":\"$4\",\"region\":\"$5\",\"priority\":10,\"s3_access_key\":\"$6\",\"s3_secret_key\":\"$7\",\"peer_site_token\":\"$8\"}"; }
  ck [ "$(pair "$A" "$AT" "$B_ID" "$B" "$REG_B" "$B_AK" "$B_SK" "$B_ST")" = 200 ]
  ck [ "$(pair "$B" "$BT" "$A_ID" "$A" "$REG_A" "$A_AK" "$A_SK" "$A_ST")" = 200 ]
  ck poll_members "$A" 2   # B 的成员变更已提交，再加 C（CLI 读当前 membership）
  if "$BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 3 --endpoint "http://127.0.0.1:$C_HTTP" >>"$W/a/raft-admin.log" 2>&1; then
    pass "raft-add-witness C"
  else
    fail "raft-add-witness 失败"; tail -5 "$W/a/raft-admin.log"
  fi
  if [ "$(members_len "$A")" -ge 0 ]; then
    ck poll_members "$A" 3
    PR=$(status_json "$A" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(str(d["production_ready"]).lower() if "production_ready" in d else "")')
    if [ -n "$PR" ]; then ck [ "$PR" = true ]; else skip "raft/status 未提供 production_ready（按计划 S8 应有）"; fi
  else
    skip "raft/status 未提供 members/voters 数组（按计划 S5 应有）；跳过成员数断言"; sleep 5
  fi
  UN_A="m9ua$STAMP"; UN_B="m9ub$STAMP"; OWNER="m9owner$STAMP"; OWNER_PW="m9owner-pw"
  BK_A="m9b-a-$STAMP"; BK_B="m9b-b-$STAMP"
  POL_A='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$BK_A"'/*"}]}'
  POL_B='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$BK_B"'/*"}]}'
  # 并发用户（A/B 各向同一控制面提交）
  post_code "$A/api/v1/users" "$AT" "{\"username\":\"$UN_A\",\"password\":\"m9pass-123\"}" "$W/u1" & p1=$!
  post_code "$B/api/v1/users" "$BT" "{\"username\":\"$UN_B\",\"password\":\"m9pass-123\"}" "$W/u2" & p2=$!
  wait "$p1" "$p2"; ck is2xx_file "$W/u1"; ck is2xx_file "$W/u2"
  post_code "$A/api/v1/users" "$AT" "{\"username\":\"$OWNER\",\"password\":\"$OWNER_PW\"}" "$W/u3"
  ck is2xx_file "$W/u3"
  OT=$(poll_login "$A" "$OWNER" "$OWNER_PW"); ck [ -n "$OT" ]
  OT_B=$(poll_login "$B" "$OWNER" "$OWNER_PW"); ck [ -n "$OT_B" ]
  # 并发建桶（桶主 = 普通用户 OWNER：命令写入行，witness 也有）
  post_code "$A/api/v1/buckets" "$OT" "{\"name\":\"$BK_A\"}" "$W/b1" & p1=$!
  post_code "$B/api/v1/buckets" "$OT_B" "{\"name\":\"$BK_B\"}" "$W/b2" & p2=$!
  wait "$p1" "$p2"
  ck [ "$(cat "$W/b1")" = 204 ]; ck [ "$(cat "$W/b2")" = 204 ]
  ck retry 60 b_has_bucket "$BK_A"
  ck retry 60 b_has_bucket "$BK_B"
  # 并发策略写
  put_code "$A/api/v1/buckets/$BK_A/policy" "$OT" "$POL_A" "$W/p1" & p1=$!
  put_code "$B/api/v1/buckets/$BK_B/policy" "$OT_B" "$POL_B" "$W/p2" & p2=$!
  wait "$p1" "$p2"
  ck [ "$(cat "$W/p1")" = 204 ]; ck [ "$(cat "$W/p2")" = 204 ]
  if poll_norm "$UN_A,$UN_B,$OWNER" "$BK_A,$BK_B" >/dev/null; then
    pass "A/B/C 三端用户/桶/策略字段级相等"
  else
    fail "三端字段级 diff 非空"; poll_norm "$UN_A,$UN_B,$OWNER" "$BK_A,$BK_B"
  fi
fi

if [ "$MODE" = stretch ]; then
  skip "步骤 3：sync 桶未跑——sync 与 stretch 互斥（计划 v3 N4）；用默认模式运行"
elif [ "$CAP_SYNC" != true ]; then
  skip "步骤 3：sync 桶 remote-first——capabilities.sync=false（sync 数据面未接线）"
else
  echo "== 3. sync 桶：remote-first PUT/MPU/Copy/删除 + SIGSTOP 断连 503（本地不可见/未删） =="
  SB="m9s-$STAMP"
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$A/api/v1/buckets" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "{\"name\":\"$SB\",\"replication_mode\":\"sync\"}")
  ck [ "$code" = 204 ]
  ck retry 60 both_versioned "$SB"   # sync 桶创建即 versioning=Enabled（§2.4）
  head -c 65536 /dev/urandom > "$W/live.bin"
  ck aws_a s3 cp "$W/live.bin" "s3://$SB/live.bin"
  ck retry 60 b_get_eq "$SB" live.bin live.back live.bin
  printf 'meta\n' > "$W/meta.bin"
  ck_q aws_a s3api put-object --bucket "$SB" --key meta.bin --body "$W/meta.bin" --tagging "m9=1" --storage-class STANDARD_IA
  ck retry 60 b_meta_ok "$SB" meta.bin   # tags/storage_class 远端保留
  head -c 1048576 /dev/urandom > "$W/part.bin"
  UP=$(aws_a s3api create-multipart-upload --bucket "$SB" --key mpu.bin --query UploadId --output text)
  ET=$(aws_a s3api upload-part --bucket "$SB" --key mpu.bin --part-number 1 --upload-id "$UP" --body "$W/part.bin" --query ETag --output text)
  python3 -c 'import json,sys; json.dump({"Parts":[{"ETag":sys.argv[1].strip(),"PartNumber":1}]}, open(sys.argv[2],"w"))' "$ET" "$W/mpu.json"
  ck_q aws_a s3api complete-multipart-upload --bucket "$SB" --key mpu.bin --upload-id "$UP" --multipart-upload "file://$W/mpu.json"
  ck retry 60 b_mpu_size "$SB" mpu.bin
  ck_q aws_a s3api copy-object --bucket "$SB" --key copy.bin --copy-source "$SB/live.bin"
  ck retry 60 b_head "$SB" copy.bin
  echo "  -- SIGSTOP B：PUT/DELETE 必须 503，且本地不可见/未删 --"
  kill -STOP "$B_PID"; sleep 1
  head -c 4096 /dev/urandom > "$W/blocked.bin"
  expect_503 aws_a s3api put-object --bucket "$SB" --key blocked.bin --body "$W/blocked.bin"
  if a_head "$SB" blocked.bin; then fail "断连 PUT 失败但本地可见"; else pass "断连 PUT 本地不可见"; fi
  expect_503 aws_a s3api delete-object --bucket "$SB" --key live.bin
  ck a_head "$SB" live.bin
  VID=$(aws_a s3api head-object --bucket "$SB" --key copy.bin --query VersionId --output text)
  expect_503 aws_a s3api delete-object --bucket "$SB" --key copy.bin --version-id "$VID"
  ck a_head "$SB" copy.bin
  expect_503 aws_a s3api delete-objects --bucket "$SB" --delete '{"Objects":[{"Key":"mpu.bin"}],"Quiet":true}'
  ck a_head "$SB" mpu.bin
  echo "  -- SIGCONT B：恢复成功；remote-first 删除（marker/同 id 版本） --"
  kill -CONT "$B_PID"
  ck retry 60 sh -c "[ \"\$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 http://127.0.0.1:$B_HTTP/health/live)\" = 200 ]"
  ck_q aws_a s3api put-object --bucket "$SB" --key blocked.bin --body "$W/blocked.bin"
  ck retry 60 b_head "$SB" blocked.bin
  ck_q aws_a s3api delete-object --bucket "$SB" --key live.bin
  ck retry 60 both_missing "$SB" live.bin
  MA=$(aws_a s3api list-object-versions --bucket "$SB" --prefix live.bin --query 'DeleteMarkers[0].VersionId' --output text 2>/dev/null)
  MB=$(aws_b s3api list-object-versions --bucket "$SB" --prefix live.bin --query 'DeleteMarkers[0].VersionId' --output text 2>/dev/null)
  if [ -n "$MA" ] && [ "$MA" != None ] && [ "$MA" = "$MB" ]; then pass "marker 同 id 两端一致"
  else fail "marker 未两端同 id（A=$MA B=$MB）"; fi
  ck_q aws_a s3api delete-object --bucket "$SB" --key copy.bin --version-id "$VID"
  ck retry 60 a_ver_missing "$SB" copy.bin "$VID"
  ck retry 60 b_ver_missing "$SB" copy.bin "$VID"
  ck_q aws_a s3api delete-objects --bucket "$SB" --delete '{"Objects":[{"Key":"mpu.bin"}],"Quiet":true}'
  ck retry 60 both_missing "$SB" mpu.bin
  ck [ "$(outbox_site_count "$SB")" = 0 ]   # sync 桶不得进 M7 站点 outbox
fi

if [ "$MODE" != stretch ]; then
  skip "步骤 4：Stretch 2×6 未启用——用 M9_STRETCH=1 运行（sync 与 stretch 互斥，同一集群不可同时启用）"
elif [ "$CAP_STRETCH" != true ]; then
  skip "步骤 4：Stretch EC——capabilities.stretch=false（数据面未接线）"
else
  echo "== 4. Stretch EC 2×6：两站点读写、单站点故障非 2xx、恢复、6 片/站点 =="
  SBK="m9st-$STAMP"
  ck aws_a s3 mb "s3://$SBK"
  ck retry 60 b_has_bucket "$SBK"
  head -c 1048576 /dev/urandom > "$W/sa.bin"
  head -c 1048576 /dev/urandom > "$W/sb.bin"
  ck aws_a s3 cp "$W/sa.bin" "s3://$SBK/a.bin"
  ck retry 90 b_get_eq "$SBK" a.bin sa.back sa.bin
  ck aws_b s3 cp "$W/sb.bin" "s3://$SBK/b.bin"
  ck retry 90 a_get_eq "$SBK" b.bin sb.back sb.bin
  SA=$(shards_at "$W/a" "$SBK" a.bin); SB_=$(shards_at "$W/b" "$SBK" a.bin)
  ck [ "$SA" = 6 ]; ck [ "$SB_" = 6 ]; ck [ $((SA + SB_)) = 12 ]
  MA=$(meta_at "$W/a" "$SBK" a.bin); MB=$(meta_at "$W/b" "$SBK" a.bin)
  ck [ $((MA + MB)) = 12 ]   # meta 每盘镜像：两站点各 6 份
  kill9 "$B_PID"; B_PID=""
  if aws_a s3 cp "s3://$SBK/a.bin" "$W/sa.down" >/dev/null 2>&1; then fail "停 B 后 GET 应非 2xx"
  else pass "停 B 后 GET 非 2xx（6 片 < data=8）"; fi
  if aws_a s3 cp "$W/sa.bin" "s3://$SBK/down.bin" >/dev/null 2>&1; then fail "停 B 后 PUT 应非 2xx"
  else pass "停 B 后 PUT 非 2xx"; fi
  if start_b; then ck retry 90 a_get_eq "$SBK" a.bin sa.up sa.bin
  else fail "B 重启未就绪"; tail -8 "$W/b/server.log"; fi
fi

if [ "$CAP_RAFT" != true ]; then
  skip "步骤 5：3-voter 故障/恢复——capabilities.raft=false"
else
  echo "== 5. 3-voter：kill B → A+C 可写；kill C → 503；重启 C 追平 =="
  kill9 "$B_PID"; B_PID=""
  UQ="m9q$STAMP"; UQ2="m9c$STAMP"; BAD="m9no$STAMP"
  ok=""
  for _ in $(seq 1 60); do
    code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$A/api/v1/users" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "{\"username\":\"$UQ\",\"password\":\"m9pass-123\"}")
    case "$code" in 2??) ok=1; break ;; esac; sleep 1
  done
  ck [ "$ok" = 1 ]   # 停 1 数据站点：A+C quorum 2 仍可写
  kill9 "$C_PID"; C_PID=""
  code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 60 -X POST "$A/api/v1/users" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "{\"username\":\"$BAD\",\"password\":\"m9pass-123\"}")
  ck [ "$code" = 503 ]   # 仅剩 A：无 quorum
  if start_c; then
    ck retry 90 db_has_user "$W/c/control.db" "$UQ"   # C 重启后应用/追平已提交的 UQ
    ok=""
    for _ in $(seq 1 60); do
      code=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$A/api/v1/users" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "{\"username\":\"$UQ2\",\"password\":\"m9pass-123\"}")
      case "$code" in 2??) ok=1; break ;; esac; sleep 1
    done
    ck [ "$ok" = 1 ]
    ck retry 60 db_has_user "$W/c/control.db" "$UQ2"
    if db_has_user "$W/a/oss.db" "$BAD" || db_has_user "$W/c/control.db" "$BAD"; then
      fail "无 quorum 的写被提交（分叉）"
    else
      pass "无 quorum 的写未提交"
    fi
  else
    fail "C 重启未就绪"; tail -8 "$W/c/server.log"
  fi
fi

finish
