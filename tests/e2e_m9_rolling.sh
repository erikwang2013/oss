#!/usr/bin/env bash
# M9 控制面 Level B 滚动升级 e2e（3 节点：A/B 数据站点 + C witness）。
#
# 场景（plan §7.2，按本版本可执行性调整）：从 **全旧（v1.13.1）集群** 起步，逐台换新
# 二进制（先 follower 后 leader）→ 验证跨版本日志同步/选举 → 全部新后启用
# feature_level=2 → 激活前回滚演练 → 旧二进制读新二进制写过的 DB → 新 leader 拒绝
# 旧 peer 成员变更。
#
# 与 plan 原文的差异（必须保留说明）：
# - 不在全旧集群里“直接 pair 新节点”：R7 要求配对双方 wire>=2，新二进制会拒绝旧
#   identity；Level B 支持的是“已有集群滚动升级”，不是混合版本新建。
# - D6：本版本没有产品级 level-2 命令（FeatureProbe 仅 cfg(test)），因此激活（步骤 8）、
#   激活后回滚/OSSCTL2（步骤 10）、level-2 写入 503 断言无法用发布二进制演练；这些路径
#   由 crates/oss-server 内 store/compat 单元测试覆盖，脚本显式 SKIP。
# - 步骤 6（跨版本快照安装）需要强制 build+purge 快照，本版本无产品入口；SKIP，快照
#   兼容由进程内 control_snapshot 测试覆盖。
#
# 用法：
#   OSS_OLD_BIN=/path/to/v1.13.1/oss-server OSS_NEW_BIN=target/debug/oss-server \
#     tests/e2e_m9_rolling.sh
#   未提供 OSS_OLD_BIN → 整体 SKIP；M9_ROLLING_STRICT=1 时 SKIP 升为 FAIL。
# 前置：两枚二进制（旧 = tag v1.13.1 / commit a58d64a）、curl、python3、回环端口。
set -uo pipefail
cd "$(dirname "$0")/.."

NEW_BIN="${OSS_NEW_BIN:-${OSS_BIN:-$PWD/target/debug/oss-server}}"
OLD_BIN="${OSS_OLD_BIN:-}"
STRICT="${M9_ROLLING_STRICT:-0}"
PASSES=0; FAILS=0; SKIPS=0
pass() { PASSES=$((PASSES+1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS+1)); echo "FAIL: $*"; }
skip() { if [ "$STRICT" = "1" ]; then fail "STRICT: $*"; else SKIPS=$((SKIPS+1)); echo "SKIP: $*"; fi; }
ck() { local m="$*"; if "$@"; then pass "$m"; else fail "$m"; fi; }
finish() {
  echo "== 结果 =="; echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS"
  [ "$FAILS" -ne 0 ] && exit 1
  echo "PASS: M9 rolling e2e（$PASSES 项通过，$SKIPS 项跳过）"
  exit 0
}

if [ ! -x "$NEW_BIN" ]; then echo "缺少新二进制（OSS_NEW_BIN / cargo build -p oss-server）"; exit 2; fi
if [ ! -x "$OLD_BIN" ]; then
  skip "缺少 OSS_OLD_BIN（v1.13.1）：双二进制滚动 e2e 无法执行；构建 a58d64a 后重跑"
  finish
fi
for c in curl python3; do command -v "$c" >/dev/null || { echo "需要 $c"; exit 2; }; done
NEW_BIN=$(realpath "$NEW_BIN"); OLD_BIN=$(realpath "$OLD_BIN")

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
USED_PORTS=""
pick_ports() {
  local p v
  for v in A_HTTP B_HTTP C_HTTP D_HTTP; do
    while :; do
      p=$(free_port)
      case " $USED_PORTS " in *" $p "*) continue ;; esac
      break
    done
    USED_PORTS="$USED_PORTS $p"; printf -v "$v" '%s' "$p"
  done
}
pick_ports
A="http://127.0.0.1:$A_HTTP"; B="http://127.0.0.1:$B_HTTP"; C="http://127.0.0.1:$C_HTTP"; D="http://127.0.0.1:$D_HTTP"
W=$(mktemp -d)
JWT_SECRET="rolling-e2e-jwt-0123456789abcdef"
CLUSTER_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
TOK_A=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_B=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
REG_A="ap-east-1"; REG_B="eu-west-1"; ADMIN="rollingadmin"; APW="rollingadmin-pw"
A_PID=""; B_PID=""; C_PID=""; D_PID=""
cleanup() { for p in "$A_PID" "$B_PID" "$C_PID" "$D_PID"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done; rm -rf "$W"; }
trap cleanup EXIT

# mk_cfg dir port region token node_id bootstrap feature
# feature 为空时**不写** feature_level：旧二进制 RaftToml 拒绝未知键。
mk_cfg() {
  local d="$W/$1" feat="${7:-}"
  mkdir -p "$d/d0" "$d/staging"
  cat > "$d/oss.toml" <<EOF
listen = "127.0.0.1:$2"
data_dir = "$d/d0"
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
inline_threshold = 131072

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
EOF
  if [ -n "$feat" ]; then echo "feature_level = $feat" >> "$d/oss.toml"; fi
}
mk_witness_cfg() { # dir node_id port feature
  mkdir -p "$W/$1"
  cat > "$W/$1/witness.toml" <<EOF
[raft]
node_id = $2
role = "witness"
bootstrap = false
advertise_endpoint = "http://127.0.0.1:$3"
cluster_token = "$CLUSTER_TOKEN"
db_path = "$W/$1/raft.db"
state_db = "$W/$1/control.db"
EOF
  if [ -n "${4:-}" ]; then echo "feature_level = $4" >> "$W/$1/witness.toml"; fi
}

A_BIN="$OLD_BIN"; B_BIN="$OLD_BIN"; C_BIN="$OLD_BIN"
start_a() { ( cd "$W/a" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$A_BIN" serve >>"$W/a/server.log" 2>&1 ) & A_PID=$!; wait_http "$A_HTTP" "$A_PID"; }
start_b() { ( cd "$W/b" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$B_BIN" serve >>"$W/b/server.log" 2>&1 ) & B_PID=$!; wait_http "$B_HTTP" "$B_PID"; }
start_c() { ( cd "$W/c" && exec "$C_BIN" witness --config "$W/c/witness.toml" >>"$W/c/server.log" 2>&1 ) & C_PID=$!; wait_any "$C_HTTP" "$C_PID"; }
start_d() { ( cd "$W/d" && exec "$OLD_BIN" witness --config "$W/d/witness.toml" >>"$W/d/server.log" 2>&1 ) & D_PID=$!; wait_any "$D_HTTP" "$D_PID"; }
wait_http() { local p=$1 pid=$2; for _ in $(seq 1 90); do kill -0 "$pid" 2>/dev/null || return 1; [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p/health/live" 2>/dev/null || true)" = 200 ] && return 0; sleep 1; done; return 1; }
wait_any() { local p=$1 pid=$2 c; for _ in $(seq 1 90); do kill -0 "$pid" 2>/dev/null || return 1; for path in /health/live /health; do c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p$path" 2>/dev/null || true); [ "$c" = 200 ] && return 0; done; sleep 1; done; return 1; }
kill9() { local pid="$1"; [ -n "$pid" ] && { kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; }; }
stop_all() { kill9 "$A_PID"; kill9 "$B_PID"; kill9 "$C_PID"; kill9 "$D_PID"; A_PID=""; B_PID=""; C_PID=""; D_PID=""; }

status_json() { curl -s --max-time 5 -H "x-oss-raft-token: $CLUSTER_TOKEN" "$1/api/v1/site/raft/status"; }
sf() { status_json "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
v=d.get(sys.argv[1],"");print("" if v is None else v)' "$2"; }
cf() { status_json "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
v=d.get("compat",{}).get(sys.argv[1],"");print("" if v is None else v)' "$2"; }
login() { curl -s --max-time 10 -X POST "$1/api/v1/login" -H 'content-type: application/json' -d "{\"username\":\"$2\",\"password\":\"$3\"}" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'; }
json_get() { python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get(sys.argv[1],""))' "$1"; }
mint() { curl -s --max-time 10 -X POST "$1/api/v1/access-keys" -H "authorization: Bearer $2" -H 'content-type: application/json' -d '{}'; }
pair() { curl -s --max-time 15 -o /dev/null -w '%{http_code}' -X POST "$1/api/v1/sites" -H "authorization: Bearer $2" -H 'content-type: application/json' -d "{\"peer_id\":\"$3\",\"endpoint\":\"$4\",\"region\":\"$5\",\"priority\":10,\"s3_access_key\":\"$6\",\"s3_secret_key\":\"$7\",\"peer_site_token\":\"$8\"}"; }
bucket_code() { curl -s --max-time 10 -o /dev/null -w '%{http_code}' -X POST "$1/api/v1/buckets" -H "authorization: Bearer $2" -H 'content-type: application/json' -d "{\"name\":\"$3\"}"; }
has_bucket() { curl -s --max-time 10 "$1/api/v1/buckets" -H "authorization: Bearer $2" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
items=d if isinstance(d,list) else d.get("buckets",[])
print(any((b.get("name") if isinstance(b,dict) else b)==sys.argv[1] for b in items))' "$3" | grep -qx True; }
poll_pred() { local n=$1; shift; for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done; return 1; }
leader_is() { [ "$(sf "$1" leader)" = "$2" ]; }
# 全体重启后 leader 不保证仍是 node 1（Raft 任意 voter 可当选，election_timeout_ms=3000）；
# A/B 都看到同一个非空 leader 即视为收敛，具体身份与后续用例无关。
any_leader() { local a b; a=$(sf "$A" leader); b=$(sf "$B" leader); [ -n "$a" ] && [ "$a" != "0" ] && [ "$a" = "$b" ]; }
members_are() { [ "$(status_json "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(len(d.get("voters") or d.get("members") or []))')" = "$2" ]; }
# effective 只在 leader 上计算；每次重新探测，避免换届后拿旧 URL 轮询 follower 的 null。
leader_eff() { local b; for b in "$A" "$B"; do [ "$(sf "$b" state)" = leader ] && [ "$(sf "$b" effective_feature_level)" = "$1" ] && return 0; done; return 1; }
applied_index() { status_json "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print((d.get("applied") or {}).get("index",0))'; }

ADMIN_A=""; ADMIN_B=""

echo "== 1. 全旧集群起步：A(old, bootstrap) + B(old) + C(old witness)，配对并写 level-1 桶 =="
mk_cfg a "$A_HTTP" "$REG_A" "$TOK_A" 1 true
mk_cfg b "$B_HTTP" "$REG_B" "$TOK_B" 2 false
mk_witness_cfg c 3 "$C_HTTP"
ck start_a; ck start_b; ck start_c
AT=$(login "$A" "$ADMIN" "$APW"); BT=$(login "$B" "$ADMIN" "$APW")
ck [ -n "$AT" ]; ck [ -n "$BT" ]
AKEYS=$(mint "$A" "$AT"); BKEYS=$(mint "$B" "$BT")
A_AK=$(printf '%s' "$AKEYS" | json_get access_key); A_SK=$(printf '%s' "$AKEYS" | json_get secret_key)
B_AK=$(printf '%s' "$BKEYS" | json_get access_key); B_SK=$(printf '%s' "$BKEYS" | json_get secret_key)
A_ID=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get id)
A_ST=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get site_token)
B_ID=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get id)
B_ST=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get site_token)
ck [ "$(pair "$A" "$AT" "$B_ID" "$B" "$REG_B" "$B_AK" "$B_SK" "$B_ST")" = 200 ]
ck [ "$(pair "$B" "$BT" "$A_ID" "$A" "$REG_A" "$A_AK" "$A_SK" "$A_ST")" = 200 ]
ck poll_pred 60 leader_is "$A" 1
ck poll_pred 60 members_are "$A" 2
ck timeout 20 "$OLD_BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 3 --endpoint "$C"
ck poll_pred 60 members_are "$A" 3
BUCKET="rolling-$RANDOM"
# 控制台建桶成功返回 204（与 e2e_m9 / e2e_m9_recovery 同口径）。
ck [ "$(bucket_code "$A" "$AT" "$BUCKET")" = 204 ]
ck poll_pred 30 has_bucket "$B" "$BT" "$BUCKET"
pass "基线：全旧 3-voter 集群与 level-1 写入就绪"

echo "== 2. 换 B 为新二进制（follower 先行），追平后可读 =="
kill9 "$B_PID"; B_BIN="$NEW_BIN"; ck start_b
ck poll_pred 60 [ "$(sf "$B" wire_version)" = 2 ]
ck [ "$(sf "$B" feature_level)" = 1 ]
ck poll_pred 60 has_bucket "$B" "$BT" "$BUCKET"

echo "== 3. 停旧 leader A：新 B 当选并写入，旧 C 接受 append =="
kill9 "$A_PID"
ck poll_pred 90 leader_is "$B" 2
U="roll-$RANDOM"
ck [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$B/api/v1/users" -H "authorization: Bearer $BT" -H 'content-type: application/json' -d "{\"username\":\"$U\",\"password\":\"roll-pass-123\"}")" = 200 ]
ck [ "$(sf "$C" leader)" = 2 ]

echo "== 4. 重启旧 A：从新 leader 追平 level-1 日志 =="
A_BIN="$OLD_BIN"; ck start_a
ck poll_pred 90 has_bucket "$A" "$AT" "$BUCKET"

echo "== 5. A/C 换新：三端 wire=2 / feature=1 / activation=1 =="
kill9 "$A_PID"; A_BIN="$NEW_BIN"; ck start_a
kill9 "$C_PID"; C_BIN="$NEW_BIN"; ck start_c
for b in "$A" "$B" "$C"; do
  ck poll_pred 60 [ "$(sf "$b" wire_version)" = 2 ]
  ck [ "$(sf "$b" feature_level)" = 1 ]
  ck [ "$(sf "$b" activation_level)" = 1 ]
done

echo "== 6. 跨版本快照安装 =="
skip "步骤 6：无产品入口强制 build/purge 快照；OSSCTL1 跨版本安装由 store 单元测试覆盖（D6）"

echo "== 7. 逐台启用 feature_level=2；混合窗口内 effective 必须仍为 1，全 2 后为 2 =="
kill9 "$C_PID"; mk_witness_cfg c 3 "$C_HTTP" 2; C_BIN="$NEW_BIN"; ck start_c
kill9 "$B_PID"; mk_cfg b "$B_HTTP" "$REG_B" "$TOK_B" 2 false 2; B_BIN="$NEW_BIN"; ck start_b
# A 仍为 feature_level=1：leader 的生效级别必须停在 1（D4）。
ck poll_pred 90 any_leader
ck leader_eff 1
kill9 "$A_PID"; mk_cfg a "$A_HTTP" "$REG_A" "$TOK_A" 1 true 2; A_BIN="$NEW_BIN"; ck start_a
ck poll_pred 60 leader_eff 2
ck poll_pred 60 [ "$(sf "$A" activation_level)" = 1 ]
skip "步骤 7 子项：level-2 写入 503 feature_unavailable——发布二进制无 level-2 产品命令（D6）"

echo "== 8. 激活（level-2 命令） =="
skip "步骤 8：本版本无 level-2 产品命令，无法激活；marker 与 apply 门控由 store 单元测试覆盖（D6）"

echo "== 9. 激活前回滚演练：全停 → 全换回旧二进制（删除 feature_level 键）→ level-1 写 =="
stop_all
mk_cfg a "$A_HTTP" "$REG_A" "$TOK_A" 1 true
mk_cfg b "$B_HTTP" "$REG_B" "$TOK_B" 2 false
mk_witness_cfg c 3 "$C_HTTP"
A_BIN="$OLD_BIN"; B_BIN="$OLD_BIN"; C_BIN="$OLD_BIN"
ck start_a; ck start_b; ck start_c
ck poll_pred 90 any_leader
AT=$(login "$A" "$ADMIN" "$APW"); ck [ -n "$AT" ]
U2="rollback-$RANDOM"
ck [ "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$A/api/v1/users" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "{\"username\":\"$U2\",\"password\":\"roll-pass-123\"}")" = 200 ]
pass "激活前回滚：旧二进制启动并完成 level-1 写"

echo "== 10. 激活后回滚 / OSSCTL2 =="
skip "步骤 10：未激活（步骤 8）+ 本版本无 OSSCTL2（D6），无法演练；R8 启动守卫由单元测试覆盖"

echo "== 12. 负向：新 leader 拒绝把旧 witness 加入集群（409 before add_learner） =="
stop_all
mk_cfg a "$A_HTTP" "$REG_A" "$TOK_A" 1 true
mk_cfg b "$B_HTTP" "$REG_B" "$TOK_B" 2 false
mk_witness_cfg c 3 "$C_HTTP"
A_BIN="$NEW_BIN"; B_BIN="$NEW_BIN"; C_BIN="$OLD_BIN"
ck start_a; ck start_b; ck start_c
ck poll_pred 90 any_leader
mk_witness_cfg d 4 "$D_HTTP"
ck start_d
OUT=$(timeout 30 "$NEW_BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 4 --endpoint "$D" 2>&1); RC=$?
ck [ "$RC" -ne 0 ]
if printf '%s' "$OUT" | grep -q peer_incompatible; then
  pass "raft-add-witness 输出包含 peer_incompatible"
else
  fail "raft-add-witness 输出缺少 peer_incompatible: $OUT"
fi
ck [ "$(status_json "$A" | python3 -c 'import json,sys;d=json.load(sys.stdin);print(len(d.get("voters") or []))')" = 3 ]
pass "旧 peer 被 409 拒绝，成员表未变"

echo "== 11. DB 往返：新二进制写过的库由旧二进制启动并读取 =="
stop_all
mk_cfg a "$A_HTTP" "$REG_A" "$TOK_A" 1 true
A_BIN="$NEW_BIN"; ck start_a
AT=$(login "$A" "$ADMIN" "$APW"); ck [ -n "$AT" ]
ck has_bucket "$A" "$AT" "$BUCKET"
kill9 "$A_PID"
A_BIN="$OLD_BIN"; ck start_a
AT=$(login "$A" "$ADMIN" "$APW"); ck [ -n "$AT" ]
ck has_bucket "$A" "$AT" "$BUCKET"
pass "旧二进制可在新二进制写过的 DB 上启动并读回 level-1 数据"

finish
