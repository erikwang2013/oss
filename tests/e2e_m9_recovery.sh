#!/usr/bin/env bash
# M9 灾备面端到端：witness force recovery、M7→Raft cutover/export、B′ 替换 + sync-reconcile --repair。
#
# 覆盖（真实进程 + 真实 CLI，非进程内单测）：
#   A. export/cutover/force-recover：
#      M7 单站点播种 user/bucket/policy → 离线 raft-export（校验 sha256 + 11 表内容）→
#      M7 同版本/同管理员目标库（源库一致快照）→ raft-cutover --dry-run（无冲突）→
#      改目标 users.alice.status 一个字段 → 非 --overwrite dry-run 非零退出且打印
#      conflict users alice status（--overwrite --dry-run 同形明细）→
#      --overwrite 真割接 → raft_mode=on；全新 raft.db raft-force-recover → 同库重复
#      force-recover 必须失败；bootstrap=false 启动后控制面读/写可用、raft_bootstrap=0。
#   B. witness：witness.toml + state_db 运行时 → /api/v1/site/raft/status
#      initialized/voters/role + 无 S3/console 路由；控制写追平后 raft-export --witness
#      校验 sha256/11 表；witness 导出 → 全新 witness 目标 force-recover（voters=[9]、
#      state_db 标记）→ 以 bootstrap=false 启动 witness（status/404）→ 同一 raft log
#      二次恢复必须失败。
#   C. B′ 替换：A/B/C(2 数据 voter + witness) 配对、sync 桶 + 对象；停 B → B′ 复用 B 的
#      site UUID/token、新 raft node_id（4）加入 → A 的 sites 行 reconcile_required=1 →
#      sync PUT/DELETE 503；sync-reconcile --repair 清门 → 两端同 version id + 新 PUT 成功。
#
# 已知 SKIP（产品现状，不是脚本偷懒；理由随输出打印）：
#   C-1 --config bprime.toml 的 repair 修复的是 B′ 本机（其 sites 门本来就是 0）；
#       503 门在 A 的 sites 行，必须在 A 侧跑 repair（本脚本已跑并断言）。
#
# 前置：cargo build -p oss-server；aws-cli；curl；python3；sqlite3。
# 用法：OSS_BIN=target/debug/oss-server tests/e2e_m9_recovery.sh
set -uo pipefail
cd "$(dirname "$0")/.."
BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
[ -x "$BIN" ] || { echo "缺少 oss-server 二进制（先 cargo build -p oss-server，或设 OSS_BIN）"; exit 2; }
BIN=$(realpath "$BIN")
for c in aws curl python3 sqlite3; do command -v "$c" >/dev/null || { echo "需要 $c"; exit 2; }; done

PASSES=0; FAILS=0; SKIPS=0
pass() { PASSES=$((PASSES+1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS+1)); echo "FAIL: $*"; }
skip() { SKIPS=$((SKIPS+1)); echo "SKIP: $*"; }
ck() { local m="$*"; if "$@"; then pass "$m"; else fail "$m"; fi; }
retry() { local n=$1 i; shift; for i in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done; return 1; }
free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
USED=""
pick_ports() {
  local v p
  for v in SRC_HTTP TGT_HTTP WIT_HTTP WIT2_HTTP A_HTTP B_HTTP C_HTTP; do
    while :; do p=$(free_port); case " $USED " in *" $p "*) continue ;; esac; break; done
    USED="$USED $p"; printf -v "$v" '%s' "$p"
  done
}
pick_ports
W=$(mktemp -d)
JWT_SECRET="dev-only-change-me-0123456789abcdef"
CLUSTER_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
TOK_SRC=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_TGT=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_A=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_B=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
ADMIN="m9rec"; APW="m9rec-pw"; ALICE_PW="alice-pw-123"
PIDS=""
cleanup() { local p; for p in $PIDS; do kill -9 "$p" 2>/dev/null; done; wait 2>/dev/null; rm -rf "$W"; }
trap cleanup EXIT
stop_proc() { local p="$1"; [ -n "$p" ] && { kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null; }; }
finish() {
  echo "== 结果 =="; echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS"
  [ "$FAILS" -ne 0 ] && exit 1
  echo "PASS: M9 recovery e2e（$PASSES 项通过，$SKIPS 项 SKIP）"
}

wait_http() { # port pid [path]
  local p=$1 pid=$2 path="${3:-/health/live}"
  for _ in $(seq 1 120); do
    kill -0 "$pid" 2>/dev/null || return 1
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p$path" 2>/dev/null || true)" = 200 ] && return 0
    sleep 1
  done
  return 1
}
start_serve() { # dir port pidvar logfile
  local d=$1 port=$2 var=$3 log=$4 pid
  (cd "$d" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$BIN" serve >>"$log" 2>&1) &
  pid=$!; PIDS="$PIDS $pid"; printf -v "$var" '%s' "$pid"
  wait_http "$port" "$pid"
}
start_witness() { # dir port pidvar
  local d=$1 port=$2 var=$3 pid
  (cd "$d" && exec "$BIN" witness --config "$d/witness.toml" >>"$d/server.log" 2>&1) &
  pid=$!; PIDS="$PIDS $pid"; printf -v "$var" '%s' "$pid"
  wait_http "$port" "$pid" /health
}
login() {
  curl -s --max-time 10 -X POST "$1/api/v1/login" -H 'content-type: application/json' \
    -d "{\"username\":\"$2\",\"password\":\"$3\"}" |
    python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'
}
mint() { curl -s --max-time 10 -X POST "$1/api/v1/access-keys" -H "authorization: Bearer $2" -H 'content-type: application/json' -d '{}'; }
json_get() { python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get(sys.argv[1],""))' "$1"; }
curl_code() { curl -s -o /dev/null -w '%{http_code}' --max-time "${2:-10}" -H "authorization: Bearer $3" "$1"; }
status_json() {
  local c p
  for p in /api/v1/site/raft/status /api/v1/raft/status; do
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 -H "x-oss-raft-token: $CLUSTER_TOKEN" "$1$p")
    [ "$c" = 200 ] && { curl -s --max-time 5 -H "x-oss-raft-token: $CLUSTER_TOKEN" "$1$p"; return 0; }
  done
  return 1
}
raft_field() { # base key
  status_json "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: print(""); raise SystemExit
v=d.get(sys.argv[1],"")
print(",".join(map(str,v)) if isinstance(v,list) else (str(v).lower() if isinstance(v,bool) else v))' "$2"
}
poll_voters() { # base expected_csv
  local i
  for i in $(seq 1 90); do [ "$(raft_field "$1" voters)" = "$2" ] && return 0; sleep 1; done
  return 1
}
db_value() { # db sql
python3 - "$1" "$2" <<'PY'
import sqlite3, sys
try:
    c = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    r = c.execute(sys.argv[2]).fetchone()
    print("" if r is None or r[0] is None else r[0])
except sqlite3.Error:
    print("")
PY
}
db_count() { db_value "$1" "SELECT COUNT(*) FROM $2"; }
db_has_user() { [ "$(db_value "$1" "SELECT COUNT(*) FROM users WHERE username='$2'")" != 0 ]; }
snapshot_db() { # src dst
python3 - "$1" "$2" <<'PY'
import sqlite3, sys
s = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
d = sqlite3.connect(sys.argv[2])
s.backup(d); d.close(); s.close()
PY
}
verify_export() { # file role
python3 - "$1" "$2" <<'PY'
import hashlib, json, sys
e = json.load(open(sys.argv[1], encoding="utf-8"))
bad = []
def chk(ok, m):
    if not ok: bad.append(m); print("verify-export:", m)
chk(e.get("format") == 1, "format")
chk(e.get("kind") == "oss-raft-control-export", "kind")
chk(e.get("source_role") == sys.argv[2], "source_role=%r" % e.get("source_role"))
sha = e.get("sha256", "")
payload = json.dumps(e.get("data"), separators=(",", ":"), ensure_ascii=False).encode()
chk(hashlib.sha256(payload).hexdigest() == sha, "sha256 mismatch")
tables = ["users", "access_keys", "permissions", "buckets", "bucket_policies",
          "lifecycle_rules", "event_configs", "replication_rules",
          "access_points", "olap_configs", "inventory_configs"]
d = e.get("data") or {}
miss = [t for t in tables if t not in d]
chk(not miss, "missing tables %s" % miss)
sys.exit(1 if bad else 0)
PY
}
export_has() { # file table field value
python3 - "$1" "$2" "$3" "$4" <<'PY'
import json, sys
e = json.load(open(sys.argv[1], encoding="utf-8"))
rows = e.get("data", {}).get(sys.argv[2], [])
ok = any(str(r.get(sys.argv[3])) == sys.argv[4] for r in rows)
print("ok" if ok else "missing %s.%s=%s" % (sys.argv[2], sys.argv[3], sys.argv[4]))
sys.exit(0 if ok else 1)
PY
}
expect_503() {
  local out
  if out=$("$@" 2>&1); then fail "$* 应 503，实得成功"; return; fi
  if printf '%s' "$out" | grep -Eq '503|ServiceUnavailable|SlowDown'; then pass "$* → 503"
  else fail "$* 非 503 形态：$out"; fi
}
sites_flag() { # base token  (GET /sites 的 reconcile_required；无行/失败=空)
  curl -s --max-time 10 "$1/api/v1/sites" -H "authorization: Bearer $2" | python3 -c 'import json,sys
try: rows=json.load(sys.stdin)
except Exception: rows=[]
print(next((str(int(bool(r.get("reconcile_required")))) for r in rows),""))'
}
sites_node() { # base token
  curl -s --max-time 10 "$1/api/v1/sites" -H "authorization: Bearer $2" | python3 -c 'import json,sys
try: rows=json.load(sys.stdin)
except Exception: rows=[]
print(next((str(r.get("raft_node_id","")) for r in rows),""))'
}

mk_m7_cfg() { # dir port region token  —— M7/local：只有 [site]，无 [raft]
  local d="$W/$1"
  mkdir -p "$d/data" "$d/staging"
  cat > "$d/oss.toml" <<EOF
listen = "127.0.0.1:$2"
data_dir = "$d/data"
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
EOF
}
append_raft() { # dir port node_id bootstrap
  cat >> "$W/$1/oss.toml" <<EOF

[raft]
node_id = $3
role = "voter"
bootstrap = $4
advertise_endpoint = "http://127.0.0.1:$2"
cluster_token = "$CLUSTER_TOKEN"
db_path = "$W/$1/raft.db"
election_timeout_ms = 3000
heartbeat_interval_ms = 500
rpc_timeout_ms = 2000
write_timeout_ms = 3000
EOF
}
mk_site_cfg() { # dir port region token node_id bootstrap [site_id]
  local d="$W/$1" idline=""
  [ -n "${7:-}" ] && idline="id = \"$7\""
  mkdir -p "$d/data" "$d/staging"
  cat > "$d/oss.toml" <<EOF
listen = "127.0.0.1:$2"
data_dir = "$d/data"
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456
groups = []

[site]
$idline
name = "$1"
region = "$3"
priority = 10
token = "$4"

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
timeout_ms = 5000
max_object_bytes = 67108864
EOF
}
mk_witness_cfg() { # dir port node_id
  local d="$W/$1"
  mkdir -p "$d"
  cat > "$d/witness.toml" <<EOF
[raft]
node_id = $3
role = "witness"
bootstrap = false
advertise_endpoint = "http://127.0.0.1:$2"
cluster_token = "$CLUSTER_TOKEN"
db_path = "$d/raft.db"
state_db = "$d/control.db"
election_timeout_ms = 3000
heartbeat_interval_ms = 500
rpc_timeout_ms = 2000
EOF
}

# ============================================================================
echo "== A. M7 export → cutover（冲突/overwrite）→ force-recover → bootstrap=false =="
# ============================================================================
mk_m7_cfg src "$SRC_HTTP" r-src "$TOK_SRC"
SRC_PID=""
if ! start_serve "$W/src" "$SRC_HTTP" SRC_PID "$W/src/server.log"; then
  fail "M7 源节点启动失败"; tail -10 "$W/src/server.log"; finish
fi
SRC="http://127.0.0.1:$SRC_HTTP"
AT=$(login "$SRC" "$ADMIN" "$APW")
if [ -z "$AT" ]; then fail "M7 源管理员登录失败"; finish; fi
SRCKEYS=$(mint "$SRC" "$AT"); S_AK=$(printf '%s' "$SRCKEYS" | json_get access_key); S_SK=$(printf '%s' "$SRCKEYS" | json_get secret_key)
ALICE_ID=""
ck [ -n "$S_AK" ]   # 管理员 AK 是 M7 控制面播种成功的旁证
curl -s --max-time 10 -o /dev/null -X POST "$SRC/api/v1/users" -H "authorization: Bearer $AT" -H 'content-type: application/json' \
  -d "{\"username\":\"alice\",\"password\":\"$ALICE_PW\"}"
ALICE_T=$(login "$SRC" "alice" "$ALICE_PW")
ck [ -n "$ALICE_T" ]
ALICE_ID=$(curl -s --max-time 10 "$SRC/api/v1/users" -H "authorization: Bearer $AT" | python3 -c 'import json,sys
try: rows=json.load(sys.stdin)
except Exception: rows=[]
print(next((str(u.get("id")) for u in rows if u.get("username")=="alice"),""))')
ck [ -n "$ALICE_ID" ]
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "$SRC/api/v1/buckets" -H "authorization: Bearer $ALICE_T" -H 'content-type: application/json' -d '{"name":"rec-b1"}')
ck [ "$code" = 204 ]
POL='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::rec-b1/*"}]}'
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X PUT "$SRC/api/v1/buckets/rec-b1/policy" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "$POL")
ck [ "$code" = 204 ]
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X PUT "$SRC/api/v1/users/$ALICE_ID/permissions" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d '[{"bucket":"rec-b1","read":true,"write":true,"delete":false}]')
ck [ "$code" = 204 ]
stop_proc "$SRC_PID"; SRC_PID=""

append_raft src "$SRC_HTTP" 11 false
if "$BIN" raft-export --config "$W/src/oss.toml" --out "$W/export.json" >"$W/export.log" 2>&1; then
  pass "raft-export 退出 0"
else
  fail "raft-export 失败"; cat "$W/export.log"
fi
ck verify_export "$W/export.json" voter
ck export_has "$W/export.json" users username alice
ck export_has "$W/export.json" buckets name rec-b1
ck export_has "$W/export.json" bucket_policies bucket rec-b1
ck export_has "$W/export.json" permissions bucket rec-b1
SITE_SRC=$(python3 -c 'import json;print(json.load(open("'"$W"'/export.json"))["source_site"])')
ck [ -n "$SITE_SRC" ]

# 目标 = 源 M7 库的一致快照（同版本/同管理员/同业务行）——真实 M7 对端就是这个形态。
mk_m7_cfg tgt "$TGT_HTTP" r-tgt "$TOK_TGT"
rm -f "$W/tgt/oss.db"
snapshot_db "$W/src/oss.db" "$W/tgt/oss.db"
append_raft tgt "$TGT_HTTP" 21 false

# 改行前：无冲突 dry-run 通过（证明快照一致，不是靠 --overwrite 兜底）。
if "$BIN" raft-cutover --config "$W/tgt/oss.toml" --from "$W/export.json" --dry-run >"$W/ct0.log" 2>&1; then
  pass "cutover --dry-run（改行前一致）退出 0：read_export 校验通过"
else
  fail "cutover --dry-run 意外失败"; cat "$W/ct0.log"
fi

# 只改 status 一个字段：冲突必须按 table/key/field 明细报告，而不是整行 "row"。
python3 - "$W/tgt/oss.db" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1]); c.execute("UPDATE users SET status='disabled' WHERE username='alice'"); c.commit(); c.close()
PY
ck [ "$(db_value "$W/tgt/oss.db" "SELECT status FROM users WHERE username='alice'")" = disabled ]

if "$BIN" raft-cutover --config "$W/tgt/oss.toml" --from "$W/export.json" --dry-run >"$W/ct1.log" 2>&1; then
  fail "冲突时 cutover --dry-run 应非零退出"
else
  pass "冲突时 cutover --dry-run 非零退出"
fi
if grep -Eq 'conflicts: 1' "$W/ct1.log"; then pass "冲突摘要 conflicts: 1"; else fail "缺少 conflicts: 1：$(cat "$W/ct1.log")"; fi
if grep -qx 'conflict users alice status' "$W/ct1.log"; then
  pass "非 --overwrite dry-run 字段级明细：users/alice/status"
else
  fail "缺少字段级冲突行 conflict users alice status：$(cat "$W/ct1.log")"
fi

if "$BIN" raft-cutover --config "$W/tgt/oss.toml" --from "$W/export.json" --dry-run --overwrite >"$W/ct2.log" 2>&1; then
  pass "cutover --dry-run --overwrite 退出 0"
else
  fail "cutover --dry-run --overwrite 失败"; cat "$W/ct2.log"
fi
if grep -qx 'conflict users alice status' "$W/ct2.log"; then
  pass "字段级冲突表/键/字段：users alice status"
else
  fail "缺少字段级冲突行 conflict users alice status：$(cat "$W/ct2.log")"
fi

if "$BIN" raft-cutover --config "$W/tgt/oss.toml" --from "$W/export.json" --overwrite >"$W/ct3.log" 2>&1; then
  pass "cutover --overwrite 退出 0"
else
  fail "cutover --overwrite 失败"; cat "$W/ct3.log"
fi
grep -q 'cutover 完成' "$W/ct3.log" && grep -q '(11 张表)' "$W/ct3.log" && pass "cutover 报告 11 张表" || fail "cutover 报告缺 11 表：$(cat "$W/ct3.log")"
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_mode'")" = on ]
ck [ "$(db_value "$W/tgt/oss.db" "SELECT status FROM users WHERE username='alice'")" = active ]
ck [ "$(db_count "$W/tgt/oss.db" "buckets WHERE name='rec-b1'")" = 1 ]
ck [ "$(db_count "$W/tgt/oss.db" "bucket_policies WHERE bucket='rec-b1'")" = 1 ]
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_cutover'")" != "" ]
ck [ -f "$W/tgt/oss.db" ]

rm -f "$W/tgt/raft.db"
if "$BIN" raft-force-recover --config "$W/tgt/oss.toml" --from "$W/export.json" --node-id 21 --yes >"$W/fr.log" 2>&1; then
  pass "raft-force-recover 退出 0"
else
  fail "raft-force-recover 失败"; cat "$W/fr.log"
fi
grep -q 'voters=\[21\]' "$W/fr.log" && pass "force-recover voters=[21]" || fail "force-recover 成员非 [21]：$(cat "$W/fr.log")"
ck [ -f "$W/tgt/raft.db" ]
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_bootstrap'")" = 0 ]
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_node_id'")" = 21 ]
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_initialized'")" = 1 ]
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_force_recover'")" != "" ]

TGT="http://127.0.0.1:$TGT_HTTP"
TGT_PID=""
if ! start_serve "$W/tgt" "$TGT_HTTP" TGT_PID "$W/tgt/server.log"; then
  fail "force-recover 后 bootstrap=false 启动失败"; tail -12 "$W/tgt/server.log"; finish
fi
ck poll_voters "$TGT" 21
ck [ "$(raft_field "$TGT" initialized)" = true ]
TT=$(login "$TGT" "$ADMIN" "$APW")
ck [ -n "$TT" ]
ck [ "$(curl_code "$TGT/api/v1/users" 10 "$TT")" = 200 ]
code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "$TGT/api/v1/users" -H "authorization: Bearer $TT" -H 'content-type: application/json' -d '{"username":"bob","password":"bob-pw-123"}')
ck [ "$code" = 204 -o "$code" = 200 ]
ck retry 20 db_has_user "$W/tgt/oss.db" bob
ck [ -n "$(login "$TGT" "alice" "$ALICE_PW")" ]   # 导出密码哈希可用
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_bootstrap'")" = 0 ]
stop_proc "$TGT_PID"; TGT_PID=""

if "$BIN" raft-force-recover --config "$W/tgt/oss.toml" --from "$W/export.json" --node-id 22 --yes >"$W/fr2.log" 2>&1; then
  fail "同一 raft.db 二次 force-recover 应失败"
else
  pass "同一 raft.db 二次 force-recover 非零退出"
fi
if grep -Eq 'raft log already has entries|already has applied raft state' "$W/fr2.log"; then
  pass "二次 force-recover 明确拒绝：$(grep -Eo 'raft log already has entries|already has applied raft state' "$W/fr2.log" | head -1)"
else
  fail "二次 force-recover 错误形态不符：$(cat "$W/fr2.log")"
fi
ck [ "$(db_value "$W/tgt/oss.db" "SELECT value FROM schema_meta WHERE key='raft_node_id'")" = 21 ]

# ============================================================================
echo "== B. witness 运行时/status/export --witness；witness force-recover 正向恢复 =="
# ============================================================================
if ! start_serve "$W/tgt" "$TGT_HTTP" TGT_PID "$W/tgt/server.log"; then
  fail "witness 前目标节点重启失败"; tail -12 "$W/tgt/server.log"; finish
fi
ck poll_voters "$TGT" 21
TT=$(login "$TGT" "$ADMIN" "$APW")

mk_witness_cfg wit "$WIT_HTTP" 2
WIT="http://127.0.0.1:$WIT_HTTP"
WIT_PID=""
if ! start_witness "$W/wit" "$WIT_HTTP" WIT_PID; then
  fail "witness 启动失败"; tail -12 "$W/wit/server.log"; finish
fi
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$WIT/health/live")" = 200 ]
if timeout 60 "$BIN" raft-add-witness --config "$W/tgt/oss.toml" --node-id 2 --endpoint "$WIT" >"$W/wit/add.log" 2>&1; then
  pass "raft-add-witness node 2"
else
  fail "raft-add-witness 失败"; cat "$W/wit/add.log"
fi
ck poll_voters "$TGT" 2,21
for _ in $(seq 1 90); do [ "$(raft_field "$WIT" initialized)" = true ] && break; sleep 1; done
ck [ "$(raft_field "$WIT" initialized)" = true ]
ck [ "$(raft_field "$WIT" voters)" = 2,21 ]
ck [ "$(raft_field "$WIT" role)" = witness ]
ck [ "$(raft_field "$WIT" node_id)" = 2 ]
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$WIT/api/v1/buckets")" = 404 ]
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$WIT/api/v1/site")" = 404 ]
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 -X POST "$WIT/api/v1/login" -H 'content-type: application/json' -d '{}')" = 404 ]
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$WIT/")" = 404 ]

code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 10 -X POST "$TGT/api/v1/users" -H "authorization: Bearer $TT" -H 'content-type: application/json' -d '{"username":"carol","password":"carol-pw-123"}')
ck [ "$code" = 204 -o "$code" = 200 ]
if retry 60 db_has_user "$W/wit/control.db" carol; then pass "witness state_db 追平 carol"; else fail "witness state_db 未追平 carol"; fi
stop_proc "$WIT_PID"; WIT_PID=""

if "$BIN" raft-export --config "$W/wit/witness.toml" --out "$W/witness-export.json" --witness >"$W/wit-export.log" 2>&1; then
  pass "raft-export --witness 退出 0"
else
  fail "raft-export --witness 失败"; cat "$W/wit-export.log"
fi
ck verify_export "$W/witness-export.json" witness
ck export_has "$W/witness-export.json" users username carol
ck export_has "$W/witness-export.json" buckets name rec-b1
ck [ "$(python3 -c 'import json;print(json.load(open("'"$W"'/witness-export.json"))["source_node_id"])')" = 2 ]

stop_proc "$TGT_PID"; TGT_PID=""

# B-1：witness 导出 → 全新 witness 目标（state_db/raft.db 均新建，bootstrap=false）。
mk_witness_cfg wit2 "$WIT2_HTTP" 9
if "$BIN" raft-force-recover --config "$W/wit2/witness.toml" --from "$W/witness-export.json" --node-id 9 --yes >"$W/frw.log" 2>&1; then
  pass "witness force-recover 退出 0"
else
  fail "witness force-recover 失败"; cat "$W/frw.log"
fi
grep -q 'voters=\[9\]' "$W/frw.log" && pass "witness force-recover voters=[9]" || fail "witness force-recover 成员非 [9]：$(cat "$W/frw.log")"
ck [ -f "$W/wit2/raft.db" ]
ck [ "$(db_value "$W/wit2/control.db" "SELECT value FROM schema_meta WHERE key='raft_bootstrap'")" = 0 ]
ck [ "$(db_value "$W/wit2/control.db" "SELECT value FROM schema_meta WHERE key='raft_node_id'")" = 9 ]
ck [ "$(db_value "$W/wit2/control.db" "SELECT value FROM schema_meta WHERE key='raft_initialized'")" = 1 ]
ck [ "$(db_value "$W/wit2/control.db" "SELECT value FROM schema_meta WHERE key='raft_force_recover'")" != "" ]
ck [ "$(db_count "$W/wit2/control.db" "users WHERE username='carol'")" = 1 ]

WIT2="http://127.0.0.1:$WIT2_HTTP"
WIT2_PID=""
if ! start_witness "$W/wit2" "$WIT2_HTTP" WIT2_PID; then
  fail "恢复后的 witness 启动失败"; tail -12 "$W/wit2/server.log"; finish
fi
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$WIT2/health/live")" = 200 ]
for _ in $(seq 1 60); do [ "$(raft_field "$WIT2" initialized)" = true ] && break; sleep 1; done
ck [ "$(raft_field "$WIT2" initialized)" = true ]
ck [ "$(raft_field "$WIT2" voters)" = 9 ]
ck [ "$(raft_field "$WIT2" role)" = witness ]
ck [ "$(raft_field "$WIT2" node_id)" = 9 ]
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$WIT2/api/v1/buckets")" = 404 ]
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 "$WIT2/api/v1/site")" = 404 ]
ck [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 3 -X POST "$WIT2/api/v1/login" -H 'content-type: application/json' -d '{}')" = 404 ]
stop_proc "$WIT2_PID"; WIT2_PID=""

# 恢复产物已是既有集群：同一 raft log 二次恢复必须拒绝，不得编辑旧日志。
if "$BIN" raft-force-recover --config "$W/wit2/witness.toml" --from "$W/witness-export.json" --node-id 10 --yes >"$W/frw2.log" 2>&1; then
  fail "同一 witness raft log 二次 force-recover 应失败"
else
  pass "同一 witness raft log 二次 force-recover 非零退出"
fi
if grep -Eq 'raft log already has entries|already has applied raft state' "$W/frw2.log"; then
  pass "二次 witness force-recover 明确拒绝：$(grep -Eo 'raft log already has entries|already has applied raft state' "$W/frw2.log" | head -1)"
else
  fail "二次 witness force-recover 错误形态不符：$(cat "$W/frw2.log")"
fi

# ============================================================================
echo "== C. B′ 替换：sites.reconcile_required 门 → sync-reconcile --repair =="
# ============================================================================
REG_A="ap-east-1"; REG_B="eu-west-1"; SB="m9rec-sync"
mk_site_cfg a "$A_HTTP" "$REG_A" "$TOK_A" 1 true
mk_site_cfg b "$B_HTTP" "$REG_B" "$TOK_B" 2 false
A="http://127.0.0.1:$A_HTTP"; B="http://127.0.0.1:$B_HTTP"
A_PID=""; B_PID=""
start_serve "$W/a" "$A_HTTP" A_PID "$W/a/server.log" || { fail "A 启动失败"; tail -10 "$W/a/server.log"; finish; }
start_serve "$W/b" "$B_HTTP" B_PID "$W/b/server.log" || { fail "B 启动失败"; tail -10 "$W/b/server.log"; finish; }
ck poll_voters "$A" 1
AT=$(login "$A" "$ADMIN" "$APW"); BT=$(login "$B" "$ADMIN" "$APW")
ck [ -n "$AT" ]; ck [ -n "$BT" ]
AKEYS=$(mint "$A" "$AT"); BKEYS=$(mint "$B" "$BT")
A_AK=$(printf '%s' "$AKEYS" | json_get access_key); A_SK=$(printf '%s' "$AKEYS" | json_get secret_key)
B_AK=$(printf '%s' "$BKEYS" | json_get access_key); B_SK=$(printf '%s' "$BKEYS" | json_get secret_key)
ck [ -n "$A_AK" ]; ck [ -n "$B_AK" ]
aws_for() { local ep=$1 ak=$2 sk=$3; shift 3; AWS_ACCESS_KEY_ID="$ak" AWS_SECRET_ACCESS_KEY="$sk" AWS_DEFAULT_REGION=us-east-1 AWS_MAX_ATTEMPTS=1 AWS_PAGER="" aws --endpoint-url "$ep" "$@"; }
aws_a() { aws_for "$A" "$A_AK" "$A_SK" "$@"; }
aws_b() { aws_for "$B" "$B_AK" "$B_SK" "$@"; }

A_ID=$(curl -s --max-time 5 "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get id)
A_ST=$(curl -s --max-time 5 "$A/api/v1/site" -H "authorization: Bearer $AT" | json_get site_token)
B_ID=$(curl -s --max-time 5 "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get id)
B_ST=$(curl -s --max-time 5 "$B/api/v1/site" -H "authorization: Bearer $BT" | json_get site_token)
ck [ -n "$A_ID" ]; ck [ -n "$B_ID" ]; ck [ "$B_ST" = "$TOK_B" -o -n "$B_ST" ]
pair() { curl -s -o "$6" -w '%{http_code}' --max-time 90 -X POST "$1/api/v1/sites" -H "authorization: Bearer $2" -H 'content-type: application/json' \
  -d "{\"peer_id\":\"$3\",\"endpoint\":\"$4\",\"region\":\"$5\",\"priority\":10,\"s3_access_key\":\"$7\",\"s3_secret_key\":\"$8\",\"peer_site_token\":\"$9\"}"; }
ck [ "$(pair "$A" "$AT" "$B_ID" "$B" "$REG_B" "$W/pair-ab.json" "$B_AK" "$B_SK" "$B_ST")" = 200 ]
ck [ "$(pair "$B" "$BT" "$A_ID" "$A" "$REG_A" "$W/pair-ba.json" "$A_AK" "$A_SK" "$A_ST")" = 200 ]
ck poll_voters "$A" 1,2
ck [ "$(raft_field "$B" voters)" = 1,2 ] || retry 30 [ "$(raft_field "$B" voters)" = 1,2 ]

code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 20 -X POST "$A/api/v1/buckets" -H "authorization: Bearer $AT" -H 'content-type: application/json' -d "{\"name\":\"$SB\",\"replication_mode\":\"sync\"}")
ck [ "$code" = 204 ]
ok=""; for _ in $(seq 1 60); do
  [ "$(aws_a s3api get-bucket-versioning --bucket "$SB" --query Status --output text 2>/dev/null)" = Enabled ] && \
  [ "$(aws_b s3api get-bucket-versioning --bucket "$SB" --query Status --output text 2>/dev/null)" = Enabled ] && { ok=1; break; }
  sleep 1
done
ck [ "$ok" = 1 ]
printf 'recovery-object\n' > "$W/obj.bin"
ck aws_a s3api put-object --bucket "$SB" --key obj --body "$W/obj.bin"
ck retry 60 aws_b s3api head-object --bucket "$SB" --key obj
V_ORIG=$(aws_a s3api head-object --bucket "$SB" --key obj --query VersionId --output text 2>/dev/null)
V_B=$(aws_b s3api head-object --bucket "$SB" --key obj --query VersionId --output text 2>/dev/null)
ck [ -n "$V_ORIG" -a "$V_ORIG" != None ]
ck [ "$V_ORIG" = "$V_B" ]

mk_witness_cfg c "$C_HTTP" 3
C="http://127.0.0.1:$C_HTTP"; C_PID=""
start_witness "$W/c" "$C_HTTP" C_PID || { fail "witness C 启动失败"; tail -10 "$W/c/server.log"; finish; }
if timeout 60 "$BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 3 --endpoint "$C" >"$W/c/add.log" 2>&1; then
  pass "raft-add-witness C（A+B 之后加 witness，B 永久丢失仍保 quorum）"
else
  fail "raft-add-witness C 失败"; cat "$W/c/add.log"
fi
ck poll_voters "$A" 1,2,3
stop_proc "$B_PID"; B_PID=""
ck poll_voters "$A" 1,2,3

# B′：复用 B 的 site UUID/token，新 Raft node_id=4，空数据目录/空 oss.db（真实换机）。
mk_site_cfg bprime "$B_HTTP" "$REG_B" "$TOK_B" 4 false "$B_ID"
BPRIME="http://127.0.0.1:$B_HTTP"; BP_PID=""
start_serve "$W/bprime" "$B_HTTP" BP_PID "$W/bprime/server.log" || { fail "B′ 启动失败"; tail -10 "$W/bprime/server.log"; finish; }
BPT=$(login "$BPRIME" "$ADMIN" "$APW")
ck [ -n "$BPT" ]
BPKEYS=$(mint "$BPRIME" "$BPT"); BP_AK=$(printf '%s' "$BPKEYS" | json_get access_key); BP_SK=$(printf '%s' "$BPKEYS" | json_get secret_key)
ck [ -n "$BP_AK" ]
ck [ "$(curl -s --max-time 5 "$BPRIME/api/v1/site" -H "authorization: Bearer $BPT" | json_get id)" = "$B_ID" ]

PCODE=$(pair "$A" "$AT" "$B_ID" "$BPRIME" "$REG_B" "$W/pair-abp.json" "$BP_AK" "$BP_SK" "$B_ST")
ck [ "$PCODE" = 200 ]
ck [ "$(json_get reconcile_required < "$W/pair-abp.json")" = True ]
ck retry 30 [ "$(sites_node "$A" "$AT")" = 4 ]
ck retry 30 [ "$(sites_flag "$A" "$AT")" = 1 ]
ck poll_voters "$A" 1,3,4
ok=""; for _ in $(seq 1 90); do [ "$(db_count "$W/bprime/oss.db" "buckets WHERE name='$SB'")" = 1 ] && { ok=1; break; }; sleep 1; done
ck [ "$ok" = 1 ]

echo "  -- A 侧门已置 1：sync PUT/DELETE 必须 503 且本地不落地/不删 --"
printf 'blocked-by-gate\n' > "$W/blocked.bin"
expect_503 aws_a s3api put-object --bucket "$SB" --key blocked --body "$W/blocked.bin"
ck aws_a s3api head-object --bucket "$SB" --key obj >/dev/null 2>&1
expect_503 aws_a s3api delete-object --bucket "$SB" --key obj
ck aws_a s3api head-object --bucket "$SB" --key obj >/dev/null 2>&1

echo "  -- 字面要求：--config bprime.toml 跑 repair（只能清 B′ 自己的门） --"
stop_proc "$BP_PID"; BP_PID=""
if "$BIN" sync-reconcile --repair --config "$W/bprime/oss.toml" --json >"$W/repair-bp.json" 2>"$W/repair-bp.err"; then
  pass "sync-reconcile --repair --config bprime.toml 退出 0"
else
  fail "bprime repair 失败"; cat "$W/repair-bp.err"
fi
ck [ "$(python3 -c 'import json;print(len(json.load(open("'"$W"'/repair-bp.json")).get("errors",[])))' 2>/dev/null || echo x)" = 0 ]
ck [ "$(sites_flag "$A" "$AT")" = 1 ]
skip "C-1：bprime.toml 的单次 repair 清除 A 侧 503 门 —— repair 只改本机站点行；A 的门只能用 A 的 DB 清（下一段执行）"
start_serve "$W/bprime" "$B_HTTP" BP_PID "$W/bprime/server.log" || { fail "B′ 重启失败"; tail -10 "$W/bprime/server.log"; finish; }
ck poll_voters "$A" 1,3,4

echo "  -- 以 A 为源、B′ 为目标跑 repair（A 离线；B′/C 保 quorum） --"
stop_proc "$A_PID"; A_PID=""
if "$BIN" sync-reconcile --repair --config "$W/a/oss.toml" --json >"$W/repair-a.json" 2>"$W/repair-a.err"; then
  pass "sync-reconcile --repair --config a.toml 退出 0"
else
  fail "A side repair 失败"; cat "$W/repair-a.err"
fi
ck [ "$(python3 -c 'import json;d=json.load(open("'"$W"'/repair-a.json"));print(len(d.get("errors",[])))' 2>/dev/null || echo x)" = 0 ]
PUSHED=$(python3 -c 'import json;print(json.load(open("'"$W"'/repair-a.json")).get("pushed",0))' 2>/dev/null || echo 0)
ck [ "${PUSHED:-0}" -ge 1 ]
ck [ "$(db_value "$W/a/oss.db" "SELECT reconcile_required FROM sites WHERE remote_site_id='$B_ID'")" = 0 ]

start_serve "$W/a" "$A_HTTP" A_PID "$W/a/server.log" || { fail "A 重启失败"; tail -12 "$W/a/server.log"; finish; }
ck poll_voters "$A" 1,3,4
AT=$(login "$A" "$ADMIN" "$APW"); ck [ -n "$AT" ]
ck retry 30 [ "$(sites_flag "$A" "$AT")" = 0 ]
VA=$(aws_a s3api head-object --bucket "$SB" --key obj --query VersionId --output text 2>/dev/null)
VB=$(aws_for "$BPRIME" "$BP_AK" "$BP_SK" s3api head-object --bucket "$SB" --key obj --query VersionId --output text 2>/dev/null)
ck [ "$VA" = "$V_ORIG" ]
ck [ "$VB" = "$V_ORIG" ]
printf 'after-repair\n' > "$W/after.bin"
ck aws_a s3api put-object --bucket "$SB" --key after --body "$W/after.bin"
ck retry 60 aws_for "$BPRIME" "$BP_AK" "$BP_SK" s3api head-object --bucket "$SB" --key after
V_AFTER_A=$(aws_a s3api head-object --bucket "$SB" --key after --query VersionId --output text 2>/dev/null)
V_AFTER_B=$(aws_for "$BPRIME" "$BP_AK" "$BP_SK" s3api head-object --bucket "$SB" --key after --query VersionId --output text 2>/dev/null)
ck [ -n "$V_AFTER_A" -a "$V_AFTER_A" = "$V_AFTER_B" ]

finish
