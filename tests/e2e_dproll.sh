#!/usr/bin/env bash
# 数据面滚动升级 e2e（gap ①）：A/B 两个数据站点 + C witness（M9 [site]+[raft] 拓扑）。
# 逐台 TERM → 同配置重启；每次重启前后用 **sync 桶**核对数据面结果不变：
# PUT/GET、ListObjects V1 两页续传（marker）、ListObjects V2 两页（continuation-token）、
# MPU init/upload/complete 成品、版本化删除标记 + 旧版本定向读。
# 停机窗口同时断言：读本地副本仍 200；sync 写 fail-closed 503；控制面由剩余两票
# 继续提交（建桶），重启节点追平该写入。
#
# 已知边界（按 docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md §11.3）：
# - 本脚本是**同版本**滚动 MVP。混版数据面不在本脚本：Level B 只保证控制面命令/快照
#   兼容；"M3/M4 数据面滚动升级仍要求停写/滚动重启，Level C 任意双向混合版本未做"，
#   DP-0 混版握手仅覆盖 M3 mesh（503 peer_incompatible），M9 sync 跨站协议无跨版本承诺。
#   因此这里不接 OSS_OLD_BIN——双二进制控制面滚动见 tests/e2e_m9_rolling.sh。
# - sync 桶写是 remote-first（无降级、无后台重试）：唯一数据 peer 停机时 PUT/DELETE
#   必须 503；GET/List 读本地副本。滚动窗口语义 = 读不断、写暂时 fail-closed。
#
# 前置：target/debug/oss-server（或 OSS_BIN）、curl >= 7.75（--aws-sigv4）、python3。
# 用法：bash tests/e2e_dproll.sh
set -uo pipefail
cd "$(dirname "$0")/.."

BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
[ -x "$BIN" ] || { echo "缺少 oss-server：$BIN（先 cargo build -p oss-server，或设 OSS_BIN）"; exit 2; }
BIN=$(realpath "$BIN")
for c in curl python3; do command -v "$c" >/dev/null || { echo "需要 $c"; exit 2; }; done
curl --help all 2>/dev/null | grep -q aws-sigv4 || { echo "需要 curl >= 7.75（--aws-sigv4）"; exit 2; }

PASSES=0; FAILS=0
pass() { PASSES=$((PASSES+1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS+1)); echo "FAIL: $*"; }
ckm() { local m=$1; shift; if "$@"; then pass "$m"; else fail "$m"; fi; }
finish() {
  echo "== 结果 =="; echo "PASS=$PASSES FAIL=$FAILS"
  [ "$FAILS" -ne 0 ] && exit 1
  echo "PASS: 数据面滚动升级 e2e（$PASSES 项通过）"
  exit 0
}

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
USED=""
pick() { local p; while :; do p=$(free_port); case " $USED " in *" $p "*) continue ;; esac; break; done; USED="$USED $p"; echo "$p"; }
A_HTTP=$(pick); B_HTTP=$(pick); C_HTTP=$(pick)
A="http://127.0.0.1:$A_HTTP"; B="http://127.0.0.1:$B_HTTP"; C="http://127.0.0.1:$C_HTTP"
W=$(mktemp -d)
JWT_SECRET="dproll-e2e-jwt-0123456789abcdef0"
CLUSTER_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
TOK_A=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
TOK_B=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
ADMIN="dprolladmin"; APW="dprolladmin-pw"
BUCKET="dproll-$RANDOM"
A_PID=""; B_PID=""; C_PID=""
cleanup() { for p in "$A_PID" "$B_PID" "$C_PID"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done; rm -rf "$W"; }
trap cleanup EXIT

cat > "$W/s3xml.py" <<'PY'
import sys, xml.etree.ElementTree as ET
r = ET.parse(sys.argv[2]).getroot()
for e in r.iter():
    e.tag = e.tag.split('}')[-1]
def t(p):
    e = r.find(p); return "" if e is None or e.text is None else e.text
m = sys.argv[1]
if m == "uploadid":
    print(t(".//UploadId"))
elif m == "v1":
    print(t(".//IsTruncated") or "false"); print(" ".join(e.text or "" for e in r.findall(".//Contents/Key")))
elif m == "v2":
    print(t(".//IsTruncated") or "false"); print(" ".join(e.text or "" for e in r.findall(".//Contents/Key")))
    print(t(".//NextContinuationToken"))
elif m == "versions":
    print(" ".join(e.text or "" for e in r.findall(".//Version/VersionId")))
    print(" ".join(e.text or "" for e in r.findall(".//DeleteMarker/VersionId")))
else:
    sys.exit("bad mode")
PY
enc() { python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1],safe=""))' "$1"; }
hdr() { tr -d '\r' < "$2" | awk -F': ' -v k="$1" 'tolower($1)==tolower(k){print $2; exit}'; }
poll() { local n=$1 i; shift; for i in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done; return 1; }

# A/B 数据站点配置；C 为 witness（只有 [raft]）。
mk_cfg() { # dir port region token node_id bootstrap
  local d="$W/$1"
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

[sync]
timeout_ms = 5000
max_object_bytes = 67108864
EOF
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
wait_http() { local p=$1 pid=$2; for _ in $(seq 1 90); do kill -0 "$pid" 2>/dev/null || return 1; [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p/health/live" 2>/dev/null || true)" = 200 ] && return 0; sleep 1; done; return 1; }
wait_any() { local p=$1 pid=$2 c; for _ in $(seq 1 90); do kill -0 "$pid" 2>/dev/null || return 1; for path in /health/live /health; do c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p$path" 2>/dev/null || true); [ "$c" = 200 ] && return 0; done; sleep 1; done; return 1; }
start_a() { (cd "$W/a" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$BIN" serve >>"$W/a/server.log" 2>&1) & A_PID=$!; wait_http "$A_HTTP" "$A_PID"; }
start_b() { (cd "$W/b" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$BIN" serve >>"$W/b/server.log" 2>&1) & B_PID=$!; wait_http "$B_HTTP" "$B_PID"; }
start_c() { (cd "$W/c" && exec "$BIN" witness --config "$W/c/witness.toml" >>"$W/c/server.log" 2>&1) & C_PID=$!; wait_any "$C_HTTP" "$C_PID"; }
# TERM → 等退出（滚动升级必须优雅停；10s 不退视为失败并 SIGKILL 兜底）。
stop_node() { local pid=$1 i; [ -z "$pid" ] && return 0; kill -TERM "$pid" 2>/dev/null || return 0
  for i in $(seq 1 40); do kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null; return 0; }; sleep 0.25; done
  kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; return 1; }

status() { curl -s --max-time 5 -H "x-oss-raft-token: $CLUSTER_TOKEN" "$1/api/v1/site/raft/status"; }
sf() { status "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
v=d.get(sys.argv[1],"");print("" if v is None else v)' "$2"; }
voters() { status "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(len(d.get("voters") or []))'; }
same_leader() { local a b; a=$(sf "$1" leader); b=$(sf "$2" leader); [ -n "$a" ] && [ "$a" != 0 ] && [ "$a" = "$b" ]; }
ab_ready() { same_leader "$A" "$B" && [ "$(voters "$A")" = 3 ] && [ "$(voters "$B")" = 3 ]; }
leader_is() { [ "$(sf "$1" leader)" = "$2" ]; }
witness_follows() { local l; l=$(sf "$A" leader); [ -n "$l" ] && [ "$l" != 0 ] && [ "$(sf "$C" leader)" = "$l" ]; }

login() { curl -s --max-time 10 -X POST "$1/api/v1/login" -H 'content-type: application/json' -d "{\"username\":\"$2\",\"password\":\"$3\"}" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'; }
mint() { curl -s --max-time 10 -X POST "$1/api/v1/access-keys" -H "authorization: Bearer $2" -H 'content-type: application/json' -d '{}'; }
jget() { python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get(sys.argv[1],""))' "$1"; }
pair() { curl -s --max-time 15 -o /dev/null -w '%{http_code}' -X POST "$1/api/v1/sites" -H "authorization: Bearer $2" -H 'content-type: application/json' -d "{\"peer_id\":\"$3\",\"endpoint\":\"$4\",\"region\":\"$5\",\"priority\":10,\"s3_access_key\":\"$6\",\"s3_secret_key\":\"$7\",\"peer_site_token\":\"$8\"}"; }
mk_bucket() { curl -s --max-time 10 -o /dev/null -w '%{http_code}' -X POST "$1/api/v1/buckets" -H "authorization: Bearer $2" -H 'content-type: application/json' -d "{\"name\":\"$3\"${4:+,\"replication_mode\":\"$4\"}}"; }
has_bucket() { curl -s --max-time 10 "$1/api/v1/buckets" -H "authorization: Bearer $2" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
items=d if isinstance(d,list) else d.get("buckets",[])
print(any((b.get("name") if isinstance(b,dict) else b)==sys.argv[1] for b in items))' "$3" | grep -qx True; }

# S3 数据面（curl --aws-sigv4）：body → $out，响应头 → $out.hdr，stdout 为 HTTP 码。
s3() { # site method path out [curl args...]
  local s=$1 m=$2 p=$3 out=$4 ep ak sk; shift 4
  case "$s" in
    A) ep=$A; ak=$A_AK; sk=$A_SK ;;
    B) ep=$B; ak=$B_AK; sk=$B_SK ;;
  esac
  curl -sS --max-time 90 -o "$out" -D "$out.hdr" -w '%{http_code}' -X "$m" \
    --aws-sigv4 "aws:amz:us-east-1:s3" --user "$ak:$sk" "$@" "$ep$p"
}

VER_VID=""; DMARK=""
# 一次数据面全量核对：GET/MPU 字节、List V1 两页、List V2 两页、删除标记/旧版本。
verify_data() { # site label
  local s=$1 lbl=$2 code m
  code=$(s3 "$s" GET "/$BUCKET/obj.bin" "$W/v.obj")
  if [ "$code" = 200 ] && cmp -s "$W/obj.bin" "$W/v.obj"; then pass "$lbl: GET obj.bin 字节一致"; else fail "$lbl: GET obj.bin code=$code 不一致"; fi
  code=$(s3 "$s" GET "/$BUCKET/mpu.bin" "$W/v.mpu")
  if [ "$code" = 200 ] && cmp -s "$W/mpu.bin" "$W/v.mpu"; then pass "$lbl: MPU 成品字节一致"; else fail "$lbl: MPU 成品 code=$code 不一致"; fi
  code=$(s3 "$s" GET "/$BUCKET?prefix=list%2F&max-keys=2" "$W/v1a")
  mapfile -t P1 < <(python3 "$W/s3xml.py" v1 "$W/v1a" 2>/dev/null)
  if [ "$code" = 200 ] && [ "${P1[0]:-}" = true ] && [ "${P1[1]:-}" = "list/a list/b" ]; then pass "$lbl: ListV1 第 1 页（截断 2 键）"; else fail "$lbl: ListV1 第 1 页 code=$code trunc=${P1[0]:-} keys=${P1[1]:-}"; fi
  m=${P1[1]:-}; m=${m##* }
  code=$(s3 "$s" GET "/$BUCKET?prefix=list%2F&max-keys=2&marker=$(enc "$m")" "$W/v1b")
  mapfile -t P2 < <(python3 "$W/s3xml.py" v1 "$W/v1b" 2>/dev/null)
  if [ "$code" = 200 ] && [ "${P2[0]:-}" = false ] && [ "${P2[1]:-}" = "list/c list/d" ]; then pass "$lbl: ListV1 marker 续页"; else fail "$lbl: ListV1 续页 code=$code trunc=${P2[0]:-} keys=${P2[1]:-}"; fi
  code=$(s3 "$s" GET "/$BUCKET?list-type=2&prefix=list%2F&max-keys=2" "$W/v2a")
  mapfile -t Q1 < <(python3 "$W/s3xml.py" v2 "$W/v2a" 2>/dev/null)
  if [ "$code" = 200 ] && [ "${Q1[0]:-}" = true ] && [ "${Q1[1]:-}" = "list/a list/b" ] && [ -n "${Q1[2]:-}" ]; then pass "$lbl: ListV2 第 1 页带 token"; else fail "$lbl: ListV2 第 1 页 code=$code trunc=${Q1[0]:-} keys=${Q1[1]:-} token=${Q1[2]:-}"; fi
  code=$(s3 "$s" GET "/$BUCKET?list-type=2&prefix=list%2F&max-keys=2&continuation-token=$(enc "${Q1[2]:-}")" "$W/v2b")
  mapfile -t Q2 < <(python3 "$W/s3xml.py" v2 "$W/v2b" 2>/dev/null)
  if [ "$code" = 200 ] && [ "${Q2[0]:-}" = false ] && [ "${Q2[1]:-}" = "list/c list/d" ]; then pass "$lbl: ListV2 continuation-token 续页"; else fail "$lbl: ListV2 续页 code=$code trunc=${Q2[0]:-} keys=${Q2[1]:-}"; fi
  code=$(s3 "$s" GET "/$BUCKET?versions&prefix=ver.bin" "$W/v.vers")
  mapfile -t VV < <(python3 "$W/s3xml.py" versions "$W/v.vers" 2>/dev/null)
  if [ "$code" = 200 ] && [ "${VV[1]:-}" = "$DMARK" ] && [ "${VV[0]:-}" = "$VER_VID" ]; then pass "$lbl: 删除标记/版本 id 不变"; else fail "$lbl: 版本列表 code=$code markers=${VV[1]:-} versions=${VV[0]:-}"; fi
  code=$(s3 "$s" GET "/$BUCKET/ver.bin" "$W/v.gone")
  if [ "$code" = 404 ]; then pass "$lbl: 已删对象 GET=404"; else fail "$lbl: 已删对象 GET=$code"; fi
  code=$(s3 "$s" GET "/$BUCKET/ver.bin?versionId=$(enc "$VER_VID")" "$W/v.old")
  if [ "$code" = 200 ] && cmp -s "$W/ver.bin" "$W/v.old"; then pass "$lbl: 旧版本按 versionId 可读"; else fail "$lbl: 旧版本 code=$code 不一致"; fi
}

echo "== 1. 起 A/B 数据站点 + C witness，配对并建 sync 桶 =="
mk_cfg a "$A_HTTP" ap-east-1 "$TOK_A" 1 true
mk_cfg b "$B_HTTP" eu-west-1 "$TOK_B" 2 false
mk_witness_cfg
ckm "A 就绪" start_a
ckm "B 就绪" start_b
ckm "C(witness) 就绪" start_c
AT=$(login "$A" "$ADMIN" "$APW"); BT=$(login "$B" "$ADMIN" "$APW")
ckm "A/B 管理员登录" [ -n "$AT" -a -n "$BT" ]
AKEYS=$(mint "$A" "$AT"); BKEYS=$(mint "$B" "$BT")
A_AK=$(printf '%s' "$AKEYS" | jget access_key); A_SK=$(printf '%s' "$AKEYS" | jget secret_key)
B_AK=$(printf '%s' "$BKEYS" | jget access_key); B_SK=$(printf '%s' "$BKEYS" | jget secret_key)
ckm "A/B 铸造 AK" [ -n "$A_AK" -a -n "$B_AK" ]
A_ID=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | jget id)
A_ST=$(curl -s "$A/api/v1/site" -H "authorization: Bearer $AT" | jget site_token)
B_ID=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | jget id)
B_ST=$(curl -s "$B/api/v1/site" -H "authorization: Bearer $BT" | jget site_token)
ckm "A→B 配对" [ "$(pair "$A" "$AT" "$B_ID" "$B" eu-west-1 "$B_AK" "$B_SK" "$B_ST")" = 200 ]
ckm "B→A 配对" [ "$(pair "$B" "$BT" "$A_ID" "$A" ap-east-1 "$A_AK" "$A_SK" "$A_ST")" = 200 ]
ckm "加入 witness C" timeout 30 "$BIN" raft-add-witness --config "$W/a/oss.toml" --node-id 3 --endpoint "$C"
ckm "A/B 同 leader 且 3 选民" poll 90 ab_ready
ckm "建 sync 桶（204）" [ "$(mk_bucket "$A" "$AT" "$BUCKET" sync)" = 204 ]
ckm "B 应用桶行" poll 60 has_bucket "$B" "$BT" "$BUCKET"

echo "== 2. 数据面基线：PUT / List V1+V2 / MPU / 删除标记 =="
head -c 4096 /dev/urandom > "$W/obj.bin"
head -c 1048576 /dev/urandom > "$W/mpu.bin"
printf 'versioned-%s' "$BUCKET" > "$W/ver.bin"
ok=1; for k in a b c d; do printf '%s' "$k" > "$W/list.$k"; [ "$(s3 A PUT "/$BUCKET/list/$k" "$W/p.$k" --data-binary "@$W/list.$k")" = 200 ] || ok=0; done
ckm "4 个 list 键 PUT 全部 200" [ "$ok" = 1 ]
ckm "PUT obj.bin → 200" [ "$(s3 A PUT "/$BUCKET/obj.bin" "$W/p.obj" --data-binary "@$W/obj.bin")" = 200 ]
code=$(s3 A POST "/$BUCKET/mpu.bin?uploads" "$W/m.init"); UP=$(python3 "$W/s3xml.py" uploadid "$W/m.init" 2>/dev/null)
ckm "MPU init 返回 UploadId" [ -n "$UP" ]
code=$(s3 A PUT "/$BUCKET/mpu.bin?partNumber=1&uploadId=$UP" "$W/m.part" --data-binary "@$W/mpu.bin"); ET=$(hdr ETag "$W/m.part.hdr")
if [ "$code" = 200 ] && [ -n "$ET" ]; then pass "MPU UploadPart 1 → 200 且带 ETag"; else fail "MPU UploadPart code=$code etag=$ET"; fi
printf '<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>%s</ETag></Part></CompleteMultipartUpload>' "$ET" > "$W/mpu.xml"
ckm "MPU complete → 200" [ "$(s3 A POST "/$BUCKET/mpu.bin?uploadId=$UP" "$W/m.done" -H 'content-type: application/xml' --data-binary "@$W/mpu.xml")" = 200 ]
code=$(s3 A PUT "/$BUCKET/ver.bin" "$W/p.ver" --data-binary "@$W/ver.bin"); VER_VID=$(hdr x-amz-version-id "$W/p.ver.hdr")
ckm "版本化 PUT 带 x-amz-version-id" [ -n "$VER_VID" ]
ckm "DELETE ver.bin → 204" [ "$(s3 A DELETE "/$BUCKET/ver.bin" "$W/d.ver")" = 204 ]
s3 A GET "/$BUCKET?versions&prefix=ver.bin" "$W/vv.a" >/dev/null
mapfile -t VA < <(python3 "$W/s3xml.py" versions "$W/vv.a" 2>/dev/null)
DMARK=${VA[1]:-}
if [ "${VA[0]:-}" = "$VER_VID" ] && [ -n "$DMARK" ]; then pass "删除标记已写入（id=$DMARK）"; else fail "缺失/多版本：versions=${VA[0]:-} markers=${VA[1]:-}"; fi
verify_data A "基线 A"
verify_data B "基线 B"

echo "== 3. 滚动 B（数据 follower）：停机读不断/写 503，重启后追平且数据不变 =="
ckm "B TERM 正常退出" stop_node "$B_PID"; B_PID=""
code=$(s3 A GET "/$BUCKET/obj.bin" "$W/x.obj")
if [ "$code" = 200 ] && cmp -s "$W/obj.bin" "$W/x.obj"; then pass "B 停机：A 本地 GET 仍 200"; else fail "B 停机：A GET code=$code"; fi
ckm "B 停机：sync PUT fail-closed 503" [ "$(s3 A PUT "/$BUCKET/blocked.bin" "$W/x.put" --data-binary "@$W/ver.bin")" = 503 ]
ckm "B 停机：blocked.bin 本地不可见" [ "$(s3 A GET "/$BUCKET/blocked.bin" "$W/x.gone")" = 404 ]
NB="dproll-b-$RANDOM"
ckm "B 停机：A+C quorum 建桶 204" [ "$(mk_bucket "$A" "$AT" "$NB")" = 204 ]
ckm "B 重启就绪" start_b
ckm "B 已升级/追平（桶 $NB 可见）" poll 60 has_bucket "$B" "$BT" "$NB"
ckm "B 重启后 A/B 同 leader 且 3 选民" poll 60 ab_ready
verify_data A "B 重启后 A"
verify_data B "B 重启后 B"

echo "== 4. 滚动 C（witness）：A+B 继续读写，C 重启后再入 quorum =="
ckm "C TERM 正常退出" stop_node "$C_PID"; C_PID=""
NC="dproll-c-$RANDOM"
ckm "C 停机：A+B quorum 建桶 204" [ "$(mk_bucket "$A" "$AT" "$NC")" = 204 ]
ckm "C 停机：A→B sync 写成功" [ "$(s3 A PUT "/$BUCKET/live.bin" "$W/x.live" --data-binary "@$W/obj.bin")" = 200 ]
code=$(s3 B GET "/$BUCKET/live.bin" "$W/x.live2")
if [ "$code" = 200 ] && cmp -s "$W/obj.bin" "$W/x.live2"; then pass "C 停机：B 读回 live.bin 一致"; else fail "C 停机：B GET live.bin code=$code"; fi
ckm "C 重启就绪" start_c
ckm "C 重启后 A/B 同 leader 且 3 选民" poll 60 ab_ready
ckm "C 重启后 witness 可见 leader" poll 60 witness_follows
verify_data A "C 重启后 A"
verify_data B "C 重启后 B"

echo "== 5. 滚动 A（leader）：B 当选，B+C quorum 提交；A 重启后追平并恢复写 =="
ckm "A TERM 正常退出" stop_node "$A_PID"; A_PID=""
ckm "A 停机：B 当选 leader" poll 90 leader_is "$B" 2
code=$(s3 B GET "/$BUCKET/obj.bin" "$W/x.obj")
if [ "$code" = 200 ] && cmp -s "$W/obj.bin" "$W/x.obj"; then pass "A 停机：B 本地 GET 仍 200"; else fail "A 停机：B GET code=$code"; fi
ckm "A 停机：sync PUT fail-closed 503" [ "$(s3 B PUT "/$BUCKET/blocked2.bin" "$W/x.put2" --data-binary "@$W/ver.bin")" = 503 ]
NA="dproll-a-$RANDOM"
ckm "A 停机：B+C quorum 建桶 204" [ "$(mk_bucket "$B" "$BT" "$NA")" = 204 ]
ckm "A 重启就绪" start_a
ckm "A 重启后同 leader 且 3 选民" poll 90 ab_ready
ckm "A 追平：桶 $NA 可见" poll 60 has_bucket "$A" "$AT" "$NA"
verify_data A "A 重启后 A"
verify_data B "A 重启后 B"
ckm "滚动完成后 sync 写恢复（A→B）" [ "$(s3 A PUT "/$BUCKET/after.bin" "$W/x.after" --data-binary "@$W/obj.bin")" = 200 ]
code=$(s3 B GET "/$BUCKET/after.bin" "$W/x.after2")
if [ "$code" = 200 ] && cmp -s "$W/obj.bin" "$W/x.after2"; then pass "滚动完成后 B 读回 after.bin 一致"; else fail "滚动完成后 B GET after.bin code=$code"; fi

finish
