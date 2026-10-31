#!/usr/bin/env bash
# 停写蓝绿（stop-the-world）升级 e2e：v1.26.0 旧二进制 → 当前工作树（v1.27）新二进制，
# 同一 data-dir 原地启动；切换前后校验 /health/* 与对象字节一致；再演练冷备回滚。
#
# 覆盖 paper-gate 的可执行面：
#   1) 旧二进制建桶/写对象 → 保存 /health/cluster → 停旧进程 → 停写后冷备
#   2) scripts/blue-green-preflight.sh：无 BLOCK、新二进制版本 >= 1.26.0
#   3) 新二进制同 data-dir 启动：GET 字节一致、ready=true、data_plane_compatible=true、wire>=2
#   4) 回滚：停新 → 恢复冷备（data-dir + 同实例 DB/配置）→ 旧二进制再次读回同一对象
#
# 用法：
#   OSS_OLD_BIN=/path/to/v1.26.0/oss-server OSS_NEW_BIN=target/debug/oss-server \
#     tests/e2e_blue_green.sh
#   旧二进制缺失时默认 FAIL（exit 2）；仅显式 OSS_E2E_SKIP_OLD=0 时打印 SKIP 并 exit 0。
# 前置：v1.26.0 二进制、当前 worktree 二进制、aws-cli、curl、python3。
set -uo pipefail
cd "$(dirname "$0")/.."

NEW_BIN="${OSS_NEW_BIN:-${OSS_BIN:-$PWD/target/debug/oss-server}}"
OLD_BIN="${OSS_OLD_BIN:-}"
SKIP_FLAG="${OSS_E2E_SKIP_OLD-__unset__}"
PASSES=0
FAILS=0
pass() { PASSES=$((PASSES + 1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS + 1)); echo "FAIL: $*"; }
ck() {
	local m=$1
	shift
	if "$@"; then pass "$m"; else fail "$m"; fi
}
finish() {
	echo "== 结果 =="
	echo "PASS=$PASSES FAIL=$FAILS"
	[ "$FAILS" -ne 0 ] && exit 1
	echo "PASS: 停写蓝绿升级 e2e（$PASSES 项通过）"
	exit 0
}

[ -x "$NEW_BIN" ] || {
	echo "缺少新二进制：$NEW_BIN（先 cargo build -p oss-server，或设 OSS_NEW_BIN）" >&2
	exit 2
}
if [ ! -x "$OLD_BIN" ]; then
	if [ "$SKIP_FLAG" = "0" ]; then
		echo "SKIP: 缺少 v1.26.0 旧二进制（OSS_OLD_BIN）；构建 tag v1.26.0 后重跑"
		echo "PASS=0 FAIL=0 SKIP=1"
		exit 0
	fi
	echo "FAIL: 缺少 v1.26.0 旧二进制（OSS_OLD_BIN）：无旧端无法执行蓝绿 e2e（默认严格）；" >&2
	echo "      本地明确接受跳过请设 OSS_E2E_SKIP_OLD=0。" >&2
	exit 2
fi
for c in curl python3 aws; do
	command -v "$c" >/dev/null 2>&1 || {
		echo "需要 $c" >&2
		exit 2
	}
done
NEW_BIN=$(realpath "$NEW_BIN")
OLD_BIN=$(realpath "$OLD_BIN")
PREFLIGHT="$PWD/scripts/blue-green-preflight.sh"
[ -x "$PREFLIGHT" ] || {
	echo "缺少 $PREFLIGHT" >&2
	exit 2
}

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
PORT=$(free_port)
EP="http://127.0.0.1:$PORT"
W=$(mktemp -d)
INST="$W/inst"
DATA="$INST/d0"
BACKUP="$W/backup"
mkdir -p "$DATA" "$INST/staging"
ADMIN="bgadmin"
APW="bgadmin-pw-$RANDOM"
JWT_SECRET="blue-green-e2e-jwt-0123456789abcdef"
BUCKET="bluegreen-$RANDOM"
KEY="obj/data.bin"
OBJ="$W/object.bin"
PID=""
cleanup() {
	if [ -n "$PID" ]; then
		kill -TERM "$PID" 2>/dev/null
		for _ in $(seq 1 40); do kill -0 "$PID" 2>/dev/null || break; sleep 0.25; done
		kill -9 "$PID" 2>/dev/null
		wait "$PID" 2>/dev/null
	fi
	rm -rf "$W"
}
trap cleanup EXIT

cat >"$INST/oss.toml" <<EOF
listen = "127.0.0.1:$PORT"
data_dir = "$DATA"
staging_dir = "$INST/staging"
database_url = "sqlite://$INST/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456
groups = []

[site]
name = "bluegreen"
region = "us-east-1"
priority = 10
token = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
EOF

wait_live() {
	local pid=$1
	for _ in $(seq 1 180); do
		kill -0 "$pid" 2>/dev/null || return 1
		[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "$EP/health/live" 2>/dev/null || true)" = 200 ] && return 0
		sleep 1
	done
	return 1
}
wait_ready() {
	local pid=$1 code
	for _ in $(seq 1 180); do
		kill -0 "$pid" 2>/dev/null || return 1
		code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "$EP/health/ready" 2>/dev/null || true)
		[ "$code" = 200 ] && return 0
		sleep 1
	done
	return 1
}
wait_stopped() {
	for _ in $(seq 1 40); do
		[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "$EP/health/live" 2>/dev/null || true)" = 000 ] && return 0
		sleep 0.25
	done
	return 1
}
start_srv() { # binary label
	local bin=$1 label=$2
	(cd "$INST" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$bin" serve \
		>>"$INST/$label.log" 2>&1) &
	PID=$!
	if wait_live "$PID" && wait_ready "$PID"; then return 0; fi
	tail -5 "$INST/$label.log" >&2 2>/dev/null
	return 1
}
stop_srv() {
	local pid=$PID
	PID=""
	[ -z "$pid" ] && return 0
	kill -TERM "$pid" 2>/dev/null || {
		wait "$pid" 2>/dev/null
		return 0
	}
	for _ in $(seq 1 80); do kill -0 "$pid" 2>/dev/null || {
		wait "$pid" 2>/dev/null
		return 0
	}; sleep 0.25; done
	kill -9 "$pid" 2>/dev/null
	wait "$pid" 2>/dev/null
	return 1
}
login() {
	local i body tok
	for i in $(seq 1 30); do
		body=$(curl -s --max-time 10 -X POST "$EP/api/v1/login" -H 'content-type: application/json' \
			-d "{\"username\":\"$ADMIN\",\"password\":\"$APW\"}")
		tok=$(printf '%s' "$body" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))')
		if [ -n "$tok" ]; then printf '%s' "$tok"; return 0; fi
		sleep 1
	done
	printf '  login 响应: %s\n' "$body" >&2
	return 1
}
jget() {
	python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
v=d.get(sys.argv[1],"")
print("" if v is None else v)' "$1"
}
s3() {
	env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN \
		AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1 \
		AWS_PAGER="" aws --endpoint-url "$EP" s3api "$@"
}
s3q() { s3 "$@" >/dev/null 2>&1; }
fetch_same() { # key out expected
	s3 get-object --bucket "$BUCKET" --key "$1" "$2" >/dev/null 2>&1 && cmp -s "$3" "$2"
}
health_field() { curl -s --max-time 5 "$EP/health/cluster" | jget "$1"; }

echo "== 1. 旧二进制（v1.26.0）起步：建桶 + 写对象 + 保存 /health/cluster =="
ck "旧二进制启动（/health/live + ready）" start_srv "$OLD_BIN" old-1
AT=$(login)
ck "管理员登录" [ -n "$AT" ]
KEYS=$(curl -s --max-time 10 -X POST "$EP/api/v1/access-keys" \
	-H "authorization: Bearer $AT" -H 'content-type: application/json' -d '{}')
AK=$(printf '%s' "$KEYS" | jget access_key)
SK=$(printf '%s' "$KEYS" | jget secret_key)
ck "铸取访问密钥" [ -n "$AK" ] && [ -n "$SK" ]
ck "建桶 $BUCKET" s3q create-bucket --bucket "$BUCKET"
head -c 2097152 /dev/urandom >"$OBJ"
ck "上传对象 $KEY" s3q put-object --bucket "$BUCKET" --key "$KEY" --body "$OBJ"
curl -s --max-time 5 "$EP/health/cluster" >"$W/health-old.json"
ck "旧端 wire>=2 且 data_plane_compatible" python3 - "$W/health-old.json" <<'PY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
assert int(d["data_wire_version"]) >= 2, d
assert d["data_plane_compatible"] is True, d
assert d["ready"] is True, d
PY
GET_RC=0
s3 get-object --bucket "$BUCKET" --key "$KEY" "$W/before.bin" >/dev/null 2>&1 || GET_RC=$?
if [ "$GET_RC" = 0 ] && cmp -s "$OBJ" "$W/before.bin"; then
	pass "旧端 GET 字节一致（基线）"
else
	fail "旧端 GET 字节一致（基线），rc=$GET_RC"
fi

echo "== 2. 停旧进程 → 停写冷备 → blue-green-preflight =="
ck "旧进程 TERM 正常退出" stop_srv
ck "旧服务无响应（切换前确认已停写）" wait_stopped
mkdir -p "$BACKUP"
ck "冷备复制实例（data-dir + DB/配置；停写后）" cp -a "$INST/." "$BACKUP/"
ck "冷备非空" test -n "$(find "$BACKUP" -mindepth 1 -print -quit)"
bash "$PREFLIGHT" --data-dir "$DATA" --backup "$BACKUP" --config "$INST/oss.toml" \
	--health-json "$W/health-old.json" --health-url "$EP/health/live" \
	--new-binary "$NEW_BIN" --min-version 1.26.0 --json >"$W/preflight.json" 2>"$W/preflight.err"
PRE_RC=$?
ck "blue-green-preflight 非 strict 退出 0" [ "$PRE_RC" = 0 ]
ck "preflight 无 BLOCK、版本 OK、备份/停写证明成立" python3 - "$W/preflight.json" <<'PY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
assert d["status"] in ("READY", "WARN"), d
assert int(d["exit_code"]) == 0, d
checks = {c["name"]: c for c in d["checks"]}
assert checks["new_binary_version"]["status"] == "OK", checks.get("new_binary_version")
assert checks["backup_ready"]["status"] == "OK", checks.get("backup_ready")
assert checks["service_stopped"]["status"] == "OK", checks.get("service_stopped")
assert checks["upgrade_preflight"]["status"] != "BLOCK", checks.get("upgrade_preflight")
assert not any(c["status"] == "BLOCK" for c in d["checks"]), d
PY

echo "== 3. 新二进制（当前 worktree）同 data-dir 启动：数据面与健康 =="
ck "新二进制启动（/health/live + ready）" start_srv "$NEW_BIN" new
curl -s --max-time 5 "$EP/health/cluster" >"$W/health-new.json"
ck "新端 ready=true / data_plane_compatible=true / wire>=2" python3 - "$W/health-new.json" <<'PY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
assert d["ready"] is True, d
assert d["data_plane_compatible"] is True, d
assert int(d["data_wire_version"]) >= 2, d
PY
ck "新端 /health/ready 返回 200" [ "$(curl -s -o "$W/ready.body" -w '%{http_code}' --max-time 5 "$EP/health/ready")" = 200 ]
ck "新端 GET 升级前对象字节一致" fetch_same "$KEY" "$W/after.bin" "$OBJ"

echo "== 4. 回滚演练：停新 → 恢复冷备 → 旧二进制重读 =="
ck "新进程 TERM 正常退出" stop_srv
ck "新服务无响应（回滚前）" wait_stopped
ck "恢复冷备到原实例目录" bash -c 'rm -rf "$1" && cp -a "$2" "$1"' _ "$INST" "$BACKUP"
ck "旧二进制在恢复后的数据上启动" start_srv "$OLD_BIN" old-2
ck "回滚后 GET 字节一致" fetch_same "$KEY" "$W/rollback.bin" "$OBJ"

finish
