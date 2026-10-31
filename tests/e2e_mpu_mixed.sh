#!/usr/bin/env bash
# 混版数据面 MPU e2e：静态 [cluster] mesh（4 节点 × 3 盘 = 12 槽），滚动一台协调者。
#
# 场景（v1.27 跨版本 MPU 有界回退的可执行验收）：
#   1) 四个 v1.16.0 节点组成 mesh 集群，建桶并做一次跨节点基线 PUT/GET
#   2) 只把 n4 换成当前二进制（n1–n3 仍是 v1.16.0 wire-1 属主）
#      → n4 /health/cluster：wire=2、compatible=true、degraded_peers=n1/n2/n3(v1)
#   3) 在 **n1（旧属主）** CreateMultipartUpload（upload_id 前缀 = n1）
#      经 **n4（新协调者）** UploadPart → 走 wire-1 有界回退（预落盘/限长/MD5）
#      ListParts/Complete 经 n4 收尾；n4 与 n1 GET 成品字节一致
#   4) 可选（OSS_V115_BIN）：≤v1.15 无 MPU RPC → 经新协调者 UploadPart 必须 503
#      fail-closed（不是 404/静默成功），旧属主本地仍可 ListParts/Abort
#
# 为什么不是 [site]+[raft] 配对：配对站点是各自独立的存储域，对象不跨站可见；
# MPU 属主前缀/转发只在静态 [cluster] mesh（cfg.cluster）下生效。新二进制不允许
# 与 wire<2 身份新建配对，但**已有** mesh 集群可滚动换二进制（见 mesh_gate 操作矩阵）。
#
# 用法：
#   OSS_OLD_BIN=/path/v1.16.0/oss-server OSS_NEW_BIN=target/debug/oss-server \
#     tests/e2e_mpu_mixed.sh
#   OSS_V115_BIN=/path/v1.15.0/oss-server  # 可选，启用 ≤v1.15 负向用例
#   缺 OSS_OLD_BIN 默认 FAIL；仅 MPU_MIXED_ALLOW_SKIP=1 时降级为 SKIP（不伪造通过）。
# 前置：两/三枚二进制、curl、python3、aws-cli。
set -uo pipefail
cd "$(dirname "$0")/.."

NEW_BIN="${OSS_NEW_BIN:-${OSS_BIN:-$PWD/target/debug/oss-server}}"
OLD_BIN="${OSS_OLD_BIN:-}"
V115_BIN="${OSS_V115_BIN:-}"
ALLOW_SKIP="${MPU_MIXED_ALLOW_SKIP:-0}"
V115_REQUIRED="${MPU_V115_REQUIRED:-0}"
PASSES=0
FAILS=0
SKIPS=0
pass() { PASSES=$((PASSES + 1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS + 1)); echo "FAIL: $*"; }
skip() { SKIPS=$((SKIPS + 1)); echo "SKIP: $*"; }
ck() {
	local m=$1
	shift
	if "$@"; then pass "$m"; else fail "$m"; fi
}
finish() {
	echo "== 结果 =="
	echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS"
	[ "$FAILS" -ne 0 ] && exit 1
	if [ "$PASSES" -eq 0 ] && [ "$SKIPS" -gt 0 ]; then
		echo "SKIP: 混版 MPU e2e 未执行（$SKIPS 项跳过）"
		exit 0
	fi
	echo "PASS: 混版 MPU e2e（$PASSES 项通过，$SKIPS 项跳过）"
	exit 0
}

[ -x "$NEW_BIN" ] || {
	echo "缺少新二进制：$NEW_BIN（先 cargo build -p oss-server，或设 OSS_NEW_BIN）" >&2
	exit 2
}
if [ ! -x "$OLD_BIN" ]; then
	if [ "$ALLOW_SKIP" = "1" ]; then
		skip "缺少 OSS_OLD_BIN（v1.16.0）：混版 MPU e2e 不执行"
		finish
	fi
	echo "FAIL: 缺少 OSS_OLD_BIN（v1.16.0）：混版 MPU e2e 无法执行（默认严格）；" >&2
	echo "      明确接受跳过请设 MPU_MIXED_ALLOW_SKIP=1。" >&2
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

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
USED=""
PICKED=""
pick_port() {
	local p
	while :; do
		p=$(free_port)
		case " $USED " in *" $p "*) continue ;; esac
		break
	done
	USED="$USED $p"
	PICKED=$p
}

IDS=(n1 n2 n3 n4)
HTTPP=()
GRPCP=()
BINS=()
PIDS=("" "" "" "")
W=""
JWT_SECRET="mpu-mixed-e2e-jwt-0123456789abcdef"
SECRET="mpu-mixed-e2e-cluster-secret"
ADMIN="mpumixadmin"
APW="mpumixadmin-pw"
AK=""
SK=""
BUCKET=""

cleanup() {
	for p in "${PIDS[@]}"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done
	PIDS=("" "" "" "")
	[ -n "$W" ] && rm -rf "$W"
	W=""
}
trap cleanup EXIT

mk_cfg() { # i
	local i=$1 d="$W/${IDS[$i]}" n
	mkdir -p "$d/d0" "$d/d1" "$d/d2" "$d/staging"
	cat >"$d/oss.toml" <<EOF
listen = "127.0.0.1:${HTTPP[$i]}"
data_dir = "$d/d0"
data_dirs = ["$d/d0", "$d/d1", "$d/d2"]
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456

[cluster]
node_id = "${IDS[$i]}"
secret = "$SECRET"
EOF
	for n in 0 1 2 3; do
		cat >>"$d/oss.toml" <<EOF

[[cluster.nodes]]
id = "${IDS[$n]}"
addr = "127.0.0.1:${GRPCP[$n]}"
drive_count = 3
EOF
	done
	cat >>"$d/oss.toml" <<'EOF'

[[groups]]
group = "file"
bucket = "files"
max_size = 0
extensions = []
EOF
}

start_i() { # i
	local i=$1
	(cd "$W/${IDS[$i]}" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "${BINS[$i]}" serve \
		>>"$W/${IDS[$i]}/server.log" 2>&1) &
	PIDS[$i]=$!
	wait_live_i "$i" || {
		tail -5 "$W/${IDS[$i]}/server.log" >&2 2>/dev/null
		return 1
	}
}
wait_live_i() { # i
	local i=$1 p=${HTTPP[$i]} pid=${PIDS[$i]}
	for _ in $(seq 1 180); do
		kill -0 "$pid" 2>/dev/null || return 1
		[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p/health/live" 2>/dev/null || true)" = 200 ] && return 0
		sleep 1
	done
	return 1
}
wait_ready_i() { # i
	local i=$1 p=${HTTPP[$i]} pid=${PIDS[$i]}
	for _ in $(seq 1 180); do
		kill -0 "$pid" 2>/dev/null || return 1
		[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$p/health/ready" 2>/dev/null || true)" = 200 ] && return 0
		sleep 1
	done
	return 1
}
stop_i() { # i：TERM 正常退出返回 0（滚动升级必须优雅停）
	local i=$1 pid=${PIDS[$i]}
	[ -z "$pid" ] && return 0
	kill -TERM "$pid" 2>/dev/null || {
		wait "$pid" 2>/dev/null
		PIDS[$i]=""
		return 0
	}
	for _ in $(seq 1 80); do
		kill -0 "$pid" 2>/dev/null || {
			wait "$pid" 2>/dev/null
			PIDS[$i]=""
			return 0
		}
		sleep 0.25
	done
	kill -9 "$pid" 2>/dev/null
	wait "$pid" 2>/dev/null
	PIDS[$i]=""
	return 1
}
node_ep() { printf 'http://127.0.0.1:%s' "${HTTPP[$1]}"; }
aws_n() { # i aws-args...
	local i=$1
	shift
	env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN \
		AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1 \
		AWS_PAGER="" AWS_MAX_ATTEMPTS=1 aws --endpoint-url "$(node_ep "$i")" "$@"
}
login() {
	local i body tok
	for i in $(seq 1 30); do
		body=$(curl -s --max-time 10 -X POST "$(node_ep 0)/api/v1/login" -H 'content-type: application/json' \
			-d "{\"username\":\"$ADMIN\",\"password\":\"$APW\"}")
		tok=$(printf '%s' "$body" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))')
		if [ -n "$tok" ]; then printf '%s' "$tok"; return 0; fi
		sleep 1
	done
	return 1
}
jget() {
	python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
v=d.get(sys.argv[1],"")
print("" if v is None else v)' "$1"
}
health_i() { curl -s --max-time 5 "$(node_ep "$1")/health/cluster"; }
health_field() { health_i "$1" | jget "$2"; }
poll_pred() { local n=$1; shift; for _ in $(seq 1 "$n"); do "$@" && return 0; sleep 1; done; return 1; }
wire_is() { [ "$(health_field "$1" data_wire_version)" = "$2" ]; }
get_same() { # i key out expected
	aws_n "$1" s3api get-object --bucket "$BUCKET" --key "$2" "$3" >/dev/null 2>&1 && cmp -s "$4" "$3"
}
degraded_ok() { # i expected_ids...
	local i=$1
	shift
	health_i "$i" | python3 -c 'import json,sys
d = json.load(sys.stdin)
ids = {p.split("(", 1)[0] for p in d.get("degraded_peers") or []}
assert d.get("ready") is True, d
assert d.get("data_plane_compatible") is True, d
assert int(d.get("data_wire_version", 0)) >= 2, d
assert set(sys.argv[1:]) <= ids, d' "$@"
}
parts_json() { # file etag1 etag2
	python3 - "$1" "$2" "$3" <<'PY'
import json
import sys

out, e1, e2 = sys.argv[1:4]
with open(out, "w", encoding="utf-8") as f:
    json.dump({"Parts": [
        {"ETag": e1, "PartNumber": 1},
        {"ETag": e2, "PartNumber": 2},
    ]}, f)
PY
}
decode_owner() {
	python3 - "$1" <<'PY'
import base64
import sys

enc = sys.argv[1].split(".", 1)[0]
print(base64.urlsafe_b64decode(enc + "=" * (-len(enc) % 4)).decode())
PY
}

# 起四节点 mesh 集群并建桶；失败返回 1。
setup_cluster() { # old_bin label
	local old=$1 label=$2 i keys
	W=$(mktemp -d "${TMPDIR:-/tmp}/mpu-mixed-$label.XXXXXX") || return 1
	HTTPP=() GRPCP=() BINS=()
	for i in 0 1 2 3; do
		pick_port
		HTTPP+=("$PICKED")
		pick_port
		GRPCP+=("$PICKED")
		BINS+=("$old")
	done
	for i in 0 1 2 3; do mk_cfg "$i"; done
	for i in 0 1 2 3; do
		start_i "$i" || {
			fail "$label: ${IDS[$i]} 启动失败（见 $W/${IDS[$i]}/server.log）"
			return 1
		}
	done
	AT=$(login)
	[ -n "$AT" ] || {
		fail "$label: 管理员登录失败"
		return 1
	}
	keys=$(curl -s --max-time 10 -X POST "$(node_ep 0)/api/v1/access-keys" \
		-H "authorization: Bearer $AT" -H 'content-type: application/json' -d '{}')
	AK=$(printf '%s' "$keys" | jget access_key)
	SK=$(printf '%s' "$keys" | jget secret_key)
	[ -n "$AK" ] && [ -n "$SK" ] || {
		fail "$label: 铸取访问密钥失败"
		return 1
	}
	# 控制面读扇出：n1 铸的 AK 在 n4 立即可用。
	poll_pred 30 aws_n 3 s3api list-buckets >/dev/null 2>&1 || {
		fail "$label: AK 未在 n4 生效（控制面扇出）"
		return 1
	}
	BUCKET="mpumix-$label-$RANDOM"
	aws_n 0 s3 mb "s3://$BUCKET" >/dev/null 2>&1 || {
		fail "$label: 建桶失败"
		aws_n 0 s3 mb "s3://$BUCKET" 2>&1 >/dev/null | tail -2 >&2
		return 1
	}
	poll_pred 30 aws_n 3 s3api head-bucket --bucket "$BUCKET" >/dev/null 2>&1 || {
		fail "$label: n4 未看到桶（控制面扇出）"
		return 1
	}
	return 0
}

baseline_ok() { # label
	local rc1 rc2 rc3
	head -c 1048576 /dev/urandom >"$W/base.bin"
	aws_n 0 s3api put-object --bucket "$BUCKET" --key base.bin --body "$W/base.bin" >/dev/null 2>"$W/base.put.err"
	rc1=$?
	aws_n 3 s3api get-object --bucket "$BUCKET" --key base.bin "$W/base.out" >/dev/null 2>"$W/base.get.err"
	rc2=$?
	aws_n 3 s3api get-object --bucket "$BUCKET" --key base.bin "$W/base.out2" >/dev/null 2>&1
	rc3=$?
	if [ "$rc1" = 0 ] && [ "$rc2" = 0 ] && [ "$rc3" = 0 ] && cmp -s "$W/base.bin" "$W/base.out"; then
		return 0
	fi
	printf '  put=%s get=%s err=%s\n' "$rc1" "$rc2" "$(tr '\n' ' ' <"$W/base.get.err")" >&2
	return 1
}

echo "== 用例 1：v1.16.0 属主 + 当前协调者（wire-1 有界回退） =="
setup_cluster "$OLD_BIN" v116 || {
	cleanup
	finish
}
pass "v1.16.0 四节点 mesh 集群、建桶、控制面扇出就绪"
if baseline_ok; then pass "升级前基线 PUT(n1)/GET(n4) 字节一致"; else fail "升级前基线 PUT(n1)/GET(n4) 字节一致"; fi

# 只换 n4：n1–n3 保持 v1.16.0 属主。
ck "停旧 n4" stop_i 3
BINS[3]=$NEW_BIN
ck "新 n4 在同配置/同数据盘启动" start_i 3
ck "新 n4 /health/ready 200" wait_ready_i 3
ck "n4 报告 wire_version=2" poll_pred 90 wire_is 3 2
ck "n4 健康：compatible=true 且 n1–n3 为 legacy 降级 peer" poll_pred 90 degraded_ok 3 n1 n2 n3
ck "滚动后新 n4 仍能读旧端写的对象" get_same 3 base.bin "$W/base.after" "$W/base.bin"

# 旧属主 n1 上创建 MPU：upload_id 前缀必须编码 n1。
UP=$(aws_n 0 s3api create-multipart-upload --bucket "$BUCKET" --key mpu-mixed.bin \
	--query UploadId --output text 2>"$W/mpu.err")
ck "旧属主 n1 CreateMultipartUpload 返回 UploadId" bash -c '[ -n "$1" ] && [ "$1" != None ]' _ "$UP"
OWNER=$(decode_owner "$UP" 2>/dev/null || true)
if [ "$OWNER" = n1 ]; then
	pass "UploadId 属主前缀 = n1（旧节点）"
else
	fail "UploadId 属主前缀 = n1（旧节点）"
	printf '  owner=%s upload_id=%s\n' "$OWNER" "$UP" >&2
fi

# 经新协调者 n4 上传：n4 必须走 wire-1 有界回退（流式 MpuForward 对 legacy 会被闸门拒绝）。
head -c 5242880 /dev/urandom >"$W/p1.bin"
head -c 1048576 /dev/urandom >"$W/p2.bin"
ET1=$(aws_n 3 s3api upload-part --bucket "$BUCKET" --key mpu-mixed.bin --upload-id "$UP" \
	--part-number 1 --body "$W/p1.bin" --query ETag --output text 2>"$W/up1.err")
RC1=$?
ET2=$(aws_n 3 s3api upload-part --bucket "$BUCKET" --key mpu-mixed.bin --upload-id "$UP" \
	--part-number 2 --body "$W/p2.bin" --query ETag --output text 2>"$W/up2.err")
RC2=$?
ck "经 n4 转发 UploadPart 1（5 MiB）" bash -c '[ "$1" = 0 ] && [ -n "$2" ]' _ "$RC1" "$ET1"
ck "经 n4 转发 UploadPart 2（1 MiB）" bash -c '[ "$1" = 0 ] && [ -n "$2" ]' _ "$RC2" "$ET2"
ck "part 1 ETag = 本地 MD5（有界转发核对）" [ "$(printf '%s' "$ET1" | tr -d '"')" = "$(md5sum "$W/p1.bin" | cut -d' ' -f1)" ]
ck "part 2 ETag = 本地 MD5（有界转发核对）" [ "$(printf '%s' "$ET2" | tr -d '"')" = "$(md5sum "$W/p2.bin" | cut -d' ' -f1)" ]

aws_n 3 s3api list-parts --bucket "$BUCKET" --key mpu-mixed.bin --upload-id "$UP" \
	--output json >"$W/parts.json" 2>"$W/lp.err"
ck "经 n4 ListParts 看到 2 个 part（转发到旧属主）" python3 - "$W/parts.json" <<'PY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
parts = sorted((p["PartNumber"], p["Size"]) for p in d.get("Parts") or [])
assert parts == [(1, 5242880), (2, 1048576)], parts
PY

parts_json "$W/complete.json" "$ET1" "$ET2"
aws_n 3 s3api complete-multipart-upload --bucket "$BUCKET" --key mpu-mixed.bin \
	--upload-id "$UP" --multipart-upload "file://$W/complete.json" \
	--query ETag --output text >"$W/done.etag" 2>"$W/done.err"
DONE_RC=$?
ck "经 n4 CompleteMultipartUpload" [ "$DONE_RC" = 0 ]
ck "Complete ETag 带 -2 后缀" grep -q -- '-2' "$W/done.etag"
cat "$W/p1.bin" "$W/p2.bin" >"$W/expected.bin"
aws_n 3 s3api get-object --bucket "$BUCKET" --key mpu-mixed.bin "$W/get_n4.bin" >/dev/null 2>"$W/getn4.err"
GET_N4=$?
aws_n 0 s3api get-object --bucket "$BUCKET" --key mpu-mixed.bin "$W/get_n1.bin" >/dev/null 2>"$W/getn1.err"
GET_N1=$?
if bash -c '[ "$1" = 0 ] && cmp -s "$2" "$3"' _ "$GET_N4" "$W/expected.bin" "$W/get_n4.bin"; then
	pass "新协调者 n4 GET 成品字节一致"
else
	fail "新协调者 n4 GET 成品字节一致"
	printf '  get rc=%s err=%s\n' "$GET_N4" "$(tr '\n' ' ' <"$W/getn4.err")" >&2
fi
ck "旧属主 n1 GET 成品字节一致" bash -c '[ "$1" = 0 ] && cmp -s "$2" "$3"' _ "$GET_N1" "$W/expected.bin" "$W/get_n1.bin"

ck "n1 侧 MPU 暂存区已清理（无活动 mpu.meta）" poll_pred 30 bash -c '! find "$1" -name mpu.meta -print -quit | grep -q .' _ "$W/n1"
cleanup
pass "用例 1 完成"

echo "== 用例 2：≤v1.15 无 MPU RPC → 经新协调者 UploadPart fail-closed =="
if [ ! -x "$V115_BIN" ]; then
	if [ "$V115_REQUIRED" = "1" ]; then
		fail "缺少 OSS_V115_BIN（v1.15.0），负向用例未执行（MPU_V115_REQUIRED=1）"
	else
		skip "缺少 OSS_V115_BIN（v1.15.0）：≤v1.15 fail-closed 用例未执行"
	fi
	finish
fi
V115_BIN=$(realpath "$V115_BIN")
setup_cluster "$V115_BIN" v115 || {
	cleanup
	finish
}
pass "v1.15.0 四节点 mesh 集群、建桶就绪"
if baseline_ok; then pass "升级前基线 PUT(n1)/GET(n4) 字节一致"; else fail "升级前基线 PUT(n1)/GET(n4) 字节一致"; fi
ck "停旧 n4" stop_i 3
BINS[3]=$NEW_BIN
ck "新 n4 在同配置/同数据盘启动" start_i 3
ck "n4 报告 wire_version=2" poll_pred 90 wire_is 3 2
ck "n4 健康：compatible=true 且 n1–n3 为 legacy 降级 peer" poll_pred 90 degraded_ok 3 n1 n2 n3

# v1.15 的 upload_id 是无前缀裸 UUID：新 n4 只能探测 peer 的 ListParts RPC（不存在）。
UP=$(aws_n 0 s3api create-multipart-upload --bucket "$BUCKET" --key mpu-old.bin \
	--query UploadId --output text 2>"$W/mpu115.err")
ck "v1.15 属主 n1 CreateMultipartUpload 返回 UploadId" bash -c '[ -n "$1" ] && [ "$1" != None ]' _ "$UP"
ck "v1.15 upload_id 为无属主前缀的裸 UUID" bash -c 'case "$1" in ""|None|*.*) exit 1;; *) exit 0;; esac' _ "$UP"
head -c 5242880 /dev/urandom >"$W/p115.bin"
UP_ERR=$(aws_n 3 s3api upload-part --bucket "$BUCKET" --key mpu-old.bin --upload-id "$UP" \
	--part-number 1 --body "$W/p115.bin" 2>&1 >/dev/null)
UP_RC=$?
ck "经新协调者向 v1.15 属主 UploadPart 非零退出" [ "$UP_RC" -ne 0 ]
ck "错误为 503/ServiceUnavailable（fail-closed，非 404）" bash -c 'case "$1" in *ServiceUnavailable*|*503*) exit 0;; *) printf "%s\n" "$1" >&2; exit 1;; esac' _ "$UP_ERR"
aws_n 0 s3api list-parts --bucket "$BUCKET" --key mpu-old.bin --upload-id "$UP" --output json >"$W/parts115.json" 2>/dev/null
ck "v1.15 属主本地 ListParts 仍可用（会话未被破坏）" python3 - "$W/parts115.json" <<'PY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
assert (d.get("Parts") or []) == [], d
PY
ck "v1.15 属主本地 Abort 清理" aws_n 0 s3api abort-multipart-upload --bucket "$BUCKET" --key mpu-old.bin --upload-id "$UP" >/dev/null 2>&1
cleanup
pass "用例 2 完成（≤v1.15 fail-closed）"

finish
