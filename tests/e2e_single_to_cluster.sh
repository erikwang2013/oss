#!/usr/bin/env bash
# 单机 → 集群重新灌入 e2e：真实 4 节点 × 3 盘集群 + 1 个单机源进程。
#
# 覆盖 scripts/single-to-cluster.sh 的可验证切片：
#   * dry-run 零写入（目标桶列表/对象均不变）
#   * 真实复制：桶集合、对象字节（cmp）、Content-Type、用户元数据、嵌套前缀、
#     >8MiB 大对象（源端走 MPU）；目标集群初始为空
#   * 重跑幂等（目标快照不变，仍是源端镜像）
#   * --delete：删除目标桶内源端不存在的对象，保留目标独有桶；源端列表不变
#   * 负例：源端版本化桶 / 开锁桶 → fail-closed 且目标零写入；源=目标 endpoint 拒绝
#
# 不覆盖（脚本明确不迁移，勿当已支持）：历史版本/删除标记、桶 ACL/策略/标签/生命周期/
#   加密/复制配置、对象锁/legal hold、未完成 MPU、SSE-C；对象 ACL/标签/SSE-KMS 仅尽力。
# 注：S3 语义下开锁桶必然版本化，脚本的版本控制守卫会先于对象锁守卫触发；两者都是
#   fail-closed。对象锁守卫分支由 tests/single_to_cluster_selfcheck.sh 的 mock 覆盖。
#
# 前置：构建 oss-server（BIN=... 或 /tmp/oss-target-reingest/debug/oss-server、
#   target/debug/oss-server），aws-cli、rclone、curl、python3。
# 退出码：0=PASS；1=FAIL；77=SKIP（二进制/依赖缺失，未执行任何断言）。
set -uo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT="$ROOT/scripts/single-to-cluster.sh"
if [ -z "${BIN:-}" ]; then
	for cand in /tmp/oss-target-reingest/debug/oss-server "$ROOT/target/debug/oss-server"; do
		[ -x "$cand" ] && BIN=$cand && break
	done
fi
BIN="${BIN:-（未找到构建产物）}"
RUN="${RUN:-/tmp/e2e-single-to-cluster}"
ADMIN_USER=erik
ADMIN_PW=pw
T0=$(date +%s)

[ -f "$SCRIPT" ] || {
	echo "SKIP: 找不到 $SCRIPT"
	exit 77
}
missing=()
for c in aws rclone curl python3; do
	command -v "$c" >/dev/null 2>&1 || missing+=("$c")
done
[ -x "$BIN" ] || missing+=("oss-server（cargo build -p oss-server）")
if ((${#missing[@]})); then
	echo "SKIP: 缺失 ${missing[*]}（BIN=$BIN）"
	exit 77
fi

free_port() {
	python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

rm -rf "$RUN"
mkdir -p "$RUN"
W=$(mktemp -d)
PIDS=()
cleanup() {
	for p in "${PIDS[@]:-}"; do
		[ -n "$p" ] && kill "$p" 2>/dev/null
	done
	rm -rf "$W"
}
trap cleanup EXIT

pass=0
fail() {
	echo "FAIL: $*" >&2
	echo "== 结果: FAIL（日志保留在 $RUN）"
	exit 1
}
ok() {
	pass=$((pass + 1))
	echo "   ok: $*"
}

# ---- 4 节点 × 3 盘集群（e2e_m3_cluster 风格，动态端口）----
IDS=(n1 n2 n3 n4)
HTTPP=()
GRPCP=()
for i in 0 1 2 3; do
	HTTPP[$i]=$(free_port)
	GRPCP[$i]=$(free_port)
done
for i in 0 1 2 3; do
	d="$RUN/${IDS[$i]}"
	mkdir -p "$d/d0" "$d/d1" "$d/d2"
	cat >"$d/oss.toml" <<EOF
listen = "127.0.0.1:${HTTPP[$i]}"
data_dir = "$d/d0"
data_dirs = ["$d/d0","$d/d1","$d/d2"]
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "dev-only-change-me-0123456789abcdef"
max_put_bytes = 268435456

[cluster]
node_id = "${IDS[$i]}"
secret = "single-to-cluster-e2e"

[[cluster.nodes]]
id = "n1"
addr = "127.0.0.1:${GRPCP[0]}"
drive_count = 3

[[cluster.nodes]]
id = "n2"
addr = "127.0.0.1:${GRPCP[1]}"
drive_count = 3

[[cluster.nodes]]
id = "n3"
addr = "127.0.0.1:${GRPCP[2]}"
drive_count = 3

[[cluster.nodes]]
id = "n4"
addr = "127.0.0.1:${GRPCP[3]}"
drive_count = 3
EOF
done

# ---- 单机源（无 [cluster]/[[groups]]）----
SRC_PORT=$(free_port)
SRC_DIR="$RUN/src"
mkdir -p "$SRC_DIR/d0"
cat >"$SRC_DIR/oss.toml" <<EOF
listen = "127.0.0.1:$SRC_PORT"
data_dir = "$SRC_DIR/d0"
staging_dir = "$SRC_DIR/staging"
database_url = "sqlite://$SRC_DIR/oss.db?mode=rwc"
jwt_secret = "dev-only-change-me-0123456789abcdef"
max_put_bytes = 268435456
EOF

start_node() { # $1 = 目录
	(cd "$1" && OSS_ADMIN_USER="$ADMIN_USER" OSS_ADMIN_PASSWORD="$ADMIN_PW" \
		exec "$BIN" serve >>"$1/server.log" 2>&1) &
	PIDS+=($!)
}
wait_http() { # $1 = HTTP 端口
	for _t in $(seq 1 90); do
		s=$(curl -s -o /dev/null -w "%{http_code}" --max-time 1 \
			"http://127.0.0.1:$1/health/live" 2>/dev/null || true)
		[ "$s" = "200" ] && return 0
		sleep 1
	done
	return 1
}

echo "== 0. 起 4 节点集群（HTTP ${HTTPP[*]}）+ 单机源 :$SRC_PORT =="
for i in 0 1 2 3; do start_node "$RUN/${IDS[$i]}"; done
start_node "$SRC_DIR"
for i in 0 1 2 3; do
	wait_http "${HTTPP[$i]}" || fail "${IDS[$i]} 未就绪（$(tail -3 "$RUN/${IDS[$i]}/server.log")）"
done
wait_http "$SRC_PORT" || fail "单机源未就绪（$(tail -3 "$SRC_DIR/server.log")）"
DST_EP="http://127.0.0.1:${HTTPP[0]}"
SRC_EP="http://127.0.0.1:$SRC_PORT"
echo "   就绪（集群 $DST_EP，源 $SRC_EP）"

login() { # $1=endpoint
	curl -s -X POST "$1/api/v1/login" -H 'content-type: application/json' \
		-d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PW\"}" |
		python3 -c 'import json,sys;print(json.load(sys.stdin).get("token",""))'
}
mint_key() { # $1=endpoint $2=token；输出 "AK SK"
	curl -s -X POST "$1/api/v1/access-keys" -H "authorization: Bearer $2" \
		-H 'content-type: application/json' -d '{}' |
		python3 -c 'import json,sys;d=json.load(sys.stdin);print(d["access_key"]+" "+d["secret_key"])'
}

TOK=$(login "$DST_EP")
[ -n "$TOK" ] || fail "集群管理员登录失败"
read -r DST_AK DST_SK <<<"$(mint_key "$DST_EP" "$TOK")"
TOK=$(login "$SRC_EP")
[ -n "$TOK" ] || fail "源管理员登录失败"
read -r SRC_AK SRC_SK <<<"$(mint_key "$SRC_EP" "$TOK")"
[ -n "$DST_AK" ] && [ -n "$DST_SK" ] && [ -n "$SRC_AK" ] && [ -n "$SRC_SK" ] ||
	fail "AK/SK 铸造失败"

ep_aws() { # $1=endpoint $2=ak $3=sk，其余透传
	local ep=$1 ak=$2 sk=$3
	shift 3
	AWS_ACCESS_KEY_ID="$ak" AWS_SECRET_ACCESS_KEY="$sk" AWS_DEFAULT_REGION=us-east-1 \
		AWS_PAGER="" AWS_MAX_ATTEMPTS=1 aws --endpoint-url "$ep" "$@"
}
src_aws() { ep_aws "$SRC_EP" "$SRC_AK" "$SRC_SK" "$@"; }
dst_aws() { ep_aws "$DST_EP" "$DST_AK" "$DST_SK" "$@"; }

list_buckets() { # $1=endpoint $2=ak $3=sk
	ep_aws "$1" "$2" "$3" s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null |
		tr '\t ' '\n' | awk 'NF && $0 != "None"' | sort
}
list_objects() { # $1=endpoint $2=ak $3=sk $4=bucket；输出 key<TAB>size（按 key 排序）
	ep_aws "$1" "$2" "$3" s3api list-objects-v2 --bucket "$4" \
		--query 'Contents[].[Key,Size]' --output text 2>/dev/null | awk 'NF' | sort
}
snapshot() { # $1=endpoint $2=ak $3=sk；桶 + 对象键/大小（不取 ETag：rclone 重传可能改变分片形态）
	local ep=$1 ak=$2 sk=$3 b
	for b in $(list_buckets "$ep" "$ak" "$sk"); do
		printf 'bucket\t%s\n' "$b"
		list_objects "$ep" "$ak" "$sk" "$b" | awk -F'\t' -v b="$b" '{print b "\t" $1 "\t" $2}'
	done
}
purge_bucket() { # $1=bucket；清版本/删除标记后强删（用于两个负例之间恢复干净源端）
	local b=$1 vy="$W/versions.json" del="$W/delete.json"
	src_aws s3api list-object-versions --bucket "$b" --output json >"$vy" ||
		fail "列举版本失败: $b"
	python3 - "$vy" >"$del" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
objs = [{"Key": o["Key"], "VersionId": o["VersionId"]}
        for o in d.get("Versions", []) + d.get("DeleteMarkers", [])]
if objs:
    json.dump({"Objects": objs, "Quiet": True}, sys.stdout)
PY
	if [ -s "$del" ]; then
		src_aws s3api delete-objects --bucket "$b" --delete "file://$del" >/dev/null ||
			fail "删除版本失败: $b"
	fi
	src_aws s3 rb "s3://$b" --force >/dev/null || fail "删除桶失败: $b"
}

STAMP="$(date +%s)$RANDOM"
SRC_B1="app-$STAMP"
SRC_B2="media-$STAMP"
DST_ONLY="dstonly-$STAMP"
VB="versioned-$STAMP"
LB="locked-$STAMP"

echo "== 1. 播种源端（普通桶 / 元数据+Content-Type / 嵌套前缀 / 10MiB MPU / 第二桶）=="
head -c 65536 /dev/urandom >"$W/small.bin"
head -c 10485760 /dev/urandom >"$W/big.bin"
printf 'second bucket\n' >"$W/tiny.txt"
src_aws s3 mb "s3://$SRC_B1" >/dev/null || fail "源建桶 $SRC_B1"
src_aws s3 cp "$W/small.bin" "s3://$SRC_B1/small.bin" \
	--content-type 'text/plain; charset=utf-8' --metadata owner=alice,lang=zh >/dev/null ||
	fail "源上传 small.bin"
src_aws s3 cp "$W/big.bin" "s3://$SRC_B1/deep/nested/big.bin" >/dev/null || fail "源上传 big.bin"
BIG_ETAG=$(src_aws s3api head-object --bucket "$SRC_B1" --key deep/nested/big.bin \
	--query ETag --output text | tr -d '"')
[[ $BIG_ETAG =~ -[0-9]+$ ]] || fail "播种大对象未走 MPU（ETag=$BIG_ETAG）"
src_aws s3 mb "s3://$SRC_B2" >/dev/null || fail "源建桶 $SRC_B2"
src_aws s3 cp "$W/tiny.txt" "s3://$SRC_B2/tiny.txt" --content-type 'text/markdown' >/dev/null ||
	fail "源上传 tiny.txt"
SRC_B1_OBJS=$(list_objects "$SRC_EP" "$SRC_AK" "$SRC_SK" "$SRC_B1")
SRC_B2_OBJS=$(list_objects "$SRC_EP" "$SRC_AK" "$SRC_SK" "$SRC_B2")
SRC_SNAP0=$(snapshot "$SRC_EP" "$SRC_AK" "$SRC_SK")
ok "源端 2 桶 3 对象（big.bin ETag 分片形态 $BIG_ETAG）"

TGT_SNAP0=$(snapshot "$DST_EP" "$DST_AK" "$DST_SK")
[ -z "$TGT_SNAP0" ] || fail "目标集群初始应为空：$TGT_SNAP0"

OUT="$W/script.out"
RC=0
invoke() { # $1=source-endpoint $2=dest-endpoint，其余透传
	local sep=$1 dep=$2
	shift 2
	if bash "$SCRIPT" --source-endpoint "$sep" --dest-endpoint "$dep" \
		--source-ak "$SRC_AK" --source-sk "$SRC_SK" \
		--dest-ak "$DST_AK" --dest-sk "$DST_SK" "$@" >"$OUT" 2>&1; then
		RC=0
	else
		RC=$?
	fi
}
run_script() { invoke "$SRC_EP" "$DST_EP" "$@"; }
assert_rc() { [ "$RC" = "$1" ] || fail "$2（exit=$RC，期望 $1）$(cat "$OUT")"; }
assert_out() { grep -qF -- "$1" "$OUT" || fail "$2（输出缺少: $1）$(cat "$OUT")"; }
assert_target_unchanged() { # $1=基线快照 $2=说明
	local now
	now=$(snapshot "$DST_EP" "$DST_AK" "$DST_SK")
	[ "$now" = "$1" ] || fail "$2：目标端被写入（前后快照不同）"
}

echo "== 2. dry-run：零写入 =="
run_script --dry-run
assert_rc 0 "dry-run 应成功"
assert_out "[dry-run] 目标桶: $SRC_B1" "dry-run 应列出目标桶"
assert_out "[dry-run] 目标桶: $SRC_B2" "dry-run 应列出第二个桶"
assert_target_unchanged "$TGT_SNAP0" "dry-run"
ok "dry-run 未创建桶/未写对象"

echo "== 3. 真实复制：目标集群镜像源端 =="
run_script
assert_rc 0 "真实复制应成功"
[ "$(snapshot "$SRC_EP" "$SRC_AK" "$SRC_SK")" = "$SRC_SNAP0" ] || fail "复制修改了源端"
[ "$(snapshot "$DST_EP" "$DST_AK" "$DST_SK")" = "$SRC_SNAP0" ] ||
	fail "目标端桶/对象未镜像源端：$(snapshot "$DST_EP" "$DST_AK" "$DST_SK")"
dst_aws s3 cp "s3://$SRC_B1/small.bin" "$W/small.back" >/dev/null || fail "目标读 small.bin"
cmp "$W/small.bin" "$W/small.back" || fail "small.bin 字节不一致"
dst_aws s3 cp "s3://$SRC_B1/deep/nested/big.bin" "$W/big.back" >/dev/null || fail "目标读 big.bin"
cmp "$W/big.bin" "$W/big.back" || fail "big.bin 字节不一致"
dst_aws s3 cp "s3://$SRC_B2/tiny.txt" "$W/tiny.back" >/dev/null || fail "目标读 tiny.txt"
cmp "$W/tiny.txt" "$W/tiny.back" || fail "tiny.txt 字节不一致"
CT=$(dst_aws s3api head-object --bucket "$SRC_B1" --key small.bin --query ContentType --output text)
[ "$CT" = "text/plain; charset=utf-8" ] || fail "Content-Type 未保留：$CT"
MOWNER=$(dst_aws s3api head-object --bucket "$SRC_B1" --key small.bin --query 'Metadata.owner' --output text)
MLANG=$(dst_aws s3api head-object --bucket "$SRC_B1" --key small.bin --query 'Metadata.lang' --output text)
[ "$MOWNER" = "alice" ] && [ "$MLANG" = "zh" ] || fail "用户元数据未保留：owner=$MOWNER lang=$MLANG"
BLEN=$(dst_aws s3api head-object --bucket "$SRC_B1" --key deep/nested/big.bin \
	--query ContentLength --output text)
[ "$BLEN" = "10485760" ] || fail "大对象长度不符：$BLEN"
# 从其它成员节点读回，证明数据落在集群而非单一节点本地
for i in 1 3; do
	ep="http://127.0.0.1:${HTTPP[$i]}"
	ep_aws "$ep" "$DST_AK" "$DST_SK" s3 cp "s3://$SRC_B1/deep/nested/big.bin" \
		"$W/big.${IDS[$i]}" >/dev/null || fail "从 ${IDS[$i]} 读 big.bin 失败"
	cmp "$W/big.bin" "$W/big.${IDS[$i]}" || fail "从 ${IDS[$i]} 读 big.bin 字节不一致"
done
ok "桶/对象/字节/Content-Type/元数据/嵌套前缀/大对象全部一致（n2/n4 读回一致）"

echo "== 4. 重跑幂等：目标快照不变 =="
TGT_SNAP1=$(snapshot "$DST_EP" "$DST_AK" "$DST_SK")
run_script
assert_rc 0 "重跑应成功"
[ "$(snapshot "$DST_EP" "$DST_AK" "$DST_SK")" = "$TGT_SNAP1" ] || fail "重跑改变了目标端快照"
[ "$(snapshot "$SRC_EP" "$SRC_AK" "$SRC_SK")" = "$SRC_SNAP0" ] || fail "重跑修改了源端"
dst_aws s3 cp "s3://$SRC_B1/small.bin" "$W/small.back2" >/dev/null
cmp "$W/small.bin" "$W/small.back2" || fail "重跑后字节不一致"
ok "重跑结果幂等"

echo "== 5. --delete：删目标多余对象、保留目标独有桶、源端不变 =="
dst_aws s3 cp "$W/tiny.txt" "s3://$SRC_B1/extra-target-only.txt" >/dev/null || fail "写入目标多余对象"
dst_aws s3 mb "s3://$DST_ONLY" >/dev/null || fail "建目标独有桶"
dst_aws s3 cp "$W/tiny.txt" "s3://$DST_ONLY/keep.txt" >/dev/null || fail "写入目标独有对象"
dst_aws s3api head-object --bucket "$SRC_B1" --key extra-target-only.txt >/dev/null ||
	fail "前置：目标多余对象不存在"
run_script --delete
assert_rc 0 "--delete 应成功"
if dst_aws s3api head-object --bucket "$SRC_B1" --key extra-target-only.txt >/dev/null 2>&1; then
	fail "--delete 未删除目标多余对象"
fi
dst_aws s3api head-object --bucket "$DST_ONLY" --key keep.txt >/dev/null ||
	fail "--delete 误删目标独有桶/对象"
[ "$(list_objects "$SRC_EP" "$SRC_AK" "$SRC_SK" "$SRC_B1")" = "$SRC_B1_OBJS" ] ||
	fail "--delete 修改了源端桶 $SRC_B1"
[ "$(list_objects "$SRC_EP" "$SRC_AK" "$SRC_SK" "$SRC_B2")" = "$SRC_B2_OBJS" ] ||
	fail "--delete 修改了源端桶 $SRC_B2"
[ "$(list_objects "$DST_EP" "$DST_AK" "$DST_SK" "$SRC_B1")" = "$SRC_B1_OBJS" ] ||
	fail "--delete 后目标桶未镜像源端"
[ "$(list_objects "$DST_EP" "$DST_AK" "$DST_SK" "$SRC_B2")" = "$SRC_B2_OBJS" ] ||
	fail "--delete 后目标第二桶未镜像源端"
ok "多余对象删除、目标独有桶保留、源端列表不变"

echo "== 6. 负例：版本化源桶 fail-closed，目标零写入 =="
TGT_SNAP_N=$(snapshot "$DST_EP" "$DST_AK" "$DST_SK")
src_aws s3 mb "s3://$VB" >/dev/null || fail "建版本化桶"
src_aws s3api put-bucket-versioning --bucket "$VB" --versioning-configuration Status=Enabled >/dev/null ||
	fail "开版本控制"
src_aws s3 cp "$W/tiny.txt" "s3://$VB/v1.txt" >/dev/null || fail "版本桶上传"
run_script
assert_rc 1 "版本化源桶应拒绝"
assert_out "$VB" "拒绝信息应点名版本化桶"
assert_out "版本状态为 Enabled" "应说明版本状态"
assert_target_unchanged "$TGT_SNAP_N" "版本化源桶拒绝时"
ok "版本化源桶 fail-closed 且目标零写入"
purge_bucket "$VB" # 否则下一轮仍先撞版本化桶，开锁桶负例测不到

echo "== 7. 负例：开锁源桶 fail-closed，目标零写入 =="
src_aws s3api create-bucket --bucket "$LB" --object-lock-enabled-for-bucket >/dev/null ||
	fail "建开锁桶"
src_aws s3 cp "$W/tiny.txt" "s3://$LB/locked.txt" >/dev/null || fail "开锁桶上传"
run_script
assert_rc 1 "开锁源桶应拒绝"
assert_out "$LB" "拒绝信息应点名开锁桶"
assert_target_unchanged "$TGT_SNAP_N" "开锁源桶拒绝时"
ok "开锁源桶 fail-closed 且目标零写入（版本控制守卫先触发，同为 fail-closed）"

echo "== 8. 负例：源=目标 endpoint 拒绝 =="
SRC_SNAP_N=$(snapshot "$SRC_EP" "$SRC_AK" "$SRC_SK")
invoke "$SRC_EP" "$SRC_EP"
assert_rc 1 "同 endpoint 应拒绝"
assert_out "相同" "应说明 endpoint 相同"
[ "$(snapshot "$SRC_EP" "$SRC_AK" "$SRC_SK")" = "$SRC_SNAP_N" ] || fail "同 endpoint 拒绝时源端被改"
assert_target_unchanged "$TGT_SNAP_N" "同 endpoint 拒绝时"
ok "源=目标 endpoint 拒绝"

echo
echo "== 结果 =="
echo "PASS: $pass 项（single-to-cluster 真实集群 e2e，耗时 $(( $(date +%s) - T0 ))s；日志 $RUN）"
