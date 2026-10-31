#!/usr/bin/env bash
# 单机 → 集群重新灌入 e2e：当前二进制、两个独立 loopback 进程、完全离线。
#
# 与 tests/e2e_single_to_cluster.sh 互补：后者需要 4 节点 × 3 盘集群和专门构建；
# 本脚本只用同一个 BIN 起「源单机 + 目标端」两个进程，覆盖：
#   * --plan --json：普通桶可迁移；版本化桶 fail-closed（status=blocked，exit 1）
#   * --plan --json --allow-versioned：版本化桶放行（status=ok，exit 0）
#   * --include-bucket 只复制普通桶：桶集合、对象键/大小、字节一致；单段 ETag 一致；
#     源端 10MiB MPU 对象 ETag 分片形态可能与目标不同，由 --verify 记 etag_unverified
#   * --verify --json：对象数/字节/ETag 无漂移
#   * --resume：首次复制落检查点，重跑跳过已完成桶
#
# 数据目录全部在 /tmp（/home 可能磁盘紧张），不访问真实网络/云。
# 需要：单机/集群二进制（BIN，默认 /tmp/target-main/debug/oss-server）、aws-cli、
#   rclone、curl、python3。
# 退出码：0=PASS；1=FAIL；77=SKIP（依赖缺失，未执行任何断言）。
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT="$ROOT/scripts/single-to-cluster.sh"
BIN="${BIN:-/tmp/target-main/debug/oss-server}"
RUN="${RUN:-/tmp/e2e-single-to-cluster-real}"
T0=$(date +%s)

[ -f "$SCRIPT" ] || {
	echo "SKIP: 找不到 $SCRIPT"
	exit 77
}
missing=()
for c in aws rclone curl python3; do
	command -v "$c" >/dev/null 2>&1 || missing+=("$c")
done
[ -x "$BIN" ] || missing+=("oss-server（BIN=$BIN）")
if ((${#missing[@]})); then
	echo "SKIP: 缺失 ${missing[*]}"
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
mkdir -p "$RUN/src/d0" "$RUN/dst/d0"
W=$(mktemp -d /tmp/e2e-single-real.XXXXXX)
PIDS=()
cleanup() {
	for p in "${PIDS[@]:-}"; do
		[ -n "$p" ] || continue
		kill "$p" 2>/dev/null || true
	done
	rm -rf "$W"
}
trap cleanup EXIT

pass=0
fail() {
	echo "FAIL: $*" >&2
	for n in src dst; do
		[ -f "$RUN/$n/server.log" ] && tail -5 "$RUN/$n/server.log" >&2 || true
	done
	echo "== 结果: FAIL（日志保留在 $RUN）" >&2
	exit 1
}
ok() {
	pass=$((pass + 1))
	echo "   ok: $*"
}

SRC_PORT=$(free_port)
DST_PORT=$(free_port)
SRC_ADMIN_USER=srcadmin
SRC_ADMIN_PW=src-pass-9f3a
DST_ADMIN_USER=dstadmin
DST_ADMIN_PW=dst-pass-7c1e

cat >"$RUN/src/oss.toml" <<EOF
listen = "127.0.0.1:$SRC_PORT"
data_dir = "$RUN/src/d0"
staging_dir = "$RUN/src/staging"
database_url = "sqlite://$RUN/src/oss.db?mode=rwc"
jwt_secret = "src-jwt-secret-0123456789abcdef0123456789"
max_put_bytes = 268435456
EOF
cat >"$RUN/dst/oss.toml" <<EOF
listen = "127.0.0.1:$DST_PORT"
data_dir = "$RUN/dst/d0"
staging_dir = "$RUN/dst/staging"
database_url = "sqlite://$RUN/dst/oss.db?mode=rwc"
jwt_secret = "dst-jwt-secret-0123456789abcdef0123456789"
max_put_bytes = 268435456
EOF

start_server() { # $1=目录 $2=管理员 $3=口令
	(cd "$1" && OSS_ADMIN_USER="$2" OSS_ADMIN_PASSWORD="$3" \
		exec "$BIN" serve >>"$1/server.log" 2>&1) &
	PIDS+=($!)
}
wait_http() { # $1=HTTP 端口
	for _t in $(seq 1 90); do
		s=$(curl -s -o /dev/null -w '%{http_code}' --noproxy '*' --max-time 1 \
			"http://127.0.0.1:$1/health/live" 2>/dev/null || true)
		[ "$s" = 200 ] && return 0
		sleep 1
	done
	return 1
}

echo "== 0. 起两个 loopback 进程（源 :$SRC_PORT，目标 :$DST_PORT）=="
start_server "$RUN/src" "$SRC_ADMIN_USER" "$SRC_ADMIN_PW"
start_server "$RUN/dst" "$DST_ADMIN_USER" "$DST_ADMIN_PW"
wait_http "$SRC_PORT" || fail "源端未就绪"
wait_http "$DST_PORT" || fail "目标端未就绪"
SRC_EP="http://127.0.0.1:$SRC_PORT"
DST_EP="http://127.0.0.1:$DST_PORT"

login() { # $1=endpoint $2=user $3=pass
	curl -s --noproxy '*' --max-time 30 -X POST "$1/api/v1/login" \
		-H 'content-type: application/json' \
		-d "{\"username\":\"$2\",\"password\":\"$3\"}" |
		python3 -c 'import json,sys;print(json.load(sys.stdin).get("token",""))'
}
mint_key() { # $1=endpoint $2=token；输出 "AK SK"
	curl -s --noproxy '*' --max-time 30 -X POST "$1/api/v1/access-keys" \
		-H "authorization: Bearer $2" -H 'content-type: application/json' -d '{}' |
		python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get("access_key",""),d.get("secret_key",""))'
}

TOK=$(login "$SRC_EP" "$SRC_ADMIN_USER" "$SRC_ADMIN_PW") || fail "源端管理员登录失败"
[ -n "$TOK" ] || fail "源端管理员登录无 token"
KEYS=$(mint_key "$SRC_EP" "$TOK") || fail "源端 AK 铸造失败"
read -r SRC_AK SRC_SK <<<"$KEYS"
TOK=$(login "$DST_EP" "$DST_ADMIN_USER" "$DST_ADMIN_PW") || fail "目标端管理员登录失败"
[ -n "$TOK" ] || fail "目标端管理员登录无 token"
KEYS=$(mint_key "$DST_EP" "$TOK") || fail "目标端 AK 铸造失败"
read -r DST_AK DST_SK <<<"$KEYS"
[ -n "$SRC_AK" ] && [ -n "$SRC_SK" ] && [ -n "$DST_AK" ] && [ -n "$DST_SK" ] ||
	fail "AK/SK 铸造失败"

ep_aws() { # $1=endpoint $2=ak $3=sk，其余透传
	local ep=$1 ak=$2 sk=$3
	shift 3
	env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
		NO_PROXY=127.0.0.1 no_proxy=127.0.0.1 \
		AWS_ACCESS_KEY_ID="$ak" AWS_SECRET_ACCESS_KEY="$sk" \
		AWS_DEFAULT_REGION=us-east-1 AWS_PAGER="" aws --endpoint-url "$ep" "$@"
}
src_aws() { ep_aws "$SRC_EP" "$SRC_AK" "$SRC_SK" "$@"; }
dst_aws() { ep_aws "$DST_EP" "$DST_AK" "$DST_SK" "$@"; }

list_buckets() { # $1=endpoint $2=ak $3=sk
	ep_aws "$1" "$2" "$3" s3api list-buckets --query 'Buckets[].Name' --output text 2>/dev/null |
		tr '\t ' '\n' | awk 'NF && $0 != "None"' | sort
}
list_objects() { # $1=endpoint $2=ak $3=sk $4=bucket；key<TAB>size
	ep_aws "$1" "$2" "$3" s3api list-objects-v2 --bucket "$4" \
		--query 'Contents[].[Key,Size]' --output text 2>/dev/null | awk 'NF' | sort
}
etag_of() { # $1=endpoint $2=ak $3=sk $4=bucket $5=key
	ep_aws "$1" "$2" "$3" s3api head-object --bucket "$4" --key "$5" \
		--query ETag --output text 2>/dev/null | tr -d '"'
}

STAMP="$(date +%s)$RANDOM"
B1="normal-$STAMP"
VB="versioned-$STAMP"

echo "== 1. 播种源端（普通桶 3 对象含 10MiB MPU；版本化桶）=="
head -c 65536 /dev/urandom >"$W/small.bin"
head -c 10485760 /dev/urandom >"$W/big.bin"
printf 'single-to-cluster\n' >"$W/tiny.txt"
TOTAL_BYTES=$((65536 + 10485760 + $(wc -c <"$W/tiny.txt")))
src_aws s3 mb "s3://$B1" >/dev/null || fail "源端建桶 $B1"
src_aws s3 cp "$W/small.bin" "s3://$B1/small.bin" >/dev/null || fail "源端上传 small.bin"
src_aws s3 cp "$W/big.bin" "s3://$B1/deep/nested/big.bin" >/dev/null ||
	fail "源端上传 big.bin"
src_aws s3 cp "$W/tiny.txt" "s3://$B1/docs/tiny.txt" >/dev/null || fail "源端上传 tiny.txt"
BIG_SRC_ETAG=$(etag_of "$SRC_EP" "$SRC_AK" "$SRC_SK" "$B1" deep/nested/big.bin)
[[ $BIG_SRC_ETAG =~ -[0-9]+$ ]] || fail "播种大对象未走 MPU（ETag=$BIG_SRC_ETAG）"
SRC_OBJS=$(list_objects "$SRC_EP" "$SRC_AK" "$SRC_SK" "$B1")
[ "$(printf '%s\n' "$SRC_OBJS" | wc -l)" = 3 ] || fail "源端普通桶对象数不是 3：$SRC_OBJS"

src_aws s3 mb "s3://$VB" >/dev/null || fail "源端建桶 $VB"
src_aws s3api put-bucket-versioning --bucket "$VB" \
	--versioning-configuration Status=Enabled >/dev/null || fail "开启版本控制失败"
src_aws s3 cp "$W/tiny.txt" "s3://$VB/v.txt" >/dev/null || fail "版本桶上传 v1"
src_aws s3 cp "$W/tiny.txt" "s3://$VB/v.txt" >/dev/null || fail "版本桶上传 v2"
[ -z "$(list_buckets "$DST_EP" "$DST_AK" "$DST_SK")" ] || fail "目标端初始应为空"
ok "两进程就绪，源端 2 桶（普通 3 对象/大对象 MPU ETag=$BIG_SRC_ETAG，版本桶 1 对象 2 版本）"

export B1 VB TOTAL_BYTES
jget() { # $1=json 文件 $2=python 表达式（d/B1/VB/TOTAL 可用）
	python3 - "$1" "$2" <<'PY'
import json, os, sys
d = json.load(open(sys.argv[1]))
B1, VB, TOTAL = os.environ["B1"], os.environ["VB"], int(os.environ["TOTAL_BYTES"])
v = eval(sys.argv[2])
if v is True:
    print("true")
elif v is False:
    print("false")
else:
    print(v)
PY
}
assert_json() { # $1=json 文件 $2=表达式 $3=说明
	[ "$(jget "$1" "$2")" = true ] || fail "$3（JSON 断言: $2）$(cat "$1")"
}

OUT="$W/script.out"
RC=0
invoke() { # $1=source-endpoint $2=dest-endpoint，其余透传
	local sep=$1 dep=$2
	shift 2
	RC=0
	env NO_PROXY=127.0.0.1 no_proxy=127.0.0.1 \
		OSS_SRC_AK="$SRC_AK" OSS_SRC_SK="$SRC_SK" \
		OSS_DST_AK="$DST_AK" OSS_DST_SK="$DST_SK" \
		bash "$SCRIPT" --source-endpoint "$sep" --dest-endpoint "$dep" "$@" >"$OUT" 2>&1 || RC=$?
}
run_script() { invoke "$SRC_EP" "$DST_EP" "$@"; }
assert_rc() { [ "$RC" = "$1" ] || fail "$2（exit=$RC，期望 $1）$(cat "$OUT")"; }
assert_out() { grep -qF -- "$1" "$OUT" || fail "$2（输出缺少: $1）$(cat "$OUT")"; }

echo "== 2. --plan --json：普通桶可迁移，版本化桶 fail-closed =="
run_script --plan --json
assert_rc 1 "含版本化桶的 plan 应 blocked"
assert_json "$OUT" 'd["status"] == "blocked" and d["exit_code"] == 1' "plan 状态应为 blocked"
assert_json "$OUT" 'd["totals"]["buckets"] == 2' "plan 应盘点两个桶"
assert_json "$OUT" \
	'any(b["name"] == B1 and b["safe"] is True and b["objects"] == 3 and b["bytes"] == TOTAL and b["dest"]["exists"] is False for b in d["buckets"])' \
	"plan 应报告普通桶可安全迁移"
assert_json "$OUT" \
	'any(b["name"] == VB and b["safe"] is False and b["versioning"] == "Enabled" and "版本状态为 Enabled" in b["reasons"] for b in d["buckets"])' \
	"plan 应因版本化拒绝该桶"
assert_json "$OUT" 'any(VB in x for x in d["blocked"]) and not any(B1 in x for x in d["blocked"])' \
	"blocked 列表应只点名版本化桶"
[ -z "$(list_buckets "$DST_EP" "$DST_AK" "$DST_SK")" ] || fail "plan 写入了目标端"
ok "plan blocked：普通桶 safe、版本化桶 fail-closed、目标零写入"

echo "== 2b. --plan --json --allow-versioned：显式放行 =="
run_script --plan --json --allow-versioned
assert_rc 0 "--allow-versioned 的 plan 应成功"
assert_json "$OUT" 'd["status"] == "ok" and d["exit_code"] == 0' "放行后 plan 应为 ok"
assert_json "$OUT" \
	'any(b["name"] == VB and b["safe"] is True and b["versioning"] == "Enabled" for b in d["buckets"])' \
	"放行后版本化桶应 safe"
assert_json "$OUT" 'any("--allow-versioned" in w for w in d["warnings"])' \
	"应警告仅迁移当前版本"
ok "plan --allow-versioned 放行版本化桶"

echo "== 3. --include-bucket 复制普通桶 =="
run_script --include-bucket "$B1"
assert_rc 0 "只复制普通桶应成功"
assert_out "完成：1 个桶已重新灌入" "应报告复制完成"
DST_BUCKETS=$(list_buckets "$DST_EP" "$DST_AK" "$DST_SK")
[ "$DST_BUCKETS" = "$B1" ] || fail "目标端桶集合应为 $B1，实际：$DST_BUCKETS"
DST_OBJS=$(list_objects "$DST_EP" "$DST_AK" "$DST_SK" "$B1")
[ "$DST_OBJS" = "$SRC_OBJS" ] || fail "目标对象键/大小未镜像源端：$DST_OBJS"
for f in small.bin docs/tiny.txt; do
	dst_aws s3 cp "s3://$B1/$f" "$W/back" >/dev/null || fail "目标端读回 $f 失败"
	cmp "$W/${f##*/}" "$W/back" || fail "$f 字节不一致"
done
dst_aws s3 cp "s3://$B1/deep/nested/big.bin" "$W/big.back" >/dev/null ||
	fail "目标端读回 big.bin 失败"
cmp "$W/big.bin" "$W/big.back" || fail "big.bin 字节不一致"
for f in small.bin docs/tiny.txt; do
	s_etag=$(etag_of "$SRC_EP" "$SRC_AK" "$SRC_SK" "$B1" "$f")
	d_etag=$(etag_of "$DST_EP" "$DST_AK" "$DST_SK" "$B1" "$f")
	[ "$s_etag" = "$d_etag" ] || fail "单段 ETag 不一致: $f（源 $s_etag / 目标 $d_etag）"
done
BIG_DST_ETAG=$(etag_of "$DST_EP" "$DST_AK" "$DST_SK" "$B1" deep/nested/big.bin)
[ -n "$BIG_DST_ETAG" ] || fail "目标大对象缺少 ETag"
# 源端经 aws-cli MPU 上传；rclone 可能单段或按不同分片大小重传，ETag 不保证一致。
# 字节一致由 cmp 保证，ETag 差异由下一步 --verify 的 etag_unverified 记录。
ok "普通桶镜像：桶集合/对象键/大小/字节一致，单段 ETag 一致（大对象源/目标 ETag=$BIG_SRC_ETAG/$BIG_DST_ETAG）"

echo "== 4. --verify --json：无漂移 =="
run_script --verify --include-bucket "$B1" --json
assert_rc 0 "verify 应无漂移"
assert_json "$OUT" 'd["status"] == "ok" and d["drift"] is False' "verify 状态应为 ok"
assert_json "$OUT" \
	'any(b["name"] == B1 and b["source_objects"] == 3 and b["dest_objects"] == 3 and b["source_bytes"] == TOTAL and b["dest_bytes"] == TOTAL and b["missing_in_dest"] == [] and b["extra_in_dest"] == [] and b["size_mismatch"] == [] and b["etag_mismatch"] == [] for b in d["buckets"])' \
	"verify 应报告对象数/字节/ETag 无漂移"
if [ "$BIG_SRC_ETAG" != "$BIG_DST_ETAG" ]; then
	assert_json "$OUT" 'any("deep/nested/big.bin" in b["etag_unverified"] for b in d["buckets"])' \
		"分片 ETag 不同时应记为 unverified"
else
	assert_json "$OUT" 'all("deep/nested/big.bin" not in b["etag_unverified"] for b in d["buckets"])' \
		"分片 ETag 相同时不应记为 unverified"
fi
ok "verify 只读、零漂移，分片 ETag 差异按 unverified 处理"

echo "== 5. --resume：落检查点后重跑跳过已完成桶 =="
RESUME="$W/resume.tsv"
run_script --include-bucket "$B1" --resume "$RESUME"
assert_rc 0 "带 --resume 的复制应成功"
awk -F'\t' -v b="$B1" '$1 == "done" && $2 == b { found = 1 } END { exit !found }' "$RESUME" ||
	fail "检查点未记录 $B1"
run_script --include-bucket "$B1" --resume "$RESUME"
assert_rc 0 "重跑 --resume 应成功"
assert_out "跳过检查点中已完成的桶: $B1" "重跑应跳过已完成桶"
[ "$(list_objects "$DST_EP" "$DST_AK" "$DST_SK" "$B1")" = "$SRC_OBJS" ] ||
	fail "重跑后目标对象键/大小漂移"
ok "检查点落盘、重跑跳过且目标仍镜像源端"

echo
echo "== 结果 =="
echo "PASS: $pass 项（single-to-cluster 双 loopback 真实 e2e，耗时 $(( $(date +%s) - T0 ))s；日志 $RUN）"
