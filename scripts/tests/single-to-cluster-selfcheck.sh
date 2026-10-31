#!/usr/bin/env bash
# scripts/single-to-cluster.sh 自检：mock aws/rclone + 固定 fixture，无网络、无真实服务端。
# 覆盖 --plan 盘点/约束/allow-deny、复制默认路径/fail-closed、--resume 检查点、
# --verify 数量/字节/ETag 漂移与 JSON 输出；并确认凭据不出现在 argv 回显/日志。
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MAIN="$HERE/../single-to-cluster.sh"
VERIFY="$HERE/../single-to-cluster-verify.sh"
[[ -x $MAIN && -x $VERIFY ]] || {
	echo "找不到可执行脚本: $MAIN / $VERIFY" >&2
	exit 1
}

TMP=$(mktemp -d "${TMPDIR:-/tmp}/single-to-cluster-test.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT
BIN="$TMP/bin"
FIX="$TMP/objects"
OUT="$TMP/out"
ERR="$TMP/err"
LOG="$TMP/rclone.log"
mkdir -p "$BIN" "$FIX"
: >"$LOG"
export MOCK_RCLONE_LOG="$LOG"

# ---------------------------------------------------------------- fixtures
printf 'a.txt\t10\t"etag-a"\ndir/b.bin\t20\t"etag-b"\n' >"$FIX/src.alpha.tsv"
printf 'a.txt\t10\t"etag-a"\ndir/b.bin\t20\t"etag-b"\n' >"$FIX/dst.alpha.tsv"
printf 'c.txt\t5\t"etag-c"\n' >"$FIX/src.beta.tsv"
printf 'c.txt\t5\tetag-c\n' >"$FIX/dst.beta.tsv" # 未加引号，验证 ETag 归一化
printf 'v.txt\t1\t"v"\n' >"$FIX/src.versioned.tsv"
printf 'l.txt\t1\t"l"\n' >"$FIX/src.locked.tsv"
printf 'big\t100\t"abc-2"\n' >"$FIX/src.mpart.tsv"
printf 'big\t100\t"xyz-2"\n' >"$FIX/dst.mpart.tsv"
printf 'k1\t1\t"e1"\n' >"$FIX/src.driftmiss.tsv"
: >"$FIX/dst.driftmiss.tsv"
printf 'k\t10\t"e1"\n' >"$FIX/src.driftsize.tsv"
printf 'k\t11\t"e1"\n' >"$FIX/dst.driftsize.tsv"
printf 'k\t10\t"e1"\n' >"$FIX/src.drifetag.tsv"
printf 'k\t10\t"e2"\n' >"$FIX/dst.drifetag.tsv"
printf 'n\t1\t"e"\n' >"$FIX/src.newbucket.tsv"
printf 'a\t1\t"e"\n' >"$FIX/src.extra.tsv"
printf 'a\t1\t"e"\nb\t2\t"e"\n' >"$FIX/dst.extra.tsv"

cat >"$BIN/aws" <<'MOCK'
#!/usr/bin/env bash
set -uo pipefail
ep="" op="" bucket="" query=""
while (($#)); do
	case "$1" in
	--endpoint-url) ep=$2; shift 2 ;;
	--bucket) bucket=$2; shift 2 ;;
	--query) query=$2; shift 2 ;;
	--output) shift 2 ;;
	s3api) shift ;;
	list-buckets | get-bucket-versioning | get-object-lock-configuration | get-bucket-encryption | list-objects-v2)
		op=$1; shift ;;
	*) shift ;;
	esac
done
[[ -n $ep ]] || { echo "mock aws: missing --endpoint-url" >&2; exit 90; }
if [[ $ep == "$MOCK_SRC_ENDPOINT" ]]; then side=src; else side=dst; fi
if [[ -n ${MOCK_AWS_ERROR:-} ]]; then
	echo "An error occurred (AccessDenied) when calling the ${op:-unknown} operation" >&2
	exit 254
fi
in_list() { case " ${1:-} " in *" $2 "*) return 0 ;; *) return 1 ;; esac; }
case "$op" in
list-buckets)
	if [[ -n ${MOCK_LISTBUCKETS_ERROR:-} ]]; then echo "InternalError" >&2; exit 254; fi
	if [[ $side == src ]]; then printf '%s\n' "${MOCK_SRC_BUCKETS:-}"; else printf '%s\n' "${MOCK_DST_BUCKETS:-}"; fi
	;;
get-bucket-versioning)
	if [[ -n ${MOCK_VERSIONING_ERROR:-} ]]; then echo "InternalError" >&2; exit 254; fi
	if [[ $side == src ]]; then v=${MOCK_SRC_VERSIONED:-}; else v=${MOCK_DST_VERSIONED:-}; fi
	if in_list "$v" "$bucket"; then echo Enabled; else echo Off; fi
	;;
get-object-lock-configuration)
	if [[ -n ${MOCK_LOCK_ERROR:-} ]]; then echo "InternalError" >&2; exit 254; fi
	if [[ $side == src ]]; then v=${MOCK_SRC_LOCKED:-}; else v=${MOCK_DST_LOCKED:-}; fi
	if in_list "$v" "$bucket"; then
		echo Enabled
	else
		echo "An error occurred (ObjectLockConfigurationNotFoundError) when calling the GetObjectLockConfiguration operation" >&2
		exit 254
	fi
	;;
get-bucket-encryption)
	if [[ -n ${MOCK_ENCRYPTION_ERROR:-} ]]; then echo "InternalError" >&2; exit 254; fi
	if [[ $side == src ]]; then v=${MOCK_SRC_ENCRYPTED:-}; else v=${MOCK_DST_ENCRYPTED:-}; fi
	if in_list "$v" "$bucket"; then printf '%s\n' "${MOCK_ENCRYPTION_VALUE:-AES256}"
	else
		echo "An error occurred (ServerSideEncryptionConfigurationNotFoundError) when calling the GetBucketEncryption operation" >&2
		exit 254
	fi
	;;
list-objects-v2)
	if [[ -n ${MOCK_LIST_ERROR:-} ]]; then echo "InternalError" >&2; exit 254; fi
	f="$MOCK_OBJECTS_DIR/$side.$bucket.tsv"
	if [[ $query == *ETag* ]]; then
		[[ -f $f ]] && while IFS= read -r line; do printf '%s\n' "$line"; done <"$f"
	elif [[ $query == *Size* ]]; then
		[[ -f $f ]] && awk -F'\t' 'NF >= 2 { print $2 }' "$f"
	fi
	;;
*)
	echo "mock aws: unexpected operation '$op'" >&2
	exit 99
	;;
esac
MOCK

cat >"$BIN/rclone" <<'MOCK'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"$MOCK_RCLONE_LOG"
if [[ -n ${MOCK_RCLONE_FAIL:-} && $* == *"$MOCK_RCLONE_FAIL"* ]]; then exit 1; fi
MOCK
chmod +x "$BIN/aws" "$BIN/rclone"
export PATH="$BIN:$PATH"
export MOCK_SRC_ENDPOINT=http://127.0.0.1:19000
export MOCK_DST_ENDPOINT=http://127.0.0.1:19001
export MOCK_OBJECTS_DIR="$FIX"
export OSS_SRC_AK=AKIA_SRC OSS_SRC_SK=SK_SRC_SECRET
export OSS_DST_AK=AKIA_DST OSS_DST_SK=SK_DST_SECRET

PASS=0
fail() { echo "FAIL: $*" >&2; exit 1; }
ok() { PASS=$((PASS + 1)); printf 'ok   %s\n' "$1"; }
RC=0
run() { bash "$MAIN" "$@" >"$OUT" 2>"$ERR"; RC=$?; }
run_missing_src_ak() { env OSS_SRC_AK= bash "$MAIN" "$@" >"$OUT" 2>"$ERR"; RC=$?; }
run_missing_dst_sk() { env OSS_DST_SK= bash "$MAIN" "$@" >"$OUT" 2>"$ERR"; RC=$?; }
assert_rc() { [[ $RC == "$1" ]] || fail "$2（exit=$RC，期望 $1）stdout=$(head -c 400 "$OUT") stderr_tail=$(tail -30 "$ERR")"; }
assert_out() { grep -qF -- "$1" "$OUT" || fail "$2（stdout 缺少: $1）$(head -c 400 "$OUT")"; }
assert_err() { grep -qF -- "$1" "$ERR" || fail "$2（stderr 缺少: $1）$(head -c 400 "$ERR")"; }
assert_log() { grep -qF -- "$1" "$LOG" || fail "$2（rclone 调用缺少: $1）$(head -c 400 "$LOG")"; }
assert_no_log() { ! grep -qF -- "$1" "$LOG" || fail "$2（不该出现: $1）$(head -c 400 "$LOG")"; }
assert_secret_free() {
	local f
	for f in "$OUT" "$ERR" "$LOG"; do
		! grep -qF -- "SK_SRC_SECRET" "$f" || fail "$1：源 SK 泄漏到 $f"
		! grep -qF -- "SK_DST_SECRET" "$f" || fail "$1：目标 SK 泄漏到 $f"
	done
}
jassert() { # desc file python-assert-expression
	local desc=$1 file=$2 expr=$3
	if python3 -c "import json,sys; d=json.load(open(sys.argv[1]))" "$file" 2>/dev/null; then :; else
		fail "$desc：输出不是合法 JSON（$(head -c 300 "$file")）"
	fi
	if python3 -c "import json,sys; d=json.load(open(sys.argv[1])); assert $expr" "$file" 2>/dev/null; then
		ok "$desc"
	else
		fail "$desc：JSON 断言失败: $expr"
	fi
}

reset_mock() {
	export MOCK_SRC_BUCKETS="alpha beta"
	export MOCK_DST_BUCKETS="alpha beta"
	export MOCK_SRC_VERSIONED="" MOCK_DST_VERSIONED=""
	export MOCK_SRC_LOCKED="" MOCK_DST_LOCKED=""
	export MOCK_SRC_ENCRYPTED="alpha" MOCK_DST_ENCRYPTED=""
	unset MOCK_AWS_ERROR MOCK_LISTBUCKETS_ERROR MOCK_VERSIONING_ERROR MOCK_LOCK_ERROR \
		MOCK_ENCRYPTION_ERROR MOCK_LIST_ERROR MOCK_RCLONE_FAIL MOCK_ENCRYPTION_VALUE
	: >"$LOG"
}
base=(--source-endpoint "$MOCK_SRC_ENDPOINT" --dest-endpoint "$MOCK_DST_ENDPOINT")

echo "== 帮助与用法 =="
reset_mock
run --help
assert_rc 0 "--help 应成功"
assert_out "--plan" "--help 应含 --plan"
assert_out "--verify" "--help 应含 --verify"
assert_out "--resume" "--help 应含 --resume"
assert_out "--allow-versioned" "--help 应含 --allow-versioned"
assert_out "OSS_SRC_AK" "--help 应说明凭据来自环境变量"
assert_no_log "copy" "--help 不应调用 rclone"
ok "帮助列出新接口"

reset_mock
run_missing_src_ak "${base[@]}" --plan
assert_rc 2 "缺源凭据应 exit 2"
ok "凭据缺失拒绝"

reset_mock
run_missing_dst_sk "${base[@]}" --verify
assert_rc 2 "缺目标凭据应 exit 2"
ok "目标凭据缺失拒绝"

reset_mock
run "${base[@]}" --source-sk=TOP_SECRET
assert_rc 2 "argv 凭据选项应被拒绝"
! grep -qF "TOP_SECRET" "$OUT" "$ERR" || fail "未知选项回显泄漏了 argv 密钥"
ok "argv 密钥不回显"

echo "== --plan：盘点、约束、JSON、零写入 =="
reset_mock
run "${base[@]}" --plan --json
assert_rc 0 "普通桶 plan 应成功"
jassert "plan JSON 合法且 status=ok" "$OUT" "d['status']=='ok' and d['exit_code']==0"
jassert "plan 逐桶对象数/字节" "$OUT" \
	"next(b for b in d['buckets'] if b['name']=='alpha')['objects']==2 and next(b for b in d['buckets'] if b['name']=='alpha')['bytes']==30"
jassert "plan 合计对象数/字节" "$OUT" "d['totals']['objects']==3 and d['totals']['bytes']==35"
jassert "plan 读取桶加密约束" "$OUT" \
	"next(b for b in d['buckets'] if b['name']=='alpha')['encryption']=='AES256'"
[[ ! -s $LOG ]] || fail "plan 不应调用 rclone: $(head -c 300 "$LOG")"
assert_secret_free "plan"
ok "只读盘点 + JSON 零写入"

reset_mock
run "${base[@]}" --dry-run
assert_rc 0 "--dry-run（=--plan）应成功"
assert_out "[plan] 目标桶: alpha" "文本 plan 应列出目标桶"
assert_out "结果: OK" "文本 plan 应给出结果"
[[ ! -s $LOG ]] || fail "--dry-run 不应调用 rclone"
ok "--dry-run 与 --plan 等价且零写入"

reset_mock
export MOCK_SRC_BUCKETS="alpha versioned" MOCK_SRC_VERSIONED="versioned"
run "${base[@]}" --plan --json
assert_rc 1 "版本化桶未 opt-in 时 plan 应 exit 1"
jassert "版本化桶标记 blocked" "$OUT" "d['status']=='blocked' and any('versioned' in x for x in d['blocked'])"
run "${base[@]}" --plan --json --allow-versioned
assert_rc 0 "显式 --allow-versioned 后 plan 应成功"
jassert "allow-versioned 记录 warning" "$OUT" "d['status']=='ok' and len(d['warnings'])>0"
ok "版本化 fail-closed / 显式 opt-in"

reset_mock
export MOCK_SRC_BUCKETS="alpha locked" MOCK_SRC_LOCKED="locked"
run "${base[@]}" --plan
assert_rc 1 "开锁桶未 opt-in 时 plan 应 exit 1"
assert_out "已启用对象锁" "应说明对象锁约束"
run "${base[@]}" --plan --allow-object-lock
assert_rc 0 "显式 --allow-object-lock 后 plan 应成功"
ok "对象锁 fail-closed / 显式 opt-in"

reset_mock
run "${base[@]}" --plan --json --include-bucket alpha
assert_rc 0 "include 过滤应成功"
jassert "include 只保留 alpha" "$OUT" "[b['name'] for b in d['buckets']]==['alpha']"
run "${base[@]}" --plan --json --exclude-bucket alpha
assert_rc 0 "exclude 过滤应成功"
jassert "exclude 去掉 alpha" "$OUT" "[b['name'] for b in d['buckets']]==['beta']"
run "${base[@]}" --plan --json --include-bucket alpha --exclude-bucket alpha
assert_rc 0 "deny 优先且无选中桶应成功"
jassert "deny 优先" "$OUT" "d['totals']['buckets']==0"
run "${base[@]}" --plan --json --include-bucket nope
assert_rc 1 "include 不存在的桶应 exit 1"
jassert "missing include 报告" "$OUT" "d['status']=='blocked' and d['missing_include']==['nope']"
ok "桶 allow/deny 列表"

reset_mock
export MOCK_LIST_ERROR=1
run "${base[@]}" --plan --json
assert_rc 1 "对象盘点失败应 fail-closed"
jassert "盘点失败标记 blocked" "$OUT" "d['status']=='blocked'"
ok "盘点失败 fail-closed"

echo "== 复制默认路径 =="
reset_mock
run "${base[@]}"
assert_rc 0 "默认复制应成功"
assert_log "mkdir dst:alpha" "应创建目标桶 alpha"
assert_log "mkdir dst:beta" "应创建目标桶 beta"
assert_log "copy src:alpha dst:alpha" "应 copy alpha"
assert_log "copy src:beta dst:beta" "应 copy beta"
assert_log "--metadata" "应保留元数据"
assert_no_log "sync" "默认不应 sync"
assert_secret_free "默认复制"
ok "默认 copy 行为保留"

reset_mock
run "${base[@]}" --delete --transfers 7 --checkers 3 --bwlimit 2M
assert_rc 0 "--delete/并发/限速应成功"
assert_log "sync src:alpha dst:alpha" "--delete 应切 sync"
assert_log "--transfers 7" "应透传 --transfers"
assert_log "--checkers 3" "应透传 --checkers"
assert_log "--bwlimit 2M" "应透传 --bwlimit"
ok "--delete 与传输参数"

reset_mock
export MOCK_SRC_BUCKETS="alpha versioned" MOCK_SRC_VERSIONED="versioned"
run "${base[@]}"
assert_rc 1 "版本化桶复制应 fail-closed"
assert_err "版本状态为 Enabled" "应说明版本状态"
[[ ! -s $LOG ]] || fail "fail-closed 时不应调用 rclone"
ok "复制前版本守卫"

reset_mock
export MOCK_SRC_BUCKETS="alpha locked" MOCK_SRC_LOCKED="locked"
run "${base[@]}"
assert_rc 1 "开锁桶复制应 fail-closed"
assert_err "已启用对象锁" "应说明对象锁"
[[ ! -s $LOG ]] || fail "fail-closed 时不应调用 rclone"
ok "复制前对象锁守卫"

reset_mock
export MOCK_SRC_BUCKETS="alpha versioned" MOCK_SRC_VERSIONED="versioned"
run "${base[@]}" --allow-versioned
assert_rc 0 "allow-versioned 后复制应成功"
assert_err "已版本化" "应打印允许风险 warning"
assert_log "copy src:versioned dst:versioned" "应复制版本化桶当前版本"
ok "显式 opt-in 后复制当前版本"

reset_mock
run "${base[@]}" --include-bucket beta
assert_rc 0 "include 复制应成功"
assert_log "copy src:beta dst:beta" "应复制 beta"
assert_no_log "src:alpha" "不应复制 alpha"
ok "复制范围 allow/deny"

echo "== --resume 检查点 =="
reset_mock
STATE="$TMP/resume.state"
rm -f "$STATE"
run "${base[@]}" --resume "$STATE"
assert_rc 0 "首次带检查点复制应成功"
grep -q '^done	alpha$' "$STATE" || fail "检查点应记录 alpha"
grep -q '^done	beta$' "$STATE" || fail "检查点应记录 beta"
: >"$LOG"
run "${base[@]}" --resume "$STATE"
assert_rc 0 "检查点重跑应成功"
assert_out "跳过检查点中已完成的桶: alpha" "重跑应跳过 alpha"
[[ ! -s $LOG ]] || fail "已完成桶重跑不应再调用 rclone: $(head -c 300 "$LOG")"
ok "检查点按桶续跑"

BAD="$TMP/bad.state"
printf 'single-to-cluster-resume-v1\nsrc\thttp://other:1\ndst\t%s\nmode\tcopy\n' \
	"$(printf '%s' "$MOCK_DST_ENDPOINT" | tr 'A-Z' 'a-z')" >"$BAD"
: >"$LOG"
run "${base[@]}" --resume "$BAD"
assert_rc 1 "检查点端点不一致应拒绝"
assert_err "源 endpoint 与本次不一致" "应说明端点不一致"
[[ ! -s $LOG ]] || fail "端点不一致时不应调用 rclone"
run "${base[@]}" --delete --resume "$STATE"
assert_rc 1 "检查点模式不一致应拒绝"
assert_err "模式为 'copy'" "应说明模式不一致"
ok "检查点端点/模式绑定"

echo "== --verify：数量/字节/ETag =="
reset_mock
run "${base[@]}" --verify --json
assert_rc 0 "一致数据 verify 应 exit 0"
jassert "verify JSON 合法且无漂移" "$OUT" "d['status']=='ok' and d['drift'] is False"
jassert "verify 桶计数/字节" "$OUT" \
	"next(b for b in d['buckets'] if b['name']=='alpha')['source_objects']==2 and next(b for b in d['buckets'] if b['name']=='alpha')['dest_bytes']==30"
[[ ! -s $LOG ]] || fail "verify 不应调用 rclone"
assert_secret_free "verify"
ok "verify 一致数据"

reset_mock
export MOCK_SRC_BUCKETS=driftmiss MOCK_DST_BUCKETS=driftmiss
run "${base[@]}" --verify --json
assert_rc 1 "缺失对象应 exit 1"
jassert "缺失键报告" "$OUT" \
	"d['drift'] is True and next(b for b in d['buckets'] if b['name']=='driftmiss')['missing_in_dest']==['k1']"
ok "verify 缺失对象漂移"

reset_mock
export MOCK_SRC_BUCKETS=driftsize MOCK_DST_BUCKETS=driftsize
run "${base[@]}" --verify --json
assert_rc 1 "大小不一致应 exit 1"
jassert "大小不一致报告" "$OUT" \
	"next(b for b in d['buckets'] if b['name']=='driftsize')['size_mismatch']==['k']"
ok "verify 大小漂移"

reset_mock
export MOCK_SRC_BUCKETS=drifetag MOCK_DST_BUCKETS=drifetag
run "${base[@]}" --verify --json
assert_rc 1 "单段 ETag 不一致应 exit 1"
jassert "单段 ETag 报告" "$OUT" \
	"next(b for b in d['buckets'] if b['name']=='drifetag')['etag_mismatch']==['k']"
ok "verify 单段 ETag 漂移"

reset_mock
export MOCK_SRC_BUCKETS=mpart MOCK_DST_BUCKETS=mpart
run "${base[@]}" --verify --json
assert_rc 0 "分片 ETag 不同但大小一致不应误判漂移"
jassert "分片 ETag 计为 unverified" "$OUT" \
	"d['drift'] is False and next(b for b in d['buckets'] if b['name']=='mpart')['etag_unverified']==['big']"
ok "verify 分片 ETag 语义"

reset_mock
export MOCK_SRC_BUCKETS=newbucket MOCK_DST_BUCKETS=""
run "${base[@]}" --verify --json
assert_rc 1 "目标桶不存在应 exit 1"
jassert "目标桶缺失报告" "$OUT" \
	"next(b for b in d['buckets'] if b['name']=='newbucket')['drift'] is True and '目标桶不存在' in next(b for b in d['buckets'] if b['name']=='newbucket')['error']"
ok "verify 目标桶缺失"

reset_mock
export MOCK_SRC_BUCKETS=extra MOCK_DST_BUCKETS=extra
run "${base[@]}" --verify --json
assert_rc 1 "目标多余对象应 exit 1"
jassert "目标多余报告" "$OUT" \
	"next(b for b in d['buckets'] if b['name']=='extra')['extra_in_dest']==['b']"
ok "verify 目标多余对象"

reset_mock
export MOCK_SRC_BUCKETS="" MOCK_DST_BUCKETS=""
run "${base[@]}" --verify --json
assert_rc 0 "空源端 verify 应成功"
jassert "空源端 JSON" "$OUT" "d['totals']['buckets']==0 and d['drift'] is False"
ok "verify 空源端"

reset_mock
run --source-endpoint "$MOCK_SRC_ENDPOINT" --dest-endpoint "$MOCK_SRC_ENDPOINT"
assert_rc 1 "源=目标 endpoint 应拒绝"
ok "同 endpoint 拒绝"

reset_mock
run --source-endpoint "http://user:pass@127.0.0.1:19000" --dest-endpoint "$MOCK_DST_ENDPOINT"
assert_rc 2 "endpoint 含 userinfo 应拒绝"
! grep -qF "pass@" "$OUT" "$ERR" || fail "userinfo 被回显"
ok "endpoint userinfo 拒绝"

reset_mock
run "${base[@]}" --json
assert_rc 2 "复制模式 --json 应拒绝"
run "${base[@]}" --plan --verify
assert_rc 2 "--plan 与 --verify 同时使用应拒绝"
run "${base[@]}" --plan --resume "$TMP/x.state"
assert_rc 2 "--plan 与 --resume 同时使用应拒绝"
ok "模式组合校验"

echo "== 目标端安全守卫 =="
reset_mock
export MOCK_DST_VERSIONED="alpha"
run "${base[@]}" --plan --json
assert_rc 1 "目标版本化桶 plan 应 blocked"
jassert "目标版本化标记 blocked" "$OUT" "d['status']=='blocked'"
: >"$LOG"
run "${base[@]}"
assert_rc 1 "目标版本化桶复制应 fail-closed"
[[ ! -s $LOG ]] || fail "目标 fail-closed 时不应调用 rclone"
ok "目标端版本化 fail-closed"

reset_mock
export MOCK_DST_LOCKED="beta"
run "${base[@]}"
assert_rc 1 "目标开锁桶复制应 fail-closed"
[[ ! -s $LOG ]] || fail "目标 fail-closed 时不应调用 rclone"
ok "目标端对象锁 fail-closed"

reset_mock
run "${base[@]}" --verify --json --include-bucket alpha
assert_rc 0 "verify 支持 include 过滤"
jassert "verify include 只校验 alpha" "$OUT" "[b['name'] for b in d['buckets']]==['alpha']"
ok "verify 范围过滤"

echo
echo "PASS: $PASS 项自检全部通过"
