#!/usr/bin/env bash
# 自检：用 mock aws/rclone 验证 scripts/single-to-cluster.sh 的参数校验与 fail-closed
# 逻辑，不需要真实服务端。真实双端复制见报告（人工执行）。
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
SCRIPT="$ROOT/scripts/single-to-cluster.sh"
[[ -f $SCRIPT ]] || {
	echo "找不到 $SCRIPT" >&2
	exit 1
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
BIN="$WORK/bin"
mkdir -p "$BIN"
LOG="$WORK/rclone.log"
OUT="$WORK/out"
export MOCK_RCLONE_LOG="$LOG"

cat >"$BIN/aws" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
ep="" op="" bucket=""
while (($#)); do
	case "$1" in
	--endpoint-url) ep=$2; shift 2 ;;
	--bucket) bucket=$2; shift 2 ;;
	--query | --output) shift 2 ;;
	s3api) shift ;;
	list-buckets | get-bucket-versioning | get-object-lock-configuration) op=$1; shift ;;
	*) shift ;;
	esac
done
if [[ -n ${MOCK_AWS_ERROR:-} ]]; then
	echo "An error occurred (AccessDenied) when calling the ${op:-unknown} operation" >&2
	exit 254
fi
if [[ $ep == "$MOCK_SRC_ENDPOINT" ]]; then
	names=${MOCK_SRC_BUCKETS:-} versioned=${MOCK_SRC_VERSIONED:-} locked=${MOCK_SRC_LOCKED:-}
else
	names=${MOCK_DST_BUCKETS:-} versioned=${MOCK_DST_VERSIONED:-} locked=${MOCK_DST_LOCKED:-}
fi
case "$op" in
list-buckets)
	printf '%s\n' "$names"
	;;
get-bucket-versioning)
	if [[ -n ${MOCK_VERSIONING_ERROR:-} ]]; then
		echo "An error occurred (InternalError) when calling the GetBucketVersioning operation" >&2
		exit 254
	fi
	case " $versioned " in
	*" $bucket "*) echo Enabled ;;
	*) echo Off ;;
	esac
	;;
get-object-lock-configuration)
	case " $locked " in
	*" $bucket "*)
		echo Enabled
		;;
	*)
		echo "An error occurred (ObjectLockConfigurationNotFoundError) when calling the GetObjectLockConfiguration operation: object lock configuration not found" >&2
		exit 254
		;;
	esac
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
MOCK
chmod +x "$BIN/aws" "$BIN/rclone"
export PATH="$BIN:$PATH"

export MOCK_SRC_ENDPOINT=http://127.0.0.1:19000
export MOCK_DST_ENDPOINT=http://127.0.0.1:19001
base=(--source-endpoint "$MOCK_SRC_ENDPOINT" --dest-endpoint "$MOCK_DST_ENDPOINT"
	--source-ak AKIA_SRC --source-sk SK_SRC --dest-ak AKIA_DST --dest-sk SK_DST)

RC=0
pass=0
fail() {
	echo "FAIL: $*" >&2
	exit 1
}
ok() {
	pass=$((pass + 1))
}
run() {
	set +e
	bash "$SCRIPT" "$@" >"$OUT" 2>&1
	RC=$?
	set -e
}
assert_rc() { [[ $RC == "$1" ]] || fail "${2}（exit=$RC，期望 $1）$(cat "$OUT")"; }
assert_out() { grep -qF -- "$1" "$OUT" || fail "${2}（输出缺少: $1）$(cat "$OUT")"; }
assert_log() { grep -qF -- "$1" "$LOG" || fail "${2}（rclone 调用缺少: $1）$(cat "$LOG")"; }
assert_no_log() { ! grep -qF -- "$1" "$LOG" || fail "${2}（不该出现: $1）$(cat "$LOG")"; }

reset_mock() {
	export MOCK_SRC_BUCKETS="alpha beta"
	export MOCK_DST_BUCKETS=""
	export MOCK_SRC_VERSIONED=""
	export MOCK_SRC_LOCKED=""
	export MOCK_DST_VERSIONED=""
	export MOCK_DST_LOCKED=""
	unset MOCK_VERSIONING_ERROR MOCK_AWS_ERROR
	: >"$LOG"
}

echo "== help =="
reset_mock
run --help
assert_rc 0 "--help 应成功"
assert_out "不迁移" "--help 应写明不迁移项"
assert_out "对象锁" "--help 应点名对象锁"
assert_out "--delete" "--help 应说明 --delete"
ok

echo "== 用法错误 =="
reset_mock
run --source-endpoint "$MOCK_SRC_ENDPOINT"
assert_rc 2 "缺参数应 exit 2"
ok
reset_mock
run "${base[@]}" --transfers 0
assert_rc 2 "--transfers 0 应 exit 2"
ok

echo "== 源/目标同 endpoint 拒绝 =="
reset_mock
run --source-endpoint "$MOCK_SRC_ENDPOINT" --dest-endpoint "$MOCK_SRC_ENDPOINT" \
	--source-ak A --source-sk S --dest-ak A --dest-sk S
assert_rc 1 "同 endpoint 应拒绝"
assert_no_log "copy"
ok

echo "== 版本化源桶 fail-closed =="
reset_mock
export MOCK_SRC_VERSIONED="alpha"
run "${base[@]}"
assert_rc 1 "版本化源桶应拒绝"
assert_out "版本" "应说明版本状态"
assert_no_log "copy"
assert_no_log "mkdir"
ok

echo "== 第二个版本化源桶同样拒绝 =="
reset_mock
export MOCK_SRC_VERSIONED="beta"
run "${base[@]}"
assert_rc 1 "多桶时任一版本化桶应拒绝"
assert_no_log "copy"
ok

echo "== 对象锁源桶 fail-closed =="
reset_mock
export MOCK_SRC_LOCKED="alpha"
run "${base[@]}"
assert_rc 1 "对象锁源桶应拒绝"
assert_out "对象锁" "应说明对象锁"
assert_no_log "copy"
ok

echo "== 版本状态无法读取 fail-closed =="
reset_mock
export MOCK_VERSIONING_ERROR=1
run "${base[@]}"
assert_rc 1 "探测失败应拒绝"
assert_no_log "copy"
ok

echo "== 目标桶已版本化也拒绝 =="
reset_mock
export MOCK_DST_BUCKETS="alpha"
export MOCK_DST_VERSIONED="alpha"
run "${base[@]}"
assert_rc 1 "目标桶已版本化应拒绝"
assert_no_log "copy"
ok

echo "== 默认 copy（不删除）=="
reset_mock
run "${base[@]}"
assert_rc 0 "正常复制应成功"
assert_log "mkdir dst:alpha"
assert_log "copy src:alpha dst:alpha"
assert_log "copy src:beta dst:beta"
assert_log "--metadata"
assert_no_log "sync"
ok

echo "== --delete 切 sync =="
reset_mock
run "${base[@]}" --delete
assert_rc 0 "--delete 应成功"
assert_log "sync src:alpha dst:alpha"
assert_no_log "copy src:alpha"
ok

echo "== --dry-run 不建桶 =="
reset_mock
run "${base[@]}" --dry-run
assert_rc 0 "--dry-run 应成功"
assert_log "--dry-run"
assert_no_log "mkdir"
assert_out "[dry-run] 目标桶: alpha"
ok

echo "== --dry-run --delete 只计划不写 =="
reset_mock
run "${base[@]}" --dry-run --delete
assert_rc 0 "--dry-run --delete 应成功"
assert_log "sync src:alpha dst:alpha"
assert_log "--dry-run"
assert_no_log "mkdir"
ok

echo "== 并发/限速参数透传 =="
reset_mock
run "${base[@]}" --transfers 7 --checkers 3 --bwlimit 2M
assert_rc 0 "参数透传应成功"
assert_log "--transfers 7"
assert_log "--checkers 3"
assert_log "--bwlimit 2M"
ok

echo "== 空源端 =="
reset_mock
export MOCK_SRC_BUCKETS=""
run "${base[@]}"
assert_rc 0 "空源端应成功"
assert_out "没有桶"
[[ ! -s $LOG ]] || fail "空源端不应调用 rclone: $(cat "$LOG")"
ok

echo "== 列举源桶失败中止（不误判为空）=="
reset_mock
export MOCK_AWS_ERROR=1
run "${base[@]}"
assert_rc 1 "列举失败应中止"
assert_no_log "copy"
ok

echo "PASS: $pass 项自检全部通过"
