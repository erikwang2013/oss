#!/usr/bin/env bash
# scripts/mpu-drain.sh 自检：mock aws + 固定分页 fixture，无网络、无真实服务端。
# 覆盖 dry-run/apply、分页 marker、多桶、--all-buckets、列举/Abort API 失败、
# 截断页缺 marker 的 fail-closed，以及凭据/会话令牌只经环境变量（不进 argv）。

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MAIN="$HERE/../mpu-drain.sh"
[[ -x $MAIN ]] || {
	echo "找不到可执行脚本: $MAIN" >&2
	exit 1
}

TMP=$(mktemp -d "${TMPDIR:-/tmp}/mpu-drain-test.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT
BIN="$TMP/bin"
FIX="$TMP/mpu"
OUT="$TMP/out"
ERR="$TMP/err"
ARGV_LOG="$TMP/argv.log"
ABORT_LOG="$TMP/abort.log"
ENV_LOG="$TMP/env.log"
mkdir -p "$BIN" "$FIX"
: >"$ARGV_LOG"
: >"$ABORT_LOG"
: >"$ENV_LOG"

PASS=0
FAIL=0
ok() {
	PASS=$((PASS + 1))
	printf 'ok   %s\n' "$1"
}
bad() {
	FAIL=$((FAIL + 1))
	printf 'FAIL %s\n' "$1"
	shift
	for line in "$@"; do printf '     %s\n' "$line"; done
}
ck() { # desc command...
	local d=$1
	shift
	if "$@"; then ok "$d"; else bad "$d"; fi
}
assert_eq() { # desc expected actual
	if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "expected=$2 actual=$3"; fi
}
assert_has() { # desc file needle
	if grep -qF -- "$3" "$2"; then ok "$1"; else bad "$1" "missing: $3"; fi
}
assert_not_has() { # desc file needle
	if grep -qF -- "$3" "$2"; then bad "$1" "unexpected: $3"; else ok "$1"; fi
}

# ---------------------------------------------------------------- mock aws
cat >"$BIN/aws" <<'MOCK'
#!/usr/bin/env bash
set -uo pipefail
printf '%s\n' "$*" >>"$MOCK_AWS_ARGV_LOG"
printf 'ak=%s sk=%s token=%s\n' "${AWS_ACCESS_KEY_ID:+set}" \
	"${AWS_SECRET_ACCESS_KEY:+set}" "${AWS_SESSION_TOKEN:+set}" >>"$MOCK_ENV_LOG"

ep=""; op=""; bucket=""; marker=""; uid=""
while (($#)); do
	case "$1" in
	--endpoint-url) ep=$2; shift 2 ;;
	--bucket) bucket=$2; shift 2 ;;
	--upload-id) uid=$2; shift 2 ;;
	--key-marker) marker=$2; shift 2 ;;
	--output | --upload-id-marker) shift 2 ;;
	s3api) shift ;;
	list-multipart-uploads | abort-multipart-upload | list-buckets) op=$1; shift ;;
	*) shift ;;
	esac
done
[[ $ep == "$MOCK_ENDPOINT" ]] || { echo "mock aws: unexpected endpoint '$ep'" >&2; exit 90; }

case "$op" in
list-multipart-uploads)
	if [[ -n ${MOCK_LIST_FAIL_BUCKET:-} && $bucket == "$MOCK_LIST_FAIL_BUCKET" ]]; then
		echo "An error occurred (InternalError) when calling the ListMultipartUploads operation" >&2
		exit 254
	fi
	page=1
	[[ -n $marker ]] && page=2
	f="$MOCK_MPU_DIR/$bucket.$page.json"
	if [[ -f $f ]]; then cat "$f"; else echo '{"Uploads":[],"IsTruncated":false}'; fi
	;;
abort-multipart-upload)
	for bad in ${MOCK_ABORT_FAIL_IDS:-}; do
		if [[ $uid == "$bad" ]]; then
			echo "An error occurred (InternalError) when calling the AbortMultipartUpload operation" >&2
			exit 254
		fi
	done
	printf '%s\n' "$uid" >>"$MOCK_ABORT_LOG"
	;;
list-buckets)
	if [[ -n ${MOCK_LISTBUCKETS_FAIL:-} ]]; then
		echo "An error occurred (AccessDenied)" >&2
		exit 254
	fi
	printf '{"Buckets":['
	sep=""
	for b in ${MOCK_BUCKETS:-}; do
		printf '%s{"Name":"%s"}' "$sep" "$b"
		sep=","
	done
	printf ']}\n'
	;;
*)
	echo "mock aws: unexpected operation '$op'" >&2
	exit 99
	;;
esac
MOCK
chmod +x "$BIN/aws"
export PATH="$BIN:$PATH"
export MOCK_ENDPOINT="http://127.0.0.1:19000"
export MOCK_AWS_ARGV_LOG="$ARGV_LOG"
export MOCK_ABORT_LOG="$ABORT_LOG"
export MOCK_ENV_LOG="$ENV_LOG"
export MOCK_MPU_DIR="$FIX"
export OSS_SRC_AK="AKIA_SELFCHECK_SRC"
export OSS_SRC_SK="sk-selfcheck-secret-value"
unset OSS_SRC_SESSION_TOKEN MOCK_LIST_FAIL_BUCKET MOCK_ABORT_FAIL_IDS \
	MOCK_LISTBUCKETS_FAIL MOCK_BUCKETS

# ---------------------------------------------------------------- fixtures
# bkt1：两页（第 1 页截断并带 NextKeyMarker/NextUploadIdMarker）。
cat >"$FIX/bkt1.1.json" <<'JSON'
{"Uploads":[
 {"UploadId":"u1","Key":"a.txt","Initiated":"2026-10-31T10:00:00Z"},
 {"UploadId":"u2","Key":"b.bin","Initiated":"2026-10-31T10:01:00Z"}],
 "IsTruncated":true,"NextKeyMarker":"b.bin","NextUploadIdMarker":"u2"}
JSON
cat >"$FIX/bkt1.2.json" <<'JSON'
{"Uploads":[
 {"UploadId":"u3","Key":"c with space.txt","Initiated":"2026-10-31T10:02:00Z"},
 {"UploadId":"u4","Key":"d","Initiated":"2026-10-31T10:03:00Z"}],
 "IsTruncated":false}
JSON
cat >"$FIX/bkt2.1.json" <<'JSON'
{"Uploads":[{"UploadId":"u5","Key":"z","Initiated":"2026-10-31T10:04:00Z"}],
 "IsTruncated":false}
JSON
# bkt9：IsTruncated=true 但缺 NextKeyMarker → 必须 fail-closed，不能静默截断。
cat >"$FIX/bkt9.1.json" <<'JSON'
{"Uploads":[{"UploadId":"u9","Key":"k","Initiated":"2026-10-31T10:05:00Z"}],
 "IsTruncated":true}
JSON

run() {
	OUT="$TMP/out"
	ERR="$TMP/err"
	: >"$OUT"
	: >"$ERR"
	"$MAIN" "$@" >"$OUT" 2>"$ERR"
	RC=$?
}
count_lines() { wc -l <"$1" | tr -d ' '; }

# ---------------------------------------------------------------- 1. 用法/边界
env OSS_SRC_AK= OSS_SRC_SK= "$MAIN" --endpoint "$MOCK_ENDPOINT" --bucket bkt1 \
	>"$OUT" 2>"$ERR"
assert_eq "缺少凭据 → rc=2" 2 $?
"$MAIN" --endpoint "ftp://x" --bucket bkt1 >"$OUT" 2>"$ERR"
assert_eq "非 http(s) endpoint → rc=2" 2 $?
"$MAIN" --endpoint "$MOCK_ENDPOINT" >"$OUT" 2>"$ERR"
assert_eq "缺 --bucket/--all-buckets → rc=2" 2 $?
"$MAIN" --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --all-buckets >"$OUT" 2>"$ERR"
assert_eq "--bucket 与 --all-buckets 互斥 → rc=2" 2 $?
"$MAIN" --endpoint "$MOCK_ENDPOINT" --bucket Bad_Bucket >"$OUT" 2>"$ERR"
assert_eq "非法桶名 → rc=2" 2 $?

# ---------------------------------------------------------------- 2. dry-run
: >"$ARGV_LOG"
: >"$ABORT_LOG"
: >"$ENV_LOG"
run --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --prefix "pre/fix"
assert_eq "dry-run rc=0" 0 "$RC"
assert_eq "dry-run 列举 4 个上传（两页）" 1 "$(grep -c 'uploads=4 planned=4 aborted=0 failed=0' "$OUT" || true)"
assert_has "dry-run 输出 upload_id/key" "$OUT" "DRY-RUN bucket=bkt1 upload_id=u1 key=a.txt"
assert_has "第二页空格键原样输出" "$OUT" "upload_id=u3 key=c with space.txt"
assert_eq "dry-run 不调用 Abort" 0 "$(count_lines "$ABORT_LOG")"
assert_has "prefix 透传" "$ARGV_LOG" "--prefix pre/fix"
assert_has "第二页带 key-marker" "$ARGV_LOG" "--key-marker b.bin"
assert_has "第二页带 upload-id-marker" "$ARGV_LOG" "--upload-id-marker u2"
assert_has "凭据以环境变量传入" "$ENV_LOG" "ak=set sk=set token="

# ---------------------------------------------------------------- 3. apply
: >"$ARGV_LOG"
: >"$ABORT_LOG"
: >"$ENV_LOG"
run --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --apply
assert_eq "apply rc=0" 0 "$RC"
assert_eq "apply 输出 4 条 ABORTED" 4 "$(grep -c '^ABORTED ' "$OUT" || true)"
printf 'u1\nu2\nu3\nu4\n' | sort >"$TMP/expected.abort"
sort "$ABORT_LOG" >"$TMP/actual.abort"
ck "apply 恰好 Abort 4 个上传（各一次）" cmp -s "$TMP/expected.abort" "$TMP/actual.abort"

# ---------------------------------------------------------------- 4. 多桶 / all-buckets
run --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --bucket bkt2
assert_eq "多桶 dry-run rc=0" 0 "$RC"
assert_has "多桶汇总 5 个" "$OUT" "uploads=5 planned=5"
assert_has "第二桶被处理" "$OUT" "upload_id=u5 key=z"
MOCK_BUCKETS="bkt1 bkt2" run --endpoint "$MOCK_ENDPOINT" --all-buckets
assert_eq "--all-buckets rc=0" 0 "$RC"
assert_has "--all-buckets 汇总 5 个" "$OUT" "uploads=5 planned=5"

# ---------------------------------------------------------------- 5. 列举 API 失败
MOCK_LIST_FAIL_BUCKET=bkt2 run --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --bucket bkt2
assert_eq "单桶列举失败 → rc=1" 1 "$RC"
assert_has "失败桶之前的桶仍被列举" "$OUT" "upload_id=u1"
assert_has "stderr 报列举失败" "$ERR" "list-multipart-uploads 失败"
assert_eq "失败不触发 Abort" 4 "$(count_lines "$ABORT_LOG")" # 上一步 apply 的 4 条仍在

# ---------------------------------------------------------------- 6. Abort API 失败
: >"$ABORT_LOG"
MOCK_ABORT_FAIL_IDS="u3" run --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --apply
assert_eq "Abort 失败 → rc=1" 1 "$RC"
assert_has "失败条目输出 FAILED" "$OUT" "FAILED bucket=bkt1 upload_id=u3"
assert_has "汇总 failed=1" "$OUT" "failed=1"
printf 'u1\nu2\nu4\n' | sort >"$TMP/expected.abort"
sort "$ABORT_LOG" >"$TMP/actual.abort"
ck "单个 Abort 失败不中断其余" cmp -s "$TMP/expected.abort" "$TMP/actual.abort"

# ---------------------------------------------------------------- 7. 截断页缺 marker
run --endpoint "$MOCK_ENDPOINT" --bucket bkt9
assert_eq "截断页缺 NextKeyMarker → rc=1" 1 "$RC"
assert_has "缺 marker 明确报错" "$ERR" "缺少 NextKeyMarker"

# ---------------------------------------------------------------- 8. JSON 输出
run --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --json
assert_eq "JSON dry-run rc=0" 0 "$RC"
python3 - "$OUT" <<'PY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
assert d["tool"] == "mpu-drain", d
assert d["mode"] == "dry-run", d
assert d["ok"] is True, d
assert len(d["uploads"]) == 4, d
keys = [u["key"] for u in d["uploads"]]
assert "c with space.txt" in keys, keys
assert all(u["status"] == "planned" for u in d["uploads"]), d
PY
ck "JSON 结构/4 个上传/空格键正确" [ $? -eq 0 ]

MOCK_ABORT_FAIL_IDS="u3" run --endpoint "$MOCK_ENDPOINT" --bucket bkt1 --apply --json
assert_eq "JSON apply 失败 rc=1" 1 "$RC"
python3 - "$OUT" <<'PY'
import json
import sys

d = json.load(open(sys.argv[1], encoding="utf-8"))
assert d["mode"] == "apply", d
assert d["ok"] is False, d
failed = [u for u in d["uploads"] if u["status"] == "failed"]
assert [u["upload_id"] for u in failed] == ["u3"], d
assert "error" in failed[0], d
PY
ck "JSON apply 失败条目带 error" [ $? -eq 0 ]

# ---------------------------------------------------------------- 9. list-buckets 失败
MOCK_LISTBUCKETS_FAIL=1 run --endpoint "$MOCK_ENDPOINT" --all-buckets
assert_eq "--all-buckets 列举桶失败 → rc=1" 1 "$RC"
assert_has "stderr 报 list-buckets 失败" "$ERR" "list-buckets 失败"

# ---------------------------------------------------------------- 10. 会话令牌只经环境
: >"$ARGV_LOG"
: >"$ENV_LOG"
OSS_SRC_SESSION_TOKEN="session-token-value" run --endpoint "$MOCK_ENDPOINT" --bucket bkt1
assert_eq "带 session token rc=0" 0 "$RC"
assert_has "session token 进入环境变量" "$ENV_LOG" "token=set"
assert_not_has "AK 不出现在 argv" "$ARGV_LOG" "$OSS_SRC_AK"
assert_not_has "SK 不出现在 argv" "$ARGV_LOG" "$OSS_SRC_SK"
assert_not_has "session token 不出现在 argv" "$ARGV_LOG" "session-token-value"

# ---------------------------------------------------------------- 结果
echo "== 结果 =="
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -ne 0 ] && exit 1
echo "PASS: mpu-drain 自检（$PASS 项通过）"
exit 0
