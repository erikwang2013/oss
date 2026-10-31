#!/usr/bin/env bash
# 排空在途 MPU（升级/降级前必须 Complete/Abort）。只调用 ListMultipartUploads +
# AbortMultipartUpload：**绝不删除任何对象**。默认 dry-run；--apply 才真正 Abort。
#
# 凭据只从 OSS_SRC_AK / OSS_SRC_SK（可选 OSS_SRC_SESSION_TOKEN）读取，不进入 argv。
# 依赖：aws-cli、python3。用法与退出码见 --help。
set -uo pipefail

usage() {
	cat <<'EOF'
用法: mpu-drain.sh --endpoint URL (--bucket NAME ...|--all-buckets) [选项]

列出并（--apply 时）中止在途 Multipart Upload。绝不删除对象/已完成分片。
凭据只从环境变量读取（不进入 argv/日志）：
  OSS_SRC_AK / OSS_SRC_SK   访问密钥（必填）
  OSS_SRC_SESSION_TOKEN     可选 STS 会话令牌（设置后作为 AWS_SESSION_TOKEN）

必填：
  --endpoint URL            服务端点，http(s)://host[:port]
范围（二选一）：
  --bucket NAME             要处理的桶，可重复
  --all-buckets             列举所有桶
选项：
  --prefix P                只处理该键前缀（透传 ListMultipartUploads）
  --apply                   真正调用 AbortMultipartUpload；默认 dry-run 只列举
  --json                    stdout 输出单个 JSON 文档
  -h, --help                显示本帮助

输出：upload_id/key/initiated（--apply 时为 aborted/failed）。
退出码：0 = 成功（无 API 失败）；1 = 列举/Abort API 失败；2 = 用法错误。
示例：
  export OSS_SRC_AK=... OSS_SRC_SK=...
  scripts/mpu-drain.sh --endpoint http://127.0.0.1:9000 --all-buckets
  scripts/mpu-drain.sh --endpoint https://oss.example --bucket data --apply --json
EOF
}

usage_err() { printf '错误: %s\n\n' "$*" >&2; usage >&2; exit 2; }
die() { printf '错误: %s\n' "$*" >&2; exit 1; }
need_arg() { (($# >= 2)) || usage_err "选项 $1 缺少参数"; }
valid_bucket_name() { [[ $1 =~ ^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$ ]]; }

ENDPOINT=""
BUCKETS=()
ALL=0
PREFIX=""
APPLY=0
JSON=0
AK="${OSS_SRC_AK:-}"
SK="${OSS_SRC_SK:-}"
TOKEN="${OSS_SRC_SESSION_TOKEN:-}"

while (($#)); do
	case "$1" in
	--endpoint) need_arg "$@"; ENDPOINT=$2; shift 2 ;;
	--bucket)
		need_arg "$@"
		valid_bucket_name "$2" || usage_err "桶名非法: $2"
		BUCKETS+=("$2")
		shift 2
		;;
	--all-buckets) ALL=1; shift ;;
	--prefix) need_arg "$@"; PREFIX=$2; shift 2 ;;
	--apply) APPLY=1; shift ;;
	--json) JSON=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) usage_err "未知选项: ${1%%=*}" ;; # 不回显可能携带密钥的 =值
	esac
done

[[ -n $ENDPOINT ]] || usage_err "--endpoint 必填"
[[ $ENDPOINT =~ ^https?://[^/]+/?$ ]] || usage_err "--endpoint 必须是 http(s)://host[:port]（带 scheme，无路径）"
[[ $ENDPOINT != *@* && ! $ENDPOINT =~ [[:space:]] ]] || usage_err "--endpoint 不能包含 userinfo/空白"
[[ -n $AK && -n $SK ]] || usage_err "环境变量 OSS_SRC_AK/OSS_SRC_SK 必填（凭据不经 argv）"
if ((ALL && ${#BUCKETS[@]} > 0)); then usage_err "--all-buckets 与 --bucket 不能同时使用"; fi
if ((!ALL && ${#BUCKETS[@]} == 0)); then usage_err "必须提供 --bucket 或 --all-buckets"; fi
for c in aws python3 base64; do
	command -v "$c" >/dev/null 2>&1 || die "缺少依赖: $c"
done

TMP=$(mktemp -d "${TMPDIR:-/tmp}/oss-mpu-drain.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT
RESULTS="$TMP/results.tsv"
ERRORS="$TMP/errors.tsv"
ERR="$TMP/aws.err"
: >"$RESULTS"
: >"$ERRORS"
FAIL=0

AWS_ENV=(env -u AWS_PROFILE -u AWS_DEFAULT_PROFILE -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN
	AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
	AWS_PAGER="" AWS_EC2_METADATA_DISABLED=true)
if [[ -n $TOKEN ]]; then
	AWS_ENV+=(AWS_SESSION_TOKEN="$TOKEN" AWS_SECURITY_TOKEN="$TOKEN")
fi
aws_s3api() { "${AWS_ENV[@]}" aws --endpoint-url "${ENDPOINT%/}" s3api "$@"; }

b64e() { printf '%s' "$1" | base64 -w0; }
b64d() { printf '%s' "$1" | base64 -d; }
err_tail() { head -c 300 "$ERR" 2>/dev/null | tr '\n' ' ' || true; }
record_error() { printf 'E\t%s\t%s\n' "$(b64e "$1")" "$(b64e "$2")" >>"$ERRORS"; FAIL=1; }
record_upload() { # bucket id key initiated status error
	printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
		"$(b64e "$1")" "$(b64e "$2")" "$(b64e "$3")" "$4" "$5" "$(b64e "$6")" >>"$RESULTS"
}

# 每页 → 一条 P（truncated/next markers，base64）与若干 U（id/key/initiated），
# 全部 base64 编码：键名可含任意 UTF-8 而不破坏行协议。
PAGE_PY=$(cat <<'PY'
import base64
import json
import sys

d = json.load(sys.stdin)


def b(s):
    return base64.b64encode((s or "").encode()).decode()


print("P", "1" if d.get("IsTruncated") else "0",
      b(d.get("NextKeyMarker")), b(d.get("NextUploadIdMarker")), sep="\t")
for u in d.get("Uploads") or []:
    print("U", b(u.get("UploadId")), b(u.get("Key")),
          u.get("Initiated") or "", sep="\t")
PY
)

# 列举一个桶的全部页，收集到 DRAIN_IDS/DRAIN_KEYS/DRAIN_INITS；失败返回 1。
collect_bucket() {
	local bucket=$1 km="" um="" page parsed tag a b c truncated nk ni pages=0
	DRAIN_IDS=() DRAIN_KEYS=() DRAIN_INITS=()
	while :; do
		pages=$((pages + 1))
		((pages <= 10000)) || {
			record_error "$bucket" "分页超过 10000 页，疑似 marker 未推进"
			return 1
		}
		local -a args=(list-multipart-uploads --bucket "$bucket" --no-paginate --output json)
		[[ -n $PREFIX ]] && args+=(--prefix "$PREFIX")
		[[ -n $km ]] && args+=(--key-marker "$km")
		[[ -n $um ]] && args+=(--upload-id-marker "$um")
		if ! page=$(aws_s3api "${args[@]}" 2>"$ERR"); then
			record_error "$bucket" "list-multipart-uploads 失败: $(err_tail)"
			return 1
		fi
		if ! parsed=$(printf '%s' "$page" | python3 -c "$PAGE_PY" 2>"$ERR"); then
			record_error "$bucket" "list-multipart-uploads 响应无法解析: $(err_tail)"
			return 1
		fi
		truncated=0 nk="" ni=""
		while IFS=$'\t' read -r tag a b c; do
			case "$tag" in
			P) truncated=$a; nk=$b; ni=$c ;;
			U)
				DRAIN_IDS+=("$(b64d "$a")")
				DRAIN_KEYS+=("$(b64d "$b")")
				DRAIN_INITS+=("$c")
				;;
			esac
		done <<<"$parsed"
		[[ $truncated == 1 ]] || return 0
		[[ -n $nk ]] || {
			record_error "$bucket" "IsTruncated=true 但缺少 NextKeyMarker（拒绝静默截断）"
			return 1
		}
		local prev_km=$km prev_um=$um
		km=$(b64d "$nk")
		um=""
		[[ -n $ni ]] && um=$(b64d "$ni")
		if [[ $km == "$prev_km" && $um == "$prev_um" ]]; then
			record_error "$bucket" "分页 marker 未推进（key=$km），拒绝死循环"
			return 1
		fi
	done
}

drain_bucket() {
	local bucket=$1 i rc=0 status err
	collect_bucket "$bucket" || rc=1
	for i in "${!DRAIN_IDS[@]}"; do
		status=planned err=""
		if ((APPLY)); then
			if aws_s3api abort-multipart-upload --bucket "$bucket" \
				--key "${DRAIN_KEYS[i]}" --upload-id "${DRAIN_IDS[i]}" 2>"$ERR"; then
				status=aborted
			else
				status=failed
				err="abort-multipart-upload 失败: $(err_tail)"
				FAIL=1
			fi
		fi
		record_upload "$bucket" "${DRAIN_IDS[i]}" "${DRAIN_KEYS[i]}" \
			"${DRAIN_INITS[i]}" "$status" "$err"
	done
	return "$rc"
}

if ((ALL)); then
	if ! out=$(aws_s3api list-buckets --output json 2>"$ERR"); then
		record_error "-" "list-buckets 失败: $(err_tail)"
	else
		mapfile -t BUCKETS < <(printf '%s' "$out" | python3 -c '
import json
import sys
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(1)
for b in d.get("Buckets") or []:
    name = b.get("Name")
    if name:
        print(name)
')
		((${#BUCKETS[@]} > 0)) || record_error "-" "list-buckets 未返回任何桶"
	fi
fi

for b in "${BUCKETS[@]}"; do
	valid_bucket_name "$b" || {
		record_error "$b" "服务端返回非法桶名"
		continue
	}
	drain_bucket "$b" || true
done

python3 - "$RESULTS" "$ERRORS" "$APPLY" "$JSON" "$ENDPOINT" "$PREFIX" <<'PY'
import base64
import json
import sys

results, errors_path, apply_s, json_s, endpoint, prefix = sys.argv[1:7]
apply = bool(int(apply_s))
as_json = bool(int(json_s))


def d(s):
    return base64.b64decode(s).decode(errors="replace")


uploads = []
errors = []
with open(results, encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) != 6:
            continue
        bucket, upload_id, key, initiated, status, err = parts
        u = {
            "bucket": d(bucket),
            "upload_id": d(upload_id),
            "key": d(key),
            "initiated": initiated,
            "status": status,
        }
        if err:
            u["error"] = d(err)
        uploads.append(u)
with open(errors_path, encoding="utf-8") as f:
    for line in f:
        parts = line.rstrip("\n").split("\t")
        if len(parts) == 3 and parts[0] == "E":
            errors.append({"bucket": d(parts[1]), "error": d(parts[2])})

mode = "apply" if apply else "dry-run"
failed = sum(1 for u in uploads if u["status"] == "failed")
ok = not errors and failed == 0

if as_json:
    print(json.dumps({
        "tool": "mpu-drain",
        "mode": mode,
        "endpoint": endpoint,
        "prefix": prefix or None,
        "ok": ok,
        "uploads": uploads,
        "errors": errors,
    }, ensure_ascii=False, indent=2))
else:
    for u in uploads:
        tag = {"planned": "DRY-RUN", "aborted": "ABORTED", "failed": "FAILED"}[u["status"]]
        line = "%s bucket=%s upload_id=%s key=%s initiated=%s" % (
            tag, u["bucket"], u["upload_id"], u["key"], u["initiated"])
        if u.get("error"):
            line += " error=%s" % u["error"]
        print(line)
    for e in errors:
        print("ERROR bucket=%s error=%s" % (e["bucket"], e["error"]), file=sys.stderr)
    counts = {}
    for u in uploads:
        counts[u["status"]] = counts.get(u["status"], 0) + 1
    print("mpu-drain: mode=%s uploads=%d planned=%d aborted=%d failed=%d errors=%d" % (
        mode, len(uploads), counts.get("planned", 0), counts.get("aborted", 0),
        counts.get("failed", 0), len(errors)))

sys.exit(0 if ok else 1)
PY
rc=$?
((FAIL)) && rc=1
exit "$rc"
