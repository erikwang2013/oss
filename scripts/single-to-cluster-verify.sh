#!/usr/bin/env bash
# single-to-cluster.sh --verify 的只读实现（也可直接调用）。
#
# 逐桶比较源/目标当前版本对象：对象数、总字节、键集合、单对象大小与 ETag。
# 只调用 aws s3api list-objects-v2，不建桶、不写对象、不写状态文件。
# ETag 判定：两侧都是单段 ETag（无 '-'）时直接比较，不同即漂移；任一侧为分片
# ETag 时大小一致也不判内容（rclone 分片形态可能不同），计入 etag_unverified。
# shortcut: 用 list-objects-v2 --output text 的 TSV 解析；键名含 tab/换行时不支持，
#           需要时改用 rclone lsjson 或逐键 HEAD。
# 退出码：0 无漂移，1 漂移/列举失败，2 用法错误。
# 凭据只从 OSS_SRC_AK/OSS_SRC_SK/OSS_DST_AK/OSS_DST_SK 读取，不进入 argv。
set -euo pipefail

usage() {
	cat <<'EOF'
用法: single-to-cluster-verify.sh --source-endpoint URL --dest-endpoint URL \
        --bucket NAME [--bucket NAME ...] [--dest-bucket NAME ...] [--json]

只读比较源/目标对象数、字节与 ETag。源桶由 --bucket 逐项给出（主脚本已按
include/exclude 选好）；--dest-bucket 表示目标端该桶存在，缺省视为目标桶不存在。
凭据环境变量：OSS_SRC_AK/OSS_SRC_SK/OSS_DST_AK/OSS_DST_SK。
  --json   机器可读输出（每类差异最多列 100 个键）
退出码：0 无漂移；1 漂移或列举失败；2 用法错误。
EOF
}
die() { printf '错误: %s\n' "$*" >&2; exit 1; }
usage_err() { printf '错误: %s\n\n' "$*" >&2; usage >&2; exit 2; }
need_arg() { (($# >= 2)) || usage_err "选项 $1 缺少参数"; }
valid_bucket_name() { [[ $1 =~ ^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$ ]]; }

SRC_ENDPOINT="${OSS_SRC_ENDPOINT:-}"
DST_ENDPOINT="${OSS_DST_ENDPOINT:-}"
SRC_AK="${OSS_SRC_AK:-}"
SRC_SK="${OSS_SRC_SK:-}"
DST_AK="${OSS_DST_AK:-}"
DST_SK="${OSS_DST_SK:-}"
REGION=us-east-1
BUCKETS=()
DEST_BUCKETS=()
JSON=0

while (($#)); do
	case "$1" in
	--source-endpoint) need_arg "$@"; SRC_ENDPOINT=$2; shift 2 ;;
	--dest-endpoint) need_arg "$@"; DST_ENDPOINT=$2; shift 2 ;;
	--bucket) need_arg "$@"; BUCKETS+=("$2"); shift 2 ;;
	--dest-bucket) need_arg "$@"; DEST_BUCKETS+=("$2"); shift 2 ;;
	--json) JSON=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) usage_err "未知选项: ${1%%=*}" ;;
	esac
done

[[ -n $SRC_ENDPOINT && -n $DST_ENDPOINT ]] || usage_err "--source-endpoint/--dest-endpoint 必填"
[[ -n $SRC_AK && -n $SRC_SK ]] || usage_err "环境变量 OSS_SRC_AK/OSS_SRC_SK 必填（凭据不经 argv）"
[[ -n $DST_AK && -n $DST_SK ]] || usage_err "环境变量 OSS_DST_AK/OSS_DST_SK 必填（凭据不经 argv）"
for ep in "$SRC_ENDPOINT" "$DST_ENDPOINT"; do
	[[ $ep =~ ^https?://[^/]+/?$ ]] || usage_err "endpoint 必须是 http(s)://host[:port]（带 scheme，无路径）"
	[[ $ep != *@* && ! $ep =~ [[:space:]] ]] || usage_err "endpoint 不能包含 userinfo/空白"
done
for b in "${BUCKETS[@]}" "${DEST_BUCKETS[@]}"; do
	valid_bucket_name "$b" || usage_err "桶名非法: $b"
done
command -v aws >/dev/null 2>&1 || die "缺少依赖: aws（需要 aws-cli）"

aws_src() {
	env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
		AWS_ACCESS_KEY_ID="$SRC_AK" AWS_SECRET_ACCESS_KEY="$SRC_SK" \
		AWS_DEFAULT_REGION="$REGION" AWS_PAGER="" aws --endpoint-url "${SRC_ENDPOINT%/}" "$@"
}
aws_dst() {
	env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
		AWS_ACCESS_KEY_ID="$DST_AK" AWS_SECRET_ACCESS_KEY="$DST_SK" \
		AWS_DEFAULT_REGION="$REGION" AWS_PAGER="" aws --endpoint-url "${DST_ENDPOINT%/}" "$@"
}

declare -A DST_EXISTS=()
for b in "${DEST_BUCKETS[@]}"; do DST_EXISTS[$b]=1; done

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
ERR="$WORK/aws.err"
err_tail() { head -c 300 "$ERR" 2>/dev/null | tr '\n' ' ' || true; }

json_escape() {
	local s=$1
	s=${s//\\/\\\\}
	s=${s//\"/\\\"}
	s=${s//$'\n'/\\n}
	s=${s//$'\r'/\\r}
	s=${s//$'\t'/\\t}
	printf '%s' "$s"
}
json_array() {
	local first=1 x
	printf '['
	for x in "$@"; do
		((first)) || printf ','
		first=0
		printf '"%s"' "$(json_escape "$x")"
	done
	printf ']'
}
json_file_array() { # file [limit]
	local f=$1 limit=${2:-100} n=0 l first=1
	printf '['
	if [[ -s $f ]]; then
		while IFS= read -r l && ((n < limit)); do
			((first)) || printf ','
			first=0
			printf '"%s"' "$(json_escape "$l")"
			n=$((n + 1))
		done <"$f"
	fi
	printf ']'
}

list_objects_tsv() { # $1=src|dst $2=bucket $3=outfile
	local f=$3
	if [[ $1 == src ]]; then
		aws_src s3api list-objects-v2 --bucket "$2" \
			--query 'Contents[].[Key,Size,ETag]' --output text >"$f" 2>"$ERR" || return 1
	else
		aws_dst s3api list-objects-v2 --bucket "$2" \
			--query 'Contents[].[Key,Size,ETag]' --output text >"$f" 2>"$ERR" || return 1
	fi
	awk -F'\t' 'NF >= 3 && $1 != "None"' "$f" |
		LC_ALL=C sort -t $'\t' -k1,1 >"$f.sorted" || return 1
	mv "$f.sorted" "$f"
}

report_keys() { # $1=label $2=count $3=file
	local label=$1 n=$2 f=$3 l i=0
	((n)) || return 0
	printf '  %s（%d）:\n' "$label" "$n"
	while IFS= read -r l && ((i < 20)); do
		printf '    - %s\n' "$l"
		i=$((i + 1))
	done <"$f"
	if ((n > 20)); then printf '    ...（其余 %d 个见 --json）\n' "$((n - 20))"; fi
}

local_dir="$WORK/verify"
mkdir -p "$local_dir"
drift=0 errs=0
declare -a V_NAME=() V_SO=() V_SB=() V_DO=() V_DB=() V_MISS=() V_EXTRA=() V_SIZE=() V_ETAG=()
declare -a V_UNVER=() V_DRIFT=() V_ERR=()
for b in "${BUCKETS[@]}"; do
	i=${#V_NAME[@]}
	f="$local_dir/$i"
	: >"$f.missing" >"$f.extra" >"$f.size" >"$f.etag" >"$f.unver" >"$f.counts"
	V_NAME[$i]=$b V_ERR[$i]="" V_DRIFT[$i]=0
	V_SO[$i]=0 V_SB[$i]=0 V_DO[$i]=0 V_DB[$i]=0 V_MISS[$i]=0 V_EXTRA[$i]=0 V_SIZE[$i]=0 V_ETAG[$i]=0 V_UNVER[$i]=0
	if ! list_objects_tsv src "$b" "$f.src"; then
		V_ERR[$i]="源端列举失败: $(err_tail)"
		V_DRIFT[$i]=1 drift=1 errs=1
		continue
	fi
	if [[ -n ${DST_EXISTS[$b]:-} ]]; then
		if ! list_objects_tsv dst "$b" "$f.dst"; then
			V_ERR[$i]="目标端列举失败: $(err_tail)"
			V_DRIFT[$i]=1 drift=1 errs=1
			continue
		fi
	else
		: >"$f.dst"
		V_ERR[$i]="目标桶不存在"
		V_DRIFT[$i]=1 drift=1
	fi
	awk -F'\t' -v dir="$local_dir" -v idx="$i" -v srcf="$f.src" '
		function clean(v) { gsub(/"/, "", v); return v }
		FILENAME == srcf { if (NF >= 2) { s[$1] = $2; e[$1] = $3; sc++; sb += $2 } next }
		{ if (NF >= 2) { d[$1] = $2; de[$1] = $3; dc++; db += $2 } }
		END {
			for (k in s) {
				if (!(k in d)) { print k > (dir "/" idx ".missing"); nm++ }
				else if (s[k] != d[k]) { print k > (dir "/" idx ".size"); nz++ }
				else if (clean(e[k]) != clean(de[k])) {
					if (index(e[k], "-") || index(de[k], "-")) print k > (dir "/" idx ".unver")
					else { print k > (dir "/" idx ".etag"); ne++ }
				}
			}
			for (k in d) if (!(k in s)) { print k > (dir "/" idx ".extra"); nx++ }
			print sc+0, sb+0, dc+0, db+0, nm+0, nz+0, ne+0, nx+0 > (dir "/" idx ".counts")
		}
	' "$f.src" "$f.dst"
	read -r sc sb dc db miss size etag extra <"$f.counts"
	V_SO[$i]=$sc V_SB[$i]=$sb V_DO[$i]=$dc V_DB[$i]=$db
	V_MISS[$i]=$miss V_SIZE[$i]=$size V_ETAG[$i]=$etag V_EXTRA[$i]=$extra
	V_UNVER[$i]=$(wc -l <"$f.unver" | tr -d ' ')
	if ((V_MISS[i] + V_EXTRA[i] + V_SIZE[i] + V_ETAG[i])); then
		V_DRIFT[$i]=1 drift=1
	fi
done

status=ok
((drift || errs)) && status=drift
if ((JSON)); then
	printf '{"tool":"single-to-cluster","mode":"verify","status":"%s","exit_code":%d,\n' \
		"$status" "$((drift || errs))"
	printf ' "source_endpoint":"%s","dest_endpoint":"%s",\n' \
		"$(json_escape "$SRC_ENDPOINT")" "$(json_escape "$DST_ENDPOINT")"
	printf ' "buckets":['
	total_so=0 total_sb=0 total_do=0 total_db=0
	for ((j = 0; j < ${#V_NAME[@]}; j++)); do
		((j)) && printf ','
		total_so=$((total_so + V_SO[j])) total_sb=$((total_sb + V_SB[j]))
		total_do=$((total_do + V_DO[j])) total_db=$((total_db + V_DB[j]))
		printf '{"name":"%s","source_objects":%d,"source_bytes":%d,' \
			"$(json_escape "${V_NAME[j]}")" "${V_SO[j]}" "${V_SB[j]}"
		printf '"dest_objects":%d,"dest_bytes":%d,"drift":%s,"error":"%s",' \
			"${V_DO[j]}" "${V_DB[j]}" \
			"$([[ ${V_DRIFT[j]} == 1 ]] && echo true || echo false)" "$(json_escape "${V_ERR[j]}")"
		printf '"missing_in_dest":%s,"extra_in_dest":%s,"size_mismatch":%s,' \
			"$(json_file_array "$local_dir/$j.missing")" "$(json_file_array "$local_dir/$j.extra")" \
			"$(json_file_array "$local_dir/$j.size")"
		printf '"etag_mismatch":%s,"etag_unverified":%s}' \
			"$(json_file_array "$local_dir/$j.etag")" "$(json_file_array "$local_dir/$j.unver")"
	done
	printf '],\n "totals":{"buckets":%d,"source_objects":%d,"source_bytes":%d,' \
		"${#V_NAME[@]}" "$total_so" "$total_sb"
	printf '"dest_objects":%d,"dest_bytes":%d},"drift":%s,"warnings":[]}\n' \
		"$total_do" "$total_db" "$([[ $drift == 1 ]] && echo true || echo false)"
else
	printf '校验（只读）：源 %s → 目标 %s\n' "$SRC_ENDPOINT" "$DST_ENDPOINT"
	for ((j = 0; j < ${#V_NAME[@]}; j++)); do
		printf '桶 %s: 源 %d 对象/%d 字节；目标 %d/%d' \
			"${V_NAME[j]}" "${V_SO[j]}" "${V_SB[j]}" "${V_DO[j]}" "${V_DB[j]}"
		[[ -n ${V_ERR[j]} ]] && printf '；%s' "${V_ERR[j]}"
		printf ' → %s\n' "$([[ ${V_DRIFT[j]} == 1 ]] && echo DRIFT || echo OK)"
		report_keys "缺失于目标" "${V_MISS[j]}" "$local_dir/$j.missing"
		report_keys "目标多余" "${V_EXTRA[j]}" "$local_dir/$j.extra"
		report_keys "大小不一致" "${V_SIZE[j]}" "$local_dir/$j.size"
		report_keys "ETag 不一致（单段）" "${V_ETAG[j]}" "$local_dir/$j.etag"
		report_keys "ETag 未判定（分片，大小一致）" "${V_UNVER[j]}" "$local_dir/$j.unver"
	done
	printf '结果: %s\n' "$([[ $status == ok ]] && echo OK || echo DRIFT)"
fi
exit "$((drift || errs))"
