#!/usr/bin/env bash
# 单机 → 集群「重新灌入」工具（单向、离线）。
#
# 不是拓扑转换，也不承诺无损迁移：用 rclone 把源端选中桶的**当前版本**对象重新上传
# 到集群端点。不迁移：历史版本/删除标记、桶 ACL/策略/标签/生命周期/加密/复制配置、
# 对象锁与 legal hold、未完成 MPU 会话、SSE-C 对象；对象 ACL/标签/SSE-KMS 仅按
# rclone 能力尽力保留。源端只读，目标端只写，源端不会被修改或删除。
#
# 前提是停写源端（或接受复制窗口内的不一致）：工具不冻结源端，也没有全局快照。
# 版本化/开锁桶默认 fail-closed，只有显式 --allow-versioned / --allow-object-lock
# 才继续（仍只迁移当前版本）。默认 copy 覆盖同键对象且不删目标端多余对象；--delete
# 才切 sync。--plan/--dry-run 只读盘点；--verify 交给只读 helper 校验数量/字节/ETag；
# --resume FILE 按桶记录进度，重跑跳过已完成桶（源端必须已停写）。
#
# 凭据只从环境变量读取，不进入 argv/日志/检查点。依赖：aws-cli（探测/盘点）、
# rclone（仅复制）。用法与完整限制见 --help。

set -euo pipefail

usage() {
	cat <<'EOF'
用法: single-to-cluster.sh --source-endpoint URL --dest-endpoint URL [选项]

把单机 endpoint 上选中桶的当前版本对象重新灌入集群 endpoint。单向：只读源端，
只写目标端。不是拓扑转换，也不是无损迁移。

端点（也可用括号内环境变量）：
  --source-endpoint URL   源单机 endpoint，必须带 scheme（OSS_SRC_ENDPOINT）
  --dest-endpoint URL     目标集群 endpoint（OSS_DST_ENDPOINT）
凭据只从环境变量读取（不进入 argv/日志/检查点）：
  OSS_SRC_AK / OSS_SRC_SK   源端访问密钥
  OSS_DST_AK / OSS_DST_SK   目标端访问密钥

模式（默认复制；--plan/--verify 只读且互斥）：
  --plan, --dry-run       只读计划：逐桶对象数/字节、目标是否存在、版本/对象锁/
                          加密约束；不创建桶、不写对象、不写检查点
  --verify                只读校验：比较源/目标对象数、字节与 ETag；漂移 exit 1
                          （按 list-objects-v2 TSV 比较；键名含 tab/换行不支持）
  --resume FILE           复制模式检查点：成功后按桶追加记录，重跑跳过已完成桶。
                          FILE 由首次运行创建；源端必须已停写。端点/模式不一致拒绝

范围与安全：
  --include-bucket NAME   只处理指定桶，可重复（也可 OSS_INCLUDE_BUCKETS=a,b）
  --exclude-bucket NAME   排除指定桶，可重复（也可 OSS_EXCLUDE_BUCKETS=a,b）；deny 优先
  --allow-versioned       显式接受版本化桶：只迁当前版本，历史版本/删除标记丢失
  --allow-object-lock     显式接受对象锁桶；通常还需同时 --allow-versioned
  --delete                用 rclone sync 镜像每个源桶：删除目标桶内源端不存在的对象。
                          不加时只复制/覆盖，绝不删除目标端对象；目标独有桶不删
  --transfers N           并发传输数（默认 4）；--checkers N 并发校验数（默认 8）
  --bwlimit RATE          rclone 限速，如 10M 或 10M:1M（默认不限）
  --json                  仅 --plan/--verify：stdout 输出 JSON（不混入文本）
  -h, --help              显示本帮助

明确不迁移：历史版本/删除标记（版本化桶默认拒绝）、桶 ACL/策略/标签/生命周期/
加密/复制配置、对象锁/legal hold（开锁桶默认拒绝）、未完成 MPU、SSE-C 对象
（无法从桶配置探测，--plan 显示 unknown）；对象 ACL/标签/SSE-KMS 仅尽力保留。

安全包络：先停写源端，或明确接受复制窗口内的不一致；默认不传播删除。复制前逐桶
探测版本/对象锁，任一无法确认或未显式接受即退出，不写目标端。源=目标 endpoint
拒绝。复制使用两个独立 rclone remote，目标端只写。重跑幂等；--verify 中任一侧为
分片 ETag 时不判内容漂移（计入 etag_unverified）。

权限：源 AK 需 ListAllMyBuckets + 读桶/对象；目标 AK 需建桶 + 写对象（--verify
需两端 ListBucket；--plan 还需读版本/锁/加密配置）。
依赖：aws-cli；复制另需 rclone。退出码：0 成功/无漂移，1 运行/安全/漂移失败，2 用法错误。
示例：
  export OSS_SRC_AK=... OSS_SRC_SK=... OSS_DST_AK=... OSS_DST_SK=...
  scripts/single-to-cluster.sh --source-endpoint http://127.0.0.1:9000 \
    --dest-endpoint http://127.0.0.1:9001 --plan --json
EOF
}

die() { printf '错误: %s\n' "$*" >&2; exit 1; }
usage_err() { printf '错误: %s\n\n' "$*" >&2; usage >&2; exit 2; }
need_arg() { (($# >= 2)) || usage_err "选项 $1 缺少参数"; }
valid_bucket_name() { [[ $1 =~ ^[a-z0-9][a-z0-9._-]{1,61}[a-z0-9]$ ]]; }

add_bucket_list() { # include|exclude csv
	local kind=$1 csv=$2 item IFS=','
	for item in $csv; do
		[[ -n $item ]] || continue
		valid_bucket_name "$item" || usage_err "$kind 桶名非法: $item"
		if [[ $kind == include ]]; then INCLUDE+=("$item"); else EXCLUDE+=("$item"); fi
	done
}

SRC_ENDPOINT="${OSS_SRC_ENDPOINT:-}"
DST_ENDPOINT="${OSS_DST_ENDPOINT:-}"
SRC_AK="${OSS_SRC_AK:-}"
SRC_SK="${OSS_SRC_SK:-}"
DST_AK="${OSS_DST_AK:-}"
DST_SK="${OSS_DST_SK:-}"
PLAN=0 VERIFY=0 JSON=0 DELETE=0 RESUME=0 ALLOW_VERSIONED=0 ALLOW_OBJECT_LOCK=0
RESUME_FILE=""
INCLUDE=() EXCLUDE=()
TRANSFERS=4 CHECKERS=8 BWLIMIT=""
REGION=us-east-1

add_bucket_list include "${OSS_INCLUDE_BUCKETS:-}"
add_bucket_list exclude "${OSS_EXCLUDE_BUCKETS:-}"

while (($#)); do
	case "$1" in
	--source-endpoint) need_arg "$@"; SRC_ENDPOINT=$2; shift 2 ;;
	--dest-endpoint) need_arg "$@"; DST_ENDPOINT=$2; shift 2 ;;
	--plan | --dry-run) PLAN=1; shift ;;
	--verify) VERIFY=1; shift ;;
	--json) JSON=1; shift ;;
	--delete) DELETE=1; shift ;;
	--allow-versioned) ALLOW_VERSIONED=1; shift ;;
	--allow-object-lock) ALLOW_OBJECT_LOCK=1; shift ;;
	--include-bucket) need_arg "$@"; add_bucket_list include "$2"; shift 2 ;;
	--exclude-bucket) need_arg "$@"; add_bucket_list exclude "$2"; shift 2 ;;
	--resume) need_arg "$@"; RESUME=1; RESUME_FILE=$2; shift 2 ;;
	--transfers) need_arg "$@"; TRANSFERS=$2; shift 2 ;;
	--checkers) need_arg "$@"; CHECKERS=$2; shift 2 ;;
	--bwlimit) need_arg "$@"; BWLIMIT=$2; shift 2 ;;
	-h | --help) usage; exit 0 ;;
	*) usage_err "未知选项: ${1%%=*}" ;; # 不回显可能携带密钥的 =值
	esac
done

if ((PLAN && VERIFY)); then usage_err "--plan/--dry-run 与 --verify 不能同时使用"; fi
if ((JSON && !PLAN && !VERIFY)); then usage_err "--json 仅用于 --plan/--verify"; fi
if ((RESUME && (PLAN || VERIFY))); then usage_err "--resume 仅用于复制模式"; fi
if ((VERIFY && DELETE)); then usage_err "--verify 与 --delete 不能同时使用"; fi
[[ -n $SRC_ENDPOINT ]] || usage_err "--source-endpoint 必填"
[[ -n $DST_ENDPOINT ]] || usage_err "--dest-endpoint 必填"
[[ -n $SRC_AK && -n $SRC_SK ]] || usage_err "环境变量 OSS_SRC_AK/OSS_SRC_SK 必填（凭据不经 argv）"
[[ -n $DST_AK && -n $DST_SK ]] || usage_err "环境变量 OSS_DST_AK/OSS_DST_SK 必填（凭据不经 argv）"
for ep in "$SRC_ENDPOINT" "$DST_ENDPOINT"; do
	[[ $ep =~ ^https?://[^/]+/?$ ]] || usage_err "endpoint 必须是 http(s)://host[:port]（带 scheme，无路径）"
	[[ $ep != *@* && ! $ep =~ [[:space:]] ]] || usage_err "endpoint 不能包含 userinfo/空白"
done
[[ $TRANSFERS =~ ^[1-9][0-9]*$ ]] || usage_err "--transfers 必须是正整数"
[[ $CHECKERS =~ ^[1-9][0-9]*$ ]] || usage_err "--checkers 必须是正整数"

norm_ep() { local s=${1,,}; printf '%s' "${s%/}"; }
[[ $(norm_ep "$SRC_ENDPOINT") != "$(norm_ep "$DST_ENDPOINT")" ]] || die "源与目标 endpoint 相同，拒绝自复制"

command -v aws >/dev/null 2>&1 || die "缺少依赖: aws（需要 aws-cli）"
if ((!PLAN && !VERIFY)); then
	command -v rclone >/dev/null 2>&1 || die "缺少依赖: rclone（复制需要 rclone）"
fi

MODE=copy
if ((DELETE)); then MODE=sync; fi
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
ERR="$WORK/aws.err"
WARNINGS=()
warn() { WARNINGS+=("$*"); }
err_tail() { head -c 300 "$ERR" 2>/dev/null | tr '\n' ' ' || true; }

# aws-cli 只读调用；清掉可能干扰的外部凭据/分页环境。
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
aws_on() { if [[ $1 == src ]]; then shift; aws_src "$@"; else shift; aws_dst "$@"; fi; }

list_buckets() { # $1=src|dst；输出一行一个桶名
	local which=$1 out
	if ! out=$(aws_on "$which" s3api list-buckets --query 'Buckets[].Name' --output text 2>"$ERR"); then
		die "列举 $which 桶失败: $(err_tail)"
	fi
	printf '%s\n' "$out" | tr '\t ' '\n' | awk 'NF && $0 != "None"'
}

SRC_LIST=$(list_buckets src)
SRC_BUCKETS=()
if [[ -n $SRC_LIST ]]; then mapfile -t SRC_BUCKETS <<<"$SRC_LIST"; fi
for b in "${SRC_BUCKETS[@]}"; do valid_bucket_name "$b" || die "服务端返回非法桶名: $b"; done
DST_LIST=$(list_buckets dst)
declare -A DST_EXISTS=()
while IFS= read -r b; do
	[[ -n $b ]] || continue
	valid_bucket_name "$b" || die "服务端返回非法桶名: $b"
	DST_EXISTS[$b]=1
done <<<"$DST_LIST"

selected() { # $1=bucket；include 为白名单，deny 永远优先
	local b=$1 x found=0
	if ((${#INCLUDE[@]})); then
		for x in "${INCLUDE[@]}"; do if [[ $x == "$b" ]]; then found=1; break; fi; done
		((found)) || return 1
	fi
	for x in "${EXCLUDE[@]}"; do if [[ $x == "$b" ]]; then return 1; fi; done
	return 0
}

SEL_BUCKETS=()
MISSING_INCLUDE=()
for b in "${SRC_BUCKETS[@]}"; do if selected "$b"; then SEL_BUCKETS+=("$b"); fi; done
for x in "${INCLUDE[@]}"; do
	found=0
	for b in "${SRC_BUCKETS[@]}"; do if [[ $x == "$b" ]]; then found=1; break; fi; done
	((found)) || MISSING_INCLUDE+=("$x")
done

# ---------------------------------------------------------------- 逐桶能力探测
PB_SAFE=1 PB_REASONS=() PB_VERSION=unknown PB_LOCK=unknown PB_ENC=unknown
probe_extra() { # $1=src|dst $2=bucket；版本/锁 fail-closed，加密仅提示
	local which=$1 b=$2 out alg key
	PB_VERSION=unknown PB_LOCK=unknown PB_ENC=unknown
	if out=$(aws_on "$which" s3api get-bucket-versioning --bucket "$b" \
		--query Status --output text 2>"$ERR"); then
		case "$out" in
		Off | None | "") PB_VERSION=${out:-None} ;;
		Enabled | Suspended)
			PB_VERSION=$out
			if ((ALLOW_VERSIONED)); then
				PB_REASONS+=("$which 桶 $b 已版本化（$out，--allow-versioned）：仅迁移当前版本")
			else
				PB_SAFE=0
				PB_REASONS+=("$which 桶 $b 版本状态为 $out：历史版本/删除标记不会被迁移，拒绝执行（fail-closed）")
			fi
			;;
		*) PB_SAFE=0; PB_REASONS+=("$which 桶 $b 版本状态无法识别（$out），拒绝执行（fail-closed）") ;;
		esac
	else
		PB_SAFE=0
		PB_REASONS+=("$which 桶 $b 版本状态无法确认（fail-closed）: $(err_tail)")
	fi
	if out=$(aws_on "$which" s3api get-object-lock-configuration --bucket "$b" \
		--query 'ObjectLockConfiguration.ObjectLockEnabled' --output text 2>"$ERR"); then
		if [[ $out == Enabled ]]; then
			PB_LOCK=Enabled
			if ((ALLOW_OBJECT_LOCK)); then
				PB_REASONS+=("$which 桶 $b 已启用对象锁（--allow-object-lock）：锁定版本/保留策略不会被迁移")
			else
				PB_SAFE=0
				PB_REASONS+=("$which 桶 $b 已启用对象锁：锁定版本/保留策略不会被迁移，拒绝执行（fail-closed）")
			fi
		else
			PB_SAFE=0
			PB_REASONS+=("$which 桶 $b 对象锁状态无法识别（$out），拒绝执行（fail-closed）")
		fi
	elif grep -q 'ObjectLockConfigurationNotFoundError' "$ERR"; then
		PB_LOCK=none
	else
		PB_SAFE=0
		PB_REASONS+=("$which 桶 $b 对象锁状态无法确认（fail-closed）: $(err_tail)")
	fi
	if out=$(aws_on "$which" s3api get-bucket-encryption --bucket "$b" \
		--query 'ServerSideEncryptionConfiguration.Rules[0].ApplyServerSideEncryptionByDefault.[SSEAlgorithm,KMSMasterKeyID]' \
		--output text 2>"$ERR"); then
		alg=${out%%$'\t'*}
		key=""
		[[ $out == *$'\t'* ]] && key=${out#*$'\t'}
		if [[ -n $alg && $alg != None ]]; then
			PB_ENC=$alg
			if [[ -n $key && $key != None ]]; then PB_ENC+=":$key"; fi
		else
			PB_ENC=none
		fi
	elif grep -q 'ServerSideEncryptionConfigurationNotFoundError' "$ERR"; then
		PB_ENC=none
	else
		PB_REASONS+=("$which 桶 $b 加密配置无法读取（按 unknown 继续）: $(err_tail)")
	fi
}
take_warnings() { local r; for r in "${PB_REASONS[@]}"; do warn "$r"; done; }

# ---------------------------------------------------------------- 检查点
declare -A RESUME_DONE=()
init_resume() {
	local dir=. key val srcseed="" dstseed="" mode=""
	[[ $RESUME_FILE == */* ]] && dir=${RESUME_FILE%/*}
	[[ -z $dir ]] && dir=/
	[[ -d $dir ]] || die "检查点目录不存在: $dir"
	if [[ -e $RESUME_FILE || -L $RESUME_FILE ]]; then
		[[ -L $RESUME_FILE ]] && die "检查点不能是符号链接: $RESUME_FILE"
		[[ -f $RESUME_FILE && -r $RESUME_FILE ]] || die "检查点不可读: $RESUME_FILE"
		while IFS=$'\t' read -r key val; do
			case "$key" in
			single-to-cluster-resume-v1 | "") ;;
			src) srcseed=$val ;;
			dst) dstseed=$val ;;
			mode) mode=$val ;;
			done) valid_bucket_name "$val" || die "检查点桶名非法: $val"; RESUME_DONE[$val]=1 ;;
			*) die "检查点格式无效（$key）；请换用新的 --resume 文件" ;;
			esac
		done <"$RESUME_FILE"
		[[ $srcseed == "$(norm_ep "$SRC_ENDPOINT")" ]] || die "检查点源 endpoint 与本次不一致"
		[[ $dstseed == "$(norm_ep "$DST_ENDPOINT")" ]] || die "检查点目标 endpoint 与本次不一致"
		[[ $mode == "$MODE" ]] || die "检查点模式为 '$mode'，与本次 '$MODE' 不一致；请换用新的 --resume 文件"
	else
		(umask 077 && printf 'single-to-cluster-resume-v1\nsrc\t%s\ndst\t%s\nmode\t%s\n' \
			"$(norm_ep "$SRC_ENDPOINT")" "$(norm_ep "$DST_ENDPOINT")" "$MODE" >"$RESUME_FILE") ||
			die "无法创建检查点: $RESUME_FILE"
	fi
}

# ---------------------------------------------------------------- JSON 工具
json_escape() {
	local s=$1
	s=${s//\\/\\\\}
	s=${s//\"/\\\"}
	s=${s//$'\n'/\\n}
	s=${s//$'\r'/\\r}
	s=${s//$'\t'/\\t}
	printf '%s' "$s"
}
json_array() { # 字符串参数 → JSON 数组
	local first=1 x
	printf '['
	for x in "$@"; do
		((first)) || printf ','
		first=0
		printf '"%s"' "$(json_escape "$x")"
	done
	printf ']'
}

# ---------------------------------------------------------------- 复制（默认）
copy_mode() {
	local b cmd extra=""
	if ((${#SEL_BUCKETS[@]} == 0)); then
		echo "没有选中任何桶，无需复制。"
		exit 0
	fi
	# 先完成所有桶的能力探测，再开始写；任一桶不安全就整体中止。
	for b in "${SEL_BUCKETS[@]}"; do
		PB_SAFE=1 PB_REASONS=()
		probe_extra src "$b"
		((PB_SAFE)) || die "${PB_REASONS[*]}"
		take_warnings
		if [[ -n ${DST_EXISTS[$b]:-} ]]; then
			PB_SAFE=1 PB_REASONS=()
			probe_extra dst "$b"
			((PB_SAFE)) || die "${PB_REASONS[*]}"
			take_warnings
		fi
	done
	for w in "${WARNINGS[@]}"; do printf '警告: %s\n' "$w" >&2; done
	if ((RESUME)); then init_resume; fi

	# rclone 用两个独立 remote；RCLONE_CONFIG 指向空文件，避免读用户配置。
	: >"$WORK/rclone.conf"
	export RCLONE_CONFIG="$WORK/rclone.conf"
	export RCLONE_CONFIG_SRC_TYPE=s3 RCLONE_CONFIG_SRC_PROVIDER=Other
	export RCLONE_CONFIG_SRC_ACCESS_KEY_ID="$SRC_AK" RCLONE_CONFIG_SRC_SECRET_ACCESS_KEY="$SRC_SK"
	export RCLONE_CONFIG_SRC_ENDPOINT="${SRC_ENDPOINT%/}" RCLONE_CONFIG_SRC_REGION="$REGION"
	export RCLONE_CONFIG_SRC_FORCE_PATH_STYLE=true RCLONE_CONFIG_SRC_LIST_VERSION=2
	export RCLONE_CONFIG_DST_TYPE=s3 RCLONE_CONFIG_DST_PROVIDER=Other
	export RCLONE_CONFIG_DST_ACCESS_KEY_ID="$DST_AK" RCLONE_CONFIG_DST_SECRET_ACCESS_KEY="$DST_SK"
	export RCLONE_CONFIG_DST_ENDPOINT="${DST_ENDPOINT%/}" RCLONE_CONFIG_DST_REGION="$REGION"
	export RCLONE_CONFIG_DST_FORCE_PATH_STYLE=true RCLONE_CONFIG_DST_LIST_VERSION=2

	for b in "${SEL_BUCKETS[@]}"; do
		if ((RESUME)) && [[ -n ${RESUME_DONE[$b]:-} ]]; then
			echo "跳过检查点中已完成的桶: $b"
			continue
		fi
		rclone mkdir "dst:$b" || die "创建目标桶失败: $b"
		cmd=(rclone "$MODE" "src:$b" "dst:$b" --transfers "$TRANSFERS" --checkers "$CHECKERS" --metadata)
		[[ -n $BWLIMIT ]] && cmd+=(--bwlimit "$BWLIMIT")
		printf '+ %s\n' "${cmd[*]}"
		"${cmd[@]}" || die "rclone $MODE 失败: $b（修复后可直接重跑；同键对象会覆盖）"
		if ((RESUME)); then
			printf 'done\t%s\n' "$b" >>"$RESUME_FILE" || die "写入检查点失败: $b"
		fi
	done
	if ((RESUME)); then extra="，检查点 $RESUME_FILE"; fi
	echo "完成：${#SEL_BUCKETS[@]} 个桶已重新灌入（$MODE$extra）。"
}

# ---------------------------------------------------------------- --plan
inventory_counts() { # $1=src|dst $2=bucket；设置 INV_COUNT/INV_BYTES
	local out
	INV_COUNT=0 INV_BYTES=0
	if ! out=$(aws_on "$1" s3api list-objects-v2 --bucket "$2" \
		--query 'Contents[].[Size]' --output text 2>"$ERR"); then return 1; fi
	read -r INV_COUNT INV_BYTES <<<"$(printf '%s\n' "$out" |
		awk '$1 ~ /^[0-9]+$/ {n++; s+=$1} END {printf "%d %d", n+0, s+0}')"
}

plan_mode() {
	local b i total_obj=0 total_bytes=0 blocked=0 src_safe
	local -a P_NAME=() P_OBJ=() P_BYTES=() P_VER=() P_LOCK=() P_ENC=()
	local -a P_DEX=() P_DVER=() P_DLOCK=() P_DENC=() P_SAFE=() P_REASON=() BLOCKED=()
	for x in "${MISSING_INCLUDE[@]}"; do
		((JSON)) || printf '[BLOCK] --include-bucket 在源端不存在: %s\n' "$x" >&2
		BLOCKED+=("include 桶在源端不存在: $x")
		blocked=1
	done
	for b in "${SEL_BUCKETS[@]}"; do
		i=${#P_NAME[@]}
		local reason=""
		PB_SAFE=1 PB_REASONS=()
		probe_extra src "$b"
		src_safe=$PB_SAFE
		reason="${PB_REASONS[*]}"
		P_VER[$i]=$PB_VERSION P_LOCK[$i]=$PB_LOCK P_ENC[$i]=$PB_ENC
		take_warnings
		P_DEX[$i]=no P_DVER[$i]=- P_DLOCK[$i]=- P_DENC[$i]=-
		if [[ -n ${DST_EXISTS[$b]:-} ]]; then
			P_DEX[$i]=yes
			PB_SAFE=1 PB_REASONS=()
			probe_extra dst "$b"
			P_DVER[$i]=$PB_VERSION P_DLOCK[$i]=$PB_LOCK P_DENC[$i]=$PB_ENC
			take_warnings
			((PB_SAFE)) || src_safe=0
			reason+="${reason:+; }${PB_REASONS[*]}"
		fi
		if ! inventory_counts src "$b"; then
			src_safe=0
			reason+="${reason:+; }源桶 $b 对象盘点失败（fail-closed）: $(err_tail)"
		fi
		P_NAME[$i]=$b P_OBJ[$i]=$INV_COUNT P_BYTES[$i]=$INV_BYTES
		P_SAFE[$i]=$src_safe P_REASON[$i]=$reason
		((src_safe)) || { blocked=1; BLOCKED+=("$b: $reason"); }
		total_obj=$((total_obj + INV_COUNT))
		total_bytes=$((total_bytes + INV_BYTES))
	done
	status=ok
	((blocked)) && status=blocked
	if ((JSON)); then
		printf '{"tool":"single-to-cluster","mode":"plan","status":"%s","exit_code":%d,\n' "$status" "$blocked"
		printf ' "source_endpoint":"%s","dest_endpoint":"%s",\n' \
			"$(json_escape "$SRC_ENDPOINT")" "$(json_escape "$DST_ENDPOINT")"
		printf ' "options":{"delete":%s,"mode":"%s","allow_versioned":%s,"allow_object_lock":%s},\n' \
			"$([[ $DELETE == 1 ]] && echo true || echo false)" "$MODE" \
			"$([[ $ALLOW_VERSIONED == 1 ]] && echo true || echo false)" \
			"$([[ $ALLOW_OBJECT_LOCK == 1 ]] && echo true || echo false)"
		printf ' "include_buckets":%s,"exclude_buckets":%s,"missing_include":%s,\n' \
			"$(json_array "${INCLUDE[@]}")" "$(json_array "${EXCLUDE[@]}")" \
			"$(json_array "${MISSING_INCLUDE[@]}")"
		printf ' "buckets":['
		for ((i = 0; i < ${#P_NAME[@]}; i++)); do
			((i)) && printf ','
			printf '{"name":"%s","selected":true,"objects":%d,"bytes":%d,"versioning":"%s",' \
				"$(json_escape "${P_NAME[$i]}")" "${P_OBJ[$i]}" "${P_BYTES[$i]}" "${P_VER[$i]}"
			printf '"object_lock":"%s","encryption":"%s","safe":%s,"reasons":"%s",' \
				"${P_LOCK[$i]}" "$(json_escape "${P_ENC[$i]}")" \
				"$([[ ${P_SAFE[$i]} == 1 ]] && echo true || echo false)" "$(json_escape "${P_REASON[$i]}")"
			printf '"dest":{"exists":%s,"versioning":"%s","object_lock":"%s","encryption":"%s"}}' \
				"$([[ ${P_DEX[$i]} == yes ]] && echo true || echo false)" \
				"${P_DVER[$i]}" "${P_DLOCK[$i]}" "$(json_escape "${P_DENC[$i]}")"
		done
		printf '],\n "totals":{"buckets":%d,"objects":%d,"bytes":%d},\n' "${#P_NAME[@]}" "$total_obj" "$total_bytes"
		printf ' "blocked":%s,"warnings":%s}\n' "$(json_array "${BLOCKED[@]}")" "$(json_array "${WARNINGS[@]}")"
	else
		printf '计划（只读，不创建桶/不写对象）：\n  源: %s\n  目标: %s\n  模式: %s；允许版本化: %s；允许对象锁: %s\n' \
			"$SRC_ENDPOINT" "$DST_ENDPOINT" "$MODE" \
			"$([[ $ALLOW_VERSIONED == 1 ]] && echo 是 || echo 否)" \
			"$([[ $ALLOW_OBJECT_LOCK == 1 ]] && echo 是 || echo 否)"
		for ((i = 0; i < ${#P_NAME[@]}; i++)); do
			printf '[plan] 目标桶: %s（不存在则在复制时创建）\n' "${P_NAME[$i]}"
			printf '  对象 %d，字节 %d；源版本=%s 锁=%s 加密=%s；目标%s（版本=%s 锁=%s 加密=%s）\n' \
				"${P_OBJ[$i]}" "${P_BYTES[$i]}" "${P_VER[$i]}" "${P_LOCK[$i]}" "${P_ENC[$i]}" \
				"$([[ ${P_DEX[$i]} == yes ]] && echo 已存在 || echo 不存在)" \
				"${P_DVER[$i]}" "${P_DLOCK[$i]}" "${P_DENC[$i]}"
			if [[ -n ${P_REASON[$i]} ]]; then printf '  说明: %s\n' "${P_REASON[$i]}"; fi
		done
		if ((${#EXCLUDE[@]})); then printf '  排除桶: %s\n' "${EXCLUDE[*]}"; fi
		for w in "${WARNINGS[@]}"; do printf '  警告: %s\n' "$w"; done
		printf '合计: %d 个桶，%d 个对象，%d 字节（约 %s）\n' "${#P_NAME[@]}" "$total_obj" "$total_bytes" \
			"$(awk -v b="$total_bytes" 'BEGIN { printf "%.2f GiB", b / 1073741824 }')"
		printf '结果: %s\n' "$([[ $status == ok ]] && echo OK || echo BLOCKED)"
	fi
	exit "$blocked"
}

if ((VERIFY)); then
	((${#MISSING_INCLUDE[@]})) && die "--include-bucket 在源端不存在: ${MISSING_INCLUDE[*]}"
	[[ -x $HERE/single-to-cluster-verify.sh ]] || die "缺少校验 helper: $HERE/single-to-cluster-verify.sh"
	vargs=(--source-endpoint "$SRC_ENDPOINT" --dest-endpoint "$DST_ENDPOINT")
	((JSON)) && vargs+=(--json)
	for b in "${SEL_BUCKETS[@]}"; do
		vargs+=(--bucket "$b")
		[[ -n ${DST_EXISTS[$b]:-} ]] && vargs+=(--dest-bucket "$b")
	done
	exec "$HERE/single-to-cluster-verify.sh" "${vargs[@]}"
elif ((PLAN)); then
	plan_mode
else
	copy_mode
fi
