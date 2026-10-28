#!/usr/bin/env bash
# 单机 → 集群「重新灌入」工具（MVP，单向、离线）。
#
# 这不是拓扑转换，也不承诺无损迁移：用 rclone 把源端每个桶的**当前版本**对象重新
# 上传到集群端点。不迁移：历史版本/删除标记、桶 ACL/策略/标签/生命周期/加密/复制
# 配置、对象锁与 legal hold、未完成 MPU 会话、SSE-C 对象；对象 ACL/标签/SSE-KMS
# 仅按 rclone 能力尽力保留。源端只读，目标端只写，源端数据不会被修改或删除。
#
# 前提是停写源端（或接受复制窗口内的不一致）：工具不冻结源端，也没有全局快照。
# 版本化桶与开锁桶一律 fail-closed 拒绝（不提供 --force）。重跑幂等：默认 copy
# 覆盖同键对象且不删目标端多余对象；--delete 才切到 sync 镜像（删除目标桶内源端
# 不存在的对象）。用法与完整限制见 --help。
#
# 依赖：aws-cli（桶能力探测/列举）、rclone（复制）。

set -euo pipefail

usage() {
	cat <<'EOF'
用法: single-to-cluster.sh --source-endpoint URL --dest-endpoint URL \
         --source-ak AK --source-sk SK --dest-ak AK --dest-sk SK [选项]

把单机 endpoint 上所有桶的当前版本对象重新灌入集群 endpoint。单向复制：只读源端，
只写目标端。不是拓扑转换，也不是无损迁移。

必填（也可用括号内环境变量，命令行优先）：
  --source-endpoint URL   源单机 endpoint，必须带 scheme（环境变量 OSS_SRC_ENDPOINT）
  --dest-endpoint URL     目标集群 endpoint（任意节点等价；环境变量 OSS_DST_ENDPOINT）
  --source-ak AK          源端访问密钥（OSS_SRC_AK）
  --source-sk SK          源端密钥（OSS_SRC_SK）
  --dest-ak AK            目标端访问密钥（OSS_DST_AK）
  --dest-sk SK            目标端密钥（OSS_DST_SK）

选项：
  --dry-run               只打印计划，不创建桶也不写对象
  --delete                用 rclone sync 镜像每个源桶：删除目标桶内源端不存在的对象。
                          不加此开关时只复制/覆盖，绝不删除目标端对象。目标端多余的桶
                          不会被删除。
  --transfers N           并发传输数（默认 4）
  --checkers N            并发校验数（默认 8）
  --bwlimit RATE          rclone 限速，如 10M 或 10M:1M（默认不限）
  -h, --help              显示本帮助

明确不迁移（缺少的能力不会被伪装成已迁移）：
  * 历史版本、删除标记（版本化桶直接拒绝；见下）
  * 桶 ACL/策略/标签/生命周期/加密/复制配置
  * 对象锁、legal hold（开锁桶直接拒绝）
  * 未完成 multipart 会话（源端不中止、不清理）
  * SSE-C 加密对象（本工具没有客户密钥）
  * 对象 ACL、对象标签、SSE-KMS 等仅按 rclone 能力尽力保留，不保证

安全包络：
  * 先停写源端，或明确接受复制窗口内的不一致；重跑可补齐新增/更新的对象，
    但不传播删除，除非显式 --delete。
  * 复制前逐桶探测版本状态与对象锁；任一无法确认或状态不安全即退出，不写目标端。
  * 源/目标 endpoint 相同则拒绝。复制使用两个独立 remote，目标端只写。
  * 重跑幂等：同键对象覆盖写；--delete 下 sync 也幂等。

权限：源端 AK 需能 ListAllMyBuckets + 读桶/对象；目标端 AK 需能建桶 + 写对象。
依赖：aws-cli、rclone。退出码：0 成功，1 运行/安全检查失败，2 用法错误。
示例：
  OSS_SRC_SK=... OSS_DST_SK=... scripts/single-to-cluster.sh \
    --source-endpoint http://127.0.0.1:9000 \
    --dest-endpoint http://127.0.0.1:9001 \
    --source-ak AKIA_SRC --dest-ak AKIA_DST --dry-run
EOF
}

die() {
	printf '错误: %s\n' "$*" >&2
	exit 1
}

usage_err() {
	printf '错误: %s\n\n' "$*" >&2
	usage >&2
	exit 2
}

need_arg() {
	(($# >= 2)) || usage_err "选项 $1 缺少参数"
}

SRC_ENDPOINT="${OSS_SRC_ENDPOINT:-}"
DST_ENDPOINT="${OSS_DST_ENDPOINT:-}"
SRC_AK="${OSS_SRC_AK:-}"
SRC_SK="${OSS_SRC_SK:-}"
DST_AK="${OSS_DST_AK:-}"
DST_SK="${OSS_DST_SK:-}"
DRY_RUN=0
DELETE=0
TRANSFERS=4
CHECKERS=8
BWLIMIT=""
REGION=us-east-1

while (($#)); do
	case "$1" in
	--source-endpoint)
		need_arg "$@"
		SRC_ENDPOINT=$2
		shift 2
		;;
	--dest-endpoint)
		need_arg "$@"
		DST_ENDPOINT=$2
		shift 2
		;;
	--source-ak)
		need_arg "$@"
		SRC_AK=$2
		shift 2
		;;
	--source-sk)
		need_arg "$@"
		SRC_SK=$2
		shift 2
		;;
	--dest-ak)
		need_arg "$@"
		DST_AK=$2
		shift 2
		;;
	--dest-sk)
		need_arg "$@"
		DST_SK=$2
		shift 2
		;;
	--dry-run)
		DRY_RUN=1
		shift
		;;
	--delete)
		DELETE=1
		shift
		;;
	--transfers)
		need_arg "$@"
		TRANSFERS=$2
		shift 2
		;;
	--checkers)
		need_arg "$@"
		CHECKERS=$2
		shift 2
		;;
	--bwlimit)
		need_arg "$@"
		BWLIMIT=$2
		shift 2
		;;
	-h | --help)
		usage
		exit 0
		;;
	*)
		usage_err "未知选项: $1"
		;;
	esac
done

[[ -n $SRC_ENDPOINT ]] || usage_err "--source-endpoint 必填"
[[ -n $DST_ENDPOINT ]] || usage_err "--dest-endpoint 必填"
[[ -n $SRC_AK && -n $SRC_SK ]] || usage_err "源端 AK/SK 必填"
[[ -n $DST_AK && -n $DST_SK ]] || usage_err "目标端 AK/SK 必填"
[[ $SRC_ENDPOINT =~ ^https?://[^/]+/?$ ]] || usage_err "源 endpoint 必须是 http(s)://host[:port]（带 scheme，无路径）"
[[ $DST_ENDPOINT =~ ^https?://[^/]+/?$ ]] || usage_err "目标 endpoint 必须是 http(s)://host[:port]（带 scheme，无路径）"
[[ $TRANSFERS =~ ^[1-9][0-9]*$ ]] || usage_err "--transfers 必须是正整数"
[[ $CHECKERS =~ ^[1-9][0-9]*$ ]] || usage_err "--checkers 必须是正整数"

# 源与目标是同一服务时拒绝，避免被 --delete 镜像成自删。
norm_ep() { local s=${1,,}; printf '%s' "${s%/}"; }
[[ $(norm_ep "$SRC_ENDPOINT") != "$(norm_ep "$DST_ENDPOINT")" ]] ||
	die "源与目标 endpoint 相同，拒绝自复制"

for c in aws rclone; do
	command -v "$c" >/dev/null 2>&1 || die "缺少依赖: $c（需要 aws-cli 与 rclone）"
done

# aws-cli 只读调用；显式清掉可能干扰的外部凭据/分页环境。
aws_src() {
	env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
		AWS_ACCESS_KEY_ID="$SRC_AK" AWS_SECRET_ACCESS_KEY="$SRC_SK" \
		AWS_DEFAULT_REGION="$REGION" AWS_PAGER="" \
		aws --endpoint-url "${SRC_ENDPOINT%/}" "$@"
}
aws_dst() {
	env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
		AWS_ACCESS_KEY_ID="$DST_AK" AWS_SECRET_ACCESS_KEY="$DST_SK" \
		AWS_DEFAULT_REGION="$REGION" AWS_PAGER="" \
		aws --endpoint-url "${DST_ENDPOINT%/}" "$@"
}

WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
ERR="$WORK/aws.err"

list_buckets() { # $1=src|dst；输出一行一个桶名
	local which=$1 out
	if [[ $which == src ]]; then
		out=$(aws_src s3api list-buckets --query 'Buckets[].Name' --output text) ||
			die "列举源桶失败"
	else
		out=$(aws_dst s3api list-buckets --query 'Buckets[].Name' --output text) ||
			die "列举目标桶失败"
	fi
	printf '%s\n' "$out" | tr '\t ' '\n' | awk 'NF && $0 != "None"'
}

check_bucket() { # $1=src|dst $2=bucket；版本化/开锁即 fail-closed
	local which=$1 b=$2 status lock rc
	if [[ $which == src ]]; then
		status=$(aws_src s3api get-bucket-versioning --bucket "$b" \
			--query Status --output text 2>"$ERR") ||
			die "读取源桶 $b 版本状态失败（fail-closed）: $(cat "$ERR")"
	else
		status=$(aws_dst s3api get-bucket-versioning --bucket "$b" \
			--query Status --output text 2>"$ERR") ||
			die "读取目标桶 $b 版本状态失败（fail-closed）: $(cat "$ERR")"
	fi
	case "$status" in
	Off | None | "") ;;
	Enabled | Suspended)
		die "$which 桶 $b 版本状态为 $status：历史版本/删除标记不会被迁移，拒绝执行（fail-closed）"
		;;
	*)
		die "$which 桶 $b 版本状态无法识别（$status），拒绝执行（fail-closed）"
		;;
	esac

	if [[ $which == src ]]; then
		set +e
		lock=$(aws_src s3api get-object-lock-configuration --bucket "$b" \
			--query 'ObjectLockConfiguration.ObjectLockEnabled' --output text 2>"$ERR")
		rc=$?
		set -e
	else
		set +e
		lock=$(aws_dst s3api get-object-lock-configuration --bucket "$b" \
			--query 'ObjectLockConfiguration.ObjectLockEnabled' --output text 2>"$ERR")
		rc=$?
		set -e
	fi
	if ((rc == 0)); then
		if [[ $lock == Enabled ]]; then
			die "$which 桶 $b 已启用对象锁：锁定版本/保留策略不会被迁移，拒绝执行（fail-closed）"
		fi
		die "$which 桶 $b 对象锁状态无法识别（$lock），拒绝执行（fail-closed）"
	fi
	grep -q 'ObjectLockConfigurationNotFoundError' "$ERR" ||
		die "$which 桶 $b 对象锁状态无法确认（fail-closed）: $(cat "$ERR")"
}

# 先落变量再切分：list_buckets 失败必须中止，不能被进程替换吞掉。
SRC_LIST=$(list_buckets src)
if [[ -z $SRC_LIST ]]; then
	echo "源端没有桶，无需复制。"
	exit 0
fi
mapfile -t SRC_BUCKETS <<<"$SRC_LIST"

DST_LIST=$(list_buckets dst)
declare -A DST_EXISTS=()
while IFS= read -r b; do
	if [[ -n $b ]]; then
		DST_EXISTS[$b]=1
	fi
done <<<"$DST_LIST"

# 先完成所有桶的能力探测，再开始写；任一桶不安全就整体中止。
for b in "${SRC_BUCKETS[@]}"; do
	check_bucket src "$b"
	if [[ -n ${DST_EXISTS[$b]:-} ]]; then
		check_bucket dst "$b"
	fi
done

# rclone 用两个独立 remote；RCLONE_CONFIG 指向空文件，避免读用户配置。
: >"$WORK/rclone.conf"
export RCLONE_CONFIG="$WORK/rclone.conf"
export RCLONE_CONFIG_SRC_TYPE=s3
export RCLONE_CONFIG_SRC_PROVIDER=Other
export RCLONE_CONFIG_SRC_ACCESS_KEY_ID="$SRC_AK"
export RCLONE_CONFIG_SRC_SECRET_ACCESS_KEY="$SRC_SK"
export RCLONE_CONFIG_SRC_ENDPOINT="${SRC_ENDPOINT%/}"
export RCLONE_CONFIG_SRC_REGION="$REGION"
export RCLONE_CONFIG_SRC_FORCE_PATH_STYLE=true
export RCLONE_CONFIG_SRC_LIST_VERSION=2
export RCLONE_CONFIG_DST_TYPE=s3
export RCLONE_CONFIG_DST_PROVIDER=Other
export RCLONE_CONFIG_DST_ACCESS_KEY_ID="$DST_AK"
export RCLONE_CONFIG_DST_SECRET_ACCESS_KEY="$DST_SK"
export RCLONE_CONFIG_DST_ENDPOINT="${DST_ENDPOINT%/}"
export RCLONE_CONFIG_DST_REGION="$REGION"
export RCLONE_CONFIG_DST_FORCE_PATH_STYLE=true
export RCLONE_CONFIG_DST_LIST_VERSION=2

MODE=copy
if ((DELETE)); then
	MODE=sync
fi

for b in "${SRC_BUCKETS[@]}"; do
	if ((DRY_RUN)); then
		echo "[dry-run] 目标桶: $b（不存在则会在正式运行时创建）"
	else
		rclone mkdir "dst:$b" || die "创建目标桶失败: $b"
	fi
	cmd=(rclone "$MODE" "src:$b" "dst:$b"
		--transfers "$TRANSFERS" --checkers "$CHECKERS" --metadata)
	if ((DRY_RUN)); then
		cmd+=(--dry-run)
	fi
	if [[ -n $BWLIMIT ]]; then
		cmd+=(--bwlimit "$BWLIMIT")
	fi
	printf '+ %s\n' "${cmd[*]}"
	"${cmd[@]}" ||
		die "rclone $MODE 失败: $b（修复后可直接重跑；同键对象会覆盖）"
done

echo "完成：${#SRC_BUCKETS[@]} 个桶已重新灌入（$([[ $DRY_RUN == 1 ]] && echo dry-run || echo apply)）。"
