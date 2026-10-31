#!/usr/bin/env bash
# 蓝绿（stop-the-world）升级就绪闸门（只读）：不停止服务、不复制数据、不改任何文件。
# 1) 委派 upgrade-preflight.sh --strict --require-no-mpu：其 BLOCK 一律 BLOCK；仅在
#    “无 [BLOCK] 行”时把 strict 下的 WARN 映射为本脚本 WARN（本脚本 --strict 决定退出码）。
#    这样 M6/M7 未提供输入导致的 WARN 不会让闸门永远无法通过；health-json 缺失同理为
#    WARN，需要 fail-closed 的发布闸门请加 --strict。
# 2) 冷备份必须已存在且非空（文件或目录）；--health-url 返回任何 HTTP 状态（含 4xx/5xx，
#    例如未 ready 的 /health/ready 503）= 旧服务未停 → BLOCK；连接失败/000 才算已停止。
# 3) --new-binary --version 必须可解析为 >= --min-version（默认 1.16.0）的语义化版本。
# 4) 无 BLOCK 时打印 stop-the-world 运行手册（含回滚边界）。
# 退出码：0 = READY（非 --strict 时允许 WARN）；1 = BLOCK 或 strict+WARN；2 = 用法错误。
set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
PREFLIGHT="$HERE/upgrade-preflight.sh"

usage() {
	cat <<'EOF'
用法: blue-green-preflight.sh --data-dir <path> [--data-dir <path>] --backup <path> [选项]
只读的蓝绿/stop-the-world 升级就绪闸门；不停止服务、不复制数据、不修改文件。
必填：
  --data-dir <path>            数据盘根目录；可多次或逗号分隔（如 a,b）；原样传给 preflight
  --backup <path>              已完成的冷备份；必须存在且非空（普通文件或非空目录）
选项：
  --config <oss.toml>          传给 upgrade-preflight（M6 site_token 检查）
  --health-json <path>         传给 upgrade-preflight（v1.16 数据面下限；严格模式下必需）
  --replica-reconcile <cmd>    传给 upgrade-preflight（M7 只读 list 命令前缀）
  --replica-scope <b>/<k>      传给 upgrade-preflight；可多次（M7 精确桶/键）
  --health-url <url>           只读 GET：任何 HTTP 响应（含 4xx/5xx）说明旧服务未停 → BLOCK
  --new-binary <path>          运行 <path> --version，要求语义化版本 >= --min-version
  --min-version <X.Y.Z>        最低新版本，默认 1.16.0
  --strict                     WARN 也非零退出（发布闸门）
  --json                       以 JSON 输出（不改变退出码语义）
  -h, --help                   显示本帮助
状态：BLOCK = 不安全/无法判定；WARN = 未提供可选项、无法完全判定；READY = 可执行切换。
退出码：0 = READY（非 strict 时 WARN 也 0）；1 = BLOCK 或 strict+WARN；2 = 用法错误。
EOF
}
usage_err() { printf '错误: %s\n\n' "$*" >&2; usage >&2; exit 2; }
need_arg() { (($# >= 2)) || usage_err "选项 $1 缺少参数"; }

DATA_DIRS=()
BACKUP=""
CONFIG=""
HEALTH_JSON=""
HEALTH_URL=""
NEW_BINARY=""
MIN_VERSION="1.16.0"
REPL_CMD_STR=""
REPL_SCOPES=()
STRICT=0
JSON=0

add_data_dirs() {
	local item IFS=','
	for item in $1; do [ -n "$item" ] && DATA_DIRS+=("$item"); done
}

while (($#)); do
	case "$1" in
	--data-dir)
		need_arg "$@"
		add_data_dirs "$2"
		shift 2
		;;
	--backup)
		need_arg "$@"
		BACKUP=$2
		shift 2
		;;
	--config)
		need_arg "$@"
		CONFIG=$2
		shift 2
		;;
	--health-json)
		need_arg "$@"
		HEALTH_JSON=$2
		shift 2
		;;
	--health-url)
		need_arg "$@"
		HEALTH_URL=$2
		shift 2
		;;
	--new-binary)
		need_arg "$@"
		NEW_BINARY=$2
		shift 2
		;;
	--replica-reconcile)
		need_arg "$@"
		REPL_CMD_STR=$2
		shift 2
		;;
	--replica-scope)
		need_arg "$@"
		REPL_SCOPES+=("$2")
		shift 2
		;;
	--min-version)
		need_arg "$@"
		MIN_VERSION=$2
		shift 2
		;;
	--json) JSON=1; shift ;;
	--strict) STRICT=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) usage_err "未知参数: $1" ;;
	esac
done
(( ${#DATA_DIRS[@]} > 0 )) || usage_err "至少提供一个 --data-dir"
[ -n "$BACKUP" ] || usage_err "必须提供 --backup（已完成的冷备份）"
[[ $MIN_VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || usage_err "--min-version 需为 X.Y.Z: $MIN_VERSION"
if [ -n "$HEALTH_URL" ] && [[ $HEALTH_URL != http://* && $HEALTH_URL != https://* ]]; then
	usage_err "--health-url 只支持 http(s)://: $HEALTH_URL"
fi
command -v python3 >/dev/null 2>&1 ||
	{ printf '[BLOCK] python3: 未找到 python3，无法比较版本/输出 JSON\n' >&2; exit 1; }

TMP=$(mktemp -d "${TMPDIR:-/tmp}/oss-blue-green.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT
RECORDS="$TMP/records"
DETAILS="$TMP/details"
mkdir -p "$DETAILS"
: >"$RECORDS"
add_check() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" >>"$RECORDS"; }
add_detail() { printf '%s\n' "$2" >>"$DETAILS/$1"; }

# ---------------------------------------------------------------- 委派 preflight
check_upgrade_preflight() {
	local name=upgrade_preflight
	local -a args=()
	local d line out rc
	for d in "${DATA_DIRS[@]}"; do args+=(--data-dir "$d"); done
	args+=(--strict --require-no-mpu)
	[ -n "$CONFIG" ] && args+=(--config "$CONFIG")
	[ -n "$HEALTH_JSON" ] && args+=(--health-json "$HEALTH_JSON")
	[ -n "$REPL_CMD_STR" ] && args+=(--replica-reconcile "$REPL_CMD_STR")
	for d in "${REPL_SCOPES[@]}"; do args+=(--replica-scope "$d"); done
	out=$(bash "$PREFLIGHT" "${args[@]}" 2>&1)
	rc=$?
	while IFS= read -r line; do add_detail "$name" "$line"; done <<<"$out"
	if ((rc == 0)); then
		add_check "$name" OK "upgrade-preflight --strict --require-no-mpu 通过（无 BLOCK/WARN）"
	elif ((rc == 1)) && ! grep -q '^\[BLOCK\]' <<<"$out"; then
		add_check "$name" WARN "upgrade-preflight 仅有 WARN（strict 下非零；多为未提供 M6/M7 输入或 health-json）；确认后继续或修复重跑"
	else
		add_check "$name" BLOCK "upgrade-preflight 未通过（rc=$rc；--strict --require-no-mpu；见详情）"
	fi
}

# ---------------------------------------------------------------- 冷备份
check_backup_ready() {
	local name=backup_ready
	if [ ! -e "$BACKUP" ]; then
		add_check "$name" BLOCK "冷备份不存在: $BACKUP（先停写并完成冷备份）"
	elif [ -d "$BACKUP" ]; then
		if [ -n "$(find "$BACKUP" -mindepth 1 -print -quit 2>/dev/null)" ]; then
			add_check "$name" OK "冷备份目录非空: $BACKUP"
		else
			add_check "$name" BLOCK "冷备份目录为空: $BACKUP"
		fi
	elif [ -f "$BACKUP" ]; then
		if [ -s "$BACKUP" ]; then
			add_check "$name" OK "冷备份文件非空: $BACKUP"
		else
			add_check "$name" BLOCK "冷备份文件为空: $BACKUP"
		fi
	else
		add_check "$name" BLOCK "冷备份不是普通文件或目录: $BACKUP"
	fi
}

# ---------------------------------------------------------------- 旧服务是否停止
check_service_stopped() {
	local name=service_stopped code rc
	if [ -z "$HEALTH_URL" ]; then
		add_check "$name" WARN "未提供 --health-url，无法判定旧服务是否已停止"
		return
	fi
	if ! command -v curl >/dev/null 2>&1; then
		add_check "$name" BLOCK "未找到 curl，无法探测 --health-url（fail-closed）"
		return
	fi
	# 不用 -f：503/500 仍是“服务在跑”，必须 BLOCK；用 -w 单独取 HTTP 状态。
	code=$(curl --max-time 3 -sS -o /dev/null -w '%{http_code}' "$HEALTH_URL" 2>"$TMP/curl_err")
	rc=$?
	if [[ $code =~ ^[0-9]{3}$ ]] && [ "$code" != 000 ]; then
		add_check "$name" BLOCK "service still running; stop writes before export/backup（$HEALTH_URL 返回 HTTP $code）"
	else
		add_check "$name" OK "旧服务无响应（curl rc=$rc，status=${code:-000}），可继续冷备份/启动新进程"
		[ -s "$TMP/curl_err" ] &&
			add_detail "$name" "curl: $(head -c 300 "$TMP/curl_err" | tr '\n' ' ')"
	fi
}

# ---------------------------------------------------------------- 新二进制版本
check_new_binary_version() {
	local name=new_binary_version out rc res ver verdict
	if [ -z "$NEW_BINARY" ]; then
		add_check "$name" WARN "未提供 --new-binary，无法判定新版本 >= $MIN_VERSION"
		return
	fi
	out=$("$NEW_BINARY" --version 2>&1)
	rc=$?
	if ((rc != 0)); then
		add_check "$name" BLOCK "无法运行 $NEW_BINARY --version（rc=$rc）；需可执行且输出版本"
		add_detail "$name" "$(printf '%s' "$out" | head -c 300 | tr '\n' ' ')"
		return
	fi
	res=$(python3 - "$MIN_VERSION" "$out" <<'PY'
import re
import sys

min_v = tuple(int(x) for x in sys.argv[1].split('.'))
m = re.search(r'\b(\d+)\.(\d+)\.(\d+)\b', sys.argv[2])
if not m:
    print('UNPARSEABLE')
else:
    v = tuple(int(x) for x in m.groups())
    print('%s\t%s' % ('.'.join(m.groups()), 'OK' if v >= min_v else 'OLD'))
PY
)
	ver=${res%%$'\t'*}
	verdict=${res#*$'\t'}
	if [ "$verdict" = OK ]; then
		add_check "$name" OK "新二进制版本 $ver >= $MIN_VERSION"
	else
		add_check "$name" BLOCK "新二进制版本不可解析或低于 $MIN_VERSION（输出: $(printf '%s' "$out" | head -c 120 | tr '\n' ' ')）"
	fi
}

check_upgrade_preflight
check_backup_ready
check_service_stopped
check_new_binary_version

# ---------------------------------------------------------------- 输出
block=0
warn=0
while IFS=$'\t' read -r _ status _; do
	case "$status" in
	BLOCK) block=$((block + 1)) ;;
	WARN) warn=$((warn + 1)) ;;
	esac
done <"$RECORDS"
if ((block > 0)); then
	overall=BLOCK exit_code=1
elif ((STRICT && warn > 0)); then
	overall=WARN exit_code=1
elif ((warn > 0)); then
	overall=WARN exit_code=0
else
	overall=READY exit_code=0
fi
if ((JSON)); then
	python3 - "$RECORDS" "$DETAILS" "$STRICT" "$overall" "$exit_code" <<'PY'
import json
import os
import sys
records, details_dir, strict, overall, exit_code = sys.argv[1:6]
checks = []
for line in open(records, encoding='utf-8'):
    name, status, summary = line.rstrip('\n').split('\t', 2)
    dpath = os.path.join(details_dir, name)
    details = []
    if os.path.exists(dpath):
        details = [ln.rstrip('\n') for ln in open(dpath, encoding='utf-8')]
    checks.append({'name': name, 'status': status, 'summary': summary, 'details': details})
print(json.dumps({
    'tool': 'blue-green-preflight',
    'status': overall,
    'strict': bool(int(strict)),
    'exit_code': int(exit_code),
    'checks': checks,
}, ensure_ascii=False, indent=2))
PY
else
	printf 'blue-green-preflight: data-dir=%s backup=%s config=%s health-json=%s health-url=%s new-binary=%s min-version=%s\n' \
		"$(IFS=,; printf '%s' "${DATA_DIRS[*]}")" "$BACKUP" "${CONFIG:-<未提供>}" \
		"${HEALTH_JSON:-<未提供>}" "${HEALTH_URL:-<未提供>}" "${NEW_BINARY:-<未提供>}" "$MIN_VERSION"
	while IFS=$'\t' read -r name status summary; do
		printf '[%s] %s: %s\n' "$status" "$name" "$summary"
		if [ -f "$DETAILS/$name" ]; then
			while IFS= read -r line; do printf '       - %s\n' "$line"; done <"$DETAILS/$name"
		fi
	done <"$RECORDS"
	if ((STRICT)); then
		printf '整体: %s（--strict；退出码 %d）\n' "$overall" "$exit_code"
	else
		printf '整体: %s（退出码 %d；--strict 下 WARN 也非零）\n' "$overall" "$exit_code"
	fi
	if ((exit_code == 0)); then
		cat <<'EOF'

运行手册（stop-the-world / 蓝绿切换；本脚本只检查，不执行任何变更）：
 1) 预检：upgrade-preflight 无 BLOCK，数据面 wire>=2，在途 MPU 已 Complete/Abort 排空
 2) 停止所有客户端写入，再停止旧服务；确认无写入者（本脚本已检查旧服务无响应）
 3) 冷备份已就绪（--backup）：必须是在停写后取得的备份，保留到验收通过
 4) 用新二进制在原有 data-dir 上启动（同一路径；不要复制/改写数据）
 5) 校验 /health/ready 与 /health/cluster：ready=true、data_plane_compatible=true、wire>=2
 6) 切换流量到新实例，观察错误率与 quorum
 7) 验收通过后再删除冷备份
回滚：停止新二进制 → 恢复冷备份 → 启动旧二进制。
注意：新格式写入或 CMK 变更（rewrap/delete/trash purge 等）之后回滚无效。
EOF
	fi
fi
exit "$exit_code"
