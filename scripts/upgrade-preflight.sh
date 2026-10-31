#!/usr/bin/env bash
# 升级前置检查（只读）：M6 无 site_token / M7 未映射 REPLICA / 存量裸 UUID MPU。
# 三项兼容边界都不能自动追溯修复；本脚本只检查/演练，不回填、不删除、不联网：
# 1) M6 无 site_token 历史远端（repl §6.1）：旧远端目标版本 id 由目标生成，源端无映射；
#    token 只对新复制生效，按内容/ETag 删除不安全。token 在 oss.toml [[remotes]].site_token
#    （[site].token 是本站身份、[stretch].mesh_secret 是 M9 mesh 密钥，均与 M6 无关）。
#    --config 只列缺/空 token 的远端 id+endpoint（绝不打印 token/密钥）；文件缺失/坏配置
#    BLOCK。仅给 --control-db 时探测 sites.site_token（当前 schema 无此列，只有
#    peer_site_token），无列/表报“无法判定”，绝不当作通过。
# 2) M7 升级前删除不回溯（repl §6.2）：删除事件未记录，目标只剩无来源 REPLICA。人工路径：
#      oss-server --config <toml> replica-reconcile --bucket B --key K --json
#    核对 version_id 后显式 --delete ID1,ID2 --apply（绝不按内容/前缀自动选择）。本脚本
#    只调用 list 模式，拒绝含 --delete/--apply 的模板；该 CLI 会按设计写一条审计行。
# 3) 存量裸 UUID MPU 粘原节点（repl §6.3）：裸 UUID 属主只在创建节点本地；升级前必须
#    Complete/Abort 排空，升级后只能在创建节点本地收尾或人工清理
#    <root>/<bucket>/<flattened-key>/<uuid>/mpu.meta。本脚本按实际暂存布局扫描（不是
#    .oss.sys/tmp，那是单段 PUT 暂存），解析 mpu.meta 顶层 owner：缺失/NULL = 旧格式。
# 4) v1.16 数据面下限（/health/cluster 离线快照）：跨版本 MPU 复用 wire>=2 的 UploadPart
#    流式 RPC，<=v1.15 没有该 RPC。--health-json 读取已保存的 /health/cluster JSON
#    （不联网）：本地 data_wire_version<2/缺失、data_plane_compatible!=true、任一 up
#    节点 wire<2/null 均 BLOCK；未提供则 WARN“无法判定”（--strict 下非零）。
# 5) 在途 MPU 闸门：--require-no-mpu 要求所有活暂存区（含带属主前缀）为 0；滚动升级前
#    必须 Complete/Abort 排空，否则旧节点离开后无法收尾。默认只把裸 UUID 计 WARN。
# 本脚本自身只读；不修改/删除数据。退出码：0 无 BLOCK（--strict 下 WARN 也非零）；
# 1 = BLOCK 或 strict+WARN；2 = 用法错误。
set -uo pipefail

usage() {
	cat <<'EOF'
用法: upgrade-preflight.sh --data-dir <path> [--data-dir <path>] [选项]
升级前置只读检查：裸 UUID MPU、v1.16 数据面下限、M6 site_token、M7 未映射 REPLICA。
不回填、不删除、不联网写入。完整说明见脚本头部注释。
必填：
  --data-dir <path>            数据盘根目录；可多次或逗号分隔（如 a,b）
                               （对应配置里的 data_dir/data_dirs）
选项：
  --config <oss.toml>          只读解析 [[remotes]].site_token（绝不打印 token/密钥）。
                               提供且远端 token 齐全时 M6 为 OK；文件缺失/解析失败 BLOCK。
  --control-db <path>          控制面 SQLite（oss.db / control.db）；只读打开。
                               与 --config 均不提供则 M6 为“无法判定”（WARN）。
  --replica-reconcile <cmd>    replica-reconcile 命令前缀，默认 "oss-server replica-reconcile"。
                               按空格拆分，可带 --config 等全局参数；不能含 --delete/--apply。
  --replica-scope <b>/<k>      M7 的精确桶/键，可多次（键可含逗号，不拆分）。
                               replica-reconcile 只支持精确 (bucket,key)，没有全局列举；
                               不提供则 M7 为“无法判定”（WARN），需升级后人工逐键列出。
  --health-json <path>         已保存的 /health/cluster JSON 快照（不联网）。校验数据面
                               下限 v1.16（wire>=2）与兼容性；缺失/坏 JSON BLOCK。
  --require-no-mpu             任一活 MPU 暂存区（含带属主前缀）即 BLOCK：升级前必须
                               Complete/Abort 排空（默认仅裸 UUID/损坏计 WARN）。
  --json                       以 JSON 输出（不改变退出码语义）
  --strict                     WARN 也非零退出（发布闸门）
  -h, --help                   显示本帮助
状态：BLOCK = 输入/工具/安全问题；WARN = 发现问题或无法判定；OK = 通过。
退出码：0 = 无 BLOCK（--strict 下 WARN 也非零）；1 = BLOCK 或 strict+WARN；2 = 用法错误。
示例：
  scripts/upgrade-preflight.sh --data-dir /data --config /etc/oss/oss.toml
  scripts/upgrade-preflight.sh --data-dir /data1,/data2 --control-db /data1/control.db \
    --replica-scope bucket-a/key1 --replica-scope bucket-b/key2 --json --strict
EOF
}
usage_err() { printf '错误: %s\n\n' "$*" >&2; usage >&2; exit 2; }
need_arg() { (($# >= 2)) || usage_err "选项 $1 缺少参数"; }

DATA_DIRS=()
CONFIG=""
CONTROL_DB=""
REPL_CMD_STR="oss-server replica-reconcile"
REPL_SCOPES=()
HEALTH_JSON=""
REQUIRE_NO_MPU=0
JSON=0
STRICT=0

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
	--config)
		need_arg "$@"
		CONFIG=$2
		shift 2
		;;
	--control-db)
		need_arg "$@"
		CONTROL_DB=$2
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
	--health-json)
		need_arg "$@"
		HEALTH_JSON=$2
		shift 2
		;;
	--require-no-mpu) REQUIRE_NO_MPU=1; shift ;;
	--json) JSON=1; shift ;;
	--strict) STRICT=1; shift ;;
	-h | --help) usage; exit 0 ;;
	*) usage_err "未知参数: $1" ;;
	esac
done
(( ${#DATA_DIRS[@]} > 0 )) || usage_err "至少提供一个 --data-dir"
command -v python3 >/dev/null 2>&1 ||
	{ printf '[BLOCK] python3: 未找到 python3，无法可靠解析 mpu.meta（msgpack）与只读 sqlite\n' >&2; exit 1; }

TMP=$(mktemp -d "${TMPDIR:-/tmp}/oss-preflight.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT
RECORDS="$TMP/records"
DETAILS="$TMP/details"
mkdir -p "$DETAILS"
: >"$RECORDS"
add_check() { printf '%s\t%s\t%s\n' "$1" "$2" "$3" >>"$RECORDS"; }
add_detail() { printf '%s\n' "$2" >>"$DETAILS/$1"; }

# ---------------------------------------------------------------- 检查实现
# 检查函数按内聚性拆到 scripts/lib/（MPU 扫描 / M6+M7+数据面）；仅被本脚本 source，
# 共用上方 TMP、add_check/add_detail 与解析后的选项。缺失即 BLOCK（fail-closed）。
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
for _lib in upgrade-preflight-mpu.sh upgrade-preflight-checks.sh; do
	[ -r "$SCRIPT_DIR/lib/$_lib" ] ||
		{ printf '[BLOCK] 缺少检查实现: %s\n' "$SCRIPT_DIR/lib/$_lib" >&2; exit 1; }
	# shellcheck source=/dev/null
	. "$SCRIPT_DIR/lib/$_lib"
done
unset _lib

check_bare_uuid_mpu
check_data_plane_floor
check_m6_site_token
check_m7_unmapped_replica
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
	overall=OK exit_code=0
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
    'tool': 'upgrade-preflight',
    'status': overall,
    'strict': bool(int(strict)),
    'exit_code': int(exit_code),
    'checks': checks,
}, ensure_ascii=False, indent=2))
PY
else
	printf 'upgrade-preflight: data-dir=%s config=%s control-db=%s health-json=%s require-no-mpu=%s\n' \
		"$(IFS=,; printf '%s' "${DATA_DIRS[*]}")" "${CONFIG:-<未提供>}" "${CONTROL_DB:-<未提供>}" \
		"${HEALTH_JSON:-<未提供>}" "$REQUIRE_NO_MPU"
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
fi
exit "$exit_code"
