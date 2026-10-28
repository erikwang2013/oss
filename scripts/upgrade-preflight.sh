#!/usr/bin/env bash
# 升级前置检查（只读）：M6 无 site_token / M7 未映射 REPLICA / 存量裸 UUID MPU。
# 三项兼容边界都不能自动追溯修复；本脚本只检查/演练，不回填、不删除、不联网：
# 1) M6 无 site_token 历史远端（repl §6.1）：旧远端目标版本 id 由目标生成，源端无映射；
#    配置 token 只对新复制生效。按内容/ETag 删除不安全。sites 表有 site_token 列时列出
#    NULL/空；无该列（当前 schema 只有 peer_site_token，M6 token 在 oss.toml
#    [[remotes]].site_token）报告“无法判定”，绝不当作通过。
# 2) M7 升级前删除不回溯（repl §6.2）：删除事件未记录，目标只剩无来源 REPLICA。人工路径：
#      oss-server --config <toml> replica-reconcile --bucket B --key K --json
#    核对 version_id 后显式 --delete ID1,ID2 --apply（绝不按内容/前缀自动选择）。本脚本
#    只调用 list 模式，拒绝含 --delete/--apply 的模板；该 CLI 会按设计写一条审计行。
# 3) 存量裸 UUID MPU 粘原节点（repl §6.3）：裸 UUID 属主只在创建节点本地；升级前必须
#    Complete/Abort 排空，升级后只能在创建节点本地收尾或人工清理
#    <root>/<bucket>/<flattened-key>/<uuid>/mpu.meta。本脚本按实际暂存布局扫描（不是
#    .oss.sys/tmp，那是单段 PUT 暂存），解析 mpu.meta 顶层 owner：缺失/NULL = 旧格式。
# 本脚本自身只读；不修改/删除数据。退出码：0 无 BLOCK（--strict 下 WARN 也非零）；
# 1 = BLOCK 或 strict+WARN；2 = 用法错误。

set -uo pipefail

usage() {
	cat <<'EOF'
用法: upgrade-preflight.sh --data-dir <path> [--data-dir <path>] [选项]

升级前置只读检查：裸 UUID MPU、M6 site_token、M7 未映射 REPLICA。
不回填、不删除、不联网写入。完整说明见脚本头部注释。

必填：
  --data-dir <path>            数据盘根目录；可多次或逗号分隔（如 a,b）
                               （对应配置里的 data_dir/data_dirs）

选项：
  --control-db <path>          控制面 SQLite（oss.db / control.db）；只读打开。
                               不提供则 M6 为“无法判定”（WARN）。
  --replica-reconcile <cmd>    replica-reconcile 命令前缀，默认 "oss-server replica-reconcile"。
                               按空格拆分，可带 --config 等全局参数；不能含 --delete/--apply。
  --replica-scope <b>/<k>      M7 的精确桶/键，可多次（键可含逗号，不拆分）。
                               replica-reconcile 只支持精确 (bucket,key)，没有全局列举；
                               不提供则 M7 为“无法判定”（WARN），需升级后人工逐键列出。
  --json                       以 JSON 输出（不改变退出码语义）
  --strict                     WARN 也非零退出（发布闸门）
  -h, --help                   显示本帮助

状态：BLOCK = 输入/工具/安全问题；WARN = 发现问题或无法判定；OK = 通过。
退出码：0 = 无 BLOCK（--strict 下 WARN 也非零）；1 = BLOCK 或 strict+WARN；2 = 用法错误。
示例：
  scripts/upgrade-preflight.sh --data-dir /data --control-db /data/oss.db
  scripts/upgrade-preflight.sh --data-dir /data1,/data2 --control-db /data1/control.db \
    --replica-scope bucket-a/key1 --replica-scope bucket-b/key2 --json --strict
  scripts/upgrade-preflight.sh --data-dir /data --control-db /data/oss.db \
    --replica-reconcile "oss-server --config /etc/oss/oss.toml replica-reconcile"
EOF
}

usage_err() {
	printf '错误: %s\n\n' "$*" >&2
	usage >&2
	exit 2
}
need_arg() { (($# >= 2)) || usage_err "选项 $1 缺少参数"; }

DATA_DIRS=()
CONTROL_DB=""
REPL_CMD_STR="oss-server replica-reconcile"
REPL_SCOPES=()
JSON=0
STRICT=0

add_data_dirs() {
	local val=$1 item
	local IFS=','
	for item in $val; do
		[ -n "$item" ] && DATA_DIRS+=("$item")
	done
}

while (($#)); do
	case "$1" in
	--data-dir)
		need_arg "$@"
		add_data_dirs "$2"
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

# ---------------------------------------------------------------- 裸 UUID MPU
# mpu.meta 是 rmp_serde::to_vec_named 的 struct-as-map。只取顶层 owner 字段：缺失/
# NULL/空串 = 旧格式（本地属主）。用最小 msgpack 跳过器，避免把 user_metadata/tags
# 里嵌套的 "owner" 误当成字段。
scan_mpu_meta() { # paths_file
	python3 - "$1" <<'PY'
import sys

# 长度前缀宽度：bin/str；array/map 递归；ext 额外加 1 字节类型。
W = {0xc4: 1, 0xc5: 2, 0xc6: 4, 0xd9: 1, 0xda: 2, 0xdb: 4,
     0xdc: 2, 0xdd: 4, 0xde: 2, 0xdf: 4, 0xc7: 1, 0xc8: 2, 0xc9: 4}
FIX = {0xcc: 1, 0xcd: 2, 0xce: 4, 0xcf: 8, 0xd0: 1, 0xd1: 2, 0xd2: 4,
       0xd3: 8, 0xca: 4, 0xcb: 8, 0xd4: 2, 0xd5: 3, 0xd6: 5, 0xd7: 9, 0xd8: 17}


def skip(b, i):
    if i >= len(b):
        raise ValueError('truncated')
    c = b[i]
    i += 1
    if c <= 0x7f or c >= 0xe0:
        return i
    if 0xa0 <= c <= 0xbf:
        return i + (c & 0x1f)
    if 0x90 <= c <= 0x9f:
        for _ in range(c & 0xf):
            i = skip(b, i)
        return i
    if 0x80 <= c <= 0x8f:
        for _ in range(2 * (c & 0xf)):
            i = skip(b, i)
        return i
    if c in (0xc0, 0xc2, 0xc3):
        return i
    if c in FIX:
        return i + FIX[c]
    if c not in W:
        raise ValueError('bad byte %#x' % c)
    w = W[c]
    n = int.from_bytes(b[i:i + w], 'big')
    j = i + w
    if c in (0xdc, 0xdd, 0xde, 0xdf):
        for _ in range(2 * n if c in (0xde, 0xdf) else n):
            j = skip(b, j)
        return j
    return j + n + (1 if c in (0xc7, 0xc8, 0xc9) else 0)


def state(path):
    try:
        with open(path, 'rb') as f:
            b = f.read()
        i = 1
        c = b[0]
        if 0x80 <= c <= 0x8f:
            n = c & 0xf
        elif c in (0xde, 0xdf):
            w = 2 if c == 0xde else 4
            n = int.from_bytes(b[i:i + w], 'big')
            i += w
        else:
            return 'CORRUPT'
        owner = None
        for _ in range(n):
            k0 = i
            i = skip(b, i)
            key = b[k0:i]
            v0 = i
            i = skip(b, i)
            if key in (b'\xa5owner', b'\xd9\x05owner'):
                owner = b[v0:i]
        if i != len(b):
            return 'CORRUPT'
    except Exception:
        return 'CORRUPT'
    return 'OWNED' if owner not in (None, b'\xc0', b'\xa0') else 'LEGACY'


for raw in open(sys.argv[1], encoding='utf-8', errors='replace'):
    p = raw.rstrip('\n')
    if p:
        print('%s\t%s' % (state(p), p))
PY
}

check_bare_uuid_mpu() {
	local name=bare-uuid-mpu root errs=0 invalid=0
	local -a scan=()
	for root in "${DATA_DIRS[@]}"; do
		if [ ! -d "$root" ]; then
			invalid=1
			add_detail "$name" "data-dir 不存在或不是目录: $root"
		fi
	done
	# 路径非法单独成 BLOCK，避免把“目录不存在”误判成“没有旧 MPU”。
	if ((invalid)); then
		add_check "$name" BLOCK "至少一个 --data-dir 不存在/不可用（见详情）"
		return
	fi
	: >"$TMP/mpu_files"
	: >"$TMP/find_err"
	for root in "${DATA_DIRS[@]}"; do
		# -H：允许 data-dir 本身是符号链接；不跟随树内符号链接。
		find -H "$root" -mindepth 4 -maxdepth 4 -type f -name mpu.meta -print \
			>>"$TMP/mpu_files" 2>>"$TMP/find_err" || errs=1
	done
	sort -u -o "$TMP/mpu_files" "$TMP/mpu_files" # 重叠/重复的 data-dir 不重复计数
	local legacy=0 owned=0 corrupt=0 unreadable=0 total=0 line st path
	if [ -s "$TMP/mpu_files" ]; then
		mapfile -t scan < <(scan_mpu_meta "$TMP/mpu_files")
		for line in "${scan[@]}"; do
			[ -n "$line" ] || continue
			st=${line%%$'\t'*}
			path=${line#*$'\t'}
			total=$((total + 1))
			case "$st" in
			LEGACY)
				legacy=$((legacy + 1))
				add_detail "$name" "裸 UUID 暂存区（升级前需 Complete/Abort）: $path"
				;;
			OWNED) owned=$((owned + 1)) ;;
			CORRUPT)
				corrupt=$((corrupt + 1))
				add_detail "$name" "mpu.meta 无法解析，人工核查: $path"
				;;
			*)
				unreadable=$((unreadable + 1))
				add_detail "$name" "mpu.meta 不可读，人工核查: $path"
				;;
			esac
		done
	fi
	local status=OK summary="扫描 $total 个活 MPU 暂存区：裸 UUID=$legacy，带属主前缀=$owned"
	if ((legacy > 0)); then
		status=WARN
		summary+="；升级前必须排空裸 UUID 在途 MPU（Complete/Abort），否则集群其他节点无法收尾"
	fi
	if ((corrupt + unreadable > 0)); then
		status=WARN
		summary+="；损坏/不可读 mpu.meta=$((corrupt + unreadable))（人工核查）"
	fi
	if ((errs)); then
		status=WARN
		summary+="；部分目录不可读，扫描可能不完整（见详情）"
		local n=0 l
		while IFS= read -r l && ((n < 20)); do
			add_detail "$name" "find: $l"
			n=$((n + 1))
		done <"$TMP/find_err"
	fi
	add_check "$name" "$status" "$summary"
}

# ---------------------------------------------------------------- M6 site_token
check_m6_site_token() {
	local name=m6-site-token
	if [ -z "$CONTROL_DB" ]; then
		add_check "$name" WARN "未提供 --control-db，无法判定（M6 token 实际在 oss.toml [[remotes]].site_token）"
		return
	fi
	if [ ! -f "$CONTROL_DB" ]; then
		add_detail "$name" "control-db 不存在: $CONTROL_DB"
		add_check "$name" BLOCK "control-db 不存在或不是文件"
		return
	fi
	local -a rows=()
	mapfile -t rows < <(python3 - "$CONTROL_DB" <<'PY'
import sqlite3
import sys
from urllib.parse import quote

try:
    con = sqlite3.connect('file:' + quote(sys.argv[1]) + '?mode=ro', uri=True)
    con.execute('PRAGMA query_only = ON')
    try:
        con.execute('SELECT 1 FROM sites LIMIT 1')
    except sqlite3.OperationalError as e:
        print('STATE\tno_table' if 'no such table' in str(e) else 'STATE\terror\t' + str(e))
        sys.exit(0)
    cols = [r[1] for r in con.execute('PRAGMA table_info(sites)')]
    print('COLS\t' + ','.join(cols))
    if 'site_token' not in cols:
        print('STATE\tno_column')
        sys.exit(0)
    total = con.execute('SELECT COUNT(*) FROM sites').fetchone()[0]
    bad = list(con.execute(
        "SELECT peer_id, name FROM sites "
        "WHERE site_token IS NULL OR TRIM(site_token) = '' ORDER BY peer_id"))
    print('TOTAL\t%d' % total)
    for peer, nm in bad:
        print('BAD\t%s\t%s' % (peer, nm))
    print('STATE\tbad_rows' if bad else 'STATE\tok')
except sqlite3.Error as e:
    print('STATE\terror\t' + str(e))
PY
)
	local state="" total=0 cols="" bad=0 line
	local -a bad_rows=()
	for line in "${rows[@]}"; do
		case "$line" in
		STATE$'\t'*) state=${line#STATE$'\t'} ;;
		TOTAL$'\t'*) total=${line#TOTAL$'\t'} ;;
		COLS$'\t'*) cols=${line#COLS$'\t'} ;;
		BAD$'\t'*) bad=$((bad + 1)); bad_rows+=("${line#BAD$'\t'}") ;;
		esac
	done
	case "$state" in
	ok) add_check "$name" OK "$total 个 sites 远端的 site_token 均非空" ;;
	bad_rows)
		for line in "${bad_rows[@]}"; do
			add_detail "$name" "site_token 为 NULL/空: ${line//$'\t'/ }"
		done
		add_check "$name" WARN "$bad/$total 个 sites 远端 site_token 为 NULL/空；这些远端仅有旧 M6 语义（版本定向删除不投递），历史副本需重传/人工"
		;;
	no_table) add_check "$name" WARN "sites 表不存在，无法判定（不同版本 schema；不做通过处理）" ;;
	no_column) add_check "$name" WARN "sites 表无 site_token 列（列: ${cols:-无}），无法判定；当前 M6 token 是 oss.toml [[remotes]].site_token，请人工核对" ;;
	error*)
		add_detail "$name" "读取 control-db 失败: ${state#error$'\t'}"
		add_check "$name" BLOCK "control-db 无法只读解析（见详情）"
		;;
	*) add_check "$name" BLOCK "control-db 检查内部错误（state=${state}）" ;;
	esac
}

# ---------------------------------------------------------------- M7 unmapped REPLICA
repl_cmd_safe() {
	local a
	for a in "$@"; do
		case "$a" in
		--delete | --delete=* | --apply | --apply=*) return 1 ;;
		esac
	done
	return 0
}

check_m7_unmapped_replica() {
	local name=m7-unmapped-replica
	local -a cmd=()
	# shortcut: 命令前缀按空格拆分，不支持带空格的参数路径（需要时用包装脚本）。
	# shellcheck disable=SC2206
	cmd=($REPL_CMD_STR)
	if ((${#cmd[@]} == 0)); then
		add_check "$name" BLOCK "--replica-reconcile 为空"
		return
	fi
	if ! repl_cmd_safe "${cmd[@]}"; then
		add_check "$name" BLOCK "命令模板含 --delete/--apply；本检查只允许只读 list（安全拒绝）"
		return
	fi
	if ((${#REPL_SCOPES[@]} == 0)); then
		add_check "$name" WARN "未提供 --replica-scope <bucket>/<key>，无法判定：replica-reconcile 只支持精确 (bucket,key)，没有全局列举；请升级后逐键人工列出"
		return
	fi
	if ! command -v "${cmd[0]}" >/dev/null 2>&1; then
		add_check "$name" WARN "找不到命令 ${cmd[0]}，无法判定；需人工运行 replica-reconcile"
		return
	fi
	local scope b k out rc unmapped=0 determined=0 undetermined=0
	local -a ids=()
	for scope in "${REPL_SCOPES[@]}"; do
		if [[ $scope != */* ]]; then
			add_detail "$name" "非法 --replica-scope（需 bucket/key）: $scope"
			undetermined=$((undetermined + 1))
			continue
		fi
		b=${scope%%/*}
		k=${scope#*/}
		out=$("${cmd[@]}" --bucket "$b" --key "$k" --json 2>"$TMP/repl_err")
		rc=$?
		if ((rc != 0)); then
			add_detail "$name" "bucket=$b key=$k 调用失败 rc=$rc: $(head -c 300 "$TMP/repl_err" | tr '\n' ' ')"
			undetermined=$((undetermined + 1))
			continue
		fi
		printf '%s' "$out" >"$TMP/repl_out"
		mapfile -t ids < <(python3 - "$TMP/repl_out" <<'PY'
import json
import sys

try:
    rows = json.load(open(sys.argv[1], encoding='utf-8'))
    assert isinstance(rows, list)
except Exception as e:
    print('ERR\t' + str(e))
    raise SystemExit(0)
print(len(rows))
for r in rows:
    if isinstance(r, dict) and r.get('version_id'):
        print(r['version_id'])
PY
)
		if ((${#ids[@]} == 0)) || [[ ! ${ids[0]} =~ ^[0-9]+$ ]] || [[ ${ids[0]} == ERR$'\t'* ]]; then
			add_detail "$name" "bucket=$b key=$k JSON 解析失败: ${ids[*]:-空输出}；原始输出: $(head -c 300 "$TMP/repl_out" | tr '\n' ' ')"
			undetermined=$((undetermined + 1))
			continue
		fi
		local n=${ids[0]} idlist="" oldifs=$IFS
		if ((n > 0)); then
			IFS=,
			idlist="${ids[*]:1}"
			IFS=$oldifs
			unmapped=$((unmapped + n))
			add_detail "$name" "bucket=$b key=$k 未映射 REPLICA=$n ids=$idlist（用显式 --delete 逐 id 人工清理）"
		else
			add_detail "$name" "bucket=$b key=$k 未映射 REPLICA=0"
		fi
		determined=$((determined + 1))
	done
	if ((unmapped > 0)); then
		local extra=""
		((undetermined > 0)) && extra="；另有 $undetermined 个键无法判定"
		add_check "$name" WARN "发现 $unmapped 个未映射 REPLICA（M7 升级前删除不回溯）；需用 replica-reconcile 按显式 version id 人工清理$extra"
	elif ((undetermined > 0)); then
		add_check "$name" WARN "$undetermined/${#REPL_SCOPES[@]} 个键无法判定（见详情）；已确认 $determined 个键无未映射副本"
	else
		add_check "$name" OK "检查 $determined 个键：未发现未映射 REPLICA"
	fi
}

check_bare_uuid_mpu
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
	printf 'upgrade-preflight: data-dir=%s control-db=%s\n' \
		"$(IFS=,; printf '%s' "${DATA_DIRS[*]}")" "${CONTROL_DB:-<未提供>}"
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
