#!/usr/bin/env bash
# upgrade-preflight.sh 的 M6/M7/数据面检查实现（被 source，不单独执行）。
# 依赖调用方提供：CONFIG、CONTROL_DB、HEALTH_JSON、REPL_CMD_STR、REPL_SCOPES、TMP、
# add_check、add_detail；M6/M7 的 python3 片段保持与主脚本相同的 fail-closed 语义。

# ---------------------------------------------------------------- v1.16 数据面下限
# 跨版本 MPU 转发走 wire>=2 的 UploadPart 流式 RPC；<=v1.15 无此 RPC。只读已保存的
# /health/cluster（不联网）：本地下限、兼容位、以及每个 up 节点的 wire 都必须 >=2；
# down 节点不下结论（与端点自身口径一致）。
check_data_plane_floor() {
	local name=data_plane_floor
	if [ -z "$HEALTH_JSON" ]; then
		add_check "$name" WARN "未提供 --health-json，无法判定；升级前请保存 /health/cluster"
		return
	fi
	local -a lines=()
	local st=BLOCK summary="数据面下限检查无结果" line
	mapfile -t lines < <(python3 - "$HEALTH_JSON" <<'PY'
import json
import sys

def result(status, summary, details=()):
    for d in details:
        print('DETAIL\t%s' % d)
    print('RESULT\t%s\t%s' % (status, summary))

try:
    with open(sys.argv[1], encoding='utf-8') as f:
        view = json.load(f)
except Exception as e:
    result('BLOCK', '/health/cluster JSON 缺失或无法解析（fail-closed；见详情）',
           ['读取/解析 %s 失败: %s: %s' % (sys.argv[1], type(e).__name__, e)])
    raise SystemExit
if not isinstance(view, dict):
    result('BLOCK', '/health/cluster JSON 顶层不是对象')
    raise SystemExit
problems = []
local = view.get('data_wire_version')
if isinstance(local, bool) or not isinstance(local, int):
    problems.append('本地 data_wire_version 缺失或不是整数: %s' % json.dumps(local, ensure_ascii=False))
elif local < 2:
    problems.append('本地 data_wire_version=%d < 2（v1.16+ 才有跨版本 MPU RPC）' % local)
if view.get('data_plane_compatible') is not True:
    bad = view.get('incompatible_peers')
    suffix = ''
    if isinstance(bad, list) and bad:
        suffix = '；incompatible_peers=%s' % ', '.join(str(x) for x in bad)
    problems.append('data_plane_compatible 不是 true%s' % suffix)
nodes = view.get('nodes')
ups = 0
if not isinstance(nodes, list):
    problems.append('nodes 缺失或不是数组')
else:
    for i, n in enumerate(nodes):
        if not isinstance(n, dict):
            problems.append('nodes[%d] 不是对象' % i)
            continue
        if n.get('up') is not True:
            continue
        ups += 1
        nid = n.get('id')
        label = str(nid) if isinstance(nid, (str, int)) and not isinstance(nid, bool) else 'nodes[%d]' % i
        v = n.get('data_wire_version')
        if isinstance(v, bool) or not isinstance(v, int):
            problems.append('up 节点 %s data_wire_version 缺失/null（需 >=2）' % label)
        elif v < 2:
            problems.append('up 节点 %s data_wire_version=%d < 2（<=v1.15 无跨版本 MPU RPC）' % (label, v))
if problems:
    result('BLOCK', '数据面低于 v1.16 下限或不兼容（%d 项；见详情）' % len(problems), problems)
else:
    result('OK', '本地 data_wire_version=%d，data_plane_compatible=true，%d 个 up 节点均 >=2' % (local, ups))
PY
)
	for line in "${lines[@]}"; do
		case "$line" in
		RESULT$'\t'*)
			line=${line#RESULT$'\t'}
			st=${line%%$'\t'*}
			summary=${line#*$'\t'}
			;;
		DETAIL$'\t'*) add_detail "$name" "${line#DETAIL$'\t'}" ;;
		esac
	done
	add_check "$name" "$st" "$summary"
}

# ---------------------------------------------------------------- M6 site_token
# M6 token 在 oss.toml [[remotes]].site_token（不在 sites 表；[site].token 是本站身份、
# [stretch].mesh_secret 是 M9 mesh 密钥，均与 M6 无关）。--config 给出时以配置为准：只列
# 缺/空 token 的远端 id+endpoint（绝不输出 token/access_key/secret_key），文件缺失或解析
# 失败 BLOCK。仅给 --control-db 时保留旧探测，无 sites.site_token 列/表报“无法判定”。
check_m6_site_token() {
	local name=m6-site-token
	if [ -z "$CONFIG" ] && [ -z "$CONTROL_DB" ]; then
		add_check "$name" WARN "未提供 --config/--control-db，无法判定（M6 token 在 oss.toml [[remotes]].site_token）"
		return
	fi
	if [ -n "$CONTROL_DB" ] && [ ! -f "$CONTROL_DB" ]; then
		add_detail "$name" "control-db 不存在: $CONTROL_DB"
		add_check "$name" BLOCK "control-db 不存在或不是文件"
		return
	fi
	local -a lines=()
	local st=BLOCK summary="M6 检查无结果（python3 需 3.11+ tomllib）" line
	mapfile -t lines < <(python3 - "$CONFIG" "$CONTROL_DB" <<'PY'
import sqlite3
import sys
import tomllib
from urllib.parse import quote

def result(status, summary, details=()):
    for d in details:
        print('DETAIL\t%s' % d)
    print('RESULT\t%s\t%s' % (status, summary))

cfg_path, db_path = sys.argv[1], sys.argv[2]
if cfg_path:
    try:
        with open(cfg_path, 'rb') as f:
            remotes = tomllib.load(f).get('remotes', [])
        if not isinstance(remotes, list) or not all(isinstance(r, dict) for r in remotes):
            raise ValueError('remotes 不是 [[remotes]] 表数组')
    except Exception as e:
        result('BLOCK', 'oss.toml 缺失或无法解析（fail-closed；见详情）',
               ['oss.toml 读取/解析失败: %s: %s' % (type(e).__name__, e)])
        raise SystemExit
    bad = [r for r in remotes
           if not isinstance(r.get('site_token'), str) or not r['site_token'].strip()]
    if bad:
        label = lambda r, k: r.get(k) if isinstance(r.get(k), str) and r.get(k) else '?'
        result('WARN', '%d/%d 个 oss.toml 远端缺 site_token；这些远端只有旧 M6 语义（版本定向删除不投递），历史副本需重传/人工' % (len(bad), len(remotes)),
               ['缺/空 site_token 的远端: id=%s endpoint=%s' % (label(r, 'id'), label(r, 'endpoint')) for r in bad])
    elif remotes:
        result('OK', '%d 个 oss.toml 远端的 site_token 均非空' % len(remotes))
    else:
        result('OK', 'oss.toml 无 [[remotes]]，无 M6 远端目标')
    raise SystemExit
try:
    con = sqlite3.connect('file:' + quote(db_path) + '?mode=ro', uri=True)
    con.execute('PRAGMA query_only = ON')
    try:
        con.execute('SELECT 1 FROM sites LIMIT 1')
    except sqlite3.OperationalError as e:
        if 'no such table' not in str(e):
            raise
        result('WARN', 'sites 表不存在，无法判定（不同版本 schema；不做通过处理）')
        raise SystemExit
    cols = [r[1] for r in con.execute('PRAGMA table_info(sites)')]
    total = con.execute('SELECT COUNT(*) FROM sites').fetchone()[0]
    if 'site_token' not in cols:
        result('WARN', 'sites 表无 site_token 列（列: %s），无法判定；当前 M6 token 是 oss.toml [[remotes]].site_token，请人工核对' % (','.join(cols) or '无'))
        raise SystemExit
    bad = list(con.execute("SELECT peer_id, name FROM sites WHERE site_token IS NULL OR TRIM(site_token) = '' ORDER BY peer_id"))
    if bad:
        result('WARN', '%d/%d 个 sites 远端 site_token 为 NULL/空；这些远端仅有旧 M6 语义（版本定向删除不投递），历史副本需重传/人工' % (len(bad), total),
               ['site_token 为 NULL/空: %s %s' % (peer, nm) for peer, nm in bad])
    else:
        result('OK', '%d 个 sites 远端的 site_token 均非空' % total)
except sqlite3.Error as e:
    result('BLOCK', 'control-db 无法只读解析（见详情）', ['读取 control-db 失败: %s' % e])
PY
)
	for line in "${lines[@]}"; do
		case "$line" in
		RESULT$'\t'*)
			line=${line#RESULT$'\t'}
			st=${line%%$'\t'*}
			summary=${line#*$'\t'}
			;;
		DETAIL$'\t'*) add_detail "$name" "${line#DETAIL$'\t'}" ;;
		esac
	done
	add_check "$name" "$st" "$summary"
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
