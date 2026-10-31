#!/usr/bin/env bash
# scripts/upgrade-preflight.sh 的自检：临时目录 + 临时 sqlite + 注入的 stub 命令。
# 不依赖 cargo / 网络 / 真实 oss-server。

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MAIN="$HERE/../upgrade-preflight.sh"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/oss-preflight-test.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT

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

assert_rc() { # desc expected actual
	if [ "$2" = "$3" ]; then ok "$1"; else bad "$1" "expected rc=$2 got rc=$3"; fi
}

assert_has() { # desc haystack needle
	case "$2" in
	*"$3"*) ok "$1" ;;
	*) bad "$1" "missing: $3" ;;
	esac
}

assert_not_has() { # desc haystack needle
	case "$2" in
	*"$3"*) bad "$1" "unexpected: $3" ;;
	*) ok "$1" ;;
	esac
}

json_status() { # file check-name
	python3 - "$1" "$2" <<'PY'
import json
import sys

with open(sys.argv[1], encoding='utf-8') as f:
    d = json.load(f)
for c in d['checks']:
    if c['name'] == sys.argv[2]:
        print(c['status'])
        break
else:
    print('MISSING')
PY
}

# ---------------------------------------------------------------- fixtures

python3 - "$TMP" <<'PY'
import json
import os
import sqlite3
import struct
import sys

tmp = sys.argv[1]


def s(x):
    b = x.encode()
    assert len(b) < 32
    return bytes([0xA0 | len(b)]) + b


def enc(v):
    if v is None:
        return b'\xc0'
    if v is False:
        return b'\xc2'
    if v is True:
        return b'\xc3'
    if isinstance(v, int):
        return b'\xcf' + struct.pack('>Q', v)
    if isinstance(v, list):
        return bytes([0x90 | len(v)]) + b''.join(enc(x) for x in v)
    if isinstance(v, dict):
        return bytes([0x80 | len(v)]) + b''.join(s(k) + enc(x) for k, x in v.items())
    return s(v)


def write_meta(path, pairs):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'wb') as f:
        f.write(bytes([0x80 | len(pairs)]) + b''.join(s(k) + enc(v) for k, v in pairs))


def mpu(root, uuid, owned):
    d = os.path.join(root, 'bucket', 'obj__XLDIR__x', uuid)
    # 覆盖 struct-as-map 的常见分支：str/null/bool/int/map/array；tags 里故意放
    # "owner" 字面量，确认解析器只看顶层字段、不被嵌套值误导。
    pairs = [('content_type', 'text/plain'), ('user_metadata', {'owner': 'nested-metadata-value'}),
             ('created_nanos', 1), ('sse_c_md5', None), ('sse_kind', None), ('lock_mode', None),
             ('lock_until_nanos', None), ('lock_legal_hold', False), ('storage_class', None),
             ('tags', [{'key': 'owner', 'value': 'owner'}])]
    if owned:
        pairs.append(('owner', 'node-1'))
    write_meta(os.path.join(d, 'mpu.meta'), pairs)
    return d


# data-a：一个带属主前缀（正常）+ 一个已完成标记 + 普通版本目录
da = os.path.join(tmp, 'data-a')
mpu(da, '11111111-1111-4111-8111-111111111111', owned=True)
done = mpu(da, '22222222-2222-4222-8222-222222222222', owned=False)
os.rename(os.path.join(done, 'mpu.meta'), os.path.join(done, 'mpu.meta.done'))
os.makedirs(os.path.join(da, 'bucket', 'plain', '33333333-3333-4333-8333-333333333333'))
with open(os.path.join(da, 'bucket', 'plain', '33333333-3333-4333-8333-333333333333', 'xl.meta'), 'wb') as f:
    f.write(b'not-msgpack-but-not-mpu-meta')

# data-b：旧格式裸 UUID + 损坏的 mpu.meta
db_root = os.path.join(tmp, 'data-b')
mpu(db_root, '44444444-4444-4444-8444-444444444444', owned=False)
broken = os.path.join(db_root, 'bucket', 'obj2', '55555555-5555-4555-8555-555555555555')
os.makedirs(broken)
with open(os.path.join(broken, 'mpu.meta'), 'wb') as f:
    f.write(b'\x81\xa5owner')  # map{owner: <missing value>}

# data-empty：无 MPU 暂存区，供 --require-no-mpu 的“干净”分支与 health-json 隔离。
os.makedirs(os.path.join(tmp, 'data-empty'))


# /health/cluster 离线快照夹具（字段与 oss-server router::cluster_view 一致）。
def health(name, local=2, compatible=True, nodes=None):
    if nodes is None:
        nodes = [
            {'id': 'n1', 'up': True, 'data_wire_version': local},
            {'id': 'n2', 'up': True, 'data_wire_version': 2},
            {'id': 'n3', 'up': False, 'data_wire_version': None},  # down 节点不下结论
        ]
    with open(os.path.join(tmp, name), 'w', encoding='utf-8') as f:
        json.dump({
            'node_id': 'n1', 'data_wire_version': local,
            'data_plane_compatible': compatible, 'degraded_peers': [],
            'incompatible_peers': [] if compatible else ['n2(v1)'],
            'nodes': nodes,
        }, f, ensure_ascii=False)


health('health-good.json')
health('health-local-old.json', local=1)
health('health-old-peer.json', nodes=[
    {'id': 'n1', 'up': True, 'data_wire_version': 2},
    {'id': 'n2', 'up': True, 'data_wire_version': 1},
])
health('health-null-peer.json', nodes=[
    {'id': 'n1', 'up': True, 'data_wire_version': 2},
    {'id': 'n2', 'up': True, 'data_wire_version': None},
])
health('health-incompatible.json', compatible=False)
with open(os.path.join(tmp, 'health-bad.json'), 'w', encoding='utf-8') as f:
    f.write('{"data_wire_version": 2,')  # 截断的 JSON


def make_db(path, rows, token_col=True, table=True):
    con = sqlite3.connect(path)
    if table:
        if token_col:
            con.execute('CREATE TABLE sites (peer_id TEXT PRIMARY KEY, name TEXT, site_token TEXT)')
            con.executemany('INSERT INTO sites VALUES (?,?,?)', rows)
        else:
            con.execute('CREATE TABLE sites (peer_id TEXT PRIMARY KEY, name TEXT, peer_site_token TEXT NOT NULL)')
            con.execute("INSERT INTO sites VALUES ('p1','r1','tok')")
    con.commit()
    con.close()


make_db(os.path.join(tmp, 'good.db'), [('p1', 'r1', 'tok')])
make_db(os.path.join(tmp, 'bad.db'), [('p1', 'r1', None), ('p2', 'r2', ''), ('p3', 'r3', 'tok')])
make_db(os.path.join(tmp, 'nocol.db'), [], token_col=False)
make_db(os.path.join(tmp, 'notable.db'), [], table=False)

# oss.toml 夹具：token 与密钥只用假值，断言输出绝不泄露。
GOOD_TOML = '''
[[remotes]]
id = "r1"
endpoint = "https://peer1:9000"
access_key = "fixture-access-a"
secret_key = "fixture-secret-a"
site_token = "fixture-token-a"

[[remotes]]
id = "r2"
endpoint = "https://peer2:9000"
access_key = "fixture-access-b"
secret_key = "fixture-secret-b"
site_token = "fixture-token-b"
'''
MISSING_TOML = '''
[[remotes]]
id = "r1"
endpoint = "https://peer1:9000"
access_key = "fixture-access-a"
secret_key = "fixture-secret-a"
site_token = "fixture-token-a"

[[remotes]]
id = "r2"
endpoint = "https://peer2:9000"
access_key = "fixture-access-b"
secret_key = "fixture-secret-b"

[[remotes]]
id = "r3"
endpoint = "https://peer3:9000"
site_token = "   "
'''
with open(os.path.join(tmp, 'config-good.toml'), 'w', encoding='utf-8') as f:
    f.write(GOOD_TOML)
with open(os.path.join(tmp, 'config-missing.toml'), 'w', encoding='utf-8') as f:
    f.write(MISSING_TOML)
with open(os.path.join(tmp, 'config-syntax.toml'), 'w', encoding='utf-8') as f:
    f.write('[[remotes]\nid = "r1"\n')
PY

STUB="$TMP/stub-replica"
cat >"$STUB" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_LOG:?}"
if [ "${STUB_MODE:-empty}" = rows ]; then
	printf '[{"version_id":"v1"},{"version_id":"v2"}]\n'
else
	printf '[]\n'
fi
SH
chmod +x "$STUB"
export STUB_LOG="$TMP/stub.log"
export STUB_MODE=empty

# ---------------------------------------------------------------- tests

bash -n "$MAIN" && ok "bash -n" || bad "bash -n"

# 1. 全部 OK（owned MPU + 无 token 问题的 DB + 空 stub）
out=$("$MAIN" --data-dir "$TMP/data-a" --control-db "$TMP/good.db" \
	--replica-reconcile "$STUB" --replica-scope b/k 2>&1)
rc=$?
assert_rc "clean: rc=0" 0 "$rc"
assert_has "clean: mpu OK" "$out" "[OK] bare-uuid-mpu"
assert_has "clean: m6 OK" "$out" "[OK] m6-site-token"
assert_has "clean: m7 OK" "$out" "[OK] m7-unmapped-replica"
out=$("$MAIN" --data-dir "$TMP/data-a" --control-db "$TMP/good.db" \
	--replica-reconcile "$STUB" --replica-scope b/k --health-json "$TMP/health-good.json" --strict 2>&1)
assert_rc "clean + strict: rc=0" 0 "$?"

# 2. 裸 UUID / token NULL / 未映射 REPLICA → WARN；strict 下非零
out=$(STUB_MODE=rows "$MAIN" --data-dir "$TMP/data-b" --control-db "$TMP/bad.db" \
	--replica-reconcile "$STUB" --replica-scope b/k 2>&1)
rc=$?
assert_rc "warn: default rc=0" 0 "$rc"
assert_has "warn: mpu WARN" "$out" "[WARN] bare-uuid-mpu"
assert_has "warn: mpu legacy path listed" "$out" "44444444-4444-4444-8444-444444444444"
assert_has "warn: mpu corrupt listed" "$out" "55555555-5555-4555-8555-555555555555"
assert_has "warn: m6 WARN" "$out" "[WARN] m6-site-token"
assert_has "warn: m6 empty token listed" "$out" "p2"
assert_has "warn: m7 WARN" "$out" "[WARN] m7-unmapped-replica"
assert_has "warn: m7 ids" "$out" "v1,v2"
out=$(STUB_MODE=rows "$MAIN" --data-dir "$TMP/data-b" --control-db "$TMP/bad.db" \
	--replica-reconcile "$STUB" --replica-scope b/k --strict 2>&1)
assert_rc "warn + strict: rc=1" 1 "$?"

# 3. stub 调用形状：只读 list，绝无 --delete/--apply
stub_args=$(cat "$TMP/stub.log" 2>/dev/null || true)
assert_has "stub: got bucket" "$stub_args" "--bucket b"
assert_has "stub: got key" "$stub_args" "--key k"
assert_has "stub: got json" "$stub_args" "--json"
assert_not_has "stub: no --delete" "$stub_args" "--delete"
assert_not_has "stub: no --apply" "$stub_args" "--apply"

# 4. sites 表无 site_token 列 → 无法判定（WARN），不能是 OK
out=$("$MAIN" --data-dir "$TMP/data-a" --control-db "$TMP/nocol.db" \
	--replica-reconcile "$STUB" --replica-scope b/k 2>&1)
assert_has "no column: WARN 无法判定" "$out" "[WARN] m6-site-token"
assert_has "no column: explains why" "$out" "无法判定"
assert_not_has "no column: not OK" "$out" "[OK] m6-site-token"

# 5. sites 表不存在 → 无法判定（WARN）
out=$("$MAIN" --data-dir "$TMP/data-a" --control-db "$TMP/notable.db" \
	--replica-reconcile "$STUB" 2>&1)
assert_has "no table: WARN" "$out" "[WARN] m6-site-token"

# 6. 不提供 config / control-db / replica-scope → 两项 WARN（无法判定），不是 OK
out=$("$MAIN" --data-dir "$TMP/data-a" 2>&1)
rc=$?
assert_rc "no optional inputs: rc=0" 0 "$rc"
assert_has "no config: WARN" "$out" "[WARN] m6-site-token"
assert_has "no config: mentions --config" "$out" "未提供 --config"
assert_not_has "no config: not OK" "$out" "[OK] m6-site-token"
assert_has "no scope: WARN" "$out" "[WARN] m7-unmapped-replica"

# 7. JSON 输出可解析，状态与退出码一致
STUB_MODE=rows "$MAIN" --data-dir "$TMP/data-b" --control-db "$TMP/bad.db" \
	--replica-reconcile "$STUB" --replica-scope b/k --json >"$TMP/out.json" 2>&1
rc=$?
assert_rc "json: rc=0" 0 "$rc"
assert_has "json: mpu WARN" "$(json_status "$TMP/out.json" bare-uuid-mpu)" "WARN"
assert_has "json: data plane WARN" "$(json_status "$TMP/out.json" data_plane_floor)" "WARN"
assert_has "json: m6 WARN" "$(json_status "$TMP/out.json" m6-site-token)" "WARN"
assert_has "json: m7 WARN" "$(json_status "$TMP/out.json" m7-unmapped-replica)" "WARN"
python3 - "$TMP/out.json" <<'PY' && ok "json: exit_code/status fields" || bad "json: exit_code/status fields"
import json
import sys
d = json.load(open(sys.argv[1], encoding='utf-8'))
assert d['status'] == 'WARN' and d['exit_code'] == 0 and d['strict'] is False
assert [c['name'] for c in d['checks']] == ['bare-uuid-mpu', 'data_plane_floor', 'm6-site-token', 'm7-unmapped-replica']
PY

# 8. 含 --delete 的命令模板 → BLOCK（且不执行）
rm -f "$TMP/stub.log"
out=$("$MAIN" --data-dir "$TMP/data-a" --control-db "$TMP/good.db" \
	--replica-reconcile "$STUB --delete x" --replica-scope b/k 2>&1)
rc=$?
assert_rc "destructive template: rc=1" 1 "$rc"
assert_has "destructive template: BLOCK" "$out" "[BLOCK] m7-unmapped-replica"
[ ! -f "$TMP/stub.log" ] && ok "destructive template: stub not invoked" || bad "destructive template: stub invoked"

# 9. 缺失 data-dir → BLOCK
out=$("$MAIN" --data-dir "$TMP/nope" --control-db "$TMP/good.db" 2>&1)
assert_rc "missing data-dir: rc=1" 1 "$?"
assert_has "missing data-dir: BLOCK" "$out" "[BLOCK] bare-uuid-mpu"

# 10. 逗号分隔的多个 data-dir：第二棵树的裸 UUID 也要扫到
out=$("$MAIN" --data-dir "$TMP/data-a,$TMP/data-b" --control-db "$TMP/good.db" 2>&1)
assert_has "comma data-dirs: second root scanned" "$out" "44444444-4444-4444-8444-444444444444"

# 11. 用法错误 → 2
"$MAIN" >/dev/null 2>&1
assert_rc "no args: rc=2" 2 "$?"
"$MAIN" --data-dir "$TMP/data-a" --bogus >/dev/null 2>&1
assert_rc "unknown arg: rc=2" 2 "$?"

# 12. --config：远端 token 齐全 → M6 OK；配置优先于 sites 无 site_token 列
out=$("$MAIN" --data-dir "$TMP/data-a" --config "$TMP/config-good.toml" 2>&1)
rc=$?
assert_rc "config good: rc=0" 0 "$rc"
assert_has "config good: m6 OK" "$out" "[OK] m6-site-token"
assert_has "config good: remote count" "$out" "2 个 oss.toml 远端"
assert_not_has "config good: no token value" "$out" "fixture-token-a"
assert_not_has "config good: no second token" "$out" "fixture-token-b"
assert_not_has "config good: no access_key" "$out" "fixture-access-a"
assert_not_has "config good: no secret_key" "$out" "fixture-secret-a"
out=$("$MAIN" --data-dir "$TMP/data-a" --config "$TMP/config-good.toml" \
	--control-db "$TMP/nocol.db" 2>&1)
assert_has "config good: beats no-column" "$out" "[OK] m6-site-token"
assert_not_has "config good: no m6 undetermined" "$out" "[WARN] m6-site-token"

# 13. --config：缺/空 site_token → WARN，只列 id/endpoint，不泄露 token/密钥；strict=1
out=$("$MAIN" --data-dir "$TMP/data-a" --config "$TMP/config-missing.toml" 2>&1)
rc=$?
assert_rc "config missing token: default rc=0" 0 "$rc"
assert_has "config missing token: WARN" "$out" "[WARN] m6-site-token"
assert_has "config missing token: count" "$out" "2/3 个 oss.toml 远端"
assert_has "config missing token: lists r2" "$out" "id=r2 endpoint=https://peer2:9000"
assert_has "config missing token: lists blank r3" "$out" "id=r3 endpoint=https://peer3:9000"
assert_not_has "config missing token: no token value" "$out" "fixture-token-a"
assert_not_has "config missing token: no access_key" "$out" "fixture-access-a"
assert_not_has "config missing token: no secret_key" "$out" "fixture-secret-a"
"$MAIN" --data-dir "$TMP/data-a" --config "$TMP/config-missing.toml" --strict >/dev/null 2>&1
assert_rc "config missing token: strict rc=1" 1 "$?"

# 14. --config 文件缺失 / 解析失败 → BLOCK（fail-closed）
out=$("$MAIN" --data-dir "$TMP/data-a" --config "$TMP/no-such.toml" 2>&1)
assert_rc "config missing file: rc=1" 1 "$?"
assert_has "config missing file: BLOCK" "$out" "[BLOCK] m6-site-token"
assert_has "config missing file: explains" "$out" "缺失或无法解析"
out=$("$MAIN" --data-dir "$TMP/data-a" --config "$TMP/config-syntax.toml" 2>&1)
assert_rc "config bad syntax: rc=1" 1 "$?"
assert_has "config bad syntax: BLOCK" "$out" "[BLOCK] m6-site-token"
assert_has "config bad syntax: parse detail" "$out" "读取/解析失败"
assert_not_has "config bad syntax: no false OK" "$out" "[OK] m6-site-token"

# 15. --health-json：本地与所有 up 节点都 >=2 且兼容 → OK；干净输入 + strict rc=0
out=$("$MAIN" --data-dir "$TMP/data-empty" --config "$TMP/config-good.toml" \
	--replica-reconcile "$STUB" --replica-scope b/k --health-json "$TMP/health-good.json" 2>&1)
rc=$?
assert_rc "health good: rc=0" 0 "$rc"
assert_has "health good: OK" "$out" "[OK] data_plane_floor"
assert_has "health good: local floor" "$out" "本地 data_wire_version=2"
assert_has "health good: up nodes counted" "$out" "2 个 up 节点均 >=2"
assert_not_has "health good: not WARN" "$out" "[WARN] data_plane_floor"
"$MAIN" --data-dir "$TMP/data-empty" --config "$TMP/config-good.toml" \
	--replica-reconcile "$STUB" --replica-scope b/k --health-json "$TMP/health-good.json" --strict >/dev/null 2>&1
assert_rc "health good + strict: rc=0" 0 "$?"

# 16. 旧 peer（up 且 wire=1）→ BLOCK，详情点名节点
out=$("$MAIN" --data-dir "$TMP/data-empty" --health-json "$TMP/health-old-peer.json" 2>&1)
assert_rc "health old peer: rc=1" 1 "$?"
assert_has "health old peer: BLOCK" "$out" "[BLOCK] data_plane_floor"
assert_has "health old peer: names n2" "$out" "up 节点 n2 data_wire_version=1 < 2"
"$MAIN" --data-dir "$TMP/data-empty" --health-json "$TMP/health-old-peer.json" --json >"$TMP/dp.json" 2>&1
assert_has "health old peer: json BLOCK" "$(json_status "$TMP/dp.json" data_plane_floor)" "BLOCK"

# 17. up 节点 wire=null（down 节点的 null 不下结论）→ BLOCK
out=$("$MAIN" --data-dir "$TMP/data-empty" --health-json "$TMP/health-null-peer.json" 2>&1)
assert_rc "health null peer: rc=1" 1 "$?"
assert_has "health null peer: BLOCK" "$out" "[BLOCK] data_plane_floor"
assert_has "health null peer: names null node" "$out" "up 节点 n2 data_wire_version 缺失/null"
assert_not_has "health null peer: no false OK" "$out" "[OK] data_plane_floor"

# 18. data_plane_compatible != true → BLOCK，详情带 incompatible_peers
out=$("$MAIN" --data-dir "$TMP/data-empty" --health-json "$TMP/health-incompatible.json" 2>&1)
assert_rc "health incompatible: rc=1" 1 "$?"
assert_has "health incompatible: BLOCK" "$out" "[BLOCK] data_plane_floor"
assert_has "health incompatible: compat detail" "$out" "data_plane_compatible 不是 true"
assert_has "health incompatible: peer list" "$out" "n2(v1)"

# 19. 本地 data_wire_version < 2 → BLOCK
out=$("$MAIN" --data-dir "$TMP/data-empty" --health-json "$TMP/health-local-old.json" 2>&1)
assert_rc "health local old: rc=1" 1 "$?"
assert_has "health local old: BLOCK" "$out" "[BLOCK] data_plane_floor"
assert_has "health local old: local detail" "$out" "本地 data_wire_version=1 < 2"

# 20. health-json 文件缺失/坏 JSON → BLOCK（fail-closed）
out=$("$MAIN" --data-dir "$TMP/data-empty" --health-json "$TMP/no-such-health.json" 2>&1)
assert_rc "health missing file: rc=1" 1 "$?"
assert_has "health missing file: BLOCK" "$out" "[BLOCK] data_plane_floor"
assert_has "health missing file: explains" "$out" "缺失或无法解析"
out=$("$MAIN" --data-dir "$TMP/data-empty" --health-json "$TMP/health-bad.json" 2>&1)
assert_rc "health bad json: rc=1" 1 "$?"
assert_has "health bad json: BLOCK" "$out" "[BLOCK] data_plane_floor"
assert_has "health bad json: parse detail" "$out" "读取/解析"
assert_not_has "health bad json: no false OK" "$out" "[OK] data_plane_floor"

# 21. 不提供 --health-json → WARN“无法判定”；strict 下非零
out=$("$MAIN" --data-dir "$TMP/data-empty" --config "$TMP/config-good.toml" \
	--replica-reconcile "$STUB" --replica-scope b/k 2>&1)
assert_rc "no health-json: rc=0" 0 "$?"
assert_has "no health-json: WARN" "$out" "[WARN] data_plane_floor"
assert_has "no health-json: explains" "$out" "无法判定；升级前请保存 /health/cluster"
assert_not_has "no health-json: not OK" "$out" "[OK] data_plane_floor"
"$MAIN" --data-dir "$TMP/data-empty" --config "$TMP/config-good.toml" \
	--replica-reconcile "$STUB" --replica-scope b/k --strict >/dev/null 2>&1
assert_rc "no health-json + strict: rc=1" 1 "$?"

# 22. --require-no-mpu：带属主的活 MPU 也 BLOCK；不加标志保持原语义
out=$("$MAIN" --data-dir "$TMP/data-a" --require-no-mpu --config "$TMP/config-good.toml" \
	--replica-reconcile "$STUB" --replica-scope b/k 2>&1)
assert_rc "require-no-mpu owned: rc=1" 1 "$?"
assert_has "require-no-mpu owned: BLOCK" "$out" "[BLOCK] bare-uuid-mpu"
assert_has "require-no-mpu owned: count" "$out" "1 个在途 MPU"
assert_has "require-no-mpu owned: drain" "$out" "Complete/Abort 排空"
assert_has "require-no-mpu owned: path listed" "$out" "11111111-1111-4111-8111-111111111111"
out=$("$MAIN" --data-dir "$TMP/data-a" --config "$TMP/config-good.toml" \
	--replica-reconcile "$STUB" --replica-scope b/k 2>&1)
assert_has "owned MPU without flag: not BLOCK" "$out" "[OK] bare-uuid-mpu"
out=$("$MAIN" --data-dir "$TMP/data-empty" --require-no-mpu --config "$TMP/config-good.toml" \
	--replica-reconcile "$STUB" --replica-scope b/k 2>&1)
assert_rc "require-no-mpu empty: rc=0" 0 "$?"
assert_has "require-no-mpu empty: OK" "$out" "[OK] bare-uuid-mpu"

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
((FAIL == 0))
