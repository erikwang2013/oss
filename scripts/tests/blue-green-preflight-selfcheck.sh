#!/usr/bin/env bash
# scripts/blue-green-preflight.sh 的自检：临时目录 + stub curl / 新二进制 / replica 命令。
# 不依赖 cargo / 网络 / 真实 oss-server。

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
MAIN="$HERE/../blue-green-preflight.sh"
TMP=$(mktemp -d "${TMPDIR:-/tmp}/oss-blue-green-test.XXXXXX") || exit 1
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

DATA_GOOD="$TMP/data-good"
mkdir -p "$DATA_GOOD"

# 带属主的活 MPU：--require-no-mpu 必须 BLOCK（msgpack map{owner: "node-1"}）。
DATA_MPU="$TMP/data-mpu"
MPU_UID=11111111-1111-4111-8111-111111111111
mkdir -p "$DATA_MPU/bucket/obj__XLDIR__x/$MPU_UID"
printf '\x81\xa5owner\xa6node-1' >"$DATA_MPU/bucket/obj__XLDIR__x/$MPU_UID/mpu.meta"

cat >"$TMP/health-good.json" <<'JSON'
{"node_id":"n1","data_wire_version":2,"data_plane_compatible":true,
 "degraded_peers":[],"incompatible_peers":[],
 "nodes":[{"id":"n1","up":true,"data_wire_version":2},
          {"id":"n3","up":false,"data_wire_version":null}]}
JSON
cat >"$TMP/health-old-peer.json" <<'JSON'
{"node_id":"n1","data_wire_version":2,"data_plane_compatible":true,
 "degraded_peers":["n2(v1)"],"incompatible_peers":[],
 "nodes":[{"id":"n1","up":true,"data_wire_version":2},
          {"id":"n2","up":true,"data_wire_version":1}]}
JSON
cat >"$TMP/config-good.toml" <<'TOML'
[[remotes]]
id = "r1"
endpoint = "https://peer1:9000"
site_token = "fixture-token"
TOML

printf 'cold-backup\n' >"$TMP/backup.bin"
mkdir -p "$TMP/backup-empty"

mkdir -p "$TMP/bin"
cat >"$TMP/bin/curl" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_CURL_LOG:?}"
# 模拟 curl -w '%{http_code}'：只在 stdout 写状态码；连接失败为 000 + 非零退出。
case "${STUB_CURL_MODE:-down}" in
up) printf '200'; exit 0 ;;
http503) printf '503'; exit 0 ;;
*) printf '000'; exit 7 ;;
esac
SH
cat >"$TMP/bin/new-old" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_BIN_LOG:?}"
printf 'oss-server 1.15.0\n'
SH
cat >"$TMP/bin/new-bad" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_BIN_LOG:?}"
printf 'oss-server development\n'
SH
cat >"$TMP/bin/new-good" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_BIN_LOG:?}"
printf 'oss-server 1.27.0\n'
SH
cat >"$TMP/bin/replica-stub" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_REPL_LOG:?}"
printf '[]\n'
SH
chmod +x "$TMP/bin/curl" "$TMP/bin/new-old" "$TMP/bin/new-bad" "$TMP/bin/new-good" "$TMP/bin/replica-stub"
export PATH="$TMP/bin:$PATH"
export STUB_CURL_LOG="$TMP/curl.log"
export STUB_BIN_LOG="$TMP/bin.log"
export STUB_REPL_LOG="$TMP/repl.log"
export STUB_CURL_MODE=down

# 让委派 preflight 全 OK 的公共参数（M6/M7/数据面/MPU 都通过）。
FULL=(
	--data-dir "$DATA_GOOD"
	--backup "$TMP/backup.bin"
	--config "$TMP/config-good.toml"
	--health-json "$TMP/health-good.json"
	--replica-reconcile "$TMP/bin/replica-stub"
	--replica-scope b/k
)

# ---------------------------------------------------------------- tests

bash -n "$MAIN" && ok "bash -n" || bad "bash -n"

# 1. 用法错误 → 2
"$MAIN" >/dev/null 2>&1
assert_rc "no args: rc=2" 2 "$?"
"$MAIN" --data-dir "$DATA_GOOD" >/dev/null 2>&1
assert_rc "missing --backup: rc=2" 2 "$?"
"$MAIN" --data-dir "$DATA_GOOD" --backup "$TMP/backup.bin" --min-version 1.16 >/dev/null 2>&1
assert_rc "bad min-version: rc=2" 2 "$?"
"$MAIN" --data-dir "$DATA_GOOD" --backup "$TMP/backup.bin" --health-url ftp://x >/dev/null 2>&1
assert_rc "bad health-url scheme: rc=2" 2 "$?"

# 2. 备份缺失 / 空目录 → BLOCK
out=$("$MAIN" "${FULL[@]}" --backup "$TMP/no-such-backup" 2>&1)
assert_rc "missing backup: rc=1" 1 "$?"
assert_has "missing backup: BLOCK" "$out" "[BLOCK] backup_ready"
assert_has "missing backup: explains" "$out" "不存在"
out=$("$MAIN" "${FULL[@]}" --backup "$TMP/backup-empty" 2>&1)
assert_rc "empty backup dir: rc=1" 1 "$?"
assert_has "empty backup dir: BLOCK" "$out" "[BLOCK] backup_ready"
assert_has "empty backup dir: explains" "$out" "为空"

# 3. 旧服务仍响应 → BLOCK（stub curl 固定 2xx/3xx 语义 = exit 0）
rm -f "$TMP/curl.log"
out=$(STUB_CURL_MODE=up "$MAIN" "${FULL[@]}" --health-url "http://127.0.0.1:9/health/cluster" 2>&1)
assert_rc "service running: rc=1" 1 "$?"
assert_has "service running: BLOCK" "$out" "[BLOCK] service_stopped"
assert_has "service running: message" "$out" "service still running"
curl_args=$(cat "$TMP/curl.log" 2>/dev/null || true)
assert_has "service running: max-time" "$curl_args" "--max-time 3"
assert_has "service running: silent show-errors" "$curl_args" "-sS"
assert_not_has "service running: no fail flag" "$curl_args" "-fsS"
assert_has "service running: status format" "$curl_args" "http_code"
assert_has "service running: discard body" "$curl_args" "-o /dev/null"
assert_has "service running: url" "$curl_args" "http://127.0.0.1:9/health/cluster"

# 3b. 旧服务返回 503（未 ready）仍是“在跑” → BLOCK（不能因 -f 的 22 误判为已停止）
out=$(STUB_CURL_MODE=http503 "$MAIN" "${FULL[@]}" --health-url "http://127.0.0.1:9/health/ready" 2>&1)
assert_rc "service 503: rc=1" 1 "$?"
assert_has "service 503: BLOCK" "$out" "[BLOCK] service_stopped"
assert_has "service 503: status shown" "$out" "HTTP 503"
assert_not_has "service 503: not reported stopped" "$out" "旧服务无响应"

# 4. 全绿：备份 + preflight + 旧服务无响应 + 新版本 → READY，打印运行手册
out=$(STUB_CURL_MODE=down "$MAIN" "${FULL[@]}" \
	--health-url "http://127.0.0.1:9/health/cluster" --new-binary "$TMP/bin/new-good" 2>&1)
assert_rc "ready: rc=0" 0 "$?"
assert_has "ready: preflight OK" "$out" "[OK] upgrade_preflight"
assert_has "ready: backup OK" "$out" "[OK] backup_ready"
assert_has "ready: service stopped OK" "$out" "[OK] service_stopped"
assert_has "ready: new binary OK" "$out" "[OK] new_binary_version"
assert_has "ready: overall READY" "$out" "整体: READY"
assert_has "ready: runbook" "$out" "运行手册"
assert_has "ready: rollback" "$out" "回滚"
assert_has "ready: rollback invalid after CMK" "$out" "CMK 变更"
assert_has "new binary: --version invoked" "$(cat "$TMP/bin.log")" "--version"
assert_has "replica: scope passed through" "$(cat "$TMP/repl.log")" "--bucket b --key k"

# 5. 健康夹具（有备份、无 health-url、无 new-binary）→ rc=0（WARN 允许）
out=$("$MAIN" "${FULL[@]}" 2>&1)
assert_rc "healthy no optional: rc=0" 0 "$?"
assert_has "healthy no optional: preflight OK" "$out" "[OK] upgrade_preflight"
assert_has "healthy no optional: service WARN" "$out" "[WARN] service_stopped"
assert_has "healthy no optional: binary WARN" "$out" "[WARN] new_binary_version"
assert_has "healthy no optional: overall WARN" "$out" "整体: WARN"
assert_has "healthy no optional: runbook printed" "$out" "运行手册"

# 6. 同样输入 + --strict：WARN 非零，且不打印运行手册
out=$("$MAIN" "${FULL[@]}" --strict 2>&1)
assert_rc "healthy no optional + strict: rc=1" 1 "$?"
assert_has "healthy no optional + strict: WARN" "$out" "[WARN] service_stopped"
assert_not_has "healthy no optional + strict: no runbook" "$out" "运行手册"

# 7. 新二进制过旧 / 不可解析 / 低于 --min-version → BLOCK
out=$(STUB_CURL_MODE=down "$MAIN" "${FULL[@]}" \
	--health-url "http://127.0.0.1:9/health/cluster" --new-binary "$TMP/bin/new-old" 2>&1)
assert_rc "new binary old: rc=1" 1 "$?"
assert_has "new binary old: BLOCK" "$out" "[BLOCK] new_binary_version"
assert_has "new binary old: version shown" "$out" "1.15.0"
out=$(STUB_CURL_MODE=down "$MAIN" "${FULL[@]}" \
	--health-url "http://127.0.0.1:9/health/cluster" --new-binary "$TMP/bin/new-bad" 2>&1)
assert_rc "new binary unparseable: rc=1" 1 "$?"
assert_has "new binary unparseable: BLOCK" "$out" "[BLOCK] new_binary_version"
assert_has "new binary unparseable: explains" "$out" "不可解析"
out=$(STUB_CURL_MODE=down "$MAIN" "${FULL[@]}" \
	--health-url "http://127.0.0.1:9/health/cluster" --new-binary "$TMP/bin/new-good" \
	--min-version 1.28.0 2>&1)
assert_rc "new binary below min: rc=1" 1 "$?"
assert_has "new binary below min: BLOCK" "$out" "[BLOCK] new_binary_version"
assert_has "new binary below min: min shown" "$out" "1.28.0"
out=$("$MAIN" "${FULL[@]}" --new-binary "$TMP/no-such-binary" 2>&1)
assert_rc "new binary missing: rc=1" 1 "$?"
assert_has "new binary missing: BLOCK" "$out" "[BLOCK] new_binary_version"

# 8. 委派 preflight 的 BLOCK 要上浮：--require-no-mpu 遇到带属主的活 MPU
out=$("$MAIN" --data-dir "$DATA_MPU" --backup "$TMP/backup.bin" \
	--config "$TMP/config-good.toml" --health-json "$TMP/health-good.json" \
	--replica-reconcile "$TMP/bin/replica-stub" --replica-scope b/k 2>&1)
assert_rc "delegated MPU block: rc=1" 1 "$?"
assert_has "delegated MPU block: BLOCK" "$out" "[BLOCK] upgrade_preflight"
assert_has "delegated MPU block: mpu row" "$out" "[BLOCK] bare-uuid-mpu"
assert_has "delegated MPU block: drain message" "$out" "在途 MPU"
assert_has "delegated MPU block: path listed" "$out" "$MPU_UID"

# 9. 委派 preflight 的 BLOCK 要上浮：假 data-dir / 旧 peer 数据面
out=$("$MAIN" --data-dir "$TMP/no-such-dir" --backup "$TMP/backup.bin" \
	--health-json "$TMP/health-good.json" 2>&1)
assert_rc "delegated fake data-dir: rc=1" 1 "$?"
assert_has "delegated fake data-dir: BLOCK" "$out" "[BLOCK] upgrade_preflight"
assert_has "delegated fake data-dir: mpu row" "$out" "[BLOCK] bare-uuid-mpu"
out=$("$MAIN" "${FULL[@]}" --health-json "$TMP/health-old-peer.json" 2>&1)
assert_rc "delegated old peer: rc=1" 1 "$?"
assert_has "delegated old peer: BLOCK" "$out" "[BLOCK] upgrade_preflight"
assert_has "delegated old peer: floor row" "$out" "[BLOCK] data_plane_floor"
assert_has "delegated old peer: names node" "$out" "up 节点 n2"

# 10. 委派 preflight 只有 WARN（缺 health-json）→ 本脚本 WARN，非 strict 仍 0
out=$("$MAIN" "${FULL[@]}" --health-json "" 2>&1)
assert_rc "delegated warn-only: rc=0" 0 "$?"
assert_has "delegated warn-only: preflight WARN" "$out" "[WARN] upgrade_preflight"
assert_has "delegated warn-only: health hint" "$out" "升级前请保存 /health/cluster"

# 11. JSON 输出：字段/状态/检查顺序与退出码一致
STUB_CURL_MODE=down "$MAIN" "${FULL[@]}" \
	--health-url "http://127.0.0.1:9/health/cluster" --new-binary "$TMP/bin/new-good" \
	--json >"$TMP/out.json" 2>&1
assert_rc "json: rc=0" 0 "$?"
assert_has "json: preflight OK" "$(json_status "$TMP/out.json" upgrade_preflight)" "OK"
assert_has "json: backup OK" "$(json_status "$TMP/out.json" backup_ready)" "OK"
assert_has "json: service OK" "$(json_status "$TMP/out.json" service_stopped)" "OK"
assert_has "json: binary OK" "$(json_status "$TMP/out.json" new_binary_version)" "OK"
python3 - "$TMP/out.json" <<'PY' && ok "json: status/exit_code/check order" || bad "json: status/exit_code/check order"
import json
import sys
d = json.load(open(sys.argv[1], encoding='utf-8'))
assert d['tool'] == 'blue-green-preflight'
assert d['status'] == 'READY' and d['exit_code'] == 0 and d['strict'] is False
assert [c['name'] for c in d['checks']] == [
    'upgrade_preflight', 'backup_ready', 'service_stopped', 'new_binary_version']
assert any('data_plane_floor' in x for x in d['checks'][0]['details'])
PY
"$MAIN" "${FULL[@]}" --strict --json >"$TMP/out-warn.json" 2>&1
assert_rc "json strict warn: rc=1" 1 "$?"
python3 - "$TMP/out-warn.json" <<'PY' && ok "json strict warn: WARN/exit_code=1" || bad "json strict warn: WARN/exit_code=1"
import json
import sys
d = json.load(open(sys.argv[1], encoding='utf-8'))
assert d['status'] == 'WARN' and d['exit_code'] == 1 and d['strict'] is True
PY

printf '\n%d passed, %d failed\n' "$PASS" "$FAIL"
((FAIL == 0))
