#!/usr/bin/env bash
# M3 shrink 4→3 真进程 loopback e2e（spec §14.3 缺口：此前只有 in-process store 级 4→3）。
#
# 拓扑：4 节点 × 3 盘 = 12 槽（EC 8+4，positional，epoch=1）。缩容后 3 节点仍是 12 槽，
# 槽 9/10/11 改由 n1/n2/n3 的 drive 0 承载（显式 [[cluster.slots]]，epoch=2）。旧配置不带
# 显式槽表（positional），新配置带；拓扑 hash 由 ClusterSets 计算，plan 生成在 n1。
#
# 覆盖：4 节点建桶写入（EC 300 KiB / inline / 双版本）→ 全节点 maintenance 重启 →
# plan → 退役节点拒绝 apply/commit → 逐节点 apply+verify（停本节点服务、peers 保持）→
# commit 前 rollback 演练（目标副本按 CRC 删除后重新 apply+verify）→ 逐节点 commit 写
# topo v4（服务保持运行使 peers 仍见旧 caps）→ 停 4 节点、3 保留节点用新配置启动 →
# 桶/对象/版本读回一致 + /api/v1/cluster/status 报 v4 → 旧配置启动保留节点失败 →
# 退役节点以旧配置无法加入新集群。
#
# 运行要求：每节点 CLI 执行期间该节点 serve 必须停止（peers 保持运行）；commit 例外——
# 实现要求「全部节点仍在旧拓扑上 commit 完」才允许换新配置重启，故 commit 在 serve
# 运行中执行（进程内 caps 仍是旧值，peers 的健康校验仍通过）。
#
# 前置：已构建 oss-server（OSS_BIN 或 target/debug/oss-server）、curl >= 7.75（--aws-sigv4）、
# python3。用法：bash tests/e2e_m3_shrink.sh
set -uo pipefail
cd "$(dirname "$0")/.."

BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
[ -x "$BIN" ] || { echo "缺少 oss-server：$BIN（先 cargo build -p oss-server，或设 OSS_BIN）"; exit 2; }
BIN=$(realpath "$BIN")
for c in curl python3; do command -v "$c" >/dev/null || { echo "需要 $c"; exit 2; }; done
curl --help all 2>/dev/null | grep -q aws-sigv4 || { echo "需要 curl >= 7.75（--aws-sigv4）"; exit 2; }

PASSES=0; FAILS=0; SKIPS=0
pass() { PASSES=$((PASSES+1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS+1)); echo "FAIL: $*"; }
skip() { SKIPS=$((SKIPS+1)); echo "SKIP: $*"; }
ck() { local m=$*; if "$@"; then pass "$m"; else fail "$m"; fi; }
ckm() { local m=$1; shift; if "$@"; then pass "$m"; else fail "$m"; fi; }
finish() {
  echo "== 结果 =="; echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS"
  [ "$FAILS" -ne 0 ] && exit 1
  echo "PASS: M3 shrink 4→3 loopback e2e（$PASSES 项通过）"
  exit 0
}

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
USED=""
pick() { local p; while :; do p=$(free_port); case " $USED " in *" $p "*) continue ;; esac; break; done; USED="$USED $p"; echo "$p"; }
IDS=(n1 n2 n3 n4)
declare -a HTTPP GRPCP PIDS
for i in 0 1 2 3; do HTTPP+=("$(pick)"); GRPCP+=("$(pick)"); PIDS+=(""); done
N1="http://127.0.0.1:${HTTPP[0]}"; N2="http://127.0.0.1:${HTTPP[1]}"
N3="http://127.0.0.1:${HTTPP[2]}"; N4="http://127.0.0.1:${HTTPP[3]}"

W=$(mktemp -d)
JWT_SECRET="shrink-e2e-jwt-0123456789abcdef01234567"
CLUSTER_TOKEN=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
ADMIN="shrinkadmin"; APW="shrinkadmin-pw"
BUCKET="shrink-$RANDOM"
cleanup() {
  local i p
  if declare -F stop_node >/dev/null; then
    for i in 0 1 2 3; do stop_node "$i" 2>/dev/null; done
  else
    for p in "${PIDS[@]}"; do [ -n "$p" ] && kill -9 "$p" 2>/dev/null; done
  fi
  # 失败时保留现场（server.log / 拓扑文件）便于排查。
  if [ "${FAILS:-0}" -eq 0 ] && [ "${KEEP:-0}" != 1 ]; then rm -rf "$W"; else echo "E2E 现场保留: $W" >&2; fi
}
trap cleanup EXIT

mk_old_cfg() { # dir-index maintenance
  local i=$1 mt=$2 d="$W/${IDS[$1]}"; mkdir -p "$d/d0" "$d/d1" "$d/d2" "$d/staging"
  {
    cat <<EOF
listen = "127.0.0.1:${HTTPP[$i]}"
data_dir = "$d/d0"
data_dirs = ["$d/d0", "$d/d1", "$d/d2"]
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456
groups = []
maintenance = $mt

[ec]
enabled = true
data = 8
parity = 4
inline_threshold = 131072

[cluster]
node_id = "${IDS[$i]}"
secret = "$CLUSTER_TOKEN"
EOF
    local j
    for j in 0 1 2 3; do
      cat <<EOF

[[cluster.nodes]]
id = "${IDS[$j]}"
addr = "127.0.0.1:${GRPCP[$j]}"
drive_count = 3
EOF
    done
  } > "$d/$([ "$mt" = true ] && echo maint.toml || echo old.toml)"
}

mk_new_cfg() { # dir-index（仅保留 n1..n3；epoch=2；槽 9/10/11 = n1/n2/n3 drive 0）
  local i=$1 d="$W/${IDS[$1]}"
  {
    cat <<EOF
listen = "127.0.0.1:${HTTPP[$i]}"
data_dir = "$d/d0"
data_dirs = ["$d/d0", "$d/d1", "$d/d2"]
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456
groups = []
maintenance = false

[ec]
enabled = true
data = 8
parity = 4
inline_threshold = 131072

[cluster]
node_id = "${IDS[$i]}"
secret = "$CLUSTER_TOKEN"
epoch = 2
EOF
    local j
    for j in 0 1 2; do
      cat <<EOF

[[cluster.nodes]]
id = "${IDS[$j]}"
addr = "127.0.0.1:${GRPCP[$j]}"
drive_count = 3
EOF
    done
    for j in 0 1 2; do
      cat <<EOF

[[cluster.slots]]
node = "${IDS[$j]}"
drive = 0

[[cluster.slots]]
node = "${IDS[$j]}"
drive = 1

[[cluster.slots]]
node = "${IDS[$j]}"
drive = 2
EOF
    done
    for j in 0 1 2; do
      cat <<EOF

[[cluster.slots]]
node = "${IDS[$j]}"
drive = 0
EOF
    done
  } > "$d/new.toml"
}

wait_ready() { local i url; i=$1; url="http://127.0.0.1:${HTTPP[$i]}"; for _ in $(seq 1 90); do
    kill -0 "${PIDS[$i]}" 2>/dev/null || return 1
    [ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "$url/health/live" 2>/dev/null || true)" = 200 ] && return 0
    sleep 1
  done; return 1; }
start_node() { # dir-index cfg-file
  # local 多赋值会在赋值前展开 RHS（`${IDS[$i]}` 会用调用者的 i）——必须分行赋。
  local i cfg d
  i=$1; cfg=$2; d="$W/${IDS[$i]}"
  ( cd "$d" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" \
      exec "$BIN" --config "$d/$cfg" serve >>"$d/server.log" 2>&1 ) &
  PIDS[$i]=$!
}
stop_node() { # dir-index
  local i p j
  i=$1; p=${PIDS[$i]}
  [ -z "$p" ] && return 0
  kill -TERM "$p" 2>/dev/null
  for j in $(seq 1 40); do kill -0 "$p" 2>/dev/null || { wait "$p" 2>/dev/null; PIDS[$i]=""; return 0; }; sleep 0.25; done
  kill -9 "$p" 2>/dev/null; wait "$p" 2>/dev/null; PIDS[$i]=""; return 1; }
stop_all() { local i; for i in 0 1 2 3; do stop_node "$i"; done; }
show_log() { local i=$1; echo "---- ${IDS[$i]}/server.log (tail) ----"; tail -15 "$W/${IDS[$i]}/server.log" 2>/dev/null; }

# 运维 CLI：本节点服务已停（commit 例外，见文件头）。
cli() { local i=$1 cfg=$2; shift 2
  OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" "$BIN" --config "$W/${IDS[$i]}/$cfg" "$@"; }

login() { curl -s --max-time 10 -X POST "$1/api/v1/login" -H 'content-type: application/json' \
  -d "{\"username\":\"$ADMIN\",\"password\":\"$APW\"}" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'; }
hdr() { tr -d '\r' < "$2" | awk -F': ' -v k="$1" 'tolower($1)==tolower(k){print $2; exit}'; }
enc() { python3 -c 'import sys,urllib.parse;print(urllib.parse.quote(sys.argv[1],safe=""))' "$1"; }
# ecat_logging 的 INFO 也写 stdout（非 stderr），--json 输出前会带日志行；截取首个行首 "{" 起。
json_clean() { python3 - "$1" <<'PY'
import json,re,sys
p=sys.argv[1]; s=open(p, errors="replace").read()
m=re.search(r"(?m)^\{", s)
if not m: sys.exit(f"stdout 无 JSON 对象: {p}")
json.dump(json.loads(s[m.start():]), open(p,"w"), ensure_ascii=False)
PY
}
# S3 数据面（curl --aws-sigv4）：endpoint method path out [curl args...]
s3req() { local ep=$1 m=$2 p=$3 out=$4; shift 4
  curl -sS --max-time 120 -o "$out" -D "$out.hdr" -w '%{http_code}' -X "$m" \
    --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" "$@" "$ep$p"; }
count_shards() { find "$W" -type f -path "*/$BUCKET/$1/*/part.*" 2>/dev/null | wc -l; }
count_node_slot() { find "$W/${IDS[$1]}/d$2" -type f -name "part.*.$3" 2>/dev/null | wc -l; }
sha() { sha256sum "$1" | cut -d' ' -f1; }

echo "== 1. 4 节点（old 配置，maintenance=false）建桶写入：EC 300 KiB / inline / 双版本 =="
for i in 0 1 2 3; do mk_old_cfg "$i" false; mk_old_cfg "$i" true; mk_new_cfg "$i"; done
for i in 0 1 2 3; do start_node "$i" old.toml; done
ok=1; for i in 0 1 2 3; do wait_ready "$i" || { show_log "$i"; ok=0; }; done
ckm "4 节点 serve 就绪" [ "$ok" = 1 ]
TOK=$(login "$N1")
ckm "n1 管理员登录" [ -n "$TOK" ]
KEYS=$(curl -s --max-time 10 -X POST "$N1/api/v1/access-keys" -H "authorization: Bearer $TOK" -H 'content-type: application/json' -d '{}')
AK=$(printf '%s' "$KEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("access_key",""))')
SK=$(printf '%s' "$KEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("secret_key",""))')
ckm "铸造 AK/SK" [ -n "$AK" -a -n "$SK" ]

head -c 307200 /dev/urandom > "$W/ec.bin"
head -c 317440 /dev/urandom > "$W/ver1.bin"
head -c 327680 /dev/urandom > "$W/ver2.bin"
printf 'inline-object-payload-%s' "$BUCKET" > "$W/small.txt"
printf '<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>' > "$W/versioning.xml"
ckm "建桶 $BUCKET（200）" [ "$(s3req "$N1" PUT "/$BUCKET" "$W/mk.out")" = 200 ]
ckm "开版本控制（200）" [ "$(s3req "$N1" PUT "/$BUCKET?versioning" "$W/ver.out" -H 'content-type: application/xml' --data-binary "@$W/versioning.xml")" = 200 ]
ckm "PUT ec.bin → 200" [ "$(s3req "$N1" PUT "/$BUCKET/ec.bin" "$W/p.ec" --data-binary "@$W/ec.bin")" = 200 ]
ckm "PUT small.txt → 200" [ "$(s3req "$N1" PUT "/$BUCKET/small.txt" "$W/p.small" --data-binary "@$W/small.txt")" = 200 ]
ckm "PUT ver.bin v1 → 200" [ "$(s3req "$N1" PUT "/$BUCKET/ver.bin" "$W/p.v1" --data-binary "@$W/ver1.bin")" = 200 ]
V1=$(hdr x-amz-version-id "$W/p.v1.hdr"); V2=""
ckm "PUT ver.bin v2 → 200" [ "$(s3req "$N1" PUT "/$BUCKET/ver.bin" "$W/p.v2" --data-binary "@$W/ver2.bin")" = 200 ]
V2=$(hdr x-amz-version-id "$W/p.v2.hdr")
ckm "双版本 version id 非空且不同" [ -n "$V1" -a -n "$V2" -a "$V1" != "$V2" ]
EC_SHA=$(sha "$W/ec.bin"); SMALL_SHA=$(sha "$W/small.txt")
V1_SHA=$(sha "$W/ver1.bin"); V2_SHA=$(sha "$W/ver2.bin")
ckm "ec.bin 12 分片（EC 8+4 全槽）" [ "$(count_shards ec.bin)" = 12 ]
ckm "ver.bin 两版本 × 12 分片" [ "$(count_shards ver.bin)" = 24 ]
ckm "small.txt 无分片（inline）" [ "$(count_shards small.txt)" = 0 ]

echo "== 2. 停全部 → maintenance=true 重启 → 生成 3 节点新配置与 plan =="
stop_all
for i in 0 1 2 3; do ckm "${IDS[$i]} TERM 正常退出" [ -z "${PIDS[$i]}" ]; start_node "$i" maint.toml; done
ok=1; for i in 0 1 2 3; do wait_ready "$i" || { show_log "$i"; ok=0; }; done
ckm "4 节点 maintenance 就绪" [ "$ok" = 1 ]
stop_node 0
cli 0 maint.toml cluster-shrink plan --to "$W/n1/new.toml" --json > "$W/plan.json" 2> "$W/plan.err"; RC=$?
ckm "plan 退出码 0" [ "$RC" = 0 ]
if [ "$RC" != 0 ]; then cat "$W/plan.err"; fi
[ "$RC" = 0 ] && { json_clean "$W/plan.json" || RC=1; }
start_node 0 maint.toml; ckm "plan 后 n1 按旧配置重启" wait_ready 0
NEW_HASH=$(python3 - "$W/plan.json" <<'PY'
import json,sys
p=json.load(open(sys.argv[1])); plan=p["plan"]
assert plan["old_epoch"]==1, plan["old_epoch"]
assert plan["new_epoch"]==2, plan["new_epoch"]
assert plan["slot_counts"]==[12], plan["slot_counts"]
assert plan["retired_nodes"]==["n4"], plan["retired_nodes"]
moves={(m["slot"],m["from_node"],m["to_node"],m["to_drive"]) for m in plan["moves"]}
assert moves=={(9,"n4","n1",0),(10,"n4","n2",0),(11,"n4","n3",0)}, moves
assert len(p["slots"])==3 and all(s["shards"]>0 for s in p["slots"]), p["slots"]
assert len(plan["new_hash"])==16
print(plan["new_hash"])
PY
) ; RC=$?
ckm "plan 内容：epoch 1→2 / 12 槽 / 3 迁移（9→n1,10→n2,11→n3）/ 退役 n4" [ "$RC" = 0 ]
ckm "plan 报告落 .oss.sys/shrink" [ -n "$(find "$W/n1/d0/.oss.sys/shrink" -name '*-plan.json' 2>/dev/null | head -1)" ]

echo "== 3. 退役节点拒绝 apply/commit（实现语义：plan_ctx 只允许保留节点执行）=="
stop_node 3
OUT=$(cli 3 maint.toml cluster-shrink apply --plan "$W/plan.json" --yes 2>&1); RC=$?
ckm "n4 apply 被拒（退役）" [ "$RC" != 0 ]
printf '%s' "$OUT" | grep -q "退役" && pass "n4 apply 错误含「退役」" || fail "n4 apply 错误缺少「退役」: $OUT"
OUT=$(cli 3 maint.toml cluster-shrink commit --plan "$W/plan.json" --yes 2>&1); RC=$?
ckm "n4 commit 被拒（退役）" [ "$RC" != 0 ]
printf '%s' "$OUT" | grep -q "退役" && pass "n4 commit 错误含「退役」" || fail "n4 commit 错误缺少「退役」: $OUT"
start_node 3 maint.toml; ckm "n4 按旧配置重启" wait_ready 3

echo "== 4. 逐节点 apply + verify（停本节点服务，peers 运行；每节点后按旧配置重启）=="
for i in 0 1 2; do
  sl=$((9+i))
  stop_node "$i"
  cli "$i" maint.toml cluster-shrink apply --plan "$W/plan.json" --yes --json > "$W/apply.$i.json" 2> "$W/apply.$i.err"; RC=$?
  ckm "${IDS[$i]} apply 退出码 0" [ "$RC" = 0 ]
  if [ "$RC" = 0 ]; then
    json_clean "$W/apply.$i.json" || RC=1
    ckm "${IDS[$i]} apply：local_slots=1 / 复制 3 片 / 留收据" python3 - "$W/apply.$i.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r["action"]=="apply" and r["local_slots"]==1, r
assert r["files_copied"]==3 and r["files_skipped"]==0, r
assert r["receipt"], r
PY
    ckm "${IDS[$i]} 目标槽 $sl 的 3 个目标分片存在" [ "$(count_node_slot "$i" 0 "$sl")" = 3 ]
  else cat "$W/apply.$i.err"; fi
  cli "$i" maint.toml cluster-shrink verify --plan "$W/plan.json" --json > "$W/verify.$i.json" 2> "$W/verify.$i.err"; RC=$?
  ckm "${IDS[$i]} verify 退出码 0" [ "$RC" = 0 ]
  if [ "$RC" = 0 ]; then
    json_clean "$W/verify.$i.json" || RC=1
    ckm "${IDS[$i]} verify：CRC 全过 / 全读 3 版本 / 留收据" python3 - "$W/verify.$i.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r["local_slots"]==1 and not r["failures"], r
assert r["shards_checked"]==3 and r["objects_read"]==3, r
assert r["receipt"], r
PY
  else cat "$W/verify.$i.err"; fi
  start_node "$i" maint.toml; ckm "${IDS[$i]} 按旧配置重启（apply/verify 后）" wait_ready "$i"
done

echo "== 5. commit 前 rollback 演练（n2）：删除 CRC 匹配的目标副本 → 重新 apply+verify =="
stop_node 1
BEFORE=$(count_node_slot 1 0 10)
cli 1 maint.toml cluster-shrink rollback --plan "$W/plan.json" --yes --json > "$W/rollback.json" 2> "$W/rollback.err"; RC=$?
ckm "n2 rollback 退出码 0" [ "$RC" = 0 ]
if [ "$RC" = 0 ]; then
  json_clean "$W/rollback.json" || RC=1
  ckm "rollback 删除 $BEFORE 片（=files_removed）" python3 - "$W/rollback.json" "$BEFORE" <<'PY'
import json,sys
r=json.load(open(sys.argv[1])); want=int(sys.argv[2])
assert r["action"]=="rollback" and r["files_removed"]==want, r
PY
  ckm "rollback 后目标副本清零" [ "$(count_node_slot 1 0 10)" = 0 ]
  ckm "rollback 不动源片（n4 drive1 的 3 片仍在）" [ "$(count_node_slot 3 1 10)" = 3 ]
else cat "$W/rollback.err"; fi
cli 1 maint.toml cluster-shrink apply --plan "$W/plan.json" --yes --json > "$W/reapply.json" 2> "$W/reapply.err"; RC=$?
ckm "rollback 后重新 apply 退出码 0" [ "$RC" = 0 ]
if [ "$RC" = 0 ]; then
  json_clean "$W/reapply.json" || RC=1
  ckm "重新 apply 复制 3 片" python3 - "$W/reapply.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r["files_copied"]==3, r
PY
  ckm "n2 目标副本恢复" [ "$(count_node_slot 1 0 10)" = 3 ]
else cat "$W/reapply.err"; fi
cli 1 maint.toml cluster-shrink verify --plan "$W/plan.json" --json > "$W/reverify.json" 2> "$W/reverify.err"; RC=$?
ckm "重新 verify 退出码 0" [ "$RC" = 0 ]
if [ "$RC" = 0 ]; then
  json_clean "$W/reverify.json" || RC=1
  ckm "重新 verify 无失败" python3 - "$W/reverify.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1])); assert not r["failures"] and r["receipt"], r
PY
else cat "$W/reverify.err"; fi
start_node 1 maint.toml; ckm "n2 按旧配置重启（rollback 演练后）" wait_ready 1

echo "== 6. 逐节点 commit 写 topo v4（serve 保持运行：peers 仍报旧 epoch/hash）=="
for i in 0 1 2; do
  cli "$i" maint.toml cluster-shrink commit --plan "$W/plan.json" --yes --json > "$W/commit.$i.json" 2> "$W/commit.$i.err"; RC=$?
  ckm "${IDS[$i]} commit 退出码 0" [ "$RC" = 0 ]
  if [ "$RC" = 0 ]; then
    json_clean "$W/commit.$i.json" || RC=1
    ckm "${IDS[$i]} commit 写 3 盘（非幂等）" python3 - "$W/commit.$i.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]))
assert r["topo_written"]==3 and r["already_committed"] is False, r
PY
  else cat "$W/commit.$i.err"; fi
done
for i in 0 1 2; do
  ok=1
  for d in 0 1 2; do
    want="v4 set=0 node=${IDS[$i]} slot=$d epoch=2 hash=$NEW_HASH"
    [ "$(tr -d '\r\n' < "$W/${IDS[$i]}/d$d/.oss.sys/topo")" = "$want" ] || { ok=0; echo "  got: $(cat "$W/${IDS[$i]}/d$d/.oss.sys/topo")"; }
  done
  ckm "${IDS[$i]} 三盘 topo = v4 epoch=2 hash=plan.new_hash" [ "$ok" = 1 ]
done
ckm "退役 n4 三盘 topo 仍为 legacy v2" bash -c 'for d in 0 1 2; do grep -q "^v2 " "'"$W"'/n4/d$d/.oss.sys/topo" || exit 1; done'
# commit 后本机盘上已是 v4，旧配置 CLI 在 Store::open 即被拒（禁止配置回退）：
# 故「CLI 层重跑 commit」按设计不可达（in-process 幂等由 store 级测试覆盖）。
cli 0 maint.toml cluster-shrink commit --plan "$W/plan.json" --yes --json > "$W/commit.idem.json" 2> "$W/commit.idem.err"; RC=$?
ckm "commit 后旧配置 CLI 被 v4 拒启（不提供回退）" [ "$RC" != 0 ]
grep -q "v4" "$W/commit.idem.err" && pass "拒启原因指向 v4" || fail "拒启原因未提及 v4: $(tail -2 "$W/commit.idem.err")"

echo "== 7. 停 4 节点 → 3 保留节点新配置启动（maintenance=false）→ 数据/版本/状态 =="
stop_all
for i in 0 1 2; do start_node "$i" new.toml; done
ok=1; for i in 0 1 2; do wait_ready "$i" || { show_log "$i"; ok=0; }; done
ckm "3 节点新配置就绪" [ "$ok" = 1 ]
OUT=$(timeout 20 "$BIN" --config "$W/n1/old.toml" serve 2>&1); RC=$?
ckm "旧配置启动保留节点 n1 失败（rc=$RC）" [ "$RC" != 0 ]
printf '%s' "$OUT" | grep -q "v4" && pass "旧配置启动错误指向 v4 不匹配" || fail "旧配置启动错误未提及 v4: $(printf '%s' "$OUT" | tail -3)"
# 保留节点换新配置后，旧配置不可再启动（v4 exact-match 防线）。
TOK=$(login "$N1"); ckm "新集群 n1 管理员登录" [ -n "$TOK" ]

for i in 0 1; do
  ep="http://127.0.0.1:${HTTPP[$i]}"
  code=$(s3req "$ep" GET "/$BUCKET/ec.bin" "$W/g.ec.$i")
  if [ "$code" = 200 ] && [ "$(sha "$W/g.ec.$i")" = "$EC_SHA" ]; then pass "n$((i+1)) GET ec.bin 内容一致"; else fail "n$((i+1)) GET ec.bin code=$code"; fi
  code=$(s3req "$ep" GET "/$BUCKET/small.txt" "$W/g.small.$i")
  if [ "$code" = 200 ] && [ "$(sha "$W/g.small.$i")" = "$SMALL_SHA" ]; then pass "n$((i+1)) GET inline 对象内容一致"; else fail "n$((i+1)) GET small.txt code=$code"; fi
  code=$(s3req "$ep" GET "/$BUCKET/ver.bin" "$W/g.v2.$i")
  if [ "$code" = 200 ] && [ "$(sha "$W/g.v2.$i")" = "$V2_SHA" ]; then pass "n$((i+1)) GET ver.bin 当前版本一致"; else fail "n$((i+1)) GET ver.bin code=$code"; fi
  code=$(s3req "$ep" GET "/$BUCKET/ver.bin?versionId=$(enc "$V1")" "$W/g.v1.$i")
  if [ "$code" = 200 ] && [ "$(sha "$W/g.v1.$i")" = "$V1_SHA" ]; then pass "n$((i+1)) GET 版本 v1 内容一致"; else fail "n$((i+1)) GET v1 code=$code"; fi
  code=$(s3req "$ep" GET "/$BUCKET/ver.bin?versionId=$(enc "$V2")" "$W/g.v2id.$i")
  if [ "$code" = 200 ] && [ "$(sha "$W/g.v2id.$i")" = "$V2_SHA" ]; then pass "n$((i+1)) GET 版本 v2 内容一致"; else fail "n$((i+1)) GET v2 code=$code"; fi
done
code=$(s3req "$N1" GET "/$BUCKET?versions&prefix=ver.bin" "$W/versions.xml")
if [ "$code" = 200 ] && grep -q "$V1" "$W/versions.xml" && grep -q "$V2" "$W/versions.xml"; then
  pass "版本列表仍含两个 version id"
else fail "版本列表 code=$code 缺版本 id"; fi

ckm "cluster/status 报告 v4 拓扑" python3 - "$(curl -s --max-time 10 -H "authorization: Bearer $TOK" "$N1/api/v1/cluster/status")" "$NEW_HASH" <<'PY'
import json,sys
s=json.loads(sys.argv[1]); h=sys.argv[2]
assert s["mode"]=="cluster" and s["node_id"]=="n1", s
assert s["sets"][0]["slot_count"]==12 and len(s["nodes"])==3, s
assert s["topo_fingerprint"]==f"v4 set=0 node=n1 slot=0 epoch=2 hash={h}", s["topo_fingerprint"]
assert s["data_plane_compatible"] is True and not s["incompatible_peers"], s
PY

echo "== 8. 退役节点 n4 以旧配置无法加入新集群 =="
start_node 3 old.toml
if wait_ready 3; then
  pass "n4 旧配置进程可启动（静态拓扑无成员协议）"
  TOK4=$(login "$N4")
  ckm "n4 status 报 data_plane 不兼容（新拓扑 peer 拒绝）" python3 - "$(curl -s --max-time 10 -H "authorization: Bearer $TOK4" "$N4/api/v1/cluster/status")" <<'PY'
import json,sys
s=json.loads(sys.argv[1])
assert s["data_plane_compatible"] is False, s
assert len(s["incompatible_peers"])==3, s["incompatible_peers"]
PY
  code=$(s3req "$N4" PUT "/$BUCKET/retired.txt" "$W/n4.put" --data-binary "@$W/small.txt")
  if [ "$code" -ge 400 ]; then pass "n4 写入被拒（HTTP $code，无法加入）"; else fail "n4 写入意外成功 HTTP $code"; fi
else
  pass "n4 旧配置启动被拒（无法加入）"
  grep -Eq "v4|拓扑|incompat" "$W/n4/server.log" && pass "n4 拒绝日志指向拓扑不匹配" || fail "n4 拒绝原因不明"
fi

finish
