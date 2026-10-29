#!/usr/bin/env bash
# M9 Level C 回滚矩阵 e2e（LC-4 可执行切片；真实进程）。
#
# 矩阵（backlog §1.6；旧端 = v1.20.1，新端 = 本工作树二进制）：
#   1. marker=1、无 Level B 行：清掉 [raft].feature_level 键，旧二进制可启动；
#   2. marker=2、有结构化生命周期行：旧二进制被 guard_startup 拒绝（配置 1 < marker 2）；
#   3. marker=3 / OSSCTL3：旧二进制可读（同库启动）与可安装（raft-force-recover），
#      marker=4（高于二进制上限 3）被拒；
#   4. 导出封套 snapshot_format/wire_version/feature_level 不兼容：cutover 与
#      force-recover 在动数据前拒绝，目标库零改动；
#   5. 激活（marker=3）后回滚：必须保留 [raft].feature_level=3，否则 R8 拒绝启动。
#   6. CMK 低成本验证：显式 CMK 写→轮换→cmk scan/rewrap(dry-run/apply)→旧/新二进制读。
#      v1.20.1 自带 CMK 读路径；未覆盖「pre-CMK 旧端/legacy provider」读路径（见报告）。
#
# 每个格打印 PASS/FAIL；缺少旧二进制整体 SKIP（M9_ROLLBACK_STRICT=1 升 FAIL）。
# 用法：
#   OSS_OLD_BIN=/path/to/v1.20.1/oss-server OSS_NEW_BIN=target/debug/oss-server \
#     tests/e2e_m9_rollback.sh
# 前置：旧二进制（tag v1.20.1；--target-dir /tmp/oss-target-lc4）、curl、python3；
#       CMK 段另需 aws-cli（缺失则该段 SKIP）。
set -uo pipefail
cd "$(dirname "$0")/.."

NEW_BIN="${OSS_NEW_BIN:-${OSS_BIN:-$PWD/target/debug/oss-server}}"
OLD_BIN="${OSS_OLD_BIN:-}"
STRICT="${M9_ROLLBACK_STRICT:-0}"
PASSES=0; FAILS=0; SKIPS=0
pass() { PASSES=$((PASSES+1)); echo "  PASS: $*"; }
fail() { FAILS=$((FAILS+1)); echo "FAIL: $*"; }
skip() { if [ "$STRICT" = "1" ]; then fail "STRICT: $*"; else SKIPS=$((SKIPS+1)); echo "SKIP: $*"; fi; }
ck() { local m="$*"; if "$@"; then pass "$m"; else fail "$m"; fi; }
finish() {
  echo "== 结果 =="; echo "PASS=$PASSES FAIL=$FAILS SKIP=$SKIPS"
  [ "$FAILS" -ne 0 ] && exit 1
  echo "PASS: M9 rollback e2e（$PASSES 项通过，$SKIPS 项跳过）"
  exit 0
}
if [ ! -x "$NEW_BIN" ]; then echo "缺少新二进制（OSS_NEW_BIN / cargo build -p oss-server）"; exit 2; fi
if [ ! -x "$OLD_BIN" ]; then
  skip "缺少 OSS_OLD_BIN（v1.20.1）：Level C 回滚矩阵无法执行；构建 tag v1.20.1 后重跑"
  finish
fi
for c in curl python3; do command -v "$c" >/dev/null || { echo "需要 $c"; exit 2; }; done
NEW_BIN=$(realpath "$NEW_BIN"); OLD_BIN=$(realpath "$OLD_BIN")
NEW_HAS_AWS=0; command -v aws >/dev/null && NEW_HAS_AWS=1   # 仅 CMK 段用；名字保持短

free_port() { python3 -c 'import socket;s=socket.socket();s.bind(("127.0.0.1",0));print(s.getsockname()[1]);s.close()'; }
P1=$(free_port); P2=$(free_port); P3=$(free_port)
W=$(mktemp -d)
JWT_SECRET="lc4-e2e-jwt-secret-0123456789abcdef"   # >=32 字符（jwt-rust 上限校验）
CLUSTER=$(python3 -c 'import secrets;print(secrets.token_hex(32))')
TOK=$(python3 -c 'import secrets;print(secrets.token_hex(16))')
ADMIN="lc4admin"; APW="lc4admin-pw"
SRV_PID=""
cleanup() { [ -n "$SRV_PID" ] && kill -9 "$SRV_PID" 2>/dev/null; rm -rf "$W"; }
trap cleanup EXIT

# mk_cfg key port node bootstrap [feature] [raft=1]：feature 非空才写键（旧端回滚时要能清掉）。
mk_cfg() {
  local d="$W/$1" feat="${5:-}" raft="${6:-1}"
  mkdir -p "$d/d0" "$d/staging"
  cat > "$d/oss.toml" <<EOF
listen = "127.0.0.1:$2"
data_dir = "$d/d0"
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "$JWT_SECRET"
max_put_bytes = 268435456
groups = []

[site]
name = "$1"
region = "lc4-region"
priority = 10
token = "$TOK"

[ec]
enabled = true
data = 8
parity = 4
inline_threshold = 131072
EOF
  if [ "$raft" = 1 ]; then
    cat >> "$d/oss.toml" <<EOF

[raft]
node_id = $3
role = "voter"
bootstrap = $4
advertise_endpoint = "http://127.0.0.1:$2"
cluster_token = "$CLUSTER"
db_path = "$d/raft.db"
election_timeout_ms = 3000
heartbeat_interval_ms = 500
rpc_timeout_ms = 2000
write_timeout_ms = 3000
EOF
  fi
  [ -n "$feat" ] && echo "feature_level = $feat" >> "$d/oss.toml"
}
mk_witness() { # key port node [feature]
  local d="$W/$1"
  mkdir -p "$d"
  cat > "$d/witness.toml" <<EOF
[raft]
node_id = $3
role = "witness"
bootstrap = false
advertise_endpoint = "http://127.0.0.1:$2"
cluster_token = "$CLUSTER"
db_path = "$d/raft.db"
state_db = "$d/control.db"
EOF
  [ -n "${4:-}" ] && echo "feature_level = $4" >> "$d/witness.toml"
}
wait_path() { # port pid path
  local i c
  for i in $(seq 1 120); do
    kill -0 "$2" 2>/dev/null || return 1
    c=$(curl -s -o /dev/null -w '%{http_code}' --max-time 1 "http://127.0.0.1:$1$3" 2>/dev/null || true)
    [ "$c" = 200 ] && return 0
    sleep 1
  done
  return 1
}
start_srv() { # key bin port
  ( cd "$W/$1" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$2" serve >>"$W/$1/server.log" 2>&1 ) &
  SRV_PID=$!
  wait_path "$3" "$SRV_PID" /health/live
}
start_witness() { # key bin port
  ( cd "$W/$1" && exec "$2" witness --config "$W/$1/witness.toml" >>"$W/$1/server.log" 2>&1 ) &
  SRV_PID=$!
  wait_path "$3" "$SRV_PID" /health
}
stop_srv() { [ -n "$SRV_PID" ] && { kill -9 "$SRV_PID" 2>/dev/null; wait "$SRV_PID" 2>/dev/null; SRV_PID=""; }; }
# 启动必须失败：进程在 30s 内退出（非 0）且日志包含全部 pattern。用独立 fail.log 防旧日志误判。
start_fails() { # key bin pattern...
  local d="$W/$1" bin="$2" log pid rc i pat; shift 2
  log="$d/fail.log"; : > "$log"
  ( cd "$d" && OSS_ADMIN_USER="$ADMIN" OSS_ADMIN_PASSWORD="$APW" exec "$bin" serve >>"$log" 2>&1 ) &
  pid=$!
  for i in $(seq 1 150); do kill -0 "$pid" 2>/dev/null || break; sleep 0.2; done
  if kill -0 "$pid" 2>/dev/null; then kill -9 "$pid" 2>/dev/null; wait "$pid" 2>/dev/null; echo "30s 内未退出"; return 1; fi
  wait "$pid"; rc=$?
  [ "$rc" -ne 0 ] || { echo "退出码为 0（应拒绝启动）"; return 1; }
  for pat in "$@"; do
    grep -qF "$pat" "$log" || { echo "日志缺少 [$pat]：$(tail -3 "$log" | tr '\n' ' ')"; return 1; }
  done
}
status() { # 数据站点走 /api/v1/raft/status；witness 只暴露 /api/v1/site/raft/status
  local c
  c=$(curl -s --max-time 5 -H "x-oss-raft-token: $CLUSTER" "http://127.0.0.1:$1/api/v1/raft/status")
  case "$c" in *'"activation_level"'*) printf '%s' "$c"; return ;; esac
  curl -s --max-time 5 -H "x-oss-raft-token: $CLUSTER" "http://127.0.0.1:$1/api/v1/site/raft/status"
}
sf() { status "$1" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
v=d.get(sys.argv[1],""); print("" if v is None else v)' "$2"; }
login() { curl -s --max-time 10 -X POST "http://127.0.0.1:$1/api/v1/login" -H 'content-type: application/json' \
  -d "{\"username\":\"$ADMIN\",\"password\":\"$APW\"}" | python3 -c 'import json,sys
try: d=json.load(sys.stdin)
except Exception: d={}
print(d.get("token",""))'; }
api() { # port method path [body]
  if [ -n "${4:-}" ]; then
    curl -s --max-time 10 -o /dev/null -w '%{http_code}' -X "$2" "http://127.0.0.1:$1$3" \
      -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d "$4"
  else
    curl -s --max-time 10 -o /dev/null -w '%{http_code}' -X "$2" "http://127.0.0.1:$1$3" -H "authorization: Bearer $TOKEN"
  fi
}
marker() { python3 -c 'import sqlite3,sys
try:
    c=sqlite3.connect(sys.argv[1])
    r=c.execute("SELECT value FROM schema_meta WHERE key=?",("raft_feature_level",)).fetchone()
    print(r[0] if r else 1)
except sqlite3.Error: print("?")' "$1"; }
set_marker() { python3 - "$1" "$2" <<'PY'
import sqlite3, sys
c = sqlite3.connect(sys.argv[1])
c.execute("INSERT INTO schema_meta(key,value) VALUES('raft_feature_level',?) "
          "ON CONFLICT(key) DO UPDATE SET value=excluded.value", (sys.argv[2],))
c.commit()
PY
}
rows() { python3 -c 'import sqlite3,sys
c=sqlite3.connect(sys.argv[1])
print(c.execute("SELECT count(*) FROM "+sys.argv[2]).fetchone()[0])' "$1" "$2"; }
jget() { python3 -c 'import json,sys
d=json.load(open(sys.argv[1]))
for k in sys.argv[2].split("."):
    d = d[int(k)] if k.isdigit() else d[k]
print(d)' "$1" "$2"; }
reject() { # pattern command...
  local pat="$1" out rc; shift
  out=$(timeout 60 "$@" 2>&1); rc=$?
  if [ "$rc" -eq 0 ]; then echo "应失败却成功：$*"; return 1; fi
  printf '%s' "$out" | grep -qF "$pat" || { echo "缺少 [$pat]：$(printf '%s' "$out" | tail -2)"; return 1; }
}

echo "== 1. marker=1 / 无 Level B 行：清 feature_level 键，旧二进制可启动 =="
mk_cfg c1 "$P1" 1 true ""          # 不写 [raft].feature_level
ck start_srv c1 "$OLD_BIN" "$P1"
ck [ "$(sf "$P1" feature_level)" = 1 ]
ck [ "$(sf "$P1" activation_level)" = 1 ]
ck [ "$(marker "$W/c1/oss.db")" = 1 ]
ck [ "$(rows "$W/c1/oss.db" lifecycle_rules)" = 0 ]
stop_srv

echo "== 2. marker=2 / 有结构化生命周期行：旧二进制被 R8 拒绝 =="
mk_cfg c2 "$P1" 1 true 2
ck start_srv c2 "$NEW_BIN" "$P1"
TOKEN=$(login "$P1"); ck [ -n "$TOKEN" ]
ck [ "$(api "$P1" POST /api/v1/buckets '{"name":"lc4-b2"}')" = 204 ]
ck [ "$(api "$P1" PUT /api/v1/buckets/lc4-b2/lifecycle '[{"rule_id":"lc4","prefix":"x/","expiration_days":30,"enabled":true}]')" = 204 ]
stop_srv
ck [ "$(rows "$W/c2/oss.db" lifecycle_rules)" -ge 1 ]
set_marker "$W/c2/oss.db" 2
mk_cfg c2 "$P1" 1 true ""          # 旧端回滚：清掉 feature_level 键（默认 1 < marker 2）
ck start_fails c2 "$OLD_BIN" "raft_feature_level=2" "feature_level=1"
mk_cfg c2 "$P1" 1 true 2           # 对照：同库显式 feature_level=2 可启动（拒因是 R8 配置，不是坏库）
ck start_srv c2 "$OLD_BIN" "$P1"; stop_srv

echo "== 3. marker=3 / OSSCTL3：v1.20.1 可读、可安装；marker=4 被拒 =="
mk_cfg c3 "$P1" 1 true 3
ck start_srv c3 "$NEW_BIN" "$P1"
TOKEN=$(login "$P1"); ck [ -n "$TOKEN" ]
ck [ "$(api "$P1" POST /api/v1/buckets '{"name":"lc4-b3"}')" = 204 ]
stop_srv
set_marker "$W/c3/oss.db" 3
ck start_srv c3 "$OLD_BIN" "$P1"   # (a) 旧二进制读 marker=3 库
ck [ "$(sf "$P1" activation_level)" = 3 ]
ck [ "$(sf "$P1" feature_level)" = 3 ]
ck [ "$(sf "$P1" snapshot_format)" = 3 ]
ck [ "$(api "$P1" GET /api/v1/buckets)" = 200 ]
stop_srv
# (b) 当前二进制导出 OSSCTL3；v1.20.1 安装（force-recover）
if timeout 120 "$NEW_BIN" raft-export --config "$W/c3/oss.toml" --out "$W/export.json" >>"$W/export.log" 2>&1; then
  pass "raft-export 生成 OSSCTL3 导出"
else
  fail "raft-export 失败：$(tail -3 "$W/export.log")"
fi
ck [ "$(jget "$W/export.json" snapshot_format)" = 3 ]
ck [ "$(jget "$W/export.json" feature_level)" = 3 ]
ck [ "$(jget "$W/export.json" wire_version)" = 2 ]
mk_witness w3 "$P2" 7 3
ck start_witness w3 "$OLD_BIN" "$P2"; stop_srv   # 先建 raft_mode=on 的空 witness 库
if timeout 120 "$OLD_BIN" raft-force-recover --config "$W/w3/witness.toml" \
    --from "$W/export.json" --node-id 7 --yes >"$W/w3/recover.log" 2>&1; then
  pass "v1.20.1 安装 OSSCTL3 导出（force-recover）"
else
  fail "v1.20.1 force-recover 失败：$(tail -3 "$W/w3/recover.log")"
fi
ck [ "$(marker "$W/w3/control.db")" = 3 ]
ck start_witness w3 "$OLD_BIN" "$P2"
ck [ "$(sf "$P2" activation_level)" = 3 ]
stop_srv
# (c) 高于二进制上限的 marker：4 > MAX_FEATURE_LEVEL=3
set_marker "$W/c3/oss.db" 4
ck start_fails c3 "$OLD_BIN" "raft_feature_level=4" "高于本二进制最高支持 3"
set_marker "$W/c3/oss.db" 3

echo "== 4. 导出封套不兼容：cutover / force-recover 拒绝且目标零改动 =="
# 正对照：新二进制 cutover --dry-run --overwrite（同版本；目标库先由无 [raft] 的 serve
# 建 schema，行差异由 --overwrite 显式接受——本格只验证封套校验不误杀合法导出）。
mk_cfg c4 "$P3" 1 false "" 0
ck start_srv c4 "$NEW_BIN" "$P3"; stop_srv
mk_cfg c4 "$P3" 1 false "" 1
if timeout 60 "$NEW_BIN" raft-cutover --config "$W/c4/oss.toml" --from "$W/export.json" --dry-run --overwrite \
    >"$W/c4/dry.log" 2>&1; then
  pass "正对照：同一导出 cutover --dry-run --overwrite 通过"
else
  fail "正对照 cutover --dry-run 失败：$(tail -3 "$W/c4/dry.log")"
fi
sha256sum "$W/c4/oss.db" | awk '{print $1}' > "$W/c4/before.sha"
mutate() { python3 - "$1" "$2" "$3" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
d[sys.argv[2]] = int(sys.argv[3])
json.dump(d, open(sys.argv[1], "w"))
PY
}
# 封套变异基线取 Level-2 导出（无 runstate）：先过 checksum/runstate 闸门才会命中
# snapshot_format/wire/feature 校验；Level-3 导出的 runstate 闸门会先拒绝 snapshot_format。
if timeout 120 "$NEW_BIN" raft-export --config "$W/c2/oss.toml" --out "$W/export2.json" >>"$W/export2.log" 2>&1; then
  pass "raft-export 生成 Level-2 导出（封套变异基线）"
else
  fail "Level-2 raft-export 失败：$(tail -3 "$W/export2.log")"
fi
# feature_level 变异用 OSSCTL1 基线：OSSCTL2 封套会先命中「不能携带 level-3 数据」闸门。
if timeout 120 "$NEW_BIN" raft-export --config "$W/c1/oss.toml" --out "$W/export1.json" >>"$W/export1.log" 2>&1; then
  pass "raft-export 生成 OSSCTL1 导出（feature_level 变异基线）"
else
  fail "OSSCTL1 raft-export 失败：$(tail -3 "$W/export1.log")"
fi
mk_witness w4 "$P2" 8 ""
for pair in "snapshot_format 4 export2.json unsupported source snapshot_format 4" \
            "wire_version 99 export2.json unsupported source wire_version 99" \
            "feature_level 4 export1.json source feature_level 4 outside supported 1..=3"; do
  read -r field val base pat <<< "$pair"
  cp "$W/$base" "$W/mut.$field.json"; mutate "$W/mut.$field.json" "$field" "$val"
  ck reject "$pat" "$OLD_BIN" raft-cutover --config "$W/c4/oss.toml" --from "$W/mut.$field.json" --dry-run
  ck reject "$pat" "$OLD_BIN" raft-force-recover --config "$W/w4/witness.toml" --from "$W/mut.$field.json" --node-id 8 --yes
  ck [ ! -e "$W/w4/control.db" ] && ck [ ! -e "$W/w4/raft.db" ]
done
ck [ "$(sha256sum "$W/c4/oss.db" | awk '{print $1}')" = "$(cat "$W/c4/before.sha")" ]

echo "== 5. 激活后回滚：必须保留 [raft].feature_level=3，否则 R8 拒绝 =="
mk_cfg c3 "$P1" 1 true ""          # 回滚时清掉 feature_level 键
ck start_fails c3 "$OLD_BIN" "raft_feature_level=3" "feature_level=1"
mk_cfg c3 "$P1" 1 true 3           # 保留激活级别才能启动
ck start_srv c3 "$OLD_BIN" "$P1"
ck [ "$(sf "$P1" activation_level)" = 3 ]
ck [ -d "$W/c3/d0/lc4-b3" ]   # 旧端可读（bucket 目录在，marker=3 库正常启动）
stop_srv

echo "== 6. CMK 降级低成本验证：显式 CMK 写 → 轮换 → scan/rewrap → 旧/新二进制读 =="
if [ "$NEW_HAS_AWS" != 1 ]; then
  skip "CMK 段：缺少 aws-cli，无法写 SSE-KMS(CMK) 对象"
else
  export OSS_KMS_DIR="$W/kms"; mkdir -p "$OSS_KMS_DIR"
  mk_cfg c5 "$P1" 1 true ""
  ck start_srv c5 "$NEW_BIN" "$P1"
  TOKEN=$(login "$P1"); ck [ -n "$TOKEN" ]
  ck [ "$(api "$P1" POST /api/v1/encryption/cmks '{"name":"lc4"}')" = 200 ]
  ck [ "$(api "$P1" POST /api/v1/buckets '{"name":"lc4-cmk"}')" = 204 ]
  KEYS=$(curl -s --max-time 10 -X POST "http://127.0.0.1:$P1/api/v1/access-keys" \
    -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d '{}')
  AK=$(printf '%s' "$KEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("access_key",""))')
  SK=$(printf '%s' "$KEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin).get("secret_key",""))')
  head -c 262144 /dev/urandom > "$W/cmk.bin"
  s3() { AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1 \
    AWS_MAX_ATTEMPTS=1 AWS_PAGER="" aws --endpoint-url "http://127.0.0.1:$P1" "$@"; }
  ck s3 s3api put-object --bucket lc4-cmk --key cmk.bin --body "$W/cmk.bin" \
    --server-side-encryption aws:kms --ssekms-key-id lc4
  ck [ "$(api "$P1" POST /api/v1/encryption/cmks/lc4/rotate)" = 200 ]
  stop_srv
  RUST_LOG=error "$NEW_BIN" cmk scan --config "$W/c5/oss.toml" --name lc4 --json > "$W/cmk-scan.json" 2>"$W/cmk-scan.err"
  ck [ "$(jget "$W/cmk-scan.json" references)" = 1 ]
  ck [ "$(jget "$W/cmk-scan.json" refs.0.key_id)" = "cmk:lc4:k0" ]
  RUST_LOG=error "$NEW_BIN" cmk rewrap --config "$W/c5/oss.toml" --name lc4 --json > "$W/cmk-dry.json" 2>"$W/cmk-dry.err"
  ck [ "$(jget "$W/cmk-dry.json" candidates)" = 1 ]
  ck [ "$(jget "$W/cmk-dry.json" items.0.status)" = preview ]
  ck [ "$(jget "$W/cmk-dry.json" items.0.old_key_id)" = "cmk:lc4:k0" ]
  RUST_LOG=error "$NEW_BIN" cmk rewrap --config "$W/c5/oss.toml" --name lc4 --apply --json > "$W/cmk-apply.json" 2>"$W/cmk-apply.err"
  ck [ "$(jget "$W/cmk-apply.json" rewrapped)" = 1 ]
  RUST_LOG=error "$NEW_BIN" cmk scan --config "$W/c5/oss.toml" --name lc4 --json > "$W/cmk-scan2.json" 2>"$W/cmk-scan2.err"
  ck [ "$(jget "$W/cmk-scan2.json" refs.0.key_id)" = "cmk:lc4:k1" ]
  # 旧端读回（v1.20.1 自带 CMK 读路径；同一 KMS 目录）
  ck start_srv c5 "$OLD_BIN" "$P1"
  ck s3 s3api get-object --bucket lc4-cmk --key cmk.bin "$W/cmk-old.out"
  stop_srv
  if [ -f "$W/cmk-old.out" ] && cmp -s "$W/cmk.bin" "$W/cmk-old.out"; then pass "旧二进制读回 rewrap 后对象字节一致"; else fail "旧二进制读回对象字节不一致"; fi
  ck start_srv c5 "$NEW_BIN" "$P1"
  ck s3 s3api get-object --bucket lc4-cmk --key cmk.bin "$W/cmk-new.out"
  stop_srv
  if [ -f "$W/cmk-new.out" ] && cmp -s "$W/cmk.bin" "$W/cmk-new.out"; then pass "新二进制读回 rewrap 后对象字节一致"; else fail "新二进制读回对象字节不一致"; fi
  unset OSS_KMS_DIR
fi

finish
