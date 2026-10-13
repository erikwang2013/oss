#!/usr/bin/env bash
# M3 集群端到端（Task 10）：4 节点真进程 + 混沌段，覆盖验收①–⑤。
#
# 拓扑：4 节点 × 3 盘 = 12 槽（Q_s=9 槽位域 / Q_n=3 节点域）。
#   **临界口径**：停 1 节点（剩 9 槽）写 = 恰好 quorum（零余量，客户端重试收敛）；
#   停 2 节点（剩 6 槽 <9）写被拒且读也不可达（GET=500 CorruptMeta，不是 404）。
#   要余量换 6×2 配置（12 槽、停 1 余 1）——README 记档。
#
# 前置：`cargo build -p oss-server`（或由调用方先构建）；本机有 aws-cli。
# 端口：HTTP/gRPC 全部动态取空闲端口（本机 9001 曾被外部服务占用——实弹教训）。
# 时间预算：⑤ 段每个被拒写 ≈ 锁 acquire 重试耗尽（~50s，SIGSTOP 进程不响应、
#   RPC 等满超时）；⑤-positive 段成功写 ≈ 40s（掉队节点的分片写逐个等超时）。
set -uo pipefail

BIN="${BIN:-/home/wwwroot/oss/target/debug/oss-server}"
RUN="${RUN:-/tmp/m3-e2e-cluster}"
[ -x "$BIN" ] || { echo "先构建：cargo build -p oss-server"; exit 1; }
command -v aws >/dev/null || { echo "需要 aws-cli"; exit 1; }
command -v python3 >/dev/null || { echo "需要 python3"; exit 1; }

free_port() {
  python3 - <<'PY'
import socket
s = socket.socket()
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

rm -rf "$RUN"; mkdir -p "$RUN"
IDS=(n1 n2 n3 n4)
declare -a HTTPP GRPCP
for i in 0 1 2 3; do
  HTTPP+=("$(free_port)"); GRPCP+=("$(free_port)")
done

for i in 0 1 2 3; do
  d="$RUN/${IDS[$i]}"
  mkdir -p "$d/d0" "$d/d1" "$d/d2"
  cat > "$d/oss.toml" <<EOF
listen = "127.0.0.1:${HTTPP[$i]}"
data_dir = "$d/d0"
data_dirs = ["$d/d0","$d/d1","$d/d2"]
staging_dir = "$d/staging"
database_url = "sqlite://$d/oss.db?mode=rwc"
jwt_secret = "dev-only-change-me-0123456789abcdef"
max_put_bytes = 268435456

[cluster]
node_id = "${IDS[$i]}"
secret = "m3-e2e-secret"

[[cluster.nodes]]
id = "n1"
addr = "127.0.0.1:${GRPCP[0]}"
drive_count = 3

[[cluster.nodes]]
id = "n2"
addr = "127.0.0.1:${GRPCP[1]}"
drive_count = 3

[[cluster.nodes]]
id = "n3"
addr = "127.0.0.1:${GRPCP[2]}"
drive_count = 3

[[cluster.nodes]]
id = "n4"
addr = "127.0.0.1:${GRPCP[3]}"
drive_count = 3
EOF
done

PIDS=()
start_node() { # $1 = 序号
  local i=$1 d="$RUN/${IDS[$1]}"
  (cd "$d" && OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw exec "$BIN" serve >>"$d/server.log" 2>&1) &
  PIDS[$i]=$!
}
wait_http() { # $1 = HTTP 端口
  for _t in $(seq 1 60); do
    s=$(curl -s -o /dev/null -w "%{http_code}" --max-time 1 "http://127.0.0.1:$1/health/live" 2>/dev/null || true)
    [ "$s" = "200" ] && return 0
    sleep 1
  done
  return 1
}

cleanup() {
  for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null; done
}
trap cleanup EXIT

echo "== 0. 起 4 节点（HTTP ${HTTPP[*]}）=="
for i in 0 1 2 3; do start_node "$i"; done
for i in 0 1 2 3; do wait_http "${HTTPP[$i]}" || { echo "FAIL: ${IDS[$i]} 未就绪"; exit 1; }; done
echo "   4 节点就绪"

N1="http://127.0.0.1:${HTTPP[0]}"; N2="http://127.0.0.1:${HTTPP[1]}"
N3="http://127.0.0.1:${HTTPP[2]}"; N4="http://127.0.0.1:${HTTPP[3]}"

# ---- 控制面读扇出（Task 9）：AK 只在 n1 铸造，其它节点直接可用 ----
echo "== 0b. 控制面读扇出：AK 在 n1 铸造 → n4 直接可用 =="
TOK=$(curl -s -X POST "$N1/api/v1/login" -H 'content-type: application/json' \
  -d '{"username":"erik","password":"pw"}' | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
[ -n "$TOK" ] || { echo "FAIL: n1 登录"; exit 1; }
# AK/SK 由服务端自动生成（空 body）：手拼曾连踩两发「≥16 位」校验 400（AK 15 位、SK 15 位）
KEYS=$(curl -s -X POST "$N1/api/v1/access-keys" -H "authorization: Bearer $TOK" \
  -H 'content-type: application/json' -d '{}')
AK=$(printf '%s' "$KEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin)["access_key"])')
SK=$(printf '%s' "$KEYS" | python3 -c 'import json,sys;print(json.load(sys.stdin)["secret_key"])')
[ -n "$AK" ] && [ -n "$SK" ] || { echo "FAIL: 铸造 AK/SK：$KEYS"; exit 1; }
export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
export AWS_MAX_ATTEMPTS=1   # ⑤ 段确定性计时：不叠加 aws 自身重试
aws --endpoint-url "$N4" s3 ls >/dev/null || { echo "FAIL: n1 铸造的 AK 在 n4 不可用（读扇出失效）"; exit 1; }
aws --endpoint-url "$N3" s3 ls >/dev/null || { echo "FAIL: n1 铸造的 AK 在 n3 不可用"; exit 1; }
echo "   扇出路径通"

W=$(mktemp -d); trap 'cleanup; rm -rf "$W"' EXIT

echo "== ①③. 写 10MiB（n1）→ 4 节点读一致（cmp） =="
aws --endpoint-url "$N1" s3 mb s3://m3sync >/dev/null
head -c 10485760 /dev/urandom > "$W/ten.bin"
aws --endpoint-url "$N1" s3 cp "$W/ten.bin" s3://m3sync/ten.bin >/dev/null
for i in 0 1 2 3; do
  ep="http://127.0.0.1:${HTTPP[$i]}"
  aws --endpoint-url "$ep" s3 cp s3://m3sync/ten.bin "$W/r$i.bin" >/dev/null
  cmp "$W/ten.bin" "$W/r$i.bin" || { echo "FAIL: ${IDS[$i]} 读不一致"; exit 1; }
done
echo "   4 节点逐字节一致 ✓"

echo "== ①. kill -9 n4 → 读写继续（9 槽=恰好 quorum，零余量临界）=="
kill -9 "${PIDS[3]}"; sleep 1
head -c 1048576 /dev/urandom > "$W/down.bin"
aws --endpoint-url "$N1" s3 cp "$W/down.bin" s3://m3sync/down.bin >/dev/null || { echo "FAIL: 停 1 节点应可写（临界）"; exit 1; }
aws --endpoint-url "$N2" s3 cp s3://m3sync/ten.bin "$W/r2b.bin" >/dev/null
cmp "$W/ten.bin" "$W/r2b.bin" || { echo "FAIL: 停 1 节点读不一致"; exit 1; }
echo "   读写继续 ✓"

echo "== ②. 拉回 n4 → fsck --repair 差集补齐（缺键补建闭环）=="
start_node 3; wait_http "${HTTPP[3]}" || { echo "FAIL: n4 拉回未就绪"; exit 1; }
OUT=$(cd "$RUN/n4" && "$BIN" fsck --repair 2>&1)
echo "$OUT" | tail -2
REPORT=$(echo "$OUT" | grep -o 'fsck 报告: .*' | sed 's/fsck 报告: //')
if [ -n "$REPORT" ]; then
  python3 - "$REPORT" <<'PY' || { echo "FAIL: fsck 报告缺键未补"; exit 1; }
import json, sys
r = json.load(open(sys.argv[1]))
assert any("down.bin" in k for k in r["missing_meta_keys"]), f"应检出缺键：{r['missing_meta_keys']}"
assert r["repaired_meta_keys"] >= 1, f"应补建：{r['repaired_meta_keys']}"
PY
fi
OUT2=$(cd "$RUN/n4" && "$BIN" fsck 2>&1)
REPORT2=$(echo "$OUT2" | grep -o 'fsck 报告: .*' | sed 's/fsck 报告: //')
if [ -n "$REPORT2" ]; then
  python3 - "$REPORT2" <<'PY' || { echo "FAIL: 复检仍有缺键"; exit 1; }
import json, sys
r = json.load(open(sys.argv[1]))
assert r["missing_meta_keys"] == [], f"复检缺键应清零：{r['missing_meta_keys']}"
PY
fi
aws --endpoint-url "$N4" s3 cp s3://m3sync/down.bin "$W/r4d.bin" >/dev/null
cmp "$W/down.bin" "$W/r4d.bin" || { echo "FAIL: n4 补齐后读 down.bin 不一致"; exit 1; }
echo "   差集检出→补建→复检清零，n4 可读 ✓"

echo "== ④. 并发写同一 key ×100 轮：读回必为某次完整写（无交错） =="
for round in $(seq 1 100); do
  for w in $(seq 1 10); do
    printf 'r%s-w%s-%s' "$round" "$w" "$(date +%s%N)" > "$W/c$w.bin"
  done
  pids=()
  for w in $(seq 1 10); do
    aws --endpoint-url "$N1" s3api put-object --bucket m3sync --key hot --body "$W/c$w.bin" >/dev/null 2>&1 &
    pids+=($!)
  done
  wait "${pids[@]}"   # 只等这 10 个 aws——裸 wait 会把常驻 server 进程也等上（实弹踩过）
  aws --endpoint-url "$N1" s3api get-object --bucket m3sync --key hot "$W/got.bin" >/dev/null 2>&1 \
    || { echo "FAIL: ④ 轮 $round 读失败"; exit 1; }
  ok=0
  for w in $(seq 1 10); do cmp -s "$W/c$w.bin" "$W/got.bin" && { ok=1; break; }; done
  [ "$ok" = "1" ] || { echo "FAIL: ④ 轮 $round 读到交错内容"; exit 1; }
done
echo "   100 轮无交错 ✓"

echo "== ⑤. SIGSTOP n3+n4（6 槽 <9、2 节点 <3）→ 写必须失败（5xx） =="
kill -STOP "${PIDS[2]}" "${PIDS[3]}"
# 就绪面：少数派 → 503（强语义就绪探针）
code=$(curl -s -o /dev/null -w "%{http_code}" --max-time 10 "$N1/health/ready" || true)
[ "$code" = "503" ] || { echo "FAIL: 少数派 ready 应 503，实得 $code"; exit 1; }
# 写必须失败：5xx（锁 quorum 未达）——单发 + 时间预算 300s（等满 RPC 超时）
if timeout 300 aws --endpoint-url "$N1" s3api put-object --bucket m3sync --key split --body "$W/ten.bin" >/dev/null 2>"$W/err.txt"; then
  echo "FAIL: 分区下写必须被拒"; exit 1
fi
grep -Eq "500|InternalError" "$W/err.txt" || { echo "FAIL: 拒绝形态应为 5xx：$(cat "$W/err.txt")"; exit 1; }
echo "   写被拒（5xx）✓"
# 同期 GET 亦不可达：500 CorruptMeta（present=6≠0，**不是 404**；读同败为预期）
if timeout 120 aws --endpoint-url "$N1" s3api get-object --bucket m3sync --key ten.bin "$W/x.bin" >/dev/null 2>"$W/err2.txt"; then
  echo "FAIL: 覆盖不足 quorum 时读也应失败"; exit 1
fi
if grep -q "NoSuchKey" "$W/err2.txt"; then echo "FAIL: 应为 500 而非 404"; exit 1; fi
echo "   读同不可达（非 404）✓"

echo "== ⑤-positive. 恢复 n4（停 1 节点=3/4 多数）→ 写必须成功 =="
kill -CONT "${PIDS[3]}"; sleep 1
# 预算：掉队节点(n3)的分片/meta RPC 逐个等超时（本节点 3 分片 ×5s + commit 重试）
head -c 524288 /dev/urandom > "$W/major.bin"
timeout 300 aws --endpoint-url "$N1" s3 cp "$W/major.bin" s3://m3sync/major.bin >/dev/null \
  || { echo "FAIL: 多数派下写必须成功"; exit 1; }
echo "   多数派写成功 ✓"

echo "== 收尾：CONT n3 → 4 节点复查读写 =="
kill -CONT "${PIDS[2]}"; sleep 1
aws --endpoint-url "$N2" s3 cp s3://m3sync/major.bin "$W/rm.bin" >/dev/null
cmp "$W/major.bin" "$W/rm.bin" || { echo "FAIL: 恢复后读不一致"; exit 1; }
aws --endpoint-url "$N3" s3 cp s3://m3sync/ten.bin "$W/rt.bin" >/dev/null
cmp "$W/ten.bin" "$W/rt.bin" || { echo "FAIL: 恢复后 n3 读不一致"; exit 1; }
curl -s "$N1/health/cluster" >/dev/null
echo "   恢复后读写一致 ✓"

echo "M3 集群验收全部通过（①③④⑤ + ⑤-positive + 控制面扇出 + fsck 补齐）"
