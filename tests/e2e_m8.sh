#!/usr/bin/env bash
# M8 控制面端到端：AP 控制面 / Batch 1000+1 / Inventory / Transition+Restore / 回归。
# 前置：已起 oss-server（S3 面 + /api/v1），aws-cli、curl(>=7.75，--aws-sigv4)、python3、gzip。
#   OSS_ADMIN_USER/OSS_ADMIN_PASSWORD 建管理员（缺省 erik/changeme，见 README「跑起来」）。
#   AK/SK 不存在时脚本用管理员 JWT 幂等补种（与 README 的 POST /access-keys 一致）。
# 分层段（步骤 4）：服务须以 `[[tiers]]` 启动（远端 S3 兼容端点），否则显式 SKIP 并记行；
#   可设 TIER_AK/TIER_SK + TIER_ENDPOINT 让脚本自动建远端桶（否则假定该桶已存在）。
# 可选：E2E_M8_SKIP_TIER=1 强制跳过分层；E2E_M8_REPL_SMOKE=1 追加 M6/M7 双实例回归。
#   E2E_M8_PARALLEL（缺省 8）/ E2E_M8_JOB_TIMEOUT（3600s；单个 1000 任务作业在
#   1 worker × 1s 间隔下约 17 分钟，本脚本跑 1000 与 1000+1 两个作业。单节点 SQLite
#   单写者，e2e 建议 OSS_JOB_WORKERS=1 + OSS_JOBS_INTERVAL_SECS=1：默认 2 worker 在
#   负载下可能触发 finish_task 锁竞争，任务停在 running 直到 deadline 回收）
#   / E2E_M8_TIER_TIMEOUT（240s）/ E2E_M8_M4_TIMEOUT（150s）。
# 不触发 CI；不修改服务端任何配置。
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
AK="${AK:-AKIA_TEST_ACCESS_KEY}"
SK="${SK:-SK_TEST_SECRET_KEY_0001}"
ADMIN_USER="${ADMIN_USER:-${OSS_ADMIN_USER:-erik}}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-${OSS_ADMIN_PASSWORD:-changeme}}"
PAR="${E2E_M8_PARALLEL:-8}"
JOB_TIMEOUT="${E2E_M8_JOB_TIMEOUT:-3600}"
TIER_TIMEOUT="${E2E_M8_TIER_TIMEOUT:-240}"
M4_TIMEOUT="${E2E_M8_M4_TIMEOUT:-150}"
RUN="$(date +%s)${RANDOM}"
WORK="$(mktemp -d)"
AP_NAME="m8ap-$RUN"
BUCKETS=("m8ap-$RUN" "m8batch-src-$RUN" "m8batch-rep-$RUN" "m8inv-src-$RUN" "m8inv-dst-$RUN"
  "m8tier-$RUN" "m8m4-$RUN" "m8std-$RUN")
TOKEN=""
TIER_BUCKET_CREATED=""
T0=$(date +%s)

export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""

die() { echo "FAIL: $*" >&2; exit 1; }
step() { echo; echo "== $* （+$(( $(date +%s) - T0 ))s）=="; }
aws_s3() { aws --endpoint-url "$ENDPOINT" s3 "$@"; }
aws_api() { aws --endpoint-url "$ENDPOINT" s3api "$@"; }

# 版本桶的 rb --force 不删版本/删除标记（AWS CLI 口径）：先按版本逐条删再删桶。
# e2e 每桶版本数很小，单次 ≤1000 直接一批；超过则需分片（本脚本不涉及）。
purge_versions() {
  local b=$1 vy="$WORK/versions.json" del="$WORK/delete.json"
  aws_api list-object-versions --bucket "$b" --output json >"$vy" 2>/dev/null || return 0
  python3 - "$vy" >"$del" <<'PY'
import json, sys
d = json.load(open(sys.argv[1]))
objs = [{"Key": o["Key"], "VersionId": o["VersionId"]}
        for o in d.get("Versions", []) + d.get("DeleteMarkers", [])]
if objs:
    json.dump({"Objects": objs, "Quiet": True}, sys.stdout)
PY
  [ -s "$del" ] && aws_api delete-objects --bucket "$b" --delete "file://$del" >/dev/null 2>&1 || true
}
cleanup() {
  for b in "${BUCKETS[@]}"; do
    purge_versions "$b"
    aws_s3 rb "s3://$b" --force >/dev/null 2>&1 || true
  done
  if [ -n "$TIER_BUCKET_CREATED" ] && [ -n "${TIER_ENDPOINT:-}" ]; then
    AWS_ACCESS_KEY_ID="${TIER_AK:-$AK}" AWS_SECRET_ACCESS_KEY="${TIER_SK:-$SK}" \
      aws --endpoint-url "$TIER_ENDPOINT" s3 rb "s3://$TIER_BUCKET_CREATED" --force >/dev/null 2>&1 || true
  fi
  if [ -n "$TOKEN" ]; then
    curl -s -o /dev/null -X DELETE "$ENDPOINT/api/v1/access-points/$AP_NAME" \
      -H "authorization: Bearer $TOKEN" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

# api METHOD PATH [BODY] [OUTFILE] → stdout = HTTP 状态码；响应体落 OUTFILE（缺省 api.json）
api() {
  local m=$1 p=$2 body=${3:-} out=${4:-$WORK/api.json}
  if [ -n "$body" ]; then
    curl -sS -o "$out" -w '%{http_code}' -X "$m" "$ENDPOINT/api/v1$p" \
      -H "authorization: Bearer $TOKEN" -H 'content-type: application/json' -d "$body"
  else
    curl -sS -o "$out" -w '%{http_code}' -X "$m" "$ENDPOINT/api/v1$p" \
      -H "authorization: Bearer $TOKEN"
  fi
}
expect() { [ "$1" = "$2" ] || die "$3：HTTP $1（期望 $2）$(head -c 300 "${4:-$WORK/api.json}" 2>/dev/null)"; }
jget() { python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))[sys.argv[2]])' "$1" "$2"; }
jjob() { python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["job"][sys.argv[2]])' "$1" "$2"; }
wait_true() { # timeout desc fn
  local deadline=$(( $(date +%s) + $1 )) what=$2 fn=$3
  while :; do "$fn" && return 0; [ "$(date +%s)" -ge "$deadline" ] && return 1; sleep 2; done
}
POLLED_STATE=""
poll_job() { # id timeout → 终态写入 POLLED_STATE（详情落 job.json）
  local id=$1 deadline=$(( $(date +%s) + $2 )) st code
  while :; do
    code=$(api GET "/jobs/$id" "" "$WORK/job.json")
    [ "$code" = 200 ] || die "GET /jobs/$id：HTTP $code"
    st=$(jjob "$WORK/job.json" state) || die "作业 $id 详情缺 state：$(head -c 200 "$WORK/job.json")"
    case "$st" in done | error | cancelled) POLLED_STATE=$st; return 0 ;; esac
    [ "$(date +%s)" -ge "$deadline" ] && die "作业 $id 超时（state=$st）"
    sleep 2
  done
}

for bin in aws curl python3 gzip; do command -v "$bin" >/dev/null || die "缺少 $bin"; done
curl --help all 2>/dev/null | grep -q aws-sigv4 || die "curl 需要 --aws-sigv4（>=7.75）"
curl -s -m 2 -o /dev/null -w '' "$ENDPOINT/health/live" || die "服务不可达：$ENDPOINT"

step "0. 管理员登录 + 幂等补种 AK"
curl -sS -o "$WORK/login.json" -w '%{http_code}' -X POST "$ENDPOINT/api/v1/login" \
  -H 'content-type: application/json' \
  -d "$(python3 -c 'import json,sys;print(json.dumps({"username":sys.argv[1],"password":sys.argv[2]}))' "$ADMIN_USER" "$ADMIN_PASSWORD")" \
  | grep -qx 200 || die "登录失败（ADMIN_USER/ADMIN_PASSWORD？）$(head -c 200 "$WORK/login.json")"
TOKEN=$(jget "$WORK/login.json" token)
[ -n "$TOKEN" ] || die "登录响应无 token"
if ! aws_api list-buckets >/dev/null 2>&1; then
  code=$(api POST /access-keys "{\"access_key\":\"$AK\",\"secret_key\":\"$SK\"}")
  [ "$code" = 200 ] || [ "$code" = 400 ] || expect "$code" 200 "补种 AK"
  aws_api list-buckets >/dev/null 2>&1 || die "AK/SK 不可用且补种失败"
fi
echo "   admin=$ADMIN_USER AK=${AK:0:8}…"

step "1. AP 控制面：建/策略/读/删 + 标准路径未回归"
aws_s3 mb "s3://${BUCKETS[0]}"
printf 'ap-body-%s' "$RUN" >"$WORK/ap.txt"
aws_s3 cp "$WORK/ap.txt" "s3://${BUCKETS[0]}/one.txt" >/dev/null
code=$(api POST /access-points "{\"name\":\"$AP_NAME\",\"bucket\":\"${BUCKETS[0]}\"}")
expect "$code" 201 "建 AP"
code=$(api GET /access-points)
expect "$code" 200 "列 AP"
python3 - "$WORK/api.json" "$AP_NAME" <<'PY' || die "AP 列表未包含新建 AP"
import json,sys
assert any(r.get("name") == sys.argv[2] for r in json.load(open(sys.argv[1])))
PY
POLICY="{\"Statement\":[{\"Effect\":\"Allow\",\"Principal\":{\"AWS\":\"*\"},\"Action\":\"s3:GetObject\",\"Resource\":\"arn:aws:s3:::${BUCKETS[0]}/*\"}]}"
code=$(api PUT "/access-points/$AP_NAME/policy" "$POLICY")
expect "$code" 204 "PUT AP 策略"
code=$(api GET "/access-points/$AP_NAME")
expect "$code" 200 "GET AP 详情"
python3 - "$WORK/api.json" "$AP_NAME" "${BUCKETS[0]}" <<'PY' || die "AP 详情/策略回读不一致"
import json,sys
d=json.load(open(sys.argv[1]))
assert d["name"] == sys.argv[2] and d["bucket"] == sys.argv[3], d
assert d["policy"]["Statement"][0]["Action"] == "s3:GetObject", d["policy"]
PY
code=$(api DELETE "/access-points/$AP_NAME")
expect "$code" 204 "删 AP"
code=$(api GET "/access-points/$AP_NAME")
expect "$code" 404 "删后 GET AP"
aws_s3 cp "s3://${BUCKETS[0]}/one.txt" "$WORK/ap.back" >/dev/null
cmp "$WORK/ap.txt" "$WORK/ap.back" || die "AP 建删后标准路径读回不一致"
echo "   AP CRUD/策略通过；标准 S3 路径未回归"

step "2. Batch：1000 全成功 → 1000+1 失败 → 报告 → retry-failed"
aws_s3 mb "s3://${BUCKETS[1]}"
aws_s3 mb "s3://${BUCKETS[2]}"
printf 'x' >"$WORK/tiny.txt"
echo "   PutObject 1000 键（xargs -P $PAR）..."
seq 0 999 | xargs -P "$PAR" -I{} aws --endpoint-url "$ENDPOINT" s3api put-object \
  --bucket "${BUCKETS[1]}" --key "obj/{}" --body "$WORK/tiny.txt" >/dev/null \
  || die "xargs 批量 PutObject 失败"
N=$(aws_api list-objects-v2 --bucket "${BUCKETS[1]}" --prefix obj/ --query 'length(Contents)' --output text)
[ "$N" = 1000 ] || die "源桶对象数 $N ≠ 1000"
new_batch() {
  aws_api put-object --bucket "${BUCKETS[1]}" --key manifest.csv --body "$WORK/manifest.csv" >/dev/null
  code=$(api POST /jobs "$(printf '{"kind":"batch","manifest_bucket":"%s","manifest_key":"manifest.csv","operation":"ReplaceTags","tags":{"batch":"1"},"report_bucket":"%s","report_prefix":"m8rep"}' "${BUCKETS[1]}" "${BUCKETS[2]}")")
  expect "$code" 201 "建批量作业"
  jget "$WORK/api.json" job_id
}
# 报告断言：表头/行数/唯一 taskId/ISO8601；resultCode 计数与 key 匹配交给调用方。
check_report() {
  python3 - "$1" "$2" "$3" "$4" <<'PY' || die "报告断言失败：$1"
import csv,sys
from datetime import datetime
rows=list(csv.reader(open(sys.argv[1], newline="")))
exp=["taskId","bucket","key","versionId","resultCode","resultString","startTime","endTime"]
assert rows[0]==exp, rows[0]
body=rows[1:]
assert len(body)==int(sys.argv[2]), len(body)
ids={r[0] for r in body}
assert len(ids)==len(body), "taskId 不唯一"
ok=sum(r[4]=="Succeeded" for r in body)
assert ok==int(sys.argv[3]), ok
bad=[r for r in body if r[4]=="PermanentFailure"]
assert len(bad)==int(sys.argv[4]), bad
if bad: assert [r[2] for r in bad]==["missing-key"], bad
for r in body:
    datetime.strptime(r[6], "%Y-%m-%dT%H:%M:%SZ")
    datetime.strptime(r[7], "%Y-%m-%dT%H:%M:%SZ")
PY
}
{ echo "bucket,key"; for i in $(seq 0 999); do echo "${BUCKETS[1]},obj/$i"; done; } >"$WORK/manifest.csv"
JOB_OK=$(new_batch)
[ "$(jget "$WORK/api.json" total)" = 1000 ] || die "全成功作业 total ≠ 1000"
echo "   job=$JOB_OK total=1000，等终态..."
poll_job "$JOB_OK" "$JOB_TIMEOUT"
[ "$POLLED_STATE" = done ] || die "全成功作业终态 $POLLED_STATE ≠ done"
[ "$(jjob "$WORK/job.json" total)" = 1000 ] && [ "$(jjob "$WORK/job.json" succeeded)" = 1000 ] \
  && [ "$(jjob "$WORK/job.json" failed)" = 0 ] || die "全成功作业计数不符：$(cat "$WORK/job.json")"
aws_api get-object --bucket "${BUCKETS[2]}" --key "m8rep/$JOB_OK/results.csv" "$WORK/report-ok.csv" >/dev/null
check_report "$WORK/report-ok.csv" 1000 1000 0
for i in 0 500 999; do
  V=$(aws_api get-object-tagging --bucket "${BUCKETS[1]}" --key "obj/$i" --query 'TagSet[?Key==`batch`].Value' --output text)
  [ "$V" = "1" ] || die "obj/$i 的 batch 标签 = '$V'（期望 1）"
done
echo "   1000 行全成功 / 报告 1000 行 / 抽样标签通过"
{ echo "bucket,key"; for i in $(seq 0 999); do echo "${BUCKETS[1]},obj/$i"; done
  echo "${BUCKETS[1]},missing-key"; } >"$WORK/manifest.csv"
JOB_BAD=$(new_batch)
[ "$(jget "$WORK/api.json" total)" = 1001 ] || die "含失败作业 total ≠ 1001"
echo "   job=$JOB_BAD total=1001，等终态..."
poll_job "$JOB_BAD" "$JOB_TIMEOUT"
[ "$POLLED_STATE" = error ] || die "含失败任务的作业应为 error，实际 $POLLED_STATE"
[ "$(jjob "$WORK/job.json" total)" = 1001 ] && [ "$(jjob "$WORK/job.json" succeeded)" = 1000 ] \
  && [ "$(jjob "$WORK/job.json" failed)" = 1 ] || die "作业计数不符：$(cat "$WORK/job.json")"
aws_api get-object --bucket "${BUCKETS[2]}" --key "m8rep/$JOB_BAD/results.csv" "$WORK/report-bad.csv" >/dev/null
check_report "$WORK/report-bad.csv" 1001 1000 1
OLD_MD5=$(md5sum "$WORK/report-bad.csv" | cut -d' ' -f1)
aws_api put-object --bucket "${BUCKETS[1]}" --key missing-key --body "$WORK/tiny.txt" >/dev/null
code=$(api POST "/jobs/$JOB_BAD/retry-failed")
expect "$code" 200 "retry-failed"
CHILD=$(jget "$WORK/api.json" job_id)
poll_job "$CHILD" "$JOB_TIMEOUT"
[ "$POLLED_STATE" = done ] || die "子作业终态 $POLLED_STATE ≠ done"
[ "$(jjob "$WORK/job.json" total)" = 1 ] && [ "$(jjob "$WORK/job.json" succeeded)" = 1 ] \
  || die "子作业计数不符：$(cat "$WORK/job.json")"
aws_api get-object --bucket "${BUCKETS[2]}" --key "m8rep/$CHILD/results.csv" "$WORK/child.csv" >/dev/null
[ "$(wc -l <"$WORK/child.csv")" = 2 ] || die "子报告应为表头+1 行"
grep -q ',missing-key,.*,Succeeded,' "$WORK/child.csv" || die "子报告未含 succeeded missing-key"
aws_api get-object --bucket "${BUCKETS[2]}" --key "m8rep/$JOB_BAD/results.csv" "$WORK/parent2.csv" >/dev/null
[ "$(md5sum "$WORK/parent2.csv" | cut -d' ' -f1)" = "$OLD_MD5" ] || die "父报告被子作业改写"
echo "   Batch 1000+1 / 报告 / 抽样标签 / retry-failed 通过"

step "3. Inventory：配置 → force-run → manifest/CSV/MD5 断言"
aws_s3 mb "s3://${BUCKETS[3]}"
aws_s3 mb "s3://${BUCKETS[4]}"
aws_api put-bucket-versioning --bucket "${BUCKETS[3]}" --versioning-configuration Status=Enabled >/dev/null
for n in 1 2; do aws_api put-object --bucket "${BUCKETS[3]}" --key a.txt --body "$WORK/tiny.txt" >/dev/null; done
aws_api put-object --bucket "${BUCKETS[3]}" --key b.txt --body "$WORK/tiny.txt" >/dev/null
aws_api delete-object --bucket "${BUCKETS[3]}" --key b.txt >/dev/null   # 1 个 delete marker
INV_ID="inv$RUN"
code=$(api PUT "/buckets/${BUCKETS[3]}/inventory" "{\"id\":\"$INV_ID\",\"frequency\":\"Daily\",\"destination_bucket\":\"${BUCKETS[4]}\",\"destination_prefix\":\"invout\",\"format\":\"CSV\",\"included_object_versions\":\"All\"}")
expect "$code" 204 "PUT inventory 配置"
code=$(api GET "/buckets/${BUCKETS[3]}/inventory")
expect "$code" 200 "GET inventory 配置"
python3 - "$WORK/api.json" "$INV_ID" <<'PY' || die "inventory 配置回读不符"
import json,sys
cfg=[c for c in json.load(open(sys.argv[1])) if c["id"] == sys.argv[2]]
assert len(cfg)==1 and cfg[0]["included_object_versions"]=="All", cfg
PY
code=$(api POST "/buckets/${BUCKETS[3]}/inventory/$INV_ID/run")
expect "$code" 202 "inventory force-run"
RUN_KEY=$(jget "$WORK/api.json" run_key)
IJOB=$(jget "$WORK/api.json" job_id)
poll_job "$IJOB" "$JOB_TIMEOUT"
[ "$POLLED_STATE" = done ] || die "inventory 作业终态 $POLLED_STATE ≠ done：$(cat "$WORK/job.json")"
MKEY="invout/${BUCKETS[3]}/$INV_ID/$RUN_KEY/manifest.json"
aws_api get-object --bucket "${BUCKETS[4]}" --key "$MKEY" "$WORK/manifest.json" >/dev/null
DKEY=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["files"][0]["key"])' "$WORK/manifest.json")
aws_api get-object --bucket "${BUCKETS[4]}" --key "$DKEY" "$WORK/data.csv.gz" >/dev/null
# manifest.files[].md5Checksum 与 AWS Inventory 一致：gzip 后字节 MD5 的 base64。
MD5=$(python3 -c 'import base64,hashlib,sys;print(base64.b64encode(hashlib.md5(open(sys.argv[1],"rb").read()).digest()).decode())' "$WORK/data.csv.gz")
SZ=$(stat -c %s "$WORK/data.csv.gz")
echo "   inventory data: key=$DKEY size=$SZ md5=$MD5"
python3 - "$WORK/manifest.json" "$DKEY" "$MD5" "$SZ" "${BUCKETS[3]}" <<'PY' || die "manifest 断言失败"
import json,sys
m=json.load(open(sys.argv[1]))
assert m["sourceBucket"]==sys.argv[5], m
assert m["fileSchema"].split(",")==["Bucket","Key","VersionId","IsLatest","DeleteMarker","Size","LastModifiedDate","ETag","StorageClass"], m["fileSchema"]
f=m["files"][0]
assert f["key"]==sys.argv[2] and f["md5Checksum"]==sys.argv[3] and int(f["size"])==int(sys.argv[4]), f
PY
gzip -dc "$WORK/data.csv.gz" >"$WORK/data.csv"
python3 - "$WORK/data.csv" "$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))["fileSchema"])' "$WORK/manifest.json")" <<'PY' || die "inventory CSV 断言失败"
import csv,sys
rows=list(csv.reader(open(sys.argv[1], newline="")))
assert ",".join(rows[0])==sys.argv[2], rows[0]
body=rows[1:]
assert len(body)==4, body            # a.txt ×2 版本 + b.txt 版本 + b.txt marker
assert sum(r[4]=="TRUE" for r in body)==1, body
assert all(r[2] for r in body), "VersionId 为空"
assert sorted(r[1] for r in body)==["a.txt","a.txt","b.txt","b.txt"], body
PY
echo "   Inventory manifest/CSV.gz/Counts 通过（4 行 = 3 版本 + 1 marker）"

step "4. Transition + Restore"
TIER_STEP=skipped
if [ "${E2E_M8_SKIP_TIER:-0}" = 1 ]; then
  echo "   SKIP：E2E_M8_SKIP_TIER=1"
else
  code=$(api GET /tiers)
  expect "$code" 200 "GET /tiers"
  TIERS_N=$(python3 -c 'import json,sys;print(len(json.load(open(sys.argv[1]))))' "$WORK/api.json")
  if [ "$TIERS_N" = 0 ]; then
    echo "   SKIP：服务未配置 [[tiers]]（前置：以含 [[tiers]] 的配置重启 oss-server）"
  else
    TIER_BUCKET_REMOTE=$(python3 -c 'import json,sys;print(json.load(open(sys.argv[1]))[0]["bucket"])' "$WORK/api.json")
    if [ -n "${TIER_ENDPOINT:-}" ] && [ -n "${TIER_AK:-}" ] && [ -n "${TIER_SK:-}" ]; then
      # 只清理本次自建的远端桶；已存在的桶不归脚本所有。
      if ! AWS_ACCESS_KEY_ID="$TIER_AK" AWS_SECRET_ACCESS_KEY="$TIER_SK" \
        aws --endpoint-url "$TIER_ENDPOINT" s3api head-bucket --bucket "$TIER_BUCKET_REMOTE" >/dev/null 2>&1; then
        AWS_ACCESS_KEY_ID="$TIER_AK" AWS_SECRET_ACCESS_KEY="$TIER_SK" \
          aws --endpoint-url "$TIER_ENDPOINT" s3 mb "s3://$TIER_BUCKET_REMOTE" >/dev/null 2>&1 || true
        TIER_BUCKET_CREATED="$TIER_BUCKET_REMOTE"
      fi
    fi
    aws_s3 mb "s3://${BUCKETS[5]}"
    printf 'tier-data-%s' "$RUN" >"$WORK/tier.bin"
    aws_api put-object --bucket "${BUCKETS[5]}" --key archived.bin --body "$WORK/tier.bin" >/dev/null
    aws_api put-bucket-lifecycle-configuration --bucket "${BUCKETS[5]}" --lifecycle-configuration \
      '{"Rules":[{"ID":"m8e2e","Filter":{"Prefix":""},"Status":"Enabled","Transitions":[{"Days":0,"StorageClass":"GLACIER"}]}]}' >/dev/null \
      || die "PUT bucket lifecycle（GLACIER, Days=0）失败"
    aws_api get-bucket-lifecycle-configuration --bucket "${BUCKETS[5]}" >"$WORK/lc.json" || die "GET lifecycle 失败"
    python3 - "$WORK/lc.json" <<'PY' || die "lifecycle 回读不符"
import json,sys
r=json.load(open(sys.argv[1]))["Rules"][0]
assert r["ID"]=="m8e2e" and r["Status"]=="Enabled", r
t=r["Transitions"][0] if "Transitions" in r else r["Transition"]
assert int(t["Days"])==0 and t["StorageClass"]=="GLACIER", t
PY
    code=$(api POST /tiers/sweep)
    expect "$code" 200 "POST /tiers/sweep"
    QUEUED=$(jget "$WORK/api.json" queued)
    [ "$QUEUED" -ge 1 ] || die "sweep queued=$QUEUED（应 ≥1）"
    echo "   sweep queued=$QUEUED，等对象转 GLACIER..."
    gl_ok() { [ "$(aws_api head-object --bucket "${BUCKETS[5]}" --key archived.bin --query StorageClass --output text 2>/dev/null)" = GLACIER ]; }
    wait_true "$TIER_TIMEOUT" "转储" gl_ok || die "对象未在 ${TIER_TIMEOUT}s 内转 GLACIER"
    # 归档 GET 必须 403 + InvalidObjectState（aws-cli 只透出错误码，状态码用签名 curl 断言）。
    GCODE=$(curl -sS -o "$WORK/arch.out" -w '%{http_code}' --aws-sigv4 "aws:amz:us-east-1:s3" \
      --user "$AK:$SK" "$ENDPOINT/${BUCKETS[5]}/archived.bin") || die "归档 GET 请求失败"
    [ "$GCODE" = 403 ] || die "归档 GET HTTP $GCODE ≠ 403：$(head -c 300 "$WORK/arch.out")"
    grep -q InvalidObjectState "$WORK/arch.out" || die "归档 GET 未报 InvalidObjectState：$(head -c 300 "$WORK/arch.out")"
    RCODE=$(curl -sS -i --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" -X POST \
      "$ENDPOINT/${BUCKETS[5]}/archived.bin?restore" -H 'x-amz-restore-request: days=1' \
      -o "$WORK/restore.http" -w '%{http_code}') || die "restore-object 请求失败"
    # 线上实际 200：s3s 0.17 的 RestoreObject 生成代码固定按 200 OK 序列化，丢弃
    # S3Response.status（server 层设的 202 只在进程内可见）。语义仍为异步受理。
    [ "$RCODE" = 200 ] || die "restore-object HTTP $RCODE ≠ 200（s3s 0.17 线上实际值）：$(head -c 300 "$WORK/restore.http")"
    grep -qi '^x-amz-restore: ongoing-request="true"' "$WORK/restore.http" || die "restore 响应缺 x-amz-restore ongoing"
    rd_ok() { aws_api get-object --bucket "${BUCKETS[5]}" --key archived.bin "$WORK/restored.out" >/dev/null 2>&1 && cmp -s "$WORK/tier.bin" "$WORK/restored.out"; }
    wait_true "$TIER_TIMEOUT" "恢复" rd_ok || die "对象未在 ${TIER_TIMEOUT}s 内可读/字节不符"
    aws_api delete-bucket-lifecycle --bucket "${BUCKETS[5]}" >/dev/null
    OUT=$(aws_api get-bucket-lifecycle-configuration --bucket "${BUCKETS[5]}" 2>&1) && die "删规则后 GET lifecycle 应 404"
    echo "$OUT" | grep -q NoSuchLifecycleConfiguration || die "删规则后未回 404：$OUT"
    TIER_STEP=ran
    echo "   Transition GLACIER / GET 403 / Restore 200+ongoing / 恢复可读 / 删规则 404 通过"
  fi
fi

step "5. 回归：标准 S3 / STS / M4 生命周期"
aws_s3 mb "s3://${BUCKETS[7]}"
printf 'std-%s' "$RUN" >"$WORK/std.txt"
aws_s3 cp "$WORK/std.txt" "s3://${BUCKETS[7]}/std.txt" >/dev/null
aws_s3 ls "s3://${BUCKETS[7]}/" | grep -q std.txt || die "标准 List 未含 std.txt"
aws_s3 cp "s3://${BUCKETS[7]}/std.txt" "$WORK/std.back" >/dev/null
cmp "$WORK/std.txt" "$WORK/std.back" || die "标准路径读回不一致"
aws_s3 rm "s3://${BUCKETS[7]}/std.txt" >/dev/null
aws_s3 ls "s3://${BUCKETS[7]}/" | grep -q std.txt && die "标准删除未生效"
GST=$(aws --endpoint-url "$ENDPOINT" sts get-session-token --duration-seconds 900 --output json)
eval "$(python3 - "$GST" <<'PY'
import json,sys,shlex
c=json.loads(sys.argv[1])["Credentials"]
print("STS_AK="+shlex.quote(c["AccessKeyId"]))
print("STS_SK="+shlex.quote(c["SecretAccessKey"]))
print("STS_TOK="+shlex.quote(c["SessionToken"]))
PY
)"
AWS_ACCESS_KEY_ID="$STS_AK" AWS_SECRET_ACCESS_KEY="$STS_SK" AWS_SESSION_TOKEN="$STS_TOK" \
  aws --endpoint-url "$ENDPOINT" s3api list-buckets >/dev/null || die "GetSessionToken 凭据不可用"
rm -f "$WORK/m4.txt"
aws_s3 mb "s3://${BUCKETS[6]}"
aws_api put-object --bucket "${BUCKETS[6]}" --key m4.txt --body "$WORK/tiny.txt" >/dev/null
code=$(api PUT "/buckets/${BUCKETS[6]}/lifecycle" '[{"prefix":"","expiration_days":0}]')
expect "$code" 204 "PUT M4 生命周期（expiration_days=0）"
m4_ok() { ! aws_api head-object --bucket "${BUCKETS[6]}" --key m4.txt >/dev/null 2>&1; }
if wait_true "$M4_TIMEOUT" "M4 过期" m4_ok; then
  echo "   M4 生命周期到期删除通过"
else
  die "M4 生命周期 ${M4_TIMEOUT}s 内未删除（worker 间隔 OSS_LIFECYCLE_INTERVAL_SECS？）"
fi
echo "   标准 S3 / STS / M4 通过"

if [ "${E2E_M8_REPL_SMOKE:-0}" = 1 ]; then
  step "6. M6/M7 复制回归（E2E_M8_REPL_SMOKE=1）"
  BIN="${OSS_BIN:-$PWD/target/debug/oss-server}"
  [ -x "$BIN" ] || die "缺少 oss-server 二进制：$BIN"
  OSS_BIN="$BIN" bash tests/e2e_replication.sh
  OSS_BIN="$BIN" bash tests/e2e_sites.sh
else
  echo; echo "== 6. M6/M7 复制回归：SKIP（默认；E2E_M8_REPL_SMOKE=1 + OSS_BIN 启用）=="
fi

echo
echo "PASS e2e_m8：AP / Batch / Inventory / Tier=$TIER_STEP / 回归；总耗时 $(( $(date +%s) - T0 ))s"
