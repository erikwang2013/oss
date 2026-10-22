#!/usr/bin/env bash
# 验收：S3 兼容性（aws-cli + rclone）。M1 基础 + M2a 的 multipart / Range / 元数据。
# 前置：本机装了 aws-cli 与 rclone。
# 可选长跑（13b，默认跳过）：E2E_LONG=1 启用设计验收 #3（rclone 并发 + 中断重试），
# E2E_LONG_N / E2E_LONG_MB 调规模（默认 100 × 100 MB，分钟级，人工触发）。
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
AK="${AK:-AKIA_TEST_ACCESS_KEY}"
SK="${SK:-SK_TEST_SECRET_KEY_0001}"
BUCKET="e2e-$(date +%s)"
# 权限段（步骤 15）的两个桶：授权桶与未授权桶；在 EXIT trap 里一并兜底清理
PERM_BUCKET="zzz-perm-read-$(date +%s)"
OTHER_BUCKET="zzz-perm-other-$(date +%s)"
POLICY_BUCKET="pol-$(date +%s)"
POLICY2_BUCKET="pol2-$(date +%s)"
# 匿名（无凭据）策略放行面（步骤 17f）
ANON_BUCKET="anon-$(date +%s)"
# M5 段（步骤 18）的两个桶：SSE 与对象锁
SSE_BUCKET="sse-$(date +%s)"
LOCK_BUCKET="lock-$(date +%s)"
# M8 前置（步骤 20）：对象标签 + CopyObject DIRECTIVE
TAG_BUCKET="tag-$(date +%s)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; for b in "$BUCKET" "$PERM_BUCKET" "$OTHER_BUCKET" "$POLICY_BUCKET" "$POLICY2_BUCKET" "$ANON_BUCKET" "$SSE_BUCKET" "$LOCK_BUCKET" "$TAG_BUCKET"; do aws --endpoint-url "$ENDPOINT" s3 rb "s3://$b" --force >/dev/null 2>&1 || true; done' EXIT

export AWS_ACCESS_KEY_ID="$AK" AWS_SECRET_ACCESS_KEY="$SK" AWS_DEFAULT_REGION=us-east-1
aws_s3() { aws --endpoint-url "$ENDPOINT" s3 "$@"; }

echo "== 1. 建桶 =="
aws_s3 mb "s3://$BUCKET"

echo "== 2. 上传（走 s3s 的 PutObject）=="
head -c 1048576 /dev/urandom > "$WORK/one.bin"
aws_s3 cp "$WORK/one.bin" "s3://$BUCKET/one.bin"

echo "== 3. 列举 =="
aws_s3 ls "s3://$BUCKET/" | tee "$WORK/ls.txt"
grep -q "one.bin" "$WORK/ls.txt"

echo "== 4. 下载并比对（字节一致）=="
aws_s3 cp "s3://$BUCKET/one.bin" "$WORK/back.bin"
cmp "$WORK/one.bin" "$WORK/back.bin"

echo "== 5. head-object 的 ETag 必须是整份 MD5 =="
LOCAL_MD5=$(md5sum "$WORK/one.bin" | cut -d' ' -f1)
REMOTE_ETAG=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key one.bin --query ETag --output text | tr -d '"')
[ "$LOCAL_MD5" = "$REMOTE_ETAG" ] || { echo "ETag 不符: $LOCAL_MD5 vs $REMOTE_ETAG"; exit 1; }

echo "== 6. 前缀与分隔符 =="
aws_s3 cp "$WORK/one.bin" "s3://$BUCKET/dir/a.bin"
aws --endpoint-url "$ENDPOINT" s3api list-objects-v2 --bucket "$BUCKET" --delimiter / --query 'CommonPrefixes[].Prefix' --output text | grep -q "dir/"
# ListObjects v1（无 list-type）：delimiter 聚合 + marker 排他分页
aws --endpoint-url "$ENDPOINT" s3api list-objects --bucket "$BUCKET" --delimiter / --query 'CommonPrefixes[].Prefix' --output text | grep -q "dir/"
V1_FIRST=$(aws --endpoint-url "$ENDPOINT" s3api list-objects --bucket "$BUCKET" --max-keys 1 --query 'Contents[0].Key' --output text)
[ "$V1_FIRST" = "dir/a.bin" ] || { echo "v1 首键不符: $V1_FIRST"; exit 1; }
V1_NEXT=$(aws --endpoint-url "$ENDPOINT" s3api list-objects --bucket "$BUCKET" --marker "$V1_FIRST" --max-keys 1 --query 'Contents[0].Key' --output text)
[ "$V1_NEXT" = "one.bin" ] || { echo "v1 marker 非排他: $V1_NEXT"; exit 1; }

echo "== 7. CopyObject =="
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$BUCKET" --key copy.bin --copy-source "$BUCKET/one.bin" >/dev/null
aws_s3 ls "s3://$BUCKET/copy.bin" | grep -q copy.bin

echo "== 8. 批量删除 =="
aws --endpoint-url "$ENDPOINT" s3api delete-objects --bucket "$BUCKET" \
  --delete "Objects=[{Key=one.bin},{Key=copy.bin}],Quiet=false" >/dev/null
aws_s3 ls "s3://$BUCKET/" | tee "$WORK/ls2.txt"
grep -q "dir/" "$WORK/ls2.txt"   # 未被删除的键仍在
if grep -q -e one.bin -e copy.bin "$WORK/ls2.txt"; then echo "批量删除未生效"; exit 1; fi

echo "== 9. rclone 对照 =="
export RCLONE_CONFIG_OSS_TYPE=s3
export RCLONE_CONFIG_OSS_PROVIDER=Other
export RCLONE_CONFIG_OSS_ACCESS_KEY_ID="$AK"
export RCLONE_CONFIG_OSS_SECRET_ACCESS_KEY="$SK"
export RCLONE_CONFIG_OSS_ENDPOINT="$ENDPOINT"           # 必须带 scheme：不带时 rclone 默认 https
export RCLONE_CONFIG_OSS_FORCE_PATH_STYLE=true
export RCLONE_CONFIG_OSS_LIST_VERSION=2                 # provider=Other 默认发 v1；M1 只实现 ListObjectsV2
rclone lsf oss:"$BUCKET"
rclone copyto "$WORK/one.bin" oss:"$BUCKET/rclone.bin"
rclone cat oss:"$BUCKET/rclone.bin" | cmp - "$WORK/one.bin"   # rclone 读回内容与本地一致
if rclone lsf oss:"$BUCKET" | grep -qx one.bin; then echo "删除未生效"; exit 1; fi

echo "== 10. Multipart 直传（≥8 MiB 自动分片；M2a）=="
head -c 10485760 /dev/urandom > "$WORK/big.bin"     # 10 MiB > aws-cli 默认阈值 8 MiB
aws_s3 cp "$WORK/big.bin" "s3://$BUCKET/big.bin"
aws_s3 cp "s3://$BUCKET/big.bin" "$WORK/big.back"
cmp "$WORK/big.bin" "$WORK/big.back"
BIG_ETAG=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key big.bin --query ETag --output text | tr -d '"')
[[ "$BIG_ETAG" =~ -[0-9]+$ ]] || { echo "multipart ETag 应带 -N 后缀: $BIG_ETAG"; exit 1; }

echo "== 11. Range GET（206 + Content-Range + 字节比对）=="
aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$BUCKET" --key big.bin \
  --range bytes=1048576-2097151 "$WORK/range.bin" | grep -q '"ContentRange": "bytes 1048576-2097151/10485760"'
dd if="$WORK/big.bin" of="$WORK/range.expect" bs=1048576 skip=1 count=1 status=none
cmp "$WORK/range.bin" "$WORK/range.expect"

echo "== 11b. 条件请求（304 / 412）=="
COND_ETAG=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key big.bin --query ETag --output text | tr -d '"')
# 304：aws-cli 2.37 实测对 HEAD 的 304 **不当错误**——退出 0、输出是空壳（无 ContentLength/ETag/
# LastModified，仅 "Metadata":{}）；正常 200 的 head-object 必带 ContentLength/ETag。故判别
# 「成功且输出无 ContentLength/ETag」，不能按「非零退出 + grep 状态码」写（会假红）。
OUT=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key big.bin --if-none-match "\"$COND_ETAG\"" 2>&1) \
  || { echo "If-None-Match 命中应 304（空输出），却报错: $OUT"; exit 1; }
if echo "$OUT" | grep -qE 'ContentLength|ETag'; then
  echo "If-None-Match 命中应 304，却返回了 200 的完整输出: $OUT"; exit 1
fi
# 412：If-Match 错值（aws-cli 对 412 报错退出，stderr 含 "(412) ... Precondition Failed"，实测）
OUT=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key big.bin --if-match '"deadbeef"' 2>&1) \
  && { echo "If-Match 错值应 412，却成功: $OUT"; exit 1; }
echo "$OUT" | grep -q '412' || { echo "未回 412: $OUT"; exit 1; }
# 对照：If-Match 命中 → 200 完整输出（ContentLength = big.bin 的 10485760）
OUT=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key big.bin --if-match "\"$COND_ETAG\"" --query ContentLength --output text 2>&1)
echo "$OUT" | grep -qx '10485760' || { echo "If-Match 命中的 200 输出异常: ContentLength=$OUT"; exit 1; }

echo "== 11c. 校验和（x-amz-checksum-*）=="
# 正确值：aws-cli 自动计算 SHA256 并随请求头发送 → 提交成功，head-object 回写
aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$BUCKET" --key ck.bin --body "$WORK/one.bin" \
  --checksum-algorithm SHA256 >/dev/null
CK_SHA=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key ck.bin --query ChecksumSHA256 --output text)
[ -n "$CK_SHA" ] && [ "$CK_SHA" != "None" ] || { echo "head-object 应回写 ChecksumSHA256"; exit 1; }
# 错值：伪造 base64 → BadDigest，且对象不得落盘（提交点前 fail-closed）
OUT=$(aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$BUCKET" --key ck-bad.bin --body "$WORK/one.bin" \
  --checksum-sha256 "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" 2>&1) \
  && { echo "错校验和应被拒，却成功: $OUT"; exit 1; }
echo "$OUT" | grep -qi "BadDigest" || { echo "应报 BadDigest: $OUT"; exit 1; }
if aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key ck-bad.bin >/dev/null 2>&1; then
  echo "校验失败不得留下对象"; exit 1
fi
# 带存储校验和的对象做 range GET：aws-cli 默认响应校验（when_supported）必须通过——
# 206 若回写整对象校验和，客户端会拿 2 字节局部内容对整对象校验而失败
# （实测故障原文："Expected checksum … did not match calculated checksum"；修复后此断言钉住 206 不回写）。
aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$BUCKET" --key ck.bin \
  --range bytes=0-1 "$WORK/ck-range.bin" >/dev/null

echo "== 11d. 多区间 Range → 降级 200 整对象（S3 面剥离 Range；鉴权仍先拦）=="
# 鉴权**先于**头部解析：未签名请求仍被鉴权层拦为 403（两条契约分别钉住）。
# 冻结裁决：多区间 Range 不实现 multipart/byteranges —— router 在 S3 fallback 层剥离
# Range 头，带鉴权的请求降级为 200 + 整对象（单区间语义不变）。
UNAUTH_BODY=$(curl -s -H 'Range: bytes=0-1,3-4' "$ENDPOINT/$BUCKET/big.bin")
echo "$UNAUTH_BODY" | grep -q 'AccessDenied' || { echo "未签名请求应被鉴权层拦为 AccessDenied: $UNAUTH_BODY"; exit 1; }
UNAUTH=$(curl -s -o /dev/null -w '%{http_code}' -H 'Range: bytes=0-1,3-4' "$ENDPOINT/$BUCKET/big.bin")
[ "$UNAUTH" = "403" ] || { echo "未签名请求应 403，实际 $UNAUTH"; exit 1; }
# 预签名 URL 的 SigV4 只签 host（X-Amz-SignedHeaders=host）：Range 不在签名内，剥离后
# 签名仍有效 → 200 整对象。
PRESIGNED=$(aws --endpoint-url "$ENDPOINT" s3 presign "s3://$BUCKET/big.bin" --expires-in 120)
MULTI_CODE=$(curl -s -o "$WORK/multi-range.out" -w '%{http_code}' -H 'Range: bytes=0-1,3-4' "$PRESIGNED")
[ "$MULTI_CODE" = "200" ] || { echo "预签名多区间 Range 应 200，实际 $MULTI_CODE"; exit 1; }
cmp "$WORK/big.bin" "$WORK/multi-range.out" || { echo "多区间 Range 应降级整对象 200"; exit 1; }
# 已知边界（记档）：aws-cli 把 Range 计入 SigV4 SignedHeaders；头在验签前被剥离 →
# 403 SignatureDoesNotMatch（AWS 原生 200；要支持签 Range 的客户端需 s3s 内部补丁）。
OUT=$(aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$BUCKET" --key big.bin --range 'bytes=0-1,3-4' "$WORK/multi-range-signed.out" 2>&1) \
  && { echo "签 Range 的 aws-cli 多区间应被签名校验拒绝，却成功: $OUT"; exit 1; }
echo "$OUT" | grep -q 'SignatureDoesNotMatch' || { echo "应报 SignatureDoesNotMatch: $OUT"; exit 1; }
# HEAD + Range：200 + 区间长度（1 MiB）
HEADLEN=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key big.bin --range 'bytes=1048576-2097151' --query ContentLength --output text)
[ "$HEADLEN" = "1048576" ] || { echo "HEAD+Range Content-Length 应为 1048576，实际 $HEADLEN"; exit 1; }

echo "== 12. 用户元数据往返（--metadata → head-object 可见）=="
aws_s3 cp "$WORK/one.bin" "s3://$BUCKET/meta.bin" --metadata foo=bar,lang=zh
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key meta.bin --query 'Metadata.foo' --output text | grep -qx bar
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key meta.bin --query 'Metadata.lang' --output text | grep -qx zh

echo "== 13. rclone 大文件（强制走 multipart）=="
export RCLONE_CONFIG_OSS_UPLOAD_CUTOFF=5M            # rclone 默认 cutoff 200Mi 会走单 PUT，压低以强制分片
export RCLONE_CONFIG_OSS_CHUNK_SIZE=5M
rclone copyto "$WORK/big.bin" oss:"$BUCKET/rclone-big.bin"
rclone cat oss:"$BUCKET/rclone-big.bin" | cmp - "$WORK/big.bin"

echo "== 13b. E2E_LONG 长跑：并发上传 + 中断重试（设计验收 #3；默认跳过）=="
if [ "${E2E_LONG:-0}" != "1" ]; then
  echo "   跳过（分钟级，人工触发）：E2E_LONG=1 启用，E2E_LONG_N/E2E_LONG_MB 调规模（默认 100×100 MB）"
else
  N="${E2E_LONG_N:-100}"; MB="${E2E_LONG_MB:-100}"
  [ "$N" -ge 3 ] || { echo "   需要 E2E_LONG_N >= 3（中断重试要有可观测的中间态）"; exit 1; }
  LONG="$WORK/long"; mkdir -p "$LONG"
  echo "   生成 $N × ${MB} MB（/dev/urandom）"
  for i in $(seq 1 "$N"); do dd if=/dev/urandom of="$LONG/f$i.bin" bs=1M count="$MB" status=none; done
  ( cd "$LONG" && md5sum ./*.bin > "$WORK/long.md5" )   # 本地基准清单（末尾逐对象读回比对）

  # 中断重试：先 transfers=1 + bwlimit 慢传（逐个对象出现，中断窗口足够大），
  # 看到第一个对象落地即 kill -9；再 transfers=8 并发续跑（rclone copy 跳过已传完的对象）。
  # 注：被 kill 的在途分片上传留在服务端，续跑会重新上传该对象——正是「中断重试」要覆盖的形态。
  echo "   中断重试：慢传至中途 kill -9 rclone，再并发续跑"
  rclone copy "$LONG" "oss:$BUCKET/long/" --transfers 1 --bwlimit 10M >/dev/null 2>&1 &
  RPID=$!
  for _ in $(seq 1 600); do
    C=$( { rclone lsf "oss:$BUCKET/long/" 2>/dev/null || true; } | wc -l )
    { [ "$C" -gt 0 ] && [ "$C" -lt "$N" ]; } && break
    kill -0 "$RPID" 2>/dev/null || break
    sleep 0.2
  done
  kill -9 "$RPID" 2>/dev/null || true
  wait "$RPID" 2>/dev/null || true
  PARTIAL=$( { rclone lsf "oss:$BUCKET/long/" || true; } | wc -l )
  echo "   中断时远端已有 $PARTIAL/$N 个对象"
  [ "$PARTIAL" -lt "$N" ] || echo "   注意：链路太快未能在中途中断（续跑语义仍由下方全量断言覆盖）"

  rclone copy "$LONG" "oss:$BUCKET/long/" --transfers 8 --checkers 8
  CNT=$( { rclone lsf "oss:$BUCKET/long/" || true; } | wc -l )
  [ "$CNT" -eq "$N" ] || { echo "   长跑：远端对象数 $CNT != $N"; exit 1; }

  # 全部校验和一致：逐对象 cat 读回，与本地 md5 比对
  BAD=0
  while read -r H F; do
    R=$(rclone cat "oss:$BUCKET/long/${F##*/}" | md5sum | cut -d' ' -f1)
    [ "$R" = "$H" ] || { echo "   校验失败: $F 本地 $H 远端 $R"; BAD=1; }
  done < "$WORK/long.md5"
  [ "$BAD" -eq 0 ] || exit 1
  echo "   长跑通过：$N×${MB}MB 全部读回校验和一致（含中断重试）"
fi

echo "== 14. 删桶 =="
aws_s3 rb "s3://$BUCKET" --force

echo "== 15. 子账户权限（三档读/写/删在 S3 面的执行）=="
# 控制台 API（curl）：管理员登录 → POST /users 建子账户 → PUT /users/{id}/permissions
# 全量替换权限行（先只授「读」）；子账户自签第二把 AK/SK（POST /access-keys）后走 aws-cli：
#   授权桶 ls/get 放行；只授读时上传 403；未授权桶 ls 403 AccessDenied；匿名 403；
#   补授写后上传放行；ListBuckets 只见授权桶（服务端按可见集过滤）；
#   停用账号 → 同一 AK 立即 403（InvalidAccessKeyId，认证层拦截）→ 旧 JWT 打控制台端点 401
#   （含 POST /buckets、POST /access-keys 两条副作用口）→ 启用恢复（15h，排在清理前）。
# 管理员账号默认 erik/changeme（README 引导段），可用 ADMIN_USER/ADMIN_PASSWORD 覆盖。
ADMIN_USER="${ADMIN_USER:-erik}"; ADMIN_PASSWORD="${ADMIN_PASSWORD:-changeme}"
ADMIN_TOKEN=$(curl -s -X POST "$ENDPOINT/api/v1/login" -H 'content-type: application/json' \
  -d "{\"username\":\"$ADMIN_USER\",\"password\":\"$ADMIN_PASSWORD\"}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
[ -n "$ADMIN_TOKEN" ] || { echo "管理员登录失败"; exit 1; }

aws_s3 mb "s3://$PERM_BUCKET" >/dev/null
aws_s3 mb "s3://$OTHER_BUCKET" >/dev/null
printf 'perm-read\n' > "$WORK/perm.bin"
aws_s3 cp "$WORK/perm.bin" "s3://$PERM_BUCKET/perm.bin" >/dev/null

SUB="sub-$(date +%s)"; SUB_PW="$SUB-pw-1234"
SUB_ID=$(curl -s -X POST "$ENDPOINT/api/v1/users" -H "authorization: Bearer $ADMIN_TOKEN" \
  -H 'content-type: application/json' -d "{\"username\":\"$SUB\",\"password\":\"$SUB_PW\"}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
[ -n "$SUB_ID" ] || { echo "建子账户失败"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/users/$SUB_ID/permissions" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d "[{\"bucket\":\"$PERM_BUCKET\",\"read\":true}]")
[ "$CODE" = "204" ] || { echo "授「读」应 204，实际 $CODE"; exit 1; }

SUB_TOKEN=$(curl -s -X POST "$ENDPOINT/api/v1/login" -H 'content-type: application/json' \
  -d "{\"username\":\"$SUB\",\"password\":\"$SUB_PW\"}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
SUB_AK="SUB$(date +%s)ACCESSKEY"; SUB_SK="subsecret$(date +%s)0123456789"
KOUT=$(curl -s -X POST "$ENDPOINT/api/v1/access-keys" -H "authorization: Bearer $SUB_TOKEN" \
  -H 'content-type: application/json' -d "{\"access_key\":\"$SUB_AK\",\"secret_key\":\"$SUB_SK\"}")
echo "$KOUT" | grep -q "$SUB_AK" || { echo "子账户签发 AK 失败: $KOUT"; exit 1; }

# 子账户的 S3 客户端：逐调用覆盖 AK/SK（脚本顶部 export 的是管理员的）
sub_aws() { AWS_ACCESS_KEY_ID="$SUB_AK" AWS_SECRET_ACCESS_KEY="$SUB_SK" aws --endpoint-url "$ENDPOINT" "$@"; }

echo "--- 15a. 授权桶：ls / get 放行 ---"
sub_aws s3 ls "s3://$PERM_BUCKET/" | tee "$WORK/sub-ls.txt"
grep -q perm.bin "$WORK/sub-ls.txt"
sub_aws s3 cp "s3://$PERM_BUCKET/perm.bin" "$WORK/perm.back"
cmp "$WORK/perm.bin" "$WORK/perm.back"

echo "--- 15b. 只授读：上传必须 403（aws-cli 报 (AccessDenied) 且退出非 0）---"
OUT=$(sub_aws s3 cp "$WORK/perm.bin" "s3://$PERM_BUCKET/denied.bin" 2>&1) \
  && { echo "只授读仍可上传: $OUT"; exit 1; }
echo "$OUT" | grep -q '(AccessDenied)' || { echo "上传应报 AccessDenied: $OUT"; exit 1; }

echo "--- 15c. 未授权桶：ls 必须 403 AccessDenied ---"
OUT=$(sub_aws s3 ls "s3://$OTHER_BUCKET/" 2>&1) \
  && { echo "未授权桶仍可列举: $OUT"; exit 1; }
echo "$OUT" | grep -q '(AccessDenied)' || { echo "未授权桶应报 AccessDenied: $OUT"; exit 1; }

echo "--- 15d. 匿名（无 AK）仍 403 ---"
ANON=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/$PERM_BUCKET/perm.bin")
[ "$ANON" = "403" ] || { echo "匿名应 403，实际 $ANON"; exit 1; }

echo "--- 15e. 管理员补授「写」→ 上传放行 ---"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/users/$SUB_ID/permissions" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d "[{\"bucket\":\"$PERM_BUCKET\",\"read\":true,\"write\":true}]")
[ "$CODE" = "204" ] || { echo "补授写应 204，实际 $CODE"; exit 1; }
sub_aws s3 cp "$WORK/perm.bin" "s3://$PERM_BUCKET/sub-up.bin"
sub_aws s3 ls "s3://$PERM_BUCKET/" | grep -q sub-up.bin

echo "--- 15f. ListBuckets 过滤：只见授权桶 ---"
sub_aws s3 ls | tee "$WORK/sub-buckets.txt"
grep -q "$PERM_BUCKET" "$WORK/sub-buckets.txt"
if grep -q "$OTHER_BUCKET" "$WORK/sub-buckets.txt"; then echo "ListBuckets 泄漏未授权桶"; exit 1; fi

echo "--- 15h. 停用账号：同一 AK 立即 403（InvalidAccessKeyId），启用后恢复 ---"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/users/$SUB_ID/status" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d '{"active":false}')
[ "$CODE" = "204" ] || { echo "停用应 204，实际 $CODE"; exit 1; }
OUT=$(sub_aws s3 ls 2>&1) && { echo "停用后 AK 仍可用: $OUT"; exit 1; }
echo "$OUT" | grep -q '(InvalidAccessKeyId)' || { echo "停用后应报 InvalidAccessKeyId: $OUT"; exit 1; }
# 停用期间：旧 JWT 打控制台「遮面端点」也必须 401（评审 P1 回归）——挑两条带副作用的：
# POST /buckets（防「拒绝但已建出桶」，启用后复查）与 POST /access-keys（防停用期铸新 AK）
DIS_B="zzz-dis-sub-$SUB_ID"
CJ=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$ENDPOINT/api/v1/buckets" \
  -H "authorization: Bearer $SUB_TOKEN" -H 'content-type: application/json' -d "{\"name\":\"$DIS_B\"}")
[ "$CJ" = "401" ] || { echo "停用后旧 JWT 建桶应 401，实际 $CJ"; exit 1; }
CJ=$(curl -s -o /dev/null -w '%{http_code}' -X POST "$ENDPOINT/api/v1/access-keys" \
  -H "authorization: Bearer $SUB_TOKEN" -H 'content-type: application/json' \
  -d "{\"access_key\":\"DISABLED${SUB_ID}ACCESSKEY\",\"secret_key\":\"disabled-secret-0123456789\"}")
[ "$CJ" = "401" ] || { echo "停用后旧 JWT 铸 AK 应 401，实际 $CJ"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/users/$SUB_ID/status" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d '{"active":true}')
[ "$CODE" = "204" ] || { echo "启用应 204，实际 $CODE"; exit 1; }
sub_aws s3 ls | grep -q "$PERM_BUCKET" || { echo "启用后 AK 未恢复"; exit 1; }
if aws_s3 ls | grep -q "$DIS_B"; then echo "停用期拒绝却建出了桶"; exit 1; fi

echo "--- 15g. 清理：删两个桶 + 删子账户 ---"
aws_s3 rb "s3://$PERM_BUCKET" --force >/dev/null
aws_s3 rb "s3://$OTHER_BUCKET" --force >/dev/null
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$ENDPOINT/api/v1/users/$SUB_ID" \
  -H "authorization: Bearer $ADMIN_TOKEN")
[ "$CODE" = "204" ] || { echo "删子账户应 204，实际 $CODE"; exit 1; }

echo "== 16. 版本控制（M4 切片 3：开启/三版本/点名读/delete marker/回滚/删版本）=="
VB="ver-$(date +%s)"
aws_s3 mb "s3://$VB"
S3API="aws --endpoint-url $ENDPOINT s3api"
# 16a. 开启 + 回读
aws --endpoint-url "$ENDPOINT" s3api put-bucket-versioning --bucket "$VB" \
  --versioning-configuration Status=Enabled || { echo "开版本控制失败"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api get-bucket-versioning --bucket "$VB" | grep -q Enabled \
  || { echo "get-bucket-versioning 非 Enabled"; exit 1; }
# 16b. PUT ×3 = 3 版本；普通 GET = 最新
echo v1 > "$WORK/v1.txt"; echo v2v2 > "$WORK/v2.txt"; echo v3v3v3 > "$WORK/v3.txt"
aws_s3 cp "$WORK/v1.txt" "s3://$VB/k.txt" >/dev/null
aws_s3 cp "$WORK/v2.txt" "s3://$VB/k.txt" >/dev/null
aws_s3 cp "$WORK/v3.txt" "s3://$VB/k.txt" >/dev/null
count_versions() {
  aws --endpoint-url "$ENDPOINT" s3api list-object-versions --bucket "$VB" | python3 -c '
import json,sys
d=json.load(sys.stdin)
print(len(d.get("Versions") or []) + len(d.get("DeleteMarkers") or []))'
}
N=$(count_versions)
[ "$N" = "3" ] || { echo "应有 3 版本，实际 $N"; exit 1; }
aws_s3 cp "s3://$VB/k.txt" "$WORK/k.back" >/dev/null
cmp "$WORK/k.back" "$WORK/v3.txt" || { echo "普通 GET 应回最新版本"; exit 1; }
# 16c. 点名读中间版本（列表新→旧；[1] = v2）
V2ID=$(aws --endpoint-url "$ENDPOINT" s3api list-object-versions --bucket "$VB" \
  --query 'Versions[1].VersionId' --output text)
[ -n "$V2ID" ] && [ "$V2ID" != "None" ] || { echo "取 v2 版本号失败"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$VB" --key k.txt \
  --version-id "$V2ID" "$WORK/v2.back" >/dev/null
cmp "$WORK/v2.back" "$WORK/v2.txt" || { echo "点名读 v2 不符"; exit 1; }
# 16d. 无版本号删除 → delete marker：GET 失败、对象列表空、版本列表见 DeleteMarkers
aws_s3 rm "s3://$VB/k.txt" >/dev/null
if aws_s3 cp "s3://$VB/k.txt" "$WORK/gone" >/dev/null 2>&1; then echo "marker 后 GET 应失败"; exit 1; fi
aws_s3 ls "s3://$VB/" | grep -q 'k.txt' && { echo "marker 后对象列表不应见 k.txt"; exit 1; }
DMID=$(aws --endpoint-url "$ENDPOINT" s3api list-object-versions --bucket "$VB" \
  --query 'DeleteMarkers[0].VersionId' --output text)
[ -n "$DMID" ] && [ "$DMID" != "None" ] || { echo "未见 DeleteMarkers"; exit 1; }
N=$(count_versions); [ "$N" = "4" ] || { echo "marker 后应 4 条（3 版本+1 marker），实际 $N"; exit 1; }
# 16e. 点名删 marker → 恢复（读回 v3）
aws --endpoint-url "$ENDPOINT" s3api delete-object --bucket "$VB" --key k.txt --version-id "$DMID" >/dev/null
aws_s3 cp "s3://$VB/k.txt" "$WORK/back2" >/dev/null
cmp "$WORK/back2" "$WORK/v3.txt" || { echo "删 marker 后应恢复 v3"; exit 1; }
# 16f. 回滚：把 v2 复制成新版本（?versionId 旧版本 → copy 出新版本）
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$VB" --key k.txt \
  --copy-source "$VB/k.txt?versionId=$V2ID" >/dev/null
aws_s3 cp "s3://$VB/k.txt" "$WORK/rolled" >/dev/null
cmp "$WORK/rolled" "$WORK/v2.txt" || { echo "回滚 copy 后应见 v2 内容"; exit 1; }
N=$(count_versions); [ "$N" = "4" ] || { echo "回滚后应 4 版本（v3+v2+v1+回滚版），实际 $N"; exit 1; }
# 16g. 点名删光所有版本 → 键彻底消失
VIDS=$(aws --endpoint-url "$ENDPOINT" s3api list-object-versions --bucket "$VB" | python3 -c '
import json,sys
d=json.load(sys.stdin)
print(" ".join([v["VersionId"] for v in (d.get("Versions") or [])] + [m["VersionId"] for m in (d.get("DeleteMarkers") or [])]))')
for vid in $VIDS; do
  aws --endpoint-url "$ENDPOINT" s3api delete-object --bucket "$VB" --key k.txt --version-id "$vid" >/dev/null
done
N=$(count_versions); [ "$N" = "0" ] || { echo "版本全删后应 0，实际 $N"; exit 1; }
aws_s3 rb "s3://$VB" --force >/dev/null
echo "   版本控制段通过（3 版本/点名读/marker/回滚/删版本）"

echo "== 17. 桶策略 + 审计（M4 切片 7：Deny 优先（含压过 admin 与授权行）/校验 400/审计查询/清策略恢复）=="
PB="$POLICY_BUCKET"
aws_s3 mb "s3://$PB" >/dev/null
printf 'open\n' > "$WORK/open.txt"; printf 'sec\n' > "$WORK/sec.txt"
aws_s3 cp "$WORK/open.txt" "s3://$PB/open.txt" >/dev/null
aws_s3 cp "$WORK/sec.txt" "s3://$PB/secret/x.txt" >/dev/null

# 拒信口径：HEAD 请求（aws s3 cp 下载会先 HeadObject）无响应体 → aws 打 "(403) ... Forbidden"，
# 带体请求（DELETE/GET）才有 "(AccessDenied)"。两种形态都接受——语义不变：仍要求命令非零退出 + 拒信。
deny_msg_ok() { echo "$1" | grep -qE 'AccessDenied|\(403\)|Forbidden'; }

echo "--- 17a. 校验 400（畸形 Action / 顶层裸数组 / Effect 拼错）+ 写策略 204 + 回读 ---"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"Statement":[{"Effect":"Allow","Action":1}]}')
[ "$CODE" = "400" ] || { echo "畸形策略应 400，实际 $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d '[]')
[ "$CODE" = "400" ] || { echo "顶层非对象应 400，实际 $CODE"; exit 1; }
# Effect 拼错必须 400（拼错 = 语句静默空转，正是「不静默忽略」要拦的）
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"Statement":[{"Effect":"Dney","Principal":{"AWS":"*"},"Action":"s3:*","Resource":"arn:aws:s3:::'"$PB"'/*"}]}')
[ "$CODE" = "400" ] || { echo "Effect 拼错应 400，实际 $CODE"; exit 1; }
# 给不存在桶写策略必须 404（防同名桶日后创建即继承）
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/zzz-nope-$(date +%s)/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"Statement":[]}')
[ "$CODE" = "404" ] || { echo "不存在桶写策略应 404，实际 $CODE"; exit 1; }
POL='{"Statement":[
  {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$PB"'/secret*"},
  {"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:DeleteObject","Resource":"arn:aws:s3:::'"$PB"'/secret*"}]}'
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d "$POL")
[ "$CODE" = "204" ] || { echo "写策略应 204，实际 $CODE"; exit 1; }
curl -s "$ENDPOINT/api/v1/buckets/$PB/policy" -H "authorization: Bearer $ADMIN_TOKEN" | grep -q secret \
  || { echo "策略回读缺 secret"; exit 1; }

echo "--- 17b. Deny 生效（显式 Deny 压过桶主/管理员——AWS 语义）：secret 403、域外放行 ---"
OUT=$(aws_s3 cp "s3://$PB/secret/x.txt" "$WORK/pol-gone" 2>&1) && { echo "Deny 前缀仍可读: $OUT"; exit 1; }
deny_msg_ok "$OUT" || { echo "Deny 前缀应报 AccessDenied/403: $OUT"; exit 1; }
aws_s3 cp "s3://$PB/open.txt" "$WORK/pol-open" >/dev/null || { echo "Deny 域外应放行"; exit 1; }
cmp "$WORK/pol-open" "$WORK/open.txt"

echo "--- 17b2. 子账户：授权行 Allow + 策略 Deny → Deny 压过授权行（融合核心语义）---"
POLSUB="polsub-$(date +%s)"; POLSUB_PW="$POLSUB-pw-1234"
SUB2_ID=$(curl -s -X POST "$ENDPOINT/api/v1/users" -H "authorization: Bearer $ADMIN_TOKEN" \
  -H 'content-type: application/json' -d "{\"username\":\"$POLSUB\",\"password\":\"$POLSUB_PW\"}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["id"])')
[ -n "$SUB2_ID" ] || { echo "建策略测试子账户失败"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/users/$SUB2_ID/permissions" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d "[{\"bucket\":\"$PB\",\"read\":true}]")
[ "$CODE" = "204" ] || { echo "授读应 204，实际 $CODE"; exit 1; }
SUB2_TOKEN=$(curl -s -X POST "$ENDPOINT/api/v1/login" -H 'content-type: application/json' \
  -d "{\"username\":\"$POLSUB\",\"password\":\"$POLSUB_PW\"}" \
  | python3 -c 'import json,sys;print(json.load(sys.stdin)["token"])')
SUB2_AK="POL$(date +%s)ACCESSKEY"; SUB2_SK="polsecret$(date +%s)0123456789"
curl -s -X POST "$ENDPOINT/api/v1/access-keys" -H "authorization: Bearer $SUB2_TOKEN" \
  -H 'content-type: application/json' -d "{\"access_key\":\"$SUB2_AK\",\"secret_key\":\"$SUB2_SK\"}" \
  | grep -q "$SUB2_AK" || { echo "子账户签发 AK 失败"; exit 1; }
pol_aws() { AWS_ACCESS_KEY_ID="$SUB2_AK" AWS_SECRET_ACCESS_KEY="$SUB2_SK" aws --endpoint-url "$ENDPOINT" "$@"; }
pol_aws s3 cp "s3://$PB/open.txt" "$WORK/pol-sub-open" >/dev/null || { echo "授权行读应放行"; exit 1; }
OUT=$(pol_aws s3 cp "s3://$PB/secret/x.txt" "$WORK/pol-sub-gone" 2>&1) && { echo "策略 Deny 前缀仍可读: $OUT"; exit 1; }
deny_msg_ok "$OUT" || { echo "策略 Deny 应压过授权行: $OUT"; exit 1; }

echo "--- 17b3. CopyObject 两轴独立（verifier 阻断 1 回归钉）---"
PB2="$POLICY2_BUCKET"
aws_s3 mb "s3://$PB2" >/dev/null
set_perms() {
  local body=$1
  CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/users/$SUB2_ID/permissions" \
    -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d "$body")
  [ "$CODE" = "204" ] || { echo "改权限应 204，实际 $CODE"; exit 1; }
}
# (1) 目标桶**无策略** + 源桶策略 Deny → 仍必须 403（旧实现 src 评估嵌在 dst 有策略分支内，此场景被整个跳过）
set_perms "[{\"bucket\":\"$PB\",\"read\":true},{\"bucket\":\"$PB2\",\"write\":true}]"
OUT=$(pol_aws s3 cp "s3://$PB/secret/x.txt" "s3://$PB2/leak.txt" 2>&1) && { echo "dst 无策略时 src Deny 被跳过: $OUT"; exit 1; }
deny_msg_ok "$OUT" || { echo "src Deny 应生效: $OUT"; exit 1; }
# (2) 源策略 Allow 不得抬掉「目标桶表无写」（旧实现 src Allow 会把整体抬成放行）
set_perms "[{\"bucket\":\"$PB\",\"read\":true}]"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"Statement":[{"Effect":"Allow","Principal":{"AWS":"uid:'"$SUB2_ID"'"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$PB"'/*"}]}')
[ "$CODE" = "204" ] || { echo "换 Allow 策略应 204，实际 $CODE"; exit 1; }
OUT=$(pol_aws s3 cp "s3://$PB/open.txt" "s3://$PB2/nowrite.txt" 2>&1) && { echo "src Allow 抬掉了 dst 表无写: $OUT"; exit 1; }
deny_msg_ok "$OUT" || { echo "dst 表无写应拒: $OUT"; exit 1; }
# (3) 源策略 Allow 提升「源表无读」（功能修复：dst 无策略时 src Allow 必须被尊重）+ dst 表写
set_perms "[{\"bucket\":\"$PB2\",\"write\":true}]"
pol_aws s3 cp "s3://$PB/open.txt" "s3://$PB2/open-copy.txt" >/dev/null || { echo "src 策略 Allow 应提升源表无读"; exit 1; }
aws_s3 cp "s3://$PB2/open-copy.txt" "$WORK/pol-copy-back" >/dev/null
cmp "$WORK/pol-copy-back" "$WORK/open.txt"
# 恢复现场：子账户权限复原、PB 策略复原为 Deny 文档（供 17c/17d 语境）
set_perms "[{\"bucket\":\"$PB\",\"read\":true},{\"bucket\":\"$PB2\",\"write\":true}]"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d "$POL")
[ "$CODE" = "204" ] || { echo "复原 Deny 策略应 204，实际 $CODE"; exit 1; }

echo "--- 17b4. 批量删逐键授权（verifier 阻断 1c 修复钉）：Deny 键拒、普通键照删（单请求混合）---"
printf 'del\n' > "$WORK/del.txt"
aws_s3 cp "$WORK/del.txt" "s3://$PB/open-del.txt" >/dev/null
set_perms "[{\"bucket\":\"$PB\",\"read\":true,\"delete\":true},{\"bucket\":\"$PB2\",\"write\":true}]"
# 基线：单键删 secret → 403（DeleteObject Deny 在 check 层按对象键命中）
OUT=$(pol_aws s3api delete-object --bucket "$PB" --key secret/x.txt 2>&1) && { echo "单键删 secret 应被 Deny: $OUT"; exit 1; }
deny_msg_ok "$OUT" || { echo "单键删 secret 应 AccessDenied/403: $OUT"; exit 1; }
# 批量删混合请求：secret 键进 Errors（旧实现整请求被绕过 = 洞），普通键进 Deleted
DOUT=$(pol_aws s3api delete-objects --bucket "$PB" \
  --delete '{"Objects":[{"Key":"secret/x.txt"},{"Key":"open-del.txt"}],"Quiet":false}' 2>&1) \
  || { echo "批量删请求应成功（部分失败语义）: $DOUT"; exit 1; }
echo "$DOUT" | python3 -c '
import json,sys
d=json.load(sys.stdin)
errs={e["Key"]:e.get("Code") for e in (d.get("Errors") or [])}
dels={x["Key"] for x in (d.get("Deleted") or [])}
assert errs.get("secret/x.txt")=="AccessDenied", f"secret 键应被拒: {d}"
assert "open-del.txt" in dels, f"普通键应删掉: {d}"
' || { echo "批量删逐键断言失败: $DOUT"; exit 1; }
# 复核落盘状态（走 ls --recursive——非递归 ls 只显示 "secret/" 前缀目录行；
# 且 GetObject 被上面的策略 Deny，不能用 cp 复核）
LS=$(aws_s3 ls --recursive "s3://$PB/")
echo "$LS" | grep -q "secret/x.txt" || { echo "secret 不应被删"; exit 1; }
if echo "$LS" | grep -q open-del.txt; then echo "open-del.txt 应已删"; exit 1; fi
# 拒键审计：批量删的按键 deny 行也要落
AJSON=$(curl -s "$ENDPOINT/api/v1/audit?limit=50&decision=deny" -H "authorization: Bearer $ADMIN_TOKEN")
echo "$AJSON" | python3 -c '
import json,sys
rows=json.load(sys.stdin)
assert any(r.get("op")=="DeleteObjects" and r.get("object_key")=="secret/x.txt" for r in rows), "缺批量删拒键审计行"
' || { echo "批量删拒键审计缺失: $AJSON"; exit 1; }

echo "--- 17c. 审计：?decision=deny 全 deny 且含本桶 deny 行 ---"
AJSON=$(curl -s "$ENDPOINT/api/v1/audit?limit=200&decision=deny" -H "authorization: Bearer $ADMIN_TOKEN")
echo "$AJSON" | PB="$PB" python3 -c '
import json,os,sys
rows=json.load(sys.stdin)
assert rows, "deny 审计为空"
assert all(r["decision"]=="deny" for r in rows), "decision 过滤失效"
assert any(r.get("bucket")==os.environ["PB"] for r in rows), "缺本桶 deny 行"
' || { echo "审计断言失败: $AJSON"; exit 1; }

echo "--- 17d. 清策略（空 statement）→ 恢复访问 ---"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"Statement":[]}')
[ "$CODE" = "204" ] || { echo "清策略应 204，实际 $CODE"; exit 1; }
aws_s3 cp "s3://$PB/secret/x.txt" "$WORK/pol-back" >/dev/null || { echo "清策略后应恢复可读"; exit 1; }
cmp "$WORK/pol-back" "$WORK/sec.txt"
# 子账户也复读：清策略后授权行恢复生效
pol_aws s3 cp "s3://$PB/secret/x.txt" "$WORK/pol-sub-back" >/dev/null || { echo "清策略后子账户应恢复可读"; exit 1; }
cmp "$WORK/pol-sub-back" "$WORK/sec.txt"

echo "--- 17e. S3 面删桶（aws rb）同样清策略：同名重建后 GET 应为 null ---"
# 用 PB2（其对象不含删 Deny 键，rb --force 的批量删可正常走完；PB 的 secret 键在 17d 清策略前删不动）
POL2='{"Statement":[{"Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$PB2"'/secret*"}]}'
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$PB2/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d "$POL2")
[ "$CODE" = "204" ] || { echo "重下策略应 204，实际 $CODE"; exit 1; }
aws_s3 rb "s3://$PB2" --force >/dev/null
aws_s3 mb "s3://$PB2" >/dev/null
curl -s "$ENDPOINT/api/v1/buckets/$PB2/policy" -H "authorization: Bearer $ADMIN_TOKEN" | grep -q null \
  || { echo "S3 面删桶未清策略（同名重建继承旧策略）"; exit 1; }
aws_s3 rb "s3://$PB2" --force >/dev/null
# PB 收尾（17d 已清策略，rb 的批量删走表权限：admin 放行）
aws_s3 rb "s3://$PB" --force >/dev/null
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$ENDPOINT/api/v1/users/$SUB2_ID" \
  -H "authorization: Bearer $ADMIN_TOKEN")
[ "$CODE" = "204" ] || { echo "删策略测试子账户应 204，实际 $CODE"; exit 1; }
echo "   桶策略段通过（Deny 优先/400 校验/审计查询/清策略恢复/S3 面删桶清策略/Deny 压过授权行）"

echo "== 17f. 匿名（无凭据）：桶策略 Principal\"*\" 仅放行数据面对象操作 =="
aws_s3 mb "s3://$ANON_BUCKET" >/dev/null
printf 'anon-open\n' > "$WORK/anon-open.txt"
printf 'anon-secret\n' > "$WORK/anon-secret.txt"
aws_s3 cp "$WORK/anon-open.txt" "s3://$ANON_BUCKET/pub/open.txt" >/dev/null
aws_s3 cp "$WORK/anon-open.txt" "s3://$ANON_BUCKET/pub/d1.txt" >/dev/null
aws_s3 cp "$WORK/anon-secret.txt" "s3://$ANON_BUCKET/priv/data.txt" >/dev/null
# 无策略：匿名数据面也 403（不回退任何用户权限）
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/$ANON_BUCKET/pub/open.txt")
[ "$CODE" = "403" ] || { echo "无策略匿名 GET 应 403，实际 $CODE"; exit 1; }
# 只给具体 AK 的策略：匿名仍 403（Principal 非 "*" 不匹配）
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$ANON_BUCKET/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d '{"Statement":[{"Effect":"Allow","Principal":{"AWS":"'"$AK"'"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$ANON_BUCKET"'/*"}]}')
[ "$CODE" = "204" ] || { echo "写具体 AK 策略应 204，实际 $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/$ANON_BUCKET/pub/open.txt")
[ "$CODE" = "403" ] || { echo "具体 AK Principal 不应放行匿名，实际 $CODE"; exit 1; }
# 通配 Allow（pub/* 读/写/删）+ priv/* 读/删显式 Deny（Deny 优先）
ANON_POL='{"Statement":[
  {"Effect":"Allow","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:PutObject","s3:DeleteObject"],"Resource":"arn:aws:s3:::'"$ANON_BUCKET"'/pub/*"},
  {"Effect":"Deny","Principal":{"AWS":"*"},"Action":["s3:GetObject","s3:DeleteObject"],"Resource":"arn:aws:s3:::'"$ANON_BUCKET"'/priv/*"}]}'
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$ANON_BUCKET/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d "$ANON_POL")
[ "$CODE" = "204" ] || { echo "写匿名策略应 204，实际 $CODE"; exit 1; }

echo "--- 17f1. 匿名 GET/HEAD 放行（pub/*）；Deny/策略外 403 无存在性预言 ---"
curl -s "$ENDPOINT/$ANON_BUCKET/pub/open.txt" | cmp - "$WORK/anon-open.txt" || { echo "匿名 GET 内容不符"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -I "$ENDPOINT/$ANON_BUCKET/pub/open.txt")
[ "$CODE" = "200" ] || { echo "匿名 HEAD 应 200，实际 $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/$ANON_BUCKET/priv/data.txt")
[ "$CODE" = "403" ] || { echo "匿名 GET 显式 Deny 应 403，实际 $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/$ANON_BUCKET/priv/missing.txt")
[ "$CODE" = "403" ] || { echo "Deny 前缀不存在键也应 403（无存在性预言），实际 $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' "$ENDPOINT/$ANON_BUCKET/other/missing.txt")
[ "$CODE" = "403" ] || { echo "策略外键应 403，实际 $CODE"; exit 1; }
# 签名（管理员）行为不变
aws_s3 cp "s3://$ANON_BUCKET/pub/open.txt" "$WORK/anon-signed" >/dev/null || { echo "签名请求回归失败"; exit 1; }

echo "--- 17f2. 匿名 PUT → 往返；匿名 DELETE 放行/拒绝 ---"
printf 'anon-put\n' > "$WORK/anon-put.txt"
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary @"$WORK/anon-put.txt" \
  "$ENDPOINT/$ANON_BUCKET/pub/up.txt")
[ "$CODE" = "200" ] || { echo "匿名 PUT 应 200，实际 $CODE"; exit 1; }
curl -s "$ENDPOINT/$ANON_BUCKET/pub/up.txt" | cmp - "$WORK/anon-put.txt" || { echo "匿名 PUT 往返不符"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$ENDPOINT/$ANON_BUCKET/priv/data.txt")
[ "$CODE" = "403" ] || { echo "匿名 DELETE 显式 Deny 应 403，实际 $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$ENDPOINT/$ANON_BUCKET/pub/up.txt")
[ "$CODE" = "204" ] || { echo "匿名 DELETE 放行应 204，实际 $CODE"; exit 1; }
CODE=$(curl -s -o /dev/null -w '%{http_code}' -I "$ENDPOINT/$ANON_BUCKET/pub/up.txt")
[ "$CODE" = "404" ] || { echo "匿名删除后 HEAD 应 404，实际 $CODE"; exit 1; }

echo "--- 17f3. 匿名 DeleteObjects：允许键 Deleted、Deny 键 Errors（请求级 200）---"
DOUT=$(curl -s -X POST "$ENDPOINT/$ANON_BUCKET?delete" -H 'content-type: application/xml' \
  --data-binary '<Delete><Object><Key>pub/d1.txt</Key></Object><Object><Key>priv/data.txt</Key></Object></Delete>')
echo "$DOUT" | grep -q '<Key>pub/d1.txt</Key>' || { echo "匿名批量删未删允许键: $DOUT"; exit 1; }
echo "$DOUT" | grep -q 'AccessDenied' || { echo "匿名批量删 Deny 键应 AccessDenied: $DOUT"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$ANON_BUCKET" --key pub/d1.txt >/dev/null 2>&1 \
  && { echo "pub/d1.txt 应已删"; exit 1; }
# 复核保留键走 ListObjects（Deny * 同样压过管理员，HEAD 会被策略拦成 403 而非 404）
LS=$(aws_s3 ls --recursive "s3://$ANON_BUCKET/")
echo "$LS" | grep -q "priv/data.txt" || { echo "priv/data.txt 不应被删"; exit 1; }

echo "--- 17f4. 匿名控制面：策略 Allow 也只放数据面，控制面一律 403 ---"
for spec in "PUT /$ANON_BUCKET?policy" "DELETE /$ANON_BUCKET" "GET /$ANON_BUCKET?versioning" \
            "GET /$ANON_BUCKET?list-type=2" "POST /$ANON_BUCKET/pub/mpu.txt?uploads"; do
  m=${spec%% *}; p=${spec#* }
  CODE=$(curl -s -o /dev/null -w '%{http_code}' -X "$m" "$ENDPOINT$p")
  [ "$CODE" = "403" ] || { echo "匿名控制面 $m $p 应 403，实际 $CODE"; exit 1; }
done

echo "--- 17f5. 匿名审计 uid=0（allow/deny 都落行）---"
AJSON=$(curl -s "$ENDPOINT/api/v1/audit?limit=200" -H "authorization: Bearer $ADMIN_TOKEN")
echo "$AJSON" | python3 -c '
import json,sys
rows=json.load(sys.stdin)
assert any(r.get("uid")==0 and r.get("decision")=="allow" and r.get("op")=="GetObject" for r in rows), "缺匿名 allow 审计行"
assert any(r.get("uid")==0 and r.get("decision")=="deny" for r in rows), "缺匿名 deny 审计行"
' || { echo "匿名审计断言失败: $AJSON"; exit 1; }
# 收尾：清策略后删桶（Deny 会挡 --force 的批量删）
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X PUT "$ENDPOINT/api/v1/buckets/$ANON_BUCKET/policy" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' -d '{"Statement":[]}')
[ "$CODE" = "204" ] || { echo "清匿名策略应 204，实际 $CODE"; exit 1; }
aws_s3 rb "s3://$ANON_BUCKET" --force >/dev/null
echo "   匿名 Principal 段通过（数据面白名单/Deny 优先/控制面 403/批量删逐键/审计 uid=0）"

echo "== 18. M5 安全：SSE 三模式 + 对象锁 + STS 临时凭据（验收 ①②③④）=="
# 前置（README「加密（M5）」运维口径）：服务须以密钥库启动（OSS_MASTER_KEY_FILE 与/或
# OSS_KMS_DIR；多节点须分发同一把主密钥）。E2E_DATA_DIR=<data_dir> 设定时额外做盘上
# 明文特征 grep（判别力对照：明文键的特征**必须**找得到）。
aws_s3 mb "s3://$SSE_BUCKET" >/dev/null

echo "--- 18a. SSE-S3：写 → 响应/HEAD 回 AES256 → 往返一致 ---"
MARK="M5PLAINTEXTMARKER$(date +%s)ZZ"
{ printf '%s' "$MARK"; head -c 200000 /dev/urandom; } > "$WORK/sec18.bin"
aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$SSE_BUCKET" --key s3enc.bin \
  --body "$WORK/sec18.bin" --server-side-encryption AES256 > "$WORK/sse_put.json"
grep -q '"ServerSideEncryption": "AES256"' "$WORK/sse_put.json" \
  || { echo "SSE-S3 PUT 未回 AES256：$(cat "$WORK/sse_put.json")"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$SSE_BUCKET" --key s3enc.bin \
  | grep -q '"ServerSideEncryption": "AES256"' || { echo "SSE-S3 HEAD 缺加密头"; exit 1; }
aws_s3 cp "s3://$SSE_BUCKET/s3enc.bin" "$WORK/sse_back.bin" >/dev/null
cmp "$WORK/sec18.bin" "$WORK/sse_back.bin" || { echo "SSE-S3 往返不一致"; exit 1; }

echo "--- 18b. SSE-KMS：写 → 回 aws:kms ---"
aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$SSE_BUCKET" --key kmsenc.bin \
  --body "$WORK/sec18.bin" --server-side-encryption aws:kms > "$WORK/kms_put.json"
grep -q '"ServerSideEncryption": "aws:kms"' "$WORK/kms_put.json" \
  || { echo "SSE-KMS PUT 未回 aws:kms：$(cat "$WORK/kms_put.json")"; exit 1; }
aws_s3 cp "s3://$SSE_BUCKET/kmsenc.bin" "$WORK/kms_back.bin" >/dev/null
cmp "$WORK/sec18.bin" "$WORK/kms_back.bin" || { echo "SSE-KMS 往返不一致"; exit 1; }

echo "--- 18c. SSE-C：客户钥直用（钥不落盘）；错钥必拒 ---"
head -c 32 /dev/urandom > "$WORK/ssec.key"
head -c 32 /dev/urandom > "$WORK/wrong.key"
aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$SSE_BUCKET" --key ssec.bin \
  --body "$WORK/sec18.bin" --sse-customer-algorithm AES256 --sse-customer-key "fileb://$WORK/ssec.key" \
  > "$WORK/ssec_put.json"
grep -q '"SSECustomerAlgorithm": "AES256"' "$WORK/ssec_put.json" \
  || { echo "SSE-C PUT 未回 algorithm：$(cat "$WORK/ssec_put.json")"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$SSE_BUCKET" --key ssec.bin \
  --sse-customer-algorithm AES256 --sse-customer-key "fileb://$WORK/ssec.key" "$WORK/ssec_back.bin" >/dev/null
cmp "$WORK/sec18.bin" "$WORK/ssec_back.bin" || { echo "SSE-C 往返不一致"; exit 1; }
if aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$SSE_BUCKET" --key ssec.bin \
    --sse-customer-algorithm AES256 --sse-customer-key "fileb://$WORK/wrong.key" "$WORK/wrong.bin" >/dev/null 2>&1; then
  echo "SSE-C 错钥必须拒（竟成功）"; exit 1
fi

echo "--- 18d. 盘上明文 grep（E2E_DATA_DIR 设定时；含判别力对照）---"
if [ -n "${E2E_DATA_DIR:-}" ]; then
  # 对照：明文键（无 SSE）→ 特征必须在盘上找得到
  printf '%s' "$MARK" > "$WORK/plain18.bin"
  aws_s3 cp "$WORK/plain18.bin" "s3://$SSE_BUCKET/plain.bin" >/dev/null
  grep -rq "$MARK" "$E2E_DATA_DIR" || { echo "对照失败：明文键的特征在盘上找不到（grep 方法无效）"; exit 1; }
  # 删除明文键后（其目录回收），加密键落盘不得含特征：直接对加密键目录 grep 亦可，
  # 但更简单的是先删明文键再全盘 grep——trash 残骸仍可能在（回收窗口），故改为直接
  # 断言：全盘 grep 中，明文特征只出现在 plain.bin（及其 trash）子树 → 精确断言：
  for key in s3enc kmsenc ssec; do
    if find "$E2E_DATA_DIR" -path "*$key*" -type f -exec grep -l "$MARK" {} + 2>/dev/null | head -1 | grep -q .; then
      echo "加密键 $key 的盘上分片含明文特征"; exit 1
    fi
  done
  echo "   加密键分片无明文特征（明文对照键有——判别有效）"
else
  echo "   （E2E_DATA_DIR 未设，跳过盘上 grep；设 data_dir 路径可启用）"
fi

echo "--- 18e. 对象锁：COMPLIANCE 拒删（含点名删）→ GOVERNANCE bypass → LegalHold ---"
aws --endpoint-url "$ENDPOINT" s3api create-bucket --bucket "$LOCK_BUCKET" --object-lock-enabled-for-bucket
aws --endpoint-url "$ENDPOINT" s3api get-bucket-versioning --bucket "$LOCK_BUCKET" | grep -q Enabled \
  || { echo "锁桶应自动开版本控制"; exit 1; }
LOCK_DATE=$(python3 -c "import datetime;print((datetime.datetime.now(datetime.timezone.utc)+datetime.timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ'))")
CID=$(aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$LOCK_BUCKET" --key comp.bin \
  --body "$WORK/plain18.bin" --object-lock-mode COMPLIANCE --object-lock-retain-until-date "$LOCK_DATE" \
  --query VersionId --output text)
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$LOCK_BUCKET" --key comp.bin \
  | grep -q '"ObjectLockMode": "COMPLIANCE"' || { echo "COMPLIANCE 头缺失"; exit 1; }
if aws --endpoint-url "$ENDPOINT" s3api delete-object --bucket "$LOCK_BUCKET" --key comp.bin --version-id "$CID" >/dev/null 2>&1; then
  echo "COMPLIANCE 未到期必须拒删"; exit 1
fi
GID=$(aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$LOCK_BUCKET" --key gov.bin \
  --body "$WORK/plain18.bin" --object-lock-mode GOVERNANCE --object-lock-retain-until-date "$LOCK_DATE" \
  --query VersionId --output text)
if aws --endpoint-url "$ENDPOINT" s3api delete-object --bucket "$LOCK_BUCKET" --key gov.bin --version-id "$GID" >/dev/null 2>&1; then
  echo "GOVERNANCE 无 bypass 必须拒删"; exit 1
fi
aws --endpoint-url "$ENDPOINT" s3api delete-object --bucket "$LOCK_BUCKET" --key gov.bin \
  --version-id "$GID" --bypass-governance-retention >/dev/null \
  || { echo "GOVERNANCE + bypass 应放行"; exit 1; }
HID=$(aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$LOCK_BUCKET" --key hold.bin \
  --body "$WORK/plain18.bin" --object-lock-legal-hold-status ON --query VersionId --output text)
if aws --endpoint-url "$ENDPOINT" s3api delete-object --bucket "$LOCK_BUCKET" --key hold.bin --version-id "$HID" >/dev/null 2>&1; then
  echo "LegalHold ON 必须拒删"; exit 1
fi
aws --endpoint-url "$ENDPOINT" s3api put-object-legal-hold --bucket "$LOCK_BUCKET" --key hold.bin \
  --version-id "$HID" --legal-hold Status=OFF >/dev/null
aws --endpoint-url "$ENDPOINT" s3api get-object-legal-hold --bucket "$LOCK_BUCKET" --key hold.bin \
  --version-id "$HID" | grep -q OFF || { echo "LegalHold OFF 读回失败"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api delete-object --bucket "$LOCK_BUCKET" --key hold.bin --version-id "$HID" >/dev/null \
  || { echo "OFF 后应放行"; exit 1; }
# 桶级默认规则 + 未开锁桶设 hold 400（P3-a 线级）
aws --endpoint-url "$ENDPOINT" s3api put-object-lock-configuration --bucket "$LOCK_BUCKET" \
  --object-lock-configuration 'ObjectLockEnabled=Enabled,Rule={DefaultRetention={Mode=GOVERNANCE,Days=1}}' >/dev/null
aws --endpoint-url "$ENDPOINT" s3api get-object-lock-configuration --bucket "$LOCK_BUCKET" \
  | grep -q '"Days": 1' || { echo "默认保留规则回读失败"; exit 1; }
if aws --endpoint-url "$ENDPOINT" s3api put-object-legal-hold --bucket "$BUCKET" --key one.bin \
    --legal-hold Status=ON >/dev/null 2>&1; then
  echo "未开锁桶设 legal hold 必须 400"; exit 1
fi
echo "   对象锁段通过（COMPLIANCE 拒含 admin / bypass / LegalHold 开关 / 默认规则 / 门禁）"

echo "--- 18f. STS：AssumeRole → 临时凭据可用；会话策略收窄生效；GetSessionToken ---"
AR_JSON=$(aws --endpoint-url "$ENDPOINT" sts assume-role \
  --role-arn "arn:oss:iam::oss:role/$ADMIN_USER" --role-session-name e2e18 \
  --duration-seconds 900 --output json)
TAK=$(echo "$AR_JSON" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["AccessKeyId"])')
TSK=$(echo "$AR_JSON" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SecretAccessKey"])')
TSTOKEN=$(echo "$AR_JSON" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SessionToken"])')
echo "$TAK" | grep -q '^ASIA' || { echo "STS AK 形态不对：$TAK"; exit 1; }
TMP_AWS() { AWS_ACCESS_KEY_ID="$TAK" AWS_SECRET_ACCESS_KEY="$TSK" AWS_SESSION_TOKEN="$TSTOKEN" \
  aws --endpoint-url "$ENDPOINT" "$@"; }
TMP_AWS s3api list-buckets | grep -q "$SSE_BUCKET" || { echo "临时凭据不可用"; exit 1; }
# 会话策略收窄：只许 GetObject $SSE_BUCKET/* → put 必 403、get 放行
SPOL='{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"*"},"Action":"s3:GetObject","Resource":"arn:aws:s3:::'"$SSE_BUCKET"'/*"}]}'
AR2=$(aws --endpoint-url "$ENDPOINT" sts assume-role --role-arn "arn:oss:iam::oss:role/$ADMIN_USER" \
  --role-session-name e2e18sp --duration-seconds 900 --policy "$SPOL" --output json)
T2AK=$(echo "$AR2" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["AccessKeyId"])')
T2SK=$(echo "$AR2" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SecretAccessKey"])')
T2TK=$(echo "$AR2" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SessionToken"])')
T2_AWS() { AWS_ACCESS_KEY_ID="$T2AK" AWS_SECRET_ACCESS_KEY="$T2SK" AWS_SESSION_TOKEN="$T2TK" \
  aws --endpoint-url "$ENDPOINT" "$@"; }
T2_AWS s3api get-object --bucket "$SSE_BUCKET" --key s3enc.bin "$WORK/sp_back.bin" >/dev/null \
  || { echo "会话策略内 GetObject 应放行"; exit 1; }
if T2_AWS s3api put-object --bucket "$SSE_BUCKET" --key sp_put.bin --body "$WORK/plain18.bin" >/dev/null 2>&1; then
  echo "会话策略外 PutObject 必须拒（交集失效）"; exit 1
fi
GST=$(aws --endpoint-url "$ENDPOINT" sts get-session-token --duration-seconds 900 --output json)
GTAK=$(echo "$GST" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["AccessKeyId"])')
GTSK=$(echo "$GST" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SecretAccessKey"])')
GTTK=$(echo "$GST" | python3 -c 'import json,sys;print(json.load(sys.stdin)["Credentials"]["SessionToken"])')
AWS_ACCESS_KEY_ID="$GTAK" AWS_SECRET_ACCESS_KEY="$GTSK" AWS_SESSION_TOKEN="$GTTK" \
  aws --endpoint-url "$ENDPOINT" s3api list-buckets >/dev/null || { echo "GetSessionToken 凭据不可用"; exit 1; }
echo "   STS 段通过（AssumeRole 可用 / 会话策略交集 / GetSessionToken；过期路径单测覆盖）"

echo "--- 18g. SK 加密存（P3-3）：建键 → 返回 SK 真签名可用；E2E_DB 设定时断言落库密文 ---"
NEW_AK="E2E18AK$(date +%s)000000"
NEW_SK="E2E18SK$(date +%s)000000SECRET"
CODE=$(curl -s -o "$WORK/sk_new.json" -w '%{http_code}' -X POST "$ENDPOINT/api/v1/access-keys" \
  -H "authorization: Bearer $ADMIN_TOKEN" -H 'content-type: application/json' \
  -d "{\"access_key\":\"$NEW_AK\",\"secret_key\":\"$NEW_SK\"}")
[ "$CODE" = "200" ] || { echo "建键应 200，实际 $CODE：$(cat "$WORK/sk_new.json")"; exit 1; }
AWS_ACCESS_KEY_ID="$NEW_AK" AWS_SECRET_ACCESS_KEY="$NEW_SK" \
  aws --endpoint-url "$ENDPOINT" s3api list-buckets >/dev/null || { echo "新键签名不可用"; exit 1; }
if [ -n "${E2E_DB:-}" ]; then
  PREFIX=$(python3 -c "
import sqlite3,sys
c=sqlite3.connect('$E2E_DB')
r=c.execute('SELECT secret_key FROM access_keys WHERE access_key=?',('$NEW_AK',)).fetchone()
print(r[0][:7] if r else 'MISSING')")
  [ "$PREFIX" = "enc:v1:" ] || { echo "SK 落库非密文（前缀 $PREFIX；库未启用则不设 E2E_DB 跑本段）"; exit 1; }
  echo "   SK 落库前缀 enc:v1: ✓"
fi
curl -s -o /dev/null -X DELETE "$ENDPOINT/api/v1/access-keys/$NEW_AK" -H "authorization: Bearer $ADMIN_TOKEN"
echo "   SK 加密段通过（建键 → 真签名 → 落库密文口径）"

aws_s3 rb "s3://$SSE_BUCKET" --force >/dev/null
# 锁桶清理必失败——COMPLIANCE 未到期版本不可删（守卫生效的直接证据）；容错式断言：
if aws_s3 rb "s3://$LOCK_BUCKET" --force >/dev/null 2>&1; then
  echo "锁桶竟删光（COMPLIANCE 守卫失效！）"; exit 1
else
  echo "   锁桶保留 COMPLIANCE 版本（预期：守卫阻止清理；到期后人工/脚本可清）"
fi
echo ""

echo "== 19. M6：S3 Select / Kafka / 桶复制 =="
# 步骤 14 已删 $BUCKET；Select 段复用同名桶，这里重建（EXIT trap 会再清理）
aws_s3 mb "s3://$BUCKET" >/dev/null
# 19a. Select：CSV + JSON LINES + --request-progress 帧可解析 + 边界 400
printf 'a,10\na,20\nb,30\n' > "$WORK/sel.csv"
aws_s3 cp "$WORK/sel.csv" "s3://$BUCKET/sel.csv" >/dev/null
aws --endpoint-url "$ENDPOINT" s3api select-object-content --bucket "$BUCKET" --key sel.csv \
  --expression "SELECT s._1 FROM S3Object WHERE s._2 > 15" --expression-type SQL \
  --input-serialization '{"CSV":{},"CompressionType":"NONE"}' \
  --output-serialization '{"CSV":{}}' "$WORK/sel.out"
grep -qx "a" "$WORK/sel.out" && grep -qx "b" "$WORK/sel.out" \
  || { echo "CSV Select 输出不符：$(cat "$WORK/sel.out")"; exit 1; }
printf '{"name":"jo","n":7}\n{"name":"al","n":9}\n' > "$WORK/sel.jsonl"
aws_s3 cp "$WORK/sel.jsonl" "s3://$BUCKET/sel.jsonl" >/dev/null
aws --endpoint-url "$ENDPOINT" s3api select-object-content --bucket "$BUCKET" --key sel.jsonl \
  --expression "SELECT s.name FROM S3Object s WHERE s.n > 8" --expression-type SQL \
  --input-serialization '{"JSON":{"Type":"LINES"},"CompressionType":"NONE"}' \
  --output-serialization '{"JSON":{}}' "$WORK/sel.jsonl.out"
python3 - "$WORK/sel.jsonl.out" <<'PY'
import json,sys
rows=[json.loads(l) for l in open(sys.argv[1]) if l.strip()]
assert rows == [{"name":"al"}], rows
print("   JSON Select ✓", rows)
PY
# >1MiB 输入 + --request-progress：CLI 必须能解析事件流（Progress/Stats/End 帧）
python3 - "$WORK/selbig.csv" <<'PY'
import sys
with open(sys.argv[1],"w") as f:
    for i in range(200000):
        f.write(f"k{i},v{i}\n")
PY
aws_s3 cp "$WORK/selbig.csv" "s3://$BUCKET/selbig.csv" >/dev/null
aws --endpoint-url "$ENDPOINT" s3api select-object-content --bucket "$BUCKET" --key selbig.csv \
  --expression "SELECT s._1 FROM S3Object LIMIT 3" --expression-type SQL --request-progress '{"Enabled": true}' \
  --input-serialization '{"CSV":{},"CompressionType":"NONE"}' \
  --output-serialization '{"CSV":{}}' "$WORK/selbig.out"
[ "$(wc -l < "$WORK/selbig.out")" -eq 3 ] || { echo "Select LIMIT 3 输出行数不符"; exit 1; }
if aws --endpoint-url "$ENDPOINT" s3api select-object-content --bucket "$BUCKET" --key sel.csv \
  --expression "SELECT nope(s._1) FROM S3Object" --expression-type SQL \
  --input-serialization '{"CSV":{},"CompressionType":"NONE"}' \
  --output-serialization '{"CSV":{}}' "$WORK/sel.bad" >/dev/null 2>&1; then
  echo "白名单外 Select 竟成功（应 400）"; exit 1
else
  echo "   Select 边界 400 ✓"
fi
echo "   Select 段通过（CSV/JSON/Progress/边界）"

# 19b. Kafka（redpanda docker；无 docker 则跳过并记行）
if command -v docker >/dev/null 2>&1; then
  echo "--- 19b. Kafka 通知三阶段（真实 redpanda）---"
  bash tests/e2e_kafka.sh
else
  echo "--- 19b. Kafka 段跳过（docker 不可用）---"
fi

# 19c. 桶复制（两个真实实例；OSS_BIN 缺省 target/debug/oss-server）
if [ "${M6_E2E_REPL:-1}" != "0" ] && [ -x "${OSS_BIN:-$PWD/target/debug/oss-server}" ]; then
  echo "--- 19c. 桶复制双实例 e2e ---"
  OSS_BIN="${OSS_BIN:-$PWD/target/debug/oss-server}" bash tests/e2e_replication.sh
else
  echo "--- 19c. 复制段跳过（无二进制或 M6_E2E_REPL=0）---"
fi

echo ""
echo "== 20. M8 前置：对象标签 + CopyObject DIRECTIVE =="
aws_s3 mb "s3://$TAG_BUCKET"
aws --endpoint-url "$ENDPOINT" s3api put-bucket-versioning --bucket "$TAG_BUCKET" \
  --versioning-configuration Status=Enabled >/dev/null
printf 'tag-source' > "$WORK/tag.src"

# 20a. PutObject x-amz-tagging（URL 解码）+ GET/HEAD tag-count
aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$TAG_BUCKET" --key src \
  --body "$WORK/tag.src" --content-type text/plain --metadata m1=v1 \
  --tagging "env=prod&team=%E4%B8%AD%E6%96%87" >/dev/null
aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" --key src \
  > "$WORK/tags.json"
python3 - "$WORK/tags.json" <<'PY'
import json,sys
ts={t['Key']:t['Value'] for t in json.load(open(sys.argv[1]))['TagSet']}
assert ts == {'env':'prod','team':'中文'}, ts
print("   PutObject tagging URL 解码 ✓", ts)
PY
TC=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$TAG_BUCKET" --key src \
  --query TagCount --output text)
[ "$TC" = "2" ] || { echo "HEAD TagCount 应为 2（实际 $TC）"; exit 1; }

# 20b. Put/DeleteObjectTagging + 标签写不新增版本
aws --endpoint-url "$ENDPOINT" s3api put-object-tagging --bucket "$TAG_BUCKET" --key src \
  --tagging 'TagSet=[{Key=a,Value=1},{Key=b,Value=2}]' >/dev/null
aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" --key src \
  --query 'TagSet[].Key' --output text | grep -q "a" \
  || { echo "PutObjectTagging 未生效"; exit 1; }
NV_BEFORE=$(aws --endpoint-url "$ENDPOINT" s3api list-object-versions --bucket "$TAG_BUCKET" \
  --prefix src --query 'length(Versions)' --output text)
aws --endpoint-url "$ENDPOINT" s3api delete-object-tagging --bucket "$TAG_BUCKET" --key src >/dev/null
aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" --key src \
  --query 'length(TagSet)' --output text | grep -qx 0 \
  || { echo "DeleteObjectTagging 未清空"; exit 1; }
NV_AFTER=$(aws --endpoint-url "$ENDPOINT" s3api list-object-versions --bucket "$TAG_BUCKET" \
  --prefix src --query 'length(Versions)' --output text)
[ "$NV_BEFORE" = "$NV_AFTER" ] || { echo "标签写新增了版本：$NV_BEFORE → $NV_AFTER"; exit 1; }

# 20c. 版本隔离：旧版本打标签不影响默认版本
V1=$(aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$TAG_BUCKET" --key ver \
  --body "$WORK/tag.src" --tagging "v=1" --query VersionId --output text)
aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$TAG_BUCKET" --key ver \
  --body "$WORK/tag.src" --tagging "v=2" >/dev/null
aws --endpoint-url "$ENDPOINT" s3api put-object-tagging --bucket "$TAG_BUCKET" --key ver \
  --version-id "$V1" --tagging 'TagSet=[{Key=v,Value=old}]' >/dev/null
aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" --key ver \
  --query 'TagSet[0].Value' --output text | grep -qx "2" \
  || { echo "旧版本标签写污染了默认版本"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" --key ver \
  --version-id "$V1" --query 'TagSet[0].Value' --output text | grep -qx "old" \
  || { echo "旧版本标签未生效"; exit 1; }

# 20d. Copy 两轴：默认全继承；REPLACE 轴独立
aws --endpoint-url "$ENDPOINT" s3api put-object-tagging --bucket "$TAG_BUCKET" --key src \
  --tagging 'TagSet=[{Key=a,Value=1}]' >/dev/null
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$TAG_BUCKET" --key cp-default \
  --copy-source "$TAG_BUCKET/src" >/dev/null
CP_META=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$TAG_BUCKET" --key cp-default \
  --query 'Metadata.m1' --output text)
[ "$CP_META" = "v1" ] || { echo "默认 copy 未继承 metadata（$CP_META）"; exit 1; }
CP_TAGS=$(aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" \
  --key cp-default --query 'TagSet[0].Key' --output text)
[ "$CP_TAGS" = "a" ] || { echo "默认 copy 未继承标签"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$TAG_BUCKET" --key cp-md \
  --copy-source "$TAG_BUCKET/src" --metadata-directive REPLACE \
  --content-type text/x-t >/dev/null
MT=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$TAG_BUCKET" --key cp-md \
  --query 'length(Metadata)' --output text)
CT=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$TAG_BUCKET" --key cp-md \
  --query ContentType --output text)
[ "$MT" = "0" ] && [ "$CT" = "text/x-t" ] || { echo "metadata REPLACE 轴不符（$CT / $MT）"; exit 1; }
CP_TAGS=$(aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" \
  --key cp-md --query 'TagSet[0].Key' --output text)
[ "$CP_TAGS" = "a" ] || { echo "metadata REPLACE 不应影响标签轴"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$TAG_BUCKET" --key cp-tags \
  --copy-source "$TAG_BUCKET/src" --tagging-directive REPLACE --tagging "x=9" >/dev/null
CP_TAGS=$(aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" \
  --key cp-tags --query 'TagSet[0].Key' --output text)
[ "$CP_TAGS" = "x" ] || { echo "tagging REPLACE 未生效"; exit 1; }
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$TAG_BUCKET" --key cp-clear \
  --copy-source "$TAG_BUCKET/src" --tagging-directive REPLACE >/dev/null
CP_TAGS=$(aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" \
  --key cp-clear --query 'length(TagSet)' --output text)
[ "$CP_TAGS" = "0" ] || { echo "tagging REPLACE 缺省应清空"; exit 1; }

# 20e. copy-to-self：仅 metadata REPLACE 放行（版本桶出新版本）
V_SELF=$(aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$TAG_BUCKET" --key self \
  --body "$WORK/tag.src" --tagging "s=1" --query VersionId --output text)
aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$TAG_BUCKET" --key self \
  --copy-source "$TAG_BUCKET/self" --metadata-directive REPLACE >/dev/null
V_SELF2=$(aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$TAG_BUCKET" --key self \
  --query VersionId --output text)
[ "$V_SELF" != "$V_SELF2" ] || { echo "copy-to-self 未产生新版本"; exit 1; }
if aws --endpoint-url "$ENDPOINT" s3api copy-object --bucket "$TAG_BUCKET" --key self \
  --copy-source "$TAG_BUCKET/self" >/dev/null 2>&1; then
  echo "copy-to-self 无 REPLACE 竟成功（应 400）"; exit 1
else
  echo "   copy-to-self 无 REPLACE 拒 ✓"
fi

# 20f. 非法标签矩阵 → 均非零退出
K129=$(printf 'k%.0s' $(seq 1 129))
MUST11=""
for i in $(seq 0 10); do MUST11="${MUST11}t${i}=v&"; done
MUST11=${MUST11%&}
for bad in "$MUST11" "a=1&a=2" "${K129}=v" "a=%0A"; do
  if aws --endpoint-url "$ENDPOINT" s3api put-object --bucket "$TAG_BUCKET" --key bad \
    --body "$WORK/tag.src" --tagging "$bad" >/dev/null 2>&1; then
    echo "非法标签竟成功：${bad:0:40}"; exit 1
  fi
done
echo "   非法标签矩阵拒绝 ✓"

# 20g. MPU：create 的 --tagging 在 complete 落地；>10 条在 create 即拒
MPU_ID=$(aws --endpoint-url "$ENDPOINT" s3api create-multipart-upload --bucket "$TAG_BUCKET" \
  --key mpu-tagged --tagging "m=1&n=2" --query UploadId --output text)
MPU_ET=$(aws --endpoint-url "$ENDPOINT" s3api upload-part --bucket "$TAG_BUCKET" --key mpu-tagged \
  --upload-id "$MPU_ID" --part-number 1 --body "$WORK/tag.src" --query ETag --output text)
python3 - "$MPU_ET" "$WORK/mpu-complete.json" <<'PY'
import json,sys
json.dump({"Parts":[{"ETag":sys.argv[1].strip(),"PartNumber":1}]}, open(sys.argv[2],"w"))
PY
aws --endpoint-url "$ENDPOINT" s3api complete-multipart-upload --bucket "$TAG_BUCKET" \
  --key mpu-tagged --upload-id "$MPU_ID" --multipart-upload "file://$WORK/mpu-complete.json" >/dev/null
aws --endpoint-url "$ENDPOINT" s3api get-object-tagging --bucket "$TAG_BUCKET" --key mpu-tagged \
  > "$WORK/mpu-tags.json"
python3 - "$WORK/mpu-tags.json" <<'PY'
import json,sys
ts={t['Key']:t['Value'] for t in json.load(open(sys.argv[1]))['TagSet']}
assert ts == {'m':'1','n':'2'}, ts
print("   MPU create 标签落地 ✓", ts)
PY
if MPU_BAD=$(aws --endpoint-url "$ENDPOINT" s3api create-multipart-upload --bucket "$TAG_BUCKET" \
  --key mpu-bad --tagging "$MUST11" --query UploadId --output text 2>/dev/null); then
  aws --endpoint-url "$ENDPOINT" s3api abort-multipart-upload --bucket "$TAG_BUCKET" \
    --key mpu-bad --upload-id "$MPU_BAD" >/dev/null 2>&1
  echo "MPU >10 标签竟成功"; exit 1
else
  echo "   MPU >10 标签拒绝 ✓"
fi
echo ""

echo "S3 验收全部通过（含 M2a multipart / Range / 元数据 / 子账户权限 / M4 版本控制 / 桶策略+审计 / M5 SSE+对象锁+STS / M6 Select+Kafka+复制 / M8 前置标签+Copy 指令）"
