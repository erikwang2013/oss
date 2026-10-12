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
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; for b in "$BUCKET" "$PERM_BUCKET" "$OTHER_BUCKET"; do aws --endpoint-url "$ENDPOINT" s3 rb "s3://$b" --force >/dev/null 2>&1 || true; done' EXIT

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

echo "== 11d. 多区间 Range → 400 InvalidArgument（鉴权后 s3s 解析层拦截）=="
# 实测（aws-cli 2.37 + 本服务）：鉴权**先于**头部解析——未签名请求被鉴权层拦为 403
# （XML body <Code>AccessDenied</Code><Message>Signature is required</Message>）；
# 带鉴权后 s3s 解析层拒多区间 → 400 InvalidArgument（"invalid header: range"）。
# 原「解析层先于鉴权、未签名 curl 也回 400」的推断不成立，两条契约分别钉住。
UNAUTH_BODY=$(curl -s -H 'Range: bytes=0-1,3-4' "$ENDPOINT/$BUCKET/big.bin")
echo "$UNAUTH_BODY" | grep -q 'AccessDenied' || { echo "未签名请求应被鉴权层拦为 AccessDenied: $UNAUTH_BODY"; exit 1; }
UNAUTH=$(curl -s -o /dev/null -w '%{http_code}' -H 'Range: bytes=0-1,3-4' "$ENDPOINT/$BUCKET/big.bin")
[ "$UNAUTH" = "403" ] || { echo "未签名请求应 403，实际 $UNAUTH"; exit 1; }
OUT=$(aws --endpoint-url "$ENDPOINT" s3api get-object --bucket "$BUCKET" --key big.bin --range 'bytes=0-1,3-4' "$WORK/multi-range.out" 2>&1) \
  && { echo "多区间 Range 应 400，却成功: $OUT"; exit 1; }
echo "$OUT" | grep -q 'InvalidArgument' || { echo "应报 InvalidArgument: $OUT"; exit 1; }

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
#   补授写后上传放行；ListBuckets 只见授权桶（服务端按可见集过滤）。
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

echo "--- 15g. 清理：删两个桶 + 删子账户 ---"
aws_s3 rb "s3://$PERM_BUCKET" --force >/dev/null
aws_s3 rb "s3://$OTHER_BUCKET" --force >/dev/null
CODE=$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$ENDPOINT/api/v1/users/$SUB_ID" \
  -H "authorization: Bearer $ADMIN_TOKEN")
[ "$CODE" = "204" ] || { echo "删子账户应 204，实际 $CODE"; exit 1; }

echo "S3 验收全部通过（含 M2a multipart / Range / 元数据 / 子账户权限）"
