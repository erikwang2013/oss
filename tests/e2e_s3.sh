#!/usr/bin/env bash
# 验收：S3 兼容性（aws-cli + rclone）。M1 基础 + M2a 的 multipart / Range / 元数据。
# 前置：本机装了 aws-cli 与 rclone。
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
AK="${AK:-AKIA_TEST_ACCESS_KEY}"
SK="${SK:-SK_TEST_SECRET_KEY_0001}"
BUCKET="e2e-$(date +%s)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"; aws --endpoint-url "$ENDPOINT" s3 rb "s3://$BUCKET" --force >/dev/null 2>&1 || true' EXIT

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

echo "== 12. 用户元数据往返（--metadata → head-object 可见）=="
aws_s3 cp "$WORK/one.bin" "s3://$BUCKET/meta.bin" --metadata foo=bar,lang=zh
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key meta.bin --query 'Metadata.foo' --output text | grep -qx bar
aws --endpoint-url "$ENDPOINT" s3api head-object --bucket "$BUCKET" --key meta.bin --query 'Metadata.lang' --output text | grep -qx zh

echo "== 13. rclone 大文件（强制走 multipart）=="
export RCLONE_CONFIG_OSS_UPLOAD_CUTOFF=5M            # rclone 默认 cutoff 200Mi 会走单 PUT，压低以强制分片
export RCLONE_CONFIG_OSS_CHUNK_SIZE=5M
rclone copyto "$WORK/big.bin" oss:"$BUCKET/rclone-big.bin"
rclone cat oss:"$BUCKET/rclone-big.bin" | cmp - "$WORK/big.bin"

echo "== 14. 删桶 =="
aws_s3 rb "s3://$BUCKET" --force

echo "S3 验收全部通过（含 M2a multipart / Range / 元数据）"
