#!/usr/bin/env bash
# M1 验收：S3 兼容性（aws-cli + rclone）+ aetherupload 上传链路。
# 前置：docker 可用（用于跑 minio/mc 作为对照）；本机装了 aws-cli 与 rclone。
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://127.0.0.1:9000}"
AK="${AK:-AKIA_TEST_ACCESS_KEY}"
SK="${SK:-SK_TEST_SECRET_KEY_0001}"
BUCKET="e2e-$(date +%s)"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

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

echo "== 10. 删桶 =="
aws_s3 rb "s3://$BUCKET" --force

echo "M1 S3 验收全部通过"
