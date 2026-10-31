#!/usr/bin/env bash
# upgrade-preflight.sh 的 MPU 扫描实现（被 source，不单独执行）。
# 依赖调用方提供：DATA_DIRS、TMP、REQUIRE_NO_MPU、add_check、add_detail。

# ---------------------------------------------------------------- 裸 UUID MPU
# mpu.meta 是 rmp_serde::to_vec_named 的 struct-as-map。只取顶层 owner 字段：缺失/
# NULL/空串 = 旧格式（本地属主）。用最小 msgpack 跳过器，避免把 user_metadata/tags
# 里嵌套的 "owner" 误当成字段。
scan_mpu_meta() { # paths_file
	python3 - "$1" <<'PY'
import sys
# 长度前缀宽度：bin/str；array/map 递归；ext 额外加 1 字节类型。
W = {0xc4: 1, 0xc5: 2, 0xc6: 4, 0xd9: 1, 0xda: 2, 0xdb: 4,
     0xdc: 2, 0xdd: 4, 0xde: 2, 0xdf: 4, 0xc7: 1, 0xc8: 2, 0xc9: 4}
FIX = {0xcc: 1, 0xcd: 2, 0xce: 4, 0xcf: 8, 0xd0: 1, 0xd1: 2, 0xd2: 4,
       0xd3: 8, 0xca: 4, 0xcb: 8, 0xd4: 2, 0xd5: 3, 0xd6: 5, 0xd7: 9, 0xd8: 17}

def skip(b, i):
    if i >= len(b):
        raise ValueError('truncated')
    c = b[i]
    i += 1
    if c <= 0x7f or c >= 0xe0:
        return i
    if 0xa0 <= c <= 0xbf:
        return i + (c & 0x1f)
    if 0x90 <= c <= 0x9f:
        for _ in range(c & 0xf):
            i = skip(b, i)
        return i
    if 0x80 <= c <= 0x8f:
        for _ in range(2 * (c & 0xf)):
            i = skip(b, i)
        return i
    if c in (0xc0, 0xc2, 0xc3):
        return i
    if c in FIX:
        return i + FIX[c]
    if c not in W:
        raise ValueError('bad byte %#x' % c)
    w = W[c]
    n = int.from_bytes(b[i:i + w], 'big')
    j = i + w
    if c in (0xdc, 0xdd, 0xde, 0xdf):
        for _ in range(2 * n if c in (0xde, 0xdf) else n):
            j = skip(b, j)
        return j
    return j + n + (1 if c in (0xc7, 0xc8, 0xc9) else 0)

def state(path):
    try:
        with open(path, 'rb') as f:
            b = f.read()
        i = 1
        c = b[0]
        if 0x80 <= c <= 0x8f:
            n = c & 0xf
        elif c in (0xde, 0xdf):
            w = 2 if c == 0xde else 4
            n = int.from_bytes(b[i:i + w], 'big')
            i += w
        else:
            return 'CORRUPT'
        owner = None
        for _ in range(n):
            k0 = i
            i = skip(b, i)
            key = b[k0:i]
            v0 = i
            i = skip(b, i)
            if key in (b'\xa5owner', b'\xd9\x05owner'):
                owner = b[v0:i]
        if i != len(b):
            return 'CORRUPT'
    except Exception:
        return 'CORRUPT'
    return 'OWNED' if owner not in (None, b'\xc0', b'\xa0') else 'LEGACY'

for raw in open(sys.argv[1], encoding='utf-8', errors='replace'):
    p = raw.rstrip('\n')
    if p:
        print('%s\t%s' % (state(p), p))
PY
}

check_bare_uuid_mpu() {
	local name=bare-uuid-mpu root errs=0 invalid=0
	local -a scan=()
	for root in "${DATA_DIRS[@]}"; do
		[ -d "$root" ] || { invalid=1; add_detail "$name" "data-dir 不存在或不是目录: $root"; }
	done
	# 路径非法单独成 BLOCK，避免把“目录不存在”误判成“没有旧 MPU”。
	if ((invalid)); then
		add_check "$name" BLOCK "至少一个 --data-dir 不存在/不可用（见详情）"
		return
	fi
	: >"$TMP/mpu_files"
	: >"$TMP/find_err"
	for root in "${DATA_DIRS[@]}"; do
		# -H：允许 data-dir 本身是符号链接；不跟随树内符号链接。
		find -H "$root" -mindepth 4 -maxdepth 4 -type f -name mpu.meta -print \
			>>"$TMP/mpu_files" 2>>"$TMP/find_err" || errs=1
	done
	sort -u -o "$TMP/mpu_files" "$TMP/mpu_files" # 重叠/重复的 data-dir 不重复计数
	local legacy=0 owned=0 corrupt=0 unreadable=0 total=0 line st path
	if [ -s "$TMP/mpu_files" ]; then
		mapfile -t scan < <(scan_mpu_meta "$TMP/mpu_files")
		for line in "${scan[@]}"; do
			[ -n "$line" ] || continue
			st=${line%%$'\t'*}
			path=${line#*$'\t'}
			total=$((total + 1))
			case "$st" in
			LEGACY)
				legacy=$((legacy + 1))
				add_detail "$name" "裸 UUID 暂存区（升级前需 Complete/Abort）: $path"
				;;
			OWNED)
				owned=$((owned + 1))
				((REQUIRE_NO_MPU)) && add_detail "$name" "在途 MPU 暂存区（--require-no-mpu）: $path"
				;;
			CORRUPT)
				corrupt=$((corrupt + 1))
				add_detail "$name" "mpu.meta 无法解析，人工核查: $path"
				;;
			*)
				unreadable=$((unreadable + 1))
				add_detail "$name" "mpu.meta 不可读，人工核查: $path"
				;;
			esac
		done
	fi
	local status=OK summary="扫描 $total 个活 MPU 暂存区：裸 UUID=$legacy，带属主前缀=$owned"
	if ((legacy > 0)); then
		status=WARN
		summary+="；升级前必须排空裸 UUID 在途 MPU（Complete/Abort），否则集群其他节点无法收尾"
	fi
	if ((corrupt + unreadable > 0)); then
		status=WARN
		summary+="；损坏/不可读 mpu.meta=$((corrupt + unreadable))（人工核查）"
	fi
	if ((errs)); then
		status=WARN
		summary+="；部分目录不可读，扫描可能不完整（见详情）"
		local n=0 l
		while IFS= read -r l && ((n < 20)); do
			add_detail "$name" "find: $l"
			n=$((n + 1))
		done <"$TMP/find_err"
	fi
	if ((REQUIRE_NO_MPU && total > 0)); then
		status=BLOCK
		summary+="；--require-no-mpu：$total 个在途 MPU（裸 UUID=$legacy，带属主=$owned）；滚动升级前必须 Complete/Abort 排空，旧节点离开后无法收尾"
	fi
	add_check "$name" "$status" "$summary"
}
