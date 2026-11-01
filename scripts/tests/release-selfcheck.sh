#!/usr/bin/env bash
# scripts/release.sh 自检：fixture 树 + stub git/npm/cargo/rsvg-convert，无网络、无真实构建。
# 覆盖 prepare 的清洁树/tag 校验、锚点替换、锁文件与 PNG 同步，以及 check 的陈旧锚点、
# sitemap 日期、changelog open、manifest、副本一致性、cargo --locked 失败。

set -uo pipefail

HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
ROOT=$(cd "$HERE/../.." && pwd)
MAIN="$ROOT/scripts/release.sh"
[[ -f $MAIN ]] || {
	echo "找不到脚本: $MAIN" >&2
	exit 1
}

TMP=$(mktemp -d "${TMPDIR:-/tmp}/oss-release-test.XXXXXX") || exit 1
trap 'rm -rf "$TMP"' EXIT
FIX="$TMP/repo"
BIN="$TMP/bin"
OUT="$TMP/out"
ERR="$TMP/err"
GIT_LOG="$TMP/git.log"
NPM_LOG="$TMP/npm.log"
CARGO_LOG="$TMP/cargo.log"
RSVG_LOG="$TMP/rsvg.log"
mkdir -p "$FIX"/{crates/{oss-server,xl-client,xl-proto,xl-s3,xl-store},web,docs/superpowers/specs,docs/assets,site/assets/img,scripts} "$BIN"
: >"$GIT_LOG"
: >"$NPM_LOG"
: >"$CARGO_LOG"
: >"$RSVG_LOG"

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
assert_has() { # desc file needle
	if grep -qF -- "$3" "$2"; then ok "$1"; else bad "$1" "missing: $3"; fi
}
assert_not_has() { # desc file needle
	if grep -qF -- "$3" "$2"; then bad "$1" "unexpected: $3"; else ok "$1"; fi
}
assert_no_vcs_mutation() { # desc
	if grep -Eq '^(commit|tag|push)( |$)' "$GIT_LOG"; then
		bad "$1" "unexpected: $(grep -E '^(commit|tag|push)( |$)' "$GIT_LOG")"
	else
		ok "$1"
	fi
}

# ---------------------------------------------------------------- fixture
cp "$ROOT"/crates/oss-server/Cargo.toml "$FIX/crates/oss-server/"
cp "$ROOT"/crates/xl-client/Cargo.toml "$FIX/crates/xl-client/"
cp "$ROOT"/crates/xl-proto/Cargo.toml "$FIX/crates/xl-proto/"
cp "$ROOT"/crates/xl-s3/Cargo.toml "$FIX/crates/xl-s3/"
cp "$ROOT"/crates/xl-store/Cargo.toml "$FIX/crates/xl-store/"
cp "$ROOT"/Cargo.lock "$FIX/"
cp "$ROOT"/web/package.json "$ROOT"/web/package-lock.json "$FIX/web/"
cp "$ROOT"/README.md "$ROOT"/README.en.md "$ROOT"/AGENTS.md "$FIX/"
cp "$ROOT"/docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md "$FIX/docs/superpowers/specs/"
cp "$ROOT"/site/index.html "$ROOT"/site/install.html "$ROOT"/site/usage.html \
	"$ROOT"/site/api.html "$ROOT"/site/changelog.html "$ROOT"/site/sitemap.xml "$FIX/site/"
cp "$ROOT"/site/assets/img/social-preview.svg "$ROOT"/site/assets/img/social-preview.png "$FIX/site/assets/img/"
cp "$ROOT"/docs/assets/social-preview.svg "$ROOT"/docs/assets/social-preview.png "$FIX/docs/assets/"
cp "$MAIN" "$FIX/scripts/release.sh"

# ---------------------------------------------------------------- stubs
cat >"$BIN/git" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_GIT_LOG:?}"
case "$*" in
"status --porcelain")
	[[ -n ${STUB_GIT_DIRTY:-} ]] && printf ' M README.md\n'
	exit 0 ;;
*"rev-parse --verify --quiet "*)
	[[ -n ${STUB_TAG_EXISTS:-} ]] && exit 0
	exit 1 ;;
esac
exit 0
SH
cat >"$BIN/npm" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_NPM_LOG:?}"
[[ $1 == version ]] || exit 1
v=$2
shift 2
dir=.
while (($#)); do
	case "$1" in
	--prefix) dir=$2; shift 2 ;;
	*) shift ;;
	esac
done
V=$v perl -pi -e 'if (/"version": "[^"]+"/ && $n++ == 0) { s/"version": "[^"]+"/"version": "$ENV{V}"/ }' "$dir/package.json"
V=$v perl -pi -e 'if (/"version": "[^"]+"/ && $n++ < 2) { s/"version": "[^"]+"/"version": "$ENV{V}"/ }' "$dir/package-lock.json"
SH
cat >"$BIN/cargo" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_CARGO_LOG:?}"
[[ ${STUB_CARGO_FAIL:-} == 1 ]] && exit 1
case " $* " in
*" metadata "*) ;;
*) exit 0 ;;
esac
case " $* " in
*" --locked "*) exit 0 ;;
esac
v=$(perl -ne 'print $1 and exit if /^version = "([^"]+)"/' crates/oss-server/Cargo.toml)
V=$v perl -0777 -pi -e 's/(\[\[package\]\]\s*\nname = "(?:oss-server|xl-client|xl-proto|xl-s3|xl-store)"\s*\nversion = ")[^"]+(")/${1}$ENV{V}${2}/g' Cargo.lock
SH
cat >"$BIN/rsvg-convert" <<'SH'
#!/usr/bin/env bash
printf '%s\n' "$*" >>"${STUB_RSVG_LOG:?}"
out=""
in=""
while (($#)); do
	case "$1" in
	-o) out=$2; shift 2 ;;
	*) in=$1; shift ;;
	esac
done
[[ -n $out && -n $in ]] || exit 2
cp "$in" "$out"
SH
chmod +x "$BIN/git" "$BIN/npm" "$BIN/cargo" "$BIN/rsvg-convert"
export PATH="$BIN:$PATH"
export STUB_GIT_LOG="$GIT_LOG"
export STUB_NPM_LOG="$NPM_LOG"
export STUB_CARGO_LOG="$CARGO_LOG"
export STUB_RSVG_LOG="$RSVG_LOG"
unset STUB_GIT_DIRTY STUB_TAG_EXISTS STUB_CARGO_FAIL

RC=0
run() {
	(cd "$FIX" && bash scripts/release.sh "$@") >"$OUT" 2>"$ERR"
	RC=$?
}

# ---------------------------------------------------------------- tests
bash -n "$MAIN" && ok "bash -n release.sh" || bad "bash -n release.sh"
bash -n "$HERE/release-selfcheck.sh" && ok "bash -n selfcheck" || bad "bash -n selfcheck"

STUB_GIT_DIRTY=1 run prepare 1.29.0 2026-12-01
assert_rc "脏工作树拒绝 prepare" 1 "$RC"
assert_has "脏工作树给出原因" "$ERR" "not clean"

STUB_TAG_EXISTS=1 run prepare 1.29.0 2026-12-01
assert_rc "已存在 tag 拒绝 prepare" 1 "$RC"
assert_has "tag 冲突给出原因" "$ERR" "already exists"

run prepare 1.28.3 2026-12-01
assert_rc "旧版本拒绝 prepare" 1 "$RC"
assert_has "旧版本给出原因" "$ERR" "older than v1.28.4"

run prepare 1.29.0 2026-10-01
assert_rc "旧日期拒绝 prepare" 1 "$RC"
assert_has "旧日期给出原因" "$ERR" "older than current"

: >"$GIT_LOG"
: >"$NPM_LOG"
: >"$CARGO_LOG"
: >"$RSVG_LOG"
run prepare 1.29.0 2026-12-01
assert_rc "prepare rc=0" 0 "$RC"
assert_has "prepare 打印 TODO" "$OUT" "mechanical anchors updated to v1.29.0"
assert_has "prepare 声明不提交" "$OUT" "Nothing was committed, tagged, or pushed."
assert_no_vcs_mutation "prepare 不 commit/tag/push"
assert_has "npm 使用指定参数" "$NPM_LOG" "version 1.29.0 --no-git-tag-version --prefix web"
assert_has "cargo 离线更新锁文件" "$CARGO_LOG" "metadata --offline --format-version 1"
assert_has "rsvg 重新生成 site PNG" "$RSVG_LOG" "site/assets/img/social-preview.svg"

for c in oss-server xl-client xl-proto xl-s3 xl-store; do
	assert_has "crate $c 版本已更新" "$FIX/crates/$c/Cargo.toml" 'version = "1.29.0"'
done
assert_has "web package 版本已更新" "$FIX/web/package.json" '"version": "1.29.0"'
assert_has "web lock 版本已更新" "$FIX/web/package-lock.json" '"version": "1.29.0"'
assert_has "Cargo.lock 版本已更新" "$FIX/Cargo.lock" 'version = "1.29.0"'
assert_has "README 锚点已更新" "$FIX/README.md" "最新发布：v1.29.0（2026-12-01）"
assert_has "README.en 锚点已更新" "$FIX/README.en.md" "Latest release: v1.29.0 (2026-12-01)"
assert_has "AGENTS 锚点已更新" "$FIX/AGENTS.md" "最新发布 **v1.29.0（2026-12-01）**"
assert_has "docs 验收记录 tag 已更新" "$FIX/docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md" 'tag `v1.29.0`'
assert_has "index softwareVersion 已更新" "$FIX/site/index.html" '"softwareVersion": "1.29.0"'
assert_has "index 当前版本已更新" "$FIX/site/index.html" "v1.29.0（最新发布）"
assert_has "index proof strip 保留版式" "$FIX/site/index.html" "<strong>v1.29.0 补丁版</strong><span>2026-12-01 · "
assert_has "install 最新发布已更新" "$FIX/site/install.html" "当前最新发布 v1.29.0"
assert_has "changelog 最新发布已更新" "$FIX/site/changelog.html" "当前最新发布：<strong>v1.29.0</strong>（2026-12-01）"
assert_has "sitemap 日期已更新" "$FIX/site/sitemap.xml" "<lastmod>2026-12-01</lastmod>"
assert_has "site SVG 版本已更新" "$FIX/site/assets/img/social-preview.svg" "· v1.29.0</text>"
assert_has "docs SVG 版本已更新" "$FIX/docs/assets/social-preview.svg" "· v1.29.0</text>"
assert_has "历史 release row 保留" "$FIX/site/index.html" "<strong>v1.28.4</strong><p>补丁版"
assert_has "历史 API bullet 保留" "$FIX/site/api.html" "<li>v1.28.4：补丁版"
assert_has "历史 changelog entry 保留" "$FIX/site/changelog.html" 'id="v1-28-4" open'

run check 1.29.0 2026-12-01
assert_rc "未写 changelog 时 check 失败" 1 "$RC"
assert_has "check 指明缺少新 nav 锚点" "$ERR" "changelog nav: missing v1.29.0 anchor #v1-29-0"

# 模拟人工 changelog：新 nav 锚点 + 新条目 + 最新条目 open。
perl -0777 -pi -e '
s/<a href="#v1-28-4">v1\.28\.4<\/a>/<a href="#v1-29-0">v1.29.0<\/a>/;
s/id="v1-28-4" open/id="v1-29-0" open/;
s/<strong>v1\.28\.4<\/strong><time datetime="2026-11-01">2026-11-01<\/time>/<strong>v1.29.0<\/strong><time datetime="2026-12-01">2026-12-01<\/time>/;
' "$FIX/site/changelog.html"
: >"$CARGO_LOG"
run check 1.29.0 2026-12-01
assert_rc "写完 changelog 后 check 通过" 0 "$RC"
assert_has "check 输出 OK" "$OUT" "release: check OK: v1.29.0 (2026-12-01)"
assert_has "check 使用 --locked" "$CARGO_LOG" "metadata --offline --locked --format-version 1"

perl -pi -e 's/下载 v1\.29\.0/下载 v1.28.4/' "$FIX/site/usage.html"
run check 1.29.0 2026-12-01
assert_rc "陈旧 nav 锚点被 check 拒绝" 1 "$RC"
assert_has "陈旧锚点给出文件" "$ERR" "site/usage.html nav"
perl -pi -e 's/下载 v1\.28\.4/下载 v1.29.0/' "$FIX/site/usage.html"

perl -0777 -pi -e 's/<lastmod>2026-12-01<\/lastmod>/<lastmod>2026-11-01<\/lastmod>/g' "$FIX/site/sitemap.xml"
run check 1.29.0 2026-12-01
assert_rc "sitemap 旧日期被 check 拒绝" 1 "$RC"
assert_has "sitemap 错误指明文件" "$ERR" "site/sitemap.xml lastmod"
perl -0777 -pi -e 's/<lastmod>2026-11-01<\/lastmod>/<lastmod>2026-12-01<\/lastmod>/g' "$FIX/site/sitemap.xml"

perl -pi -e 's/<details class="changelog-entry" id="v1-29-0" open>/<details class="changelog-entry" id="v1-29-0">/' "$FIX/site/changelog.html"
run check 1.29.0 2026-12-01
assert_rc "最新 changelog 未 open 被拒绝" 1 "$RC"
assert_has "changelog open 错误明确" "$ERR" "is not open"
perl -pi -e 's/<details class="changelog-entry" id="v1-29-0">/<details class="changelog-entry" id="v1-29-0" open>/' "$FIX/site/changelog.html"

perl -pi -e 's/^version = "1\.29\.0"$/version = "1.29.1"/' "$FIX/crates/xl-store/Cargo.toml"
run check 1.29.0 2026-12-01
assert_rc "crate 版本不一致被拒绝" 1 "$RC"
assert_has "crate 版本错误指名文件" "$ERR" "crates/xl-store/Cargo.toml"
perl -pi -e 's/^version = "1\.29\.1"$/version = "1.29.0"/' "$FIX/crates/xl-store/Cargo.toml"

printf x >>"$FIX/docs/assets/social-preview.png"
run check 1.29.0 2026-12-01
assert_rc "PNG 副本不一致被拒绝" 1 "$RC"
assert_has "PNG 副本错误明确" "$ERR" "PNG copies differ"
cp "$FIX/site/assets/img/social-preview.png" "$FIX/docs/assets/social-preview.png"

STUB_CARGO_FAIL=1 run check 1.29.0 2026-12-01
assert_rc "cargo --locked 失败被拒绝" 1 "$RC"
assert_has "cargo 失败错误明确" "$ERR" "cargo metadata --offline --locked failed"

# ---------------------------------------------------------------- 结果
echo "== 结果 =="
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" -ne 0 ] && exit 1
echo "PASS: release 自检（$PASS 项通过）"
exit 0
