#!/usr/bin/env bash
# Mechanical release version/date updater. Never commits, tags, or pushes.
# Usage: scripts/release.sh prepare <version> <date>
#        scripts/release.sh check   <version> <date>
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"

CRATES=(oss-server xl-client xl-proto xl-s3 xl-store)
SITE_PAGES=(site/index.html site/install.html site/usage.html site/api.html site/changelog.html)
SOCIAL_SVG=(site/assets/img/social-preview.svg docs/assets/social-preview.svg)

die() { printf 'release: %s\n' "$*" >&2; exit 1; }
usage() { printf 'usage: %s prepare|check <version> <date>\n' "$0" >&2; exit 2; }

[[ $# -eq 3 ]] || usage
ACTION=$1
VERSION=$2
DATE=$3
[[ $ACTION == prepare || $ACTION == check ]] || usage
[[ $VERSION =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "invalid version: $VERSION"
[[ $DATE =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || die "invalid date: $DATE"
perl -MTime::Piece -e 'exit(Time::Piece->strptime($ARGV[0], "%Y-%m-%d")->strftime("%Y-%m-%d") eq $ARGV[0] ? 0 : 1)' "$DATE" 2>/dev/null ||
	die "invalid calendar date: $DATE"

pkg_version() {
	perl -ne 'print $1 and exit if /^version = "([^"]+)"/' "$1"
}
first_json_version() {
	perl -ne 'print $1 and exit if /"version": "([^"]+)"/' "$1"
}
second_json_version() {
	perl -ne 'print $1 and exit if /"version": "([^"]+)"/ && ++$n == 2' "$1"
}
lock_versions() {
	perl -0777 -ne 'while (/\[\[package\]\]\s*\nname = "([^"]+)"\s*\nversion = "([^"]+)"/g) { print "$1 $2\n" }' Cargo.lock
}

one() { # desc file perl-regex expected
	local desc=$1 file=$2 re=$3 want=$4 got
	[[ -f $file ]] || die "$desc: missing $file"
	got=$(RE="$re" perl -0777 -ne 'while (/$ENV{RE}/g) { print "$1\n" }' "$file" | sort -u)
	[[ $got == "$want" ]] || die "$desc: expected '$want', got '${got:-<none>}'"
}

verify_manifests() { # version
	local v=$1 f got name lv n=0
	for f in crates/*/Cargo.toml; do
		n=$((n + 1))
		got=$(pkg_version "$f")
		[[ $got == "$v" ]] || die "$f: version '$got' != $v"
	done
	[[ $n -eq 5 ]] || die "expected 5 crate manifests, found $n"
	for f in web/package.json web/package-lock.json; do
		got=$(first_json_version "$f")
		[[ $got == "$v" ]] || die "$f: version '$got' != $v"
	done
	got=$(second_json_version web/package-lock.json)
	[[ $got == "$v" ]] || die "web/package-lock.json root package: '$got' != $v"
	lv=$(lock_versions)
	for name in "${CRATES[@]}"; do
		got=$(printf '%s\n' "$lv" | awk -v n="$name" '$1 == n { print $2 }')
		[[ $got == "$v" ]] || die "Cargo.lock $name: '${got:-missing}' != $v"
	done
}

verify_anchors() { # version date
	local v=$1 d=$2 f dates
	one "README.md release" README.md '最新发布：v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
	one "README.md date" README.md '最新发布：v[0-9.]+（([0-9]{4}-[0-9]{2}-[0-9]{2})）' "$d"
	one "README.en.md release" README.en.md 'Latest release: v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
	one "README.en.md date" README.en.md 'Latest release: v[0-9.]+ \(([0-9]{4}-[0-9]{2}-[0-9]{2})\)' "$d"
	one "AGENTS.md release" AGENTS.md '最新发布 \*\*v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
	one "AGENTS.md date" AGENTS.md '最新发布 \*\*v[0-9.]+（([0-9]{4}-[0-9]{2}-[0-9]{2})）' "$d"
	one "capability-gaps release" docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md '最新发布：\*\*v([0-9]+\.[0-9]+\.[0-9]+)\*\*' "$v"
	one "capability-gaps date" docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md '最新发布：\*\*v[0-9.]+\*\*（([0-9]{4}-[0-9]{2}-[0-9]{2})，' "$d"
	one "capability-gaps tag" docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md 'tag `v([0-9]+\.[0-9]+\.[0-9]+)`' "$v"
	for f in "${SITE_PAGES[@]}"; do
		one "$f nav" "$f" '下载 v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
		one "$f footer" "$f" 'Rust 2024 · v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
	done
	one "site/index.html softwareVersion" site/index.html '"softwareVersion": "([0-9]+\.[0-9]+\.[0-9]+)"' "$v"
	one "site/index.html tag link" site/index.html 'releases/tag/v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
	one "site/index.html tarball" site/index.html 'refs/tags/v([0-9]+\.[0-9]+\.[0-9]+)\.tar\.gz' "$v"
	one "site/index.html current version" site/index.html '<dd>v([0-9]+\.[0-9]+\.[0-9]+)（最新发布）</dd>' "$v"
	one "site/index.html download heading" site/index.html '<h2>下载 v([0-9]+\.[0-9]+\.[0-9]+)</h2>' "$v"
	one "site/index.html release date" site/index.html '（([0-9]{4}-[0-9]{2}-[0-9]{2})）</dd>' "$d"
	one "site/index.html proof version" site/index.html '<strong>v([0-9]+\.[0-9]+\.[0-9]+) [^<]*</strong><span>' "$v"
	one "site/index.html proof date" site/index.html '<strong>v[0-9.]+ [^<]*</strong><span>([0-9]{4}-[0-9]{2}-[0-9]{2}) · ' "$d"
	one "site/install.html current release" site/install.html '当前最新发布 v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
	one "site/install.html recommendation" site/install.html '请使用 v([0-9]+\.[0-9]+\.[0-9]+)（' "$v"
	one "site/install.html tag link" site/install.html 'releases/tag/v([0-9]+\.[0-9]+\.[0-9]+)' "$v"
	one "site/install.html tarball" site/install.html 'refs/tags/v([0-9]+\.[0-9]+\.[0-9]+)\.tar\.gz' "$v"
	one "site/install.html button" site/install.html '打开 v([0-9]+\.[0-9]+\.[0-9]+) Release' "$v"
	one "site/api.html range" site/api.html '从 v1\.0\.0 到 v([0-9]+\.[0-9]+\.[0-9]+)。' "$v"
	one "site/changelog.html meta range" site/changelog.html '从 v1\.0\.0 到 v([0-9]+\.[0-9]+\.[0-9]+)，' "$v"
	one "site/changelog.html social range" site/changelog.html '再到 v([0-9]+\.[0-9]+\.[0-9]+) ' "$v"
	one "site/changelog.html intro range" site/changelog.html '到 v([0-9]+\.[0-9]+\.[0-9]+) 的' "$v"
	one "site/changelog.html latest" site/changelog.html '当前最新发布：<strong>v([0-9]+\.[0-9]+\.[0-9]+)</strong>' "$v"
	one "site/changelog.html latest date" site/changelog.html '当前最新发布：<strong>v[0-9.]+</strong>（([0-9]{4}-[0-9]{2}-[0-9]{2})）' "$d"
	dates=$(perl -0777 -ne 'print "$1\n" while /<lastmod>([0-9]{4}-[0-9]{2}-[0-9]{2})<\/lastmod>/g' site/sitemap.xml | sort -u)
	[[ $dates == "$d" ]] || die "site/sitemap.xml lastmod: expected $d, got '${dates:-<none>}'"
	[[ $(grep -c '<lastmod>' site/sitemap.xml) -eq 5 ]] || die "site/sitemap.xml: expected 5 lastmod entries"
	for f in "${SOCIAL_SVG[@]}"; do
		one "$f version" "$f" '· v([0-9]+\.[0-9]+\.[0-9]+)</text>' "$v"
	done
	cmp -s site/assets/img/social-preview.svg docs/assets/social-preview.svg || die "social-preview SVG copies differ"
	[[ -s site/assets/img/social-preview.png && -s docs/assets/social-preview.png ]] || die "social-preview PNG missing/empty"
	cmp -s site/assets/img/social-preview.png docs/assets/social-preview.png || die "social-preview PNG copies differ"
}

verify_changelog() { # version date
	local v=$1 d=$2 slug=${1//./-} nav id first summary
	nav=$(perl -0777 -ne 'print $1 if /<aside class="doc-nav"[^>]*>(.*?)<\/aside>/s' site/changelog.html)
	[[ -n $nav ]] || die "site/changelog.html: doc-nav not found"
	while IFS= read -r id; do
		if [[ $id == earlier ]]; then
			grep -Fq 'id="earlier"' site/changelog.html || die "changelog nav #earlier has no matching id"
		else
			grep -Fq "id=\"$id\"" site/changelog.html || die "changelog nav #$id has no matching id"
		fi
	done < <(printf '%s\n' "$nav" | perl -ne 'print "$1\n" while /href="#([^"]+)"/g')
	grep -Fq "<a href=\"#v$slug\">v$v</a>" site/changelog.html || die "changelog nav: missing v$v anchor #v$slug"
	first=$(grep -m1 '<details class="changelog-entry"' site/changelog.html) || die "changelog entries not found"
	[[ $first == *"id=\"v$slug\""* ]] || die "changelog: newest entry is not #v$slug"
	[[ $first == *" open"* ]] || die "changelog: newest entry #v$slug is not open"
	summary=$(perl -0777 -ne 'if (/<details class="changelog-entry"[^>]*>\s*<summary><strong>v([0-9]+\.[0-9]+\.[0-9]+)<\/strong><time datetime="([0-9]{4}-[0-9]{2}-[0-9]{2})"/) { print "$1 $2" }' site/changelog.html)
	[[ $summary == "$v $d" ]] || die "changelog: newest summary expected '$v $d', got '${summary:-<none>}'"
}

swap() { # file perl-expression (uses $o/$n/$od/$nd)
	local file=$1 expr=$2
	[[ -f $file ]] || die "missing $file"
	OLD=$OLD_VERSION NEW=$VERSION OD=$OLD_DATE ND=$DATE \
		perl -0777 -pi -e '$o=$ENV{OLD}; $n=$ENV{NEW}; $od=$ENV{OD}; $nd=$ENV{ND}; '"$expr" "$file"
}

prepare_release() {
	local status f
	status=$(git status --porcelain) || die "git status failed"
	[[ -z $status ]] || die "worktree is not clean; commit or stash first"
	if git rev-parse --verify --quiet "refs/tags/v$VERSION" >/dev/null; then
		die "tag v$VERSION already exists"
	fi
	OLD_VERSION=$(pkg_version crates/oss-server/Cargo.toml)
	[[ -n $OLD_VERSION ]] || die "cannot read current version"
	OLD_DATE=$(perl -ne 'if (/最新发布：v[0-9.]+（([0-9]{4}-[0-9]{2}-[0-9]{2})）/) { print $1; exit }' README.md)
	[[ -n $OLD_DATE ]] || die "cannot read current release date from README.md"
	[[ $OLD_VERSION != "$VERSION" ]] || die "already at v$OLD_VERSION"
	[[ $(printf '%s\n%s\n' "$OLD_VERSION" "$VERSION" | sort -V | tail -n1) == "$VERSION" ]] ||
		die "v$VERSION is older than v$OLD_VERSION"
	if [[ $DATE < $OLD_DATE ]]; then
		die "release date $DATE is older than current $OLD_DATE"
	fi
	verify_manifests "$OLD_VERSION"
	verify_anchors "$OLD_VERSION" "$OLD_DATE"
	verify_changelog "$OLD_VERSION" "$OLD_DATE"

	for f in crates/*/Cargo.toml; do
		OLD="$OLD_VERSION" NEW="$VERSION" \
			perl -0777 -pi -e 's/^version = "\Q$ENV{OLD}\E"$/version = "$ENV{NEW}"/m or die "version line not found: $ARGV\n"' "$f"
	done
	npm version "$VERSION" --no-git-tag-version --prefix web >/dev/null || die "npm version failed"
	cargo metadata --offline --format-version 1 >/dev/null || die "cargo metadata --offline failed"

	swap README.md 's{\*\*最新发布：v\Q$o\E（\Q$od\E）。\*\*}{**最新发布：v$n（$nd）。**} or die "README.md anchor: $ARGV\n"'
	swap README.en.md 's{\*\*Latest release: v\Q$o\E \(\Q$od\E\)\.\*\*}{**Latest release: v$n ($nd).**} or die "README.en.md anchor: $ARGV\n"'
	swap AGENTS.md 's{\Q最新发布 **v$o（$od）**\E}{最新发布 **v$n（$nd）**} or die "AGENTS.md anchor: $ARGV\n"'
	swap docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md \
		's{\Q- 最新发布：**v$o**（$od\E}{- 最新发布：**v$n**（$nd} or die "capability-gaps release: $ARGV\n"'
	swap docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md \
		's{\Qtag `v$o`\E}{tag `v$n`} or die "capability-gaps tag: $ARGV\n"'

	swap site/index.html 's{"softwareVersion": "\Q$o\E"}{"softwareVersion": "$n"} or die "index softwareVersion: $ARGV\n"'
	swap site/index.html 's{\Q下载 v$o\E}{下载 v$n}g or die "index download: $ARGV\n"'
	swap site/index.html 's{\Q<strong>v$o \E}{<strong>v$n } or die "index proof version: $ARGV\n"'
	swap site/index.html 's{\Q<span>$od · \E}{<span>$nd · } or die "index proof date: $ARGV\n"'
	swap site/index.html 's{\Q<dd>v$o（最新发布）</dd>\E}{<dd>v$n（最新发布）</dd>} or die "index current version: $ARGV\n"'
	swap site/index.html 's{\Q（$od）</dd>\E}{（$nd）</dd>} or die "index release date: $ARGV\n"'
	swap site/index.html 's{\Qreleases/tag/v$o\E}{releases/tag/v$n}g or die "index tag link: $ARGV\n"'
	swap site/index.html 's{\Qrefs/tags/v$o.tar.gz\E}{refs/tags/v$n.tar.gz}g or die "index tarball: $ARGV\n"'
	swap site/index.html 's{\QApache-2.0 · Rust 2024 · v$o\E}{Apache-2.0 · Rust 2024 · v$n}g or die "index footer: $ARGV\n"'

	swap site/install.html 's{\Q下载 v$o\E}{下载 v$n}g or die "install nav: $ARGV\n"'
	swap site/install.html 's{\Q当前最新发布 v$o；\E}{当前最新发布 v$n；} or die "install current release: $ARGV\n"'
	swap site/install.html 's{\Q请使用 v$o（\E}{请使用 v$n（} or die "install recommendation: $ARGV\n"'
	swap site/install.html 's{\Qreleases/tag/v$o\E}{releases/tag/v$n}g or die "install tag link: $ARGV\n"'
	swap site/install.html 's{\Q打开 v$o Release\E}{打开 v$n Release} or die "install button: $ARGV\n"'
	swap site/install.html 's{\Qrefs/tags/v$o.tar.gz\E}{refs/tags/v$n.tar.gz}g or die "install tarball: $ARGV\n"'
	swap site/install.html 's{\QApache-2.0 · Rust 2024 · v$o\E}{Apache-2.0 · Rust 2024 · v$n}g or die "install footer: $ARGV\n"'

	swap site/usage.html 's{\Q下载 v$o\E}{下载 v$n}g or die "usage nav: $ARGV\n"'
	swap site/usage.html 's{\QApache-2.0 · Rust 2024 · v$o\E}{Apache-2.0 · Rust 2024 · v$n}g or die "usage footer: $ARGV\n"'

	swap site/api.html 's{\Q下载 v$o\E}{下载 v$n}g or die "api nav: $ARGV\n"'
	swap site/api.html 's{\Q从 v1.0.0 到 v$o。\E}{从 v1.0.0 到 v$n。} or die "api range: $ARGV\n"'
	swap site/api.html 's{\QApache-2.0 · Rust 2024 · v$o\E}{Apache-2.0 · Rust 2024 · v$n}g or die "api footer: $ARGV\n"'

	swap site/changelog.html 's{\Q下载 v$o\E}{下载 v$n}g or die "changelog nav: $ARGV\n"'
	swap site/changelog.html 's{\Q从 v1.0.0 到 v$o，\E}{从 v1.0.0 到 v$n，} or die "changelog meta range: $ARGV\n"'
	swap site/changelog.html 's{\Q再到 v$o \E}{再到 v$n }g or die "changelog social range: $ARGV\n"'
	swap site/changelog.html 's{\Q到 v$o 的\E}{到 v$n 的} or die "changelog intro range: $ARGV\n"'
	swap site/changelog.html 's{\Q当前最新发布：<strong>v$o</strong>（$od）。\E}{当前最新发布：<strong>v$n</strong>（$nd）。} or die "changelog latest: $ARGV\n"'
	swap site/changelog.html 's{\QApache-2.0 · Rust 2024 · v$o\E}{Apache-2.0 · Rust 2024 · v$n}g or die "changelog footer: $ARGV\n"'

	swap site/sitemap.xml 's{\Q<lastmod>$od</lastmod>\E}{<lastmod>$nd</lastmod>}g or die "sitemap lastmod: $ARGV\n"'
	for f in "${SOCIAL_SVG[@]}"; do
		swap "$f" 's{\Qgithub.com/erikwang2013/oss · v$o\E}{github.com/erikwang2013/oss · v$n} or die "social SVG version: $ARGV\n"'
	done
	command -v rsvg-convert >/dev/null || die "rsvg-convert not found"
	for f in "${SOCIAL_SVG[@]}"; do
		rsvg-convert -o "${f%.svg}.png" "$f" || die "rsvg-convert failed: $f"
	done

	cat <<EOF

prepare: mechanical anchors updated to v$VERSION ($DATE).
TODO (human, not done by prepare):
  - Rewrite release prose in README.md / README.en.md and the AGENTS.md status paragraph.
  - Update the acceptance-record prose in docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md
    and the historical patch paragraph in docs/superpowers/specs/2026-10-24-architecture-backlog.md.
  - site/index.html proof strip, project facts, intro and release-list rows.
  - site/api.html: add the v$VERSION bullet; keep older rows.
  - site/changelog.html: add the nav anchor and entry body, and make the newest <details> open.
  - GitHub Release notes.
Nothing was committed, tagged, or pushed.
Run: scripts/release.sh check $VERSION $DATE
EOF
}

check_release() {
	verify_manifests "$VERSION"
	verify_anchors "$VERSION" "$DATE"
	verify_changelog "$VERSION" "$DATE"
	cargo metadata --offline --locked --format-version 1 >/dev/null || die "cargo metadata --offline --locked failed"
	printf 'release: check OK: v%s (%s)\n' "$VERSION" "$DATE"
}

case $ACTION in
prepare) prepare_release ;;
check) check_release ;;
esac
