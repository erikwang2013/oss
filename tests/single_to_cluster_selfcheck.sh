#!/usr/bin/env bash
# 兼容入口：single-to-cluster 自检已迁移并扩展到 scripts/tests/ 下。
set -euo pipefail
exec "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/scripts/tests/single-to-cluster-selfcheck.sh" "$@"
