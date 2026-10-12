# aetherupload-rust 升级 checklist

> 本清单由 1.0.4→1.0.5、1.0.5→2.1.0、2.1.0→3.0.0 三轮升级沉淀；aetherupload 是我们上传面的核心集成
> crate，升级属高风险变更，逐项走完再提交。

## 0. 准备

```bash
cargo update -n -v | grep aetherupload          # 看有无新版（跨大版本时 cargo 只报 available）
# 跨大版本：直接取 tarball 供离线审读（registry src 要构建时才解包）
curl -sL -o /tmp/au.crate https://static.crates.io/crates/aetherupload-rust/aetherupload-rust-X.Y.Z.crate
tar xzf /tmp/au.crate -C /tmp/au3/
```

## 1. 依赖面核对（决定能否升）

- `Cargo.toml`：`axum` 版本（必须仍是 0.8.x，与 ecat-transport-http / 我们的 router 同锚）、
  `rust-version`、`edition`、`ecat` feature 的构成（1.x 起恒为 `["axum"]`）。
- 任何超出约束的依赖（如某版要求 axum 0.9）→ 停下评估，不能硬升。

## 2. 集成面逐文件 diff（本仓库的引用面，`grep -rn "aetherupload::" crates/`）

```
src/events.rs         EventSink（IngestSink 实现）
src/instant.rs        InstantStore / InstantIndex / KEY_PREFIX（SqlInstantStore 实现）
src/config.rs         Config / GroupConfig / SubdirRule / StorageDriver
src/console.rs        list_groups / build_redis_hashes / clean_up_directory
src/header.rs         HEADER_DIR
src/runtime.rs        Runtime::{new, with_instant, with_events, config, upload_root}
src/saved_path.rs     SavedPath::{encode, decode}
src/resource.rs       Resource（字段）
src/integrations/ecat.rs + axum.rs   routes(runtime) -> Router
src/error.rs          Error（我们只构造 Io；变体增删要 grep）
```

用 python difflib 逐文件看（**本机系统 diff 不可靠**，见记忆 broken-diff-on-path），
并过滤纯注释改动——2.1.0/3.0.0 的经验：多数差异在文档注释，公共 API 面很稳。

## 3. JS 资产（`assets/aetherupload-all.js`）

```bash
sha256sum <新版>/assets/aetherupload-all.js web/public/aetherupload-assets/aetherupload-all.js
```

- **变了** → 拷贝进 `web/public/aetherupload-assets/`，同步 `web/src/__tests__/assets.test.ts`
  的 `STOCK_SHA256` 与历史注记（注明该版的变化与为何兼容）。
- **没变** → 只更新注记里的版本号。
- 无论变否，都核对：4 个 DOM id、`window.aetherupload` 工厂与 `setPreprocessRoute/setUploadingRoute`、
  成功文案（`上传成功` / `上传成功（秒传）`）——这三样是控制台（useUpload）与 e2e 的契约。

## 4. 协议面变更判定（2.0 协议类）

响应形态/字段名变化（如 2.0：preprocess/uploading 改 200 snake_case JSON + RFC 9457 问题响应）时：

- 服务端 HTTP 路径由 crate 的 integrations 层负责，我们只经 `ecat::routes` 接入——**无需改代码**，
  但必须同步：vendored 脚本（配对方）＋ **测试断言**（grep `"error"`/旧字段名/旧文案）；
- 控制台消费路径：问题响应 title 会由脚本写进 `#aetherupload-output`；
  已有 e2e 用例「上传超限 → 问题响应 title」钉住（`web/e2e/console.spec.ts`）。

## 5. i18n 文案 diff

`src/i18n.rs` 的表逐条对：我们仓库里没有任何文案匹配的地方**除** e2e 的问题响应用例
（钉「错误：无效的文件大小」）——文案变化时同步该断言。

## 6. 代码影响 grep

```bash
grep -rn "PreprocessResult\|SaveChunkResult\|is_known\|\.localized(\|CreateHeaderFail" crates/
```

（历史被删项清单；新版有变体/导出增删再往这里加。）

## 7. 验证（四闸 + 两套 e2e + 指纹）

```bash
cargo test --workspace -j 2            # 四闸之三：fmt / clippy -D warnings / 全量测试
cd web && npm run build && npm test    # web 侧（vitest 慢机注意 startupTimeout，见 vite.config.ts 注释）
```

- e2e 两套（控制台 Playwright + `tests/e2e_s3.sh`）按高负载跑法（预构建二进制 + /tmp 全新目录 + `--workers=1`）。
- **二进制指纹**（可选但便宜）：升级带来的文案/行为变更以
  `strings target/debug/oss-server | grep <新文案>` 证明确实进了产物（3.0.0 实践）。

## 8. 提交

`chore(deps): aetherupload-rust X → Y`——正文写清：变更面（API/JS/协议/文案逐项）、
我们的改动面（应当极小）、四闸与 e2e 实跑数字；有对外协议变化时在版本策略上按 minor 发布。
