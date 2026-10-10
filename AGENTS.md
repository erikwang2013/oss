# oss

自研分布式对象存储系统（S3 兼容，MinIO 的等价实现）。Rust 2024。

## 当前状态

规划完成、代码未开工。**以 docs/ 为准**：

- `docs/superpowers/specs/2026-10-10-minio-clone-design.md` —— 总体设计（9 阶段 M1–M9、盘布局、xl.meta v1）
- `docs/superpowers/specs/2026-10-10-feature-spec.md` —— 19 个功能域规格（行为/接口/边界/错误码）
- `docs/superpowers/plans/2026-10-10-m1a-backend-slice.md` —— M1a 实现计划（16 个 task，含端到端验收脚本）

## 结构（按 M1a 计划）

`crates/xl-store`（存储内核，不依赖任何框架）→ `crates/xl-s3`（s3s 的 S3 trait 实现）→ `crates/oss-server`（e-cat 应用 + 控制台 API + aetherupload 接线）

## 构建与测试

```bash
cargo check --workspace
cargo test --workspace
```

## 注意

- 依赖版本锁在 workspace `Cargo.toml`；axum 必须全仓唯一 0.8.x（ecat 与 aetherupload 都锚定它）
- 执行 M1a 计划时按 task 顺序逐任务做，端到端验收脚本是 `tests/e2e_s3.sh`
