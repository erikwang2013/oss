# OSS 控制台（web）

「oss」对象存储的 Web 控制台：登录、桶与对象浏览（上传/下载/删除）、访问密钥管理、
子账户与授权、集群状态（按 set 分组）。桶策略 / 生命周期规则 / 作业（jobs）/ 审计查询
目前为**控制台 API 面**（`/api/v1`，见根 README「桶策略与审计」「生命周期」节），无独立页面。
React 19 + antd 5 + TanStack Query + react-router，Vite 构建。项目总览与后端见根 [README](../README.md)。

## 命令

- `npm run dev` — 开发服务器（http://127.0.0.1:5173；`/api/`、`/aetherupload/` 代理到后端 9000）
- `npm run build` — 类型检查（`tsc -b`）+ 产物输出到 `dist/`
- `npm test` — 单元测试（vitest + jsdom）
- `npm run e2e` — Playwright 关键路径；webServer 会自动拉起 vite 与后端 `cargo run -p oss-server`
  （后端使用临时数据目录/DB，不复用已运行的服务）。首次运行前先安装浏览器：
  `npx playwright install --with-deps chromium`
- `npm run lint` / `npm run typecheck:e2e` — oxlint 与 e2e 目录类型检查

## 目录

- `src/api.ts` — API 客户端（`api`/`rawApi`，401 统一清 token 跳登录）与 token 存取（localStorage + cookie）
- `src/pages/` — 登录 / 桶 / 对象 / 密钥 / 子账户（Users）/ 集群（Cluster）页面；`src/upload/` — aetherupload 分块上传
- `src/__tests__/`、`e2e/` — 单元测试与 Playwright 端到端用例
