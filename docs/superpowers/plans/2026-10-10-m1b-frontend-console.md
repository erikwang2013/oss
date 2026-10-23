# M1b 前端控制台 实现计划

> **历史文档（2026-10-23 注）**：本文件记录当时的计划与边界。部分“未实现/非目标/后置”
> 已在 2026-10-23 能力清理批次或更早落地；以实现代码、README 与
> `docs/superpowers/specs/2026-10-23-capability-gaps-acceptance.md` 为准。


> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐 M1 的浏览器侧垂直切片：React 控制台（登录 / 桶列表 / 对象浏览器 / 上传面板 / 访问密钥），上传复用 aetherupload 自带 `aetherupload-all.js`（不重写分片逻辑），让设计文档 M1 验收的第 2、3、4 条（1 GB 上传 + 断线续传、秒传命中、控制台可见可下载可删除）在浏览器里全部成立。

**Architecture:** `web/` 是独立 Vite 应用，开发期经 Vite 代理把 `/api/` 与 `/aetherupload/` 转发到 `oss-server`（:9000），生产由 nginx/静态托管（部署在 M2，见设计文档 §12）。上传不做任何分片逻辑：渲染 aetherupload 要求的固定 DOM 结构（`#aetherupload-wrapper` 下的四个 id），由自带脚本驱动分块、续传与秒传，React 只负责宿主容器、分组选择、结果上报（`POST /api/v1/uploads/register`）与历史展示。

**Tech Stack:** React 19 · Vite · TypeScript · antd v5 · @tanstack/react-query v5 · react-router-dom v7 · vitest + @testing-library/react（逻辑单测）· Playwright（关键路径 e2e）· aetherupload-all.js（原样复用，含 zepto + spark-md5）

**Spec:** `docs/superpowers/specs/2026-10-10-minio-clone-design.md` §3.1/§6.2/§8 M1；`docs/superpowers/specs/2026-10-10-feature-spec.md` §5/§17。前置：M1a 全部 16 个 task 已完成（控制台 API 在 `crates/oss-server/src/console.rs`）。

---

## 执行前必读：三件事

1. **aetherupload-all.js 的集成契约是 DOM，不是 JS API**（详见 Task 8）：全局函数 `aetherupload(fileInput)` 通过 `$(resource).parents('#aetherupload-wrapper')` 找容器，再在容器里找 `#aetherupload-resource`（文件 input）、`#aetherupload-output`（文本输出）、`#aetherupload-progressbar`（进度条，脚本直接改 css width）、`#aetherupload-savedpath`（完成后填 savedPath）。**这些节点必须由 React 渲染出来且不允许 React 再渲染其内容**（文本与宽度由 zepto 直接改）。脚本只有 `success(callback)` 一个回调，没有 error/onProgress 回调——错误只写进 `#aetherupload-output`（已含中文文案「错误：上传失败」「网络故障，正在重试……」）。
2. **秒传在协议层的表现**：`/aetherupload/preprocess` 响应里 `savedPath` 非空 = 秒传命中，一个分块都不会传。前端靠「本次上传期间没有 POST 过 `/aetherupload/uploading`」判定秒传（Task 8 的 XHR 计数包装）。不要解析 `#aetherupload-output` 的中文文案。
3. **Vite 代理的坑**：`/aetherupload-assets/`（自带脚本本体）由 Vite 从 `public/` 直接服务，代理规则必须写成 `'/aetherupload/'`（带尾斜杠），否则会被 `'/aetherupload'` 前缀吞掉转发到后端 404。

---

## 文件结构

```
oss/
├── .github/workflows/ci.yml             # 已就位（本计划落 rust + web 两个 job，web 在 package.json 出现后自动生效）
└── web/
    ├── package.json
    ├── vite.config.ts                   # dev 代理 + vitest 配置
    ├── tsconfig.json · tsconfig.node.json
    ├── index.html                       # 挂 /aetherupload-assets/aetherupload-all.js
    ├── playwright.config.ts             # 双 webServer：oss-server + vite
    ├── public/
    │   └── aetherupload-assets/aetherupload-all.js   # 从 crate assets 原样拷贝（Task 8）
    ├── src/
    │   ├── main.tsx                     # QueryClientProvider + ConfigProvider(zh_CN) + RouterProvider
    │   ├── App.tsx                      # 路由表 + RequireAuth + 侧边栏 Layout
    │   ├── api.ts                       # fetch 封装（token/401/错误体）+ 类型
    │   ├── pages/
    │   │   ├── Login.tsx
    │   │   ├── Buckets.tsx
    │   │   ├── Objects.tsx              # 面包屑前缀导航 + 列表 + 下载 + 删除 + 上传入口
    │   │   └── Keys.tsx
    │   └── upload/
    │       ├── useUpload.ts             # 驱动 aetherupload-all.js + 秒传计数 + register 上报
    │       └── UploadPanel.tsx          # 分组选择 · 进度条宿主 · 输出区 · 秒传提示 · 上传历史
    ├── src/__tests__/api.test.ts
    ├── src/__tests__/useUpload.test.ts
    └── e2e/
        ├── console.spec.ts              # 登录→建桶→上传→列表→下载→删除
        └── instant.spec.ts              # 同文件二传：无 /uploading POST
```

**与 M1a 的边界**：M1b 只改一处后端——新增 `GET /api/v1/groups`（Task 7，上传面板的分组下拉需要它）。其余全部是新增 `web/`。

---

## Task 1: web/ 脚手架 + 代理 + 构建

**Files:**
- Create: `web/`（Vite 模板 + 手改配置）
- Modify: `web/vite.config.ts`、`web/index.html`、`web/package.json`

- [x] **Step 1: 生成模板并装依赖**

```bash
cd /home/wwwroot/oss
npm create vite@latest web -- --template react-ts
cd web && npm install
npm install antd@5 @tanstack/react-query react-router-dom   # 裸装 antd 已是 v6，按技术栈钉 v5
npm install -D vitest jsdom @testing-library/react @playwright/test
```

Expected: `web/` 目录生成，`package.json` 出现上述依赖。React 版本以模板为准（应为 19.x）。

- [x] **Step 2: `package.json` 的 scripts**

```json
"scripts": {
  "dev": "vite",
  "build": "tsc -b && vite build",
  "preview": "vite preview",
  "test": "vitest run --passWithNoTests",
  "e2e": "playwright test"
}
```

- [x] **Step 3: `vite.config.ts`（代理 + vitest）**

```ts
/// <reference types="vitest/config" />
import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'

const backend = 'http://127.0.0.1:9000'

export default defineConfig({
  plugins: [react()],
  server: {
    // Vite 8 默认 host 'localhost' 在本机只解析到 ::1，而计划里 Playwright/浏览器
    // 用的都是 http://127.0.0.1:5173 —— 显式绑 IPv4 保证两边一致
    host: '127.0.0.1',
    proxy: {
      // 注意尾斜杠：'/aetherupload/' 不能写成 '/aetherupload'，
      // 否则 /aetherupload-assets/（public 下的自带脚本）会被误转发到后端 404
      '/api/': backend,
      '/aetherupload/': backend,
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.test.ts', 'src/**/*.test.tsx'],
  },
})
```

- [x] **Step 4: `index.html` 挂自带脚本**

`web/index.html` 的 `<head>` 末尾加（放 head 里可避免 React 首屏后脚本未加载）：

```html
<script src="/aetherupload-assets/aetherupload-all.js"></script>
```

（`public/aetherupload-assets/` 的内容在 Task 8 拷贝；此步先建空目录占位 `mkdir -p public/aetherupload-assets`。）

- [x] **Step 5: 验证 dev 起得来、构建通过**

```bash
npm run dev &      # 浏览器打开 http://127.0.0.1:5173 看到模板页即成功（此时后端可不在）
sleep 3 && kill %1
npm run build      # 须成功（tsc -b && vite build）
```

- [x] **Step 6: 提交**

```bash
cd /home/wwwroot/oss
git add web/ && git commit -m "chore(web): Vite + React19 + antd5 脚手架，代理 /api 与 /aetherupload"
```

---

## Task 2: api.ts —— fetch 封装与类型（TDD）

**Files:**
- Create: `web/src/api.ts`
- Test: `web/src/__tests__/api.test.ts`

- [x] **Step 1: 写失败的测试**

```ts
// web/src/__tests__/api.test.ts
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, api, getToken, setToken } from '../api'

afterEach(() => {
  vi.unstubAllGlobals()
  localStorage.clear()
})

describe('token 存取', () => {
  it('set/get/清除', () => {
    expect(getToken()).toBeNull()
    setToken('t1')
    expect(getToken()).toBe('t1')
    setToken(null)
    expect(getToken()).toBeNull()
  })
})

describe('api()', () => {
  it('带 token 时加 Authorization 头', async () => {
    setToken('tok')
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ ok: 1 }), { status: 200, headers: { 'content-type': 'application/json' } }),
    )
    vi.stubGlobal('fetch', fetchMock)

    const out = await api<{ ok: number }>('/api/v1/buckets')
    expect(out.ok).toBe(1)
    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(new Headers(init.headers).get('authorization')).toBe('Bearer tok')
  })

  it('401 时清 token 且抛 ApiError', async () => {
    setToken('tok')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('用户名或密码错误', { status: 401 })))
    const p = api('/api/v1/buckets')
    await expect(p).rejects.toBeInstanceOf(ApiError)
    await expect(p).rejects.toMatchObject({ status: 401, message: '用户名或密码错误' })
    expect(getToken()).toBeNull()
  })

  it('非 2xx 抛 ApiError 且带状态码与响应文本', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('桶非空', { status: 409 })))
    // api<never>：不写类型参数时 T 推断为 unknown，catch 结果会塌成 unknown 而无法访问 .status
    const err = await api<never>('/api/v1/buckets/a').catch((e) => e as ApiError)
    expect(err.status).toBe(409)
    expect(err.message).toBe('桶非空')
  })

  it('204 返回 undefined', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))
    await expect(api('/api/v1/buckets/a', { method: 'DELETE' })).resolves.toBeUndefined()
  })

  it('有 body 设 content-type: application/json，无 body 不设', async () => {
    const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }))
    vi.stubGlobal('fetch', fetchMock)
    await api('/api/v1/buckets', { method: 'POST', body: JSON.stringify({ bucket: 'b' }) })
    const withBody = fetchMock.mock.calls[0][1] as RequestInit
    expect(new Headers(withBody.headers).get('content-type')).toBe('application/json')
    await api('/api/v1/buckets')
    const noBody = fetchMock.mock.calls[1][1] as RequestInit
    expect(new Headers(noBody.headers).has('content-type')).toBe(false)
  })
})
```
- [x] **Step 2: 跑测试确认失败**

Run: `cd web && npm test`
Expected: FAIL（`../api` 不存在）

- [x] **Step 3: 实现 `web/src/api.ts`**

```ts
const TOKEN_KEY = 'oss.token'

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}
export function setToken(t: string | null): void {
  if (t) localStorage.setItem(TOKEN_KEY, t)
  else localStorage.removeItem(TOKEN_KEY)
}

export class ApiError extends Error {
  status: number // 手写赋值：tsconfig 开了 erasableSyntaxOnly，参数属性（constructor(public x)）不可用
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

/** 统一的控制台 API 客户端。401 时清 token 并跳登录页。 */
export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  const token = getToken()
  if (token) headers.set('authorization', `Bearer ${token}`)
  // FormData 让 fetch 自己带 multipart boundary，手设 content-type 会把 boundary 弄丢
  if (init.body && !(init.body instanceof FormData) && !headers.has('content-type')) headers.set('content-type', 'application/json')

  const resp = await fetch(path, { ...init, headers })
  if (resp.status === 401) {
    setToken(null)
    const msg = (await resp.text()) || '未登录'
    if (window.location.pathname !== '/login') window.location.href = '/login'
    throw new ApiError(401, msg)
  }
  if (!resp.ok) throw new ApiError(resp.status, (await resp.text()) || resp.statusText)
  if (resp.status === 204) return undefined as T
  return (await resp.json()) as T
}

// ——— 服务端响应类型（与 M1a `console.rs` 对齐）———

export interface UploadRecord {
  saved_path: string
  bucket: string
  key: string
  group_name: string
  size: number
  etag: string
  original_name: string | null
  created_at: number
}

export interface ObjectEntry {
  key: string
  size: number
  etag: string
  content_type: string | null
}
export interface ObjectList {
  objects: ObjectEntry[]
  common_prefixes: string[]
  is_truncated: boolean
  next_token: string | null
}

export interface GroupInfo {
  group: string
  bucket: string
  max_size: number
  extensions: string[]
}

export interface AccessKey {
  access_key: string
  secret_key: string
}
```
> 注：`tsconfig.app.json` 开了 `erasableSyntaxOnly`（Vite 8 脚手架默认），TS 参数属性（`constructor(public status: number)`）会报 TS1294，故 `ApiError` 用显式字段。另开了 `verbatimModuleSyntax`：从本模块导入类型须写 `import { type ObjectList }` 形式，否则报 TS1484。

- [x] **Step 4: 跑测试**

Run: `cd web && npm test`
Expected: PASS（5 个用例）

- [x] **Step 5: 提交**

```bash
git add web/src/api.ts web/src/__tests__/api.test.ts
git commit -m "feat(web): 控制台 API 客户端（token/401/错误体）"
```

---

## Task 3: App 骨架 —— 路由 · 鉴权 · 布局

**Files:**
- Create: `web/src/App.tsx`（覆盖模板）、`web/src/main.tsx`（覆盖模板）
- Modify: `web/index.html`（`lang="zh-CN"`、`<title>OSS 控制台</title>`）、`web/.gitignore`（补 e2e 产物三行）
- Delete: 模板孤儿资产（App.tsx/main.tsx 覆盖后全部失去引用）——`web/src/App.css`、`web/src/index.css`、`web/src/assets/`（整目录：hero.png、vite.svg、react.svg）、`web/public/icons.svg`

- [x] **Step 1: 实现 `web/src/App.tsx`**

```tsx
import { Navigate, Outlet, createBrowserRouter } from 'react-router-dom'
import { Layout, Menu } from 'antd'
import { Link, useLocation } from 'react-router-dom'
import { DatabaseOutlined, KeyOutlined } from '@ant-design/icons'
import { getToken } from './api'
import Login from './pages/Login'
import Buckets from './pages/Buckets'
import Objects from './pages/Objects'
import Keys from './pages/Keys'

function Shell() {
  const loc = useLocation()
  const selected = loc.pathname.startsWith('/keys') ? '/keys' : '/buckets'
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="light">
        <Menu
          mode="inline"
          selectedKeys={[selected]}
          items={[
            { key: '/buckets', icon: <DatabaseOutlined />, label: <Link to="/buckets">桶与对象</Link> },
            { key: '/keys', icon: <KeyOutlined />, label: <Link to="/keys">访问密钥</Link> },
          ]}
        />
      </Layout.Sider>
      <Layout.Content style={{ padding: 24 }}>
        <Outlet />
      </Layout.Content>
    </Layout>
  )
}

function RequireAuth() {
  if (!getToken()) return <Navigate to="/login" replace />
  return <Shell />
}

export const router = createBrowserRouter([
  { path: '/login', element: <Login /> },
  {
    element: <RequireAuth />,
    children: [
      { path: '/', element: <Navigate to="/buckets" replace /> },
      { path: '/buckets', element: <Buckets /> },
      { path: '/buckets/:bucket', element: <Objects /> },
      { path: '/keys', element: <Keys /> },
    ],
  },
])
```

`@ant-design/icons` 需安装：`npm install @ant-design/icons@^5`（钉 v5：裸装是 v6、配套 antd v6，本项目 antd 5.29.x 与 icons v5 配对，且会与 antd 内部依赖去重）（并入 Step 5 的提交）。

- [x] **Step 2: 覆盖 `web/src/main.tsx`**

```tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import { RouterProvider } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { App as AntApp, ConfigProvider } from 'antd'
import zhCN from 'antd/locale/zh_CN'
import 'antd/dist/reset.css'
import { router } from './App'

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider locale={zhCN}>
      <AntApp>
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </AntApp>
    </ConfigProvider>
  </React.StrictMode>,
)
```

模板 `App.tsx` 其余内容整份替换；`App.css`、`index.css`、`src/assets/`、`public/icons.svg` 随之失去全部引用，一并删除（见 Files 的 Delete 行）。

同一轮模板收尾：`web/index.html` 的 `lang="en"` → `lang="zh-CN"`、`<title>web</title>` → `<title>OSS 控制台</title>`；`web/.gitignore` 补 `coverage`、`playwright-report`、`test-results` 三行（Task 11 跑 e2e 会产生后两者）。

- [x] **Step 3: 先建四个页面的空壳**（让编译成立，Task 4-6/10 逐个填）

```tsx
// web/src/pages/Login.tsx / Buckets.tsx / Objects.tsx / Keys.tsx 各一
export default function Login() { return <div>登录</div> }
```

- [x] **Step 4: 跑构建验证**

Run: `cd web && npm run build`
Expected: 成功。

- [x] **Step 5: 提交**

```bash
git add web/package.json web/package-lock.json web/src/
git commit -m "feat(web): 路由 + 鉴权壳 + antd 布局"
```

---

## Task 4: 登录页

**Files:**
- Modify: `web/src/pages/Login.tsx`

- [x] **Step 1: 实现**

```tsx
import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { App, Button, Card, Form, Input } from 'antd'
import { api, setToken } from '../api'

export default function Login() {
  const nav = useNavigate()
  const { message } = App.useApp()
  const [loading, setLoading] = useState(false)

  const onFinish = async (v: { username: string; password: string }) => {
    setLoading(true)
    try {
      const { token } = await api<{ token: string }>('/api/v1/login', {
        method: 'POST',
        body: JSON.stringify(v),
      })
      setToken(token)
      nav('/buckets', { replace: true })
    } catch (e) {
      message.error(e instanceof Error && e.message ? e.message : '登录失败')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ display: 'flex', justifyContent: 'center', paddingTop: 120 }}>
      <Card title="OSS 控制台" style={{ width: 360 }}>
        <Form onFinish={onFinish} layout="vertical">
          <Form.Item name="username" label="用户名" rules={[{ required: true }]}>
            <Input autoFocus />
          </Form.Item>
          <Form.Item name="password" label="口令" rules={[{ required: true }]}>
            <Input.Password />
          </Form.Item>
          <Button type="primary" htmlType="submit" block loading={loading}>
            登录
          </Button>
        </Form>
      </Card>
    </div>
  )
}
```

注意：登录接口失败时后端返回纯文本（非 JSON），`api()` 抛出的 `ApiError.message` 就是「用户名或密码错误」——直接展示。

> 补丁（Task 8 轮次并入，后经质量评审收口）：种 cookie 的责任落在 `api.ts` 的 `setToken()` 里
> （`document.cookie = \`oss_token=${t}; path=/; SameSite=Lax; max-age=43200\``；`setToken(null)` 时
> `max-age=0` 清掉），本页无需改动。
> 原因：aetherupload-all.js 的分块 XHR 虽然 `withCredentials: true`，但它不经 `api()`、不会加
> `Authorization` 头；后端 `/aetherupload/*` 的 JWT 覆盖层（`router.rs`）认 cookie `oss_token`。
> 不种 cookie，浏览器上传首块就 401。放进 `setToken` 是因为 cookie 必须与 localStorage 里的 token
> 同生命周期——否则 401 清 token / 将来登出后，残留 cookie 仍带着一个已被丢弃的凭据。

- [x] **Step 2: 验证**

```bash
cd /home/wwwroot/oss
# 起后端（前提：M1a 已完成）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw cargo run -p oss-server &
cd web && npm run dev &
```

浏览器打开 `http://127.0.0.1:5173/login`：错密码提示「用户名或密码错误」；对密码跳转 `/buckets`（此时桶页还是壳）。验证完 `kill %1 %2`。

> 执行注记：后端控制台 API 在 M1a 完成后才能实跑手验，本轮（M1b worktree 内无 `oss-server`）跳过浏览器手验；真实登录流程由 Task 11 的 Playwright e2e 覆盖。本轮替代验证：`cd web && npm test`（6 用例不回归）、`npm run build`（tsc 通过）、`npm run lint`。

- [x] **Step 3: 提交**

```bash
git add web/src/pages/Login.tsx
git commit -m "feat(web): 登录页"
```

---

## Task 5: 桶列表页

**Files:**
- Modify: `web/src/pages/Buckets.tsx`

- [x] **Step 1: 实现**

```tsx
import { useState } from 'react'
import { Link } from 'react-router-dom'
import { App, Button, Input, Modal, Popconfirm, Table } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../api'

export default function Buckets() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')

  const { data: buckets = [], isLoading } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<string[]>('/api/v1/buckets'),
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['buckets'] })

  const create = useMutation({
    mutationFn: (name: string) =>
      api('/api/v1/buckets', { method: 'POST', body: JSON.stringify({ name }) }),
    onSuccess: () => { message.success('已创建'); setCreating(false); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const remove = useMutation({
    mutationFn: (name: string) => api(`/api/v1/buckets/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    onSuccess: () => { message.success('已删除'); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  return (
    <>
      <Button type="primary" onClick={() => { setNewName(''); setCreating(true) }} style={{ marginBottom: 16 }}>
        新建桶
      </Button>
      <Table
        rowKey={(r) => r}
        loading={isLoading}
        dataSource={buckets}
        columns={[
          // 不用 dataIndex：antd 对 string[] 记录取 dataIndex:0 会拿到首字符（"alpha"→"a"）；无 dataIndex 时 render 第一参即整条记录
          { title: '桶名', render: (name: string) => <Link to={`/buckets/${encodeURIComponent(name)}`}>{name}</Link> },
          {
            title: '操作',
            width: 120,
            render: (_: unknown, name: string) => (
              <Popconfirm title={`删除桶 ${name}？（须为空桶）`} onConfirm={() => remove.mutate(name)}>
                <Button danger size="small">删除</Button>
              </Popconfirm>
            ),
          },
        ]}
      />
      <Modal
        title="新建桶"
        open={creating}
        onCancel={() => setCreating(false)}
        onOk={() => {
          if (!newName) { message.warning('请输入桶名'); return }
          create.mutate(newName)
        }}
        confirmLoading={create.isPending}
      >
        <Input
          id="new-bucket-name"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="3-63 字符，小写字母数字与 - ."
          autoFocus
        />
      </Modal>
    </>
  )
}
```

实现注记：桶名列去掉了计划初稿的 `dataIndex: 0`——antd 对 `string[]` 记录按 `get(record, [0])` 取值，`"alpha"` 会渲染成首字符 `"a"`；去掉后 `render` 第一参即是整条记录（已用 antd Table 实测两种写法的渲染结果）。

- [x] **Step 2: 验证**

浏览器 `/buckets`：建 `alpha` → 列表出现；点进去壳页；回列表删 `alpha`（空桶）成功；删非空桶报 `BucketNotEmpty`（错误文案来自后端纯文本）。

> 执行注记：同 Task 4——后端控制台 API（M1a Task 15 未落地）本轮无实跑条件，跳过浏览器手验；真实流程（建桶 / 进入 / 删空桶 / 删非空桶报 `BucketNotEmpty`）由 Task 11 的 Playwright e2e 覆盖。本轮替代验证：`cd web && npm test`（6 用例不回归）、`npm run build`、`npm run lint`。

- [x] **Step 3: 提交**

```bash
git add web/src/pages/Buckets.tsx
git commit -m "feat(web): 桶列表页（建/删/进入）"
```

---

## Task 6: 对象浏览器页

**Files:**
- Modify: `web/src/pages/Objects.tsx`

- [x] **Step 1: 实现**（前缀导航来自 `common_prefixes`，下载走 fetch→blob，避免给浏览器下载请求塞 JWT 头的难题）

```tsx
import { useMemo } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { App, Breadcrumb, Button, Popconfirm, Table, Tooltip, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, getToken, type ObjectList } from '../api'

function humanSize(n: number) {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`
}

// 显式给出行类型：否则 antd Table 会从各列 render 的参数注解反推 RecordType，推出带 string 字面量的错误类型，rowKey 与 render 都编译不过
type Row = { kind: 'dir'; key: string } | { kind: 'obj'; key: string; size: number; etag: string }

export default function Objects() {
  const { bucket = '' } = useParams()
  const { message } = App.useApp()
  const qc = useQueryClient()
  // prefix 的单一事实源是 URL query：Link 是 SPA 导航、同路由仅 query 变化不重挂载，useState 快照会让面包屑/手改 URL/返回键都失效
  const [sp, setSp] = useSearchParams()
  const prefix = sp.get('prefix') ?? ''

  const { data, isLoading } = useQuery({
    queryKey: ['objects', bucket, prefix],
    queryFn: () =>
      api<ObjectList>(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/objects?prefix=${encodeURIComponent(prefix)}&delimiter=/`,
      ),
    enabled: !!bucket,
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['objects', bucket] })

  // 对象键一律整键 encodeURIComponent（删除/下载同款）：axum 的 {*key} 通配段先按路径匹配再解码，%2F 不会被拆段，含空格/#/? 的键才能走对
  const remove = useMutation({
    mutationFn: (key: string) =>
      api(`/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`, { method: 'DELETE' }),
    onSuccess: () => { message.success('已删除'); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const download = async (key: string) => {
    try {
      const resp = await fetch(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`,
        { headers: { authorization: `Bearer ${getToken() ?? ''}` } },
      )
      if (!resp.ok) throw new Error(await resp.text())
      const blob = await resp.blob() // M1 大对象全量进内存的上限，M2 流式
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = key.split('/').pop() || 'download'
      a.click()
      setTimeout(() => URL.revokeObjectURL(url), 0)
    } catch (e) {
      message.error(e instanceof Error ? e.message : '下载失败')
    }
  }

  const crumbs = useMemo(() => {
    const parts = prefix.split('/').filter(Boolean)
    return [{ title: <Link to={`/buckets/${encodeURIComponent(bucket)}`}>{bucket}</Link> }].concat(
      parts.map((p, i) => ({
        title: (
          <Link to={`/buckets/${encodeURIComponent(bucket)}?prefix=${encodeURIComponent(parts.slice(0, i + 1).join('/') + '/')}`}>
            {p}
          </Link>
        ),
      })),
    )
  }, [bucket, prefix])

  const rows = [
    ...(data?.common_prefixes ?? []).map((p) => ({ kind: 'dir' as const, key: p })),
    ...(data?.objects ?? []).map((o) => ({ kind: 'obj' as const, key: o.key, size: o.size, etag: o.etag })),
  ]

  return (
    <>
      <Breadcrumb items={crumbs} style={{ marginBottom: 16 }} />
      <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
        上传入口在 Task 9 接；M1 上传面板就绪前可用 `aws s3 cp` / `rclone` 注入对象。
      </Typography.Text>
      <Table<Row>
        rowKey={(r) => r.key}
        loading={isLoading}
        dataSource={rows}
        pagination={false}
        columns={[
          {
            title: '名称',
            dataIndex: 'key',
            render: (key: string, r: { kind: 'dir' | 'obj' }) =>
              r.kind === 'dir' ? (
                <Button type="link" size="small" onClick={() => setSp(key ? { prefix: key } : {})}>
                  {key.slice(prefix.length)}
                </Button>
              ) : (
                <Tooltip title={key}>{key.slice(prefix.length)}</Tooltip>
              ),
          },
          {
            title: '大小',
            width: 120,
            render: (_: unknown, r: { kind: string; size?: number }) => (r.kind === 'dir' ? '-' : humanSize(r.size ?? 0)),
          },
          { title: 'ETag', dataIndex: 'etag', width: 320, render: (v?: string) => v ?? '-' },
          {
            title: '操作',
            width: 160,
            render: (_: unknown, r: { kind: 'dir' | 'obj'; key: string }) =>
              r.kind === 'obj' && (
                <>
                  <Button size="small" onClick={() => download(r.key)} style={{ marginRight: 8 }}>下载</Button>
                  <Popconfirm title={`删除 ${r.key}？`} onConfirm={() => remove.mutate(r.key)}>
                    <Button size="small" danger>删除</Button>
                  </Popconfirm>
                </>
              ),
          },
        ]}
      />
    </>
  )
}
```

实现注记：`<Table>` 加了显式泛型 `<Table<Row>>`（`Row` = dir/obj 联合类型）——计划初稿的无泛型写法下，antd 会从各列 `render` 的参数注解反推 `RecordType`，推出 `{ kind: string; size?: number }` 这类错误类型，`rowKey` 与三处 render 全部编译不过（`tsc` 报 TS2339/TS2322）；列定义本身未动。

面包屑/前缀注记（裁决重开）：计划初稿假设「面包屑 `Link` 整页跳转、组件重挂载、状态自然重置」不成立——react-router `Link` 是 SPA 导航，同路由仅 query 变化不重挂载，`useState` 快照会让面包屑回退/手改 URL/返回键失效；经评审改为 `useSearchParams` 单一事实源（此前《注意》段的「URLSearchParams 初始化」写法作废，原段与「整页跳转」表述已删）。下载与可达性小修随本轮评审一并吸收：`getToken()` 取代直读 localStorage、`revokeObjectURL` 延迟到宏任务、`|| 'download'` 空串边界、blob 全量入内存上限注释、dir 点击改 `<Button type="link">` 键盘可达。

- [x] **Step 2: 验证**

用 `aws s3 cp` 往 `alpha` 放 `docs/a.txt`、`docs/b.txt`、`top.txt` → 控制台 `/buckets/alpha` 看到 `docs/` 与 `top.txt`；点 `docs/` 进前缀，面包屑回根；下载 `a.txt` 字节一致（`cmp`）；删 `top.txt` 后列表刷新。

> 执行注记：同 Task 4——后端控制台 API（M1a Task 15 未落地）本轮无实跑条件，跳过浏览器手验；真实流程（前缀导航 / 下载字节一致 / 删除刷新）由 Task 11 的 Playwright e2e 覆盖。本轮替代验证：`cd web && npm test`（6 用例不回归）、`npm run build`、`npm run lint`。另：下载/删除路径的整键 `encodeURIComponent` 与 axum `{*key}` 通配段的解码行为本轮无法本地复核，一并留给 Task 11 e2e 验证。

- [x] **Step 3: 提交**

```bash
git add web/src/pages/Objects.tsx
git commit -m "feat(web): 对象浏览器（前缀导航/下载/删除）"
```

---

## Task 7: 后端补一个接口 —— `GET /api/v1/groups`

上传面板的分组下拉需要知道 `oss.toml` 的 `[[groups]]`。这是 M1b 唯一的后端改动。

**Files:**
- Modify: `crates/oss-server/src/config.rs`（`GroupTarget` 加 `Serialize`）
- Modify: `crates/oss-server/src/console.rs`（新路由）
- Test: `crates/oss-server/tests/console_api.rs`

- [x] **Step 1: 写失败的测试**（追加到 `console_api.rs`）

```rust
#[tokio::test]
async fn list_groups_returns_configured_groups() {
    let dir = tempfile::tempdir().unwrap();
    let st = state(dir.path()).await; // common::state() 里已配 file → files
    st.db.create_user("erik", "pw", true).await.unwrap();
    let app = build_router(st);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let base = format!("http://{addr}");

    let client = reqwest::Client::new();
    let login: serde_json::Value = client
        .post(format!("{base}/api/v1/login"))
        .json(&serde_json::json!({"username": "erik", "password": "pw"}))
        .send().await.unwrap().json().await.unwrap();
    let token = login["token"].as_str().unwrap();

    let groups: serde_json::Value = client
        .get(format!("{base}/api/v1/groups"))
        .bearer_auth(token)
        .send().await.unwrap().json().await.unwrap();

    assert_eq!(groups[0]["group"], "file");
    assert_eq!(groups[0]["bucket"], "files");
}
```

- [x] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test console_api list_groups 2>&1 | tail -8`
Expected: FAIL（404）

- [x] **Step 3: 实现**

`config.rs`：`GroupTarget` 的 derive 加 `Serialize`（文件顶部 `use serde::{Deserialize, Serialize};` 或合并到现有 use）：

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GroupTarget {
    pub group: String,
    pub bucket: String,
    pub max_size: u64,
    pub extensions: Vec<String>,
}
```

`console.rs`：路由表加一行 `GroupInfo`，并加 handler：

```rust
        .route("/groups", get(list_groups))
```

```rust
/// 上传分组（oss.toml 的 `[[groups]]`），供上传面板的分组下拉使用。
async fn list_groups(State(state): State<AppState>, headers: HeaderMap) -> impl IntoResponse {
    if let Err(s) = auth(&state, &headers) { return s.into_response(); }
    Json(state.cfg.groups.clone()).into_response()
}
```

- [x] **Step 4: 跑测试**

Run: `cargo test -p oss-server 2>&1 | tail -8`
Expected: 全绿

- [x] **Step 5: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): GET /api/v1/groups（上传分组）"
```

---

## Task 8: 拷贝自带脚本 + `useUpload` hook（TDD）

**Files:**
- Create: `web/public/aetherupload-assets/aetherupload-all.js`（从 crate 原样拷贝）
- Create: `web/src/upload/useUpload.ts`
- Test: `web/src/__tests__/useUpload.test.ts`

- [x] **Step 1: 拷贝脚本**

```bash
cd /home/wwwroot/oss
mkdir -p web/public/aetherupload-assets
cp "$(find ~/.cargo/registry/src -path '*aetherupload-rust-1.0.3/assets/aetherupload-all.js' | head -1)" \
   web/public/aetherupload-assets/
ls -l web/public/aetherupload-assets/   # 应有 ~48KB 的 aetherupload-all.js
```

若 `~/.cargo/registry` 里找不到（还没构建过），先在仓库根跑一次 `cargo fetch` 再重试。
拷贝后**先读一遍 crate 内的 `assets/example.html`**（在 `~/.cargo/registry/src/*/aetherupload-rust-1.0.3/assets/`）——它是集成契约的权威示例，DOM 结构与本 hook 的用法都以它为准。

- [x] **Step 2: 写失败的测试**

```ts
// web/src/__tests__/useUpload.test.ts
import { afterEach, describe, expect, it, vi } from 'vitest'
import { renderHook } from '@testing-library/react'
import { countingUploadingPosts, useUpload, type AetherInstance } from '../upload/useUpload'

afterEach(() => {
  vi.unstubAllGlobals()
  document.body.innerHTML = ''
})

describe('countingUploadingPosts', () => {
  it('只统计 /aetherupload/uploading 的 POST', () => {
    const c = countingUploadingPosts()
    const a = new XMLHttpRequest()
    a.open('POST', '/aetherupload/uploading')
    const b = new XMLHttpRequest()
    b.open('POST', '/aetherupload/preprocess')
    const d = new XMLHttpRequest()
    d.open('GET', '/aetherupload/uploading')
    expect(c.count()).toBe(1)
    c.restore()
  })

  it('restore 后恢复原始 XMLHttpRequest', () => {
    const orig = window.XMLHttpRequest
    const c = countingUploadingPosts()
    expect(window.XMLHttpRequest).not.toBe(orig)
    c.restore()
    expect(window.XMLHttpRequest).toBe(orig)
  })

  it('后装先还原：restore 只摘自己那一层，不踩掉更晚的包装', () => {
    const orig = window.XMLHttpRequest
    const older = countingUploadingPosts()
    const newer = countingUploadingPosts()
    older.restore() // 内层先还原：是 no-op，外层仍在位
    new XMLHttpRequest().open('POST', '/aetherupload/uploading')
    expect(newer.count()).toBe(1)
    newer.restore()
    expect(window.XMLHttpRequest).not.toBe(orig) // 轮到内层包装回到栈顶
    older.restore()
    expect(window.XMLHttpRequest).toBe(orig)
  })
})

/** 面板 DOM：库从 `parents('#aetherupload-wrapper')` 里找 #aetherupload-savedpath。 */
function panelDom(savedPath: string): HTMLInputElement {
  document.body.innerHTML = `
    <div id="aetherupload-wrapper">
      <input type="file" id="aetherupload-resource" />
      <input type="text" id="aetherupload-savedpath" value="${savedPath}" />
    </div>`
  return document.querySelector<HTMLInputElement>('#aetherupload-resource')!
}

/** 假 window.aetherupload：upload() 时按用例安排的动作触发 success 回调。 */
function stubAetherupload(upload: (fire: () => void) => void): void {
  let fire: (() => void) | undefined
  const inst: AetherInstance = {
    setGroup: () => inst,
    setSavedPathField: () => inst,
    setPreprocessRoute: () => inst,
    setUploadingRoute: () => inst,
    setLaxMode: () => inst,
    success: (cb) => ((fire = cb), inst),
    upload: () => upload(fire!),
  }
  vi.stubGlobal('aetherupload', () => inst)
}

describe('useUpload', () => {
  it('零 uploading POST ⇒ instant: true，savedPath 从面板读出，且清掉 input disabled', () => {
    const input = panelDom('uploads/abc.txt')
    input.setAttribute('disabled', '') // 上次上传后脚本留下的
    stubAetherupload((fire) => fire())
    const onFinished = vi.fn()
    const { result } = renderHook(() => useUpload({ onFinished }))

    result.current.start(input, 'file')

    expect(input.hasAttribute('disabled')).toBe(false)
    expect(onFinished).toHaveBeenCalledWith({ savedPath: 'uploads/abc.txt', instant: true })
  })

  it('有过 uploading POST ⇒ instant: false', () => {
    const input = panelDom('uploads/def.txt')
    stubAetherupload((fire) => {
      new XMLHttpRequest().open('POST', '/aetherupload/uploading') // 模拟真分块传输
      fire()
    })
    const onFinished = vi.fn()
    const { result } = renderHook(() => useUpload({ onFinished }))

    result.current.start(input, 'file')

    expect(onFinished).toHaveBeenCalledWith({ savedPath: 'uploads/def.txt', instant: false })
  })

  it('window.aetherupload 缺失时报明确错误', () => {
    const { result } = renderHook(() => useUpload({}))
    expect(() => result.current.start(panelDom(''), 'file')).toThrow(/aetherupload-all\.js/)
  })
})
```

- [x] **Step 3: 跑测试确认失败**

Run: `cd web && npm test`
Expected: FAIL（`../upload/useUpload` 不存在）

- [x] **Step 4: 实现 `web/src/upload/useUpload.ts`**

```ts
import { useCallback } from 'react'

/** aetherupload-all.js 暴露的全局工厂实例（无类型定义，按契约声明）。 */
export type AetherInstance = {
  setGroup(g: string): AetherInstance
  setSavedPathField(sel: string): AetherInstance
  setPreprocessRoute(r: string): AetherInstance
  setUploadingRoute(r: string): AetherInstance
  setLaxMode(b: boolean): AetherInstance
  success(cb: () => void): AetherInstance
  upload(): void
}
declare global {
  interface Window {
    aetherupload?: (el: Element) => AetherInstance
  }
}

/**
 * 统计上传期间对 `/aetherupload/uploading` 的 POST 次数。
 * 成功且 count() === 0 ⇒ 秒传命中（preprocess 直接回传了 savedPath，零分块传输）。
 * zepto 的 $.ajax 底层走 window.XMLHttpRequest，所以包一层即可；success 回调里 restore。
 * ponytail: 包全局 XHR 而非改脚本；若上游以后加 onProgress/onInstant 回调，删掉它。
 */
export function countingUploadingPosts() {
  const Orig = window.XMLHttpRequest
  let count = 0
  class Counting extends Orig {
    open(method: string, url: string | URL, ...rest: unknown[]) {
      if (String(method).toUpperCase() === 'POST' && String(url).includes('/aetherupload/uploading')) {
        count++
      }
      return (super.open as (...a: unknown[]) => void)(method, url, ...rest)
    }
  }
  window.XMLHttpRequest = Counting as unknown as typeof XMLHttpRequest
  return {
    count: () => count,
    restore: () => {
      // 只在顶层还是自己时才还原：后装先还原会踩掉更晚的包装
      if ((window.XMLHttpRequest as unknown) === Counting) window.XMLHttpRequest = Orig
    },
  }
}

export interface UploadOutcome {
  savedPath: string
  instant: boolean
}

/**
 * 驱动 aetherupload-all.js。调用前 `#aetherupload-wrapper` 下的四个 id 必须已在 DOM 里
 * （Task 9 的 UploadPanel 渲染），fileInput 内必须已有用户选好的文件。
 * savedPath 不调 setSavedPathField：库默认就在 wrapper 内找 `#aetherupload-savedpath`。
 */
export function useUpload(opts: { onFinished?: (outcome: UploadOutcome) => void }) {
  const start = useCallback(
    (fileInput: HTMLInputElement, group: string) => {
      const factory = window.aetherupload
      if (!factory)
        throw new Error('aetherupload-all.js 未加载：检查 index.html 的 /aetherupload-assets/aetherupload-all.js')
      // 脚本成功后会给 input 置 disabled，React 不会复位——同面板二次上传必须先清掉
      fileInput.removeAttribute('disabled')
      const counter = countingUploadingPosts()
      factory(fileInput)
        .setGroup(group)
        .setPreprocessRoute('/aetherupload/preprocess')
        .setUploadingRoute('/aetherupload/uploading')
        .setLaxMode(false)
        .success(() => {
          counter.restore()
          // 库在 callback 之前已把 savedPath 写进该节点（aetherupload-all.js 225/325 行）
          const savedPath =
            (
              fileInput.closest('#aetherupload-wrapper')?.querySelector('#aetherupload-savedpath') as
                | HTMLInputElement
                | null
            )?.value ?? ''
          opts.onFinished?.({ savedPath, instant: counter.count() === 0 })
        })
        .upload()
    },
    [opts],
  )
  return { start }
}
```

- [x] **Step 5: 跑测试**

Run: `cd web && npm test`
Expected: PASS（含 Task 2 的 4 个用例）

- [x] **Step 6: 提交**

```bash
git add web/public/aetherupload-assets/ web/src/upload/useUpload.ts web/src/__tests__/useUpload.test.ts
git commit -m "feat(web): useUpload 驱动 aetherupload-all.js（含秒传判定）"
```

> 执行注记：脚本 `cmp` 与 crate 内原件逐字节一致（48810 B）。本轮并入三项：
> (1) cookie 种在 `setToken()`（`api.ts`，原因见 Task 4 补丁；后端 cookie 契约已由
> `tests/aether_auth.rs` 覆盖，浏览器侧留给 Task 11 e2e）；
> (2) `console_api.rs` groups 断言补全形（quality-m1b-7 Minor 1）；
> (3) `api.ts` `max_size` 加「0 = 不限」注释（Minor 2）。
> 浏览器手验（登录后看 cookie）同 Task 4，留给 Task 11 e2e。验证：`cargo test --workspace -j 2`
> 98 全绿；`npm test` 13 用例、`npm run build`、`npm run lint`（vendored 脚本已 ignore，余 2 条
> App.tsx 既有 warning）。

## 定稿记录（Task 8 质量评审收尾，commit `fix(web): cookie 入 setToken 同生命周期；input 复位与 hook 收口`）

两条 Important（已落入代码与本计划代码块）：

- **I-1 cookie 生命周期**：cookie 的写入/清除移进 `api.ts` 的 `setToken()`（`max-age=43200` 对齐服务端
  12h，`setToken(null)` 用 `max-age=0` 清），`Login.tsx` 不再手写 cookie。
  原因：cookie 是同一凭据的第二份拷贝，必须与 localStorage 同生命周期；散在登录页写、却没有任何
  地方清，401 清 token 或将来加登出后都会留下一个带着废弃凭据的 cookie。
- **I-2 input disabled 复位**：`useUpload.start` 开头 `fileInput.removeAttribute('disabled')`。
  原因：vendored 脚本上传成功后会给 `#aetherupload-resource` 置 `disabled`（aetherupload-all.js 上游 227/327 行 / 本地 243/343 行），
  而该节点由 React 渲染、重渲染不会复位——同面板二次上传必然卡死在这行上。

Minor（同轮）：`restore` 仅在 `window.XMLHttpRequest === Counting` 时还原（后装先还原不踩更晚的包装）；
删 `setSavedPathField` 调用（库默认就在 wrapper 内找），success 里改从 `fileInput.closest('#aetherupload-wrapper')`
读 savedPath；补 `useUpload` 的三条承诺测试（假 `window.aetherupload`：`instant: true`/`false` 两态 +
缺 factory 报错）；`.oxlintrc.json` 加 `ignorePatterns: ["public/aetherupload-assets/**"]`（vendored 脚本 92 条 warning）。

---

## Task 9: 上传面板（分组 · 进度 · 秒传提示 · 上传历史）

**Files:**
- Create: `web/src/format.ts`（从 Task 6 抽出 `humanSize` 共用）
- Create: `web/src/upload/UploadPanel.tsx`
- Create: `web/src/__tests__/assets.test.ts`（vendored 脚本补丁漂移守卫，收尾轮补）
- Modify: `web/src/pages/Objects.tsx`（引用 format.ts、加「上传」按钮）

- [x] **Step 1: `web/src/format.ts`**

```ts
export function humanSize(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`
}
```

`Objects.tsx`：删掉内联的 `humanSize`，改为 `import { humanSize } from '../format'`。

- [x] **Step 2: 实现 `web/src/upload/UploadPanel.tsx`**

```tsx
import { useRef, useState } from 'react'
import { App, Button, Modal, Select, Space, Table, Tag, Typography } from 'antd'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type GroupInfo, type UploadRecord } from '../api'
import { humanSize } from '../format'
import { useUpload, type UploadOutcome } from './useUpload'

export default function UploadPanel({
  open,
  bucket,
  onClose,
}: {
  open: boolean
  bucket: string
  onClose: () => void
}) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [group, setGroup] = useState('file')
  const [last, setLast] = useState<UploadOutcome | null>(null)
  // 上传中禁用再选。复位点：onFinished（成功）、start 同步抛错、面板关闭；
  // 库对硬失败（如 preprocess 报错）不回调也不回调错误，只往 #aetherupload-output 写文案，
  // 那种情况靠关闭面板复位（不解析中文文案）。
  const [busy, setBusy] = useState(false)
  const fileRef = useRef<HTMLInputElement>(null)
  // onFilePicked 会清空 input.value（让同一文件再选也触发 onChange），清空后 files 为空——
  // 原始文件名必须选文件当刻留档，onFinished（异步）里再读就已经是 '' 了
  const originalNameRef = useRef('')

  const { data: groups = [] } = useQuery({
    queryKey: ['groups'],
    queryFn: () => api<GroupInfo[]>('/api/v1/groups'),
    enabled: open,
  })

  const { data: history = [] } = useQuery({
    queryKey: ['uploads'],
    queryFn: () => api<UploadRecord[]>('/api/v1/uploads'),
    enabled: open,
    refetchInterval: 3000,
  })

  const { start } = useUpload({
    onFinished: async (outcome) => {
      setBusy(false) // 先复位：后面的 register 上报失败也不影响
      // 脚本成功后会给 input 置 disabled（aetherupload-all.js 上游 227/327 行 / 本地 243/343 行）；不清掉的话
      // 「选择文件」按钮的 click 落在 disabled input 上无任何反应——真人没法在同面板再选（秒传演示就断了）。
      // useUpload.start 里那处清理只在 change 事件能触发时才轮得到，救不了这一手。
      fileRef.current?.removeAttribute('disabled')
      setLast(outcome)
      message.success(outcome.instant ? '上传成功（秒传）' : '上传成功')
      if (outcome.savedPath) {
        await api('/api/v1/uploads/register', {
          method: 'POST',
          body: JSON.stringify({ saved_path: outcome.savedPath, original_name: originalNameRef.current }),
        }).catch((e) => {
          console.error(e)
          message.warning('上传记录登记失败（对象本身已入库）')
        })
      }
      qc.invalidateQueries({ queryKey: ['objects', bucket] })
      qc.invalidateQueries({ queryKey: ['uploads'] })
    },
  })

  // 让「同一文件再选一次」也能触发 onChange（秒传验证要用）
  const onFilePicked = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target
    if (!input.files?.[0]) return
    originalNameRef.current = input.files[0].name
    setLast(null)
    setBusy(true)
    try {
      start(input, group)
    } catch (err) {
      setBusy(false) // 同步抛错（如脚本未加载）不能让按钮永久禁用
      throw err
    }
    input.value = ''
  }

  const close = () => {
    setBusy(false) // 硬失败没有回调，关闭面板兜底复位
    onClose()
  }

  return (
    <Modal title={`上传到 ${bucket}`} open={open} onCancel={close} footer={null} width={640} destroyOnHidden>
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Space>
          <Select
            value={group}
            onChange={setGroup}
            style={{ width: 280 }}
            options={groups.map((g) => ({
              value: g.group,
              label: `${g.group} → ${g.bucket}${g.max_size ? `（≤ ${humanSize(g.max_size)}）` : ''}`,
            }))}
          />
          <Button
            onClick={() => fileRef.current?.click()}
            disabled={busy || !window.aetherupload}
            title={!window.aetherupload ? 'aetherupload-all.js 未加载' : undefined}
          >
            选择文件并开始上传
          </Button>
        </Space>

        {/*
          aetherupload-all.js 的固定 DOM 契约：wrapper 下这四个 id 必须存在，
          内容（输出文案、进度条宽度、savedPath）全部由脚本直接改。
          React 只渲染一次空节点，此后不再碰它们 —— 不要在这里放受控内容。
        */}
        <div id="aetherupload-wrapper">
          <input
            ref={fileRef}
            id="aetherupload-resource"
            type="file"
            style={{ display: 'none' }}
            onChange={onFilePicked}
          />
          <div style={{ height: 8, background: '#f0f0f0', borderRadius: 4, overflow: 'hidden' }}>
            <div
              id="aetherupload-progressbar"
              style={{ height: '100%', width: '0%', background: '#1677ff', transition: 'width .2s' }}
            />
          </div>
          <div id="aetherupload-output" style={{ minHeight: 22, color: '#666', marginTop: 8 }} />
          <input id="aetherupload-savedpath" type="hidden" />
        </div>

        {last && (
          <Tag color={last.instant ? 'gold' : 'green'}>
            {last.instant ? '秒传命中（0 分块传输）' : '分块上传完成'}
          </Tag>
        )}
        <Typography.Text type="secondary">
          断网/弱网时脚本会自动重试当前分块（服务端对重发分块幂等），进度条会继续推进——这就是断线续传。
          M1 的续传限于同一页面会话：刷新页面后需重新上传。
        </Typography.Text>

        <Table
          size="small"
          rowKey="saved_path"
          dataSource={history}
          pagination={{ pageSize: 5 }}
          columns={[
            { title: '文件', dataIndex: 'original_name', render: (v: string | null, r) => v ?? r.key.split('/').pop() },
            { title: '桶/键', render: (_, r) => `${r.bucket}/${r.key}` },
            { title: '大小', dataIndex: 'size', render: humanSize },
            { title: '时间', dataIndex: 'created_at', render: (t: number) => new Date(t * 1000).toLocaleString() },
          ]}
        />
      </Space>
    </Modal>
  )
}
```

- [x] **Step 3: `Objects.tsx` 接入上传按钮**

页头加状态与按钮，Modal 挂在页面底部：

```tsx
const [uploading, setUploading] = useState(false)
// …页头：
<Button type="primary" onClick={() => setUploading(true)} style={{ marginBottom: 16 }}>上传</Button>
// …组件返回的末尾：
<UploadPanel open={uploading} bucket={bucket} onClose={() => setUploading(false)} />
```

对应 import：`import UploadPanel from '../upload/UploadPanel'`。

- [x] **Step 4: 验证**

后端 + `npm run dev`，打开 `/buckets/<桶>` → 点「上传」→ 选文件 → 进度条推进 → 「上传成功」，历史表出现记录；同一文件再传一次 → 「上传成功（秒传）」+ 金色 Tag。

- [x] **Step 5: 提交**

```bash
git add web/src/format.ts web/src/upload/UploadPanel.tsx web/src/pages/Objects.tsx
git commit -m "feat(web): 上传面板（分组/进度/秒传提示/上传历史）"
```

---

## 定稿记录（Task 9 收尾，commit `fix(web): preprocess 补丁（multipart）；zh_CN 与 React19 补丁；destroyOnHidden`）

**A 补丁（跨任务 blocker，team-lead 裁决）**：浏览器实测上传 100% 失败 —— `POST /aetherupload/preprocess`
返回 **400 `Invalid `boundary` for `multipart/form-data` request`**。根因：crate 自带
`assets/aetherupload-all.js`（`core.js` 同）的 preprocess 用 zepto `data: {…}` 发
**application/x-www-form-urlencoded**（PHP 时代写法），而 `aetherupload-rust` 全部 Rust 适配层
（axum/ecat 走 `src/integrations/axum.rs:54` 的 `Multipart` 提取器）要 multipart；分块上传本就
是 FormData，只有 preprocess 一处。M1a Task 14 用 `curl -F`（multipart）验证，故未暴露；Task 11 两个
e2e 都会卡在此处。

处置：`web/public/aetherupload-assets/aetherupload-all.js` 的 preprocess 改为
FormData + `processData: false` + `contentType: false`（zepto 原生支持），文件头部加注释说明本地补丁
与「升级脚本时重新评估」。**该文件不再与 crate 1.0.3 逐字节相同**（Task 8 的 `cmp` 结论自此失效）；
上游 `aetherupload-rust` 修复此编码不一致后可撤补丁。

补丁后浏览器全链实测（后端 + vite + Playwright）：preprocess 200 → uploading 200 → 输出「上传成功」、
进度条 100%、savedPath 回填 → register 后历史表出现记录且「文件」列显示原始文件名（验证 carry-forward
的 `originalNameRef` 留档时机）→ 对象列表自动刷新 → 同文件二传「上传成功（秒传）」+ 金色 Tag
「秒传命中（0 分块传输）」且本轮 `/aetherupload/uploading` POST 数为 0。

**同轮 front 修复三条**：
1. `main.tsx` locale 改 `import zhCN from 'antd/es/locale/zh_CN'`（ESM 源）。原因：`antd/locale/zh_CN`
   是 CJS 包，经 Vite 8 预打包后默认导出成为 `{ default: <真 locale> }` 双层，ConfigProvider 取不到
   `Modal.okText`，Modal 按钮渲染成英文 OK/Cancel。
2. `UploadPanel.tsx` `destroyOnClose` → `destroyOnHidden`（antd 5.29 起前者是弃用别名，运行时会打警告）。
3. 装 `@ant-design/v5-patch-for-react-19` 并在 `main.tsx` 顶部 import（消 `[antd: compatible] antd v5
   support React is 16 ~ 18` 警告、恢复静态 API 正常性）。

---

## 定稿记录（Task 9 质量评审收尾，commit `fix(web): 上传面板 busy 护栏与失败留痕；漂移守卫测试`）

**I-1 busy 并发护栏**：`UploadPanel` 加 `busy` state——选文件置 true，`onFinished` 开头（在 register await
之前）置 false；按钮 `disabled={busy || !window.aetherupload}`。复位点共三处，因为**库对硬失败没有任何回调**：
`aetherupload-all.js` 只在成功分支调 `_this.callback()`（savedPath 非空那支），失败（preprocess 报错、
分块服务端错误）只把文案写进 `#aetherupload-output` 就 return。故除 `onFinished`（成功）外，另加
`start()` 同步抛错的 try/catch 复位、以及关闭面板时复位（`close()`），保证按钮不会永久禁用；不解析中文文案
（计划「执行前必读」第 2 条）。文件名留档仍用共享 `originalNameRef`（按次闭包需改 `useUpload` 签名，
而该 hook 已有 3 条承诺测试，不动更稳）。

**残留（共享 ref 决策的代价）**：上传进行中关闭面板→重开，会解除 busy 护栏（`close()` 复位）；
该路径下并发选文件仍可致 `original_name` 错配（对象数据无损）。

**另加一条自测发现的交互缺陷（同轮修复）**：脚本成功后会给自己那个 input 置 `disabled`
（上游 227/327 行 / 本地 243/343 行），而「选择文件并开始上传」的 click 落在 disabled input 上**不派发任何事件**——真人第一次上传成功后，
同面板再点按钮毫无反应，「同一文件再选一次看秒传」在真浏览器里不可达（Playwright 的 `setInputFiles`
绕过可操作性检查，所以 e2e 照写也照样绿，只有人工点才发现）。修法：`onFinished` 里
`fileRef.current?.removeAttribute('disabled')`，并用 `filechooser` 事件走了真人路径复验（首传后按钮 enabled
→ 点按钮弹选择器 → 二传 `上传成功（秒传）`、`/aetherupload/uploading` POST 0）。

**Minor 四条**：M1 register 失败 `.catch((e) => { console.error(e); message.warning(...) })` 留痕；
M2 按钮 `loading={!window.aetherupload ? true : undefined}` 改为 `disabled={!window.aetherupload}` + `title`
（原写法在脚本缺失时永久转圈且吞点击）；M3 新增 `web/src/__tests__/assets.test.ts` 漂移守卫——读 vendored
脚本原文，断言 `processData: false`/`contentType: false` 各 ≥2 处、且含 `new FormData()` 与
`form.append('resource_name'`（补丁特有；原版 preprocess 没有 FormData），防手滑 re-copy 覆盖补丁；
（已实测：去掉补丁跑该测试 2 条全红。）M4 `refetchInterval: open ? 3000 : false` → `3000`（`enabled: open`
已管门）。M5（历史表保留已删对象行）/ M6（bisect 中间态）记录不处理；extensions 展示（nit）不做。

---

## 定稿记录（Task 9 追加，commit `chore(web): 移除 JS 补丁（上游 1.0.4 已修 urlencoded）；守卫改 stock 哈希`）

**补丁退役**：`aetherupload-rust` 1.0.4（2026-10-11）在 Rust 侧修掉编码不一致——`src/form.rs` 新增
`FormData::from_urlencoded`，`integrations/{axum,actix,poem,warp}` 的 `preprocess` 按 Content-Type 分流、
urlencoded 与 multipart 都收（`assets/` 零差异——上游没动 JS；`ecat = ["axum"]`，本项目走的就是这条路径）。
本地 JS 补丁（补丁定稿见上方 Task 9 记录）随之移除：`web/public/aetherupload-assets/aetherupload-all.js`
以二进制 `cp` 恢复为 crate 1.0.4 的 stock 资产（`cmp` 逐字节一致，sha256 `d639686a…`），补丁历史只留在 git。
`web/src/__tests__/assets.test.ts` 守卫从「保护补丁」改为「保护 stock」：钉 vendored 文件的 sha256 常量
（升级 crate 时随之更新），原 `processData/FormData` 断言删除。浏览器链路不变地走
preprocess → uploading：preprocess 现在发 **urlencoded**，由服务端接受（e2e 覆盖真实路径）。

---

## Task 10: 访问密钥页

**Files:**
- Modify: `web/src/pages/Keys.tsx`

- [x] **Step 1: 实现**

```tsx
import { useState } from 'react'
import { Alert, App, Button, Modal, Table, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type AccessKey } from '../api'

export default function Keys() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [fresh, setFresh] = useState<AccessKey | null>(null)

  const { data: keys = [], isLoading } = useQuery({
    queryKey: ['access-keys'],
    queryFn: () => api<string[]>('/api/v1/access-keys'),
  })

  const create = useMutation({
    mutationFn: () =>
      api<AccessKey>('/api/v1/access-keys', { method: 'POST', body: JSON.stringify({}) }),
    onSuccess: (k) => {
      setFresh(k)
      qc.invalidateQueries({ queryKey: ['access-keys'] })
    },
    onError: (e: Error) => message.error(e.message),
  })

  const close = () => {
    setFresh(null)
    create.reset() // 关窗即清 MutationCache 里的 data：SK 只该活在弹窗这一个副本里
  }

  return (
    <>
      <Button type="primary" loading={create.isPending} onClick={() => create.mutate()} style={{ marginBottom: 16 }}>
        新建访问密钥
      </Button>
      <Table
        rowKey={(r) => r}
        loading={isLoading}
        dataSource={keys}
        columns={[
          // 不用 dataIndex：antd 对 string[] 记录取 dataIndex:0 会拿到首字符；无 dataIndex 时 render 第一参即整条记录
          { title: 'Access Key', render: (ak: string) => ak },
          { title: '说明', render: () => '启用/禁用与过期在 M5；M1 只有创建与使用' },
        ]}
      />
      <Modal title="密钥已创建" open={!!fresh} footer={null} maskClosable={false} onCancel={close}>
        {fresh && (
          <>
            <Alert type="warning" showIcon style={{ marginBottom: 12 }}
              message="Secret Key 只显示这一次，请立即保存（M5 前它是明文存库的）。" />
            {/* copyable={{ text }} 显式给裸值：裸 copyable 会连「Access Key：」前缀一起复制 */}
            <Typography.Paragraph copyable={{ text: fresh.access_key }}>Access Key：{fresh.access_key}</Typography.Paragraph>
            <Typography.Paragraph copyable={{ text: fresh.secret_key }}>Secret Key：{fresh.secret_key}</Typography.Paragraph>
          </>
        )}
      </Modal>
    </>
  )
}
```

- [x] **Step 2: 验证**

`/keys`：新建 → 弹窗显示 AK/SK（点复制可用）；列表出现新 AK；用该 AK 配 `aws-cli` 能 `aws s3 ls`（M1a 验收的无损回归）。

- [x] **Step 3: 提交**

```bash
git add web/src/pages/Keys.tsx
git commit -m "feat(web): 访问密钥页（列表 + 一次性展示 SK）"
```

> 执行注记：/keys 的浏览器闭环（新建 → 弹窗显示 AK/SK → 点复制 → 关窗 → 列表出现新 AK）由 Task 11 的
> Playwright e2e（`console.spec.ts` 第二条用例）覆盖，见下方 Minor 3 转记；本轮替代验证：`npm test`、
> `npm run build`、`npm run lint`（复制内容与关窗行为另有定稿记录中的浏览器实测）。

---

## 定稿记录（Task 10 质量评审收尾，commit `fix(web): 密钥复制内容去前缀；关闭清 mutation 态；防误触遮罩`）

**I-1 复制内容去前缀**：两处 `Typography.Paragraph` 的裸 `copyable` 改为 `copyable={{ text: … }}` 显式
传裸值。原因：antd 裸 `copyable` 走 `copy-to-clipboard` 的节点取文本，复制的是**整个子节点文本**——
即带「Access Key：」/「Secret Key：」前缀的整行；SK 只展示一次，用户粘到 aws-cli 配置里多一截前缀
即废（且粘贴处不一定报错）。浏览器实测：点复制后剪贴板内容 `=== fresh.secret_key`（无前缀）。

**Minor 2 关窗清 mutation 态**：关闭弹窗统一走 `close()`：`setFresh(null)` + `create.reset()`——
组件渲染虽只读 `fresh`，但 `create.data` 仍把刚生成的 SK 留在 MutationCache 里，关窗后应一并清掉。

**Minor 4 防误触丢 SK**：Modal 加 `maskClosable={false}`——`footer={null}` 时无「取消」抓手，
误点遮罩即关窗，而 SK 只此一次展示；键盘 Esc 与右上角关闭钮仍可关（均走 `onCancel` → `close()`）。

**Minor 3（转记）**：`/keys` 的自动化覆盖并入 Task 11：新建 → 弹窗显示 AK/SK → 复制内容等于裸 SK
（clipboard 断言）→ 关弹窗 → 列表出现新 AK。

---

## Task 11: Playwright e2e（关键路径）

> **locator 约定（Task 9 浏览器实测所得，写用例前先读）**
> 1. **两字 CJK 按钮名带空格**：antd 对恰好两个汉字的按钮自动插空格，实际渲染 `登 录` / `上 传` /
>    `删 除` / `确 定`、`取 消`。整串匹配 `{ name: '登录' }`、`{ name: '上传' }` 必然超时；裸子串正则
>    `/上传/` 在上传面板里会同时命中「选择文件并开始上传」→ strict mode violation（两个元素）。写法用
>    `{ name: '上 传', exact: true }` 或 `/^上\s*传$/`；三字及以上（`新建桶`、`选择文件并开始上传`）无空格。
> 2. **zh_CN 已生效**（Task 9 收尾修 `main.tsx` 的 locale import）：Modal 页脚按钮渲染 `确 定`/`取 消`，
>    `{ name: /确\s*定/ }` 可用。若某天又见英文 OK/Cancel，先查 locale import 是否被改回 `antd/locale/zh_CN`。
> 3. **Modal 关闭钮**：aria-label 固定 `Close`（antd 默认，不随 locale 变），`{ name: 'Close' }` 无需改动。
>
> 另：vendored `web/public/aetherupload-assets/aetherupload-all.js` 已回归 crate 1.0.4 stock
> （JS 补丁退役见 Task 9 追加定稿记录）——preprocess 发 urlencoded，须由 **aetherupload-rust ≥ 1.0.4**
> 的后端接受；e2e 起服务前确认 9000 上没有 1.0.3 时代的残留进程（`reuseExistingServer: true` 会直接复用它，
> 表现为 preprocess 400）。

**Files:**
- Create: `web/playwright.config.ts`、`web/e2e/console.spec.ts`、`web/e2e/instant.spec.ts`

- [x] **Step 1: 配置**

```ts
// web/playwright.config.ts
import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  use: { baseURL: 'http://127.0.0.1:5173' },
  // 两个 webServer：后端（cargo）与前端（vite）。后端数据目录沿用 oss.toml 默认；
  // 用例用「时间戳桶名 + 时间戳文件内容」保证可重复跑。
  webServer: [
    {
      command: 'cargo run -p oss-server',
      cwd: '..',
      url: 'http://127.0.0.1:9000/health/live',
      reuseExistingServer: true,
      timeout: 180_000,
      env: { OSS_ADMIN_USER: 'erik', OSS_ADMIN_PASSWORD: 'pw' },
    },
    {
      command: 'npm run dev',
      url: 'http://127.0.0.1:5173',
      reuseExistingServer: true,
      timeout: 60_000,
    },
  ],
})
```

- [x] **Step 2: `e2e/console.spec.ts`——闭环**

```ts
import { expect, test } from '@playwright/test'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

test('登录 → 建桶 → 上传 → 列表 → 下载 → 删除', async ({ page }) => {
  const bucket = `e2e-${Date.now()}`
  const dir = mkdtempSync(join(tmpdir(), 'oss-e2e-'))
  const content = `hello-${Date.now()}`
  const file = join(dir, 'hello.txt')
  writeFileSync(file, content)

  await page.goto('/login')
  await page.getByLabel('用户名').fill('erik')
  await page.getByLabel('口令').fill('pw')
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page).toHaveURL(/\/buckets/)

  await page.getByRole('button', { name: '新建桶' }).click()
  await page.locator('#new-bucket-name').fill(bucket)
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await expect(page.getByRole('link', { name: bucket })).toBeVisible()

  await page.getByRole('link', { name: bucket }).click()
  await page.getByRole('button', { name: '上 传', exact: true }).click()
  await page.locator('#aetherupload-resource').setInputFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功', { timeout: 30_000 })
  await page.getByRole('button', { name: 'Close' }).click() // antd Modal 的关闭按钮 aria-label

  const row = page.getByRole('row', { name: /hello\.txt/ })
  await expect(row).toBeVisible()

  const downloadPromise = page.waitForEvent('download')
  await row.getByRole('button', { name: '下 载', exact: true }).click()
  const download = await downloadPromise
  const saved = await download.path()
  expect(saved).toBeTruthy()

  await row.getByRole('button', { name: '删 除', exact: true }).click()
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await expect(page.getByRole('row', { name: /hello\.txt/ })).toHaveCount(0)
})
```

- [x] **Step 3: `e2e/instant.spec.ts`——秒传零分块**

```ts
import { expect, test } from '@playwright/test'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

test('同一文件二次上传命中秒传：不再产生 /aetherupload/uploading 请求', async ({ page }) => {
  const bucket = `e2e-${Date.now()}`
  const dir = mkdtempSync(join(tmpdir(), 'oss-e2e-'))
  const file = join(dir, 'instant.txt')
  writeFileSync(file, `instant-${Date.now()}`)

  const uploadingPosts: string[] = []
  page.on('request', (r) => {
    if (r.method() === 'POST' && r.url().includes('/aetherupload/uploading')) uploadingPosts.push(r.url())
  })

  await page.goto('/login')
  await page.getByLabel('用户名').fill('erik')
  await page.getByLabel('口令').fill('pw')
  await page.getByRole('button', { name: '登 录', exact: true }).click()

  await page.getByRole('button', { name: '新建桶' }).click()
  await page.locator('#new-bucket-name').fill(bucket)
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await page.getByRole('link', { name: bucket }).click()

  // 第一次：真分块
  await page.getByRole('button', { name: '上 传', exact: true }).click()
  await page.locator('#aetherupload-resource').setInputFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功', { timeout: 30_000 })
  expect(uploadingPosts.length).toBeGreaterThan(0)

  // 第二次：同文件 → 秒传，0 个分块请求
  const before = uploadingPosts.length
  await page.locator('#aetherupload-resource').setInputFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功（秒传）', { timeout: 15_000 })
  await expect(page.getByText('秒传命中（0 分块传输）')).toBeVisible()
  expect(uploadingPosts.length).toBe(before)
})
```

- [x] **Step 4: 跑 e2e**

```bash
cd /home/wwwroot/oss/web
npx playwright install chromium   # 首次
npm run e2e
```

Expected: 2 passed。（若本机已有后端在跑，`reuseExistingServer` 会直接复用——注意它会用运行中实例的数据目录。）

- [x] **Step 5: 提交**

```bash
git add web/playwright.config.ts web/e2e/
git commit -m "test(web): Playwright 关键路径（闭环 + 秒传零分块）"
```

**执行注记（本轮实跑修正，3 用例全绿）**：Step 2 的上传落点假设与实现不符——上传按「分组 → 桶」映射落桶
（`IngestSink::resolve`），group `file` → 桶 `files`，对象名是**内容 md5**（`get_file_name(md5, ext)`，非原文件名），
子目录是 `SubdirRule::Month` 的 **UTC 年月**。故闭环用例上传后改去 `/buckets/files?prefix=<UTC 年月>/` 断言
列表/下载/删除（下载加断言字节一致，兑现 Task 6 执行注记）；建桶仍留在用例里（Task 5 覆盖）。配置的 `use`
补 `locale: 'zh-CN'`：aetherupload-all.js 按 `navigator.language` 选文案表，Playwright 默认 en-US 时
`#aetherupload-output` 写英文「upload succeed」，中文断言必超时。另：`/keys` 一条（Task 10 Minor 3 转记）
并入 `console.spec.ts`；秒传用例二传走「按钮 → filechooser」真人路径（去掉 `onFinished` 里的
`removeAttribute('disabled')` 反跑即红，已验）。

**执行注记补（抗并发 / 分页，随压力实跑修正）**：断言不得依赖「新桶在第 1 页」——桶列表按名升序、10 行/页
（xl-store `list_buckets` + antd 默认分页），共享 data 目录里历史桶只增不减，同前缀的新桶照样会被挤出第 1 页
（`0-` 前缀一样会漂）。现用 `zzz-` 前缀（恒大于既有桶名 → 新桶恒在最后一页）并先「点最后一页 → 找新桶」再断言，
整体用 `expect(...).toPass()` 重试：建桶后的 `invalidateQueries` refetch 与翻页点有竞态。`/keys` 同理由
（`created_at` 升序 → 新键在最后一页）。文件内容加 `randomUUID()`：并行 worker 同毫秒起步时会写出同内容 → 同 md5
→ **同一个对象键**，两个运行互相踩（一个删掉对象，另一个断言的行消失）——压力实跑复现，已从 DB 证实。
桶名同样补唯一化后缀（`zzz-e2e-<时间戳>-<UUID 前 8 位>`）：同毫秒并发建桶会撞名 → 409 → Modal 不关 → 假红，
内容侧已用 UUID、名字侧补齐。`/keys` 的翻页（原「退一页」特判兜底，在 refetch 窗口里反而会点错页）统一成与桶列表
同款 `expect(...).toPass()`（点末页 → 断言可见，自纠）。`playwright.config.ts` 注释随动为「时间戳 + UUID」。
验证：`npm run e2e` 3 passed；`--repeat-each=3 --workers=3` 9 passed、`--repeat-each=4 --workers=4` 12 passed（修正后再跑仍全绿）。

**执行注记补（tsconfig，偏离④）**：新增 `web/playwright.config.ts` 后 `tsc -b`（`npm run build`）不会检查它——根
`tsconfig.json` 是 solution-style（`files: []` + references），只管 `tsconfig.app.json`（src）与 `tsconfig.node.json`
（原 include 仅 `vite.config.ts`）。故把 `playwright.config.ts` 补进 `tsconfig.node.json` 的 `include`（偏离计划 Task 11
的 Files 清单，随本轮首笔提交带上）。**不改**：`e2e/*.spec.ts` 不在任何 project 的 include 里、`tsc -b` 不检查它们
（由 Playwright 自己的 esbuild 转译；M3 记录，不处理）。

---

## Task 12: 1 GB 断线续传手测清单 + README + CI 验收

**Files:**
- Modify: `README.md`

- [x] **Step 1: 1 GB 上传 + 断线续传手测**（设计文档 M1 验收第 2 条，浏览器部分）

```bash
head -c 1073741824 /dev/urandom > /tmp/1g.bin
```

在控制台对象页 →「上传」→ 选 `/tmp/1g.bin`，按清单逐项确认：

1. 进度条持续推进，直到「上传成功」；对象浏览器出现该文件。
2. **断线续传**：上传中途打开 DevTools → Network → 切 Offline（或拔网线）→ 输出区出现「网络故障，正在重试……」→ 恢复网络 → 上传自动继续直到成功（服务端对重发分块幂等跳过，见规格 §5.2）。
3. 下载该 1 GB 文件，`cmp /tmp/1g.bin <下载文件>` 一致。
4. `uploads` 表可见记录（上传历史面板）；二次传同一文件命中秒传。
5. 已知边界（写进 README）：**刷新页面后续传不了**——M1 的续传限于同一页面会话；跨刷新的断点续传要等服务端进度查询 + Multipart（M2）。

**执行注记（本轮真跑，不是纯文字清单；临时脚本 `/tmp/oss-1gb/*.cjs` 不提交）**：复用 e2e 的常驻后端（`OSS_ADMIN_PASSWORD=pw`）
+ vite（5173），用 playwright-core 走真人路径（登录 → `/buckets/files` →「上传」→ `setInputFiles` 一个
`head -c 1073741824 /dev/urandom` 的 1 GiB 文件，本地 `md5sum 86274962ca6e5229c52180e5fadfa42b`）：

1. **进度推进 → 成功**：`#aetherupload-output` 采样 98 次，`0% → 3% → 28% → 53% → 78% → 100%「上传成功」`；对象页出现
   `202610/86274962ca6e5229c52180e5fadfa42b.bin`，大小列 `1.00 GiB`、ETag 列 = 本地 md5。客户端先算 1 GB SparkMD5
   （~17s）再发 1074 × 1 MB 分块；从提交到成功 537.5s，其中约 7 分钟是最后一块在等服务端 merge（暂存盘 1 GiB .bin）
   + 原生 ingest（store 写 part.1 并 fsync）——本机 io full≈75%、load≈13 的脏环境实测，非稳态基线。
2. **断线续传（真断网，非文字）**：进度 3% 时 `browserContext.setOffline(true)`（CDP offline，等价 DevTools →
   Network → Offline）持续 ~25s → 输出区出现「网络故障，正在重试……」（脚本 status===0 分支：busy-wait 5s 后重发同一分块）
   → `setOffline(false)` → 同一页面自动继续 `3% → 28% → 53% → 78% → 100%` 直至成功（服务端对同序号重发分块幂等）。
3. **下载比对**：对象页行内「下载」（真人路径：控制台 API → blob → 浏览器下载）落盘 1073741824 B，
   `cmp` 逐字节一致，`md5sum`/`sha256sum` 双一致（sha256 `b0dfd66c7229edff710b5013782844da7323e977a88afabb3e106db56cf6bb0e`）。
4. **uploads 记录 + 秒传**：`GET /api/v1/uploads` 有 `{saved_path: file_202610_86274962….bin, bucket: files,
   key: 202610/86274962….bin, size: 1073741824, etag: 86274962…, original_name: 1g.bin}`；**二次传同一 1 GB 文件**
   输出「上传成功（秒传）」+ 标签「秒传命中（0 分块传输）」，`/aetherupload/uploading` POST 计数 **0**（14s 全花在客户端算 hash）。
5. 已知边界按计划写进 README（刷新后需重传；跨刷新续传在 M2）。

实测后清理：`/tmp/oss-1gb/{1g.bin,downloaded.bin}`（2 GiB）已删；`/tmp/oss-1gb/run.log` 留档（不入库）。**未验（留给人工）**：
真·拔网线/DevTools 手动操作路径（本轮用 CDP offline 等价替代，断的时机与恢复由脚本控）与刷新页面路径（已知边界，规格内不修）。

**team-lead 独立复验**：收尾轮从控制台 API 重新拉取该 1 GiB 对象（1073741824 B）——`md5` = `86274962ca6e5229c52180e5fadfa42b`（与记录本地 md5 及服务端 ETag 三重一致）、`sha256` = `b0dfd66…bb0e`（与上文一致）。下载完整性无缺口。

**留档补充说明**：`run.log` 中的两行 ERROR（首次点「下 载」30s 超时、`download.cjs` 的 `suggestedFileName` API 误用）均为临时脚本自身问题，各自重试/修复后成功；上文下载结论以最终成功次为准。

- [x] **Step 2: README 增加「控制台（M1b）」段**

```markdown
## 控制台（M1b）

```bash
# 1. 起后端（同 M1a）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=changeme cargo run -p oss-server
# 2. 起前端（另开终端）
cd web && npm install && npm run dev
# 3. 打开 http://127.0.0.1:5173 ，用上面的账号登录
```

- 上传复用 aetherupload 自带脚本（分块 / 断线续传 / 秒传），前端不含分片逻辑
- **上传落点**：按上传分组落桶（`oss.toml` 的 `[[groups]]`，如 `file` → `files`）；对象名 = 内容的整份 MD5 +
  扩展名，子目录 = UTC 年月（如 `202610/<md5>.bin`）。所以在对象浏览器里是根据内容哈希找文件，不是原始文件名
- 秒传提示「上传成功（秒传）」表示 0 分块传输
- e2e：`cd web && npx playwright install chromium && npm run e2e`。Playwright 自带双 webServer
  （`cargo run` + `vite`），后端按 `web/playwright.config.ts` 里的 `pw` 自启；**已有后端在跑时会被复用**
  （`reuseExistingServer`），此时运行中实例的管理员口令必须也是 `pw`，否则登录 401——口令不一致就先停掉
  自起的服务，让 Playwright 用 `pw` 起自己的（「跑起来」段用的 `changeme` 只是示例占位）。
- 1 GB 上传与断线续传为**人工验证项**（不在 e2e 里）：
  `head -c 1073741824 /dev/urandom > /tmp/1g.bin` → 面板上传观察进度推进 → 中途 DevTools → Network → Offline
  （或拔网线）→ 输出区出现「网络故障，正在重试……」→ 恢复网络 → 自动继续至成功；下载后 `cmp` 与本地一致。
- 断线续传限于同一页面会话；刷新后需重传（跨刷新续传在 M2）
```

- [x] **Step 3: CI 验收**（本地预演已全绿；勾选待推送后 GitHub Actions 实跑确认）

  > **2026-10-12 实况**：推送已完成（main 至 v1.0.0 全量在远端），但 GitHub Actions 在**账号层面**被拒——两个 job 均在 2 秒内
  > 失败，注释原文：「The job was not started because recent account payments have failed or your spending limit needs to be increased」。
  > **不是代码问题**：修复 GitHub Billing 后重跑最近一次 run，两个 job 绿了再勾选本项。

  > **2026-10-14 结项（口径变更关闭）**：CI 已按用户指令改为**仅 `workflow_dispatch` 手动触发**（`.github/workflows/ci.yml` 的 `on:` 段），
  > push/PR 不再触发 Actions——「推送后 Actions 实跑」不再是本项的验收路径。验收口径 = **本地四闸**（fmt / clippy -D warnings /
  > 全量 test + 各 e2e），均已在本地全绿。GitHub Billing 问题不再阻塞本项，本勾选按口径变更关闭（上条 billing 注释为历史事实记录，保留）。

CI 文件（`.github/workflows/ci.yml`，已随本计划就位）有两个 job：
- `rust`：`cargo fmt --check` / `clippy -D warnings` / `test` —— 仓库无 `Cargo.toml` 时自动跳过（规划阶段不红）
- `web`：`npm ci && npm run build && npm test` —— `web/package.json` 出现后自动生效

本地预演 web job：

```bash
cd /home/wwwroot/oss/web && npm ci && npm run build && npm test
```

推送后到 GitHub Actions 确认两个 job 均绿。

**执行注记（team-lead 收尾轮实测）**：本地预演 web job —— `npm ci && npm run build && npm test` 全绿（build 1.45s、15 tests）；另跑 `npm run lint` exit 0、`npm run e2e` **3 passed（21.6s，webServer 自启模式）**、`cargo test --workspace -j 2` 98 passed。**本分支尚未推送，CI 从未实际跑过**（`.github/workflows/ci.yml` 的触发是 push main / PR）；推送后回来看两个 job 即可。

**CI 前提备忘（若将来把 e2e 接进 CI，质量评审给的五条）**：① 先 `cargo build -p oss-server` 预热（依赖含 s3s/ecat/sqlx/axum，GitHub runner 冷构建超过 webServer 的 180s 上限）；② vite 那条 webServer 补 `stdout: 'pipe'`（默认 ignore，起不来时是 60s 静默超时）；③ `reuseExistingServer: !process.env.CI`；④ `retries: process.env.CI ? 1 : 0`；⑤ 浏览器安装用 `--with-deps`。

- [x] **Step 4: 提交**

```bash
git add README.md && git commit -m "docs: 控制台使用说明与 1GB 上传清单"
```

---

## M1 完成标准（对照设计文档 M1 验收，M1a + M1b 合计）

| 设计文档的验收项 | 覆盖 |
|---|---|
| aws-cli 的 mb/ls/cp/rm 通过 | M1a Task 16 脚本第 1-8 步 |
| rclone 增删查列通过 | M1a Task 16 脚本第 9 步 |
| 浏览器 1 GB 上传 + 断线续传 | **M1b** Task 9 面板 + Task 12 手测清单 |
| 秒传命中，0 分块传输 | **M1b** Task 8 计数 + Task 9 提示 + Task 11 `instant.spec` |
| 控制台可浏览/下载/删除 | **M1b** Task 6 + Task 11 `console.spec` |
| `curl` 打 preprocess 返回协议一致的 JSON | M1a Task 14 Step 7 |

> **M1b 明确不做**（留给后续，别顺手做）：
> - 概览页（容量/请求速率需要指标端点，随 M4 集群面板一起）
> - 设置页（上传分组只读展示；分组编辑是 M2）
> - 跨刷新断点续传（服务端进度查询 + Multipart，M2）
> - 生产静态托管（构建产物由 nginx 服务、TLS、X-Accel-Redirect 下载——部署随 M2，见设计文档 §12）
>
> **本地卫生遗留（非功能项）**：e2e 使用共享 dev 数据目录（`data/`）、不建独立环境也不清桶——反复跑会在 dev 数据里累积
> `*e2e*` 桶（写此注时 79 个，含 65 个 e2e 桶），需手工 `rm -rf data/*e2e*` 或将来给 e2e 配独立数据目录。

## 已知风险与处置

| 风险 | 处置 |
|---|---|
| aetherupload-all.js 的实际行为与本文档描述不符 | Task 8 Step 1 要求先读 crate 自带 `assets/example.html`（权威集成示例）；DOM 契约（wrapper + 四个 id + success 回调）来自 1.0.3 源码逐行核对 |
| zepto 直改 DOM 与 React 重渲染打架 | 四个 id 节点 React 只渲染一次空内容，此后交给脚本；Modal 用 `destroyOnHidden` 保证每次打开是干净节点（antd 5.29 起 `destroyOnClose` 为弃用别名） |
| antd 双字按钮文案带空格（「确 定」） | e2e 统一用 `{ name: '确 定', exact: true }` 形态（含 登 录/上 传/下 载/删 除；勿用裸子串匹配——会撞「选择文件并开始上传」） |
| 秒传用例在旧数据目录上误判 | 用例的文件内容带时间戳，第一次必然走真分块 |
| `/aetherupload-assets` 被代理吞掉 | 代理键写成 `'/aetherupload/'`（带尾斜杠），Task 1 已注明 |
| 将来加 CORS 时预检被 401 吃掉 | `CorsLayer` 必须叠在 `/aetherupload` 的 auth 中间件**之外**，否则预检 OPTIONS 会被 401（质量评审结论） |

