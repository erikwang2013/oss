# M1b 前端控制台 实现计划

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
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('unauthorized', { status: 401 })))
    await expect(api('/api/v1/buckets')).rejects.toBeInstanceOf(ApiError)
    expect(getToken()).toBeNull()
  })

  it('非 2xx 抛 ApiError 且带状态码与响应文本', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('桶非空', { status: 409 })))
    const err = await api('/api/v1/buckets/a').catch((e) => e as ApiError)
    expect(err.status).toBe(409)
    expect(err.message).toBe('桶非空')
  })

  it('204 返回 undefined', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))
    await expect(api('/api/v1/buckets/a', { method: 'DELETE' })).resolves.toBeUndefined()
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
  constructor(public status: number, message: string) {
    super(message)
  }
}

/** 统一的控制台 API 客户端。401 时清 token 并跳登录页。 */
export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  const token = getToken()
  if (token) headers.set('authorization', `Bearer ${token}`)
  if (init.body && !headers.has('content-type')) headers.set('content-type', 'application/json')

  const resp = await fetch(path, { ...init, headers })
  if (resp.status === 401) {
    setToken(null)
    if (window.location.pathname !== '/login') window.location.href = '/login'
    throw new ApiError(401, '未登录')
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

- [x] **Step 4: 跑测试**

Run: `cd web && npm test`
Expected: PASS（4 个用例）

- [x] **Step 5: 提交**

```bash
git add web/src/api.ts web/src/__tests__/api.test.ts
git commit -m "feat(web): 控制台 API 客户端（token/401/错误体）"
```

---

## Task 3: App 骨架 —— 路由 · 鉴权 · 布局

**Files:**
- Create: `web/src/App.tsx`（覆盖模板）、`web/src/main.tsx`（覆盖模板）
- Delete: 模板的 `web/src/App.css`、`web/src/assets/react.svg`

- [ ] **Step 1: 实现 `web/src/App.tsx`**

```tsx
import { Navigate, Outlet, createBrowserRouter } from 'react-router-dom'
import { Layout, Menu } from 'antd'
import { Link, useLocation } from 'react-router-dom'
import { DatabaseOutlined, KeyOutlined, UploadOutlined } from '@ant-design/icons'
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

`@ant-design/icons` 需安装：`npm install @ant-design/icons`（并入 Step 4 的提交）。

- [ ] **Step 2: 覆盖 `web/src/main.tsx`**

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

删掉模板的 `web/src/App.css`、`web/src/index.css` 里不需要的样式引用（`main.tsx` 不再 import 它们）；模板 `App.tsx` 其余内容整份替换。

- [ ] **Step 3: 先建四个页面的空壳**（让编译成立，Task 4-6/10 逐个填）

```tsx
// web/src/pages/Login.tsx / Buckets.tsx / Objects.tsx / Keys.tsx 各一
export default function Login() { return <div>登录</div> }
```

- [ ] **Step 4: 跑构建验证**

Run: `cd web && npm run build`
Expected: 成功。

- [ ] **Step 5: 提交**

```bash
git add web/package.json web/package-lock.json web/src/
git commit -m "feat(web): 路由 + 鉴权壳 + antd 布局"
```

---

## Task 4: 登录页

**Files:**
- Modify: `web/src/pages/Login.tsx`

- [ ] **Step 1: 实现**

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

- [ ] **Step 2: 验证**

```bash
cd /home/wwwroot/oss
# 起后端（前提：M1a 已完成）
OSS_ADMIN_USER=erik OSS_ADMIN_PASSWORD=pw cargo run -p oss-server &
cd web && npm run dev &
```

浏览器打开 `http://127.0.0.1:5173/login`：错密码提示「用户名或密码错误」；对密码跳转 `/buckets`（此时桶页还是壳）。验证完 `kill %1 %2`。

- [ ] **Step 3: 提交**

```bash
git add web/src/pages/Login.tsx
git commit -m "feat(web): 登录页"
```

---

## Task 5: 桶列表页

**Files:**
- Modify: `web/src/pages/Buckets.tsx`

- [ ] **Step 1: 实现**

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
      <Button type="primary" onClick={() => setCreating(true)} style={{ marginBottom: 16 }}>
        新建桶
      </Button>
      <Table
        rowKey={(r) => r}
        loading={isLoading}
        dataSource={buckets}
        columns={[
          { title: '桶名', dataIndex: 0, render: (name: string) => <Link to={`/buckets/${encodeURIComponent(name)}`}>{name}</Link> },
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
          const el = document.getElementById('new-bucket-name') as HTMLInputElement | null
          if (el?.value) create.mutate(el.value)
        }}
        confirmLoading={create.isPending}
      >
        <Input id="new-bucket-name" placeholder="3-63 字符，小写字母数字与 - ." autoFocus />
      </Modal>
    </>
  )
}
```

- [ ] **Step 2: 验证**

浏览器 `/buckets`：建 `alpha` → 列表出现；点进去壳页；回列表删 `alpha`（空桶）成功；删非空桶报 `BucketNotEmpty`（错误文案来自后端纯文本）。

- [ ] **Step 3: 提交**

```bash
git add web/src/pages/Buckets.tsx
git commit -m "feat(web): 桶列表页（建/删/进入）"
```

---

## Task 6: 对象浏览器页

**Files:**
- Modify: `web/src/pages/Objects.tsx`

- [ ] **Step 1: 实现**（前缀导航来自 `common_prefixes`，下载走 fetch→blob，避免给浏览器下载请求塞 JWT 头的难题）

```tsx
import { useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { App, Breadcrumb, Button, Popconfirm, Table, Tooltip, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, ObjectList } from '../api'

function humanSize(n: number) {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`
}

export default function Objects() {
  const { bucket = '' } = useParams()
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [prefix, setPrefix] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['objects', bucket, prefix],
    queryFn: () =>
      api<ObjectList>(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/objects?prefix=${encodeURIComponent(prefix)}&delimiter=/`,
      ),
    enabled: !!bucket,
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['objects', bucket] })

  const remove = useMutation({
    mutationFn: (key: string) =>
      api(`/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${key}`, { method: 'DELETE' }),
    onSuccess: () => { message.success('已删除'); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const download = async (key: string) => {
    try {
      const resp = await fetch(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${key}`,
        { headers: { authorization: `Bearer ${localStorage.getItem('oss.token') ?? ''}` } },
      )
      if (!resp.ok) throw new Error(await resp.text())
      const blob = await resp.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = key.split('/').pop() ?? 'download'
      a.click()
      URL.revokeObjectURL(url)
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
      <Table
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
                <a onClick={() => setPrefix(key)}>{key.slice(prefix.length)}</a>
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

注意：`prefix` 初始值从 URL query（面包屑链接用 `?prefix=`）读取——在组件顶部补一行：

```tsx
const [prefix, setPrefix] = useState(() => new URLSearchParams(window.location.search).get('prefix') ?? '')
```

（覆盖上面 `useState('')`；面包屑链接切换时整页跳转、组件重挂载，状态自然重置。）

- [ ] **Step 2: 验证**

用 `aws s3 cp` 往 `alpha` 放 `docs/a.txt`、`docs/b.txt`、`top.txt` → 控制台 `/buckets/alpha` 看到 `docs/` 与 `top.txt`；点 `docs/` 进前缀，面包屑回根；下载 `a.txt` 字节一致（`cmp`）；删 `top.txt` 后列表刷新。

- [ ] **Step 3: 提交**

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

- [ ] **Step 1: 写失败的测试**（追加到 `console_api.rs`）

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

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p oss-server --test console_api list_groups 2>&1 | tail -8`
Expected: FAIL（404）

- [ ] **Step 3: 实现**

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

- [ ] **Step 4: 跑测试**

Run: `cargo test -p oss-server 2>&1 | tail -8`
Expected: 全绿

- [ ] **Step 5: 提交**

```bash
git add crates/oss-server/ && git commit -m "feat(server): GET /api/v1/groups（上传分组）"
```

---

## Task 8: 拷贝自带脚本 + `useUpload` hook（TDD）

**Files:**
- Create: `web/public/aetherupload-assets/aetherupload-all.js`（从 crate 原样拷贝）
- Create: `web/src/upload/useUpload.ts`
- Test: `web/src/__tests__/useUpload.test.ts`

- [ ] **Step 1: 拷贝脚本**

```bash
cd /home/wwwroot/oss
mkdir -p web/public/aetherupload-assets
cp "$(find ~/.cargo/registry/src -path '*aetherupload-rust-1.0.3/assets/aetherupload-all.js' | head -1)" \
   web/public/aetherupload-assets/
ls -l web/public/aetherupload-assets/   # 应有 ~48KB 的 aetherupload-all.js
```

若 `~/.cargo/registry` 里找不到（还没构建过），先在仓库根跑一次 `cargo fetch` 再重试。
拷贝后**先读一遍 crate 内的 `assets/example.html`**（在 `~/.cargo/registry/src/*/aetherupload-rust-1.0.3/assets/`）——它是集成契约的权威示例，DOM 结构与本 hook 的用法都以它为准。

- [ ] **Step 2: 写失败的测试**

```ts
// web/src/__tests__/useUpload.test.ts
import { afterEach, describe, expect, it } from 'vitest'
import { countingUploadingPosts } from '../upload/useUpload'

afterEach(() => {
  // 每个用例后若还有包装，靠 restore 恢复；这里兜底断言无泄漏
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
})
```

- [ ] **Step 3: 跑测试确认失败**

Run: `cd web && npm test`
Expected: FAIL（`../upload/useUpload` 不存在）

- [ ] **Step 4: 实现 `web/src/upload/useUpload.ts`**

```ts
import { useCallback } from 'react'

/** aetherupload-all.js 暴露的全局工厂实例（无类型定义，按契约声明）。 */
type AetherInstance = {
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
      window.XMLHttpRequest = Orig
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
 */
export function useUpload(opts: { onFinished?: (outcome: UploadOutcome) => void }) {
  const start = useCallback(
    (fileInput: HTMLInputElement, group: string) => {
      const counter = countingUploadingPosts()
      window
        .aetherupload!(fileInput)
        .setGroup(group)
        .setSavedPathField('#aetherupload-savedpath')
        .setPreprocessRoute('/aetherupload/preprocess')
        .setUploadingRoute('/aetherupload/uploading')
        .setLaxMode(false)
        .success(() => {
          counter.restore()
          const savedPath =
            (document.querySelector('#aetherupload-savedpath') as HTMLInputElement | null)?.value ?? ''
          opts.onFinished?.({ savedPath, instant: counter.count() === 0 })
        })
        .upload()
    },
    [opts],
  )
  return { start }
}
```

- [ ] **Step 5: 跑测试**

Run: `cd web && npm test`
Expected: PASS（含 Task 2 的 4 个用例）

- [ ] **Step 6: 提交**

```bash
git add web/public/aetherupload-assets/ web/src/upload/useUpload.ts web/src/__tests__/useUpload.test.ts
git commit -m "feat(web): useUpload 驱动 aetherupload-all.js（含秒传判定）"
```

---

## Task 9: 上传面板（分组 · 进度 · 秒传提示 · 上传历史）

**Files:**
- Create: `web/src/format.ts`（从 Task 6 抽出 `humanSize` 共用）
- Create: `web/src/upload/UploadPanel.tsx`
- Modify: `web/src/pages/Objects.tsx`（引用 format.ts、加「上传」按钮）

- [ ] **Step 1: `web/src/format.ts`**

```ts
export function humanSize(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`
}
```

`Objects.tsx`：删掉内联的 `humanSize`，改为 `import { humanSize } from '../format'`。

- [ ] **Step 2: 实现 `web/src/upload/UploadPanel.tsx`**

```tsx
import { useRef, useState } from 'react'
import { App, Button, Modal, Select, Space, Table, Tag, Typography } from 'antd'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api, GroupInfo, UploadRecord } from '../api'
import { humanSize } from '../format'
import { UploadOutcome, useUpload } from './useUpload'

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
  const fileRef = useRef<HTMLInputElement>(null)

  const { data: groups = [] } = useQuery({
    queryKey: ['groups'],
    queryFn: () => api<GroupInfo[]>('/api/v1/groups'),
    enabled: open,
  })

  const { data: history = [] } = useQuery({
    queryKey: ['uploads'],
    queryFn: () => api<UploadRecord[]>('/api/v1/uploads'),
    enabled: open,
    refetchInterval: open ? 3000 : false,
  })

  const { start } = useUpload({
    onFinished: async (outcome) => {
      setLast(outcome)
      message.success(outcome.instant ? '上传成功（秒传）' : '上传成功')
      const originalName = fileRef.current?.files?.[0]?.name
      if (outcome.savedPath) {
        await api('/api/v1/uploads/register', {
          method: 'POST',
          body: JSON.stringify({ saved_path: outcome.savedPath, original_name: originalName ?? '' }),
        }).catch(() => message.warning('上传记录登记失败（对象本身已入库）'))
      }
      qc.invalidateQueries({ queryKey: ['objects', bucket] })
      qc.invalidateQueries({ queryKey: ['uploads'] })
    },
  })

  // 让「同一文件再选一次」也能触发 onChange（秒传验证要用）
  const onFilePicked = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target
    if (!input.files?.[0]) return
    setLast(null)
    start(input, group)
    input.value = ''
  }

  return (
    <Modal title={`上传到 ${bucket}`} open={open} onCancel={onClose} footer={null} width={640} destroyOnClose>
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
          <Button onClick={() => fileRef.current?.click()} loading={!window.aetherupload ? true : undefined}>
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

- [ ] **Step 3: `Objects.tsx` 接入上传按钮**

页头加状态与按钮，Modal 挂在页面底部：

```tsx
const [uploading, setUploading] = useState(false)
// …页头：
<Button type="primary" onClick={() => setUploading(true)} style={{ marginBottom: 16 }}>上传</Button>
// …组件返回的末尾：
<UploadPanel open={uploading} bucket={bucket} onClose={() => setUploading(false)} />
```

对应 import：`import UploadPanel from '../upload/UploadPanel'`。

- [ ] **Step 4: 验证**

后端 + `npm run dev`，打开 `/buckets/<桶>` → 点「上传」→ 选文件 → 进度条推进 → 「上传成功」，历史表出现记录；同一文件再传一次 → 「上传成功（秒传）」+ 金色 Tag。

- [ ] **Step 5: 提交**

```bash
git add web/src/format.ts web/src/upload/UploadPanel.tsx web/src/pages/Objects.tsx
git commit -m "feat(web): 上传面板（分组/进度/秒传提示/上传历史）"
```

---

## Task 10: 访问密钥页

**Files:**
- Modify: `web/src/pages/Keys.tsx`

- [ ] **Step 1: 实现**

```tsx
import { useState } from 'react'
import { Alert, App, Button, Modal, Table, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, AccessKey } from '../api'

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
          { title: 'Access Key', dataIndex: 0 },
          { title: '说明', render: () => '启用/禁用与过期在 M5；M1 只有创建与使用' },
        ]}
      />
      <Modal title="密钥已创建" open={!!fresh} footer={null} onCancel={() => setFresh(null)}>
        {fresh && (
          <>
            <Alert type="warning" showIcon style={{ marginBottom: 12 }}
              message="Secret Key 只显示这一次，请立即保存（M5 前它是明文存库的）。" />
            <Typography.Paragraph copyable>Access Key：{fresh.access_key}</Typography.Paragraph>
            <Typography.Paragraph copyable>Secret Key：{fresh.secret_key}</Typography.Paragraph>
          </>
        )}
      </Modal>
    </>
  )
}
```

- [ ] **Step 2: 验证**

`/keys`：新建 → 弹窗显示 AK/SK（点复制可用）；列表出现新 AK；用该 AK 配 `aws-cli` 能 `aws s3 ls`（M1a 验收的无损回归）。

- [ ] **Step 3: 提交**

```bash
git add web/src/pages/Keys.tsx
git commit -m "feat(web): 访问密钥页（列表 + 一次性展示 SK）"
```

---

## Task 11: Playwright e2e（关键路径）

**Files:**
- Create: `web/playwright.config.ts`、`web/e2e/console.spec.ts`、`web/e2e/instant.spec.ts`

- [ ] **Step 1: 配置**

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

- [ ] **Step 2: `e2e/console.spec.ts`——闭环**

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
  await page.getByRole('button', { name: '登录' }).click()
  await expect(page).toHaveURL(/\/buckets/)

  await page.getByRole('button', { name: '新建桶' }).click()
  await page.locator('#new-bucket-name').fill(bucket)
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await expect(page.getByRole('link', { name: bucket })).toBeVisible()

  await page.getByRole('link', { name: bucket }).click()
  await page.getByRole('button', { name: '上传' }).click()
  await page.locator('#aetherupload-resource').setInputFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功', { timeout: 30_000 })
  await page.getByRole('button', { name: 'Close' }).click() // antd Modal 的关闭按钮 aria-label

  const row = page.getByRole('row', { name: /hello\.txt/ })
  await expect(row).toBeVisible()

  const downloadPromise = page.waitForEvent('download')
  await row.getByRole('button', { name: '下载' }).click()
  const download = await downloadPromise
  const saved = await download.path()
  expect(saved).toBeTruthy()

  await row.getByRole('button', { name: '删除' }).click()
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await expect(page.getByRole('row', { name: /hello\.txt/ })).toHaveCount(0)
})
```

- [ ] **Step 3: `e2e/instant.spec.ts`——秒传零分块**

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
  await page.getByRole('button', { name: '登录' }).click()

  await page.getByRole('button', { name: '新建桶' }).click()
  await page.locator('#new-bucket-name').fill(bucket)
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await page.getByRole('link', { name: bucket }).click()

  // 第一次：真分块
  await page.getByRole('button', { name: '上传' }).click()
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

- [ ] **Step 4: 跑 e2e**

```bash
cd /home/wwwroot/oss/web
npx playwright install chromium   # 首次
npm run e2e
```

Expected: 2 passed。（若本机已有后端在跑，`reuseExistingServer` 会直接复用——注意它会用运行中实例的数据目录。）

- [ ] **Step 5: 提交**

```bash
git add web/playwright.config.ts web/e2e/
git commit -m "test(web): Playwright 关键路径（闭环 + 秒传零分块）"
```

---

## Task 12: 1 GB 断线续传手测清单 + README + CI 验收

**Files:**
- Modify: `README.md`

- [ ] **Step 1: 1 GB 上传 + 断线续传手测**（设计文档 M1 验收第 2 条，浏览器部分）

```bash
head -c 1073741824 /dev/urandom > /tmp/1g.bin
```

在控制台对象页 →「上传」→ 选 `/tmp/1g.bin`，按清单逐项确认：

1. 进度条持续推进，直到「上传成功」；对象浏览器出现该文件。
2. **断线续传**：上传中途打开 DevTools → Network → 切 Offline（或拔网线）→ 输出区出现「网络故障，正在重试……」→ 恢复网络 → 上传自动继续直到成功（服务端对重发分块幂等跳过，见规格 §5.2）。
3. 下载该 1 GB 文件，`cmp /tmp/1g.bin <下载文件>` 一致。
4. `uploads` 表可见记录（上传历史面板）；二次传同一文件命中秒传。
5. 已知边界（写进 README）：**刷新页面后续传不了**——M1 的续传限于同一页面会话；跨刷新的断点续传要等服务端进度查询 + Multipart（M2）。

- [ ] **Step 2: README 增加「控制台（M1b）」段**

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
- 秒传提示「上传成功（秒传）」表示 0 分块传输
- e2e：`cd web && npx playwright install chromium && npm run e2e`
- 断线续传限于同一页面会话；刷新后需重传（跨刷新续传在 M2）
```

- [ ] **Step 3: CI 验收**

CI 文件（`.github/workflows/ci.yml`，已随本计划就位）有两个 job：
- `rust`：`cargo fmt --check` / `clippy -D warnings` / `test` —— 仓库无 `Cargo.toml` 时自动跳过（规划阶段不红）
- `web`：`npm ci && npm run build && npm test` —— `web/package.json` 出现后自动生效

本地预演 web job：

```bash
cd /home/wwwroot/oss/web && npm ci && npm run build && npm test
```

推送后到 GitHub Actions 确认两个 job 均绿。

- [ ] **Step 4: 提交**

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

## 已知风险与处置

| 风险 | 处置 |
|---|---|
| aetherupload-all.js 的实际行为与本文档描述不符 | Task 8 Step 1 要求先读 crate 自带 `assets/example.html`（权威集成示例）；DOM 契约（wrapper + 四个 id + success 回调）来自 1.0.3 源码逐行核对 |
| zepto 直改 DOM 与 React 重渲染打架 | 四个 id 节点 React 只渲染一次空内容，此后交给脚本；Modal 用 `destroyOnClose` 保证每次打开是干净节点 |
| antd 双字按钮文案带空格（「确 定」） | e2e 用 `/确\s*定/` 正则匹配 |
| 秒传用例在旧数据目录上误判 | 用例的文件内容带时间戳，第一次必然走真分块 |
| `/aetherupload-assets` 被代理吞掉 | 代理键写成 `'/aetherupload/'`（带尾斜杠），Task 1 已注明 |

