const TOKEN_KEY = 'oss.token'
// aetherupload-all.js 的分块 XHR 带 withCredentials 但不会加 Authorization 头；
// 后端 /aetherupload/* 的 JWT 覆盖层认 cookie `oss_token`。寿命对齐服务端 12h（console.rs）。
const TOKEN_COOKIE = 'oss_token'
const TOKEN_MAX_AGE = 12 * 3600

export function getToken(): string | null {
  return localStorage.getItem(TOKEN_KEY)
}
/** 写/清 token：localStorage 与 cookie 同生命周期（登出、401 清 token 时 cookie 一并清掉）。 */
export function setToken(t: string | null): void {
  if (t) {
    localStorage.setItem(TOKEN_KEY, t)
    document.cookie = `${TOKEN_COOKIE}=${t}; path=/; SameSite=Lax; max-age=${TOKEN_MAX_AGE}`
  } else {
    localStorage.removeItem(TOKEN_KEY)
    document.cookie = `${TOKEN_COOKIE}=; path=/; max-age=0`
  }
}

export class ApiError extends Error {
  status: number // 手写赋值：tsconfig 开了 erasableSyntaxOnly，参数属性（constructor(public x)）不可用
  constructor(status: number, message: string) {
    super(message)
    this.status = status
  }
}

/**
 * 裸 Response 版（下载等要原始 body 的场景）。401 处理（清 token + 跳登录）与
 * 非 2xx 抛 ApiError 都在这里；api() 只是其上的 JSON 便捷层，两条路径行为一致。
 */
export async function rawApi(path: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers)
  const token = getToken()
  if (token) headers.set('authorization', `Bearer ${token}`)
  // FormData 让 fetch 自己带 multipart boundary，手设 content-type 会把 boundary 弄丢
  if (init.body && !(init.body instanceof FormData) && !headers.has('content-type')) headers.set('content-type', 'application/json')

  const resp = await fetch(path, { ...init, headers })
  // 错误文案一处收口：HTTP/2 下 statusText 常为空、body 也可能空，兜到 HTTP <status>，
  // 保证 ApiError.message 永不为空串（否则 UI 出现空 toast）
  const errText = async () => (await resp.text()).trim() || resp.statusText || `HTTP ${resp.status}`
  if (resp.status === 401) {
    setToken(null)
    if (window.location.pathname !== '/login') window.location.href = '/login'
    throw new ApiError(401, await errText())
  }
  if (!resp.ok) throw new ApiError(resp.status, await errText())
  return resp
}

/** 统一的控制台 API 客户端。 */
export async function api<T>(path: string, init: RequestInit = {}): Promise<T> {
  const resp = await rawApi(path, init)
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
  max_size: number // 0 = 不限（上游语义，Task 9 展示层按此处理）
  extensions: string[]
}

export interface AccessKey {
  access_key: string
  secret_key: string
}

// ——— 子账户与权限（console.rs 权限体系）———

export interface Me {
  id: number
  username: string
  is_admin: boolean
}

/** 桶列表项（带能力位；管理员全 true；子账户按自有/授权计算） */
export interface BucketInfo {
  name: string
  is_owner: boolean
  can_read: boolean
  can_write: boolean
  can_delete: boolean
}

/** 子账户 × 桶 的三档权限 */
export interface Permission {
  bucket: string
  read: boolean
  write: boolean
  delete: boolean
}

export interface UserRow {
  id: number
  username: string
  is_admin: boolean
  /** "active" / "disabled"（停用即全链路失效） */
  status: string
  created_at: number
  permissions: Permission[]
}
