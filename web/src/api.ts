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
  max_size: number // 0 = 不限（上游语义，Task 9 展示层按此处理）
  extensions: string[]
}

export interface AccessKey {
  access_key: string
  secret_key: string
}
