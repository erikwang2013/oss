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
  /** M8：存储类（生命周期 Transition 后变化）；从未设置 = null */
  storage_class?: string | null
  /** M8：归档阶段 Debug 名——Archived / Restoring / Restored */
  tier_phase?: string | null
  /** M8：临时恢复到期（unix 秒）；null = 无临时副本 */
  restore_expires_at?: number | null
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
  /** "active" / "disabled"（停用即全链路失效）；可选：旧后端/缓存响应可能缺字段 */
  status?: string
  created_at: number
  permissions: Permission[]
}

// ——— 集群状态（console.rs /cluster/status；M4 观测面）———

export interface ClusterNode {
  id: string
  addr: string
  drive_count: number
  up: boolean
}

export interface ClusterStatus {
  mode: 'single' | 'cluster'
  node_id: string | null
  slot_count: number
  topo_fingerprint: string | null
  nodes: ClusterNode[]
  quorum_unmet: { write: number; lock: number }
  /** M4 切片 8：多 set 分组视图（每 set 独立槽域，跨 set 槽号会重号）；单机/旧后端缺省 */
  sets?: ClusterSet[]
}

export interface ClusterSet {
  set: number
  slot_count: number
  /** 本节点是否属于该 set */
  self: boolean
  nodes: ClusterNode[]
}

// ——— M8 控制面（Task 6a 页面用）———

/** 访问点（DB 行的投影；policy 为已解析 JSON 或 null）。 */
export interface AccessPointInfo {
  name: string
  bucket: string
  owner_id: number
  policy: unknown | null
  created_at: number
  updated_at: number
}

/** 变换器配置读视图：不返回模块字节，只回摘要/大小。 */
export interface TransformInfo {
  kind: 'http' | 'wasm'
  endpoint: string | null
  module_sha256: string | null
  module_size: number | null
  timeout_ms: number
  max_response_bytes: number
  updated_at: number
}

/**
 * 作业行。列表端点（GET /jobs）只投影历史列（scanned/deleted/skipped），
 * 作业引擎字段（priority/total/succeeded/failed/requested_by）只在详情端点出现。
 */
export interface JobSummary {
  id: number
  kind: string
  bucket: string
  prefix: string
  state: string
  scanned?: number
  deleted?: number
  skipped?: number
  error?: string | null
  created_at: number
  updated_at: number
  progress?: number
  total?: number
  succeeded?: number
  failed?: number
  priority?: number
  requested_by?: number | null
  report_bucket?: string | null
  report_prefix?: string | null
  started_at?: number | null
  finished_at?: number | null
}

export interface JobTask {
  task_no: number
  action: string
  state: string
  attempt: number
  next_at: number
  result_code: string | null
  result_string: string | null
  started_at: number | null
  finished_at: number | null
}

export interface JobDetail {
  job: JobSummary
  tasks: JobTask[]
  offset: number
  limit: number
}

export interface TierInfo {
  id: string
  endpoint: string
  bucket: string
  healthy: boolean
}

export interface InventoryConfig {
  id: string
  is_enabled: boolean
  frequency: string
  destination_bucket: string
  destination_prefix: string | null
  format: string
  filter_prefix: string | null
  included_object_versions: string
  optional_fields: string[]
}

export interface PolicyTemplate {
  id: string
  name: string
  description: string
  doc: unknown
}

export interface AuditRow {
  id: number
  ts: number
  uid: number
  op: string
  bucket: string | null
  object_key: string | null
  decision: string
  detail: string | null
}

/** GET /encryption（只读；无变更端点）。 */
export interface EncryptionStatus {
  sse_s3: boolean
  sse_kms: boolean
  kms_current_key_id: string | null
}

export interface BucketEncryption {
  /** null = 未配置 */
  algorithm: string | null
}

export interface NotificationQueue {
  id?: string
  queue_arn?: string
  events?: string[]
  filter?: { key?: { filter_rules?: { name?: string; value?: string }[] } }
}

/** 与 S3 GetBucketNotificationConfiguration 同形的线格式（snake_case）。 */
export interface NotificationConfig {
  queue_configurations?: NotificationQueue[]
}

export interface DeadEvent {
  id: number
  bucket: string
  object_key: string | null
  event: string | null
  attempts: number
  last_error: string | null
  created_at: number
}

export interface ObjectTag {
  key: string
  value: string
}

export interface SelectResult {
  /** CSV 输出 → string[][]；JSON 输出 → JSON 值数组 */
  records: unknown[]
  stats: { bytes_scanned: number; bytes_processed: number; bytes_returned: number }
}
