import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError, api, getToken, rawApi, setToken } from '../api'

afterEach(() => {
  vi.unstubAllGlobals()
  localStorage.clear()
  document.cookie = 'oss_token=; path=/; max-age=0'
})

describe('token 存取', () => {
  it('set/get/清除', () => {
    expect(getToken()).toBeNull()
    setToken('t1')
    expect(getToken()).toBe('t1')
    setToken(null)
    expect(getToken()).toBeNull()
  })

  it('setToken 同生命周期种/清 cookie oss_token（aetherupload 分块 XHR 只认 cookie）', () => {
    setToken('c1')
    expect(document.cookie).toContain('oss_token=c1')
    setToken(null)
    expect(document.cookie).not.toContain('oss_token')
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

describe('rawApi()', () => {
  it('成功返回 Response 本体（不消费 body）且带 Authorization 头', async () => {
    setToken('tok')
    const fetchMock = vi.fn().mockResolvedValue(new Response('bytes', { status: 200 }))
    vi.stubGlobal('fetch', fetchMock)

    const resp = await rawApi('/api/v1/buckets/b/objects/k')
    expect(resp.status).toBe(200)
    await expect(resp.text()).resolves.toBe('bytes')
    const init = fetchMock.mock.calls[0][1] as RequestInit
    expect(new Headers(init.headers).get('authorization')).toBe('Bearer tok')
  })

  it('401 时清 token 且抛 ApiError（与 api() 同一处理）', async () => {
    setToken('tok')
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('未登录', { status: 401 })))
    const p = rawApi('/api/v1/buckets/b/objects/k')
    await expect(p).rejects.toBeInstanceOf(ApiError)
    await expect(p).rejects.toMatchObject({ status: 401, message: '未登录' })
    expect(getToken()).toBeNull()
  })

  it('非 ok（非 401）抛 ApiError，不给 Response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response('内部错误', { status: 500 })))
    const p = rawApi('/x')
    await expect(p).rejects.toBeInstanceOf(ApiError)
    await expect(p).rejects.toMatchObject({ status: 500, message: '内部错误' })
  })
})
