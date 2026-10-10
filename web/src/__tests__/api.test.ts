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
