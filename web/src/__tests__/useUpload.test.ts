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
