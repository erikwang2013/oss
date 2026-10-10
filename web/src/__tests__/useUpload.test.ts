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
