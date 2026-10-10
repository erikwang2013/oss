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
