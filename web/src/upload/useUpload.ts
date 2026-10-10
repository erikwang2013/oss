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
