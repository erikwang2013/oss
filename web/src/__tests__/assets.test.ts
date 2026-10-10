import { describe, expect, it } from 'vitest'
// Vite 的 ?raw：直接拿 vendored 脚本原文（不走 node:fs，jsdom 环境下也成立）
import src from '../../public/aetherupload-assets/aetherupload-all.js?raw'

// 上游漂移守卫：vendored 脚本带本地补丁（preprocess 从 zepto urlencoded 改成 multipart/FormData）。
// 若有人重新从 crate 原样拷贝覆盖，这个测试会红 —— 补丁内容、400 证据与原因见
// 计划 `docs/superpowers/plans/2026-10-10-m1b-frontend-console.md` 的 Task 9 定稿记录。
describe('vendored aetherupload-all.js 的 preprocess 补丁', () => {
  it('preprocess 与分块上传都走 multipart：processData/contentType 各 ≥2 处 false', () => {
    expect(src.match(/processData: false/g)?.length ?? 0).toBeGreaterThanOrEqual(2)
    expect(src.match(/contentType: false/g)?.length ?? 0).toBeGreaterThanOrEqual(2)
  })

  it('preprocess 用 FormData 且带 resource_name 字段（补丁特有；原版 preprocess 没有 FormData）', () => {
    expect(src).toContain('new FormData()')
    expect(src).toContain("form.append('resource_name'")
  })
})
