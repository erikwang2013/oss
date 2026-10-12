import { createHash } from 'node:crypto'
import { describe, expect, it } from 'vitest'
// Vite 的 ?raw：直接拿 vendored 脚本原文（不走 node:fs，jsdom 环境下也成立）
import src from '../../public/aetherupload-assets/aetherupload-all.js?raw'

// 上游漂移守卫：vendored 脚本必须与 crate 的 `assets/aetherupload-all.js` 逐字节一致。
// 下面是 aetherupload-rust **2.1.0** 该文件的 sha256（与 crate 2.1.0 逐字节一致；升级 crate 时随之更新）。
// 历史：1.0.3 时代 Rust 适配层只吃 multipart，本地曾把 preprocess 改成 FormData 发送
// （补丁与 400 证据见计划 `docs/superpowers/plans/2026-10-10-m1b-frontend-console.md` Task 9 定稿记录，
// 补丁代码在 git 历史里）；1.0.4 起服务端 urlencoded/multipart 双收，补丁退役、脚本回归 stock，
// 本守卫随之从「保护补丁」改为「保护 stock」。1.0.5 上游把脚本的 preprocess 自身改成发 multipart
// （与我们此前的本地补丁殊途同归）、示例页补上 wrapper——我们的服务端两种编码都收，行为不变。
// 2.1.0（2.0 协议）：响应对改为「200 JSON 成功 / RFC 9457 问题响应」，脚本与适配层同步演进；
// 我们只经 HTTP 接它，DOM 契约（wrapper + 四个 id）、工厂签名与成功文案均未变（逐项核对过），
// 守卫随之钉到 2.1.0。
const STOCK_SHA256 = '2566a9a5c0b8f325fada1e2c1978ff905daa3e788f818a139bbdba2818780762'

describe('vendored aetherupload-all.js 与 crate 2.1.0 stock 逐字节一致', () => {
  it(`sha256 == ${STOCK_SHA256}`, () => {
    expect(createHash('sha256').update(src, 'utf8').digest('hex')).toBe(STOCK_SHA256)
  })
})
