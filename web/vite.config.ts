import react from '@vitejs/plugin-react'
// 从 vitest/config 取 defineConfig：`test` 字段（含 poolOptions）的类型只有这个入口认；
// 用 vite 的 defineConfig + 三斜线引用在 tsc -b 下会报 TS2769（poolOptions 不存在）
import { defineConfig } from 'vitest/config'

const backend = 'http://127.0.0.1:9000'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  server: {
    // Vite 8 默认 host 'localhost' 在本机只解析到 ::1，而计划里 Playwright/浏览器
    // 用的都是 http://127.0.0.1:5173 —— 显式绑 IPv4 保证两边一致
    host: '127.0.0.1',
    proxy: {
      // 注意尾斜杠：'/aetherupload/' 不能写成 '/aetherupload'，
      // 否则 /aetherupload-assets/（public 下的自带脚本）会被误转发到后端 404
      '/api/': backend,
      '/aetherupload/': backend,
    },
  },
  test: {
    environment: 'jsdom',
    globals: true,
    include: ['src/**/*.test.ts', 'src/**/*.test.tsx'],
    // 注意：vitest 5 起 worker 启动超时是硬编码常量（START_TIMEOUT=60s，无配置旋钮；
    // v3 的 poolOptions.forks.startupTimeout 已删除）。本机高负载下若撞
    // 「Timeout waiting for worker to respond」，用 `vitest run --no-file-parallelism`
    // 减少并发启动，或错峰跑——不要再往这里加 poolOptions（会被判 removed 并忽略）。
  },
})
