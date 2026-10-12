/// <reference types="vitest/config" />
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

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
    // 高负载机器上 worker 启动可能远超默认 60s（本机实测 ~80s）——放宽避免整体假失败。
    // 注意键名是 startupTimeout（写成 startTimeout 会静默无效）。
    poolOptions: { forks: { startupTimeout: 120_000 } },
  },
})
