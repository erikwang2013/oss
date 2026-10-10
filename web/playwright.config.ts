import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  // locale 必须显式 zh-CN：aetherupload-all.js 按 navigator.language 选文案表
  // （getLocalizedMessages），Playwright 默认 en-US 时 #aetherupload-output 写的是
  // 「upload succeed」，中文断言必然超时。控制台本身也是 zh-CN（index.html + antd locale）。
  use: { baseURL: 'http://127.0.0.1:5173', locale: 'zh-CN' },
  // 两个 webServer：后端（cargo）与前端（vite）。后端数据目录沿用 oss.toml 默认；
  // 用例用「时间戳 + UUID 的桶名 / 文件内容」保证可重复、可并发跑。
  webServer: [
    {
      command: 'cargo run -p oss-server',
      cwd: '..',
      url: 'http://127.0.0.1:9000/health/live',
      reuseExistingServer: true,
      timeout: 180_000,
      env: { OSS_ADMIN_USER: 'erik', OSS_ADMIN_PASSWORD: 'pw' },
    },
    {
      command: 'npm run dev',
      url: 'http://127.0.0.1:5173',
      reuseExistingServer: true,
      timeout: 60_000,
    },
  ],
})
