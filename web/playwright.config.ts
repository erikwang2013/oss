import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { defineConfig } from '@playwright/test'

// 独立临时数据目录/DB：不读写开发者的 ./data 与 ./oss.db。后端只接受配置文件，
// 故生成一份临时 oss.toml（分组映射 file→files / image→images 与控制台用例一致）。
const e2eRoot = mkdtempSync(join(tmpdir(), 'oss-web-e2e-'))
const e2eConfig = join(e2eRoot, 'oss.toml')
const q = JSON.stringify
writeFileSync(
  e2eConfig,
  `
listen = "127.0.0.1:9000"
data_dir = ${q(join(e2eRoot, 'data'))}
staging_dir = ${q(join(e2eRoot, 'staging'))}
database_url = "sqlite://${join(e2eRoot, 'oss.db')}?mode=rwc"
jwt_secret = "web-e2e-only-jwt-secret-0123456789abcdef"
max_put_bytes = 268435456

[[groups]]
group = "image"
bucket = "images"
max_size = 10485760
extensions = ["jpg", "jpeg", "png", "gif", "webp", "svg"]

[[groups]]
group = "video"
bucket = "videos"
max_size = 0
extensions = ["mp4", "mov", "mkv", "webm"]

[[groups]]
group = "file"
bucket = "files"
max_size = 0
extensions = []
`,
)

export default defineConfig({
  testDir: './e2e',
  timeout: 60_000,
  // locale 必须显式 zh-CN：aetherupload-all.js 按 navigator.language 选文案表
  // （getLocalizedMessages），Playwright 默认 en-US 时 #aetherupload-output 写的是
  // 「upload succeed」，中文断言必然超时。控制台本身也是 zh-CN（index.html + antd locale）。
  use: { baseURL: 'http://127.0.0.1:5173', locale: 'zh-CN' },
  // 两个 webServer：后端（cargo）与前端（vite）。后端跑在临时目录/DB 上（见上），
  // 用例用「时间戳 + UUID 的桶名 / 文件内容」保证同一轮内可重复、可并发跑。
  // 不复用已在运行的服务：否则会连到开发者的 ./data 或错误端口的进程。
  webServer: [
    {
      command: `cargo run -p oss-server -- --config ${q(e2eConfig)} serve`,
      cwd: '..',
      url: 'http://127.0.0.1:9000/health/live',
      reuseExistingServer: false,
      timeout: 180_000,
      env: { OSS_ADMIN_USER: 'erik', OSS_ADMIN_PASSWORD: 'pw' },
    },
    {
      command: 'npm run dev',
      url: 'http://127.0.0.1:5173',
      reuseExistingServer: false,
      timeout: 60_000,
    },
  ],
})
