import { expect, test } from '@playwright/test'
import { createHash } from 'node:crypto'
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

test('登录 → 建桶 → 上传 → 列表 → 下载 → 删除', async ({ page }) => {
  const bucket = `e2e-${Date.now()}`
  const dir = mkdtempSync(join(tmpdir(), 'oss-e2e-'))
  const content = `hello-${Date.now()}`
  const file = join(dir, 'hello.txt')
  writeFileSync(file, content)

  // 上传的落点是「分组 → 桶」映射（IngestSink::resolve），不是当前浏览的桶：group `file` → 桶
  // `files`，键 `<UTC 年月>/<内容 md5>.<ext>`（aether.rs 固定 SubdirRule::Month；crate
  // util.rs 的 generate_sub_dir_name 走 UTC，见其与 PHP 的差异注释）。
  const now = new Date()
  const subdir = `${now.getUTCFullYear()}${String(now.getUTCMonth() + 1).padStart(2, '0')}`
  const name = `${createHash('md5').update(content).digest('hex')}.txt`

  await page.goto('/login')
  await page.getByLabel('用户名').fill('erik')
  await page.getByLabel('口令').fill('pw')
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page).toHaveURL(/\/buckets/)

  await page.getByRole('button', { name: '新建桶' }).click()
  await page.locator('#new-bucket-name').fill(bucket)
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await expect(page.getByRole('link', { name: bucket })).toBeVisible()

  await page.getByRole('link', { name: bucket }).click()
  await page.getByRole('button', { name: '上 传', exact: true }).click()
  await page.locator('#aetherupload-resource').setInputFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功', { timeout: 30_000 })
  await page.getByRole('button', { name: 'Close' }).click() // antd Modal 的关闭按钮 aria-label

  await page.goto(`/buckets/files?prefix=${encodeURIComponent(subdir + '/')}`)
  const row = page.getByRole('row', { name })
  await expect(row).toBeVisible()

  const downloadPromise = page.waitForEvent('download')
  await row.getByRole('button', { name: '下 载', exact: true }).click()
  const download = await downloadPromise
  const saved = await download.path()
  expect(saved).toBeTruthy()
  expect(readFileSync(saved!, 'utf8')).toBe(content) // 下载字节一致（Task 6 执行注记留给 Task 11 的那条）

  await row.getByRole('button', { name: '删 除', exact: true }).click()
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await expect(page.getByRole('row', { name })).toHaveCount(0)
})

// Task 10 的 Minor 3 转记：/keys 的浏览器路径并入本任务。
test('新建访问密钥 → 弹窗展示 AK/SK → 复制裸 SK → 列表出现新 AK', async ({ page, context }) => {
  await context.grantPermissions(['clipboard-read', 'clipboard-write'])

  await page.goto('/login')
  await page.getByLabel('用户名').fill('erik')
  await page.getByLabel('口令').fill('pw')
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page).toHaveURL(/\/buckets/)

  await page.getByRole('link', { name: '访问密钥' }).click()
  await page.getByRole('button', { name: '新建访问密钥' }).click()

  const paras = page.locator('.ant-modal .ant-typography')
  await expect(paras).toHaveCount(2) // AK 一行、SK 一行
  const ak = (await paras.nth(0).innerText()).replace(/^Access Key：/, '').trim()
  const sk = (await paras.nth(1).innerText()).replace(/^Secret Key：/, '').trim()
  expect(ak).not.toBe('')
  expect(sk).not.toBe('')

  // 复制内容必须等于裸值（Task 10 定稿记录 I-1：裸 copyable 会连「Secret Key：」前缀一起复制）
  await paras.nth(1).locator('.ant-typography-copy').click()
  expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(sk)

  // 关窗用右上角关闭钮（aria-label 固定 Close；Esc 在 rc-dialog 里有焦点问题）
  await page.getByRole('button', { name: 'Close' }).click()
  await expect(page.getByRole('cell', { name: ak, exact: true })).toBeVisible()
})
