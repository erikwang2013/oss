import { expect, test } from '@playwright/test'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

test('同一文件二次上传命中秒传：不再产生 /aetherupload/uploading 请求', async ({ page }) => {
  const bucket = `e2e-${Date.now()}`
  const dir = mkdtempSync(join(tmpdir(), 'oss-e2e-'))
  const file = join(dir, 'instant.txt')
  writeFileSync(file, `instant-${Date.now()}`)

  const uploadingPosts: string[] = []
  page.on('request', (r) => {
    if (r.method() === 'POST' && r.url().includes('/aetherupload/uploading')) uploadingPosts.push(r.url())
  })

  await page.goto('/login')
  await page.getByLabel('用户名').fill('erik')
  await page.getByLabel('口令').fill('pw')
  await page.getByRole('button', { name: '登 录', exact: true }).click()

  await page.getByRole('button', { name: '新建桶' }).click()
  await page.locator('#new-bucket-name').fill(bucket)
  await page.getByRole('button', { name: /确\s*定/ }).click()
  await page.getByRole('link', { name: bucket }).click()

  // 第一次：真分块
  await page.getByRole('button', { name: '上 传', exact: true }).click()
  await page.locator('#aetherupload-resource').setInputFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功', { timeout: 30_000 })
  expect(uploadingPosts.length).toBeGreaterThan(0)

  // 第二次：真人路径（Task 9 定稿记录：脚本成功后给 input 置 disabled，onFinished 负责清除）。
  // setInputFiles 会绕过 disabled——只作断言会漏掉该缺陷，故走「按钮 → filechooser」：
  // input 仍 disabled 时 input.click() 不派发事件，filechooser 永不出现，本用例即红。
  const choose = page.getByRole('button', { name: '选择文件并开始上传' })
  await expect(choose).toBeEnabled()
  const before = uploadingPosts.length
  const chooser = page.waitForEvent('filechooser')
  await choose.click()
  await (await chooser).setFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功（秒传）', { timeout: 15_000 })
  await expect(page.getByText('秒传命中（0 分块传输）')).toBeVisible()
  expect(uploadingPosts.length).toBe(before)
})
