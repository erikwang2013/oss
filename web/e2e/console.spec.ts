import { expect, test } from '@playwright/test'
import { createHash, randomUUID } from 'node:crypto'
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

test('登录 → 建桶 → 上传 → 列表 → 下载 → 删除', async ({ page }) => {
  // 前缀 `zzz-`：桶列表按桶名升序且 10 行/页（xl-store list_buckets 排序 + antd 默认分页），
  // 共享 data 目录里历史桶会一直堆积——`zzz-` 恒大于既有桶名，新桶永远排在最后一页最后一行，
  // 断言先翻到最后一页即可，不随数据量/并发漂移（曾用 `0-` 前缀，但并用运行一多，
  // 同前缀的新桶照样被挤出第 1 页）。
  const bucket = `zzz-e2e-${Date.now()}`
  const dir = mkdtempSync(join(tmpdir(), 'oss-e2e-'))
  // 内容必须逐次唯一：只有时间戳时，并行 worker 可能同毫秒起步 → 同内容 → 同 md5 → **同一个对象键**，
  // 两个运行会互相踩（一个把对象删了，另一个断言时行就没了）。对象名即内容 md5，见下方。
  const content = `hello-${Date.now()}-${randomUUID()}`
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
  // 建桶成功后列表会 refetch（invalidateQueries）且新桶在最后一页；refetch 与翻页有竞态，
  // 故整体重试「点最后一页 → 找新桶」。
  const bucketLink = page.getByRole('link', { name: bucket })
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(bucketLink).toBeVisible({ timeout: 1_000 })
  }).toPass({ timeout: 15_000 })

  await bucketLink.click()
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
  // 密钥列表按 created_at 升序、10 行/页（db.rs list_access_keys + antd 默认分页），共享 data
  // 目录里已 >10 条：新建的键在最后一页，别假设它在第 1 页。并发跑时恰好翻页会让它退一页，兜底再试。
  const pages = page.locator('.ant-pagination-item')
  const cell = page.getByRole('cell', { name: ak, exact: true })
  await pages.last().click()
  try {
    await expect(cell).toBeVisible({ timeout: 2_000 })
  } catch {
    await pages.nth((await pages.count()) - 2).click()
  }
  await expect(cell).toBeVisible()
})
