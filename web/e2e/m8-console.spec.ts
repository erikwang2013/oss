import { expect, test, type Page } from '@playwright/test'
import { createHash, randomUUID } from 'node:crypto'
import { mkdtempSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'

// M8 控制台页面验收：策略/审计/设置/事件/Select/访问点/作业/存储层/清单/加密/对象标签。
// 共享 data 目录：所有新桶用 zzz-e2e-z- 前缀保证排在桶列表最后（见 console.spec 的注释）。

async function login(page: Page) {
  await page.goto('/login')
  await page.getByLabel('用户名').fill('erik')
  await page.getByLabel('口令').fill('pw')
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page).toHaveURL(/\/buckets/)
}

/** 走浏览器 fetch（带当前 token）建桶，避开桶列表分页。 */
async function newBucket(page: Page): Promise<string> {
  const bucket = `zzz-e2e-z-${Date.now()}-${randomUUID().slice(0, 8)}`
  const status = await page.evaluate(async (name) => {
    const token = localStorage.getItem('oss.token')
    const r = await fetch('/api/v1/buckets', {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ name }),
    })
    return r.status
  }, bucket)
  expect(status).toBe(204)
  return bucket
}

/** 走浏览器 fetch 建访问点（避开创建弹窗的桶下拉选择）。 */
async function newAccessPoint(page: Page, name: string, bucket: string) {
  const status = await page.evaluate(async ({ name, bucket }) => {
    const token = localStorage.getItem('oss.token')
    const r = await fetch('/api/v1/access-points', {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ name, bucket }),
    })
    return r.status
  }, { name, bucket })
  expect(status).toBe(201)
}

/** 上传到默认分组（file → files），返回服务端落点键。 */
async function uploadToFiles(page: Page, name: string, content: string): Promise<string> {
  const dir = mkdtempSync(join(tmpdir(), 'oss-e2e-m8-'))
  const file = join(dir, name)
  writeFileSync(file, content)

  await page.goto('/buckets/files')
  await page.getByRole('button', { name: /上\s*传/ }).click()
  await page.locator('#aetherupload-resource').setInputFiles(file)
  await expect(page.locator('#aetherupload-output')).toContainText('上传成功', { timeout: 30_000 })
  await page.getByRole('button', { name: 'Close' }).click()

  const now = new Date()
  const subdir = `${now.getUTCFullYear()}${String(now.getUTCMonth() + 1).padStart(2, '0')}/`
  const ext = name.split('.').pop() ?? 'bin'
  return subdir + createHash('md5').update(content).digest('hex') + '.' + ext
}

test('桶策略页：本地拦截无效 JSON，合法 JSON 保存成功', async ({ page }) => {
  await login(page)
  const bucket = await newBucket(page)
  await page.goto(`/buckets/${bucket}/policy`)

  const editor = page.getByLabel('桶策略 JSON')
  await expect(editor).toBeVisible()
  await editor.fill('{invalid')
  await page.getByRole('button', { name: '保存策略' }).click()
  await expect(page.getByText(/JSON 解析失败/)).toBeVisible()

  await editor.fill('{"Statement":[]}')
  await page.getByRole('button', { name: '保存策略' }).click()
  await expect(page.getByText('策略已保存')).toBeVisible({ timeout: 15_000 })
})

test('审计页：应用桶过滤并渲染行或空状态', async ({ page }) => {
  await login(page)
  await page.getByRole('link', { name: '审计' }).click()
  await expect(page.getByRole('heading', { name: '审计' })).toBeVisible()

  await page.getByLabel('桶').fill('files')
  await page.getByRole('button', { name: /查\s*询/ }).click()

  // 行或统一空状态（共享 data 目录里两者都可能）
  await expect(
    page.locator('.ant-table-row').first().or(page.getByText('暂无数据').first()),
  ).toBeVisible({ timeout: 15_000 })
  await expect(page.getByRole('columnheader', { name: '判定' })).toBeVisible()
})

test('设置页：渲染加密/KMS 只读状态', async ({ page }) => {
  await login(page)
  await page.getByRole('link', { name: '设置' }).click()
  await expect(page.getByRole('heading', { name: '设置' })).toBeVisible()
  await expect(page.getByText('SSE-S3（主密钥文件）')).toBeVisible()
  await expect(page.getByText('SSE-KMS')).toBeVisible()
  await expect(page.getByText('当前 KMS 主密钥')).toBeVisible()
  await expect(page.getByText(/环境变量驱动/)).toBeVisible()
})

test('事件通知：保存目标并回读，死信区块可见', async ({ page }) => {
  await login(page)
  const bucket = await newBucket(page)
  await page.getByRole('link', { name: '事件通知' }).click()

  // 选桶：antd Select 的 selection-item 会拦截 input 点击，点外层再键盘输入过滤
  await page.getByLabel('通知桶').first().click()
  await page.keyboard.type(bucket)
  await page.locator('.ant-select-item-option').filter({ hasText: bucket }).first().click()

  await page.getByRole('button', { name: '添加目标' }).click()
  await page.getByLabel('通知配置 ID').fill('cfg-e2e')
  await page.getByLabel('Kafka target').fill('t-e2e')
  await page.getByLabel('通知事件').first().click()
  await page.locator('.ant-select-item-option').filter({ hasText: 'ObjectCreated:Put' }).first().click()
  await page.keyboard.press('Escape')
  await page.getByRole('button', { name: /保\s*存/ }).click()
  await expect(page.getByText('通知配置已保存')).toBeVisible({ timeout: 15_000 })

  // 回读：刷新后重选桶，配置行仍在
  await page.reload()
  await page.getByLabel('通知桶').first().click()
  await page.keyboard.type(bucket)
  await page.locator('.ant-select-item-option').filter({ hasText: bucket }).first().click()
  await expect(page.getByLabel('通知配置 ID')).toHaveValue('cfg-e2e')

  await expect(page.getByRole('heading', { name: '死信事件' })).toBeVisible()
  await expect(page.locator('.ant-table').last().getByText('暂无数据')).toBeVisible()
})

test('Select 工作台：上传 CSV 后执行 SELECT * 并渲染记录', async ({ page }) => {
  await login(page)
  const content = `name,age\nalice,30\nbob,25-${Date.now()}-${randomUUID()}\n`
  const key = await uploadToFiles(page, 'data.csv', content)

  await page.goto('/buckets/files/select')
  await page.getByLabel('对象键').fill(key)
  await page.getByRole('button', { name: /执\s*行/ }).click()

  // 默认不把首行当表头：3 条记录（表头 + 2 行数据）
  await expect(page.getByText('记录数：3')).toBeVisible({ timeout: 20_000 })
  await expect(page.getByRole('cell', { name: 'alice', exact: true })).toBeVisible()
})

test('访问点/作业/存储层：渲染表格并打开创建弹窗', async ({ page }) => {
  await login(page)

  await page.getByRole('link', { name: '访问点' }).click()
  await expect(page.getByRole('heading', { name: '访问点' })).toBeVisible()
  await page.getByRole('button', { name: '新建访问点' }).click()
  await expect(page.getByLabel('访问点名称')).toBeVisible()
  await page.locator('.ant-modal').getByRole('button', { name: /取\s*消/ }).click()

  await page.getByRole('link', { name: '作业' }).click()
  await expect(page.getByRole('heading', { name: '作业' })).toBeVisible()
  await page.getByRole('button', { name: '新建 Batch 作业' }).click()
  await expect(page.getByPlaceholder('清单对象所在桶')).toBeVisible()
  await page.locator('.ant-modal').getByRole('button', { name: /取\s*消/ }).click()

  await page.getByRole('link', { name: '存储层' }).click()
  await expect(page.getByRole('heading', { name: '存储层' })).toBeVisible()
  await expect(page.getByRole('button', { name: /强制 sweep/ })).toBeVisible()
  await expect(page.getByRole('columnheader', { name: 'Endpoint' })).toBeVisible()
  await expect(page.getByRole('columnheader', { name: '健康' })).toBeVisible()
  await expect(
    page.locator('.ant-table-row').first().or(page.getByText('暂无数据').first()),
  ).toBeVisible()
})

test('访问点：策略与变换器保存后回读（max_response_bytes 不丢）', async ({ page }) => {
  await login(page)
  const bucket = await newBucket(page)
  const ap = `zzz-e2e-ap-${Date.now()}-${randomUUID().slice(0, 8)}`
  await newAccessPoint(page, ap, bucket)

  await page.getByRole('link', { name: '访问点' }).click()
  const row = page.getByRole('row', { name: new RegExp(ap) })
  await expect(row).toBeVisible()

  // 策略：保存后列表 refetch，等「已配置」再重开断言
  await row.getByRole('button', { name: /策\s*略/ }).click()
  await page.getByLabel('访问点策略 JSON').fill('{"Statement":[]}')
  await page.locator('.ant-modal').getByRole('button', { name: /保\s*存/ }).click()
  await expect(page.getByText('策略已保存')).toBeVisible({ timeout: 15_000 })
  await expect(row.getByText('已配置')).toBeVisible({ timeout: 15_000 })
  await row.getByRole('button', { name: /策\s*略/ }).click()
  await expect(page.getByLabel('访问点策略 JSON')).toHaveValue(/"Statement"/)
  await page.locator('.ant-modal').getByRole('button', { name: /取\s*消/ }).click()

  // 变换器：自定义 max_response_bytes 必须在重开时回读（I1 回归）
  await row.getByRole('button', { name: '变换器' }).click()
  await page.getByLabel('变换器 endpoint').fill('https://example.com/lambda')
  await page.getByLabel('变换器 max_response_bytes').fill('1048576')
  await page.locator('.ant-modal').getByRole('button', { name: /保\s*存/ }).click()
  await expect(page.getByText('变换器已保存')).toBeVisible({ timeout: 15_000 })
  await row.getByRole('button', { name: '变换器' }).click()
  await expect(page.getByLabel('变换器 max_response_bytes')).toHaveValue('1048576')
})

test('桶清单与默认加密：保存配置并回读', async ({ page }) => {
  await login(page)
  const bucket = await newBucket(page)
  await page.goto('/buckets')

  const row = page.getByRole('row', { name: new RegExp(bucket) })
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(row).toBeVisible({ timeout: 1_000 })
  }).toPass({ timeout: 15_000 })

  // 清单：建一份配置 → 列表出现
  await row.getByRole('button', { name: /清\s*单/ }).click()
  await expect(page.getByText(`清单配置 · ${bucket}`)).toBeVisible()
  await page.getByLabel('配置 ID').fill('daily-e2e')
  await page.getByLabel('目标桶').fill(bucket)
  await page.getByRole('button', { name: '保存配置' }).click()
  await expect(page.getByText('清单配置已保存')).toBeVisible({ timeout: 15_000 })
  await expect(page.locator('.ant-modal').getByText('daily-e2e')).toBeVisible()
  await page.locator('.ant-modal').getByRole('button', { name: /取\s*消/ }).click()

  // 默认加密：AES256 → 保存 → 重新打开回读
  await row.getByRole('button', { name: /加\s*密/ }).click()
  await page.getByLabel('默认加密算法').first().click()
  await page.locator('.ant-select-item-option').filter({ hasText: 'SSE-S3（AES256）' }).click()
  await page.locator('.ant-modal-footer').getByRole('button', { name: /保\s*存/ }).click()
  await expect(page.getByText('默认加密已保存')).toBeVisible({ timeout: 15_000 })

  await row.getByRole('button', { name: /加\s*密/ }).click()
  await expect(page.locator('.ant-modal .ant-select-selection-item')).toContainText('AES256')
})

test('对象标签：上传后添加标签，重开弹窗回读', async ({ page }) => {
  await login(page)
  const content = `tag-${Date.now()}-${randomUUID()}`
  const key = await uploadToFiles(page, 'tag.txt', content)
  const fileName = key.split('/').pop()!

  await page.goto(`/buckets/files?prefix=${encodeURIComponent(key.slice(0, key.lastIndexOf('/') + 1))}`)
  const row = page.getByRole('row', { name: fileName })
  await expect(row).toBeVisible()

  await row.getByRole('button', { name: /标\s*签/ }).click()
  await page.getByRole('button', { name: '添加标签' }).click()
  await page.getByLabel('标签键').fill('env')
  await page.getByLabel('标签值').fill('e2e')
  await page.locator('.ant-modal').getByRole('button', { name: /保\s*存/ }).click()
  await expect(page.getByText('标签已保存')).toBeVisible({ timeout: 15_000 })

  await row.getByRole('button', { name: /标\s*签/ }).click()
  await expect(page.getByLabel('标签键')).toHaveValue('env')
  await expect(page.getByLabel('标签值')).toHaveValue('e2e')
})
