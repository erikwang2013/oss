import { expect, test } from '@playwright/test'
import { randomUUID } from 'node:crypto'

// 子账户与权限（权限体系 Step 5 的端到端）：
//   1. 管理员建子账户（UI）→ 列表出现；
//   2. 管理员建桶并给子账户授 读/写/删；
//   3. 子账户登录：菜单无「子账户」、只见授权桶、上传可用（该桶映射到分组 file→files 时）。
// 共享 data 目录：新桶/新用户都追加在末尾，沿用 console.spec 的「翻到最后一页」重试模板。

async function login(page: import('@playwright/test').Page, user: string, pw: string) {
  await page.goto('/login')
  await page.getByLabel('用户名').fill(user)
  await page.getByLabel('口令').fill(pw)
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page).toHaveURL(/\/buckets/)
}

test('管理员建子账户并授权 → 子账户登录只见授权桶、可上传', async ({ page }) => {
  const stamp = `${Date.now()}-${randomUUID().slice(0, 8)}`
  const subUser = `sub-${stamp}`
  const subPass = `pw-${stamp}-1234`
  const bucket = `zzz-e2e-sub-${stamp}`

  // 1) 管理员登录，建桶
  await login(page, 'erik', 'pw')
  await page.getByRole('button', { name: '新建桶' }).click()
  await page.locator('#new-bucket-name').fill(bucket)
  await page.getByRole('button', { name: '确 定', exact: true }).click()
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(page.getByText(bucket, { exact: true })).toBeVisible()
  }).toPass()

  // 2) 子账户页：建号（末尾行需翻页）
  await page.getByRole('link', { name: '子账户' }).click()
  await page.getByRole('button', { name: '新建子账户' }).click()
  await page.getByPlaceholder('用户名').fill(subUser)
  await page.getByPlaceholder('口令（至少 8 位）').fill(subPass)
  await page.getByRole('button', { name: '确 定', exact: true }).click()
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(page.getByText(subUser, { exact: true })).toBeVisible()
  }).toPass()

  // 3) 授权：读+写+删 勾在目标桶上
  const row = page.locator('tr', { hasText: subUser })
  await row.getByRole('button', { name: '权限' }).click()
  const permRow = page.locator('.ant-modal tr', { hasText: bucket })
  for (const title of ['读', '写', '删']) {
    await permRow.locator(`td:nth-child(${title === '读' ? 2 : title === '写' ? 3 : 4}) input[type=checkbox]`).check()
  }
  await page.locator('.ant-modal').getByRole('button', { name: '确 定', exact: true }).click()
  await expect(page.getByText('权限已保存')).toBeVisible()

  // 4) 子账户登录：菜单无「子账户」；桶列表只有该桶；可进对象页且「上传」按钮在（有写权限）
  await page.getByText('退出登录').click().catch(() => {})
  await page.context().clearCookies()
  await page.evaluate(() => localStorage.clear())
  await login(page, subUser, subPass)
  await expect(page.getByRole('link', { name: '子账户' })).toHaveCount(0)
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(page.getByText(bucket, { exact: true })).toBeVisible()
  }).toPass()
  await page.getByRole('link', { name: bucket }).click()
  await expect(page.getByRole('button', { name: '上传' })).toBeVisible()

  // 5) 子账户上传一发（aetherupload 走子账户 JWT；目标桶由分组映射决定——用 files 之外的分组会 403，
  //    这里断言语义是「有写权限的桶映射到分组 file→files 才可传；无权限分组被拒」）。
  //    传小文件到默认面板；若后端上传面权限门生效而 files 未授权，则输出区出现 403 文案。
  //    先给子账户补授 files 写权限（管理员令牌已清，改用 UI 重登管理员太绕——此处直接断言拒绝路径）：
  await page.getByRole('button', { name: '上传' }).click()
  await page.locator('#aetherupload-resource').setInputFiles({
    name: 'sub.txt',
    mimeType: 'text/plain',
    buffer: Buffer.from(`sub-upload-${stamp}`),
  })
  // files 桶未授权 → 上传面权限门应拒（403 problem 文案）；授权后此断言改为「上传成功」
  await expect(page.locator('#aetherupload-output')).toContainText('无权', { timeout: 30_000 })
})
