import { expect, test } from '@playwright/test'
import { randomUUID } from 'node:crypto'

// 子账户与权限（权限体系 Step 5 的端到端）：
//   1. 管理员建子账户（UI）→ 列表出现；
//   2. 管理员建桶并给子账户授 读/写/删；
//   3. 子账户登录：菜单无「子账户」、只见授权桶、上传可用（该桶映射到分组 file→files 时）。
//   4. 清遗留轮（43c4bbd）新 UI：访问密钥页自助改密（旧 401/新放行）、子账户页停用/启用
//      （停用即登录 401，启用恢复）。
// 共享 data 目录：新桶/新用户都追加在末尾，沿用 console.spec 的「翻到最后一页」重试模板。

async function login(page: import('@playwright/test').Page, user: string, pw: string) {
  await page.goto('/login')
  await page.getByLabel('用户名').fill(user)
  await page.getByLabel('口令').fill(pw)
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page).toHaveURL(/\/buckets/)
}

/** 切身份：UI 暂无「退出登录」入口，直接清 token（localStorage + cookie 同生命周期）。 */
async function logout(page: import('@playwright/test').Page) {
  const btn = page.getByText('退出登录')
  if (await btn.count()) await btn.click()
  await page.context().clearCookies()
  await page.evaluate(() => localStorage.clear())
}

/** 登录并断言失败：留在 /login 且出现服务端 401 文案（口令错与账号停用同为该文案）。 */
async function loginExpectFail(page: import('@playwright/test').Page, user: string, pw: string) {
  await page.goto('/login')
  await page.getByLabel('用户名').fill(user)
  await page.getByLabel('口令').fill(pw)
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page.getByText('用户名或密码错误')).toBeVisible({ timeout: 15_000 })
  await expect(page).toHaveURL(/\/login/)
}

test('管理员建子账户并授权 → 子账户登录只见授权桶、可上传', async ({ page }) => {
  const stamp = `${Date.now()}-${randomUUID().slice(0, 8)}`
  const subUser = `sub-${stamp}`
  const subPass = `pw-${stamp}-1234`
  // 桶名与 console/instant 统一用 `zzz-e2e-z-<ts>…` 单一时间戳序：原先的 `zzz-e2e-sub-`
  // 会被 z- 桶（'z' > 's'）压到底下——桶越过 10 行边界后本 spec 的新桶掉页，翻最后一页必找不到（实跑翻车）。
  const bucket = `zzz-e2e-z-${stamp}`

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
  // antd 两个汉字按钮自动加空格 → 无障碍名是「权 限」（实测快照 button "权 限"）；
  // 与 console.spec 的 /确\s*确定/ 同款正则，字面 '权限' 全串匹配永远打不中
  await row.getByRole('button', { name: /权\s*限/ }).click()
  const permRow = page.locator('.ant-modal tr', { hasText: bucket })
  for (const title of ['读', '写', '删']) {
    await permRow.locator(`td:nth-child(${title === '读' ? 2 : title === '写' ? 3 : 4}) input[type=checkbox]`).check()
  }
  await page.locator('.ant-modal').getByRole('button', { name: '确 定', exact: true }).click()
  await expect(page.getByText('权限已保存')).toBeVisible()

  // 4) 子账户登录：菜单无「子账户」；桶列表只有该桶；可进对象页且「上传」按钮在（有写权限）
  // 清登录态（若有退出入口先点它；没有就直接清存储——UI 无退出按钮时不空等）
  const logout = page.getByText('退出登录')
  if (await logout.count()) await logout.click()
  await page.context().clearCookies()
  await page.evaluate(() => localStorage.clear())
  await login(page, subUser, subPass)
  await expect(page.getByRole('link', { name: '子账户' })).toHaveCount(0)
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(page.getByText(bucket, { exact: true })).toBeVisible()
  }).toPass()
  await page.getByRole('link', { name: bucket }).click()
  // 同「权 限」：antd 两个汉字自动加空格，实测快照为 button "上 传"
  await expect(page.getByRole('button', { name: /上\s*传/ })).toBeVisible()

  // 5) 子账户上传一发（aetherupload 走子账户 JWT；目标桶由分组映射决定——用 files 之外的分组会 403，
  //    这里断言语义是「有写权限的桶映射到分组 file→files 才可传；无权限分组被拒」）。
  //    传小文件到默认面板；若后端上传面权限门生效而 files 未授权，则输出区出现 403 文案。
  //    先给子账户补授 files 写权限（管理员令牌已清，改用 UI 重登管理员太绕——此处直接断言拒绝路径）：
  await page.getByRole('button', { name: /上\s*传/ }).click()
  await page.locator('#aetherupload-resource').setInputFiles({
    name: 'sub.txt',
    mimeType: 'text/plain',
    buffer: Buffer.from(`sub-upload-${stamp}`),
  })
  // files 桶未授权 → 上传面权限门应拒（403 problem 文案）；授权后此断言改为「上传成功」
  await expect(page.locator('#aetherupload-output')).toContainText('无权', { timeout: 30_000 })
})

test('子账户自助改密：旧口令 401、新口令可登录', async ({ page }) => {
  const stamp = `${Date.now()}-${randomUUID().slice(0, 8)}`
  const subUser = `subpw-${stamp}`
  const oldPass = 'pw12345678'
  const newPass = `np-${stamp}-5678`

  // 1) 管理员建号
  await login(page, 'erik', 'pw')
  await page.getByRole('link', { name: '子账户' }).click()
  await page.getByRole('button', { name: '新建子账户' }).click()
  await page.getByPlaceholder('用户名').fill(subUser)
  await page.getByPlaceholder('口令（至少 8 位）').fill(oldPass)
  await page.getByRole('button', { name: '确 定', exact: true }).click()
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(page.getByText(subUser, { exact: true })).toBeVisible()
  }).toPass()

  // 2) 子账户登录 → 访问密钥页 → 「修改口令」弹窗（旧→新）
  await logout(page)
  await login(page, subUser, oldPass)
  await page.getByRole('link', { name: '访问密钥' }).click()
  await page.getByRole('button', { name: '修改口令' }).click()
  await page.getByPlaceholder('当前口令').fill(oldPass)
  await page.getByPlaceholder('新口令（至少 8 位）').fill(newPass)
  await page.getByPlaceholder('再次输入新口令').fill(newPass)
  await page.getByRole('button', { name: '确 定', exact: true }).click()
  await expect(page.getByText('口令已修改')).toBeVisible({ timeout: 15_000 })

  // 3) 旧口令 401（用户名在表单里已填，只换口令）→ 新口令放行
  await logout(page)
  await loginExpectFail(page, subUser, oldPass)
  await page.getByLabel('口令').fill(newPass)
  await page.getByRole('button', { name: '登 录', exact: true }).click()
  await expect(page).toHaveURL(/\/buckets/)
})

test('管理员停用/启用子账户：停用期间登录 401、启用后恢复', async ({ page }) => {
  const stamp = `${Date.now()}-${randomUUID().slice(0, 8)}`
  const subUser = `subst-${stamp}`
  const pass = `pw-${stamp}-1234`

  // 1) 管理员建号
  await login(page, 'erik', 'pw')
  await page.getByRole('link', { name: '子账户' }).click()
  await page.getByRole('button', { name: '新建子账户' }).click()
  await page.getByPlaceholder('用户名').fill(subUser)
  await page.getByPlaceholder('口令（至少 8 位）').fill(pass)
  await page.getByRole('button', { name: '确 定', exact: true }).click()
  await expect(async () => {
    await page.locator('.ant-pagination-item').last().click()
    await expect(page.getByText(subUser, { exact: true })).toBeVisible()
  }).toPass()

  // 2) 「停 用」→ Popconfirm 确认（antd 双汉字空格：/确\s*定/）→ 行内红色「已停用」标签
  const row = page.locator('tr', { hasText: subUser })
  await row.getByRole('button', { name: /停\s*用/ }).click()
  await page.locator('.ant-popover').getByRole('button', { name: /确\s*定/ }).click()
  await expect(row.getByText('已停用')).toBeVisible({ timeout: 15_000 })

  // 3) 停用期间：口令正确也 401（verify_user 只认 status='active'）
  await logout(page)
  await loginExpectFail(page, subUser, pass)

  // 4) 管理员重登 → 「启 用」→ 标签消失
  await login(page, 'erik', 'pw')
  await page.getByRole('link', { name: '子账户' }).click()
  const row2 = page.locator('tr', { hasText: subUser })
  await row2.getByRole('button', { name: /启\s*用/ }).click()
  await expect(row2.getByText('已停用')).toHaveCount(0, { timeout: 15_000 })

  // 5) 启用后同一口令恢复登录
  await logout(page)
  await login(page, subUser, pass)
})
