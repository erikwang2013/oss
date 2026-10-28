import { humanSize } from './format'

/** 系统账户（admin/NULL 桶的归属；不可删除、不可挂载成员）。 */
export const SYSTEM_ACCOUNT = '000000000000'

export const accountPath = (id: string) => `/api/v1/accounts/${encodeURIComponent(id)}`

/** 配额展示：null / 负数 = 不限（沿用后端口径）。 */
export function quotaText(q: number | null): string {
  return q == null || q < 0 ? '不限' : humanSize(q)
}
