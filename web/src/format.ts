export function humanSize(n: number): string {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`
}

/** unix 秒 → 本地时间（控制台所有时间列共用）。 */
export function fmtTime(ts?: number | null): string {
  return ts ? new Date(ts * 1000).toLocaleString('zh-CN', { hour12: false }) : '-'
}
