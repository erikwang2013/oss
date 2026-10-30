import { describe, expect, it } from 'vitest'
import { mergeObjectPages, type ObjectList } from '../api'

const page = (
  objects: string[],
  common_prefixes: string[],
  is_truncated = false,
  next_cursor: string | null = null,
): ObjectList => ({
  objects: objects.map((key) => ({ key, size: 1, etag: 'etag', content_type: null })),
  common_prefixes,
  is_truncated,
  next_token: next_cursor,
  next_cursor,
})

describe('mergeObjectPages', () => {
  it('按页序拼接并去重续页边界重发的 common prefix', () => {
    const merged = mergeObjectPages([
      page(['a.txt'], [], true, 'docs/'),
      page([], ['docs/'], true, 'z.txt'),
      page(['z.txt'], [], false, null),
    ])
    expect(merged.common_prefixes).toEqual(['docs/'])
    expect(merged.objects.map((o) => o.key)).toEqual(['a.txt', 'z.txt'])
  })

  it('同键对象只保留首次出现的一页', () => {
    const merged = mergeObjectPages([page(['k'], [], true, 'k'), page(['k', 'k2'], [], false, null)])
    expect(merged.objects.map((o) => o.key)).toEqual(['k', 'k2'])
  })

  it('无页时返回空列表', () => {
    expect(mergeObjectPages([])).toEqual({ common_prefixes: [], objects: [] })
  })
})
