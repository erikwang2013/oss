import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import {
  Alert,
  App,
  AutoComplete,
  Button,
  Card,
  Divider,
  Input,
  InputNumber,
  Select,
  Space,
  Switch,
  Typography,
} from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  api,
  type LifecycleRule,
  type LifecycleRuleInput,
  type Me,
  type TierInfo,
} from '../api'
import PageHeader from '../PageHeader'

// 与 xl_s3::lifecycle::TRANSITION_CLASSES 对齐
const CLASSES = ['STANDARD_IA', 'ONEZONE_IA', 'INTELLIGENT_TIERING', 'GLACIER', 'GLACIER_IR', 'DEEP_ARCHIVE']
const CLASS_LABEL: Record<string, string> = {
  STANDARD_IA: '标准-低频',
  ONEZONE_IA: '单区低频',
  INTELLIGENT_TIERING: '智能分层',
  GLACIER: '归档',
  GLACIER_IR: '归档-即时取回',
  DEEP_ARCHIVE: '深度归档',
}
const CLASS_OPTIONS = CLASSES.map((c) => ({ value: c, label: `${c} · ${CLASS_LABEL[c]}` }))
const ARCHIVE = new Set(['GLACIER', 'GLACIER_IR', 'DEEP_ARCHIVE'])

type ExpirationKind = 'none' | 'days' | 'date' | 'eodm'
type ThresholdMode = 'days' | 'date'

interface DraftTransition {
  mode: ThresholdMode
  days: number | null
  date: string
  storage_class: string
  tier: string | null
}

interface DraftNoncurrentTransition {
  days: number | null
  newer: number | null
  storage_class: string
  tier: string | null
}

interface DraftRule {
  rule_id: string
  enabled: boolean
  prefix: string
  tags: { key: string; value: string }[]
  size_gt: number | null
  size_lt: number | null
  expiration: ExpirationKind
  expiration_days: number | null
  expiration_date: string
  transitions: DraftTransition[]
  noncurrent: boolean
  noncurrent_days: number | null
  noncurrent_newer: number | null
  noncurrent_transitions: DraftNoncurrentTransition[]
}

/** 服务端规则（新字段优先，旧扁平字段兜底）→ 可编辑草稿。 */
function toDraft(r: LifecycleRule): DraftRule {
  const f = r.filter ?? {}
  const expiration: ExpirationKind = r.expiration_date
    ? 'date'
    : r.expired_object_delete_marker
      ? 'eodm'
      : r.expiration_days != null
        ? 'days'
        : 'none'
  const list = r.transitions?.length
    ? r.transitions
    : r.transition_days != null && r.transition_class
      ? [{ days: r.transition_days, date: null, storage_class: r.transition_class, tier: r.transition_tier }]
      : []
  return {
    rule_id: r.rule_id ?? '',
    enabled: r.enabled,
    prefix: f.prefix ?? r.prefix ?? '',
    tags: (f.tags ?? []).map((t) => ({ ...t })),
    size_gt: f.object_size_greater_than ?? null,
    size_lt: f.object_size_less_than ?? null,
    expiration,
    expiration_days: r.expiration_days,
    expiration_date: r.expiration_date ?? '',
    transitions: list.map((t) =>
      t.date
        ? { mode: 'date' as const, days: null, date: t.date, storage_class: t.storage_class, tier: t.tier ?? null }
        : { mode: 'days' as const, days: t.days ?? null, date: '', storage_class: t.storage_class, tier: t.tier ?? null },
    ),
    noncurrent: r.noncurrent_version_expiration != null,
    noncurrent_days: r.noncurrent_version_expiration?.noncurrent_days ?? 30,
    noncurrent_newer: r.noncurrent_version_expiration?.newer_noncurrent_versions ?? null,
    noncurrent_transitions: (r.noncurrent_version_transitions ?? []).map((t) => ({
      days: t.noncurrent_days,
      newer: t.newer_noncurrent_versions ?? null,
      storage_class: t.storage_class,
      tier: t.tier ?? null,
    })),
  }
}

/** 草稿 → PUT 体；只发结构化字段（不发旧扁平字段，避免两套字段互斥冲突）。 */
function toBody(d: DraftRule): LifecycleRuleInput {
  return {
    rule_id: d.rule_id.trim() || null,
    enabled: d.enabled,
    filter: {
      prefix: d.prefix,
      tags: d.tags.filter((t) => t.key.trim()).map((t) => ({ key: t.key, value: t.value })),
      object_size_greater_than: d.size_gt,
      object_size_less_than: d.size_lt,
    },
    expiration_days: d.expiration === 'days' ? d.expiration_days : null,
    expiration_date: d.expiration === 'date' ? d.expiration_date || null : null,
    expired_object_delete_marker: d.expiration === 'eodm' ? true : undefined,
    transitions: d.transitions.map((t) => ({
      days: t.mode === 'days' ? t.days : null,
      date: t.mode === 'date' ? t.date : null,
      storage_class: t.storage_class,
      tier: t.tier,
    })),
    noncurrent_version_expiration:
      d.noncurrent && d.noncurrent_days != null
        ? { noncurrent_days: d.noncurrent_days, newer_noncurrent_versions: d.noncurrent_newer }
        : null,
    noncurrent_version_transitions: d.noncurrent_transitions.map((t) => ({
      noncurrent_days: t.days ?? 0,
      newer_noncurrent_versions: t.newer,
      storage_class: t.storage_class,
      tier: t.tier,
    })),
  }
}

function blankRule(): DraftRule {
  return {
    rule_id: '',
    enabled: true,
    prefix: '',
    tags: [],
    size_gt: null,
    size_lt: null,
    expiration: 'days',
    expiration_days: 30,
    expiration_date: '',
    transitions: [],
    noncurrent: false,
    noncurrent_days: 30,
    noncurrent_newer: null,
    noncurrent_transitions: [],
  }
}

const EXPIRATION_OPTIONS = [
  { value: 'none', label: '不过期' },
  { value: 'days', label: '按天数（Expiration.Days）' },
  { value: 'date', label: '按日期（Expiration.Date）' },
  { value: 'eodm', label: '删除孤立删除标记（EODM）' },
]

const MODE_OPTIONS = [
  { value: 'days', label: '按天数' },
  { value: 'date', label: '按日期' },
]

/** 桶生命周期：过滤条件 / Expiration / 多 Transition / Noncurrent 版本动作，整表保存。 */
export default function Lifecycle() {
  const { bucket = '' } = useParams()
  const { message } = App.useApp()
  const qc = useQueryClient()
  // draft=null 表示尚未编辑，直接展示服务端值（与 BucketPolicy 同模式）
  const [draft, setDraft] = useState<DraftRule[] | null>(null)
  const [err, setErr] = useState<string | null>(null)

  const { data, isLoading, error } = useQuery({
    queryKey: ['lifecycle', bucket],
    queryFn: () => api<LifecycleRule[]>(`/api/v1/buckets/${encodeURIComponent(bucket)}/lifecycle`),
    enabled: !!bucket,
  })
  const { data: me } = useQuery({ queryKey: ['me'], queryFn: () => api<Me>('/api/v1/me') })
  // tier 列表仅管理员可读；子账户手填 tier id（单 tier 时服务端自动补全）
  const { data: tiers = [] } = useQuery({
    queryKey: ['tiers'],
    queryFn: () => api<TierInfo[]>('/api/v1/tiers'),
    enabled: !!me?.is_admin,
  })
  const tierOptions = tiers.map((t) => ({ value: t.id, label: t.id }))

  const rows = draft ?? (data ?? []).map(toDraft)
  const edit = (next: DraftRule[]) => {
    setDraft(next)
    setErr(null)
  }
  const patch = (i: number, p: Partial<DraftRule>) =>
    edit(rows.map((r, j) => (j === i ? { ...r, ...p } : r)))

  const save = useMutation({
    mutationFn: (rules: LifecycleRuleInput[]) =>
      api(`/api/v1/buckets/${encodeURIComponent(bucket)}/lifecycle`, {
        method: 'PUT',
        body: JSON.stringify(rules),
      }),
    onSuccess: () => {
      message.success('生命周期规则已保存')
      setDraft(null)
      qc.invalidateQueries({ queryKey: ['lifecycle', bucket] })
    },
    onError: (e: Error) => setErr(e.message),
  })

  const submit = () => {
    for (const [i, r] of rows.entries()) {
      if (r.expiration === 'days' && r.expiration_days == null) {
        setErr(`规则 ${i + 1}：请填写 Expiration 天数`)
        return
      }
      if (r.expiration === 'date' && !r.expiration_date) {
        setErr(`规则 ${i + 1}：请选择 Expiration 日期`)
        return
      }
      if (r.transitions.some((t) => (t.mode === 'days' ? t.days == null : !t.date))) {
        setErr(`规则 ${i + 1}：每条 Transition 都要填写天数或日期`)
        return
      }
      if (r.noncurrent_transitions.some((t) => t.days == null)) {
        setErr(`规则 ${i + 1}：NoncurrentVersionTransition 需要 NoncurrentDays`)
        return
      }
    }
    save.mutate(rows.map(toBody))
  }

  return (
    <>
      <PageHeader
        title={`生命周期 · ${bucket}`}
        sub={<Link to={`/buckets/${encodeURIComponent(bucket)}`}>返回对象列表</Link>}
        extra={
          <Space>
            <Button onClick={() => edit([...rows, blankRule()])}>添加规则</Button>
            <Button type="primary" loading={save.isPending} onClick={submit}>
              保存全部
            </Button>
          </Space>
        }
      />
      {error && <Alert type="error" showIcon message={error.message} style={{ marginBottom: 12 }} />}
      {err && (
        <Alert
          type="error"
          showIcon
          message={err}
          style={{ marginBottom: 12 }}
          closable
          onClose={() => setErr(null)}
        />
      )}
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 12 }}
        message="过期/转换语义"
        description={
          <span style={{ fontSize: 12 }}>
            过滤条件中的前缀、标签、大小需全部命中；Expiration.Date 清理该日期（UTC 午夜）前已存在的对象且必须是未来时间；
            EODM 仅在删除标记是对象唯一版本时移除，不能与标签条件同用；Noncurrent 版本以较新版本的创建时间计算年龄，保留数
            （NewerNoncurrentVersions）要求非空过滤条件；归档类（GLACIER/GLACIER_IR/DEEP_ARCHIVE）需选择 tier。
          </span>
        }
      />
      {isLoading && <Typography.Text type="secondary">加载中…</Typography.Text>}
      {!isLoading && rows.length === 0 && (
        <Card>
          <Typography.Text type="secondary">暂无生命周期规则，点击「添加规则」开始配置。</Typography.Text>
        </Card>
      )}
      {rows.map((r, i) => (
        <Card
          key={i}
          style={{ marginBottom: 12 }}
          title={<span>规则 {i + 1}{r.rule_id && ` · ${r.rule_id}`}</span>}
          extra={
            <Space>
              <Typography.Text type="secondary">启用</Typography.Text>
              <Switch checked={r.enabled} onChange={(v) => patch(i, { enabled: v })} />
              <Button danger type="text" onClick={() => edit(rows.filter((_, j) => j !== i))}>
                删除
              </Button>
            </Space>
          }
        >
          <Space direction="vertical" size={10} style={{ width: '100%' }}>
            <Space wrap>
              <Typography.Text type="secondary">规则 ID</Typography.Text>
              <Input
                value={r.rule_id}
                onChange={(e) => patch(i, { rule_id: e.target.value })}
                placeholder="留空自动生成"
                aria-label={`规则 ${i + 1} 的 rule_id`}
                style={{ width: 200 }}
              />
            </Space>

            <Divider orientation="left" plain style={{ margin: 0 }}>
              过滤条件
            </Divider>
            <Space wrap>
              <Input
                value={r.prefix}
                onChange={(e) => patch(i, { prefix: e.target.value })}
                placeholder="前缀（可空 = 全桶）"
                aria-label={`规则 ${i + 1} 前缀`}
                style={{ width: 240 }}
              />
              <InputNumber
                value={r.size_gt}
                onChange={(v) => patch(i, { size_gt: v })}
                min={0}
                placeholder="ObjectSizeGreaterThan"
                aria-label={`规则 ${i + 1} 大小下界`}
                style={{ width: 190 }}
              />
              <InputNumber
                value={r.size_lt}
                onChange={(v) => patch(i, { size_lt: v })}
                min={0}
                placeholder="ObjectSizeLessThan"
                aria-label={`规则 ${i + 1} 大小上界`}
                style={{ width: 190 }}
              />
            </Space>
            {r.tags.map((t, ti) => (
              <Space key={ti} wrap>
                <Input
                  value={t.key}
                  onChange={(e) =>
                    patch(i, { tags: r.tags.map((x, j) => (j === ti ? { ...x, key: e.target.value } : x)) })
                  }
                  placeholder="标签键"
                  aria-label={`规则 ${i + 1} 标签键`}
                  style={{ width: 180 }}
                />
                <Input
                  value={t.value}
                  onChange={(e) =>
                    patch(i, { tags: r.tags.map((x, j) => (j === ti ? { ...x, value: e.target.value } : x)) })
                  }
                  placeholder="标签值"
                  aria-label={`规则 ${i + 1} 标签值`}
                  style={{ width: 180 }}
                />
                <Button type="text" danger onClick={() => patch(i, { tags: r.tags.filter((_, j) => j !== ti) })}>
                  删除
                </Button>
              </Space>
            ))}
            <Button size="small" onClick={() => patch(i, { tags: [...r.tags, { key: '', value: '' }] })}>
              添加标签条件
            </Button>

            <Divider orientation="left" plain style={{ margin: 0 }}>
              当前版本过期
            </Divider>
            <Space wrap>
              <Select
                value={r.expiration}
                onChange={(v) => patch(i, { expiration: v })}
                options={EXPIRATION_OPTIONS}
                aria-label={`规则 ${i + 1} 过期方式`}
                style={{ width: 250 }}
              />
              {r.expiration === 'days' && (
                <InputNumber
                  value={r.expiration_days}
                  onChange={(v) => patch(i, { expiration_days: v })}
                  min={1}
                  max={36500}
                  placeholder="Days"
                  aria-label={`规则 ${i + 1} 过期天数`}
                />
              )}
              {r.expiration === 'date' && (
                <Input
                  type="date"
                  value={r.expiration_date}
                  onChange={(e) => patch(i, { expiration_date: e.target.value })}
                  aria-label={`规则 ${i + 1} 过期日期`}
                  style={{ width: 180 }}
                />
              )}
            </Space>

            <Divider orientation="left" plain style={{ margin: 0 }}>
              存储类转换（Transition）
            </Divider>
            {r.transitions.map((t, ti) => {
              const tpatch = (p: Partial<DraftTransition>) =>
                patch(i, { transitions: r.transitions.map((x, j) => (j === ti ? { ...x, ...p } : x)) })
              return (
                <Space key={ti} wrap>
                  <Select
                    value={t.mode}
                    onChange={(v) => tpatch({ mode: v })}
                    options={MODE_OPTIONS}
                    aria-label={`规则 ${i + 1} 转换 ${ti + 1} 阈值方式`}
                    style={{ width: 110 }}
                  />
                  {t.mode === 'days' ? (
                    <InputNumber
                      value={t.days}
                      onChange={(v) => tpatch({ days: v })}
                      min={1}
                      max={36500}
                      placeholder="Days"
                      aria-label={`规则 ${i + 1} 转换 ${ti + 1} 天数`}
                    />
                  ) : (
                    <Input
                      type="date"
                      value={t.date}
                      onChange={(e) => tpatch({ date: e.target.value })}
                      aria-label={`规则 ${i + 1} 转换 ${ti + 1} 日期`}
                      style={{ width: 170 }}
                    />
                  )}
                  <Select
                    value={t.storage_class}
                    onChange={(v) => tpatch({ storage_class: v })}
                    options={CLASS_OPTIONS}
                    aria-label={`规则 ${i + 1} 转换 ${ti + 1} 存储类`}
                    style={{ width: 200 }}
                  />
                  {ARCHIVE.has(t.storage_class) && (
                    <AutoComplete
                      value={t.tier ?? ''}
                      onChange={(v) => tpatch({ tier: v || null })}
                      options={tierOptions}
                      placeholder="tier（单 tier 可留空）"
                      aria-label={`规则 ${i + 1} 转换 ${ti + 1} tier`}
                      style={{ width: 180 }}
                    />
                  )}
                  <Button
                    type="text"
                    danger
                    onClick={() => patch(i, { transitions: r.transitions.filter((_, j) => j !== ti) })}
                  >
                    删除
                  </Button>
                </Space>
              )
            })}
            <Button
              size="small"
              onClick={() =>
                patch(i, {
                  transitions: [
                    ...r.transitions,
                    { mode: 'days', days: 30, date: '', storage_class: 'STANDARD_IA', tier: null },
                  ],
                })
              }
            >
              添加转换
            </Button>

            <Divider orientation="left" plain style={{ margin: 0 }}>
              非当前版本（Noncurrent）
            </Divider>
            <Space wrap>
              <Switch checked={r.noncurrent} onChange={(v) => patch(i, { noncurrent: v })} />
              <Typography.Text type="secondary">NoncurrentVersionExpiration</Typography.Text>
              {r.noncurrent && (
                <>
                  <InputNumber
                    value={r.noncurrent_days}
                    onChange={(v) => patch(i, { noncurrent_days: v })}
                    min={1}
                    max={36500}
                    placeholder="NoncurrentDays"
                    aria-label={`规则 ${i + 1} 非当前版本天数`}
                  />
                  <InputNumber
                    value={r.noncurrent_newer}
                    onChange={(v) => patch(i, { noncurrent_newer: v })}
                    min={1}
                    max={100}
                    placeholder="保留新版数（可空）"
                    aria-label={`规则 ${i + 1} 保留非当前版本数`}
                  />
                </>
              )}
            </Space>
            {r.noncurrent_transitions.map((t, ti) => {
              const tpatch = (p: Partial<DraftNoncurrentTransition>) =>
                patch(i, {
                  noncurrent_transitions: r.noncurrent_transitions.map((x, j) => (j === ti ? { ...x, ...p } : x)),
                })
              return (
                <Space key={ti} wrap>
                  <InputNumber
                    value={t.days}
                    onChange={(v) => tpatch({ days: v })}
                    min={1}
                    max={36500}
                    placeholder="NoncurrentDays"
                    aria-label={`规则 ${i + 1} 非当前转换 ${ti + 1} 天数`}
                  />
                  <InputNumber
                    value={t.newer}
                    onChange={(v) => tpatch({ newer: v })}
                    min={1}
                    max={100}
                    placeholder="保留新版数（可空）"
                    aria-label={`规则 ${i + 1} 非当前转换 ${ti + 1} 保留数`}
                  />
                  <Select
                    value={t.storage_class}
                    onChange={(v) => tpatch({ storage_class: v })}
                    options={CLASS_OPTIONS}
                    aria-label={`规则 ${i + 1} 非当前转换 ${ti + 1} 存储类`}
                    style={{ width: 200 }}
                  />
                  {ARCHIVE.has(t.storage_class) && (
                    <AutoComplete
                      value={t.tier ?? ''}
                      onChange={(v) => tpatch({ tier: v || null })}
                      options={tierOptions}
                      placeholder="tier（单 tier 可留空）"
                      aria-label={`规则 ${i + 1} 非当前转换 ${ti + 1} tier`}
                      style={{ width: 180 }}
                    />
                  )}
                  <Button
                    type="text"
                    danger
                    onClick={() =>
                      patch(i, {
                        noncurrent_transitions: r.noncurrent_transitions.filter((_, j) => j !== ti),
                      })
                    }
                  >
                    删除
                  </Button>
                </Space>
              )
            })}
            <Button
              size="small"
              onClick={() =>
                patch(i, {
                  noncurrent_transitions: [
                    ...r.noncurrent_transitions,
                    { days: 30, newer: null, storage_class: 'STANDARD_IA', tier: null },
                  ],
                })
              }
            >
              添加非当前版本转换
            </Button>
          </Space>
        </Card>
      ))}
    </>
  )
}
