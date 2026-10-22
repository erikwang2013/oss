import { useState } from 'react'
import { Alert, App, Button, Card, Input, Popconfirm, Select, Space, Table, Tag } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  api,
  type BucketInfo,
  type DeadEvent,
  type Me,
  type NotificationConfig,
  type NotificationQueue,
} from '../api'
import { fmtTime } from '../format'
import PageHeader from '../PageHeader'

// 与 xl_s3::notify::ALLOWED_EVENTS 对齐（M8 从 8 个扩到 11 个：Transition/Restore；
// 读路径补全增 ObjectAccessed 的 Get/Head 与尾星通配）
const EVENTS = [
  's3:ObjectCreated:Put',
  's3:ObjectCreated:Post',
  's3:ObjectCreated:Copy',
  's3:ObjectCreated:CompleteMultipartUpload',
  's3:ObjectRemoved:Delete',
  's3:ObjectRemoved:DeleteMarkerCreated',
  's3:LifecycleExpiration:Delete',
  's3:LifecycleExpiration:DeleteMarkerCreated',
  's3:LifecycleTransition',
  's3:ObjectRestore:Post',
  's3:ObjectRestore:Completed',
  's3:ObjectAccessed:Get',
  's3:ObjectAccessed:Head',
  's3:ObjectAccessed:*',
]

interface TargetRow {
  key: string
  id: string
  target: string
  events: string[]
  prefix: string
  suffix: string
}

function parseArn(arn?: string): string {
  return arn?.match(/^arn:oss:sqs::([^:]+):kafka$/)?.[1] ?? ''
}

function toRows(cfg?: NotificationConfig): TargetRow[] {
  return (cfg?.queue_configurations ?? []).map((q, i) => {
    const rules = q.filter?.key?.filter_rules ?? []
    return {
      key: `${i}-${q.id ?? ''}`,
      id: q.id ?? `cfg-${i}`,
      target: parseArn(q.queue_arn),
      events: q.events ?? [],
      prefix: rules.find((r) => r.name === 'prefix')?.value ?? '',
      suffix: rules.find((r) => r.name === 'suffix')?.value ?? '',
    }
  })
}

export default function Events() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [bucket, setBucket] = useState<string>()
  // 编辑中的行绑定到桶；切桶后自动回落到服务端值，避免把旧桶配置存到新桶
  const [draft, setDraft] = useState<{ bucket?: string; rows: TargetRow[] } | null>(null)

  const { data: buckets = [] } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<BucketInfo[]>('/api/v1/buckets'),
  })
  const { data: me } = useQuery({ queryKey: ['me'], queryFn: () => api<Me>('/api/v1/me') })

  // 未手动选择时默认第一个可见桶；桶列表 refetch 不覆盖用户选择
  const effBucket = bucket ?? buckets[0]?.name

  const { data, error, isLoading } = useQuery({
    queryKey: ['notifications', effBucket],
    queryFn: () =>
      api<NotificationConfig>(`/api/v1/buckets/${encodeURIComponent(effBucket!)}/notifications`),
    enabled: !!effBucket,
  })

  const rows = draft && draft.bucket === effBucket ? draft.rows : toRows(data)

  const save = useMutation({
    mutationFn: (cfg: NotificationConfig) =>
      api(`/api/v1/buckets/${encodeURIComponent(effBucket!)}/notifications`, {
        method: 'PUT',
        body: JSON.stringify(cfg),
      }),
    onSuccess: () => {
      message.success('通知配置已保存')
      setDraft(null)
      qc.invalidateQueries({ queryKey: ['notifications', effBucket] })
    },
    onError: (e: Error) => message.error(e.message),
  })

  const clear = useMutation({
    mutationFn: () =>
      api(`/api/v1/buckets/${encodeURIComponent(effBucket!)}/notifications`, { method: 'DELETE' }),
    onSuccess: () => {
      message.success('通知配置已清空')
      setDraft(null)
      qc.invalidateQueries({ queryKey: ['notifications', effBucket] })
    },
    onError: (e: Error) => message.error(e.message),
  })

  const submit = () => {
    for (const r of rows) {
      if (!r.target.trim()) {
        message.warning('每个目标的 ARN 都需要 target id')
        return
      }
      if (r.events.length === 0) {
        message.warning('每个目标至少选择一个事件')
        return
      }
    }
    const queues: NotificationQueue[] = rows.map((r, i) => {
      const rules = [
        ...(r.prefix ? [{ name: 'prefix', value: r.prefix }] : []),
        ...(r.suffix ? [{ name: 'suffix', value: r.suffix }] : []),
      ]
      return {
        id: r.id || `cfg-${i}`,
        queue_arn: `arn:oss:sqs::${r.target.trim()}:kafka`,
        events: r.events,
        ...(rules.length ? { filter: { key: { filter_rules: rules } } } : {}),
      }
    })
    save.mutate({ queue_configurations: queues })
  }

  const patch = (key: string, p: Partial<TargetRow>) =>
    setDraft({ bucket: effBucket, rows: rows.map((r) => (r.key === key ? { ...r, ...p } : r)) })

  return (
    <>
      <PageHeader
        title="事件通知"
        sub="桶事件推送到 Kafka target（arn:oss:sqs::<id>:kafka）；全量替换语义"
        extra={
          <>
            <Select
              value={effBucket}
              onChange={setBucket}
              placeholder="选择桶"
              aria-label="通知桶"
              showSearch
              optionFilterProp="label"
              style={{ width: 200 }}
              options={buckets.map((b) => ({ value: b.name, label: b.name }))}
            />
            <Button
              disabled={!effBucket || isLoading}
              onClick={() =>
                setDraft({
                  bucket: effBucket,
                  rows: [
                    ...rows,
                    { key: String(Date.now()), id: `cfg-${rows.length}`, target: '', events: [], prefix: '', suffix: '' },
                  ],
                })
              }
            >
              添加目标
            </Button>
            <Button type="primary" loading={save.isPending} onClick={submit} disabled={!effBucket || isLoading}>
              保存
            </Button>
            <Popconfirm title="清空该桶的全部通知配置？" onConfirm={() => clear.mutate()}>
              <Button danger disabled={!effBucket || isLoading}>清空</Button>
            </Popconfirm>
          </>
        }
      />
      {error && <Alert type="error" showIcon message={error.message} style={{ marginBottom: 12 }} />}
      <Card styles={{ body: { padding: 0 } }}>
        <Table<TargetRow>
          rowKey="key"
          loading={isLoading}
          dataSource={rows}
          pagination={false}
          columns={[
            {
              title: 'ID',
              width: 130,
              render: (_: unknown, r: TargetRow) => (
                <Input value={r.id} onChange={(e) => patch(r.key, { id: e.target.value })} aria-label="通知配置 ID" />
              ),
            },
            {
              title: 'ARN',
              render: (_: unknown, r: TargetRow) => (
                <Input
                  value={r.target}
                  onChange={(e) => patch(r.key, { target: e.target.value })}
                  addonBefore="arn:oss:sqs::"
                  addonAfter=":kafka"
                  placeholder="target"
                  aria-label="Kafka target"
                />
              ),
            },
            {
              title: '事件',
              width: 280,
              render: (_: unknown, r: TargetRow) => (
                <Select
                  mode="multiple"
                  value={r.events}
                  onChange={(v) => patch(r.key, { events: v })}
                  aria-label="通知事件"
                  style={{ width: '100%' }}
                  options={EVENTS.map((e) => ({ value: e, label: e.replace(/^s3:/, '') }))}
                />
              ),
            },
            {
              title: '前缀',
              width: 120,
              render: (_: unknown, r: TargetRow) => (
                <Input value={r.prefix} onChange={(e) => patch(r.key, { prefix: e.target.value })} aria-label="通知前缀" />
              ),
            },
            {
              title: '后缀',
              width: 120,
              render: (_: unknown, r: TargetRow) => (
                <Input value={r.suffix} onChange={(e) => patch(r.key, { suffix: e.target.value })} aria-label="通知后缀" />
              ),
            },
            {
              title: '',
              width: 70,
              render: (_: unknown, r: TargetRow) => (
                <Button size="small" danger onClick={() => setDraft({ bucket: effBucket, rows: rows.filter((x) => x.key !== r.key) })}>
                  移除
                </Button>
              ),
            },
          ]}
        />
      </Card>

      {me?.is_admin && <DeadLetters />}
    </>
  )
}

/** 死信（管理员）：state='dead' 的事件；重投复位 attempts/last_error 后交给原 worker。 */
function DeadLetters() {
  const { message } = App.useApp()
  const qc = useQueryClient()

  const { data: dead = [], isLoading, error } = useQuery({
    queryKey: ['dead-events'],
    queryFn: () => api<DeadEvent[]>('/api/v1/events/dead?limit=100'),
  })

  const redeliver = useMutation({
    mutationFn: (id: number) => api(`/api/v1/events/${id}/redeliver`, { method: 'POST' }),
    onSuccess: () => {
      message.success('已重新入队')
      qc.invalidateQueries({ queryKey: ['dead-events'] })
    },
    onError: (e: Error) => message.error(e.message),
  })

  return (
    <>
      <PageHeader title="死信事件" sub="投递失败达到上限的事件；重投不删除原行" />
      {error && <Alert type="error" showIcon message={error.message} style={{ marginBottom: 12 }} />}
      <Card styles={{ body: { padding: 0 } }}>
        <Table<DeadEvent>
          rowKey="id"
          loading={isLoading}
          dataSource={dead}
          columns={[
            { title: 'ID', dataIndex: 'id', width: 70 },
            { title: '桶', dataIndex: 'bucket', width: 120 },
            { title: '对象键', dataIndex: 'object_key', ellipsis: true, render: (v: string | null) => v ?? '—' },
            {
              title: '事件',
              dataIndex: 'event',
              width: 220,
              render: (v: string | null) => (v ? <Tag>{v}</Tag> : '—'),
            },
            { title: '尝试', dataIndex: 'attempts', width: 70 },
            { title: '最后错误', dataIndex: 'last_error', ellipsis: true, render: (v: string | null) => v ?? '—' },
            { title: '时间', dataIndex: 'created_at', width: 180, render: (v: number) => fmtTime(v) },
            {
              title: '操作',
              width: 90,
              render: (_: unknown, d: DeadEvent) => (
                <Space>
                  <Button size="small" onClick={() => redeliver.mutate(d.id)}>重投</Button>
                </Space>
              ),
            },
          ]}
        />
      </Card>
    </>
  )
}
