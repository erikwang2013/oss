import { useState } from 'react'
import { Alert, Button, Card, DatePicker, Input, InputNumber, Select, Space, Table, Tag } from 'antd'
import type { Dayjs } from 'dayjs'
import { useQuery } from '@tanstack/react-query'
import { api, type AuditRow } from '../api'
import { fmtTime } from '../format'
import PageHeader from '../PageHeader'

interface Filters {
  from?: number
  to?: number
  uid?: number
  op?: string
  bucket?: string
  key?: string
  decision?: string
  limit: number
}

const EMPTY: Filters = { limit: 100 }

export default function Audit() {
  const [range, setRange] = useState<[Dayjs, Dayjs] | null>(null)
  const [draft, setDraft] = useState<{ uid?: number; op: string; bucket: string; key: string; decision?: string; limit: number }>({
    op: '',
    bucket: '',
    key: '',
    limit: 100,
  })
  const [applied, setApplied] = useState<Filters>(EMPTY)

  const { data: rows = [], isLoading, error } = useQuery({
    queryKey: ['audit', applied],
    queryFn: () => {
      const p = new URLSearchParams()
      if (applied.from != null) p.set('from', String(applied.from))
      if (applied.to != null) p.set('to', String(applied.to))
      if (applied.uid != null) p.set('uid', String(applied.uid))
      if (applied.op) p.set('op', applied.op)
      if (applied.bucket) p.set('bucket', applied.bucket)
      if (applied.key) p.set('key', applied.key)
      if (applied.decision) p.set('decision', applied.decision)
      p.set('limit', String(applied.limit))
      return api<AuditRow[]>(`/api/v1/audit?${p.toString()}`)
    },
  })

  const search = () => {
    setApplied({
      from: range?.[0]?.startOf('day').unix(),
      to: range?.[1]?.endOf('day').unix(),
      uid: draft.uid ?? undefined,
      op: draft.op.trim() || undefined,
      bucket: draft.bucket.trim() || undefined,
      key: draft.key.trim() || undefined,
      decision: draft.decision || undefined,
      limit: draft.limit,
    })
  }

  return (
    <>
      <PageHeader title="审计" sub="access log：时间窗、uid、动作、桶、键前缀与判定过滤" />
      {error && <Alert type="error" showIcon message={error.message} style={{ marginBottom: 12 }} />}
      <Card style={{ marginBottom: 16 }}>
        <Space wrap>
          <DatePicker.RangePicker
            value={range}
            onChange={(v) => setRange(v && v[0] && v[1] ? [v[0], v[1]] : null)}
            aria-label="时间范围"
          />
          <InputNumber
            value={draft.uid}
            onChange={(v) => setDraft((s) => ({ ...s, uid: v ?? undefined }))}
            placeholder="uid"
            aria-label="uid"
            min={0}
            style={{ width: 90 }}
          />
          <Input
            value={draft.op}
            onChange={(e) => setDraft((s) => ({ ...s, op: e.target.value }))}
            placeholder="操作（GetObject）"
            aria-label="操作"
            style={{ width: 160 }}
          />
          <Input
            value={draft.bucket}
            onChange={(e) => setDraft((s) => ({ ...s, bucket: e.target.value }))}
            placeholder="桶"
            aria-label="桶"
            style={{ width: 130 }}
          />
          <Input
            value={draft.key}
            onChange={(e) => setDraft((s) => ({ ...s, key: e.target.value }))}
            placeholder="键前缀"
            aria-label="键前缀"
            style={{ width: 150 }}
          />
          <Select
            value={draft.decision}
            onChange={(v) => setDraft((s) => ({ ...s, decision: v }))}
            placeholder="判定"
            aria-label="判定"
            allowClear
            style={{ width: 110 }}
            options={[
              { value: 'allow', label: 'allow' },
              { value: 'deny', label: 'deny' },
            ]}
          />
          <InputNumber
            value={draft.limit}
            onChange={(v) => setDraft((s) => ({ ...s, limit: v ?? 100 }))}
            min={1}
            max={1000}
            aria-label="条数"
            style={{ width: 90 }}
          />
          <Button type="primary" onClick={search}>查询</Button>
        </Space>
      </Card>
      <Card styles={{ body: { padding: 0 } }}>
        <Table<AuditRow>
          rowKey="id"
          loading={isLoading}
          dataSource={rows}
          columns={[
            { title: 'ID', dataIndex: 'id', width: 70 },
            { title: '时间', dataIndex: 'ts', width: 180, render: (v: number) => fmtTime(v) },
            { title: 'uid', dataIndex: 'uid', width: 70 },
            { title: '操作', dataIndex: 'op', width: 150 },
            { title: '桶', dataIndex: 'bucket', width: 120, render: (v: string | null) => v ?? '—' },
            {
              title: '对象键',
              dataIndex: 'object_key',
              ellipsis: true,
              render: (v: string | null) => v ?? '—',
            },
            {
              title: '判定',
              dataIndex: 'decision',
              width: 90,
              render: (v: string) => (v === 'deny' ? <Tag color="red">deny</Tag> : <Tag color="green">allow</Tag>),
            },
            { title: '详情', dataIndex: 'detail', ellipsis: true, render: (v: string | null) => v ?? '—' },
          ]}
        />
      </Card>
    </>
  )
}
