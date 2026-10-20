import { useState } from 'react'
import { App, Button, Card, Input, InputNumber, Modal, Popconfirm, Select, Space, Table, Tag } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  ApiError,
  api,
  type AccessPointInfo,
  type BucketInfo,
  type TransformInfo,
} from '../api'
import { fmtTime } from '../format'
import PageHeader from '../PageHeader'

// 服务端更严：还要求首尾非 '-'；此正则一并覆盖（3..=63）
const NAME_RE = /^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$/

export default function AccessPoints() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [createOpen, setCreateOpen] = useState(false)
  const [name, setName] = useState('')
  const [bucket, setBucket] = useState<string>()
  const [policyText, setPolicyText] = useState('')
  const [policyAp, setPolicyAp] = useState<AccessPointInfo | null>(null)
  const [policyDraft, setPolicyDraft] = useState('')
  const [transformAp, setTransformAp] = useState<AccessPointInfo | null>(null)

  const { data: aps = [], isLoading } = useQuery({
    queryKey: ['access-points'],
    queryFn: () => api<AccessPointInfo[]>('/api/v1/access-points'),
  })
  const { data: buckets = [] } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<BucketInfo[]>('/api/v1/buckets'),
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['access-points'] })

  const create = useMutation({
    mutationFn: (body: { name: string; bucket: string; policy?: unknown }) =>
      api('/api/v1/access-points', { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => {
      message.success('访问点已创建')
      setCreateOpen(false)
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const savePolicy = useMutation({
    mutationFn: (p: { ap: AccessPointInfo; text: string }) =>
      api(`/api/v1/access-points/${encodeURIComponent(p.ap.name)}/policy`, {
        method: 'PUT',
        body: p.text,
      }),
    onSuccess: () => {
      message.success('策略已保存')
      setPolicyAp(null)
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const remove = useMutation({
    mutationFn: (apName: string) =>
      api(`/api/v1/access-points/${encodeURIComponent(apName)}`, { method: 'DELETE' }),
    onSuccess: () => {
      message.success('已删除')
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const submitCreate = () => {
    if (!NAME_RE.test(name)) {
      message.warning('名称须为 3-63 位小写字母/数字/-，且首尾不是 -')
      return
    }
    if (!bucket) {
      message.warning('请选择桶')
      return
    }
    let policy: unknown = undefined
    if (policyText.trim()) {
      try {
        policy = JSON.parse(policyText)
      } catch {
        message.error('策略不是合法 JSON')
        return
      }
    }
    create.mutate({ name, bucket, policy })
  }

  const openPolicy = (ap: AccessPointInfo) => {
    setPolicyAp(ap)
    setPolicyDraft(ap.policy == null ? '' : JSON.stringify(ap.policy, null, 2))
  }

  const submitPolicy = () => {
    if (!policyAp) return
    const text = policyDraft.trim() || 'null'
    try {
      JSON.parse(text)
    } catch {
      message.error('策略不是合法 JSON')
      return
    }
    savePolicy.mutate({ ap: policyAp, text })
  }

  return (
    <>
      <PageHeader
        title="访问点"
        sub="访问点把桶映射到独立端点（/ap/{name}），可叠加策略与 Object Lambda 变换器"
        extra={
          <Button type="primary" onClick={() => { setName(''); setBucket(undefined); setPolicyText(''); setCreateOpen(true) }}>
            新建访问点
          </Button>
        }
      />
      <Card styles={{ body: { padding: 0 } }}>
        <Table<AccessPointInfo>
          rowKey="name"
          loading={isLoading}
          dataSource={aps}
          columns={[
            { title: '名称', dataIndex: 'name' },
            { title: '桶', dataIndex: 'bucket' },
            { title: 'owner_id', dataIndex: 'owner_id', width: 100 },
            {
              title: '创建时间',
              dataIndex: 'created_at',
              width: 180,
              render: (v: number) => fmtTime(v),
            },
            {
              title: '策略',
              width: 90,
              render: (_: unknown, ap: AccessPointInfo) =>
                ap.policy ? <Tag color="blue">已配置</Tag> : <Tag>无</Tag>,
            },
            {
              title: '操作',
              width: 230,
              render: (_: unknown, ap: AccessPointInfo) => (
                <Space size={8}>
                  <Button size="small" onClick={() => openPolicy(ap)}>策略</Button>
                  <Button size="small" onClick={() => setTransformAp(ap)}>变换器</Button>
                  <Popconfirm title={`删除访问点 ${ap.name}？`} onConfirm={() => remove.mutate(ap.name)}>
                    <Button size="small" danger>删除</Button>
                  </Popconfirm>
                </Space>
              ),
            },
          ]}
        />
      </Card>

      <Modal
        title="新建访问点"
        open={createOpen}
        onCancel={() => setCreateOpen(false)}
        onOk={submitCreate}
        confirmLoading={create.isPending}
      >
        <Input
          value={name}
          onChange={(e) => setName(e.target.value)}
          placeholder="名称（3-63 位小写字母/数字/-）"
          aria-label="访问点名称"
          autoFocus
          style={{ marginBottom: 12 }}
        />
        <Select
          value={bucket}
          onChange={setBucket}
          placeholder="选择桶"
          aria-label="访问点桶"
          style={{ width: '100%', marginBottom: 12 }}
          options={buckets.map((b) => ({ value: b.name, label: b.name }))}
        />
        <Input.TextArea
          value={policyText}
          onChange={(e) => setPolicyText(e.target.value)}
          placeholder="可选：策略 JSON（留空则不配置）"
          aria-label="访问点策略"
          rows={6}
        />
      </Modal>

      <Modal
        title={`策略：${policyAp?.name ?? ''}`}
        open={!!policyAp}
        width={720}
        onCancel={() => setPolicyAp(null)}
        onOk={submitPolicy}
        confirmLoading={savePolicy.isPending}
        okText="保存"
      >
        <Input.TextArea
          value={policyDraft}
          onChange={(e) => setPolicyDraft(e.target.value)}
          rows={14}
          aria-label="访问点策略 JSON"
          placeholder='{"Statement": [...]}；留空保存为 null（清除策略）'
          style={{ fontFamily: 'monospace' }}
        />
      </Modal>

      {transformAp && (
        <TransformModal key={transformAp.name} ap={transformAp} onClose={() => setTransformAp(null)} />
      )}
    </>
  )
}

/** 变换器配置：GET 404 = 未配置；wasm 模块字节不回传，编辑时须重新粘贴。 */
function TransformModal({ ap, onClose }: { ap: AccessPointInfo; onClose: () => void }) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  // 未编辑字段直接展示服务端值，避免 effect 回填
  const [draft, setDraft] = useState<Partial<TransformInfo>>({})
  const [moduleB64, setModuleB64] = useState('')

  const { data, isLoading } = useQuery({
    queryKey: ['ap-transform', ap.name],
    queryFn: async () => {
      try {
        return await api<TransformInfo>(`/api/v1/access-points/${encodeURIComponent(ap.name)}/transform`)
      } catch (e) {
        if (e instanceof ApiError && e.status === 404) return null
        throw e
      }
    },
  })

  const kind = draft.kind ?? data?.kind ?? 'http'
  const endpoint = draft.endpoint ?? data?.endpoint ?? ''
  const timeoutMs = draft.timeout_ms ?? data?.timeout_ms ?? 5000
  const maxBytes = draft.max_response_bytes ?? data?.max_response_bytes ?? 64 * 1024 * 1024

  const save = useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      api(`/api/v1/access-points/${encodeURIComponent(ap.name)}/transform`, {
        method: 'PUT',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      message.success('变换器已保存')
      qc.invalidateQueries({ queryKey: ['ap-transform', ap.name] })
      onClose()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const submit = () => {
    if (timeoutMs < 1000 || timeoutMs > 30000) {
      message.warning('timeout_ms 须在 1000-30000')
      return
    }
    const body: Record<string, unknown> = {
      kind,
      timeout_ms: timeoutMs,
      max_response_bytes: maxBytes,
    }
    if (kind === 'http') {
      if (!endpoint.trim()) {
        message.warning('http 变换器需要 endpoint')
        return
      }
      body.endpoint = endpoint.trim()
    } else {
      if (!moduleB64.trim()) {
        message.warning('wasm 需要粘贴模块 base64（服务端不回传模块字节）')
        return
      }
      body.module_b64 = moduleB64.trim()
    }
    save.mutate(body)
  }

  return (
    <Modal
      title={`变换器：${ap.name}`}
      open
      width={640}
      onCancel={onClose}
      onOk={submit}
      confirmLoading={save.isPending}
      okText="保存"
    >
      <Select
        value={kind}
        onChange={(v) => setDraft((d) => ({ ...d, kind: v }))}
        aria-label="变换器类型"
        style={{ width: '100%', marginBottom: 12 }}
        options={[
          { value: 'http', label: 'HTTP' },
          { value: 'wasm', label: 'WASM' },
        ]}
      />
      {kind === 'http' ? (
        <Input
          value={endpoint}
          onChange={(e) => setDraft((d) => ({ ...d, endpoint: e.target.value }))}
          placeholder="endpoint（http(s)://…）"
          aria-label="变换器 endpoint"
          style={{ marginBottom: 12 }}
        />
      ) : (
        <Input.TextArea
          value={moduleB64}
          onChange={(e) => setModuleB64(e.target.value)}
          placeholder="WASM 模块 base64（≤16 MiB）"
          aria-label="WASM 模块 base64"
          rows={6}
          style={{ marginBottom: 12 }}
        />
      )}
      {data?.module_sha256 && (
        <div style={{ fontSize: 12, color: '#64748b', marginBottom: 12 }}>
          当前模块 sha256：{data.module_sha256}（{data.module_size ?? 0} 字节）
        </div>
      )}
      <Space size={12}>
        <InputNumber
          value={timeoutMs}
          onChange={(v) => setDraft((d) => ({ ...d, timeout_ms: v ?? 5000 }))}
          min={1000}
          max={30000}
          addonBefore="timeout_ms"
          aria-label="变换器 timeout_ms"
        />
        <InputNumber
          value={maxBytes}
          onChange={(v) => setDraft((d) => ({ ...d, max_response_bytes: v ?? 64 * 1024 * 1024 }))}
          min={1}
          addonBefore="max_response_bytes"
          aria-label="变换器 max_response_bytes"
        />
      </Space>
      {isLoading && <div style={{ marginTop: 12, color: '#64748b' }}>加载中…</div>}
    </Modal>
  )
}
