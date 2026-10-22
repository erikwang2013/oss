import { useMemo, useState } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { App, Breadcrumb, Button, Card, Input, Modal, Popconfirm, Space, Table, Tag, Tooltip } from 'antd'
import { FileOutlined, FolderOutlined, ReloadOutlined } from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, rawApi, type BucketInfo, type ObjectList, type ObjectTag } from '../api'
import { fmtTime, humanSize } from '../format'
import InventoryModal from '../InventoryModal'
import PageHeader from '../PageHeader'
import UploadPanel from '../upload/UploadPanel'

// 显式给出行类型：否则 antd Table 会从各列 render 的参数注解反推 RecordType，推出带 string 字面量的错误类型，rowKey 与 render 都编译不过
type Row =
  | { kind: 'dir'; key: string }
  | {
      kind: 'obj'
      key: string
      size: number
      etag: string
      storage_class?: string | null
      tier_phase?: string | null
      restore_expires_at?: number | null
      lock_mode?: string | null
      lock_retain_until?: number | null
      legal_hold?: boolean
    }

export default function Objects() {
  const { bucket = '' } = useParams()
  const { message } = App.useApp()
  const qc = useQueryClient()
  // prefix 的单一事实源是 URL query：Link 是 SPA 导航、同路由仅 query 变化不重挂载，useState 快照会让面包屑/手改 URL/返回键都失效
  const [sp, setSp] = useSearchParams()
  const prefix = sp.get('prefix') ?? ''
  const [uploading, setUploading] = useState(false)
  const [inventoryOpen, setInventoryOpen] = useState(false)
  const [tagsKey, setTagsKey] = useState<string | null>(null)

  const { data, isLoading, isFetching, refetch } = useQuery({
    queryKey: ['objects', bucket, prefix],
    queryFn: () =>
      api<ObjectList>(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/objects?prefix=${encodeURIComponent(prefix)}&delimiter=/`,
      ),
    enabled: !!bucket,
  })

  // 能力位（与 Buckets 页共用同一缓存）：按位显隐上传/删除；后端另有 403 兜底
  const { data: buckets = [] } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<BucketInfo[]>('/api/v1/buckets'),
  })
  const ability = buckets.find((b) => b.name === bucket)
  const canWrite = ability?.can_write ?? false
  const canDelete = ability?.can_delete ?? false

  const invalidate = () => qc.invalidateQueries({ queryKey: ['objects', bucket] })

  // 对象键一律整键 encodeURIComponent（删除/下载同款）：axum 的 {*key} 通配段先按路径匹配再解码，%2F 不会被拆段，含空格/#/? 的键才能走对
  const remove = useMutation({
    mutationFn: (key: string) =>
      api(`/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`, { method: 'DELETE' }),
    onSuccess: () => { message.success('已删除'); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const download = async (key: string) => {
    try {
      const resp = await rawApi(`/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`)
      const blob = await resp.blob() // M1 大对象全量进内存的上限，M2 流式
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = key.split('/').pop() || 'download'
      a.click()
      setTimeout(() => URL.revokeObjectURL(url), 0)
    } catch (e) {
      message.error(e instanceof Error ? e.message : '下载失败')
    }
  }

  const crumbs = useMemo(() => {
    const parts = prefix.split('/').filter(Boolean)
    return [{ title: <Link to={`/buckets/${encodeURIComponent(bucket)}`}>{bucket}</Link> }].concat(
      parts.map((p, i) => ({
        title: (
          <Link to={`/buckets/${encodeURIComponent(bucket)}?prefix=${encodeURIComponent(parts.slice(0, i + 1).join('/') + '/')}`}>
            {p}
          </Link>
        ),
      })),
    )
  }, [bucket, prefix])

  const rows: Row[] = [
    ...(data?.common_prefixes ?? []).map((p) => ({ kind: 'dir' as const, key: p })),
    ...(data?.objects ?? []).map((o) => ({
      kind: 'obj' as const,
      key: o.key,
      size: o.size,
      etag: o.etag,
      storage_class: o.storage_class,
      tier_phase: o.tier_phase,
      restore_expires_at: o.restore_expires_at,
      lock_mode: o.lock_mode,
      lock_retain_until: o.lock_retain_until,
      legal_hold: o.legal_hold,
    })),
  ]

  return (
    <>
      <Breadcrumb items={crumbs} style={{ marginBottom: 12 }} />
      <PageHeader
        title={bucket}
        sub={prefix ? `前缀 ${prefix}` : '对象浏览'}
        extra={
          <>
            <Link to={`/buckets/${encodeURIComponent(bucket)}/policy`}>
              <Button>桶策略</Button>
            </Link>
            <Link to={`/buckets/${encodeURIComponent(bucket)}/select`}>
              <Button>Select 查询</Button>
            </Link>
            <Button onClick={() => setInventoryOpen(true)}>清单配置</Button>
            <Button icon={<ReloadOutlined />} onClick={() => refetch()} loading={isFetching} aria-label="刷新" />
            {canWrite && (
              <Button type="primary" onClick={() => setUploading(true)}>上传</Button>
            )}
          </>
        }
      />
      <Card styles={{ body: { padding: 0 } }}>
        <Table<Row>
          rowKey={(r) => r.key}
          loading={isLoading}
          dataSource={rows}
          pagination={false}
          columns={[
            {
              title: '名称',
              dataIndex: 'key',
              render: (key: string, r: { kind: 'dir' | 'obj' }) =>
                r.kind === 'dir' ? (
                  <Button type="link" size="small" style={{ paddingInline: 0 }} onClick={() => setSp(key ? { prefix: key } : {})}>
                    <FolderOutlined style={{ color: '#f59e0b', marginRight: 6 }} />
                    {key.slice(prefix.length)}
                  </Button>
                ) : (
                  <Space size={6}>
                    <FileOutlined style={{ color: '#94a3b8' }} />
                    <Tooltip title={key}>{key.slice(prefix.length)}</Tooltip>
                  </Space>
                ),
            },
            {
              title: '大小',
              width: 110,
              render: (_: unknown, r: { kind: string; size?: number }) => (r.kind === 'dir' ? '-' : humanSize(r.size ?? 0)),
            },
            {
              title: '存储类',
              width: 120,
              render: (_: unknown, r: { kind: string; storage_class?: string | null }) =>
                r.kind === 'dir' ? '-' : (r.storage_class ?? '-'),
            },
            {
              // tier_phase 直接来自控制台列表（无需逐对象 HEAD）；null = 未归档
              title: '恢复状态',
              width: 200,
              render: (_: unknown, r: { kind: string; tier_phase?: string | null; restore_expires_at?: number | null }) => {
                if (r.kind === 'dir' || !r.tier_phase) return '-'
                const label =
                  { Archived: '已归档', Restoring: '恢复中', Restored: '已恢复' }[r.tier_phase] ?? r.tier_phase
                const color = r.tier_phase === 'Restored' ? 'green' : r.tier_phase === 'Restoring' ? 'processing' : 'default'
                return (
                  <Tag color={color}>
                    {label}
                    {r.tier_phase === 'Restored' && r.restore_expires_at
                      ? `（至 ${fmtTime(r.restore_expires_at)}）`
                      : ''}
                  </Tag>
                )
              },
            },
            {
              // M5：锁状态直接来自控制台列表（无需逐对象 HEAD）；retention 与 legal hold 独立
              title: '对象锁',
              width: 210,
              render: (
                _: unknown,
                r: { kind: string; lock_mode?: string | null; lock_retain_until?: number | null; legal_hold?: boolean },
              ) => {
                if (r.kind === 'dir') return '-'
                if (!r.lock_mode && !r.legal_hold) return '-'
                return (
                  <Space size={4}>
                    {r.lock_mode && (
                      <Tag color={r.lock_mode === 'COMPLIANCE' ? 'red' : 'blue'}>
                        {r.lock_mode}
                        {r.lock_retain_until ? `（至 ${fmtTime(r.lock_retain_until)}）` : ''}
                      </Tag>
                    )}
                    {r.legal_hold && <Tag color="orange">Legal Hold</Tag>}
                  </Space>
                )
              },
            },
            { title: 'ETag', dataIndex: 'etag', width: 280, render: (v?: string) => v ?? '-' },
            {
              title: '操作',
              width: 220,
              render: (_: unknown, r: { kind: 'dir' | 'obj'; key: string }) =>
                r.kind === 'obj' && (
                  <>
                    <Button size="small" onClick={() => download(r.key)} style={{ marginRight: 8 }}>下载</Button>
                    <Button size="small" onClick={() => setTagsKey(r.key)} style={{ marginRight: 8 }}>标签</Button>
                    {canDelete && (
                      <Popconfirm title={`删除 ${r.key}？`} onConfirm={() => remove.mutate(r.key)}>
                        <Button size="small" danger>删除</Button>
                      </Popconfirm>
                    )}
                  </>
                ),
            },
          ]}
        />
      </Card>
      <UploadPanel open={uploading} bucket={bucket} onClose={() => setUploading(false)} />
      {inventoryOpen && <InventoryModal bucket={bucket} onClose={() => setInventoryOpen(false)} />}
      {tagsKey && (
        <TagsModal
          bucket={bucket}
          objectKey={tagsKey}
          canWrite={canWrite}
          onClose={() => setTagsKey(null)}
        />
      )}
    </>
  )
}

/** 对象标签（链尾版本）：读需 Read、写/清需 Write；键唯一且非空，上限 10。 */
function TagsModal({
  bucket,
  objectKey,
  canWrite,
  onClose,
}: {
  bucket: string
  objectKey: string
  canWrite: boolean
  onClose: () => void
}) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  // 未编辑时直接展示服务端值，避免 effect 回填
  const [draft, setDraft] = useState<ObjectTag[] | null>(null)

  const { data, isLoading } = useQuery({
    queryKey: ['object-tags', bucket, objectKey],
    queryFn: () =>
      api<{ tags: ObjectTag[] }>(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/tags/${encodeURIComponent(objectKey)}`,
      ),
  })

  const rows = draft ?? data?.tags ?? []

  const path = `/api/v1/buckets/${encodeURIComponent(bucket)}/tags/${encodeURIComponent(objectKey)}`
  const invalidate = () => qc.invalidateQueries({ queryKey: ['object-tags', bucket, objectKey] })

  const save = useMutation({
    mutationFn: (tags: ObjectTag[]) => api(path, { method: 'PUT', body: JSON.stringify({ tags }) }),
    onSuccess: () => {
      message.success('标签已保存')
      invalidate()
      onClose()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const clear = useMutation({
    mutationFn: () => api(path, { method: 'DELETE' }),
    onSuccess: () => {
      message.success('标签已清空')
      setDraft([])
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const submit = () => {
    const tags = rows.filter((r) => r.key.trim() || r.value.trim())
    if (tags.some((r) => !r.key.trim())) {
      message.warning('标签键不能为空')
      return
    }
    if (tags.length > 10) {
      message.warning('每个对象最多 10 个标签')
      return
    }
    save.mutate(tags.map((r) => ({ key: r.key.trim(), value: r.value })))
  }

  return (
    <Modal
      title={`对象标签 · ${objectKey}`}
      open
      width={640}
      onCancel={onClose}
      onOk={submit}
      confirmLoading={save.isPending}
      okText="保存"
      okButtonProps={{ disabled: !canWrite }}
      footer={(_, { OkBtn, CancelBtn }) => (
        <>
          <Popconfirm title="清空该对象的全部标签？" onConfirm={() => clear.mutate()} disabled={!canWrite || isLoading}>
            <Button danger disabled={!canWrite || isLoading} style={{ float: 'left' }}>
              清空全部
            </Button>
          </Popconfirm>
          <CancelBtn />
          <OkBtn />
        </>
      )}
    >
      {!canWrite && (
        <Tag color="orange" style={{ marginBottom: 12 }}>
          当前账号对该桶没有写权限，只能查看
        </Tag>
      )}
      {rows.map((r, i) => (
        <Space key={i} align="baseline" style={{ display: 'flex', marginBottom: 8 }}>
          <Input
            value={r.key}
            onChange={(e) => setDraft(rows.map((x, j) => (j === i ? { ...x, key: e.target.value } : x)))}
            placeholder="键"
            aria-label="标签键"
            style={{ width: 220 }}
          />
          <Input
            value={r.value}
            onChange={(e) => setDraft(rows.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))}
            placeholder="值"
            aria-label="标签值"
            style={{ width: 260 }}
          />
          <Button size="small" disabled={!canWrite} onClick={() => setDraft(rows.filter((_, j) => j !== i))}>
            移除
          </Button>
        </Space>
      ))}
      <Button
        size="small"
        disabled={!canWrite || isLoading || rows.length >= 10}
        onClick={() => setDraft([...rows, { key: '', value: '' }])}
      >
        添加标签
      </Button>
      {isLoading && <div style={{ marginTop: 8, color: '#64748b' }}>加载中…</div>}
    </Modal>
  )
}
