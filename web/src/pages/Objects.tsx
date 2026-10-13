import { useMemo, useState } from 'react'
import { Link, useParams, useSearchParams } from 'react-router-dom'
import { App, Breadcrumb, Button, Card, Popconfirm, Space, Table, Tooltip } from 'antd'
import { FileOutlined, FolderOutlined, ReloadOutlined } from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, rawApi, type BucketInfo, type ObjectList } from '../api'
import { humanSize } from '../format'
import PageHeader from '../PageHeader'
import UploadPanel from '../upload/UploadPanel'

// 显式给出行类型：否则 antd Table 会从各列 render 的参数注解反推 RecordType，推出带 string 字面量的错误类型，rowKey 与 render 都编译不过
type Row = { kind: 'dir'; key: string } | { kind: 'obj'; key: string; size: number; etag: string }

export default function Objects() {
  const { bucket = '' } = useParams()
  const { message } = App.useApp()
  const qc = useQueryClient()
  // prefix 的单一事实源是 URL query：Link 是 SPA 导航、同路由仅 query 变化不重挂载，useState 快照会让面包屑/手改 URL/返回键都失效
  const [sp, setSp] = useSearchParams()
  const prefix = sp.get('prefix') ?? ''
  const [uploading, setUploading] = useState(false)

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

  const rows = [
    ...(data?.common_prefixes ?? []).map((p) => ({ kind: 'dir' as const, key: p })),
    ...(data?.objects ?? []).map((o) => ({ kind: 'obj' as const, key: o.key, size: o.size, etag: o.etag })),
  ]

  return (
    <>
      <Breadcrumb items={crumbs} style={{ marginBottom: 12 }} />
      <PageHeader
        title={bucket}
        sub={prefix ? `前缀 ${prefix}` : '对象浏览'}
        extra={
          <>
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
              width: 120,
              render: (_: unknown, r: { kind: string; size?: number }) => (r.kind === 'dir' ? '-' : humanSize(r.size ?? 0)),
            },
            { title: 'ETag', dataIndex: 'etag', width: 320, render: (v?: string) => v ?? '-' },
            {
              title: '操作',
              width: 160,
              render: (_: unknown, r: { kind: 'dir' | 'obj'; key: string }) =>
                r.kind === 'obj' && (
                  <>
                    <Button size="small" onClick={() => download(r.key)} style={{ marginRight: 8 }}>下载</Button>
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
    </>
  )
}
