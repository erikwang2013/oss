import { useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { App, Breadcrumb, Button, Popconfirm, Table, Tooltip, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type ObjectList } from '../api'

function humanSize(n: number) {
  if (n < 1024) return `${n} B`
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`
  if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(1)} MiB`
  return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`
}

// 显式给出行类型：否则 antd Table 会从各列 render 的参数注解反推 RecordType，推出带 string 字面量的错误类型，rowKey 与 render 都编译不过
type Row = { kind: 'dir'; key: string } | { kind: 'obj'; key: string; size: number; etag: string }

export default function Objects() {
  const { bucket = '' } = useParams()
  const { message } = App.useApp()
  const qc = useQueryClient()
  // 面包屑链接带 ?prefix= 整页跳转、组件重挂载，前缀状态即可由此初始化
  const [prefix, setPrefix] = useState(() => new URLSearchParams(window.location.search).get('prefix') ?? '')

  const { data, isLoading } = useQuery({
    queryKey: ['objects', bucket, prefix],
    queryFn: () =>
      api<ObjectList>(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/objects?prefix=${encodeURIComponent(prefix)}&delimiter=/`,
      ),
    enabled: !!bucket,
  })

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
      const resp = await fetch(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeURIComponent(key)}`,
        { headers: { authorization: `Bearer ${localStorage.getItem('oss.token') ?? ''}` } },
      )
      if (!resp.ok) throw new Error(await resp.text())
      const blob = await resp.blob()
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = key.split('/').pop() ?? 'download'
      a.click()
      URL.revokeObjectURL(url)
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
      <Breadcrumb items={crumbs} style={{ marginBottom: 16 }} />
      <Typography.Text type="secondary" style={{ display: 'block', marginBottom: 8 }}>
        上传入口在 Task 9 接；M1 上传面板就绪前可用 `aws s3 cp` / `rclone` 注入对象。
      </Typography.Text>
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
                <a onClick={() => setPrefix(key)}>{key.slice(prefix.length)}</a>
              ) : (
                <Tooltip title={key}>{key.slice(prefix.length)}</Tooltip>
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
                  <Popconfirm title={`删除 ${r.key}？`} onConfirm={() => remove.mutate(r.key)}>
                    <Button size="small" danger>删除</Button>
                  </Popconfirm>
                </>
              ),
          },
        ]}
      />
    </>
  )
}
