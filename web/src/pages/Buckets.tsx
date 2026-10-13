import { useState } from 'react'
import { Link } from 'react-router-dom'
import { App, Button, Card, Input, Modal, Popconfirm, Space, Table, Tag, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type BucketInfo } from '../api'
import PageHeader from '../PageHeader'

export default function Buckets() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')

  const { data: buckets = [], isLoading } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<BucketInfo[]>('/api/v1/buckets'),
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['buckets'] })

  const create = useMutation({
    mutationFn: (name: string) =>
      api('/api/v1/buckets', { method: 'POST', body: JSON.stringify({ name }) }),
    onSuccess: () => { message.success('已创建'); setCreating(false); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const remove = useMutation({
    mutationFn: (name: string) => api(`/api/v1/buckets/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    onSuccess: () => { message.success('已删除'); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  return (
    <>
      <PageHeader
        title="桶与对象"
        sub="点击桶名进入浏览；任何登录用户都可建桶（建桶者即桶主，自动全权）"
        extra={
          // 任何登录用户都可建桶（建桶者即桶主，自动全权）
          <Button type="primary" onClick={() => { setNewName(''); setCreating(true) }}>
            新建桶
          </Button>
        }
      />
      <Card styles={{ body: { padding: 0 } }}>
        <Table<BucketInfo>
          rowKey="name"
          loading={isLoading}
          dataSource={buckets}
          columns={[
            {
              title: '桶名',
              dataIndex: 'name',
              render: (name: string) => <Link to={`/buckets/${encodeURIComponent(name)}`}>{name}</Link>,
            },
            {
              title: '归属',
              width: 100,
              render: (_: unknown, b: BucketInfo) => (b.is_owner ? <Tag color="geekblue">自有</Tag> : <Tag>授权</Tag>),
            },
            {
              title: '权限',
              width: 180,
              render: (_: unknown, b: BucketInfo) => {
                const bits = [
                  b.can_read && <Tag key="r" color="blue">读</Tag>,
                  b.can_write && <Tag key="w" color="green">写</Tag>,
                  b.can_delete && <Tag key="d" color="red">删</Tag>,
                ].filter(Boolean)
                return bits.length ? <Space size={4}>{bits}</Space> : <Typography.Text type="secondary">-</Typography.Text>
              },
            },
            {
              title: '操作',
              width: 120,
              // 删除按钮按 can_delete 显隐（后端另有「桶主或管理员」403 兜底）
              render: (_: unknown, b: BucketInfo) =>
                b.can_delete && (
                  <Popconfirm title={`删除桶 ${b.name}？（须为空桶）`} onConfirm={() => remove.mutate(b.name)}>
                    <Button danger size="small">删除</Button>
                  </Popconfirm>
                ),
            },
          ]}
        />
      </Card>
      <Modal
        title="新建桶"
        open={creating}
        onCancel={() => setCreating(false)}
        onOk={() => {
          if (!newName) { message.warning('请输入桶名'); return }
          create.mutate(newName)
        }}
        confirmLoading={create.isPending}
      >
        <Input
          id="new-bucket-name"
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="3-63 字符，小写字母数字与 - ."
          autoFocus
        />
      </Modal>
    </>
  )
}
