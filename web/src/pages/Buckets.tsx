import { useState } from 'react'
import { Link } from 'react-router-dom'
import { App, Button, Input, Modal, Popconfirm, Table } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api } from '../api'

export default function Buckets() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')

  const { data: buckets = [], isLoading } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<string[]>('/api/v1/buckets'),
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
      <Button type="primary" onClick={() => { setNewName(''); setCreating(true) }} style={{ marginBottom: 16 }}>
        新建桶
      </Button>
      <Table
        rowKey={(r) => r}
        loading={isLoading}
        dataSource={buckets}
        columns={[
          // 不用 dataIndex：antd 对 string[] 记录取 dataIndex:0 会拿到首字符（"alpha"→"a"）；无 dataIndex 时 render 第一参即整条记录
          { title: '桶名', render: (name: string) => <Link to={`/buckets/${encodeURIComponent(name)}`}>{name}</Link> },
          {
            title: '操作',
            width: 120,
            render: (_: unknown, name: string) => (
              <Popconfirm title={`删除桶 ${name}？（须为空桶）`} onConfirm={() => remove.mutate(name)}>
                <Button danger size="small">删除</Button>
              </Popconfirm>
            ),
          },
        ]}
      />
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
