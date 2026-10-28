import { useState } from 'react'
import { Alert, App, Button, Card, Input, Modal, Popconfirm, Space, Table, Tag, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type AccountRow, type Me } from '../api'
import { MembersModal, QuotaModal, UsageModal } from '../AccountModals'
import { SYSTEM_ACCOUNT, accountPath, quotaText } from '../accounts'
import PageHeader from '../PageHeader'

export default function Accounts() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [renaming, setRenaming] = useState<{ id: string; name: string } | null>(null)
  const [quotaFor, setQuotaFor] = useState<AccountRow | null>(null)
  const [membersFor, setMembersFor] = useState<AccountRow | null>(null)
  const [usageFor, setUsageFor] = useState<AccountRow | null>(null)

  // GET /accounts 对成员返回其名下账户（不是 403），所以页面门禁必须显式判角色
  const { data: me, isLoading: meLoading } = useQuery({
    queryKey: ['me'],
    queryFn: () => api<Me>('/api/v1/me'),
  })
  const isAdmin = me?.is_admin === true
  // 写后 invalidate 触发本查询重取：带 consistent=1，避免 follower 上刚写完读到旧列表
  const { data: accounts = [], isLoading, error } = useQuery({
    queryKey: ['accounts'],
    queryFn: () => api<AccountRow[]>('/api/v1/accounts', {}, true),
    enabled: isAdmin,
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['accounts'] })

  const create = useMutation({
    mutationFn: (name: string) =>
      api<{ id: string }>('/api/v1/accounts', { method: 'POST', body: JSON.stringify({ name }) }),
    onSuccess: (r) => {
      message.success(`已创建账户 ${r.id}`)
      setCreating(false)
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  // 改名与启停共用 PUT /accounts/{id}；后端拒绝（如在册全局管理员）时透传文案
  const update = useMutation({
    mutationFn: (p: { id: string; patch: { name?: string; status?: 'active' | 'disabled' } }) =>
      api(accountPath(p.id), { method: 'PUT', body: JSON.stringify(p.patch) }),
    onSuccess: (_r, p) => {
      message.success(p.patch.status ? (p.patch.status === 'active' ? '已启用' : '已停用') : '已改名')
      setRenaming(null)
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const remove = useMutation({
    mutationFn: (id: string) => api(accountPath(id), { method: 'DELETE' }),
    onSuccess: () => {
      message.success('已删除')
      invalidate()
    },
    // 仍有成员或桶时后端 400，直接展示后端文案
    onError: (e: Error) => message.error(e.message),
  })

  if (meLoading) return <Card loading />
  if (!isAdmin) {
    return (
      <>
        <PageHeader title="账户" sub="多租户账户、成员与配额" />
        <Alert type="warning" showIcon message="需要管理员权限" description="账户管理仅全局管理员可用。" />
      </>
    )
  }

  return (
    <>
      <PageHeader
        title="账户"
        sub="多租户账户：成员、桶数量配额、软字节配额（只告警不阻写）与用量"
        extra={
          <Button
            type="primary"
            onClick={() => {
              setNewName('')
              setCreating(true)
            }}
          >
            新建账户
          </Button>
        }
      />
      {error && <Alert type="error" showIcon message={error.message} style={{ marginBottom: 12 }} />}
      <Card styles={{ body: { padding: 0 } }}>
        <Table<AccountRow>
          rowKey="id"
          loading={isLoading}
          dataSource={accounts}
          columns={[
            {
              title: '账户',
              render: (_: unknown, a: AccountRow) => (
                <Space direction="vertical" size={0}>
                  <Space size={6}>
                    <span style={{ fontWeight: 600 }}>{a.name}</span>
                    {a.id === SYSTEM_ACCOUNT && <Tag>系统</Tag>}
                  </Space>
                  <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                    {a.id}
                  </Typography.Text>
                </Space>
              ),
            },
            {
              title: '状态',
              width: 90,
              render: (_: unknown, a: AccountRow) =>
                a.status === 'active' ? <Tag color="green">启用</Tag> : <Tag color="red">停用</Tag>,
            },
            { title: '成员', dataIndex: 'member_count', width: 70 },
            { title: '桶数', dataIndex: 'bucket_count', width: 70 },
            {
              title: '软配额',
              width: 110,
              render: (_: unknown, a: AccountRow) => quotaText(a.soft_byte_quota),
            },
            {
              title: '操作',
              width: 360,
              render: (_: unknown, a: AccountRow) => (
                <Space size={8} wrap>
                  <Button size="small" onClick={() => setMembersFor(a)}>
                    成员
                  </Button>
                  <Button size="small" onClick={() => setQuotaFor(a)}>
                    配额
                  </Button>
                  <Button size="small" onClick={() => setUsageFor(a)}>
                    用量
                  </Button>
                  <Button size="small" onClick={() => setRenaming({ id: a.id, name: a.name })}>
                    改名
                  </Button>
                  {a.status === 'active' ? (
                    <Popconfirm
                      title={`停用账户 ${a.name}？成员登录与 S3 访问将立即失效`}
                      onConfirm={() => update.mutate({ id: a.id, patch: { status: 'disabled' } })}
                    >
                      <Button size="small" disabled={update.isPending}>
                        停用
                      </Button>
                    </Popconfirm>
                  ) : (
                    <Button
                      size="small"
                      disabled={update.isPending}
                      onClick={() => update.mutate({ id: a.id, patch: { status: 'active' } })}
                    >
                      启用
                    </Button>
                  )}
                  {a.id !== SYSTEM_ACCOUNT && (
                    <Popconfirm
                      title={`删除账户 ${a.name}？`}
                      description="账户仍有成员或桶时后端会拒绝"
                      onConfirm={() => remove.mutate(a.id)}
                    >
                      <Button danger size="small" disabled={remove.isPending}>
                        删除
                      </Button>
                    </Popconfirm>
                  )}
                </Space>
              ),
            },
          ]}
        />
      </Card>

      <Modal
        title="新建账户"
        open={creating}
        onCancel={() => setCreating(false)}
        onOk={() => {
          const name = newName.trim()
          if (!name) {
            message.warning('请输入账户名')
            return
          }
          create.mutate(name)
        }}
        confirmLoading={create.isPending}
      >
        <Input
          value={newName}
          onChange={(e) => setNewName(e.target.value)}
          placeholder="账户名（1-128 字符）"
          autoFocus
          onPressEnter={() => {
            if (!create.isPending && newName.trim()) create.mutate(newName.trim())
          }}
        />
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          创建后可在「配额」中设置桶数量配额与软字节配额。
        </Typography.Text>
      </Modal>

      <Modal
        title="账户改名"
        open={!!renaming}
        onCancel={() => setRenaming(null)}
        onOk={() => {
          if (!renaming) return
          const name = renaming.name.trim()
          if (!name) {
            message.warning('请输入账户名')
            return
          }
          update.mutate({ id: renaming.id, patch: { name } })
        }}
        confirmLoading={update.isPending}
      >
        <Input
          value={renaming?.name ?? ''}
          onChange={(e) => setRenaming((s) => (s ? { ...s, name: e.target.value } : s))}
          placeholder="账户名（1-128 字符）"
          autoFocus
        />
      </Modal>

      {quotaFor && <QuotaModal account={quotaFor} onClose={() => setQuotaFor(null)} />}
      {membersFor && <MembersModal account={membersFor} onClose={() => setMembersFor(null)} />}
      {usageFor && <UsageModal account={usageFor} onClose={() => setUsageFor(null)} />}
    </>
  )
}
