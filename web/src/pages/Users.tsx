import { useMemo, useState } from 'react'
import { App, Button, Checkbox, Input, Modal, Popconfirm, Table, Tag } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type BucketInfo, type Me, type Permission, type UserRow } from '../api'

/** 权限摘要文案：`alpha: 读/写；beta: 删` */
function summary(perms: Permission[]): string {
  const active = perms.filter((p) => p.read || p.write || p.delete)
  if (active.length === 0) return '—'
  return active
    .map((p) => `${p.bucket}: ${[p.read && '读', p.write && '写', p.delete && '删'].filter(Boolean).join('/')}`)
    .join('；')
}

export default function Users() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [newUser, setNewUser] = useState({ username: '', password: '' })
  // 权限编辑：null = 关闭；否则承载目标用户与逐桶勾选状态
  const [editing, setEditing] = useState<{ user: UserRow; perms: Record<string, Permission> } | null>(null)

  const { data: users = [], isLoading } = useQuery({
    queryKey: ['users'],
    queryFn: () => api<UserRow[]>('/api/v1/users'),
  })
  // 自己那行的「停用」必吃 400（后端防呆）——前端直接禁用，别让用户白点
  const { data: me } = useQuery({ queryKey: ['me'], queryFn: () => api<Me>('/api/v1/me') })
  const { data: buckets = [] } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<BucketInfo[]>('/api/v1/buckets'),
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['users'] })

  const create = useMutation({
    mutationFn: (u: { username: string; password: string }) =>
      api('/api/v1/users', { method: 'POST', body: JSON.stringify(u) }),
    onSuccess: () => { message.success('已创建'); setCreating(false); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const remove = useMutation({
    mutationFn: (id: number) => api(`/api/v1/users/${id}`, { method: 'DELETE' }),
    onSuccess: () => { message.success('已删除'); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const setStatus = useMutation({
    mutationFn: (payload: { id: number; active: boolean }) =>
      api(`/api/v1/users/${payload.id}/status`, {
        method: 'PUT',
        body: JSON.stringify({ active: payload.active }),
      }),
    onSuccess: (_r, v) => { message.success(v.active ? '已启用' : '已停用'); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const savePerms = useMutation({
    mutationFn: (payload: { id: number; rows: Permission[] }) =>
      api(`/api/v1/users/${payload.id}/permissions`, {
        method: 'PUT',
        body: JSON.stringify(payload.rows),
      }),
    onSuccess: () => { message.success('权限已保存'); setEditing(null); invalidate() },
    onError: (e: Error) => message.error(e.message),
  })

  const openPerms = (user: UserRow) => {
    const perms: Record<string, Permission> = {}
    for (const b of buckets) perms[b.name] = { bucket: b.name, read: false, write: false, delete: false }
    for (const p of user.permissions) perms[p.bucket] = { ...p }
    setEditing({ user, perms })
  }

  const editRows = useMemo(
    () => (editing ? Object.values(editing.perms).sort((a, b) => a.bucket.localeCompare(b.bucket)) : []),
    [editing],
  )

  const toggle = (bucket: string, field: 'read' | 'write' | 'delete', checked: boolean) => {
    setEditing((prev) =>
      prev ? { ...prev, perms: { ...prev.perms, [bucket]: { ...prev.perms[bucket], [field]: checked } } } : prev,
    )
  }

  return (
    <>
      <Button type="primary" onClick={() => { setNewUser({ username: '', password: '' }); setCreating(true) }} style={{ marginBottom: 16 }}>
        新建子账户
      </Button>
      <Table<UserRow>
        rowKey="id"
        loading={isLoading}
        dataSource={users}
        columns={[
          { title: '用户名', dataIndex: 'username' },
          {
            title: '角色',
            width: 140,
            render: (_: unknown, u: UserRow) => (
              <>
                {u.is_admin ? <Tag color="gold">管理员</Tag> : <Tag>子账户</Tag>}
                {/* 显式判 disabled：字段缺失（旧后端/缓存）时不误报停用 */}
                {u.status === 'disabled' && <Tag color="red">已停用</Tag>}
              </>
            ),
          },
          {
            title: '权限',
            render: (_: unknown, u: UserRow) => (u.is_admin ? '全部' : summary(u.permissions)),
          },
          {
            title: '操作',
            width: 260,
            render: (_: unknown, u: UserRow) => (
              <>
                <Button size="small" onClick={() => openPerms(u)} disabled={u.is_admin} style={{ marginRight: 8 }}>
                  权限
                </Button>
                {/* 启停用：停用即全链路失效（登录/S3/上传面），旧 JWT 下一请求 401；自己那行禁用 */}
                {u.status !== 'disabled' ? (
                  <Popconfirm title={`停用 ${u.username}？其 AK 与登录将立即失效`} onConfirm={() => setStatus.mutate({ id: u.id, active: false })}>
                    <Button size="small" disabled={u.id === me?.id} style={{ marginRight: 8 }}>停用</Button>
                  </Popconfirm>
                ) : (
                  <Button size="small" onClick={() => setStatus.mutate({ id: u.id, active: true })} style={{ marginRight: 8 }}>
                    启用
                  </Button>
                )}
                {!u.is_admin && (
                  <Popconfirm title={`删除子账户 ${u.username}？`} onConfirm={() => remove.mutate(u.id)}>
                    <Button danger size="small">删除</Button>
                  </Popconfirm>
                )}
              </>
            ),
          },
        ]}
      />
      <Modal
        title="新建子账户"
        open={creating}
        onCancel={() => setCreating(false)}
        onOk={() => {
          if (!newUser.username.trim()) { message.warning('请输入用户名'); return }
          if (newUser.password.length < 8) { message.warning('口令至少 8 位'); return }
          create.mutate({ username: newUser.username.trim(), password: newUser.password })
        }}
        confirmLoading={create.isPending}
      >
        <Input
          value={newUser.username}
          onChange={(e) => setNewUser((s) => ({ ...s, username: e.target.value }))}
          placeholder="用户名"
          autoFocus
          style={{ marginBottom: 12 }}
        />
        <Input.Password
          value={newUser.password}
          onChange={(e) => setNewUser((s) => ({ ...s, password: e.target.value }))}
          placeholder="口令（至少 8 位）"
        />
      </Modal>
      <Modal
        title={`权限：${editing?.user.username ?? ''}`}
        open={!!editing}
        width={560}
        onCancel={() => setEditing(null)}
        onOk={() => {
          if (!editing) return
          // 全量替换语义：只发有三档勾选的桶（全空的由后端跳过）
          savePerms.mutate({ id: editing.user.id, rows: Object.values(editing.perms).filter((p) => p.read || p.write || p.delete) })
        }}
        confirmLoading={savePerms.isPending}
      >
        <Table<Permission>
          rowKey="bucket"
          dataSource={editRows}
          pagination={false}
          size="small"
          columns={[
            { title: '桶', dataIndex: 'bucket' },
            {
              title: '读',
              width: 80,
              render: (_: unknown, p: Permission) => (
                <Checkbox checked={p.read} onChange={(e) => toggle(p.bucket, 'read', e.target.checked)} />
              ),
            },
            {
              title: '写',
              width: 80,
              render: (_: unknown, p: Permission) => (
                <Checkbox checked={p.write} onChange={(e) => toggle(p.bucket, 'write', e.target.checked)} />
              ),
            },
            {
              title: '删',
              width: 80,
              render: (_: unknown, p: Permission) => (
                <Checkbox checked={p.delete} onChange={(e) => toggle(p.bucket, 'delete', e.target.checked)} />
              ),
            },
          ]}
        />
      </Modal>
    </>
  )
}
