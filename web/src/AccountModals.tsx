import { useState, type ReactNode } from 'react'
import {
  Alert,
  App,
  Button,
  Descriptions,
  InputNumber,
  Modal,
  Popconfirm,
  Select,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  api,
  type AccountMemberRow,
  type AccountRow,
  type AccountUsage,
  type UserRow,
} from './api'
import { SYSTEM_ACCOUNT, accountPath, quotaText } from './accounts'
import { humanSize } from './format'

/** 两个配额端点相互独立（软配额不会误清桶配额，反之亦然）。 */
export function QuotaModal({ account, onClose }: { account: AccountRow; onClose: () => void }) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  // 负数 = 不限：编辑时归一成空（null），保存即写 NULL
  const [bucketQuota, setBucketQuota] = useState<number | null>(
    account.bucket_quota != null && account.bucket_quota >= 0 ? account.bucket_quota : null,
  )
  const [softQuota, setSoftQuota] = useState<number | null>(
    account.soft_byte_quota != null && account.soft_byte_quota >= 0 ? account.soft_byte_quota : null,
  )
  const saved = () => qc.invalidateQueries({ queryKey: ['accounts'] })

  const saveBucket = useMutation({
    mutationFn: () =>
      api(`${accountPath(account.id)}/quota`, {
        method: 'PUT',
        body: JSON.stringify({ bucket_quota: bucketQuota }),
      }),
    onSuccess: () => {
      message.success('桶数量配额已保存')
      saved()
    },
    onError: (e: Error) => message.error(e.message),
  })
  const saveSoft = useMutation({
    mutationFn: () =>
      api(`${accountPath(account.id)}/soft-quota`, {
        method: 'PUT',
        body: JSON.stringify({ soft_byte_quota: softQuota }),
      }),
    onSuccess: () => {
      message.success('软字节配额已保存')
      saved()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const row = (label: string, input: ReactNode, onSave: () => void, pending: boolean, hint: string) => (
    <div>
      <Typography.Text strong>{label}</Typography.Text>
      <Space.Compact style={{ width: '100%', marginTop: 8, marginBottom: 4 }}>
        {input}
        <Button type="primary" loading={pending} onClick={onSave}>
          保存
        </Button>
      </Space.Compact>
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        {hint}
      </Typography.Text>
    </div>
  )

  return (
    <Modal title={`配额 · ${account.name}`} open onCancel={onClose} footer={null}>
      <Space direction="vertical" size={16} style={{ width: '100%' }}>
        {row(
          '桶数量配额',
          <InputNumber
            value={bucketQuota}
            onChange={setBucketQuota}
            min={0}
            precision={0}
            placeholder="留空 = 不限"
            style={{ width: '100%' }}
            aria-label="桶数量配额"
          />,
          () => saveBucket.mutate(),
          saveBucket.isPending,
          '达到上限后新建桶被拒绝；留空 = 不限。',
        )}
        {row(
          '软字节配额',
          <InputNumber
            value={softQuota}
            onChange={setSoftQuota}
            min={0}
            precision={0}
            placeholder="留空 = 不限（单位：字节）"
            style={{ width: '100%' }}
            aria-label="软字节配额"
          />,
          () => saveSoft.mutate(),
          saveSoft.isPending,
          '只告警不阻写：超限写入仍成功；当前用量见「用量」，留空 = 不限。',
        )}
      </Space>
    </Modal>
  )
}

/** 成员挂载/角色变更共用 POST /members；后端无移除接口，只能停用。 */
export function MembersModal({ account, onClose }: { account: AccountRow; onClose: () => void }) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [uid, setUid] = useState<number>()
  const [role, setRole] = useState<'member' | 'admin'>('member')
  const isSystem = account.id === SYSTEM_ACCOUNT

  const { data: members = [], isLoading, error } = useQuery({
    queryKey: ['account-members', account.id],
    queryFn: () => api<AccountMemberRow[]>(`${accountPath(account.id)}/members`),
  })
  const { data: users = [] } = useQuery({
    queryKey: ['users'],
    queryFn: () => api<UserRow[]>('/api/v1/users'),
    enabled: !isSystem,
  })

  const invalidate = () => {
    qc.invalidateQueries({ queryKey: ['account-members', account.id] })
    qc.invalidateQueries({ queryKey: ['accounts'] })
  }

  const attach = useMutation({
    mutationFn: (p: { uid: number; role: string }) =>
      api(`${accountPath(account.id)}/members`, { method: 'POST', body: JSON.stringify(p) }),
    onSuccess: () => {
      message.success('成员已保存')
      setUid(undefined)
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })
  const setStatus = useMutation({
    mutationFn: (p: { uid: number; active: boolean }) =>
      api(`${accountPath(account.id)}/members/${p.uid}/status`, {
        method: 'PUT',
        body: JSON.stringify({ active: p.active }),
      }),
    onSuccess: (_r, p) => {
      message.success(p.active ? '已启用' : '已停用')
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  // 全局管理员不挂账户；已是本账户成员的从候选里剔除（角色在行内改）
  const candidates = users.filter((u) => !u.is_admin && !members.some((m) => m.uid === u.id))

  return (
    <Modal title={`成员 · ${account.name}`} open onCancel={onClose} footer={null} width={720}>
      {error && <Alert type="error" showIcon message={error.message} style={{ marginBottom: 12 }} />}
      {isSystem ? (
        <Alert type="info" showIcon message="系统账户不挂载成员" style={{ marginBottom: 12 }} />
      ) : (
        <>
          <Space wrap style={{ marginBottom: 4 }}>
            <Select
              showSearch
              optionFilterProp="label"
              value={uid}
              onChange={(v: number) => setUid(v)}
              placeholder="选择已有用户"
              style={{ width: 220 }}
              aria-label="成员用户"
              options={candidates.map((u) => ({ value: u.id, label: `${u.username} (#${u.id})` }))}
              disabled={attach.isPending}
            />
            <Select
              value={role}
              onChange={(v: 'member' | 'admin') => setRole(v)}
              style={{ width: 140 }}
              aria-label="成员角色"
              options={[
                { value: 'member', label: 'member' },
                { value: 'admin', label: '账户管理员' },
              ]}
              disabled={attach.isPending}
            />
            <Button
              type="primary"
              loading={attach.isPending}
              disabled={uid == null}
              onClick={() => uid != null && attach.mutate({ uid, role })}
            >
              添加
            </Button>
          </Space>
          <Typography.Text type="secondary" style={{ display: 'block', fontSize: 12, marginBottom: 12 }}>
            挂载已有用户会将其迁入本账户（解除原账户成员关系）；全局管理员不可挂载；后端无移除接口，停用即收回访问。
          </Typography.Text>
        </>
      )}
      <Table<AccountMemberRow>
        rowKey="uid"
        loading={isLoading}
        dataSource={members}
        pagination={false}
        size="small"
        columns={[
          {
            title: '用户',
            render: (_: unknown, m: AccountMemberRow) => (
              <Space size={6}>
                {m.username}
                <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                  #{m.uid}
                </Typography.Text>
                {m.is_admin && <Tag color="gold">全局管理员</Tag>}
              </Space>
            ),
          },
          {
            title: '角色',
            width: 170,
            render: (_: unknown, m: AccountMemberRow) =>
              m.role === 'owner' ? (
                <Tag color="purple">owner（派生账户）</Tag>
              ) : (
                <Select
                  size="small"
                  value={m.role}
                  style={{ width: 130 }}
                  aria-label={`${m.username} 角色`}
                  disabled={attach.isPending}
                  onChange={(v: string) => attach.mutate({ uid: m.uid, role: v })}
                  options={[
                    { value: 'member', label: 'member' },
                    { value: 'admin', label: '账户管理员' },
                  ]}
                />
              ),
          },
          {
            title: '状态',
            width: 90,
            render: (_: unknown, m: AccountMemberRow) =>
              m.status === 'active' ? <Tag color="green">启用</Tag> : <Tag color="red">停用</Tag>,
          },
          {
            title: '操作',
            width: 110,
            render: (_: unknown, m: AccountMemberRow) =>
              m.status === 'active' ? (
                <Popconfirm
                  title={`停用 ${m.username}？其登录与 AK 将立即失效`}
                  onConfirm={() => setStatus.mutate({ uid: m.uid, active: false })}
                >
                  <Button size="small" disabled={setStatus.isPending}>
                    停用
                  </Button>
                </Popconfirm>
              ) : (
                <Button
                  size="small"
                  disabled={setStatus.isPending}
                  onClick={() => setStatus.mutate({ uid: m.uid, active: true })}
                >
                  启用
                </Button>
              ),
          },
        ]}
      />
    </Modal>
  )
}

export function UsageModal({ account, onClose }: { account: AccountRow; onClose: () => void }) {
  // staleTime 缺省 0：每次打开都重新列举（用量端点 O(对象数)，不轮询）
  const { data, isLoading, error } = useQuery({
    queryKey: ['account-usage', account.id],
    queryFn: () => api<AccountUsage>(`${accountPath(account.id)}/usage`),
  })
  const unlimited = data ? data.soft_byte_quota == null || data.soft_byte_quota < 0 : true

  return (
    <Modal title={`用量 · ${account.name}`} open onCancel={onClose} footer={null}>
      {error ? (
        <Alert type="error" showIcon message={error.message} />
      ) : (
        <Spin spinning={isLoading}>
          <Descriptions column={1} bordered size="small">
            <Descriptions.Item label="已用字节">
              {data ? `${humanSize(data.used_bytes)}（${data.used_bytes.toLocaleString('zh-CN')} B）` : '—'}
            </Descriptions.Item>
            <Descriptions.Item label="软配额">
              {data ? quotaText(data.soft_byte_quota) : '—'}
            </Descriptions.Item>
            <Descriptions.Item label="告警状态">
              {!data ? '—' : unlimited ? <Tag>未配置</Tag> : data.exceeded ? <Tag color="red">已超限</Tag> : <Tag color="green">未超限</Tag>}
            </Descriptions.Item>
            <Descriptions.Item label="统计完整性">
              {!data ? '—' : data.complete ? <Tag color="green">完整</Tag> : <Tag color="gold">下界（集群有不可达 peer）</Tag>}
            </Descriptions.Item>
            <Descriptions.Item label="桶数">{data?.bucket_count ?? '—'}</Descriptions.Item>
          </Descriptions>
          <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
            用量 = 当前版本对象字节和（不含非当前版本与未完成 MPU）；软配额只告警不阻写。
          </Typography.Text>
        </Spin>
      )}
    </Modal>
  )
}
