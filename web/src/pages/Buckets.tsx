import { useState } from 'react'
import { Link } from 'react-router-dom'
import { App, Button, Card, Input, Modal, Popconfirm, Select, Space, Table, Tag, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type BucketEncryption, type BucketInfo, type RaftStatus } from '../api'
import InventoryModal from '../InventoryModal'
import PageHeader from '../PageHeader'

export default function Buckets() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [creating, setCreating] = useState(false)
  const [newName, setNewName] = useState('')
  const [newMode, setNewMode] = useState<'async' | 'sync'>('async')
  const [inventoryBucket, setInventoryBucket] = useState<string | null>(null)
  const [encryptionBucket, setEncryptionBucket] = useState<string | null>(null)

  const { data: buckets = [], isLoading } = useQuery({
    queryKey: ['buckets'],
    queryFn: () => api<BucketInfo[]>('/api/v1/buckets'),
  })
  // M9：后端能力位（S0 全 false → sync 选项置灰）
  const { data: raft } = useQuery({
    queryKey: ['raft-status'],
    queryFn: () => api<RaftStatus>('/api/v1/raft/status'),
    staleTime: 30_000,
  })
  const syncAvailable = raft?.capabilities?.sync === true

  const invalidate = () => qc.invalidateQueries({ queryKey: ['buckets'] })

  const create = useMutation({
    mutationFn: ({ name, mode }: { name: string; mode: 'async' | 'sync' }) =>
      api('/api/v1/buckets', {
        method: 'POST',
        body: JSON.stringify({ name, replication_mode: mode }),
      }),
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
          <Button type="primary" onClick={() => { setNewName(''); setNewMode('async'); setCreating(true) }}>
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
              title: '复制模式',
              width: 110,
              render: (_: unknown, b: BucketInfo) =>
                b.replication_mode === 'sync'
                  ? <Tag color="blue">sync</Tag>
                  : <Tag>async</Tag>,
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
              width: 250,
              render: (_: unknown, b: BucketInfo) => (
                <Space size={8}>
                  {/* 清单/加密：owner 或 admin（后端判权；非 owner 点保存会看到 403 文案） */}
                  <Button size="small" onClick={() => setInventoryBucket(b.name)}>清单</Button>
                  <Button size="small" onClick={() => setEncryptionBucket(b.name)}>加密</Button>
                  {/* 删除按钮按 can_delete 显隐（后端另有「桶主或管理员」403 兜底） */}
                  {b.can_delete && (
                    <Popconfirm title={`删除桶 ${b.name}？（须为空桶）`} onConfirm={() => remove.mutate(b.name)}>
                      <Button danger size="small">删除</Button>
                    </Popconfirm>
                  )}
                </Space>
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
          create.mutate({ name: newName, mode: newMode })
        }}
        confirmLoading={create.isPending}
      >
        <Space direction="vertical" size={12} style={{ width: '100%' }}>
          <Input
            id="new-bucket-name"
            value={newName}
            onChange={(e) => setNewName(e.target.value)}
            placeholder="3-63 字符，小写字母数字与 - ."
            autoFocus
          />
          <Select
            id="new-bucket-mode"
            value={newMode}
            onChange={(v: 'async' | 'sync') => setNewMode(v)}
            style={{ width: '100%' }}
            options={[
              { value: 'async', label: 'async（异步复制，缺省）' },
              {
                value: 'sync',
                label: syncAvailable
                  ? 'sync（远端确认后才返回）'
                  : 'sync（当前后端未启用）',
                disabled: !syncAvailable,
              },
            ]}
          />
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            3-voter 控制面；sync 写/删均远端确认；单数据站点宕机控制面可写但 sync 503；
            stretch 两站点缺一不可。sync 创建后不可改回 async；写/删计入远端 RTT（默认超时
            10 s）；默认单对象上限 64 MiB，面向小对象/合规。
          </Typography.Text>
        </Space>
      </Modal>
      {inventoryBucket && (
        <InventoryModal bucket={inventoryBucket} onClose={() => setInventoryBucket(null)} />
      )}
      {encryptionBucket && (
        <EncryptionModal bucket={encryptionBucket} onClose={() => setEncryptionBucket(null)} />
      )}
    </>
  )
}

/** `aws:kms:<name|arn>` → 展示名（ARN 取 `key/` 后段）。 */
function cmkDisplayName(stored: string) {
  const raw = stored.slice('aws:kms:'.length)
  const i = raw.lastIndexOf('key/')
  return raw.startsWith('arn:') && i >= 0 ? raw.slice(i + 4) : raw
}

/** 桶默认加密：GET 读现状；AES256/aws:kms/aws:kms:<cmk> → PUT，none → DELETE（清除）。 */
function EncryptionModal({ bucket, onClose }: { bucket: string; onClose: () => void }) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  // 未编辑时直接展示服务端值，避免 effect 回填
  const [draft, setDraft] = useState<string>()

  const { data, isLoading } = useQuery({
    queryKey: ['bucket-encryption', bucket],
    queryFn: () =>
      api<BucketEncryption>(`/api/v1/buckets/${encodeURIComponent(bucket)}/encryption`),
  })

  const stored = data?.algorithm ?? null
  // 3.16：保留既有 `aws:kms:<cmk>`（否则会被误显示为「不加密」，保存即清除）
  const isCmk = stored?.startsWith('aws:kms:') ?? false
  const algorithm =
    draft ??
    (stored === 'AES256' || stored === 'aws:kms' || isCmk ? stored! : 'none')

  const save = useMutation({
    mutationFn: () =>
      algorithm === 'none'
        ? api(`/api/v1/buckets/${encodeURIComponent(bucket)}/encryption`, { method: 'DELETE' })
        : api(`/api/v1/buckets/${encodeURIComponent(bucket)}/encryption`, {
            method: 'PUT',
            body: JSON.stringify({ algorithm }),
          }),
    onSuccess: () => {
      message.success(algorithm === 'none' ? '已清除默认加密' : '默认加密已保存')
      qc.invalidateQueries({ queryKey: ['bucket-encryption', bucket] })
      onClose()
    },
    onError: (e: Error) => message.error(e.message),
  })

  return (
    <Modal
      title={`默认加密 · ${bucket}`}
      open
      onCancel={onClose}
      onOk={() => save.mutate()}
      confirmLoading={save.isPending || isLoading}
      okText="保存"
    >
      <Select
        value={algorithm}
        onChange={setDraft}
        aria-label="默认加密算法"
        style={{ width: '100%', marginBottom: 12 }}
        options={[
          { value: 'none', label: '不加密（清除配置）' },
          { value: 'AES256', label: 'SSE-S3（AES256）' },
          { value: 'aws:kms', label: 'SSE-KMS（默认密钥）' },
          ...(isCmk && stored
            ? [{ value: stored, label: `SSE-KMS（CMK: ${cmkDisplayName(stored)}）` }]
            : []),
        ]}
      />
      <Typography.Text type="secondary" style={{ fontSize: 12 }}>
        对桶内新写入对象生效（PUT / 分片 / Copy）。KMS 需在服务端配置 OSS_KMS_DIR。
      </Typography.Text>
    </Modal>
  )
}
