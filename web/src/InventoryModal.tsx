import { useEffect, useState } from 'react'
import { App, Button, Form, Input, Modal, Popconfirm, Select, Space, Table, Tag } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type InventoryConfig } from './api'

const OPTIONAL_FIELDS = [
  'ReplicationStatus',
  'EncryptionStatus',
  'ObjectLockRetainUntilDate',
  'ObjectLockMode',
  'ObjectLockLegalHoldStatus',
  'ChecksumAlgorithm',
]

/**
 * 桶清单配置（S5）：列表 + 表单合一弹窗；一份配置一次全量 PUT。
 * Buckets 列表与 Objects 页头共用，避免两处复制。
 */
export default function InventoryModal({ bucket, onClose }: { bucket: string; onClose: () => void }) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [form] = Form.useForm()
  // 编辑既有配置：id 只读，is_enabled 保持原值（保存是整行 upsert）
  const [editing, setEditing] = useState<InventoryConfig | null>(null)

  const { data: configs = [], isLoading } = useQuery({
    queryKey: ['inventory', bucket],
    queryFn: () => api<InventoryConfig[]>(`/api/v1/buckets/${encodeURIComponent(bucket)}/inventory`),
  })

  const invalidate = () => qc.invalidateQueries({ queryKey: ['inventory', bucket] })

  const save = useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      api(`/api/v1/buckets/${encodeURIComponent(bucket)}/inventory`, {
        method: 'PUT',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      message.success('清单配置已保存')
      setEditing(null)
      form.setFieldsValue({ id: undefined })
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const remove = useMutation({
    mutationFn: (id: string) =>
      api(`/api/v1/buckets/${encodeURIComponent(bucket)}/inventory/${encodeURIComponent(id)}`, {
        method: 'DELETE',
      }),
    onSuccess: () => {
      message.success('已删除')
      invalidate()
    },
    onError: (e: Error) => message.error(e.message),
  })

  const runNow = useMutation({
    mutationFn: (id: string) =>
      api<{ job_id: number; run_key: string }>(
        `/api/v1/buckets/${encodeURIComponent(bucket)}/inventory/${encodeURIComponent(id)}/run`,
        { method: 'POST' },
      ),
    onSuccess: (r) => message.success(`已入队作业 #${r.job_id}（${r.run_key}）`),
    onError: (e: Error) => message.error(e.message),
  })

  const submit = async () => {
    const v = await form.validateFields()
    save.mutate({
      id: v.id,
      is_enabled: editing?.is_enabled ?? true,
      frequency: v.frequency,
      destination_bucket: v.destination_bucket,
      destination_prefix: v.destination_prefix || null,
      format: 'CSV',
      filter_prefix: v.filter_prefix || null,
      included_object_versions: v.included_object_versions,
      optional_fields: v.optional_fields ?? [],
    })
  }

  const edit = (c: InventoryConfig) => {
    setEditing(c)
    form.setFieldsValue({
      id: c.id,
      frequency: c.frequency,
      filter_prefix: c.filter_prefix ?? '',
      destination_bucket: c.destination_bucket,
      destination_prefix: c.destination_prefix ?? '',
      included_object_versions: c.included_object_versions,
      optional_fields: c.optional_fields,
    })
  }

  // 打开时给表单一组默认值（id 留空，用户填）
  useEffect(() => {
    form.setFieldsValue({ frequency: 'Daily', included_object_versions: 'Current', optional_fields: [] })
  }, [form])

  return (
    <Modal
      title={`清单配置 · ${bucket}`}
      open
      width={920}
      onCancel={onClose}
      onOk={submit}
      confirmLoading={save.isPending}
      okText="保存配置"
      styles={{ body: { maxHeight: '72vh', overflow: 'auto' } }}
    >
      <Table<InventoryConfig>
        rowKey="id"
        size="small"
        loading={isLoading}
        dataSource={configs}
        pagination={false}
        style={{ marginBottom: 16 }}
        columns={[
          { title: 'ID', dataIndex: 'id', width: 130 },
          { title: '频率', dataIndex: 'frequency', width: 90 },
          {
            title: '目标',
            render: (_: unknown, c: InventoryConfig) =>
              `${c.destination_bucket}/${c.destination_prefix ?? ''}`,
          },
          {
            title: '版本',
            dataIndex: 'included_object_versions',
            width: 90,
            render: (v: string) => <Tag>{v}</Tag>,
          },
          {
            title: '操作',
            width: 220,
            render: (_: unknown, c: InventoryConfig) => (
              <Space size={8}>
                <Button size="small" onClick={() => edit(c)}>编辑</Button>
                <Button size="small" onClick={() => runNow.mutate(c.id)}>立即运行</Button>
                <Popconfirm title={`删除配置 ${c.id}？`} onConfirm={() => remove.mutate(c.id)}>
                  <Button size="small" danger>删除</Button>
                </Popconfirm>
              </Space>
            ),
          },
        ]}
      />
      <Form form={form} layout="vertical">
        <Space wrap size={12} align="start">
          <Form.Item name="id" label="配置 ID" rules={[{ required: true }]} style={{ width: 180 }}>
            <Input placeholder="daily-inventory" readOnly={editing !== null} />
          </Form.Item>
          <Form.Item name="frequency" label="频率" rules={[{ required: true }]}>
            <Select
              style={{ width: 120 }}
              options={[
                { value: 'Daily', label: 'Daily' },
                { value: 'Weekly', label: 'Weekly' },
              ]}
            />
          </Form.Item>
          <Form.Item name="filter_prefix" label="过滤前缀" style={{ width: 180 }}>
            <Input placeholder="可选" />
          </Form.Item>
        </Space>
        <Space wrap size={12} align="start">
          <Form.Item
            name="destination_bucket"
            label="目标桶"
            rules={[{ required: true }]}
            style={{ width: 200 }}
          >
            <Input />
          </Form.Item>
          <Form.Item name="destination_prefix" label="目标前缀" style={{ width: 200 }}>
            <Input placeholder="可选" />
          </Form.Item>
          <Form.Item name="included_object_versions" label="对象版本" rules={[{ required: true }]}>
            <Select
              style={{ width: 130 }}
              options={[
                { value: 'Current', label: 'Current' },
                { value: 'All', label: 'All' },
              ]}
            />
          </Form.Item>
        </Space>
        <Form.Item name="optional_fields" label="可选列">
          <Select
            mode="multiple"
            placeholder="可选（默认只含基础字段）"
            options={OPTIONAL_FIELDS.map((f) => ({ value: f, label: f }))}
          />
        </Form.Item>
      </Form>
    </Modal>
  )
}
