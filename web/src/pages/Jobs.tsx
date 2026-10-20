import { useState } from 'react'
import {
  App,
  Button,
  Card,
  Descriptions,
  Drawer,
  Form,
  Input,
  InputNumber,
  Modal,
  Popconfirm,
  Select,
  Space,
  Table,
  Tag,
} from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type JobDetail, type JobSummary } from '../api'
import { fmtTime } from '../format'
import PageHeader from '../PageHeader'

const STATE_COLOR: Record<string, string> = {
  queued: 'blue',
  running: 'processing',
  paused: 'orange',
  done: 'green',
  error: 'red',
  cancelled: 'magenta',
}

const OPERATIONS = ['CopyObject', 'DeleteObject', 'ReplaceTags', 'RestoreObject', 'InvokeLambda']

const TASK_PAGE = 20

export default function Jobs() {
  const [batchOpen, setBatchOpen] = useState(false)
  const [detailId, setDetailId] = useState<number | null>(null)

  const { data: jobs = [], isLoading } = useQuery({
    queryKey: ['jobs'],
    queryFn: () => api<JobSummary[]>('/api/v1/jobs'),
    // 列表兼作进度面板：作业执行在后台，定时刷新即可（详情另有 2s 轮询）
    refetchInterval: 15_000,
  })

  return (
    <>
      <PageHeader
        title="作业"
        sub="Batch 作业：清单对象每行一个动作；支持暂停/恢复/取消/重试失败任务"
        extra={
          <Button type="primary" onClick={() => setBatchOpen(true)}>
            新建 Batch 作业
          </Button>
        }
      />
      <Card styles={{ body: { padding: 0 } }}>
        <Table<JobSummary>
          rowKey="id"
          loading={isLoading}
          dataSource={jobs}
          columns={[
            { title: 'ID', dataIndex: 'id', width: 70 },
            { title: '类型', dataIndex: 'kind', width: 100 },
            {
              title: '状态',
              dataIndex: 'state',
              width: 100,
              render: (s: string) => <Tag color={STATE_COLOR[s] ?? 'default'}>{s}</Tag>,
            },
            { title: '优先级', dataIndex: 'priority', width: 80, render: (v?: number) => v ?? '—' },
            {
              title: '进度/总数',
              width: 110,
              render: (_: unknown, j: JobSummary) => `${j.progress ?? j.scanned ?? '—'} / ${j.total ?? '—'}`,
            },
            {
              title: '成功/失败',
              width: 110,
              render: (_: unknown, j: JobSummary) => `${j.succeeded ?? '—'} / ${j.failed ?? '—'}`,
            },
            {
              title: '发起人',
              dataIndex: 'requested_by',
              width: 90,
              render: (v?: number | null) => v ?? '—',
            },
            { title: '创建时间', dataIndex: 'created_at', width: 180, render: (v: number) => fmtTime(v) },
            {
              title: '操作',
              width: 90,
              render: (_: unknown, j: JobSummary) => (
                <Button size="small" onClick={() => setDetailId(j.id)}>详情</Button>
              ),
            },
          ]}
        />
      </Card>

      <BatchModal open={batchOpen} onClose={() => setBatchOpen(false)} onCreated={(id) => setDetailId(id)} />
      {detailId != null && <JobDrawer id={detailId} onClose={() => setDetailId(null)} />}
    </>
  )
}

function BatchModal({
  open,
  onClose,
  onCreated,
}: {
  open: boolean
  onClose: () => void
  onCreated: (id: number) => void
}) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [form] = Form.useForm()
  const operation = Form.useWatch('operation', form) as string | undefined

  const create = useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      api<{ job_id: number; total: number }>('/api/v1/jobs', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (r) => {
      message.success(`已创建作业 #${r.job_id}（${r.total} 个任务）`)
      qc.invalidateQueries({ queryKey: ['jobs'] })
      onClose()
      onCreated(r.job_id)
    },
    onError: (e: Error) => message.error(e.message),
  })

  const submit = async () => {
    const v = await form.validateFields()
    const body: Record<string, unknown> = {
      kind: 'batch',
      manifest_bucket: v.manifest_bucket,
      manifest_key: v.manifest_key,
      operation: v.operation,
      priority: v.priority ?? 0,
    }
    if (v.operation === 'CopyObject') {
      body.target = { bucket: v.target_bucket, prefix: v.target_prefix ?? '' }
    }
    if (v.operation === 'ReplaceTags') {
      const tags = (v.tags ?? []).filter((t: { key?: string } | undefined) => t?.key)
      if (tags.length === 0) {
        message.warning('ReplaceTags 至少需要一个标签')
        return
      }
      body.tags = tags.map((t: { key: string; value?: string }) => ({ key: t.key, value: t.value ?? '' }))
    }
    if (v.operation === 'RestoreObject') body.restore = { days: v.restore_days ?? 1 }
    if (v.operation === 'InvokeLambda') body.transform_access_point = v.transform_access_point
    if (v.report_bucket) body.report_bucket = v.report_bucket
    if (v.report_prefix) body.report_prefix = v.report_prefix
    create.mutate(body)
  }

  return (
    <Modal
      title="新建 Batch 作业"
      open={open}
      width={680}
      onCancel={onClose}
      onOk={submit}
      confirmLoading={create.isPending}
      okText="创建"
      styles={{ body: { maxHeight: '70vh', overflow: 'auto' } }}
      destroyOnHidden
    >
      <Form
        form={form}
        layout="vertical"
        initialValues={{ operation: 'CopyObject', priority: 0, restore_days: 1, tags: [{}] }}
      >
        <Form.Item name="manifest_bucket" label="清单桶" rules={[{ required: true }]}>
          <Input placeholder="清单对象所在桶" />
        </Form.Item>
        <Form.Item name="manifest_key" label="清单键" rules={[{ required: true }]}>
          <Input placeholder="清单 CSV 键（manifest.csv）" />
        </Form.Item>
        <Form.Item name="operation" label="操作" rules={[{ required: true }]}>
          <Select options={OPERATIONS.map((o) => ({ value: o, label: o }))} />
        </Form.Item>

        {operation === 'CopyObject' && (
          <>
            <Form.Item name="target_bucket" label="目标桶" rules={[{ required: true }]}>
              <Input placeholder="复制到哪个桶" />
            </Form.Item>
            <Form.Item name="target_prefix" label="目标前缀">
              <Input placeholder="可选；如 archive/" />
            </Form.Item>
          </>
        )}

        {operation === 'ReplaceTags' && (
          <Form.Item label="标签（最多 10 个）">
            <Form.List name="tags">
              {(fields, { add, remove }) => (
                <>
                  {fields.map((f) => (
                    <Space key={f.key} align="baseline" style={{ display: 'flex', marginBottom: 8 }}>
                      <Form.Item name={[f.name, 'key']} noStyle>
                        <Input placeholder="键" aria-label="标签键" />
                      </Form.Item>
                      <Form.Item name={[f.name, 'value']} noStyle>
                        <Input placeholder="值" aria-label="标签值" />
                      </Form.Item>
                      <Button size="small" onClick={() => remove(f.name)}>移除</Button>
                    </Space>
                  ))}
                  <Button size="small" disabled={fields.length >= 10} onClick={() => add({})}>
                    添加标签
                  </Button>
                </>
              )}
            </Form.List>
          </Form.Item>
        )}

        {operation === 'RestoreObject' && (
          <Form.Item name="restore_days" label="恢复天数（1-365）" rules={[{ required: true }]}>
            <InputNumber min={1} max={365} style={{ width: '100%' }} />
          </Form.Item>
        )}

        {operation === 'InvokeLambda' && (
          <Form.Item
            name="transform_access_point"
            label="变换器访问点"
            rules={[{ required: true }]}
          >
            <Input placeholder="访问点名（须已配置 http 变换器）" />
          </Form.Item>
        )}

        <Space size={12} style={{ display: 'flex' }}>
          <Form.Item name="report_bucket" label="报告桶" style={{ flex: 1 }}>
            <Input placeholder="可选" />
          </Form.Item>
          <Form.Item name="report_prefix" label="报告前缀" style={{ flex: 1 }}>
            <Input placeholder="可选" />
          </Form.Item>
          <Form.Item name="priority" label="优先级">
            <InputNumber />
          </Form.Item>
        </Space>
      </Form>
    </Modal>
  )
}

function JobDrawer({ id, onClose }: { id: number; onClose: () => void }) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [offset, setOffset] = useState(0)

  const { data, isLoading } = useQuery({
    queryKey: ['job', id, offset],
    queryFn: () => api<JobDetail>(`/api/v1/jobs/${id}?offset=${offset}&limit=${TASK_PAGE}`),
    refetchInterval: (q) => {
      const s = q.state.data?.job.state
      return s && ['queued', 'running', 'paused'].includes(s) ? 2000 : false
    },
  })

  const act = useMutation({
    mutationFn: (action: string) =>
      api<{ job_id?: number } | undefined>(`/api/v1/jobs/${id}/${action}`, { method: 'POST' }),
    onSuccess: (r, action) => {
      message.success(action === 'retry-failed' && r?.job_id ? `已重试为作业 #${r.job_id}` : '已提交')
      qc.invalidateQueries({ queryKey: ['job', id] })
      qc.invalidateQueries({ queryKey: ['jobs'] })
    },
    onError: (e: Error) => message.error(e.message),
  })

  const job = data?.job
  const state = job?.state

  return (
    <Drawer
      title={`作业 #${id}`}
      open
      width={760}
      onClose={onClose}
      extra={
        <Space size={8}>
          <Button size="small" disabled={state !== 'running'} onClick={() => act.mutate('pause')}>暂停</Button>
          <Button size="small" disabled={state !== 'paused'} onClick={() => act.mutate('resume')}>恢复</Button>
          <Popconfirm title="取消该作业？" onConfirm={() => act.mutate('cancel')}>
            <Button size="small" danger disabled={!state || ['done', 'error', 'cancelled'].includes(state)}>
              取消
            </Button>
          </Popconfirm>
          <Popconfirm title="重试全部失败任务？" onConfirm={() => act.mutate('retry-failed')}>
            <Button size="small" disabled={!job?.failed}>重试失败</Button>
          </Popconfirm>
        </Space>
      }
    >
      {job && (
        <Descriptions size="small" column={2} bordered style={{ marginBottom: 16 }}>
          <Descriptions.Item label="类型">{job.kind}</Descriptions.Item>
          <Descriptions.Item label="状态">
            <Tag color={STATE_COLOR[job.state] ?? 'default'}>{job.state}</Tag>
          </Descriptions.Item>
          <Descriptions.Item label="桶">{job.bucket}</Descriptions.Item>
          <Descriptions.Item label="前缀/键">{job.prefix || '—'}</Descriptions.Item>
          <Descriptions.Item label="进度">
            {job.progress ?? '—'} / {job.total ?? '—'}
          </Descriptions.Item>
          <Descriptions.Item label="成功/失败">
            {job.succeeded ?? '—'} / {job.failed ?? '—'}
          </Descriptions.Item>
          <Descriptions.Item label="优先级">{job.priority ?? '—'}</Descriptions.Item>
          <Descriptions.Item label="发起人">{job.requested_by ?? '—'}</Descriptions.Item>
          <Descriptions.Item label="创建时间">{fmtTime(job.created_at)}</Descriptions.Item>
          <Descriptions.Item label="开始时间">{fmtTime(job.started_at)}</Descriptions.Item>
          <Descriptions.Item label="结束时间">{fmtTime(job.finished_at)}</Descriptions.Item>
          <Descriptions.Item label="报告">
            {job.report_bucket ? `${job.report_bucket}/${job.report_prefix ?? ''}` : '—'}
          </Descriptions.Item>
          {job.error && (
            <Descriptions.Item label="错误" span={2}>
              {job.error}
            </Descriptions.Item>
          )}
        </Descriptions>
      )}
      <Table
        rowKey="task_no"
        size="small"
        loading={isLoading}
        dataSource={data?.tasks ?? []}
        pagination={{
          current: Math.floor(offset / TASK_PAGE) + 1,
          pageSize: TASK_PAGE,
          total: data?.job.total ?? 0,
          showSizeChanger: false,
          onChange: (p) => setOffset((p - 1) * TASK_PAGE),
        }}
        columns={[
          { title: '#', dataIndex: 'task_no', width: 60 },
          { title: '动作', dataIndex: 'action', width: 160 },
          {
            title: '状态',
            dataIndex: 'state',
            width: 100,
            render: (s: string) => <Tag color={STATE_COLOR[s] ?? 'default'}>{s}</Tag>,
          },
          { title: '尝试', dataIndex: 'attempt', width: 70 },
          {
            title: '结果',
            render: (_: unknown, t: { result_code: string | null; result_string: string | null }) =>
              [t.result_code, t.result_string].filter(Boolean).join(' · ') || '—',
          },
          { title: '结束时间', dataIndex: 'finished_at', width: 170, render: (v: number | null) => fmtTime(v) },
        ]}
      />
    </Drawer>
  )
}
