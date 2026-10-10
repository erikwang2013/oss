import { useRef, useState } from 'react'
import { App, Button, Modal, Select, Space, Table, Tag, Typography } from 'antd'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type GroupInfo, type UploadRecord } from '../api'
import { humanSize } from '../format'
import { useUpload, type UploadOutcome } from './useUpload'

export default function UploadPanel({
  open,
  bucket,
  onClose,
}: {
  open: boolean
  bucket: string
  onClose: () => void
}) {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [group, setGroup] = useState('file')
  const [last, setLast] = useState<UploadOutcome | null>(null)
  const fileRef = useRef<HTMLInputElement>(null)
  // onFilePicked 会清空 input.value（让同一文件再选也触发 onChange），清空后 files 为空——
  // 原始文件名必须选文件当刻留档，onFinished（异步）里再读就已经是 '' 了
  const originalNameRef = useRef('')

  const { data: groups = [] } = useQuery({
    queryKey: ['groups'],
    queryFn: () => api<GroupInfo[]>('/api/v1/groups'),
    enabled: open,
  })

  const { data: history = [] } = useQuery({
    queryKey: ['uploads'],
    queryFn: () => api<UploadRecord[]>('/api/v1/uploads'),
    enabled: open,
    refetchInterval: open ? 3000 : false,
  })

  const { start } = useUpload({
    onFinished: async (outcome) => {
      setLast(outcome)
      message.success(outcome.instant ? '上传成功（秒传）' : '上传成功')
      if (outcome.savedPath) {
        await api('/api/v1/uploads/register', {
          method: 'POST',
          body: JSON.stringify({ saved_path: outcome.savedPath, original_name: originalNameRef.current }),
        }).catch(() => message.warning('上传记录登记失败（对象本身已入库）'))
      }
      qc.invalidateQueries({ queryKey: ['objects', bucket] })
      qc.invalidateQueries({ queryKey: ['uploads'] })
    },
  })

  // 让「同一文件再选一次」也能触发 onChange（秒传验证要用）
  const onFilePicked = (e: React.ChangeEvent<HTMLInputElement>) => {
    const input = e.target
    if (!input.files?.[0]) return
    originalNameRef.current = input.files[0].name
    setLast(null)
    start(input, group)
    input.value = ''
  }

  return (
    <Modal title={`上传到 ${bucket}`} open={open} onCancel={onClose} footer={null} width={640} destroyOnHidden>
      <Space direction="vertical" style={{ width: '100%' }} size="middle">
        <Space>
          <Select
            value={group}
            onChange={setGroup}
            style={{ width: 280 }}
            options={groups.map((g) => ({
              value: g.group,
              label: `${g.group} → ${g.bucket}${g.max_size ? `（≤ ${humanSize(g.max_size)}）` : ''}`,
            }))}
          />
          <Button onClick={() => fileRef.current?.click()} loading={!window.aetherupload ? true : undefined}>
            选择文件并开始上传
          </Button>
        </Space>

        {/*
          aetherupload-all.js 的固定 DOM 契约：wrapper 下这四个 id 必须存在，
          内容（输出文案、进度条宽度、savedPath）全部由脚本直接改。
          React 只渲染一次空节点，此后不再碰它们 —— 不要在这里放受控内容。
        */}
        <div id="aetherupload-wrapper">
          <input
            ref={fileRef}
            id="aetherupload-resource"
            type="file"
            style={{ display: 'none' }}
            onChange={onFilePicked}
          />
          <div style={{ height: 8, background: '#f0f0f0', borderRadius: 4, overflow: 'hidden' }}>
            <div
              id="aetherupload-progressbar"
              style={{ height: '100%', width: '0%', background: '#1677ff', transition: 'width .2s' }}
            />
          </div>
          <div id="aetherupload-output" style={{ minHeight: 22, color: '#666', marginTop: 8 }} />
          <input id="aetherupload-savedpath" type="hidden" />
        </div>

        {last && (
          <Tag color={last.instant ? 'gold' : 'green'}>
            {last.instant ? '秒传命中（0 分块传输）' : '分块上传完成'}
          </Tag>
        )}
        <Typography.Text type="secondary">
          断网/弱网时脚本会自动重试当前分块（服务端对重发分块幂等），进度条会继续推进——这就是断线续传。
          M1 的续传限于同一页面会话：刷新页面后需重新上传。
        </Typography.Text>

        <Table
          size="small"
          rowKey="saved_path"
          dataSource={history}
          pagination={{ pageSize: 5 }}
          columns={[
            { title: '文件', dataIndex: 'original_name', render: (v: string | null, r) => v ?? r.key.split('/').pop() },
            { title: '桶/键', render: (_, r) => `${r.bucket}/${r.key}` },
            { title: '大小', dataIndex: 'size', render: humanSize },
            { title: '时间', dataIndex: 'created_at', render: (t: number) => new Date(t * 1000).toLocaleString() },
          ]}
        />
      </Space>
    </Modal>
  )
}
