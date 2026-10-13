import { useState } from 'react'
import { Alert, App, Button, Input, Modal, Table, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type AccessKey } from '../api'

export default function Keys() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [fresh, setFresh] = useState<AccessKey | null>(null)
  const [changing, setChanging] = useState(false)
  const [pw, setPw] = useState({ old: '', new1: '', new2: '' })

  const { data: keys = [], isLoading } = useQuery({
    queryKey: ['access-keys'],
    queryFn: () => api<string[]>('/api/v1/access-keys'),
  })

  const create = useMutation({
    mutationFn: () =>
      api<AccessKey>('/api/v1/access-keys', { method: 'POST', body: JSON.stringify({}) }),
    onSuccess: (k) => {
      setFresh(k)
      qc.invalidateQueries({ queryKey: ['access-keys'] })
    },
    onError: (e: Error) => message.error(e.message),
  })

  const changePw = useMutation({
    mutationFn: (body: { old_password: string; new_password: string }) =>
      api('/api/v1/me/password', { method: 'POST', body: JSON.stringify(body) }),
    onSuccess: () => { message.success('口令已修改'); setChanging(false) },
    onError: (e: Error) => message.error(e.message),
  })

  const close = () => {
    setFresh(null)
    create.reset() // 关窗即清 MutationCache 里的 data：SK 只该活在弹窗这一个副本里
  }

  return (
    <>
      <Button type="primary" loading={create.isPending} onClick={() => create.mutate()} style={{ marginBottom: 16 }}>
        新建访问密钥
      </Button>
      <Button onClick={() => { setPw({ old: '', new1: '', new2: '' }); setChanging(true) }} style={{ marginLeft: 8, marginBottom: 16 }}>
        修改口令
      </Button>
      <Table
        rowKey={(r) => r}
        loading={isLoading}
        dataSource={keys}
        columns={[
          // 不用 dataIndex：antd 对 string[] 记录取 dataIndex:0 会拿到首字符；无 dataIndex 时 render 第一参即整条记录
          { title: 'Access Key', render: (ak: string) => ak },
          { title: '说明', render: () => '启用/禁用与过期在 M5；M1 只有创建与使用' },
        ]}
      />
      <Modal title="密钥已创建" open={!!fresh} footer={null} maskClosable={false} onCancel={close}>
        {fresh && (
          <>
            <Alert type="warning" showIcon style={{ marginBottom: 12 }}
              message="Secret Key 只显示这一次，请立即保存（M5 前它是明文存库的）。" />
            {/* copyable={{ text }} 显式给裸值：裸 copyable 会连「Access Key：」前缀一起复制 */}
            <Typography.Paragraph copyable={{ text: fresh.access_key }}>Access Key：{fresh.access_key}</Typography.Paragraph>
            <Typography.Paragraph copyable={{ text: fresh.secret_key }}>Secret Key：{fresh.secret_key}</Typography.Paragraph>
          </>
        )}
      </Modal>
      <Modal
        title="修改口令"
        open={changing}
        onCancel={() => setChanging(false)}
        confirmLoading={changePw.isPending}
        onOk={() => {
          if (pw.new1.length < 8) { message.warning('新口令至少 8 位'); return }
          if (pw.new1 !== pw.new2) { message.warning('两次输入的新口令不一致'); return }
          changePw.mutate({ old_password: pw.old, new_password: pw.new1 })
        }}
      >
        <Input.Password
          value={pw.old}
          onChange={(e) => setPw((s) => ({ ...s, old: e.target.value }))}
          placeholder="当前口令"
          autoFocus
          style={{ marginBottom: 12 }}
        />
        <Input.Password
          value={pw.new1}
          onChange={(e) => setPw((s) => ({ ...s, new1: e.target.value }))}
          placeholder="新口令（至少 8 位）"
          style={{ marginBottom: 12 }}
        />
        <Input.Password
          value={pw.new2}
          onChange={(e) => setPw((s) => ({ ...s, new2: e.target.value }))}
          placeholder="再次输入新口令"
        />
      </Modal>
    </>
  )
}
