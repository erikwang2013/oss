import { useState } from 'react'
import { Alert, App, Button, Modal, Table, Typography } from 'antd'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type AccessKey } from '../api'

export default function Keys() {
  const { message } = App.useApp()
  const qc = useQueryClient()
  const [fresh, setFresh] = useState<AccessKey | null>(null)

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

  return (
    <>
      <Button type="primary" loading={create.isPending} onClick={() => create.mutate()} style={{ marginBottom: 16 }}>
        新建访问密钥
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
      <Modal title="密钥已创建" open={!!fresh} footer={null} onCancel={() => setFresh(null)}>
        {fresh && (
          <>
            <Alert type="warning" showIcon style={{ marginBottom: 12 }}
              message="Secret Key 只显示这一次，请立即保存（M5 前它是明文存库的）。" />
            <Typography.Paragraph copyable>Access Key：{fresh.access_key}</Typography.Paragraph>
            <Typography.Paragraph copyable>Secret Key：{fresh.secret_key}</Typography.Paragraph>
          </>
        )}
      </Modal>
    </>
  )
}
