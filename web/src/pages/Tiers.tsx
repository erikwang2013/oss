import { App, Button, Card, Table, Tag } from 'antd'
import { CloudUploadOutlined } from '@ant-design/icons'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { api, type TierInfo } from '../api'
import PageHeader from '../PageHeader'

/** `[[tiers]]` 只读状态（管理员）：healthy = 哨兵键 HEAD 可达；凭证不投影。 */
export default function Tiers() {
  const { message } = App.useApp()
  const qc = useQueryClient()

  const { data: tiers = [], isLoading, isFetching, refetch } = useQuery({
    queryKey: ['tiers'],
    queryFn: () => api<TierInfo[]>('/api/v1/tiers'),
  })

  const sweep = useMutation({
    mutationFn: () => api<{ queued: number }>('/api/v1/tiers/sweep', { method: 'POST' }),
    onSuccess: (r) => {
      message.success(`已入队 ${r.queued} 个 Transition 任务`)
      qc.invalidateQueries({ queryKey: ['jobs'] })
    },
    onError: (e: Error) => message.error(e.message),
  })

  return (
    <>
      <PageHeader
        title="存储层"
        sub="按 [[tiers]] 配置的远端存储与健康状态（哨兵键探活）"
        extra={
          <>
            <Button onClick={() => refetch()} loading={isFetching}>刷新</Button>
            <Button
              type="primary"
              icon={<CloudUploadOutlined />}
              loading={sweep.isPending}
              onClick={() => sweep.mutate()}
            >
              强制 sweep
            </Button>
          </>
        }
      />
      <Card styles={{ body: { padding: 0 } }}>
        <Table<TierInfo>
          rowKey="id"
          loading={isLoading}
          dataSource={tiers}
          columns={[
            { title: 'ID', dataIndex: 'id', width: 140 },
            { title: 'Endpoint', dataIndex: 'endpoint' },
            { title: '远端桶', dataIndex: 'bucket', width: 200 },
            {
              title: '健康',
              dataIndex: 'healthy',
              width: 110,
              render: (v: boolean) =>
                v ? <Tag color="green">可达</Tag> : <Tag color="red">不可达</Tag>,
            },
          ]}
        />
      </Card>
    </>
  )
}
