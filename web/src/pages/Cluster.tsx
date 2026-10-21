import { Button, Card, Col, Flex, Row, Statistic, Table, Tag, Typography } from 'antd'
import { ReloadOutlined } from '@ant-design/icons'
import { useQuery } from '@tanstack/react-query'
import { api, type ClusterStatus, type RaftStatus } from '../api'
import PageHeader from '../PageHeader'
import Mascot from '../Mascot'

/** 集群面板（管理员）：本节点视角的拓扑快照、可达性与 quorum 计数（后端探测 800ms/节点）。 */
export default function Cluster() {
  const { data, isLoading, isError, isFetching, refetch } = useQuery({
    queryKey: ['cluster-status'],
    queryFn: () => api<ClusterStatus>('/api/v1/cluster/status'),
    refetchInterval: 15000, // 面板自刷新：探测成本 = 每 peer 一次 800ms 上限的 health RPC
  })

  const { data: raft } = useQuery({
    queryKey: ['raft-status'],
    queryFn: () => api<RaftStatus>('/api/v1/raft/status'),
    refetchInterval: 15000,
  })
  const isCluster = data?.mode === 'cluster'
  // 无数据 ≠ 单机：403（非管理员直访）/加载中都不能断言部署形态
  const subtitle = isCluster
    ? '本节点视角的拓扑与可达性'
    : isError
      ? '需要管理员权限'
      : data
        ? '单机模式（oss.toml 未配置 [cluster]）'
        : '加载中…'
  const quorum = data ? `${data.quorum_unmet.write} / ${data.quorum_unmet.lock}` : '—'

  return (
    <>
      <PageHeader
        title="集群"
        sub={subtitle}
        extra={
          <Button icon={<ReloadOutlined />} loading={isFetching} onClick={() => refetch()}>
            刷新
          </Button>
        }
      />
      <Row gutter={[16, 16]}>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic title="模式" value={isCluster ? '集群' : data?.mode === 'single' ? '单机' : '—'} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic title="全局槽位" value={data?.slot_count ?? '—'} />
          </Card>
        </Col>
        <Col xs={24} sm={8}>
          <Card>
            <Statistic title="quorum 未达（写 / 锁）" value={quorum} />
          </Card>
        </Col>
      </Row>

      {raft?.enabled && (
        <Card title="Raft 控制面（数据站点 + witness）" style={{ marginTop: 16 }}>
          <Row gutter={[16, 16]}>
            <Col xs={12} sm={6}>
              <Statistic title="角色" value={raft.role ?? '—'} />
            </Col>
            <Col xs={12} sm={6}>
              <Statistic title="leader" value={raft.leader ?? '—'} />
            </Col>
            <Col xs={12} sm={6}>
              <Statistic title="term / applied" value={`${raft.term ?? '—'} / ${raft.applied ?? '—'}`} />
            </Col>
            <Col xs={12} sm={6}>
              <Statistic
                title="voters / production"
                value={`${raft.voters?.length ?? 0} / ${raft.production_ready ? 'ready' : 'single'}`}
              />
            </Col>
          </Row>
          <Flex gap={8} wrap style={{ marginTop: 12 }}>
            <Tag color="blue">voters: {(raft.voters ?? []).join(', ') || '—'}</Tag>
            <Tag>learners: {(raft.learners ?? []).join(', ') || '—'}</Tag>
            <Tag color={raft.capabilities?.sync ? 'green' : 'default'}>
              sync {raft.capabilities?.sync ? 'on' : 'off'}
            </Tag>
            <Tag color={raft.capabilities?.stretch ? 'green' : 'default'}>
              stretch {raft.capabilities?.stretch ? 'on' : 'off'}
            </Tag>
          </Flex>
          <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
            witness 只存控制面 11 表、不存对象；production_ready 需 ≥3 voters。
          </Typography.Text>
        </Card>
      )}

      <Card title="节点" style={{ marginTop: 16 }} styles={{ body: { padding: 0 } }}>
        <Table
          rowKey="id"
          loading={isLoading}
          dataSource={data?.nodes ?? []}
          pagination={false}
          columns={[
            { title: '节点', dataIndex: 'id' },
            { title: '地址', dataIndex: 'addr' },
            { title: '盘数', dataIndex: 'drive_count', width: 90 },
            {
              title: '状态',
              dataIndex: 'up',
              width: 110,
              render: (up: boolean) =>
                up ? <Tag color="green">可达</Tag> : <Tag color="red">不可达</Tag>,
            },
          ]}
        />
      </Card>

      {isCluster && (data?.sets?.length ?? 0) > 1 && (
        <Card title="set 分组（多 set）" style={{ marginTop: 16 }} styles={{ body: { padding: 0 } }}>
          <Table
            rowKey="set"
            dataSource={data?.sets ?? []}
            pagination={false}
            columns={[
              {
                title: 'set',
                dataIndex: 'set',
                width: 80,
                render: (v: number) => <Tag color="blue">set {v}</Tag>,
              },
              { title: '槽位', dataIndex: 'slot_count', width: 100 },
              {
                title: '本节点',
                dataIndex: 'self',
                width: 100,
                render: (v: boolean) =>
                  v ? <Tag color="green">在册</Tag> : <Tag>不在册</Tag>,
              },
              {
                title: '节点',
                dataIndex: 'nodes',
                render: (nodes: { id: string; drive_count: number }[]) => (
                  <Flex gap={6} wrap>
                    {nodes.map((n) => (
                      <Tag key={n.id}>
                        {n.id} × {n.drive_count}
                      </Tag>
                    ))}
                  </Flex>
                ),
              },
            ]}
          />
          <Typography.Text
            type="secondary"
            style={{ display: 'block', padding: '8px 16px', fontSize: 12 }}
          >
            各 set 槽号是独立槽域、跨 set 会重号——按 set 分组读；桶创建时钉定 set，跨 set 不搬迁。
          </Typography.Text>
        </Card>
      )}

      {isCluster && data?.topo_fingerprint && (
        <Typography.Text type="secondary" style={{ display: 'block', marginTop: 8, fontSize: 12 }}>
          拓扑指纹：{data.topo_fingerprint}（本节点首盘 .oss.sys/topo 记录，逐盘 slot 递增；不一致即拒启）
        </Typography.Text>
      )}
      {data?.mode === 'single' && (
        <Flex align="center" gap={12} style={{ marginTop: 16 }}>
          <Mascot mood="smile" size={40} alt="单机模式" />
          <Typography.Text type="secondary">
            单机部署没有集群拓扑；配置 [cluster] 后此处显示节点表、可达性与指纹。
          </Typography.Text>
        </Flex>
      )}
    </>
  )
}
