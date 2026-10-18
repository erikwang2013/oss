import { Navigate, Outlet, createBrowserRouter } from 'react-router-dom'
import { Avatar, Dropdown, Flex, Layout, Menu, Tag, Typography } from 'antd'
import { Link, useLocation, useNavigate } from 'react-router-dom'
import { ClusterOutlined, DatabaseOutlined, KeyOutlined, LogoutOutlined, TeamOutlined, UserOutlined } from '@ant-design/icons'
import { useQuery } from '@tanstack/react-query'
import { api, getToken, setToken, type Me } from './api'
import Mascot from './Mascot'
import Login from './pages/Login'
import Buckets from './pages/Buckets'
import Objects from './pages/Objects'
import Keys from './pages/Keys'
import Users from './pages/Users'
import Cluster from './pages/Cluster'

// Shell/RequireAuth 与 router（本文件导出）同文件：Fast Refresh 对它只会整页刷新，
// 拆文件只增样板，接受这一取舍
// oxlint-disable-next-line react/only-export-components
function Shell() {
  const loc = useLocation()
  const nav = useNavigate()
  // 角色唯一来源：/me（JWT 里只有 sub；401 由 api.ts 全局处理，其余错误由 QueryCache 桥接）
  const { data: me } = useQuery({ queryKey: ['me'], queryFn: () => api<Me>('/api/v1/me') })
  // 选中态：按前缀匹配——三分支起手写三元会在新路由上误亮 /buckets
  const selected = ['/keys', '/users', '/cluster'].find((p) => loc.pathname.startsWith(p)) ?? '/buckets'

  const logout = () => {
    setToken(null) // localStorage 与 cookie 同清（api.ts 的 setToken 契约）
    nav('/login', { replace: true })
  }

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="light" width={216} style={{ borderRight: '1px solid #eef1f5' }}>
        {/* 侧栏品牌位：吉祥物当 logo（导航项本身保留语义图标——14px 的小脸认不出来） */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 10, padding: '16px 20px 10px' }}>
          <Mascot mood="smile" size={32} alt="OSS 控制台" />
          <span style={{ fontWeight: 700, fontSize: 16 }}>OSS 控制台</span>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[selected]}
          style={{ borderInlineEnd: 'none' }}
          items={[
            { key: '/buckets', icon: <DatabaseOutlined />, label: <Link to="/buckets">桶与对象</Link> },
            { key: '/keys', icon: <KeyOutlined />, label: <Link to="/keys">访问密钥</Link> },
            // 子账户管理与集群面板仅管理员可见（后端另有 403 兜底）
            ...(me?.is_admin
              ? [
                  { key: '/users', icon: <TeamOutlined />, label: <Link to="/users">子账户</Link> },
                  { key: '/cluster', icon: <ClusterOutlined />, label: <Link to="/cluster">集群</Link> },
                ]
              : []),
          ]}
        />
      </Layout.Sider>
      <Layout>
        {/* 顶栏：当前用户 + 退出登录（此前 UI 没有登出入口，切账号只能清浏览器存储） */}
        <Layout.Header
          style={{
            borderBottom: '1px solid #eef1f5',
            paddingInline: 24,
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-end',
          }}
        >
          <Dropdown
            placement="bottomRight"
            menu={{
              items: [
                {
                  key: 'who',
                  disabled: true,
                  label: (
                    <Typography.Text type="secondary">
                      {me ? `${me.username}${me.is_admin ? ' · 管理员' : ' · 子账户'}` : '加载中…'}
                    </Typography.Text>
                  ),
                },
                { type: 'divider' },
                { key: 'logout', icon: <LogoutOutlined />, label: '退出登录', onClick: logout },
              ],
            }}
          >
            <Flex align="center" gap={8} style={{ cursor: 'pointer', paddingInline: 4 }}>
              <Avatar size={30} style={{ background: '#3b82f6' }} icon={me ? undefined : <UserOutlined />}>
                {me?.username?.[0]?.toUpperCase()}
              </Avatar>
              <span style={{ fontWeight: 500 }}>{me?.username ?? ''}</span>
              {me?.is_admin && <Tag color="gold" style={{ marginInlineEnd: 0 }}>管理员</Tag>}
            </Flex>
          </Dropdown>
        </Layout.Header>
        <Layout.Content style={{ padding: 24 }}>
          {/* 内容定宽居中：宽屏下表格不横贯整个视口 */}
          <div style={{ maxWidth: 1160, margin: '0 auto' }}>
            <Outlet />
          </div>
        </Layout.Content>
      </Layout>
    </Layout>
  )
}

// oxlint-disable-next-line react/only-export-components
function RequireAuth() {
  if (!getToken()) return <Navigate to="/login" replace />
  return <Shell />
}

export const router = createBrowserRouter([
  { path: '/login', element: <Login /> },
  {
    element: <RequireAuth />,
    children: [
      { path: '/', element: <Navigate to="/buckets" replace /> },
      { path: '/buckets', element: <Buckets /> },
      { path: '/buckets/:bucket', element: <Objects /> },
      { path: '/keys', element: <Keys /> },
      { path: '/users', element: <Users /> },
      { path: '/cluster', element: <Cluster /> },
    ],
  },
])
