import { Navigate, Outlet, createBrowserRouter } from 'react-router-dom'
import { Layout, Menu } from 'antd'
import { Link, useLocation } from 'react-router-dom'
import { DatabaseOutlined, KeyOutlined, TeamOutlined } from '@ant-design/icons'
import { useQuery } from '@tanstack/react-query'
import { api, getToken, type Me } from './api'
import Mascot from './Mascot'
import Login from './pages/Login'
import Buckets from './pages/Buckets'
import Objects from './pages/Objects'
import Keys from './pages/Keys'
import Users from './pages/Users'

// Shell/RequireAuth 与 router（本文件导出）同文件：Fast Refresh 对它只会整页刷新，
// 拆文件只增样板，接受这一取舍
// oxlint-disable-next-line react/only-export-components
function Shell() {
  const loc = useLocation()
  // 角色唯一来源：/me（JWT 里只有 sub；401 由 api.ts 全局处理，其余错误由 QueryCache 桥接）
  const { data: me } = useQuery({ queryKey: ['me'], queryFn: () => api<Me>('/api/v1/me') })
  // 选中态：按前缀匹配——三分支起手写三元会在新路由上误亮 /buckets
  const selected = ['/keys', '/users'].find((p) => loc.pathname.startsWith(p)) ?? '/buckets'
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="light">
        {/* 侧栏品牌位：吉祥物当 logo（导航项本身保留语义图标——14px 的小脸认不出来） */}
        <div style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '14px 16px 6px' }}>
          <Mascot mood="happy" size={28} alt="OSS 控制台" />
          <span style={{ fontWeight: 600, fontSize: 15 }}>OSS 控制台</span>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[selected]}
          items={[
            { key: '/buckets', icon: <DatabaseOutlined />, label: <Link to="/buckets">桶与对象</Link> },
            { key: '/keys', icon: <KeyOutlined />, label: <Link to="/keys">访问密钥</Link> },
            // 子账户管理仅管理员可见（后端另有 403 兜底）
            ...(me?.is_admin
              ? [{ key: '/users', icon: <TeamOutlined />, label: <Link to="/users">子账户</Link> }]
              : []),
          ]}
        />
      </Layout.Sider>
      <Layout.Content style={{ padding: 24 }}>
        <Outlet />
      </Layout.Content>
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
    ],
  },
])
