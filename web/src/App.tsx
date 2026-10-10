import { Navigate, Outlet, createBrowserRouter } from 'react-router-dom'
import { Layout, Menu } from 'antd'
import { Link, useLocation } from 'react-router-dom'
import { DatabaseOutlined, KeyOutlined } from '@ant-design/icons'
import { getToken } from './api'
import Login from './pages/Login'
import Buckets from './pages/Buckets'
import Objects from './pages/Objects'
import Keys from './pages/Keys'

function Shell() {
  const loc = useLocation()
  const selected = loc.pathname.startsWith('/keys') ? '/keys' : '/buckets'
  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Layout.Sider theme="light">
        <Menu
          mode="inline"
          selectedKeys={[selected]}
          items={[
            { key: '/buckets', icon: <DatabaseOutlined />, label: <Link to="/buckets">桶与对象</Link> },
            { key: '/keys', icon: <KeyOutlined />, label: <Link to="/keys">访问密钥</Link> },
          ]}
        />
      </Layout.Sider>
      <Layout.Content style={{ padding: 24 }}>
        <Outlet />
      </Layout.Content>
    </Layout>
  )
}

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
    ],
  },
])
