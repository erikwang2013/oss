import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { App, Button, Card, Form, Input } from 'antd'
import { api, setToken } from '../api'
import Mascot from '../Mascot'

export default function Login() {
  const nav = useNavigate()
  const { message } = App.useApp()
  const [loading, setLoading] = useState(false)

  const onFinish = async (v: { username: string; password: string }) => {
    setLoading(true)
    try {
      const { token } = await api<{ token: string }>('/api/v1/login', {
        method: 'POST',
        body: JSON.stringify(v),
      })
      setToken(token)
      nav('/buckets', { replace: true })
    } catch (e) {
      message.error({
        content: e instanceof Error && e.message ? e.message : '登录失败',
        icon: <Mascot mood="shocked" size={22} alt="" />,
      })
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'center', paddingTop: 96 }}>
      <Mascot mood="happy" size={112} alt="OSS 控制台吉祥物" />
      <Card title="OSS 控制台" style={{ width: 360, marginTop: 8 }}>
        <Form onFinish={onFinish} layout="vertical">
          <Form.Item name="username" label="用户名" rules={[{ required: true }]}>
            <Input autoFocus />
          </Form.Item>
          <Form.Item name="password" label="口令" rules={[{ required: true }]}>
            <Input.Password />
          </Form.Item>
          <Button type="primary" htmlType="submit" block loading={loading}>
            登录
          </Button>
        </Form>
      </Card>
    </div>
  )
}
