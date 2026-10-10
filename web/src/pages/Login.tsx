import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { App, Button, Card, Form, Input } from 'antd'
import { api, setToken } from '../api'

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
      // aetherupload-all.js 的分块 XHR 带 withCredentials，但不会加 Authorization 头；
      // 后端 /aetherupload/* 的 JWT 覆盖层认 cookie `oss_token`——不种 cookie 浏览器上传 401
      document.cookie = `oss_token=${token}; path=/; SameSite=Lax`
      nav('/buckets', { replace: true })
    } catch (e) {
      message.error(e instanceof Error && e.message ? e.message : '登录失败')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div style={{ display: 'flex', justifyContent: 'center', paddingTop: 120 }}>
      <Card title="OSS 控制台" style={{ width: 360 }}>
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
