import '@ant-design/v5-patch-for-react-19' // antd v5 的 React 19 兼容层，须在任何 antd 调用前 import
import React from 'react'
import ReactDOM from 'react-dom/client'
import { RouterProvider } from 'react-router-dom'
import { QueryCache, QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { App as AntApp, ConfigProvider } from 'antd'
import { ApiError } from './api'
// 走 antd/es 的 ESM 源：`antd/locale/zh_CN`（CJS 包）经 Vite 8 预打包后默认导出是 { default: locale } 双层，
// ConfigProvider 取不到 Modal.okText 等字段，Modal 会退回英文 OK/Cancel
import zhCN from 'antd/es/locale/zh_CN'
import 'antd/dist/reset.css'
import { router } from './App'

// QueryCache.onError 在 React 树外拿不到 antd 的 message 实例：挂载一个空组件把
// message.error 注册到模块单例做桥接（全局仅挂一次，StrictMode 下重复赋值也无副作用）
let notifyError: (msg: string) => void = () => {}
// 入口文件无导出、不涉及 Fast Refresh；此组件只为把 message 注册进桥接单例
// oxlint-disable-next-line react/only-export-components
function GlobalNotifier() {
  const { message } = AntApp.useApp()
  React.useEffect(() => {
    notifyError = (msg: string) => { message.error(msg) }
    return () => { notifyError = () => {} }
  }, [message])
  return null
}

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
  queryCache: new QueryCache({
    onError: (err) => {
      // 401 已由 api 层清 token + 跳登录，静默；其余查询错误要可见——否则查询失败与「暂无数据」在页面上无从区分
      if (err instanceof ApiError && err.status === 401) return
      notifyError(err.message || '请求失败')
    },
  }),
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider locale={zhCN}>
      <AntApp>
        <GlobalNotifier />
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </AntApp>
    </ConfigProvider>
  </React.StrictMode>,
)
