import '@ant-design/v5-patch-for-react-19' // antd v5 的 React 19 兼容层，须在任何 antd 调用前 import
import React from 'react'
import ReactDOM from 'react-dom/client'
import { RouterProvider } from 'react-router-dom'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { App as AntApp, ConfigProvider } from 'antd'
// 走 antd/es 的 ESM 源：`antd/locale/zh_CN`（CJS 包）经 Vite 8 预打包后默认导出是 { default: locale } 双层，
// ConfigProvider 取不到 Modal.okText 等字段，Modal 会退回英文 OK/Cancel
import zhCN from 'antd/es/locale/zh_CN'
import 'antd/dist/reset.css'
import { router } from './App'

const queryClient = new QueryClient({
  defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } },
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <ConfigProvider locale={zhCN}>
      <AntApp>
        <QueryClientProvider client={queryClient}>
          <RouterProvider router={router} />
        </QueryClientProvider>
      </AntApp>
    </ConfigProvider>
  </React.StrictMode>,
)
