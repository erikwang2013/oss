import type { ReactNode } from 'react'
import { Flex, Typography } from 'antd'

/**
 * 页面级标题行：左侧标题 + 可选副标题，右侧操作区（按钮组）。
 * 四个管理页共用，保证标题层级与间距一致（Console 页面的统一门面）。
 */
export default function PageHeader({
  title,
  sub,
  extra,
}: {
  title: ReactNode
  sub?: ReactNode
  extra?: ReactNode
}) {
  return (
    <Flex justify="space-between" align="center" gap={12} wrap style={{ marginBottom: 16 }}>
      <div>
        <Typography.Title level={4} style={{ margin: 0 }}>
          {title}
        </Typography.Title>
        {sub && (
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            {sub}
          </Typography.Text>
        )}
      </div>
      {extra && <Flex gap={8} wrap>{extra}</Flex>}
    </Flex>
  )
}
