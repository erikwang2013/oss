import { Alert, Card, Descriptions, Space, Tag } from 'antd'
import { useQuery } from '@tanstack/react-query'
import { api, type EncryptionStatus } from '../api'
import PageHeader from '../PageHeader'

/** 服务端加密状态（管理员）。默认环只来自环境变量；命名 CMK 可经管理 API 创建/轮换。 */
export default function Settings() {
  const { data, isLoading, error } = useQuery({
    queryKey: ['encryption-status'],
    queryFn: () => api<EncryptionStatus>('/api/v1/encryption'),
  })

  return (
    <>
      <PageHeader title="设置" sub="服务端加密（SSE）与 KMS 状态" />
      <Alert
        type="info"
        showIcon
        style={{ marginBottom: 16 }}
        message="默认密钥由环境变量驱动（OSS_MASTER_KEY_FILE / OSS_KMS_DIR），修改后需重启；命名 CMK 经管理员 API（POST /api/v1/encryption/cmks）创建/轮换，仅新写入用新版本。"
      />
      {error ? (
        <Alert type="error" showIcon message={error.message} />
      ) : (
        <Card loading={isLoading} style={{ maxWidth: 640 }}>
          <Descriptions column={1} bordered>
            <Descriptions.Item label="SSE-S3（主密钥文件）">
              {data?.sse_s3 ? <Tag color="green">已启用</Tag> : <Tag>未启用</Tag>}
            </Descriptions.Item>
            <Descriptions.Item label="SSE-KMS">
              {data?.sse_kms ? <Tag color="green">已启用</Tag> : <Tag>未启用</Tag>}
            </Descriptions.Item>
            <Descriptions.Item label="当前 KMS 主密钥">
              {data?.kms_current_key_id ?? '未配置'}
            </Descriptions.Item>
            <Descriptions.Item label="命名 CMK">
              {data && data.cmks.length > 0 ? (
                <Space direction="vertical" size={2}>
                  {data.cmks.map((c) => (
                    <span key={c.name}>
                      <Tag color="blue">{c.name}</Tag>
                      当前 k{c.current_version}（共 {c.versions.length} 个版本，旧版本保留供解密）
                    </span>
                  ))}
                </Space>
              ) : (
                '无'
              )}
            </Descriptions.Item>
          </Descriptions>
        </Card>
      )}
    </>
  )
}
