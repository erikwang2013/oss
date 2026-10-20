import { Alert, Card, Descriptions, Tag } from 'antd'
import { useQuery } from '@tanstack/react-query'
import { api, type EncryptionStatus } from '../api'
import PageHeader from '../PageHeader'

/** 服务端加密只读状态（管理员）。配置只来自环境变量，无变更端点。 */
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
        message="配置由环境变量驱动（OSS_MASTER_KEY_FILE / OSS_KMS_DIR），修改后需重启服务；控制台不提供变更端点。"
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
          </Descriptions>
        </Card>
      )}
    </>
  )
}
