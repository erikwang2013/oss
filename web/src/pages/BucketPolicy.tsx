import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { Alert, App, Button, Card, Input, Select, Space, Typography } from 'antd'
import { useMutation, useQuery } from '@tanstack/react-query'
import { api, type PolicyTemplate } from '../api'
import PageHeader from '../PageHeader'

/** 桶策略：JSON 文本域 + 服务端模板；本地 JSON.parse 预检，服务端 400 文案原样显示。 */
export default function BucketPolicy() {
  const { bucket = '' } = useParams()
  const { message } = App.useApp()
  // draft=null 表示「尚未编辑」，直接展示服务端值（避免 effect 回填与级联渲染）
  const [draft, setDraft] = useState<string | null>(null)
  const [templateId, setTemplateId] = useState<string>()
  const [prefix, setPrefix] = useState('')
  const [err, setErr] = useState<string | null>(null)

  const { data, error } = useQuery({
    queryKey: ['bucket-policy', bucket],
    queryFn: () => api<unknown | null>(`/api/v1/buckets/${encodeURIComponent(bucket)}/policy`),
    enabled: !!bucket,
  })
  const { data: templates = [] } = useQuery({
    queryKey: ['policy-templates'],
    queryFn: () => api<PolicyTemplate[]>('/api/v1/policy-templates'),
  })

  const text = draft ?? (data == null ? '' : JSON.stringify(data, null, 2))

  const save = useMutation({
    mutationFn: (body: string) =>
      api(`/api/v1/buckets/${encodeURIComponent(bucket)}/policy`, { method: 'PUT', body }),
    onSuccess: () => {
      message.success('策略已保存')
      setErr(null)
    },
    onError: (e: Error) => setErr(e.message),
  })

  const applyTemplate = () => {
    const tpl = templates.find((t) => t.id === templateId)
    if (!tpl) {
      message.warning('请选择模板')
      return
    }
    const replaced = JSON.stringify(tpl.doc, null, 2)
      .replaceAll('${BUCKET}', bucket)
      .replaceAll('${PREFIX}', prefix)
    setDraft(replaced)
    setErr(null)
  }

  const submit = () => {
    const body = text.trim() || 'null'
    try {
      JSON.parse(body)
    } catch (e) {
      setErr(`JSON 解析失败：${e instanceof Error ? e.message : String(e)}`)
      return
    }
    save.mutate(body)
  }

  return (
    <>
      <PageHeader
        title={`桶策略 · ${bucket}`}
        sub={<Link to={`/buckets/${encodeURIComponent(bucket)}`}>返回对象列表</Link>}
        extra={
          <Button type="primary" loading={save.isPending} onClick={submit}>
            保存策略
          </Button>
        }
      />
      {error && <Alert type="error" showIcon message={error.message} style={{ marginBottom: 12 }} />}
      {err && <Alert type="error" showIcon message={err} style={{ marginBottom: 12 }} closable onClose={() => setErr(null)} />}
      <Card style={{ marginBottom: 16 }}>
        <Space wrap>
          <Select
            value={templateId}
            onChange={setTemplateId}
            placeholder="选择模板"
            style={{ width: 180 }}
            aria-label="策略模板"
            options={templates.map((t) => ({ value: t.id, label: t.name, title: t.description }))}
          />
          <Input
            value={prefix}
            onChange={(e) => setPrefix(e.target.value)}
            placeholder="前缀（替换 ${PREFIX}）"
            aria-label="模板前缀"
            style={{ width: 220 }}
          />
          <Button onClick={applyTemplate}>应用模板</Button>
          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
            BUCKET/PREFIX 占位符自动替换；PRINCIPAL 请按模板说明改为 AK 或 uid:n
          </Typography.Text>
        </Space>
      </Card>
      <Card styles={{ body: { padding: 0 } }}>
        <Input.TextArea
          value={text}
          onChange={(e) => setDraft(e.target.value)}
          rows={20}
          aria-label="桶策略 JSON"
          placeholder='{"Statement": [...]}；留空保存为 null（清除策略）'
          style={{ fontFamily: 'monospace', border: 'none', borderRadius: 8 }}
        />
      </Card>
    </>
  )
}
