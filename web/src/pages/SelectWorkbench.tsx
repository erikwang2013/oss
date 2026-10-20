import { useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import {
  Alert,
  Button,
  Card,
  Form,
  Input,
  InputNumber,
  Select,
  Space,
  Table,
  Typography,
} from 'antd'
import { useMutation } from '@tanstack/react-query'
import { api, type SelectResult } from '../api'
import PageHeader from '../PageHeader'

type Row = Record<string, unknown>

function renderCell(v: unknown) {
  if (v == null) return '—'
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

/** CSV 输出是 string[][]（按列序号），JSON 输出是对象数组（按键并集）——两种都支持。 */
function columnsFor(records: unknown[]) {
  const first = records[0]
  // JSON 输出可能是标量数组（如 SELECT count(*)），按单列渲染
  if (first !== undefined && (first === null || typeof first !== 'object')) {
    return [{ title: '值', render: (_: unknown, r: Row) => renderCell(r.__row) }]
  }
  if (first && typeof first === 'object' && !Array.isArray(first)) {
    const keys = Array.from(
      new Set(records.flatMap((r) => (r && typeof r === 'object' ? Object.keys(r) : []))),
    )
    return keys.map((k) => ({
      title: k,
      render: (_: unknown, r: Row) => renderCell((r.__row as Record<string, unknown> | undefined)?.[k]),
    }))
  }
  const width = records.reduce<number>(
    (n, r) => Math.max(n, Array.isArray(r) ? r.length : 1),
    0,
  )
  return Array.from({ length: width }, (_, i) => ({
    title: `第 ${i + 1} 列`,
    render: (_: unknown, r: Row) => renderCell((r.__row as unknown[] | undefined)?.[i]),
  }))
}

/** 一次性 Select 代理：表单 → POST /buckets/{bucket}/select → records + stats。 */
export default function SelectWorkbench() {
  const { bucket = '' } = useParams()
  const [form] = Form.useForm()
  const inFormat = Form.useWatch('in_format', form) as string | undefined
  const outFormat = Form.useWatch('out_format', form) as string | undefined
  const [err, setErr] = useState<string | null>(null)

  const run = useMutation({
    mutationFn: (body: Record<string, unknown>) =>
      api<SelectResult>(`/api/v1/buckets/${encodeURIComponent(bucket)}/select`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => setErr(null),
    onError: (e: Error) => setErr(e.message),
  })

  const submit = async () => {
    const v = await form.validateFields()
    const input =
      v.in_format === 'CSV'
        ? {
            format: 'CSV',
            csv: {
              field_delimiter: v.in_delimiter || undefined,
              quote_character: v.in_quote || undefined,
              file_header_info: v.in_header,
              comments: v.in_comments || undefined,
            },
          }
        : { format: 'JSON', json: { type: v.in_json_type } }
    const output =
      v.out_format === 'CSV'
        ? {
            format: 'CSV',
            csv: {
              field_delimiter: v.out_delimiter || undefined,
              quote_character: v.out_quote || undefined,
              quote_fields: v.out_quote_fields,
            },
          }
        : { format: 'JSON' }
    const body: Record<string, unknown> = {
      key: v.key,
      expression: v.expression,
      input,
      output,
    }
    if (v.version_id) body.version_id = v.version_id
    if (v.scan_start != null || v.scan_end != null) {
      body.scan_range = { start: v.scan_start ?? undefined, end: v.scan_end ?? undefined }
    }
    run.mutate(body)
  }

  const records = run.data?.records ?? []
  const rows: Row[] = records.map((r, i) => ({ __k: i, __row: r }))

  return (
    <>
      <PageHeader
        title={`Select · ${bucket}`}
        sub={<Link to={`/buckets/${encodeURIComponent(bucket)}`}>返回对象列表</Link>}
        extra={
          <Button type="primary" loading={run.isPending} onClick={submit}>
            执行
          </Button>
        }
      />
      {err && <Alert type="error" showIcon message={err} style={{ marginBottom: 12 }} closable onClose={() => setErr(null)} />}
      <Card style={{ marginBottom: 16 }}>
        <Form
          form={form}
          layout="vertical"
          initialValues={{
            expression: 'SELECT * FROM S3Object',
            in_format: 'CSV',
            in_delimiter: ',',
            in_quote: '"',
            in_comments: '#',
            in_header: 'NONE',
            in_json_type: 'DOCUMENT',
            out_format: 'CSV',
            out_delimiter: ',',
            out_quote: '"',
            out_quote_fields: 'ASNEEDED',
          }}
        >
          <Space wrap size={12} align="start">
            <Form.Item name="key" label="对象键" rules={[{ required: true }]} style={{ width: 280 }}>
              <Input placeholder="path/to/data.csv" />
            </Form.Item>
            <Form.Item name="version_id" label="版本号（可选）" style={{ width: 180 }}>
              <Input placeholder="缺省 = 链尾" />
            </Form.Item>
            <Form.Item label="输入格式">
              <Form.Item name="in_format" noStyle>
                <Select style={{ width: 100 }} options={[{ value: 'CSV' }, { value: 'JSON' }]} />
              </Form.Item>
            </Form.Item>
            <Form.Item label="输出格式">
              <Form.Item name="out_format" noStyle>
                <Select style={{ width: 100 }} options={[{ value: 'CSV' }, { value: 'JSON' }]} />
              </Form.Item>
            </Form.Item>
          </Space>
          <Form.Item name="expression" label="SQL 表达式" rules={[{ required: true }]}>
            <Input.TextArea rows={3} style={{ fontFamily: 'monospace' }} />
          </Form.Item>

          {inFormat === 'CSV' ? (
            <Space wrap size={12} align="start">
              <Form.Item name="in_delimiter" label="字段分隔符">
                <Input style={{ width: 90 }} />
              </Form.Item>
              <Form.Item name="in_quote" label="引号字符">
                <Input style={{ width: 90 }} />
              </Form.Item>
              <Form.Item name="in_header" label="首行">
                <Select
                  style={{ width: 120 }}
                  options={[
                    { value: 'NONE', label: 'NONE' },
                    { value: 'USE', label: 'USE（表头）' },
                    { value: 'IGNORE', label: 'IGNORE（跳过）' },
                  ]}
                />
              </Form.Item>
              <Form.Item name="in_comments" label="注释字符">
                <Input style={{ width: 90 }} />
              </Form.Item>
            </Space>
          ) : (
            <Form.Item name="in_json_type" label="JSON 类型">
              <Select
                style={{ width: 180 }}
                options={[
                  { value: 'DOCUMENT', label: 'DOCUMENT（单个文档）' },
                  { value: 'LINES', label: 'LINES（每行一个）' },
                ]}
              />
            </Form.Item>
          )}

          {outFormat === 'CSV' && (
            <Space wrap size={12} align="start">
              <Form.Item name="out_delimiter" label="输出字段分隔符">
                <Input style={{ width: 90 }} />
              </Form.Item>
              <Form.Item name="out_quote" label="输出引号字符">
                <Input style={{ width: 90 }} />
              </Form.Item>
              <Form.Item name="out_quote_fields" label="引号策略">
                <Select
                  style={{ width: 140 }}
                  options={[
                    { value: 'ASNEEDED', label: 'ASNEEDED' },
                    { value: 'ALWAYS', label: 'ALWAYS' },
                  ]}
                />
              </Form.Item>
            </Space>
          )}

          <Space wrap size={12} align="start">
            <Form.Item name="scan_start" label="ScanRange 起点（字节）">
              <InputNumber min={0} style={{ width: 160 }} />
            </Form.Item>
            <Form.Item name="scan_end" label="ScanRange 终点（字节）">
              <InputNumber min={0} style={{ width: 160 }} />
            </Form.Item>
            <Typography.Text type="secondary" style={{ fontSize: 12 }}>
              JSON DOCUMENT 输入不支持 ScanRange；上限 256 MiB / 10000 行
            </Typography.Text>
          </Space>
        </Form>
      </Card>

      {run.data && (
        <>
          <Card style={{ marginBottom: 16 }}>
            <Space size={24} wrap>
              <Typography.Text>扫描字节：{run.data.stats.bytes_scanned}</Typography.Text>
              <Typography.Text>处理字节：{run.data.stats.bytes_processed}</Typography.Text>
              <Typography.Text>返回字节：{run.data.stats.bytes_returned}</Typography.Text>
              <Typography.Text>记录数：{records.length}</Typography.Text>
            </Space>
          </Card>
          <Card styles={{ body: { padding: 0 } }}>
            <Table<Row>
              rowKey="__k"
              dataSource={rows}
              columns={columnsFor(records)}
              scroll={{ x: 'max-content' }}
              pagination={{ pageSize: 20, showSizeChanger: false }}
            />
          </Card>
        </>
      )}
    </>
  )
}
