//! M6 切片 2：S3 Select 服务线级测试——handler 全链（事件流序列/Stats/Progress/ScanRange/
//! GZIP/SSE-C/边界 400）。帧的线上成帧属 s3s（R4 口径），此处钉**事件序列**。

use std::sync::Arc;

use bytes::Bytes;
use futures::StreamExt as _;
use s3s::dto::*;
use s3s::{S3, S3Request};
use xl_s3::service::OssS3;
use xl_store::store::{Store, StoreConfig};

/// 加密接线版 service（SSE-C 用例需要 master key；其余用例同路径）。
async fn service() -> (OssS3, tempfile::TempDir) {
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(StoreConfig::new(dir.path())).await.unwrap());
    let sys = xl_store::layout::sys_dir(dir.path(), "");
    store
        .init_encryption(Some(sys.join("master.key")), Some(sys.join("kms")))
        .await
        .unwrap();
    (OssS3::new(store, acl().await, 16 * 1024 * 1024), dir)
}

/// 最小判权库（直调 service 方法、不走 s3s 授权链）。
async fn acl() -> xl_s3::access::Acl {
    let pool = sqlx::SqlitePool::connect("sqlite::memory:").await.unwrap();
    sqlx::raw_sql(
        "CREATE TABLE buckets (name TEXT PRIMARY KEY, owner_id INTEGER, creation_date INTEGER NOT NULL, versioning TEXT NOT NULL DEFAULT 'Off', set_id INTEGER NOT NULL DEFAULT 0, object_lock INTEGER NOT NULL DEFAULT 0, object_lock_default_mode TEXT, object_lock_default_days INTEGER, default_encryption TEXT, replication_mode TEXT NOT NULL DEFAULT 'async', deleting INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE permissions (user_id INTEGER NOT NULL, bucket TEXT NOT NULL, \
          can_read INTEGER NOT NULL DEFAULT 0, can_write INTEGER NOT NULL DEFAULT 0, can_delete INTEGER NOT NULL DEFAULT 0, \
          PRIMARY KEY (user_id, bucket));\
         CREATE TABLE bucket_policies (bucket TEXT PRIMARY KEY, policy_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);\
         CREATE TABLE audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, ts INTEGER NOT NULL, uid INTEGER NOT NULL, op TEXT NOT NULL, bucket TEXT, object_key TEXT, decision TEXT NOT NULL, detail TEXT);\
         CREATE TABLE access_keys (access_key TEXT PRIMARY KEY, secret_key TEXT NOT NULL, user_id INTEGER NOT NULL, \
          status TEXT NOT NULL DEFAULT 'active', expires_at INTEGER, created_at INTEGER NOT NULL, \
          parent_ak TEXT, session_policy TEXT, kind TEXT NOT NULL DEFAULT 'long-term');\
         CREATE TABLE event_configs (bucket TEXT PRIMARY KEY, config_json TEXT NOT NULL, updated_at INTEGER NOT NULL DEFAULT 0);",
    )
    .execute(&pool)
    .await
    .unwrap();
    xl_s3::access::Acl::new(pool)
}

fn req<T>(input: T) -> S3Request<T> {
    S3Request {
        input,
        method: http::Method::GET,
        uri: "/".parse().unwrap(),
        headers: Default::default(),
        extensions: Default::default(),
        credentials: None,
        region: None,
        service: None,
        trailing_headers: None,
    }
}

fn sel_request(
    expression: &str,
    csv_in: bool,
    json_lines: bool,
    gzip: bool,
) -> SelectObjectContentRequest {
    let input_serialization = if csv_in {
        InputSerialization {
            csv: Some(CSVInput::default()),
            compression_type: Some(CompressionType::from_static(if gzip {
                "GZIP"
            } else {
                "NONE"
            })),
            json: None,
            parquet: None,
        }
    } else {
        InputSerialization {
            csv: None,
            compression_type: Some(CompressionType::from_static(if gzip {
                "GZIP"
            } else {
                "NONE"
            })),
            json: Some(JSONInput {
                type_: Some(JSONType::from_static(if json_lines {
                    "LINES"
                } else {
                    "DOCUMENT"
                })),
            }),
            parquet: None,
        }
    };
    SelectObjectContentRequest {
        expression: expression.to_string(),
        expression_type: ExpressionType::from_static("SQL"),
        input_serialization,
        output_serialization: OutputSerialization {
            csv: Some(CSVOutput::default()),
            json: None,
        },
        request_progress: None,
        scan_range: None,
    }
}

async fn put_object(s3: &OssS3, bucket: &str, key: &str, body: &[u8]) {
    s3.put_object(req(PutObjectInput {
        bucket: bucket.into(),
        key: key.into(),
        body: Some(StreamingBlob::from_bytes(Bytes::copy_from_slice(body))),
        ..Default::default()
    }))
    .await
    .unwrap();
}

fn select_input(
    bucket: &str,
    key: &str,
    request: SelectObjectContentRequest,
) -> SelectObjectContentInput {
    SelectObjectContentInput {
        bucket: bucket.into(),
        key: key.into(),
        request,
        expected_bucket_owner: None,
        sse_customer_algorithm: None,
        sse_customer_key: None,
        sse_customer_key_md5: None,
    }
}

async fn collect_events(out: SelectObjectContentOutput) -> Vec<SelectObjectContentEvent> {
    out.payload
        .expect("必须有事件流")
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .map(|r| r.unwrap())
        .collect()
}

async fn run_select(
    s3: &OssS3,
    bucket: &str,
    key: &str,
    request: SelectObjectContentRequest,
) -> Vec<SelectObjectContentEvent> {
    let out = s3
        .select_object_content(req(select_input(bucket, key, request)))
        .await
        .unwrap()
        .output;
    collect_events(out).await
}

fn records(evs: &[SelectObjectContentEvent]) -> Vec<u8> {
    let mut out = Vec::new();
    for e in evs {
        if let SelectObjectContentEvent::Records(r) = e {
            out.extend_from_slice(r.payload.as_ref().expect("Records 载荷"));
        }
    }
    out
}

#[tokio::test]
async fn select_csv_filter_stats_and_end() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "selb".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let data = b"a,10\na,20\nb,30\n";
    put_object(&s3, "selb", "d.csv", data).await;
    let evs = run_select(
        &s3,
        "selb",
        "d.csv",
        sel_request(
            "SELECT s._1, s._2 FROM S3Object WHERE s._2 > 15",
            true,
            false,
            false,
        ),
    )
    .await;
    assert_eq!(records(&evs), b"a,20\nb,30\n".to_vec());
    // Stats 恒等式 + End 收尾
    let stats = evs
        .iter()
        .find_map(|e| {
            if let SelectObjectContentEvent::Stats(s) = e {
                s.details.as_ref().map(|d| {
                    (
                        d.bytes_scanned.unwrap(),
                        d.bytes_processed.unwrap(),
                        d.bytes_returned.unwrap(),
                    )
                })
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(stats.0, data.len() as i64);
    assert_eq!(stats.1, data.len() as i64);
    assert_eq!(stats.2, 10);
    assert!(matches!(evs.last(), Some(SelectObjectContentEvent::End(_))));
}

#[tokio::test]
async fn select_progress_rhythm_frames() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "selp".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // >1MiB 输入（进度节奏约 1MiB 一帧）
    let mut data = String::with_capacity(1_300_000);
    let mut i = 0usize;
    while data.len() < 1_200_000 {
        data.push_str(&format!("k{i},v{i}\n"));
        i += 1;
    }
    put_object(&s3, "selp", "big.csv", data.as_bytes()).await;
    let mut r = sel_request("SELECT s._1 FROM S3Object", true, false, false);
    r.request_progress = Some(RequestProgress {
        enabled: Some(true),
    });
    let evs = run_select(&s3, "selp", "big.csv", r).await;
    let n_progress = evs
        .iter()
        .filter(|e| matches!(e, SelectObjectContentEvent::Progress(_)))
        .count();
    // 1.2MB 输入 → 恰好 floor(1.2M/1MiB)=1（钉 ~1MiB 节奏）
    assert_eq!(n_progress, 1, "–request-progress 应恰好 1 帧 Progress");
    // 关闭时零 Progress
    let evs = run_select(
        &s3,
        "selp",
        "big.csv",
        sel_request("SELECT s._1 FROM S3Object", true, false, false),
    )
    .await;
    assert!(
        evs.iter()
            .all(|e| !matches!(e, SelectObjectContentEvent::Progress(_))),
        "未启用不得发 Progress"
    );
    // 输出多帧：s._1 投影输出约 0.8MB → Records ≥ 2 帧，且每帧 ≤256KiB
    let n_records = evs
        .iter()
        .filter(|e| matches!(e, SelectObjectContentEvent::Records(_)))
        .count();
    assert!(n_records >= 2, "大输出应多 Records 帧（实得 {n_records}）");
    for e in &evs {
        if let SelectObjectContentEvent::Records(r) = e {
            assert!(r.payload.as_ref().unwrap().len() <= 256 * 1024);
        }
    }
}

#[tokio::test]
async fn select_json_lines_and_output() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "selj".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let data = b"{\"name\":\"jo\",\"n\":7}\n{\"name\":\"al\",\"n\":9}\n";
    put_object(&s3, "selj", "d.json", data).await;
    let evs = run_select(
        &s3,
        "selj",
        "d.json",
        SelectObjectContentRequest {
            expression: "SELECT s.name FROM S3Object s WHERE s.n > 8".to_string(),
            expression_type: ExpressionType::from_static("SQL"),
            input_serialization: InputSerialization {
                csv: None,
                compression_type: Some(CompressionType::from_static("NONE")),
                json: Some(JSONInput {
                    type_: Some(JSONType::from_static("LINES")),
                }),
                parquet: None,
            },
            output_serialization: OutputSerialization {
                csv: None,
                json: Some(JSONOutput::default()),
            },
            request_progress: None,
            scan_range: None,
        },
    )
    .await;
    let text = String::from_utf8(records(&evs)).unwrap();
    let line: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
    assert_eq!(line["name"], "al");
}

#[tokio::test]
async fn select_scan_range_and_gzip() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "selr".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    put_object(&s3, "selr", "d.csv", b"a,1\nb,2\nc,3\n").await;
    // AWS 语义：记录首字节落在区间内则整条处理（非字节切片）
    let scan = |start: Option<i64>, end: Option<i64>| {
        let mut r = sel_request("SELECT * FROM S3Object", true, false, false);
        r.scan_range = Some(ScanRange { start, end });
        r
    };
    // 非对齐起点 2..=6：仅 b（首字节 4）命中；旧字节切片会多出半条 “1”
    let evs = run_select(&s3, "selr", "d.csv", scan(Some(2), Some(6))).await;
    assert_eq!(records(&evs), b"b,2\n".to_vec());
    // 对齐区间 4..=7
    let evs = run_select(&s3, "selr", "d.csv", scan(Some(4), Some(7))).await;
    assert_eq!(records(&evs), b"b,2\n".to_vec());
    // end 落在记录中间：b 首字节 4 在 [4,5] → 整条输出（不截断）
    let evs = run_select(&s3, "selr", "d.csv", scan(Some(4), Some(5))).await;
    assert_eq!(records(&evs), b"b,2\n".to_vec());
    // 仅给 End=4：AWS 解释为末尾 4 字节 [8..=11] → c（首字节 8）
    let evs = run_select(&s3, "selr", "d.csv", scan(None, Some(4))).await;
    assert_eq!(records(&evs), b"c,3\n".to_vec());
    // GZIP 输入
    {
        use flate2::Compression;
        use flate2::write::GzEncoder;
        use std::io::Write as _;
        let mut enc = GzEncoder::new(Vec::new(), Compression::default());
        enc.write_all(b"x,1\ny,2\n").unwrap();
        let gz = enc.finish().unwrap();
        put_object(&s3, "selr", "d.csv.gz", &gz).await;
        let evs = run_select(
            &s3,
            "selr",
            "d.csv.gz",
            sel_request("SELECT s._2 FROM S3Object", true, false, true),
        )
        .await;
        assert_eq!(records(&evs), b"1\n2\n".to_vec());
        // R4：GZIP 下 BytesScanned=压缩后、BytesProcessed=解压后（恒等式可区分）
        let stats = evs
            .iter()
            .find_map(|e| {
                if let SelectObjectContentEvent::Stats(s) = e {
                    s.details
                        .as_ref()
                        .map(|d| (d.bytes_scanned.unwrap(), d.bytes_processed.unwrap()))
                } else {
                    None
                }
            })
            .unwrap();
        assert_eq!(stats.0, gz.len() as i64);
        assert_eq!(stats.1, 8);
    }
}

#[tokio::test]
async fn select_utf8_comments_and_quoted_newline() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "selu".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    // UTF-8 原样往返
    put_object(&s3, "selu", "u.csv", "héllo,1\n".as_bytes()).await;
    let evs = run_select(
        &s3,
        "selu",
        "u.csv",
        sel_request("SELECT s._1 FROM S3Object", true, false, false),
    )
    .await;
    assert_eq!(records(&evs), "héllo\n".as_bytes().to_vec());
    // 注释缺省 '#'（AWS）：# 行不进入结果
    put_object(&s3, "selu", "c.csv", b"#skip\na,1\n").await;
    let evs = run_select(
        &s3,
        "selu",
        "c.csv",
        sel_request("SELECT * FROM S3Object", true, false, false),
    )
    .await;
    assert_eq!(records(&evs), b"a,1\n".to_vec());
    // 引号内换行：缺省 AllowQuotedRecordDelimiter=false → 400
    put_object(&s3, "selu", "q.csv", b"\"a\nb\",1\n").await;
    let e = s3
        .select_object_content(req(select_input(
            "selu",
            "q.csv",
            sel_request("SELECT * FROM S3Object", true, false, false),
        )))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    // 显式开启后放行
    let mut r = sel_request("SELECT * FROM S3Object", true, false, false);
    r.input_serialization
        .csv
        .as_mut()
        .unwrap()
        .allow_quoted_record_delimiter = Some(true);
    let evs = run_select(&s3, "selu", "q.csv", r).await;
    assert_eq!(records(&evs), b"\"a\nb\",1\n".to_vec());
}

#[tokio::test]
async fn select_aggregate_alias_json_wire() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "sela".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    put_object(&s3, "sela", "n.csv", b"1\n2\n").await;
    let out = s3
        .select_object_content(req(select_input(
            "sela",
            "n.csv",
            SelectObjectContentRequest {
                expression: "SELECT count(*) AS total FROM S3Object".to_string(),
                expression_type: ExpressionType::from_static("SQL"),
                input_serialization: InputSerialization {
                    csv: Some(CSVInput::default()),
                    compression_type: Some(CompressionType::from_static("NONE")),
                    json: None,
                    parquet: None,
                },
                output_serialization: OutputSerialization {
                    csv: None,
                    json: Some(JSONOutput::default()),
                },
                request_progress: None,
                scan_range: None,
            },
        )))
        .await
        .unwrap()
        .output;
    let evs = collect_events(out).await;
    let text = String::from_utf8(records(&evs)).unwrap();
    let v: serde_json::Value = serde_json::from_str(text.trim()).unwrap();
    assert_eq!(v["total"], 2, "聚合别名应成为 JSON 输出键");
    assert!(v.get("_1").is_none());
}

#[tokio::test]
async fn select_json_document_scan_rejected_and_bad_compression() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "selv".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    put_object(&s3, "selv", "d.json", b"[{\"a\":1},{\"a\":2}]").await;
    // ScanRange + JSON DOCUMENT → 400（不静默错结果）
    let mut r = sel_request("SELECT s.a FROM S3Object s", false, false, false);
    r.scan_range = Some(ScanRange {
        start: Some(0),
        end: Some(5),
    });
    let e = s3
        .select_object_content(req(select_input("selv", "d.json", r)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    // 未知压缩类型 → 400（文案不再误导为 BZIP2）
    let mut r = sel_request("SELECT * FROM S3Object", true, false, false);
    r.input_serialization.compression_type = Some(CompressionType::from_static("ZSTD"));
    let e = s3
        .select_object_content(req(select_input("selv", "d.json", r)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn select_boundary_errors_400() {
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "sele".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    put_object(&s3, "sele", "d.csv", b"a,1\n").await;
    // 白名单外语法 → InvalidRequest
    let e = s3
        .select_object_content(req(select_input(
            "sele",
            "d.csv",
            sel_request("SELECT nope(s._1) FROM S3Object", true, false, false),
        )))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    // 零参函数（曾 panic 的形态）→ 400，且与数据无关
    let e = s3
        .select_object_content(req(select_input(
            "sele",
            "d.csv",
            sel_request("SELECT lower() FROM S3Object", true, false, false),
        )))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    // GZIP + ScanRange 互斥
    let mut r = sel_request("SELECT * FROM S3Object", true, false, true);
    r.scan_range = Some(ScanRange {
        start: Some(0),
        end: None,
    });
    let e = s3
        .select_object_content(req(select_input("sele", "d.csv", r)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
    // BZIP2 → 400（记档延后）
    let mut r = sel_request("SELECT * FROM S3Object", true, false, false);
    r.input_serialization.compression_type = Some(CompressionType::from_static("BZIP2"));
    let e = s3
        .select_object_content(req(select_input("sele", "d.csv", r)))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidRequest);
}

#[tokio::test]
async fn select_sse_c_object_requires_key() {
    use base64::Engine as _;
    let (s3, _d) = service().await;
    s3.create_bucket(req(CreateBucketInput {
        bucket: "sels".into(),
        ..Default::default()
    }))
    .await
    .unwrap();
    let key = [5u8; 32];
    let k = xl_store::crypto::SseCKey::from_key(key);
    let b64 = base64::engine::general_purpose::STANDARD.encode(key);
    let md5 = k.md5_b64().to_string();
    s3.put_object(req(PutObjectInput {
        bucket: "sels".into(),
        key: "c.csv".into(),
        body: Some(StreamingBlob::from_bytes(Bytes::from_static(b"p,1\nq,2\n"))),
        sse_customer_algorithm: Some("AES256".into()),
        sse_customer_key: Some(b64.clone()),
        sse_customer_key_md5: Some(md5.clone()),
        ..Default::default()
    }))
    .await
    .unwrap();
    // 带钥 ✓
    let out = s3
        .select_object_content(req(SelectObjectContentInput {
            bucket: "sels".into(),
            key: "c.csv".into(),
            expected_bucket_owner: None,
            sse_customer_algorithm: Some("AES256".into()),
            sse_customer_key: Some(b64),
            sse_customer_key_md5: Some(md5),
            request: sel_request("SELECT * FROM S3Object", true, false, false),
        }))
        .await
        .unwrap()
        .output;
    let evs = collect_events(out).await;
    assert_eq!(records(&evs), b"p,1\nq,2\n".to_vec());
    // 缺钥 → 400
    let e = s3
        .select_object_content(req(select_input(
            "sels",
            "c.csv",
            sel_request("SELECT * FROM S3Object", true, false, false),
        )))
        .await
        .unwrap_err();
    assert_eq!(e.code(), &s3s::S3ErrorCode::InvalidArgument);
}
