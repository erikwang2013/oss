//! M8：Inventory Parquet 输出、Hive `symlink.txt` 清单与失败清理。
//!
//! 用低层 `parquet` reader（无 Arrow）读回真实落盘文件，校验 schema/类型/行组。

mod common;

use std::sync::Arc;

use bytes::Bytes;
use common::state;
use oss_server::inventory::enqueue_run;
use oss_server::jobs::{JobEnv, worker_once};
use oss_server::state::AppState;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field as PField, Row};
use s3s::dto::*;
use xl_store::store::ListRequest;

const ENV: JobEnv = JobEnv {
    workers: 1,
    interval_secs: 1,
    max_attempts: 1,
    timeout_secs: 60,
    tier_timeout_secs: 30,
};

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

async fn boot(dir: &tempfile::TempDir) -> (AppState, i64) {
    let st = state(dir.path()).await;
    let uid = st.db.create_user("erik", "pw", true).await.unwrap();
    (st, uid)
}

async fn add_bucket(st: &AppState, name: &str) {
    st.store.create_bucket(name).await.unwrap();
    st.db.insert_bucket(name, None).await.unwrap();
}

fn cfg_parquet(id: &str, dest: &str) -> InventoryConfiguration {
    InventoryConfiguration {
        destination: InventoryDestination {
            s3_bucket_destination: InventoryS3BucketDestination {
                account_id: None,
                bucket: dest.to_string(),
                encryption: None,
                format: InventoryFormat::from_static(InventoryFormat::PARQUET),
                prefix: None,
            },
        },
        filter: None,
        id: id.to_string(),
        included_object_versions: InventoryIncludedObjectVersions::from_static(
            InventoryIncludedObjectVersions::CURRENT,
        ),
        is_enabled: true,
        optional_fields: None,
        schedule: InventorySchedule {
            frequency: InventoryFrequency::from("Daily".to_string()),
        },
    }
}

fn with_optional(mut c: InventoryConfiguration, fields: &[&str]) -> InventoryConfiguration {
    c.optional_fields = Some(
        fields
            .iter()
            .map(|f| InventoryOptionalField::from(f.to_string()))
            .collect(),
    );
    c
}

async fn put(st: &AppState, uid: i64, bucket: &str, c: &InventoryConfiguration) {
    st.inventory
        .put_config(
            bucket,
            &xl_s3::access::AuthedUser {
                uid,
                is_admin: true,
                ak: "test-ak".into(),
            },
            c,
        )
        .await
        .unwrap();
}

async fn run_once(st: &AppState) {
    let got = worker_once(&st.db, &st.store, st.inventory_runner.as_ref(), ENV, now())
        .await
        .unwrap();
    assert!(got, "worker 应领到 inventory 任务");
}

/// 手动入队一次运行并跑完；返回 manifest 与数据字节。
async fn run_manual(st: &AppState, uid: i64, run_key: &str) -> (serde_json::Value, Vec<u8>) {
    enqueue_run(&st.db, "src", "c1", run_key, Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(st).await;
    let run = st
        .db
        .inventory_run_by_key("src", "c1", run_key)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.state, "done", "run error: {:?}", run.error);
    let mkey = run.manifest_key.expect("manifest_key");
    let (_, mbytes) = st.store.get_object("dst", &mkey).await.unwrap();
    let manifest: serde_json::Value = serde_json::from_slice(&mbytes).unwrap();
    let dkey = manifest["files"][0]["key"].as_str().unwrap();
    let (_, data) = st.store.get_object("dst", dkey).await.unwrap();
    (manifest, data.to_vec())
}

fn reader(data: &[u8]) -> SerializedFileReader<Bytes> {
    SerializedFileReader::new(Bytes::copy_from_slice(data)).unwrap()
}

fn parquet_rows(data: &[u8]) -> Vec<Row> {
    reader(data)
        .get_row_iter(None)
        .unwrap()
        .map(|r| r.unwrap())
        .collect()
}

fn field<'a>(row: &'a Row, name: &str) -> &'a PField {
    row.get_column_iter()
        .find(|(n, _)| n.as_str() == name)
        .map(|(_, f)| f)
        .unwrap_or_else(|| panic!("缺列 {name}"))
}

fn schema_names(data: &[u8]) -> Vec<String> {
    reader(data)
        .metadata()
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .map(|c| c.name().to_string())
        .collect()
}

const PARQUET_COLUMNS: [&str; 10] = [
    "bucket",
    "key",
    "version_id",
    "is_latest",
    "is_delete_marker",
    "size",
    "last_modified_date",
    "e_tag",
    "storage_class",
    "replication_status",
];

const PARQUET_SCHEMA: &str = "\
message s3.inventory {
  required binary bucket (UTF8);
  required binary key (UTF8);
  optional binary version_id (UTF8);
  optional boolean is_latest;
  optional boolean is_delete_marker;
  optional int64 size;
  optional int64 last_modified_date (TIMESTAMP_MILLIS);
  optional binary e_tag (UTF8);
  optional binary storage_class (UTF8);
  optional binary replication_status (UTF8);
}";

#[tokio::test]
async fn empty_bucket_schema_and_hive_symlink_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src").await;
    add_bucket(&st, "dst").await;
    let mut c = with_optional(cfg_parquet("c1", "dst"), &["ReplicationStatus"]);
    c.destination.s3_bucket_destination.prefix = Some("inv/parquet".into());
    put(&st, uid, "src", &c).await;

    let key = "manual-empty";
    let (manifest, data) = run_manual(&st, uid, key).await;
    assert_eq!(manifest["fileFormat"], "Parquet");
    assert_eq!(manifest["fileSchema"], PARQUET_SCHEMA);
    assert_eq!(
        manifest["creationTimestamp"].as_str().unwrap().len(),
        24,
        "ISO8601 UTC 毫秒"
    );
    let dkey = manifest["files"][0]["key"].as_str().unwrap();
    assert!(dkey.ends_with(".parquet"), "{dkey}");
    assert_eq!(
        manifest["files"][0]["size"].as_u64().unwrap(),
        data.len() as u64
    );
    let digest = {
        use base64::Engine as _;
        use md5::{Digest as _, Md5};
        base64::engine::general_purpose::STANDARD.encode(Md5::digest(&data))
    };
    assert_eq!(manifest["files"][0]["md5Checksum"], digest);

    // schema 用 lower_snake_case；空桶 = 合法 0 行文件、0 行组。
    assert_eq!(schema_names(&data), PARQUET_COLUMNS);
    let r = reader(&data);
    assert_eq!(r.metadata().num_row_groups(), 0);
    assert!(parquet_rows(&data).is_empty());
    let head = st.store.head_object("dst", dkey).await.unwrap();
    assert_eq!(
        head.content_type.as_deref(),
        Some("application/vnd.apache.parquet")
    );

    // Hive 清单：同目录树 hive/dt=YYYY-MM-DD-HH-MM/symlink.txt，一行 S3 URI。
    let base = format!("inv/parquet/src/c1/{key}");
    let listed = st
        .store
        .list_objects(
            "dst",
            &ListRequest {
                prefix: Some(format!("{base}/hive/")),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(listed.objects.len(), 1, "应有且仅有一份 symlink.txt");
    let hive_key = &listed.objects[0].key;
    let dt = hive_key
        .split('/')
        .find(|s| s.starts_with("dt="))
        .expect("dt 分区");
    assert_eq!(dt.len(), "dt=YYYY-MM-DD-HH-MM".len(), "{dt}");
    let (_, body) = st.store.get_object("dst", hive_key).await.unwrap();
    assert_eq!(
        std::str::from_utf8(&body).unwrap(),
        format!("s3://dst/{dkey}\n")
    );
}

#[tokio::test]
async fn parquet_round_trip_rows_and_types() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src").await;
    add_bucket(&st, "dst").await;
    let t0 = now();
    st.store
        .put_object("src", "b/c.txt", Bytes::from_static(b""), None)
        .await
        .unwrap();
    st.store
        .put_object("src", "a.txt", Bytes::from_static(b"abc"), None)
        .await
        .unwrap();
    put(
        &st,
        uid,
        "src",
        &with_optional(
            cfg_parquet("c1", "dst"),
            &["ReplicationStatus", "ChecksumAlgorithm"],
        ),
    )
    .await;

    let (manifest, data) = run_manual(&st, uid, "manual-rows").await;
    let schema = manifest["fileSchema"].as_str().unwrap();
    assert!(schema.contains("  optional binary checksum_algorithm (UTF8);"));
    assert_eq!(
        schema_names(&data),
        PARQUET_COLUMNS
            .iter()
            .copied()
            .chain(["checksum_algorithm"])
            .collect::<Vec<_>>()
    );

    let rows = parquet_rows(&data);
    assert_eq!(rows.len(), 2);
    let keys: Vec<&str> = rows
        .iter()
        .map(|r| match field(r, "key") {
            PField::Str(s) => s.as_str(),
            other => panic!("key 类型错误：{other:?}"),
        })
        .collect();
    assert_eq!(keys, vec!["a.txt", "b/c.txt"]);

    let a = &rows[0];
    assert_eq!(field(a, "bucket"), &PField::Str("src".into()));
    assert_eq!(field(a, "size"), &PField::Long(3));
    assert_eq!(field(a, "is_latest"), &PField::Bool(true));
    assert_eq!(field(a, "is_delete_marker"), &PField::Bool(false));
    assert_eq!(field(a, "storage_class"), &PField::Str("STANDARD".into()));
    // 未开启版本控制的对象沿用存储层既有语义（与 CSV 一致：字面量 "null"）。
    assert_eq!(field(a, "version_id"), &PField::Str("null".into()));
    assert!(matches!(field(a, "replication_status"), PField::Null));
    assert!(matches!(field(a, "checksum_algorithm"), PField::Null));
    match field(a, "e_tag") {
        PField::Str(s) => assert!(!s.is_empty()),
        other => panic!("e_tag 类型错误：{other:?}"),
    }
    match field(a, "last_modified_date") {
        PField::TimestampMillis(ms) => {
            assert!(*ms >= t0 * 1000, "mtime 应为毫秒时间戳：{ms}");
            assert!(*ms <= (now() + 60) * 1000);
        }
        other => panic!("last_modified_date 类型错误：{other:?}"),
    }
    // 空对象也必须写成长度 0（不是 null）。
    assert_eq!(field(&rows[1], "size"), &PField::Long(0));
}

#[tokio::test]
async fn large_output_spans_multiple_row_groups() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src").await;
    add_bucket(&st, "dst").await;
    let store = Arc::clone(&st.store);
    use futures::StreamExt as _;
    futures::stream::iter(0..8200usize)
        .map(|i| {
            let store = Arc::clone(&store);
            async move {
                store
                    .put_object(
                        "src",
                        &format!("obj/o{i:05}"),
                        Bytes::from_static(b"x"),
                        None,
                    )
                    .await
                    .unwrap();
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<()>>()
        .await;
    put(&st, uid, "src", &cfg_parquet("c1", "dst")).await;

    let (manifest, data) = run_manual(&st, uid, "manual-large").await;
    assert_eq!(manifest["fileFormat"], "Parquet");
    let r = reader(&data);
    assert_eq!(
        r.metadata().num_row_groups(),
        2,
        "8192 行/组：8200 行 = 2 组"
    );
    let rows = parquet_rows(&data);
    assert_eq!(rows.len(), 8200);
    assert_eq!(field(&rows[0], "key"), &PField::Str("obj/o00000".into()));
    assert_eq!(field(&rows[8199], "key"), &PField::Str("obj/o08199".into()));
}

#[tokio::test]
async fn destination_deny_leaves_no_parquet_objects() {
    let dir = tempfile::tempdir().unwrap();
    let (st, uid) = boot(&dir).await;
    add_bucket(&st, "src").await;
    add_bucket(&st, "dst").await;
    st.store
        .put_object("src", "a.txt", Bytes::from_static(b"x"), None)
        .await
        .unwrap();
    put(&st, uid, "src", &cfg_parquet("c1", "dst")).await;
    let deny = serde_json::json!({"Version":"2012-10-17","Statement":[{
        "Effect":"Deny","Principal":{"AWS":"*"},"Action":"s3:PutObject",
        "Resource":"arn:aws:s3:::dst/*"}]})
    .to_string();
    st.db.set_bucket_policy("dst", &deny, 0).await.unwrap();

    enqueue_run(&st.db, "src", "c1", "manual-deny", Some(uid), now())
        .await
        .unwrap()
        .unwrap();
    run_once(&st).await;
    let run = st
        .db
        .inventory_run_by_key("src", "c1", "manual-deny")
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.state, "error");
    assert_eq!(run.manifest_key, None);
    assert!(run.error.as_deref().unwrap_or("").contains("PutObject"));
    assert!(
        st.store
            .list_objects("dst", &Default::default())
            .await
            .unwrap()
            .objects
            .is_empty(),
        "失败后不得留下 data/manifest/hive 任一对象"
    );
}
