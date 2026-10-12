//! 多盘寻址与分发（M2b-2）。
//!
//! 铁律⑤：分片位置只存**拓扑下标**——分片 → 盘的映射是确定性函数
//! （`drive_index`），盘路径由下标解引用 drives[i]，换挂载点/换盘不影响寻址。
//!
//! 每盘持有完整命名空间：对象目录（含 xl.meta 副本）在各盘同构存在；
//! 版本目录 `<drive>/<bucket>/<展平键>/<uuid>/` 里放**落到本盘的那些分片**。
//! 单盘形态（drives.len() == 1）下所有分片都在 drive 0，与 M2b-1 兼容。

use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::layout;
use crate::meta::ShardRef;

/// 分片 → 盘下标（确定性；单盘恒 0）。M2b 单 set = 全部盘；多 set 是 M3 的事。
pub fn drive_index(shard: u8, n_drives: usize) -> usize {
    (shard as usize) % n_drives.max(1)
}

/// 对象 → set 映射（设计 §5.3 的确定性哈希）。
/// **留形不留行为**：M2b 单 set（sets == 1）取模恒 0；M3 引入多 set / rebalance 时本公式直接生效。
#[allow(dead_code)]
pub fn set_index(bucket: &str, key: &str, sets: usize) -> usize {
    let h = crc32c::crc32c(format!("{bucket}/{key}").as_bytes());
    (h as usize) % sets.max(1)
}

/// 分片在指定盘上的路径：`<drive>/<bucket>/<展平键>/<uuid>/part.<part>.<shard>`。
pub fn shard_path(
    drive: &Path,
    bucket: &str,
    key: &str,
    uuid: &str,
    s: &ShardRef,
) -> Result<PathBuf> {
    Ok(layout::object_dir(drive, bucket, key)?
        .join(uuid)
        .join(format!("part.{}.{}", s.part, s.shard)))
}

/// 盘下标 → 盘根（越界返回 None——meta 里的 drive 下标是外部输入，须防呆）。
pub fn drive_root(drives: &[PathBuf], index: u8) -> Option<&Path> {
    drives.get(index as usize).map(PathBuf::as_path)
}

/// 盘存活探测（探针最小版）：`.oss.sys` 目录存在即存活——它在 `Store::open` 时建于每盘。
///
/// 为什么写盘前必须探测：挂载点掉线后，挂载点**目录**仍留在父文件系统上，
/// 直接 `create_dir_all` + 写会把数据写进根盘（并误当写成功）。探测为「不在册」的盘
/// 一律跳过（不创建任何目录），由 quorum 计数消化。
pub async fn drive_up(drive: &Path) -> bool {
    matches!(
        tokio::fs::try_exists(drive.join(layout::SYS_DIR)).await,
        Ok(true)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drive_index_is_deterministic_and_single_drive_is_zero() {
        // 单盘：全部分片落 0（M2b-1 兼容）
        for i in 0..12u8 {
            assert_eq!(drive_index(i, 1), 0);
        }
        // 12 盘：一片一盘
        for i in 0..12u8 {
            assert_eq!(drive_index(i, 12), i as usize);
        }
        // 13 盘：分片铺满前 12 盘（第 13 盘不用）
        assert_eq!(drive_index(12, 13), 12);
        assert_eq!(drive_index(0, 13), 0);
    }

    #[test]
    fn set_index_single_set_is_zero_and_deterministic() {
        assert_eq!(set_index("bucket", "a/b.txt", 1), 0);
        // 多 set 时同键恒同 set（rebalance 之外不允许漂移）
        let a = set_index("bucket", "a/b.txt", 7);
        assert_eq!(a, set_index("bucket", "a/b.txt", 7));
        assert!(a < 7);
    }

    #[test]
    fn shard_path_uses_topology_index_only() {
        let p = shard_path(
            Path::new("/d0"),
            "bucket",
            "a/b.txt",
            "uuid-1",
            &ShardRef {
                drive: 3,
                part: 1,
                shard: 5,
                size: 10,
                crc32c: 0,
            },
        )
        .unwrap();
        assert_eq!(p, Path::new("/d0/bucket/a__XLDIR__b.txt/uuid-1/part.1.5"));
        // drive 下标是数据不是路径：shard_path 的盘由调用方解引用，这里不接受 drive 字段拼路径
        assert!(drive_root(&[PathBuf::from("/d0")], 3).is_none());
    }
}
