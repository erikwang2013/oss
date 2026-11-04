use std::collections::BTreeMap;

use super::super::{KeyComparator, TableTree};
use super::{TestIo, key, row};

#[derive(Clone, Copy)]
struct LeU64;

impl KeyComparator for LeU64 {
    fn compare(&self, a: &[u8], b: &[u8]) -> std::cmp::Ordering {
        let a = u64::from_le_bytes(a.try_into().unwrap());
        let b = u64::from_le_bytes(b.try_into().unwrap());
        a.cmp(&b)
    }
}

#[tokio::test]
async fn typed_comparator_drives_order_and_lookup() {
    let io = TestIo::default();
    let mut tree = TableTree::with_comparator(Box::new(LeU64));
    for n in [256u64, 1, 2, 65_536] {
        tree.stage_put(&n.to_le_bytes(), row(&n.to_le_bytes()));
    }
    tree.flush(&io).await.unwrap();
    assert_eq!(
        tree.get(&io, &256u64.to_le_bytes()).await.unwrap(),
        Some(256u64.to_le_bytes().to_vec())
    );
    let all = tree
        .scan_leaf(&io, &0u64.to_le_bytes()[..]..&u64::MAX.to_le_bytes()[..])
        .await
        .unwrap();
    let order: Vec<u64> = all
        .iter()
        .map(|(k, _)| u64::from_le_bytes(k.as_slice().try_into().unwrap()))
        .collect();
    assert_eq!(order, vec![1, 2, 256, 65_536]);
}

struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.0
    }
}

#[tokio::test]
async fn randomized_ops_match_btreemap_oracle() {
    let io = TestIo::default();
    let mut tree = TableTree::new();
    let mut oracle: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    let mut rng = Lcg(0x5eed_1234);

    for step in 0..300 {
        let k = key((rng.next() % 80) as u32);
        if rng.next().is_multiple_of(3) {
            tree.stage_delete(&k);
            oracle.remove(&k);
        } else {
            let len = (rng.next() % 64) as usize;
            let v: Vec<u8> = (0..len).map(|i| (rng.next() + i as u64) as u8).collect();
            tree.stage_put(&k, row(&v));
            oracle.insert(k, v);
        }
        if step % 20 == 19 {
            tree.flush(&io).await.unwrap();
            let all: BTreeMap<Vec<u8>, Vec<u8>> = tree
                .scan(&io, &b""[..]..&b"zzzzz"[..])
                .await
                .unwrap()
                .into_iter()
                .collect();
            assert_eq!(all, oracle, "full scan divergence at step {step}");
            for i in 0..80 {
                let k = key(i);
                assert_eq!(
                    tree.get(&io, &k).await.unwrap(),
                    oracle.get(&k).cloned(),
                    "point divergence at step {step} key {i}"
                );
            }
            assert_eq!(tree.rows(), oracle.len() as u64);
            assert_eq!(
                tree.bytes(),
                oracle.values().map(|v| v.len() as u64).sum::<u64>()
            );
        }
    }
    tree.flush(&io).await.unwrap();
    let reopened = TableTree::from_root(tree.root_hash().unwrap(), tree.rows(), tree.bytes());
    let all: BTreeMap<Vec<u8>, Vec<u8>> = reopened
        .scan(&io, &b""[..]..&b"zzzzz"[..])
        .await
        .unwrap()
        .into_iter()
        .collect();
    assert_eq!(all, oracle);
}
