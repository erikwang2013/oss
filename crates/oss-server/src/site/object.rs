//! Object-backend M7 apply/ship paths, dispatched by `site.rs` when the
//! control backend is `objects`. SQLite keeps its original raw-SQL path.

mod apply;
mod entity;
mod pair;
mod ship;

pub(crate) use apply::apply_changelog;
pub(crate) use pair::insert_peer_with_snapshot;
pub(crate) use ship::ship_once;

#[cfg(test)]
mod tests;
