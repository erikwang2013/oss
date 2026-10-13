//! 节点间 gRPC 协议（oss.v1）：分片 / meta / 版本目录 / 回收 / 桶 / 列表 / 健康 / 锁 / 控制面。

// tonic 生成的 client 签名 Err 为 tonic::Status（176B），框架固有形态——在生成模块内放行；
// 平铺 re-export 保持原路径（xl_proto::mesh_client::MeshClient）。
mod generated {
    #![allow(clippy::result_large_err)]
    tonic::include_proto!("oss.v1");
}
pub use generated::*;

/// 本 crate 版本号，供 NodeHealth.version 上报。
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

#[cfg(test)]
mod tests {
    #[test]
    fn generated_surface_present() {
        // 类型存在性由编译保证；运行时仅断言版本常量已接通。
        let _ = std::any::type_name::<super::mesh_client::MeshClient<tonic::transport::Channel>>();
        let _ = std::any::type_name::<super::mesh_server::MeshServer<()>>();
        let _ = std::any::type_name::<super::NodeHealth>();
        assert_eq!(super::VERSION, env!("CARGO_PKG_VERSION"));
    }
}
