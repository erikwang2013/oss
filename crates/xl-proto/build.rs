// 编译 proto/oss.proto → oss.v1 模块（含 client/server 代码）。
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/oss.proto");
    tonic_build::compile_protos("proto/oss.proto")?;
    Ok(())
}
